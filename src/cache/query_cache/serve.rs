use std::time::Instant;

use ecow::EcoString;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::error::SendError;
use tokio_util::bytes::{Buf, Bytes};
use tracing::{debug, trace};

use crate::pg::protocol::encode::{
    BIND_COMPLETE_MSG, PARSE_COMPLETE_MSG, READY_FOR_QUERY_IDLE_MSG,
};
use crate::query::{Fingerprint, QueryShape};

use crate::cache::fast_path::{self, MvDecision};
use crate::cache::memo::{MemoHit, MemoKey, MemoShape};
use crate::cache::messages::{CacheOutcome, CacheReply, PipelineDescribe, slices_concat};
use crate::cache::mv::MvServe;
use crate::cache::types::SharedResolved;
use crate::cache::write_queue::WriteQueue;
use crate::cache::{CacheError, CacheResult};

use super::dispatch::reply_forward;
use super::{CacheDispatch, CoalescedClient, QueryRequest, QueryType, ServeJob, ServeRequest};

/// A pre-checked plan to serve a request from an in-process memo snapshot: the
/// matched snapshot plus which envelope frames to regenerate around its core.
struct MemoServe {
    hit: MemoHit,
    has_parse: bool,
    has_bind: bool,
    /// Whether the client expects a RowDescription (simple query, or extended
    /// with Describe). When false the stored leading RowDescription is sliced off.
    wants_row_description: bool,
    emit_rfq: bool,
}

impl CacheDispatch {
    /// Build and send a ServeRequest for serving a query from cache.
    /// Serve a Ready cache hit. Two backends for the same logical work: the
    /// in-process memo (served inline on the connection thread — no serve hop,
    /// no cache-DB round trip) when a live snapshot matches this request's
    /// (format, shape), otherwise a pool serve (from the MV table or source
    /// rows, decided here). Scoped to the single-request Ready path: coalesced
    /// groups and subsumption serves dispatch to the serve pool directly.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn hit_serve(
        &self,
        fingerprint: Fingerprint,
        msg: QueryRequest,
        resolved: SharedResolved,
        deparsed_sql: EcoString,
        serve_shape: Option<QueryShape>,
        generation: u64,
        rows_needed: Option<u64>,
    ) -> CacheResult<()> {
        if let Some(serve) = self.memo_serve_plan(fingerprint, &msg) {
            return self.memo_serve(msg, fingerprint, serve).await;
        }
        // No memo: hand off to the serve pool. `mv_dispatch_decide` picks the MV fast
        // path vs source-row fallthrough and, on a dirty MV, schedules a rebuild.
        let mv = self.mv_dispatch_decide(fingerprint, rows_needed);
        self.pool_serve(
            fingerprint,
            msg,
            resolved,
            deparsed_sql,
            serve_shape,
            generation,
            mv,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn pool_serve(
        &self,
        fingerprint: Fingerprint,
        msg: QueryRequest,
        resolved: SharedResolved,
        deparsed_sql: EcoString,
        serve_shape: Option<QueryShape>,
        generation: u64,
        mv: MvServe,
    ) -> CacheResult<()> {
        self.pool_serve_coalesced(
            fingerprint,
            msg,
            resolved,
            deparsed_sql,
            serve_shape,
            generation,
            mv,
            vec![],
        )
    }

    /// Build a plan to serve this Ready hit from an in-process memo, or `None`
    /// to fall through to the serve pool. Returns `None` for: memoization disabled,
    /// a non-keyable LIMIT/OFFSET shape, cold-path extended (no pipeline), a
    /// `Describe('S')` that needs a regenerated `ParameterDescription` (v1
    /// skips), a memo miss, or a stale snapshot (dropped by `get`).
    fn memo_serve_plan(&self, fingerprint: Fingerprint, msg: &QueryRequest) -> Option<MemoServe> {
        if !self.state_view.memo.enabled() {
            return None;
        }
        let (has_parse, has_bind, wants_row_description, emit_rfq) = match msg.query_type {
            QueryType::Simple => (false, false, true, true),
            QueryType::Extended => {
                let pipeline = msg.pipeline.as_ref()?;
                let wants_rd = match pipeline.describe {
                    PipelineDescribe::Statement => return None,
                    PipelineDescribe::Portal => true,
                    PipelineDescribe::None => false,
                };
                (
                    pipeline.has_parse,
                    pipeline.has_bind,
                    wants_rd,
                    pipeline.emit_rfq,
                )
            }
        };
        let binary = msg.result_formats.is_binary();
        let shape = MemoShape::from_limit(&msg.cacheable_query.query.limit)?;
        let key = MemoKey {
            fingerprint,
            binary,
            shape,
        };
        let hit = self.state_view.memo.get(&key)?;
        // A captured snapshot always carries a RowDescription; guard anyway so a
        // RowDescription-wanting client never gets a body with none.
        if wants_row_description && hit.rd_len == 0 {
            return None;
        }
        Some(MemoServe {
            hit,
            has_parse,
            has_bind,
            wants_row_description,
            emit_rfq,
        })
    }

    /// Serve a memo snapshot inline on the connection thread: regenerate the
    /// per-client envelope (ParseComplete / BindComplete / ReadyForQuery) around
    /// the cached core, slicing off the leading RowDescription when the client
    /// doesn't expect one, write it to the client, and return the socket.
    async fn memo_serve(
        &self,
        mut msg: QueryRequest,
        fingerprint: Fingerprint,
        serve: MemoServe,
    ) -> CacheResult<()> {
        let MemoServe {
            hit,
            has_parse,
            has_bind,
            wants_row_description,
            emit_rfq,
        } = serve;
        // The cached core is a refcounted `Bytes`; push it around the small
        // static envelope frames instead of memcpying the whole body. Slicing
        // off the leading RowDescription is a zero-copy `Bytes::slice`.
        let core: Bytes = if wants_row_description {
            hit.core
        } else {
            hit.core.slice(hit.rd_len.min(hit.core.len())..)
        };
        let mut buf = WriteQueue::new();
        if has_parse {
            buf.push(Bytes::from_static(PARSE_COMPLETE_MSG));
        }
        if has_bind {
            buf.push(Bytes::from_static(BIND_COMPLETE_MSG));
        }
        buf.push(core);
        if emit_rfq {
            buf.push(Bytes::from_static(READY_FOR_QUERY_IDLE_MSG));
        }

        let served = buf.remaining() as u64;
        trace!("memo serve {served} bytes inline");
        crate::metrics::handles().cache.memo_hits.increment(1);
        // Keep per-fingerprint served-byte volume in step with the serve path
        // (serve_metrics_record), so served bytes don't appear to drop to zero
        // as a fingerprint moves onto the memo fast path.
        if let Some(mut m) = self.state_view.metrics.get_mut(&fingerprint) {
            m.total_bytes_served += served;
        }
        msg.timing.query_done_at = Some(Instant::now());
        msg.timing.response_written_at = Some(Instant::now());
        if let Err(e) = msg.client_socket.write_all_buf(&mut buf).await {
            // The client is gone. Still reply `Complete` (not `Error`): the
            // response was already (partially) written, so forwarding to origin
            // would re-execute and double-respond; the connection detects the
            // dead socket on its next use and tears down.
            debug!("memo serve: client write failed: {e}");
        }
        msg.reply_tx
            .send(CacheReply {
                socket: msg.client_socket,
                outcome: CacheOutcome::Complete(Some(msg.timing)),
            })
            .map_err(|_| CacheError::Reply.into())
    }

    /// Inspect `mv_state` to decide whether this dispatch serves from the MV
    /// fast path.
    ///
    /// On `Pending { has_table }`, transitions to `Scheduled { has_table }` and
    /// sends `MvBuild`. The current request falls through to source-row eval;
    /// the next hit after the writer builds the MV gets the fast path.
    ///
    /// Fast path (Fresh / terminal / already-scheduled states) takes only a
    /// shared DashMap guard; the write guard is acquired only for the
    /// `Pending → Scheduled` flip.
    pub(super) fn mv_dispatch_decide(
        &self,
        fingerprint: Fingerprint,
        rows_needed: Option<u64>,
    ) -> MvServe {
        match fast_path::mv_serve_decide(&self.state_view, fingerprint, rows_needed) {
            MvDecision::Serve(mv) => mv,
            // CacheDispatch owns `query_tx`: flip Pending → Scheduled and dispatch
            // the build, then serve this request from source rows.
            MvDecision::NeedsSchedule { has_table } => {
                if let Some(cmd) = fast_path::mv_schedule(&self.state_view, fingerprint, has_table)
                {
                    let _ = self.query_tx.send(cmd);
                }
                MvServe::SourceRow
            }
        }
    }

    /// Build and send a ServeRequest with coalesced clients attached.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn pool_serve_coalesced(
        &self,
        fingerprint: Fingerprint,
        msg: QueryRequest,
        resolved: SharedResolved,
        deparsed_sql: EcoString,
        serve_shape: Option<QueryShape>,
        generation: u64,
        mv: MvServe,
        coalesced: Vec<CoalescedClient>,
    ) -> CacheResult<()> {
        // `lookup_complete_at` is stamped earlier in `query_dispatch` (and
        // copied through coalesce drains), so it's already set on
        // `msg.timing` at this point.
        let (
            emit_rfq,
            has_parse,
            has_bind,
            pipeline_describe,
            parameter_description,
            forward_bytes,
        ) = match msg.pipeline {
            Some(pipeline) => (
                pipeline.emit_rfq,
                pipeline.has_parse,
                pipeline.has_bind,
                pipeline.describe,
                pipeline.parameter_description,
                Some(pipeline.buffered_bytes),
            ),
            None => (false, false, false, PipelineDescribe::None, None, None),
        };

        if let Err(SendError(job)) = self.serve_tx.send(ServeJob::Query(ServeRequest {
            fingerprint,
            query_type: msg.query_type,
            data: msg.data,
            resolved,
            deparsed_sql,
            serve_shape,
            generation,
            mv,
            result_formats: msg.result_formats,
            client_socket: msg.client_socket,
            reply_tx: msg.reply_tx,
            timing: msg.timing,
            limit: msg.cacheable_query.query.limit.clone(),
            emit_rfq,
            has_parse,
            has_bind,
            pipeline_describe,
            parameter_description,
            forward_bytes,
            coalesced,
        })) {
            // Worker channel closed (cache subsystem torn down or restarting):
            // degrade gracefully by forwarding the query — and any coalesced
            // waiters — to origin rather than surfacing a hard cache error.
            debug!("serve channel closed; forwarding query to origin");
            let ServeJob::Query(req) = job else {
                return Ok(());
            };
            let buf = req
                .forward_bytes
                .map_or(req.data, |slices| slices_concat(&slices));
            let _ = reply_forward(req.reply_tx, req.client_socket, None, buf, req.timing);
            for c in req.coalesced {
                let _ = reply_forward(c.reply_tx, c.client_socket, None, c.data, c.timing);
            }
        }
        Ok(())
    }
}
