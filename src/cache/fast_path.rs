//! Shared cache-dispatch helpers.
//!
//! These functions are the building blocks of [`CacheDispatch::query_dispatch`]:
//! the allowlist check, the hit-path mutations (metrics, CLOCK bit), the MV
//! serve decision, and the worker-request construction. They operate on the
//! `Send` shared state ([`CacheStateView`], the worker channel) and are factored
//! out so the dispatch logic stays readable.

use std::num::NonZeroU64;
use std::sync::atomic::Ordering;

use ecow::EcoString;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot;
use tokio_util::bytes::BytesMut;
use tracing::{debug, error};

use crate::proxy::ClientSocket;
use crate::query::ast::{LimitClause, QueryExpr, TableNode};
use crate::settings::{Allowlist, CachePolicy};
use crate::timing::{QueryTiming, duration_to_ns_u64};

use super::{
    CacheResult,
    messages::{CacheOutcome, CacheReply, PipelineContext, PipelineDescribe, QueryCommand},
    mv::{MvServe, MvState},
    query::limit_is_sufficient,
    query_cache::{CoalescedClient, QueryType, WorkerRequest},
    types::{CacheStateView, SharedResolved},
};

/// MV serve decision.
pub(crate) enum MvDecision {
    Serve(MvServe),
    /// MV is `Pending`; needs a `Pending → Scheduled` flip plus an `MvBuild`
    /// send (owned by the dispatcher, which has `query_tx`).
    NeedsSchedule {
        has_table: bool,
    },
}

/// Check whether all tables in the query are in the allowlist. Returns true if
/// no allowlist is configured (all tables allowed).
pub(crate) fn query_allowlist_check(allowlist: &Allowlist, query: &QueryExpr) -> bool {
    let Some(entries) = allowlist else {
        return true;
    };
    query.nodes::<TableNode>().all(|t| {
        let table_name = t.name.to_lowercase();
        let table_schema = t.schema.as_ref().map(|s| s.to_lowercase());
        entries.iter().any(|(ws, wt)| {
            *wt == table_name
                && match ws {
                    Some(ws) => table_schema.as_deref() == Some(ws.as_str()),
                    None => true,
                }
        })
    })
}

/// Record a cache hit in the shared view: bump the GC hit counter and the
/// per-query metrics. Concurrency-safe (atomic + DashMap shard locks); called
/// inline from connection tasks.
pub(crate) fn metrics_hit_record(state_view: &CacheStateView, fingerprint: u64) {
    state_view.hits_since_gc.fetch_add(1, Ordering::Relaxed);
    if let Some(mut m) = state_view.metrics.get_mut(&fingerprint) {
        m.hit_count += 1;
        m.last_hit_at_ns = NonZeroU64::new(duration_to_ns_u64(state_view.started_at.elapsed()));
    }
}

/// Set the CLOCK reference bit for eviction tracking.
pub(crate) fn clock_reference_set(
    state_view: &CacheStateView,
    cache_policy: CachePolicy,
    fingerprint: &u64,
) {
    if cache_policy == CachePolicy::Clock
        && let Some(mut entry) = state_view.cached_queries.get_mut(fingerprint)
    {
        entry.referenced = true;
    }
}

/// Inspect `mv_state` to decide whether to serve from the MV fast path, source
/// rows, or (Pending) defer to the dispatcher for scheduling. The single site
/// for `mv_hits`/`mv_fallthrough` counting — including the `Pending` case, which
/// falls through to source rows while the dispatcher schedules the build.
pub(crate) fn mv_serve_decide(
    state_view: &CacheStateView,
    fingerprint: u64,
    rows_needed: Option<u64>,
) -> MvDecision {
    let observed = state_view
        .cached_queries
        .get(&fingerprint)
        .map(|e| (e.mv.state, e.mv.output_columns.clone(), e.mv.limit));

    match observed {
        None => MvDecision::Serve(MvServe::SourceRow),
        Some((MvState::Fresh, Some(cols), mv_limit))
            if limit_is_sufficient(mv_limit, rows_needed) =>
        {
            crate::metrics::handles().cache.mv_hits.increment(1);
            MvDecision::Serve(MvServe::Mv(cols))
        }
        Some((MvState::Fresh, Some(_), _)) => {
            crate::metrics::handles().cache.mv_fallthrough.increment(1);
            MvDecision::Serve(MvServe::SourceRow)
        }
        Some((MvState::Fresh, None, _)) => {
            error!(
                fingerprint,
                "MV is Fresh but output columns were never captured; serving from source rows"
            );
            crate::metrics::handles().cache.mv_fallthrough.increment(1);
            MvDecision::Serve(MvServe::SourceRow)
        }
        Some((MvState::Pending { has_table }, _, _)) => {
            crate::metrics::handles().cache.mv_fallthrough.increment(1);
            MvDecision::NeedsSchedule { has_table }
        }
        Some((MvState::Scheduled { .. }, _, _)) => {
            crate::metrics::handles().cache.mv_fallthrough.increment(1);
            MvDecision::Serve(MvServe::SourceRow)
        }
        Some((MvState::Skipped | MvState::Ineligible, _, _)) => {
            MvDecision::Serve(MvServe::SourceRow)
        }
    }
}

/// Check-and-transition under write guard: `Pending { has_table } → Scheduled
/// { has_table }`. Returns the command to send when the transition wins the
/// race; `None` when another dispatch beat us or the entry moved elsewhere.
pub(crate) fn mv_schedule(
    state_view: &CacheStateView,
    fingerprint: u64,
    has_table: bool,
) -> Option<QueryCommand> {
    let mut entry = state_view.cached_queries.get_mut(&fingerprint)?;
    if entry.mv.state != (MvState::Pending { has_table }) {
        return None;
    }
    entry.mv.state = MvState::Scheduled { has_table };
    Some(QueryCommand::MvBuild { fingerprint })
}

/// Fields for [`worker_request_send`].
pub(crate) struct WorkerSendParts {
    pub fingerprint: u64,
    pub query_type: QueryType,
    pub data: BytesMut,
    pub resolved: SharedResolved,
    pub deparsed_sql: EcoString,
    pub generation: u64,
    pub mv: MvServe,
    pub result_formats: Vec<i16>,
    pub client_socket: ClientSocket,
    pub reply_tx: oneshot::Sender<CacheReply>,
    pub timing: QueryTiming,
    pub limit: Option<LimitClause>,
    pub pipeline: Option<PipelineContext>,
    pub coalesced: Vec<CoalescedClient>,
}

/// Build and send a [`WorkerRequest`] to serve a query from cache.
pub(crate) fn worker_request_send(
    worker_tx: &UnboundedSender<WorkerRequest>,
    parts: WorkerSendParts,
) -> CacheResult<()> {
    let (emit_rfq, has_parse, has_bind, pipeline_describe, parameter_description, forward_bytes) =
        match parts.pipeline {
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

    if let Err(SendError(req)) = worker_tx.send(WorkerRequest {
        fingerprint: parts.fingerprint,
        query_type: parts.query_type,
        data: parts.data,
        resolved: parts.resolved,
        deparsed_sql: parts.deparsed_sql,
        generation: parts.generation,
        mv: parts.mv,
        result_formats: parts.result_formats,
        client_socket: parts.client_socket,
        reply_tx: parts.reply_tx,
        timing: parts.timing,
        limit: parts.limit,
        emit_rfq,
        has_parse,
        has_bind,
        pipeline_describe,
        parameter_description,
        forward_bytes,
        coalesced: parts.coalesced,
    }) {
        // Worker channel closed (cache subsystem torn down or restarting):
        // degrade gracefully by forwarding the query — and any coalesced
        // waiters — to origin rather than surfacing a hard cache error.
        debug!("worker channel closed; forwarding query to origin");
        origin_forward(
            req.reply_tx,
            req.client_socket,
            req.forward_bytes.unwrap_or(req.data),
            req.timing,
        );
        for c in req.coalesced {
            origin_forward(c.reply_tx, c.client_socket, c.data, c.timing);
        }
    }
    Ok(())
}

/// Hand a query back to the connection for origin forwarding. A failed send
/// means the client already departed, which is harmless here.
fn origin_forward(
    reply_tx: oneshot::Sender<CacheReply>,
    socket: ClientSocket,
    buf: BytesMut,
    timing: QueryTiming,
) {
    let _ = reply_tx.send(CacheReply {
        socket,
        outcome: CacheOutcome::Forward(buf, timing),
    });
}
