//! Inline cache-hit fast path (PGC: hop-1 elimination).
//!
//! A clean cache hit needs only a wait-free read of [`CacheStateView`] plus a
//! send to the worker — no coordinator-thread state. [`CacheFastPath`] bundles
//! the `Send` handles a connection thread needs to make that decision and
//! dispatch inline, skipping the connection→coordinator hop. Anything that is
//! not an obvious hit (miss, Loading, Invalidated, insufficient limit, MV needs
//! scheduling, CDC down) returns the message back to the caller, which routes
//! it to the coordinator exactly as before.
//!
//! The decision/serve helpers here are the single source of truth: the
//! coordinator's [`QueryCache::query_dispatch`] calls the same functions.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::num::NonZeroU64;
use std::time::Instant;

use ecow::EcoString;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio_util::bytes::BytesMut;
use tracing::error;

use crate::proxy::ClientSocket;
use crate::query::ast::{LimitClause, QueryExpr, TableNode, query_expr_fingerprint};
use crate::query::transform::query_expr_parameters_replace;
use crate::settings::{Allowlist, CachePolicy, DynamicConfigHandle};
use crate::timing::{QueryTiming, duration_to_ns_u64};

use super::{
    CacheError, CacheResult,
    messages::{CacheMessage, CacheReply, PipelineContext, PipelineDescribe, ProxyMessage, QueryCommand},
    mv::{MvServe, MvState},
    query::{limit_is_sufficient, limit_rows_needed},
    query_cache::{CoalescedClient, QueryType, WorkerRequest},
    types::{CacheStateView, CachedQueryState, CachedQueryView, SharedResolved},
};

/// `Send` bundle a connection thread uses to serve a cache hit inline.
#[derive(Clone)]
pub struct CacheFastPath {
    pub state_view: Arc<CacheStateView>,
    pub worker_tx: UnboundedSender<WorkerRequest>,
    pub dynamic: DynamicConfigHandle,
    /// Shared CDC-liveness flag (owned by the coordinator loop). Hits are only
    /// served inline while CDC is connected; otherwise the message falls back
    /// to the coordinator, which forwards to origin.
    pub cdc_connected: Arc<AtomicBool>,
}

/// Connection-side handle to the current [`CacheFastPath`]. Hot-swaps across
/// cache restarts via a `watch` channel (mirrors `CacheSender`). `None` before
/// the cache is ready or while it is restarting — callers fall back to the
/// coordinator channel.
#[derive(Clone)]
pub struct CacheFastPathHandle {
    rx: watch::Receiver<Option<CacheFastPath>>,
}

impl CacheFastPathHandle {
    /// Snapshot the current fast path, if the cache is up.
    pub fn current(&self) -> Option<CacheFastPath> {
        self.rx.borrow().clone()
    }
}

/// Publish handle held by `cache_run` to advertise its freshly-built fast path
/// (and to retract it on exit).
pub struct CacheFastPathPublisher {
    tx: watch::Sender<Option<CacheFastPath>>,
}

impl CacheFastPathPublisher {
    pub fn publish(&self, fp: CacheFastPath) {
        let _ = self.tx.send(Some(fp));
    }

    pub fn clear(&self) {
        let _ = self.tx.send(None);
    }
}

/// Supervisor-side owner of the fast-path `watch`. Hands out subscriber handles
/// for connection threads and a publisher for `cache_run`; clears on cache exit.
pub struct CacheFastPathUpdater {
    tx: watch::Sender<Option<CacheFastPath>>,
}

impl CacheFastPathUpdater {
    pub fn new() -> (Self, CacheFastPathHandle) {
        let (tx, rx) = watch::channel(None);
        (Self { tx }, CacheFastPathHandle { rx })
    }

    pub fn publisher(&self) -> CacheFastPathPublisher {
        CacheFastPathPublisher {
            tx: self.tx.clone(),
        }
    }

    pub fn subscribe(&self) -> CacheFastPathHandle {
        CacheFastPathHandle {
            rx: self.tx.subscribe(),
        }
    }

    pub fn clear(&self) {
        let _ = self.tx.send(None);
    }
}

/// Outcome of the by-reference cacheability probe.
struct Probe {
    fingerprint: u64,
    rows_needed: Option<u64>,
    limit: Option<LimitClause>,
}

/// MV serve decision shared by the coordinator and the inline path.
pub(crate) enum MvDecision {
    Serve(MvServe),
    /// MV is `Pending`; needs a `Pending → Scheduled` flip plus an `MvBuild`
    /// send. Only the coordinator (which owns `query_tx`) handles this; the
    /// inline path declines and falls back.
    NeedsSchedule { has_table: bool },
}

impl CacheFastPath {
    /// Attempt to serve `proxy_msg` as a cache hit without the coordinator hop.
    ///
    /// On a clean hit, dispatches a [`WorkerRequest`] and returns `Ok(())`.
    /// Otherwise returns `Err(proxy_msg)` **unchanged** so the caller routes it
    /// to the coordinator (identical behavior to before this fast path existed).
    // The large `Err` payload is intentional: it hands the original message back
    // by move for coordinator fallback. Boxing would allocate on every non-hit.
    #[allow(clippy::result_large_err)]
    pub fn hit_dispatch_try(&self, mut proxy_msg: ProxyMessage) -> Result<(), ProxyMessage> {
        // CDC down: never serve inline (coordinator forwards to origin).
        if !self.cdc_connected.load(Ordering::Relaxed) {
            return Err(proxy_msg);
        }

        let cfg = self.dynamic.load();
        let Some(probe) = message_probe(&proxy_msg.message, &cfg.allowed_tables_parsed) else {
            return Err(proxy_msg);
        };

        let lookup_start = Instant::now();
        let entry = self
            .state_view
            .cached_queries
            .get(&probe.fingerprint)
            .map(|e| e.clone());
        crate::metrics::handles()
            .cache
            .lookup_latency
            .record(lookup_start.elapsed().as_secs_f64());

        // Only a Ready, fully-populated entry with a sufficient limit is a hit.
        let Some(CachedQueryView {
            state: CachedQueryState::Ready,
            generation,
            resolved: Some(resolved),
            deparsed_sql: Some(deparsed_sql),
            max_limit,
            ..
        }) = entry
        else {
            return Err(proxy_msg);
        };
        if !limit_is_sufficient(max_limit, probe.rows_needed) {
            return Err(proxy_msg);
        }

        // MV that needs scheduling is coordinator-only — fall back.
        let mv = match mv_serve_decide(&self.state_view, probe.fingerprint, probe.rows_needed) {
            MvDecision::Serve(mv) => mv,
            MvDecision::NeedsSchedule { .. } => return Err(proxy_msg),
        };

        metrics_hit_record(&self.state_view, probe.fingerprint);
        clock_reference_set(&self.state_view, cfg.cache_policy, &probe.fingerprint);

        proxy_msg.timing.lookup_complete_at = Some(Instant::now());

        // Commit: consume the message and build the worker request.
        let ProxyMessage {
            message,
            client_socket,
            reply_tx,
            timing,
            pipeline,
            ..
        } = proxy_msg;
        let (data, result_formats, query_type) = match message {
            CacheMessage::Query(data, _) => (data, Vec::new(), QueryType::Simple),
            CacheMessage::QueryParameterized(data, _, _, result_formats) => {
                (data, result_formats, QueryType::Extended)
            }
        };

        // Worker-send failure means the worker thread is gone (cache restart);
        // the dropped reply_tx surfaces to the connection as a closed reply
        // channel, matching the coordinator's behavior. The socket is consumed
        // either way, so there is nothing to hand back.
        let _ = worker_request_send(
            &self.worker_tx,
            WorkerSendParts {
                fingerprint: probe.fingerprint,
                query_type,
                data,
                resolved,
                deparsed_sql,
                generation,
                mv,
                result_formats,
                client_socket,
                reply_tx,
                timing,
                limit: probe.limit,
                pipeline,
                coalesced: Vec::new(),
            },
        );
        Ok(())
    }
}

/// Derive fingerprint/limit from a message without consuming it. Returns `None`
/// (caller falls back to the coordinator) on parameter-substitution error or
/// when the query is not in the table allowlist.
fn message_probe(message: &CacheMessage, allowlist: &Allowlist) -> Option<Probe> {
    match message {
        CacheMessage::Query(_, cacheable_query) => probe_from(&cacheable_query.query, allowlist),
        CacheMessage::QueryParameterized(_, cacheable_query, parameters, _) => {
            let replaced = query_expr_parameters_replace(&cacheable_query.query, parameters).ok()?;
            probe_from(&replaced, allowlist)
        }
    }
}

fn probe_from(query: &QueryExpr, allowlist: &Allowlist) -> Option<Probe> {
    if !query_allowlist_check(allowlist, query) {
        return None;
    }
    Some(Probe {
        fingerprint: query_expr_fingerprint(query),
        rows_needed: limit_rows_needed(&query.limit),
        limit: query.limit.clone(),
    })
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
/// from the coordinator and from connection threads.
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
/// rows, or (Pending) defer to the coordinator for scheduling. Counts
/// `mv_hits`/`mv_fallthrough` for the decisions it resolves; the coordinator
/// counts the fallthrough for `NeedsSchedule`.
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
        Some((MvState::Fresh, Some(cols), mv_limit)) if limit_is_sufficient(mv_limit, rows_needed) => {
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
        Some((MvState::Pending { has_table }, _, _)) => MvDecision::NeedsSchedule { has_table },
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

/// Fields for [`worker_request_send`]. Shared by the coordinator
/// (`QueryRequest`-derived) and the inline path (`ProxyMessage`-derived).
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

    worker_tx
        .send(WorkerRequest {
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
        })
        .map_err(|e| {
            error!("worker send {e}");
            CacheError::WorkerSend.into()
        })
}
