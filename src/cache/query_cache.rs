use crate::query::{Fingerprint, FingerprintMap};
use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use dashmap::Entry;

use ecow::EcoString;
use tokio::io::AsyncWriteExt;
use tokio::sync::{
    mpsc::{UnboundedSender, error::SendError},
    oneshot, watch,
};
use tokio_util::bytes::{Buf, Bytes, BytesMut};
use tracing::{debug, error, info, instrument, trace};

use crate::pg::protocol::encode::{
    BIND_COMPLETE_MSG, PARSE_COMPLETE_MSG, READY_FOR_QUERY_IDLE_MSG,
};
use crate::pg::protocol::extended::ResultFormats;
use crate::proxy::ClientSocket;
use crate::query::ast::{LimitClause, query_expr_fingerprint};
use crate::result::error_chain_format;
use crate::settings::{CachePolicy, DynamicConfig, DynamicConfigHandle, Settings};
use crate::timing::{QueryTiming, duration_to_ns_u64};

use super::{
    CacheError, CacheResult,
    fast_path::{self, MvDecision},
    memo::{MemoHit, MemoKey, MemoShape},
    messages::{
        AdmitAction, CacheOutcome, CacheReply, MessageSlices, PipelineContext, PipelineDescribe,
        ProxyMessage, QueryCommand, SubsumptionResult, slices_concat,
    },
    mv::{MvMeta, MvServe, ShapeGate},
    query::{CacheableQuery, limit_is_sufficient, limit_rows_needed},
    reply::ReplySender,
    types::{
        CacheStateView, CachedQueryState, CachedQueryView, PinnedQuery, QueryMetrics,
        SharedResolved,
    },
    write_queue::WriteQueue,
};

/// Minimum credit stamped on a Pending entry. Provides a survival floor during
/// cold start (when `last_hits_per_gc` is zero) and for low-traffic workloads.
const MIN_PENDING_CREDIT: u32 = 100;

/// Test-only deterministic fault injection for the coalesce enqueue/drain race.
/// The race window between observing `Loading` and enqueuing the waiter is a few
/// microseconds and cannot be provoked probabilistically, so a stress test widens
/// it here. Compiled out entirely unless built with `--features fault-injection`.
#[cfg(feature = "fault-injection")]
mod fault {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    static COALESCE_DELAY: AtomicBool = AtomicBool::new(false);

    /// Arm from the environment (read once at `CacheDispatch` construction).
    pub(super) fn init() {
        if std::env::var_os("PGCACHE_FAULT_COALESCE_DELAY").is_some() {
            COALESCE_DELAY.store(true, Ordering::Relaxed);
        }
    }

    /// When armed, delay between the `Loading` observation and the enqueue so a
    /// concurrently-completing population's drain reliably interleaves.
    pub(super) async fn coalesce_enqueue_delay() {
        if COALESCE_DELAY.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

#[cfg(feature = "fault-injection")]
async fn fault_coalesce_enqueue_delay() {
    fault::coalesce_enqueue_delay().await;
}
#[cfg(not(feature = "fault-injection"))]
async fn fault_coalesce_enqueue_delay() {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueryType {
    Simple,
    Extended,
}

/// Key for grouping coalesced requests. Requests in the same group
/// produce identical wire protocol bytes and can share a single serve execution.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CoalesceKey {
    query_type: QueryType,
    emit_rfq: bool,
    has_parse: bool,
    has_bind: bool,
    pipeline_describe: PipelineDescribe,
    result_formats: ResultFormats,
    limit: Option<LimitClause>,
}

/// A client waiting to receive coalesced response bytes from a shared serve execution.
pub struct CoalescedClient {
    pub client_socket: ClientSocket,
    pub reply_tx: ReplySender<CacheReply>,
    pub timing: QueryTiming,
    /// Pre-computed origin fallback bytes (pipeline.buffered_bytes or raw data).
    pub data: BytesMut,
}

/// Outer key: fingerprint (O(1) drain on Ready/Failed).
/// Inner key: CoalesceKey grouping requests that share identical response bytes.
type WaitingQueue = FingerprintMap<HashMap<CoalesceKey, Vec<QueryRequest>>>;

/// Coalescing wait queue with the enqueue/drain ordering invariant encapsulated.
///
/// The `Mutex` is private and [`enqueue_if_loading`](Self::enqueue_if_loading) is
/// the only way to add a waiter — it re-checks the entry state *under the lock*
/// and refuses to enqueue if the state has advanced. This makes the
/// orphaned-waiter race unrepresentable: a waiter cannot be added after the
/// `Ready` drain has run, because the writer sets `Ready` before sending the
/// notify that drives [`drain`](Self::drain), and that drain removes under the
/// same lock. Callers therefore cannot skip the re-check or get the
/// waiting→cached_queries lock ordering wrong.
pub(super) struct CoalesceQueue {
    inner: Mutex<WaitingQueue>,
}

impl CoalesceQueue {
    fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::default()),
        }
    }

    /// Enqueue `msg` as a coalesced waiter iff `fingerprint` is still `Loading`
    /// (re-checked under the lock). Returns `Err(msg)` when the state has
    /// advanced so the caller can re-dispatch against the current state.
    // The large `Err` payload is intentional: it returns the message by move for
    // re-dispatch. Boxing would allocate on the (rare) state-advanced path.
    #[allow(clippy::result_large_err)]
    fn enqueue_if_loading(
        &self,
        state_view: &CacheStateView,
        fingerprint: Fingerprint,
        key: CoalesceKey,
        msg: QueryRequest,
    ) -> Result<(), QueryRequest> {
        let mut guard = self.inner.lock().expect("lock coalesce queue");
        let still_loading = state_view.cached_queries.get(&fingerprint).map(|e| e.state)
            == Some(CachedQueryState::Loading);
        if !still_loading {
            return Err(msg);
        }
        guard
            .entry(fingerprint)
            .or_default()
            .entry(key)
            .or_default()
            .push(msg);
        Ok(())
    }

    /// Remove and return all waiter groups for a fingerprint (Ready/Failed drain).
    fn drain(&self, fingerprint: Fingerprint) -> Option<HashMap<CoalesceKey, Vec<QueryRequest>>> {
        self.inner
            .lock()
            .expect("lock coalesce queue")
            .remove(&fingerprint)
    }

    /// Total waiters across all groups (gauge).
    fn waiter_count(&self) -> usize {
        self.inner
            .lock()
            .expect("lock coalesce queue")
            .values()
            .flat_map(|groups| groups.values())
            .map(Vec::len)
            .sum()
    }
}

pub struct QueryRequest {
    pub query_type: QueryType,
    pub data: BytesMut,
    pub cacheable_query: Arc<CacheableQuery>,
    pub result_formats: ResultFormats,
    pub client_socket: ClientSocket,
    pub reply_tx: ReplySender<CacheReply>,
    /// Resolved search_path for schema resolution
    pub search_path: Arc<[EcoString]>,
    /// Per-query timing data
    pub timing: QueryTiming,
    /// Pipeline context from the proxy (None for simple queries and cold-path extended)
    pub pipeline: Option<PipelineContext>,
}

/// Request sent to cache serve for executing cached queries.
/// Contains the resolved AST with schema-qualified table names.
pub struct ServeRequest {
    pub fingerprint: Fingerprint,
    pub query_type: QueryType,
    pub data: BytesMut,
    pub resolved: SharedResolved,
    /// Precomputed deparsed SQL body of `resolved`. Spliced into the SET +
    /// body + LIMIT wire string the serve pool sends to the cache DB.
    pub deparsed_sql: EcoString,
    /// Generation number for row tracking in pgcache_pgrx extension
    pub generation: u64,
    /// Serve from the MV (carrying its aliased output column names) or
    /// from source rows. Decided on the dispatch path.
    pub mv: MvServe,
    pub result_formats: ResultFormats,
    pub client_socket: ClientSocket,
    pub reply_tx: ReplySender<CacheReply>,
    /// Per-query timing data
    pub timing: QueryTiming,
    /// Incoming query's LIMIT clause, appended to SQL at serve time
    pub limit: Option<LimitClause>,
    /// Whether the serve path should append ReadyForQuery after this execute's
    /// response (the trailing execute of a Sync-terminated dispatch).
    pub emit_rfq: bool,
    /// Whether Parse was buffered in the pipeline.
    /// False for Bind-only pipelines (named statement re-execution without Parse).
    pub has_parse: bool,
    /// Whether Bind was buffered in the pipeline.
    /// False when Bind was flushed separately (e.g., JDBC Parse/Bind/Describe/Flush then Execute/Sync).
    pub has_bind: bool,
    /// Whether the pipeline includes a Describe message and which type.
    pub pipeline_describe: PipelineDescribe,
    /// Stored ParameterDescription bytes for Describe('S') responses in the pipeline.
    pub parameter_description: Option<Bytes>,
    /// Buffered message slices for origin fallback on serve error. Concatenated
    /// only on that (cold) path; a successful serve drops them untouched.
    pub forward_bytes: Option<MessageSlices>,
    /// Additional clients to receive the same response bytes.
    /// Empty for non-coalesced requests.
    pub coalesced: Vec<CoalescedClient>,
}

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

/// Per-connection inline dispatch against the cache: routes queries and
/// delegates writes to the writer thread. `Send + Clone`; each connection holds
/// one (via the watch handle) and dispatches against it directly.
#[derive(Clone)]
pub struct CacheDispatch {
    query_tx: UnboundedSender<QueryCommand>,
    serve_tx: UnboundedSender<ServeRequest>,
    state_view: Arc<CacheStateView>,
    dynamic: DynamicConfigHandle,
    waiting: Arc<CoalesceQueue>,
    /// CDC-liveness flag (set by the CDC thread). While CDC is down, queries are
    /// forwarded to origin rather than served from cache, to avoid stale reads.
    cdc_connected: Arc<AtomicBool>,
}

/// Connection-side handle to the current [`CacheDispatch`]. Hot-swaps across cache
/// restarts via a `watch` channel; `None` before the cache is ready or while it
/// is restarting (connections then forward to origin).
#[derive(Clone)]
pub struct CacheDispatchHandle {
    rx: watch::Receiver<Option<CacheDispatch>>,
}

impl CacheDispatchHandle {
    /// Snapshot the current cache, if it is up. The clone is cheap (channels +
    /// `Arc`s) and gives the caller an owned `CacheDispatch` to dispatch against.
    pub fn current(&self) -> Option<CacheDispatch> {
        self.rx.borrow().clone()
    }
}

/// Publish handle held by `cache_setup` to advertise its built `CacheDispatch`
/// (and retract it on exit).
pub struct CacheDispatchPublisher {
    tx: watch::Sender<Option<CacheDispatch>>,
}

impl CacheDispatchPublisher {
    pub fn publish(&self, dispatch: CacheDispatch) {
        let _ = self.tx.send(Some(dispatch));
    }

    pub fn clear(&self) {
        let _ = self.tx.send(None);
    }
}

/// Supervisor-side owner of the `CacheDispatch` watch. Hands out subscriber handles
/// for connection tasks and a publisher for `cache_setup`; clears on cache exit.
pub struct CacheDispatchUpdater {
    tx: watch::Sender<Option<CacheDispatch>>,
}

impl CacheDispatchUpdater {
    pub fn new() -> (Self, CacheDispatchHandle) {
        let (tx, rx) = watch::channel(None);
        (Self { tx }, CacheDispatchHandle { rx })
    }

    pub fn publisher(&self) -> CacheDispatchPublisher {
        CacheDispatchPublisher {
            tx: self.tx.clone(),
        }
    }

    pub fn subscribe(&self) -> CacheDispatchHandle {
        CacheDispatchHandle {
            rx: self.tx.subscribe(),
        }
    }

    pub fn clear(&self) {
        let _ = self.tx.send(None);
    }
}

impl CacheDispatch {
    pub async fn new(
        settings: &Settings,
        query_tx: UnboundedSender<QueryCommand>,
        serve_tx: UnboundedSender<ServeRequest>,
        state_view: Arc<CacheStateView>,
        cdc_connected: Arc<AtomicBool>,
    ) -> CacheResult<Self> {
        #[cfg(feature = "fault-injection")]
        fault::init();
        let cfg = settings.dynamic.load();
        match &cfg.allowed_tables_parsed {
            Some(_entries) => {
                let names: Vec<&str> = cfg
                    .allowed_tables
                    .as_ref()
                    .map(|v| v.iter().map(String::as_str).collect())
                    .unwrap_or_default();
                info!("table allowlist enabled: {names:?}");
            }
            None => info!("table allowlist disabled, all tables cacheable"),
        }

        Ok(Self {
            query_tx,
            serve_tx,
            state_view,
            dynamic: settings.dynamic.clone(),
            waiting: Arc::new(CoalesceQueue::new()),
            cdc_connected,
        })
    }

    /// Inline dispatch entry point for a connection task. Applies CDC-liveness
    /// gating, converts the proxy message (parameter substitution), and routes
    /// to [`query_dispatch`](Self::query_dispatch). Replaces the former central
    /// dispatch hop: every connection calls this directly.
    pub async fn dispatch_proxy(&mut self, proxy_msg: ProxyMessage) {
        if !self.cdc_connected.load(Ordering::Relaxed) {
            // CDC down: forward to origin rather than serve possibly-stale data.
            let data = proxy_msg.message.into_data();
            let _ = reply_forward(
                proxy_msg.reply_tx,
                proxy_msg.client_socket,
                proxy_msg.pipeline,
                data,
                proxy_msg.timing,
            );
            return;
        }

        match proxy_msg.message.into_query_data() {
            Ok(query_data) => {
                let request = QueryRequest {
                    query_type: query_data.query_type,
                    data: query_data.data,
                    cacheable_query: query_data.cacheable_query,
                    result_formats: query_data.result_formats,
                    client_socket: proxy_msg.client_socket,
                    reply_tx: proxy_msg.reply_tx,
                    search_path: proxy_msg.search_path,
                    timing: proxy_msg.timing,
                    pipeline: proxy_msg.pipeline,
                };
                if let Err(e) = self.query_dispatch(request).await {
                    error!(
                        "query dispatch failed: {}",
                        error_chain_format(e.current_context()),
                    );
                }
            }
            Err((e, data)) => {
                debug!("forwarding to origin due to parameter conversion error: {e}");
                let _ = reply_forward(
                    proxy_msg.reply_tx,
                    proxy_msg.client_socket,
                    proxy_msg.pipeline,
                    data,
                    proxy_msg.timing,
                );
            }
        }
    }

    // Span at trace level: at info/debug the fmt layer allocates per-span
    // extensions, which would put one heap allocation on every cache hit.
    #[instrument(skip_all, level = "trace")]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn query_dispatch(&mut self, mut msg: QueryRequest) -> CacheResult<()> {
        let cfg = self.dynamic.load();
        if !fast_path::query_allowlist_check(&cfg.allowed_tables_parsed, &msg.cacheable_query.query)
        {
            crate::metrics::handles()
                .query
                .allowlist_skipped
                .increment(1);
            return reply_forward(
                msg.reply_tx,
                msg.client_socket,
                msg.pipeline,
                msg.data,
                msg.timing,
            );
        }

        let fingerprint = query_expr_fingerprint(&msg.cacheable_query.query);
        trace!("{fingerprint}");

        let rows_needed = limit_rows_needed(&msg.cacheable_query.query.limit);

        let lookup_start = Instant::now();
        let mut cache_entry = self
            .state_view
            .cached_queries
            .get(&fingerprint)
            .map(|entry| entry.clone());
        crate::metrics::handles()
            .cache
            .lookup_latency
            .record(lookup_start.elapsed().as_secs_f64());
        // Stamp lookup_complete uniformly across all paths so `lookup_seconds`
        // means "proxy dispatch → cache state lookup done." Path-specific
        // post-lookup work is captured by dedicated histograms
        // (forward_decision / coalesce_intake / coalesce_wait).
        msg.timing.lookup_complete_at = Some(Instant::now());

        // Retry loop: the `Loading` arm may observe (under the waiting lock) that
        // the state has advanced since the snapshot above and fall through to
        // re-dispatch against the fresh state. All other arms are terminal.
        loop {
            match &cache_entry {
                // Cache hit: Ready with sufficient rows — serve from cache
                Some(CachedQueryView {
                    state: CachedQueryState::Ready,
                    generation,
                    resolved: Some(resolved),
                    deparsed_sql: Some(deparsed_sql),
                    max_limit,
                    ..
                }) if limit_is_sufficient(*max_limit, rows_needed) => {
                    self.metrics_hit_record(fingerprint);
                    self.clock_reference_set(cfg.cache_policy, &fingerprint);
                    return self
                        .hit_serve(
                            fingerprint,
                            msg,
                            Arc::clone(resolved),
                            deparsed_sql.clone(),
                            *generation,
                            rows_needed,
                        )
                        .await;
                }

                // Cache hit: Ready but insufficient rows — forward and request limit bump
                Some(CachedQueryView {
                    state: CachedQueryState::Ready,
                    max_limit,
                    ..
                }) => {
                    trace!("limit bump {fingerprint} cached={max_limit:?} needed={rows_needed:?}");
                    // CAS Ready(insufficient)→Loading: a single dispatch claims the
                    // bump. If we lose (another bumper won, or a completed bump made
                    // the entry sufficient), re-dispatch against the fresh state.
                    if self.transition_if(
                        fingerprint,
                        |v| {
                            matches!(v.state, CachedQueryState::Ready)
                                && !limit_is_sufficient(v.max_limit, rows_needed)
                        },
                        CachedQueryState::Loading,
                    ) {
                        self.metrics_miss_record(fingerprint);
                        reply_forward(
                            msg.reply_tx,
                            msg.client_socket,
                            msg.pipeline,
                            msg.data,
                            msg.timing,
                        )?;
                        self.query_tx
                            .send(QueryCommand::LimitBump {
                                fingerprint,
                                max_limit: rows_needed,
                            })
                            .map_err(|_| CacheError::WriterSend)?;
                        return Ok(());
                    }
                    // Lost the race; fall through to re-read and re-dispatch.
                }

                // Loading — coalesce: queue request for later dispatch from cache.
                // The state is re-checked under the waiting lock to avoid an
                // orphaned waiter: the writer sets `Ready` before sending the
                // notify that drains this queue, so if we still observe `Loading`
                // while holding the lock, the drain has not yet removed our group
                // (or will see us); otherwise we fall through and re-dispatch.
                Some(CachedQueryView {
                    state: CachedQueryState::Loading,
                    ..
                }) => {
                    // Under memory pressure this query's population is being
                    // skipped (it won't become Ready), so forward to origin
                    // rather than coalesce-wait on a load that won't complete.
                    if self.state_view.throttled() {
                        crate::metrics::handles()
                            .cache
                            .registration_throttled_total
                            .increment(1);
                        return reply_forward(
                            msg.reply_tx,
                            msg.client_socket,
                            msg.pipeline,
                            msg.data,
                            msg.timing,
                        );
                    }
                    trace!("cache loading, coalesce {fingerprint}");
                    fault_coalesce_enqueue_delay().await;
                    let key = Self::coalesce_key_from_request(&msg);
                    msg.timing.waiter_enqueued_at = Some(Instant::now());
                    // `enqueue_if_loading` re-checks state under the lock; on
                    // `Err` the state advanced and we re-dispatch the returned msg.
                    match self
                        .waiting
                        .enqueue_if_loading(&self.state_view, fingerprint, key, msg)
                    {
                        Ok(()) => {
                            self.metrics_miss_record(fingerprint);
                            #[allow(clippy::cast_precision_loss)]
                            // queue depth, never near 2^53
                            crate::metrics::handles()
                                .cache
                                .coalesce_waiting
                                .set(self.waiting.waiter_count() as f64);
                            return Ok(());
                        }
                        Err(returned) => {
                            msg = returned;
                            // State advanced; fall through to re-read and re-dispatch.
                        }
                    }
                }

                // Pending — increment hit count under the guard and admit if the
                // threshold is reached. The read-modify-write happens atomically in
                // `pending_admit`; if the entry is no longer Pending we re-dispatch.
                Some(CachedQueryView {
                    state: CachedQueryState::Pending { .. },
                    ..
                }) => {
                    trace!("pending {fingerprint}");
                    let credit = self.pending_initial_credit();
                    match self.pending_admit(fingerprint, cfg.admission_threshold, credit) {
                        Some(action) => {
                            return self.subsumption_await(msg, fingerprint, action).await;
                        }
                        None => {
                            // No longer Pending; fall through to re-read and re-dispatch.
                        }
                    }
                }

                // Invalidated — fast-readmit (skip admission gate). CAS the
                // Invalidated→Loading transition so only one dispatch readmits.
                Some(CachedQueryView {
                    state: CachedQueryState::Invalidated,
                    ..
                }) => {
                    trace!("invalidated readmit {fingerprint}");
                    if self.transition_if(
                        fingerprint,
                        |v| matches!(v.state, CachedQueryState::Invalidated),
                        CachedQueryState::Loading,
                    ) {
                        return self
                            .subsumption_await(msg, fingerprint, AdmitAction::Admit)
                            .await;
                    }
                    // Lost the race; fall through to re-read and re-dispatch.
                }

                // Cache miss — claim the entry atomically; only the winner
                // registers, losers re-dispatch against the now-present entry.
                None => {
                    trace!("cache miss {fingerprint}");
                    // Under memory pressure, don't register a brand-new query
                    // (each registration costs in-process memory). Forward it to
                    // origin instead; already-tracked queries are unaffected.
                    if self.state_view.throttled() {
                        crate::metrics::handles()
                            .cache
                            .registration_throttled_total
                            .increment(1);
                        return reply_forward(
                            msg.reply_tx,
                            msg.client_socket,
                            msg.pipeline,
                            msg.data,
                            msg.timing,
                        );
                    }
                    match self.first_miss_claim(fingerprint, &cfg) {
                        Some(action) => {
                            return self.subsumption_await(msg, fingerprint, action).await;
                        }
                        None => {
                            // Another dispatch inserted it first; fall through to retry.
                        }
                    }
                }
            }

            // Reached only when the Loading arm fell through: re-read the entry
            // and re-dispatch against the now-current state.
            cache_entry = self
                .state_view
                .cached_queries
                .get(&fingerprint)
                .map(|entry| entry.clone());
        }
    }

    /// Record a cache hit in per-query metrics.
    fn metrics_hit_record(&self, fingerprint: Fingerprint) {
        fast_path::metrics_hit_record(&self.state_view, fingerprint);
    }

    /// Credit stamped on a Pending entry at insert and on each re-hit. Sized to
    /// the previous GC tick's hit count (floored at `MIN_PENDING_CREDIT`) so
    /// candidates survive ~1 GC interval of activity unless re-hit. The writer
    /// decays `credit` by the current tick's hit delta on every GC pass and
    /// purges entries that drain to zero.
    fn pending_initial_credit(&self) -> u32 {
        self.state_view
            .last_hits_per_gc
            .load(Ordering::Relaxed)
            .max(MIN_PENDING_CREDIT)
    }

    /// Record a cache miss in per-query metrics.
    fn metrics_miss_record(&self, fingerprint: Fingerprint) {
        if let Some(mut m) = self.state_view.metrics.get_mut(&fingerprint) {
            m.miss_count += 1;
        }
    }

    /// Set the CLOCK reference bit for eviction tracking.
    fn clock_reference_set(&self, cache_policy: CachePolicy, fingerprint: &Fingerprint) {
        fast_path::clock_reference_set(&self.state_view, cache_policy, fingerprint);
    }

    /// Atomically transition `fingerprint` to `new` iff the entry still satisfies
    /// `pred` *under the write guard*. Returns `true` if this caller performed the
    /// transition, `false` if the entry advanced (the caller re-dispatches). This
    /// is the compare-and-set that makes the cold dispatch arms race-safe under
    /// the multi-thread runtime (cf. `fast_path::mv_schedule`).
    fn transition_if(
        &self,
        fingerprint: Fingerprint,
        pred: impl Fn(&CachedQueryView) -> bool,
        new: CachedQueryState,
    ) -> bool {
        if let Some(mut entry) = self.state_view.cached_queries.get_mut(&fingerprint)
            && pred(&entry)
        {
            entry.state = new;
            return true;
        }
        false
    }

    /// Under the write guard: if still `Pending`, increment the hit count and
    /// either admit (→ `Loading`, returning `Admit`) or bump the credit
    /// (returning `CheckOnly`). Returns `None` if the entry is no longer
    /// `Pending` (the caller re-dispatches). Race-safe read-modify-write.
    fn pending_admit(
        &self,
        fingerprint: Fingerprint,
        threshold: u32,
        credit: u32,
    ) -> Option<AdmitAction> {
        let mut entry = self.state_view.cached_queries.get_mut(&fingerprint)?;
        let CachedQueryState::Pending { hit_count, .. } = entry.state else {
            return None;
        };
        let new_count = hit_count + 1;
        if new_count >= threshold {
            entry.state = CachedQueryState::Loading;
            Some(AdmitAction::Admit)
        } else {
            entry.state = CachedQueryState::Pending {
                hit_count: new_count,
                credit,
            };
            Some(AdmitAction::CheckOnly)
        }
    }

    /// Claim a cold fingerprint atomically: insert the initial cache view iff the
    /// entry is vacant. Returns the `AdmitAction` to register with when this
    /// caller won the insert; `None` if another dispatch already inserted it (the
    /// caller re-dispatches against the now-present entry). Prevents concurrent
    /// first-misses from double-registering.
    fn first_miss_claim(
        &self,
        fingerprint: Fingerprint,
        cfg: &DynamicConfig,
    ) -> Option<AdmitAction> {
        let immediate_admit = cfg.cache_policy == CachePolicy::Fifo || cfg.admission_threshold <= 1;
        let (initial_state, action) = if immediate_admit {
            (CachedQueryState::Loading, AdmitAction::Admit)
        } else {
            (
                CachedQueryState::Pending {
                    hit_count: 1,
                    credit: self.pending_initial_credit(),
                },
                AdmitAction::CheckOnly,
            )
        };

        match self.state_view.cached_queries.entry(fingerprint) {
            Entry::Occupied(_) => return None, // lost the race; re-dispatch
            Entry::Vacant(slot) => {
                slot.insert(CachedQueryView {
                    state: initial_state,
                    generation: 0,
                    resolved: None,
                    deparsed_sql: None,
                    max_limit: None,
                    referenced: false,
                    // Writer fills this in after resolution/classification.
                    mv: MvMeta::new(ShapeGate::Skip, None),
                });
            }
        }

        let now = NonZeroU64::new(duration_to_ns_u64(self.state_view.started_at.elapsed()));
        self.state_view
            .metrics
            .entry(fingerprint)
            .or_insert_with(|| QueryMetrics::new(now));
        Some(action)
    }

    /// Build and send a ServeRequest for serving a query from cache.
    /// Serve a Ready cache hit. Two backends for the same logical work: the
    /// in-process memo (served inline on the connection thread — no serve hop,
    /// no cache-DB round trip) when a live snapshot matches this request's
    /// (format, shape), otherwise a pool serve (from the MV table or source
    /// rows, decided here). Scoped to the single-request Ready path: coalesced
    /// groups and subsumption serves dispatch to the serve pool directly.
    async fn hit_serve(
        &self,
        fingerprint: Fingerprint,
        msg: QueryRequest,
        resolved: SharedResolved,
        deparsed_sql: EcoString,
        generation: u64,
        rows_needed: Option<u64>,
    ) -> CacheResult<()> {
        if let Some(serve) = self.memo_serve_plan(fingerprint, &msg) {
            return self.memo_serve(msg, fingerprint, serve).await;
        }
        // No memo: hand off to the serve pool. `mv_dispatch_decide` picks the MV fast
        // path vs source-row fallthrough and, on a dirty MV, schedules a rebuild.
        let mv = self.mv_dispatch_decide(fingerprint, rows_needed);
        self.pool_serve(fingerprint, msg, resolved, deparsed_sql, generation, mv)
    }

    fn pool_serve(
        &self,
        fingerprint: Fingerprint,
        msg: QueryRequest,
        resolved: SharedResolved,
        deparsed_sql: EcoString,
        generation: u64,
        mv: MvServe,
    ) -> CacheResult<()> {
        self.pool_serve_coalesced(
            fingerprint,
            msg,
            resolved,
            deparsed_sql,
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
    fn mv_dispatch_decide(&self, fingerprint: Fingerprint, rows_needed: Option<u64>) -> MvServe {
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
    fn pool_serve_coalesced(
        &self,
        fingerprint: Fingerprint,
        msg: QueryRequest,
        resolved: SharedResolved,
        deparsed_sql: EcoString,
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

        if let Err(SendError(req)) = self.serve_tx.send(ServeRequest {
            fingerprint,
            query_type: msg.query_type,
            data: msg.data,
            resolved,
            deparsed_sql,
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
        }) {
            // Worker channel closed (cache subsystem torn down or restarting):
            // degrade gracefully by forwarding the query — and any coalesced
            // waiters — to origin rather than surfacing a hard cache error.
            debug!("serve channel closed; forwarding query to origin");
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

    /// Build a CoalesceKey from a QueryRequest's pipeline context.
    fn coalesce_key_from_request(msg: &QueryRequest) -> CoalesceKey {
        let (emit_rfq, has_parse, has_bind, pipeline_describe) = match &msg.pipeline {
            Some(p) => (p.emit_rfq, p.has_parse, p.has_bind, p.describe),
            None => (false, false, false, PipelineDescribe::None),
        };
        CoalesceKey {
            query_type: msg.query_type,
            emit_rfq,
            has_parse,
            has_bind,
            pipeline_describe,
            result_formats: msg.result_formats.clone(),
            limit: msg.cacheable_query.query.limit.clone(),
        }
    }

    /// Total number of requests waiting across all coalescing groups.
    fn waiting_count(&self) -> usize {
        self.waiting.waiter_count()
    }

    /// Drain all coalesced waiters for a fingerprint that became Ready.
    /// Each coalescing group dispatches a single serve request that broadcasts
    /// response bytes to all clients in the group.
    pub fn waiting_drain_ready(
        &self,
        fingerprint: Fingerprint,
        generation: u64,
        resolved: SharedResolved,
        deparsed_sql: EcoString,
        max_limit: Option<u64>,
    ) {
        let Some(groups) = self.waiting.drain(fingerprint) else {
            return;
        };

        let mut served = 0u64;
        for (_key, mut waiters) in groups {
            // Stamp drain_started_at on every waiter (including the one that
            // becomes primary) so `coalesce_wait_seconds` fires per-waiter,
            // not just per-group.
            let drain_started = Instant::now();
            for w in &mut waiters {
                w.timing.drain_started_at = Some(drain_started);
            }
            let primary = waiters.remove(0);

            // Check whether the cached rows cover this group's LIMIT
            let primary_needed = limit_rows_needed(&primary.cacheable_query.query.limit);
            if !limit_is_sufficient(max_limit, primary_needed) {
                let _ = reply_forward(
                    primary.reply_tx,
                    primary.client_socket,
                    primary.pipeline,
                    primary.data,
                    primary.timing,
                );
                for msg in waiters {
                    let _ = reply_forward(
                        msg.reply_tx,
                        msg.client_socket,
                        msg.pipeline,
                        msg.data,
                        msg.timing,
                    );
                }
                continue;
            }

            served += waiters.len() as u64;

            let coalesced: Vec<CoalescedClient> = waiters
                .into_iter()
                .map(|msg| {
                    let fallback = match msg.pipeline {
                        Some(pipeline) => slices_concat(&pipeline.buffered_bytes),
                        None => msg.data,
                    };
                    CoalescedClient {
                        client_socket: msg.client_socket,
                        reply_tx: msg.reply_tx,
                        timing: msg.timing,
                        data: fallback,
                    }
                })
                .collect();

            // Coalesced dispatch: MV decision is made once for the whole group
            // (all waiters share the same fingerprint and dispatch moment).
            // The group already passed `limit_is_sufficient(max_limit, primary_needed)`
            // above; reuse `primary_needed` as the rows-needed witness.
            let mv = self.mv_dispatch_decide(fingerprint, primary_needed);
            if let Err(e) = self.pool_serve_coalesced(
                fingerprint,
                primary,
                Arc::clone(&resolved),
                deparsed_sql.clone(),
                generation,
                mv,
                coalesced,
            ) {
                error!(
                    "coalesce serve failed: {}",
                    error_chain_format(e.current_context()),
                );
            }
        }

        if served > 0 {
            crate::metrics::handles()
                .cache
                .coalesce_served
                .increment(served);
        }
        #[allow(clippy::cast_precision_loss)] // queue depth, never near 2^53
        crate::metrics::handles()
            .cache
            .coalesce_waiting
            .set(self.waiting_count() as f64);
    }

    /// Drain all coalesced waiters for a fingerprint that failed.
    /// Falls back to forwarding each waiter to origin.
    pub fn waiting_drain_failed(&self, fingerprint: Fingerprint) {
        let Some(groups) = self.waiting.drain(fingerprint) else {
            return;
        };

        for (_key, waiters) in groups {
            let drain_started = Instant::now();
            for mut msg in waiters {
                msg.timing.drain_started_at = Some(drain_started);
                let _ = reply_forward(
                    msg.reply_tx,
                    msg.client_socket,
                    msg.pipeline,
                    msg.data,
                    msg.timing,
                );
            }
        }

        #[allow(clippy::cast_precision_loss)] // queue depth, never near 2^53
        crate::metrics::handles()
            .cache
            .coalesce_waiting
            .set(self.waiting_count() as f64);
    }

    /// Register pinned queries at startup by sending Register commands with `pinned: true`.
    pub fn pinned_queries_register(&self, pinned: &[PinnedQuery]) -> CacheResult<()> {
        for pq in pinned {
            // Set Loading state in CacheStateView
            self.state_view.cached_queries.insert(
                pq.fingerprint,
                CachedQueryView {
                    state: CachedQueryState::Loading,
                    generation: 0,
                    resolved: None,
                    deparsed_sql: None,
                    max_limit: None,
                    referenced: false,
                    // Writer fills this in after resolution/classification.
                    mv: MvMeta::new(ShapeGate::Skip, None),
                },
            );
            let now = NonZeroU64::new(duration_to_ns_u64(self.state_view.started_at.elapsed()));
            self.state_view
                .metrics
                .entry(pq.fingerprint)
                .or_insert_with(|| QueryMetrics::new(now));

            let (subsumption_tx, _subsumption_rx) = oneshot::channel();
            self.query_tx
                .send(QueryCommand::Register {
                    fingerprint: pq.fingerprint,
                    cacheable_query: Arc::clone(&pq.cacheable_query),
                    search_path: vec!["public".into()].into(),
                    started_at: Instant::now(),
                    subsumption_tx,
                    admit_action: AdmitAction::Admit,
                    pinned: true,
                })
                .map_err(|_| CacheError::WriterSend)?;
        }
        Ok(())
    }

    /// Send a Register command to the writer thread with a subsumption oneshot.
    fn query_register_send(
        &self,
        fingerprint: Fingerprint,
        cacheable_query: Arc<CacheableQuery>,
        search_path: Arc<[EcoString]>,
        subsumption_tx: oneshot::Sender<SubsumptionResult>,
        admit_action: AdmitAction,
    ) -> CacheResult<()> {
        self.query_tx
            .send(QueryCommand::Register {
                fingerprint,
                cacheable_query,
                search_path,
                started_at: Instant::now(),
                subsumption_tx,
                admit_action,
                pinned: false,
            })
            .map_err(|_| CacheError::WriterSend.into())
    }

    /// Hold a request, send Register with subsumption oneshot, and route
    /// based on the writer's response. Subsumed → serve from cache,
    /// NotSubsumed → forward to origin.
    async fn subsumption_await(
        &self,
        msg: QueryRequest,
        fingerprint: Fingerprint,
        admit_action: AdmitAction,
    ) -> CacheResult<()> {
        let (subsumption_tx, subsumption_rx) = oneshot::channel();

        if self
            .query_register_send(
                fingerprint,
                Arc::clone(&msg.cacheable_query),
                Arc::clone(&msg.search_path),
                subsumption_tx,
                admit_action,
            )
            .is_err()
        {
            // Writer channel closed (cache subsystem torn down or restarting):
            // degrade by forwarding to origin rather than failing the client.
            debug!("register channel closed; forwarding query to origin");
            self.metrics_miss_record(fingerprint);
            return reply_forward(
                msg.reply_tx,
                msg.client_socket,
                msg.pipeline,
                msg.data,
                msg.timing,
            );
        }

        match subsumption_rx.await {
            Ok(SubsumptionResult::Subsumed {
                generation,
                resolved,
                deparsed_sql,
            }) => {
                self.metrics_hit_record(fingerprint);
                // Subsumed queries have mv_state = MeasurePending (see Future Work:
                // "MV first-pop for subsumed queries"); mv_dispatch_decide returns
                // false and the serve goes through the fallthrough path.
                let rows_needed = limit_rows_needed(&msg.cacheable_query.query.limit);
                let mv = self.mv_dispatch_decide(fingerprint, rows_needed);
                self.pool_serve(fingerprint, msg, resolved, deparsed_sql, generation, mv)
            }
            Ok(SubsumptionResult::NotSubsumed) | Err(_) => {
                self.metrics_miss_record(fingerprint);
                reply_forward(
                    msg.reply_tx,
                    msg.client_socket,
                    msg.pipeline,
                    msg.data,
                    msg.timing,
                )
            }
        }
    }
}

/// Forward a query to origin by sending the reply through the oneshot channel.
/// Returns the leased client write half to the connection.
pub(super) fn reply_forward(
    reply_tx: ReplySender<CacheReply>,
    socket: ClientSocket,
    pipeline: Option<PipelineContext>,
    data: BytesMut,
    timing: QueryTiming,
) -> CacheResult<()> {
    let buf = match pipeline {
        Some(pipeline) => slices_concat(&pipeline.buffered_bytes),
        None => data,
    };
    reply_tx
        .send(CacheReply {
            socket,
            outcome: CacheOutcome::Forward(buf, timing),
        })
        .map_err(|_| CacheError::Reply.into())
}
