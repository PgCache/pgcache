use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use dashmap::Entry;
use ecow::EcoString;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tokio_util::bytes::BytesMut;
use tracing::{debug, error, info, instrument, trace};

use crate::proxy::ClientSocket;
use crate::query::Fingerprint;
use crate::query::ast::query_expr_fingerprint;
use crate::result::error_chain_format;
use crate::settings::{CachePolicy, DynamicConfig, Settings};
use crate::timing::{QueryTiming, duration_to_ns_u64};

use crate::cache::coalesce_queue::{CoalesceKey, CoalesceQueue, coalesce_deadline};
use crate::cache::messages::{
    AdmitAction, CacheOutcome, CacheReply, PipelineContext, ProxyMessage, QueryCommand,
    SubsumptionResult, slices_concat,
};
use crate::cache::mv::{MvMeta, ShapeGate};
use crate::cache::query::{CacheableQuery, limit_is_sufficient, limit_rows_needed};
use crate::cache::reg_bucket::RegRateBucket;
use crate::cache::reply::ReplySender;
use crate::cache::types::{
    CacheStateView, CachedQueryState, CachedQueryView, PinnedQuery, QueryMetrics,
};
use crate::cache::{CacheError, CacheResult, fast_path};

use super::{CacheDispatch, QueryRequest, ServeRequest};

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

        let reg_bucket = Arc::new(RegRateBucket::new(Arc::clone(&state_view.reg_gate)));
        Ok(Self {
            query_tx,
            serve_tx,
            state_view,
            dynamic: settings.dynamic.clone(),
            waiting: Arc::new(CoalesceQueue::new()),
            cdc_connected,
            reg_bucket,
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
                    serve_shape,
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
                            serve_shape.clone(),
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
                    let key = CoalesceKey::from_request(&msg);
                    let now = Instant::now();
                    msg.timing.waiter_enqueued_at = Some(now);
                    // Forward to origin once this waiter has waited longer than
                    // the population is expected to take (cold: fixed; re-pop:
                    // scaled by the per-query fetch+stage estimate), so a slow
                    // population can't stall serving (PGC-335).
                    let estimate = self
                        .state_view
                        .metrics
                        .get(&fingerprint)
                        .and_then(|m| m.population_fetch_stage_ewma_ms);
                    msg.timing.deadline_at = Some(now + coalesce_deadline(estimate));
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
                    // PGC-277 prototype: cap the new-registration rate so the
                    // population storm can't saturate the box. No token -> forward
                    // to origin without registering (origin is the source of truth).
                    if !self.reg_bucket.try_take() {
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
                    serve_shape: None,
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
                    serve_shape: None,
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
                self.pool_serve(
                    fingerprint,
                    msg,
                    resolved,
                    deparsed_sql,
                    None,
                    generation,
                    mv,
                )
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
