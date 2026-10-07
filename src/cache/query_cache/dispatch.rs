use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use dashmap::Entry;
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::bytes::BytesMut;
use tracing::{debug, error, info, instrument, trace};

use super::explain::ExplainClient;
use super::{CacheDispatch, QueryRequest, ServeJob};
use crate::cache::Generation;
use crate::cache::coalesce_queue::{CoalesceKey, CoalesceQueue, coalesce_deadline};
use crate::cache::messages::{
    CacheMessage, CacheOutcome, CacheReply, PipelineContext, ProxyMessage, QueryCommand,
    slices_concat,
};
use crate::cache::mv::MvMeta;
use crate::cache::mv_shape::ShapeGate;
use crate::cache::query::{CacheableQuery, limit_rows_needed};
use crate::cache::reg_bucket::RegRateBucket;
use crate::cache::reply::ReplySender;
use crate::cache::serve_decision::{DecisionInput, EntrySnapshot, ServeDecision, serve_decide};
use crate::cache::types::{
    CacheStateView, CachedQueryState, CachedQueryView, QueryMetrics, SharedResolved,
};
use crate::cache::{CacheError, CacheResult, fast_path};
use crate::pg::Lsn;
use crate::proxy::ClientSocket;
use crate::query::Fingerprint;
use crate::query::ast::query_expr_fingerprint;
use crate::result::error_chain_format;
use crate::settings::{CachePolicy, DynamicConfig, Settings};
use crate::timing::{QueryTiming, duration_to_ns_u64};

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

/// The per-dispatch identity and decision inputs every decision arm reads.
struct DispatchTarget {
    fingerprint: Fingerprint,
    input: DecisionInput,
}

impl QueryRequest {
    /// Forward this request to origin.
    pub(super) fn forward(self) -> CacheResult<()> {
        reply_forward(
            self.reply_tx,
            self.client_socket,
            forward_bytes(self.pipeline, self.data),
            self.timing,
        )
    }
}

impl CacheDispatch {
    /// Whether the cache is under memory pressure. Read by the proxy to drop
    /// its interned cacheability verdicts as a last resort; the flag is
    /// per-cache-generation and this dispatch is republished on restart, so
    /// callers always observe the live one rather than a stale capture.
    pub fn memory_pressure(&self) -> bool {
        self.state_view
            .registration_throttled
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The settled watermark for this generation — every origin
    /// transaction committing at or below it is either applied to the cache or
    /// produced no decodable output. Read by proxy connections to clear
    /// per-connection read-after-write logs (PGC-124). Read through the current
    /// dispatch (never a cached `Arc`) so a cache restart never surfaces a
    /// stale-high value.
    pub fn settled_lsn(&self) -> Lsn {
        Lsn::from_raw(self.state_view.settled_lsn.load(Ordering::Relaxed))
    }

    /// The decode-stage receive cursor for this generation — the highest WAL
    /// position origin has delivered. Read by the read-after-write gate's
    /// forward attribution (PGC-440) to split apply lag from delivery lag.
    pub fn received_lsn(&self) -> Lsn {
        Lsn::from_raw(self.state_view.received_lsn.load(Ordering::Relaxed))
    }

    /// The resolved form of a registered (Ready) query, if present. Used by the
    /// read-after-write gate for row-level INSERT disjointness (PGC-124) — the
    /// proxy has no catalog to resolve the query itself, so it reads the resolved
    /// node the writer produced at registration. `None` for unregistered or
    /// still-loading queries (the gate then forwards conservatively).
    pub fn cached_query_resolved(&self, fingerprint: Fingerprint) -> Option<SharedResolved> {
        let view = self.state_view.cached_queries.get(&fingerprint)?;
        matches!(view.state, CachedQueryState::Ready)
            .then(|| view.resolved.clone())
            .flatten()
    }

    pub async fn new(
        settings: &Settings,
        query_tx: UnboundedSender<QueryCommand>,
        serve_tx: UnboundedSender<ServeJob>,
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
        let ProxyMessage {
            message,
            client_socket,
            reply_tx,
            search_path,
            timing,
            pipeline,
            transaction_status,
        } = proxy_msg;

        // `pgcache_explain(...)` is a diagnostic that runs against cached state
        // directly; route it before CDC-liveness gating and query conversion.
        if let CacheMessage::Explain(spec, _) = message {
            self.explain_dispatch(
                spec,
                ExplainClient {
                    client_socket,
                    reply_tx,
                    timing,
                    transaction_status,
                },
            );
            return;
        }

        if !self.cdc_connected.load(Ordering::Relaxed) {
            // CDC down: forward to origin rather than serve possibly-stale data.
            let data = message.into_data();
            let _ = reply_forward(
                reply_tx,
                client_socket,
                forward_bytes(pipeline, data),
                timing,
            );
            return;
        }

        match message.into_query_data() {
            Ok(query_data) => {
                let request = QueryRequest {
                    query_type: query_data.query_type,
                    data: query_data.data,
                    cacheable_query: query_data.cacheable_query,
                    result_formats: query_data.result_formats,
                    client_socket,
                    reply_tx,
                    search_path,
                    timing,
                    pipeline,
                    transaction_status,
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
                    reply_tx,
                    client_socket,
                    forward_bytes(pipeline, data),
                    timing,
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
        if !fast_path::query_allowlist_check(
            &cfg.allowed_tables_parsed,
            msg.cacheable_query.query(),
        ) {
            crate::metrics::handles()
                .query
                .allowlist_skipped
                .increment(1);
            return msg.forward();
        }

        let target = self.dispatch_target(&msg.cacheable_query, &cfg);
        let fingerprint = target.fingerprint;
        trace!("{fingerprint}");

        let mut cache_entry = self.entry_lookup_timed(fingerprint);
        // Stamp lookup_complete uniformly across all paths so `lookup_seconds`
        // means "proxy dispatch → cache state lookup done." Path-specific
        // post-lookup work is captured by dedicated histograms
        // (forward_decision / coalesce_intake / coalesce_wait).
        msg.timing.lookup_complete_at = Some(Instant::now());

        // Retry loop: decisions that write state re-decide under the write
        // guard (`transition_apply`), and the coalesce arm re-checks under the
        // waiting lock. Losing either race falls through to re-read the entry
        // and re-dispatch against the fresh state.
        loop {
            let snapshot = cache_entry.as_ref().map(EntrySnapshot::from);
            let decision = serve_decide(snapshot.as_ref(), &target.input, || {
                self.reg_bucket.try_take()
            });
            match decision {
                ServeDecision::Hit => {
                    return self.ready_hit(&target, cache_entry.as_ref(), msg).await;
                }

                // Ready but insufficient rows — forward and request a limit bump.
                // A single dispatch claims the bump; if another bumper won (or a
                // completed bump made the entry sufficient), re-dispatch.
                ServeDecision::LimitBump { .. } => {
                    trace!(
                        "limit bump {fingerprint} cached={:?} needed={:?}",
                        snapshot.and_then(|s| s.max_limit),
                        target.input.rows_needed
                    );
                    if self
                        .transition_apply(fingerprint, decision, &target.input)
                        .is_some()
                    {
                        return self.limit_bump_forward(&target, msg);
                    }
                }

                ServeDecision::Coalesce => match self.coalesce_enqueue(&target, msg).await {
                    Ok(()) => return Ok(()),
                    Err(returned) => msg = returned,
                },

                // Pending (count a hit, admit at threshold), Invalidated (fast
                // readmit) or cold (claim the slot): the writer runs the
                // subsumption check and, on `Admit`, registers and populates.
                ServeDecision::Register { .. } => {
                    trace!(
                        "register {fingerprint} from {:?}",
                        snapshot.map(|s| s.state)
                    );
                    if let Some(ServeDecision::Register { action, .. }) =
                        self.transition_apply(fingerprint, decision, &target.input)
                    {
                        return self.subsumption_await(msg, fingerprint, action).await;
                    }
                }

                // Memory pressure or the new-registration rate cap (PGC-277):
                // forward to origin without touching cache state.
                ServeDecision::Forward(reason) => {
                    trace!("forward {fingerprint}: {reason:?}");
                    crate::metrics::handles()
                        .cache
                        .registration_throttled_total
                        .increment(1);
                    return msg.forward();
                }
            }

            // Lost a race: re-read the entry and re-dispatch against the
            // now-current state.
            cache_entry = self.entry_snapshot_read(fingerprint);
        }
    }

    /// The dispatch's fingerprint and decision inputs.
    fn dispatch_target(
        &self,
        cacheable_query: &CacheableQuery,
        cfg: &DynamicConfig,
    ) -> DispatchTarget {
        DispatchTarget {
            fingerprint: query_expr_fingerprint(cacheable_query.query()),
            input: DecisionInput {
                rows_needed: limit_rows_needed(&cacheable_query.query().limit),
                admission_threshold: cfg.admission_threshold,
                cache_policy: cfg.cache_policy,
                throttled: self.state_view.throttled(),
                pending_credit: self.pending_initial_credit(),
            },
        }
    }

    /// The first entry lookup, timed into `lookup_latency`.
    fn entry_lookup_timed(&self, fingerprint: Fingerprint) -> Option<CachedQueryView> {
        let lookup_start = Instant::now();
        let entry = self.entry_snapshot_read(fingerprint);
        crate::metrics::handles()
            .cache
            .lookup_latency
            .record(lookup_start.elapsed().as_secs_f64());
        entry
    }

    /// A clone of the query's current state-view entry, if any.
    fn entry_snapshot_read(&self, fingerprint: Fingerprint) -> Option<CachedQueryView> {
        self.state_view
            .cached_queries
            .get(&fingerprint)
            .map(|entry| entry.clone())
    }

    /// Serve a Ready query from the cache.
    async fn ready_hit(
        &self,
        target: &DispatchTarget,
        entry: Option<&CachedQueryView>,
        msg: QueryRequest,
    ) -> CacheResult<()> {
        let fingerprint = target.fingerprint;
        let Some(CachedQueryView {
            generation,
            resolved: Some(resolved),
            deparsed_sql: Some(deparsed_sql),
            serve_shape,
            ..
        }) = entry
        else {
            // The writer publishes the resolved form together with Ready;
            // serve from origin rather than guess if not.
            debug_assert!(false, "Ready entry without resolved form {fingerprint}");
            debug!("ready entry without resolved form, forwarding {fingerprint}");
            return msg.forward();
        };
        self.metrics_hit_record(fingerprint);
        self.clock_reference_set(target.input.cache_policy, &fingerprint);
        self.hit_serve(
            fingerprint,
            msg,
            Arc::clone(resolved),
            deparsed_sql.clone(),
            serve_shape.clone(),
            *generation,
            target.input.rows_needed,
        )
        .await
    }

    /// The limit bump was claimed: forward this request and ask the writer to
    /// re-populate with the larger limit.
    fn limit_bump_forward(&self, target: &DispatchTarget, msg: QueryRequest) -> CacheResult<()> {
        self.metrics_miss_record(target.fingerprint);
        msg.forward()?;
        self.query_tx
            .send(QueryCommand::LimitBump {
                fingerprint: target.fingerprint,
                max_limit: target.input.rows_needed,
            })
            .map_err(|_| CacheError::WriterSend)?;
        Ok(())
    }

    /// Loading — coalesce: queue the request for dispatch from cache once the
    /// population completes. The state is re-checked under the waiting lock to
    /// avoid an orphaned waiter: the writer sets `Ready` before sending the
    /// notify that drains this queue, so if we still observe `Loading` while
    /// holding the lock, the drain has not yet removed our group (or will see
    /// us). Otherwise the request comes back for re-dispatch.
    // The large `Err` payload is intentional, as in `enqueue_if_loading`: it
    // returns the message by move for re-dispatch, and boxing would allocate
    // on the (rare) state-advanced path.
    #[allow(clippy::result_large_err)]
    async fn coalesce_enqueue(
        &self,
        target: &DispatchTarget,
        mut msg: QueryRequest,
    ) -> Result<(), QueryRequest> {
        let fingerprint = target.fingerprint;
        trace!("cache loading, coalesce {fingerprint}");
        fault_coalesce_enqueue_delay().await;
        let key = CoalesceKey::from_request(&msg);
        let now = Instant::now();
        msg.timing.waiter_enqueued_at = Some(now);
        // Forward to origin once this waiter has waited longer than the
        // population is expected to take (cold: fixed; re-pop: scaled by the
        // per-query fetch+stage estimate), so a slow population can't stall
        // serving (PGC-335).
        let estimate = self
            .state_view
            .metrics
            .get(&fingerprint)
            .and_then(|m| m.population_fetch_stage_ewma_ms);
        msg.timing.deadline_at = Some(now + coalesce_deadline(estimate));
        self.waiting
            .enqueue_if_loading(&self.state_view, fingerprint, key, msg)?;
        self.metrics_miss_record(fingerprint);
        #[allow(clippy::cast_precision_loss)]
        // queue depth, never near 2^53
        crate::metrics::handles()
            .cache
            .coalesce_waiting
            .set(self.waiting.waiter_count() as f64);
        Ok(())
    }

    /// Record a cache hit in per-query metrics.
    pub(super) fn metrics_hit_record(&self, fingerprint: Fingerprint) {
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
    pub(super) fn metrics_miss_record(&self, fingerprint: Fingerprint) {
        if let Some(mut m) = self.state_view.metrics.get_mut(&fingerprint) {
            m.miss_count += 1;
        }
    }

    /// Set the CLOCK reference bit for eviction tracking.
    fn clock_reference_set(&self, cache_policy: CachePolicy, fingerprint: &Fingerprint) {
        fast_path::clock_reference_set(&self.state_view, cache_policy, fingerprint);
    }

    /// Apply a decision's state write under the write guard. For an existing
    /// entry the decision is re-made from the guarded state rather than
    /// trusting the caller's snapshot — the compare-and-set that makes the cold
    /// arms race-safe under the multi-thread runtime (cf.
    /// `fast_path::mv_schedule`); a cold claim inserts iff the slot is still
    /// vacant. Returns the decision actually applied (same kind as `decision`,
    /// possibly with a different admit action), or `None` when the entry
    /// advanced and the caller must re-dispatch.
    fn transition_apply(
        &self,
        fingerprint: Fingerprint,
        decision: ServeDecision,
        input: &DecisionInput,
    ) -> Option<ServeDecision> {
        let transition = decision.transition()?;
        if transition.expected.is_none() {
            // Cold claim: the caller's decision already consumed the
            // registration budget, so insert exactly what it decided.
            match self.state_view.cached_queries.entry(fingerprint) {
                Entry::Occupied(_) => return None,
                Entry::Vacant(slot) => {
                    slot.insert(CachedQueryView {
                        state: transition.new,
                        generation: Generation::ZERO,
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
                .or_insert_with(|| QueryMetrics::new(now, &self.state_view.latency_template));
            return Some(decision);
        }

        let mut entry = self.state_view.cached_queries.get_mut(&fingerprint)?;
        // An existing entry never consults the registration budget.
        let guarded = serve_decide(Some(&EntrySnapshot::from(&*entry)), input, || false);
        if std::mem::discriminant(&guarded) != std::mem::discriminant(&decision) {
            return None;
        }
        entry.state = guarded.transition()?.new;
        Some(guarded)
    }
}

/// What a forwarded request sends to origin: the buffered pipeline bytes for
/// an extended-protocol request, else the request's own bytes.
pub(super) fn forward_bytes(pipeline: Option<PipelineContext>, data: BytesMut) -> BytesMut {
    match pipeline {
        Some(pipeline) => slices_concat(&pipeline.buffered_bytes),
        None => data,
    }
}

/// Forward a query to origin by sending the reply through the oneshot channel.
/// Returns the leased client write half to the connection.
pub(super) fn reply_forward(
    reply_tx: ReplySender<CacheReply>,
    socket: ClientSocket,
    buf: BytesMut,
    timing: QueryTiming,
) -> CacheResult<()> {
    reply_tx
        .send(CacheReply {
            socket,
            outcome: CacheOutcome::Forward(buf, timing),
        })
        .map_err(|_| CacheError::Reply.into())
}
