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

use crate::proxy::{ClientSocket, ExplainSpec, ExplainTarget};
use crate::query::Fingerprint;
use crate::query::ast::{query_expr_convert_raw, query_expr_fingerprint};
use crate::result::error_chain_format;
use crate::settings::{CachePolicy, Settings};
use crate::timing::{QueryTiming, duration_to_ns_u64};

use crate::cache::coalesce_queue::{CoalesceKey, CoalesceQueue, coalesce_deadline};
use crate::cache::explain::{ExplainJob, ExplainKind};
use crate::cache::messages::{
    AdmitAction, CacheMessage, CacheOutcome, CacheReply, PipelineContext, ProxyMessage,
    QueryCommand, SubsumptionResult, slices_concat,
};
use crate::cache::mv::{MvMeta, MvServe, MvState, ShapeGate};
use crate::cache::query::{CacheableQuery, limit_rows_needed};
use crate::cache::reg_bucket::RegRateBucket;
use crate::cache::reply::ReplySender;
use crate::cache::serve_decision::{DecisionInput, EntrySnapshot, ServeDecision, serve_decide};
use crate::cache::types::{
    CacheStateView, CachedQueryState, CachedQueryView, PinnedQuery, QueryMetrics,
};
use crate::cache::{CacheError, CacheResult, fast_path};

use super::{CacheDispatch, QueryRequest, ServeJob};

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
        } = proxy_msg;

        // `pgcache_explain(...)` is a diagnostic that runs against cached state
        // directly; route it before CDC-liveness gating and query conversion.
        if let CacheMessage::Explain(spec, _) = message {
            self.explain_dispatch(spec, client_socket, reply_tx, timing);
            return;
        }

        if !self.cdc_connected.load(Ordering::Relaxed) {
            // CDC down: forward to origin rather than serve possibly-stale data.
            let data = message.into_data();
            let _ = reply_forward(reply_tx, client_socket, pipeline, data, timing);
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
                let _ = reply_forward(reply_tx, client_socket, pipeline, data, timing);
            }
        }
    }

    /// Route a `pgcache_explain(...)` request: resolve its target to a cached
    /// query and hand an [`ExplainJob`] to the serve pool (PGC-345). The serve
    /// pool borrows a connection and runs the actual EXPLAIN off this thread.
    fn explain_dispatch(
        &self,
        spec: ExplainSpec,
        client_socket: ClientSocket,
        reply_tx: ReplySender<CacheReply>,
        timing: QueryTiming,
    ) {
        let kind = self.explain_kind_build(spec);
        let job = ExplainJob {
            client_socket,
            reply_tx,
            timing,
            kind,
        };
        if self.serve_tx.send(ServeJob::Explain(job)).is_err() {
            // Serve channel closed (subsystem teardown): the leased socket drops
            // with the job and the connection tears down.
            debug!("serve channel closed; dropping explain request");
        }
    }

    /// Resolve an [`ExplainSpec`] to the concrete work the serve pool should do:
    /// a [`ExplainKind::Run`] for a Ready cached query, or
    /// [`ExplainKind::Unavailable`] with a reason otherwise.
    fn explain_kind_build(&self, spec: ExplainSpec) -> ExplainKind {
        let fingerprint = match &spec.target {
            ExplainTarget::Fingerprint(value) => Fingerprint::from_raw(*value),
            ExplainTarget::Sql(sql) => match explain_sql_fingerprint(sql) {
                Some(fingerprint) => fingerprint,
                None => {
                    return ExplainKind::Unavailable {
                        message: "could not parse query for explain".into(),
                    };
                }
            },
        };

        let Some(view) = self
            .state_view
            .cached_queries
            .get(&fingerprint)
            .map(|view| view.clone())
        else {
            return ExplainKind::Unavailable {
                message: format!("query not cached (fingerprint {fingerprint})").into(),
            };
        };

        let CachedQueryView {
            state,
            resolved,
            serve_shape,
            mv,
            ..
        } = view;
        match state {
            CachedQueryState::Ready => match resolved {
                Some(resolved) => {
                    // Read-only backend decision: reflect what would serve now
                    // without the serve-path `mv_dispatch_decide` side effects (a
                    // diagnostic must not schedule an MV build or move serve
                    // metrics). A Fresh MV with captured columns serves from the
                    // MV; everything else serves from source rows.
                    let mv = match (mv.state(), mv.output_columns) {
                        (MvState::Fresh, Some(columns)) => MvServe::Mv(columns),
                        _ => MvServe::SourceRow,
                    };
                    ExplainKind::Run {
                        fingerprint,
                        mv,
                        serve_shape,
                        resolved,
                        options: spec.options,
                    }
                }
                None => ExplainKind::Unavailable {
                    message: "query cannot be served from cache (no resolved form)".into(),
                },
            },
            state @ (CachedQueryState::Pending { .. }
            | CachedQueryState::Loading
            | CachedQueryState::Invalidated) => ExplainKind::Unavailable {
                message: format!("query cannot be served from cache (state {state:?})").into(),
            },
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
            return reply_forward(
                msg.reply_tx,
                msg.client_socket,
                msg.pipeline,
                msg.data,
                msg.timing,
            );
        }

        let fingerprint = query_expr_fingerprint(msg.cacheable_query.query());
        trace!("{fingerprint}");

        let input = DecisionInput {
            rows_needed: limit_rows_needed(&msg.cacheable_query.query().limit),
            admission_threshold: cfg.admission_threshold,
            cache_policy: cfg.cache_policy,
            throttled: self.state_view.throttled(),
            pending_credit: self.pending_initial_credit(),
        };

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

        // Retry loop: decisions that write state re-decide under the write
        // guard (`transition_apply`), and the coalesce arm re-checks under the
        // waiting lock. Losing either race falls through to re-read the entry
        // and re-dispatch against the fresh state.
        loop {
            let snapshot = cache_entry.as_ref().map(EntrySnapshot::from);
            let decision = serve_decide(snapshot.as_ref(), &input, || self.reg_bucket.try_take());
            match decision {
                ServeDecision::Hit => {
                    let Some(CachedQueryView {
                        generation,
                        resolved: Some(resolved),
                        deparsed_sql: Some(deparsed_sql),
                        serve_shape,
                        ..
                    }) = &cache_entry
                    else {
                        // The writer publishes the resolved form together with
                        // Ready; serve from origin rather than guess if not.
                        debug_assert!(false, "Ready entry without resolved form {fingerprint}");
                        debug!("ready entry without resolved form, forwarding {fingerprint}");
                        return reply_forward(
                            msg.reply_tx,
                            msg.client_socket,
                            msg.pipeline,
                            msg.data,
                            msg.timing,
                        );
                    };
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
                            input.rows_needed,
                        )
                        .await;
                }

                // Ready but insufficient rows — forward and request a limit bump.
                // A single dispatch claims the bump; if another bumper won (or a
                // completed bump made the entry sufficient), re-dispatch.
                ServeDecision::LimitBump { .. } => {
                    trace!(
                        "limit bump {fingerprint} cached={:?} needed={:?}",
                        snapshot.and_then(|s| s.max_limit),
                        input.rows_needed
                    );
                    if self
                        .transition_apply(fingerprint, decision, &input)
                        .is_some()
                    {
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
                                max_limit: input.rows_needed,
                            })
                            .map_err(|_| CacheError::WriterSend)?;
                        return Ok(());
                    }
                }

                // Loading — coalesce: queue request for later dispatch from cache.
                // The state is re-checked under the waiting lock to avoid an
                // orphaned waiter: the writer sets `Ready` before sending the
                // notify that drains this queue, so if we still observe `Loading`
                // while holding the lock, the drain has not yet removed our group
                // (or will see us); otherwise we fall through and re-dispatch.
                ServeDecision::Coalesce => {
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
                        }
                    }
                }

                // Pending (count a hit, admit at threshold), Invalidated (fast
                // readmit) or cold (claim the slot): the writer runs the
                // subsumption check and, on `Admit`, registers and populates.
                ServeDecision::Register { .. } => {
                    trace!(
                        "register {fingerprint} from {:?}",
                        snapshot.map(|s| s.state)
                    );
                    if let Some(ServeDecision::Register { action, .. }) =
                        self.transition_apply(fingerprint, decision, &input)
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
                    return reply_forward(
                        msg.reply_tx,
                        msg.client_socket,
                        msg.pipeline,
                        msg.data,
                        msg.timing,
                    );
                }
            }

            // Lost a race: re-read the entry and re-dispatch against the
            // now-current state.
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
                let rows_needed = limit_rows_needed(&msg.cacheable_query.query().limit);
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

/// Fingerprint the inline SQL of a `pgcache_explain('<sql>')` request, the same
/// way registration keys it (raw-tree convert → `query_expr_fingerprint`), so the
/// lookup hits the cached entry. `None` if the argument doesn't parse as a SELECT.
fn explain_sql_fingerprint(sql: &str) -> Option<Fingerprint> {
    pg_query::parse_raw_scoped(sql, |tree| unsafe { query_expr_convert_raw(tree) })
        .ok()
        .and_then(Result::ok)
        .map(|query| query_expr_fingerprint(&query))
}
