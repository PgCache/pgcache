use std::cell::RefCell;
use std::collections::HashMap;
use std::num::NonZeroU64;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use ecow::EcoString;
use tokio::sync::{mpsc::UnboundedSender, oneshot};
use tokio_util::bytes::BytesMut;
use tracing::{error, info, instrument, trace};

use crate::proxy::ClientSocket;
use crate::query::ast::{LimitClause, QueryExpr, TableNode, query_expr_fingerprint};
use crate::result::error_chain_format;
use crate::settings::{Allowlist, CachePolicy, DynamicConfig, DynamicConfigHandle, Settings};
use crate::timing::{QueryTiming, duration_to_ns_u64};

use super::{
    CacheError, CacheResult,
    messages::{
        AdmitAction, CacheOutcome, CacheReply, PipelineContext, PipelineDescribe, QueryCommand,
        SubsumptionResult,
    },
    mv::{MvMeta, MvServe, MvState, ShapeGate},
    query::{CacheableQuery, limit_is_sufficient, limit_rows_needed},
    types::{
        CacheStateView, CachedQueryState, CachedQueryView, PinnedQuery, QueryMetrics,
        SharedResolved,
    },
};

/// Minimum credit stamped on a Pending entry. Provides a survival floor during
/// cold start (when `last_hits_per_gc` is zero) and for low-traffic workloads.
const MIN_PENDING_CREDIT: u32 = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueryType {
    Simple,
    Extended,
}

/// Key for grouping coalesced requests. Requests in the same group
/// produce identical wire protocol bytes and can share a single worker execution.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CoalesceKey {
    query_type: QueryType,
    emit_rfq: bool,
    has_parse: bool,
    has_bind: bool,
    pipeline_describe: PipelineDescribe,
    result_formats: Vec<i16>,
    limit: Option<LimitClause>,
}

/// A client waiting to receive coalesced response bytes from a shared worker execution.
pub struct CoalescedClient {
    pub client_socket: ClientSocket,
    pub reply_tx: oneshot::Sender<CacheReply>,
    pub timing: QueryTiming,
    /// Pre-computed origin fallback bytes (pipeline.buffered_bytes or raw data).
    pub data: BytesMut,
}

/// Outer key: fingerprint (O(1) drain on Ready/Failed).
/// Inner key: CoalesceKey grouping requests that share identical response bytes.
type WaitingQueue = HashMap<u64, HashMap<CoalesceKey, Vec<QueryRequest>>>;

pub struct QueryRequest {
    pub query_type: QueryType,
    pub data: BytesMut,
    pub cacheable_query: Arc<CacheableQuery>,
    pub result_formats: Vec<i16>,
    pub client_socket: ClientSocket,
    pub reply_tx: oneshot::Sender<CacheReply>,
    /// Resolved search_path for schema resolution
    pub search_path: Vec<EcoString>,
    /// Per-query timing data
    pub timing: QueryTiming,
    /// Pipeline context from the proxy (None for simple queries and cold-path extended)
    pub pipeline: Option<PipelineContext>,
}

/// Request sent to cache worker for executing cached queries.
/// Contains the resolved AST with schema-qualified table names.
pub struct WorkerRequest {
    pub fingerprint: u64,
    pub query_type: QueryType,
    pub data: BytesMut,
    pub resolved: SharedResolved,
    /// Precomputed deparsed SQL body of `resolved`. Spliced into the SET +
    /// body + LIMIT wire string the worker sends to the cache DB.
    pub deparsed_sql: EcoString,
    /// Generation number for row tracking in pgcache_pgrx extension
    pub generation: u64,
    /// Serve from the MV (carrying its aliased output column names) or
    /// from source rows. Decided by the coordinator at dispatch time.
    pub mv: MvServe,
    pub result_formats: Vec<i16>,
    pub client_socket: ClientSocket,
    pub reply_tx: oneshot::Sender<CacheReply>,
    /// Per-query timing data
    pub timing: QueryTiming,
    /// Incoming query's LIMIT clause, appended to SQL at serve time
    pub limit: Option<LimitClause>,
    /// Whether the worker should append ReadyForQuery after this execute's
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
    pub parameter_description: Option<BytesMut>,
    /// Buffered bytes for origin fallback on worker error.
    pub forward_bytes: Option<BytesMut>,
    /// Additional clients to receive the same response bytes.
    /// Empty for non-coalesced requests.
    pub coalesced: Vec<CoalescedClient>,
}

/// Query cache coordinator - routes queries and delegates writes to the writer thread.
#[derive(Clone)]
pub struct QueryCache {
    query_tx: UnboundedSender<QueryCommand>,
    worker_tx: UnboundedSender<WorkerRequest>,
    state_view: Arc<CacheStateView>,
    dynamic: DynamicConfigHandle,
    waiting: Rc<RefCell<WaitingQueue>>,
}

impl QueryCache {
    pub async fn new(
        settings: &Settings,
        query_tx: UnboundedSender<QueryCommand>,
        worker_tx: UnboundedSender<WorkerRequest>,
        state_view: Arc<CacheStateView>,
    ) -> CacheResult<Self> {
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
            worker_tx,
            state_view,
            dynamic: settings.dynamic.clone(),
            waiting: Rc::new(RefCell::new(HashMap::new())),
        })
    }

    /// Check whether all tables in the query are in the allowlist.
    /// Returns true if no allowlist is configured (all tables allowed).
    fn query_allowlist_check(allowlist: &Allowlist, query: &QueryExpr) -> bool {
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

    #[instrument(skip_all)]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn query_dispatch(&mut self, mut msg: QueryRequest) -> CacheResult<()> {
        let cfg = self.dynamic.load();
        if !Self::query_allowlist_check(&cfg.allowed_tables_parsed, &msg.cacheable_query.query) {
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
        let cache_entry = self
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
                // Decide MV fast path vs fallthrough based on current mv_state.
                // Side effect: on Dirty, transitions to RebuildScheduled and sends
                // QueryCommand::MvRebuild to the writer.
                let mv = self.mv_dispatch_decide(fingerprint, rows_needed);
                self.worker_request_send(
                    fingerprint,
                    msg,
                    Arc::clone(resolved),
                    deparsed_sql.clone(),
                    *generation,
                    mv,
                )
            }

            // Cache hit: Ready but insufficient rows — forward and request limit bump
            Some(CachedQueryView {
                state: CachedQueryState::Ready,
                max_limit,
                ..
            }) => {
                trace!("limit bump {fingerprint} cached={max_limit:?} needed={rows_needed:?}");
                self.metrics_miss_record(fingerprint);
                // Set Loading immediately to prevent duplicate LimitBump commands from racing
                self.cached_query_state_set(&fingerprint, CachedQueryState::Loading);
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
                    .map_err(|_| CacheError::WorkerSend)?;
                Ok(())
            }

            // Loading — coalesce: queue request for later dispatch from cache
            Some(CachedQueryView {
                state: CachedQueryState::Loading,
                ..
            }) => {
                trace!("cache loading, coalesce {fingerprint}");
                self.metrics_miss_record(fingerprint);
                let key = Self::coalesce_key_from_request(&msg);
                msg.timing.waiter_enqueued_at = Some(Instant::now());
                self.waiting
                    .borrow_mut()
                    .entry(fingerprint)
                    .or_default()
                    .entry(key)
                    .or_default()
                    .push(msg);
                #[allow(clippy::cast_precision_loss)]
                // queue depth, never near 2^53
                crate::metrics::handles()
                    .cache
                    .coalesce_waiting
                    .set(self.waiting_count() as f64);
                Ok(())
            }

            // Pending — hold request, check subsumption, increment hit count, admit if threshold reached
            Some(CachedQueryView {
                state: CachedQueryState::Pending { hit_count, .. },
                ..
            }) => {
                let new_count = hit_count + 1;
                trace!("pending {fingerprint} count={new_count}");

                if new_count >= cfg.admission_threshold {
                    self.cached_query_state_set(&fingerprint, CachedQueryState::Loading);
                    self.subsumption_await(msg, fingerprint, AdmitAction::Admit)
                        .await
                } else {
                    self.cached_query_state_set(
                        &fingerprint,
                        CachedQueryState::Pending {
                            hit_count: new_count,
                            credit: self.pending_initial_credit(),
                        },
                    );
                    self.subsumption_await(msg, fingerprint, AdmitAction::CheckOnly)
                        .await
                }
            }

            // Invalidated — hold request, check subsumption, fast-readmit (skip admission gate)
            Some(CachedQueryView {
                state: CachedQueryState::Invalidated,
                ..
            }) => {
                trace!("invalidated readmit {fingerprint}");
                self.cached_query_state_set(&fingerprint, CachedQueryState::Loading);
                self.subsumption_await(msg, fingerprint, AdmitAction::Admit)
                    .await
            }

            // Cache miss — hold request, check subsumption
            None => {
                trace!("cache miss {fingerprint}");
                self.query_first_miss_handle(fingerprint, msg, &cfg).await
            }
        }
    }

    /// Record a cache hit in per-query metrics.
    fn metrics_hit_record(&self, fingerprint: u64) {
        self.state_view
            .hits_since_gc
            .fetch_add(1, Ordering::Relaxed);
        if let Some(mut m) = self.state_view.metrics.get_mut(&fingerprint) {
            m.hit_count += 1;
            m.last_hit_at_ns =
                NonZeroU64::new(duration_to_ns_u64(self.state_view.started_at.elapsed()));
        }
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
    fn metrics_miss_record(&self, fingerprint: u64) {
        if let Some(mut m) = self.state_view.metrics.get_mut(&fingerprint) {
            m.miss_count += 1;
        }
    }

    /// Set the CLOCK reference bit for eviction tracking.
    fn clock_reference_set(&self, cache_policy: CachePolicy, fingerprint: &u64) {
        if cache_policy == CachePolicy::Clock
            && let Some(mut entry) = self.state_view.cached_queries.get_mut(fingerprint)
        {
            entry.referenced = true;
        }
    }

    /// Update a cached query's state in the shared view.
    fn cached_query_state_set(&self, fingerprint: &u64, state: CachedQueryState) {
        if let Some(mut entry) = self.state_view.cached_queries.get_mut(fingerprint) {
            entry.state = state;
        }
    }

    /// Build and send a WorkerRequest for serving a query from cache.
    fn worker_request_send(
        &self,
        fingerprint: u64,
        msg: QueryRequest,
        resolved: SharedResolved,
        deparsed_sql: EcoString,
        generation: u64,
        mv: MvServe,
    ) -> CacheResult<()> {
        self.worker_request_send_with(
            fingerprint,
            msg,
            resolved,
            deparsed_sql,
            generation,
            mv,
            vec![],
        )
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
    fn mv_dispatch_decide(&self, fingerprint: u64, rows_needed: Option<u64>) -> MvServe {
        // Local outcome: `Fallthrough` is counted in `mv_fallthrough`;
        // `NoMv` is not (Skipped/Ineligible/missing have no MV and never
        // will). Exhaustive matching below means a future MvState variant
        // forces a deliberate choice between the two.
        enum Decision {
            Hit(Arc<[EcoString]>),
            Fallthrough,
            NoMv,
        }

        let observed = self
            .state_view
            .cached_queries
            .get(&fingerprint)
            .map(|e| (e.mv.state, e.mv.output_columns.clone(), e.mv.limit));

        let decision = match observed {
            None => Decision::NoMv,
            Some((MvState::Fresh, Some(cols), mv_limit))
                if limit_is_sufficient(mv_limit, rows_needed) =>
            {
                Decision::Hit(cols)
            }
            // MV built at a smaller LIMIT than this variant needs.
            Some((MvState::Fresh, Some(_), _)) => Decision::Fallthrough,
            Some((MvState::Fresh, None, _)) => {
                error!(
                    fingerprint,
                    "MV is Fresh but output columns were never captured; \
                     serving from source rows"
                );
                Decision::Fallthrough
            }
            Some((MvState::Pending { has_table }, _, _)) => {
                if let Some(cmd) = self.mv_schedule(fingerprint, has_table) {
                    let _ = self.query_tx.send(cmd);
                }
                Decision::Fallthrough
            }
            Some((MvState::Scheduled { .. }, _, _)) => Decision::Fallthrough,
            Some((MvState::Skipped | MvState::Ineligible, _, _)) => Decision::NoMv,
        };

        match decision {
            Decision::Hit(cols) => {
                crate::metrics::handles().cache.mv_hits.increment(1);
                MvServe::Mv(cols)
            }
            Decision::Fallthrough => {
                crate::metrics::handles().cache.mv_fallthrough.increment(1);
                MvServe::SourceRow
            }
            Decision::NoMv => MvServe::SourceRow,
        }
    }

    /// Check-and-transition under write guard: `Pending { has_table } →
    /// Scheduled { has_table }`. Returns the command to send when the
    /// transition wins the race; `None` when another dispatch beat us or the
    /// entry moved elsewhere.
    fn mv_schedule(&self, fingerprint: u64, has_table: bool) -> Option<QueryCommand> {
        let mut entry = self.state_view.cached_queries.get_mut(&fingerprint)?;
        if entry.mv.state != (MvState::Pending { has_table }) {
            return None;
        }
        entry.mv.state = MvState::Scheduled { has_table };
        Some(QueryCommand::MvBuild { fingerprint })
    }

    /// Build and send a WorkerRequest with coalesced clients attached.
    #[allow(clippy::too_many_arguments)]
    fn worker_request_send_with(
        &self,
        fingerprint: u64,
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
        let timing = msg.timing;

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

        self.worker_tx
            .send(WorkerRequest {
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
                timing,
                limit: msg.cacheable_query.query.limit.clone(),
                emit_rfq,
                has_parse,
                has_bind,
                pipeline_describe,
                parameter_description,
                forward_bytes,
                coalesced,
            })
            .map_err(|e| {
                error!("worker send {e}");
                CacheError::WorkerSend.into()
            })
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
        self.waiting
            .borrow()
            .values()
            .flat_map(|groups| groups.values())
            .map(Vec::len)
            .sum()
    }

    /// Drain all coalesced waiters for a fingerprint that became Ready.
    /// Each coalescing group dispatches a single worker request that broadcasts
    /// response bytes to all clients in the group.
    pub fn waiting_drain_ready(
        &self,
        fingerprint: u64,
        generation: u64,
        resolved: SharedResolved,
        deparsed_sql: EcoString,
        max_limit: Option<u64>,
    ) {
        let Some(groups) = self.waiting.borrow_mut().remove(&fingerprint) else {
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
                        Some(pipeline) => pipeline.buffered_bytes,
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
            if let Err(e) = self.worker_request_send_with(
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
    pub fn waiting_drain_failed(&self, fingerprint: u64) {
        let Some(groups) = self.waiting.borrow_mut().remove(&fingerprint) else {
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
                    search_path: vec!["public".into()],
                    started_at: Instant::now(),
                    subsumption_tx,
                    admit_action: AdmitAction::Admit,
                    pinned: true,
                })
                .map_err(|_| CacheError::WorkerSend)?;
        }
        Ok(())
    }

    /// Send a Register command to the writer thread with a subsumption oneshot.
    fn query_register_send(
        &self,
        fingerprint: u64,
        cacheable_query: Arc<CacheableQuery>,
        search_path: Vec<EcoString>,
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
            .map_err(|_| CacheError::WorkerSend.into())
    }

    /// Hold a request, send Register with subsumption oneshot, and route
    /// based on the writer's response. Subsumed → serve from cache,
    /// NotSubsumed → forward to origin.
    async fn subsumption_await(
        &self,
        msg: QueryRequest,
        fingerprint: u64,
        admit_action: AdmitAction,
    ) -> CacheResult<()> {
        let (subsumption_tx, subsumption_rx) = oneshot::channel();

        self.query_register_send(
            fingerprint,
            Arc::clone(&msg.cacheable_query),
            msg.search_path.clone(),
            subsumption_tx,
            admit_action,
        )?;

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
                self.worker_request_send(fingerprint, msg, resolved, deparsed_sql, generation, mv)
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

    /// Handle first cache miss: hold request, check subsumption.
    /// Register immediately (FIFO/threshold≤1) or start Pending.
    async fn query_first_miss_handle(
        &self,
        fingerprint: u64,
        msg: QueryRequest,
        cfg: &DynamicConfig,
    ) -> CacheResult<()> {
        let immediate_admit = cfg.cache_policy == CachePolicy::Fifo || cfg.admission_threshold <= 1;

        let initial_state = if immediate_admit {
            CachedQueryState::Loading
        } else {
            CachedQueryState::Pending {
                hit_count: 1,
                credit: self.pending_initial_credit(),
            }
        };

        self.state_view.cached_queries.insert(
            fingerprint,
            CachedQueryView {
                state: initial_state,
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
            .entry(fingerprint)
            .or_insert_with(|| QueryMetrics::new(now));

        if immediate_admit {
            trace!("send to writer {fingerprint}");
            self.subsumption_await(msg, fingerprint, AdmitAction::Admit)
                .await
        } else {
            trace!("pending new {fingerprint}");
            self.subsumption_await(msg, fingerprint, AdmitAction::CheckOnly)
                .await
        }
    }
}

/// Forward a query to origin by sending the reply through the oneshot channel.
/// Returns the leased client write half to the connection.
pub(super) fn reply_forward(
    reply_tx: oneshot::Sender<CacheReply>,
    socket: ClientSocket,
    pipeline: Option<PipelineContext>,
    data: BytesMut,
    timing: QueryTiming,
) -> CacheResult<()> {
    let buf = match pipeline {
        Some(pipeline) => pipeline.buffered_bytes,
        None => data,
    };
    reply_tx
        .send(CacheReply {
            socket,
            outcome: CacheOutcome::Forward(buf, timing),
        })
        .map_err(|_| CacheError::Reply.into())
}
