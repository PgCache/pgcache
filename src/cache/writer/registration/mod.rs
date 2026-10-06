//! The query registration / population lifecycle on the writer: resolving and
//! admitting queries (`resolve`, `register`), population worker scaling and
//! dispatch (`pool`), lifecycle transitions (`lifecycle`), and the population
//! merge drain (`merge`). This module holds the shared types and the
//! `QueryCommand` router.

use std::cmp::Reverse;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Instant;

use ecow::EcoString;
use metrics::Histogram;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::spawn_local;
use tokio_postgres::Client;
use tracing::{debug, error, trace};

use super::core::WriterCore;
use super::merge_queue::PendingMerge;
use super::population::{
    PopulationSpawnContext, population_dispatcher, population_worker_connect, population_worker_run,
};
use crate::cache::Generation;
use crate::cache::messages::{QueryCommand, RegisterRequest};
use crate::cache::mv_shape::ShapeGate;
use crate::cache::population_pool::PopulationPool;
use crate::cache::types::{CachedQuery, SharedResolved};
use crate::cache::{CacheError, CacheResult, MapIntoReport, ReportExt};
use crate::catalog::{TableMetadata, aggregate_functions_load};
use crate::oid::Oid;
use crate::query::ast::QueryExpr;
use crate::query::resolved::ResolvedSelectNode;
use crate::query::{Fingerprint, QueryShape};
use crate::result::error_chain_format;
use crate::settings::Settings;

mod lifecycle;
mod merge;
mod pool;
mod register;
mod resolve;

/// Work item for population worker pool.
pub(crate) struct PopulationWork {
    pub fingerprint: Fingerprint,
    pub generation: Generation,
    pub table_metadata: Vec<TableMetadata>,
    /// SELECT branches extracted from the query at registration time.
    /// For simple SELECT queries, this contains one branch.
    /// For set operations (UNION/INTERSECT/EXCEPT), contains all branches.
    pub branches: Vec<ResolvedSelectNode>,
    /// Maximum rows to fetch during population. `None` = fetch all rows.
    pub max_limit: Option<u64>,
    /// Staging table per relation, checked out from the pool at dispatch
    /// (PGC-293): `(relation_oid, table_name, needs_create)`. The worker loads
    /// these instead of minting per-population names; `needs_create` is set only
    /// for a freshly minted slot (the worker `CREATE … IF NOT EXISTS`es it).
    pub staging: Vec<(Oid, EcoString, bool)>,
    /// Stamped at construction; used by the population worker to record
    /// `pgcache.cache.population.wait_seconds`.
    pub enqueued_at: Instant,
}

/// Intermediate result from resolving a query before subsumption check or population.
pub(super) struct QueryResolution {
    pub(super) resolved: SharedResolved,
    /// Deparsed SQL body of `resolved`. Computed once here and reused on the
    /// serving hot path; see `CachedQuery.deparsed_sql`.
    pub(super) deparsed_sql: EcoString,
    /// Parameterized serve shape of `resolved` (PGC-294), computed alongside
    /// `deparsed_sql`; see `CachedQuery.serve_shape`.
    pub(super) serve_shape: QueryShape,
    pub(super) relation_oids: Vec<Oid>,
    pub(super) base_query: QueryExpr,
    pub(super) max_limit: Option<u64>,
    /// MV cap, separate from `max_limit`. Set for join shapes only (the
    /// MV body applies the user's LIMIT over the source-row cache);
    /// `None` for other reducers, whose results are already collapsed.
    pub(super) mv_limit: Option<u64>,
    /// MV shape gate. Also gates `max_limit`: reducer shapes force
    /// `max_limit = None` so source-row population isn't truncated in a way
    /// that would break re-evaluation (aggregates, GROUP BY, DISTINCT,
    /// windows all depend on the full input row set to produce correct
    /// result rows).
    pub(super) shape_gate: ShapeGate,
}

/// Who is registering a cached-query entry: carried from the `Register`
/// request into the entry it creates.
#[derive(Clone, Copy)]
pub(super) struct RegistrationIdentity {
    pub(super) fingerprint: Fingerprint,
    pub(super) started_at: Instant,
    /// Pinned queries are protected from eviction and auto-readmitted after
    /// invalidation.
    pub(super) pinned: bool,
}

/// One population to start: which query and generation, and what to fetch.
struct PopulationTarget<'a> {
    fingerprint: Fingerprint,
    generation: Generation,
    resolved: &'a SharedResolved,
    max_limit: Option<u64>,
}

/// Assign a generation and insert the `CachedQuery` entry built from
/// `resolution`. Returns `(generation, relations_changed)`: `relations_changed`
/// is true if any relation became active (0→1 refcount), so the caller knows
/// whether the publication needs syncing inline.
pub(super) fn cached_query_insert(
    core: &mut WriterCore,
    identity: RegistrationIdentity,
    resolution: QueryResolution,
) -> (Generation, bool) {
    let generation = core.cache.generation_allocate();
    let changed = core.active_relations_acquire(&resolution.relation_oids);
    core.cache.cached_queries.insert_overwrite(CachedQuery {
        fingerprint: identity.fingerprint,
        generation,
        relation_oids: resolution.relation_oids,
        query: resolution.base_query,
        resolved: resolution.resolved,
        deparsed_sql: resolution.deparsed_sql,
        serve_shape: resolution.serve_shape,
        max_limit: resolution.max_limit,
        cached_bytes: 0,
        registration_started_at: Some(identity.started_at),
        invalidated: false,
        pinned: identity.pinned,
    });
    (generation, changed)
}

/// Test-only evict-mid-build (`PGCACHE_FAULT_MV_EVICT_ON_BUILD`): one-shot,
/// consumed on the first `MvBuild` dispatch that actually launched a task, so
/// a test can deterministically exercise eviction while a build is in flight
/// (deferred re-dispatch + stale-completion discard). Always `false` unless
/// built with `--features fault-injection`.
#[cfg(feature = "fault-injection")]
fn fault_mv_evict_on_build(core: &WriterCore, fingerprint: Fingerprint) -> bool {
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicBool, Ordering};
    static ARMED: OnceLock<AtomicBool> = OnceLock::new();
    let armed = ARMED.get_or_init(|| {
        AtomicBool::new(std::env::var_os("PGCACHE_FAULT_MV_EVICT_ON_BUILD").is_some())
    });
    core.mv_builds_inflight.contains(&fingerprint) && armed.swap(false, Ordering::Relaxed)
}
#[cfg(not(feature = "fault-injection"))]
fn fault_mv_evict_on_build(_core: &WriterCore, _fingerprint: Fingerprint) -> bool {
    false
}

/// The per-command handle-time histogram.
fn command_histogram(cmd: &QueryCommand) -> &'static Histogram {
    let reg = &crate::metrics::handles().reg;
    match cmd {
        QueryCommand::Register(_) => &reg.cmd_register,
        QueryCommand::Merge(_) => &reg.cmd_ready,
        QueryCommand::Failed { .. } => &reg.cmd_failed,
        QueryCommand::LimitBump { .. } => &reg.cmd_limit_bump,
        QueryCommand::Readmit { .. } => &reg.cmd_readmit,
        QueryCommand::MvBuild { .. } => &reg.cmd_mv_build,
        QueryCommand::MvBuildComplete { .. } => &reg.cmd_mv_build_complete,
    }
}

/// Most registration failures are routing decisions (the query isn't
/// cacheable and is forwarded to origin), not faults — log at debug so
/// swallowed-error scanners don't treat an expected forward as a fault. A
/// table with no primary key surfaces as `UnknownTable` (PGC-135, the
/// documented "forwarded silently" path); a query the resolver can't model
/// (ambiguous self-join columns, USING/NATURAL qualifiers, etc.) as a
/// `ResolveError`; a correlated subquery that can't be decorrelated as a
/// `DecorrelateError`. A forwarded query still returns the correct result; a
/// wrong cached result is caught by the result-diff path, and "should have
/// cached" by routing assertions — neither relies on this error log.
fn register_failure_log(fingerprint: Fingerprint, error: &CacheError) {
    if matches!(
        error,
        CacheError::DecorrelateError(_)
            | CacheError::ResolveError(_)
            | CacheError::UnknownTable { .. }
    ) {
        debug!(
            "query {fingerprint} forwarded (not cacheable): {}",
            error_chain_format(error),
        );
    } else {
        error!(
            "query register failed for {fingerprint}: {}",
            error_chain_format(error),
        );
    }
}

/// Owns the query registration / population path: consumes `QueryCommand`s
/// and drives resolution, subsumption, population dispatch, and lifecycle
/// transitions against the shared `WriterCore`. Holds the population worker
/// channels and aggregate-function catalog (used for decorrelation).
pub(super) struct WriterRegistration {
    /// Shared population work queue; a dispatcher task pairs each item with
    /// the next idle worker.
    populate_tx: UnboundedSender<PopulationWork>,
    /// Spawn ingredients for elastic worker scale-up (PGC-437).
    spawn_ctx: PopulationSpawnContext,
    /// No spawn attempts before this instant (set after a connect failure).
    spawn_cooldown_until: std::cell::Cell<Option<Instant>>,
    /// Aggregate function names from pg_proc, used for scalar subquery decorrelation.
    aggregate_functions: std::collections::HashSet<EcoString>,
}

impl WriterRegistration {
    pub(super) async fn new(
        settings: &Settings,
        db_origin: &Rc<Client>,
        query_tx: UnboundedSender<QueryCommand>,
        registration_throttled: Arc<AtomicBool>,
        pool: Arc<PopulationPool>,
    ) -> CacheResult<Self> {
        let aggregate_functions = aggregate_functions_load(db_origin)
            .await
            .map_into_report::<CacheError>()
            .attach_loc("loading aggregate functions")?;

        // Shared work queue and idle-slot channel; the dispatcher pairs each
        // work item with the next idle worker.
        let (populate_tx, work_rx) = tokio::sync::mpsc::unbounded_channel();
        let (idle_tx, idle_rx) = tokio::sync::mpsc::unbounded_channel();
        spawn_local(population_dispatcher(
            work_rx,
            idle_rx,
            query_tx.clone(),
            Arc::clone(&pool),
        ));

        pool.desired_workers_set(settings.population_workers_min);
        let spawn_ctx = PopulationSpawnContext {
            idle_tx,
            cache_settings: settings.cache.clone(),
            origin_settings: settings.origin.clone(),
            query_tx,
            throttled: registration_throttled,
            pool,
        };

        // Spawn the initial worker set, failing writer startup if any
        // connection can't open (matching pre-elastic behavior); later
        // scale-up spawns are best-effort (see population_pool_reconcile).
        for _ in 0..settings.population_workers_min {
            let id = spawn_ctx.pool.worker_reserve();
            let connections = population_worker_connect(&spawn_ctx, id).await?;
            spawn_local(population_worker_run(spawn_ctx.clone(), id, connections));
        }

        Ok(Self {
            populate_tx,
            spawn_ctx,
            spawn_cooldown_until: std::cell::Cell::new(None),
            aggregate_functions,
        })
    }

    /// Handle a query command, dispatching to the appropriate method.
    pub(super) async fn query_command_handle(
        &mut self,
        core: &mut WriterCore,
        cmd: QueryCommand,
    ) -> CacheResult<()> {
        let histogram = command_histogram(&cmd);
        let handle_start = Instant::now();
        match cmd {
            QueryCommand::Register(request) => self.register_command(core, request).await,
            QueryCommand::Merge(merge) => {
                // Queue the merge; the writer loop drains it once no CDC frame
                // is open (PGC-250) AND the apply watermark has reached its
                // snapshot LSN (PGC-272). Running it here could be mid-frame,
                // racing the CDC writer's frame txn on the shared cache table.
                core.merges.pending.push(Reverse(PendingMerge(merge)));
            }
            QueryCommand::Failed {
                fingerprint,
                generation,
            } => self.population_failed(core, fingerprint, generation),
            QueryCommand::LimitBump {
                fingerprint,
                max_limit,
            } => self.limit_bump_command(core, fingerprint, max_limit).await,
            QueryCommand::Readmit { fingerprint } => self.readmit_command(core, fingerprint).await,
            QueryCommand::MvBuild { fingerprint } => mv_build_command(core, fingerprint).await,
            QueryCommand::MvBuildComplete {
                fingerprint,
                outcome,
            } => {
                trace!("command mv build complete {fingerprint}");
                core.mv_build_complete(fingerprint, outcome).await;
            }
        }
        // Publication dirty drain runs per-command because it's correctness
        // work (it surfaces relation changes to the CDC publication). Gauge
        // emission is on a periodic tick in `writer_run` — iterating the
        // state_view DashMap per command dominated writer time at scale.
        core.publication_dirty_drain().await?;
        histogram.record(handle_start.elapsed().as_secs_f64());
        Ok(())
    }

    async fn register_command(&mut self, core: &mut WriterCore, request: RegisterRequest) {
        let fingerprint = request.fingerprint;
        trace!("command query register {fingerprint}");
        if let Err(e) = self.query_register(core, request).await {
            register_failure_log(fingerprint, e.current_context());
            self.query_failed_cleanup(core, fingerprint);
        }
    }

    fn population_failed(
        &self,
        core: &mut WriterCore,
        fingerprint: Fingerprint,
        generation: Generation,
    ) {
        core.population_deleted_keys
            .deactivate(fingerprint, generation);
        // Whatever the worker staged before failing is emptied in chunks, not
        // with one DELETE on the writer (PGC-293 keeps the tables checked out
        // until that check-in).
        core.population_discard_enqueue(fingerprint, generation);
        self.query_failed_cleanup(core, fingerprint);
    }

    async fn limit_bump_command(
        &mut self,
        core: &mut WriterCore,
        fingerprint: Fingerprint,
        max_limit: Option<u64>,
    ) {
        trace!("command limit bump {fingerprint} max_limit={max_limit:?}");
        if let Err(e) = self.limit_bump_handle(core, fingerprint, max_limit).await {
            error!(
                "limit bump failed for {fingerprint}: {}",
                error_chain_format(e.current_context()),
            );
            // Forward rollback isn't reliable: by the time population dispatch
            // could fail, the writer has already bumped generation/max_limit
            // and the cache table rows are stamped with the old generation.
            // Tear down so reads aren't served against an unpopulated new
            // generation.
            self.query_failed_cleanup(core, fingerprint);
        }
    }

    async fn readmit_command(&mut self, core: &mut WriterCore, fingerprint: Fingerprint) {
        trace!("command readmit {fingerprint}");
        if let Err(e) = self.query_readmit(core, fingerprint, Instant::now()).await {
            error!(
                "pinned readmit failed for {fingerprint}: {}",
                error_chain_format(e.current_context()),
            );
            self.query_failed_cleanup(core, fingerprint);
        }
    }

    /// Clean up after a failed register/populate/readmit/limit-bump.
    ///
    /// Always clears the dispatch-owned `state_view` entry and drains any
    /// coalesced `waiting` requests via `WriterNotify::Failed` — even when the
    /// fingerprint never made it into `cached_queries` (e.g. the resolver
    /// rejected the query). Without this, a failed Register would leave
    /// `state_view` stuck in `Loading` and every subsequent client request for
    /// that fingerprint would coalesce into `waiting` and hang.
    pub(super) fn query_failed_cleanup(&self, core: &mut WriterCore, fingerprint: Fingerprint) {
        trace!("query_failed_cleanup {fingerprint}");

        // Deleted-key tracking is released per `(fingerprint, generation)` by the
        // population's terminal handler (Merge flush / Failed command), not here —
        // this fingerprint may have a superseded generation still in flight.
        match core.cache.cached_queries.remove1(&fingerprint) {
            Some(query) => {
                core.cache.generations.remove(&query.generation);
                core.cache
                    .update_queries_remove_fingerprint(fingerprint, &query.relation_oids);
                core.active_relations_release(&query.relation_oids);
                debug!("cleaned up failed query {fingerprint}");
            }
            None => {
                // No cached_query but `update_queries_register` may have run
                // before the failure — sweep orphan entries by fingerprint.
                for mut entry in core.cache.update_queries.iter_mut() {
                    entry.query_remove(fingerprint);
                    entry.subsumption.remove(fingerprint);
                    entry.eval_index.remove(fingerprint);
                }
            }
        }

        core.state_view.cached_queries.remove(&fingerprint);
        core.waiters_fail(fingerprint);
    }
}

async fn mv_build_command(core: &mut WriterCore, fingerprint: Fingerprint) {
    trace!("command mv build {fingerprint}");
    core.mv_build_dispatch(fingerprint);
    // Fault injection (evict-mid-build): evict the entry right after its build
    // task launched, exercising the deferred re-dispatch + stale-completion
    // discard path deterministically.
    if fault_mv_evict_on_build(core, fingerprint) {
        error!("fault injection: evicting {fingerprint} mid-build");
        if let Err(e) = core.cache_query_evict(fingerprint).await {
            error!(
                "fault eviction failed for {fingerprint}: {}",
                error_chain_format(e.current_context()),
            );
        }
    }
}
