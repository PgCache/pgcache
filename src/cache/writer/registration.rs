use crate::oid::Oid;
use crate::pg::Lsn;
use crate::query::{Fingerprint, QueryShape, query_shape_derive};
use std::cmp::Reverse;
use std::num::NonZeroU64;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Instant;

use ecow::EcoString;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tokio::task::spawn_local;
use tokio_postgres::Client;
use tracing::{debug, error, instrument, trace};

use crate::cache::coalesce_queue::fetch_stage_ewma_update;
use crate::catalog::{TableMetadata, aggregate_functions_load};
use crate::query::ast::{AstNode, Deparse, QueryExpr, TableNode};
use crate::query::decorrelate::query_expr_decorrelate;
use crate::query::resolved::{
    ResolvedQueryExpr, ResolvedSelectNode, ResolvedTableNode, enum_order_dependence_check,
    query_expr_resolve,
};
use crate::query::transform::predicate_pushdown_apply;
use crate::result::error_chain_format;
use crate::settings::Settings;
use crate::timing::{duration_to_ns_u64, duration_to_us_u64};

use super::super::admission::{
    AdmissionDepth, base_query_prepare, query_admission_analyze, shape_gate_classify,
};
use super::super::{
    CacheError, CacheResult, MapIntoReport, ReportExt,
    messages::{AdmitAction, QueryCommand, SubsumptionResult},
    mv::{ShapeGate, resolved_has_join, resolved_has_window},
    query::CacheableQuery,
    types::{CachedQuery, QueryMetrics, SharedResolved},
    update_query::UpdateQueries,
};
use super::core::{MERGE_FLUSH_FORCE_AFTER, PendingMerge, WriterCore};
use super::population::population_worker;
use super::staging::MergeOutcome;
use crate::pg;

/// Minimum number of persistent population workers.
const MIN_POPULATE_POOL_SIZE: usize = 2;

/// Work item for population worker pool.
pub struct PopulationWork {
    pub fingerprint: Fingerprint,
    pub generation: u64,
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

/// Owns the query registration / population path: consumes `QueryCommand`s
/// and drives resolution, subsumption, population dispatch, and lifecycle
/// transitions against the shared `WriterCore`. Holds the population worker
/// channels and aggregate-function catalog (used for decorrelation).
pub(super) struct WriterRegistration {
    /// Channels to persistent population workers (round-robin dispatch).
    populate_txs: Vec<UnboundedSender<PopulationWork>>,
    /// Index for round-robin dispatch to population workers.
    populate_next: usize,
    /// Aggregate function names from pg_proc, used for scalar subquery decorrelation.
    aggregate_functions: std::collections::HashSet<EcoString>,
}

impl WriterRegistration {
    pub async fn new(
        settings: &Settings,
        db_origin: &Rc<Client>,
        query_tx: UnboundedSender<QueryCommand>,
        registration_throttled: Arc<AtomicBool>,
    ) -> CacheResult<Self> {
        let aggregate_functions = aggregate_functions_load(db_origin)
            .await
            .map_into_report::<CacheError>()
            .attach_loc("loading aggregate functions")?;

        // Spawn persistent population workers (each with its own cache connection)
        let populate_pool_size = settings.num_workers.max(MIN_POPULATE_POOL_SIZE);
        let mut populate_txs = Vec::with_capacity(populate_pool_size);

        for i in 0..populate_pool_size {
            let cache_conn = pg::connect(&settings.cache, &format!("population worker {i}"))
                .await
                .map_into_report::<CacheError>()?;
            // Staging setup runs `DROP TABLE IF EXISTS` defensively before every
            // CREATE; the table almost never exists (names embed the
            // generation), so PG would emit a "does not exist, skipping" NOTICE
            // per population — pure log noise. Suppress notices on this session.
            cache_conn
                .batch_execute("SET client_min_messages = warning")
                .await
                .map_into_report::<CacheError>()?;

            // Each worker reads from origin on its own connection so the origin
            // executes population SELECTs concurrently rather than serializing
            // them on one shared backend.
            let origin_conn = pg::connect(&settings.origin, &format!("population origin {i}"))
                .await
                .map_into_report::<CacheError>()?;

            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            populate_txs.push(tx);

            let worker_origin_settings = settings.origin.clone();
            let worker_query_tx = query_tx.clone();
            let worker_throttled = Arc::clone(&registration_throttled);

            spawn_local(async move {
                population_worker(
                    i,
                    rx,
                    origin_conn,
                    worker_origin_settings,
                    cache_conn,
                    worker_query_tx,
                    worker_throttled,
                )
                .await;
            });
        }

        Ok(Self {
            populate_txs,
            populate_next: 0,
            aggregate_functions,
        })
    }

    /// Handle a query command, dispatching to the appropriate method.
    pub async fn query_command_handle(
        &mut self,
        core: &mut WriterCore,
        cmd: QueryCommand,
    ) -> CacheResult<()> {
        let reg = &crate::metrics::handles().reg;
        let cmd_handle = match &cmd {
            QueryCommand::Register { .. } => &reg.cmd_register,
            QueryCommand::Merge(_) => &reg.cmd_ready,
            QueryCommand::Failed { .. } => &reg.cmd_failed,
            QueryCommand::LimitBump { .. } => &reg.cmd_limit_bump,
            QueryCommand::Readmit { .. } => &reg.cmd_readmit,
            QueryCommand::MvBuild { .. } => &reg.cmd_mv_build,
            QueryCommand::MvBuildComplete { .. } => &reg.cmd_mv_build_complete,
        };
        let handle_start = Instant::now();
        match cmd {
            QueryCommand::Register {
                fingerprint,
                cacheable_query,
                search_path,
                started_at,
                subsumption_tx,
                admit_action,
                pinned,
            } => {
                trace!("command query register {fingerprint}");
                let search_path_refs: Vec<&str> =
                    search_path.iter().map(EcoString::as_str).collect();
                if let Err(e) = self
                    .query_register(
                        core,
                        fingerprint,
                        &cacheable_query,
                        &search_path_refs,
                        started_at,
                        subsumption_tx,
                        admit_action,
                        pinned,
                    )
                    .await
                {
                    // Most registration failures are routing decisions (the
                    // query isn't cacheable and is forwarded to origin), not
                    // faults — log at debug so swallowed-error scanners don't
                    // treat an expected forward as a fault. A table with no
                    // primary key surfaces as `UnknownTable` (PGC-135, the
                    // documented "forwarded silently" path); a query the
                    // resolver can't model (ambiguous self-join columns,
                    // USING/NATURAL qualifiers, etc.) as a `ResolveError`; a
                    // correlated subquery that can't be decorrelated as a
                    // `DecorrelateError`. A forwarded query still returns the
                    // correct result; a wrong cached result is caught by the
                    // result-diff path, and "should have cached" by routing
                    // assertions — neither relies on this error log.
                    let ctx = e.current_context();
                    if matches!(
                        ctx,
                        CacheError::DecorrelateError(_)
                            | CacheError::ResolveError(_)
                            | CacheError::UnknownTable { .. }
                    ) {
                        debug!(
                            "query {fingerprint} forwarded (not cacheable): {}",
                            error_chain_format(ctx),
                        );
                    } else {
                        error!(
                            "query register failed for {fingerprint}: {}",
                            error_chain_format(ctx),
                        );
                    }
                    self.query_failed_cleanup(core, fingerprint);
                }
            }
            QueryCommand::Merge(merge) => {
                // Queue the merge; the writer loop drains it once no CDC frame
                // is open (PGC-250) AND the apply watermark has reached its
                // snapshot LSN (PGC-272). Running it here could be mid-frame,
                // racing the CDC writer's frame txn on the shared cache table.
                core.pending_merges.push(Reverse(PendingMerge(merge)));
            }
            QueryCommand::Failed {
                fingerprint,
                generation,
            } => {
                core.population_deleted_keys
                    .deactivate(fingerprint, generation);
                // Return the population's staging tables to the pool (PGC-293):
                // the worker no longer drops them on failure.
                core.staging_checkin(fingerprint, generation).await;
                self.query_failed_cleanup(core, fingerprint);
            }
            QueryCommand::LimitBump {
                fingerprint,
                max_limit,
            } => {
                trace!("command limit bump {fingerprint} max_limit={max_limit:?}");
                if let Err(e) = self.limit_bump_handle(core, fingerprint, max_limit).await {
                    error!(
                        "limit bump failed for {fingerprint}: {}",
                        error_chain_format(e.current_context()),
                    );
                    // Forward rollback isn't reliable: by the time
                    // `populate_work_dispatch` could fail, the writer has already
                    // bumped generation/max_limit and the cache table rows are
                    // stamped with the old generation. Tear down so reads aren't
                    // served against an unpopulated new generation.
                    self.query_failed_cleanup(core, fingerprint);
                }
            }
            QueryCommand::Readmit { fingerprint } => {
                trace!("command readmit {fingerprint}");
                if let Err(e) = self.query_readmit(core, fingerprint, Instant::now()).await {
                    error!(
                        "pinned readmit failed for {fingerprint}: {}",
                        error_chain_format(e.current_context()),
                    );
                    self.query_failed_cleanup(core, fingerprint);
                }
            }
            QueryCommand::MvBuild { fingerprint } => {
                trace!("command mv build {fingerprint}");
                core.mv_build_dispatch(fingerprint);
                // Fault injection (evict-mid-build): evict the entry right
                // after its build task launched, exercising the deferred
                // re-dispatch + stale-completion discard path deterministically.
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
        cmd_handle.record(handle_start.elapsed().as_secs_f64());
        Ok(())
    }

    /// Build population work for a query, handling decorrelation and branch extraction.
    ///
    /// Decorrelates the resolved AST so correlated subqueries are merged into JOINs,
    /// then extracts SELECT branches, collects table metadata, and builds PopulationWork.
    fn population_work_build(
        &self,
        core: &WriterCore,
        fingerprint: Fingerprint,
        generation: u64,
        resolved: &SharedResolved,
        max_limit: Option<u64>,
    ) -> PopulationWork {
        let population_resolved = query_expr_decorrelate(resolved, &self.aggregate_functions)
            .map(|d| {
                if d.transformed {
                    d.resolved
                } else {
                    ResolvedQueryExpr::clone(resolved)
                }
            })
            .unwrap_or_else(|_| ResolvedQueryExpr::clone(resolved));

        let branches: Vec<ResolvedSelectNode> = population_resolved
            .select_nodes()
            .into_iter()
            .cloned()
            .collect();

        let branch_relation_oids: Vec<Oid> = branches
            .iter()
            .flat_map(|branch: &ResolvedSelectNode| branch.nodes::<ResolvedTableNode>())
            .map(|tn| tn.relation_oid)
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        let table_metadata: Vec<TableMetadata> = branch_relation_oids
            .iter()
            .filter_map(|oid| core.cache.tables.get1(oid).cloned())
            .collect();

        PopulationWork {
            fingerprint,
            generation,
            table_metadata,
            branches,
            max_limit,
            // Filled in at dispatch from the staging pool (needs `&mut core`).
            staging: Vec::new(),
            enqueued_at: Instant::now(),
        }
    }

    /// Resolve schema for a table: use explicit schema if provided, otherwise lookup via search path.
    async fn table_schema_resolve(
        &self,
        core: &WriterCore,
        table_name: &str,
        explicit_schema: Option<&str>,
        search_path: &[&str],
    ) -> CacheResult<String> {
        if let Some(schema) = explicit_schema {
            Ok(schema.to_owned())
        } else {
            core.schema_for_table_find(table_name, search_path).await
        }
    }

    /// Dispatch population work to next worker using round-robin scheduling.
    fn populate_work_dispatch(
        &mut self,
        core: &mut WriterCore,
        mut work: PopulationWork,
    ) -> CacheResult<()> {
        let fingerprint = work.fingerprint;
        let generation = work.generation;
        // Begin recording CDC deletes for this population's relations *before*
        // the worker reads its snapshot (PGC-250). Released at merge or on
        // failure.
        let relation_oids: Vec<Oid> = work.table_metadata.iter().map(|t| t.relation_oid).collect();
        // Anchor floor: a lower bound on this population's snapshot LSN, used to
        // prune deleted keys it can no longer need (PGC-250).
        let anchor_floor = core.last_applied_lsn;
        core.population_deleted_keys.activate(
            fingerprint,
            generation,
            &relation_oids,
            anchor_floor,
        );
        // Check out a reusable staging table per relation (PGC-293); the writer
        // returns them to the pool at merge / failure.
        work.staging = core
            .staging_pool
            .checkout(fingerprint, generation, &relation_oids);

        let idx = self.populate_next;
        self.populate_next = (self.populate_next + 1) % self.populate_txs.len();

        let Some(tx) = self.populate_txs.get(idx) else {
            core.population_deleted_keys
                .deactivate(fingerprint, generation);
            core.staging_pool.forget(fingerprint, generation);
            return Err(CacheError::Other.into());
        };

        if tx.send(work).is_err() {
            error!("population worker {idx} channel closed");
            core.population_deleted_keys
                .deactivate(fingerprint, generation);
            core.staging_pool.forget(fingerprint, generation);
        }

        Ok(())
    }

    /// Ensure all tables referenced in the query exist in the cache.
    /// Resolves schemas and creates cache tables as needed.
    async fn cache_tables_ensure(
        &self,
        core: &mut WriterCore,
        base_query: &QueryExpr,
        search_path: &[&str],
    ) -> CacheResult<()> {
        for table_node in base_query.nodes::<TableNode>() {
            let table_name = table_node.name.as_str();
            let schema = self
                .table_schema_resolve(core, table_name, table_node.schema.as_deref(), search_path)
                .await?;

            if !core
                .cache
                .tables
                .contains_key2(&(schema.as_str(), table_name))
            {
                let table = core.cache_table_create(Some(&schema), table_name).await?;
                core.cache.tables.insert_overwrite(table);
            }
        }
        Ok(())
    }

    /// Run the pure admission analysis (decorrelation, per-table update
    /// queries, subsumer eligibility) and store the results: the update-query
    /// map plus the constraint indexes for sub-linear subsumption candidate
    /// lookup. Returns the relation OIDs that have update queries registered.
    fn update_queries_register(
        &self,
        core: &mut WriterCore,
        fingerprint: Fingerprint,
        resolved: &SharedResolved,
        has_limit: bool,
    ) -> CacheResult<Vec<Oid>> {
        let analysis = query_admission_analyze(
            resolved,
            fingerprint,
            has_limit,
            &self.aggregate_functions,
            &core.cache.tables,
            AdmissionDepth::Full,
        )?;

        let mut relation_oids = Vec::new();
        for admission in analysis.tables {
            let mut queries = core
                .cache
                .update_queries
                .entry(admission.relation_oid)
                .or_insert_with(|| UpdateQueries::new(admission.relation_oid));

            let relation_oid = admission.relation_oid;
            queries.query_insert(admission.update_query);

            if admission.subsumer_eligible {
                queries
                    .subsumption
                    .insert(fingerprint, &admission.index_constraints);
            }
            queries
                .eval_index
                .insert(fingerprint, &admission.index_constraints);
            relation_oids.push(relation_oid);
        }
        Ok(relation_oids)
    }

    /// Assign a generation number and insert the CachedQuery entry.
    /// Returns `(generation, relations_changed)`.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn cached_query_insert(
        &self,
        core: &mut WriterCore,
        fingerprint: Fingerprint,
        relation_oids: Vec<Oid>,
        base_query: QueryExpr,
        resolved: SharedResolved,
        deparsed_sql: EcoString,
        serve_shape: QueryShape,
        max_limit: Option<u64>,
        started_at: Instant,
        pinned: bool,
    ) -> (u64, bool) {
        core.cache.generation_counter += 1;
        let generation = core.cache.generation_counter;
        core.cache.generations.insert(generation);

        // Increment per-relation refcounts. `changed` is true if any oid
        // transitioned 0→1 (new active relation) — caller uses this to
        // decide whether the publication needs syncing inline.
        let changed = core.active_relations_acquire(&relation_oids);

        let cached_query = CachedQuery {
            fingerprint,
            generation,
            relation_oids,
            query: base_query,
            resolved,
            deparsed_sql,
            serve_shape,
            max_limit,
            cached_bytes: 0,
            registration_started_at: Some(started_at),
            invalidated: false,
            pinned,
        };

        core.cache.cached_queries.insert_overwrite(cached_query);
        (generation, changed)
    }

    /// Resolve a query's tables and AST, register update queries, and extract constraints.
    /// This is the first phase of registration, before subsumption or population.
    async fn query_resolve(
        &self,
        core: &mut WriterCore,
        fingerprint: Fingerprint,
        cacheable_query: &CacheableQuery,
        search_path: &[&str],
    ) -> CacheResult<QueryResolution> {
        let (base_query, user_max_limit) = base_query_prepare(cacheable_query.query());

        self.cache_tables_ensure(core, &base_query, search_path)
            .await?;

        let resolved: SharedResolved = Arc::new(
            query_expr_resolve(&base_query, &core.cache.tables, search_path)
                .map_err(|e| e.context_transform(CacheError::from))
                .attach_loc("resolving query expression")
                .map(predicate_pushdown_apply)?,
        );

        enum_order_dependence_check(&resolved)
            .map_err(|e| e.context_transform(CacheError::from))
            .attach_loc("enum order-dependence gate")?;

        // Deparse once at registration. The output is a pure function of the
        // resolved AST, so every cache hit can splice it in instead of
        // re-running the deparse traversal.
        let deparse_start = Instant::now();
        let mut buf = String::with_capacity(256);
        resolved.deparse(&mut buf);
        let deparsed_sql: EcoString = buf.into();
        crate::metrics::handles()
            .reg
            .resolve_deparse
            .record(deparse_start.elapsed().as_secs_f64());

        // Parameterized shape (PGC-294): the per-shape serve statement + binds,
        // derived from the same resolved AST. Additive to the fingerprint.
        let serve_shape = query_shape_derive(&resolved);

        // Classify the shape once here; `query_register` and MV setup both reuse
        // the result via `QueryResolution.shape_gate` to avoid re-running
        // decorrelation + classification.
        let shape_gate = shape_gate_classify(&resolved, &self.aggregate_functions);

        // Reducer shapes transform row cardinality — applying the user's
        // LIMIT to source-row population truncates the input and breaks
        // re-evaluation (e.g. `SELECT count(*) FROM t LIMIT 3` cached with 3
        // source rows returns 3, not the real count). Force unbounded
        // population for those shapes.
        let max_limit = if shape_gate.is_reducer() {
            None
        } else {
            user_max_limit
        };

        // `mv_limit` caps the MV body to a top-N, independent of the population
        // cap. Only joins benefit (other reducers already collapse their input),
        // and never window functions — a windowed MV must store the full result
        // because the window depends on the whole partition.
        let mv_limit = if resolved_has_join(&resolved) && !resolved_has_window(&resolved) {
            user_max_limit
        } else {
            None
        };

        let uq_start = Instant::now();
        let relation_oids =
            self.update_queries_register(core, fingerprint, &resolved, max_limit.is_some())?;
        crate::metrics::handles()
            .reg
            .resolve_update_queries_register
            .record(uq_start.elapsed().as_secs_f64());

        Ok(QueryResolution {
            resolved,
            deparsed_sql,
            serve_shape,
            relation_oids,
            base_query,
            max_limit,
            mv_limit,
            shape_gate,
        })
    }

    /// Registers a query in the cache. Checks subsumption first — if the data
    /// is already cached by a broader query, stamps rows and marks Ready immediately.
    /// Otherwise, dispatches population (if `admit_action` is `Admit`).
    ///
    /// If the query was previously invalidated (CLOCK policy), takes the fast
    /// readmission path that reuses existing metadata.
    #[instrument(skip_all)]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    #[allow(clippy::too_many_arguments)]
    pub async fn query_register(
        &mut self,
        core: &mut WriterCore,
        fingerprint: Fingerprint,
        cacheable_query: &CacheableQuery,
        search_path: &[&str],
        started_at: Instant,
        subsumption_tx: oneshot::Sender<SubsumptionResult>,
        admit_action: AdmitAction,
        pinned: bool,
    ) -> CacheResult<()> {
        // Fast readmit path for invalidated queries — skip subsumption
        if let Some(query) = core.cache.cached_queries.get1(&fingerprint)
            && query.invalidated
        {
            let _ = subsumption_tx.send(SubsumptionResult::NotSubsumed);
            return self.query_readmit(core, fingerprint, started_at).await;
        }

        // Phase 1: Resolve
        let resolve_start = Instant::now();
        let resolution = self
            .query_resolve(core, fingerprint, cacheable_query, search_path)
            .await?;
        crate::metrics::handles()
            .reg
            .register_resolve
            .record(resolve_start.elapsed().as_secs_f64());

        // Classify shape for MV eligibility. Sticky — readmit/limit-bump
        // preserve the result through state_view_write. Classification was
        // done in `query_resolve`; reuse here.
        core.mv_state_set(fingerprint, resolution.shape_gate, resolution.mv_limit);

        // Phase 2: Subsumption check
        let subsumption_start = Instant::now();
        let subsumed = self.subsumption_check(core, &resolution);
        crate::metrics::handles()
            .reg
            .register_subsumption_check
            .record(subsumption_start.elapsed().as_secs_f64());

        if subsumed {
            // Phase 3a: Subsume — stamp rows, mark Ready
            let fallback_resolved = Arc::clone(&resolution.resolved);
            let fallback_max_limit = resolution.max_limit;

            let subsume_start = Instant::now();
            let subsume_result = self
                .query_subsume(core, fingerprint, resolution, started_at, pinned)
                .await?;
            crate::metrics::handles()
                .reg
                .register_subsume
                .record(subsume_start.elapsed().as_secs_f64());
            match subsume_result {
                Some((generation, resolved, deparsed_sql)) => {
                    let _ = subsumption_tx.send(SubsumptionResult::Subsumed {
                        generation,
                        resolved,
                        deparsed_sql,
                    });
                    return Ok(());
                }
                None => {
                    // Cache DB execution failed — fall back to population.
                    // The query was already inserted by query_subsume, so we need
                    // to clean it up and re-insert properly, or just populate.
                    // Since cached_query_insert was already called, just dispatch population.
                    let _ = subsumption_tx.send(SubsumptionResult::NotSubsumed);
                    let generation = core
                        .cache
                        .cached_queries
                        .get1(&fingerprint)
                        .map(|q| q.generation)
                        .unwrap_or(0);
                    if generation > 0 {
                        let work = self.population_work_build(
                            core,
                            fingerprint,
                            generation,
                            &fallback_resolved,
                            fallback_max_limit,
                        );
                        self.populate_work_dispatch(core, work)?;
                        trace!("subsumption fallback: population queued {fingerprint}");
                    }
                    return Ok(());
                }
            }
        }

        // Phase 3b: Not subsumed
        let _ = subsumption_tx.send(SubsumptionResult::NotSubsumed);

        if admit_action == AdmitAction::CheckOnly {
            // Pending below threshold — don't register, don't populate.
            // Clean up the update_queries we registered in query_resolve.
            core.cache
                .update_queries_remove_fingerprint(fingerprint, &resolution.relation_oids);
            return Ok(());
        }

        // Register and populate
        let insert_start = Instant::now();
        let (generation, relations_changed) = self.cached_query_insert(
            core,
            fingerprint,
            resolution.relation_oids,
            resolution.base_query,
            Arc::clone(&resolution.resolved),
            resolution.deparsed_sql,
            resolution.serve_shape,
            resolution.max_limit,
            started_at,
            pinned,
        );
        let now = NonZeroU64::new(duration_to_ns_u64(core.state_view.started_at.elapsed()));
        core.state_view
            .metrics
            .entry(fingerprint)
            .or_insert_with(|| QueryMetrics::new(now));
        crate::metrics::handles()
            .reg
            .register_insert
            .record(insert_start.elapsed().as_secs_f64());

        if relations_changed {
            let pub_start = Instant::now();
            core.publication_update().await?;
            crate::metrics::handles()
                .reg
                .register_publication_update
                .record(pub_start.elapsed().as_secs_f64());
        }

        let dispatch_start = Instant::now();
        let work = self.population_work_build(
            core,
            fingerprint,
            generation,
            &resolution.resolved,
            resolution.max_limit,
        );
        self.populate_work_dispatch(core, work)?;
        crate::metrics::handles()
            .reg
            .register_populate_dispatch
            .record(dispatch_start.elapsed().as_secs_f64());
        trace!("population work queued for query {fingerprint}");
        Ok(())
    }

    /// Fast readmission for a CDC-invalidated query.
    /// Reuses existing metadata (relation_oids, resolved, update_queries) and
    /// dispatches population work without re-resolving tables.
    pub(super) async fn query_readmit(
        &mut self,
        core: &mut WriterCore,
        fingerprint: Fingerprint,
        started_at: Instant,
    ) -> CacheResult<()> {
        debug!("readmitting query {fingerprint}");
        crate::metrics::handles().state.readmissions.increment(1);
        if let Some(mut m) = core.state_view.metrics.get_mut(&fingerprint) {
            m.readmission_count += 1;
        }

        // Assign new generation
        core.cache.generation_counter += 1;
        let new_generation = core.cache.generation_counter;
        core.cache.generations.insert(new_generation);

        // Extract data before remove/reinsert (generation is key2)
        let Some(mut cached) = core.cache.cached_queries.remove1(&fingerprint) else {
            return Ok(());
        };

        let resolved = Arc::clone(&cached.resolved);
        let deparsed_sql = cached.deparsed_sql.clone();
        let max_limit = cached.max_limit;

        cached.generation = new_generation;
        cached.invalidated = false;
        cached.cached_bytes = 0;
        cached.registration_started_at = Some(started_at);
        // Refcount unchanged — readmit reuses the existing relation_oids set.
        core.cache.cached_queries.insert_overwrite(cached);

        core.state_loading_transition(
            fingerprint,
            new_generation,
            &resolved,
            &deparsed_sql,
            max_limit,
        );

        let work =
            self.population_work_build(core, fingerprint, new_generation, &resolved, max_limit);
        self.populate_work_dispatch(core, work)?;
        trace!("readmission population queued for query {fingerprint}");
        Ok(())
    }

    /// Mark a query as ready after successful population.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn query_ready_mark(
        &self,
        core: &mut WriterCore,
        fingerprint: Fingerprint,
        cached_bytes: usize,
        row_count: u64,
        fetch_stage_ms: f64,
    ) {
        trace!("query_ready_mark {fingerprint}");
        let update_info = if let Some(mut query) = core.cache.cached_queries.get1_mut(&fingerprint)
        {
            query.cached_bytes = cached_bytes;
            let started_at = query.registration_started_at.take();
            Some((
                query.generation,
                Arc::clone(&query.resolved),
                query.deparsed_sql.clone(),
                query.max_limit,
                started_at,
            ))
        } else {
            None
        };

        if let Some((generation, resolved, deparsed_sql, max_limit, started_at)) = update_info {
            // Record registration latency metric
            let population_duration_us = started_at.map(|s| {
                let latency = s.elapsed();
                crate::metrics::handles()
                    .reg
                    .registration_latency
                    .record(latency.as_secs_f64());
                duration_to_us_u64(latency)
            });

            // Record per-query population metrics
            if let Some(mut m) = core.state_view.metrics.get_mut(&fingerprint) {
                m.population_count += 1;
                m.population_row_count = row_count;
                m.cached_since_ns =
                    NonZeroU64::new(duration_to_ns_u64(core.state_view.started_at.elapsed()));
                m.last_population_duration_us = population_duration_us.and_then(NonZeroU64::new);
                m.population_fetch_stage_ewma_ms = Some(fetch_stage_ewma_update(
                    m.population_fetch_stage_ewma_ms,
                    fetch_stage_ms,
                ));
            }

            core.state_ready_transition(fingerprint, generation, resolved, deparsed_sql, max_limit);

            // One unit of drained registration work, for the adaptive-gate
            // drain-rate (capacity) estimate (PGC-277).
            core.state_view.reg_gate.completed_inc();

            trace!(
                "cached query ready, cached_bytes={cached_bytes} rows={row_count} {fingerprint}"
            );
        }
    }

    /// Drain queued population merges in watermark-deadline order (PGC-250,
    /// PGC-272). Called from the writer loop only when no CDC frame is open,
    /// so a merge never races the CDC frame txn on the shared cache table.
    ///
    /// Each merge is additionally gated on the apply watermark reaching its
    /// `snapshot_lsn`: snapshot-state rows must not enter the shared table
    /// before CDC has applied past the snapshot, or already-Ready bystander
    /// queries over the relation would serve a torn mix of two origin points
    /// in time (PGC-272). The heap is a min-heap on that deadline, so one
    /// peek decides whether anything is releasable.
    ///
    /// A successful merge marks the query Ready inline: the watermark is
    /// already at/past the snapshot when the gate releases, so the old
    /// deferred-Ready parking (PGC-250 Slice B) would be a no-op double-gate.
    pub(super) async fn pending_merges_drain(
        &self,
        core: &mut WriterCore,
        applied_lsn: Lsn,
    ) -> CacheResult<()> {
        let mut merged_any = false;
        // Copy the head's fields out in the condition so the `peek` borrow ends
        // before the body re-borrows `core` mutably (pop / merge / staging).
        while let Some((fingerprint, generation, snapshot_lsn)) = core
            .pending_merges
            .peek()
            .map(|Reverse(top)| (top.0.fingerprint, top.0.generation, top.0.snapshot_lsn))
        {
            // Tombstone check before the deadline check: a superseded /
            // invalidated / evicted entry is droppable regardless of the
            // watermark — release its tracking and staging now rather than
            // holding them until a deadline that no longer matters. (Stale
            // entries buried below a live top are reaped lazily when they
            // surface.) The successor population has its own entry.
            if !core.population_is_current(fingerprint, generation) {
                let Some(Reverse(PendingMerge(_))) = core.pending_merges.pop() else {
                    break;
                };
                core.population_deleted_keys
                    .deactivate(fingerprint, generation);
                core.staging_checkin(fingerprint, generation).await;
                continue;
            }

            // Earliest live deadline not reached: nothing below it can be
            // releasable either.
            if snapshot_lsn > applied_lsn {
                break;
            }

            let Some(Reverse(PendingMerge(merge))) = core.pending_merges.pop() else {
                break;
            };
            let outcome = core.population_merge_apply(&merge).await;
            // Return the population's staging tables to the pool (PGC-293)
            // regardless of outcome, before any `?` below could short-circuit
            // the drain and leak them.
            core.staging_checkin(merge.fingerprint, merge.generation)
                .await;
            match outcome {
                Ok(MergeOutcome::Merged) => {
                    merged_any = true;
                    let mh = crate::metrics::handles();
                    mh.reg
                        .merge_wait
                        .record(merge.enqueued_at.elapsed().as_secs_f64());
                    mh.reg.merges_applied.increment(1);
                    core.population_deleted_keys
                        .deactivate(fingerprint, generation);
                    self.query_ready_finalize(
                        core,
                        fingerprint,
                        merge.cached_bytes,
                        merge.row_count,
                        merge.fetch_stage_ms,
                    )
                    .await?;
                }
                Ok(MergeOutcome::Aborted) => {
                    debug!("population merge aborted (overflow / truncate) {fingerprint}");
                    core.population_deleted_keys
                        .deactivate(fingerprint, generation);
                    self.query_failed_cleanup(core, fingerprint);
                }
                Err(e) => {
                    error!(
                        "population merge failed for {fingerprint}: {}",
                        error_chain_format(e.current_context()),
                    );
                    core.population_deleted_keys
                        .deactivate(fingerprint, generation);
                    self.query_failed_cleanup(core, fingerprint);
                }
            }
        }
        // A released merge means the watermark is advancing on its own; restart
        // the stall clock so the grace window times the *current* gated head.
        if merged_any {
            core.merge_stall_since = None;
        }

        // Peek the gated head and drop the borrow before mutating `core`.
        let gated_snapshot_lsn = core
            .pending_merges
            .peek()
            .map(|Reverse(top)| top.0.snapshot_lsn);
        match gated_snapshot_lsn {
            // Top still gated (the loop broke at the deadline check). Nudge for
            // an immediate keepalive, and once it has been stuck past the grace
            // window, force an origin WAL flush so its snapshot LSN becomes
            // reachable (PGC-290). `last_flush_marker_lsn` suppresses re-emits
            // once a marker already covers the gated backlog.
            Some(snapshot_lsn) => {
                core.watermark_nudge.notify_one();
                let stalled_since = *core.merge_stall_since.get_or_insert_with(Instant::now);
                if stalled_since.elapsed() >= MERGE_FLUSH_FORCE_AFTER
                    && snapshot_lsn > core.last_flush_marker_lsn
                {
                    core.last_flush_marker_lsn = core.origin_flush_force().await?;
                }
            }
            None => core.merge_stall_since = None,
        }
        Ok(())
    }

    /// Finalize a population: mark the query Ready and bootstrap any pinned MV.
    /// Shared by the immediate and deferred (`pending_ready`) paths.
    ///
    /// Eviction is NOT done here: it runs on the 1s writer tick (`eviction_run`,
    /// statvfs-driven). A per-Ready `SELECT pgcache_total_size()` round-trip
    /// (O(#cache tables)) serialized the single-threaded writer and backed up the
    /// population-merge queue under high-cardinality registration (PGC-276).
    /// Deferring to the tick keeps Ready handling off the cache DB; eviction is
    /// best-effort and the reserve headroom absorbs up to one tick of growth.
    async fn query_ready_finalize(
        &self,
        core: &mut WriterCore,
        fingerprint: Fingerprint,
        cached_bytes: usize,
        row_count: u64,
        fetch_stage_ms: f64,
    ) -> CacheResult<()> {
        self.query_ready_mark(core, fingerprint, cached_bytes, row_count, fetch_stage_ms);
        core.mv_pinned_bootstrap(fingerprint);
        Ok(())
    }

    /// Clean up after a failed register/populate/readmit/limit-bump.
    ///
    /// Always clears the dispatch-owned `state_view` entry and drains any
    /// coalesced `waiting` requests via `WriterNotify::Failed` — even when the
    /// fingerprint never made it into `cached_queries` (e.g. the resolver
    /// rejected the query). Without this, a failed Register would leave
    /// `state_view` stuck in `Loading` and every subsequent client request for
    /// that fingerprint would coalesce into `waiting` and hang.
    pub fn query_failed_cleanup(&self, core: &mut WriterCore, fingerprint: Fingerprint) {
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

    /// Handle a limit bump: re-populate with a higher limit.
    ///
    /// Bumps the generation number, updates max_limit, and re-populates.
    /// During re-population the query state goes to Loading.
    #[instrument(skip_all)]
    pub async fn limit_bump_handle(
        &mut self,
        core: &mut WriterCore,
        fingerprint: Fingerprint,
        new_max_limit: Option<u64>,
    ) -> CacheResult<()> {
        let Some(cached_query) = core.cache.cached_queries.get1(&fingerprint) else {
            // Gone before the bump ran. No drain needed here: the only path that
            // removes a fingerprint while it could hold parked waiters
            // (cache_query_cdc_invalidate / cache_query_evict) already drained
            // them via waiters_fail before removal. Relied-upon invariant — keep
            // it true if new removal paths are added.
            trace!("limit bump: query {fingerprint} not found, skipping");
            return Ok(());
        };

        // A larger max_limit means the existing MV (sized for the old max_limit)
        // is short of rows. Flip Fresh → Dirty before any other mutation so
        // dispatches fall through while the new population runs.
        core.mv_dirty_mark(fingerprint);

        // Collect data needed before mutating
        let resolved = Arc::clone(&cached_query.resolved);
        let deparsed_sql = cached_query.deparsed_sql.clone();
        let relation_oids = cached_query.relation_oids.clone();
        let old_generation = cached_query.generation;

        // Bump generation
        core.cache.generation_counter += 1;
        let new_generation = core.cache.generation_counter;
        core.cache.generations.insert(new_generation);
        core.cache.generations.remove(&old_generation);

        // Update cached query — must remove and reinsert because generation is key2
        if let Some(mut cached) = core.cache.cached_queries.remove1(&fingerprint) {
            cached.generation = new_generation;
            cached.max_limit = new_max_limit;
            cached.registration_started_at = Some(Instant::now());
            core.cache.cached_queries.insert_overwrite(cached);
        }

        // Update has_limit on update queries. Limited queries are ineligible
        // parents for subsumption, so the index entry is dropped when the bit
        // goes false → true and (re-)added when it goes true → false.
        let has_limit = new_max_limit.is_some();
        for oid in &relation_oids {
            let table_name = core.cache.tables.get1(oid).map(|t| t.name.clone());
            if let Some(mut queries) = core.cache.update_queries.get_mut(oid) {
                if let Some(uq) = queries.queries.get_mut(&fingerprint) {
                    uq.has_limit = has_limit;
                }
                if has_limit {
                    queries.subsumption.remove(fingerprint);
                } else if let Some(name) = table_name
                    && let Some(uq) = queries.queries.get(&fingerprint)
                    && uq.constraints.where_analysis_complete
                {
                    let tcs = uq
                        .constraints
                        .table_constraints
                        .get(name.as_str())
                        .cloned()
                        .unwrap_or_default();
                    queries.subsumption.insert(fingerprint, &tcs);
                }
            }
        }

        core.state_loading_transition(
            fingerprint,
            new_generation,
            &resolved,
            &deparsed_sql,
            new_max_limit,
        );

        let work =
            self.population_work_build(core, fingerprint, new_generation, &resolved, new_max_limit);
        self.populate_work_dispatch(core, work)?;
        trace!("limit bump population queued for query {fingerprint}");
        Ok(())
    }
}
