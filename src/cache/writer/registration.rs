use std::collections::HashSet;
use std::num::NonZeroU64;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use ecow::EcoString;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tokio::task::spawn_local;
use tokio_postgres::Client;
use tracing::{debug, error, info, instrument, trace};

use crate::cache::query::limit_rows_needed;
use crate::catalog::{TableMetadata, aggregate_functions_load};
use crate::query::ast::{Deparse, QueryBody, QueryExpr, TableNode};
use crate::query::constraints::{
    TableConstraint, analyze_query_constraints, table_constraints_subsumed,
};
use crate::query::decorrelate::{DecorrelateError, query_expr_decorrelate};
use crate::query::evaluate::resolved_where_expr_supported;
use crate::query::resolved::{
    ResolvedColumnNode, ResolvedQueryExpr, ResolvedScalarExpr, ResolvedSelectColumns,
    ResolvedSelectNode, ResolvedTableNode, query_expr_resolve,
};
use crate::query::transform::predicate_pushdown_apply;
use crate::query::update::query_table_update_queries;
use crate::result::error_chain_format;
use crate::settings::Settings;
use crate::timing::{duration_to_ns_u64, duration_to_us_u64};

use super::super::{
    CacheError, CacheResult, MapIntoReport, ReportExt,
    messages::{AdmitAction, QueryCommand, SubsumptionResult, WriterNotify},
    mv::{ShapeGate, resolved_has_join, shape_classify},
    query::CacheableQuery,
    types::{
        CachedQuery, QueryMetrics, SharedResolved, UpdateEvalStrategy, UpdateQueries, UpdateQuery,
        UpdateQuerySource,
    },
};
use super::core::WriterCore;
use super::population::population_worker;
use crate::pg;

/// Minimum number of persistent population workers.
const MIN_POPULATE_POOL_SIZE: usize = 2;

/// Work item for population worker pool.
pub struct PopulationWork {
    pub fingerprint: u64,
    pub generation: u64,
    pub table_metadata: Vec<TableMetadata>,
    /// SELECT branches extracted from the query at registration time.
    /// For simple SELECT queries, this contains one branch.
    /// For set operations (UNION/INTERSECT/EXCEPT), contains all branches.
    pub branches: Vec<ResolvedSelectNode>,
    /// Maximum rows to fetch during population. `None` = fetch all rows.
    pub max_limit: Option<u64>,
    /// Stamped at construction; used by the population worker to record
    /// `pgcache.cache.population.wait_seconds`.
    pub enqueued_at: Instant,
}

/// Decide whether CDC can evaluate this update query's WHERE in Rust.
///
/// Conservative classifier: rejects anything the Rust evaluator can't decide
/// from a single CDC row. GROUP BY / HAVING are rejected because row-level
/// matching doesn't capture post-aggregation filtering. Non-FromClause sources
/// are rejected because their CDC semantics (subquery membership, outer join
/// null-padding cascade) aren't expressible as a row-level predicate.
fn update_eval_strategy_classify(
    resolved: &ResolvedQueryExpr,
    source: UpdateQuerySource,
) -> UpdateEvalStrategy {
    if source != UpdateQuerySource::FromClause {
        return UpdateEvalStrategy::PgEval;
    }
    let Some(select) = resolved.as_select() else {
        return UpdateEvalStrategy::PgEval;
    };
    if !select.is_single_table() {
        return UpdateEvalStrategy::PgEval;
    }
    if !select.group_by.is_empty() || select.having.is_some() {
        return UpdateEvalStrategy::PgEval;
    }
    let Some(where_expr) = &select.where_clause else {
        return UpdateEvalStrategy::LocalEval;
    };
    if resolved_where_expr_supported(where_expr) {
        UpdateEvalStrategy::LocalEval
    } else {
        UpdateEvalStrategy::PgEval
    }
}

/// Collect column names on `table_name` that participate in the parent
/// query's LIMIT-window definition: top-level ORDER BY, WHERE, and HAVING.
///
/// PGC-94: used by row_cached_invalidation_check to decide whether a CDC
/// UPDATE on a cached row may shift the window such that an untracked
/// row needs to fill the gap. Returns an empty set when the query has
/// no window-defining references on this table.
///
/// Aliased ORDER BY (`ORDER BY count(*) DESC LIMIT 10`) carries no table
/// reference and naturally produces no entries here. Those shapes are
/// Measure queries that already use PgEval/MV invalidation paths.
fn limit_window_columns_collect(
    resolved: &ResolvedQueryExpr,
    table_name: &str,
) -> HashSet<EcoString> {
    let mut cols = HashSet::new();
    let mut push_if_local = |col: &ResolvedColumnNode| {
        if col.table.as_str() == table_name {
            cols.insert(col.column.clone());
        }
    };

    let select = resolved.as_select();

    for clause in &resolved.order_by {
        for col in clause.expr.nodes::<ResolvedColumnNode>() {
            push_if_local(col);
        }
        // Aliased `ORDER BY value` resolves to `Identifier`; chase it through
        // the SELECT-list to recover the underlying base-table column refs.
        if let ResolvedScalarExpr::Identifier(name) = &clause.expr
            && let Some(select) = select
            && let ResolvedSelectColumns::Columns(select_cols) = &select.columns
        {
            for select_col in select_cols {
                if select_col.output_name() == Some(name) {
                    for col in select_col.expr.nodes::<ResolvedColumnNode>() {
                        push_if_local(col);
                    }
                }
            }
        }
    }

    if let Some(select) = select {
        if let Some(where_expr) = &select.where_clause {
            for col in where_expr.nodes::<ResolvedColumnNode>() {
                push_if_local(col);
            }
        }
        if let Some(having) = &select.having {
            for col in having.nodes::<ResolvedColumnNode>() {
                push_if_local(col);
            }
        }
    }

    cols
}

/// Intermediate result from resolving a query before subsumption check or population.
struct QueryResolution {
    resolved: SharedResolved,
    /// Deparsed SQL body of `resolved`. Computed once here and reused on the
    /// serving hot path; see `CachedQuery.deparsed_sql`.
    deparsed_sql: EcoString,
    relation_oids: Vec<u32>,
    base_query: QueryExpr,
    max_limit: Option<u64>,
    /// MV cap, separate from `max_limit`. Set for join shapes only (the
    /// MV body applies the user's LIMIT over the source-row cache);
    /// `None` for other reducers, whose results are already collapsed.
    mv_limit: Option<u64>,
    /// MV shape gate. Also gates `max_limit`: reducer shapes force
    /// `max_limit = None` so source-row population isn't truncated in a way
    /// that would break re-evaluation (aggregates, GROUP BY, DISTINCT,
    /// windows all depend on the full input row set to produce correct
    /// result rows).
    shape_gate: ShapeGate,
}

/// Clone the query, strip LIMIT, and compute max_limit for population.
/// Set operations force max_limit = None since population runs per-branch.
fn base_query_prepare(query: &QueryExpr) -> (QueryExpr, Option<u64>) {
    let is_set_op = matches!(query.body, QueryBody::SetOp(_));
    let max_limit = if is_set_op {
        None
    } else {
        limit_rows_needed(&query.limit)
    };
    let mut base_query = query.clone();
    base_query.limit = None;
    (base_query, max_limit)
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

            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            populate_txs.push(tx);

            let worker_db_origin = Rc::clone(db_origin);
            let worker_query_tx = query_tx.clone();

            spawn_local(async move {
                population_worker(i, rx, worker_db_origin, cache_conn, worker_query_tx).await;
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
            QueryCommand::Ready { .. } => &reg.cmd_ready,
            QueryCommand::Failed { .. } => &reg.cmd_failed,
            QueryCommand::LimitBump { .. } => &reg.cmd_limit_bump,
            QueryCommand::Readmit { .. } => &reg.cmd_readmit,
            QueryCommand::MvBuild { .. } => &reg.cmd_mv_build,
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
                    // Non-decorrelatable subqueries are a routing decision
                    // (forward to origin), not a fault — log at debug.
                    let ctx = e.current_context();
                    if matches!(
                        ctx,
                        CacheError::DecorrelateError(DecorrelateError::NonDecorrelatable { .. })
                    ) {
                        debug!(
                            "query {fingerprint} forwarded (not decorrelatable): {}",
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
            QueryCommand::Ready {
                fingerprint,
                cached_bytes,
                row_count,
            } => {
                self.query_ready_mark(core, fingerprint, cached_bytes, row_count);
                core.mv_pinned_bootstrap(fingerprint);
                core.cache.current_size = core.cache_size_load().await?;
                core.eviction_run().await?;
            }
            QueryCommand::Failed { fingerprint } => {
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
                if let Err(e) = core.mv_build(fingerprint).await {
                    error!(
                        "mv build failed for {fingerprint}: {}",
                        error_chain_format(e.current_context()),
                    );
                    // The cache itself is intact and serving Ready; only the MV
                    // build path failed. Revert to Pending so the next hit retries
                    // (matches mv_build's own SQL-error fallback at writer/mv.rs).
                    core.mv_build_failed_reset(fingerprint);
                }
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

    /// Classify a resolved query's shape for MV eligibility. Runs decorrelation
    /// first so the classification matches what first-population / rebuild will
    /// actually see (correlated subqueries get rewritten to JOIN + DISTINCT,
    /// which affects classification). Falls back to the original resolved form
    /// if decorrelation fails.
    ///
    /// NOTE: `population_work_build` and `update_queries_register` also decorrelate
    /// the same resolved query. Factoring these three callers onto a single
    /// decorrelation pass is a worthwhile follow-up but out of scope for v1.
    fn shape_gate_classify(&self, resolved: &SharedResolved) -> ShapeGate {
        let decorrelated = query_expr_decorrelate(resolved, &self.aggregate_functions).ok();
        let query: &ResolvedQueryExpr = match &decorrelated {
            Some(d) if d.transformed => &d.resolved,
            _ => resolved,
        };
        shape_classify(query, &self.aggregate_functions)
    }

    /// Build population work for a query, handling decorrelation and branch extraction.
    ///
    /// Decorrelates the resolved AST so correlated subqueries are merged into JOINs,
    /// then extracts SELECT branches, collects table metadata, and builds PopulationWork.
    fn population_work_build(
        &self,
        core: &WriterCore,
        fingerprint: u64,
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

        let branch_relation_oids: Vec<u32> = branches
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

    /// Register an update query for a relation. Maintains complexity-sorted
    /// fingerprint order for CDC iteration and indexes the constraints for
    /// sub-linear subsumption candidate lookup.
    fn update_query_register(
        &self,
        core: &mut WriterCore,
        relation_oid: u32,
        table_name: &str,
        mut update_query: UpdateQuery,
    ) {
        let fingerprint = update_query.fingerprint;
        let complexity = update_query.complexity;
        let has_limit = update_query.has_limit;

        // Whether a CDC UPDATE for this query can ever invalidate — i.e. whether
        // `handle_update` must run `query_row_changes` + the invalidation check
        // rather than skip them (PGC-227). Derived from the single source of
        // truth so the flag can't drift from the actual checks.
        update_query.change_dependent = update_query.update_invalidation_possible(table_name);
        // Index the per-table constraints for subsumption lookup. Skip
        // entries that are ineligible parents:
        // - has_limit: limited queries are excluded by `subsumption_check`.
        // - !where_analysis_complete: the WHERE clause couldn't be fully
        //   analyzed, so we can't reason about coverage (PGC-106). The
        //   detailed check would reject these anyway; skipping the index
        //   entry saves a per-lookup candidate visit.
        // Queries with empty per-table constraints (full-table scans) are
        // indexed with `&[]` — they're the broadest subsumers and live in
        // the empty `ColumnSet` class.
        let index_eligible = !has_limit && update_query.constraints.where_analysis_complete;
        let table_constraints = index_eligible.then(|| {
            update_query
                .constraints
                .table_constraints
                .get(table_name)
                .cloned()
                .unwrap_or_default()
        });

        let mut queries = core
            .cache
            .update_queries
            .entry(relation_oid)
            .or_insert_with(|| UpdateQueries::new(relation_oid));

        queries.query_insert(update_query);
        // Insert fingerprint into complexity_order at the correct position
        // (ascending by complexity, then by fingerprint for stability).
        let pos = queries
            .complexity_order
            .binary_search_by(|fp| {
                let c = queries
                    .queries
                    .get(fp)
                    .map(|q| q.complexity)
                    .unwrap_or(usize::MAX);
                c.cmp(&complexity).then_with(|| fp.cmp(&fingerprint))
            })
            .unwrap_or_else(|p| p);
        queries.complexity_order.insert(pos, fingerprint);

        if let Some(tcs) = table_constraints {
            queries.subsumption.insert(fingerprint, &tcs);
        }
    }

    /// Dispatch population work to next worker using round-robin scheduling.
    fn populate_work_dispatch(&mut self, work: PopulationWork) -> CacheResult<()> {
        let idx = self.populate_next;
        self.populate_next = (self.populate_next + 1) % self.populate_txs.len();

        let Some(tx) = self.populate_txs.get(idx) else {
            return Err(CacheError::Other.into());
        };

        if tx.send(work).is_err() {
            error!("population worker {idx} channel closed");
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

    /// Decorrelate the resolved AST and register update queries for each table.
    /// Returns the relation OIDs that have update queries registered.
    fn update_queries_register(
        &self,
        core: &mut WriterCore,
        fingerprint: u64,
        resolved: &SharedResolved,
        has_limit: bool,
    ) -> CacheResult<Vec<u32>> {
        let decorrelated = query_expr_decorrelate(resolved, &self.aggregate_functions)
            .map_err(|e| e.context_transform(CacheError::from))
            .attach_loc("decorrelating correlated subqueries")?;
        let update_source = if decorrelated.transformed {
            &decorrelated.resolved
        } else {
            resolved
        };

        let mut relation_oids = Vec::new();
        for (table_node, update_resolved, source) in query_table_update_queries(update_source) {
            let relation_oid = table_node.relation_oid;
            let constraints = update_resolved
                .as_select()
                .map(analyze_query_constraints)
                .unwrap_or_default();
            let complexity = update_resolved.complexity();
            let eval_strategy = update_eval_strategy_classify(&update_resolved, source);
            // Walk the parent `resolved` — `update_resolved` has ORDER BY stripped.
            let limit_window_columns = if has_limit {
                limit_window_columns_collect(resolved, table_node.name.as_str())
            } else {
                HashSet::new()
            };
            let update_query = UpdateQuery {
                fingerprint,
                resolved: update_resolved,
                complexity,
                source,
                constraints,
                has_limit,
                eval_strategy,
                limit_window_columns,
                // Set authoritatively in update_query_register (needs table_name).
                change_dependent: false,
            };

            self.update_query_register(core, relation_oid, table_node.name.as_str(), update_query);
            relation_oids.push(relation_oid);
        }
        Ok(relation_oids)
    }

    /// Assign a generation number and insert the CachedQuery entry.
    /// Returns `(generation, relations_changed)`.
    #[allow(clippy::too_many_arguments)]
    fn cached_query_insert(
        &self,
        core: &mut WriterCore,
        fingerprint: u64,
        relation_oids: Vec<u32>,
        base_query: QueryExpr,
        resolved: SharedResolved,
        deparsed_sql: EcoString,
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
        fingerprint: u64,
        cacheable_query: &CacheableQuery,
        search_path: &[&str],
    ) -> CacheResult<QueryResolution> {
        let (base_query, user_max_limit) = base_query_prepare(&cacheable_query.query);

        self.cache_tables_ensure(core, &base_query, search_path)
            .await?;

        let resolved: SharedResolved = Arc::new(
            query_expr_resolve(&base_query, &core.cache.tables, search_path)
                .map_err(|e| e.context_transform(CacheError::from))
                .attach_loc("resolving query expression")
                .map(predicate_pushdown_apply)?,
        );

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

        // Classify the shape once here; `query_register` and MV setup both reuse
        // the result via `QueryResolution.shape_gate` to avoid re-running
        // decorrelation + classification.
        let shape_gate = self.shape_gate_classify(&resolved);

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

        // `mv_limit` caps the MV body, independent of the population cap.
        // Only joins benefit; other reducers already collapse their input.
        let mv_limit = if matches!(shape_gate, ShapeGate::Measure) && resolved_has_join(&resolved) {
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
            relation_oids,
            base_query,
            max_limit,
            mv_limit,
            shape_gate,
        })
    }

    /// Check whether all tables in the new query are covered by existing cached queries.
    /// Returns true only if every relation_oid has at least one Ready, non-limited
    /// UpdateQuery whose equality constraints are implied by the new query's constraints.
    fn subsumption_check(&self, core: &WriterCore, resolution: &QueryResolution) -> bool {
        if resolution.relation_oids.is_empty() {
            return false;
        }

        // Set operations (UNION/INTERSECT/EXCEPT) require per-branch constraint
        // analysis which isn't implemented yet. Reject unconditionally for now.
        let Some(select) = resolution.resolved.as_select() else {
            return false;
        };

        let new_constraints = analyze_query_constraints(select);

        for &oid in &resolution.relation_oids {
            let Some(update_queries) = core.cache.update_queries.get(&oid) else {
                return false;
            };

            let Some(table_meta) = core.cache.tables.get1(&oid) else {
                return false;
            };
            let table_name = &table_meta.name;

            // Sub-linear candidate lookup via the per-relation subsumption index.
            // Returns parents whose constraint-column set is a subset of new's;
            // we still need to apply parent_ready / single-table / fine-grained
            // constraint checks per candidate, but the candidate set is
            // typically far smaller than `queries.len()`.
            let empty: Vec<TableConstraint> = Vec::new();
            let new_table_constraints = new_constraints
                .table_constraints
                .get(table_name.as_str())
                .unwrap_or(&empty);
            let candidate_fps = update_queries.subsumption.candidates(new_table_constraints);

            let table_covered = candidate_fps.into_iter().any(|fp| {
                let Some(uq) = update_queries.queries.get(&fp) else {
                    return false;
                };
                if uq.has_limit {
                    return false;
                }

                let parent = core.cache.cached_queries.get1(&uq.fingerprint);

                let parent_ready =
                    parent.is_some_and(|q| !q.invalidated && q.registration_started_at.is_none());
                if !parent_ready {
                    return false;
                }

                // Only single-table cached queries are subsumption candidates.
                // Multi-table queries have implicit join filtering that constraint
                // analysis doesn't capture, so we can't safely reason about coverage.
                let parent_single_table = parent.is_some_and(|q| q.relation_oids.len() == 1);
                if !parent_single_table {
                    return false;
                }

                table_constraints_subsumed(&new_constraints, &uq.constraints, table_name)
            });

            if !table_covered {
                return false;
            }
        }

        true
    }

    /// Handle a subsumed query: assign generation, stamp rows in cache DB, mark Ready.
    /// Returns (generation, resolved) on success. Falls back to None if cache DB execution fails.
    async fn query_subsume(
        &self,
        core: &mut WriterCore,
        fingerprint: u64,
        resolution: QueryResolution,
        started_at: Instant,
        pinned: bool,
    ) -> CacheResult<Option<(u64, SharedResolved, EcoString)>> {
        let subsume_start = Instant::now();

        let (generation, relations_changed) = self.cached_query_insert(
            core,
            fingerprint,
            resolution.relation_oids,
            resolution.base_query,
            Arc::clone(&resolution.resolved),
            resolution.deparsed_sql.clone(),
            resolution.max_limit,
            started_at,
            pinned,
        );

        if relations_changed {
            core.publication_update().await?;
        }

        // Stamp rows: SET generation, execute query, reset generation
        let set_gen_sql = format!("SET mem.query_generation = {generation}");
        if let Err(e) = core
            .db_cache
            .batch_execute(&set_gen_sql)
            .await
            .map_into_report::<CacheError>()
        {
            error!(
                "subsumption generation set failed: {}",
                error_chain_format(e.current_context()),
            );
            return Ok(None);
        }

        let cache_exec_result = core
            .db_cache
            .batch_execute(resolution.deparsed_sql.as_str())
            .await
            .map_into_report::<CacheError>();

        // Always reset generation, even on failure
        let _ = core
            .db_cache
            .batch_execute("SET mem.query_generation = 0")
            .await;

        if let Err(e) = cache_exec_result {
            error!(
                "subsumption cache query failed: {}",
                error_chain_format(e.current_context()),
            );
            return Ok(None);
        }

        core.state_ready_transition(
            fingerprint,
            generation,
            Arc::clone(&resolution.resolved),
            resolution.deparsed_sql.clone(),
            resolution.max_limit,
        );

        // Clear registration_started_at to signal completion
        if let Some(mut q) = core.cache.cached_queries.get1_mut(&fingerprint) {
            q.registration_started_at = None;
        }

        // Record per-query metrics for subsumption
        if let Some(mut m) = core.state_view.metrics.get_mut(&fingerprint) {
            m.cached_since_ns =
                NonZeroU64::new(duration_to_ns_u64(core.state_view.started_at.elapsed()));
            m.subsumption_count += 1;
        }

        crate::metrics::handles().reg.subsumptions.increment(1);
        crate::metrics::handles()
            .reg
            .subsumption_latency
            .record(subsume_start.elapsed().as_secs_f64());

        debug!("query subsumed {fingerprint}");
        Ok(Some((
            generation,
            resolution.resolved,
            resolution.deparsed_sql,
        )))
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
        fingerprint: u64,
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
                        self.populate_work_dispatch(work)?;
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
        self.populate_work_dispatch(work)?;
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
        fingerprint: u64,
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
        self.populate_work_dispatch(work)?;
        trace!("readmission population queued for query {fingerprint}");
        Ok(())
    }

    /// Mark a query as ready after successful population.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn query_ready_mark(
        &self,
        core: &mut WriterCore,
        fingerprint: u64,
        cached_bytes: usize,
        row_count: u64,
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
            }

            core.state_ready_transition(fingerprint, generation, resolved, deparsed_sql, max_limit);

            trace!(
                "cached query ready, cached_bytes={cached_bytes} rows={row_count} {fingerprint}"
            );
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
    pub fn query_failed_cleanup(&self, core: &mut WriterCore, fingerprint: u64) {
        trace!("query_failed_cleanup {fingerprint}");

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
                    entry.complexity_order.retain(|fp| *fp != fingerprint);
                    entry.subsumption.remove(fingerprint);
                }
            }
        }

        core.state_view.cached_queries.remove(&fingerprint);
        let _ = core.notify_tx.send(WriterNotify::Failed { fingerprint });
    }

    /// Handle a limit bump: re-populate with a higher limit.
    ///
    /// Bumps the generation number, updates max_limit, and re-populates.
    /// During re-population the query state goes to Loading.
    #[instrument(skip_all)]
    pub async fn limit_bump_handle(
        &mut self,
        core: &mut WriterCore,
        fingerprint: u64,
        new_max_limit: Option<u64>,
    ) -> CacheResult<()> {
        let Some(cached_query) = core.cache.cached_queries.get1(&fingerprint) else {
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
        self.populate_work_dispatch(work)?;
        trace!("limit bump population queued for query {fingerprint}");
        Ok(())
    }
}

impl WriterCore {
    /// Register table metadata from CDC processing.
    #[instrument(skip_all)]
    pub(super) async fn cache_table_register(
        &mut self,
        mut table_metadata: TableMetadata,
    ) -> CacheResult<()> {
        let relation_oid = table_metadata.relation_oid;

        let table_exists = self.cache.tables.contains_key1(&relation_oid);
        if table_exists {
            if let Some(current_table) = self.cache.tables.get1(&relation_oid)
                && current_table.schema_eq(&table_metadata)
            {
                return Ok(());
            }

            info!(
                "Table {} (OID: {}) recreating table, invalidating queries",
                table_metadata.name, relation_oid
            );

            self.cache_table_invalidate(relation_oid).await?;
        }

        if table_metadata.indexes.is_empty() {
            table_metadata.indexes = self.query_table_indexes_get(relation_oid).await?;
        }

        self.cache_table_create_from_metadata(&table_metadata)
            .await?;

        self.cache.tables.insert_overwrite(table_metadata);

        Ok(())
    }
}

#[cfg(test)]
mod classify_tests {

    use super::*;

    use std::collections::HashMap;

    use iddqd::BiHashMap;
    use tokio_postgres::types::Type;

    use crate::cache::query::CacheableQuery;
    use crate::catalog::{ColumnMetadata, ColumnStore, TableMetadata};
    use crate::query::ast::query_expr_parse;
    use crate::query::resolved::query_expr_resolve;

    fn make_table(name: &str, oid: u32, columns: &[&str]) -> TableMetadata {
        let cols = ColumnStore::new(columns.iter().enumerate().map(|(i, c)| {
            let is_pk = i == 0;
            ColumnMetadata {
                name: (*c).into(),
                position: i16::try_from(i + 1).expect("column position fits in i16"),
                type_oid: if is_pk { 23 } else { 25 },
                data_type: if is_pk { Type::INT4 } else { Type::TEXT },
                type_name: if is_pk { "int4" } else { "text" }.into(),
                cache_type_name: if is_pk { "int4" } else { "text" }.into(),
                is_primary_key: is_pk,
            }
        }));
        TableMetadata {
            relation_oid: oid,
            name: name.into(),
            schema: "public".into(),
            primary_key_columns: vec![columns[0].into()],
            columns: cols,
            indexes: Vec::new(),
        }
    }

    fn resolve(sql: &str, tables: &BiHashMap<TableMetadata>) -> ResolvedQueryExpr {
        let query_expr = query_expr_parse(sql).expect("convert");
        let cacheable = CacheableQuery::try_new(&query_expr, &HashMap::new()).expect("cacheable");
        query_expr_resolve(&cacheable.query, tables, &["public"]).expect("resolve")
    }

    fn classify_single_table(sql: &str) -> UpdateEvalStrategy {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(make_table("t", 1, &["id", "name", "status", "age"]));
        let resolved = resolve(sql, &tables);
        update_eval_strategy_classify(&resolved, UpdateQuerySource::FromClause)
    }

    #[test]
    fn simple_equality_is_local_eval() {
        assert_eq!(
            classify_single_table("SELECT * FROM t WHERE id = 5"),
            UpdateEvalStrategy::LocalEval
        );
    }

    #[test]
    fn no_where_is_local_eval() {
        assert_eq!(
            classify_single_table("SELECT * FROM t"),
            UpdateEvalStrategy::LocalEval
        );
    }

    #[test]
    fn and_or_with_comparisons_is_local_eval() {
        assert_eq!(
            classify_single_table("SELECT * FROM t WHERE (id = 1 OR id = 2) AND name IS NOT NULL"),
            UpdateEvalStrategy::LocalEval
        );
    }

    #[test]
    fn in_list_is_pg_eval() {
        // IN is a Multi op — not yet evaluable in Rust
        assert_eq!(
            classify_single_table("SELECT * FROM t WHERE id IN (1, 2, 3)"),
            UpdateEvalStrategy::PgEval
        );
    }

    #[test]
    fn like_is_pg_eval() {
        assert_eq!(
            classify_single_table("SELECT * FROM t WHERE name LIKE 'j%'"),
            UpdateEvalStrategy::PgEval
        );
    }

    #[test]
    fn group_by_is_pg_eval() {
        assert_eq!(
            classify_single_table("SELECT status, count(*) FROM t GROUP BY status"),
            UpdateEvalStrategy::PgEval
        );
    }

    #[test]
    fn multi_table_is_pg_eval() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(make_table("a", 1, &["id", "bid"]));
        tables.insert_overwrite(make_table("b", 2, &["id", "name"]));
        let resolved = resolve("SELECT * FROM a JOIN b ON a.bid = b.id", &tables);
        assert_eq!(
            update_eval_strategy_classify(&resolved, UpdateQuerySource::FromClause),
            UpdateEvalStrategy::PgEval
        );
    }

    #[test]
    fn non_fromclause_source_is_pg_eval() {
        use crate::cache::SubqueryKind;
        let resolved = resolve("SELECT * FROM t WHERE id = 5", &{
            let mut tables = BiHashMap::new();
            tables.insert_overwrite(make_table("t", 1, &["id", "name"]));
            tables
        });
        // Same query, but classified as a subquery-sourced update query
        assert_eq!(
            update_eval_strategy_classify(
                &resolved,
                UpdateQuerySource::Subquery(SubqueryKind::Inclusion),
            ),
            UpdateEvalStrategy::PgEval
        );
    }
}
