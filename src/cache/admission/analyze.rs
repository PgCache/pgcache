//! The per-table admission derivation, extracted verbatim from the writer's
//! registration path (PGC-391) so pgcache-fit runs the identical analysis.

use std::collections::HashSet;
use std::sync::Arc;

use ecow::EcoString;

use super::update_classify::{
    limit_order_keys_collect, limit_window_columns_collect, pg_batchable_classify,
    predicate_columns_collect, update_eval_strategy_classify,
};
use super::{AdmissionAnalysis, TableAdmission};
use crate::cache::mv_shape::{ShapeGate, shape_classify};
use crate::cache::query::{limit_clause_of, limit_rows_needed};
use crate::cache::update_query::{UpdateEvalStrategy, UpdateQuery, UpdateQuerySource};
use crate::cache::{CacheError, CacheResult};
use crate::catalog::Catalog;
use crate::query::Fingerprint;
use crate::query::ast::rope::{Body, NodeKind, QueryTree, TreeBuilder, View};
use crate::query::constraints::{QueryConstraints, query_constraints_analyze};
use crate::query::decorrelate::query_decorrelate;
use crate::query::literal_settings::LiteralSettings;
use crate::query::predicate::CompiledPredicate;
use crate::query::resolve::ResolveResult;
use crate::query::resolve::{QueryBranch, ResolvedQuery};
use crate::query::transform::{
    LiteralCanonOutcome, PgEvalTemplate, query_literals_canonicalise, query_predicate_pushdown,
};
use crate::query::update::query_table_update_queries;
use crate::result::ReportExt;

/// How much of each [`TableAdmission`] to build. The writer needs the full
/// [`UpdateQuery`] including its CDC-eval caches; offline analysis
/// (pgcache-fit) keeps only the decision fields, so the caches — a per-table
/// deparse among them — are skipped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionDepth {
    Full,
    DecisionOnly,
}

/// The query without its root LIMIT, and max_limit for population. Set
/// operations force max_limit = None since population runs per-branch.
pub fn base_query_prepare(query: &QueryTree) -> (QueryTree, Option<u64>) {
    let root = query.root();
    let max_limit = match root.body() {
        Body::SetOp(_) => None,
        Body::Select(_) | Body::Values(_) => limit_rows_needed(&limit_clause_of(root)),
    };
    (query_tree_without_limit(query), max_limit)
}

/// `query` with its root LIMIT/OFFSET dropped: the root re-emitted without
/// the limit children, everything under it kept. The tree itself when it
/// has none.
fn query_tree_without_limit(query: &QueryTree) -> QueryTree {
    let root = query.root();
    let NodeKind::Query {
        cte_count,
        order_by_count,
        ..
    } = *root.node_ref().kind()
    else {
        return query.clone();
    };
    if !root.has_limit() {
        return query.clone();
    }
    let mut builder = TreeBuilder::over(query);
    let open = builder.open_from(
        NodeKind::Query {
            cte_count,
            order_by_count,
            has_limit_count: false,
            has_limit_offset: false,
        },
        root.id(),
    );
    for cte in root.ctes() {
        builder.keep(cte.id());
    }
    builder.keep(root.body().id());
    for clause in root.order_by() {
        builder.keep(clause.id());
    }
    builder.close(open);
    builder.finish().0
}

/// The identity-bearing form of a query.
pub struct CanonicalResolution {
    /// The canonical plain tree with its bindings (ADR-057).
    pub bound: Arc<ResolvedQuery>,
    pub literals: LiteralCanonOutcome,
}

/// Resolve `base_query` on `search_path` and canonicalise its date-time
/// literals under `settings`.
pub fn query_resolve_canonical(
    base_query: QueryTree,
    tables: &Catalog,
    search_path: &[&str],
    settings: &LiteralSettings,
) -> ResolveResult<CanonicalResolution> {
    let bound = ResolvedQuery::resolve(base_query, tables, search_path)?;
    let (bound, literals) = query_literals_canonicalise(bound, settings);
    Ok(CanonicalResolution {
        bound: Arc::new(bound),
        literals,
    })
}

/// Predicate pushdown (ADR-012) on the bound query; the query comes back as
/// it was when nothing moves.
pub fn query_pushdown(bound: Arc<ResolvedQuery>) -> Arc<ResolvedQuery> {
    match query_predicate_pushdown(&bound) {
        Some(pushed) => Arc::new(pushed),
        None => bound,
    }
}

/// Decorrelation (ADR-014 to ADR-017) on the bound query; `None` when there
/// is nothing to decorrelate. Runs once at registration for the shape gate,
/// admission and population alike.
pub fn query_decorrelate_checked(
    bound: &ResolvedQuery,
    aggregate_functions: &HashSet<EcoString>,
) -> CacheResult<Option<ResolvedQuery>> {
    query_decorrelate(bound, aggregate_functions)
        .map_err(|e| e.context_transform(CacheError::from))
        .attach_loc("decorrelating correlated subqueries")
}

/// The constraints of an update branch, read at the branch's position.
fn branch_constraints(bound: &ResolvedQuery, branch: QueryBranch<'_>) -> QueryConstraints {
    query_constraints_analyze(bound, branch.select)
}

/// Classify a query's shape for MV eligibility on its decorrelated form
/// when it has one, so the shape classified is the shape population and
/// rebuild see.
pub fn shape_gate_classify(
    bound: &ResolvedQuery,
    decorrelated: Option<&ResolvedQuery>,
    aggregate_functions: &HashSet<EcoString>,
) -> ShapeGate {
    shape_classify(decorrelated.unwrap_or(bound), aggregate_functions)
}

/// The query as admission reads it: as registered, and decorrelated when
/// it had correlated subqueries.
pub struct AdmissionQuery<'a> {
    pub bound: &'a Arc<ResolvedQuery>,
    pub decorrelated: Option<&'a Arc<ResolvedQuery>>,
}

/// Derive one [`TableAdmission`] per update query of the query, or of its
/// decorrelated form when it had correlated subqueries: the built
/// [`UpdateQuery`], its constraints, and the subsumer-eligibility gates.
/// Pure — the caller stores (writer) or simulates (fit) the results.
pub fn query_admission_analyze(
    query: AdmissionQuery<'_>,
    fingerprint: Fingerprint,
    has_limit: bool,
    aggregate_functions: &HashSet<EcoString>,
    tables: &Catalog,
    depth: AdmissionDepth,
) -> CacheResult<AdmissionAnalysis> {
    let AdmissionQuery {
        bound,
        decorrelated,
    } = query;
    let update_source = decorrelated.unwrap_or(bound);

    let mut admissions = Vec::new();
    for bound_update in query_table_update_queries(update_source) {
        let table_node = bound_update.table;
        let source = bound_update.source;
        let relation_oid = bound_update.relation_oid;
        let branch = bound_update.branch;
        let constraints = branch_constraints(update_source, branch);
        let eval_strategy = update_eval_strategy_classify(branch, source);
        let pg_batchable = depth == AdmissionDepth::Full
            && eval_strategy == UpdateEvalStrategy::PgEval
            && pg_batchable_classify(branch, relation_oid, aggregate_functions);
        // Walk the whole query: the branch has no ORDER BY.
        let limit_window_columns = if has_limit {
            limit_window_columns_collect(bound, table_node.name().as_str())
        } else {
            HashSet::new()
        };
        let predicate_columns = predicate_columns_collect(branch, table_node.name().as_str());
        let is_single_table = branch.is_single_table();
        // Direction-aware window spec (PGC-334) — the shapes the cached-path
        // window check can reason about. Walks the parent query like
        // `limit_window_columns` (the branch has no ORDER BY).
        let order_by_keys =
            (has_limit && is_single_table && matches!(source, UpdateQuerySource::FromClause))
                .then(|| limit_order_keys_collect(bound, table_node.name().as_str()))
                .flatten();
        // Compile the LocalEval WHERE once so the per-row CDC membership probe
        // doesn't re-destructure the tree (PGC-339). PgEval queries never
        // consult this, so skip building it.
        let compiled_where =
            if depth == AdmissionDepth::Full && eval_strategy == UpdateEvalStrategy::LocalEval {
                branch.select.where_clause().map(|w| {
                    CompiledPredicate::compile(update_source, w, table_node.name().as_str())
                })
            } else {
                None
            };
        // Precompute the PgEval membership template so the per-row check
        // skips the per-row deparse (PGC-343). Only PgEval uses
        // it; needs the relation's TableMetadata.
        let pg_eval_template =
            if depth == AdmissionDepth::Full && eval_strategy == UpdateEvalStrategy::PgEval {
                tables
                    .get1(&relation_oid)
                    .and_then(|table_metadata| PgEvalTemplate::build(branch, table_metadata))
            } else {
                None
            };

        let table_name = table_node.name().clone();
        let mut update_query = UpdateQuery {
            fingerprint,
            ordinal: 0,
            bound: Arc::clone(update_source),
            branch: branch.select.id(),
            source,
            constraints,
            has_limit,
            eval_strategy,
            limit_window_columns,
            order_by_keys,
            change_dependent: false,
            pg_batchable,
            predicate_columns,
            is_single_table,
            compiled_where,
            pg_eval_template,
        };
        // Whether a CDC UPDATE for this query can ever invalidate — i.e.
        // whether `handle_update` must run `query_row_changes` + the
        // invalidation check rather than skip them (PGC-227). Derived from
        // the single source of truth so the flag can't drift.
        if depth == AdmissionDepth::Full {
            update_query.change_dependent = update_query.update_invalidation_possible(&table_name);
        }

        // A relation appearing more than once (a self-join) has its
        // constraints extracted per occurrence but keyed by table *name*, so
        // they collapse into one set that only holds for one arm (PGC-256).
        // Index it as *unconstrained* — the broadest candidate class — and
        // keep it out of subsumption, where "no constraints" means the
        // opposite (the parent loaded every row).
        let self_joined = update_query.bound.relation_occurrences(relation_oid) > 1;
        let index_constraints = if self_joined {
            Vec::new()
        } else {
            update_query
                .constraints
                .table_constraints
                .get(table_name.as_str())
                .cloned()
                .unwrap_or_default()
        };
        // !where_analysis_complete: the WHERE couldn't be fully analyzed, so
        // coverage isn't reasonable (PGC-106). A LIMIT also excludes a parent.
        let subsumer_shape = !self_joined && update_query.constraints.where_analysis_complete;

        admissions.push(TableAdmission {
            relation_oid,
            table_name,
            update_query,
            subsumer_shape,
            subsumer_eligible: subsumer_shape && !has_limit,
            index_constraints,
        });
    }

    Ok(AdmissionAnalysis {
        transformed: decorrelated.is_some(),
        tables: admissions,
    })
}
