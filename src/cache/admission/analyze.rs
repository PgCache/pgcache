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
use crate::cache::query::limit_rows_needed;
use crate::cache::update_query::{UpdateEvalStrategy, UpdateQuery, UpdateQuerySource};
use crate::cache::{CacheError, CacheResult};
use crate::catalog::Catalog;
use crate::query::Fingerprint;
use crate::query::ast::{AstNode, QueryBody, QueryExpr};
use crate::query::constraints::query_constraints_analyze;
use crate::query::decorrelate::query_decorrelate;
use crate::query::literal_settings::LiteralSettings;
use crate::query::predicate::CompiledPredicate;
use crate::query::resolve::{ResolvedQuery, query_expr_resolve, query_expr_resolve_bound};
use crate::query::resolved::{ResolveResult, ResolvedQueryExpr, ResolvedTableNode};
use crate::query::transform::{
    LiteralCanonOutcome, PgEvalTemplate, query_literals_canonicalise, query_predicate_pushdown,
    resolved_literals_canonicalise,
};
use crate::query::update::{query_table_update_queries, query_table_update_queries_bound};
use crate::result::ReportExt;

/// How much of each [`TableAdmission`] to build. The writer needs the full
/// [`UpdateQuery`] including its CDC-eval caches; offline analysis
/// (pgcache-fit) keeps only the decision fields, so the caches — a per-table
/// AST clone + deparse among them — are skipped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionDepth {
    Full,
    DecisionOnly,
}

/// Clone the query, strip LIMIT, and compute max_limit for population.
/// Set operations force max_limit = None since population runs per-branch.
pub fn base_query_prepare(query: &QueryExpr) -> (QueryExpr, Option<u64>) {
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

/// The identity-bearing form of a query in both representations.
pub struct CanonicalResolution {
    /// Canonical resolved tree: the analysis source until those analyses
    /// move onto `bound`.
    pub resolved: ResolvedQueryExpr,
    /// The canonical plain tree with its bindings (ADR-057).
    pub bound: Arc<ResolvedQuery>,
    pub literals: LiteralCanonOutcome,
}

/// Resolve `base_query` on `search_path` and canonicalise its date-time
/// literals under `settings`. `base_query` is the caller's own copy; it is
/// canonicalised in place and becomes the shared tree.
pub fn query_resolve_canonical(
    mut base_query: QueryExpr,
    tables: &Catalog,
    search_path: &[&str],
    settings: &LiteralSettings,
) -> ResolveResult<CanonicalResolution> {
    let (mut resolved, bindings) = query_expr_resolve_bound(&base_query, tables, search_path)?;
    let literals = query_literals_canonicalise(&mut base_query, &bindings, settings);
    let resolved_literals = resolved_literals_canonicalise(&mut resolved, settings);
    debug_assert_eq!(
        literals, resolved_literals,
        "plain and resolved canonicalisation disagree"
    );
    Ok(CanonicalResolution {
        resolved,
        bound: Arc::new(ResolvedQuery::new(Arc::new(base_query), bindings)),
        literals,
    })
}

/// Predicate pushdown (ADR-012) on the bound query, with the pushed-down
/// tree resolved for the analyses still on the resolved tree. Both come
/// back as they were when nothing moves.
pub fn query_pushdown_resolve(
    bound: Arc<ResolvedQuery>,
    resolved: ResolvedQueryExpr,
    tables: &Catalog,
    search_path: &[&str],
) -> ResolveResult<(Arc<ResolvedQuery>, ResolvedQueryExpr)> {
    match query_predicate_pushdown(&bound) {
        Some(pushed) => {
            let resolved = query_expr_resolve(pushed.tree(), tables, search_path)?;
            Ok((Arc::new(pushed), resolved))
        }
        None => Ok((bound, resolved)),
    }
}

/// Decorrelation (ADR-014 to ADR-017) on the bound query, with the
/// rewritten tree resolved likewise; `None` when there is nothing to
/// decorrelate. Runs once at registration for the shape gate, admission
/// and population alike.
pub fn query_decorrelate_resolve(
    bound: &ResolvedQuery,
    aggregate_functions: &HashSet<EcoString>,
    tables: &Catalog,
    search_path: &[&str],
) -> CacheResult<Option<(ResolvedQuery, ResolvedQueryExpr)>> {
    let Some(decorrelated) = query_decorrelate(bound, aggregate_functions)
        .map_err(|e| e.context_transform(CacheError::from))
        .attach_loc("decorrelating correlated subqueries")?
    else {
        return Ok(None);
    };
    let resolved = query_expr_resolve(decorrelated.tree(), tables, search_path)
        .map_err(|e| e.context_transform(CacheError::from))
        .attach_loc("resolving the decorrelated query")?;
    Ok(Some((decorrelated, resolved)))
}

/// Classify a query's shape for MV eligibility on its decorrelated form
/// when it has one, so the shape classified is the shape population and
/// rebuild see.
pub fn shape_gate_classify(
    resolved: &ResolvedQueryExpr,
    decorrelated: Option<&ResolvedQueryExpr>,
    aggregate_functions: &HashSet<EcoString>,
) -> ShapeGate {
    shape_classify(decorrelated.unwrap_or(resolved), aggregate_functions)
}

/// The query as admission reads it: as registered in both representations,
/// and decorrelated when it had correlated subqueries.
pub struct AdmissionQuery<'a> {
    pub resolved: &'a ResolvedQueryExpr,
    pub bound: &'a ResolvedQuery,
    pub decorrelated: Option<&'a (ResolvedQuery, ResolvedQueryExpr)>,
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
        resolved,
        bound,
        decorrelated,
    } = query;
    let (bound_source, update_source) = match decorrelated {
        Some((bound, resolved)) => (bound, resolved),
        None => (bound, resolved),
    };
    let bound_updates = query_table_update_queries_bound(bound_source);
    debug_assert_eq!(
        bound_updates.len(),
        query_table_update_queries(update_source).len(),
        "the bound and resolved derivations disagree on the update queries"
    );

    let mut admissions = Vec::new();
    for ((table_node, update_resolved, source), bound_update) in
        query_table_update_queries(update_source)
            .into_iter()
            .zip(bound_updates)
    {
        debug_assert!(
            table_node.name == bound_update.table.name && source == bound_update.source,
            "the bound and resolved derivations disagree on an update query"
        );
        let relation_oid = table_node.relation_oid;
        let constraints = bound_update
            .bound
            .select()
            .map(|select| query_constraints_analyze(&bound_update.bound, select))
            .unwrap_or_default();
        let eval_strategy = update_eval_strategy_classify(&update_resolved, source);
        let pg_batchable = depth == AdmissionDepth::Full
            && eval_strategy == UpdateEvalStrategy::PgEval
            && pg_batchable_classify(&update_resolved, relation_oid, aggregate_functions);
        // Walk the parent `resolved` — `update_resolved` has ORDER BY stripped.
        let limit_window_columns = if has_limit {
            limit_window_columns_collect(resolved, table_node.name.as_str())
        } else {
            HashSet::new()
        };
        // Walk `update_resolved`: it is the AST CDC eval actually runs.
        let predicate_columns =
            predicate_columns_collect(&update_resolved, table_node.name.as_str());
        let is_single_table = update_resolved.is_single_table();
        // Direction-aware window spec (PGC-334) — the shapes the cached-path
        // window check can reason about. Walks the parent `resolved` like
        // `limit_window_columns` (ORDER BY is stripped from `update_resolved`).
        let order_by_keys =
            (has_limit && is_single_table && matches!(source, UpdateQuerySource::FromClause))
                .then(|| limit_order_keys_collect(resolved, table_node.name.as_str()))
                .flatten();
        // Compile the LocalEval WHERE once so the per-row CDC membership probe
        // doesn't re-destructure the resolved AST (PGC-339). PgEval queries
        // never consult this, so skip building it.
        let compiled_where =
            if depth == AdmissionDepth::Full && eval_strategy == UpdateEvalStrategy::LocalEval {
                update_resolved
                    .as_select()
                    .and_then(|s| s.where_clause.as_ref())
                    .map(|w| CompiledPredicate::compile(w, table_node.name.as_str()))
            } else {
                None
            };
        // Precompute the PgEval membership template so the per-row check
        // skips the resolved-AST clone + deparse (PGC-343). Only PgEval uses
        // it; needs the relation's TableMetadata.
        let pg_eval_template =
            if depth == AdmissionDepth::Full && eval_strategy == UpdateEvalStrategy::PgEval {
                tables.get1(&relation_oid).and_then(|table_metadata| {
                    PgEvalTemplate::build(&bound_update.bound, table_metadata)
                })
            } else {
                None
            };

        let table_name = table_node.name.clone();
        let mut update_query = UpdateQuery {
            fingerprint,
            resolved: update_resolved,
            bound: bound_update.bound,
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
        let self_joined = update_query
            .resolved
            .nodes::<ResolvedTableNode>()
            .filter(|t| t.relation_oid == relation_oid)
            .count()
            > 1;
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
        // - has_limit: limited queries are excluded from subsumption.
        // - !where_analysis_complete: the WHERE couldn't be fully analyzed,
        //   so coverage isn't reasonable (PGC-106).
        let subsumer_eligible =
            !self_joined && !has_limit && update_query.constraints.where_analysis_complete;

        admissions.push(TableAdmission {
            relation_oid,
            table_name,
            update_query,
            subsumer_eligible,
            index_constraints,
        });
    }

    Ok(AdmissionAnalysis {
        transformed: decorrelated.is_some(),
        tables: admissions,
    })
}
