//! The per-table admission derivation, extracted verbatim from the writer's
//! registration path (PGC-391) so pgcache-fit runs the identical analysis.

use std::collections::HashSet;

use ecow::EcoString;
use iddqd::BiHashMap;

use crate::catalog::TableMetadata;
use crate::query::Fingerprint;
use crate::query::ast::{AstNode, QueryBody, QueryExpr};
use crate::query::constraints::analyze_query_constraints;
use crate::query::decorrelate::query_expr_decorrelate;
use crate::query::predicate::CompiledPredicate;
use crate::query::resolved::{ResolvedQueryExpr, ResolvedTableNode};
use crate::query::transform::PgEvalTemplate;
use crate::query::update::query_table_update_queries;
use crate::result::ReportExt;

use super::super::mv::{ShapeGate, shape_classify};
use super::super::query::limit_rows_needed;
use super::super::update_query::{UpdateEvalStrategy, UpdateQuery, UpdateQuerySource};
use super::super::{CacheError, CacheResult};
use super::update_classify::{
    limit_order_keys_collect, limit_window_columns_collect, pg_batchable_classify,
    predicate_columns_collect, update_eval_strategy_classify,
};
use super::{AdmissionAnalysis, TableAdmission};

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

/// Classify a resolved query's shape for MV eligibility. Runs decorrelation
/// first so the classification matches what first-population / rebuild will
/// actually see (correlated subqueries get rewritten to JOIN + DISTINCT,
/// which affects classification). Falls back to the original resolved form
/// if decorrelation fails.
///
/// NOTE: `population_work_build` and `query_admission_analyze` also decorrelate
/// the same resolved query. Factoring these callers onto a single
/// decorrelation pass is a worthwhile follow-up but out of scope for v1.
pub fn shape_gate_classify(
    resolved: &ResolvedQueryExpr,
    aggregate_functions: &HashSet<EcoString>,
) -> ShapeGate {
    let decorrelated = query_expr_decorrelate(resolved, aggregate_functions).ok();
    let query: &ResolvedQueryExpr = match &decorrelated {
        Some(d) if d.transformed => &d.resolved,
        _ => resolved,
    };
    shape_classify(query, aggregate_functions)
}

/// Decorrelate the resolved query and derive one [`TableAdmission`] per
/// update query: the built [`UpdateQuery`], its constraints, and the
/// subsumer-eligibility gates. Pure — the caller stores (writer) or
/// simulates (fit) the results.
pub fn query_admission_analyze(
    resolved: &ResolvedQueryExpr,
    fingerprint: Fingerprint,
    has_limit: bool,
    aggregate_functions: &HashSet<EcoString>,
    tables: &BiHashMap<TableMetadata>,
    depth: AdmissionDepth,
) -> CacheResult<AdmissionAnalysis> {
    let decorrelated = query_expr_decorrelate(resolved, aggregate_functions)
        .map_err(|e| e.context_transform(CacheError::from))
        .attach_loc("decorrelating correlated subqueries")?;
    let update_source = if decorrelated.transformed {
        &decorrelated.resolved
    } else {
        resolved
    };

    let mut admissions = Vec::new();
    for (table_node, update_resolved, source) in query_table_update_queries(update_source) {
        let relation_oid = table_node.relation_oid;
        let constraints = update_resolved
            .as_select()
            .map(analyze_query_constraints)
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
                update_resolved.as_select().and_then(|select| {
                    tables
                        .get1(&relation_oid)
                        .and_then(|table_metadata| PgEvalTemplate::build(select, table_metadata))
                })
            } else {
                None
            };

        let table_name = table_node.name.clone();
        let mut update_query = UpdateQuery {
            fingerprint,
            resolved: update_resolved,
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
        transformed: decorrelated.transformed,
        tables: admissions,
    })
}
