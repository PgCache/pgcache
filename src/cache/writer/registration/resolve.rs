//! Registration phase 1: ensure the query's tables exist in the cache, resolve
//! and deparse it, classify its shape and limits, and register its per-table
//! update queries.

use std::sync::Arc;
use std::time::Instant;

use ecow::EcoString;

use super::{QueryResolution, WriterRegistration};
use crate::cache::admission::{
    AdmissionDepth, base_query_prepare, query_admission_analyze, shape_gate_classify,
};
use crate::cache::messages::RegisterRequest;
use crate::cache::mv::{resolved_has_join, resolved_has_window};
use crate::cache::mv_shape::ShapeGate;
use crate::cache::types::SharedResolved;
use crate::cache::update_query::UpdateQueries;
use crate::cache::writer::core::WriterCore;
use crate::cache::{CacheError, CacheResult, ReportExt};
use crate::oid::Oid;
use crate::query::Fingerprint;
use crate::query::ast::{AstNode, Deparse, QueryExpr, TableNode};
use crate::query::query_shape_derive;
use crate::query::resolved::{ResolvedQueryExpr, enum_order_dependence_check, query_expr_resolve};
use crate::query::transform::predicate_pushdown_apply;

/// Resolve schema for a table: use explicit schema if provided, otherwise lookup via search path.
async fn table_schema_resolve(
    core: &WriterCore,
    table_node: &TableNode,
    search_path: &[&str],
) -> CacheResult<String> {
    match table_node.schema.as_deref() {
        Some(schema) => Ok(schema.to_owned()),
        None => {
            core.schema_for_table_find(table_node.name.as_str(), search_path)
                .await
        }
    }
}

/// Ensure all tables referenced in the query exist in the cache.
/// Resolves schemas and creates cache tables as needed.
async fn cache_tables_ensure(
    core: &mut WriterCore,
    base_query: &QueryExpr,
    search_path: &[&str],
) -> CacheResult<()> {
    for table_node in base_query.nodes::<TableNode>() {
        let table_name = table_node.name.as_str();
        let schema = table_schema_resolve(core, table_node, search_path).await?;
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

/// Deparse once at registration. The output is a pure function of the
/// resolved AST, so every cache hit can splice it in instead of re-running
/// the deparse traversal.
fn resolved_deparse(resolved: &ResolvedQueryExpr) -> EcoString {
    let deparse_start = Instant::now();
    let mut buf = String::with_capacity(256);
    resolved.deparse(&mut buf);
    let deparsed_sql: EcoString = buf.into();
    crate::metrics::handles()
        .reg
        .resolve_deparse
        .record(deparse_start.elapsed().as_secs_f64());
    deparsed_sql
}

/// `(max_limit, mv_limit)` for a resolved query.
///
/// Reducer shapes transform row cardinality — applying the user's LIMIT to
/// source-row population truncates the input and breaks re-evaluation (e.g.
/// `SELECT count(*) FROM t LIMIT 3` cached with 3 source rows returns 3, not
/// the real count). Force unbounded population for those shapes.
///
/// `mv_limit` caps the MV body to a top-N, independent of the population cap.
/// Only joins benefit (other reducers already collapse their input), and never
/// window functions — a windowed MV must store the full result because the
/// window depends on the whole partition.
fn resolution_limits(
    resolved: &ResolvedQueryExpr,
    shape_gate: ShapeGate,
    user_max_limit: Option<u64>,
) -> (Option<u64>, Option<u64>) {
    let max_limit = if shape_gate.is_reducer() {
        None
    } else {
        user_max_limit
    };
    let mv_limit = if resolved_has_join(resolved) && !resolved_has_window(resolved) {
        user_max_limit
    } else {
        None
    };
    (max_limit, mv_limit)
}

impl WriterRegistration {
    /// Resolve a query's tables and AST, register update queries, and extract constraints.
    /// This is the first phase of registration, before subsumption or population.
    pub(super) async fn query_resolve(
        &self,
        core: &mut WriterCore,
        request: &RegisterRequest,
    ) -> CacheResult<QueryResolution> {
        let search_path: Vec<&str> = request.search_path.iter().map(EcoString::as_str).collect();
        let (base_query, user_max_limit) = base_query_prepare(request.cacheable_query.query());

        cache_tables_ensure(core, &base_query, &search_path).await?;

        let resolved: SharedResolved = Arc::new(
            query_expr_resolve(&base_query, &core.cache.tables, &search_path)
                .map_err(|e| e.context_transform(CacheError::from))
                .attach_loc("resolving query expression")
                .map(predicate_pushdown_apply)?,
        );

        enum_order_dependence_check(&resolved)
            .map_err(|e| e.context_transform(CacheError::from))
            .attach_loc("enum order-dependence gate")?;

        let deparsed_sql = resolved_deparse(&resolved);

        // Parameterized shape (PGC-294): the per-shape serve statement + binds,
        // derived from the same resolved AST. Additive to the fingerprint.
        let serve_shape = query_shape_derive(&resolved);

        // Classify the shape once here; `query_register` and MV setup both reuse
        // the result via `QueryResolution.shape_gate` to avoid re-running
        // decorrelation + classification.
        let shape_gate = shape_gate_classify(&resolved, &self.aggregate_functions);
        let (max_limit, mv_limit) = resolution_limits(&resolved, shape_gate, user_max_limit);

        let uq_start = Instant::now();
        let relation_oids = self.update_queries_register(
            core,
            request.fingerprint,
            &resolved,
            max_limit.is_some(),
        )?;
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
            let relation_oid = admission.relation_oid;
            let mut queries = core
                .cache
                .update_queries
                .entry(relation_oid)
                .or_insert_with(|| UpdateQueries::new(relation_oid));
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
}
