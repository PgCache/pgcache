//! Registration phase 1: ensure the query's tables exist in the cache, resolve
//! it, render its serve shape, classify its shape and limits, and register
//! its per-table update queries.

use std::sync::Arc;
use std::time::Instant;

use ecow::EcoString;

use super::{QueryResolution, WriterRegistration};
use crate::cache::admission::{
    AdmissionDepth, base_query_prepare, query_admission_analyze, shape_gate_classify,
};
use crate::cache::messages::RegisterRequest;
use crate::cache::mv::{query_has_join, query_has_window};
use crate::cache::mv_shape::ShapeGate;
use crate::cache::types::{SharedResolved, SharedResolvedQuery};
use crate::cache::update_query::UpdateQueries;
use crate::cache::writer::core::WriterCore;
use crate::cache::{CacheError, CacheResult, ReportExt};
use crate::oid::Oid;
use crate::query::Fingerprint;
use crate::query::QueryShape;
use crate::query::ast::{AstNode, QueryExpr, TableNode};
use crate::query::constraint_index::table_constraints_keyed;
use crate::query::keying_settings::KeyingSettings;
use crate::query::resolve::{ResolvedQuery, query_expr_resolve, query_expr_resolve_bound};
use crate::query::resolved::ResolvedQueryExpr;
use crate::query::transform::query_predicate_pushdown;
use crate::query::transform::{
    LiteralCanonOutcome, query_literals_canonicalise, resolved_literals_canonicalise,
};

/// Resolve schema for a table: use explicit schema if provided, otherwise lookup via search path.
async fn table_schema_resolve(
    core: &mut WriterCore,
    table_node: &TableNode,
    search_path: &Arc<[EcoString]>,
) -> CacheResult<EcoString> {
    match &table_node.schema {
        Some(schema) => Ok(schema.clone()),
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
    search_path: &Arc<[EcoString]>,
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
            core.cache.tables.insert_overwrite(table.into());
        }
    }
    Ok(())
}

/// The fingerprint of a query under the session's keying settings: the hash
/// of its serve SQL (schema-qualified tables, canonical literals). Ensures
/// the cache tables exist and resolves exactly as registration does, so the
/// two agree.
pub(super) async fn query_fingerprint_resolve(
    core: &mut WriterCore,
    query: &QueryExpr,
    keying: &KeyingSettings,
) -> CacheResult<Fingerprint> {
    let (base_query, _) = base_query_prepare(query);
    cache_tables_ensure(core, &base_query, &keying.search_path).await?;
    let resolution = query_resolve_canonical(core, base_query, keying)
        .attach_loc("resolving query expression for its fingerprint")?;
    Ok(resolution.bound.fingerprint())
}

/// The identity-bearing form of a query in both representations.
struct CanonicalResolution {
    /// Canonical resolved tree: the rewrite and analysis source until those
    /// move onto `bound`.
    resolved: ResolvedQueryExpr,
    /// The canonical plain tree with its bindings (ADR-057).
    bound: SharedResolvedQuery,
    literals: LiteralCanonOutcome,
}

/// Resolve `base_query` on the session's search_path and canonicalise its
/// date-time literals under the session's settings. `base_query` is the
/// caller's own copy; it is canonicalised in place and becomes the shared
/// tree.
fn query_resolve_canonical(
    core: &WriterCore,
    mut base_query: QueryExpr,
    keying: &KeyingSettings,
) -> CacheResult<CanonicalResolution> {
    let path: Vec<&str> = keying.search_path.iter().map(EcoString::as_str).collect();
    let (mut resolved, bindings) = query_expr_resolve_bound(&base_query, &core.cache.tables, &path)
        .map_err(|e| e.context_transform(CacheError::from))?;
    let literals = query_literals_canonicalise(&mut base_query, &bindings, &keying.literals);
    let resolved_literals = resolved_literals_canonicalise(&mut resolved, &keying.literals);
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

/// Render the serve shape once at registration (PGC-294): the per-shape
/// serve statement and its binds, which every cache hit splices in instead
/// of re-rendering the tree.
fn serve_shape_render(bound: &ResolvedQuery) -> QueryShape {
    let render_start = Instant::now();
    let shape = bound.shape();
    crate::metrics::handles()
        .reg
        .resolve_deparse
        .record(render_start.elapsed().as_secs_f64());
    shape
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
    query: &QueryExpr,
    shape_gate: ShapeGate,
    user_max_limit: Option<u64>,
) -> (Option<u64>, Option<u64>) {
    let max_limit = if shape_gate.is_reducer() {
        None
    } else {
        user_max_limit
    };
    let mv_limit = if query_has_join(query) && !query_has_window(query) {
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
        let (base_query, user_max_limit) = base_query_prepare(request.cacheable_query.query());

        cache_tables_ensure(core, &base_query, &request.keying.search_path).await?;

        let CanonicalResolution {
            resolved,
            bound,
            literals,
        } = query_resolve_canonical(core, base_query, &request.keying)
            .attach_loc("resolving query expression")?;
        // The identity dispatch resolved this pair to must be the one this
        // resolution produces.
        debug_assert_eq!(
            bound.fingerprint(),
            request.fingerprint,
            "registration resolved to a different fingerprint than the alias"
        );
        // Pushdown (ADR-012) happens on the bound query; the resolved tree
        // its remaining consumers read is the pushed-down tree resolved.
        let (bound, resolved) = match query_predicate_pushdown(&bound) {
            Some(pushed) => {
                let path: Vec<&str> = request
                    .keying
                    .search_path
                    .iter()
                    .map(EcoString::as_str)
                    .collect();
                let resolved = query_expr_resolve(pushed.tree(), &core.cache.tables, &path)
                    .map_err(|e| e.context_transform(CacheError::from))
                    .attach_loc("resolving the pushed-down query")?;
                (Arc::new(pushed), resolved)
            }
            None => (bound, resolved),
        };
        let resolved: SharedResolved = Arc::new(resolved);

        bound
            .enum_order_dependence_check()
            .map_err(|e| e.context_transform(CacheError::from))
            .attach_loc("enum order-dependence gate")?;

        let serve_shape = serve_shape_render(&bound);

        // Classify the shape once here; `query_register` and MV setup both reuse
        // the result via `QueryResolution.shape_gate` to avoid re-running
        // decorrelation + classification.
        let shape_gate = shape_gate_classify(&resolved, &self.aggregate_functions);
        let (max_limit, mv_limit) = resolution_limits(bound.tree(), shape_gate, user_max_limit);

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
            serve_shape,
            relation_oids,
            bound,
            max_limit,
            mv_limit,
            shape_gate,
            reference_only: literals.residual,
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
            // Index under each literal's key form for its column's type, so
            // the row probe and the region probe match on value (PGC-478).
            let keyed = core
                .cache
                .tables
                .get1(&relation_oid)
                .map(|table| table_constraints_keyed(&admission.index_constraints, table))
                .unwrap_or_else(|| admission.index_constraints.clone());
            let mut queries = core
                .cache
                .update_queries
                .entry(relation_oid)
                .or_insert_with(|| UpdateQueries::new(relation_oid));
            queries.query_insert(admission.update_query);
            if admission.subsumer_eligible {
                queries.subsumption.insert(fingerprint, &keyed);
            }
            queries.eval_index.insert(fingerprint, &keyed);
            relation_oids.push(relation_oid);
        }
        Ok(relation_oids)
    }
}
