//! Registration phase 1: ensure the query's tables exist in the cache, resolve
//! it, render its serve shape, classify its shape and limits, and register
//! its per-table update queries.

use std::sync::Arc;
use std::time::Instant;

use ecow::EcoString;

use super::{QueryResolution, WriterRegistration};
use crate::cache::admission::{
    AdmissionDepth, AdmissionQuery, CanonicalResolution, base_query_prepare,
    query_admission_analyze, query_decorrelate_checked, query_pushdown, query_resolve_canonical,
    shape_gate_classify,
};
use crate::cache::messages::RegisterRequest;
use crate::cache::mv::{query_has_join, query_has_window};
use crate::cache::mv_shape::ShapeGate;
use crate::cache::types::SharedQueryShape;
use crate::cache::update_query::UpdateQueries;
use crate::cache::writer::core::WriterCore;
use crate::cache::{CacheError, CacheResult, ReportExt};
use crate::oid::Oid;
use crate::query::Fingerprint;
use crate::query::ast::rope::{Query, QueryTree};
use crate::query::constraint_index::table_constraints_keyed;
use crate::query::keying_settings::KeyingSettings;
use crate::query::resolve::ResolvedQuery;

/// Resolve schema for a table: use explicit schema if provided, otherwise lookup via search path.
async fn table_schema_resolve(
    core: &mut WriterCore,
    schema: Option<&str>,
    table_name: &str,
    search_path: &Arc<[EcoString]>,
) -> CacheResult<EcoString> {
    match schema {
        Some(schema) => Ok(EcoString::from(schema)),
        None => core.schema_for_table_find(table_name, search_path).await,
    }
}

/// Ensure all tables referenced in the query exist in the cache.
/// Resolves schemas and creates cache tables as needed.
async fn cache_tables_ensure(
    core: &mut WriterCore,
    base_query: &QueryTree,
    search_path: &Arc<[EcoString]>,
) -> CacheResult<()> {
    let tables: Vec<(Option<EcoString>, EcoString)> = base_query
        .table_names()
        .map(|(schema, name)| (schema.map(EcoString::from), EcoString::from(name)))
        .collect();
    for (schema, table_name) in &tables {
        let table_name = table_name.as_str();
        let schema = table_schema_resolve(core, schema.as_deref(), table_name, search_path).await?;
        if !core
            .cache
            .tables
            .contains_key2(&(schema.as_str(), table_name))
        {
            let table = core.cache_table_create(Some(&schema), table_name).await?;
            core.cache.tables.insert_overwrite(table.into());
            core.write_aliases_publish();
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
    query: &QueryTree,
    keying: &KeyingSettings,
) -> CacheResult<Fingerprint> {
    let (base_query, _) = base_query_prepare(query);
    cache_tables_ensure(core, &base_query, &keying.search_path).await?;
    let path = search_path(keying);
    let resolution =
        query_resolve_canonical(base_query, &core.cache.tables, &path, &keying.literals)
            .map_err(|e| e.context_transform(CacheError::from))
            .attach_loc("resolving query expression for its fingerprint")?;
    Ok(resolution.bound.fingerprint())
}

fn search_path(keying: &KeyingSettings) -> Vec<&str> {
    keying.search_path.iter().map(EcoString::as_str).collect()
}

/// Render the serve shape once at registration (PGC-294): the per-shape
/// serve statement and its binds, which every cache hit splices in instead
/// of re-rendering the tree.
fn serve_shape_render(bound: &ResolvedQuery) -> SharedQueryShape {
    let render_start = Instant::now();
    let shape = Arc::new(bound.shape());
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
    query: Query<'_>,
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
        let (base_query, user_max_limit) = base_query_prepare(request.cacheable_query.tree());

        cache_tables_ensure(core, &base_query, &request.keying.search_path).await?;

        let path = search_path(&request.keying);
        let CanonicalResolution { bound, literals } = query_resolve_canonical(
            base_query,
            &core.cache.tables,
            &path,
            &request.keying.literals,
        )
        .map_err(|e| e.context_transform(CacheError::from))
        .attach_loc("resolving query expression")?;
        // The identity dispatch resolved this pair to must be the one this
        // resolution produces.
        debug_assert_eq!(
            bound.fingerprint(),
            request.fingerprint,
            "registration resolved to a different fingerprint than the alias"
        );
        let bound = query_pushdown(bound);

        bound
            .enum_order_dependence_check()
            .map_err(|e| e.context_transform(CacheError::from))
            .attach_loc("enum order-dependence gate")?;

        let serve_shape = serve_shape_render(&bound);

        // Decorrelate once; the shape gate and admission read the result,
        // and `query_register` / MV setup reuse `shape_gate`.
        let decorrelated =
            query_decorrelate_checked(&bound, &self.aggregate_functions)?.map(Arc::new);
        let shape_gate =
            shape_gate_classify(&bound, decorrelated.as_deref(), &self.aggregate_functions);
        let (max_limit, mv_limit) =
            resolution_limits(bound.tree().root(), shape_gate, user_max_limit);

        let uq_start = Instant::now();
        let relation_oids = self.update_queries_register(
            core,
            request.fingerprint,
            &bound,
            decorrelated.as_ref(),
            max_limit.is_some(),
        )?;
        crate::metrics::handles()
            .reg
            .resolve_update_queries_register
            .record(uq_start.elapsed().as_secs_f64());

        let population = decorrelated.unwrap_or_else(|| Arc::clone(&bound));
        Ok(QueryResolution {
            serve_shape,
            relation_oids,
            bound,
            population,
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
        bound: &Arc<ResolvedQuery>,
        decorrelated: Option<&Arc<ResolvedQuery>>,
        has_limit: bool,
    ) -> CacheResult<Vec<Oid>> {
        let analysis = query_admission_analyze(
            AdmissionQuery {
                bound,
                decorrelated,
            },
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
