//! Registration phase 1: ensure the query's tables exist in the cache and
//! bind it to them; then, for a query that gets an entry, render its serve
//! shape, classify its shape and limits, and register its per-table update
//! queries.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use ecow::EcoString;

use super::{QueryResolution, WriterRegistration};
use crate::cache::admission::{
    AdmissionDepth, AdmissionQuery, CanonicalResolution, base_query_prepare, population_max_limit,
    query_admission_analyze, query_decorrelate_checked, query_pushdown, query_resolve_canonical,
    shape_gate_classify,
};
use crate::cache::messages::RegisterRequest;
use crate::cache::mv::{query_has_join, query_has_window};
use crate::cache::types::{AliasResolution, ReadFacts, SharedQueryShape, SharedResolvedQuery};
use crate::cache::update_query::{UpdateQueries, UpdateQuery};
use crate::cache::writer::core::WriterCore;
use crate::cache::{CacheError, CacheResult, ReportExt};
use crate::oid::Oid;
use crate::query::Fingerprint;
use crate::query::ast::rope::QueryTree;
use crate::query::keying_settings::KeyingSettings;
use crate::query::resolve::{ResolvedQuery, TableSlot};

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
            core.write_targets_publish();
        }
    }
    Ok(())
}

/// Ensure the query's tables exist in the cache and bind it to them under
/// the session's keying settings, with the user's LIMIT split off. The one
/// path to a query's canonical form: the fingerprint dispatch resolves and
/// the one registration asserts on both come from here.
async fn query_bind_canonical(
    core: &mut WriterCore,
    query: &QueryTree,
    keying: &KeyingSettings,
) -> CacheResult<(CanonicalResolution, Option<u64>)> {
    let (base_query, user_max_limit) = base_query_prepare(query);
    cache_tables_ensure(core, &base_query, &keying.search_path).await?;
    let path = search_path(keying);
    let mut resolution =
        query_resolve_canonical(base_query, &core.cache.tables, &path, &keying.literals)
            .map_err(|e| e.context_transform(CacheError::from))
            .attach_loc("resolving query expression")?;
    // A secured relation's references take the policies that apply to the
    // read's role (PGC-113); the inlined form is the one the query is
    // registered and keyed under, and it may name tables of its own.
    if let Some(inlined) = core.rls_inline(&resolution.bound, keying).await? {
        cache_tables_ensure(core, &inlined, &keying.search_path).await?;
        let path = search_path(keying);
        resolution = query_resolve_canonical(inlined, &core.cache.tables, &path, &keying.literals)
            .map_err(|e| e.context_transform(CacheError::from))
            .attach_loc("resolving the query with its policies inlined")?;
    }
    Ok((resolution, user_max_limit))
}

/// The fingerprint of a query under the session's keying settings, the hash
/// of its serve SQL (schema-qualified tables, canonical literals), with the
/// binding that produced it for the registration to reuse.
pub(super) async fn query_fingerprint_resolve(
    core: &mut WriterCore,
    query: &QueryTree,
    keying: &KeyingSettings,
) -> CacheResult<(Fingerprint, AliasResolution)> {
    let (CanonicalResolution { bound, literals }, user_max_limit) =
        query_bind_canonical(core, query, keying)
            .await
            .attach_loc("resolving query expression for its fingerprint")?;
    let fingerprint = bound.fingerprint();
    Ok((
        fingerprint,
        AliasResolution {
            bound,
            reference_only: literals.residual,
            user_max_limit,
        },
    ))
}

/// The binding the Resolve command left on the request's alias, when every
/// table it bound is still the catalog's entry; a table replaced since
/// (schema change) binds afresh against the current catalog.
fn alias_resolution_take(core: &WriterCore, request: &RegisterRequest) -> Option<AliasResolution> {
    let resolution = core
        .state_view
        .aliases
        .get_mut(&request.session_key)?
        .resolution
        .take()?;
    let current = resolution
        .bound
        .bindings()
        .slots()
        .iter()
        .all(|slot| match slot {
            TableSlot::Base { table, .. } => core
                .cache
                .tables
                .get1(&table.relation_oid)
                .is_some_and(|catalog| Arc::ptr_eq(catalog.shared(), table)),
            TableSlot::Derived { .. } => true,
        });
    current.then_some(resolution)
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

/// `(max_limit, mv_limit)` for a resolved query and its decorrelated form:
/// the population cap of [`population_max_limit`], and the MV cap.
///
/// `mv_limit` caps the MV body to a top-N, independent of the population cap.
/// Only joins benefit (other reducers already collapse their input), and never
/// window functions — a windowed MV must store the full result because the
/// window depends on the whole partition.
fn resolution_limits(
    bound: &ResolvedQuery,
    decorrelated: Option<&ResolvedQuery>,
    aggregate_functions: &HashSet<EcoString>,
    user_max_limit: Option<u64>,
) -> (Option<u64>, Option<u64>) {
    let max_limit = population_max_limit(bound, decorrelated, aggregate_functions, user_max_limit);
    let query = bound.tree().root();
    let mv_limit = if query_has_join(query) && !query_has_window(query) {
        user_max_limit
    } else {
        None
    };
    (max_limit, mv_limit)
}

/// A query bound against the cache catalog: what the subsumption check reads.
/// A check-only miss nothing covers stops here, so the rest of the
/// resolution is never built for it.
pub(super) struct BoundQuery {
    pub(super) bound: SharedResolvedQuery,
    user_max_limit: Option<u64>,
    reference_only: bool,
}

impl WriterRegistration {
    /// The query bound to its cache tables: the Resolve command's binding
    /// when its alias still holds it, else bound here (a pinned query, a
    /// re-registration, or a schema change since).
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) async fn query_bind(
        &self,
        core: &mut WriterCore,
        request: &RegisterRequest,
    ) -> CacheResult<BoundQuery> {
        let AliasResolution {
            bound,
            reference_only,
            user_max_limit,
        } = match alias_resolution_take(core, request) {
            Some(resolution) => resolution,
            None => {
                let bind_start = Instant::now();
                let (CanonicalResolution { bound, literals }, user_max_limit) =
                    query_bind_canonical(core, request.cacheable_query.tree(), &request.keying)
                        .await?;
                crate::metrics::handles()
                    .reg
                    .register_bind
                    .record(bind_start.elapsed().as_secs_f64());
                AliasResolution {
                    bound,
                    reference_only: literals.residual,
                    user_max_limit,
                }
            }
        };
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

        Ok(BoundQuery {
            bound,
            user_max_limit,
            reference_only,
        })
    }

    /// Complete a bound query's resolution for its entry: render the serve
    /// shape, classify shape and limits, register the update queries and
    /// build the read facts.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) fn query_resolve(
        &self,
        core: &mut WriterCore,
        fingerprint: Fingerprint,
        query: BoundQuery,
    ) -> CacheResult<QueryResolution> {
        let BoundQuery {
            bound,
            user_max_limit,
            reference_only,
        } = query;

        let serve_shape = serve_shape_render(&bound);

        // Decorrelate once; the shape gate and admission read the result,
        // and `query_register` / MV setup reuse `shape_gate`.
        let decorrelated =
            query_decorrelate_checked(&bound, &self.aggregate_functions)?.map(Arc::new);
        let shape_gate =
            shape_gate_classify(&bound, decorrelated.as_deref(), &self.aggregate_functions);
        let (max_limit, mv_limit) = resolution_limits(
            &bound,
            decorrelated.as_deref(),
            &self.aggregate_functions,
            user_max_limit,
        );

        let uq_start = Instant::now();
        let relation_oids = self.update_queries_register(
            core,
            fingerprint,
            &bound,
            decorrelated.as_ref(),
            max_limit.is_some(),
        )?;
        crate::metrics::handles()
            .reg
            .resolve_update_queries_register
            .record(uq_start.elapsed().as_secs_f64());

        let population = decorrelated.unwrap_or_else(|| Arc::clone(&bound));
        let read_facts = Arc::new(ReadFacts::build(&bound));
        Ok(QueryResolution {
            serve_shape,
            relation_oids,
            bound,
            read_facts,
            population,
            max_limit,
            mv_limit,
            shape_gate,
            reference_only,
        })
    }

    /// Run the pure admission analysis (decorrelation, per-table update
    /// queries, subsumer eligibility) and store the results: the update-query
    /// map plus the constraint indexes for sub-linear subsumption candidate
    /// lookup, and the query as a join parent when it can be one. Returns
    /// the relation OIDs that have update queries registered.
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
                fingerprint,
                has_limit,
                join_region: bound.predicates().join_region(),
            },
            &self.aggregate_functions,
            &core.cache.tables,
            AdmissionDepth::Full,
        )?;

        let mut relation_oids = Vec::new();
        let mut registrations: Vec<(Oid, Vec<UpdateQuery>)> = Vec::new();
        for admission in analysis.tables {
            let relation_oid = admission.relation_oid;
            let update_query = admission.update_query;
            match registrations
                .iter_mut()
                .find(|(oid, _)| *oid == relation_oid)
            {
                Some((_, group)) => group.push(update_query),
                None => registrations.push((relation_oid, vec![update_query])),
            }
            relation_oids.push(relation_oid);
        }
        for (relation_oid, group) in registrations {
            core.cache
                .update_queries
                .entry(relation_oid)
                .or_insert_with(|| UpdateQueries::new(relation_oid))
                .fingerprint_insert(fingerprint, group);
        }
        if let Some(region) = &analysis.join_region {
            core.cache
                .join_subsumers
                .fingerprint_insert(fingerprint, region, has_limit);
        }
        Ok(relation_oids)
    }
}
