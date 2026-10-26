//! Subsumption: can a newly-registered or readmitted query be served from
//! data an existing broader cached query already holds? If so it is stamped
//! and marked Ready without a population round-trip.

use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Instant;

use tracing::{debug, error};

use super::core::{QueryServing, WriterCore};
use super::registration::{
    QueryResolution, RegistrationIdentity, WriterRegistration, cached_query_insert,
};
use crate::cache::admission::{
    SubsumerCandidate, SubsumerSource, SubsumptionCover, SubsumptionReference, subsumption_covered,
    subsumption_references,
};
use crate::cache::messages::SubsumptionResult;
use crate::cache::types::Cache;
use crate::cache::{CacheError, CacheResult, MapIntoReport};
use crate::oid::Oid;
use crate::query::Fingerprint;
use crate::query::constraint_index::table_constraints_keyed;
use crate::query::constraints::TableRanges;
use crate::query::resolve::ResolvedQuery;
use crate::result::error_chain_format;
use crate::timing::duration_to_ns_u64;

/// Candidate source over the writer's per-relation subsumption index plus
/// parent readiness state.
struct WriterSubsumerSource<'a> {
    cache: &'a Cache,
}

impl SubsumerSource for WriterSubsumerSource<'_> {
    fn candidates(
        &self,
        reference: &SubsumptionReference,
    ) -> impl Iterator<Item = SubsumerCandidate<'_>> {
        let relation_oid = reference.relation_oid;
        // Probe with the new query's constraints in their key forms, the way
        // the index was built (PGC-478).
        let keyed = TableRanges::build(&self.cache.tables.get1(&relation_oid).map_or_else(
            || reference.filters.clone(),
            |t| table_constraints_keyed(&reference.filters, t),
        ));
        self.cache
            .update_queries
            .get(&relation_oid)
            .into_iter()
            .flat_map(move |update_queries| {
                // Sub-linear candidate lookup via the per-relation subsumption
                // index; the fine-grained gates run per candidate in
                // `subsumption_covered`.
                update_queries
                    .subsumption_candidates(&keyed)
                    .into_iter()
                    .filter_map(move |key| {
                        let region = update_queries.subsumer_region(key)?;
                        let fingerprint = key.fingerprint;
                        let parent = self.cache.cached_queries.get1(&fingerprint);
                        Some(SubsumerCandidate {
                            region: &region.ranges,
                            has_limit: update_queries
                                .queries_of(fingerprint)
                                .iter()
                                .any(|update_query| update_query.has_limit),
                            ready: parent.is_some_and(|q| {
                                !q.invalidated && q.registration_started_at.is_none()
                            }),
                        })
                    })
            })
    }
}

impl WriterRegistration {
    /// Whether every table reference of `bound` is covered by the regions of
    /// Ready, LIMIT-free cached queries on its relation, and how.
    pub(super) fn subsumption_check(
        &self,
        core: &WriterCore,
        relation_oids: &[Oid],
        bound: &ResolvedQuery,
    ) -> Option<SubsumptionCover> {
        if relation_oids.is_empty() {
            return None;
        }

        // A set operation is covered when every arm is (PGC-495).
        let references = subsumption_references(bound)?;
        subsumption_covered(&references, &WriterSubsumerSource { cache: &core.cache })
    }

    /// Handle a subsumed new query: insert its entry, then stamp its rows and
    /// mark it Ready. `NotSubsumed` when the cache DB failed and the caller
    /// must populate the entry instead.
    pub(super) async fn query_subsume(
        &self,
        core: &mut WriterCore,
        identity: RegistrationIdentity,
        resolution: QueryResolution,
        cover: SubsumptionCover,
    ) -> CacheResult<SubsumptionResult> {
        let fingerprint = identity.fingerprint;
        let bound = Arc::clone(&resolution.bound);
        let read_facts = Arc::clone(&resolution.read_facts);
        let serve_shape = Arc::clone(&resolution.serve_shape);
        let max_limit = resolution.max_limit;

        let (generation, relations_changed) = cached_query_insert(core, identity, resolution);

        if relations_changed {
            core.publication_relations_add().await?;
        }

        let serving = QueryServing {
            generation,
            bound,
            read_facts,
            serve_shape,
            max_limit,
        };
        Ok(subsumed_ready_mark(core, fingerprint, serving, cover).await)
    }
}

/// Stamp the rows a subsumed query reads under its generation and mark it
/// Ready, for a new entry or a readmitted one. `NotSubsumed` when stamping
/// failed on the cache DB; the entry is left as it was, Loading, for the
/// caller to populate.
pub(super) async fn subsumed_ready_mark(
    core: &mut WriterCore,
    fingerprint: Fingerprint,
    serving: QueryServing,
    cover: SubsumptionCover,
) -> SubsumptionResult {
    let subsume_start = Instant::now();
    let generation = serving.generation;
    // Running the query under the new generation stamps the rows it reads,
    // which the subsumer already holds.
    let stamp_sql = serving.bound.sql();

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
        return SubsumptionResult::NotSubsumed;
    }

    let cache_exec_result = core
        .db_cache
        .batch_execute(&stamp_sql)
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
        return SubsumptionResult::NotSubsumed;
    }

    let bound = Arc::clone(&serving.bound);
    let serve_shape = Arc::clone(&serving.serve_shape);
    core.state_ready_transition(fingerprint, serving);

    // Clear registration_started_at to signal completion
    if let Some(mut q) = core.cache.cached_queries.get1_mut(fingerprint) {
        q.registration_started_at = None;
    }

    // Record per-query metrics for subsumption
    if let Some(mut m) = core.state_view.metrics.get_mut(&fingerprint) {
        m.cached_since_ns =
            NonZeroU64::new(duration_to_ns_u64(core.state_view.started_at.elapsed()));
        m.subsumption_count += 1;
    }

    let reg = &crate::metrics::handles().reg;
    reg.subsumptions.increment(1);
    if cover == SubsumptionCover::Split {
        reg.subsumptions_split.increment(1);
    }
    reg.subsumption_latency
        .record(subsume_start.elapsed().as_secs_f64());

    debug!("query subsumed {fingerprint}");
    SubsumptionResult::Subsumed {
        generation,
        bound,
        serve_shape,
    }
}
