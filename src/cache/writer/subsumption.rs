//! Subsumption: can a newly-registered query be served from data an existing
//! broader cached query already holds? If so it is stamped and marked Ready
//! without a population round-trip.

use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Instant;

use ecow::EcoString;
use tracing::{debug, error};

use crate::oid::Oid;
use crate::query::Fingerprint;
use crate::query::constraints::{TableConstraint, analyze_query_constraints};
use crate::result::error_chain_format;
use crate::timing::duration_to_ns_u64;

use super::super::admission::{SubsumerCandidate, SubsumerSource, subsumption_covered};
use super::super::types::{Cache, SharedResolved};
use super::super::{CacheError, CacheResult, MapIntoReport};
use super::core::WriterCore;
use super::registration::{QueryResolution, WriterRegistration};

/// Candidate source over the writer's per-relation subsumption index plus
/// parent readiness state.
struct WriterSubsumerSource<'a> {
    cache: &'a Cache,
}

impl SubsumerSource for WriterSubsumerSource<'_> {
    fn candidates(
        &self,
        relation_oid: Oid,
        table_constraints: &[TableConstraint],
    ) -> impl Iterator<Item = SubsumerCandidate<'_>> {
        self.cache
            .update_queries
            .get(&relation_oid)
            .into_iter()
            .flat_map(move |update_queries| {
                // Sub-linear candidate lookup via the per-relation subsumption
                // index; the fine-grained gates run per candidate in
                // `subsumption_covered`.
                update_queries
                    .subsumption
                    .candidates(table_constraints)
                    .into_iter()
                    .filter_map(move |fingerprint| {
                        let update_query = update_queries.queries.get(&fingerprint)?;
                        let parent = self.cache.cached_queries.get1(&fingerprint);
                        Some(SubsumerCandidate {
                            constraints: &update_query.constraints,
                            has_limit: update_query.has_limit,
                            ready: parent.is_some_and(|q| {
                                !q.invalidated && q.registration_started_at.is_none()
                            }),
                            single_relation: parent.is_some_and(|q| q.relation_oids.len() == 1),
                        })
                    })
            })
    }
}

impl WriterRegistration {
    /// Check whether all tables in the new query are covered by existing cached queries.
    /// Returns true only if every relation_oid has at least one Ready, non-limited
    /// UpdateQuery whose equality constraints are implied by the new query's constraints.
    pub(super) fn subsumption_check(
        &self,
        core: &WriterCore,
        resolution: &QueryResolution,
    ) -> bool {
        if resolution.relation_oids.is_empty() {
            return false;
        }

        // Set operations (UNION/INTERSECT/EXCEPT) require per-branch constraint
        // analysis which isn't implemented yet. Reject unconditionally for now.
        let Some(select) = resolution.resolved.as_select() else {
            return false;
        };

        let new_constraints = analyze_query_constraints(select);

        let mut relations = Vec::with_capacity(resolution.relation_oids.len());
        for &oid in &resolution.relation_oids {
            let Some(table_meta) = core.cache.tables.get1(&oid) else {
                return false;
            };
            relations.push((oid, table_meta.name.as_str()));
        }

        subsumption_covered(
            &new_constraints,
            &relations,
            &WriterSubsumerSource { cache: &core.cache },
        )
    }

    /// Handle a subsumed query: assign generation, stamp rows in cache DB, mark Ready.
    /// Returns (generation, resolved) on success. Falls back to None if cache DB execution fails.
    pub(super) async fn query_subsume(
        &self,
        core: &mut WriterCore,
        fingerprint: Fingerprint,
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
            resolution.serve_shape.clone(),
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
}
