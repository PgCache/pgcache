//! Subsumption: can a newly-registered query be served from data an existing
//! broader cached query already holds? If so it is stamped and marked Ready
//! without a population round-trip.

use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Instant;

use ecow::EcoString;
use tracing::{debug, error};

use crate::query::Fingerprint;
use crate::query::constraints::{
    TableConstraint, analyze_query_constraints, table_constraints_subsumed,
};
use crate::result::error_chain_format;
use crate::timing::duration_to_ns_u64;

use super::super::types::SharedResolved;
use super::super::{CacheError, CacheResult, MapIntoReport};
use super::core::WriterCore;
use super::registration::{QueryResolution, WriterRegistration};

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
