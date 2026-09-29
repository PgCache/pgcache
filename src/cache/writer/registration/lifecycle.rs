//! Lifecycle transitions of a registered query: readmission after CDC
//! invalidation, re-population on a limit bump, and Ready after a merge.

use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Instant;

use tracing::{debug, instrument, trace};

use super::{PopulationTarget, WriterRegistration};
use crate::cache::CacheResult;
use crate::cache::coalesce_queue::fetch_stage_ewma_update;
use crate::cache::messages::PopulationMerge;
use crate::cache::writer::core::WriterCore;
use crate::oid::Oid;
use crate::query::Fingerprint;
use crate::timing::{duration_to_ns_u64, duration_to_us_u64};

/// Update the `has_limit` bit on the query's update queries. Limited queries
/// are ineligible parents for subsumption, so the index entry is dropped when
/// the bit goes false → true and (re-)added when it goes true → false.
fn subsumption_limit_reindex(
    core: &mut WriterCore,
    fingerprint: Fingerprint,
    relation_oids: &[Oid],
    has_limit: bool,
) {
    for oid in relation_oids {
        let table_name = core.cache.tables.get1(oid).map(|t| t.name.clone());
        let Some(mut queries) = core.cache.update_queries.get_mut(oid) else {
            continue;
        };
        if let Some(uq) = queries.queries.get_mut(&fingerprint) {
            uq.has_limit = has_limit;
        }
        if has_limit {
            queries.subsumption.remove(fingerprint);
            continue;
        }
        let Some(name) = table_name else {
            continue;
        };
        let constraints = queries
            .queries
            .get(&fingerprint)
            .filter(|uq| uq.constraints.where_analysis_complete)
            .map(|uq| {
                uq.constraints
                    .table_constraints
                    .get(name.as_str())
                    .cloned()
                    .unwrap_or_default()
            });
        if let Some(tcs) = constraints {
            queries.subsumption.insert(fingerprint, &tcs);
        }
    }
}

impl WriterRegistration {
    /// Fast readmission for a CDC-invalidated query.
    /// Reuses existing metadata (relation_oids, resolved, update_queries) and
    /// dispatches population work without re-resolving tables.
    pub(super) async fn query_readmit(
        &mut self,
        core: &mut WriterCore,
        fingerprint: Fingerprint,
        started_at: Instant,
    ) -> CacheResult<()> {
        debug!("readmitting query {fingerprint}");
        crate::metrics::handles().state.readmissions.increment(1);
        if let Some(mut m) = core.state_view.metrics.get_mut(&fingerprint) {
            m.readmission_count += 1;
        }

        let new_generation = core.cache.generation_allocate();

        // Extract data before remove/reinsert (generation is key2)
        let Some(mut cached) = core.cache.cached_queries.remove1(&fingerprint) else {
            return Ok(());
        };

        let resolved = Arc::clone(&cached.resolved);
        let deparsed_sql = cached.deparsed_sql.clone();
        let max_limit = cached.max_limit;

        cached.generation = new_generation;
        cached.invalidated = false;
        cached.cached_bytes = 0;
        cached.registration_started_at = Some(started_at);
        // Refcount unchanged — readmit reuses the existing relation_oids set.
        core.cache.cached_queries.insert_overwrite(cached);

        core.state_loading_transition(
            fingerprint,
            new_generation,
            &resolved,
            &deparsed_sql,
            max_limit,
        );

        self.population_start(
            core,
            PopulationTarget {
                fingerprint,
                generation: new_generation,
                resolved: &resolved,
                max_limit,
            },
        )?;
        trace!("readmission population queued for query {fingerprint}");
        Ok(())
    }

    /// Mark a query as ready after its population `merge` applied.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) fn query_ready_mark(&self, core: &mut WriterCore, merge: &PopulationMerge) {
        let fingerprint = merge.fingerprint;
        trace!("query_ready_mark {fingerprint}");
        let update_info = core
            .cache
            .cached_queries
            .get1_mut(&fingerprint)
            .map(|mut query| {
                query.cached_bytes = merge.cached_bytes;
                let started_at = query.registration_started_at.take();
                (
                    query.generation,
                    Arc::clone(&query.resolved),
                    query.deparsed_sql.clone(),
                    query.max_limit,
                    started_at,
                )
            });
        let Some((generation, resolved, deparsed_sql, max_limit, started_at)) = update_info else {
            return;
        };

        // Record registration latency metric
        let population_duration_us = started_at.map(|s| {
            let latency = s.elapsed();
            crate::metrics::handles()
                .reg
                .registration_latency
                .record(latency.as_secs_f64());
            duration_to_us_u64(latency)
        });

        // Record per-query population metrics
        if let Some(mut m) = core.state_view.metrics.get_mut(&fingerprint) {
            m.population_count += 1;
            m.population_row_count = merge.row_count;
            m.cached_since_ns =
                NonZeroU64::new(duration_to_ns_u64(core.state_view.started_at.elapsed()));
            m.last_population_duration_us = population_duration_us.and_then(NonZeroU64::new);
            m.population_fetch_stage_ewma_ms = Some(fetch_stage_ewma_update(
                m.population_fetch_stage_ewma_ms,
                merge.fetch_stage_ms,
            ));
        }

        core.state_ready_transition(fingerprint, generation, resolved, deparsed_sql, max_limit);

        // One unit of drained registration work, for the adaptive-gate
        // drain-rate (capacity) estimate (PGC-277).
        core.state_view.reg_gate.completed_inc();

        trace!(
            "cached query ready, cached_bytes={} rows={} {fingerprint}",
            merge.cached_bytes, merge.row_count
        );
    }

    /// Finalize a population: mark the query Ready and bootstrap any pinned MV.
    ///
    /// Eviction is NOT done here: it runs on the 1s writer tick (`eviction_run`,
    /// statvfs-driven). A per-Ready `SELECT pgcache_total_size()` round-trip
    /// (O(#cache tables)) serialized the single-threaded writer and backed up the
    /// population-merge queue under high-cardinality registration (PGC-276).
    /// Deferring to the tick keeps Ready handling off the cache DB; eviction is
    /// best-effort and the reserve headroom absorbs up to one tick of growth.
    pub(super) fn query_ready_finalize(&self, core: &mut WriterCore, merge: &PopulationMerge) {
        self.query_ready_mark(core, merge);
        core.mv_pinned_bootstrap(merge.fingerprint);
    }

    /// Handle a limit bump: re-populate with a higher limit.
    ///
    /// Bumps the generation number, updates max_limit, and re-populates.
    /// During re-population the query state goes to Loading.
    #[instrument(skip_all)]
    pub(super) async fn limit_bump_handle(
        &mut self,
        core: &mut WriterCore,
        fingerprint: Fingerprint,
        new_max_limit: Option<u64>,
    ) -> CacheResult<()> {
        let Some(cached_query) = core.cache.cached_queries.get1(&fingerprint) else {
            // Gone before the bump ran. No drain needed here: the only path that
            // removes a fingerprint while it could hold parked waiters
            // (cache_query_cdc_invalidate / cache_query_evict) already drained
            // them via waiters_fail before removal. Relied-upon invariant — keep
            // it true if new removal paths are added.
            trace!("limit bump: query {fingerprint} not found, skipping");
            return Ok(());
        };

        // A larger max_limit means the existing MV (sized for the old max_limit)
        // is short of rows. Flip Fresh → Dirty before any other mutation so
        // dispatches fall through while the new population runs.
        core.mv_dirty_mark(fingerprint);

        // Collect data needed before mutating
        let resolved = Arc::clone(&cached_query.resolved);
        let deparsed_sql = cached_query.deparsed_sql.clone();
        let relation_oids = cached_query.relation_oids.clone();
        let old_generation = cached_query.generation;

        let new_generation = core.cache.generation_allocate();
        core.cache.generations.remove(&old_generation);

        // Update cached query — must remove and reinsert because generation is key2
        if let Some(mut cached) = core.cache.cached_queries.remove1(&fingerprint) {
            cached.generation = new_generation;
            cached.max_limit = new_max_limit;
            cached.registration_started_at = Some(Instant::now());
            core.cache.cached_queries.insert_overwrite(cached);
        }

        subsumption_limit_reindex(core, fingerprint, &relation_oids, new_max_limit.is_some());

        core.state_loading_transition(
            fingerprint,
            new_generation,
            &resolved,
            &deparsed_sql,
            new_max_limit,
        );

        self.population_start(
            core,
            PopulationTarget {
                fingerprint,
                generation: new_generation,
                resolved: &resolved,
                max_limit: new_max_limit,
            },
        )?;
        trace!("limit bump population queued for query {fingerprint}");
        Ok(())
    }
}
