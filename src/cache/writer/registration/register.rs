//! Registration phases 2 and 3: subsumption (serve from a broader cached
//! query's rows) or admission (insert the entry and populate from origin).

use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Instant;

use tracing::{instrument, trace};

use super::{
    PopulationTarget, QueryResolution, RegistrationIdentity, WriterRegistration,
    cached_query_insert,
};
use crate::cache::CacheResult;
use crate::cache::messages::{AdmitAction, RegisterRequest, SubsumptionResult};
use crate::cache::types::{QueryMetrics, SharedResolved};
use crate::cache::writer::core::WriterCore;
use crate::query::Fingerprint;
use crate::timing::duration_to_ns_u64;

/// What the subsumed path falls back to populating if stamping the subsumed
/// rows fails on the cache DB.
struct SubsumptionFallback {
    fingerprint: Fingerprint,
    resolved: SharedResolved,
    max_limit: Option<u64>,
}

impl WriterRegistration {
    /// Registers a query in the cache. Checks subsumption first — if the data
    /// is already cached by a broader query, stamps rows and marks Ready immediately.
    /// Otherwise, dispatches population (if `admit_action` is `Admit`).
    ///
    /// If the query was previously invalidated (CLOCK policy), takes the fast
    /// readmission path that reuses existing metadata.
    #[instrument(skip_all)]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) async fn query_register(
        &mut self,
        core: &mut WriterCore,
        request: RegisterRequest,
    ) -> CacheResult<()> {
        let identity = RegistrationIdentity {
            fingerprint: request.fingerprint,
            started_at: request.started_at,
            pinned: request.pinned,
        };
        let fingerprint = identity.fingerprint;

        // Fast readmit path for invalidated queries — skip subsumption
        if core
            .cache
            .cached_queries
            .get1(&fingerprint)
            .is_some_and(|query| query.invalidated)
        {
            let _ = request.subsumption_tx.send(SubsumptionResult::NotSubsumed);
            return self
                .query_readmit(core, fingerprint, identity.started_at)
                .await;
        }

        // Phase 1: Resolve
        let resolve_start = Instant::now();
        let resolution = self.query_resolve(core, &request).await?;
        crate::metrics::handles()
            .reg
            .register_resolve
            .record(resolve_start.elapsed().as_secs_f64());

        // Classify shape for MV eligibility. Sticky — readmit/limit-bump
        // preserve the result through state_view_write. Classification was
        // done in `query_resolve`; reuse here.
        core.mv_state_set(fingerprint, resolution.shape_gate, resolution.mv_limit);

        // Phase 2: Subsumption check
        let subsumption_start = Instant::now();
        let subsumed = self.subsumption_check(core, &resolution);
        crate::metrics::handles()
            .reg
            .register_subsumption_check
            .record(subsumption_start.elapsed().as_secs_f64());

        if subsumed {
            // Phase 3a: Subsume — stamp rows, mark Ready
            let fallback = SubsumptionFallback {
                fingerprint,
                resolved: Arc::clone(&resolution.resolved),
                max_limit: resolution.max_limit,
            };
            let result = self.query_subsume_timed(core, identity, resolution).await?;
            let fell_back = matches!(result, SubsumptionResult::NotSubsumed);
            let _ = request.subsumption_tx.send(result);
            if fell_back {
                self.subsumption_fallback_populate(core, &fallback)?;
            }
            return Ok(());
        }

        // Phase 3b: Not subsumed
        let _ = request.subsumption_tx.send(SubsumptionResult::NotSubsumed);

        if request.admit_action == AdmitAction::CheckOnly {
            // Pending below threshold — don't register, don't populate.
            // Clean up the update_queries we registered in query_resolve.
            core.cache
                .update_queries_remove_fingerprint(fingerprint, &resolution.relation_oids);
            return Ok(());
        }

        self.query_admit(core, identity, resolution).await
    }

    /// Stamp the subsumed rows and mark Ready, as the reply for the dispatch.
    /// `NotSubsumed` means stamping failed on the cache DB and the caller
    /// falls back to population.
    async fn query_subsume_timed(
        &self,
        core: &mut WriterCore,
        identity: RegistrationIdentity,
        resolution: QueryResolution,
    ) -> CacheResult<SubsumptionResult> {
        let subsume_start = Instant::now();
        let subsume_result = self.query_subsume(core, identity, resolution).await?;
        crate::metrics::handles()
            .reg
            .register_subsume
            .record(subsume_start.elapsed().as_secs_f64());
        Ok(match subsume_result {
            Some((generation, resolved, deparsed_sql)) => SubsumptionResult::Subsumed {
                generation,
                resolved,
                deparsed_sql,
            },
            None => SubsumptionResult::NotSubsumed,
        })
    }

    /// Cache DB execution failed during subsumption. `query_subsume` already
    /// inserted the entry, so just dispatch population for it.
    fn subsumption_fallback_populate(
        &mut self,
        core: &mut WriterCore,
        fallback: &SubsumptionFallback,
    ) -> CacheResult<()> {
        let fingerprint = fallback.fingerprint;
        let generation = core
            .cache
            .cached_queries
            .get1(&fingerprint)
            .map(|q| q.generation);
        if let Some(generation) = generation {
            self.population_start(
                core,
                PopulationTarget {
                    fingerprint,
                    generation,
                    resolved: &fallback.resolved,
                    max_limit: fallback.max_limit,
                },
            )?;
            trace!("subsumption fallback: population queued {fingerprint}");
        }
        Ok(())
    }

    /// Admit a non-subsumed query: insert the entry, sync the publication if a
    /// relation became active, and dispatch its population.
    async fn query_admit(
        &mut self,
        core: &mut WriterCore,
        identity: RegistrationIdentity,
        resolution: QueryResolution,
    ) -> CacheResult<()> {
        let fingerprint = identity.fingerprint;
        let resolved = Arc::clone(&resolution.resolved);
        let max_limit = resolution.max_limit;

        let insert_start = Instant::now();
        let (generation, relations_changed) = cached_query_insert(core, identity, resolution);
        let now = NonZeroU64::new(duration_to_ns_u64(core.state_view.started_at.elapsed()));
        core.state_view
            .metrics
            .entry(fingerprint)
            .or_insert_with(|| QueryMetrics::new(now, &core.state_view.latency_template));
        crate::metrics::handles()
            .reg
            .register_insert
            .record(insert_start.elapsed().as_secs_f64());

        if relations_changed {
            let pub_start = Instant::now();
            core.publication_update().await?;
            crate::metrics::handles()
                .reg
                .register_publication_update
                .record(pub_start.elapsed().as_secs_f64());
        }

        let dispatch_start = Instant::now();
        self.population_start(
            core,
            PopulationTarget {
                fingerprint,
                generation,
                resolved: &resolved,
                max_limit,
            },
        )?;
        crate::metrics::handles()
            .reg
            .register_populate_dispatch
            .record(dispatch_start.elapsed().as_secs_f64());
        trace!("population work queued for query {fingerprint}");
        Ok(())
    }
}
