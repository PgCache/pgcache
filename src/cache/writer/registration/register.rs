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
use crate::cache::admission::SubsumptionCover;
use crate::cache::messages::{AdmitAction, RegisterRequest, SubsumptionResult};
use crate::cache::types::{QueryMetrics, SharedResolvedQuery};
use crate::cache::writer::core::WriterCore;
use crate::query::Fingerprint;
use crate::timing::duration_to_ns_u64;

/// What the subsumed path falls back to populating if stamping the subsumed
/// rows fails on the cache DB.
struct SubsumptionFallback {
    fingerprint: Fingerprint,
    population: SharedResolvedQuery,
    max_limit: Option<u64>,
}

impl WriterRegistration {
    /// Registers a query in the cache. Checks subsumption on the bound query
    /// first — if the data is already cached by a broader query, stamps rows
    /// and marks Ready immediately. Otherwise, dispatches population (if
    /// `admit_action` is `Admit`); a `CheckOnly` miss ends at the check.
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

        // Fast readmit path for invalidated queries: reuses the entry's
        // metadata and runs its own subsumption check.
        if core
            .cache
            .cached_queries
            .get1(&fingerprint)
            .is_some_and(|query| query.invalidated)
        {
            let result = self
                .query_readmit(core, fingerprint, identity.started_at)
                .await?;
            let _ = request.subsumption_tx.send(result);
            return Ok(());
        }

        // Phase 1: Bind. The subsumption check reads only the bound query;
        // the rest of the resolution is built once an entry will be created.
        let bind_start = Instant::now();
        let query = self.query_bind(core, &request).await?;
        crate::metrics::handles()
            .reg
            .register_bind
            .record(bind_start.elapsed().as_secs_f64());

        // Phase 2: Subsumption check
        let subsumption_start = Instant::now();
        let cover = self.subsumption_check(core, &query.bound, query.join_region.as_ref());
        crate::metrics::handles()
            .reg
            .register_subsumption_check
            .record(subsumption_start.elapsed().as_secs_f64());

        if cover.is_none() && request.admit_action == AdmitAction::CheckOnly {
            // Pending below threshold: no entry, nothing more to build.
            let _ = request.subsumption_tx.send(SubsumptionResult::NotSubsumed);
            return Ok(());
        }

        let resolve_start = Instant::now();
        let resolution = self.query_resolve(core, fingerprint, query)?;
        crate::metrics::handles()
            .reg
            .register_resolve
            .record(resolve_start.elapsed().as_secs_f64());

        // The registering role's verdicts, before Ready: its first hit then
        // serves instead of forwarding while a lookup runs (PGC-463).
        if let Some(role) = &request.effective_role {
            core.privilege_prefetch(role, &resolution.read_facts.privilege_needs)
                .await;
        }

        // Classify shape for MV eligibility. Sticky — readmit/limit-bump
        // preserve the result through state_view_write. Classification was
        // done in `query_resolve`; reuse here.
        core.mv_state_set(fingerprint, resolution.shape_gate, resolution.mv_limit);

        if let Some(cover) = cover {
            // Phase 3a: Subsume — stamp rows, mark Ready
            let fallback = SubsumptionFallback {
                fingerprint,
                population: Arc::clone(&resolution.population),
                max_limit: resolution.max_limit,
            };
            let result = self
                .query_subsume_timed(core, identity, resolution, cover)
                .await?;
            let fell_back = matches!(result, SubsumptionResult::NotSubsumed);
            let _ = request.subsumption_tx.send(result);
            if fell_back {
                self.subsumption_fallback_populate(core, &fallback)?;
            }
            return Ok(());
        }

        // Phase 3b: Admit
        let _ = request.subsumption_tx.send(SubsumptionResult::NotSubsumed);
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
        cover: SubsumptionCover,
    ) -> CacheResult<SubsumptionResult> {
        let subsume_start = Instant::now();
        let result = self
            .query_subsume(core, identity, resolution, cover)
            .await?;
        crate::metrics::handles()
            .reg
            .register_subsume
            .record(subsume_start.elapsed().as_secs_f64());
        Ok(result)
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
                    population: &fallback.population,
                    max_limit: fallback.max_limit,
                },
            )?;
            trace!("subsumption fallback: population queued {fingerprint}");
        }
        Ok(())
    }

    /// Admit a non-subsumed query: insert the entry, publish any relation that
    /// became active, and dispatch its population.
    async fn query_admit(
        &mut self,
        core: &mut WriterCore,
        identity: RegistrationIdentity,
        resolution: QueryResolution,
    ) -> CacheResult<()> {
        let fingerprint = identity.fingerprint;
        let population = Arc::clone(&resolution.population);
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
            core.publication_relations_add().await?;
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
                population: &population,
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
