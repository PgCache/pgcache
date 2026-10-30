//! A registration parked on a populating parent that covers it (PGC-496):
//! parked instead of populating, released when the parent becomes Ready or
//! never will, to a stamp from whatever cover it has then or to its own
//! population. Its requests forward meanwhile, as a cold entry's do.

use std::sync::Arc;

use tracing::{error, trace};

use super::{PopulationTarget, WriterRegistration};
use crate::cache::CacheResult;
use crate::cache::messages::SubsumptionResult;
use crate::cache::types::{QueryServing, ServingAt, SharedResolvedQuery};
use crate::cache::writer::core::WriterCore;
use crate::cache::writer::subsumption::subsumed_ready_mark;
use crate::query::Fingerprint;
use crate::result::error_chain_format;

/// What a parked entry needs to be stamped or populated.
struct WaitingQuery {
    population: SharedResolvedQuery,
    at: ServingAt,
}

impl WriterCore {
    /// Park `fingerprint`'s entry on `parent` until that parent is released.
    pub(super) fn query_park(&mut self, fingerprint: Fingerprint, parent: Fingerprint) {
        if let Some(mut query) = self.cache.cached_queries.get1_mut(fingerprint) {
            query.waiting_on = Some(parent);
        }
        self.cache
            .waiting_children
            .entry(parent)
            .or_default()
            .push(fingerprint);
        crate::metrics::handles().reg.subsumption_waits.increment(1);
        trace!("query {fingerprint} waits on populating parent {parent}");
    }

    /// Take `fingerprint`'s parked entry out of its wait on `parent`, with
    /// what releasing it needs. `None` when it waits on another parent, is
    /// gone, or was invalidated meanwhile: an invalidated entry's next read
    /// readmits it through the same checks.
    fn waiting_query_take(
        &mut self,
        parent: Fingerprint,
        fingerprint: Fingerprint,
    ) -> Option<WaitingQuery> {
        let mut query = self.cache.cached_queries.get1_mut(fingerprint)?;
        if query.waiting_on != Some(parent) {
            return None;
        }
        query.waiting_on = None;
        if query.invalidated {
            return None;
        }
        Some(WaitingQuery {
            population: Arc::clone(&query.population),
            at: ServingAt {
                generation: query.generation,
                serving: Arc::new(QueryServing {
                    bound: Arc::clone(&query.bound),
                    read_facts: Arc::clone(&query.read_facts),
                    serve_shape: Arc::clone(&query.serve_shape),
                    max_limit: query.max_limit,
                    reference_only: query.reference_only,
                }),
            },
        })
    }
}

impl WriterRegistration {
    /// Release every entry parked on `parent`.
    pub(super) async fn waiters_release_command(
        &mut self,
        core: &mut WriterCore,
        parent: Fingerprint,
    ) {
        trace!("command waiters release {parent}");
        let children = core
            .cache
            .waiting_children
            .remove(&parent)
            .unwrap_or_default();
        for child in children {
            if let Err(e) = self.waiter_release(core, parent, child).await {
                error!(
                    "releasing {child} from its wait on {parent} failed: {}",
                    error_chain_format(e.current_context()),
                );
                self.query_failed_cleanup(core, child);
            }
        }
    }

    /// Stamp `child` from a Ready cover, park it again on another populating
    /// parent, or populate it.
    async fn waiter_release(
        &mut self,
        core: &mut WriterCore,
        parent: Fingerprint,
        child: Fingerprint,
    ) -> CacheResult<()> {
        let Some(waiting) = core.waiting_query_take(parent, child) else {
            return Ok(());
        };
        let bound = Arc::clone(&waiting.at.serving.bound);
        let join = bound.predicates().join_region();
        match self.subsumption_check(core, child, &bound, join) {
            Some(cover) => {
                let generation = waiting.at.generation;
                let max_limit = waiting.at.serving.max_limit;
                if let SubsumptionResult::Subsumed { .. } =
                    subsumed_ready_mark(core, child, waiting.at, cover).await
                {
                    trace!("query {child} stamped after waiting on {parent}");
                    return Ok(());
                }
                self.waiter_populate(core, child, generation, &waiting.population, max_limit)
            }
            None => {
                if let Some(next) = self.subsumption_pending_parent(core, child, &bound, join) {
                    core.query_park(child, next);
                    return Ok(());
                }
                let generation = waiting.at.generation;
                let max_limit = waiting.at.serving.max_limit;
                self.waiter_populate(core, child, generation, &waiting.population, max_limit)
            }
        }
    }

    fn waiter_populate(
        &mut self,
        core: &mut WriterCore,
        child: Fingerprint,
        generation: crate::cache::Generation,
        population: &SharedResolvedQuery,
        max_limit: Option<u64>,
    ) -> CacheResult<()> {
        crate::metrics::handles()
            .reg
            .subsumption_wait_fallbacks
            .increment(1);
        self.population_start(
            core,
            PopulationTarget {
                fingerprint: child,
                generation,
                population,
                max_limit,
            },
        )?;
        trace!("query {child} populates after its wait");
        Ok(())
    }
}
