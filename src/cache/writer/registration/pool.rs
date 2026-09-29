//! Population workers: elastic scale-up toward the controller's target, and
//! building / dispatching population work onto the shared queue.

use std::collections::HashSet;
use std::time::Instant;

use tokio::task::spawn_local;
use tracing::error;

use super::{PopulationTarget, PopulationWork, WriterRegistration};
use crate::cache::CacheResult;
use crate::cache::types::SharedResolved;
use crate::cache::writer::core::WriterCore;
use crate::cache::writer::population::{
    POPULATION_SPAWN_COOLDOWN, population_worker_connect, population_worker_run,
};
use crate::catalog::TableMetadata;
use crate::oid::Oid;
use crate::query::ast::AstNode;
use crate::query::decorrelate::query_expr_decorrelate;
use crate::query::resolved::{ResolvedQueryExpr, ResolvedSelectNode, ResolvedTableNode};
use crate::result::error_chain_format;

impl WriterRegistration {
    /// Reconcile the live population worker set toward the controller's
    /// target (PGC-437). Called from the writer's 1s gauge tick; scale-down is
    /// claimed by surplus workers themselves between work items, so only
    /// scale-up needs action here.
    pub(crate) fn population_pool_reconcile(&self) {
        if !self.spawn_cooldown_passed() {
            return;
        }
        let pool = &self.spawn_ctx.pool;
        while pool.live_workers() + pool.unpark_pending() < pool.desired_workers() {
            // A parked worker covers one deficit unit without a reconnect.
            if pool.unpark_request() {
                continue;
            }
            self.population_worker_spawn();
        }
        // Live-worker gauge; counts never approach 2^53.
        #[allow(clippy::cast_precision_loss)]
        crate::metrics::handles()
            .reg
            .population_workers
            .set(pool.live_workers() as f64);
    }

    /// Arm the spawn cooldown after a connect failure; false while it runs.
    fn spawn_cooldown_passed(&self) -> bool {
        if self.spawn_ctx.pool.spawn_failure_take() {
            self.spawn_cooldown_until
                .set(Some(Instant::now() + POPULATION_SPAWN_COOLDOWN));
        }
        match self.spawn_cooldown_until.get() {
            Some(until) if Instant::now() < until => false,
            Some(_) => {
                self.spawn_cooldown_until.set(None);
                true
            }
            None => true,
        }
    }

    /// Reserve a worker slot and connect it in the background; a failed
    /// connect marks the failure (arming the cooldown) and releases the slot.
    fn population_worker_spawn(&self) {
        let id = self.spawn_ctx.pool.worker_reserve();
        let ctx = self.spawn_ctx.clone();
        spawn_local(async move {
            match population_worker_connect(&ctx, id).await {
                Ok(connections) => population_worker_run(ctx, id, connections).await,
                Err(e) => {
                    error!(
                        "population worker {id} scale-up connect failed: {}",
                        error_chain_format(e.current_context()),
                    );
                    ctx.pool.spawn_failure_mark();
                    ctx.pool.pending_connect_done();
                    ctx.pool.worker_exit(id);
                }
            }
        });
    }

    /// Build and dispatch the population work for `target`.
    pub(super) fn population_start(
        &mut self,
        core: &mut WriterCore,
        target: PopulationTarget<'_>,
    ) -> CacheResult<()> {
        let work = self.population_work_build(core, target);
        self.populate_work_dispatch(core, work)
    }

    /// Build population work for a query, handling decorrelation and branch extraction.
    ///
    /// Decorrelates the resolved AST so correlated subqueries are merged into JOINs,
    /// then extracts SELECT branches, collects table metadata, and builds PopulationWork.
    fn population_work_build(
        &self,
        core: &WriterCore,
        target: PopulationTarget<'_>,
    ) -> PopulationWork {
        let resolved: &SharedResolved = target.resolved;
        let population_resolved = query_expr_decorrelate(resolved, &self.aggregate_functions)
            .map(|d| {
                if d.transformed {
                    d.resolved
                } else {
                    ResolvedQueryExpr::clone(resolved)
                }
            })
            .unwrap_or_else(|_| ResolvedQueryExpr::clone(resolved));

        let branches: Vec<ResolvedSelectNode> = population_resolved
            .select_nodes()
            .into_iter()
            .cloned()
            .collect();

        let branch_relation_oids: Vec<Oid> = branches
            .iter()
            .flat_map(|branch: &ResolvedSelectNode| branch.nodes::<ResolvedTableNode>())
            .map(|tn| tn.relation_oid)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        let table_metadata: Vec<TableMetadata> = branch_relation_oids
            .iter()
            .filter_map(|oid| core.cache.tables.get1(oid).cloned())
            .collect();

        PopulationWork {
            fingerprint: target.fingerprint,
            generation: target.generation,
            table_metadata,
            branches,
            max_limit: target.max_limit,
            // Filled in at dispatch from the staging pool (needs `&mut core`).
            staging: Vec::new(),
            enqueued_at: Instant::now(),
        }
    }

    /// Enqueue population work on the shared queue; the dispatcher hands it to
    /// the next idle worker.
    fn populate_work_dispatch(
        &mut self,
        core: &mut WriterCore,
        mut work: PopulationWork,
    ) -> CacheResult<()> {
        let fingerprint = work.fingerprint;
        let generation = work.generation;
        // Begin recording CDC deletes for this population's relations *before*
        // the worker reads its snapshot (PGC-250). Released at merge or on
        // failure.
        let relation_oids: Vec<Oid> = work.table_metadata.iter().map(|t| t.relation_oid).collect();
        // Anchor floor: a lower bound on this population's snapshot LSN, used to
        // prune deleted keys it can no longer need (PGC-250).
        let anchor_floor = core.last_received_lsn;
        core.population_deleted_keys.activate(
            fingerprint,
            generation,
            &relation_oids,
            anchor_floor,
        );
        // Check out a reusable staging table per relation (PGC-293); the writer
        // returns them to the pool at merge / failure.
        work.staging = core
            .staging_pool
            .checkout(fingerprint, generation, &relation_oids);

        if self.populate_tx.send(work).is_err() {
            error!("population dispatcher channel closed");
            core.population_deleted_keys
                .deactivate(fingerprint, generation);
            core.staging_pool.forget(fingerprint, generation);
        }

        Ok(())
    }
}
