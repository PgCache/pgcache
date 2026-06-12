//! Writer-side MV operations: build dispatch and completion, dirty-marking,
//! eviction helpers.
//!
//! First-pop and rebuild share a single state-machine — the `has_table` bit
//! carried by `Pending` / `Scheduled` / `Building` tells the build which SQL
//! variant to run (`CREATE UNLOGGED TABLE AS` for first-pop,
//! `BEGIN; TRUNCATE; INSERT; COMMIT;` for rebuild) and whether a Measure gate
//! is still owed (only before the first successful build).
//!
//! Build SQL runs off-thread (`mv_build.rs`) so a backlog of builds never
//! blocks CDC apply, but all `MvState` transitions stay on the writer: the
//! `Fresh` flip is serialized against CDC dirty-marking, so a build raced by
//! a relevant change is always observed as `BuildingDirty` at completion and
//! discarded (the data a build reads is snapshot-consistent either way; the
//! race is only about whether the table may claim to be current).

use std::sync::Arc;

use tracing::{debug, error, trace};

use crate::result::error_chain_format;

use super::super::{
    CacheError, CacheResult, MapIntoReport, ReportExt,
    messages::{MvBuildOutcome, QueryCommand},
    mv::{MvState, ShapeGate, mv_table_name},
    types::CachedQueryState,
};
use super::core::WriterCore;
use super::mv_build::{MvBuildContext, mv_build_spawn};

impl WriterCore {
    /// Pinned queries bypass the dispatch-driven "first hit triggers MV
    /// build" flow so they stay warm across startup and readmits. Called from
    /// the Ready handler. Performs the same check-and-transition the
    /// dispatch would do on first hit and self-sends an `MvBuild` command.
    pub(super) fn mv_pinned_bootstrap(&self, fingerprint: u64) {
        let is_pinned = self
            .cache
            .cached_queries
            .get1(&fingerprint)
            .is_some_and(|q| q.pinned);
        if !is_pinned {
            return;
        }
        let Some(mut view) = self.state_view.cached_queries.get_mut(&fingerprint) else {
            return;
        };
        let MvState::Pending { has_table } = view.mv.state else {
            return;
        };
        view.mv.state = MvState::Scheduled { has_table };
        drop(view);
        let _ = self.query_tx.send(QueryCommand::MvBuild { fingerprint });
    }

    /// Handle `QueryCommand::MvBuild`. Precondition: `mv_state ==
    /// Scheduled { .. }` (set by the dispatch or pinned bootstrap before
    /// the command was enqueued) and source-row state `Ready`.
    ///
    /// Snapshot the build context, flip `Scheduled → Building`, and spawn the
    /// SQL onto the shared runtime. The build reports back via
    /// `MvBuildComplete`; `mv_build_complete` applies the terminal transition.
    pub(super) fn mv_build_dispatch(&self, fingerprint: u64) {
        let Some(ctx) = self.mv_context_snapshot(fingerprint) else {
            trace!("mv build: precondition not met for {fingerprint}");
            crate::metrics::handles().mv.skipped_rebuilds.increment(1);
            return;
        };
        self.mv_state_transition(
            fingerprint,
            MvState::Building {
                has_table: ctx.has_table,
            },
        );
        mv_build_spawn(
            &self.runtime,
            &self.mv_build_pool,
            ctx,
            self.query_tx.clone(),
        );
    }

    /// Handle `QueryCommand::MvBuildComplete`: apply the state transition for
    /// a finished build task. Running here (not in the task) serializes the
    /// `Fresh` flip against CDC dirty-marking — a build raced by a relevant
    /// change always lands in `BuildingDirty` first and is discarded.
    pub(super) async fn mv_build_complete(&self, fingerprint: u64, outcome: MvBuildOutcome) {
        let state = self
            .state_view
            .cached_queries
            .get(&fingerprint)
            .map(|v| v.mv.state);

        let Some(state) = state else {
            // Evicted during the build. Eviction skipped the MV drop (the
            // build held locks on the table); drop the orphan now that the
            // build has finished and released them.
            if matches!(outcome, MvBuildOutcome::Built { .. }) {
                self.mv_table_drop(fingerprint).await;
            }
            return;
        };

        match outcome {
            MvBuildOutcome::Built {
                output_columns,
                was_first_build,
            } => {
                // State re-check only matters for first-pop: CREATE TABLE AS is
                // atomic but not transactional with any follow-up — if source-row
                // state flipped during the async SQL, drop the (now stale) table
                // and retry. Rebuild is fully wrapped in BEGIN/COMMIT and takes
                // the same snapshot-consistent "overlapping read" accommodation
                // as the existing design.
                if was_first_build && !self.source_row_state_is_ready(fingerprint) {
                    debug!(
                        "mv build: source-row state changed during first build for {fingerprint}, \
                         dropping and resetting"
                    );
                    self.mv_table_drop(fingerprint).await;
                    self.mv_state_transition(fingerprint, MvState::Pending { has_table: false });
                    return;
                }

                match state {
                    MvState::Building { .. } => {
                        if let Some(mut view) = self.state_view.cached_queries.get_mut(&fingerprint)
                        {
                            view.mv.output_columns = Some(output_columns);
                            view.mv.state = MvState::Fresh;
                        }
                        crate::metrics::handles().mv.rebuilds.increment(1);
                        trace!("mv build: fresh for {fingerprint}");
                    }
                    MvState::BuildingDirty { .. } => {
                        // A CDC change relevant to this query landed while the
                        // build was in flight; the table contents predate it.
                        self.mv_state_transition(fingerprint, MvState::Pending { has_table: true });
                        crate::metrics::handles().mv.skipped_rebuilds.increment(1);
                        trace!("mv build: discarded (dirtied in flight) for {fingerprint}");
                    }
                    MvState::Skipped
                    | MvState::Ineligible
                    | MvState::Pending { .. }
                    | MvState::Scheduled { .. }
                    | MvState::Fresh => {
                        // Entry was reset by a re-registration path (readmit /
                        // re-register) while the build ran; the new entry
                        // expects no table.
                        debug!(
                            "mv build: state moved to {state:?} during build for {fingerprint}, \
                             dropping table"
                        );
                        self.mv_table_drop(fingerprint).await;
                    }
                }
            }
            MvBuildOutcome::Ineligible => {
                // Gate verdict is sticky regardless of in-flight dirtying —
                // no table was created (the gate runs before CREATE).
                self.mv_state_transition(fingerprint, MvState::Ineligible);
                trace!("mv build: size gate failed for {fingerprint}");
            }
            MvBuildOutcome::Failed { has_table } => {
                if matches!(
                    state,
                    MvState::Building { .. } | MvState::BuildingDirty { .. }
                ) {
                    self.mv_state_transition(fingerprint, MvState::Pending { has_table });
                }
            }
        }
    }

    /// `DROP TABLE IF EXISTS` for a fingerprint's MV table on the writer's
    /// cache connection, logging failures. Safe to run from completion
    /// handling: the build task has finished, so no build locks are held.
    async fn mv_table_drop(&self, fingerprint: u64) {
        let mv_table = mv_table_name(fingerprint);
        if let Err(e) = self
            .db_cache
            .batch_execute(&format!("DROP TABLE IF EXISTS {mv_table}"))
            .await
            .map_into_report::<CacheError>()
        {
            error!(
                "mv table drop failed for {fingerprint}: {}",
                error_chain_format(e.current_context()),
            );
        }
    }

    /// Snapshot everything a build task needs from the state_view entry and
    /// the writer-only catalog (`core.cache`). Returns None when the entry is
    /// missing, `mv_state` isn't `Scheduled { .. }`, or the source-row state
    /// isn't `Ready` (races resolved at the call site).
    fn mv_context_snapshot(&self, fingerprint: u64) -> Option<MvBuildContext> {
        let view = self.state_view.cached_queries.get(&fingerprint)?;
        let MvState::Scheduled { has_table } = view.mv.state else {
            return None;
        };
        if view.state != CachedQueryState::Ready {
            return None;
        }
        let resolved = view.resolved.as_ref().map(Arc::clone)?;
        let shape_gate = view.mv.shape_gate;
        // MV body uses its own cap (joins only); independent of the
        // source-row population cap `view.max_limit`.
        let max_limit = view.mv.limit;
        let generation = view.generation;
        let output_columns = view.mv.output_columns.as_ref().map(Arc::clone);
        drop(view);

        // Measure-gate denominator tables, resolved from the writer-only
        // catalog here because the task can't read `core.cache`. An empty
        // list makes the gate fail — the safe default.
        let gate_tables = if !has_table && shape_gate == ShapeGate::Measure {
            self.cache
                .cached_queries
                .get1(&fingerprint)
                .map(|q| {
                    q.relation_oids
                        .iter()
                        .filter_map(|oid| {
                            self.cache
                                .tables
                                .get1(oid)
                                .map(|t| (t.schema.clone(), t.name.clone()))
                        })
                        .collect()
                })
                .unwrap_or_default()
        } else {
            Vec::new()
        };

        Some(MvBuildContext {
            fingerprint,
            has_table,
            shape_gate,
            max_limit,
            generation,
            resolved,
            output_columns,
            gate_tables,
            mv_size_ratio: u64::from(self.cache.dynamic.load().mv_size_ratio),
        })
    }

    fn source_row_state_is_ready(&self, fingerprint: u64) -> bool {
        self.state_view
            .cached_queries
            .get(&fingerprint)
            .is_some_and(|v| v.state == CachedQueryState::Ready)
    }

    /// Apply the dirty transition (`MvState::dirtied`) for this fingerprint:
    /// `Fresh → Pending { has_table: true }`, `Building → BuildingDirty`.
    /// No-op for any other state — dirty-marking has no meaningful effect.
    ///
    /// Takes `&self` (mutation is via DashMap interior mutability) so callers
    /// holding `&self` in CDC paths don't need to become `&mut self`.
    pub(super) fn mv_dirty_mark(&self, fingerprint: u64) {
        if let Some(mut view) = self.state_view.cached_queries.get_mut(&fingerprint)
            && let Some(dirtied) = view.mv.state.dirtied()
        {
            view.mv.state = dirtied;
        }
    }

    /// Dirty-mark every dirtyable MV among the relation's update-queries (PGC-254
    /// rung 1). Used on CDC removals (DELETE / UPDATE-out): unlike the upsert
    /// path, we can't evaluate which queries actually contained the removed row
    /// — REPLICA IDENTITY DEFAULT carries only the PK, not the non-PK columns a
    /// predicate needs — so this is relation-level. `mv_dirty_mark` self-gates
    /// (Fresh and Building), and the rebuild is lazy; under delete-heavy load the MVs stay
    /// Pending and the query serves from the (correct) source rows.
    pub(super) fn mv_dirty_mark_relation(&self, relation_oid: u32) {
        if let Some(update_queries) = self.cache.update_queries.get(&relation_oid) {
            for query in update_queries.iter_complexity_ordered() {
                self.mv_dirty_mark(query.fingerprint);
            }
        }
    }

    /// Whether this query's MV state can still be dirtied by a CDC change
    /// (`Fresh` or `Building`). Only these queries need full CDC evaluation
    /// (so `mv_dirty_mark` can fire on a match); other states are
    /// short-circuitable in the membership check.
    pub(super) fn mv_dirty_eval_required(&self, fingerprint: u64) -> bool {
        self.state_view
            .cached_queries
            .get(&fingerprint)
            .is_some_and(|v| v.mv.state.dirtied().is_some())
    }

    /// Eviction pre-sweep: truncate every MV in `Pending { has_table: true }`
    /// so the bytes are reclaimed before we start evicting live cache entries.
    ///
    /// These entries hold rows that will never be served — dead weight until
    /// the next hit rebuilds or eviction drops the table. Reclaiming them first
    /// means size pressure preferentially removes dead weight rather than
    /// evicting cache entries that might still be useful. The table persists
    /// (empty) so the next rebuild's `BEGIN; TRUNCATE; INSERT; COMMIT` still
    /// hits an existing table; state stays `Pending { has_table: true }` so
    /// dispatches keep falling through.
    ///
    /// Does **not** touch `Scheduled { .. }` (a build is queued; truncating
    /// would churn), `Building { .. }` / `BuildingDirty { .. }` (the build
    /// task holds locks on the table — truncating would block the writer), or
    /// `Fresh` (still serving the fast path). Collects fingerprints into a
    /// Vec first so we don't hold a DashMap guard across awaits.
    pub(super) async fn mv_dirty_sweep(&self) -> CacheResult<()> {
        let dirty: Vec<u64> = self
            .state_view
            .cached_queries
            .iter()
            .filter(|entry| entry.mv.state == MvState::Pending { has_table: true })
            .map(|entry| *entry.key())
            .collect();

        if dirty.is_empty() {
            return Ok(());
        }

        for fingerprint in dirty {
            let mv_table = mv_table_name(fingerprint);
            if let Err(e) = self
                .db_cache
                .batch_execute(&format!("TRUNCATE {mv_table}"))
                .await
                .map_into_report::<CacheError>()
                .attach_loc("truncating dirty MV in eviction sweep")
            {
                error!(
                    "mv dirty-sweep truncate failed for {fingerprint}: {}",
                    error_chain_format(e.current_context()),
                );
            } else {
                crate::metrics::handles().mv.dirty_truncates.increment(1);
            }
        }

        Ok(())
    }

    /// Drop the MV table for a fingerprint if its state indicates an on-disk
    /// table exists. Called from `cache_query_evict` before the `CachedQueryView`
    /// entry is removed. The caller is expected to pass the current `mv_state`
    /// read just before the evict (post-read the state_view entry will be gone).
    pub(super) async fn mv_drop(&self, fingerprint: u64, mv_state: MvState) -> CacheResult<()> {
        // An in-flight build task holds locks on the MV table; dropping now
        // would block the writer until the build finishes. Defer to the
        // MvBuildComplete handler, which drops the table when the entry is gone.
        if matches!(
            mv_state,
            MvState::Building { .. } | MvState::BuildingDirty { .. }
        ) {
            return Ok(());
        }
        if !mv_state.has_table() {
            return Ok(());
        }
        let mv_table = mv_table_name(fingerprint);
        self.db_cache
            .batch_execute(&format!("DROP TABLE IF EXISTS {mv_table}"))
            .await
            .map_into_report::<CacheError>()
            .attach_loc("dropping MV table on eviction")?;
        Ok(())
    }

    /// Mutate `mv_state` on the state_view entry. No-op when the entry is gone
    /// (evicted during the build).
    fn mv_state_transition(&self, fingerprint: u64, new_state: MvState) {
        if let Some(mut view) = self.state_view.cached_queries.get_mut(&fingerprint) {
            view.mv.state = new_state;
        }
    }
}
