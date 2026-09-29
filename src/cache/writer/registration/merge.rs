//! Population merge drain (PGC-250, PGC-272, PGC-418): release queued merges
//! in watermark-deadline order, advance the active merge or discard chunk by
//! chunk, and finalize the query on a terminal outcome.

use std::cmp::Reverse;
use std::time::Instant;

use tracing::{debug, error};

use super::WriterRegistration;
use crate::cache::writer::core::WriterCore;
use crate::cache::writer::merge_queue::{
    DrainTarget, HeapStop, MERGE_FLUSH_FORCE_AFTER, MergeStep, PendingMerge,
};
use crate::cache::{CacheError, CacheResult, Report};
use crate::pg::Lsn;
use crate::query::Fingerprint;
use crate::result::error_chain_format;

/// Walk the pending-merge heap: reap tombstones, stop at the first entry the
/// watermark or the busy slot holds back, and start at most one releasable
/// merge. Returns why the walk stopped and whether a merge started.
fn merge_heap_advance(core: &mut WriterCore, applied_lsn: Lsn) -> (HeapStop, bool) {
    let mut started_any = false;
    let stop = loop {
        // Copy the head's fields out so the `peek` borrow ends before the
        // body re-borrows `core` mutably (pop / merge / staging).
        let Some((fingerprint, generation, snapshot_lsn)) = core
            .merges
            .pending
            .peek()
            .map(|Reverse(top)| (top.0.fingerprint, top.0.generation, top.0.snapshot_lsn))
        else {
            break HeapStop::Exhausted;
        };
        // Tombstone check before the deadline check: a superseded /
        // invalidated / evicted entry is droppable regardless of the
        // watermark — release its tracking and staging now rather than
        // holding them until a deadline that no longer matters. (Stale
        // entries buried below a live top are reaped lazily when they
        // surface.) The successor population has its own entry.
        if !core.population_is_current(fingerprint, generation) {
            if core.merges.pending.pop().is_none() {
                break HeapStop::Exhausted;
            }
            core.population_deleted_keys
                .deactivate(fingerprint, generation);
            // Its staging (a complete population) is emptied in chunks, not
            // with one DELETE on the writer.
            core.population_discard_enqueue(fingerprint, generation);
            continue;
        }

        // Earliest live deadline not reached: nothing below it can be
        // releasable either.
        if snapshot_lsn > applied_lsn {
            break HeapStop::HeadGated(snapshot_lsn);
        }

        // One merge at a time: the released head starts now and is advanced
        // chunk by chunk from the writer loop (PGC-418); the next releasable
        // entry starts once it finishes.
        if core.merges.active.is_some() {
            break HeapStop::SlotBusy;
        }
        let Some(Reverse(PendingMerge(merge))) = core.merges.pending.pop() else {
            break HeapStop::Exhausted;
        };
        started_any = true;
        crate::metrics::handles()
            .reg
            .merge_wait
            .record(merge.enqueued_at.elapsed().as_secs_f64());
        core.population_merge_start(merge);
    };
    (stop, started_any)
}

/// React to why the heap walk stopped.
async fn merge_stall_handle(core: &mut WriterCore, stop: HeapStop) -> CacheResult<()> {
    match stop {
        // Head gated on the watermark. Nudge for an immediate keepalive, and
        // once it has been stuck past the grace window, force an origin WAL
        // flush so its snapshot LSN becomes reachable (PGC-290).
        // `flush_marker_lsn` suppresses re-emits once a marker already covers
        // the gated backlog.
        HeapStop::HeadGated(snapshot_lsn) => {
            core.merges.watermark_nudge.notify_one();
            let stalled_since = *core.merges.stall_since.get_or_insert_with(Instant::now);
            if stalled_since.elapsed() >= MERGE_FLUSH_FORCE_AFTER
                && snapshot_lsn > core.merges.flush_marker_lsn
            {
                core.merges.flush_marker_lsn = core.origin_flush_force().await?;
            }
        }
        // Nothing is stalled on the watermark: a head waiting for the slot is
        // released as soon as the active drain finishes.
        HeapStop::Exhausted | HeapStop::SlotBusy => core.merges.stall_since = None,
    }
    Ok(())
}

/// The active drain's identity, copied out before stepping it.
#[derive(Clone, Copy)]
struct MergeHead {
    fingerprint: Fingerprint,
    generation: u64,
    /// Applying a population (vs. discarding staging).
    applying: bool,
}

impl WriterRegistration {
    /// Drain queued population merges in watermark-deadline order (PGC-250,
    /// PGC-272). Called from the writer loop only when no CDC frame is open,
    /// so a merge never races the CDC frame txn on the shared cache table —
    /// on every such iteration, including while a drain is active: tombstones
    /// are reaped and a gated head is nudged regardless of the slot, only the
    /// start of the next releasable merge waits for it.
    ///
    /// Each merge is additionally gated on the apply watermark reaching its
    /// `snapshot_lsn`: snapshot-state rows must not enter the shared table
    /// before CDC has applied past the snapshot, or already-Ready bystander
    /// queries over the relation would serve a torn mix of two origin points
    /// in time (PGC-272). The heap is a min-heap on that deadline, so one
    /// peek decides whether anything is releasable.
    ///
    /// A successful merge marks the query Ready inline: the watermark is
    /// already at/past the snapshot when the gate releases, so the old
    /// deferred-Ready parking (PGC-250 Slice B) would be a no-op double-gate.
    pub(crate) async fn pending_merges_drain(
        &self,
        core: &mut WriterCore,
        applied_lsn: Lsn,
    ) -> CacheResult<()> {
        let (stop, started_any) = merge_heap_advance(core, applied_lsn);
        // Real merges take the slot first; a queued discard runs when none is
        // releasable, so pool tables come back without ever stalling apply.
        if core.merges.active.is_none()
            && let Some(discard) = core.merges.discards.pop_front()
        {
            core.population_discard_start(discard);
        }
        // A released merge means the watermark is advancing on its own; restart
        // the stall clock so the grace window times the *current* gated head.
        if started_any {
            core.merges.stall_since = None;
        }
        merge_stall_handle(core, stop).await
    }

    /// Run one chunk of the in-progress population merge or discard and
    /// finalize on a terminal outcome (PGC-418). Query-side finalization
    /// happens the moment applying stops — `Done` marks the query Ready,
    /// `Aborted` / error fail it, a superseded / invalidated / evicted query
    /// is left to its successor — but the staging tables are only checked
    /// back in once their rows have been drained in chunks
    /// (`DrainTarget::Discard`), never with one DELETE over the remainder.
    pub(crate) async fn merge_in_progress_step(&self, core: &mut WriterCore) -> CacheResult<()> {
        let Some(head) = core.merges.active.as_ref().map(|m| MergeHead {
            fingerprint: m.fingerprint,
            generation: m.generation,
            applying: m.is_applying(),
        }) else {
            return Ok(());
        };
        if head.applying && !core.population_is_current(head.fingerprint, head.generation) {
            core.population_deleted_keys
                .deactivate(head.fingerprint, head.generation);
            core.population_merge_discard_remaining();
            return Ok(());
        }
        match core.population_merge_step().await {
            Ok(MergeStep::Continue) => {}
            Ok(MergeStep::Done) => self.merge_done(core, head).await,
            Ok(MergeStep::Aborted) => {
                debug!(
                    "population merge aborted (overflow / truncate) {}",
                    head.fingerprint
                );
                self.merge_apply_abandon(core, head);
            }
            Err(e) => self.merge_step_failed(core, head, &e).await,
        }
        Ok(())
    }

    /// The drain finished: check the staging tables back in (PGC-293), then
    /// mark the query Ready if this was an applying merge.
    async fn merge_done(&self, core: &mut WriterCore, head: MergeHead) {
        let Some(finished) = core.merges.active.take() else {
            return;
        };
        core.staging_checkin(head.fingerprint, head.generation)
            .await;
        match finished.target {
            DrainTarget::Apply(applied) => {
                debug!(
                    "population merge applied in {} chunks / {} rows over {:?} {}",
                    finished.chunks,
                    finished.drained_rows,
                    finished.started_at.elapsed(),
                    head.fingerprint
                );
                core.population_deleted_keys
                    .deactivate(head.fingerprint, head.generation);
                crate::metrics::handles().reg.merges_applied.increment(1);
                self.query_ready_finalize(core, &applied);
            }
            DrainTarget::Discard => {
                debug!(
                    "population staging discarded in {} chunks / {} rows {}",
                    finished.chunks, finished.drained_rows, head.fingerprint
                );
            }
        }
    }

    async fn merge_step_failed(
        &self,
        core: &mut WriterCore,
        head: MergeHead,
        e: &Report<CacheError>,
    ) {
        error!(
            "population merge chunk failed for {}: {}",
            head.fingerprint,
            error_chain_format(e.current_context()),
        );
        if head.applying {
            self.merge_apply_abandon(core, head);
        } else {
            // The discard itself failed: fall back to the one-statement
            // check-in rather than retry forever.
            core.merges.active = None;
            core.staging_checkin(head.fingerprint, head.generation)
                .await;
        }
    }

    /// Fail the query of an applying merge that can't finish, and drain its
    /// remaining staging in chunks.
    fn merge_apply_abandon(&self, core: &mut WriterCore, head: MergeHead) {
        core.population_deleted_keys
            .deactivate(head.fingerprint, head.generation);
        self.query_failed_cleanup(core, head.fingerprint);
        core.population_merge_discard_remaining();
    }
}
