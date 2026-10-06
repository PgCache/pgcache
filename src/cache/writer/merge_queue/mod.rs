//! The writer's merge pipeline (PGC-272 / PGC-418): staged populations
//! awaiting the drain gate, staging discards awaiting the slot, the one drain
//! in progress, and the gate's bookkeeping.
//!
//! A population's staging→cache merge is withheld until the writer is
//! quiescent (no CDC frame open) and the apply watermark has reached the
//! population's snapshot LSN (ADR-035). It is then drained in bounded chunks
//! — heap-page windows of the staging table — one chunk per writer-loop
//! iteration, so CDC apply interleaves with the merge instead of waiting
//! behind one statement over the whole table. Staging that will never be
//! merged is emptied the same way (`DrainTarget::Discard`) rather than with
//! one `DELETE` per table on the writer.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ecow::EcoString;
use tokio::sync::Notify;

use super::staging::PopulationDeletedKeys;
use crate::cache::Generation;
use crate::cache::messages::PopulationMerge;
use crate::oid::Oid;
use crate::pg::Lsn;
use crate::query::Fingerprint;

mod drain;
mod sql;
mod step;

/// How long a population merge may stay gated on the apply watermark before the
/// writer forces an origin WAL flush (`origin_flush_force`) to make its snapshot
/// LSN reachable (PGC-290). Above the nudge→keepalive round-trip so a healthy
/// active origin, where the flush pointer catches up on its own, never triggers
/// a marker; the cost is at most this much extra ready-latency on a stalled
/// (idle / async-commit) origin.
pub(super) const MERGE_FLUSH_FORCE_AFTER: Duration = Duration::from_millis(100);

/// Outcome of one merge chunk.
pub(super) enum MergeStep {
    /// More staging rows remain; run another chunk on a later iteration.
    Continue,
    /// Every staged relation is drained; mark the query Ready.
    Done,
    /// A relation's deleted-key set overflowed or a bulk invalidation passed
    /// the snapshot — later chunks could resurrect removed rows, so the merge
    /// stops and the query is failed (it repopulates later). Rows merged by
    /// earlier chunks stay: they were filtered against a complete set when
    /// they landed and CDC maintains them from then on.
    Aborted,
}

/// Why the pending-heap walk stopped, which decides whether the head needs
/// the watermark nudge and flush-force tail: only a gated head is stalled on
/// the watermark. A head waiting for the merge slot is not, and arming the
/// stall clock for it would fire the origin flush without its grace window
/// on the first walk after the active drain finishes.
pub(super) enum HeapStop {
    /// Nothing pending.
    Exhausted,
    /// The head's snapshot is past the apply watermark.
    HeadGated(Lsn),
    /// The head is releasable but a drain is already active.
    SlotBusy,
}

/// What a drain does with each staging window.
pub(super) enum DrainTarget {
    /// Upsert into the cache table: the population merge, carrying the
    /// population for Ready finalization.
    Apply(PopulationMerge),
    /// Delete only: the population was superseded, invalidated, evicted,
    /// aborted, or failed, and its staging must be emptied for pool reuse —
    /// in the same bounded chunks, so the check-in's one-statement `DELETE`
    /// over up to millions of rows does not stall CDC apply (PGC-418).
    Discard,
}

/// Where a drain is within its staged relations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DrainCursor {
    /// About to start `staged[index]`; its size is read on the next step.
    RelationStart { index: usize },
    /// Inside `staged[index]`: the next window starts at `next_block` and the
    /// table has `blocks` heap blocks — from `pg_relation_size` at relation
    /// start, the file size rather than `pg_class.relpages`, since pooled
    /// tables carry dead space from earlier populations and the stats can lag
    /// by half the file.
    InRelation {
        index: usize,
        next_block: u32,
        blocks: u32,
    },
    /// Every staged relation is drained.
    Done,
}

/// A population's staging tables awaiting a chunked discard.
pub(super) struct StagingDiscard {
    pub(super) fingerprint: Fingerprint,
    pub(super) generation: Generation,
    pub(super) staged: Vec<(Oid, EcoString)>,
}

/// The deleted-key filter rendered for one relation of a drain, reused across
/// chunks while the relation's key set stays at `version`. Up to
/// `POPULATION_DELETED_KEY_CAP` tuples, so re-rendering per chunk would cost
/// every chunk a fixed price the size controller would then chase.
struct FilterCache {
    relation_oid: Oid,
    version: u64,
    predicate: String,
}

/// One heap-page window of a staging table, the unit a merge step drains.
struct ChunkWindow {
    /// Index into the drain's `staged` list.
    index: usize,
    lo_block: u32,
    hi_block: u32,
    /// Total blocks in the relation's staging table.
    blocks: u32,
}

/// One relation's deleted-key set in a drain, with its PK columns as
/// rendered for the key predicates.
#[derive(Clone, Copy)]
struct KeyScope<'a> {
    keys: &'a PopulationDeletedKeys,
    relation_oid: Oid,
    pk_columns_paren: &'a str,
}

/// One chunk statement's staging window and generation stamp.
#[derive(Clone, Copy)]
struct ChunkStatement<'a> {
    staging: &'a str,
    generation: Generation,
    lo_block: u32,
    hi_block: u32,
}

/// What a merge step drains: one window of one relation's staging table.
struct ChunkTarget {
    relation_oid: Oid,
    staging: EcoString,
    window: ChunkWindow,
}

/// The toast-stale key set a drain last probed its staging table against
/// (PGC-464), so a chunk re-probes only when the set moved.
#[derive(Clone, Copy)]
struct StaleProbeCache {
    relation_oid: Oid,
    version: u64,
}

/// What the writer's input queues held when the active drain last took a
/// chunk (or started). Under a backlog the next chunk is due only once each
/// queue has yielded its unit of foreground work since: a CDC batch flush,
/// or the query commands that were queued at the boundary. That owed set is
/// the arrivals during one chunk, so the interleave balances by the
/// commands' actual cost — a cheap burst drains in a blink and the merge
/// resumes, an expensive one is still bounded to a chunk's worth of arrivals
/// — with no clock and no count knob.
#[derive(Clone, Copy, Default)]
struct ChunkBoundary {
    batch_flush_seq: u64,
    commands_handled: u64,
    commands_owed: u64,
}

/// Emptiness of the writer's input queues at a chunk decision.
#[derive(Clone, Copy)]
pub(super) struct InputQueues {
    pub(super) cdc_empty: bool,
    pub(super) query_empty: bool,
    pub(super) internal_empty: bool,
}

/// A staging drain in progress — a population merge, or a discard — advanced
/// one chunk at a time (PGC-418). At most one is in progress; the writer loop
/// advances it by one chunk per iteration while no CDC frame is open, so CDC
/// apply interleaves with it instead of waiting behind a single statement
/// over the whole staging table.
pub(super) struct MergeInProgress {
    pub(super) fingerprint: Fingerprint,
    pub(super) generation: Generation,
    /// `(relation_oid, staging table name in pgcache_stage)` per relation.
    staged: Vec<(Oid, EcoString)>,
    pub(super) target: DrainTarget,
    cursor: DrainCursor,
    chunk_blocks: u32,
    boundary: ChunkBoundary,
    filter: Option<FilterCache>,
    stale_probe: Option<StaleProbeCache>,
    /// Rows drained from staging so far (before the deleted-key filter).
    pub(super) drained_rows: u64,
    pub(super) chunks: u64,
    pub(super) started_at: Instant,
}

impl MergeInProgress {
    pub(super) fn is_applying(&self) -> bool {
        matches!(self.target, DrainTarget::Apply(_))
    }
}

/// A queued population merge, ordered by its watermark deadline (PGC-272).
/// The ordering key is `(snapshot_lsn, generation)`: `generation` comes from
/// the single global monotonic counter, so the tuple is a total order even
/// when two populations capture identical snapshot LSNs. Deliberately NOT
/// `fingerprint` — two populations of one fingerprint at different
/// generations can be in flight simultaneously and must not tie. The payload
/// is excluded from the ordering.
pub(super) struct PendingMerge(pub(super) PopulationMerge);

impl PendingMerge {
    fn key(&self) -> (Lsn, Generation) {
        (self.0.snapshot_lsn, self.0.generation)
    }
}

impl Ord for PendingMerge {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key().cmp(&other.key())
    }
}

impl PartialOrd for PendingMerge {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for PendingMerge {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}

impl Eq for PendingMerge {}

/// The writer's merge pipeline: two inputs feeding one slot.
pub(super) struct MergeQueue {
    /// Population merges awaiting both a quiescent (frame-Idle) writer and the
    /// CDC apply watermark reaching their snapshot LSN (PGC-272): a min-heap
    /// on `(snapshot_lsn, generation)`, drained in deadline order by
    /// `pending_merges_drain` as the watermark advances. Gating the merge —
    /// not just Ready — keeps snapshot-state rows out of the shared table
    /// until CDC has applied past the snapshot, so already-Ready bystander
    /// queries can never serve a torn mix of two origin points in time.
    pub(super) pending: BinaryHeap<Reverse<PendingMerge>>,
    /// Superseded populations whose staging awaits a chunked discard, started
    /// when the slot is free and no releasable merge is waiting.
    pub(super) discards: VecDeque<StagingDiscard>,
    /// The drain being advanced chunk by chunk, popped from `pending` once its
    /// gate released, or started from `discards`.
    pub(super) active: Option<MergeInProgress>,
    /// Count of CDC batch flushes (each returns the frame to Idle); see
    /// `ChunkBoundary`.
    batch_flush_seq: u64,
    /// Count of handled query commands (registrations and the like — not
    /// population-task completions, which are trivial and bounded by the
    /// workers in flight, so a chunk simply waits for them); see
    /// `ChunkBoundary`.
    commands_handled: u64,
    /// Signals the CDC thread to request an immediate keepalive (reply-requested
    /// standby status update), advancing `last_received_lsn` so a gated query's
    /// snapshot LSN is reached within a round-trip instead of waiting for the
    /// next periodic keepalive.
    pub(super) watermark_nudge: Arc<Notify>,
    /// When the earliest gated merge first became gated, or `None` when
    /// nothing is gated. Times the grace window before `origin_flush_force`
    /// is used to make a stuck snapshot LSN reachable (PGC-290).
    pub(super) stall_since: Option<Instant>,
    /// LSN of the last `origin_flush_force` marker. A merge whose snapshot LSN
    /// is at or below this has already had the flush pointer forced past it,
    /// so it needs no further marker — this gates re-emits to roughly one per
    /// stuck wave rather than one per gated merge (PGC-290).
    pub(super) flush_marker_lsn: Lsn,
}

impl MergeQueue {
    pub(super) fn new(watermark_nudge: Arc<Notify>) -> Self {
        Self {
            pending: BinaryHeap::new(),
            discards: VecDeque::new(),
            active: None,
            batch_flush_seq: 0,
            commands_handled: 0,
            watermark_nudge,
            stall_since: None,
            flush_marker_lsn: Lsn::from_raw(0),
        }
    }

    pub(super) fn batch_flushed(&mut self) {
        self.batch_flush_seq = self.batch_flush_seq.wrapping_add(1);
    }

    pub(super) fn command_handled(&mut self) {
        self.commands_handled = self.commands_handled.wrapping_add(1);
    }

    /// Claim the single drain slot (PGC-418: at most one drain — a merge or a
    /// discard — is in progress) with the drain `drain` builds from the
    /// current chunk boundary.
    fn drain_begin(&mut self, drain: impl FnOnce(ChunkBoundary) -> MergeInProgress) {
        debug_assert!(
            self.active.is_none(),
            "drain started while another is in progress"
        );
        self.active = Some(drain(self.boundary(0)));
    }

    fn boundary(&self, commands_owed: u64) -> ChunkBoundary {
        ChunkBoundary {
            batch_flush_seq: self.batch_flush_seq,
            commands_handled: self.commands_handled,
            commands_owed,
        }
    }

    /// Mark a chunk boundary for the active drain — a chunk just ran, or the
    /// drain just started — owing the `query_queued` commands waiting now.
    pub(super) fn chunk_boundary_mark(&mut self, query_queued: usize) {
        let boundary = self.boundary(query_queued as u64);
        if let Some(active) = self.active.as_mut() {
            active.boundary = boundary;
        }
    }

    /// Whether the active drain may take its next chunk now: every input
    /// queue is drained — never ahead of queued real work — or has yielded
    /// its unit since the last boundary (`ChunkBoundary`). Population-task
    /// completions are always drained first.
    pub(super) fn chunk_due(&self, queues: InputQueues) -> bool {
        let Some(active) = &self.active else {
            return false;
        };
        let boundary = active.boundary;
        let flushed_since = self.batch_flush_seq != boundary.batch_flush_seq;
        let owed_handled = self
            .commands_handled
            .wrapping_sub(boundary.commands_handled)
            >= boundary.commands_owed;
        (queues.cdc_empty || flushed_since)
            && (queues.query_empty || owed_handled)
            && queues.internal_empty
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    const ALL_EMPTY: InputQueues = InputQueues {
        cdc_empty: true,
        query_empty: true,
        internal_empty: true,
    };

    fn merges_with_active_drain() -> MergeQueue {
        let mut merges = MergeQueue::new(Arc::new(Notify::new()));
        merges.active = Some(MergeInProgress::new(
            Fingerprint::from_raw(1),
            Generation::from_raw(1),
            vec![],
            DrainTarget::Discard,
            merges.boundary(0),
        ));
        merges
    }

    #[test]
    fn test_chunk_due_only_with_an_active_drain() {
        let merges = MergeQueue::new(Arc::new(Notify::new()));
        assert!(!merges.chunk_due(ALL_EMPTY));
        assert!(merges_with_active_drain().chunk_due(ALL_EMPTY));
    }

    #[test]
    fn test_chunk_waits_for_a_batch_flush_under_a_cdc_backlog() {
        let mut merges = merges_with_active_drain();
        let cdc_backlog = InputQueues {
            cdc_empty: false,
            ..ALL_EMPTY
        };
        assert!(
            !merges.chunk_due(cdc_backlog),
            "no flush since the boundary"
        );
        merges.batch_flushed();
        assert!(merges.chunk_due(cdc_backlog), "a batch flushed since");
        merges.chunk_boundary_mark(0);
        assert!(
            !merges.chunk_due(cdc_backlog),
            "boundary consumed the flush"
        );
    }

    #[test]
    fn test_chunk_waits_for_the_commands_owed_at_the_boundary() {
        let mut merges = merges_with_active_drain();
        let query_backlog = InputQueues {
            query_empty: false,
            ..ALL_EMPTY
        };
        merges.chunk_boundary_mark(3);
        merges.command_handled();
        merges.command_handled();
        assert!(
            !merges.chunk_due(query_backlog),
            "one of three owed still queued"
        );
        merges.command_handled();
        assert!(merges.chunk_due(query_backlog), "owed set drained");
        merges.command_handled();
        assert!(
            merges.chunk_due(query_backlog),
            "arrivals beyond the owed set do not re-gate this chunk"
        );
        merges.chunk_boundary_mark(0);
        assert!(
            merges.chunk_due(query_backlog),
            "nothing owed: arrivals after the boundary compete"
        );
    }

    #[test]
    fn test_chunk_always_waits_for_population_completions() {
        let mut merges = merges_with_active_drain();
        let internal_backlog = InputQueues {
            internal_empty: false,
            ..ALL_EMPTY
        };
        assert!(!merges.chunk_due(internal_backlog));
        merges.batch_flushed();
        merges.command_handled();
        assert!(!merges.chunk_due(internal_backlog), "no unit satisfies it");
    }
}
