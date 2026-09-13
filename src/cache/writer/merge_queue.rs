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
use postgres_protocol::escape;
use rootcause::Report;
use tokio::sync::Notify;
use tokio_postgres::SimpleQueryMessage;
use tracing::debug;

use crate::catalog::TableMetadata;
use crate::oid::Oid;
use crate::pg::Lsn;
use crate::query::Fingerprint;

use super::super::messages::PopulationMerge;
use super::super::{CacheError, CacheResult, MapIntoReport, ReportExt};
use super::core::WriterCore;
use super::staging::PopulationDeletedKeys;

/// How long a population merge may stay gated on the apply watermark before the
/// writer forces an origin WAL flush (`origin_flush_force`) to make its snapshot
/// LSN reachable (PGC-290). Above the nudge→keepalive round-trip so a healthy
/// active origin, where the flush pointer catches up on its own, never triggers
/// a marker; the cost is at most this much extra ready-latency on a stalled
/// (idle / async-commit) origin.
pub(super) const MERGE_FLUSH_FORCE_AFTER: Duration = Duration::from_millis(100);

/// Heap blocks drained per merge chunk on a merge's first chunk; adapted
/// afterwards so each chunk lands near `MERGE_CHUNK_TARGET`. Chunks are page
/// windows rather than row counts: a `LIMIT`-bounded scan is only complete
/// under a forward TID range scan, and on a pooled staging table whose stats
/// were taken while empty the planner picks a sequential scan instead — which,
/// synchronized, can start mid-table and silently skip rows. A window drains
/// every row in it under any plan.
const MERGE_CHUNK_BLOCKS_INITIAL: u32 = 64;
const MERGE_CHUNK_BLOCKS_MIN: u32 = 4;
const MERGE_CHUNK_BLOCKS_MAX: u32 = 8_192;
/// Wall-time target per merge chunk. The writer runs one chunk per loop
/// iteration while a drain is in progress and a chunk is due
/// (`MergeInProgress::chunk_due`), so this bounds how long a CDC frame waits
/// behind the merge (PGC-418).
const MERGE_CHUNK_TARGET: Duration = Duration::from_millis(100);

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

impl DrainCursor {
    /// The cursor at the start of `staged[0]`, or `Done` for an empty set.
    fn first(staged_len: usize) -> Self {
        Self::after_relation(0, staged_len)
    }

    /// The cursor once `staged[index - 1]` (or nothing, for `index == 0`) is
    /// drained: the start of `staged[index]`, or `Done` past the end.
    fn after_relation(index: usize, staged_len: usize) -> Self {
        if index < staged_len {
            Self::RelationStart { index }
        } else {
            Self::Done
        }
    }
}

/// A population's staging tables awaiting a chunked discard.
pub(super) struct StagingDiscard {
    pub(super) fingerprint: Fingerprint,
    pub(super) generation: u64,
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
    pub(super) generation: u64,
    /// `(relation_oid, staging table name in pgcache_stage)` per relation.
    staged: Vec<(Oid, EcoString)>,
    pub(super) target: DrainTarget,
    cursor: DrainCursor,
    chunk_blocks: u32,
    boundary: ChunkBoundary,
    filter: Option<FilterCache>,
    /// Rows drained from staging so far (before the deleted-key filter).
    pub(super) drained_rows: u64,
    pub(super) chunks: u64,
    pub(super) started_at: Instant,
}

impl MergeInProgress {
    fn new(
        fingerprint: Fingerprint,
        generation: u64,
        staged: Vec<(Oid, EcoString)>,
        target: DrainTarget,
        boundary: ChunkBoundary,
    ) -> Self {
        let cursor = DrainCursor::first(staged.len());
        Self {
            fingerprint,
            generation,
            staged,
            target,
            cursor,
            chunk_blocks: fault_merge_chunk_blocks().unwrap_or(MERGE_CHUNK_BLOCKS_INITIAL),
            boundary,
            filter: None,
            drained_rows: 0,
            chunks: 0,
            started_at: Instant::now(),
        }
    }

    pub(super) fn is_applying(&self) -> bool {
        matches!(self.target, DrainTarget::Apply(_))
    }

    /// The deleted-key filter for the chunk about to run, rendered from `keys`
    /// only when the relation's key set changed since the last rendering, so a
    /// removal landing between chunks is honored without paying the rendering
    /// on every chunk.
    fn filter_predicate(
        &mut self,
        keys: &PopulationDeletedKeys,
        relation_oid: Oid,
        pk_columns_paren: &str,
    ) -> Option<&str> {
        let version = keys.filter_version(relation_oid)?;
        let current = self
            .filter
            .as_ref()
            .is_some_and(|c| c.relation_oid == relation_oid && c.version == version);
        if !current {
            let predicate = keys.filter_predicate(relation_oid, pk_columns_paren)?;
            self.filter = Some(FilterCache {
                relation_oid,
                version,
                predicate,
            });
        }
        self.filter.as_ref().map(|c| c.predicate.as_str())
    }

    /// Scale the next chunk toward `MERGE_CHUNK_TARGET` from the last chunk's
    /// wall time, clamped so a pathological sample can't swing it to extremes.
    fn chunk_blocks_adapt(&mut self, elapsed: Duration) {
        if fault_merge_chunk_blocks().is_some() {
            return;
        }
        let ratio = if elapsed.is_zero() {
            f64::from(u32::MAX)
        } else {
            MERGE_CHUNK_TARGET.as_secs_f64() / elapsed.as_secs_f64()
        };
        // Truncation is the intent: the product is clamped to a small range.
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let next = (f64::from(self.chunk_blocks) * ratio.clamp(0.25, 4.0)) as u32;
        self.chunk_blocks = next.clamp(MERGE_CHUNK_BLOCKS_MIN, MERGE_CHUNK_BLOCKS_MAX);
    }
}

/// Test-only fixed chunk size (fault-injection feature): pins the heap blocks
/// per chunk so a test can make a small population span a known number of
/// chunks.
#[cfg(feature = "fault-injection")]
fn fault_merge_chunk_blocks() -> Option<u32> {
    std::env::var("PGCACHE_FAULT_MERGE_CHUNK_BLOCKS")
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .filter(|blocks| *blocks > 0)
}
#[cfg(not(feature = "fault-injection"))]
fn fault_merge_chunk_blocks() -> Option<u32> {
    None
}

/// Test-only delay after each merge chunk (fault-injection feature): stretches
/// a merge so a test can land CDC events between its chunks. Blocks the writer
/// like a slow chunk would.
#[cfg(feature = "fault-injection")]
async fn fault_merge_chunk_delay() {
    if let Some(ms) = std::env::var("PGCACHE_FAULT_MERGE_CHUNK_DELAY_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|ms| *ms > 0)
    {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }
}
#[cfg(not(feature = "fault-injection"))]
async fn fault_merge_chunk_delay() {}

/// A queued population merge, ordered by its watermark deadline (PGC-272).
/// The ordering key is `(snapshot_lsn, generation)`: `generation` comes from
/// the single global monotonic counter, so the tuple is a total order even
/// when two populations capture identical snapshot LSNs. Deliberately NOT
/// `fingerprint` — two populations of one fingerprint at different
/// generations can be in flight simultaneously and must not tie. The payload
/// is excluded from the ordering.
pub(super) struct PendingMerge(pub(super) PopulationMerge);

impl PendingMerge {
    fn key(&self) -> (Lsn, u64) {
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

/// Pre-rendered, owned SQL fragments for merging one relation's staging table
/// into its shared cache table. Built while borrowing `TableMetadata`, so the
/// borrow doesn't span the async DB calls.
struct MergePlan {
    schema: EcoString,
    name: EcoString,
    /// `"c1","c2",...` — all columns, position order.
    columns_csv: String,
    /// `("p1","p2")` — primary-key columns.
    pk_columns_paren: String,
    /// `ON CONFLICT ("p1") DO UPDATE SET "p1" = EXCLUDED."p1"` — re-stamps the
    /// generation of pre-existing rows without overwriting data (CDC owns it).
    conflict: String,
}

impl MergePlan {
    fn build(table: &TableMetadata) -> Self {
        let columns_csv = table
            .columns
            .iter()
            .map(|c| escape::escape_identifier(&c.name))
            .collect::<Vec<_>>()
            .join(",");
        let pk_quoted: Vec<String> = table
            .primary_key_columns
            .iter()
            .map(|c| escape::escape_identifier(c))
            .collect();
        let pk_columns_paren = format!("({})", pk_quoted.join(","));
        let conflict_assign = pk_quoted
            .iter()
            .map(|c| format!("{c} = EXCLUDED.{c}"))
            .collect::<Vec<_>>()
            .join(", ");
        let conflict = format!("ON CONFLICT {pk_columns_paren} DO UPDATE SET {conflict_assign}");
        Self {
            schema: table.schema.clone(),
            name: table.name.clone(),
            columns_csv,
            pk_columns_paren,
            conflict,
        }
    }

    /// One merge chunk: drain every staging row in heap blocks `[lo, hi)` into
    /// the cache table and report the row count.
    ///
    /// Drains with `DELETE … RETURNING` rather than `SELECT` + a later
    /// `DROP TABLE` (PGC-293): the rows feed the upsert and the table is left
    /// empty for pooled reuse, so a population emits *no* DDL — and thus no
    /// relcache/plan-cache invalidation, the per-population cost that
    /// collapses write-mixed throughput at high query cardinality (PGC-294).
    /// The filter (CDC-removed keys) gates only which drained rows are
    /// reinserted, so the table still ends empty. DISTINCT ON the PK collapses
    /// duplicate keys a set-operation query can stage (same relation in
    /// multiple branches) — without it the upsert would error "ON CONFLICT
    /// cannot affect row a second time". A duplicate split across two chunks
    /// conflicts on the second and is re-stamped, which is the same outcome.
    ///
    /// A page window is complete under any plan: the planner picks a TID range
    /// scan for it even with stale stats, and a sequential-scan fallback is a
    /// filter over the window, slower but never skipping. `SET LOCAL` scopes
    /// the generation stamp to the implicit transaction of this
    /// multi-statement string, which also rolls back as a unit on error and
    /// leaves the connection outside any transaction.
    fn chunk_sql(
        &self,
        staging: &str,
        generation: u64,
        lo_block: u32,
        hi_block: u32,
        filter: Option<&str>,
    ) -> String {
        let where_clause = filter.map_or_else(String::new, |f| format!(" WHERE {f}"));
        format!(
            "SET LOCAL mem.query_generation = {generation}; \
             WITH d AS (DELETE FROM pgcache_stage.{staging} \
                 WHERE ctid >= '({lo_block},0)'::tid AND ctid < '({hi_block},0)'::tid \
                 RETURNING {cols}), \
             i AS (INSERT INTO {schema}.{name} ({cols}) \
                 SELECT DISTINCT ON {pk} {cols} FROM d{where_clause} {conflict}) \
             SELECT count(*) FROM d",
            schema = escape::escape_identifier(&self.schema),
            name = escape::escape_identifier(&self.name),
            cols = self.columns_csv,
            pk = self.pk_columns_paren,
            conflict = self.conflict,
        )
    }
}

/// One discard chunk: delete every staging row in heap blocks `[lo, hi)`.
/// Same window shape as an apply chunk, so it is complete under any plan.
fn discard_sql(staging: &str, lo_block: u32, hi_block: u32) -> String {
    format!(
        "DELETE FROM pgcache_stage.{staging} \
         WHERE ctid >= '({lo_block},0)'::tid AND ctid < '({hi_block},0)'::tid"
    )
}

/// Read a discard statement's row count from its command tag.
fn discard_result_parse(messages: &[SimpleQueryMessage]) -> u64 {
    messages
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::CommandComplete(rows) => Some(*rows),
            SimpleQueryMessage::Row(_) | SimpleQueryMessage::RowDescription(_) | _ => None,
        })
        .unwrap_or(0)
}

/// Read a chunk statement's row count.
fn chunk_result_parse(messages: &[SimpleQueryMessage]) -> CacheResult<u64> {
    messages
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(row),
            SimpleQueryMessage::CommandComplete(_) | SimpleQueryMessage::RowDescription(_) | _ => {
                None
            }
        })
        .and_then(|row| row.get(0))
        .and_then(|c| c.parse::<u64>().ok())
        .ok_or_else(|| Report::from(CacheError::Other))
        .attach_loc("merge chunk returned no row count")
}

impl WriterCore {
    /// Begin merging one population's staging tables into the shared cache
    /// tables; `population_merge_step` drains them chunk by chunk. Generation is
    /// stamped per chunk (moved off the population worker). The caller
    /// deactivates the deleted-key set, checks staging back in, and marks the
    /// query Ready / Failed when the step reports a terminal outcome.
    pub(super) fn population_merge_start(&mut self, merge: PopulationMerge) {
        debug_assert!(
            self.merges.active.is_none(),
            "population merge started while another is in progress"
        );
        let staged = merge.staged.clone();
        self.merges.active = Some(MergeInProgress::new(
            merge.fingerprint,
            merge.generation,
            staged,
            DrainTarget::Apply(merge),
            self.merges.boundary(0),
        ));
    }

    /// Begin emptying a superseded population's staging tables in chunks (it
    /// never reached its merge — the tombstone path of the pending-merge drain).
    pub(super) fn population_discard_start(&mut self, discard: StagingDiscard) {
        debug_assert!(
            self.merges.active.is_none(),
            "population discard started while a merge is in progress"
        );
        self.merges.active = Some(MergeInProgress::new(
            discard.fingerprint,
            discard.generation,
            discard.staged,
            DrainTarget::Discard,
            self.merges.boundary(0),
        ));
    }

    /// Queue a population's staging for a chunked discard once the merge slot
    /// is free: a superseded population that never reached its merge, or one
    /// whose worker failed mid-stream. The tables are read from the pool's
    /// ledger, which keeps them checked out under the key until the discard's
    /// check-in.
    pub(super) fn population_discard_enqueue(&mut self, fingerprint: Fingerprint, generation: u64) {
        let staged = self.staging_pool.held(fingerprint, generation);
        if staged.is_empty() {
            return;
        }
        self.merges.discards.push_back(StagingDiscard {
            fingerprint,
            generation,
            staged,
        });
    }

    /// Stop applying the in-progress merge — the query was superseded,
    /// invalidated, evicted, aborted, or the merge failed — and switch it to
    /// discarding its remaining staging rows from where it stopped. Rows already
    /// merged stay: each was snapshot state behind the watermark, filtered
    /// against a complete deleted-key set when it landed, and is maintained by
    /// CDC from then on, so they cannot tear a bystander's result; they're
    /// reclaimed by generation once nothing references them.
    pub(super) fn population_merge_discard_remaining(&mut self) {
        if let Some(in_progress) = self.merges.active.as_mut()
            && in_progress.is_applying()
        {
            debug!(
                "population merge stopped after {} chunks / {} rows, discarding the rest {}",
                in_progress.chunks, in_progress.drained_rows, in_progress.fingerprint
            );
            in_progress.target = DrainTarget::Discard;
        }
    }

    /// Run one chunk of the in-progress drain (PGC-418): size the relation the
    /// cursor is about to enter, or drain one window of the relation it is in.
    /// A merge filters keys CDC removed during the population (PGC-250); the
    /// filter is re-rendered whenever the key set changed since the last chunk
    /// so a removal that lands between chunks is honored. Returns `Continue`
    /// while staging rows remain; on `Done` / `Aborted` the caller takes the
    /// active drain and finalizes.
    pub(super) async fn population_merge_step(&mut self) -> CacheResult<MergeStep> {
        let Some(in_progress) = self.merges.active.as_ref() else {
            return Ok(MergeStep::Done);
        };
        let staged_len = in_progress.staged.len();
        let (index, lo_block, blocks) = match in_progress.cursor {
            DrainCursor::Done => return Ok(MergeStep::Done),
            DrainCursor::RelationStart { index } => {
                let Some((_, staging)) = in_progress.staged.get(index) else {
                    return Ok(MergeStep::Done);
                };
                let blocks = self.staging_blocks(staging).await?;
                let cursor = if blocks == 0 {
                    DrainCursor::after_relation(index + 1, staged_len)
                } else {
                    DrainCursor::InRelation {
                        index,
                        next_block: 0,
                        blocks,
                    }
                };
                if let Some(in_progress) = self.merges.active.as_mut() {
                    in_progress.cursor = cursor;
                }
                return Ok(MergeStep::Continue);
            }
            DrainCursor::InRelation {
                index,
                next_block,
                blocks,
            } => (index, next_block, blocks),
        };
        let Some((relation_oid, staging)) = in_progress.staged.get(index).cloned() else {
            return Ok(MergeStep::Done);
        };
        // Abort if any relation lost keys (overflow) or was bulk-invalidated
        // (TRUNCATE / recovery) at an LSN past this population's snapshot —
        // a later chunk could resurrect removed rows. Checked per chunk:
        // either can happen in a CDC frame applied between chunks.
        if let DrainTarget::Apply(merge) = &in_progress.target
            && in_progress.staged.iter().any(|(oid, _)| {
                self.population_deleted_keys
                    .should_abort(*oid, merge.snapshot_lsn)
            })
        {
            return Ok(MergeStep::Aborted);
        }
        let generation = in_progress.generation;
        let hi_block = lo_block
            .saturating_add(in_progress.chunk_blocks)
            .min(blocks);

        // A relation evicted mid-population has no cache table to merge into;
        // its staging rows are discarded in chunks like the rest.
        let plan = match &in_progress.target {
            DrainTarget::Apply(_) => self.cache.tables.get1(&relation_oid).map(MergePlan::build),
            DrainTarget::Discard => None,
        };
        let sql = match &plan {
            Some(plan) => {
                let filter = self.merges.active.as_mut().and_then(|in_progress| {
                    in_progress.filter_predicate(
                        &self.population_deleted_keys,
                        relation_oid,
                        &plan.pk_columns_paren,
                    )
                });
                plan.chunk_sql(&staging, generation, lo_block, hi_block, filter)
            }
            None => discard_sql(&staging, lo_block, hi_block),
        };

        let started = Instant::now();
        let messages = self
            .db_cache
            .simple_query(&sql)
            .await
            .map_into_report::<CacheError>()
            .attach_loc("population merge chunk")?;
        let elapsed = started.elapsed();
        let count = match &plan {
            Some(_) => chunk_result_parse(&messages)?,
            None => discard_result_parse(&messages),
        };
        fault_merge_chunk_delay().await;

        let mh = crate::metrics::handles();
        match &plan {
            Some(_) => mh.reg.merge_chunks.increment(1),
            None => mh.reg.merge_discard_chunks.increment(1),
        }
        mh.reg.merge_chunk.record(elapsed.as_secs_f64());

        let Some(in_progress) = self.merges.active.as_mut() else {
            return Ok(MergeStep::Done);
        };
        in_progress.chunks += 1;
        in_progress.drained_rows += count;
        in_progress.chunk_blocks_adapt(elapsed);
        in_progress.cursor = if hi_block >= blocks {
            DrainCursor::after_relation(index + 1, staged_len)
        } else {
            DrainCursor::InRelation {
                index,
                next_block: hi_block,
                blocks,
            }
        };
        Ok(MergeStep::Continue)
    }

    /// Heap blocks in a staging table's main fork, from the file size.
    async fn staging_blocks(&self, staging: &str) -> CacheResult<u32> {
        let row = self
            .db_cache
            .query_one(
                &format!(
                    "SELECT (pg_relation_size('pgcache_stage.{staging}') \
                     / current_setting('block_size')::bigint)::bigint"
                ),
                &[],
            )
            .await
            .map_into_report::<CacheError>()
            .attach_loc("reading staging table size")?;
        let blocks: i64 = row.get(0);
        u32::try_from(blocks)
            .map_err(|_| Report::from(CacheError::Other))
            .attach_loc("staging table block count out of range")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const REL: Oid = Oid::from_raw(10);

    fn drain() -> MergeInProgress {
        MergeInProgress::new(
            Fingerprint::from_raw(1),
            1,
            vec![(REL, EcoString::from("stage_10_0"))],
            DrainTarget::Discard,
            ChunkBoundary::default(),
        )
    }

    fn keys_recording() -> PopulationDeletedKeys {
        let mut keys = PopulationDeletedKeys::default();
        keys.activate(Fingerprint::from_raw(1), 1, &[REL], Lsn::from_raw(1));
        keys
    }

    /// The rendered filter is reused across chunks while the key set holds and
    /// re-rendered as soon as a key lands or is cancelled between chunks.
    #[test]
    fn test_filter_cache_follows_key_set_changes() {
        let mut keys = keys_recording();
        let mut drain = drain();
        assert!(drain.filter_predicate(&keys, REL, "(id)").is_none());

        keys.record(REL, EcoString::from("4"), Lsn::from_raw(10));
        let first = drain
            .filter_predicate(&keys, REL, "(id)")
            .expect("filter present")
            .as_ptr();
        let again = drain
            .filter_predicate(&keys, REL, "(id)")
            .expect("filter present")
            .as_ptr();
        assert_eq!(first, again, "unchanged key set reuses the rendering");

        keys.record(REL, EcoString::from("7"), Lsn::from_raw(11));
        let grown = drain
            .filter_predicate(&keys, REL, "(id)")
            .expect("filter present");
        assert!(grown.contains("(4)") && grown.contains("(7)"), "{grown}");

        assert!(keys.cancel(REL, "4"));
        let shrunk = drain
            .filter_predicate(&keys, REL, "(id)")
            .expect("filter present");
        assert!(
            !shrunk.contains("(4)") && shrunk.contains("(7)"),
            "{shrunk}"
        );

        assert!(keys.cancel(REL, "7"));
        assert!(drain.filter_predicate(&keys, REL, "(id)").is_none());
    }

    fn plan() -> MergePlan {
        MergePlan {
            schema: "public".into(),
            name: "t".into(),
            columns_csv: "\"id\",\"v\"".to_owned(),
            pk_columns_paren: "(\"id\")".to_owned(),
            conflict: "ON CONFLICT (\"id\") DO UPDATE SET \"id\" = EXCLUDED.\"id\"".to_owned(),
        }
    }

    #[test]
    fn test_chunk_sql_drains_a_page_window_and_stamps_generation_locally() {
        let sql = plan().chunk_sql("stage_1_0", 9, 3, 67, None);
        assert!(
            sql.starts_with("SET LOCAL mem.query_generation = 9;"),
            "{sql}"
        );
        assert!(
            sql.contains("WHERE ctid >= '(3,0)'::tid AND ctid < '(67,0)'::tid"),
            "{sql}"
        );
        assert!(
            !sql.contains("LIMIT"),
            "a row limit is plan-dependent: {sql}"
        );
        assert!(sql.contains("DELETE FROM pgcache_stage.stage_1_0"), "{sql}");
        assert!(sql.contains("RETURNING \"id\",\"v\""), "{sql}");
        assert!(
            sql.contains("INSERT INTO \"public\".\"t\" (\"id\",\"v\") SELECT DISTINCT ON (\"id\") \"id\",\"v\" FROM d ON CONFLICT"),
            "{sql}"
        );
        assert!(sql.ends_with("SELECT count(*) FROM d"), "{sql}");
        assert!(
            !sql.contains("BEGIN"),
            "explicit BEGIN would strand an aborted txn: {sql}"
        );
    }

    #[test]
    fn test_drain_cursor_walks_relations_then_finishes() {
        assert_eq!(DrainCursor::first(0), DrainCursor::Done);
        assert_eq!(
            DrainCursor::first(2),
            DrainCursor::RelationStart { index: 0 }
        );
        assert_eq!(
            DrainCursor::after_relation(1, 2),
            DrainCursor::RelationStart { index: 1 }
        );
        assert_eq!(DrainCursor::after_relation(2, 2), DrainCursor::Done);
    }

    fn merges_with_active_drain() -> MergeQueue {
        let mut merges = MergeQueue::new(Arc::new(Notify::new()));
        merges.active = Some(MergeInProgress::new(
            Fingerprint::from_raw(1),
            1,
            vec![],
            DrainTarget::Discard,
            merges.boundary(0),
        ));
        merges
    }

    const ALL_EMPTY: InputQueues = InputQueues {
        cdc_empty: true,
        query_empty: true,
        internal_empty: true,
    };

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

    #[test]
    fn test_discard_sql_is_the_same_page_window_without_the_insert() {
        let sql = discard_sql("stage_1_0", 3, 67);
        assert_eq!(
            sql,
            "DELETE FROM pgcache_stage.stage_1_0 WHERE ctid >= '(3,0)'::tid AND ctid < '(67,0)'::tid"
        );
    }

    #[test]
    fn test_chunk_sql_applies_deleted_key_filter_to_insert_only() {
        let sql = plan().chunk_sql("s", 1, 0, 10, Some("(\"id\") NOT IN ((4))"));
        assert!(
            sql.contains("FROM d WHERE (\"id\") NOT IN ((4)) ON CONFLICT"),
            "{sql}"
        );
        let (delete, _) = sql.split_once("RETURNING").expect("returning clause");
        assert!(
            !delete.contains("NOT IN"),
            "filter must not gate the drain: {sql}"
        );
    }

    #[test]
    fn test_chunk_blocks_adapt_targets_chunk_time_within_clamps() {
        let merge = PopulationMerge {
            fingerprint: Fingerprint::from_raw(1),
            generation: 1,
            staged: vec![],
            cached_bytes: 0,
            row_count: 0,
            snapshot_lsn: Lsn::from_raw(0),
            enqueued_at: Instant::now(),
            fetch_stage_ms: 0.0,
        };
        let mut m = MergeInProgress::new(
            merge.fingerprint,
            merge.generation,
            merge.staged.clone(),
            DrainTarget::Apply(merge),
            ChunkBoundary::default(),
        );
        assert_eq!(m.chunk_blocks, MERGE_CHUNK_BLOCKS_INITIAL);
        m.chunk_blocks_adapt(MERGE_CHUNK_TARGET / 2);
        assert_eq!(m.chunk_blocks, MERGE_CHUNK_BLOCKS_INITIAL * 2);
        m.chunk_blocks_adapt(MERGE_CHUNK_TARGET * 8);
        assert_eq!(
            m.chunk_blocks,
            MERGE_CHUNK_BLOCKS_INITIAL * 2 / 4,
            "growth ratio clamped at 4x/0.25x"
        );
        for _ in 0..10 {
            m.chunk_blocks_adapt(Duration::from_nanos(1));
        }
        assert_eq!(m.chunk_blocks, MERGE_CHUNK_BLOCKS_MAX);
        for _ in 0..10 {
            m.chunk_blocks_adapt(Duration::from_secs(60));
        }
        assert_eq!(m.chunk_blocks, MERGE_CHUNK_BLOCKS_MIN);
    }
}
