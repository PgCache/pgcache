//! The drain in progress: its cursor over the staged relations, the cached
//! deleted-key filter and toast-stale probe, and adaptive chunk sizing.

use std::time::{Duration, Instant};

use ecow::EcoString;

use super::{
    ChunkBoundary, ChunkWindow, DrainCursor, DrainTarget, FilterCache, KeyScope, MergeInProgress,
    StaleProbeCache,
};
use crate::oid::Oid;
use crate::pg::Lsn;
use crate::query::Fingerprint;

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

impl DrainCursor {
    /// The cursor at the start of `staged[0]`, or `Done` for an empty set.
    pub(super) fn first(staged_len: usize) -> Self {
        Self::after_relation(0, staged_len)
    }

    /// The cursor once `staged[index - 1]` (or nothing, for `index == 0`) is
    /// drained: the start of `staged[index]`, or `Done` past the end.
    pub(super) fn after_relation(index: usize, staged_len: usize) -> Self {
        if index < staged_len {
            Self::RelationStart { index }
        } else {
            Self::Done
        }
    }
}

impl MergeInProgress {
    pub(super) fn new(
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
            stale_probe: None,
            drained_rows: 0,
            chunks: 0,
            started_at: Instant::now(),
        }
    }

    /// The deleted-key filter for the chunk about to run, rendered from `keys`
    /// only when the relation's key set changed since the last rendering, so a
    /// removal landing between chunks is honored without paying the rendering
    /// on every chunk.
    pub(super) fn filter_predicate(&mut self, scope: KeyScope<'_>) -> Option<&str> {
        let KeyScope {
            keys,
            relation_oid,
            pk_columns_paren,
        } = scope;
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

    /// The toast-stale membership predicate to probe the staging table with
    /// before the chunk, or `None` when the set is unchanged since the last
    /// probe (or empty above this population's floor).
    pub(super) fn stale_probe_predicate(
        &mut self,
        scope: KeyScope<'_>,
        floor: Lsn,
    ) -> Option<String> {
        let KeyScope {
            keys,
            relation_oid,
            pk_columns_paren,
        } = scope;
        let version = keys.stale_version_above(relation_oid, floor)?;
        let probed = self
            .stale_probe
            .is_some_and(|c| c.relation_oid == relation_oid && c.version == version);
        if probed {
            return None;
        }
        let predicate = keys.stale_predicate(relation_oid, floor, pk_columns_paren)?;
        self.stale_probe = Some(StaleProbeCache {
            relation_oid,
            version,
        });
        Some(predicate)
    }

    /// Account a drained window and move the cursor past it (into the next
    /// relation once the table is exhausted).
    pub(super) fn chunk_finish(&mut self, window: &ChunkWindow, count: u64, elapsed: Duration) {
        self.chunks += 1;
        self.drained_rows += count;
        self.chunk_blocks_adapt(elapsed);
        self.cursor = if window.hi_block >= window.blocks {
            DrainCursor::after_relation(window.index + 1, self.staged.len())
        } else {
            DrainCursor::InRelation {
                index: window.index,
                next_block: window.hi_block,
                blocks: window.blocks,
            }
        };
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

#[cfg(test)]
mod tests {

    use super::*;
    use crate::cache::messages::PopulationMerge;
    use crate::cache::writer::staging::PopulationDeletedKeys;

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

    fn scope(keys: &PopulationDeletedKeys) -> KeyScope<'_> {
        KeyScope {
            keys,
            relation_oid: REL,
            pk_columns_paren: "(id)",
        }
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
        assert!(drain.filter_predicate(scope(&keys)).is_none());

        keys.record(REL, EcoString::from("4"), Lsn::from_raw(10));
        let first = drain
            .filter_predicate(scope(&keys))
            .expect("filter present")
            .as_ptr();
        let again = drain
            .filter_predicate(scope(&keys))
            .expect("filter present")
            .as_ptr();
        assert_eq!(first, again, "unchanged key set reuses the rendering");

        keys.record(REL, EcoString::from("7"), Lsn::from_raw(11));
        let grown = drain
            .filter_predicate(scope(&keys))
            .expect("filter present");
        assert!(grown.contains("(4)") && grown.contains("(7)"), "{grown}");

        assert!(keys.cancel(REL, "4"));
        let shrunk = drain
            .filter_predicate(scope(&keys))
            .expect("filter present");
        assert!(
            !shrunk.contains("(4)") && shrunk.contains("(7)"),
            "{shrunk}"
        );

        assert!(keys.cancel(REL, "7"));
        assert!(drain.filter_predicate(scope(&keys)).is_none());
    }

    /// PGC-464: toast-stale keys render only above the population's floor, the
    /// probe predicate is re-rendered exactly when that set moves, a delete
    /// supersedes a stale key, and a stale key cancels a tracked delete.
    #[test]
    fn test_stale_probe_follows_key_set_and_floor() {
        let mut keys = keys_recording();
        let mut drain = drain();
        let floor = Lsn::from_raw(5);
        assert!(drain.stale_probe_predicate(scope(&keys), floor).is_none());

        // At or below the floor: the population's snapshot already reflects it.
        keys.record_toast_stale(REL, EcoString::from("4"), Lsn::from_raw(5));
        assert!(drain.stale_probe_predicate(scope(&keys), floor).is_none());

        keys.record_toast_stale(REL, EcoString::from("7"), Lsn::from_raw(10));
        let first = drain
            .stale_probe_predicate(scope(&keys), floor)
            .expect("predicate present");
        assert!(first.contains("(7)") && !first.contains("(4)"), "{first}");
        assert!(
            drain.stale_probe_predicate(scope(&keys), floor).is_none(),
            "unchanged set is not re-probed"
        );

        // A stale key cancels a tracked delete of the same row (alive at origin).
        keys.record(REL, EcoString::from("9"), Lsn::from_raw(11));
        assert!(
            keys.filter_predicate(REL, "(id)")
                .expect("delete filter")
                .contains("(9)")
        );
        keys.record_toast_stale(REL, EcoString::from("9"), Lsn::from_raw(12));
        assert!(keys.filter_predicate(REL, "(id)").is_none());
        let grown = drain
            .stale_probe_predicate(scope(&keys), floor)
            .expect("predicate present");
        assert!(grown.contains("(7)") && grown.contains("(9)"), "{grown}");

        // A later delete supersedes the stale key: the merge omits the row.
        keys.record(REL, EcoString::from("9"), Lsn::from_raw(13));
        let shrunk = drain
            .stale_probe_predicate(scope(&keys), floor)
            .expect("predicate present");
        assert!(
            shrunk.contains("(7)") && !shrunk.contains("(9)"),
            "{shrunk}"
        );
        assert_eq!(
            keys.floor(REL, Fingerprint::from_raw(1), 1),
            Some(Lsn::from_raw(1))
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
