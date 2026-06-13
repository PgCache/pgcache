//! In-process hot-result cache (PGC-236).
//!
//! A second materialization backend, mutually exclusive with MVs: the hottest
//! MV-ineligible (`SourceRow`-served) cache hits are memoized as client-ready
//! wire bytes (`RowDescription` + `DataRow*` + `CommandComplete`) so they can be
//! served straight from process memory — no worker hop, no cache-DB round trip.
//!
//! ## Consistency: version slots
//!
//! A memoized result is a finished snapshot, so *any* byte-changing mutation to a
//! relation it reads must invalidate it — broader than the grow-only query
//! invalidation path (an in-place UPDATE/DELETE never moves `generation`/`state`,
//! yet still changes the snapshot). We track this with **version slots**: the CDC
//! path advances a slot's version across every change; a memo stamps the slots it
//! depends on at capture and is served only while every stamped version still
//! matches. The version is a seqlock bracketing the commit (see below).
//!
//! The store is **rung-agnostic** — only two policies differ per precision rung:
//! which slots a memo stamps (capture) and which slots a CDC change bumps (evict).
//! Rung 1 (this version) uses [`SlotKey::Relation`] only: any change to relation
//! R bumps R, busting every memo over R. The finer [`SlotKey`] variants are
//! reserved for rung 2 (column-aware eviction).
//!
//! ## Correctness: the slot version is a seqlock
//!
//! Capture runs on the worker thread; the slot update runs on the writer thread.
//! The in-memory version bump and the cache-DB COMMIT (the point a change becomes
//! visible) are *not* atomic, so a single one-sided bump always leaves a stale
//! window on one side: bump-after-commit serves an existing memo stale in the gap
//! between COMMIT-visible and the bump; bump-before-commit lets a capture whose
//! query ran pre-COMMIT stamp stale-as-fresh. Neither is no-stale-safe alone.
//!
//! So each slot version is a **seqlock** the writer brackets around the commit:
//!
//! 1. **Pre-commit:** [`ResultMemo::slot_dirty_begin`] — `version` even→odd
//!    ("write in progress"). This must run *before* the frame COMMIT.
//! 2. **Post-commit:** [`ResultMemo::slot_dirty_end`] — `version` odd→even (a new
//!    stable version). This must run *after* the frame COMMIT.
//!
//! - **Serve:** a memo is live iff every stamped version still equals the current
//!   version. Stamps are always even, so equality implies a stable version.
//! - **Capture:** [`ResultMemo::slots_stamp`] returns `None` if any dependency
//!   slot is odd (mid-write), so a capture never stamps a pending version.
//!
//! The odd (pending) interval brackets the visibility transition on both ends:
//! existing memos over R miss from the moment pre-commit fires (a slight, bounded
//! over-invalidation), and captures refuse to stamp during the window. Every stale
//! path resolves to a conservative miss, never a stale serve.
//!
//! The writer is single-threaded and processes frames serially, so `begin`/`end`
//! are balanced per relation per frame and the version never sticks odd.

use crate::query::Fingerprint;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use bytes::{Bytes, BytesMut};
use dashmap::DashMap;

use crate::query::ast::{LimitClause, LiteralValue};
use crate::settings::DynamicConfigHandle;

/// Per-entry size cap. Results above this are never memoized — the in-memory
/// backend targets the small/stable results that dominate source-row traffic.
const MAX_MEMO_ENTRY_BYTES: usize = 128 * 1024;

/// A fingerprint is memoized only after this many hits, so one-shot queries
/// don't churn the bounded store. Hits are counted at dispatch (every serve),
/// so the Nth serve is the first capture candidate.
pub const MEMO_CAPTURE_MIN_HITS: u64 = 4;

/// A dependency slot a memoized result is invalidated against. The CDC path
/// bumps slots; a memo stamps the slots it read and is served only while every
/// stamped epoch still matches.
///
/// Rung 1 uses [`SlotKey::Relation`] exclusively. The remaining variants are the
/// rung-2 (column-aware) refinement and are not yet bumped or stamped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SlotKey {
    /// Any change (insert/update/delete) to this relation evicts. Rung 1.
    Relation(u32),
    /// A membership change (INSERT/DELETE) on this relation. Rung 2: a snapshot
    /// can always grow on insert / shrink on delete, so these always evict.
    RelationMembership(u32),
    /// A specific column of this relation changed (`(relation_oid, attnum)`).
    /// Rung 2: evict only memos that read this column (SELECT list or WHERE).
    RelationColumn(u32, u32),
}

/// The result-shape component of a memo key. Cross-shape slicing is unsafe
/// without `ORDER BY`, so v1 serves only the exact captured shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MemoShape {
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

impl MemoShape {
    /// Derive the shape key from a request's LIMIT/OFFSET. Returns `None` when
    /// either is present but not an integer literal (post-bind these are
    /// literals; a non-literal is rare and simply not memoized).
    pub fn from_limit(limit: &Option<LimitClause>) -> Option<Self> {
        let Some(clause) = limit else {
            return Some(MemoShape {
                limit: None,
                offset: None,
            });
        };
        let limit = match &clause.count {
            None => None,
            Some(LiteralValue::Integer(n)) => Some(u64::try_from(*n).ok()?),
            Some(_) => return None,
        };
        let offset = match &clause.offset {
            None => None,
            Some(LiteralValue::Integer(n)) => Some(u64::try_from(*n).ok()?),
            Some(_) => return None,
        };
        Some(MemoShape { limit, offset })
    }
}

/// Identity of a memoized result. A query is materialized in at most one memo
/// per `(result format, shape)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MemoKey {
    pub fingerprint: Fingerprint,
    /// Result format negotiated at Bind: text (`false`) vs binary (`true`).
    pub binary: bool,
    pub shape: MemoShape,
}

/// A captured result snapshot: client-ready core wire bytes plus the slot epochs
/// it was valid against.
#[derive(Debug, Clone)]
pub struct MemoEntry {
    /// `RowDescription` + `DataRow*` + `CommandComplete`, as a single contiguous
    /// buffer. The per-client envelope (Parse/Bind/RFQ) is regenerated per serve
    /// and is not stored here.
    pub core: Bytes,
    /// Byte length of the leading `RowDescription` frame within `core` (`0` if
    /// none was captured). Serve writes the whole buffer for a request that
    /// wants a RowDescription, or `core[rd_len..]` for one that does not.
    pub rd_len: usize,
    /// Slots stamped at capture; the entry is live while all still match.
    pub stamped: Box<[(SlotKey, u64)]>,
}

impl MemoEntry {
    fn size(&self) -> usize {
        self.core.len()
    }
}

/// A live memo lookup result, ready to serve.
#[derive(Debug, Clone)]
pub struct MemoHit {
    pub core: Bytes,
    pub rd_len: usize,
}

/// In-process hot-result store. Shared (via `CacheStateView`) across the writer
/// (slot bump), worker (capture), and connection dispatch (serve).
#[derive(Debug)]
pub struct ResultMemo {
    entries: DashMap<MemoKey, MemoEntry>,
    slot_epochs: DashMap<SlotKey, u64>,
    total_bytes: AtomicUsize,
    /// Set when a slot is bumped (an entry may have become stale); cleared by
    /// `gc`. Lets the periodic sweep skip the full scan when nothing changed
    /// since the last sweep — the read-heavy/write-light case the memo targets.
    gc_pending: AtomicBool,
    dynamic: DynamicConfigHandle,
}

impl ResultMemo {
    pub fn new(dynamic: DynamicConfigHandle) -> Self {
        Self {
            entries: DashMap::new(),
            slot_epochs: DashMap::new(),
            total_bytes: AtomicUsize::new(0),
            gc_pending: AtomicBool::new(false),
            dynamic,
        }
    }

    /// Total-bytes budget from dynamic config. `0` disables memoization.
    pub fn budget(&self) -> usize {
        self.dynamic.load().memo_cache_size
    }

    pub fn enabled(&self) -> bool {
        self.budget() > 0
    }

    /// Enter the pending (odd) phase of a slot's seqlock: `version` even→odd.
    /// Writer/CDC path — call this *before* the frame COMMIT for every relation
    /// the frame touches. Existing memos over the slot stop being served at once.
    pub fn slot_dirty_begin(&self, slot: SlotKey) {
        *self.slot_epochs.entry(slot).or_insert(0) += 1;
        // An entry may now be stale; arm the next gc sweep.
        self.gc_pending.store(true, Ordering::Relaxed);
    }

    /// Leave the pending phase, publishing a new stable version: `version`
    /// odd→even. Writer/CDC path — call this *after* the frame COMMIT. Balances
    /// the preceding [`slot_dirty_begin`](Self::slot_dirty_begin).
    pub fn slot_dirty_end(&self, slot: SlotKey) {
        *self.slot_epochs.entry(slot).or_insert(0) += 1;
    }

    /// Current seqlock version of a slot (`0` if never touched; even = stable,
    /// odd = a write is in progress).
    pub fn slot_version(&self, slot: SlotKey) -> u64 {
        self.slot_epochs.get(&slot).map(|e| *e).unwrap_or(0)
    }

    /// Snapshot the current versions of `slots` (worker/capture path), to be
    /// called *before* issuing the serve query whose bytes will be captured.
    /// Returns `None` if any slot is mid-write (odd) — capture must skip rather
    /// than stamp a pending version.
    pub fn slots_stamp(&self, slots: &[SlotKey]) -> Option<Box<[(SlotKey, u64)]>> {
        slots
            .iter()
            .map(|&slot| {
                let v = self.slot_version(slot);
                v.is_multiple_of(2).then_some((slot, v))
            })
            .collect()
    }

    /// Whether every stamped version still matches the current version. Stamps are
    /// always even, so equality implies the slot is still on that stable version.
    pub fn slots_valid(&self, stamped: &[(SlotKey, u64)]) -> bool {
        stamped
            .iter()
            .all(|&(slot, version)| self.slot_version(slot) == version)
    }

    /// Look up a live memo. Stale entries (a stamped version has advanced) are
    /// dropped lazily and reported as a miss.
    pub fn get(&self, key: &MemoKey) -> Option<MemoHit> {
        {
            let entry = self.entries.get(key)?;
            if self.slots_valid(&entry.stamped) {
                return Some(MemoHit {
                    core: entry.core.clone(),
                    rd_len: entry.rd_len,
                });
            }
        } // drop the read guard before taking the write lock below

        // Stale: evict — but only if it's *still* stale under the write lock, so
        // a capture that re-inserted a fresh entry for this key in the gap isn't
        // clobbered.
        if let Some((_, removed)) = self
            .entries
            .remove_if(key, |_, v| !self.slots_valid(&v.stamped))
        {
            self.bytes_reclaim(removed.size());
        }
        None
    }

    /// Insert a captured snapshot. Rejected (returns `false`) when memoization is
    /// disabled, the entry exceeds the per-entry cap, or it would exceed the
    /// total budget. Replacing an existing key reuses its byte accounting.
    pub fn insert(&self, key: MemoKey, entry: MemoEntry) -> bool {
        let size = entry.size();
        let budget = self.budget();
        if budget == 0 || size > MAX_MEMO_ENTRY_BYTES {
            return false;
        }

        // `total_bytes` is read non-atomically w.r.t. concurrent `remove`
        // (lazy eviction on the dispatch thread, gc on the writer), so a remove
        // of this key landing between the `get` above and this load would make a
        // plain subtraction underflow (panic under overflow-checks). Saturate;
        // the budget is a soft cap and the authoritative accounting below is the
        // per-op atomic add/sub keyed on the actual map mutation.
        let prev_size = self.entries.get(&key).map(|e| e.size());
        let projected = self
            .total_bytes
            .load(Ordering::Relaxed)
            .saturating_sub(prev_size.unwrap_or(0))
            + size;
        if projected > budget {
            return false;
        }

        if let Some(prev) = self.entries.insert(key, entry) {
            self.total_bytes.fetch_sub(prev.size(), Ordering::Relaxed);
        }
        self.total_bytes.fetch_add(size, Ordering::Relaxed);
        crate::metrics::handles().cache.memo_captures.increment(1);
        true
    }

    /// Remove an entry, returning its byte accounting to the budget.
    pub fn remove(&self, key: &MemoKey) {
        if let Some((_, entry)) = self.entries.remove(key) {
            self.bytes_reclaim(entry.size());
        }
    }

    /// Return a removed entry's bytes to the budget and count the eviction.
    fn bytes_reclaim(&self, size: usize) {
        self.total_bytes.fetch_sub(size, Ordering::Relaxed);
        crate::metrics::handles().cache.memo_evictions.increment(1);
    }

    /// Publish the current entry count and byte total as gauges. Called on the
    /// writer's periodic gauge tick rather than per-mutation.
    pub fn metrics_publish(&self) {
        let cache = &crate::metrics::handles().cache;
        #[allow(clippy::cast_precision_loss)]
        cache.memo_entries.set(self.entries.len() as f64);
        #[allow(clippy::cast_precision_loss)]
        cache.memo_bytes.set(self.total_bytes() as f64);
    }

    /// Sweep entries whose stamped versions have advanced, reclaiming their
    /// bytes. Skips the full scan entirely when no slot has been bumped since the
    /// last sweep (lazy `get`-eviction already reclaims accessed stale entries;
    /// this only catches stale entries never re-requested).
    pub fn gc(&self) {
        if !self.gc_pending.swap(false, Ordering::Relaxed) {
            return;
        }
        let stale: Vec<MemoKey> = self
            .entries
            .iter()
            .filter(|e| !self.slots_valid(&e.stamped))
            .map(|e| *e.key())
            .collect();
        for key in stale {
            self.remove(&key);
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn total_bytes(&self) -> usize {
        self.total_bytes.load(Ordering::Relaxed)
    }
}

/// In-flight capture of a serve's core bytes on the worker. Created *before* the
/// serve query is issued (so the stamped versions precede the query snapshot),
/// fed the `RowDescription`/`DataRow*`/`CommandComplete` frames as they relay,
/// and finalized after a clean serve.
///
/// Correctness: the stamp is taken at `begin`; `finish` re-checks the slots and
/// only inserts if none advanced across the whole capture. So an inserted memo's
/// bytes provably reflect exactly the stamped version — any CDC change to a read
/// relation during the serve drops the capture rather than storing a snapshot
/// that might predate it.
pub struct MemoCapture {
    key: MemoKey,
    stamped: Box<[(SlotKey, u64)]>,
    buf: BytesMut,
    rd_len: usize,
    /// Set once the accumulated bytes exceed the per-entry cap; the capture is
    /// then a no-op through `finish`.
    aborted: bool,
}

impl MemoCapture {
    /// Begin a capture for `key` over `relation_oids`. Returns `None` when
    /// memoization is disabled or any read relation is mid-write (odd slot) —
    /// the caller skips capturing and serves normally.
    pub fn begin(memo: &ResultMemo, key: MemoKey, relation_oids: &[u32]) -> Option<Self> {
        if !memo.enabled() {
            return None;
        }
        let slots: Vec<SlotKey> = relation_oids
            .iter()
            .map(|&oid| SlotKey::Relation(oid))
            .collect();
        let stamped = memo.slots_stamp(&slots)?;
        Some(MemoCapture {
            key,
            stamped,
            buf: BytesMut::new(),
            rd_len: 0,
            aborted: false,
        })
    }

    fn append(&mut self, data: &[u8]) {
        if self.aborted {
            return;
        }
        if self.buf.len() + data.len() > MAX_MEMO_ENTRY_BYTES {
            // Too large to memoize — drop what we have and stop accumulating.
            self.aborted = true;
            self.buf = BytesMut::new();
            return;
        }
        self.buf.extend_from_slice(data);
    }

    /// Record the leading `RowDescription` frame (must precede any data).
    pub fn row_description_push(&mut self, data: &[u8]) {
        self.append(data);
        if !self.aborted {
            self.rd_len = self.buf.len();
        }
    }

    /// Record a `DataRow` (or batched data) frame.
    pub fn data_push(&mut self, data: &[u8]) {
        self.append(data);
    }

    /// Record the trailing `CommandComplete` frame.
    pub fn command_complete_push(&mut self, data: &[u8]) {
        self.append(data);
    }

    /// Finalize: insert the captured snapshot iff it wasn't aborted and no read
    /// relation changed since `begin`. Returns whether an entry was stored.
    pub fn finish(self, memo: &ResultMemo) -> bool {
        if self.aborted || !memo.slots_valid(&self.stamped) {
            return false;
        }
        memo.insert(
            self.key,
            MemoEntry {
                core: self.buf.freeze(),
                rd_len: self.rd_len,
                stamped: self.stamped,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn memo(budget: usize) -> ResultMemo {
        let dynamic = DynamicConfigHandle::test_default();
        let mut cfg = (**dynamic.load()).clone();
        cfg.memo_cache_size = budget;
        dynamic.update(cfg);
        ResultMemo::new(dynamic)
    }

    fn entry(bytes: &[u8], stamped: &[(SlotKey, u64)]) -> MemoEntry {
        MemoEntry {
            core: Bytes::copy_from_slice(bytes),
            rd_len: 0,
            stamped: stamped.into(),
        }
    }

    fn key(fp: u64) -> MemoKey {
        MemoKey {
            fingerprint: Fingerprint::from_raw(fp),
            binary: false,
            shape: MemoShape {
                limit: None,
                offset: None,
            },
        }
    }

    fn stamp(m: &ResultMemo, slots: &[SlotKey]) -> Box<[(SlotKey, u64)]> {
        m.slots_stamp(slots).expect("slots stable")
    }

    #[test]
    fn test_slot_stamp_then_valid() {
        let m = memo(1 << 20);
        let slots = [SlotKey::Relation(10), SlotKey::Relation(20)];
        let stamped = stamp(&m, &slots);
        assert_eq!(
            *stamped,
            [(SlotKey::Relation(10), 0), (SlotKey::Relation(20), 0)]
        );
        assert!(m.slots_valid(&stamped), "unchanged versions stay valid");
    }

    #[test]
    fn test_dirty_cycle_invalidates_stamp() {
        let m = memo(1 << 20);
        let slots = [SlotKey::Relation(10)];
        let stamped = stamp(&m, &slots);
        m.slot_dirty_begin(SlotKey::Relation(10));
        m.slot_dirty_end(SlotKey::Relation(10));
        assert!(!m.slots_valid(&stamped), "a write cycle busts the stamp");
        // An unrelated relation's change does not bust it.
        let fresh = stamp(&m, &slots);
        m.slot_dirty_begin(SlotKey::Relation(99));
        m.slot_dirty_end(SlotKey::Relation(99));
        assert!(m.slots_valid(&fresh), "unrelated slot is independent");
    }

    #[test]
    fn test_stamp_skips_pending_slot() {
        let m = memo(1 << 20);
        let slots = [SlotKey::Relation(10), SlotKey::Relation(20)];
        m.slot_dirty_begin(SlotKey::Relation(20)); // odd: write in progress
        assert!(
            m.slots_stamp(&slots).is_none(),
            "capture refuses to stamp while a dependency slot is mid-write"
        );
        m.slot_dirty_end(SlotKey::Relation(20)); // even again
        assert!(
            m.slots_stamp(&slots).is_some(),
            "stable once write completes"
        );
    }

    #[test]
    fn test_existing_memo_misses_during_pending_window() {
        // The serve-side window: an existing memo must stop being served the
        // instant the writer enters the pending phase, before COMMIT visibility.
        let m = memo(1 << 20);
        let k = key(1);
        m.insert(k, entry(b"snapshot", &[(SlotKey::Relation(10), 0)]));
        m.slot_dirty_begin(SlotKey::Relation(10)); // pre-commit, not yet ended
        assert!(m.get(&k).is_none(), "pending (odd) version busts the memo");
    }

    #[test]
    fn test_insert_get_roundtrip() {
        let m = memo(1 << 20);
        let k = key(1);
        assert!(m.insert(k, entry(b"core-bytes", &[(SlotKey::Relation(10), 0)])));
        let hit = m.get(&k).expect("present and live");
        assert_eq!(&hit.core[..], b"core-bytes");
        assert_eq!(m.total_bytes(), b"core-bytes".len());
    }

    #[test]
    fn test_get_evicts_after_dirty_cycle() {
        let m = memo(1 << 20);
        let k = key(1);
        m.insert(k, entry(b"snapshot", &[(SlotKey::Relation(10), 0)]));
        m.slot_dirty_begin(SlotKey::Relation(10));
        m.slot_dirty_end(SlotKey::Relation(10));
        assert!(m.get(&k).is_none(), "stale entry not served");
        assert_eq!(m.total_bytes(), 0, "stale entry reclaimed lazily on get");
        assert!(m.is_empty());
    }

    #[test]
    fn test_per_entry_cap_rejects() {
        let m = memo(1 << 30);
        let big = vec![0u8; MAX_MEMO_ENTRY_BYTES + 1];
        assert!(
            !m.insert(key(1), entry(&big, &[])),
            "over per-entry cap rejected"
        );
        assert!(m.is_empty());
    }

    #[test]
    fn test_total_budget_rejects() {
        let m = memo(16);
        assert!(m.insert(key(1), entry(b"0123456789", &[])), "fits");
        assert!(
            !m.insert(key(2), entry(b"0123456789", &[])),
            "would exceed budget"
        );
        assert_eq!(m.total_bytes(), 10);
    }

    #[test]
    fn test_disabled_when_budget_zero() {
        let m = memo(0);
        assert!(!m.enabled());
        assert!(
            !m.insert(key(1), entry(b"x", &[])),
            "disabled rejects insert"
        );
    }

    #[test]
    fn test_insert_replace_reuses_accounting() {
        let m = memo(32);
        m.insert(key(1), entry(b"0123456789", &[]));
        assert!(
            m.insert(key(1), entry(b"abcde", &[])),
            "replacing the same key nets the size delta, not a fresh add"
        );
        assert_eq!(m.total_bytes(), 5);
        assert_eq!(m.len(), 1);
    }

    #[test]
    fn test_get_serves_fresh_reinsert_over_stale() {
        let m = memo(1 << 20);
        let k = key(1);
        // Entry stamped at version 0, then the slot advances → stale.
        m.insert(k, entry(b"old", &[(SlotKey::Relation(10), 0)]));
        m.slot_dirty_begin(SlotKey::Relation(10));
        m.slot_dirty_end(SlotKey::Relation(10)); // slot now 2
        // A capture re-inserts a fresh entry for the same key at the new version.
        m.insert(k, entry(b"new", &[(SlotKey::Relation(10), 2)]));
        // get serves the fresh entry and does not evict it.
        assert_eq!(&m.get(&k).expect("fresh served").core[..], b"new");
        assert!(m.get(&k).is_some(), "fresh entry not clobbered by get");
    }

    #[test]
    fn test_gc_skips_when_unarmed() {
        let m = memo(1 << 20);
        m.insert(key(1), entry(b"x", &[(SlotKey::Relation(10), 0)]));
        // No slot bumped since insert → gc is a no-op, entry survives.
        m.gc();
        assert!(m.get(&key(1)).is_some(), "entry survives an unarmed gc");
        // A bump arms gc; the now-stale entry is swept.
        m.slot_dirty_begin(SlotKey::Relation(10));
        m.slot_dirty_end(SlotKey::Relation(10));
        m.gc();
        assert!(m.is_empty(), "stale entry swept once gc is armed");
    }

    #[test]
    fn test_gc_sweeps_stale() {
        let m = memo(1 << 20);
        m.insert(key(1), entry(b"aaa", &[(SlotKey::Relation(10), 0)]));
        m.insert(key(2), entry(b"bbb", &[(SlotKey::Relation(20), 0)]));
        m.slot_dirty_begin(SlotKey::Relation(10));
        m.slot_dirty_end(SlotKey::Relation(10));
        m.gc();
        assert!(m.get(&key(2)).is_some(), "live entry survives gc");
        assert_eq!(m.len(), 1);
        assert_eq!(m.total_bytes(), 3);
    }

    fn int_limit(count: Option<i64>, offset: Option<i64>) -> Option<LimitClause> {
        Some(LimitClause {
            count: count.map(LiteralValue::Integer),
            offset: offset.map(LiteralValue::Integer),
        })
    }

    #[test]
    fn test_memo_shape_from_limit() {
        assert_eq!(
            MemoShape::from_limit(&None),
            Some(MemoShape {
                limit: None,
                offset: None
            })
        );
        assert_eq!(
            MemoShape::from_limit(&int_limit(Some(10), Some(5))),
            Some(MemoShape {
                limit: Some(10),
                offset: Some(5)
            })
        );
        // Non-integer literal → not keyable → skip.
        let text_limit = Some(LimitClause {
            count: Some(LiteralValue::String("x".into())),
            offset: None,
        });
        assert_eq!(MemoShape::from_limit(&text_limit), None);
    }

    #[test]
    fn test_capture_roundtrip_and_rd_len() {
        let m = memo(1 << 20);
        let mut cap =
            MemoCapture::begin(&m, key(1), &[10, 20]).expect("relations stable, memo enabled");
        cap.row_description_push(b"RD");
        cap.data_push(b"row1");
        cap.data_push(b"row2");
        cap.command_complete_push(b"CC");
        assert!(cap.finish(&m), "clean capture inserts");

        let hit = m.get(&key(1)).expect("memoized");
        assert_eq!(&hit.core[..], b"RDrow1row2CC");
        assert_eq!(hit.rd_len, 2, "RowDescription length recorded");
    }

    #[test]
    fn test_capture_dropped_when_relation_changes_mid_serve() {
        let m = memo(1 << 20);
        let mut cap = MemoCapture::begin(&m, key(1), &[10]).expect("stable at begin");
        cap.row_description_push(b"RD");
        cap.data_push(b"row");
        cap.command_complete_push(b"CC");
        // A CDC frame touches relation 10 during the serve.
        m.slot_dirty_begin(SlotKey::Relation(10));
        m.slot_dirty_end(SlotKey::Relation(10));
        assert!(
            !cap.finish(&m),
            "stamp advanced → capture dropped, not stored"
        );
        assert!(m.get(&key(1)).is_none());
    }

    #[test]
    fn test_capture_skips_when_relation_mid_write() {
        let m = memo(1 << 20);
        m.slot_dirty_begin(SlotKey::Relation(10)); // odd: write in progress
        assert!(
            MemoCapture::begin(&m, key(1), &[10]).is_none(),
            "no capture started while a read relation is mid-write"
        );
    }

    #[test]
    fn test_capture_aborts_over_cap() {
        let m = memo(1 << 30);
        let mut cap = MemoCapture::begin(&m, key(1), &[10]).expect("enabled");
        cap.row_description_push(b"RD");
        cap.data_push(&vec![0u8; MAX_MEMO_ENTRY_BYTES + 1]);
        assert!(!cap.finish(&m), "oversized capture is dropped");
        assert!(m.is_empty());
    }

    #[test]
    fn test_capture_disabled_returns_none() {
        let m = memo(0);
        assert!(MemoCapture::begin(&m, key(1), &[10]).is_none());
    }
}
