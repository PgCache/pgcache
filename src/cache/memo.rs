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
//! A memo stamps its own [`SlotKey::Memo`]; the CDC frame bumps it for every
//! memo-eligible query the eval-index candidate probe returns for a changed
//! row, new and old image alike (ADR-045), minus the candidates an in-place
//! UPDATE provably left untouched because it changed no column the query reads
//! (PGC-247). [`SlotKey::Relation`] is bumped for every touched relation as
//! the capture-window guard only.
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
//!
//! ## Slot lifetime: versions never decrease
//!
//! A `Memo` slot lives exactly as long as its fingerprint is capturable: the
//! writer creates it as the query turns memo-eligible and removes it when the
//! query is evicted or fails. A missing `Memo` slot reads [`ResultMemo`]'s
//! floor rather than 0, and both create and remove first raise the floor above
//! every version ever written. So the version a slot reads never decreases, a
//! stamp matches only while its slot has not moved past it, and a removal can
//! only turn a stamp stale — never make a stale stamp valid again, whatever
//! entries or captures are in flight. A missing `Memo` slot is never stamped,
//! so every stamp sits below every later floor. Relation slots are never
//! removed, so they start at 0 and a floor raise leaves them alone.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use bytes::{Bytes, BytesMut};
use dashmap::DashMap;

use crate::cache::output_settings::OutputProfile;
use crate::oid::Oid;
use crate::query::Fingerprint;
use crate::query::ast::{LimitClause, LiteralValue};
use crate::settings::DynamicConfigHandle;

/// Per-entry size cap. Results above this are never memoized — the in-memory
/// backend targets the small/stable results that dominate source-row traffic.
const MAX_MEMO_ENTRY_BYTES: usize = 128 * 1024;

/// A fingerprint is memoized from its first eligible serve. Capture only happens
/// for already-cacheable source-row serves and the store is byte-bounded, so a
/// higher gate mainly delays relief: under a high-cardinality registration storm
/// it leaves most cache hits on the serve pool until each key clears the gate.
/// Capturing on the first serve maximizes serve-pool relief (PGC-277).
pub(crate) const MEMO_CAPTURE_MIN_HITS: u64 = 1;

/// A dependency slot a memoized result is invalidated against. The CDC path
/// bumps slots; a memo stamps the slots it read and is served only while every
/// stamped epoch still matches.
///
/// Rung 3 (PGC-248) stamps [`SlotKey::Memo`] per fingerprint as the entry's only
/// serve-time dependency, so a change evicts only the memos it actually affects.
/// The CDC frame bumps `Memo(F)` for every memo-eligible candidate of its rows,
/// whether or not an entry exists yet, so a capture that lands mid-frame is
/// caught by the same slot it stamped (PGC-350).
/// [`SlotKey::Relation`] is retained as the *capture-window guard*: the CDC frame
/// still bumps it for every touched relation (begin/end), and a capture re-checks
/// it at finish, so a frame committing mid-capture drops the snapshot even when
/// the change leaves the fingerprint's own slot alone. It is NOT stored in
/// `MemoEntry`, so it never busts a stored entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SlotKey {
    /// Per-fingerprint eviction slot (rung 3). The only slot a memo stamps.
    Memo(Fingerprint),
    /// Capture-window guard: bumped for every touched relation each frame and
    /// re-checked at capture finish, but not stored in the entry (rung 3).
    Relation(Oid),
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
    /// The rendering settings the bytes were produced under (PGC-287).
    pub profile: OutputProfile,
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
    /// Slots stamped at capture; the entry is live while all still match. Holds
    /// only the [`SlotKey::Memo`] slot (rung 3) — never a `Relation` guard slot.
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
    /// Version of every missing `Memo` slot, and the version one is created at.
    /// Raised above `version_max` by every `Memo` slot create and remove.
    slot_floor: AtomicU64,
    /// Highest version any `Memo` slot has been bumped to. Writer only.
    version_max: AtomicU64,
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
            slot_floor: AtomicU64::new(0),
            version_max: AtomicU64::new(0),
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

    /// Whether a frame must bracket the slots of the relations it touches.
    /// An empty store is not "no reader": a capture can be in flight on a
    /// worker, stamped against the current versions, and it stores its
    /// snapshot at finish unless a slot moved. A capture begins only while
    /// enabled, and one that outlives a disable is refused at insert, so the
    /// bracket is needed exactly while enabled or while entries exist.
    pub fn bracket_wanted(&self) -> bool {
        self.enabled() || !self.is_empty()
    }

    /// Enter the pending (odd) phase of a slot's seqlock: `version` even→odd.
    /// Writer/CDC path — call this *before* the frame COMMIT for every relation
    /// the frame touches. Existing memos over the slot stop being served at once.
    pub fn slot_dirty_begin(&self, slot: SlotKey) {
        self.slot_advance(slot);
        // An entry may now be stale; arm the next gc sweep.
        self.gc_pending.store(true, Ordering::Relaxed);
    }

    /// Leave the pending phase, publishing a new stable version: `version`
    /// odd→even. Writer/CDC path — call this *after* the frame COMMIT. Balances
    /// the preceding [`slot_dirty_begin`](Self::slot_dirty_begin).
    pub fn slot_dirty_end(&self, slot: SlotKey) {
        // Only an open bracket closes: a slot created even mid-bracket would
        // otherwise stick odd and never be captured again.
        let Some(mut version) = self.slot_epochs.get_mut(&slot) else {
            return;
        };
        if version.is_multiple_of(2) {
            return;
        }
        *version += 1;
        self.version_max_track(slot, *version);
    }

    fn slot_advance(&self, slot: SlotKey) {
        let mut version = self
            .slot_epochs
            .entry(slot)
            .or_insert_with(|| self.slot_base(slot));
        *version += 1;
        self.version_max_track(slot, *version);
    }

    /// Only `Memo` slots read the floor, so only their versions bound it.
    fn version_max_track(&self, slot: SlotKey, version: u64) {
        if matches!(slot, SlotKey::Memo(_)) && version > self.version_max.load(Ordering::Relaxed) {
            self.version_max.store(version, Ordering::Relaxed);
        }
    }

    /// Current seqlock version of a slot (its base if it has no entry; even =
    /// stable, odd = a write is in progress).
    pub fn slot_version(&self, slot: SlotKey) -> u64 {
        self.slot_epochs
            .get(&slot)
            .map_or_else(|| self.slot_base(slot), |e| *e)
    }

    /// Version of a slot with no entry: the floor for a `Memo` slot, 0 for the
    /// never-removed relation slot.
    fn slot_base(&self, slot: SlotKey) -> u64 {
        match slot {
            SlotKey::Memo(_) => self.floor(),
            SlotKey::Relation(_) => 0,
        }
    }

    fn floor(&self) -> u64 {
        self.slot_floor.load(Ordering::Acquire)
    }

    /// Snapshot the current versions of `slots` (worker/capture path), to be
    /// called *before* issuing the serve query whose bytes will be captured.
    /// Returns `None` if any slot is mid-write (odd) — capture must skip rather
    /// than stamp a pending version — or a `Memo` slot is missing: a capturable
    /// query always has one, so its query is retired and a stamp at the floor
    /// could outlive it.
    pub fn slots_stamp(&self, slots: &[SlotKey]) -> Option<Box<[(SlotKey, u64)]>> {
        slots
            .iter()
            .map(|&slot| {
                let version = match slot {
                    SlotKey::Memo(_) => *self.slot_epochs.get(&slot)?,
                    SlotKey::Relation(_) => self.slot_version(slot),
                };
                version.is_multiple_of(2).then_some((slot, version))
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

    /// True if any *live* memo entry exists for `fingerprint` (across result
    /// formats/shapes) — i.e. a matching request would currently serve from the
    /// in-process memo without a cache-DB round trip. Read-only: unlike
    /// [`get`](Self::get) it neither evicts stale entries nor records metrics
    /// (used by `pgcache_explain` diagnostics).
    pub fn fingerprint_memoized(&self, fingerprint: Fingerprint) -> bool {
        self.entries
            .iter()
            .any(|entry| entry.key().fingerprint == fingerprint && self.slots_valid(&entry.stamped))
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
        #[allow(clippy::cast_precision_loss)]
        cache.memo_slots.set(self.slot_epochs.len() as f64);
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

    /// Create a fingerprint's `Memo` slot as the query turns memo-eligible.
    /// Writer only. Raising the floor first puts the slot above any stamp taken
    /// while it was missing, such as a capture that passed the serve gate just
    /// before the fingerprint's last eviction.
    pub fn memo_slot_create(&self, fingerprint: Fingerprint) {
        let slot = SlotKey::Memo(fingerprint);
        if self.slot_epochs.contains_key(&slot) {
            return;
        }
        let floor = self.floor_raise();
        self.slot_epochs.insert(slot, floor);
    }

    /// Remove a fingerprint's `Memo` slot once the query can no longer be
    /// captured. Writer only. The floor is raised first, so the missing slot
    /// reads above every stamp taken against it and its entries and in-flight
    /// captures turn stale. Safe mid-bracket: the raise covers an odd version.
    pub fn memo_slot_remove(&self, fingerprint: Fingerprint) {
        self.floor_raise();
        self.slot_epochs.remove(&SlotKey::Memo(fingerprint));
        self.gc_pending.store(true, Ordering::Relaxed);
    }

    fn floor_raise(&self) -> u64 {
        let version_max = self.version_max.load(Ordering::Relaxed).max(self.floor());
        let floor = (version_max + 2) & !1;
        self.slot_floor.store(floor, Ordering::Release);
        floor
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
pub(crate) struct MemoCapture {
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
    pub(crate) fn begin(memo: &ResultMemo, key: MemoKey, relation_oids: &[Oid]) -> Option<Self> {
        if !memo.enabled() {
            return None;
        }
        // Stamp the per-fingerprint `Memo` slot (the serve-time dependency) plus
        // every read relation's `Relation` guard slot; `finish` re-checks the
        // whole set, so a frame committing mid-capture on a read relation drops
        // the snapshot even when the change leaves this fingerprint's slot alone.
        let mut slots: Vec<SlotKey> = relation_oids
            .iter()
            .map(|&oid| SlotKey::Relation(oid))
            .collect();
        slots.push(SlotKey::Memo(key.fingerprint));
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
    pub(crate) fn row_description_push(&mut self, data: &[u8]) {
        self.append(data);
        if !self.aborted {
            self.rd_len = self.buf.len();
        }
    }

    /// Record a `DataRow` (or batched data) frame.
    pub(crate) fn data_push(&mut self, data: &[u8]) {
        self.append(data);
    }

    /// Record the trailing `CommandComplete` frame.
    pub(crate) fn command_complete_push(&mut self, data: &[u8]) {
        self.append(data);
    }

    /// Finalize: insert the captured snapshot iff it wasn't aborted and no read
    /// relation changed since `begin`. Returns whether an entry was stored.
    pub(crate) fn finish(self, memo: &ResultMemo) -> bool {
        if self.aborted || !memo.slots_valid(&self.stamped) {
            return false;
        }
        // The entry stamps ONLY its `Memo` slot — serving is per-fingerprint
        // precise. The `Relation` versions in `self.stamped` were the
        // capture-window guard (re-checked above), not a serve dependency.
        let stamped: Box<[(SlotKey, u64)]> = self
            .stamped
            .iter()
            .copied()
            .filter(|(slot, _)| matches!(slot, SlotKey::Memo(_)))
            .collect();
        memo.insert(
            self.key,
            MemoEntry {
                core: self.buf.freeze(),
                rd_len: self.rd_len,
                stamped,
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
            profile: OutputProfile::default(),
        }
    }

    fn stamp(m: &ResultMemo, slots: &[SlotKey]) -> Box<[(SlotKey, u64)]> {
        m.slots_stamp(slots).expect("slots stable")
    }

    #[test]
    fn test_capture_stamps_only_memo_slot() {
        let m = memo(1 << 20);
        m.memo_slot_create(Fingerprint::from_raw(7));
        let oid = Oid::from_raw(10);
        let k = key(7);

        let mut cap = MemoCapture::begin(&m, k, &[oid]).expect("capture begins");
        cap.row_description_push(b"rd");
        cap.command_complete_push(b"cc");
        assert!(cap.finish(&m), "capture finishes");

        // Stored entry depends only on its per-fingerprint Memo slot — never a
        // Relation guard slot.
        let stored = m.entries.get(&k).expect("entry stored");
        assert_eq!(stored.stamped.len(), 1);
        assert_eq!(stored.stamped[0].0, SlotKey::Memo(Fingerprint::from_raw(7)));
    }

    /// PGC-350: a capture that begins and finishes while a frame is replaying
    /// stores a snapshot from before the frame's commit. The frame decided to
    /// bump `Memo(F)` before any entry for F existed, and the bump still has to
    /// kill the entry the capture stored in the meantime.
    #[test]
    fn test_capture_stored_mid_replay_is_busted_by_its_memo_bump() {
        let m = memo(1 << 20);
        m.memo_slot_create(Fingerprint::from_raw(7));
        let oid = Oid::from_raw(10);
        let k = key(7);
        let slot = SlotKey::Memo(k.fingerprint);

        let mut cap = MemoCapture::begin(&m, k, &[oid]).expect("capture begins");
        cap.row_description_push(b"rd");
        cap.command_complete_push(b"cc");
        assert!(cap.finish(&m), "no slot moved yet, so the capture stores");

        m.slot_dirty_begin(slot);
        m.slot_dirty_end(slot);
        assert!(
            m.get(&k).is_none(),
            "the pre-commit snapshot is never served"
        );
    }

    /// The other side of the same race: `finish` validates its stamps, the
    /// frame bumps `Memo(F)`, then the entry lands. It must be dead on arrival.
    #[test]
    fn test_entry_inserted_after_its_memo_bump_is_not_served() {
        let m = memo(1 << 20);
        m.memo_slot_create(Fingerprint::from_raw(7));
        let k = key(7);
        let slot = SlotKey::Memo(k.fingerprint);
        let stamped = stamp(&m, &[slot]);

        m.slot_dirty_begin(slot);
        m.slot_dirty_end(slot);
        assert!(
            m.insert(k, entry(b"stale", &stamped)),
            "insert itself succeeds"
        );
        assert!(m.get(&k).is_none(), "the entry predates the bump");
    }

    /// A frame that commits while the first capture is in flight has no
    /// entry to look at, yet its bracket must still move the relation slot
    /// or the capture stores a snapshot from before the commit.
    #[test]
    fn test_capture_over_an_empty_store_is_dropped_by_the_bracket() {
        let m = memo(1 << 20);
        m.memo_slot_create(Fingerprint::from_raw(7));
        let oid = Oid::from_raw(10);
        assert!(m.is_empty());
        assert!(
            m.bracket_wanted(),
            "an enabled memo may have a capture in flight"
        );
        let mut cap = MemoCapture::begin(&m, key(7), &[oid]).expect("capture begins");
        cap.row_description_push(b"rd");
        cap.command_complete_push(b"cc");
        if m.bracket_wanted() {
            m.slot_dirty_begin(SlotKey::Relation(oid));
            m.slot_dirty_end(SlotKey::Relation(oid));
        }
        assert!(!cap.finish(&m), "the capture predates the commit");
        assert!(m.is_empty());

        let disabled = memo(0);
        assert!(
            !disabled.bracket_wanted(),
            "nothing can capture while disabled"
        );
        assert!(MemoCapture::begin(&disabled, key(7), &[oid]).is_none());
    }

    /// The create-memo vs. frame-update race: a frame that commits on a relation
    /// the in-flight capture reads must drop the capture — even when the frame
    /// does NOT bump that fingerprint's own `Memo` slot (rung 3b leaves it alone
    /// for a change that doesn't match the query). The `Relation` capture-window
    /// guard — stamped at `begin`, re-checked at `finish`, never stored in the
    /// entry — is what closes the race; per-fingerprint slots alone would store a
    /// snapshot predating the commit.
    #[test]
    fn test_capture_dropped_when_read_relation_written_mid_window() {
        let m = memo(1 << 20);
        m.memo_slot_create(Fingerprint::from_raw(7));
        let created = m.slot_version(SlotKey::Memo(Fingerprint::from_raw(7)));
        let oid = Oid::from_raw(10);
        let k = key(7);

        let mut cap = MemoCapture::begin(&m, k, &[oid]).expect("capture begins");
        cap.row_description_push(b"rd");
        cap.command_complete_push(b"cc");

        // A frame commits on the read relation mid-window, bumping only the
        // `Relation` guard — the per-fingerprint `Memo(7)` slot is untouched, as
        // it would be for a non-matching change.
        m.slot_dirty_begin(SlotKey::Relation(oid));
        m.slot_dirty_end(SlotKey::Relation(oid));
        assert_eq!(
            m.slot_version(SlotKey::Memo(Fingerprint::from_raw(7))),
            created,
            "the fingerprint's own Memo slot was not bumped"
        );

        // The capture must drop rather than store a snapshot predating the commit.
        assert!(
            !cap.finish(&m),
            "a capture over a relation written mid-window must drop"
        );
        assert!(m.entries.get(&k).is_none(), "nothing stored");
    }

    #[test]
    fn test_slot_stamp_then_valid() {
        let m = memo(1 << 20);
        let slots = [
            SlotKey::Relation(Oid::from_raw(10)),
            SlotKey::Relation(Oid::from_raw(20)),
        ];
        let stamped = stamp(&m, &slots);
        assert_eq!(
            *stamped,
            [
                (SlotKey::Relation(Oid::from_raw(10)), 0),
                (SlotKey::Relation(Oid::from_raw(20)), 0)
            ]
        );
        assert!(m.slots_valid(&stamped), "unchanged versions stay valid");
    }

    #[test]
    fn test_dirty_cycle_invalidates_stamp() {
        let m = memo(1 << 20);
        let slots = [SlotKey::Relation(Oid::from_raw(10))];
        let stamped = stamp(&m, &slots);
        m.slot_dirty_begin(SlotKey::Relation(Oid::from_raw(10)));
        m.slot_dirty_end(SlotKey::Relation(Oid::from_raw(10)));
        assert!(!m.slots_valid(&stamped), "a write cycle busts the stamp");
        // An unrelated relation's change does not bust it.
        let fresh = stamp(&m, &slots);
        m.slot_dirty_begin(SlotKey::Relation(Oid::from_raw(99)));
        m.slot_dirty_end(SlotKey::Relation(Oid::from_raw(99)));
        assert!(m.slots_valid(&fresh), "unrelated slot is independent");
    }

    #[test]
    fn test_stamp_skips_pending_slot() {
        let m = memo(1 << 20);
        let slots = [
            SlotKey::Relation(Oid::from_raw(10)),
            SlotKey::Relation(Oid::from_raw(20)),
        ];
        m.slot_dirty_begin(SlotKey::Relation(Oid::from_raw(20))); // odd: write in progress
        assert!(
            m.slots_stamp(&slots).is_none(),
            "capture refuses to stamp while a dependency slot is mid-write"
        );
        m.slot_dirty_end(SlotKey::Relation(Oid::from_raw(20))); // even again
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
        m.insert(
            k,
            entry(b"snapshot", &[(SlotKey::Relation(Oid::from_raw(10)), 0)]),
        );
        m.slot_dirty_begin(SlotKey::Relation(Oid::from_raw(10))); // pre-commit, not yet ended
        assert!(m.get(&k).is_none(), "pending (odd) version busts the memo");
    }

    #[test]
    fn test_insert_get_roundtrip() {
        let m = memo(1 << 20);
        let k = key(1);
        assert!(m.insert(
            k,
            entry(b"core-bytes", &[(SlotKey::Relation(Oid::from_raw(10)), 0)])
        ));
        let hit = m.get(&k).expect("present and live");
        assert_eq!(&hit.core[..], b"core-bytes");
        assert_eq!(m.total_bytes(), b"core-bytes".len());
    }

    #[test]
    fn test_get_evicts_after_dirty_cycle() {
        let m = memo(1 << 20);
        let k = key(1);
        m.insert(
            k,
            entry(b"snapshot", &[(SlotKey::Relation(Oid::from_raw(10)), 0)]),
        );
        m.slot_dirty_begin(SlotKey::Relation(Oid::from_raw(10)));
        m.slot_dirty_end(SlotKey::Relation(Oid::from_raw(10)));
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
        m.insert(
            k,
            entry(b"old", &[(SlotKey::Relation(Oid::from_raw(10)), 0)]),
        );
        m.slot_dirty_begin(SlotKey::Relation(Oid::from_raw(10)));
        m.slot_dirty_end(SlotKey::Relation(Oid::from_raw(10))); // slot now 2
        // A capture re-inserts a fresh entry for the same key at the new version.
        m.insert(
            k,
            entry(b"new", &[(SlotKey::Relation(Oid::from_raw(10)), 2)]),
        );
        // get serves the fresh entry and does not evict it.
        assert_eq!(&m.get(&k).expect("fresh served").core[..], b"new");
        assert!(m.get(&k).is_some(), "fresh entry not clobbered by get");
    }

    #[test]
    fn test_gc_skips_when_unarmed() {
        let m = memo(1 << 20);
        m.insert(
            key(1),
            entry(b"x", &[(SlotKey::Relation(Oid::from_raw(10)), 0)]),
        );
        // No slot bumped since insert → gc is a no-op, entry survives.
        m.gc();
        assert!(m.get(&key(1)).is_some(), "entry survives an unarmed gc");
        // A bump arms gc; the now-stale entry is swept.
        m.slot_dirty_begin(SlotKey::Relation(Oid::from_raw(10)));
        m.slot_dirty_end(SlotKey::Relation(Oid::from_raw(10)));
        m.gc();
        assert!(m.is_empty(), "stale entry swept once gc is armed");
    }

    fn slot_bump(m: &ResultMemo, slot: SlotKey) {
        m.slot_dirty_begin(slot);
        m.slot_dirty_end(slot);
    }

    /// A capture validated before its query was evicted, inserted only after the
    /// slot was removed, must not be served.
    #[test]
    fn test_memo_slot_remove_late_insert_not_served() {
        let m = memo(1 << 20);
        let fingerprint = Fingerprint::from_raw(1);
        m.memo_slot_create(fingerprint);
        let stamped = stamp(&m, &[SlotKey::Memo(fingerprint)]);
        assert!(m.slots_valid(&stamped), "capture validated before removal");

        m.memo_slot_remove(fingerprint);
        m.insert(key(1), entry(b"orphan", &stamped));

        assert!(m.get(&key(1)).is_none(), "orphan not served");
    }

    #[test]
    fn test_memo_slot_remove_drops_inflight_capture() {
        let m = memo(1 << 20);
        let fingerprint = Fingerprint::from_raw(1);
        m.memo_slot_create(fingerprint);
        let mut capture =
            MemoCapture::begin(&m, key(1), &[Oid::from_raw(10)]).expect("capture begins");
        capture.command_complete_push(b"C");

        m.memo_slot_remove(fingerprint);

        assert!(!capture.finish(&m), "capture across removal dropped");
        assert!(m.is_empty());
    }

    /// A capture whose serve gate passed just before its query was evicted must
    /// not stamp the missing slot: a floor stamp could outlive a re-registration
    /// that never turns eligible, and so never raises the floor.
    #[test]
    fn test_capture_refused_on_removed_memo_slot() {
        let m = memo(1 << 20);
        let fingerprint = Fingerprint::from_raw(1);
        m.memo_slot_create(fingerprint);
        m.memo_slot_remove(fingerprint);

        assert!(MemoCapture::begin(&m, key(1), &[Oid::from_raw(10)]).is_none());
    }

    #[test]
    fn test_memo_slot_recreated_above_old_stamps() {
        let m = memo(1 << 20);
        let fingerprint = Fingerprint::from_raw(1);
        m.memo_slot_create(fingerprint);
        slot_bump(&m, SlotKey::Memo(fingerprint));
        let old = stamp(&m, &[SlotKey::Memo(fingerprint)]);
        m.insert(key(1), entry(b"old", &old));
        m.memo_slot_remove(fingerprint);

        m.memo_slot_create(fingerprint);

        assert!(m.slot_version(SlotKey::Memo(fingerprint)) > old[0].1);
        assert!(
            m.get(&key(1)).is_none(),
            "old entry not served after re-create"
        );
    }

    /// A query turning eligible inside a frame's bracket gets an even slot; the
    /// bracket's end must not flip it odd.
    #[test]
    fn test_slot_dirty_end_leaves_even_slot() {
        let m = memo(1 << 20);
        let fingerprint = Fingerprint::from_raw(1);
        m.memo_slot_create(fingerprint);
        let created = m.slot_version(SlotKey::Memo(fingerprint));

        m.slot_dirty_end(SlotKey::Memo(fingerprint));
        m.slot_dirty_end(SlotKey::Memo(Fingerprint::from_raw(2)));

        assert_eq!(m.slot_version(SlotKey::Memo(fingerprint)), created);
        assert!(
            !m.slot_epochs
                .contains_key(&SlotKey::Memo(Fingerprint::from_raw(2))),
            "end creates no slot"
        );
    }

    #[test]
    fn test_memo_slot_remove_leaves_others_served() {
        let m = memo(1 << 20);
        let kept = Fingerprint::from_raw(1);
        let removed = Fingerprint::from_raw(2);
        m.memo_slot_create(kept);
        m.memo_slot_create(removed);
        slot_bump(&m, SlotKey::Memo(kept));
        m.insert(key(1), entry(b"one", &stamp(&m, &[SlotKey::Memo(kept)])));
        m.insert(key(2), entry(b"two", &stamp(&m, &[SlotKey::Memo(removed)])));
        let mut capture = MemoCapture::begin(
            &m,
            MemoKey {
                binary: true,
                ..key(1)
            },
            &[Oid::from_raw(10)],
        )
        .expect("capture begins");
        capture.command_complete_push(b"C");

        m.memo_slot_remove(removed);

        assert!(m.get(&key(1)).is_some(), "other entry still served");
        assert!(m.get(&key(2)).is_none(), "removed entry stale");
        assert!(
            capture.finish(&m),
            "capture over an unwritten relation kept"
        );
        assert_eq!(m.slot_epochs.len(), 1, "only the kept slot remains");
    }

    /// Eviction can land inside a frame's bracket: the removed slot must read
    /// an even version above the odd one, and nothing can stamp the odd value.
    #[test]
    fn test_memo_slot_remove_mid_bracket() {
        let m = memo(1 << 20);
        let fingerprint = Fingerprint::from_raw(1);
        let slot = SlotKey::Memo(fingerprint);
        m.memo_slot_create(fingerprint);
        m.slot_dirty_begin(slot);
        let open = m.slot_version(slot);

        m.memo_slot_remove(fingerprint);

        let after = m.slot_version(slot);
        assert!(
            after > open && after.is_multiple_of(2),
            "{after} after {open}"
        );
    }

    #[test]
    fn test_relation_slot_unaffected_by_floor_raise() {
        let m = memo(1 << 20);
        let relation = SlotKey::Relation(Oid::from_raw(10));
        slot_bump(&m, relation);
        let fingerprint = Fingerprint::from_raw(1);
        m.memo_slot_create(fingerprint);
        m.memo_slot_remove(fingerprint);

        assert_eq!(m.slot_version(relation), 2, "relation slot untouched");
        assert_eq!(
            m.slot_version(SlotKey::Relation(Oid::from_raw(20))),
            0,
            "missing relation slot reads 0"
        );
    }

    #[test]
    fn test_gc_sweeps_stale() {
        let m = memo(1 << 20);
        m.insert(
            key(1),
            entry(b"aaa", &[(SlotKey::Relation(Oid::from_raw(10)), 0)]),
        );
        m.insert(
            key(2),
            entry(b"bbb", &[(SlotKey::Relation(Oid::from_raw(20)), 0)]),
        );
        m.slot_dirty_begin(SlotKey::Relation(Oid::from_raw(10)));
        m.slot_dirty_end(SlotKey::Relation(Oid::from_raw(10)));
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
        m.memo_slot_create(Fingerprint::from_raw(1));
        let mut cap = MemoCapture::begin(&m, key(1), &[Oid::from_raw(10), Oid::from_raw(20)])
            .expect("relations stable, memo enabled");
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
        m.memo_slot_create(Fingerprint::from_raw(1));
        let mut cap =
            MemoCapture::begin(&m, key(1), &[Oid::from_raw(10)]).expect("stable at begin");
        cap.row_description_push(b"RD");
        cap.data_push(b"row");
        cap.command_complete_push(b"CC");
        // A CDC frame touches relation 10 during the serve.
        m.slot_dirty_begin(SlotKey::Relation(Oid::from_raw(10)));
        m.slot_dirty_end(SlotKey::Relation(Oid::from_raw(10)));
        assert!(
            !cap.finish(&m),
            "stamp advanced → capture dropped, not stored"
        );
        assert!(m.get(&key(1)).is_none());
    }

    #[test]
    fn test_capture_skips_when_relation_mid_write() {
        let m = memo(1 << 20);
        m.slot_dirty_begin(SlotKey::Relation(Oid::from_raw(10))); // odd: write in progress
        assert!(
            MemoCapture::begin(&m, key(1), &[Oid::from_raw(10)]).is_none(),
            "no capture started while a read relation is mid-write"
        );
    }

    #[test]
    fn test_capture_aborts_over_cap() {
        let m = memo(1 << 30);
        m.memo_slot_create(Fingerprint::from_raw(1));
        let mut cap = MemoCapture::begin(&m, key(1), &[Oid::from_raw(10)]).expect("enabled");
        cap.row_description_push(b"RD");
        cap.data_push(&vec![0u8; MAX_MEMO_ENTRY_BYTES + 1]);
        assert!(!cap.finish(&m), "oversized capture is dropped");
        assert!(m.is_empty());
    }

    #[test]
    fn test_capture_disabled_returns_none() {
        let m = memo(0);
        assert!(MemoCapture::begin(&m, key(1), &[Oid::from_raw(10)]).is_none());
    }
}
