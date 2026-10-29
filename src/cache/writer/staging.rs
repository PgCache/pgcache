//! Population staging + the population-vs-CDC deleted-key set (PGC-250, Slice A).
//!
//! A population streams its origin snapshot into a per-relation staging table in
//! `pgcache_stage` (loaded by the worker's connection), then asks the writer to
//! merge it into the shared cache table. The merge runs only when no CDC frame
//! is open, so it never races the CDC writer's frame transaction on the shared
//! table.
//!
//! The hazard the merge guards against: a row the population read at its snapshot
//! can be removed at origin *during* the population (DELETE, UPDATE out of the
//! predicate, PK change). CDC applies that removal to a cache table that doesn't
//! hold the row yet (a no-op), and the merge would otherwise resurrect it
//! permanently. `PopulationDeletedKeys` records every key CDC removes while a
//! population over that relation is in flight; the merge filters those keys out.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use ecow::EcoString;
use postgres_protocol::escape;
use tracing::error;

use super::core::WriterCore;
use crate::cache::Generation;
use crate::catalog::TableMetadata;
use crate::oid::Oid;
use crate::pg::protocol::ByteString;
use crate::pg::{Lsn, OriginSnapshot};
use crate::query::Fingerprint;

/// Per-relation backstop cap on retained distinct deleted-key tuples. The
/// primary bound is the prune (`PopulationDeletedKeys::relation_prune`): a key
/// is dropped once no in-flight population can be missing the event that
/// recorded it, so the set is bounded by the oldest in-flight population's
/// window. The cap only bites if a single population stays in flight long
/// enough on a high-delete relation to accumulate this many keys; on overflow
/// the keys are dropped and every merge over the relation is aborted (the query
/// repopulates with a fresh snapshot) until the last in-flight population
/// deactivates. It also bounds a population's toast-fallback matches awaiting
/// its snapshot.
const POPULATION_DELETED_KEY_CAP: usize = 100_000;

/// Tracks primary keys removed by CDC while populations are in flight, per
/// relation, so a population merge can filter out rows CDC already removed.
///
/// Each key is stamped with the commit LSN and xid of the transaction that
/// recorded it. A population can be missing that event only if it was applied
/// after the population began recording and its xid is not visible to the
/// origin snapshot the population took before reading (PGC-484). Once no
/// in-flight population can be missing a key's event, the key is pruned. This
/// bounds the set to the oldest in-flight population's window rather than
/// letting it grow with total delete volume under sustained overlap.
/// Identifies one in-flight population: `(fingerprint, generation)`. Keying on
/// the pair (not fingerprint alone) keeps two populations of the same query —
/// e.g. one parked post-merge while a readmit dispatches the next generation —
/// tracked independently, so deactivating one never tears down the other.
pub(super) type PopulationKey = (Fingerprint, Generation);

/// The source transaction a CDC event committed in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct EventStamp {
    pub(super) lsn: Lsn,
    /// `None` when the transaction is unknown; treated as unseen by every
    /// snapshot.
    pub(super) xid: Option<u32>,
}

#[derive(Default)]
pub(super) struct PopulationDeletedKeys {
    relations: HashMap<Oid, DeletedKeyEntry>,
    inflight: HashMap<PopulationKey, InflightPopulation>,
}

struct InflightPopulation {
    relation_oids: Vec<Oid>,
    /// `last_received_lsn` at dispatch. An event stamped at or below it was
    /// applied before recording began and is assumed visible to the reads
    /// (PGC-485).
    activated_at: Lsn,
    /// The origin snapshot taken before the reads, once the population reports
    /// it.
    snapshot: Option<Arc<OriginSnapshot>>,
    /// Transactions whose toast fallback matched this population's query,
    /// awaiting the snapshot.
    fallback_pending: Vec<Option<u32>>,
    /// A toast fallback matched the query in a transaction the reads may have
    /// missed, so the merge must abort.
    fallback_missed: bool,
}

impl InflightPopulation {
    /// Whether the reads may be missing the event stamped `stamp`.
    fn misses(&self, stamp: EventStamp) -> bool {
        stamp.lsn > self.activated_at && !self.sees(stamp.xid)
    }

    fn sees(&self, xid: Option<u32>) -> bool {
        matches!((&self.snapshot, xid), (Some(snapshot), Some(xid)) if snapshot.sees(xid))
    }
}

#[derive(Default)]
struct DeletedKeyEntry {
    /// In-flight populations over this relation. Empty ⇒ remove the entry.
    populations: HashSet<PopulationKey>,
    /// Rendered PK tuple body (e.g. `42` or `'a','b'`) → the delete's stamp.
    keys: HashMap<EcoString, EventStamp>,
    /// Bumped whenever the key *set* changes (not a restamp), so a merge can
    /// reuse its rendered filter across chunks until the set moves.
    version: u64,
    /// PK tuple body → stamp of a toast fallback on that row (PGC-464): a row
    /// CDC could neither upsert (incomplete image) nor record as deleted
    /// (alive at origin). A population whose staging holds the row from before
    /// that transaction cannot be repaired at merge and must repopulate.
    stale_keys: HashMap<EcoString, EventStamp>,
    /// Bumped whenever `stale_keys` changes, for the merge's probe cache.
    stale_version: u64,
    /// Set once `keys` exceeds the cap; keys are dropped and *every* merge over
    /// the relation aborts (keys genuinely lost) until the last population leaves.
    overflowed: bool,
    /// Highest LSN of a bulk invalidation (TRUNCATE, 40P01 recovery) over this
    /// relation. A merge whose snapshot predates it would resurrect rows the
    /// event removed, so it aborts; a population that snapshotted at/after it is
    /// unaffected — so unlike `overflowed`, this self-clears for fresh snapshots.
    aborted_below: Lsn,
}

impl DeletedKeyEntry {
    fn stale_key_insert(&mut self, key: EcoString, stamp: EventStamp) {
        // A later fallback on the same key wins: its writer waited on the
        // earlier one's row lock, so a snapshot that sees it sees both.
        let slot = self.stale_keys.entry(key).or_insert(stamp);
        if stamp.lsn >= slot.lsn {
            *slot = stamp;
        }
        self.stale_version += 1;
    }

    fn stale_keys_retain(&mut self, keep: impl Fn(EventStamp) -> bool) {
        let before = self.stale_keys.len();
        self.stale_keys.retain(|_, stamp| keep(*stamp));
        if self.stale_keys.len() != before {
            self.stale_version += 1;
        }
    }

    fn key_insert(&mut self, key: EcoString, stamp: EventStamp) {
        if self.keys.insert(key, stamp).is_none() {
            self.version += 1;
        }
    }

    /// Drop a key; returns whether it was tracked.
    fn key_remove(&mut self, key: &str) -> bool {
        let removed = self.keys.remove(key).is_some();
        if removed {
            self.version += 1;
        }
        removed
    }

    fn keys_retain(&mut self, keep: impl Fn(EventStamp) -> bool) {
        let before = self.keys.len();
        self.keys.retain(|_, stamp| keep(*stamp));
        if self.keys.len() != before {
            self.version += 1;
        }
    }

    /// Cap overflow: keys are lost, so every merge over this relation must abort
    /// until the last in-flight population leaves.
    fn disable(&mut self) {
        self.keys.clear();
        self.keys.shrink_to_fit();
        self.stale_keys.clear();
        self.stale_keys.shrink_to_fit();
        self.overflowed = true;
        self.version += 1;
        self.stale_version += 1;
    }

    /// A bulk invalidation committed at `lsn` emptied/dropped rows; raise the
    /// abort watermark and drop now-stale keys (anything `<= lsn` is below the
    /// truncate and irrelevant to surviving post-event snapshots).
    fn abort_below(&mut self, lsn: Lsn) {
        self.aborted_below = self.aborted_below.max(lsn);
        self.keys_retain(|stamp| stamp.lsn > lsn);
        self.stale_keys_retain(|stamp| stamp.lsn > lsn);
    }

    /// Whether a merge whose snapshot is `snapshot_lsn` must abort: keys were
    /// lost (overflow), or the snapshot predates a bulk invalidation.
    fn should_abort(&self, snapshot_lsn: Lsn) -> bool {
        self.overflowed || snapshot_lsn < self.aborted_below
    }

    /// Whether a merge filter would render: keys are held and not lost.
    fn filters(&self) -> bool {
        !self.overflowed && !self.keys.is_empty()
    }
}

impl PopulationDeletedKeys {
    /// Begin recording deletes for a population's relations. Called at
    /// dispatch, before the worker reads its snapshot. `activated_at` is the
    /// receive watermark at dispatch: events stamped at or below it were
    /// applied before recording began. Idempotent per population.
    pub(super) fn activate(
        &mut self,
        fingerprint: Fingerprint,
        generation: Generation,
        relation_oids: &[Oid],
        activated_at: Lsn,
    ) {
        let key = (fingerprint, generation);
        if self.inflight.contains_key(&key) {
            return;
        }
        for &oid in relation_oids {
            self.relations
                .entry(oid)
                .or_default()
                .populations
                .insert(key);
        }
        self.inflight.insert(
            key,
            InflightPopulation {
                relation_oids: relation_oids.to_vec(),
                activated_at,
                snapshot: None,
                fallback_pending: Vec::new(),
                fallback_missed: false,
            },
        );
    }

    /// Stop recording for a population and prune what only it needed. Removes
    /// a relation's entry once its last in-flight population leaves.
    pub(super) fn deactivate(&mut self, fingerprint: Fingerprint, generation: Generation) {
        let key = (fingerprint, generation);
        let Some(population) = self.inflight.remove(&key) else {
            return;
        };
        for oid in population.relation_oids {
            let Some(entry) = self.relations.get_mut(&oid) else {
                continue;
            };
            entry.populations.remove(&key);
            if entry.populations.is_empty() {
                self.relations.remove(&oid);
            } else {
                self.relation_prune(oid);
            }
        }
    }

    /// Record the origin snapshot a population took before its reads. Settles
    /// the toast-fallback matches it was holding and prunes what the snapshot
    /// shows it already read.
    pub(super) fn snapshot_set(
        &mut self,
        fingerprint: Fingerprint,
        generation: Generation,
        snapshot: Arc<OriginSnapshot>,
    ) {
        let Some(population) = self.inflight.get_mut(&(fingerprint, generation)) else {
            return;
        };
        population.snapshot = Some(snapshot);
        let pending = std::mem::take(&mut population.fallback_pending);
        let missed = pending.into_iter().any(|xid| !population.sees(xid));
        population.fallback_missed |= missed;
        for oid in population.relation_oids.clone() {
            self.relation_prune(oid);
        }
    }

    /// Drop keys no in-flight population over `relation_oid` can be missing.
    fn relation_prune(&mut self, relation_oid: Oid) {
        let inflight = &self.inflight;
        let Some(entry) = self.relations.get_mut(&relation_oid) else {
            return;
        };
        let populations: Vec<&InflightPopulation> = entry
            .populations
            .iter()
            .filter_map(|key| inflight.get(key))
            .collect();
        let needed = |stamp: EventStamp| populations.iter().any(|p| p.misses(stamp));
        entry.keys_retain(needed);
        entry.stale_keys_retain(needed);
    }

    /// Record a removed PK for `relation_oid` if a population is recording it.
    pub(super) fn record(&mut self, relation_oid: Oid, key: EcoString, stamp: EventStamp) {
        let Some(entry) = self.relations.get_mut(&relation_oid) else {
            return;
        };
        if entry.overflowed {
            return;
        }
        // A delete after a fallback supersedes it: the merge omits the row.
        if entry.stale_keys.remove(&key).is_some() {
            entry.stale_version += 1;
        }
        entry.key_insert(key, stamp);
        if entry.keys.len() > POPULATION_DELETED_KEY_CAP {
            entry.disable();
            error!(
                relation_oid = %relation_oid,
                "population deleted-key set overflowed cap {POPULATION_DELETED_KEY_CAP}; \
                 affected populations will repopulate"
            );
        }
    }

    /// Record a toast fallback on the row at `key` (PGC-464) if a population is
    /// recording `relation_oid`. The row is alive at origin, so it also cancels
    /// any tracked delete of the key. Shares the deleted-key cap: overflow
    /// aborts every merge over the relation until the last population leaves.
    pub(super) fn record_toast_stale(
        &mut self,
        relation_oid: Oid,
        key: EcoString,
        stamp: EventStamp,
    ) {
        let Some(entry) = self.relations.get_mut(&relation_oid) else {
            return;
        };
        if entry.overflowed {
            return;
        }
        entry.key_remove(&key);
        entry.stale_key_insert(key, stamp);
        if entry.stale_keys.len() > POPULATION_DELETED_KEY_CAP {
            entry.disable();
            error!(
                relation_oid = %relation_oid,
                "population toast-stale key set overflowed cap {POPULATION_DELETED_KEY_CAP}; \
                 affected populations will repopulate"
            );
        }
    }

    /// Record that a toast fallback in transaction `xid` matched `population`'s
    /// query (PGC-484). If the population's reads may have missed it, the row
    /// may be absent or stale in staging, so the merge aborts.
    pub(super) fn fallback_match_record(&mut self, population: PopulationKey, xid: Option<u32>) {
        let Some(inflight) = self.inflight.get_mut(&population) else {
            return;
        };
        if inflight.fallback_missed {
            return;
        }
        if inflight.snapshot.is_some() {
            inflight.fallback_missed = !inflight.sees(xid);
            return;
        }
        if inflight.fallback_pending.last() != Some(&xid) {
            inflight.fallback_pending.push(xid);
        }
        if inflight.fallback_pending.len() > POPULATION_DELETED_KEY_CAP {
            inflight.fallback_pending = Vec::new();
            inflight.fallback_missed = true;
        }
    }

    /// Whether a toast fallback matched the population's query in a
    /// transaction its reads may have missed.
    pub(super) fn fallback_missed(&self, fingerprint: Fingerprint, generation: Generation) -> bool {
        self.inflight
            .get(&(fingerprint, generation))
            .is_some_and(|population| population.fallback_missed)
    }

    /// Whether `population` is in flight and recording `relation_oid`.
    pub(super) fn records(&self, population: PopulationKey, relation_oid: Oid) -> bool {
        self.relations
            .get(&relation_oid)
            .is_some_and(|entry| entry.populations.contains(&population))
    }

    /// Drop a toast-stale key whose row CDC has re-written with a complete
    /// image (PGC-464): the live row is in the shared table, which merges never
    /// overwrite, so no staged copy can regress it. Returns whether the key was
    /// tracked, so the caller applies the same force-upsert rule as
    /// [`Self::cancel`] (PGC-260).
    pub(super) fn cancel_toast_stale(&mut self, relation_oid: Oid, key: &str) -> bool {
        let Some(entry) = self.relations.get_mut(&relation_oid) else {
            return false;
        };
        let removed = entry.stale_keys.remove(key).is_some();
        if removed {
            entry.stale_version += 1;
        }
        removed
    }

    /// Version of the toast-stale key set for `relation_oid`, or `None` when
    /// `population` can be missing none of them. Changes exactly when the set a
    /// merge would probe with could, so the merge can cache its probe result.
    pub(super) fn stale_version_for(
        &self,
        relation_oid: Oid,
        population: PopulationKey,
    ) -> Option<u64> {
        let entry = self.relations.get(&relation_oid)?;
        let inflight = self.inflight.get(&population)?;
        entry
            .stale_keys
            .values()
            .any(|stamp| inflight.misses(*stamp))
            .then_some(entry.stale_version)
    }

    /// Render the `(<pk cols>) IN (...)` membership predicate over the
    /// toast-stale keys `population` can be missing, or `None` when there are
    /// none.
    pub(super) fn stale_predicate(
        &self,
        relation_oid: Oid,
        population: PopulationKey,
        pk_columns_paren: &str,
    ) -> Option<String> {
        let entry = self.relations.get(&relation_oid)?;
        let inflight = self.inflight.get(&population)?;
        let mut tuples = String::new();
        for key in entry
            .stale_keys
            .iter()
            .filter(|(_, stamp)| inflight.misses(**stamp))
            .map(|(key, _)| key)
        {
            if !tuples.is_empty() {
                tuples.push(',');
            }
            tuples.push('(');
            tuples.push_str(key);
            tuples.push(')');
        }
        (!tuples.is_empty()).then(|| format!("{pk_columns_paren} IN ({tuples})"))
    }

    /// Whether any population is recording deletes for `relation_oid`. Lets the
    /// CDC delete path skip rendering a key when nothing would consume it.
    pub(super) fn is_recording(&self, relation_oid: Oid) -> bool {
        self.relations.contains_key(&relation_oid)
    }

    /// Drop a tracked key whose row CDC has re-written (PGC-260): the row is
    /// alive at origin again, so filtering its key would make a population
    /// merge omit a live row — while resurrection of the old version is
    /// impossible anyway once the live row is in the shared cache table
    /// (merges never overwrite). Returns whether the key was tracked.
    pub(super) fn cancel(&mut self, relation_oid: Oid, key: &str) -> bool {
        self.relations
            .get_mut(&relation_oid)
            .is_some_and(|entry| entry.key_remove(key))
    }

    /// Raise the abort watermark for `relation_oid` to `lsn` — a bulk
    /// invalidation (TRUNCATE / 40P01 recovery) committed there. Merges whose
    /// snapshot predates `lsn` abort and repopulate; a population that
    /// snapshotted at/after `lsn` is unaffected (it sees the empty table), so
    /// this self-clears rather than blanket-aborting like overflow. No-op if no
    /// population is recording the relation.
    pub(super) fn abort_below(&mut self, relation_oid: Oid, lsn: Lsn) {
        if let Some(entry) = self.relations.get_mut(&relation_oid) {
            entry.abort_below(lsn);
        }
    }

    /// Whether a merge over `relation_oid` with snapshot `snapshot_lsn` must
    /// abort (cap overflow, or snapshot predates a bulk invalidation).
    pub(super) fn should_abort(&self, relation_oid: Oid, snapshot_lsn: Lsn) -> bool {
        self.relations
            .get(&relation_oid)
            .is_some_and(|e| e.should_abort(snapshot_lsn))
    }

    /// Version of the filter `filter_predicate` would render for
    /// `relation_oid`, or `None` when it would render nothing. Changes exactly
    /// when the rendered filter would, so a merge can key a cached rendering on
    /// it across chunks. Valid only while the relation's entry lives; a
    /// draining population's own floor keeps it alive for the whole drain.
    pub(super) fn filter_version(&self, relation_oid: Oid) -> Option<u64> {
        let entry = self.relations.get(&relation_oid)?;
        entry.filters().then_some(entry.version)
    }

    /// Build the `(<pk cols>) NOT IN (...)` predicate excluding recorded deletes,
    /// or `None` when there's nothing to exclude.
    pub(super) fn filter_predicate(
        &self,
        relation_oid: Oid,
        pk_columns_paren: &str,
    ) -> Option<String> {
        let entry = self.relations.get(&relation_oid)?;
        if !entry.filters() {
            return None;
        }
        let mut tuples = String::new();
        for (i, key) in entry.keys.keys().enumerate() {
            if i > 0 {
                tuples.push(',');
            }
            tuples.push('(');
            tuples.push_str(key);
            tuples.push(')');
        }
        Some(format!("{pk_columns_paren} NOT IN ({tuples})"))
    }
}

/// Per-relation pool of reusable population staging tables (PGC-293).
///
/// Each population checks out one staging table per relation it reads — at
/// dispatch, writer-side — loads it, and the writer returns it (emptied) to the
/// free-list after the merge. Tables are minted once per slot
/// (`CREATE … IF NOT EXISTS`) and reused via `DELETE`, never `DROP`/`CREATE` per
/// population, so a population emits no DDL — hence no relcache/plan-cache
/// invalidation, the per-population cost that collapses write-mixed throughput at
/// high query cardinality (PGC-294). The pool grows to the peak number of
/// concurrently in-flight populations per relation.
///
/// Single-threaded: workers and the writer share one `LocalSet` thread, so plain
/// maps suffice (no locking). The `pgcache_stage` schema is dropped wholesale on
/// a cache-database reset, which recreates `WriterCore` and hence this pool.
#[derive(Default)]
pub(super) struct StagingPool {
    /// Empty tables ready for reuse, per relation (bloat reclaimed by autovacuum).
    free: HashMap<Oid, Vec<EcoString>>,
    /// Next slot id per relation, for minting new table names.
    next_slot: HashMap<Oid, u32>,
    /// Per-relation shape epoch, bumped on a relation schema change
    /// (`relation_epoch_bump`). A staging table is `CREATE (LIKE cache_table)`
    /// once and reused; on a schema change the cache table is recreated with a
    /// new column shape, so any pooled table minted under an earlier epoch has
    /// the wrong columns and must not be reused — it is dropped at check-in.
    epoch: HashMap<Oid, u32>,
    /// Tables a population currently holds, keyed by `(fingerprint, generation)`,
    /// so the writer can return them on any terminal path (merge / abort /
    /// failure / dispatch-fail). Each carries the relation's epoch at checkout.
    checked_out: HashMap<PopulationKey, Vec<StagingHold>>,
}

/// One staging table a population holds: `(relation, table_name, epoch_at_checkout)`.
type StagingHold = (Oid, EcoString, u32);

impl StagingPool {
    /// Check out one staging table per relation for a population. Reuses a free
    /// (empty) table when one is available, else mints a new slot
    /// (`needs_create`). Returns `(relation, table, needs_create)` per relation
    /// for the work item.
    pub(super) fn checkout(
        &mut self,
        fingerprint: Fingerprint,
        generation: Generation,
        relation_oids: &[Oid],
    ) -> Vec<(Oid, EcoString, bool)> {
        let mut out = Vec::with_capacity(relation_oids.len());
        let mut held = Vec::with_capacity(relation_oids.len());
        for &oid in relation_oids {
            let epoch = *self.epoch.get(&oid).unwrap_or(&0);
            let (table, needs_create) = match self.free.get_mut(&oid).and_then(Vec::pop) {
                Some(table) => (table, false),
                None => {
                    let slot = self.next_slot.entry(oid).or_default();
                    let table = EcoString::from(format!("stage_{oid}_{slot}"));
                    *slot += 1;
                    (table, true)
                }
            };
            held.push((oid, table.clone(), epoch));
            out.push((oid, table, needs_create));
        }
        // A `(fingerprint, generation)` is dispatched once; a double checkout
        // would leak the prior hold's tables.
        let prev = self.checked_out.insert((fingerprint, generation), held);
        debug_assert!(
            prev.is_none(),
            "staging pool double checkout {fingerprint}/{generation}"
        );
        out
    }

    /// Take the tables a population holds, for return to the pool. Empty if the
    /// population was never checked out. Each entry carries the epoch at checkout.
    fn take(&mut self, fingerprint: Fingerprint, generation: Generation) -> Vec<StagingHold> {
        self.checked_out
            .remove(&(fingerprint, generation))
            .unwrap_or_default()
    }

    /// Populations currently holding staging tables.
    pub(super) fn held_count(&self) -> usize {
        self.checked_out.len()
    }

    /// The tables a population currently holds, without releasing them.
    pub(super) fn held(
        &self,
        fingerprint: Fingerprint,
        generation: Generation,
    ) -> Vec<(Oid, EcoString)> {
        self.checked_out
            .get(&(fingerprint, generation))
            .map(|holds| holds.iter().map(|(oid, t, _)| (*oid, t.clone())).collect())
            .unwrap_or_default()
    }

    /// Return an emptied table to its relation's free-list.
    fn release(&mut self, oid: Oid, table: EcoString) {
        self.free.entry(oid).or_default().push(table);
    }

    /// Whether `epoch` (stamped on a checked-out table at checkout) is still the
    /// relation's current shape epoch. A stale epoch means the relation's schema
    /// changed since checkout, so the table's columns are wrong and it must be
    /// dropped rather than reused.
    fn epoch_current(&self, oid: Oid, epoch: u32) -> bool {
        *self.epoch.get(&oid).unwrap_or(&0) == epoch
    }

    /// Bump a relation's shape epoch on a schema change and return its now-stale
    /// free tables so the caller can drop them. Tables still checked out from the
    /// old epoch are dropped when they check in (`epoch_current` returns false).
    fn relation_epoch_bump(&mut self, oid: Oid) -> Vec<EcoString> {
        *self.epoch.entry(oid).or_default() += 1;
        self.free.remove(&oid).unwrap_or_default()
    }

    /// Drop a population's checkout without returning the tables (rare
    /// dispatch-failure / shutdown path; a freshly minted slot was never created,
    /// and a reused table is orphaned until the next cache reset).
    pub(super) fn forget(&mut self, fingerprint: Fingerprint, generation: Generation) {
        self.checked_out.remove(&(fingerprint, generation));
    }
}

/// Render a row's primary-key values as a tuple body (escaped literals joined by
/// `,`), matching how the staging columns were loaded and how `cache_delete_into`
/// renders PK values. `None` if no PK value is present.
pub(super) fn pk_body_render(
    table_metadata: &TableMetadata,
    row_data: &[Option<ByteString>],
) -> Option<EcoString> {
    // Escapes straight into the EcoString (inline storage for short keys):
    // this runs per CDC event, so no intermediate String allocations.
    let mut body = EcoString::new();
    let mut first = true;
    for pk_column in &table_metadata.primary_key_columns {
        let column_meta = table_metadata.columns.get(pk_column.as_str())?;
        let row_value = row_data.get(column_meta.index())?;
        if !first {
            body.push(',');
        }
        match row_value.as_deref() {
            Some(value) => {
                let _ = escape::escape_literal_into(value, &mut body);
            }
            None => body.push_str("NULL"),
        }
        first = false;
    }
    if first { None } else { Some(body) }
}

impl WriterCore {
    /// A CDC insert/update re-wrote the row at `row_data`'s PK — cancel any
    /// tracked deletion of that key (PGC-260): the frame-pending entry (an
    /// earlier delete in this frame nets out, in event order) and the recorded
    /// set (deletes from earlier frames), and any toast-stale record of it
    /// (PGC-464). A lingering key would make population merges omit — or
    /// needlessly abort over — a legitimately live row. Returns whether
    /// anything was tracked, so the insert path can force-upsert an
    /// otherwise-unmatched row (the live row in the shared table is what makes
    /// cancellation safe).
    pub(super) fn population_deleted_key_cancel(
        &mut self,
        relation_oid: Oid,
        row_data: &[Option<ByteString>],
    ) -> bool {
        let pending = self
            .frame_deleted_keys
            .iter()
            .any(|(oid, _)| *oid == relation_oid);
        if !pending && !self.population_deleted_keys.is_recording(relation_oid) {
            return false;
        }
        let Some(table_metadata) = self.cache.tables.get1(&relation_oid) else {
            return false;
        };
        let Some(key) = pk_body_render(table_metadata, row_data) else {
            return false;
        };
        let mut tracked = false;
        if pending {
            self.frame_deleted_keys.retain(|(oid, pending_key)| {
                let matches = *oid == relation_oid && *pending_key == key;
                tracked |= matches;
                !matches
            });
        }
        tracked |= self.population_deleted_keys.cancel(relation_oid, &key);
        tracked |= self
            .population_deleted_keys
            .cancel_toast_stale(relation_oid, &key);
        tracked
    }

    /// Return a population's staging tables to the pool (PGC-293): empty each
    /// (idempotent — the merge's `DELETE … RETURNING` may have already drained
    /// it), then release to the free-list. `DELETE` is pure DML, so check-in
    /// emits no relcache invalidation — important because under high cached-plan
    /// cardinality (PGC-294) every relcache invalidation costs an O(plans)
    /// `PlanCacheRelCallback` scan. Bloat from the dead tuples is left to
    /// autovacuum, which for near-empty staging tables fires only after ~50 dead
    /// tuples accumulate — i.e. batched across many reuses — so it can't run a
    /// per-population vacuum (a per-population VACUUM, like VACUUM FULL, re-emits
    /// the very invalidation this scheme avoids). Called when a merge completes
    /// (the tables are already empty) and as the fallback when a worker-side
    /// discard failed. Best-effort — a leak is recovered by the next cache reset.
    pub(super) async fn staging_checkin(
        &mut self,
        fingerprint: Fingerprint,
        generation: Generation,
    ) {
        self.staging_return(fingerprint, generation, true).await;
    }

    /// Return a population's staging tables a worker has already emptied
    /// (`Discarded`): release to the free-list without touching them.
    async fn staging_release(&mut self, fingerprint: Fingerprint, generation: Generation) {
        self.staging_return(fingerprint, generation, false).await;
    }

    /// A population worker finished emptying a discarded population's staging
    /// (PGC-553): return the tables to the pool, emptying them here only if the
    /// worker could not.
    pub(super) async fn population_discarded(
        &mut self,
        fingerprint: Fingerprint,
        generation: Generation,
        emptied: bool,
    ) {
        self.merges.discards_pending = self.merges.discards_pending.saturating_sub(1);
        if emptied {
            self.staging_release(fingerprint, generation).await;
        } else {
            self.staging_checkin(fingerprint, generation).await;
        }
    }

    async fn staging_return(
        &mut self,
        fingerprint: Fingerprint,
        generation: Generation,
        empty: bool,
    ) {
        for (oid, staging, epoch) in self.staging_pool.take(fingerprint, generation) {
            // A relation schema change since checkout makes this table's shape
            // stale (`epoch_current` false) — drop it rather than reuse it.
            if !self.staging_pool.epoch_current(oid, epoch) {
                if let Err(e) = self
                    .db_cache
                    .batch_execute(&format!("DROP TABLE IF EXISTS pgcache_stage.{staging}"))
                    .await
                {
                    error!("staging checkin: dropping stale {staging}: {e}");
                }
                continue;
            }
            if empty
                && let Err(e) = self
                    .db_cache
                    .batch_execute(&format!("DELETE FROM pgcache_stage.{staging}"))
                    .await
            {
                error!("staging checkin: emptying {staging}: {e}");
            }
            self.staging_pool.release(oid, staging);
        }
    }

    /// Invalidate a relation's pooled staging tables on a schema change
    /// (PGC-293): bump the relation's epoch and drop its now-stale free tables.
    /// Pooled tables are `CREATE (LIKE cache_table)` once; when the cache table
    /// is recreated with a new shape, reusing an old-shape staging table would
    /// merge misaligned columns, so they must be discarded. Runs only on the
    /// (rare) schema-change path — which already recreates the cache table — so
    /// the drops don't reintroduce per-population DDL.
    pub(super) async fn staging_pool_relation_purge(&mut self, relation_oid: Oid) {
        for staging in self.staging_pool.relation_epoch_bump(relation_oid) {
            if let Err(e) = self
                .db_cache
                .batch_execute(&format!("DROP TABLE IF EXISTS pgcache_stage.{staging}"))
                .await
            {
                error!("staging pool purge: dropping {staging}: {e}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const REL: Oid = Oid::from_raw(10);
    const GEN: Generation = Generation::from_raw(1);

    const FP1: Fingerprint = Fingerprint::from_raw(1);
    const FP2: Fingerprint = Fingerprint::from_raw(2);

    /// A stamp whose xid equals its LSN, which keeps the tests' arithmetic
    /// readable.
    fn stamp(lsn: u64) -> EventStamp {
        EventStamp {
            lsn: Lsn::from_raw(lsn),
            xid: u32::try_from(lsn).ok(),
        }
    }

    fn record(keys: &mut PopulationDeletedKeys, body: &str, lsn: Lsn) {
        keys.record(REL, EcoString::from(body), stamp(lsn.get()));
    }

    fn filtered(keys: &PopulationDeletedKeys) -> String {
        keys.filter_predicate(REL, "(id)").unwrap_or_default()
    }

    /// The filter version moves exactly when the rendered filter would: on a
    /// new key, a cancel, a prune, a bulk-invalidation drop, and overflow — not
    /// on a re-record of a key already held.
    #[test]
    fn test_filter_version_tracks_key_set_changes() {
        let mut keys = PopulationDeletedKeys::default();
        assert!(keys.filter_version(REL).is_none(), "no entry");
        keys.activate(Fingerprint::from_raw(1), GEN, &[REL], Lsn::from_raw(100));
        keys.activate(Fingerprint::from_raw(2), GEN, &[REL], Lsn::from_raw(200));
        assert!(keys.filter_version(REL).is_none(), "no keys");

        record(&mut keys, "5", Lsn::from_raw(150));
        let v1 = keys.filter_version(REL).expect("filter present");
        record(&mut keys, "5", Lsn::from_raw(160));
        assert_eq!(keys.filter_version(REL), Some(v1), "restamp keeps the set");
        record(&mut keys, "25", Lsn::from_raw(250));
        let v2 = keys.filter_version(REL).expect("filter present");
        assert_ne!(v1, v2, "new key");

        assert!(keys.cancel(REL, "25"));
        let v3 = keys.filter_version(REL).expect("filter present");
        assert_ne!(v2, v3, "cancel");
        assert!(!keys.cancel(REL, "25"));
        assert_eq!(
            keys.filter_version(REL),
            Some(v3),
            "cancel of untracked key"
        );

        // fp1 leaves: fp2 activated at 200, so the delete at 160 is pruned.
        keys.deactivate(Fingerprint::from_raw(1), GEN);
        assert!(keys.filter_version(REL).is_none(), "pruned to empty");

        record(&mut keys, "30", Lsn::from_raw(300));
        let v4 = keys.filter_version(REL).expect("filter present");
        keys.abort_below(REL, Lsn::from_raw(250));
        assert_eq!(
            keys.filter_version(REL),
            Some(v4),
            "key above the truncate survives"
        );
        keys.abort_below(REL, Lsn::from_raw(350));
        assert!(
            keys.filter_version(REL).is_none(),
            "dropped below the truncate"
        );
    }

    /// Deactivating a population prunes deletes every remaining in-flight
    /// population had already received before it began recording.
    #[test]
    fn test_deactivate_prunes_keys_received_before_remaining_activations() {
        let mut keys = PopulationDeletedKeys::default();
        keys.activate(Fingerprint::from_raw(1), GEN, &[REL], Lsn::from_raw(100));
        keys.activate(Fingerprint::from_raw(2), GEN, &[REL], Lsn::from_raw(200));
        record(&mut keys, "5", Lsn::from_raw(50));
        record(&mut keys, "15", Lsn::from_raw(150));
        record(&mut keys, "25", Lsn::from_raw(250));

        // Min floor is 100 (fp1); the merge still filters all three.
        let before = keys.filter_predicate(REL, "(id)").expect("filter present");
        assert!(before.contains("(5)") && before.contains("(15)") && before.contains("(25)"));

        // fp1 leaves → deletes received before fp2 activated (LSN <= 200) are
        // irrelevant to fp2 and pruned.
        keys.deactivate(Fingerprint::from_raw(1), GEN);
        let after = keys.filter_predicate(REL, "(id)").expect("filter present");
        assert!(after.contains("(25)"), "kept recent delete: {after}");
        assert!(
            !after.contains("(5)") && !after.contains("(15)"),
            "pruned stale deletes: {after}"
        );
    }

    /// Two populations of the *same* fingerprint at different generations are
    /// tracked independently — deactivating one keeps the other recording.
    #[test]
    fn test_generations_of_same_fingerprint_are_independent() {
        let mut keys = PopulationDeletedKeys::default();
        keys.activate(
            Fingerprint::from_raw(7),
            Generation::from_raw(5),
            &[REL],
            Lsn::from_raw(100),
        ); // gen 5, parked
        keys.activate(
            Fingerprint::from_raw(7),
            Generation::from_raw(8),
            &[REL],
            Lsn::from_raw(100),
        ); // gen 8, readmitted

        // gen 5 finishes; gen 8 must still be recording for the relation.
        keys.deactivate(Fingerprint::from_raw(7), Generation::from_raw(5));
        assert!(keys.is_recording(REL), "gen 8 still in flight");
        record(&mut keys, "5", Lsn::from_raw(150));
        assert!(
            keys.filter_predicate(REL, "(id)").is_some(),
            "gen 8 still records deletes"
        );

        // Once gen 8 also leaves, the entry clears.
        keys.deactivate(Fingerprint::from_raw(7), Generation::from_raw(8));
        assert!(!keys.is_recording(REL));
    }

    /// The relation's entry disappears once the last population leaves.
    #[test]
    fn test_deactivate_last_population_clears_entry() {
        let mut keys = PopulationDeletedKeys::default();
        keys.activate(Fingerprint::from_raw(1), GEN, &[REL], Lsn::from_raw(100));
        record(&mut keys, "5", Lsn::from_raw(150));
        keys.deactivate(Fingerprint::from_raw(1), GEN);
        assert!(keys.filter_predicate(REL, "(id)").is_none());
    }

    /// Deletes for a relation no population is reading are dropped on the floor.
    #[test]
    fn test_record_without_active_population_is_noop() {
        let mut keys = PopulationDeletedKeys::default();
        record(&mut keys, "5", Lsn::from_raw(150));
        assert!(keys.filter_predicate(REL, "(id)").is_none());
    }

    /// Exceeding the cap drops keys and aborts every merge over the relation.
    #[test]
    fn test_overflow_aborts_and_disables_filtering() {
        let mut keys = PopulationDeletedKeys::default();
        keys.activate(Fingerprint::from_raw(1), GEN, &[REL], Lsn::from_raw(0));
        for i in 0..=POPULATION_DELETED_KEY_CAP {
            record(&mut keys, &i.to_string(), Lsn::from_raw(1));
        }
        // Overflow aborts regardless of snapshot LSN, and there's no filter.
        assert!(keys.should_abort(REL, Lsn::from_raw(u64::MAX)));
        assert!(keys.filter_predicate(REL, "(id)").is_none());
    }

    /// A bulk invalidation aborts only merges whose snapshot predates it; a
    /// population that snapshotted at/after it is unaffected (self-clearing),
    /// and now-stale keys are pruned.
    #[test]
    fn test_abort_below_aborts_only_older_snapshots() {
        let mut keys = PopulationDeletedKeys::default();
        keys.activate(Fingerprint::from_raw(1), GEN, &[REL], Lsn::from_raw(100));
        record(&mut keys, "5", Lsn::from_raw(150));
        record(&mut keys, "25", Lsn::from_raw(250));

        keys.abort_below(REL, Lsn::from_raw(200)); // e.g. a TRUNCATE committed at LSN 200

        // A population that read before the truncate must abort; one that read
        // at/after it must not.
        assert!(
            keys.should_abort(REL, Lsn::from_raw(150)),
            "pre-truncate snapshot aborts"
        );
        assert!(
            !keys.should_abort(REL, Lsn::from_raw(200)),
            "snapshot at the truncate is fine"
        );
        assert!(
            !keys.should_abort(REL, Lsn::from_raw(300)),
            "post-truncate snapshot is fine"
        );

        // Keys at/below the truncate LSN are pruned; later ones survive.
        let filter = keys.filter_predicate(REL, "(id)").expect("filter present");
        assert!(
            filter.contains("(25)") && !filter.contains("(5)"),
            "filter: {filter}"
        );
    }

    /// A reported snapshot prunes the keys whose transactions it sees and
    /// keeps the ones it doesn't.
    #[test]
    fn test_snapshot_prunes_keys_it_sees() {
        let mut keys = PopulationDeletedKeys::default();
        keys.activate(FP1, GEN, &[REL], Lsn::from_raw(100));
        record(&mut keys, "5", Lsn::from_raw(150));
        record(&mut keys, "6", Lsn::from_raw(160));
        record(&mut keys, "7", Lsn::from_raw(170));

        // Sees xids below 170 except 160.
        keys.snapshot_set(FP1, GEN, Arc::new(OriginSnapshot::new(150, 170, vec![160])));
        let filter = filtered(&keys);
        assert!(
            !filter.contains("(5)") && filter.contains("(6)") && filter.contains("(7)"),
            "{filter}"
        );
    }

    /// A population that hasn't reported its snapshot holds every key
    /// received since it activated, even ones another population has seen.
    #[test]
    fn test_population_without_snapshot_holds_its_keys() {
        let mut keys = PopulationDeletedKeys::default();
        keys.activate(FP1, GEN, &[REL], Lsn::from_raw(100));
        keys.activate(FP2, GEN, &[REL], Lsn::from_raw(100));
        record(&mut keys, "5", Lsn::from_raw(150));

        keys.snapshot_set(FP1, GEN, Arc::new(OriginSnapshot::new(200, 200, vec![])));
        assert!(filtered(&keys).contains("(5)"), "fp2 may still need it");

        keys.snapshot_set(FP2, GEN, Arc::new(OriginSnapshot::new(200, 200, vec![])));
        assert!(keys.filter_predicate(REL, "(id)").is_none(), "both saw it");
    }

    /// Fallback matches held before the snapshot arrives are settled by it: a
    /// seen transaction leaves the merge alone, an unseen one aborts it.
    #[test]
    fn test_fallback_matches_settled_by_snapshot() {
        let mut keys = PopulationDeletedKeys::default();
        keys.activate(FP1, GEN, &[REL], Lsn::from_raw(100));
        keys.activate(FP2, GEN, &[REL], Lsn::from_raw(100));
        keys.fallback_match_record((FP1, GEN), Some(50));
        keys.fallback_match_record((FP2, GEN), Some(60));

        // Sees xids below 70 except 60.
        keys.snapshot_set(FP1, GEN, Arc::new(OriginSnapshot::new(55, 70, vec![60])));
        keys.snapshot_set(FP2, GEN, Arc::new(OriginSnapshot::new(55, 70, vec![60])));
        assert!(!keys.fallback_missed(FP1, GEN), "fp1's match was seen");
        assert!(
            keys.fallback_missed(FP2, GEN),
            "fp2's match was in progress"
        );

        // After the snapshot, a match is judged on arrival.
        keys.fallback_match_record((FP1, GEN), Some(65));
        assert!(
            !keys.fallback_missed(FP1, GEN),
            "committed before the snapshot"
        );
        keys.fallback_match_record((FP1, GEN), Some(80));
        assert!(
            keys.fallback_missed(FP1, GEN),
            "committed after the snapshot"
        );

        keys.deactivate(FP1, GEN);
        assert!(
            !keys.fallback_missed(FP1, GEN),
            "cleared with the population"
        );
    }

    /// A match in an unknown transaction is never assumed seen, and too many
    /// matches waiting on the snapshot abort the merge.
    #[test]
    fn test_fallback_match_unknown_or_overflowing_is_missed() {
        let mut keys = PopulationDeletedKeys::default();
        keys.activate(FP1, GEN, &[REL], Lsn::from_raw(100));
        keys.activate(FP2, GEN, &[REL], Lsn::from_raw(100));
        keys.fallback_match_record((FP1, GEN), None);
        keys.snapshot_set(FP1, GEN, Arc::new(OriginSnapshot::new(1000, 1000, vec![])));
        assert!(keys.fallback_missed(FP1, GEN));

        for xid in 0..=POPULATION_DELETED_KEY_CAP {
            keys.fallback_match_record((FP2, GEN), u32::try_from(xid).ok());
        }
        assert!(keys.fallback_missed(FP2, GEN), "overflow aborts");
    }

    /// A returned table is reused by the next checkout (no recreate).
    #[test]
    fn test_staging_pool_reuses_freed_table() {
        let mut pool = StagingPool::default();
        let fp = Fingerprint::from_raw(1);

        let out = pool.checkout(fp, Generation::from_raw(1), &[REL]);
        let (oid, name, needs_create) = out[0].clone();
        assert_eq!(oid, REL);
        assert!(needs_create, "first checkout mints a new slot");

        for (o, t, _epoch) in pool.take(fp, Generation::from_raw(1)) {
            pool.release(o, t);
        }

        let out2 = pool.checkout(fp, Generation::from_raw(2), &[REL]);
        let (_, name2, needs_create2) = out2[0].clone();
        assert_eq!(name2, name, "reuses the freed table");
        assert!(!needs_create2, "reused table is not recreated");
    }

    /// A relation schema change bumps the epoch, so a table checked out under the
    /// old epoch is detected as stale (dropped at check-in, not reused).
    #[test]
    fn test_staging_pool_epoch_marks_checked_out_table_stale() {
        let mut pool = StagingPool::default();
        let fp = Fingerprint::from_raw(1);

        pool.checkout(fp, Generation::from_raw(1), &[REL]);
        let held = pool.take(fp, Generation::from_raw(1));
        let (oid, _name, epoch_at_checkout) = held[0].clone();
        assert!(pool.epoch_current(oid, epoch_at_checkout));

        // Schema change: the checked-out table's epoch is now stale.
        let stale_free = pool.relation_epoch_bump(REL);
        assert!(
            stale_free.is_empty(),
            "the only table is checked out, not free"
        );
        assert!(!pool.epoch_current(oid, epoch_at_checkout));
    }

    /// A schema change returns the relation's free tables for dropping and clears
    /// the free-list, so the next checkout mints a fresh (correct-shape) slot.
    #[test]
    fn test_staging_pool_epoch_bump_returns_and_clears_free() {
        let mut pool = StagingPool::default();
        let fp = Fingerprint::from_raw(1);

        let name = pool.checkout(fp, Generation::from_raw(1), &[REL])[0]
            .1
            .clone();
        for (o, t, _epoch) in pool.take(fp, Generation::from_raw(1)) {
            pool.release(o, t);
        }

        let stale = pool.relation_epoch_bump(REL);
        assert_eq!(
            stale,
            vec![name],
            "schema change returns free tables to drop"
        );

        let out = pool.checkout(fp, Generation::from_raw(2), &[REL]);
        assert!(out[0].2, "after purge the next checkout mints a fresh slot");
    }
}
