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

use std::collections::HashMap;

use ecow::EcoString;
use postgres_protocol::escape;
use tracing::error;

use crate::catalog::TableMetadata;

use super::super::messages::PopulationMerge;
use super::super::{CacheError, CacheResult, MapIntoReport, ReportExt};
use super::core::WriterCore;

/// Per-relation backstop cap on retained distinct deleted-key tuples. The
/// primary bound is the LSN-anchored prune (`DeletedKeyEntry::prune`): a deleted
/// key is dropped once every in-flight population's snapshot is at or after the
/// delete, so the set is bounded by the oldest in-flight population's window.
/// The cap only bites if a single population stays in flight long enough on a
/// high-delete relation to accumulate this many keys; on overflow the keys are
/// dropped and every merge over the relation is aborted (the query repopulates
/// with a fresh snapshot) until the last in-flight population deactivates.
const POPULATION_DELETED_KEY_CAP: usize = 100_000;

/// Outcome of a population merge.
pub(super) enum MergeOutcome {
    /// Staging merged into the cache; mark the query Ready.
    Merged,
    /// A relation's deleted-key set overflowed — the merge can't guarantee no
    /// resurrected rows, so the query is failed and repopulates later.
    Aborted,
}

/// Tracks primary keys removed by CDC while populations are in flight, per
/// relation, so a population merge can filter out rows CDC already removed.
///
/// Each deleted key is stamped with the commit LSN at which it was removed, and
/// each in-flight population contributes an *anchor floor* (a lower bound on its
/// snapshot LSN). A delete at `lsn_d` only matters to a population whose
/// snapshot predates it (`snapshot < lsn_d`); once every in-flight population's
/// floor reaches `lsn_d`, the key is irrelevant and pruned. This bounds the set
/// to the oldest in-flight population's window rather than letting it grow with
/// total delete volume under sustained overlap.
/// Identifies one in-flight population: `(fingerprint, generation)`. Keying on
/// the pair (not fingerprint alone) keeps two populations of the same query —
/// e.g. one parked post-merge while a readmit dispatches the next generation —
/// tracked independently, so deactivating one never tears down the other.
type PopulationKey = (u64, u64);

#[derive(Default)]
pub(super) struct PopulationDeletedKeys {
    relations: HashMap<u32, DeletedKeyEntry>,
    /// population → relations it activated, so deactivate is exact.
    inflight: HashMap<PopulationKey, Vec<u32>>,
}

#[derive(Default)]
struct DeletedKeyEntry {
    /// In-flight populations over this relation: population → anchor floor
    /// (lower bound on the population's snapshot LSN). Empty ⇒ remove the entry.
    floors: HashMap<PopulationKey, u64>,
    /// Rendered PK tuple body (e.g. `42` or `'a','b'`) → commit LSN of the delete.
    keys: HashMap<EcoString, u64>,
    /// Set once `keys` exceeds the cap; keys are dropped and *every* merge over
    /// the relation aborts (keys genuinely lost) until the last population leaves.
    overflowed: bool,
    /// Highest LSN of a bulk invalidation (TRUNCATE, 40P01 recovery) over this
    /// relation. A merge whose snapshot predates it would resurrect rows the
    /// event removed, so it aborts; a population that snapshotted at/after it is
    /// unaffected — so unlike `overflowed`, this self-clears for fresh snapshots.
    aborted_below: u64,
}

impl DeletedKeyEntry {
    /// Drop keys no in-flight population can still need: a delete at `lsn_d` is
    /// irrelevant once every floor is `>= lsn_d` (every snapshot is at/after the
    /// delete, so none read the row alive). Keeps keys with `lsn_d > min(floor)`.
    fn prune(&mut self) {
        let Some(min_floor) = self.floors.values().copied().min() else {
            return;
        };
        self.keys.retain(|_, lsn| *lsn > min_floor);
    }

    /// Cap overflow: keys are lost, so every merge over this relation must abort
    /// until the last in-flight population leaves.
    fn disable(&mut self) {
        self.keys.clear();
        self.keys.shrink_to_fit();
        self.overflowed = true;
    }

    /// A bulk invalidation committed at `lsn` emptied/dropped rows; raise the
    /// abort watermark and drop now-stale keys (anything `<= lsn` is below the
    /// truncate and irrelevant to surviving post-event snapshots).
    fn abort_below(&mut self, lsn: u64) {
        self.aborted_below = self.aborted_below.max(lsn);
        self.keys.retain(|_, key_lsn| *key_lsn > lsn);
    }

    /// Whether a merge whose snapshot is `snapshot_lsn` must abort: keys were
    /// lost (overflow), or the snapshot predates a bulk invalidation.
    fn should_abort(&self, snapshot_lsn: u64) -> bool {
        self.overflowed || snapshot_lsn < self.aborted_below
    }
}

impl PopulationDeletedKeys {
    /// Begin recording deletes for a population's relations. Called at dispatch,
    /// before the worker reads its snapshot — a delete CDC processed earlier has
    /// an LSN below the watermark, hence below the snapshot boundary, so its row
    /// isn't in the snapshot and can't be resurrected. `anchor_floor` is the
    /// apply watermark at dispatch, a lower bound on this population's snapshot
    /// LSN, used to prune keys it can no longer need. Idempotent per fingerprint.
    pub(super) fn activate(
        &mut self,
        fingerprint: u64,
        generation: u64,
        relation_oids: &[u32],
        anchor_floor: u64,
    ) {
        let key = (fingerprint, generation);
        if self.inflight.contains_key(&key) {
            return;
        }
        for &oid in relation_oids {
            self.relations
                .entry(oid)
                .or_default()
                .floors
                .insert(key, anchor_floor);
        }
        self.inflight.insert(key, relation_oids.to_vec());
    }

    /// Stop recording for a population: drop its floor and prune (its departure
    /// may have raised the relation's min floor). Removes a relation's entry once
    /// its last in-flight population leaves.
    pub(super) fn deactivate(&mut self, fingerprint: u64, generation: u64) {
        let key = (fingerprint, generation);
        let Some(oids) = self.inflight.remove(&key) else {
            return;
        };
        for oid in oids {
            if let Some(entry) = self.relations.get_mut(&oid) {
                entry.floors.remove(&key);
                if entry.floors.is_empty() {
                    self.relations.remove(&oid);
                } else {
                    entry.prune();
                }
            }
        }
    }

    /// Record a removed PK (stamped with the delete's commit LSN) for
    /// `relation_oid` if a population is recording it.
    pub(super) fn record(&mut self, relation_oid: u32, key: EcoString, lsn: u64) {
        let Some(entry) = self.relations.get_mut(&relation_oid) else {
            return;
        };
        if entry.overflowed {
            return;
        }
        entry.keys.insert(key, lsn);
        if entry.keys.len() > POPULATION_DELETED_KEY_CAP {
            entry.disable();
            error!(
                relation_oid,
                "population deleted-key set overflowed cap {POPULATION_DELETED_KEY_CAP}; \
                 affected populations will repopulate"
            );
        }
    }

    /// Whether any population is recording deletes for `relation_oid`. Lets the
    /// CDC delete path skip rendering a key when nothing would consume it.
    pub(super) fn is_recording(&self, relation_oid: u32) -> bool {
        self.relations.contains_key(&relation_oid)
    }

    /// Drop a tracked key whose row CDC has re-written (PGC-260): the row is
    /// alive at origin again, so filtering its key would make a population
    /// merge omit a live row — while resurrection of the old version is
    /// impossible anyway once the live row is in the shared cache table
    /// (merges never overwrite). Returns whether the key was tracked.
    pub(super) fn cancel(&mut self, relation_oid: u32, key: &str) -> bool {
        self.relations
            .get_mut(&relation_oid)
            .is_some_and(|entry| entry.keys.remove(key).is_some())
    }

    /// Raise the abort watermark for `relation_oid` to `lsn` — a bulk
    /// invalidation (TRUNCATE / 40P01 recovery) committed there. Merges whose
    /// snapshot predates `lsn` abort and repopulate; a population that
    /// snapshotted at/after `lsn` is unaffected (it sees the empty table), so
    /// this self-clears rather than blanket-aborting like overflow. No-op if no
    /// population is recording the relation.
    pub(super) fn abort_below(&mut self, relation_oid: u32, lsn: u64) {
        if let Some(entry) = self.relations.get_mut(&relation_oid) {
            entry.abort_below(lsn);
        }
    }

    /// Whether a merge over `relation_oid` with snapshot `snapshot_lsn` must
    /// abort (cap overflow, or snapshot predates a bulk invalidation).
    fn should_abort(&self, relation_oid: u32, snapshot_lsn: u64) -> bool {
        self.relations
            .get(&relation_oid)
            .is_some_and(|e| e.should_abort(snapshot_lsn))
    }

    /// Build the `(<pk cols>) NOT IN (...)` predicate excluding recorded deletes,
    /// or `None` when there's nothing to exclude.
    fn filter_predicate(&self, relation_oid: u32, pk_columns_paren: &str) -> Option<String> {
        let entry = self.relations.get(&relation_oid)?;
        if entry.overflowed || entry.keys.is_empty() {
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

/// Render a row's primary-key values as a tuple body (escaped literals joined by
/// `,`), matching how the staging columns were loaded and how `cache_delete_into`
/// renders PK values. `None` if no PK value is present.
pub(super) fn pk_body_render(
    table_metadata: &TableMetadata,
    row_data: &[Option<String>],
) -> Option<EcoString> {
    let mut body = String::new();
    let mut first = true;
    for pk_column in &table_metadata.primary_key_columns {
        let column_meta = table_metadata.columns.get(pk_column.as_str())?;
        let row_value = row_data.get(column_meta.index())?;
        let literal = row_value
            .as_deref()
            .map_or_else(|| "NULL".to_owned(), escape::escape_literal);
        if !first {
            body.push(',');
        }
        body.push_str(&literal);
        first = false;
    }
    if first {
        None
    } else {
        Some(EcoString::from(body))
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
            .map(|c| format!("\"{}\"", c.name))
            .collect::<Vec<_>>()
            .join(",");
        let pk_quoted: Vec<String> = table
            .primary_key_columns
            .iter()
            .map(|c| format!("\"{c}\""))
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

    fn merge_sql(&self, staging: &str, generation: u64, filter: Option<&str>) -> String {
        let where_clause = filter.map_or_else(String::new, |f| format!(" WHERE {f}"));
        // DISTINCT ON the PK collapses duplicate keys a set-operation query can
        // stage (same relation in multiple branches) — without it the upsert
        // would error "ON CONFLICT cannot affect row a second time". The full
        // row is identical per PK under a snapshot, so the pick is immaterial.
        format!(
            "SET mem.query_generation = {generation}; \
             INSERT INTO {schema}.{name} ({cols}) \
             SELECT DISTINCT ON {pk} {cols} FROM pgcache_stage.{staging}{where_clause} {conflict}; \
             SET mem.query_generation = 0",
            schema = self.schema,
            name = self.name,
            cols = self.columns_csv,
            pk = self.pk_columns_paren,
            conflict = self.conflict,
        )
    }
}

impl WriterCore {
    /// A CDC insert/update re-wrote the row at `row_data`'s PK — cancel any
    /// tracked deletion of that key (PGC-260): the frame-pending entry (an
    /// earlier delete in this frame nets out, in event order) and the recorded
    /// set (deletes from earlier frames). A lingering key would make population
    /// merges omit a legitimately live row. Returns whether anything was
    /// tracked, so the insert path can force-upsert an otherwise-unmatched row
    /// (the live row in the shared table is what makes cancellation safe).
    pub(super) fn population_deleted_key_cancel(
        &mut self,
        relation_oid: u32,
        row_data: &[Option<String>],
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
        tracked
    }

    /// Merge one population's staging tables into the shared cache tables,
    /// filtering keys CDC removed during the population (PGC-250). Generation is
    /// stamped here (moved off the population worker). Best-effort drops staging
    /// on every path. Caller deactivates the deleted-key set and marks the query
    /// Ready / Failed based on the outcome.
    pub(super) async fn population_merge_apply(
        &mut self,
        merge: &PopulationMerge,
    ) -> CacheResult<MergeOutcome> {
        // Abort if any relation lost keys (overflow) or was bulk-invalidated
        // (TRUNCATE / recovery) at an LSN past this population's snapshot —
        // merging would resurrect removed rows. Let the query repopulate.
        if merge.staged.iter().any(|(oid, _)| {
            self.population_deleted_keys
                .should_abort(*oid, merge.snapshot_lsn)
        }) {
            for (_, staging) in &merge.staged {
                self.staging_drop(staging).await;
            }
            return Ok(MergeOutcome::Aborted);
        }

        for (relation_oid, staging) in &merge.staged {
            let plan = {
                // Scope the metadata borrow so it doesn't span the await below.
                let Some(table) = self.cache.tables.get1(relation_oid) else {
                    // Relation evicted mid-population; nothing to merge into.
                    self.staging_drop(staging).await;
                    continue;
                };
                MergePlan::build(table)
            };
            let filter = self
                .population_deleted_keys
                .filter_predicate(*relation_oid, &plan.pk_columns_paren);
            let sql = plan.merge_sql(staging, merge.generation, filter.as_deref());

            self.db_cache
                .batch_execute(&sql)
                .await
                .map_into_report::<CacheError>()
                .attach_loc("population merge")?;
            self.staging_drop(staging).await;
        }
        Ok(MergeOutcome::Merged)
    }

    /// Drop all of a population's staging tables (used when a queued merge is
    /// abandoned because its query was evicted/superseded before draining).
    pub(super) async fn population_staging_drop(&self, staged: &[(u32, EcoString)]) {
        for (_, staging) in staged {
            self.staging_drop(staging).await;
        }
    }

    /// Best-effort drop of a staging table. A leak here is recovered by the next
    /// cache-database reset (the whole `pgcache_stage` schema is dropped).
    async fn staging_drop(&self, staging: &str) {
        let sql = format!("DROP TABLE IF EXISTS pgcache_stage.{staging}");
        if let Err(e) = self.db_cache.batch_execute(&sql).await {
            error!("dropping staging table {staging}: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const REL: u32 = 10;
    const GEN: u64 = 1;

    fn record(keys: &mut PopulationDeletedKeys, body: &str, lsn: u64) {
        keys.record(REL, EcoString::from(body), lsn);
    }

    /// Deactivating a population raises the relation's min floor, pruning deletes
    /// that predate every remaining in-flight population's snapshot.
    #[test]
    fn deactivate_prunes_below_min_floor() {
        let mut keys = PopulationDeletedKeys::default();
        keys.activate(1, GEN, &[REL], 100);
        keys.activate(2, GEN, &[REL], 200);
        record(&mut keys, "5", 50);
        record(&mut keys, "15", 150);
        record(&mut keys, "25", 250);

        // Min floor is 100 (fp1); the merge still filters all three.
        let before = keys.filter_predicate(REL, "(id)").expect("filter present");
        assert!(before.contains("(5)") && before.contains("(15)") && before.contains("(25)"));

        // fp1 leaves → min floor becomes 200 → deletes at LSN <= 200 are
        // irrelevant to fp2 (snapshot >= 200) and pruned.
        keys.deactivate(1, GEN);
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
    fn generations_of_same_fingerprint_are_independent() {
        let mut keys = PopulationDeletedKeys::default();
        keys.activate(7, 5, &[REL], 100); // gen 5, parked
        keys.activate(7, 8, &[REL], 100); // gen 8, readmitted

        // gen 5 finishes; gen 8 must still be recording for the relation.
        keys.deactivate(7, 5);
        assert!(keys.is_recording(REL), "gen 8 still in flight");
        record(&mut keys, "5", 150);
        assert!(
            keys.filter_predicate(REL, "(id)").is_some(),
            "gen 8 still records deletes"
        );

        // Once gen 8 also leaves, the entry clears.
        keys.deactivate(7, 8);
        assert!(!keys.is_recording(REL));
    }

    /// The relation's entry disappears once the last population leaves.
    #[test]
    fn deactivate_last_population_clears_entry() {
        let mut keys = PopulationDeletedKeys::default();
        keys.activate(1, GEN, &[REL], 100);
        record(&mut keys, "5", 150);
        keys.deactivate(1, GEN);
        assert!(keys.filter_predicate(REL, "(id)").is_none());
    }

    /// Deletes for a relation no population is reading are dropped on the floor.
    #[test]
    fn record_without_active_population_is_noop() {
        let mut keys = PopulationDeletedKeys::default();
        record(&mut keys, "5", 150);
        assert!(keys.filter_predicate(REL, "(id)").is_none());
    }

    /// Exceeding the cap drops keys and aborts every merge over the relation.
    #[test]
    fn overflow_aborts_and_disables_filtering() {
        let mut keys = PopulationDeletedKeys::default();
        keys.activate(1, GEN, &[REL], 0);
        for i in 0..=POPULATION_DELETED_KEY_CAP {
            record(&mut keys, &i.to_string(), 1);
        }
        // Overflow aborts regardless of snapshot LSN, and there's no filter.
        assert!(keys.should_abort(REL, u64::MAX));
        assert!(keys.filter_predicate(REL, "(id)").is_none());
    }

    /// A bulk invalidation aborts only merges whose snapshot predates it; a
    /// population that snapshotted at/after it is unaffected (self-clearing),
    /// and now-stale keys are pruned.
    #[test]
    fn abort_below_aborts_only_older_snapshots() {
        let mut keys = PopulationDeletedKeys::default();
        keys.activate(1, GEN, &[REL], 100);
        record(&mut keys, "5", 150);
        record(&mut keys, "25", 250);

        keys.abort_below(REL, 200); // e.g. a TRUNCATE committed at LSN 200

        // A population that read before the truncate must abort; one that read
        // at/after it must not.
        assert!(keys.should_abort(REL, 150), "pre-truncate snapshot aborts");
        assert!(
            !keys.should_abort(REL, 200),
            "snapshot at the truncate is fine"
        );
        assert!(
            !keys.should_abort(REL, 300),
            "post-truncate snapshot is fine"
        );

        // Keys at/below the truncate LSN are pruned; later ones survive.
        let filter = keys.filter_predicate(REL, "(id)").expect("filter present");
        assert!(
            filter.contains("(25)") && !filter.contains("(5)"),
            "filter: {filter}"
        );
    }
}
