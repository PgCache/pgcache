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

use ecow::EcoString;
use postgres_protocol::escape;
use tracing::error;

use crate::catalog::TableMetadata;

use super::super::messages::PopulationMerge;
use super::super::{CacheError, CacheResult, MapIntoReport, ReportExt};
use super::core::WriterCore;

/// Per-relation cap on retained distinct deleted-key tuples before the set
/// overflows. A backstop against unbounded growth under sustained overlapping
/// populations on a hot, high-delete relation (refcount never reaching 0) — on
/// overflow the keys are dropped to free memory and every merge over the
/// relation is aborted (the query repopulates with a fresh snapshot) until the
/// last in-flight population deactivates. Slice B replaces this blunt cap with
/// an LSN-anchored prune.
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
#[derive(Default)]
pub(super) struct PopulationDeletedKeys {
    relations: HashMap<u32, DeletedKeyEntry>,
    /// fingerprint → relations it activated, so deactivate is exact.
    inflight: HashMap<u64, Vec<u32>>,
}

#[derive(Default)]
struct DeletedKeyEntry {
    /// Number of in-flight populations over this relation.
    refcount: usize,
    /// Rendered PK tuple bodies (e.g. `42` or `'a','b'`).
    keys: HashSet<EcoString>,
    /// Set once `keys` exceeds the cap; keys are dropped and merges abort until
    /// `refcount` returns to 0.
    overflowed: bool,
}

impl PopulationDeletedKeys {
    /// Begin recording deletes for a population's relations. Called at dispatch,
    /// before the worker reads its snapshot — a delete CDC processed earlier has
    /// an LSN below the watermark, hence below the snapshot boundary, so its row
    /// isn't in the snapshot and can't be resurrected. Idempotent per fingerprint.
    pub(super) fn activate(&mut self, fingerprint: u64, relation_oids: &[u32]) {
        if self.inflight.contains_key(&fingerprint) {
            return;
        }
        for &oid in relation_oids {
            self.relations.entry(oid).or_default().refcount += 1;
        }
        self.inflight.insert(fingerprint, relation_oids.to_vec());
    }

    /// Stop recording for a population. Clears a relation's set once its last
    /// in-flight population leaves.
    pub(super) fn deactivate(&mut self, fingerprint: u64) {
        let Some(oids) = self.inflight.remove(&fingerprint) else {
            return;
        };
        for oid in oids {
            if let Some(entry) = self.relations.get_mut(&oid) {
                entry.refcount = entry.refcount.saturating_sub(1);
                if entry.refcount == 0 {
                    self.relations.remove(&oid);
                }
            }
        }
    }

    /// Record a removed PK for `relation_oid` if a population is recording it.
    pub(super) fn record(&mut self, relation_oid: u32, key: EcoString) {
        let Some(entry) = self.relations.get_mut(&relation_oid) else {
            return;
        };
        if entry.overflowed {
            return;
        }
        entry.keys.insert(key);
        if entry.keys.len() > POPULATION_DELETED_KEY_CAP {
            entry.keys.clear();
            entry.keys.shrink_to_fit();
            entry.overflowed = true;
            error!(
                relation_oid,
                "population deleted-key set overflowed cap {POPULATION_DELETED_KEY_CAP}; \
                 affected populations will repopulate"
            );
        }
    }

    fn overflowed(&self, relation_oid: u32) -> bool {
        self.relations
            .get(&relation_oid)
            .is_some_and(|e| e.overflowed)
    }

    /// Build the `(<pk cols>) NOT IN (...)` predicate excluding recorded deletes,
    /// or `None` when there's nothing to exclude.
    fn filter_predicate(&self, relation_oid: u32, pk_columns_paren: &str) -> Option<String> {
        let entry = self.relations.get(&relation_oid)?;
        if entry.overflowed || entry.keys.is_empty() {
            return None;
        }
        let mut tuples = String::new();
        for (i, key) in entry.keys.iter().enumerate() {
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
    /// Merge one population's staging tables into the shared cache tables,
    /// filtering keys CDC removed during the population (PGC-250). Generation is
    /// stamped here (moved off the population worker). Best-effort drops staging
    /// on every path. Caller deactivates the deleted-key set and marks the query
    /// Ready / Failed based on the outcome.
    pub(super) async fn population_merge_apply(
        &mut self,
        merge: &PopulationMerge,
    ) -> CacheResult<MergeOutcome> {
        // If any relation's set overflowed we lost deleted keys and can't
        // guarantee no resurrected rows — abort and let the query repopulate.
        if merge
            .staged
            .iter()
            .any(|(oid, _)| self.population_deleted_keys.overflowed(*oid))
        {
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

    /// Best-effort drop of a staging table. A leak here is recovered by the next
    /// cache-database reset (the whole `pgcache_stage` schema is dropped).
    async fn staging_drop(&self, staging: &str) {
        let sql = format!("DROP TABLE IF EXISTS pgcache_stage.{staging}");
        if let Err(e) = self.db_cache.batch_execute(&sql).await {
            error!("dropping staging table {staging}: {e}");
        }
    }
}
