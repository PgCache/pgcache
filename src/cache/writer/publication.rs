use std::collections::HashSet;
use std::sync::Arc;

use postgres_protocol::escape;
use tracing::debug;

use super::core::WriterCore;
use crate::cache::{CacheError, CacheResult, MapIntoReport, ReportExt};
use crate::oid::Oid;

impl WriterCore {
    /// Increment refcounts for each relation_oid the new cached_query
    /// touches. On 0→1 transitions, clone-mutate-swap the
    /// `active_relations` snapshot and set `relations_dirty` so the next
    /// `publication_dirty_drain` syncs the origin publication.
    ///
    /// O(|oids| + |active_set|) per call vs. the previous O(|cached_queries|)
    /// rebuild — typically a handful of integer ops since most registers
    /// add no new tables. Returns `true` if the active set changed; a
    /// registration then adds the new relations inline
    /// (`publication_relations_add`), since they must be in the publication
    /// before its population snapshots origin.
    pub(super) fn active_relations_acquire(&mut self, oids: &[Oid]) -> bool {
        let mut newly_active: Vec<Oid> = Vec::new();
        for &oid in oids {
            let count = self.relation_refcounts.entry(oid).or_insert(0);
            if *count == 0 {
                newly_active.push(oid);
            }
            *count += 1;
        }
        if newly_active.is_empty() {
            return false;
        }
        let mut new_set = (**self.active_relations.load()).clone();
        for oid in newly_active {
            new_set.insert(oid);
        }
        self.active_relations.store(Arc::new(new_set));
        self.relations_dirty = true;
        true
    }

    /// Decrement refcounts. On 1→0 transitions, drop the oid from the
    /// `active_relations` snapshot and set `relations_dirty`. Removal paths
    /// don't need to sync the publication inline — stale subscriptions
    /// to dropped relations are filtered out by the writer ignoring CDC
    /// events for relations not in `active_relations`. Returns `true` if
    /// the active set changed.
    pub(super) fn active_relations_release(&mut self, oids: &[Oid]) -> bool {
        let mut newly_inactive: Vec<Oid> = Vec::new();
        for &oid in oids {
            if let Some(count) = self.relation_refcounts.get_mut(&oid) {
                *count -= 1;
                if *count == 0 {
                    self.relation_refcounts.remove(&oid);
                    newly_inactive.push(oid);
                }
            }
        }
        if newly_inactive.is_empty() {
            return false;
        }
        let mut new_set = (**self.active_relations.load()).clone();
        for oid in newly_inactive {
            new_set.remove(&oid);
        }
        self.active_relations.store(Arc::new(new_set));
        self.relations_dirty = true;
        true
    }

    /// Add the active relations not yet published, inline at registration.
    /// Only additions: a removal drops the relation's cache table with it,
    /// and that `DROP TABLE` on the cache DB would wait on the locks an open
    /// frame holds, with the writer loop that commits the frame stuck here
    /// (PGC-501). Removals wait for the guarded drain.
    pub(super) async fn publication_relations_add(&mut self) -> CacheResult<()> {
        let added: Vec<Oid> = self
            .active_relations
            .load()
            .iter()
            .copied()
            .filter(|oid| {
                !self.publication_oids.contains(oid) && self.cache.tables.contains_key1(oid)
            })
            .collect();
        if added.is_empty() {
            return Ok(());
        }
        let sql = format!(
            "ALTER PUBLICATION {} ADD TABLE {}",
            self.publication_name,
            self.oids_to_table_list(&added)
        );
        debug!("publication add: {sql}");
        self.db_origin
            .batch_execute(&sql)
            .await
            .map_into_report::<CacheError>()
            .attach_loc("adding relations to the publication")?;
        self.publication_oids.extend(added);
        Ok(())
    }

    /// Bring the publication to the active set and drop the cache tables of
    /// everything that left it. Only the guarded drain
    /// (`publication_dirty_drain`) runs this: the drops need locks an open
    /// frame may hold.
    async fn publication_update(&mut self) -> CacheResult<()> {
        let new_oids: HashSet<Oid> = (**self.active_relations.load()).clone();

        if new_oids == self.publication_oids {
            // Already in sync. Clear the dirty flag so a deferred drain
            // doesn't redo this comparison.
            self.relations_dirty = false;
            self.forgotten_tables_drop().await;
            return Ok(());
        }

        let removed: Vec<Oid> = self
            .publication_oids
            .difference(&new_oids)
            .copied()
            .collect();

        let sql = if new_oids.is_empty() {
            let published: Vec<Oid> = self.publication_oids.iter().copied().collect();
            let table_list = self.origin_table_list(&published).await?;
            (!table_list.is_empty()).then(|| {
                format!(
                    "ALTER PUBLICATION {} DROP TABLE {}",
                    self.publication_name, table_list
                )
            })
        } else {
            let table_list = self.oids_to_table_list(&new_oids.iter().copied().collect::<Vec<_>>());
            Some(format!(
                "ALTER PUBLICATION {} SET TABLE {}",
                self.publication_name, table_list
            ))
        };

        if let Some(sql) = sql {
            debug!("publication update: {sql}");
            self.db_origin
                .batch_execute(&sql)
                .await
                .map_into_report::<CacheError>()
                .attach_loc("updating publication table list")?;
        }
        self.publication_oids = new_oids;

        if !removed.is_empty() {
            self.cache_tables_drop(&removed).await;
        }
        self.forgotten_tables_drop().await;
        // Publication now matches active_relations; any pending drain is
        // satisfied by this call.
        self.relations_dirty = false;
        Ok(())
    }

    /// Resolve active relations to a comma-separated `schema.table` string
    /// from the writer's catalog.
    fn oids_to_table_list(&self, oids: &[Oid]) -> String {
        oids.iter()
            .filter_map(|oid| self.cache.tables.get1(oid))
            .map(|table| table_qualified(&table.schema, &table.name))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Resolve relations leaving the publication to their current names on
    /// origin. A forgotten relation is out of the writer's catalog, and the
    /// name it was registered under may be stale by now (`RENAME`, `SET
    /// SCHEMA`); one dropped since is already out of the publication and is
    /// left out here.
    async fn origin_table_list(&self, oids: &[Oid]) -> CacheResult<String> {
        let rows = self
            .db_origin
            .query(
                "SELECT n.nspname, c.relname FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = ANY($1)",
                &[&oids],
            )
            .await
            .map_into_report::<CacheError>()
            .attach_loc("resolving the names of relations leaving the publication")?;
        Ok(rows
            .iter()
            .map(|row| table_qualified(row.get(0), row.get(1)))
            .collect::<Vec<_>>()
            .join(", "))
    }
}

fn table_qualified(schema: &str, name: &str) -> String {
    format!(
        "{}.{}",
        escape::escape_identifier(schema),
        escape::escape_identifier(name)
    )
}

impl WriterCore {
    /// Drain the dirty flag: sync the origin publication (which also drops
    /// orphaned cache tables). `active_relations` is kept up to date
    /// incrementally via `active_relations_acquire` / `_release`, so the only
    /// remaining work here is the publication sync itself.
    pub(super) async fn publication_dirty_drain(&mut self) -> CacheResult<()> {
        if !self.relations_dirty {
            return Ok(());
        }
        // Defer while a CDC frame is open: publication_update's
        // cache_tables_drop (DROP TABLE on db_cache) would block on the
        // frame's uncommitted cache-table locks. relations_dirty stays set;
        // the drain re-runs after frame_commit at CommitMark.
        if self.frame_holds_locks() {
            return Ok(());
        }
        self.relations_dirty = false;
        self.publication_update().await?;
        Ok(())
    }
}
