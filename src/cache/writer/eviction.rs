use crate::catalog::Oid;
use crate::query::Fingerprint;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use tracing::{debug, error, trace, warn};

use crate::query::ast::Deparse;
use crate::result::error_chain_format;
use crate::settings::CachePolicy;

use super::super::memo::SlotKey;
use super::super::{CacheError, CacheResult, MapIntoReport, types::CachedQueryState};

use super::core::*;

impl WriterCore {
    /// Refresh the cached `statvfs` reading for the cache PG data directory (one
    /// syscall); a no-op leaving the last reading if the data directory wasn't
    /// discovered or can't be stat'd. Disk usage is read from the filesystem,
    /// not summed via `pgcache_total_size()` (PGC-276).
    pub(super) fn disk_stats_refresh(&mut self) {
        let Some(dir) = self.data_dir.as_deref() else {
            return;
        };
        if let Some((total, available)) = crate::memory::disk_stats_bytes(dir) {
            self.disk_total = total;
            self.disk_available = available;
        }
        // Re-resolve the effective cap (config may have changed via PUT /config,
        // and the auto value tracks disk_total).
        self.disk_limit_effective = crate::memory::disk_limit_resolve(
            self.disk_total,
            self.cache.dynamic.load().disk_limit,
        );
    }

    /// Bytes in use on the cache volume (whole-filesystem, not cache-table
    /// specific). Informational gauge value.
    pub(super) fn disk_used(&self) -> u64 {
        self.disk_total.saturating_sub(self.disk_available)
    }

    /// Whether the cache volume is under disk pressure — used bytes exceed the
    /// effective `disk_limit`. `disk_total == 0` (no statvfs reading) disables
    /// disk eviction. Best-effort: reclaim sheds a bounded amount per tick and
    /// re-evaluates next tick, since DROP'd space is reclaimed asynchronously
    /// (PGC-276).
    pub(super) fn disk_pressure(&self) -> bool {
        #[cfg(feature = "fault-injection")]
        if fault::disk_pressure_forced() {
            return true;
        }
        self.disk_total != 0 && self.disk_used() > self.disk_limit_effective
    }

    /// The query-count cap eviction drives toward: the memory-derived cap
    /// (PGC-251), tightened by the fault-injection override when set.
    fn eviction_count_cap(&self) -> usize {
        let cap = self.state_view.query_count_cap.load(Ordering::Relaxed);
        #[cfg(feature = "fault-injection")]
        let cap = fault::eviction_count_cap().map_or(cap, |o| cap.min(o));
        cap
    }

    /// Run eviction loop. For CLOCK policy, uses second-chance algorithm with reference bit.
    /// For FIFO policy, evicts the oldest-registered query.
    ///
    /// `max_evictions` bounds full evictions per call (`None` = unbounded). The
    /// periodic tick passes a bound so a large count-cap overshoot is reclaimed
    /// gradually across ticks instead of stalling the single-threaded writer.
    pub(super) async fn eviction_run(&mut self, max_evictions: Option<usize>) -> CacheResult<()> {
        // Defer while a CDC frame is open: cache_query_evict / generation_purge
        // would block on the frame's uncommitted cache-table locks. Eviction is
        // periodic/best-effort — the next Ready after the frame commits runs it.
        if self.frame_holds_locks() {
            return Ok(());
        }
        /// Maximum number of generation bumps (second chances) per eviction round.
        /// Bounds re-stamping work and prevents pathological case where all queries are referenced.
        const MAX_BUMPS: usize = 5;
        let mut bumps = 0;
        let mut pinned_skips = 0;
        let mut evicted = 0usize;

        let cfg = self.cache.dynamic.load();
        // Memory count cap (PGC-251): evict down to it. `usize::MAX` = uncapped.
        // Disk pressure is handled separately (throttle + escalating reclaim in
        // `disk_pressure_handle`, PGC-276), not here.
        let count_cap = self.eviction_count_cap();

        debug!(
            count = self.cache.cached_queries.len(),
            count_cap,
            cache_policy = ?cfg.cache_policy,
            "eviction_run entry"
        );

        loop {
            if self.cache.cached_queries.len() <= count_cap {
                break;
            }
            let Some(&min_gen) = self.cache.generations.first() else {
                break;
            };
            let Some(query) = self.cache.cached_queries.get2(&min_gen) else {
                break;
            };
            let fingerprint = query.fingerprint;
            let query_pinned = query.pinned;

            // Pinned queries are never evicted — always bump to move past them.
            // Unlike CLOCK bumps, pinned bumps are not bounded by MAX_BUMPS.
            if query_pinned {
                trace!("pinned bump {fingerprint}");
                crate::metrics::handles()
                    .state
                    .evictions_pinned_bump
                    .increment(1);
                self.cache_query_generation_bump(fingerprint).await?;
                pinned_skips += 1;
                if pinned_skips >= self.cache.cached_queries.len() {
                    break; // all remaining candidates are pinned
                }
                continue;
            }

            // CLOCK second-chance: referenced queries get bumped (bounded by MAX_BUMPS)
            if cfg.cache_policy == CachePolicy::Clock && bumps < MAX_BUMPS {
                let referenced = self
                    .state_view
                    .cached_queries
                    .get(&fingerprint)
                    .map(|e| e.referenced)
                    .unwrap_or(false);

                if referenced {
                    trace!("clock bump {fingerprint}");
                    crate::metrics::handles().state.evictions_bump.increment(1);
                    self.cache_query_generation_bump(fingerprint).await?;
                    bumps += 1;
                    continue;
                }
            }

            // Evict (full removal) — cache_query_evict emits its own entry log
            crate::metrics::handles().state.evictions.increment(1);
            self.cache_query_evict(fingerprint).await?;
            // publication_dirty_drain drops the orphaned cache tables; the freed
            // disk space is reclaimed asynchronously and observed by a later
            // tick's statvfs read, not measured here (PGC-276).
            self.publication_dirty_drain().await?;
            bumps = 0;
            pinned_skips = 0;
            evicted += 1;
            if max_evictions.is_some_and(|m| evicted >= m) {
                break;
            }
        }

        // stale_entries_cleanup runs on the 1s gauges tick instead of here —
        // it is GC of dead Pending/Invalidated entries, not eviction-critical,
        // and its O(cached_queries) scan would dominate Ready handling.
        Ok(())
    }

    /// Disk-pressure handling on the 1 s tick (PGC-276). Disk is cheap and
    /// plentiful, so hitting the reserve is treated as an emergency ("don't fill
    /// the volume"): set the `disk_throttle` flag so dispatch stops admitting new
    /// queries, then take one escalating reclaim step per tick.
    ///
    /// Reclaim can't be a tight loop — `DROP TABLE` frees disk asynchronously and
    /// statvfs won't reflect it until a later read — so we pace it: rung 1 purges
    /// dead-generation rows, rung 2 sweeps Dirty MVs, rung 3+ drops the
    /// fewest-queries source table (least collateral). After a rung-3 drop we
    /// skip one tick so the freed space lands in statvfs before deciding again.
    pub(super) async fn disk_pressure_handle(&mut self) -> CacheResult<()> {
        let pressure = self.disk_pressure();
        self.state_view
            .disk_throttle
            .store(pressure, Ordering::Relaxed);
        if !pressure {
            self.disk_pressure_ticks = 0;
            self.disk_drop_backoff = false;
            return Ok(());
        }
        // Reclaim mutates db_cache (purge/evict/drop); defer while a frame holds
        // cache-table locks — the throttle alone holds the line until it commits.
        if self.frame_holds_locks() {
            return Ok(());
        }
        // One-tick backoff after a dramatic drop so the async reclaim shows up in
        // statvfs before we consider dropping another table.
        if self.disk_drop_backoff {
            self.disk_drop_backoff = false;
            return Ok(());
        }

        self.disk_pressure_ticks = self.disk_pressure_ticks.saturating_add(1);
        match self.disk_pressure_ticks {
            1 => {
                // Rung 1: reclaim disk held by superseded generations (no query impact).
                let threshold = self.cache.generation_purge_threshold();
                self.generation_purge(threshold).await?;
            }
            2 => {
                // Rung 2: truncate Dirty MV tables (queries fall back to source eval).
                self.mv_dirty_sweep().await?;
            }
            _ => {
                // Rung 3+: drop the fewest-queries source table — coarse, but disk
                // pressure is an emergency. Back off a tick afterwards.
                self.disk_reclaim_drop_smallest().await?;
                self.disk_drop_backoff = true;
            }
        }
        Ok(())
    }

    /// Emergency reclaim: drop the source cache table referenced by the fewest
    /// cached queries (least collateral) by evicting all of its queries — the
    /// last release leaves the table unreferenced, and `publication_dirty_drain`
    /// drops it, freeing disk. No-op when the cache is empty.
    async fn disk_reclaim_drop_smallest(&mut self) -> CacheResult<()> {
        let fingerprints = self
            .cache
            .update_queries
            .iter()
            .filter(|entry| !entry.queries.is_empty())
            .min_by_key(|entry| entry.queries.len())
            .map(|entry| entry.queries.keys().copied().collect::<Vec<Fingerprint>>());
        let Some(fingerprints) = fingerprints else {
            return Ok(());
        };
        warn!(
            query_count = fingerprints.len(),
            "disk pressure: dropping the fewest-queries source table and invalidating its queries"
        );
        for fingerprint in fingerprints {
            crate::metrics::handles().state.evictions.increment(1);
            self.cache_query_evict(fingerprint).await?;
        }
        // Drop the now-unreferenced source cache table(s), freeing disk.
        self.publication_dirty_drain().await?;
        Ok(())
    }

    /// Bump a cached query's generation to give it a second chance in CLOCK eviction.
    /// Re-executes the query against cache DB so the CustomScan tracker re-stamps
    /// dshash entries from old_gen to new_gen.
    async fn cache_query_generation_bump(&mut self, fingerprint: Fingerprint) -> CacheResult<()> {
        let Some(query) = self.cache.cached_queries.get1(&fingerprint) else {
            return Ok(());
        };

        let old_generation = query.generation;
        let resolved = Arc::clone(&query.resolved);

        // 1. Assign new generation (insert before removing old — keeps old gen valid for re-stamp)
        self.cache.generation_counter += 1;
        let new_generation = self.cache.generation_counter;
        self.cache.generations.insert(new_generation);

        // 2. Set query generation on cache DB connection for row tracking
        let set_gen_sql = format!("SET mem.query_generation = {new_generation}");
        self.db_cache
            .batch_execute(&set_gen_sql)
            .await
            .map_into_report::<CacheError>()?;

        // 3. Re-execute query against cache DB (discard results).
        //    The CustomScan tracker side-effect updates dshash from old_gen to new_gen.
        let mut sql = String::with_capacity(512);
        Deparse::deparse(&*resolved, &mut sql);
        self.db_cache
            .batch_execute(&sql)
            .await
            .map_into_report::<CacheError>()?;

        // 4. Reset query generation
        self.db_cache
            .batch_execute("SET mem.query_generation = 0")
            .await
            .map_into_report::<CacheError>()?;

        // 5. Now safe to remove old generation (rows are re-stamped)
        self.cache.generations.remove(&old_generation);

        // 6. Update CachedQuery in BiHashMap (generation is key2, must remove/reinsert)
        if let Some(mut cached) = self.cache.cached_queries.remove1(&fingerprint) {
            cached.generation = new_generation;
            self.cache.cached_queries.insert_overwrite(cached);
        }

        // 7. Clear reference bit and update generation in state_view
        if let Some(mut entry) = self.state_view.cached_queries.get_mut(&fingerprint) {
            entry.referenced = false;
            entry.generation = new_generation;
        }

        Ok(())
    }

    /// GC dead entries across writer state and the shared state view.
    ///
    /// Four passes:
    /// - Snapshot the hit counter into `last_hits_per_gc`; the delta seeds
    ///   dispatch-side Pending-credit sizing and decays existing credits.
    /// - Invalidated, non-pinned entries in `cache.cached_queries` whose
    ///   generation is below the purge threshold (CLOCK-policy carryover
    ///   after CDC invalidation that wasn't readmitted).
    /// - Entries in `state_view.cached_queries`: Pending entries decay their
    ///   credit by the tick delta and are retained iff credit remains;
    ///   Invalidated entries are retained iff generation is above the purge
    ///   threshold.
    /// - Orphaned per-query entries in `state_view.metrics` whose
    ///   fingerprint no longer exists in either map.
    ///
    /// Runs on the 1s gauges tick, not per-command — see callsite.
    pub(super) fn stale_entries_cleanup(&mut self) {
        let cleanup_threshold = self.cache.generation_purge_threshold();

        let hit_delta = self.state_view.hits_since_gc.swap(0, Ordering::Relaxed);
        self.state_view
            .last_hits_per_gc
            .store(hit_delta, Ordering::Relaxed);

        // Remove invalidated entries from cached_queries that are below threshold
        let stale_fingerprints: Vec<Fingerprint> = self
            .cache
            .cached_queries
            .iter()
            .filter(|q| q.invalidated && !q.pinned && q.generation < cleanup_threshold)
            .map(|q| q.fingerprint)
            .collect();

        for fp in &stale_fingerprints {
            if let Some(query) = self.cache.cached_queries.remove1(fp) {
                self.cache
                    .update_queries_remove_fingerprint(*fp, &query.relation_oids);
                self.active_relations_release(&query.relation_oids);
            }
        }

        self.state_view
            .cached_queries
            .retain(|_fp, entry| match &mut entry.state {
                CachedQueryState::Pending { credit, .. } => {
                    *credit = credit.saturating_sub(hit_delta);
                    *credit > 0
                }
                CachedQueryState::Invalidated => entry.generation >= cleanup_threshold,
                CachedQueryState::Loading | CachedQueryState::Ready => true,
            });

        // Remove metrics for fingerprints no longer in either map
        self.state_view.metrics.retain(|fp, _| {
            self.cache.cached_queries.contains_key1(fp)
                || self.state_view.cached_queries.contains_key(fp)
        });
    }

    /// Promote generation-0 entries to `generation_counter + 1` so they become
    /// purgeable in future cycles. Only bumps the counter if entries were promoted.
    async fn generation_zero_promote(&mut self) -> CacheResult<()> {
        let new_gen = self.cache.generation_counter + 1;
        let new_gen_i64 = i64::try_from(new_gen).expect("generation counter fits in i64");
        let promoted: i64 = self
            .db_cache
            .query_one(
                "SELECT pgcache_generation_zero_promote($1)",
                &[&new_gen_i64],
            )
            .await
            .map_into_report::<CacheError>()?
            .get(0);

        if promoted > 0 {
            self.cache.generation_counter = new_gen;
            debug!("promoted {promoted} gen-0 entries to generation {new_gen}");
        }

        Ok(())
    }

    /// Purge rows with generation <= threshold.
    /// First promotes any gen-0 entries so they become purgeable in future cycles.
    ///
    /// Returns `Ok(0)` *without purging* while a CDC frame is open (the purge
    /// is deferred to `CommitMark`). Callers must not treat that `0` as
    /// "nothing to reclaim" — the deferred purge runs once the frame commits.
    pub(super) async fn generation_purge(&mut self, threshold: u64) -> CacheResult<i64> {
        // Defer while a CDC frame is open: pgcache_purge_rows DELETEs source
        // cache-table rows on db_cache, which would block on the frame's
        // uncommitted locks. Record the intent; flushed after frame_commit.
        if self.frame_holds_locks() {
            self.purge_pending = true;
            return Ok(0);
        }
        debug!(threshold, "generation_purge entry");
        self.generation_zero_promote().await?;

        if threshold > 0 {
            let threshold_i64 = i64::try_from(threshold).expect("generation threshold fits in i64");
            let deleted: i64 = self
                .db_cache
                .query_one("SELECT pgcache_purge_rows($1)", &[&threshold_i64])
                .await
                .map_into_report::<CacheError>()?
                .get(0);
            debug!(threshold, deleted, "generation_purge complete");
            Ok(deleted)
        } else {
            Ok(0)
        }
    }
    /// Invalidate all cached queries that reference a table.
    pub(super) async fn cache_table_invalidate(&mut self, relation_oid: Oid) -> CacheResult<()> {
        let fingerprints: Vec<Fingerprint> = self
            .cache
            .cached_queries
            .iter()
            .filter(|q| q.relation_oids.contains(&relation_oid))
            .map(|q| q.fingerprint)
            .collect();

        for fp in fingerprints {
            self.cache_query_evict(fp).await?;
        }
        Ok(())
    }

    /// Fully evict a cached query: remove from all data structures and purge rows.
    /// Used by the eviction loop and schema-change (table) invalidation.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) async fn cache_query_evict(&mut self, fingerprint: Fingerprint) -> CacheResult<()> {
        let Some(query) = self.cache.cached_queries.remove1(&fingerprint) else {
            trace!(fingerprint = %fingerprint, "cache_query_evict: not found, skipping");
            return Ok(());
        };

        debug!(
            fingerprint = %fingerprint,
            generation = query.generation,
            relation_oids = ?query.relation_oids,
            "cache_query_evict entry"
        );
        if let Some(mut m) = self.state_view.metrics.get_mut(&fingerprint) {
            m.eviction_count += 1;
            m.cached_since_ns = None;
        }
        // Removal paths defer publication sync to the end-of-command drain
        // (publication_dirty_drain) — stale subscriptions to the dropped
        // oid are filtered out by the writer ignoring its CDC events.
        self.active_relations_release(&query.relation_oids);

        let prev_generation_threshold = self.cache.generation_purge_threshold();

        // Remove generation from tracking
        self.cache.generations.remove(&query.generation);

        // Drop the MV table (if any) before removing the state_view entry so we
        // can read the mv_state. Errors are logged but don't abort the eviction.
        // Unlike the other db_cache maintenance, this is NOT frame-deferred:
        // MV tables (pgcache_mv schema) are never written by the frame, which
        // only touches source cache tables — so a DROP here can't deadlock on
        // the frame's locks even when reached in-frame (e.g. via truncate
        // invalidation).
        let mv_state = self
            .state_view
            .cached_queries
            .get(&fingerprint)
            .map(|v| v.mv.state);
        if let Some(mv_state) = mv_state
            && let Err(e) = self.mv_drop(fingerprint, mv_state).await
        {
            error!(
                "mv drop on eviction failed for {fingerprint}: {}",
                error_chain_format(e.current_context()),
            );
        }

        // Remove from state view
        self.state_view.cached_queries.remove(&fingerprint);

        // Eagerly invalidate this query's captured memo by bumping its
        // per-fingerprint slot (ADR-045). The memo-eviction pass no longer scans
        // every memo per CDC row, so it can't catch an evicted query's orphan
        // memo once the query leaves `update_queries`/`eval_index`; without this
        // bump a re-registration could serve a stale orphan (the first serve
        // precedes re-capture). The bump (begin→end, +2) makes any stamped
        // orphan version stale at once, so `memo.get` rejects it; the next
        // capture re-stamps the new version.
        self.state_view
            .memo
            .slot_dirty_begin(SlotKey::Memo(fingerprint));
        self.state_view
            .memo
            .slot_dirty_end(SlotKey::Memo(fingerprint));

        // Drain coalesced waiters parked on the now-removed query (eviction can
        // remove a Loading query whose waiters would otherwise never be drained).
        self.waiters_fail(fingerprint);

        self.cache
            .update_queries_remove_fingerprint(fingerprint, &query.relation_oids);

        // Purge generations when the threshold moved and the cache volume is
        // under disk pressure (statvfs, PGC-276).
        let new_threshold = self.cache.generation_purge_threshold();
        if new_threshold > prev_generation_threshold && self.disk_pressure() {
            self.generation_purge(new_threshold).await?;
        }

        Ok(())
    }
}
