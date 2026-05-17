use std::collections::HashSet;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use ecow::EcoString;
use tokio::runtime::Builder;
use tokio::sync::mpsc::{Receiver, UnboundedReceiver, UnboundedSender};
use tokio::task::{LocalSet, yield_now};
use tokio_postgres::Client;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, trace};

use crate::cache::status::{
    CacheStatusData, CdcStatusData, LatencyStats, QueryStatusData, StatusRequest, StatusResponse,
};
use crate::metrics::names;
use crate::pg;
use crate::query::ast::Deparse;
use crate::result::error_chain_format;
use crate::settings::{CachePolicy, Settings};

use super::super::{
    CacheError, CacheResult, MapIntoReport, ReportExt,
    messages::{CdcCommand, QueryCommand, WriterNotify},
    mv::{MvMeta, ShapeGate},
    types::{
        ActiveRelations, Cache, CacheStateView, CachedQueryState, CachedQueryView, SharedResolved,
    },
};
use super::cdc::WriterCdc;
use super::registration::WriterRegistration;

/// Shared writer state for the CDC apply and registration/population paths.
/// `WriterCdc` and `WriterRegistration` borrow `&mut WriterCore` per command;
/// the single-owner `writer_run` select loop serializes mutations (no
/// locking), preserving the no-race-between-registration-and-purging invariant.
pub struct WriterCore {
    pub(super) cache: Cache,
    pub(super) db_cache: Client,
    pub(super) db_origin: Rc<Client>,
    pub(super) state_view: Arc<CacheStateView>,
    /// Shared set of relation OIDs with active cached queries (read by CDC processor).
    active_relations: ActiveRelations,
    /// Per-relation_oid refcount of cached queries that reference each
    /// relation. Pairs with `active_relations` — the snapshot is only
    /// updated on 0↔1 transitions instead of rebuilt by walking
    /// `cached_queries` on every register/evict.
    relation_refcounts: std::collections::HashMap<u32, usize>,
    /// Publication name for dynamic table management.
    publication_name: String,
    /// OIDs currently in the publication (mirrors the origin-side state).
    publication_oids: HashSet<u32>,
    /// Set when a removal path changes active relations; drained by command handlers.
    pub(super) relations_dirty: bool,
    /// Loopback command channel into the writer select loop. Used by CDC
    /// invalidation to defer pinned readmits, by MV to schedule builds, and
    /// cloned to population workers so they can report Ready/Failed.
    pub(super) query_tx: UnboundedSender<QueryCommand>,
    /// Notifications to coordinator for coalescing queue drain.
    pub(super) notify_tx: UnboundedSender<WriterNotify>,
    /// True while `WriterCdc` holds an open `BEGIN` on its write connection
    /// (set/cleared by `WriterCdc::frame_ensure`/`frame_commit` through their
    /// `&mut WriterCore`). The maintenance paths read it to defer cache-table
    /// DDL/purges: a `db_cache` DROP/DELETE on a table the open frame wrote
    /// would block on the frame's row locks, and the frame only commits at the
    /// later `CommitMark` — a permanent stall.
    pub(super) frame_open: bool,
    /// Set when a generation purge was skipped because a frame was open;
    /// flushed after the frame commits at `CommitMark`.
    pub(super) purge_pending: bool,
}

impl WriterCore {
    pub async fn new(
        settings: &Settings,
        state_view: Arc<CacheStateView>,
        active_relations: ActiveRelations,
        notify_tx: UnboundedSender<WriterNotify>,
        query_tx: UnboundedSender<QueryCommand>,
    ) -> CacheResult<Self> {
        let cache_client = pg::connect(&settings.cache, "writer cache")
            .await
            .map_into_report::<CacheError>()?;

        let origin_client = pg::connect(&settings.origin, "writer origin")
            .await
            .map_into_report::<CacheError>()
            .attach_loc("connecting to origin database")?;

        Ok(Self {
            cache: Cache::new(settings),
            db_cache: cache_client,
            db_origin: Rc::new(origin_client),
            state_view,
            active_relations,
            relation_refcounts: std::collections::HashMap::new(),
            publication_name: settings.cdc.publication_name.clone(),
            publication_oids: HashSet::new(),
            relations_dirty: false,
            query_tx,
            notify_tx,
            frame_open: false,
            purge_pending: false,
        })
    }

    /// Increment refcounts for each relation_oid the new cached_query
    /// touches. On 0→1 transitions, clone-mutate-swap the
    /// `active_relations` snapshot and set `relations_dirty` so the next
    /// `publication_dirty_drain` syncs the origin publication.
    ///
    /// O(|oids| + |active_set|) per call vs. the previous O(|cached_queries|)
    /// rebuild — typically a handful of integer ops since most registers
    /// add no new tables. Returns `true` if the active set changed; callers
    /// may sync the publication inline for cases where the new relation
    /// must be in the publication before subsequent work (e.g., population
    /// fetches from origin).
    pub(super) fn active_relations_acquire(&mut self, oids: &[u32]) -> bool {
        let mut newly_active: Vec<u32> = Vec::new();
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
    pub(super) fn active_relations_release(&mut self, oids: &[u32]) -> bool {
        let mut newly_inactive: Vec<u32> = Vec::new();
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

    /// Sync the origin publication to `active_relations` and drop any cache
    /// tables that just fell out of the active set. The drop happens here,
    /// after the ALTER PUBLICATION, because `oids_to_table_list` resolves
    /// oid → schema.name from `cache.tables` — if we dropped first that
    /// lookup would return empty.
    pub(super) async fn publication_update(&mut self) -> CacheResult<()> {
        let new_oids: HashSet<u32> = (**self.active_relations.load()).clone();

        if new_oids == self.publication_oids {
            // Already in sync. Clear the dirty flag so a deferred drain
            // doesn't redo this comparison.
            self.relations_dirty = false;
            return Ok(());
        }

        let removed: Vec<u32> = self
            .publication_oids
            .difference(&new_oids)
            .copied()
            .collect();

        let sql = if new_oids.is_empty() {
            let table_list =
                self.oids_to_table_list(&self.publication_oids.iter().copied().collect::<Vec<_>>());
            format!(
                "ALTER PUBLICATION {} DROP TABLE {}",
                self.publication_name, table_list
            )
        } else {
            let table_list = self.oids_to_table_list(&new_oids.iter().copied().collect::<Vec<_>>());
            format!(
                "ALTER PUBLICATION {} SET TABLE {}",
                self.publication_name, table_list
            )
        };

        debug!("publication update: {sql}");
        self.db_origin
            .batch_execute(&sql)
            .await
            .map_into_report::<CacheError>()
            .attach_loc("updating publication table list")?;
        self.publication_oids = new_oids;

        if !removed.is_empty() {
            self.cache_tables_drop(&removed).await;
        }
        // Publication now matches active_relations; any pending drain is
        // satisfied by this call.
        self.relations_dirty = false;
        Ok(())
    }

    /// Resolve a list of OIDs to a comma-separated `schema.table` string.
    fn oids_to_table_list(&self, oids: &[u32]) -> String {
        oids.iter()
            .filter_map(|oid| {
                self.cache
                    .tables
                    .get1(oid)
                    .map(|t| format!("{}.{}", t.schema, t.name))
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

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
        if self.frame_open {
            return Ok(());
        }
        self.relations_dirty = false;
        self.publication_update().await?;
        Ok(())
    }

    // Helper methods

    /// Set the shape-gate classification and derive the initial MvState for a
    /// cached query. Called once per fresh registration (not on readmit / limit
    /// bump, since classification is sticky). The state_view entry is expected
    /// to exist — it is inserted by the coordinator before dispatching
    /// `QueryCommand::Register`.
    pub(super) fn mv_state_set(&self, fingerprint: u64, shape_gate: ShapeGate) {
        if let Some(mut view) = self.state_view.cached_queries.get_mut(&fingerprint) {
            view.mv = MvMeta::new(shape_gate);
        }
    }

    /// Preserves shape_gate and mv_state. Private — callers must go through
    /// the public `state_*_transition` wrappers so paired side effects (notify
    /// on Ready) aren't skipped.
    fn state_view_write(
        &self,
        fingerprint: u64,
        state: CachedQueryState,
        generation: u64,
        resolved: &SharedResolved,
        deparsed_sql: &EcoString,
        max_limit: Option<u64>,
    ) {
        self.state_view
            .cached_queries
            .entry(fingerprint)
            .and_modify(|v| {
                v.state = state;
                v.generation = generation;
                v.resolved = Some(Arc::clone(resolved));
                v.deparsed_sql = Some(deparsed_sql.clone());
                v.max_limit = max_limit;
                v.referenced = false;
            })
            .or_insert_with(|| CachedQueryView {
                state,
                generation,
                resolved: Some(Arc::clone(resolved)),
                deparsed_sql: Some(deparsed_sql.clone()),
                max_limit,
                referenced: false,
                mv: MvMeta::new(ShapeGate::Skip),
            });
    }

    /// Caller must follow up with population work (or another Ready/Failed
    /// transition); otherwise coalesced waiters stay stuck.
    pub(super) fn state_loading_transition(
        &self,
        fingerprint: u64,
        generation: u64,
        resolved: &SharedResolved,
        deparsed_sql: &EcoString,
        max_limit: Option<u64>,
    ) {
        self.state_view_write(
            fingerprint,
            CachedQueryState::Loading,
            generation,
            resolved,
            deparsed_sql,
            max_limit,
        );
    }

    /// Mark Ready and notify the cache loop. Skipping the notify leaves
    /// coalesced waiters hung forever — always go through this wrapper.
    pub(super) fn state_ready_transition(
        &self,
        fingerprint: u64,
        generation: u64,
        resolved: SharedResolved,
        deparsed_sql: EcoString,
        max_limit: Option<u64>,
    ) {
        self.state_view_write(
            fingerprint,
            CachedQueryState::Ready,
            generation,
            &resolved,
            &deparsed_sql,
            max_limit,
        );
        let _ = self.notify_tx.send(WriterNotify::Ready {
            fingerprint,
            generation,
            resolved,
            deparsed_sql,
            max_limit,
        });
    }

    /// Update cache state gauges with current values.
    //
    // Counts and byte totals are converted to f64 for Prometheus gauges; gauges
    // accept f64 by API and the precision loss only matters above 2^53.
    #[allow(clippy::cast_precision_loss)]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) fn state_gauges_update(&self) {
        metrics::gauge!(names::CACHE_QUERIES_REGISTERED)
            .set(self.cache.cached_queries.len() as f64);

        {
            let mut loading_count = 0;
            let mut pending_count = 0;
            let mut invalidated_count = 0;

            for entry in self.state_view.cached_queries.iter() {
                match entry.value().state {
                    CachedQueryState::Loading => loading_count += 1,
                    CachedQueryState::Pending { .. } => pending_count += 1,
                    CachedQueryState::Invalidated => invalidated_count += 1,
                    CachedQueryState::Ready => {}
                }
            }

            metrics::gauge!(names::CACHE_QUERIES_LOADING).set(loading_count as f64);
            metrics::gauge!(names::CACHE_QUERIES_PENDING).set(pending_count as f64);
            metrics::gauge!(names::CACHE_QUERIES_INVALIDATED).set(invalidated_count as f64);
        }

        metrics::gauge!(names::CACHE_SIZE_BYTES).set(self.cache.current_size as f64);
        if let Some(limit) = self.cache.dynamic.load().cache_size {
            metrics::gauge!(names::CACHE_SIZE_LIMIT_BYTES).set(limit as f64);
        }
        metrics::gauge!(names::CACHE_GENERATION).set(self.cache.generation_counter as f64);
        metrics::gauge!(names::CACHE_TABLES_TRACKED).set(self.cache.tables.len() as f64);
    }

    /// Update gauges that correlate Register cost against state size. Suspected
    /// O(N) hot spots (`subsumption_check`, `update_query_register` sort) scale
    /// with these.
    #[allow(clippy::cast_precision_loss)]
    pub(super) fn writer_scale_gauges_update(&self) {
        let (total, max_per_relation) = self
            .cache
            .update_queries
            .iter()
            .map(|entry| entry.queries.len())
            .fold((0usize, 0usize), |(sum, max), n| (sum + n, max.max(n)));
        metrics::gauge!(names::CACHE_WRITER_UPDATE_QUERIES_TOTAL).set(total as f64);
        metrics::gauge!(names::CACHE_WRITER_UPDATE_QUERIES_MAX_PER_RELATION)
            .set(max_per_relation as f64);
    }

    /// Utility function to get the size of the currently cached data.
    ///
    /// Returns the last known `current_size` *without querying* while a CDC
    /// frame is open. `pgcache_total_size()` does `pg_total_relation_size`
    /// (an `ACCESS SHARE` open) over the tracked cache tables; an in-frame
    /// `TRUNCATE` holds `ACCESS EXCLUSIVE` on one of them, so the query would
    /// block on the frame until `CommitMark` — a deadlock. The size is an
    /// explicitly-drifting estimate and self-corrects on the next load once
    /// the frame has committed.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) async fn cache_size_load(&mut self) -> CacheResult<usize> {
        if self.frame_open {
            return Ok(self.cache.current_size);
        }
        let size: i64 = self
            .db_cache
            .query_one("SELECT pgcache_total_size()", &[])
            .await
            .map_into_report::<CacheError>()?
            .get(0);

        Ok(usize::try_from(size).unwrap_or(0))
    }

    /// Run eviction loop. For CLOCK policy, uses second-chance algorithm with reference bit.
    /// For FIFO policy, evicts the oldest-registered query.
    pub(super) async fn eviction_run(&mut self) -> CacheResult<()> {
        // Defer while a CDC frame is open: cache_query_evict / generation_purge
        // would block on the frame's uncommitted cache-table locks. Eviction is
        // periodic/best-effort — the next Ready after the frame commits runs it.
        if self.frame_open {
            return Ok(());
        }
        /// Maximum number of generation bumps (second chances) per eviction round.
        /// Bounds re-stamping work and prevents pathological case where all queries are referenced.
        const MAX_BUMPS: usize = 5;
        let mut bumps = 0;
        let mut pinned_skips = 0;

        let cfg = self.cache.dynamic.load();

        debug!(
            current_size = self.cache.current_size,
            cache_size = ?cfg.cache_size,
            cache_policy = ?cfg.cache_policy,
            "eviction_run entry"
        );

        // Pre-sweep: reclaim bytes held by Dirty MVs before considering live
        // entries for eviction. If this alone brings current_size under the
        // limit, the loop below exits immediately without evicting anything.
        if cfg.cache_size.is_some_and(|s| self.cache.current_size > s) {
            self.mv_dirty_sweep().await?;
            self.cache.current_size = self.cache_size_load().await?;
        }

        while cfg.cache_size.is_some_and(|s| self.cache.current_size > s) {
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
                metrics::counter!(names::CACHE_EVICTIONS, "result" => "pinned_bump").increment(1);
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
                    metrics::counter!(names::CACHE_EVICTIONS, "result" => "bump").increment(1);
                    self.cache_query_generation_bump(fingerprint).await?;
                    bumps += 1;
                    continue;
                }
            }

            // Evict (full removal) — cache_query_evict emits its own entry log
            metrics::counter!(names::CACHE_EVICTIONS).increment(1);
            self.cache_query_evict(fingerprint).await?;
            // publication_dirty_drain drops the orphaned cache tables; the
            // trigger is what pgcache_total_size sums, so the next iteration's
            // cache_size_load needs the drain to observe a shrink.
            self.publication_dirty_drain().await?;
            self.cache.current_size = self.cache_size_load().await?;
            bumps = 0;
            pinned_skips = 0;
        }

        // stale_entries_cleanup runs on the 1s gauges tick instead of here —
        // it is GC of dead Pending/Invalidated entries, not eviction-critical,
        // and its O(cached_queries) scan would dominate Ready handling.
        Ok(())
    }

    /// Bump a cached query's generation to give it a second chance in CLOCK eviction.
    /// Re-executes the query against cache DB so the CustomScan tracker re-stamps
    /// dshash entries from old_gen to new_gen.
    async fn cache_query_generation_bump(&mut self, fingerprint: u64) -> CacheResult<()> {
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
    ///   coordinator-side Pending-credit sizing and decays existing credits.
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
        let stale_fingerprints: Vec<u64> = self
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

    /// Build and send a status response for an admin `/status` request.
    async fn status_respond(&self, req: StatusRequest, last_applied_lsn: u64) {
        let cache = &self.cache;

        let (mut total_hits, mut total_misses) = (0u64, 0u64);
        for entry in self.state_view.metrics.iter() {
            total_hits += entry.hit_count;
            total_misses += entry.miss_count;
        }

        let dynamic = cache.dynamic.load();
        let cache_status = CacheStatusData {
            size_bytes: cache.current_size,
            size_limit_bytes: dynamic.cache_size,
            generation: cache.generation_counter,
            tables_tracked: cache.tables.len(),
            policy: format!("{:?}", dynamic.cache_policy),
            queries_registered: cache.cached_queries.len(),
            uptime_ms: u64::try_from(self.state_view.started_at.elapsed().as_millis())
                .unwrap_or(u64::MAX),
            cache_hits: total_hits,
            cache_misses: total_misses,
        };

        let mut queries: Vec<QueryStatusData> = Vec::with_capacity(cache.cached_queries.len());
        for q in &cache.cached_queries {
            let mut sql_preview = String::with_capacity(128);
            Deparse::deparse(&*q.resolved, &mut sql_preview);
            sql_preview.truncate(200);

            let tables: Vec<String> = q
                .relation_oids
                .iter()
                .filter_map(|oid| {
                    cache
                        .tables
                        .get1(oid)
                        .map(|t| format!("{}.{}", t.schema, t.name))
                })
                .collect();

            let state = self
                .state_view
                .cached_queries
                .get(&q.fingerprint)
                .map(|entry| format!("{:?}", entry.value().state))
                .unwrap_or_else(|| "Unknown".to_owned());

            // Look up per-query metrics (shared read access)
            let metrics = self.state_view.metrics.get(&q.fingerprint);
            let now_ns =
                u64::try_from(self.state_view.started_at.elapsed().as_nanos()).unwrap_or(u64::MAX);
            let (
                hit_count,
                miss_count,
                idle_duration_ms,
                registered_duration_ms,
                cached_duration_ms,
                invalidation_count,
                readmission_count,
                eviction_count,
                subsumption_count,
                population_count,
                last_population_duration_ms,
                total_bytes_served,
                population_row_count,
                cache_hit_latency,
            ) = match &metrics {
                Some(m) => {
                    let latency_stats = if !m.cache_hit_latency.is_empty() {
                        Some(LatencyStats {
                            count: m.cache_hit_latency.len(),
                            mean_us: m.cache_hit_latency.mean(),
                            p50_us: m.cache_hit_latency.value_at_quantile(0.5),
                            p95_us: m.cache_hit_latency.value_at_quantile(0.95),
                            p99_us: m.cache_hit_latency.value_at_quantile(0.99),
                            min_us: m.cache_hit_latency.min(),
                            max_us: m.cache_hit_latency.max(),
                        })
                    } else {
                        None
                    };

                    (
                        m.hit_count,
                        m.miss_count,
                        m.last_hit_at_ns
                            .map(|ns| now_ns.saturating_sub(ns.get()) / 1_000_000),
                        m.registered_at_ns
                            .map(|ns| now_ns.saturating_sub(ns.get()) / 1_000_000),
                        m.cached_since_ns
                            .map(|ns| now_ns.saturating_sub(ns.get()) / 1_000_000),
                        m.invalidation_count,
                        m.readmission_count,
                        m.eviction_count,
                        m.subsumption_count,
                        m.population_count,
                        m.last_population_duration_us.map(|us| us.get() / 1_000),
                        m.total_bytes_served,
                        m.population_row_count,
                        latency_stats,
                    )
                }
                None => (0, 0, None, None, None, 0, 0, 0, 0, 0, None, 0, 0, None),
            };

            queries.push(QueryStatusData {
                fingerprint: q.fingerprint,
                sql_preview,
                tables,
                state,
                cached_bytes: q.cached_bytes,
                max_limit: q.max_limit,
                pinned: q.pinned,
                hit_count,
                miss_count,
                idle_duration_ms,
                registered_duration_ms,
                cached_duration_ms,
                invalidation_count,
                readmission_count,
                eviction_count,
                subsumption_count,
                population_count,
                last_population_duration_ms,
                total_bytes_served,
                population_row_count,
                cache_hit_latency,
            });

            yield_now().await;
        }

        let response = StatusResponse {
            cache: cache_status,
            cdc: CdcStatusData { last_applied_lsn },
            queries,
        };

        let _ = req.reply_tx.send(response);
    }

    /// Purge rows with generation <= threshold.
    /// First promotes any gen-0 entries so they become purgeable in future cycles.
    ///
    /// Returns `Ok(0)` *without purging* while a CDC frame is open (the purge
    /// is deferred to `CommitMark`). Callers must not treat that `0` as
    /// "nothing to reclaim" — e.g. a following `cache_size_load` will read a
    /// pre-purge size. The size estimate is allowed to drift and self-corrects
    /// once the deferred purge runs.
    pub(super) async fn generation_purge(&mut self, threshold: u64) -> CacheResult<i64> {
        // Defer while a CDC frame is open: pgcache_purge_rows DELETEs source
        // cache-table rows on db_cache, which would block on the frame's
        // uncommitted locks. Record the intent; flushed after frame_commit.
        if self.frame_open {
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
}

/// Main writer runtime. Owns `WriterCore` plus the two responsibility
/// managers (`WriterCdc`, `WriterRegistration`) and serializes their access
/// to the core through one select loop.
#[allow(clippy::too_many_arguments)]
pub fn writer_run(
    settings: &Settings,
    mut query_rx: UnboundedReceiver<QueryCommand>,
    mut cdc_rx: UnboundedReceiver<CdcCommand>,
    state_view: Arc<CacheStateView>,
    active_relations: ActiveRelations,
    notify_tx: UnboundedSender<WriterNotify>,
    cancel: CancellationToken,
    mut status_rx: Receiver<StatusRequest>,
) -> CacheResult<()> {
    let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_into_report::<CacheError>()?;

    debug!("writer loop");
    rt.block_on(async {
        // Create internal channel for population workers to send query commands back
        let (query_tx, mut internal_rx) = tokio::sync::mpsc::unbounded_channel();

        LocalSet::new()
            .run_until(async move {
                // Built inside the LocalSet so WriterRegistration can spawn_local
                // its population workers.
                let mut core = WriterCore::new(
                    settings,
                    state_view,
                    active_relations,
                    notify_tx,
                    query_tx.clone(),
                )
                .await?;
                let mut registration =
                    WriterRegistration::new(settings, &core.db_origin, query_tx).await?;
                let mut writer_cdc = WriterCdc::new(settings).await?;

                // Gauges (queries_loading/pending/invalidated, cache_size_bytes,
                // generation, tables_tracked, update_queries_total/max) used to
                // be emitted from every query/CDC command. state_gauges_update
                // iterates the entire state_view DashMap, which dominated
                // writer per-command time at scale. Emit on a 1s tick instead —
                // well below typical Prometheus scrape intervals.
                let mut gauges_interval = tokio::time::interval(Duration::from_secs(1));
                gauges_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            debug!("writer shutdown signal received");
                            break;
                        }
                        _ = gauges_interval.tick() => {
                            core.stale_entries_cleanup();
                            core.state_gauges_update();
                            core.writer_scale_gauges_update();
                        }
                        // Handle query commands from coordinator
                        msg = query_rx.recv() => {
                            match msg {
                                Some(cmd) => {
                                    if let Err(e) =
                                        registration.query_command_handle(&mut core, cmd).await
                                    {
                                        error!(
                                            "writer query command failed: {}",
                                            error_chain_format(e.current_context()),
                                        );
                                    }
                                }
                                None => {
                                    debug!("writer query channel closed, shutting down");
                                    break;
                                }
                            }
                        }
                        // Handle CDC commands from coordinator
                        msg = cdc_rx.recv() => {
                            match msg {
                                Some(cmd) => {
                                    if let Err(e) =
                                        writer_cdc.cdc_command_handle(&mut core, cmd).await
                                    {
                                        // Propagate: tears down the cache
                                        // subsystem so the supervisor restart
                                        // rebuilds it from a clean reset.
                                        error!(
                                            "writer cdc command failed, resetting cache: {}",
                                            error_chain_format(e.current_context()),
                                        );
                                        return Err(e);
                                    }
                                }
                                None => {
                                    debug!("writer cdc channel closed, shutting down");
                                    break;
                                }
                            }
                        }
                        // Handle commands from spawned population tasks
                        msg = internal_rx.recv() => {
                            match msg {
                                Some(cmd) => {
                                    if let Err(e) =
                                        registration.query_command_handle(&mut core, cmd).await
                                    {
                                        error!(
                                            "writer internal command failed: {}",
                                            error_chain_format(e.current_context()),
                                        );
                                    }
                                }
                                None => {
                                    debug!("writer internal channel closed, shutting down");
                                    break;
                                }
                            }
                        }
                        // Handle status requests from admin HTTP server
                        msg = status_rx.recv() => {
                            if let Some(req) = msg {
                                core.status_respond(req, writer_cdc.last_applied_lsn).await;
                            }
                        }
                    }

                    // Channel depths are reported as f64 gauges; queue sizes never approach 2^53.
                    #[allow(clippy::cast_precision_loss)]
                    {
                        metrics::gauge!(names::CACHE_WRITER_QUERY_QUEUE).set(query_rx.len() as f64);
                        metrics::gauge!(names::CACHE_WRITER_CDC_QUEUE).set(cdc_rx.len() as f64);
                        metrics::gauge!(names::CACHE_WRITER_INTERNAL_QUEUE)
                            .set(internal_rx.len() as f64);
                    }
                }

                Ok(())
            })
            .await
    })
}
