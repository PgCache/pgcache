use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;

use ecow::EcoString;
use postgres_types::PgLsn;
use tokio::runtime::Handle;
use tokio::sync::Notify;
use tokio::sync::mpsc::UnboundedSender;
use tokio_postgres::Client;
use tracing::debug;

use super::frame::{FRAME_BUF_CAPACITY, FrameRowEvent, FrameState, OverlayEntry};
use super::merge_queue::MergeQueue;
use super::mv_build::MvBuildPool;
use super::staging::{PopulationDeletedKeys, StagingPool};
use crate::cache::Generation;
use crate::cache::{
    CacheError, CacheResult, MapIntoReport, ReportExt,
    messages::{QueryCommand, WriterNotify},
    mv::MvMeta,
    mv_shape::ShapeGate,
    types::{
        ActiveRelations, Cache, CacheStateView, CachedQueryState, CachedQueryView, SharedResolved,
    },
};
use crate::oid::Oid;
use crate::pg;
use crate::pg::Lsn;
use crate::pg::protocol::ByteString;
use crate::query::{Fingerprint, FingerprintSet};
use crate::settings::{PgSettings, Settings};

/// Deterministic fault injection for the restart supervisor: kill the writer on
/// a sentinel CDC insert so a test can drive a real subsystem death → rebuild.
/// Compiled out entirely unless built with `--features fault-injection`.
#[cfg(feature = "fault-injection")]
pub(crate) mod fault {
    use std::sync::Once;
    use std::sync::atomic::{AtomicBool, Ordering};

    use crate::cache::messages::CdcValue;

    /// A CDC insert carrying this value in any column trips the one-shot.
    pub(crate) const WRITER_DIE_SENTINEL: &str = "__PGCACHE_WRITER_DIE__";

    static ARMED: AtomicBool = AtomicBool::new(false);
    static INIT: Once = Once::new();

    /// Arm from the environment, once for the process (first generation).
    pub(crate) fn init() {
        INIT.call_once(|| {
            if std::env::var_os("PGCACHE_FAULT_WRITER_DIE").is_some() {
                ARMED.store(true, Ordering::Relaxed);
            }
        });
    }

    /// Test override for the eviction count cap
    /// (`PGCACHE_FAULT_EVICTION_COUNT_CAP`): forces count-driven eviction down to
    /// N cached queries, so eviction tests don't need a disk byte-cap (PGC-276).
    pub(crate) fn eviction_count_cap() -> Option<usize> {
        std::env::var("PGCACHE_FAULT_EVICTION_COUNT_CAP")
            .ok()
            .and_then(|s| s.parse().ok())
    }

    /// Force `disk_pressure()` true while a sentinel file exists. The env var
    /// `PGCACHE_FAULT_DISK_PRESSURE` names the path; a test toggles pressure by
    /// creating/removing it, exercising the throttle + escalating reclaim
    /// deterministically without filling the host disk (PGC-276).
    pub(crate) fn disk_pressure_forced() -> bool {
        std::env::var_os("PGCACHE_FAULT_DISK_PRESSURE")
            .is_some_and(|p| std::path::Path::new(&p).exists())
    }

    /// One-shot: fire when armed and a row carries the sentinel, then disarm so
    /// the rebuilt generation (and the slot's redelivery of the same insert)
    /// survives instead of looping.
    pub(crate) fn writer_die_check(row_data: &[CdcValue]) -> bool {
        if !ARMED.load(Ordering::Relaxed) {
            return false;
        }
        let hit = row_data
            .iter()
            .any(|v| matches!(v, CdcValue::Text(text) if text == WRITER_DIE_SENTINEL));
        if hit {
            ARMED.store(false, Ordering::Relaxed);
        }
        hit
    }
}

/// Cap on retained candidate scratch sets. At most two are alive at once
/// (`handle_update`), so this is generous headroom; it bounds the pool the way
/// `ROW_VEC_POOL_MAX` / `TOAST_OVERLAY_POOL_MAX` bound their pools, in case a
/// future caller ever returns more sets than it took.
const CANDIDATE_SCRATCH_MAX: usize = 8;

/// Handles the writer shares with the rest of the cache subsystem, moved into
/// `WriterCore` at startup.
pub(crate) struct WriterShared {
    pub(crate) state_view: Arc<CacheStateView>,
    /// Shared set of relation OIDs with active cached queries (read by CDC processor).
    pub(crate) active_relations: ActiveRelations,
    /// Notifications to dispatch for coalescing queue drain.
    pub(crate) notify_tx: UnboundedSender<WriterNotify>,
    /// Prompts the CDC thread for an immediate keepalive when a populated
    /// query is gated on the apply watermark (PGC-250 Slice B).
    pub(crate) watermark_nudge: Arc<Notify>,
    /// Shared multi-thread runtime; MV build tasks are spawned here.
    pub(crate) runtime: Handle,
}

/// What a cached query serves at a generation: mirrored into the state view
/// on every Loading / Ready transition and sent with the Ready notify.
pub(super) struct QueryServing {
    pub(super) generation: Generation,
    pub(super) resolved: SharedResolved,
    pub(super) deparsed_sql: EcoString,
    pub(super) max_limit: Option<u64>,
}

/// The cache volume, as read once at startup (PGC-251 Slice 2).
struct InitialDiskStats {
    data_dir: Option<PathBuf>,
    total: u64,
    available: u64,
    limit_effective: u64,
}

/// Shared writer state for the CDC apply and registration/population paths.
/// `WriterCdc` and `WriterRegistration` borrow `&mut WriterCore` per command;
/// the single-owner `writer_run` select loop serializes mutations (no
/// locking), preserving the no-race-between-registration-and-purging invariant.
pub(super) struct WriterCore {
    pub(super) cache: Cache,
    pub(super) db_cache: Client,
    pub(super) db_origin: Rc<Client>,
    pub(super) state_view: Arc<CacheStateView>,
    /// Shared set of relation OIDs with active cached queries (read by CDC processor).
    pub(super) active_relations: ActiveRelations,
    /// Per-relation_oid refcount of cached queries that reference each
    /// relation. Pairs with `active_relations` — the snapshot is only
    /// updated on 0↔1 transitions instead of rebuilt by walking
    /// `cached_queries` on every register/evict.
    pub(super) relation_refcounts: std::collections::HashMap<Oid, usize>,
    /// Publication name for dynamic table management.
    pub(super) publication_name: EcoString,
    /// OIDs currently in the publication (mirrors the origin-side state).
    pub(super) publication_oids: HashSet<Oid>,
    /// Set when a removal path changes active relations; drained by command handlers.
    pub(super) relations_dirty: bool,
    /// Loopback command channel into the writer select loop. Used by CDC
    /// invalidation to defer pinned readmits, by MV to schedule builds, and
    /// cloned to population workers so they can report Ready/Failed.
    pub(super) query_tx: UnboundedSender<QueryCommand>,
    /// Shared multi-thread runtime handle; MV build tasks are spawned here so
    /// their SQL never blocks the writer's event loop.
    pub(super) runtime: Handle,
    /// Dedicated cache-DB connections for MV build tasks (also the build
    /// concurrency limit) — builds never borrow `db_cache` or serve-pool slots.
    pub(super) mv_build_pool: Arc<MvBuildPool>,
    /// Fingerprints with a build task in flight. Enforces at most one build
    /// per fingerprint ever (tasks share one MV table per fingerprint), even
    /// across evict + re-register of the entry: a dispatch that finds its
    /// fingerprint here defers, and the completion handler re-dispatches.
    pub(super) mv_builds_inflight: FingerprintSet,
    /// Notifications to dispatch for coalescing queue drain.
    pub(super) notify_tx: UnboundedSender<WriterNotify>,
    /// CDC source-transaction frame state (driven by
    /// `WriterCdc::frame_begin_ensure`/`frame_commit`/recovery through their
    /// `&mut WriterCore`). Maintenance paths gate on `frame_holds_locks()` to
    /// defer cache-table DDL/purges for the whole `TxnOpen` window: a frame's
    /// buffered writes can flush to the server at any point (chunk-flush) and
    /// then hold row locks, so a racing `db_cache` DROP/DELETE would block until
    /// `CommitMark` — a permanent stall. `Recovering` holds no locks.
    pub(super) frame_state: FrameState,
    /// Fingerprints flagged for invalidation by the in-progress `Open`
    /// frame's handlers, applied just before `frame_commit` (so invalidation
    /// is atomic with the maintenance it accompanies, not visible mid-frame).
    pub(super) frame_invalidations: FingerprintSet,
    /// Memoized fingerprints whose in-process snapshot the in-progress frame's
    /// row changes affect (rung 3b). Bumped via `SlotKey::Memo` at the frame
    /// flush so eviction is predicate-matched, not relation-coarse: a change
    /// that doesn't touch a memo's predicate/membership leaves it intact.
    pub(super) frame_memo_evictions: FingerprintSet,
    /// Relation OIDs touched by the in-progress frame, accumulated from frame
    /// start so a mid-frame `40P01` can invalidate+truncate every affected
    /// relation (commands applied before the deadlock were rolled back too).
    pub(super) frame_relation_oids: HashSet<Oid>,
    /// Set when a generation purge was skipped because a frame was open;
    /// flushed after the frame commits at `CommitMark`.
    pub(super) purge_pending: bool,
    /// Reusable candidate `FingerprintSet`s for the per-row CDC probes
    /// (`eval_candidates` / `eval_candidates_removed`). Taken out for the
    /// duration of a handler and returned cleared, so the backing allocation —
    /// large for the old-image wildcard probe — is reused across rows instead of
    /// re-allocated per probe (PGC-341/344). Depth ≤ the sets alive at once
    /// (2 in `handle_update`); balanced take/return keeps it there.
    candidate_scratch: Vec<FingerprintSet>,
    /// Buffered SQL for the in-progress frame's cache-table writes (PGC-228).
    /// Statements are appended here instead of executed eagerly; the whole
    /// `BEGIN; …; COMMIT` is flushed in one round-trip at `CommitMark` (or
    /// chunk-flushed mid-frame when it exceeds `FRAME_BUF_CAPACITY`). Holds only
    /// `cdc_write_conn` writes — invalidations/purges run out-of-band on
    /// `db_cache`. Reused across frames; never reallocates in steady state.
    pub(super) frame_buf: String,
    /// The in-progress frame's row events, collected at arrival and replayed in
    /// arrival order at the `CommitMark` flush (PGC-241: collect → evaluate →
    /// emit at the flush boundary; partial replay at `FRAME_ROWS_CAPACITY`).
    /// Buffer reused across frames.
    pub(super) frame_rows: Vec<FrameRowEvent>,
    /// Whether a chunk of `frame_buf` has already been flushed to
    /// `cdc_write_conn` this frame (so the `BEGIN` is live on the server). Drives
    /// whether `40P01` recovery must issue an explicit `ROLLBACK`.
    pub(super) frame_chunk_flushed: bool,
    /// Relations whose cache-table writes are already in `frame_buf` (buffered
    /// or chunk-executed) for the open cache txn — spans batched frames
    /// (PGC-242). A mid-frame DDL recreating one of these can't be handled by
    /// discarding `frame_rows` (the writes naming the old columns are already
    /// committed to the buffer / executed), so it escalates to frame recovery.
    /// Maintained at the single write chokepoint [`frame_begin_ensure`];
    /// cleared with `frame_buf` at every cache-txn boundary, never on a
    /// mid-txn chunk flush.
    pub(super) frame_buf_relations: HashSet<Oid>,
    /// Keys CDC removed while populations are in flight, so a population merge
    /// doesn't resurrect them (PGC-250). Activated at dispatch, recorded at
    /// `frame_cache_delete`, consulted/cleared at merge.
    pub(super) population_deleted_keys: PopulationDeletedKeys,
    /// Per-relation pool of reusable population staging tables (PGC-293):
    /// checked out at dispatch, returned (emptied + vacuumed) at merge, so a
    /// population emits no DDL.
    pub(super) staging_pool: StagingPool,
    /// Population merges and staging discards: the gated queue, the one drain
    /// in progress, and the gate's bookkeeping (PGC-272 / PGC-418).
    pub(super) merges: MergeQueue,
    /// Mirror of `WriterCdc.last_received_lsn`, updated as the CDC path advances
    /// the watermark. Read at population dispatch to seed the deleted-key
    /// anchor floor (a lower bound on the population's snapshot LSN).
    pub(super) last_received_lsn: Lsn,
    /// Mirror of `WriterCdc.last_applied_lsn` — the commit-only apply
    /// watermark, advanced only on an actual cache commit.
    pub(super) last_applied_lsn: Lsn,
    /// PK tuple bodies removed by the in-progress CDC frame, drained at
    /// `CommitMark` and recorded into `population_deleted_keys` stamped with the
    /// frame's commit LSN (rolled-back frames clear it instead). Buffered because
    /// the commit LSN isn't known until the frame commits.
    pub(super) frame_deleted_keys: Vec<(Oid, EcoString)>,
    /// PK tuple bodies of toast fallbacks in the in-progress frame (PGC-464),
    /// drained at `CommitMark` into `population_deleted_keys`' toast-stale set
    /// — same commit-LSN deferral as `frame_deleted_keys`.
    pub(super) frame_toast_stale_keys: Vec<(Oid, EcoString)>,
    /// Relations bulk-invalidated by the in-progress frame (TRUNCATE, or 40P01
    /// recovery), drained at `CommitMark` to raise their deleted-key abort
    /// watermark to the commit LSN — same commit-LSN-deferral as
    /// `frame_deleted_keys`.
    pub(super) frame_truncated_relations: Vec<Oid>,
    /// Relations bulk-invalidated outside replay (mid-batch intra-txn DDL
    /// drops, 40P01 recovery), drained at the batch flush and stamped with the
    /// flush LSN — an upper bound on the triggering frame's commit, which
    /// over-aborts (safe) where a replay-boundary stamp could under-abort
    /// (PGC-242).
    pub(super) batch_truncated_relations: Vec<Oid>,
    /// Complete source frames accumulated in the current batch (PGC-242):
    /// boundaries pushed since the last flush.
    pub(super) batch_frames: usize,
    /// Row events accumulated in the current batch, counted at push — survives
    /// mid-frame partial replays draining `frame_rows`, so the flush size cap
    /// sees the true batch size.
    pub(super) batch_events: usize,
    /// The last accumulated frame's commit LSN — the watermark target when a
    /// flush is forced between CommitMarks (KeepAliveMark).
    pub(super) batch_last_lsn: Lsn,
    /// Whether a source frame is open (between `Begin` and `CommitMark`).
    /// `frame_state` no longer distinguishes this once batches span frames.
    pub(super) frame_open: bool,
    /// PKs the current batch has deleted from cache tables (and not since
    /// re-upserted). Row-change presence lookups read the pre-batch committed
    /// state; a later frame updating one of these PKs must be classified
    /// UNCACHED (`row_changes = None`) or the entering-invalidation the
    /// per-frame flow produced is lost (PGC-242; `test_cache_join`'s PK flip).
    pub(super) batch_deleted_pks: HashSet<(Oid, EcoString)>,
    /// Last in-batch write per PK of the toastable columns' values (PGC-264).
    /// The toast-repair lookup reads the pre-batch committed state, which is
    /// stale for any PK this batch has already written; the overlay supplies
    /// the in-batch value instead (an in-memory repair, no fallback), and
    /// `Deleted` tombstones block the stale lookup outright. Maintained in
    /// arrival order by the replay pre-pass; only relations with a toastable
    /// column pay for it. Same lifecycle as `batch_deleted_pks`.
    pub(super) batch_toast_overlay: HashMap<(Oid, EcoString), OverlayEntry>,
    /// Recycled `OverlayEntry::Values` allocations: batch reset harvests
    /// cleared Vecs here instead of dropping them, so steady-state overlay
    /// recording allocates no per-event Vec. Bounded by
    /// [`TOAST_OVERLAY_POOL_MAX`]. Shared by both overlays.
    pub(super) toast_overlay_pool: Vec<Vec<(usize, Option<ByteString>)>>,
    /// Last in-batch write per PK of the eval-index columns' values — the
    /// rung-1 source for the precise old-image probe (PGC-255). The batched
    /// old-image lookup reads the pre-batch committed state, which is stale
    /// for any PK this batch has already written; the overlay supplies the
    /// in-batch values instead, and `Deleted` tombstones block the stale
    /// lookup. Maintained in arrival order by the segment pre-pass; only
    /// relations with a non-empty eval-index column set pay for it. Same
    /// lifecycle as `batch_toast_overlay`.
    pub(super) batch_old_image_overlay: HashMap<(Oid, EcoString), OverlayEntry>,
    /// Relations truncated in the current batch: their pre-batch committed
    /// images are untrustworthy as an old-image source; only overlay values
    /// written after the truncate resolve. Mirror of `batch_toast_guard_oids`.
    pub(super) batch_old_image_guard_oids: HashSet<Oid>,
    /// Registration epoch (`UpdateQueries::epoch`) stamped at each relation's
    /// first old-image activity in the batch. Overlay entries are
    /// batch-lifetime but recording is gated on the *current* query set, so a
    /// mid-batch registration/eviction changes what gets recorded — an epoch
    /// mismatch drops the relation's entries and guards it for the batch
    /// remainder (`old_image_overlay_epoch_reconcile`).
    pub(super) batch_old_image_epochs: HashMap<Oid, u64>,
    /// Recycled row Vecs (`cdc_values_convert` output): replay-drained
    /// `FrameRowEvent`s return their row vecs here so conversion reuses them
    /// instead of allocating per event. Bounded by [`ROW_VEC_POOL_MAX`].
    pub(super) row_vec_pool: Vec<Vec<Option<ByteString>>>,
    /// Relations truncated or DDL-recreated in the current batch (PGC-264).
    /// Their pre-batch committed images are wholesale untrustworthy as a
    /// toast-repair source; only overlay values written after the truncate
    /// can repair. Same lifecycle as `batch_deleted_pks`.
    pub(super) batch_toast_guard_oids: HashSet<Oid>,
    /// Cache PG data directory, discovered once at startup, for `statvfs` to
    /// auto-size the disk eviction limit (PGC-251 Slice 2). `None` if it couldn't
    /// be read (non-superuser, or not visible) — auto disk limit then disabled.
    pub(super) data_dir: Option<PathBuf>,
    /// Last `statvfs` reading of the data directory's filesystem (total,
    /// available) in bytes; refreshed on the 1 s tick. `disk_total == 0` means
    /// "no reading" — disk eviction is then disabled.
    pub(super) disk_total: u64,
    pub(super) disk_available: u64,
    /// Effective cache-volume usage cap in bytes, resolved from the `disk_limit`
    /// config (auto-derived when unset). Recomputed whenever the statvfs reading
    /// refreshes, so the rest of the writer compares against a concrete value
    /// rather than re-defaulting an `Option` (PGC-276).
    pub(super) disk_limit_effective: u64,
    /// Consecutive 1 s ticks the cache volume has been under disk pressure,
    /// driving the escalating reclaim ladder (purge → MV sweep → drop the
    /// fewest-queries source table). Reset to 0 when pressure clears (PGC-276).
    pub(super) disk_pressure_ticks: u32,
    /// Set after a dramatic source-table drop so the next tick skips reclaim,
    /// giving the asynchronous disk reclaim time to land in the next `statvfs`
    /// read before deciding to drop again (avoids lag-driven over-dropping).
    pub(super) disk_drop_backoff: bool,
}

/// Whether a parked population (a queued merge or a gated ready entry) at
/// `parked_generation` may still finalize. `live` is the current cached query's
/// `(generation, invalidated)`, or `None` if it was evicted. Finalize only when
/// the live query exists, hasn't been superseded by a readmit (generation
/// bumped), and isn't invalidated — otherwise the parked entry is stale and
/// finalizing it would mark a superseded/invalidated result Ready (PGC-250).
fn population_finalize_allowed(
    live: Option<(Generation, bool)>,
    parked_generation: Generation,
) -> bool {
    matches!(live, Some((generation, invalidated)) if generation == parked_generation && !invalidated)
}

/// Read the cache PG's `data_directory` so `statvfs` can size the disk limit
/// against the real volume (PGC-251 Slice 2). `None` on any error (it's a
/// superuser-only GUC) — the caller then disables the auto disk limit.
async fn data_directory_query(client: &Client) -> Option<PathBuf> {
    match client
        .query_one("SELECT current_setting('data_directory')", &[])
        .await
    {
        Ok(row) => {
            let dir: String = row.get(0);
            Some(PathBuf::from(dir))
        }
        Err(e) => {
            debug!("data_directory query failed ({e}); disk auto-limit disabled");
            None
        }
    }
}

async fn initial_disk_stats(cache_client: &Client, settings: &Settings) -> InitialDiskStats {
    let data_dir = data_directory_query(cache_client).await;
    let (total, available) = data_dir
        .as_deref()
        .and_then(crate::memory::disk_stats_bytes)
        .unwrap_or((0, 0));
    let limit_effective =
        crate::memory::disk_limit_resolve(total, settings.dynamic.load().disk_limit);
    InitialDiskStats {
        data_dir,
        total,
        available,
        limit_effective,
    }
}

/// Connect the writer's origin session. `origin_flush_force` relies on its
/// marker's commit flushing WAL before returning, so its LSN is reachable by
/// the apply watermark (PGC-290). This is the only session that writes the
/// marker; reads/DDL here are unaffected by the durability setting.
async fn writer_origin_connect(origin: &PgSettings) -> CacheResult<Client> {
    let origin_client = pg::connect(origin, "writer origin")
        .await
        .map_into_report::<CacheError>()
        .attach_loc("connecting to origin database")?;
    origin_client
        .batch_execute("SET synchronous_commit = on")
        .await
        .map_into_report::<CacheError>()
        .attach_loc("setting synchronous_commit on writer origin")?;
    Ok(origin_client)
}

impl WriterCore {
    pub(super) async fn new(
        settings: &Settings,
        shared: WriterShared,
        query_tx: UnboundedSender<QueryCommand>,
    ) -> CacheResult<Self> {
        let cache_client = pg::connect(&settings.cache, "writer cache")
            .await
            .map_into_report::<CacheError>()?;
        let origin_client = writer_origin_connect(&settings.origin).await?;
        let disk = initial_disk_stats(&cache_client, settings).await;
        let WriterShared {
            state_view,
            active_relations,
            notify_tx,
            watermark_nudge,
            runtime,
        } = shared;

        Ok(Self {
            cache: Cache::new(settings),
            db_cache: cache_client,
            db_origin: Rc::new(origin_client),
            state_view,
            active_relations,
            relation_refcounts: std::collections::HashMap::new(),
            publication_name: settings.cdc.publication_name.as_str().into(),
            publication_oids: HashSet::new(),
            relations_dirty: false,
            query_tx,
            runtime,
            mv_build_pool: Arc::new(MvBuildPool::new(settings.cache.clone())),
            mv_builds_inflight: HashSet::default(),
            notify_tx,
            frame_state: FrameState::Idle,
            frame_invalidations: HashSet::default(),
            frame_memo_evictions: HashSet::default(),
            candidate_scratch: Vec::new(),
            frame_relation_oids: HashSet::new(),
            purge_pending: false,
            frame_buf: String::with_capacity(FRAME_BUF_CAPACITY),
            frame_rows: Vec::new(),
            frame_chunk_flushed: false,
            frame_buf_relations: HashSet::new(),
            population_deleted_keys: PopulationDeletedKeys::default(),
            staging_pool: StagingPool::default(),
            merges: MergeQueue::new(watermark_nudge),
            last_received_lsn: Lsn::from_raw(0),
            last_applied_lsn: Lsn::from_raw(0),
            frame_deleted_keys: Vec::new(),
            frame_toast_stale_keys: Vec::new(),
            frame_truncated_relations: Vec::new(),
            batch_truncated_relations: Vec::new(),
            batch_frames: 0,
            batch_events: 0,
            batch_last_lsn: Lsn::from_raw(0),
            frame_open: false,
            batch_deleted_pks: HashSet::new(),
            batch_toast_overlay: HashMap::new(),
            toast_overlay_pool: Vec::new(),
            batch_old_image_overlay: HashMap::new(),
            batch_old_image_guard_oids: HashSet::new(),
            batch_old_image_epochs: HashMap::new(),
            row_vec_pool: Vec::new(),
            batch_toast_guard_oids: HashSet::new(),
            data_dir: disk.data_dir,
            disk_total: disk.total,
            disk_available: disk.available,
            disk_limit_effective: disk.limit_effective,
            disk_pressure_ticks: 0,
            disk_drop_backoff: false,
        })
    }

    /// Borrow a cleared candidate `FingerprintSet` from the scratch pool
    /// (PGC-341/344). Reuses a previously-returned set — retaining its backing
    /// capacity — or allocates a fresh one when the pool is empty. Pair with
    /// [`candidate_set_return`]; the set is owned by the caller in between, so it
    /// can be read across `&mut self` calls without borrowing `self`.
    pub(super) fn candidate_set_take(&mut self) -> FingerprintSet {
        self.candidate_scratch.pop().unwrap_or_default()
    }

    /// Return a candidate set to the scratch pool, clearing it (capacity kept).
    /// Dropped rather than pooled past `CANDIDATE_SCRATCH_MAX`.
    pub(super) fn candidate_set_return(&mut self, mut set: FingerprintSet) {
        if self.candidate_scratch.len() >= CANDIDATE_SCRATCH_MAX {
            return;
        }
        set.clear();
        self.candidate_scratch.push(set);
    }

    /// Whether the population identified by `(fingerprint, generation)` is still
    /// the live, non-invalidated cached query — i.e. a parked merge/ready entry
    /// hasn't been superseded by a readmit (generation bump), invalidated, or
    /// evicted while it waited (PGC-250).
    pub(super) fn population_is_current(
        &self,
        fingerprint: Fingerprint,
        generation: Generation,
    ) -> bool {
        let live = self
            .cache
            .cached_queries
            .get1(&fingerprint)
            .map(|q| (q.generation, q.invalidated));
        population_finalize_allowed(live, generation)
    }

    /// Force the origin to flush WAL past a stuck merge snapshot.
    ///
    /// Emits a tiny transactional logical-decoding marker; the session's
    /// `synchronous_commit = on` flushes WAL through it before the call returns,
    /// advancing the flush pointer (and so, via the decoder + keepalive, the
    /// apply watermark) past every snapshot LSN at or below the marker. The
    /// marker is later in WAL than any gated snapshot, so one marker unsticks the
    /// whole gated backlog. It is not streamed to pgcache (no `messages` option)
    /// and a `Message` record is ignored if it ever arrived. Returns the marker
    /// LSN. See PGC-290.
    pub(super) async fn origin_flush_force(&self) -> CacheResult<Lsn> {
        let row = self
            .db_origin
            .query_one("SELECT pg_logical_emit_message(true, 'pgcache', '')", &[])
            .await
            .map_into_report::<CacheError>()
            .attach_loc("forcing origin WAL flush")?;
        Ok(Lsn::from(row.get::<_, PgLsn>(0)))
    }

    /// Whether the population merge drain has work and the writer is
    /// quiescent (no CDC frame open).
    pub(super) fn merges_drain_wanted(&self) -> bool {
        self.frame_state == FrameState::Idle
            && (!self.merges.pending.is_empty() || !self.merges.discards.is_empty())
    }

    /// Set the shape-gate classification and derive the initial MvState for a
    /// cached query. Called once per fresh registration (not on readmit / limit
    /// bump, since classification is sticky). The state_view entry is expected
    /// to exist — it is inserted on the dispatch path before dispatching
    /// `QueryCommand::Register`.
    pub(super) fn mv_state_set(
        &self,
        fingerprint: Fingerprint,
        shape_gate: ShapeGate,
        mv_limit: Option<u64>,
    ) {
        if let Some(mut view) = self.state_view.cached_queries.get_mut(&fingerprint) {
            view.mv = MvMeta::new(shape_gate, mv_limit);
        }
    }

    /// Preserves shape_gate and mv_state. Private — callers must go through
    /// the public `state_*_transition` wrappers so paired side effects (notify
    /// on Ready) aren't skipped.
    fn state_view_write(
        &self,
        fingerprint: Fingerprint,
        state: CachedQueryState,
        serving: &QueryServing,
    ) {
        // The serve shape mirrors `CachedQuery.serve_shape`; the cached query is
        // already inserted at every transition, so read it from there rather
        // than thread it through every transition caller (PGC-294).
        let serve_shape = self
            .cache
            .cached_queries
            .get1(&fingerprint)
            .map(|q| q.serve_shape.clone());
        self.state_view
            .cached_queries
            .entry(fingerprint)
            .and_modify(|v| {
                v.state = state;
                v.generation = serving.generation;
                v.resolved = Some(Arc::clone(&serving.resolved));
                v.deparsed_sql = Some(serving.deparsed_sql.clone());
                v.serve_shape = serve_shape.clone();
                v.max_limit = serving.max_limit;
                v.referenced = false;
            })
            .or_insert_with(|| CachedQueryView {
                state,
                generation: serving.generation,
                resolved: Some(Arc::clone(&serving.resolved)),
                deparsed_sql: Some(serving.deparsed_sql.clone()),
                serve_shape,
                max_limit: serving.max_limit,
                referenced: false,
                mv: MvMeta::new(ShapeGate::Skip, None),
            });
    }

    /// Caller must follow up with population work (or another Ready/Failed
    /// transition); otherwise coalesced waiters stay stuck.
    pub(super) fn state_loading_transition(
        &self,
        fingerprint: Fingerprint,
        serving: &QueryServing,
    ) {
        self.state_view_write(fingerprint, CachedQueryState::Loading, serving);
    }

    /// Mark Ready and notify the cache loop. Skipping the notify leaves
    /// coalesced waiters hung forever — always go through this wrapper.
    pub(super) fn state_ready_transition(&self, fingerprint: Fingerprint, serving: QueryServing) {
        self.state_view_write(fingerprint, CachedQueryState::Ready, &serving);
        let QueryServing {
            generation,
            resolved,
            deparsed_sql,
            max_limit,
        } = serving;
        let _ = self.notify_tx.send(WriterNotify::Ready {
            fingerprint,
            generation,
            resolved,
            deparsed_sql,
            max_limit,
        });
    }

    /// Drain any coalesced waiters parked on `fingerprint` to origin (the
    /// `Failed` counterpart to `state_ready_transition`). Call this whenever a
    /// query is abandoned mid-population — invalidated, evicted, or its
    /// register/populate failed: the `Ready` those waiters were parked on is
    /// dead, and under sustained churn a successor `Ready` may never come, so
    /// without this they hang forever. A no-op when nothing is parked.
    pub(super) fn waiters_fail(&self, fingerprint: Fingerprint) {
        let _ = self.notify_tx.send(WriterNotify::Failed { fingerprint });
    }
}

#[cfg(test)]
mod tests {
    use super::population_finalize_allowed;
    use crate::cache::Generation;

    /// Live query at the parked generation, not invalidated → finalize.
    #[test]
    fn test_finalize_allowed_when_current() {
        assert!(population_finalize_allowed(
            Some((Generation::from_raw(5), false)),
            Generation::from_raw(5)
        ));
    }

    /// Readmit bumped the generation while the entry was parked → skip.
    #[test]
    fn test_finalize_skipped_after_readmit() {
        assert!(!population_finalize_allowed(
            Some((Generation::from_raw(8), false)),
            Generation::from_raw(5)
        ));
    }

    /// Query invalidated while parked (a growing change superseded it) → skip.
    #[test]
    fn test_finalize_skipped_when_invalidated() {
        assert!(!population_finalize_allowed(
            Some((Generation::from_raw(5), true)),
            Generation::from_raw(5)
        ));
    }

    /// Query evicted while parked → skip.
    #[test]
    fn test_finalize_skipped_when_evicted() {
        assert!(!population_finalize_allowed(None, Generation::from_raw(5)));
    }
}
