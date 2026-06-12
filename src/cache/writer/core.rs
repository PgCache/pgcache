use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use ecow::EcoString;
use tokio::runtime::{Builder, Handle};
use tokio::sync::Notify;
use tokio::sync::mpsc::{Receiver, UnboundedReceiver, UnboundedSender};
use tokio::task::{LocalSet, yield_now};
use tokio_postgres::Client;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, trace};

use crate::cache::status::{
    CacheStatusData, CdcStatusData, LatencyStats, QueryStatusData, StatusRequest, StatusResponse,
};
use crate::pg;
use crate::query::ast::Deparse;
use crate::result::error_chain_format;
use crate::settings::{CachePolicy, Settings};

use super::super::{
    CacheError, CacheResult, MapIntoReport, ReportExt,
    messages::{CdcCommand, PopulationMerge, QueryCommand, WriterNotify},
    mv::{MvMeta, ShapeGate},
    types::{
        ActiveRelations, Cache, CacheStateView, CachedQueryState, CachedQueryView, SharedResolved,
    },
};
use super::cdc::WriterCdc;
use super::mv_build::MvBuildPool;
use super::registration::WriterRegistration;
use super::staging::PopulationDeletedKeys;

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

/// Preallocated capacity for the per-frame SQL write buffer (PGC-228). Fixed up
/// front so the buffer never reallocates in steady state; also the byte
/// threshold at which a frame's writes are chunk-flushed to bound memory.
pub(super) const FRAME_BUF_CAPACITY: usize = 256 * 1024;

/// Max full evictions per periodic-tick `eviction_run` call (PGC-251). Bounds the
/// single-threaded writer stall when reclaiming a large count-cap overshoot; the
/// remainder is reclaimed on subsequent ticks.
const EVICTION_TICK_BUDGET: usize = 512;

/// Maximum buffered row events per frame before a mid-frame partial replay —
/// bounds frame memory the way `FRAME_BUF_CAPACITY` bounds `frame_buf`.
/// Replaying a prefix early is exactly the per-arrival behavior, so ordering
/// and results are unchanged.
pub(super) const FRAME_ROWS_CAPACITY: usize = 4096;

/// One buffered row event of the in-progress CDC frame (PGC-241). Events are
/// collected at arrival and replayed at the `CommitMark` flush boundary, in
/// arrival order — order is what makes the deferral pure: same-key sequences
/// (an INSERT then DELETE of one PK) and TRUNCATE-vs-row interleavings emit
/// exactly as per-arrival handling did.
pub(super) enum FrameRowEvent {
    Insert {
        relation_oid: u32,
        row_data: Vec<Option<String>>,
    },
    Update {
        relation_oid: u32,
        key_data: Vec<Option<String>>,
        new_row_data: Vec<Option<String>>,
    },
    /// An UPDATE whose image carries unchanged-toast markers, awaiting repair
    /// (PGC-264). Resolved by the replay pre-pass (`toast_repair_events`) into
    /// a plain `Update` (values from the batch overlay or the batched cache
    /// lookup) or an `UpdateToastFallback` — no other consumer ever sees one.
    /// `Toasted` values are already mapped to `None` in `new_row_data`;
    /// `toasted` holds their column indexes.
    UpdateToasted {
        relation_oid: u32,
        key_data: Vec<Option<String>>,
        new_row_data: Vec<Option<String>>,
        toasted: Vec<usize>,
    },
    /// An UPDATE whose unchanged-toast columns could not be repaired (row
    /// absent from the cache table, or its in-batch state untrustworthy —
    /// PGC-264). Excluded from segment eval; the decide pass conservatively
    /// invalidates affected queries instead of upserting the incomplete image.
    /// `Toasted` values are already mapped to `None` in `new_row_data`;
    /// `toasted_columns` names the elided columns.
    UpdateToastFallback {
        relation_oid: u32,
        key_data: Vec<Option<String>>,
        new_row_data: Vec<Option<String>>,
        toasted_columns: Vec<EcoString>,
    },
    Delete {
        relation_oid: u32,
        row_data: Vec<Option<String>>,
    },
    Truncate {
        relation_oids: Vec<u32>,
    },
    /// A source-transaction commit boundary (PGC-242). Carries the frame's
    /// commit LSN so per-frame bookkeeping produced *during replay* — deleted
    /// keys (PGC-250) and truncate abort watermarks — is stamped with the
    /// right frame's LSN when the log spans multiple frames. Does not split
    /// eval segments (cross-frame batching is the point).
    Boundary {
        commit_lsn: u64,
    },
}

/// One entry of `batch_toast_overlay` (PGC-264): what this batch last did to
/// a PK's toastable columns.
pub(super) enum ToastOverlayEntry {
    /// Toastable-column `(position, value)` pairs from the last in-batch
    /// write of the row.
    Values(Vec<(usize, Option<String>)>),
    /// The PK was deleted this batch with no subsequent write. The pre-batch
    /// image is stale, and origin cannot update a deleted row, so a toasted
    /// update hitting this is defensive-fallback territory.
    Deleted,
}

/// CDC source-txn frame state on `WriterCdc`'s write connection (PGC-108).
/// `Idle →Begin Active →write TxnOpen →Commit Idle`; `* →40P01 Recovering`.
/// Writes are buffered (PGC-228): a `BEGIN` and the cache-table statements
/// accumulate in `frame_buf` and reach the server only when the buffer is
/// chunk-flushed mid-frame or flushed as one `BEGIN; …; COMMIT` at `CommitMark`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FrameState {
    /// Between source transactions.
    Idle,
    /// In a frame, no cache-table write yet — no `BEGIN`/locks, but
    /// invalidations/oids may be accumulating for the `CommitMark` flush.
    Active,
    /// A cache-table write has been buffered: a `BEGIN` is owed and a `COMMIT`
    /// pending at `CommitMark` (was `frame_open`). Holds no server-side locks
    /// until the buffer flushes (chunk-flush or `CommitMark`); from the first
    /// flush on, the open txn holds row locks until `COMMIT`.
    TxnOpen,
    /// Hit `40P01`, rolled back; drains the rest of the txn with no DB apply,
    /// recovers affected relations at `CommitMark` (PGC-147). Holds no locks.
    Recovering,
}

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
    publication_name: EcoString,
    /// OIDs currently in the publication (mirrors the origin-side state).
    publication_oids: HashSet<u32>,
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
    pub(super) frame_invalidations: HashSet<u64>,
    /// Relation OIDs touched by the in-progress frame, accumulated from frame
    /// start so a mid-frame `40P01` can invalidate+truncate every affected
    /// relation (commands applied before the deadlock were rolled back too).
    pub(super) frame_relation_oids: HashSet<u32>,
    /// Set when a generation purge was skipped because a frame was open;
    /// flushed after the frame commits at `CommitMark`.
    pub(super) purge_pending: bool,
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
    /// Keys CDC removed while populations are in flight, so a population merge
    /// doesn't resurrect them (PGC-250). Activated at dispatch, recorded at
    /// `frame_cache_delete`, consulted/cleared at merge.
    pub(super) population_deleted_keys: PopulationDeletedKeys,
    /// Population merges awaiting both a quiescent (frame-Idle) writer and the
    /// CDC apply watermark reaching their snapshot LSN (PGC-272): a min-heap
    /// on `(snapshot_lsn, generation)`, drained in deadline order by
    /// `pending_merges_drain` as the watermark advances. Gating the merge —
    /// not just Ready — keeps snapshot-state rows out of the shared table
    /// until CDC has applied past the snapshot, so already-Ready bystander
    /// queries can never serve a torn mix of two origin points in time.
    pub(super) pending_merges: BinaryHeap<Reverse<PendingMerge>>,
    /// Signals the CDC thread to request an immediate keepalive (reply-requested
    /// standby status update), advancing `last_applied_lsn` so a gated query's
    /// snapshot LSN is reached within a round-trip instead of waiting for the
    /// next periodic keepalive.
    pub(super) watermark_nudge: Arc<Notify>,
    /// Mirror of `WriterCdc.last_applied_lsn`, updated as the CDC path advances
    /// the watermark. Read at population dispatch to seed the deleted-key
    /// anchor floor (a lower bound on the population's snapshot LSN).
    pub(super) last_applied_lsn: u64,
    /// PK tuple bodies removed by the in-progress CDC frame, drained at
    /// `CommitMark` and recorded into `population_deleted_keys` stamped with the
    /// frame's commit LSN (rolled-back frames clear it instead). Buffered because
    /// the commit LSN isn't known until the frame commits.
    pub(super) frame_deleted_keys: Vec<(u32, EcoString)>,
    /// Relations bulk-invalidated by the in-progress frame (TRUNCATE, or 40P01
    /// recovery), drained at `CommitMark` to raise their deleted-key abort
    /// watermark to the commit LSN — same commit-LSN-deferral as
    /// `frame_deleted_keys`.
    pub(super) frame_truncated_relations: Vec<u32>,
    /// Relations bulk-invalidated outside replay (mid-batch intra-txn DDL
    /// drops, 40P01 recovery), drained at the batch flush and stamped with the
    /// flush LSN — an upper bound on the triggering frame's commit, which
    /// over-aborts (safe) where a replay-boundary stamp could under-abort
    /// (PGC-242).
    pub(super) batch_truncated_relations: Vec<u32>,
    /// Complete source frames accumulated in the current batch (PGC-242):
    /// boundaries pushed since the last flush.
    pub(super) batch_frames: usize,
    /// Row events accumulated in the current batch, counted at push — survives
    /// mid-frame partial replays draining `frame_rows`, so the flush size cap
    /// sees the true batch size.
    pub(super) batch_events: usize,
    /// The last accumulated frame's commit LSN — the watermark target when a
    /// flush is forced between CommitMarks (KeepAliveMark).
    pub(super) batch_last_lsn: u64,
    /// Whether a source frame is open (between `Begin` and `CommitMark`).
    /// `frame_state` no longer distinguishes this once batches span frames.
    pub(super) frame_open: bool,
    /// PKs the current batch has deleted from cache tables (and not since
    /// re-upserted). Row-change presence lookups read the pre-batch committed
    /// state; a later frame updating one of these PKs must be classified
    /// UNCACHED (`row_changes = None`) or the entering-invalidation the
    /// per-frame flow produced is lost (PGC-242; `test_cache_join`'s PK flip).
    pub(super) batch_deleted_pks: HashSet<(u32, EcoString)>,
    /// Last in-batch write per PK of the toastable columns' values (PGC-264).
    /// The toast-repair lookup reads the pre-batch committed state, which is
    /// stale for any PK this batch has already written; the overlay supplies
    /// the in-batch value instead (an in-memory repair, no fallback), and
    /// `Deleted` tombstones block the stale lookup outright. Maintained in
    /// arrival order by the replay pre-pass; only relations with a toastable
    /// column pay for it. Same lifecycle as `batch_deleted_pks`.
    pub(super) batch_toast_overlay: HashMap<(u32, EcoString), ToastOverlayEntry>,
    /// Relations truncated or DDL-recreated in the current batch (PGC-264).
    /// Their pre-batch committed images are wholesale untrustworthy as a
    /// toast-repair source; only overlay values written after the truncate
    /// can repair. Same lifecycle as `batch_deleted_pks`.
    pub(super) batch_toast_guard_oids: HashSet<u32>,
    /// Cache PG data directory, discovered once at startup, for `statvfs` to
    /// auto-size the disk eviction limit (PGC-251 Slice 2). `None` if it couldn't
    /// be read (non-superuser, or not visible) — auto disk limit then disabled.
    data_dir: Option<PathBuf>,
    /// Last `statvfs` reading of the data directory's filesystem (total,
    /// available) in bytes; refreshed on the 1 s tick. `disk_total == 0` means
    /// "no reading" — `disk_limit_compute` then takes no auto limit.
    disk_total: u64,
    disk_available: u64,
}

/// A queued population merge, ordered by its watermark deadline (PGC-272).
/// The ordering key is `(snapshot_lsn, generation)`: `generation` comes from
/// the single global monotonic counter, so the tuple is a total order even
/// when two populations capture identical snapshot LSNs. Deliberately NOT
/// `fingerprint` — two populations of one fingerprint at different
/// generations can be in flight simultaneously and must not tie. The payload
/// is excluded from the ordering.
pub(super) struct PendingMerge(pub(super) PopulationMerge);

impl PendingMerge {
    fn key(&self) -> (u64, u64) {
        (self.0.snapshot_lsn, self.0.generation)
    }
}

impl Ord for PendingMerge {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key().cmp(&other.key())
    }
}

impl PartialOrd for PendingMerge {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for PendingMerge {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}

impl Eq for PendingMerge {}

/// Whether a parked population (a queued merge or a gated ready entry) at
/// `parked_generation` may still finalize. `live` is the current cached query's
/// `(generation, invalidated)`, or `None` if it was evicted. Finalize only when
/// the live query exists, hasn't been superseded by a readmit (generation
/// bumped), and isn't invalidated — otherwise the parked entry is stale and
/// finalizing it would mark a superseded/invalidated result Ready (PGC-250).
fn population_finalize_allowed(live: Option<(u64, bool)>, parked_generation: u64) -> bool {
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

impl WriterCore {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        settings: &Settings,
        state_view: Arc<CacheStateView>,
        active_relations: ActiveRelations,
        notify_tx: UnboundedSender<WriterNotify>,
        query_tx: UnboundedSender<QueryCommand>,
        watermark_nudge: Arc<Notify>,
        runtime: Handle,
    ) -> CacheResult<Self> {
        let cache_client = pg::connect(&settings.cache, "writer cache")
            .await
            .map_into_report::<CacheError>()?;

        let origin_client = pg::connect(&settings.origin, "writer origin")
            .await
            .map_into_report::<CacheError>()
            .attach_loc("connecting to origin database")?;

        let data_dir = data_directory_query(&cache_client).await;
        let (disk_total, disk_available) = data_dir
            .as_deref()
            .and_then(crate::memory::disk_stats_bytes)
            .unwrap_or((0, 0));

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
            notify_tx,
            frame_state: FrameState::Idle,
            frame_invalidations: HashSet::new(),
            frame_relation_oids: HashSet::new(),
            purge_pending: false,
            frame_buf: String::with_capacity(FRAME_BUF_CAPACITY),
            frame_rows: Vec::new(),
            frame_chunk_flushed: false,
            population_deleted_keys: PopulationDeletedKeys::default(),
            pending_merges: BinaryHeap::new(),
            watermark_nudge,
            last_applied_lsn: 0,
            frame_deleted_keys: Vec::new(),
            frame_truncated_relations: Vec::new(),
            batch_truncated_relations: Vec::new(),
            batch_frames: 0,
            batch_events: 0,
            batch_last_lsn: 0,
            frame_open: false,
            batch_deleted_pks: HashSet::new(),
            batch_toast_overlay: HashMap::new(),
            batch_toast_guard_oids: HashSet::new(),
            data_dir,
            disk_total,
            disk_available,
        })
    }

    /// Whether the population identified by `(fingerprint, generation)` is still
    /// the live, non-invalidated cached query — i.e. a parked merge/ready entry
    /// hasn't been superseded by a readmit (generation bump), invalidated, or
    /// evicted while it waited (PGC-250).
    pub(super) fn population_is_current(&self, fingerprint: u64, generation: u64) -> bool {
        let live = self
            .cache
            .cached_queries
            .get1(&fingerprint)
            .map(|q| (q.generation, q.invalidated));
        population_finalize_allowed(live, generation)
    }

    /// Buffer the frame's `BEGIN` on the first cache-table write (`Active →
    /// TxnOpen`); idempotent for later writes. The actual `BEGIN` reaches the
    /// server only when `frame_buf` is flushed. A write while `Idle` (no
    /// preceding `Begin`) is a bug.
    pub(super) fn frame_begin_ensure(&mut self) {
        debug_assert!(
            !matches!(self.frame_state, FrameState::Idle),
            "cache-table write before Begin (frame not entered)"
        );
        if self.frame_state == FrameState::Active {
            self.frame_buf.push_str("BEGIN; ");
            self.frame_state = FrameState::TxnOpen;
        }
    }

    /// Frame holds row locks (a `BEGIN` is open) — maintenance paths defer
    /// cache-table DDL/purges while true.
    pub(super) fn frame_holds_locks(&self) -> bool {
        matches!(self.frame_state, FrameState::TxnOpen)
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
        if self.frame_holds_locks() {
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
    /// to exist — it is inserted on the dispatch path before dispatching
    /// `QueryCommand::Register`.
    pub(super) fn mv_state_set(
        &self,
        fingerprint: u64,
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
                mv: MvMeta::new(ShapeGate::Skip, None),
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

    /// Drain any coalesced waiters parked on `fingerprint` to origin (the
    /// `Failed` counterpart to `state_ready_transition`). Call this whenever a
    /// query is abandoned mid-population — invalidated, evicted, or its
    /// register/populate failed: the `Ready` those waiters were parked on is
    /// dead, and under sustained churn a successor `Ready` may never come, so
    /// without this they hang forever. A no-op when nothing is parked.
    pub(super) fn waiters_fail(&self, fingerprint: u64) {
        let _ = self.notify_tx.send(WriterNotify::Failed { fingerprint });
    }

    /// Update cache state gauges with current values.
    //
    // Counts and byte totals are converted to f64 for Prometheus gauges; gauges
    // accept f64 by API and the precision loss only matters above 2^53.
    #[allow(clippy::cast_precision_loss)]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) fn state_gauges_update(&self) {
        let registered = self.cache.cached_queries.len();
        crate::metrics::handles()
            .state
            .queries_registered
            .set(registered as f64);
        // Publish for the memory monitor to size the count cap (PGC-251).
        self.state_view
            .registered_count
            .store(registered, Ordering::Relaxed);

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

            crate::metrics::handles()
                .state
                .queries_loading
                .set(loading_count as f64);
            crate::metrics::handles()
                .state
                .queries_pending
                .set(pending_count as f64);
            crate::metrics::handles()
                .state
                .queries_invalidated
                .set(invalidated_count as f64);
        }

        crate::metrics::handles()
            .state
            .size_bytes
            .set(self.cache.current_size as f64);
        if let Some(limit) = self.disk_limit_compute(self.cache.dynamic.load().cache_size) {
            crate::metrics::handles()
                .state
                .size_limit_bytes
                .set(limit as f64);
        }
        crate::metrics::handles()
            .state
            .generation
            .set(self.cache.generation_counter as f64);
        crate::metrics::handles()
            .state
            .tables_tracked
            .set(self.cache.tables.len() as f64);
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
        crate::metrics::handles()
            .state
            .update_queries_total
            .set(total as f64);
        crate::metrics::handles()
            .state
            .update_queries_max_per_relation
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
        if self.frame_holds_locks() {
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

    /// Refresh the cached `statvfs` reading for the cache PG data directory,
    /// used to auto-size the disk eviction limit (PGC-251 Slice 2). One syscall;
    /// a no-op leaving the last reading if the data directory wasn't discovered
    /// or can't be stat'd.
    pub(super) fn disk_stats_refresh(&mut self) {
        let Some(dir) = self.data_dir.as_deref() else {
            return;
        };
        if let Some((total, available)) = crate::memory::disk_stats_bytes(dir) {
            self.disk_total = total;
            self.disk_available = available;
        }
    }

    /// Effective disk-size eviction limit in bytes, or `None` for no disk-driven
    /// eviction. An explicit `cache_size` config is a hard override; otherwise the
    /// limit is auto-derived from live filesystem free space (PGC-251 Slice 2),
    /// disabled only when no `statvfs` reading is available.
    pub(super) fn disk_limit_compute(&self, cache_size: Option<usize>) -> Option<usize> {
        if let Some(explicit) = cache_size {
            return Some(explicit);
        }
        if self.disk_total == 0 {
            return None;
        }
        let limit = crate::memory::disk_limit_auto(
            self.cache.current_size as u64,
            self.disk_total,
            self.disk_available,
        );
        Some(usize::try_from(limit).unwrap_or(usize::MAX))
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
        // Memory count cap (PGC-251): evict down to it independently of the disk
        // limit. `usize::MAX` = uncapped (no count-driven eviction).
        let count_cap = self.state_view.query_count_cap.load(Ordering::Relaxed);
        // Disk-size limit: explicit `cache_size` override, else auto-derived from
        // live free space (PGC-251 Slice 2). Snapshotted once; the loop drives
        // current_size down against this fixed anchor.
        let disk_limit = self.disk_limit_compute(cfg.cache_size);

        debug!(
            current_size = self.cache.current_size,
            disk_limit = ?disk_limit,
            count = self.cache.cached_queries.len(),
            count_cap,
            cache_policy = ?cfg.cache_policy,
            "eviction_run entry"
        );

        // Pre-sweep: reclaim bytes held by Dirty MVs before considering live
        // entries for eviction. If this alone brings current_size under the
        // limit, the loop below exits immediately without evicting anything.
        if disk_limit.is_some_and(|s| self.cache.current_size > s) {
            self.mv_dirty_sweep().await?;
            self.cache.current_size = self.cache_size_load().await?;
        }

        loop {
            let over_disk = disk_limit.is_some_and(|s| self.cache.current_size > s);
            let over_count = self.cache.cached_queries.len() > count_cap;
            if !over_disk && !over_count {
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
            // publication_dirty_drain drops the orphaned cache tables; the
            // trigger is what pgcache_total_size sums, so the next iteration's
            // cache_size_load needs the drain to observe a shrink.
            self.publication_dirty_drain().await?;
            // Reload disk size only when the disk limit is active; the count-cap
            // path needs no pgcache_total_size() round-trip per eviction.
            if disk_limit.is_some() {
                self.cache.current_size = self.cache_size_load().await?;
            }
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

            let (state, mv_state) = self
                .state_view
                .cached_queries
                .get(&q.fingerprint)
                .map(|entry| {
                    (
                        format!("{:?}", entry.value().state),
                        format!("{:?}", entry.value().mv.state),
                    )
                })
                .unwrap_or_else(|| ("Unknown".to_owned(), "Unknown".to_owned()));

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
                mv_state,
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
            fault_injection: cfg!(feature = "fault-injection"),
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
    watermark_nudge: Arc<Notify>,
    shared_runtime: Handle,
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
                    watermark_nudge,
                    shared_runtime,
                )
                .await?;
                let mut registration = WriterRegistration::new(
                    settings,
                    &core.db_origin,
                    query_tx,
                    Arc::clone(&core.state_view.registration_throttled),
                )
                .await?;
                let mut writer_cdc = WriterCdc::new(settings).await?;

                // Gauges (queries_loading/pending/invalidated, cache_size_bytes,
                // generation, tables_tracked, update_queries_total/max) used to
                // be emitted from every query/CDC command. state_gauges_update
                // iterates the entire state_view DashMap, which dominated
                // writer per-command time at scale. Emit on a 1s tick instead —
                // well below typical Prometheus scrape intervals.
                let mut gauges_interval = tokio::time::interval(Duration::from_secs(1));
                gauges_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

                #[cfg(feature = "fault-injection")]
                fault::init();

                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            debug!("writer shutdown signal received");
                            break;
                        }
                        _ = gauges_interval.tick() => {
                            core.disk_stats_refresh();
                            core.stale_entries_cleanup();
                            core.state_gauges_update();
                            core.writer_scale_gauges_update();
                            core.state_view.memo.gc();
                            core.state_view.memo.metrics_publish();
                            // Enforce the memory count cap independently of
                            // registration: under throttle-freeze no Ready events
                            // arrive, so the registration-path eviction can't run
                            // (PGC-251). Bounded per tick; log-and-continue so a
                            // periodic best-effort eviction never kills the writer.
                            if let Err(e) = core.eviction_run(Some(EVICTION_TICK_BUDGET)).await {
                                error!(
                                    "periodic eviction failed: {}",
                                    error_chain_format(e.current_context())
                                );
                            }
                        }
                        // Handle query commands from dispatch
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
                        // Handle CDC commands from the CDC thread
                        msg = cdc_rx.recv() => {
                            match msg {
                                Some(cmd) => {
                                    #[cfg(feature = "fault-injection")]
                                    if let CdcCommand::Insert { row_data, .. } = &cmd
                                        && fault::writer_die_check(row_data)
                                    {
                                        error!("fault injection: writer exiting on sentinel CDC insert to exercise restart");
                                        return Err(CacheError::CdcFailure.into());
                                    }
                                    // Queue depth after this command drives the
                                    // batch flush decision (PGC-242): an empty
                                    // queue flushes immediately; a backlog
                                    // accumulates frames.
                                    let queued = cdc_rx.len();
                                    if let Err(e) = writer_cdc
                                        .cdc_command_handle(&mut core, cmd, queued)
                                        .await
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

                    // Drain population merges while the writer is quiescent
                    // (no CDC frame open), so neither the merge nor eviction
                    // (both on db_cache) races the CDC writer's frame txn on
                    // the shared cache table (PGC-250). Each merge is
                    // additionally gated on the apply watermark reaching its
                    // snapshot LSN (PGC-272); the watermark advances on the
                    // CDC path, so re-check on every quiescent iteration.
                    if core.frame_state == FrameState::Idle
                        && !core.pending_merges.is_empty()
                        && let Err(e) = registration
                            .pending_merges_drain(&mut core, writer_cdc.last_applied_lsn)
                            .await
                    {
                        error!(
                            "population merge drain failed: {}",
                            error_chain_format(e.current_context()),
                        );
                    }

                    // Channel depths are reported as f64 gauges; queue sizes never approach 2^53.
                    #[allow(clippy::cast_precision_loss)]
                    {
                        crate::metrics::handles()
                            .state
                            .queue_writer_query
                            .set(query_rx.len() as f64);
                        crate::metrics::handles()
                            .state
                            .queue_writer_cdc
                            .set(cdc_rx.len() as f64);
                        crate::metrics::handles()
                            .state
                            .queue_writer_internal
                            .set(internal_rx.len() as f64);
                    }
                }

                Ok(())
            })
            .await
    })
}

#[cfg(test)]
mod tests {
    use super::population_finalize_allowed;

    /// Live query at the parked generation, not invalidated → finalize.
    #[test]
    fn finalize_allowed_when_current() {
        assert!(population_finalize_allowed(Some((5, false)), 5));
    }

    /// Readmit bumped the generation while the entry was parked → skip.
    #[test]
    fn finalize_skipped_after_readmit() {
        assert!(!population_finalize_allowed(Some((8, false)), 5));
    }

    /// Query invalidated while parked (a growing change superseded it) → skip.
    #[test]
    fn finalize_skipped_when_invalidated() {
        assert!(!population_finalize_allowed(Some((5, true)), 5));
    }

    /// Query evicted while parked → skip.
    #[test]
    fn finalize_skipped_when_evicted() {
        assert!(!population_finalize_allowed(None, 5));
    }
}
