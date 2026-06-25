use crate::catalog::Oid;
use crate::pg::Lsn;
use crate::query::{Fingerprint, FingerprintSet};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use ecow::EcoString;
use postgres_types::PgLsn;
use tokio::runtime::{Builder, Handle};
use tokio::sync::Notify;
use tokio::sync::mpsc::{Receiver, UnboundedReceiver, UnboundedSender};
use tokio::task::LocalSet;
use tokio_postgres::Client;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use crate::cache::status::StatusRequest;
use crate::pg;
use crate::pg::protocol::ByteString;
use crate::result::error_chain_format;
use crate::settings::Settings;

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
use super::staging::{PopulationDeletedKeys, StagingPool};

use super::frame::*;

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

/// Max full evictions per periodic-tick `eviction_run` call (PGC-251). Bounds the
/// single-threaded writer stall when reclaiming a large count-cap overshoot; the
/// remainder is reclaimed on subsequent ticks.
const EVICTION_TICK_BUDGET: usize = 512;

/// How long a population merge may stay gated on the apply watermark before the
/// writer forces an origin WAL flush (`origin_flush_force`) to make its snapshot
/// LSN reachable (PGC-290). Above the nudge→keepalive round-trip so a healthy
/// active origin, where the flush pointer catches up on its own, never triggers
/// a marker; the cost is at most this much extra ready-latency on a stalled
/// (idle / async-commit) origin.
pub(super) const MERGE_FLUSH_FORCE_AFTER: Duration = Duration::from_millis(100);

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
    /// When the earliest gated population merge first became gated, or `None`
    /// when nothing is gated. Times the grace window before `origin_flush_force`
    /// is used to make a stuck snapshot LSN reachable (PGC-290).
    pub(super) merge_stall_since: Option<std::time::Instant>,
    /// LSN of the last `origin_flush_force` marker. A merge whose snapshot LSN is
    /// at or below this has already had the flush pointer forced past it, so it
    /// needs no further marker — this gates re-emits to roughly one per stuck
    /// wave rather than one per gated merge (PGC-290).
    pub(super) last_flush_marker_lsn: Lsn,
    /// Mirror of `WriterCdc.last_applied_lsn`, updated as the CDC path advances
    /// the watermark. Read at population dispatch to seed the deleted-key
    /// anchor floor (a lower bound on the population's snapshot LSN).
    pub(super) last_applied_lsn: Lsn,
    /// PK tuple bodies removed by the in-progress CDC frame, drained at
    /// `CommitMark` and recorded into `population_deleted_keys` stamped with the
    /// frame's commit LSN (rolled-back frames clear it instead). Buffered because
    /// the commit LSN isn't known until the frame commits.
    pub(super) frame_deleted_keys: Vec<(Oid, EcoString)>,
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
    pub(super) batch_toast_overlay: HashMap<(Oid, EcoString), ToastOverlayEntry>,
    /// Recycled `ToastOverlayEntry::Values` allocations: batch reset harvests
    /// cleared Vecs here instead of dropping them, so steady-state overlay
    /// recording allocates no per-event Vec. Bounded by
    /// [`TOAST_OVERLAY_POOL_MAX`].
    pub(super) toast_overlay_pool: Vec<Vec<(usize, Option<ByteString>)>>,
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

/// A queued population merge, ordered by its watermark deadline (PGC-272).
/// The ordering key is `(snapshot_lsn, generation)`: `generation` comes from
/// the single global monotonic counter, so the tuple is a total order even
/// when two populations capture identical snapshot LSNs. Deliberately NOT
/// `fingerprint` — two populations of one fingerprint at different
/// generations can be in flight simultaneously and must not tie. The payload
/// is excluded from the ordering.
pub(super) struct PendingMerge(pub(super) PopulationMerge);

impl PendingMerge {
    fn key(&self) -> (Lsn, u64) {
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

                // Gauges (queries_loading/pending/invalidated, disk_used_bytes,
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
                            // Diagnose merge-gate stalls (PGC-290): if merges are
                            // parked, report why the drain gate (frame_state ==
                            // Idle && watermark >= min snapshot_lsn) is not firing.
                            if !core.pending_merges.is_empty() {
                                let min_snap =
                                    core.pending_merges.peek().map(|Reverse(m)| m.0.snapshot_lsn);
                                debug!(
                                    "merge-gate: frame_state={:?} frame_open={} pending={} min_snapshot_lsn={:?} watermark={:?}",
                                    core.frame_state,
                                    core.frame_open,
                                    core.pending_merges.len(),
                                    min_snap,
                                    writer_cdc.last_applied_lsn,
                                );
                            }
                            #[allow(clippy::cast_precision_loss)]
                            crate::metrics::handles()
                                .reg
                                .merge_pending_depth
                                .set(core.pending_merges.len() as f64);
                            core.disk_stats_refresh();
                            core.stale_entries_cleanup();
                            core.state_gauges_update();
                            core.writer_scale_gauges_update();
                            core.state_view.memo.gc();
                            core.state_view.memo.metrics_publish();
                            // Eviction runs only here now (not per Ready, PGC-276).
                            // Enforce the memory count cap independently of
                            // registration: under throttle-freeze no Ready events
                            // arrive (PGC-251). Bounded per tick; log-and-continue so a
                            // periodic best-effort eviction never kills the writer.
                            if let Err(e) = core.eviction_run(Some(EVICTION_TICK_BUDGET)).await {
                                error!(
                                    "periodic eviction failed: {}",
                                    error_chain_format(e.current_context())
                                );
                            }
                            // Disk-pressure throttle + escalating reclaim (PGC-276).
                            if let Err(e) = core.disk_pressure_handle().await {
                                error!(
                                    "disk pressure handling failed: {}",
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

                    // Fold the writer backlog into the adaptive-gate window every
                    // iteration (PGC-277): catches the drain-to-empty moments the
                    // controller's coarse tick would miss. The internal channel
                    // (population completions) is the backlog that saturates first.
                    let internal_depth = internal_rx.len();
                    core.state_view.reg_gate.queue_observe(internal_depth);

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
                            .set(internal_depth as f64);
                    }
                }

                Ok(())
            })
            .await
    })
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
        // `origin_flush_force` relies on its marker's commit flushing WAL before
        // returning, so its LSN is reachable by the apply watermark (PGC-290).
        // This is the only session that writes the marker; reads/DDL here are
        // unaffected by the durability setting.
        origin_client
            .batch_execute("SET synchronous_commit = on")
            .await
            .map_into_report::<CacheError>()
            .attach_loc("setting synchronous_commit on writer origin")?;

        let data_dir = data_directory_query(&cache_client).await;
        let (disk_total, disk_available) = data_dir
            .as_deref()
            .and_then(crate::memory::disk_stats_bytes)
            .unwrap_or((0, 0));
        let disk_limit_effective =
            crate::memory::disk_limit_resolve(disk_total, settings.dynamic.load().disk_limit);

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
            frame_relation_oids: HashSet::new(),
            purge_pending: false,
            frame_buf: String::with_capacity(FRAME_BUF_CAPACITY),
            frame_rows: Vec::new(),
            frame_chunk_flushed: false,
            frame_buf_relations: HashSet::new(),
            population_deleted_keys: PopulationDeletedKeys::default(),
            staging_pool: StagingPool::default(),
            pending_merges: BinaryHeap::new(),
            watermark_nudge,
            merge_stall_since: None,
            last_flush_marker_lsn: Lsn::from_raw(0),
            last_applied_lsn: Lsn::from_raw(0),
            frame_deleted_keys: Vec::new(),
            frame_truncated_relations: Vec::new(),
            batch_truncated_relations: Vec::new(),
            batch_frames: 0,
            batch_events: 0,
            batch_last_lsn: Lsn::from_raw(0),
            frame_open: false,
            batch_deleted_pks: HashSet::new(),
            batch_toast_overlay: HashMap::new(),
            toast_overlay_pool: Vec::new(),
            row_vec_pool: Vec::new(),
            batch_toast_guard_oids: HashSet::new(),
            data_dir,
            disk_total,
            disk_available,
            disk_limit_effective,
            disk_pressure_ticks: 0,
            disk_drop_backoff: false,
        })
    }

    /// Whether the population identified by `(fingerprint, generation)` is still
    /// the live, non-invalidated cached query — i.e. a parked merge/ready entry
    /// hasn't been superseded by a readmit (generation bump), invalidated, or
    /// evicted while it waited (PGC-250).
    pub(super) fn population_is_current(&self, fingerprint: Fingerprint, generation: u64) -> bool {
        let live = self
            .cache
            .cached_queries
            .get1(&fingerprint)
            .map(|q| (q.generation, q.invalidated));
        population_finalize_allowed(live, generation)
    }

    /// Sync the origin publication to `active_relations` and drop any cache
    /// tables that just fell out of the active set. The drop happens here,
    /// after the ALTER PUBLICATION, because `oids_to_table_list` resolves
    /// oid → schema.name from `cache.tables` — if we dropped first that
    /// lookup would return empty.
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

    // Helper methods

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
        generation: u64,
        resolved: &SharedResolved,
        deparsed_sql: &EcoString,
        max_limit: Option<u64>,
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
                v.generation = generation;
                v.resolved = Some(Arc::clone(resolved));
                v.deparsed_sql = Some(deparsed_sql.clone());
                v.serve_shape = serve_shape.clone();
                v.max_limit = max_limit;
                v.referenced = false;
            })
            .or_insert_with(|| CachedQueryView {
                state,
                generation,
                resolved: Some(Arc::clone(resolved)),
                deparsed_sql: Some(deparsed_sql.clone()),
                serve_shape,
                max_limit,
                referenced: false,
                mv: MvMeta::new(ShapeGate::Skip, None),
            });
    }

    /// Caller must follow up with population work (or another Ready/Failed
    /// transition); otherwise coalesced waiters stay stuck.
    pub(super) fn state_loading_transition(
        &self,
        fingerprint: Fingerprint,
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
        fingerprint: Fingerprint,
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
    pub(super) fn waiters_fail(&self, fingerprint: Fingerprint) {
        let _ = self.notify_tx.send(WriterNotify::Failed { fingerprint });
    }
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
