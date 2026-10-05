//! CDC command intake: frame begin / commit / keepalive bookkeeping, relation
//! registration, and converting row commands into buffered frame events that
//! the replay (`apply`) later decides.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use lru::LruCache;
use metrics::Histogram;
use tracing::trace;

use super::segment_eval::PreparedEvalKey;
use super::{BATCH_FRAMES_MAX, PREPARED_EVAL_CACHE_CAPACITY, SQL_BUFFER_CAPACITY, WriterCdc};
use crate::cache::messages::{CdcCommand, CdcValue};
use crate::cache::writer::core::WriterCore;
use crate::cache::writer::frame::{FRAME_ROWS_CAPACITY, FrameRowEvent, FrameState, ToastState};
use crate::cache::{CacheError, CacheResult, MapIntoReport, ReportExt};
use crate::catalog::TableMetadata;
use crate::oid::Oid;
use crate::pg;
use crate::pg::Lsn;
use crate::settings::Settings;

fn cdc_command_histogram(cmd: &CdcCommand) -> &'static Histogram {
    let m = crate::metrics::handles();
    match cmd {
        CdcCommand::Begin { .. } => &m.cdc.cmd_begin,
        CdcCommand::TableRegister(_) => &m.cdc.cmd_table_register,
        CdcCommand::Insert { .. } => &m.cdc.cmd_insert,
        CdcCommand::Update { .. } => &m.cdc.cmd_update,
        CdcCommand::Delete { .. } => &m.cdc.cmd_delete,
        CdcCommand::Truncate { .. } => &m.cdc.cmd_truncate,
        CdcCommand::CommitMark { .. } => &m.cdc.cmd_commit_mark,
        CdcCommand::KeepAliveMark { .. } => &m.cdc.cmd_keepalive_mark,
    }
}

fn frame_begin_handle(core: &mut WriterCore, xid: u32) {
    debug_assert!(
        !core.frame_open,
        "Begin within an open source-transaction frame"
    );
    trace!(xid, "cdc frame begin");
    core.frame_open = true;
    // Reset only at a fresh batch — when accumulating (PGC-242) the log,
    // buffers, and pending bookkeeping span frames.
    if core.frame_state == FrameState::Idle {
        core.frame_state = FrameState::Active;
        core.frame_buf.clear();
        core.frame_buf_relations.clear();
        core.frame_rows.clear();
        core.frame_chunk_flushed = false;
        core.frame_deleted_keys.clear();
        core.frame_toast_stale_keys.clear();
        core.frame_truncated_relations.clear();
        core.batch_deleted_pks.clear();
        core.toast_overlay_reset();
        core.batch_toast_guard_oids.clear();
    }
}

/// Record `relation_oid` as touched by the frame, and whether the frame still
/// takes events. Relation OIDs are recorded from frame start so a mid-frame
/// 40P01 can recover every relation the frame touched (pre-deadlock writes
/// rolled back too).
fn frame_relation_accept(core: &mut WriterCore, relation_oid: Oid) -> bool {
    core.frame_relation_oids.insert(relation_oid);
    core.frame_state != FrameState::Recovering
}

fn update_command_event(
    core: &mut WriterCore,
    relation_oid: Oid,
    key_data: Vec<CdcValue>,
    row_data: Vec<CdcValue>,
) -> Option<FrameRowEvent> {
    if !frame_relation_accept(core, relation_oid) {
        return None;
    }
    let (key_data, key_toasted) = core.row_convert(key_data);
    let (new_row_data, toasted) = core.row_convert(row_data);
    // Key tuples carry real values under every replica identity; a toasted
    // one can't even key the old-PK delete.
    if !key_toasted.is_empty() {
        WriterCdc::toast_unexpected_invalidate(core, relation_oid, "update key tuple");
        return None;
    }
    // Toasted images are resolved by the replay pre-pass
    // (`toast_repair_events`) — batched there instead of a per-event lookup
    // here.
    let toast = if toasted.is_empty() {
        ToastState::Complete
    } else {
        ToastState::Pending(toasted)
    };
    Some(FrameRowEvent::Update {
        relation_oid,
        key_data,
        new_row_data,
        toast,
    })
}

fn delete_command_event(
    core: &mut WriterCore,
    relation_oid: Oid,
    row_data: Vec<CdcValue>,
) -> Option<FrameRowEvent> {
    if !frame_relation_accept(core, relation_oid) {
        return None;
    }
    let (row_data, toasted) = core.row_convert(row_data);
    // Delete images are key/old tuples — same reasoning as the update key
    // tuple.
    if !toasted.is_empty() {
        WriterCdc::toast_unexpected_invalidate(core, relation_oid, "delete");
        return None;
    }
    Some(FrameRowEvent::Delete {
        relation_oid,
        row_data,
    })
}

fn truncate_command_event(core: &mut WriterCore, relation_oids: Vec<Oid>) -> Option<FrameRowEvent> {
    core.frame_relation_oids
        .extend(relation_oids.iter().copied());
    if core.frame_state == FrameState::Recovering {
        return None;
    }
    // Toast-repair guarding happens at the event's replay position
    // (PGC-264): pre-truncate events may still trust the pre-batch image.
    Some(FrameRowEvent::Truncate { relation_oids })
}

impl WriterCdc {
    /// Handle a CDC command, dispatching to the appropriate method.
    pub(crate) async fn cdc_command_handle(
        &mut self,
        core: &mut WriterCore,
        cmd: CdcCommand,
        queued: usize,
    ) -> CacheResult<()> {
        let cmd_handle = cdc_command_histogram(&cmd);
        let handle_start = Instant::now();
        let row_event = match cmd {
            CdcCommand::Begin { xid } => {
                frame_begin_handle(core, xid);
                None
            }
            CdcCommand::TableRegister(table_metadata) => {
                self.table_register_handle(core, table_metadata).await?;
                None
            }
            CdcCommand::Insert {
                relation_oid,
                row_data,
            } => {
                self.insert_command_event(core, relation_oid, row_data)
                    .await?
            }
            CdcCommand::Update {
                relation_oid,
                key_data,
                row_data,
            } => update_command_event(core, relation_oid, key_data, row_data),
            CdcCommand::Delete {
                relation_oid,
                row_data,
            } => delete_command_event(core, relation_oid, row_data),
            CdcCommand::Truncate { relation_oids } => truncate_command_event(core, relation_oids),
            CdcCommand::CommitMark { lsn } => {
                self.commit_mark_handle(core, lsn, queued).await?;
                None
            }
            CdcCommand::KeepAliveMark { lsn } => {
                self.keepalive_mark_handle(core, lsn).await?;
                None
            }
        };
        if let Some(event) = row_event {
            self.frame_row_push(core, event).await?;
        }
        // Self-defers while the frame is open; flushes here at CommitMark
        // (frame just committed) and KeepAlive (no frame).
        core.publication_dirty_drain().await?;
        cmd_handle.record(handle_start.elapsed().as_secs_f64());
        Ok(())
    }

    async fn insert_command_event(
        &mut self,
        core: &mut WriterCore,
        relation_oid: Oid,
        row_data: Vec<CdcValue>,
    ) -> CacheResult<Option<FrameRowEvent>> {
        if !frame_relation_accept(core, relation_oid) {
            return Ok(None);
        }
        if fault_cdc_deadlock_should_inject(core) {
            // Behave exactly as a real 40P01 victim (PGC-147).
            self.frame_recover_enter(core)
                .await
                .attach_loc("fault: injected cdc deadlock")?;
            return Ok(None);
        }
        let (row_data, toasted) = core.row_convert(row_data);
        // pgoutput never elides toast from INSERT images; dropping the event
        // keeps the NULL-holed row out of the shared table (handle_insert's
        // tracked-key upsert would write it, and merges never overwrite).
        if !toasted.is_empty() {
            Self::toast_unexpected_invalidate(core, relation_oid, "insert");
            return Ok(None);
        }
        Ok(Some(FrameRowEvent::Insert {
            relation_oid,
            row_data,
        }))
    }

    /// Append a row event to the frame log, replaying early if it is full.
    async fn frame_row_push(
        &mut self,
        core: &mut WriterCore,
        event: FrameRowEvent,
    ) -> CacheResult<()> {
        core.frame_rows.push(event);
        core.batch_events += 1;
        self.frame_rows_replay_if_full(core).await
    }

    async fn keepalive_mark_handle(&mut self, core: &mut WriterCore, lsn: Lsn) -> CacheResult<()> {
        // Keepalives only arrive between source transactions, so no frame may
        // be open. The guard keeps the watermark from advancing past an open
        // frame if that ever breaks.
        debug_assert!(
            !core.frame_open,
            "keepalive received with an open source-transaction frame"
        );
        if core.frame_open {
            return Ok(());
        }
        // The keepalive LSN is past every accumulated frame: flush first so
        // the watermark never claims unapplied events (PGC-242).
        if core.batch_frames > 0 {
            let up_to = core.batch_last_lsn;
            self.batch_flush(core, up_to).await?;
        }
        self.received_lsn_advance(lsn);
        core.last_received_lsn = self.last_received_lsn;
        // The writer is drained (no open frame, batch flushed) and a
        // keepalive's LSN bounds the walsender's *sent* position — every
        // decodable commit at or below it was already emitted in stream
        // order, so everything it will ever deliver up to `lsn` is applied.
        // Publish it so read-after-write logs can clear across non-decodable
        // WAL (DDL, unpublished-table commits, idle origin), where the
        // commit-only `last_applied_lsn` cannot advance (PGC-124).
        self.settled_lsn.fetch_max(lsn.get(), Ordering::Relaxed);
        Ok(())
    }

    pub(super) async fn table_register_handle(
        &mut self,
        core: &mut WriterCore,
        mut table_metadata: TableMetadata,
    ) -> CacheResult<()> {
        // Must precede the schema_eq below: the decoder's TEXT fallback for
        // origin-only types would otherwise read as a schema change on
        // every Relation message (PGC-266).
        core.table_metadata_types_resolve(&mut table_metadata).await;
        core.frame_relation_oids.insert(table_metadata.relation_oid);
        let relation_oid = table_metadata.relation_oid;
        // A mid-frame Relation message whose metadata CHANGED (intra-txn
        // DDL): the relation's buffered events were captured under the
        // old column layout, so replaying them after the recreate
        // misaligns position-based lookups and references dropped
        // columns. (An identical re-sent Relation — e.g. after a
        // publication change — leaves everything in place.)
        let metadata_changed = core
            .cache
            .tables
            .get1(&relation_oid)
            .is_none_or(|current| !current.schema_eq(&table_metadata));
        if metadata_changed {
            if core.frame_buf_relations.contains(&relation_oid) {
                // The relation's cache-table writes (naming the old
                // columns) are already committed to `frame_buf` or
                // executed in the open cache txn — a partial replay
                // moved them out of `frame_rows`, so discarding events
                // can't retract them, and at COMMIT they would run
                // against the recreated table and fail. Escalate to
                // frame recovery: roll the cache txn back and let
                // CommitMark invalidate + repopulate every relation the
                // frame touched from post-DDL origin (PGC-264).
                self.frame_recover_enter(core)
                    .await
                    .attach_loc("mid-frame DDL on a buffered relation")?;
            } else {
                // No buffered writes yet: the relation's events are all
                // still in `frame_rows`. Discard them and purge its
                // toast overlay (a different relation's partial replay
                // may have recorded a stale entry under the old layout);
                // the recreate evicts its queries and empties the table,
                // so it rebuilds from origin (PGC-264).
                core.toast_overlay_relation_invalidate(relation_oid);
                let before = core.frame_rows.len();
                core.frame_rows.retain(|event| match event {
                    FrameRowEvent::Insert {
                        relation_oid: r, ..
                    }
                    | FrameRowEvent::Update {
                        relation_oid: r, ..
                    }
                    | FrameRowEvent::Delete {
                        relation_oid: r, ..
                    } => *r != relation_oid,
                    FrameRowEvent::Truncate { .. } | FrameRowEvent::Boundary { .. } => true,
                });
                if core.frame_rows.len() != before {
                    core.batch_truncated_relations.push(relation_oid);
                }
            }
        }
        // Schema change: prepared eval SQL embeds the column list —
        // drop the relation's cached statements so the next use
        // re-prepares against the new shape.
        self.prepared_row_change.pop(&relation_oid);
        let stale: Vec<PreparedEvalKey> = self
            .prepared_membership
            .iter()
            .map(|(key, _)| *key)
            .filter(|key| key.relation_oid == relation_oid)
            .collect();
        for key in stale {
            self.prepared_membership.pop(&key);
        }
        core.cache_table_register(table_metadata)
            .await
            .attach_loc("cdc table register")?;
        Ok(())
    }

    pub(super) async fn commit_mark_handle(
        &mut self,
        core: &mut WriterCore,
        lsn: Lsn,
        queued: usize,
    ) -> CacheResult<()> {
        // The frame's commit boundary rides in the event log (PGC-242):
        // deleted keys and truncate watermarks are produced *during
        // replay* (`frame_cache_delete` runs in the decide pass), so
        // the per-frame LSN context must travel with the events for
        // logs that span multiple frames.
        core.frame_rows
            .push(FrameRowEvent::Boundary { commit_lsn: lsn });
        core.batch_frames += 1;
        core.batch_last_lsn = lsn;
        core.frame_open = false;

        // Flush decision (PGC-242): an empty queue flushes immediately
        // (caught up — today's per-frame behavior, zero added
        // latency); a backlog accumulates, amortizing eval and commit
        // round-trips over the frames that would otherwise wait in the
        // queue anyway. `Recovering` flushes now (recovery semantics
        // are batch-terminal), and the size caps bound memory, the
        // memo-bracket window, and the recovery blast radius — they
        // override a fault-injected hold; the queue-empty trigger
        // respects it.
        let flush = core.frame_state == FrameState::Recovering
            || core.batch_events >= FRAME_ROWS_CAPACITY
            || core.batch_frames >= BATCH_FRAMES_MAX
            || (queued == 0 && !fault_cdc_hold_flush(core.batch_frames));
        if flush {
            self.batch_flush(core, lsn).await?;
        }
        Ok(())
    }
}

impl WriterCdc {
    pub(crate) async fn new(settings: &Settings, settled_lsn: Arc<AtomicU64>) -> CacheResult<Self> {
        let cache_eval_conn = pg::connect(&settings.cache, "cache eval")
            .await
            .map_into_report::<CacheError>()?;

        let cdc_write_conn = pg::connect(&settings.cache, "cdc write")
            .await
            .map_into_report::<CacheError>()?;

        #[cfg(feature = "fault-injection")]
        fault::init();

        Ok(Self {
            cache_eval_conn,
            cdc_write_conn,
            last_received_lsn: Lsn::from_raw(0),
            last_applied_lsn: Lsn::from_raw(0),
            settled_lsn,
            pg_eval_buf: String::with_capacity(SQL_BUFFER_CAPACITY),
            prepared_membership: LruCache::new(PREPARED_EVAL_CACHE_CAPACITY),
            prepared_row_change: LruCache::new(PREPARED_EVAL_CACHE_CAPACITY),
        })
    }
}

/// Test-only deterministic fault injection (PGC-147). Compiled out entirely
/// unless built with `--features fault-injection`; the writer-side CDC `40P01`
/// is a timing race that cannot be provoked probabilistically, so the recovery
/// path is exercised by forcing it here.
#[cfg(feature = "fault-injection")]
mod fault {
    use std::sync::atomic::{AtomicBool, Ordering};

    static CDC_DEADLOCK_ONCE: AtomicBool = AtomicBool::new(false);

    /// Minimum batch size before the queue-empty flush trigger fires
    /// (`PGCACHE_FAULT_CDC_HOLD_FLUSH_FRAMES`), read once.
    pub(super) fn hold_flush_frames() -> Option<usize> {
        use std::sync::OnceLock;
        static HOLD: OnceLock<Option<usize>> = OnceLock::new();
        *HOLD.get_or_init(|| {
            std::env::var("PGCACHE_FAULT_CDC_HOLD_FLUSH_FRAMES")
                .ok()
                .and_then(|s| s.parse().ok())
                .filter(|n| *n > 0)
        })
    }

    /// Arm the one-shot from the environment (read once at writer startup).
    pub(super) fn init() {
        if std::env::var_os("PGCACHE_FAULT_CDC_DEADLOCK_ONCE").is_some() {
            CDC_DEADLOCK_ONCE.store(true, Ordering::Relaxed);
        }
    }

    /// True exactly once if armed — consumes the one-shot.
    pub(super) fn cdc_deadlock_take() -> bool {
        CDC_DEADLOCK_ONCE.swap(false, Ordering::Relaxed)
    }
}

/// Whether to simulate a CDC-frame `40P01` for the current insert. Always
/// `false` (and `core` untouched) unless built with `fault-injection`. The
/// one-shot is consumed only once a query is cached, so fixture-load inserts
/// (which precede any cached query) don't trip it — the injected deadlock
/// lands on a frame that actually has a relation to recover.
#[cfg(feature = "fault-injection")]
fn fault_cdc_deadlock_should_inject(core: &WriterCore) -> bool {
    core.cache.cached_queries.iter().next().is_some() && fault::cdc_deadlock_take()
}

#[cfg(not(feature = "fault-injection"))]
fn fault_cdc_deadlock_should_inject(_core: &WriterCore) -> bool {
    false
}

/// Test-only flush hold (PGC-242): with `PGCACHE_FAULT_CDC_HOLD_FLUSH_FRAMES=N`
/// the queue-empty trigger is suppressed until N frames have accumulated, so
/// tests can provoke deterministic multi-frame batches without real queue
/// pressure. Size caps and `Recovering` still force a flush.
#[cfg(feature = "fault-injection")]
fn fault_cdc_hold_flush(batch_frames: usize) -> bool {
    fault::hold_flush_frames().is_some_and(|n| batch_frames < n)
}

#[cfg(not(feature = "fault-injection"))]
fn fault_cdc_hold_flush(_batch_frames: usize) -> bool {
    false
}
