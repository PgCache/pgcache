use crate::catalog::Oid;
use crate::query::{Fingerprint, FingerprintSet};
use std::collections::HashMap;
use std::time::Instant;

use ecow::EcoString;
use tracing::{error, instrument, trace};

use lru::LruCache;

use crate::pg;
use crate::pg::protocol::ByteString;
use crate::settings::Settings;

use super::super::super::messages::CdcCommand;
use super::super::super::update_query::{UpdateEvalStrategy, UpdateQuery};
use super::super::super::{CacheError, CacheResult, MapIntoReport, ReportExt};
use super::super::core::WriterCore;
use super::super::frame::{FRAME_ROWS_CAPACITY, FrameRowEvent, FrameState};
use super::super::staging::pk_body_render;

use super::*;
use crate::catalog::TableMetadata;
use crate::pg::Lsn;

impl WriterCdc {
    /// Handle a CDC command, dispatching to the appropriate method.
    pub async fn cdc_command_handle(
        &mut self,
        core: &mut WriterCore,
        cmd: CdcCommand,
        queued: usize,
    ) -> CacheResult<()> {
        let m = crate::metrics::handles();
        let cmd_handle = match &cmd {
            CdcCommand::Begin { .. } => &m.cdc.cmd_begin,
            CdcCommand::TableRegister(_) => &m.cdc.cmd_table_register,
            CdcCommand::Insert { .. } => &m.cdc.cmd_insert,
            CdcCommand::Update { .. } => &m.cdc.cmd_update,
            CdcCommand::Delete { .. } => &m.cdc.cmd_delete,
            CdcCommand::Truncate { .. } => &m.cdc.cmd_truncate,
            CdcCommand::CommitMark { .. } => &m.cdc.cmd_commit_mark,
            CdcCommand::KeepAliveMark { .. } => &m.cdc.cmd_keepalive_mark,
        };
        let handle_start = Instant::now();
        // Relation OIDs are recorded from frame start so a mid-frame 40P01 can
        // recover every relation the frame touched (pre-deadlock writes rolled
        // back too).
        match cmd {
            CdcCommand::Begin { xid } => {
                debug_assert!(
                    !core.frame_open,
                    "Begin within an open source-transaction frame"
                );
                trace!(xid, "cdc frame begin");
                core.frame_open = true;
                // Reset only at a fresh batch — when accumulating (PGC-242)
                // the log, buffers, and pending bookkeeping span frames.
                if core.frame_state == FrameState::Idle {
                    core.frame_state = FrameState::Active;
                    core.frame_buf.clear();
                    core.frame_buf_relations.clear();
                    core.frame_rows.clear();
                    core.frame_chunk_flushed = false;
                    core.frame_deleted_keys.clear();
                    core.frame_truncated_relations.clear();
                    core.batch_deleted_pks.clear();
                    core.toast_overlay_reset();
                    core.batch_toast_guard_oids.clear();
                }
            }
            CdcCommand::TableRegister(table_metadata) => {
                self.table_register_handle(core, table_metadata).await?;
            }
            CdcCommand::Insert {
                relation_oid,
                row_data,
            } => {
                core.frame_relation_oids.insert(relation_oid);
                if core.frame_state != FrameState::Recovering {
                    if fault_cdc_deadlock_should_inject(core) {
                        // Behave exactly as a real 40P01 victim (PGC-147).
                        self.frame_recover_enter(core)
                            .await
                            .attach_loc("fault: injected cdc deadlock")?;
                    } else {
                        let (row_data, toasted) = core.row_convert(row_data);
                        // pgoutput never elides toast from INSERT images;
                        // dropping the event keeps the NULL-holed row out of
                        // the shared table (handle_insert's tracked-key upsert
                        // would write it, and merges never overwrite).
                        if !toasted.is_empty() {
                            Self::toast_unexpected_invalidate(core, relation_oid, "insert");
                        } else {
                            core.frame_rows.push(FrameRowEvent::Insert {
                                relation_oid,
                                row_data,
                            });
                            core.batch_events += 1;
                            self.frame_rows_replay_if_full(core).await?;
                        }
                    }
                }
            }
            CdcCommand::Update {
                relation_oid,
                key_data,
                row_data,
            } => {
                core.frame_relation_oids.insert(relation_oid);
                if core.frame_state != FrameState::Recovering {
                    let (key_data, key_toasted) = core.row_convert(key_data);
                    let (new_row_data, toasted) = core.row_convert(row_data);
                    // Key tuples carry real values under every replica
                    // identity; a toasted one can't even key the old-PK delete.
                    if !key_toasted.is_empty() {
                        Self::toast_unexpected_invalidate(core, relation_oid, "update key tuple");
                    } else {
                        // Toasted images are resolved by the replay pre-pass
                        // (`toast_repair_events`) — batched there instead of a
                        // per-event lookup here.
                        let event = if toasted.is_empty() {
                            FrameRowEvent::Update {
                                relation_oid,
                                key_data,
                                new_row_data,
                            }
                        } else {
                            FrameRowEvent::UpdateToasted {
                                relation_oid,
                                key_data,
                                new_row_data,
                                toasted,
                            }
                        };
                        core.frame_rows.push(event);
                        core.batch_events += 1;
                        self.frame_rows_replay_if_full(core).await?;
                    }
                }
            }
            CdcCommand::Delete {
                relation_oid,
                row_data,
            } => {
                core.frame_relation_oids.insert(relation_oid);
                if core.frame_state != FrameState::Recovering {
                    let (row_data, toasted) = core.row_convert(row_data);
                    // Delete images are key/old tuples — same reasoning as the
                    // update key tuple above.
                    if !toasted.is_empty() {
                        Self::toast_unexpected_invalidate(core, relation_oid, "delete");
                    } else {
                        core.frame_rows.push(FrameRowEvent::Delete {
                            relation_oid,
                            row_data,
                        });
                        core.batch_events += 1;
                        self.frame_rows_replay_if_full(core).await?;
                    }
                }
            }
            CdcCommand::Truncate { relation_oids } => {
                core.frame_relation_oids
                    .extend(relation_oids.iter().copied());
                if core.frame_state != FrameState::Recovering {
                    // Toast-repair guarding happens at the event's replay
                    // position (PGC-264): pre-truncate events may still trust
                    // the pre-batch image.
                    core.frame_rows
                        .push(FrameRowEvent::Truncate { relation_oids });
                    core.batch_events += 1;
                    self.frame_rows_replay_if_full(core).await?;
                }
            }
            CdcCommand::CommitMark { lsn } => {
                self.commit_mark_handle(core, lsn, queued).await?;
            }
            CdcCommand::KeepAliveMark { lsn } => {
                // Keepalives only arrive between source transactions, so no
                // frame may be open. The guard keeps the watermark from
                // advancing past an open frame if that ever breaks.
                debug_assert!(
                    !core.frame_open,
                    "keepalive received with an open source-transaction frame"
                );
                if !core.frame_open {
                    // The keepalive LSN is past every accumulated frame:
                    // flush first so the watermark never claims unapplied
                    // events (PGC-242).
                    if core.batch_frames > 0 {
                        let up_to = core.batch_last_lsn;
                        self.batch_flush(core, up_to).await?;
                    }
                    self.applied_lsn_advance(lsn);
                    core.last_applied_lsn = self.last_applied_lsn;
                }
            }
        }
        // Self-defers while the frame is open; flushes here at CommitMark
        // (frame just committed) and KeepAlive (no frame).
        core.publication_dirty_drain().await?;
        cmd_handle.record(handle_start.elapsed().as_secs_f64());
        Ok(())
    }

    pub(super) async fn table_register_handle(
        &mut self,
        core: &mut WriterCore,
        table_metadata: TableMetadata,
    ) -> CacheResult<()> {
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
                    | FrameRowEvent::UpdateToasted {
                        relation_oid: r, ..
                    }
                    | FrameRowEvent::UpdateToastFallback {
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

    /// Buffer an unconditional upsert of `row_data` into the relation's cache
    /// table in the open frame (PGC-228), opening the frame txn if needed.
    pub(super) async fn frame_cache_upsert(
        &mut self,
        core: &mut WriterCore,
        relation_oid: Oid,
        row_data: &[Option<ByteString>],
    ) -> CacheResult<()> {
        core.frame_begin_ensure([relation_oid]);
        let table_metadata =
            core.cache
                .tables
                .get1(&relation_oid)
                .ok_or(CacheError::UnknownTable {
                    oid: Some(relation_oid),
                    name: None,
                })?;
        // A re-upserted PK is present again for later batched frames' row-
        // change classification (PGC-242). Gated: rendering is free when no
        // batch deletes are outstanding.
        if !core.batch_deleted_pks.is_empty()
            && let Some(key) = pk_body_render(table_metadata, row_data)
        {
            core.batch_deleted_pks.remove(&(relation_oid, key));
        }
        self.cache_upsert_unconditional_into(&mut core.frame_buf, table_metadata, row_data);
        self.frame_write_finish(core).await
    }

    /// Buffer a PK-qualified delete of `row_data` from the relation's cache
    /// table into the open frame (PGC-228), opening the frame txn if needed.
    /// The relation is known-present by the time a handler reaches a delete, so
    /// a missing entry is a hard `UnknownTable` error rather than a skip.
    async fn frame_cache_delete(
        &mut self,
        core: &mut WriterCore,
        relation_oid: Oid,
        row_data: &[Option<ByteString>],
    ) -> CacheResult<()> {
        self.frame_cache_delete_inner(core, relation_oid, row_data, true)
            .await
    }

    /// `frame_cache_delete` without the PGC-250 lost-key record: for evicting
    /// a row that is still alive at origin while every query over the relation
    /// is being invalidated (toast fallback under active tracking, PGC-264).
    /// Recording the key would make later populations' merges omit the live
    /// row (PGC-261); the invalidations supersede the in-flight populations
    /// (generation bump), so nothing can resurrect the evicted stale version.
    async fn frame_cache_delete_unrecorded(
        &mut self,
        core: &mut WriterCore,
        relation_oid: Oid,
        row_data: &[Option<ByteString>],
    ) -> CacheResult<()> {
        self.frame_cache_delete_inner(core, relation_oid, row_data, false)
            .await
    }

    async fn frame_cache_delete_inner(
        &mut self,
        core: &mut WriterCore,
        relation_oid: Oid,
        row_data: &[Option<ByteString>],
        record_lost_key: bool,
    ) -> CacheResult<()> {
        core.frame_begin_ensure([relation_oid]);
        let table_metadata =
            core.cache
                .tables
                .get1(&relation_oid)
                .ok_or(CacheError::UnknownTable {
                    oid: Some(relation_oid),
                    name: None,
                })?;
        // Buffer the removed PK for any in-flight population over this relation
        // so its merge doesn't resurrect the row (PGC-250). Stamped with the
        // frame's commit LSN and recorded at CommitMark (the commit LSN isn't
        // known yet); dropped if the frame rolls back. Skip rendering the key
        // entirely when no population is recording this relation (the steady
        // state) — `record` would discard it anyway.
        let deleted_key =
            if record_lost_key && core.population_deleted_keys.is_recording(relation_oid) {
                pk_body_render(table_metadata, row_data)
            } else {
                None
            };
        // Track batch-deleted PKs so a later batched frame's row-change
        // classification sees the deletion the pre-batch snapshot can't
        // (PGC-242). Re-rendered when not already rendered for PGC-250.
        if let Some(key) = deleted_key
            .clone()
            .or_else(|| pk_body_render(table_metadata, row_data))
        {
            core.batch_deleted_pks.insert((relation_oid, key));
        }
        self.cache_delete_into(&mut core.frame_buf, table_metadata, row_data)?;
        if let Some(key) = deleted_key {
            core.frame_deleted_keys.push((relation_oid, key));
        }
        self.frame_write_finish(core).await
    }

    /// Handle INSERT operation.
    // Trace level: at info/debug the fmt layer allocates per-span extensions,
    // which would put a heap allocation on every CDC event.
    #[instrument(skip_all, level = "trace")]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(in crate::cache::writer::cdc) async fn handle_insert(
        &mut self,
        core: &mut WriterCore,
        relation_oid: Oid,
        row_data: &[Option<ByteString>],
        batch: Option<BatchEvalView<'_>>,
    ) -> CacheResult<()> {
        let start = Instant::now();
        crate::metrics::handles().cdc.handle_inserts.increment(1);

        // CDC event for a relation we don't cache (never cached, or its
        // queries were evicted) is a benign no-op — not a frame-consistency
        // failure. Skip without erroring so it doesn't trip the reset path.
        if !core.cache.tables.contains_key1(&relation_oid) {
            return Ok(());
        }

        let fp_list = self
            .update_queries_check_invalidate(
                core,
                relation_oid,
                None,
                row_data,
                None,
                CdcOperation::Upsert,
            )
            .attach_loc("checking for query invalidations")?;

        // Defer the actual invalidation to just before the frame COMMIT
        // (frame_invalidations_flush) so it is atomic with the maintenance
        // it accompanies rather than visible mid-frame.
        core.frame_invalidations.extend(fp_list);

        // Probe the LocalEval index once; both the in-place matcher and the memo
        // eviction pass below consume the same candidate set.
        let local_candidates = eval_candidates(core, relation_oid, row_data);
        let matched = self
            .update_queries_execute_batch(core, relation_oid, row_data, batch, &local_candidates)
            .await?;

        // Rung 3b: evict memos this insert grows into (predicate-matched).
        memo_frame_accumulate(
            core,
            relation_oid,
            MemoOp::Insert,
            row_data,
            None,
            &local_candidates,
        );

        // The inserted row is alive at origin: cancel any tracked deletion of
        // its key so population merges don't omit it (PGC-260). When the key
        // was tracked but no query matched (nothing upserted), write the row
        // anyway — its presence in the shared table is what makes the
        // cancellation safe in every merge interleaving (merges never
        // overwrite, so neither an old-snapshot nor a new-snapshot population
        // can regress it).
        let tracked = core.population_deleted_key_cancel(relation_oid, row_data);
        if tracked && !matched {
            self.frame_cache_upsert(core, relation_oid, row_data)
                .await?;
        }

        crate::metrics::handles()
            .cdc
            .handle_insert_seconds
            .record(start.elapsed().as_secs_f64());
        Ok(())
    }

    /// Handle UPDATE operation.
    // Trace level: at info/debug the fmt layer allocates per-span extensions,
    // which would put a heap allocation on every CDC event.
    #[instrument(skip_all, level = "trace")]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(in crate::cache::writer::cdc) async fn handle_update(
        &mut self,
        core: &mut WriterCore,
        relation_oid: Oid,
        key_data: &[Option<ByteString>],
        new_row_data: &[Option<ByteString>],
        batch: Option<BatchEvalView<'_>>,
    ) -> CacheResult<()> {
        let start = Instant::now();
        crate::metrics::handles().cdc.handle_updates.increment(1);

        // See handle_insert: an untracked relation's CDC is a benign skip.
        if !core.cache.tables.contains_key1(&relation_oid) {
            return Ok(());
        }

        // Probe the LocalEval index once; the memo eviction pass and the in-place
        // matcher (`update_queries_execute_batch` below) share this candidate set.
        let local_candidates = eval_candidates(core, relation_oid, new_row_data);

        // PGC-227: when no cached query over this relation can have its UPDATE
        // invalidation depend on which columns changed or whether the row is
        // cached, both `query_row_changes` (a SELECT round-trip) and the
        // invalidation check are provably no-ops — skip them.
        let needs_change_eval = core
            .cache
            .update_queries
            .get(&relation_oid)
            .is_some_and(|q| q.needs_change_eval());

        if needs_change_eval {
            // The batch deleted this PK after the pre-batch snapshot that the
            // row-change lookups read: classify it UNCACHED so the
            // entering-invalidation the per-frame flow produced still fires
            // (PGC-242; lost otherwise on cross-frame delete/PK-flip + update).
            let batch_deleted = !core.batch_deleted_pks.is_empty()
                && core
                    .cache
                    .tables
                    .get1(&relation_oid)
                    .and_then(|table_metadata| pk_body_render(table_metadata, new_row_data))
                    .is_some_and(|key| core.batch_deleted_pks.contains(&(relation_oid, key)));
            // Batched row-change result if the segment eval covered this event
            // (PGC-241 stage 3), else the per-row SELECT.
            let row_changes_fallback;
            let row_changes: Option<&HashMap<EcoString, bool>> = if batch_deleted {
                None
            } else {
                match batch.as_ref().and_then(|view| view.row_change) {
                    Some(batched) => batched,
                    None => {
                        row_changes_fallback = self
                            .query_row_changes(core, relation_oid, new_row_data)
                            .await?;
                        row_changes_fallback.as_ref()
                    }
                }
            };
            trace!("row_changes {:?}", row_changes);

            let fp_list = self.update_queries_check_invalidate(
                core,
                relation_oid,
                row_changes,
                new_row_data,
                Some(key_data),
                CdcOperation::Upsert,
            )?;
            trace!("invalidation_count {}", fp_list.len());
            // Deferred to frame_invalidations_flush (see handle_insert).
            core.frame_invalidations.extend(fp_list);
            // Rung 3b: predicate-matched memo eviction with the changed-column
            // set available (precise grow / flip-out / projected-value change).
            memo_frame_accumulate(
                core,
                relation_oid,
                MemoOp::Update,
                new_row_data,
                row_changes,
                &local_candidates,
            );
        } else {
            // No `row_changes` computed (PGC-227 skip): a membership flip-out is
            // undetectable, so memo eviction is conservative for this relation.
            memo_frame_accumulate(
                core,
                relation_oid,
                MemoOp::Update,
                new_row_data,
                None,
                &local_candidates,
            );
        }

        let matched = self
            .update_queries_execute_batch(
                core,
                relation_oid,
                new_row_data,
                batch,
                &local_candidates,
            )
            .await?;

        if matched {
            // The upserted row supersedes any tracked deletion of its key —
            // including a previously-deleted PK this row's new PK reuses
            // (PGC-260).
            core.population_deleted_key_cancel(relation_oid, new_row_data);
        } else {
            // Update-out: the row left every live predicate, but it is still
            // alive at origin. While populations are in flight, deleting it
            // and recording its key would make a later population's merge omit
            // a live row (PGC-261) — instead upsert the new version: serving
            // re-evaluates predicates so nothing serves it, an old-snapshot
            // merge can't resurrect the old version (merges never overwrite),
            // and a later population finds it present. With no population in
            // flight, keep the delete (shared-table leanness; the key record
            // would be discarded anyway).
            if core.population_deleted_keys.is_recording(relation_oid) {
                core.population_deleted_key_cancel(relation_oid, new_row_data);
                self.frame_cache_upsert(core, relation_oid, new_row_data)
                    .await?;
            } else {
                self.frame_cache_delete(core, relation_oid, new_row_data)
                    .await?;
            }
        }

        // Any update may move the row out of a Fresh MV's predicate, and only
        // membership *hits* dirty-mark — a row leaving query A while still
        // matching query B (`matched` above), or a PK-change with other columns
        // changed, would otherwise leave A's MV serving the departed row forever
        // (PGC-254/PGC-265; the old image isn't available to detect departure
        // precisely — PGC-255 tracks precision). Probe the eval index on the old
        // PK (PK-only: the old non-PK values are gone, so any non-PK predicate
        // matches conservatively via the `Unknown` wildcard) and dirty-mark the
        // candidates; `mv_dirty_mark` self-gates (Fresh and Building only).
        core.mv_dirty_mark_removed_row(
            relation_oid,
            if key_data.is_empty() {
                new_row_data
            } else {
                key_data
            },
            true,
        );

        // A non-empty `key_data` means the PK changed; delete the old PK too.
        if !key_data.is_empty() {
            self.frame_cache_delete(core, relation_oid, key_data)
                .await?;
        }

        crate::metrics::handles()
            .cdc
            .handle_update_seconds
            .record(start.elapsed().as_secs_f64());
        Ok(())
    }

    /// Conservative decide-pass path for an UPDATE whose unchanged-toast
    /// columns could not be repaired (PGC-264). The image is incomplete: it
    /// must never reach the shared cache table, and predicates over the elided
    /// columns can't be evaluated.
    ///
    /// With no population recording the relation: invalidate every query that
    /// might be affected (structural sensitivity, or membership match — the
    /// matched row can't be upserted); provably unaffected queries need
    /// nothing beyond the row's eviction.
    ///
    /// With recording active, the PGC-261 hazard applies: the row is alive at
    /// origin, so deleting it with a lost-key record makes later populations'
    /// merges omit it — including merges for queries registered *after* this
    /// event, which no invalidation here can cover. Instead invalidate every
    /// query over the relation (superseding their in-flight populations via
    /// the generation bump) and evict the stale row without a record; later
    /// populations snapshot post-update origin and merge cleanly.
    // Trace level: at info/debug the fmt layer allocates per-span extensions,
    // which would put a heap allocation on every CDC event.
    #[instrument(skip_all, level = "trace")]
    pub(super) async fn handle_update_toast_fallback(
        &mut self,
        core: &mut WriterCore,
        relation_oid: Oid,
        key_data: &[Option<ByteString>],
        new_row_data: &[Option<ByteString>],
        toasted_columns: &[EcoString],
    ) -> CacheResult<()> {
        // See handle_insert: an untracked relation's CDC is a benign skip.
        if !core.cache.tables.contains_key1(&relation_oid) {
            return Ok(());
        }

        let recording = core.population_deleted_keys.is_recording(relation_oid);
        let mut fp_list: Vec<Fingerprint> = Vec::new();
        if let (Some(update_queries), Some(table_metadata)) = (
            core.cache.update_queries.get(&relation_oid),
            core.cache.tables.get1(&relation_oid),
        ) {
            if recording {
                fp_list.extend(
                    update_queries
                        .queries
                        .values()
                        .map(|q| q.fingerprint)
                        .filter(|fp| !core.frame_invalidations.contains(fp)),
                );
            } else {
                let mut pg_eval: Vec<&UpdateQuery> = Vec::new();
                for update_query in update_queries.queries.values() {
                    if core.frame_invalidations.contains(&update_query.fingerprint) {
                        continue;
                    }
                    if Self::toast_fallback_structural_invalidate(
                        update_query,
                        table_metadata,
                        new_row_data,
                        key_data,
                        toasted_columns,
                    ) {
                        fp_list.push(update_query.fingerprint);
                        continue;
                    }
                    match update_query.eval_strategy {
                        UpdateEvalStrategy::LocalEval => {
                            if update_query_matches_locally(
                                update_query,
                                table_metadata,
                                new_row_data,
                            ) {
                                fp_list.push(update_query.fingerprint);
                            }
                        }
                        UpdateEvalStrategy::PgEval => pg_eval.push(update_query),
                    }
                }
                if !pg_eval.is_empty() {
                    let matched = self
                        .pg_eval_matches(&pg_eval, table_metadata, new_row_data)
                        .await
                        .attach_loc("toast fallback membership eval")?;
                    fp_list.extend(matched);
                }
            }
        }
        trace!(
            relation_oid = %relation_oid,
            recording,
            invalidations = fp_list.len(),
            "toast fallback handled"
        );
        // Deferred to frame_invalidations_flush (see handle_insert).
        core.frame_invalidations.extend(fp_list);

        // Same Fresh-MV rule as handle_update (PGC-254), narrowed via the
        // eval-index probe (PGC-292). Old non-PK values are gone, so PK-only.
        core.mv_dirty_mark_removed_row(
            relation_oid,
            if key_data.is_empty() {
                new_row_data
            } else {
                key_data
            },
            true,
        );

        // The new-PK row is alive at origin: under recording its eviction must
        // not be recorded (see doc comment). The old PK after a PK change is
        // genuinely dead at origin, so that delete records normally.
        if recording {
            self.frame_cache_delete_unrecorded(core, relation_oid, new_row_data)
                .await?;
        } else {
            self.frame_cache_delete(core, relation_oid, new_row_data)
                .await?;
        }
        if !key_data.is_empty() {
            self.frame_cache_delete(core, relation_oid, key_data)
                .await?;
        }
        Ok(())
    }

    /// Handle DELETE operation.
    ///
    /// Deletes the row from cache tables and checks for subquery invalidations.
    /// For Exclusion subquery tables (NOT IN, NOT EXISTS), a DELETE shrinks the
    /// exclusion set, which grows the outer result set — requiring invalidation.
    // Trace level: at info/debug the fmt layer allocates per-span extensions,
    // which would put a heap allocation on every CDC event.
    #[instrument(skip_all, level = "trace")]
    pub async fn handle_delete(
        &mut self,
        core: &mut WriterCore,
        relation_oid: Oid,
        row_data: &[Option<ByteString>],
    ) -> CacheResult<()> {
        let start = Instant::now();
        crate::metrics::handles().cdc.handle_deletes.increment(1);

        if !core.cache.tables.contains_key1(&relation_oid) {
            error!("No table metadata found for relation_oid: {}", relation_oid);
            crate::metrics::handles()
                .cdc
                .handle_delete_seconds
                .record(start.elapsed().as_secs_f64());
            return Ok(());
        }

        // Buffer the delete for the frame flush (PGC-228).
        self.frame_cache_delete(core, relation_oid, row_data)
            .await?;

        // Rung 3b: the delete's old image is PK-only, so a non-PK WHERE can't be
        // probed — conservatively evict every memo over the relation.
        // Delete eviction is conservative (no candidate probe needed).
        memo_frame_accumulate(
            core,
            relation_oid,
            MemoOp::Delete,
            row_data,
            None,
            &FingerprintSet::default(),
        );

        // A deleted row leaves stale rows in any Fresh MV that materialized it;
        // CDC removals never went through the upsert path's dirty-mark, so the
        // MV would serve the deleted row forever. Probe the eval index on the
        // delete tuple (PGC-292): it's the genuine old image, so `pk_only=false`
        // uses whatever columns it carries — exact under REPLICA IDENTITY FULL,
        // PK-only (non-PK absent → `Unknown` wildcard) under DEFAULT (PGC-254).
        core.mv_dirty_mark_removed_row(relation_oid, row_data, false);

        // Check for subquery invalidations — removing a row can expand the
        // final result set for Exclusion/Scalar subquery tables
        if core.cache.update_queries.contains_key(&relation_oid) {
            let fp_list = self
                .update_queries_check_invalidate(
                    core,
                    relation_oid,
                    None,
                    row_data,
                    None,
                    CdcOperation::Delete,
                )
                .attach_loc("checking delete invalidations")?;

            // Deferred to frame_invalidations_flush (see handle_insert).
            core.frame_invalidations.extend(fp_list);
        }

        crate::metrics::handles()
            .cdc
            .handle_delete_seconds
            .record(start.elapsed().as_secs_f64());
        Ok(())
    }

    /// Handle TRUNCATE operation.
    ///
    /// The physical `TRUNCATE` of the source tables' cache tables runs in-frame
    /// on `cdc_write_conn` (atomic with the rest of the source transaction).
    /// Additionally, every cached query referencing a truncated relation is
    /// invalidated: a table-wide empty can change derived/multi-table results
    /// in ways the in-place model can't track, so those queries repopulate
    /// from origin.
    #[instrument(skip_all)]
    pub async fn handle_truncate(
        &mut self,
        core: &mut WriterCore,
        relation_oids: &[Oid],
    ) -> CacheResult<()> {
        if let Some(sql) = Self::truncate_sql_build(core, relation_oids.iter().copied()) {
            core.frame_begin_ensure(relation_oids.iter().copied());
            core.frame_buf.push_str(&sql);
            self.frame_write_finish(core).await?;
        }

        for oid in relation_oids {
            core.cache_table_invalidate(*oid)
                .await
                .attach_loc("invalidating queries on truncate")?;
            // A population reading this relation with a pre-truncate snapshot
            // would resurrect truncated rows on merge. Raise its abort watermark
            // to the truncate's commit LSN at CommitMark (PGC-250).
            core.frame_truncated_relations.push(*oid);
        }

        Ok(())
    }
}

impl WriterCdc {
    pub async fn new(settings: &Settings) -> CacheResult<Self> {
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
            last_applied_lsn: Lsn::from_raw(0),
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
