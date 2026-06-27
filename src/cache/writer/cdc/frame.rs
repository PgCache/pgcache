use crate::catalog::Oid;
use crate::pg::Lsn;
use crate::query::Fingerprint;

use ecow::EcoString;
use tracing::{error, info};

use super::super::super::memo::SlotKey;
use super::super::super::{CacheError, CacheResult, MapIntoReport, ReportExt};
use super::super::core::WriterCore;
use super::super::deadlock::{SQLSTATE_DEADLOCK, cache_error_sqlstate};
use super::super::frame::{FRAME_BUF_CAPACITY, FRAME_ROWS_CAPACITY, FrameRowEvent, FrameState};

use super::*;

impl WriterCdc {
    /// Finish a buffered statement: append the separator, then chunk-flush if
    /// `frame_buf` has grown past `FRAME_BUF_CAPACITY` (bounds memory for large
    /// source transactions). The chunk goes out inside the open frame txn
    /// (`BEGIN` was buffered as the first write), which stays open server-side
    /// until the `COMMIT` at `frame_commit`. The flag is set before the send so
    /// `40P01` recovery knows a `BEGIN` reached the server.
    pub(super) async fn frame_write_finish(&mut self, core: &mut WriterCore) -> CacheResult<()> {
        core.frame_buf.push_str("; ");
        if core.frame_buf.len() >= FRAME_BUF_CAPACITY {
            core.frame_chunk_flushed = true;
            self.cdc_write_conn
                .batch_execute(&core.frame_buf)
                .await
                .map_into_report::<CacheError>()?;
            core.frame_buf.clear();
        }
        Ok(())
    }

    /// Flush the frame's buffered writes as a single `BEGIN; …; COMMIT`
    /// round-trip (PGC-228). A write-less frame stayed `Active` and flushes
    /// nothing (its invalidations flush separately at `CommitMark`). If chunks
    /// were already flushed, `frame_buf` holds only the tail + `COMMIT`.
    async fn frame_commit(&mut self, core: &mut WriterCore) -> CacheResult<()> {
        if core.frame_state == FrameState::TxnOpen {
            core.frame_buf.push_str("COMMIT");
            self.cdc_write_conn
                .batch_execute(&core.frame_buf)
                .await
                .map_into_report::<CacheError>()?;
            core.frame_buf.clear();
            core.frame_buf_relations.clear();
            core.frame_state = FrameState::Idle;
        }
        Ok(())
    }

    /// `40P01` aborted the frame: discard buffered writes, roll back the
    /// server-side txn if a chunk had already opened one, and enter `Recovering`
    /// (relation-level recovery happens at `CommitMark`, PGC-147).
    pub(super) async fn frame_recover_enter(&mut self, core: &mut WriterCore) -> CacheResult<()> {
        // A fully-buffered frame never sent anything (a failed `BEGIN; …; COMMIT`
        // batch auto-rolls-back), so only an already-flushed chunk leaves a live
        // aborted txn block that needs an explicit ROLLBACK.
        if core.frame_chunk_flushed {
            self.cdc_write_conn
                .batch_execute("ROLLBACK")
                .await
                .map_into_report::<CacheError>()?;
        }
        core.frame_buf.clear();
        core.frame_buf_relations.clear();
        // Unreplayed row events are dropped — relation-level recovery
        // (evict + truncate) supersedes whatever they would have applied.
        core.frame_rows.clear();
        // Rolled-back deletes/truncates must not affect population merges — the
        // rows may still exist (PGC-250). The recovery's own truncate re-adds the
        // affected relations in `frame_recover`.
        core.frame_deleted_keys.clear();
        core.frame_truncated_relations.clear();
        core.batch_deleted_pks.clear();
        core.toast_overlay_reset();
        core.batch_toast_guard_oids.clear();
        core.frame_state = FrameState::Recovering;
        Ok(())
    }

    /// Apply the frame's deferred invalidations just before `COMMIT` — atomic
    /// with the maintenance, past the last deadlock-retriable point (a bare
    /// `COMMIT` under READ COMMITTED can't `40P01`).
    async fn frame_invalidations_flush(&mut self, core: &mut WriterCore) -> CacheResult<()> {
        if core.frame_invalidations.is_empty() {
            return Ok(());
        }
        // Collected out: `cache_query_cdc_invalidate` needs `&mut core`, so we
        // can't hold a borrow of `core.frame_invalidations` across the loop.
        let fps: Vec<Fingerprint> = core.frame_invalidations.iter().copied().collect();
        // Count only fingerprints that actually transitioned (Ready→Invalidated
        // or FIFO-evicted), not every enqueue: the same standing-invalidated
        // query gets re-flagged each frame it keeps matching writes, and counting
        // those no-ops inflated the metric ~8x over real transitions.
        let mut count = 0u64;
        for fp in fps {
            if self
                .cache_query_cdc_invalidate(core, fp)
                .await
                .attach_loc("flushing deferred invalidation")?
            {
                count += 1;
            }
        }
        crate::metrics::handles().cdc.invalidations.increment(count);
        core.state_gauges_update();
        Ok(())
    }

    /// Recover an aborted (`40P01`) frame: evict every query over the affected
    /// relations, then truncate those cache tables in a dedicated txn — a
    /// skipped Delete/Truncate may have left rows origin no longer has, and
    /// the queries repopulate from origin anyway.
    async fn frame_recover(&mut self, core: &mut WriterCore) -> CacheResult<()> {
        // Collected out: `cache_table_invalidate` needs `&mut core`.
        let oids: Vec<Oid> = core.frame_relation_oids.iter().copied().collect();
        info!(
            relations = oids.len(),
            "cdc frame recovery: invalidating + truncating affected relations"
        );
        // Evict first so no query can read a mid-truncate cache table. Memos
        // need no separate handling: eviction removes the query from
        // `cached_queries`, so it is not Ready and its orphan memo is unreachable
        // (re-captured on re-register) — exactly as for a normal TRUNCATE.
        for oid in &oids {
            core.cache_table_invalidate(*oid)
                .await
                .attach_loc("recover: invalidating affected relation")?;
        }
        // Like an explicit TRUNCATE, abort in-flight populations over these
        // relations whose snapshot predates the recovery (PGC-250); stamped with
        // the commit LSN at CommitMark.
        core.batch_truncated_relations.extend(oids.iter().copied());
        if let Some(truncate) = Self::truncate_sql_build(core, oids.into_iter()) {
            self.cdc_write_conn
                .batch_execute(&format!("BEGIN; {truncate}; COMMIT"))
                .await
                .map_into_report::<CacheError>()
                .attach_loc("recover: truncating affected cache tables")?;
        }
        Ok(())
    }

    /// `40P01` from a DML handler → enter `Recovering` and swallow (PGC-147);
    /// any other error propagates (cache subsystem reset, as before).
    async fn frame_dml_result(
        &mut self,
        core: &mut WriterCore,
        r: CacheResult<()>,
    ) -> CacheResult<()> {
        let Err(e) = r else { return Ok(()) };
        if core.frame_state != FrameState::Recovering
            && cache_error_sqlstate(e.current_context()) == Some(SQLSTATE_DEADLOCK)
        {
            info!(
                relations = core.frame_relation_oids.len(),
                "cdc frame deadlocked (40P01); recovering affected relations"
            );
            self.frame_recover_enter(core).await?;
            return Ok(());
        }
        Err(e)
    }

    /// Replay the frame's buffered row events in arrival order (PGC-241:
    /// collect at arrival, evaluate + emit at the flush boundary). Arrival
    /// order is what makes the deferral pure: same-key sequences (an INSERT
    /// then DELETE of one PK) and TRUNCATE-vs-row interleavings emit exactly
    /// as per-arrival handling did.
    ///
    /// Events run in segments split at Truncate boundaries (a truncate evicts
    /// queries over its relations, changing the update-query set). Each
    /// segment's PgEval membership is batch-evaluated up front — one statement
    /// per relation per row/query chunk instead of per row — and the ordered
    /// decide/emit pass consumes the precomputed matrix. Handler errors route
    /// through `frame_dml_result` (`40P01` → `Recovering`); once `Recovering`,
    /// the remaining events are dropped, matching the per-arrival path where
    /// post-deadlock commands skip handling.
    async fn frame_rows_replay(&mut self, core: &mut WriterCore) -> CacheResult<()> {
        let mut events = std::mem::take(&mut core.frame_rows);
        // Resolve unchanged-toast images first (PGC-264): segment eval and
        // the decide pass below must only ever see complete row images.
        Self::toast_repair_events(core, &mut events).await;
        // Segments end at (and include) each Truncate: a truncate evicts
        // queries over its relations, changing the update-query set the batch
        // eval is built from, so eval never spans one. `base` keeps the global
        // event index — the identity the eval matrix is keyed by.
        let mut base = 0;
        'segments: for segment in
            events.split_inclusive(|e| matches!(e, FrameRowEvent::Truncate { .. }))
        {
            if core.frame_state == FrameState::Recovering {
                break;
            }
            let (rows, trailing_truncate) = match segment.split_last() {
                Some((FrameRowEvent::Truncate { relation_oids }, rows)) => {
                    (rows, Some(relation_oids))
                }
                _ => (segment, None),
            };

            // Batched membership + row-change for the segment's rows
            // (PGC-241), then the ordered decide/emit pass over the same
            // events.
            let membership = if rows.is_empty() {
                SegmentMembership::default()
            } else {
                match self.segment_eval(core, rows, base).await {
                    Ok(m) => m,
                    Err(e) => {
                        self.frame_dml_result(core, Err(e))
                            .await
                            .attach_loc("cdc segment batch eval")?;
                        // Swallowed 40P01 → Recovering; the loop exits above.
                        base += segment.len();
                        continue;
                    }
                }
            };

            for (offset, event) in rows.iter().enumerate() {
                if core.frame_state == FrameState::Recovering {
                    break 'segments;
                }
                let i = base + offset;
                let (r, loc) = match event {
                    FrameRowEvent::Insert {
                        relation_oid,
                        row_data,
                    } => (
                        self.handle_insert(
                            core,
                            *relation_oid,
                            row_data,
                            membership.view(*relation_oid, i),
                        )
                        .await,
                        "cdc replay insert",
                    ),
                    FrameRowEvent::Update {
                        relation_oid,
                        key_data,
                        new_row_data,
                    } => (
                        self.handle_update(
                            core,
                            *relation_oid,
                            key_data,
                            new_row_data,
                            membership.view(*relation_oid, i),
                        )
                        .await,
                        "cdc replay update",
                    ),
                    FrameRowEvent::UpdateToastFallback {
                        relation_oid,
                        key_data,
                        new_row_data,
                        toasted_columns,
                    } => (
                        self.handle_update_toast_fallback(
                            core,
                            *relation_oid,
                            key_data,
                            new_row_data,
                            toasted_columns,
                        )
                        .await,
                        "cdc replay update toast fallback",
                    ),
                    // Unreachable by construction (`toast_repair_events`
                    // resolved every one before the segment loop); degrade to
                    // the conservative fallback rather than panicking.
                    FrameRowEvent::UpdateToasted {
                        relation_oid,
                        key_data,
                        new_row_data,
                        toasted,
                    } => {
                        debug_assert!(false, "UpdateToasted survived the repair pre-pass");
                        error!(relation_oid = %relation_oid, "unrepaired toasted update at decide time");
                        let toasted_columns: Vec<EcoString> = core
                            .cache
                            .tables
                            .get1(relation_oid)
                            .map(|t| {
                                t.columns
                                    .iter()
                                    .filter(|c| toasted.contains(&c.index()))
                                    .map(|c| c.name.clone())
                                    .collect()
                            })
                            .unwrap_or_default();
                        (
                            self.handle_update_toast_fallback(
                                core,
                                *relation_oid,
                                key_data,
                                new_row_data,
                                &toasted_columns,
                            )
                            .await,
                            "cdc replay unrepaired toasted update",
                        )
                    }
                    FrameRowEvent::Delete {
                        relation_oid,
                        row_data,
                    } => (
                        self.handle_delete(core, *relation_oid, row_data).await,
                        "cdc replay delete",
                    ),
                    // Unreachable by construction (`split_last` separated the
                    // trailing Truncate); a no-op keeps this panic-free.
                    FrameRowEvent::Truncate { .. } => (Ok(()), "cdc replay truncate"),
                    // Frame commit boundary (PGC-242): stamp the bookkeeping
                    // this frame's replay produced with its commit LSN.
                    FrameRowEvent::Boundary { commit_lsn } => {
                        let frame_deletes = std::mem::take(&mut core.frame_deleted_keys);
                        for (rel, key) in frame_deletes {
                            core.population_deleted_keys.record(rel, key, *commit_lsn);
                        }
                        let frame_truncated = std::mem::take(&mut core.frame_truncated_relations);
                        for rel in frame_truncated {
                            core.population_deleted_keys.abort_below(rel, *commit_lsn);
                        }
                        (Ok(()), "cdc replay boundary")
                    }
                };
                self.frame_dml_result(core, r).await.attach_loc(loc)?;
            }

            if let Some(relation_oids) = trailing_truncate
                && core.frame_state != FrameState::Recovering
            {
                let r = self.handle_truncate(core, relation_oids).await;
                self.frame_dml_result(core, r)
                    .await
                    .attach_loc("cdc frame replay truncate")?;
            }
            base += segment.len();
        }
        // Hand the cleared buffer back so its capacity is reused across
        // frames, recycling each event's row Vecs into the pool on the way.
        let mut events = events;
        for event in events.drain(..) {
            core.row_vecs_recycle(event);
        }
        core.frame_rows = events;
        Ok(())
    }

    /// Mid-frame partial replay once the event log reaches its cap, bounding
    /// frame memory the way `frame_buf`'s chunk flush does.
    pub(super) async fn frame_rows_replay_if_full(
        &mut self,
        core: &mut WriterCore,
    ) -> CacheResult<()> {
        if core.frame_rows.len() >= FRAME_ROWS_CAPACITY {
            self.frame_rows_replay(core)
                .await
                .attach_loc("cdc mid-frame replay")?;
        }
        Ok(())
    }

    /// Flush the accumulated batch (PGC-242): memo-bracket the union of the
    /// batched frames' relations, replay + commit the whole event log as one
    /// cache transaction, advance the watermark to `lsn` (the last batched
    /// frame's commit), and run the deferred bookkeeping. With an empty queue
    /// every CommitMark flushes its own frame — the pre-batching behavior.
    pub(super) async fn batch_flush(&mut self, core: &mut WriterCore, lsn: Lsn) -> CacheResult<()> {
        // In-process memo seqlock (PGC-236, rung 1): bracket the batch's
        // visibility with begin (even→odd) before and end (odd→even) after,
        // over every relation the batched frames touched, so any memo over
        // them is invalidated atomically with the batch becoming visible.
        // Conceptually the relation-granularity twin of the per-query MV
        // dirty hooks in `frame_finalize`.
        //
        // Gated on the store being non-empty rather than on `enabled()`:
        // when no memo exists there is nothing to bust, and (unlike an
        // `enabled()` gate) this keeps the seqlock authoritative even
        // while memoization is toggled off at runtime — a change landing
        // during the disabled window still bumps the slot, so a later
        // re-enable cannot serve a snapshot that predates it.
        // Replay the buffered row events first (PGC-241): this opens the cache
        // txn (`Active → TxnOpen`) or enters `Recovering`, and — for rung 3b —
        // runs the per-row handlers that accumulate the predicate-matched
        // `frame_memo_evictions`. It MUST precede the memo seqlock `begin` below
        // so begin/end bracket the identical (now-final) eviction set across the
        // commit. Buffered writes go to `frame_buf`; the COMMIT is in
        // `frame_finalize`, which the bracket spans.
        self.frame_rows_replay(core).await?;

        let memo_active = !core.state_view.memo.is_empty();
        // Rung 3b: bump `Memo(F)` only for the predicate-matched eviction set
        // (`frame_memo_evictions`) — a change that doesn't touch a memo's
        // predicate/membership leaves it intact. The `Relation` slot is still
        // bumped for every touched relation as the capture-window guard.
        if memo_active {
            for &oid in &core.frame_relation_oids {
                core.state_view
                    .memo
                    .slot_dirty_begin(SlotKey::Relation(oid));
            }
        }
        for &fp in &core.frame_memo_evictions {
            core.state_view.memo.slot_dirty_begin(SlotKey::Memo(fp));
        }

        // Always publish the post-commit version (end the seqlock) before
        // propagating any finalize error: a slot left odd would permanently
        // disable memo serving for that relation on a live subsystem.
        let finalize = self.frame_finalize(core).await;
        if memo_active {
            for &oid in &core.frame_relation_oids {
                core.state_view.memo.slot_dirty_end(SlotKey::Relation(oid));
            }
        }
        for &fp in &core.frame_memo_evictions {
            core.state_view.memo.slot_dirty_end(SlotKey::Memo(fp));
        }
        finalize?;

        core.frame_state = FrameState::Idle;
        core.frame_invalidations.clear();
        core.frame_memo_evictions.clear();
        core.frame_relation_oids.clear();
        core.frame_buf.clear();
        core.frame_buf_relations.clear();
        core.frame_rows.clear();
        core.frame_chunk_flushed = false;
        core.batch_frames = 0;
        core.batch_events = 0;
        core.batch_deleted_pks.clear();
        core.toast_overlay_reset();
        core.batch_toast_guard_oids.clear();
        self.applied_lsn_advance(lsn);
        core.last_applied_lsn = self.last_applied_lsn;
        // Per-frame deletes/truncates were stamped by Boundary events during
        // replay; these drains catch what replay didn't reach — pending
        // entries from a frame that entered `Recovering` before its boundary
        // (no-ops in the normal path, where the lists are already empty).
        let frame_deletes = std::mem::take(&mut core.frame_deleted_keys);
        for (relation_oid, key) in frame_deletes {
            core.population_deleted_keys.record(relation_oid, key, lsn);
        }
        let frame_truncated = std::mem::take(&mut core.frame_truncated_relations);
        for relation_oid in frame_truncated {
            core.population_deleted_keys.abort_below(relation_oid, lsn);
        }
        // Bulk invalidations recorded outside replay (mid-batch DDL drops,
        // 40P01 recovery) — flush-LSN-stamped by design (an upper bound on
        // the triggering frame's commit; over-aborts, never under-aborts).
        let batch_truncated = std::mem::take(&mut core.batch_truncated_relations);
        for relation_oid in batch_truncated {
            core.population_deleted_keys.abort_below(relation_oid, lsn);
        }
        // The batch is closed; flush maintenance that was deferred while it
        // was open (it would have deadlocked on the frame's locks).
        if core.purge_pending {
            let threshold = core.cache.generation_purge_threshold();
            core.generation_purge(threshold)
                .await
                .attach_loc("deferred generation purge")?;
            core.purge_pending = false;
        }
        Ok(())
    }

    /// Apply a `CommitMark`: flush the frame's deferred invalidations and commit
    /// (or recover) per its state. Extracted from the `CommitMark` handler so the
    /// memo seqlock bracket can publish the post-commit version on every exit
    /// path — including an error return from here.
    /// Commit (or recover) the frame. The caller (`batch_flush`) must have
    /// already run `frame_rows_replay` — that opens the txn (`Active → TxnOpen`)
    /// or enters `Recovering` and, for rung 3b, accumulates
    /// `frame_memo_evictions` — so the memo seqlock bracket sees the final
    /// state/eviction-set before the commit this performs.
    async fn frame_finalize(&mut self, core: &mut WriterCore) -> CacheResult<()> {
        match core.frame_state {
            FrameState::TxnOpen => {
                self.frame_invalidations_flush(core).await?;
                // The buffered writes flush as one BEGIN; …; COMMIT here
                // (PGC-228). A 40P01 now surfaces at flush, not per statement:
                // route it through frame_dml_result so the frame enters
                // Recovering, then run relation-level recovery (same as a
                // mid-frame deadlock would).
                let r = self.frame_commit(core).await;
                self.frame_dml_result(core, r)
                    .await
                    .attach_loc("cdc commit frame")?;
                if core.frame_state == FrameState::Recovering {
                    self.frame_recover(core)
                        .await
                        .attach_loc("cdc recover frame at commit")?;
                }
            }
            // A frame can flag queries for invalidation without any in-place
            // write (e.g. a growing-join insert excluded from in-place
            // maintenance) — no `BEGIN` was opened, but the flagged queries must
            // still be invalidated here.
            FrameState::Active => {
                self.frame_invalidations_flush(core).await?;
            }
            FrameState::Recovering => {
                // Relation-level recovery evicts every query over the affected
                // relations — a superset of the selectively flagged fps, so the
                // fp flush is subsumed.
                self.frame_recover(core)
                    .await
                    .attach_loc("cdc recover frame")?;
            }
            // CommitMark without a preceding Begin: pgoutput always pairs them
            // for published txns, so this is unreachable.
            FrameState::Idle => {
                debug_assert!(false, "CommitMark without a preceding Begin");
            }
        }
        Ok(())
    }

    /// Advance `last_applied_lsn` forward to `lsn`, updating the Prometheus
    /// gauge. No-op if `lsn` does not advance the watermark.
    pub(super) fn applied_lsn_advance(&mut self, lsn: Lsn) {
        if lsn > self.last_applied_lsn {
            self.last_applied_lsn = lsn;
            // LSNs past 2^53 lose precision in f64 (~9 PB of WAL — irrelevant).
            #[allow(clippy::cast_precision_loss)]
            crate::metrics::handles()
                .cdc
                .applied_lsn
                .set(lsn.get() as f64);
        }
    }
}
