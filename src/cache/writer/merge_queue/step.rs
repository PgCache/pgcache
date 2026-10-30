//! Advancing the active drain one chunk at a time on the writer (PGC-418).

use std::fmt::Write as _;
use std::time::{Duration, Instant};

use rootcause::Report;
use tokio_postgres::SimpleQueryMessage;
use tracing::{debug, error};

use super::sql::{MergePlan, chunk_result_parse};
use super::{
    ChunkStatement, ChunkTarget, ChunkWindow, DrainCursor, KeyScope, MergeInProgress, MergeStep,
    StagingDiscard,
};
use crate::cache::Generation;
use crate::cache::messages::PopulationMerge;
use crate::cache::writer::core::WriterCore;
use crate::cache::{CacheError, CacheResult, MapIntoReport, ReportExt};
use crate::query::Fingerprint;

/// What one merge step reads from the active drain up front.
struct MergeStepInputs {
    cursor: DrainCursor,
    chunk_blocks: u32,
    generation: Generation,
    population: (Fingerprint, Generation),
}

/// Test-only delay after each merge chunk (fault-injection feature): stretches
/// a merge so a test can land CDC events between its chunks. Blocks the writer
/// like a slow chunk would.
#[cfg(feature = "fault-injection")]
async fn fault_merge_chunk_delay() {
    if let Some(ms) = std::env::var("PGCACHE_FAULT_MERGE_CHUNK_DELAY_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|ms| *ms > 0)
    {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }
}
#[cfg(not(feature = "fault-injection"))]
async fn fault_merge_chunk_delay() {}

impl WriterCore {
    /// Begin merging one population's staging tables into the shared cache
    /// tables; `population_merge_step` drains them chunk by chunk. Generation is
    /// stamped per chunk (moved off the population worker). The caller
    /// deactivates the deleted-key set, checks staging back in, and marks the
    /// query Ready / Failed when the step reports a terminal outcome.
    pub(crate) fn population_merge_start(&mut self, merge: PopulationMerge) {
        self.merges
            .drain_begin(|boundary| MergeInProgress::new(merge, boundary));
    }

    /// Hand a population's staging tables to a population worker to empty, one
    /// `DELETE` per table on the worker's own connection: the writer never runs
    /// the statement and the merge slot stays free for merges (PGC-553). The
    /// pool ledger keeps the tables checked out under the key until the
    /// worker's `Discarded` ack releases them. No-op without a hold (never
    /// checked out, or already handed off).
    pub(crate) fn population_discard_send(
        &mut self,
        fingerprint: Fingerprint,
        generation: Generation,
    ) {
        let staged = self.staging_pool().held(fingerprint, generation);
        if staged.is_empty() {
            return;
        }
        let discard = StagingDiscard {
            fingerprint,
            generation,
            staged,
        };
        if self.discard_tx.send(discard).is_err() {
            // Shutdown: the workers are gone. The tables stay orphaned until
            // the next cache reset, like a dispatch failure.
            error!("staging discard channel closed; orphaning staging of {fingerprint}");
            self.staging_pool().forget(fingerprint, generation);
            return;
        }
        self.merges.discards_pending += 1;
        crate::metrics::handles().reg.merge_discards.increment(1);
    }

    /// Drop a staged population that stopped being current — invalidated,
    /// superseded, evicted, or its query failed — at that moment rather than
    /// when its tombstone surfaces at the heap head: release its deleted-key
    /// tracking and send its staging for discard. True when this call claimed
    /// it; false when it was not staged or was claimed already, so the eager
    /// drop and the later tombstone pop never release twice.
    pub(crate) fn population_staged_drop(
        &mut self,
        fingerprint: Fingerprint,
        generation: Generation,
    ) -> bool {
        if !self.merges.staged_take((fingerprint, generation)) {
            return false;
        }
        self.population_deleted_keys
            .deactivate(fingerprint, generation);
        self.population_discard_send(fingerprint, generation);
        crate::metrics::handles().reg.merges_superseded.increment(1);
        true
    }

    /// Stop the in-progress merge — the query was superseded, invalidated,
    /// evicted, aborted, or a chunk failed — freeing the slot at once and
    /// sending its staging for discard. Rows already merged stay: each was
    /// snapshot state behind the watermark, filtered against a complete
    /// deleted-key set when it landed, and is maintained by CDC from then on,
    /// so they cannot tear a bystander's result; they're reclaimed by
    /// generation once nothing references them.
    pub(crate) fn population_merge_abandon(&mut self) {
        let Some(in_progress) = self.merges.active.take() else {
            return;
        };
        debug!(
            "population merge stopped after {} chunks / {} rows, discarding the rest {}",
            in_progress.chunks, in_progress.drained_rows, in_progress.fingerprint
        );
        self.population_deleted_keys
            .deactivate(in_progress.fingerprint, in_progress.generation);
        self.population_discard_send(in_progress.fingerprint, in_progress.generation);
    }

    /// Run one chunk of the in-progress drain (PGC-418): size the relation the
    /// cursor is about to enter, or drain one window of the relation it is in.
    /// A merge filters keys CDC removed during the population (PGC-250); the
    /// filter is re-rendered whenever the key set changed since the last chunk
    /// so a removal that lands between chunks is honored. Returns `Continue`
    /// while staging rows remain; on `Done` / `Aborted` the caller takes the
    /// active drain and finalizes.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(crate) async fn population_merge_step(&mut self) -> CacheResult<MergeStep> {
        let Some(step) = self.merge_step_inputs() else {
            return Ok(MergeStep::Done);
        };
        let window = match step.cursor {
            DrainCursor::Done => return Ok(MergeStep::Done),
            DrainCursor::RelationStart { index } => {
                self.merge_relation_enter(index).await?;
                return Ok(MergeStep::Continue);
            }
            DrainCursor::InRelation {
                index,
                next_block,
                blocks,
            } => ChunkWindow {
                index,
                lo_block: next_block,
                hi_block: next_block.saturating_add(step.chunk_blocks).min(blocks),
                blocks,
            },
        };
        let Some((relation_oid, staging)) = self
            .merges
            .active
            .as_ref()
            .and_then(|in_progress| in_progress.staged_at(window.index))
        else {
            return Ok(MergeStep::Done);
        };
        let target = ChunkTarget {
            relation_oid,
            staging,
            window,
        };
        if self.merge_keys_lost() || self.merge_fallback_missed() {
            return Ok(MergeStep::Aborted);
        }
        // The plan was rendered on entering the relation; none means the
        // relation was evicted mid-population and has no cache table to
        // merge into. Taken out for the DB calls, put back after.
        let Some(plan) = self
            .merges
            .active
            .as_mut()
            .and_then(|in_progress| in_progress.plan.take())
        else {
            return Ok(MergeStep::Aborted);
        };
        let outcome = self.merge_chunk(&plan, &target, &step).await;
        if let Some(in_progress) = self.merges.active.as_mut() {
            in_progress.plan = Some(plan);
        }
        outcome
    }

    /// The toast-stale probe and the chunk itself, under the relation's plan.
    async fn merge_chunk(
        &mut self,
        plan: &MergePlan,
        target: &ChunkTarget,
        step: &MergeStepInputs,
    ) -> CacheResult<MergeStep> {
        if self
            .merge_toast_stale_hit(target, plan, step.population)
            .await?
        {
            return Ok(MergeStep::Aborted);
        }
        let (count, elapsed) = self.merge_chunk_run(plan, target, step.generation).await?;
        if let Some(in_progress) = self.merges.active.as_mut() {
            in_progress.chunk_finish(&target.window, count, elapsed);
        }
        Ok(MergeStep::Continue)
    }

    /// The active drain's state a step reads before it touches `self`
    /// mutably, or `None` when no drain is active.
    fn merge_step_inputs(&self) -> Option<MergeStepInputs> {
        let in_progress = self.merges.active.as_ref()?;
        Some(MergeStepInputs {
            cursor: in_progress.cursor,
            chunk_blocks: in_progress.chunk_blocks,
            generation: in_progress.generation,
            population: (in_progress.fingerprint, in_progress.generation),
        })
    }

    /// Enter `staged[index]`: render the relation's merge plan, size its
    /// staging table and point the cursor at its first window (or past it,
    /// if it is empty).
    async fn merge_relation_enter(&mut self, index: usize) -> CacheResult<()> {
        let Some(in_progress) = self.merges.active.as_ref() else {
            return Ok(());
        };
        let staged_len = in_progress.staged.len();
        let Some((relation_oid, staging)) = in_progress.staged_at(index) else {
            return Ok(());
        };
        let plan = self
            .cache
            .tables
            .get1(&relation_oid)
            .map(|table| MergePlan::build(table));
        let blocks = self.staging_blocks(&staging).await?;
        let cursor = if blocks == 0 {
            DrainCursor::after_relation(index + 1, staged_len)
        } else {
            DrainCursor::InRelation {
                index,
                next_block: 0,
                blocks,
            }
        };
        if let Some(in_progress) = self.merges.active.as_mut() {
            in_progress.cursor = cursor;
            in_progress.plan = plan;
        }
        Ok(())
    }

    /// Whether the active merge must abort because a relation lost keys
    /// (overflow) or was bulk-invalidated (TRUNCATE / recovery) at an LSN past
    /// this population's snapshot — a later chunk could resurrect removed rows.
    /// Checked per chunk: either can happen in a CDC frame applied between
    /// chunks.
    fn merge_keys_lost(&self) -> bool {
        let Some(in_progress) = self.merges.active.as_ref() else {
            return false;
        };
        in_progress.staged.iter().any(|(oid, _)| {
            self.population_deleted_keys
                .should_abort(*oid, in_progress.merge.snapshot_lsn)
        })
    }

    /// Whether a toast fallback matched the active merge's query in a
    /// transaction its reads may have missed (PGC-484): the row may be absent
    /// or stale in staging, so the merge aborts and the query repopulates.
    fn merge_fallback_missed(&self) -> bool {
        let Some(in_progress) = self.merges.active.as_ref() else {
            return false;
        };
        let missed = self
            .population_deleted_keys
            .fallback_missed(in_progress.fingerprint, in_progress.generation);
        if missed {
            crate::metrics::handles()
                .cdc
                .toast_stale_aborts
                .increment(1);
        }
        missed
    }

    /// Whether this population staged a row a toast fallback has since left
    /// unrepairable (PGC-464): keys its reads may have missed that are present
    /// in its staging table. Probed only when that key set moved since the
    /// last chunk.
    async fn merge_toast_stale_hit(
        &mut self,
        target: &ChunkTarget,
        plan: &MergePlan,
        population: (Fingerprint, Generation),
    ) -> CacheResult<bool> {
        let relation_oid = target.relation_oid;
        let Some(predicate) = self.merges.active.as_mut().and_then(|in_progress| {
            let scope = KeyScope {
                keys: &self.population_deleted_keys,
                relation_oid,
                key_columns: &plan.key_columns,
            };
            in_progress.stale_probe_predicate(scope, population)
        }) else {
            return Ok(false);
        };
        self.merge_buf.clear();
        let _ = write!(
            self.merge_buf,
            "SELECT 1 FROM pgcache_stage.{} AS d WHERE {predicate} LIMIT 1",
            target.staging
        );
        let hit = self
            .db_cache
            .simple_query(&self.merge_buf)
            .await
            .map_into_report::<CacheError>()
            .attach_loc("population merge toast-stale probe")?
            .iter()
            .any(|m| matches!(m, SimpleQueryMessage::Row(_)));
        if hit {
            crate::metrics::handles()
                .cdc
                .toast_stale_aborts
                .increment(1);
        }
        Ok(hit)
    }

    /// Drain one window of a staging table: a filtered upsert into the cache
    /// table under `plan`. Returns the rows drained and the statement's wall
    /// time.
    async fn merge_chunk_run(
        &mut self,
        plan: &MergePlan,
        target: &ChunkTarget,
        generation: Generation,
    ) -> CacheResult<(u64, Duration)> {
        let statement = ChunkStatement {
            staging: &target.staging,
            generation,
            lo_block: target.window.lo_block,
            hi_block: target.window.hi_block,
        };
        let filter = self.merges.active.as_mut().and_then(|in_progress| {
            in_progress.filter_predicate(KeyScope {
                keys: &self.population_deleted_keys,
                relation_oid: target.relation_oid,
                key_columns: &plan.key_columns,
            })
        });
        plan.chunk_sql_into(&mut self.merge_buf, statement, filter);

        let started = Instant::now();
        let messages = self
            .db_cache
            .simple_query(&self.merge_buf)
            .await
            .map_into_report::<CacheError>()
            .attach_loc("population merge chunk")?;
        let elapsed = started.elapsed();
        let count = chunk_result_parse(&messages)?;
        fault_merge_chunk_delay().await;

        let mh = crate::metrics::handles();
        mh.reg.merge_chunks.increment(1);
        mh.reg.merge_chunk.record(elapsed.as_secs_f64());
        Ok((count, elapsed))
    }

    /// Heap blocks in a staging table's main fork, from the file size.
    async fn staging_blocks(&mut self, staging: &str) -> CacheResult<u32> {
        self.merge_buf.clear();
        let _ = write!(
            self.merge_buf,
            "SELECT (pg_relation_size('pgcache_stage.{staging}') \
             / current_setting('block_size')::bigint)::bigint"
        );
        // A simple query: the extended protocol would prepare and close a
        // statement around one integer, two round trips per relation.
        let messages = self
            .db_cache
            .simple_query(&self.merge_buf)
            .await
            .map_into_report::<CacheError>()
            .attach_loc("reading staging table size")?;
        chunk_result_parse(&messages).and_then(|blocks| {
            u32::try_from(blocks)
                .map_err(|_| Report::from(CacheError::Other))
                .attach_loc("staging table block count out of range")
        })
    }
}
