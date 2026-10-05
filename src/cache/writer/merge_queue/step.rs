//! Advancing the active drain one chunk at a time on the writer (PGC-418).

use std::time::{Duration, Instant};

use ecow::EcoString;
use rootcause::Report;
use tokio_postgres::SimpleQueryMessage;
use tracing::debug;

use super::sql::{MergePlan, chunk_result_parse, discard_result_parse, discard_sql};
use super::{
    ChunkStatement, ChunkTarget, ChunkWindow, DrainCursor, DrainTarget, KeyScope, MergeInProgress,
    MergeStep, StagingDiscard,
};
use crate::cache::messages::PopulationMerge;
use crate::cache::writer::core::WriterCore;
use crate::cache::{CacheError, CacheResult, MapIntoReport, ReportExt};
use crate::oid::Oid;
use crate::query::Fingerprint;

/// What one merge step reads from the active drain up front.
struct MergeStepInputs {
    cursor: DrainCursor,
    chunk_blocks: u32,
    generation: u64,
    staged: Vec<(Oid, EcoString)>,
    /// `(fingerprint, generation)` of an applying merge; `None` for a discard.
    population: Option<(Fingerprint, u64)>,
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
        // The staged list is copied onto the drain: a merge can switch to
        // discarding mid-way (`population_merge_discard_remaining`) and keep
        // draining the same tables after the target drops the population.
        let staged = merge.staged.clone();
        self.merges.drain_begin(|boundary| {
            MergeInProgress::new(
                merge.fingerprint,
                merge.generation,
                staged,
                DrainTarget::Apply(merge),
                boundary,
            )
        });
    }

    /// Begin emptying a superseded population's staging tables in chunks (it
    /// never reached its merge — the tombstone path of the pending-merge drain).
    pub(crate) fn population_discard_start(&mut self, discard: StagingDiscard) {
        self.merges.drain_begin(|boundary| {
            MergeInProgress::new(
                discard.fingerprint,
                discard.generation,
                discard.staged,
                DrainTarget::Discard,
                boundary,
            )
        });
    }

    /// Queue a population's staging for a chunked discard once the merge slot
    /// is free: a superseded population that never reached its merge, or one
    /// whose worker failed mid-stream. The tables are read from the pool's
    /// ledger, which keeps them checked out under the key until the discard's
    /// check-in.
    pub(crate) fn population_discard_enqueue(&mut self, fingerprint: Fingerprint, generation: u64) {
        let staged = self.staging_pool.held(fingerprint, generation);
        if staged.is_empty() {
            return;
        }
        self.merges.discards.push_back(StagingDiscard {
            fingerprint,
            generation,
            staged,
        });
    }

    /// Stop applying the in-progress merge — the query was superseded,
    /// invalidated, evicted, aborted, or the merge failed — and switch it to
    /// discarding its remaining staging rows from where it stopped. Rows already
    /// merged stay: each was snapshot state behind the watermark, filtered
    /// against a complete deleted-key set when it landed, and is maintained by
    /// CDC from then on, so they cannot tear a bystander's result; they're
    /// reclaimed by generation once nothing references them.
    pub(crate) fn population_merge_discard_remaining(&mut self) {
        if let Some(in_progress) = self.merges.active.as_mut()
            && in_progress.is_applying()
        {
            debug!(
                "population merge stopped after {} chunks / {} rows, discarding the rest {}",
                in_progress.chunks, in_progress.drained_rows, in_progress.fingerprint
            );
            in_progress.target = DrainTarget::Discard;
        }
    }

    /// Run one chunk of the in-progress drain (PGC-418): size the relation the
    /// cursor is about to enter, or drain one window of the relation it is in.
    /// A merge filters keys CDC removed during the population (PGC-250); the
    /// filter is re-rendered whenever the key set changed since the last chunk
    /// so a removal that lands between chunks is honored. Returns `Continue`
    /// while staging rows remain; on `Done` / `Aborted` the caller takes the
    /// active drain and finalizes.
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
        let Some((relation_oid, staging)) = step.staged.get(window.index).cloned() else {
            return Ok(MergeStep::Done);
        };
        let target = ChunkTarget {
            relation_oid,
            staging,
            window,
        };
        if self.merge_keys_lost() {
            return Ok(MergeStep::Aborted);
        }
        // A relation evicted mid-population has no cache table to merge into;
        // its staging rows are discarded in chunks like the rest.
        let plan = step
            .population
            .and_then(|_| self.cache.tables.get1(&relation_oid).map(MergePlan::build));
        if let (Some(plan), Some(population)) = (&plan, step.population)
            && self
                .merge_toast_stale_hit(&target, plan, population)
                .await?
        {
            return Ok(MergeStep::Aborted);
        }

        let (count, elapsed) = self
            .merge_chunk_run(plan.as_ref(), &target, step.generation)
            .await?;
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
            staged: in_progress.staged.clone(),
            population: match &in_progress.target {
                DrainTarget::Apply(merge) => Some((merge.fingerprint, merge.generation)),
                DrainTarget::Discard => None,
            },
        })
    }

    /// Enter `staged[index]`: size its staging table and point the cursor at
    /// its first window (or past it, if it is empty).
    async fn merge_relation_enter(&mut self, index: usize) -> CacheResult<()> {
        let Some(in_progress) = self.merges.active.as_ref() else {
            return Ok(());
        };
        let staged_len = in_progress.staged.len();
        let Some((_, staging)) = in_progress.staged.get(index) else {
            return Ok(());
        };
        let staging = staging.clone();
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
        }
        Ok(())
    }

    /// Whether the active merge must abort because a relation lost keys
    /// (overflow) or was bulk-invalidated (TRUNCATE / recovery) at an LSN past
    /// this population's snapshot — a later chunk could resurrect removed rows.
    /// Checked per chunk: either can happen in a CDC frame applied between
    /// chunks. Discards never abort.
    fn merge_keys_lost(&self) -> bool {
        let Some(in_progress) = self.merges.active.as_ref() else {
            return false;
        };
        let DrainTarget::Apply(merge) = &in_progress.target else {
            return false;
        };
        in_progress.staged.iter().any(|(oid, _)| {
            self.population_deleted_keys
                .should_abort(*oid, merge.snapshot_lsn)
        })
    }

    /// Whether this population staged a row a toast fallback has since left
    /// unrepairable (PGC-464): keys recorded above its anchor floor that are
    /// present in its staging table. Probed only when that key set moved since
    /// the last chunk.
    async fn merge_toast_stale_hit(
        &mut self,
        target: &ChunkTarget,
        plan: &MergePlan,
        (fingerprint, generation): (Fingerprint, u64),
    ) -> CacheResult<bool> {
        let relation_oid = target.relation_oid;
        let Some(floor) = self
            .population_deleted_keys
            .floor(relation_oid, fingerprint, generation)
        else {
            return Ok(false);
        };
        let Some(predicate) = self.merges.active.as_mut().and_then(|in_progress| {
            let scope = KeyScope {
                keys: &self.population_deleted_keys,
                relation_oid,
                pk_columns_paren: &plan.pk_columns_paren,
            };
            in_progress.stale_probe_predicate(scope, floor)
        }) else {
            return Ok(false);
        };
        let probe = format!(
            "SELECT 1 FROM pgcache_stage.{} WHERE {predicate} LIMIT 1",
            target.staging
        );
        let hit = self
            .db_cache
            .simple_query(&probe)
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
    /// table under `plan`, or a plain discard without one. Returns the rows
    /// drained and the statement's wall time.
    async fn merge_chunk_run(
        &mut self,
        plan: Option<&MergePlan>,
        target: &ChunkTarget,
        generation: u64,
    ) -> CacheResult<(u64, Duration)> {
        let statement = ChunkStatement {
            staging: &target.staging,
            generation,
            lo_block: target.window.lo_block,
            hi_block: target.window.hi_block,
        };
        let sql = match plan {
            Some(plan) => {
                let filter = self.merges.active.as_mut().and_then(|in_progress| {
                    in_progress.filter_predicate(KeyScope {
                        keys: &self.population_deleted_keys,
                        relation_oid: target.relation_oid,
                        pk_columns_paren: &plan.pk_columns_paren,
                    })
                });
                plan.chunk_sql(statement, filter)
            }
            None => discard_sql(statement),
        };

        let started = Instant::now();
        let messages = self
            .db_cache
            .simple_query(&sql)
            .await
            .map_into_report::<CacheError>()
            .attach_loc("population merge chunk")?;
        let elapsed = started.elapsed();
        let count = match plan {
            Some(_) => chunk_result_parse(&messages)?,
            None => discard_result_parse(&messages),
        };
        fault_merge_chunk_delay().await;

        let mh = crate::metrics::handles();
        match plan {
            Some(_) => mh.reg.merge_chunks.increment(1),
            None => mh.reg.merge_discard_chunks.increment(1),
        }
        mh.reg.merge_chunk.record(elapsed.as_secs_f64());
        Ok((count, elapsed))
    }

    /// Heap blocks in a staging table's main fork, from the file size.
    async fn staging_blocks(&self, staging: &str) -> CacheResult<u32> {
        let row = self
            .db_cache
            .query_one(
                &format!(
                    "SELECT (pg_relation_size('pgcache_stage.{staging}') \
                     / current_setting('block_size')::bigint)::bigint"
                ),
                &[],
            )
            .await
            .map_into_report::<CacheError>()
            .attach_loc("reading staging table size")?;
        let blocks: i64 = row.get(0);
        u32::try_from(blocks)
            .map_err(|_| Report::from(CacheError::Other))
            .attach_loc("staging table block count out of range")
    }
}
