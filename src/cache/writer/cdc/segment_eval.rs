use crate::catalog::Oid;
use crate::query::{Fingerprint, FingerprintSet};
use std::collections::{HashMap, HashSet};
use std::fmt::Write;

use ecow::EcoString;
use futures_util::future;
use postgres_protocol::escape;
use tokio_postgres::types::ToSql;
use tokio_postgres::{SimpleQueryMessage, Statement};
use tracing::{error, warn};

use crate::catalog::TableMetadata;
use crate::pg::protocol::ByteString;

use crate::query::ast::Deparse;
use crate::query::transform::{
    BATCH_IDX_COLUMN, resolved_select_node_table_replace_with_unnest,
    resolved_select_node_table_replace_with_values_batch,
};

use super::super::super::update_query::{
    RowChanges, UpdateEvalStrategy, UpdateQueries, UpdateQuery,
};
use super::super::super::{CacheError, CacheResult, MapIntoReport};
use super::super::core::WriterCore;
use super::super::frame::FrameRowEvent;
use super::invalidation::{
    OLD_IS_NULL_ALIAS_PREFIX, OLD_LESS_THAN_ALIAS_PREFIX, pg_bool_text, row_change_column_fold,
};
use crate::result::error_chain_format;

use super::*;

/// Shared array params for a prepared eval statement over one row chunk:
/// `$1` = row ordinals, `$2..` = one `text[]` per column in
/// `table_metadata.columns` order. The unnest transform's parameter numbering
/// and the prepared row-change SQL builder both follow the same column order —
/// this is the single place the binding contract is produced.
fn chunk_arrays_build<'a>(
    table_metadata: &TableMetadata,
    row_chunk: &[(usize, &'a [Option<ByteString>])],
) -> (Vec<i32>, Vec<Vec<Option<&'a str>>>) {
    // Chunk length is bounded by PG_EVAL_ROW_CHUNK (64): never wraps.
    #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
    let ordinals: Vec<i32> = (0..row_chunk.len() as i32).collect();
    let column_arrays = table_metadata
        .columns
        .iter()
        .map(|column_meta| {
            row_chunk
                .iter()
                .map(|(_, row)| row.get(column_meta.index()).and_then(|v| v.as_deref()))
                .collect()
        })
        .collect();
    (ordinals, column_arrays)
}

/// Append the row-change SELECT list — `SELECT v.<idx>, o.X IS DISTINCT FROM
/// v.X AS X, …` — shared by the prepared and inline row-change builders so the
/// changed-column contract can't drift between them. Limit-window ORDER BY key
/// columns additionally project old-vs-new ordering (`o.X < v.X`,
/// `o.X IS NULL`) for the window-direction check (PGC-334).
fn row_change_select_into(
    buf: &mut String,
    table_metadata: &TableMetadata,
    order_columns: &HashSet<&EcoString>,
) {
    buf.push_str("SELECT v.");
    buf.push_str(BATCH_IDX_COLUMN);
    for column_meta in &table_metadata.columns {
        let _ = write!(
            buf,
            ", o.{name} IS DISTINCT FROM v.{name} AS {name}",
            name = column_meta.name
        );
        if order_columns.contains(&column_meta.name) {
            let _ = write!(
                buf,
                ", o.{c} < v.{c} AS {OLD_LESS_THAN_ALIAS_PREFIX}{c}, o.{c} IS NULL AS {OLD_IS_NULL_ALIAS_PREFIX}{c}",
                c = column_meta.name
            );
        }
    }
}

/// The relation's limit-window ORDER BY key columns — the set
/// `row_change_select_into` projects ordering for.
fn relation_order_columns(update_queries: Option<&UpdateQueries>) -> HashSet<&EcoString> {
    update_queries
        .map(|uq| uq.limit_order_columns().collect())
        .unwrap_or_default()
}

/// Append the row-change PK join — ` JOIN <schema>.<table> o ON o.pk = v.pk…`
/// — shared by the prepared and inline row-change builders.
fn row_change_join_on_into(buf: &mut String, table_metadata: &TableMetadata) {
    let _ = write!(
        buf,
        " JOIN {}.{} o ON ",
        table_metadata.schema, table_metadata.name
    );
    for (i, pk_column) in table_metadata.primary_key_columns.iter().enumerate() {
        if i > 0 {
            buf.push_str(" AND ");
        }
        let _ = write!(buf, "o.{pk_column} = v.{pk_column}");
    }
}

/// Statement-cache key for prepared membership eval. Deliberately a named
/// type: when update queries become shape-parameterized (PGC-257) this swaps
/// to `(relation, shape)` here, and the cache machinery carries over.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct PreparedEvalKey {
    pub(super) relation_oid: Oid,
    fingerprint: Fingerprint,
}

/// Per-relation membership-eval rows of one segment: `(event index, row)`.
type SegmentRows<'a> = HashMap<Oid, Vec<(usize, &'a [Option<ByteString>])>>;

/// Batched PgEval membership + row-change results for one segment of frame
/// row events (PGC-241). Built by `segment_membership_eval` /
/// `segment_row_changes_eval`, consumed per event by the decide pass.
#[derive(Default)]
pub(super) struct SegmentMembership {
    relations: HashMap<Oid, RelationBatch>,
}

/// One relation's batched-eval results within a segment. Every event belongs
/// to exactly one relation, so the coverage invariants (which fingerprints a
/// covered event may consult) live inside one entry instead of across
/// parallel collections.
#[derive(Default)]
struct RelationBatch {
    /// Batchable Fresh-MV fingerprints, evaluated for every `covered` event
    /// (Fresh queries are always fully evaluated so every match dirty-marks —
    /// same as the per-row path).
    fresh_fps: FingerprintSet,
    /// Batchable non-Fresh fingerprints, evaluated only for `rest_covered`
    /// events (rows with no LocalEval match and no fresh hit) — mirroring the
    /// per-row path's `if !matched` short-circuit, which does zero PgEval
    /// round-trips for locally-matched rows.
    rest_fps: FingerprintSet,
    /// Event indexes whose row was in the fresh membership batch (rows of
    /// unexpected arity stay out and fall back to per-row eval).
    covered: HashSet<usize>,
    /// Event indexes whose row was in the rest membership batch.
    rest_covered: HashSet<usize>,
    /// `(event index, fingerprint)` membership hits.
    hits: HashSet<(usize, Fingerprint)>,
    /// Update events whose row-change SELECT was batched. Covered-but-absent
    /// from `row_changes` ⇒ the row isn't in the cache table (the per-row
    /// `None` case).
    row_change_covered: HashSet<usize>,
    /// Per covered update event: column → changed (`IS DISTINCT FROM`).
    row_changes: HashMap<usize, RowChanges>,
}

impl SegmentMembership {
    /// The matrix view for one event, or `None` if neither the membership nor
    /// the row-change batch covered it.
    pub(super) fn view(&self, relation_oid: Oid, event_idx: usize) -> Option<BatchEvalView<'_>> {
        let batch = self.relations.get(&relation_oid)?;
        let fresh_fps = batch
            .covered
            .contains(&event_idx)
            .then_some(&batch.fresh_fps);
        let rest_fps = batch
            .rest_covered
            .contains(&event_idx)
            .then_some(&batch.rest_fps);
        let row_change = batch
            .row_change_covered
            .contains(&event_idx)
            .then(|| batch.row_changes.get(&event_idx));
        if fresh_fps.is_none() && rest_fps.is_none() && row_change.is_none() {
            return None;
        }
        Some(BatchEvalView {
            fresh_fps,
            rest_fps,
            hits: &batch.hits,
            event_idx,
            row_change,
        })
    }
}

/// One event's window into a [`SegmentMembership`] matrix.
pub(super) struct BatchEvalView<'a> {
    fresh_fps: Option<&'a FingerprintSet>,
    rest_fps: Option<&'a FingerprintSet>,
    hits: &'a HashSet<(usize, Fingerprint)>,
    event_idx: usize,
    /// Outer `None` = row-change not batched for this event (fall back to the
    /// per-row SELECT); `Some(inner)` mirrors `query_row_changes`' return.
    pub(super) row_change: Option<Option<&'a RowChanges>>,
}

impl BatchEvalView<'_> {
    /// Whether `fingerprint` was batch-evaluated (consult `hit` instead of a
    /// per-row round-trip). A rest query outside both covered sets falls back
    /// to the per-row path, whose `if !matched` guard skips it exactly as the
    /// pre-batch flow did.
    pub(super) fn covers(&self, fingerprint: Fingerprint) -> bool {
        self.fresh_fps.is_some_and(|fps| fps.contains(&fingerprint))
            || self.rest_fps.is_some_and(|fps| fps.contains(&fingerprint))
    }

    /// Whether this event's row matched `fingerprint`'s predicate.
    pub(super) fn hit(&self, fingerprint: Fingerprint) -> bool {
        self.hits.contains(&(self.event_idx, fingerprint))
    }
}

impl WriterCdc {
    /// Run both batch passes for a segment: PgEval membership, then row-change
    /// detection, into one [`SegmentMembership`] matrix (PGC-241).
    pub(super) async fn segment_eval(
        &mut self,
        core: &WriterCore,
        events: &[FrameRowEvent],
        base_idx: usize,
    ) -> CacheResult<SegmentMembership> {
        let mut membership = self.segment_membership_eval(core, events, base_idx).await?;
        self.segment_row_changes_eval(core, events, base_idx, &mut membership)
            .await?;
        Ok(membership)
    }

    /// Batch-evaluate PgEval membership for one segment of row events: per
    /// relation, every batchable query (`UpdateQuery::pg_batchable`) is
    /// evaluated against all the segment's rows in `UNION ALL`-combined
    /// multi-row VALUES statements — `⌈rows/PG_EVAL_ROW_CHUNK⌉ ×
    /// ⌈queries/PG_EVAL_CHUNK⌉` round-trips instead of one per row (PGC-241).
    ///
    /// The matrix is built unfiltered (no `frame_invalidations` / Fresh-MV
    /// partition); the decide pass applies those — they evolve as earlier
    /// events in the segment are decided. Reads run on `cache_eval_conn`'s
    /// pre-transaction snapshot, identical to the per-row path.
    async fn segment_membership_eval(
        &mut self,
        core: &WriterCore,
        events: &[FrameRowEvent],
        base_idx: usize,
    ) -> CacheResult<SegmentMembership> {
        let mut membership = SegmentMembership::default();

        // Bucket membership-eval rows per relation: inserts and updates (new
        // row image); deletes carry no membership question.
        let mut rows_by_relation: SegmentRows<'_> = HashMap::new();
        for (offset, event) in events.iter().enumerate() {
            let (relation_oid, row): (Oid, &[Option<ByteString>]) = match event {
                FrameRowEvent::Insert {
                    relation_oid,
                    row_data,
                } => (*relation_oid, row_data),
                FrameRowEvent::Update {
                    relation_oid,
                    new_row_data,
                    ..
                } => (*relation_oid, new_row_data),
                // UpdateToastFallback is excluded by design: its row image is
                // incomplete, so the decide pass invalidates instead of
                // evaluating membership from it. UpdateToasted no longer
                // exists at eval time (resolved by the repair pre-pass)
                // (PGC-264).
                FrameRowEvent::UpdateToasted { .. }
                | FrameRowEvent::UpdateToastFallback { .. }
                | FrameRowEvent::Delete { .. }
                | FrameRowEvent::Truncate { .. }
                | FrameRowEvent::Boundary { .. } => continue,
            };
            rows_by_relation
                .entry(relation_oid)
                .or_default()
                .push((base_idx + offset, row));
        }

        for (relation_oid, rows) in rows_by_relation {
            let Some(update_queries) = core.cache.update_queries.get(&relation_oid) else {
                continue;
            };
            let Some(table_metadata) = core.cache.tables.get1(&relation_oid) else {
                continue;
            };
            let batchable: Vec<&UpdateQuery> = update_queries
                .queries
                .values()
                .filter(|q| q.eval_strategy == UpdateEvalStrategy::PgEval && q.pg_batchable)
                .collect();
            if batchable.is_empty() {
                continue;
            }

            // A multi-row VALUES needs uniform arity; rows narrower than the
            // relation (e.g. truncated tuples) fall back to per-row eval by
            // staying out of `covered`.
            let full_width = table_metadata.columns.len();
            let batch_rows: Vec<(usize, &[Option<ByteString>])> = rows
                .into_iter()
                .filter(|(_, row)| row.len() == full_width)
                .collect();
            if batch_rows.is_empty() {
                continue;
            }

            // Mirror the per-row fresh/rest split: dirtyable-MV queries
            // (Fresh or Building) are always fully evaluated (every match
            // must dirty-mark)…
            let (fresh, rest): (Vec<&UpdateQuery>, Vec<&UpdateQuery>) = batchable
                .into_iter()
                .partition(|q| core.mv_dirty_eval_required(q.fingerprint));

            let batch = membership.relations.entry(relation_oid).or_default();
            if !fresh.is_empty() {
                self.membership_chunks_eval(table_metadata, &fresh, &batch_rows, &mut batch.hits)
                    .await?;
            }

            // …while rest queries only decide the shared-table upsert, so they
            // are only worth evaluating for rows nothing else matched. Gating
            // pre-pass: a row with a LocalEval match or a fresh hit needs no
            // rest eval — the per-row path's `if !matched` short-circuit, which
            // does zero PgEval round-trips for locally-matched rows. (A gating
            // miss is safe: uncovered rows fall back to per-row `pg_eval_any`,
            // itself guarded by `if !matched` at decide time.)
            let rest_rows: Vec<(usize, &[Option<ByteString>])> = if rest.is_empty() {
                Vec::new()
            } else {
                batch_rows
                    .iter()
                    .filter(|(event_idx, row)| {
                        if fresh
                            .iter()
                            .any(|q| batch.hits.contains(&(*event_idx, q.fingerprint)))
                        {
                            return false;
                        }
                        !update_queries.queries.values().any(|q| {
                            q.eval_strategy == UpdateEvalStrategy::LocalEval
                                && !core.frame_invalidations.contains(&q.fingerprint)
                                && update_query_matches_locally(q, table_metadata, row)
                        })
                    })
                    .copied()
                    .collect()
            };
            if !rest.is_empty() && !rest_rows.is_empty() {
                self.membership_chunks_eval(table_metadata, &rest, &rest_rows, &mut batch.hits)
                    .await?;
            }

            batch
                .covered
                .extend(batch_rows.iter().map(|(event_idx, _)| *event_idx));
            batch
                .rest_covered
                .extend(rest_rows.iter().map(|(event_idx, _)| *event_idx));
            batch.fresh_fps = fresh.iter().map(|q| q.fingerprint).collect();
            batch.rest_fps = rest.iter().map(|q| q.fingerprint).collect();
        }

        Ok(membership)
    }

    /// Evaluate `queries` against `rows` in `UNION ALL`-combined multi-row
    /// VALUES statements (`PG_EVAL_ROW_CHUNK` rows × `PG_EVAL_CHUNK` queries per
    /// statement), inserting `(event index, fingerprint)` matches into `hits`.
    async fn membership_chunks_eval(
        &mut self,
        table_metadata: &TableMetadata,
        queries: &[&UpdateQuery],
        rows: &[(usize, &[Option<ByteString>])],
        hits: &mut HashSet<(usize, Fingerprint)>,
    ) -> CacheResult<()> {
        for row_chunk in rows.chunks(PG_EVAL_ROW_CHUNK) {
            // Prepared per-query statements, pipelined (PGC-241 stage 4);
            // self-heal on failure: drop the cached statements and run the
            // inlined-VALUES form for this chunk (re-prepare on next use).
            if let Err(e) = self
                .membership_chunk_prepared(table_metadata, queries, row_chunk, hits)
                .await
            {
                warn!(
                    "prepared membership eval failed; falling back to inline: {}",
                    error_chain_format(e.current_context()),
                );
                self.membership_chunk_inline(table_metadata, queries, row_chunk, hits)
                    .await?;
            }
        }
        Ok(())
    }

    /// Prepared per-query membership for one row chunk (PGC-241 stage 4): one
    /// prepared statement per `(relation, query)` — shape fixed regardless of
    /// row count — bound to shared array parameters and executed concurrently,
    /// which tokio-postgres pipelines on `cache_eval_conn` in one flush.
    async fn membership_chunk_prepared(
        &mut self,
        table_metadata: &TableMetadata,
        queries: &[&UpdateQuery],
        row_chunk: &[(usize, &[Option<ByteString>])],
        hits: &mut HashSet<(usize, Fingerprint)>,
    ) -> CacheResult<()> {
        let (ordinals, column_arrays) = chunk_arrays_build(table_metadata, row_chunk);
        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(1 + column_arrays.len());
        params.push(&ordinals);
        for array in &column_arrays {
            params.push(array);
        }

        // Get-or-prepare. Misses prepare sequentially (cold cost, amortized:
        // steady state is all hits).
        let mut statements: Vec<(Fingerprint, Statement)> = Vec::with_capacity(queries.len());
        for update_query in queries {
            let key = PreparedEvalKey {
                relation_oid: table_metadata.relation_oid,
                fingerprint: update_query.fingerprint,
            };
            let statement = if let Some(statement) = self.prepared_membership.get(&key) {
                crate::metrics::handles().cdc.prepared_hits.increment(1);
                statement.clone()
            } else {
                crate::metrics::handles().cdc.prepared_misses.increment(1);
                let select = update_query
                    .resolved
                    .as_select()
                    .ok_or(CacheError::InvalidQuery)?;
                let unnest_select =
                    resolved_select_node_table_replace_with_unnest(select, table_metadata)
                        .map_err(|e| e.context_transform(CacheError::from))?;
                self.pg_eval_buf.clear();
                Deparse::deparse(&unnest_select, &mut self.pg_eval_buf);
                let statement = self
                    .cache_eval_conn
                    .prepare(&self.pg_eval_buf)
                    .await
                    .map_into_report::<CacheError>()?;
                self.prepared_membership.put(key, statement.clone());
                statement
            };
            statements.push((update_query.fingerprint, statement));
        }

        // Concurrent execution = pipelined on the single connection.
        let executions = future::join_all(
            statements
                .iter()
                .map(|(_, statement)| self.cache_eval_conn.query(statement, &params)),
        )
        .await;

        // Counted per successful execution only: failed statements re-run via
        // the inline fallback, which does its own counting.
        let mut executed = 0u64;
        let mut first_error = None;
        for ((fingerprint, _), execution) in statements.iter().zip(executions) {
            let result_rows = match execution {
                Ok(rows) => rows,
                Err(e) => {
                    // Self-heal: drop the (likely stale) statement; the caller
                    // falls back to inline for this chunk.
                    self.prepared_membership.pop(&PreparedEvalKey {
                        relation_oid: table_metadata.relation_oid,
                        fingerprint: *fingerprint,
                    });
                    first_error.get_or_insert(CacheError::PgError(e));
                    continue;
                }
            };
            executed += 1;
            for row in result_rows {
                let Ok(local_idx) = row.try_get::<_, i32>(0) else {
                    continue;
                };
                #[allow(clippy::cast_sign_loss)] // ordinals are 0..chunk len
                if let Some(&(event_idx, _)) = row_chunk.get(local_idx as usize) {
                    hits.insert((event_idx, *fingerprint));
                }
            }
        }
        crate::metrics::handles()
            .cdc
            .pg_eval_hits
            .increment(executed);
        match first_error {
            Some(e) => Err(e.into()),
            None => Ok(()),
        }
    }

    /// Inlined-VALUES membership for one row chunk: `UNION ALL`-combined arms
    /// per `PG_EVAL_CHUNK` queries via `simple_query`. The fallback when the
    /// prepared path fails (statement invalidated by DDL, etc.).
    async fn membership_chunk_inline(
        &mut self,
        table_metadata: &TableMetadata,
        queries: &[&UpdateQuery],
        row_chunk: &[(usize, &[Option<ByteString>])],
        hits: &mut HashSet<(usize, Fingerprint)>,
    ) -> CacheResult<()> {
        let chunk_rows: Vec<&[Option<ByteString>]> =
            row_chunk.iter().map(|(_, row)| *row).collect();
        for query_chunk in queries.chunks(PG_EVAL_CHUNK) {
            self.pg_eval_buf.clear();
            for (ordinal, update_query) in query_chunk.iter().enumerate() {
                if ordinal > 0 {
                    self.pg_eval_buf.push_str(" UNION ALL ");
                }
                let select = update_query
                    .resolved
                    .as_select()
                    .ok_or(CacheError::InvalidQuery)?;
                // Ordinal is bounded by PG_EVAL_CHUNK (32): never wraps.
                #[allow(clippy::cast_possible_wrap)]
                let batch_select = resolved_select_node_table_replace_with_values_batch(
                    select,
                    table_metadata,
                    &chunk_rows,
                    ordinal as i64,
                )
                .map_err(|e| e.context_transform(CacheError::from))?;
                Deparse::deparse(&batch_select, &mut self.pg_eval_buf);
            }

            let msgs = match self.cache_eval_conn.simple_query(&self.pg_eval_buf).await {
                Ok(m) => m,
                Err(e) => {
                    error!("batched predicate eval error: {}", error_chain_format(&e));
                    return Err(CacheError::PgError(e).into());
                }
            };
            crate::metrics::handles().cdc.pg_eval_hits.increment(1);
            for msg in msgs {
                let SimpleQueryMessage::Row(row) = msg else {
                    continue;
                };
                let (Some(ordinal), Some(local_idx)) = (
                    row.get(0).and_then(|v| v.parse::<usize>().ok()),
                    row.get(1).and_then(|v| v.parse::<usize>().ok()),
                ) else {
                    continue;
                };
                if let (Some(update_query), Some(&(event_idx, _))) =
                    (query_chunk.get(ordinal), row_chunk.get(local_idx))
                {
                    hits.insert((event_idx, update_query.fingerprint));
                }
            }
        }
        Ok(())
    }

    /// Batch the segment's row-change SELECTs (PGC-241 stage 3): per relation,
    /// every update event's `col IS DISTINCT FROM <new>` comparison runs in one
    /// statement per row chunk, joining the new tuples against the cache table
    /// by PK — `⌈K/PG_EVAL_ROW_CHUNK⌉` round-trips instead of one per row. A
    /// tuple absent from the join is the per-row "row not cached" (`None`)
    /// case: covered-but-absent in the matrix.
    ///
    /// Runs on `db_cache` like the per-row `query_row_changes`; all of a
    /// frame's reads see the same pre-frame committed state either way (the
    /// frame's own writes are buffered, or sit uncommitted on
    /// `cdc_write_conn`), so batching up front is snapshot-equivalent.
    async fn segment_row_changes_eval(
        &mut self,
        core: &WriterCore,
        events: &[FrameRowEvent],
        base_idx: usize,
        membership: &mut SegmentMembership,
    ) -> CacheResult<()> {
        let mut rows_by_relation: SegmentRows<'_> = HashMap::new();
        for (offset, event) in events.iter().enumerate() {
            let FrameRowEvent::Update {
                relation_oid,
                new_row_data,
                ..
            } = event
            else {
                continue;
            };
            // PGC-227: skip relations where no query's UPDATE invalidation
            // depends on changed columns — handle_update never reads changes.
            if !core
                .cache
                .update_queries
                .get(relation_oid)
                .is_some_and(|q| q.needs_change_eval())
            {
                continue;
            }
            rows_by_relation
                .entry(*relation_oid)
                .or_default()
                .push((base_idx + offset, new_row_data));
        }

        for (relation_oid, rows) in rows_by_relation {
            let Some(table_metadata) = core.cache.tables.get1(&relation_oid) else {
                continue;
            };
            if table_metadata.primary_key_columns.is_empty() {
                continue;
            }
            // Uniform VALUES arity, as in the membership batch.
            let full_width = table_metadata.columns.len();
            let batch_rows: Vec<(usize, &[Option<ByteString>])> = rows
                .into_iter()
                .filter(|(_, row)| row.len() == full_width)
                .collect();

            let batch = membership.relations.entry(relation_oid).or_default();
            for row_chunk in batch_rows.chunks(PG_EVAL_ROW_CHUNK) {
                // Prepared per-relation statement (PGC-241 stage 4); self-heal
                // on failure: drop the cached statement and run the inlined
                // form for this chunk (re-prepare on next use).
                if let Err(e) = self
                    .row_change_chunk_prepared(core, table_metadata, row_chunk, batch)
                    .await
                {
                    warn!(
                        "prepared row-change eval failed; falling back to inline: {}",
                        error_chain_format(e.current_context()),
                    );
                    self.row_change_chunk_inline(core, table_metadata, row_chunk, batch)
                        .await?;
                }
            }

            batch
                .row_change_covered
                .extend(batch_rows.iter().map(|(event_idx, _)| *event_idx));
        }

        Ok(())
    }

    /// Prepared row-change for one chunk: one statement per relation —
    /// `unnest()` array params, shape fixed regardless of row count — on
    /// `db_cache`, matching the per-row `query_row_changes` connection.
    async fn row_change_chunk_prepared(
        &mut self,
        core: &WriterCore,
        table_metadata: &TableMetadata,
        row_chunk: &[(usize, &[Option<ByteString>])],
        batch: &mut RelationBatch,
    ) -> CacheResult<()> {
        let relation_oid = table_metadata.relation_oid;
        let update_queries = core.cache.update_queries.get(&relation_oid);
        // The projection list depends on the relation's registered queries
        // (their ORDER BY key columns) — a stale statement would silently drop
        // the ordering projections, so the epoch is part of the cache hit.
        let epoch = update_queries.map_or(0, UpdateQueries::epoch);
        let cached = self
            .prepared_row_change
            .get(&relation_oid)
            .filter(|(cached_epoch, _)| *cached_epoch == epoch);
        let statement = if let Some((_, statement)) = cached {
            crate::metrics::handles().cdc.prepared_hits.increment(1);
            statement.clone()
        } else {
            crate::metrics::handles().cdc.prepared_misses.increment(1);
            self.pg_eval_buf.clear();
            row_change_select_into(
                &mut self.pg_eval_buf,
                table_metadata,
                &relation_order_columns(update_queries),
            );
            let _ = write!(
                self.pg_eval_buf,
                " FROM (SELECT unnest($1::int4[]) AS {BATCH_IDX_COLUMN}"
            );
            for (i, column_meta) in table_metadata.columns.iter().enumerate() {
                let _ = write!(
                    self.pg_eval_buf,
                    ", unnest(${}::text[])::{} AS {}",
                    i + 2,
                    column_meta.cache_type_name,
                    column_meta.name
                );
            }
            self.pg_eval_buf.push_str(") AS v");
            row_change_join_on_into(&mut self.pg_eval_buf, table_metadata);
            let statement = core
                .db_cache
                .prepare(&self.pg_eval_buf)
                .await
                .map_into_report::<CacheError>()?;
            self.prepared_row_change
                .put(relation_oid, (epoch, statement.clone()));
            statement
        };

        let (ordinals, column_arrays) = chunk_arrays_build(table_metadata, row_chunk);
        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(1 + column_arrays.len());
        params.push(&ordinals);
        for array in &column_arrays {
            params.push(array);
        }

        let result_rows = match core.db_cache.query(&statement, &params).await {
            Ok(rows) => rows,
            Err(e) => {
                // Self-heal: drop the (likely stale) statement; the caller
                // falls back to inline for this chunk.
                self.prepared_row_change.pop(&relation_oid);
                return Err(CacheError::PgError(e).into());
            }
        };
        for row in result_rows {
            let Ok(local_idx) = row.try_get::<_, i32>(0) else {
                continue;
            };
            #[allow(clippy::cast_sign_loss)] // ordinals are 0..chunk len
            let Some(&(event_idx, _)) = row_chunk.get(local_idx as usize) else {
                continue;
            };
            let mut changes = RowChanges::with_capacity(row.len().saturating_sub(1));
            for (col_idx, col) in row.columns().iter().enumerate().skip(1) {
                row_change_column_fold(
                    &mut changes,
                    col.name(),
                    row.try_get::<_, Option<bool>>(col_idx).ok().flatten(),
                );
            }
            batch.row_changes.insert(event_idx, changes);
        }
        Ok(())
    }

    /// Inlined-VALUES row-change for one chunk via `simple_query` — the
    /// fallback when the prepared path fails.
    async fn row_change_chunk_inline(
        &mut self,
        core: &WriterCore,
        table_metadata: &TableMetadata,
        row_chunk: &[(usize, &[Option<ByteString>])],
        batch: &mut RelationBatch,
    ) -> CacheResult<()> {
        self.pg_eval_buf.clear();
        row_change_select_into(
            &mut self.pg_eval_buf,
            table_metadata,
            &relation_order_columns(core.cache.update_queries.get(&table_metadata.relation_oid)),
        );
        self.pg_eval_buf.push_str(" FROM (VALUES ");
        for (i, (_, row)) in row_chunk.iter().enumerate() {
            if i > 0 {
                self.pg_eval_buf.push_str(", ");
            }
            let _ = write!(self.pg_eval_buf, "({i}");
            for column_meta in &table_metadata.columns {
                let value = row
                    .get(column_meta.index())
                    .and_then(|v| v.as_deref())
                    .map_or_else(|| "NULL".to_owned(), escape::escape_literal);
                let _ = write!(
                    self.pg_eval_buf,
                    ", {value}::{}",
                    column_meta.cache_type_name
                );
            }
            self.pg_eval_buf.push(')');
        }
        let _ = write!(self.pg_eval_buf, ") AS v({BATCH_IDX_COLUMN}");
        for column_meta in &table_metadata.columns {
            let _ = write!(self.pg_eval_buf, ", {}", column_meta.name);
        }
        self.pg_eval_buf.push(')');
        row_change_join_on_into(&mut self.pg_eval_buf, table_metadata);

        let msgs = match core.db_cache.simple_query(&self.pg_eval_buf).await {
            Ok(m) => m,
            Err(e) => {
                error!("batched row-change eval error: {}", error_chain_format(&e));
                return Err(CacheError::PgError(e).into());
            }
        };
        for msg in msgs {
            let SimpleQueryMessage::Row(row) = msg else {
                continue;
            };
            let Some(local_idx) = row.get(0).and_then(|v| v.parse::<usize>().ok()) else {
                continue;
            };
            let Some(&(event_idx, _)) = row_chunk.get(local_idx) else {
                continue;
            };
            let mut changes = RowChanges::with_capacity(row.len().saturating_sub(1));
            for (col_idx, col) in row.columns().iter().enumerate().skip(1) {
                row_change_column_fold(&mut changes, col.name(), pg_bool_text(row.get(col_idx)));
            }
            batch.row_changes.insert(event_idx, changes);
        }
        Ok(())
    }
}
