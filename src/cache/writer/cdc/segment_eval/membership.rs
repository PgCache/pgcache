//! Batched PgEval membership for one segment: every batchable query evaluated
//! against all the segment's rows in multi-row statements.

use std::collections::{HashMap, HashSet};

use futures_util::future;
use tokio_postgres::types::ToSql;
use tokio_postgres::{SimpleQueryMessage, Statement};
use tracing::{error, warn};

use super::{PreparedEvalKey, RelationBatch, SegmentMembership, chunk_arrays_build};
use crate::cache::update_query::{UpdateEvalStrategy, UpdateQueries, UpdateQuery};
use crate::cache::writer::cdc::row_match::update_query_matches_locally;
use crate::cache::writer::cdc::{PG_EVAL_CHUNK, PG_EVAL_ROW_CHUNK, WriterCdc};
use crate::cache::writer::core::WriterCore;
use crate::cache::writer::frame::{FrameRowEvent, ToastState};
use crate::cache::{CacheError, CacheResult, MapIntoReport};
use crate::catalog::TableMetadata;
use crate::oid::Oid;
use crate::pg::protocol::ByteString;
use crate::query::Fingerprint;
use crate::query::ast::Deparse;
use crate::query::transform::{
    resolved_select_node_table_replace_with_unnest,
    resolved_select_node_table_replace_with_values_batch,
};
use crate::result::error_chain_format;

/// One membership-eval row: `(event index, row image)`.
type EventRow<'a> = (usize, &'a [Option<ByteString>]);

/// The row image an event asks a membership question about: the new row for
/// inserts and updates. Deletes carry no membership question.
/// An `Unrepaired` toast image is excluded by design: it is incomplete, so
/// the decide pass invalidates instead of evaluating membership from it.
/// `Pending` no longer exists at eval time (resolved by the repair pre-pass)
/// (PGC-264).
fn event_membership_row(event: &FrameRowEvent) -> Option<(Oid, &[Option<ByteString>])> {
    match event {
        FrameRowEvent::Insert {
            relation_oid,
            row_data,
        } => Some((*relation_oid, row_data)),
        FrameRowEvent::Update {
            relation_oid,
            new_row_data,
            toast: ToastState::Complete,
            ..
        } => Some((*relation_oid, new_row_data)),
        FrameRowEvent::Update {
            toast: ToastState::Pending(_) | ToastState::Unrepaired(_),
            ..
        }
        | FrameRowEvent::Delete { .. }
        | FrameRowEvent::Truncate { .. }
        | FrameRowEvent::Boundary { .. } => None,
    }
}

/// Bucket the segment's membership-eval rows per relation.
fn segment_rows_bucket(
    events: &[FrameRowEvent],
    base_idx: usize,
) -> HashMap<Oid, Vec<EventRow<'_>>> {
    let mut rows_by_relation: HashMap<Oid, Vec<EventRow<'_>>> = HashMap::new();
    for (offset, event) in events.iter().enumerate() {
        if let Some((relation_oid, row)) = event_membership_row(event) {
            rows_by_relation
                .entry(relation_oid)
                .or_default()
                .push((base_idx + offset, row));
        }
    }
    rows_by_relation
}

/// Whether some fresh query already matched this event.
fn fresh_hit(
    fresh: &[&UpdateQuery],
    hits: &HashSet<(usize, Fingerprint)>,
    event_idx: usize,
) -> bool {
    fresh
        .iter()
        .any(|q| hits.contains(&(event_idx, q.fingerprint)))
}

/// Whether a still-maintained LocalEval query matches the row.
fn local_eval_match_any(
    core: &WriterCore,
    update_queries: &UpdateQueries,
    row: &[Option<ByteString>],
) -> bool {
    update_queries.queries.values().any(|q| {
        q.eval_strategy == UpdateEvalStrategy::LocalEval
            && !core.frame_invalidations.contains(&q.fingerprint)
            && update_query_matches_locally(q, row)
    })
}

impl WriterCdc {
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
    pub(super) async fn segment_membership_eval(
        &mut self,
        core: &WriterCore,
        events: &[FrameRowEvent],
        base_idx: usize,
    ) -> CacheResult<SegmentMembership> {
        let mut membership = SegmentMembership::default();
        for (relation_oid, rows) in segment_rows_bucket(events, base_idx) {
            self.relation_membership_eval(core, relation_oid, rows, &mut membership)
                .await?;
        }
        Ok(membership)
    }

    async fn relation_membership_eval(
        &mut self,
        core: &WriterCore,
        relation_oid: Oid,
        rows: Vec<EventRow<'_>>,
        membership: &mut SegmentMembership,
    ) -> CacheResult<()> {
        let (Some(update_queries), Some(table_metadata)) = (
            core.cache.update_queries.get(&relation_oid),
            core.cache.tables.get1(&relation_oid),
        ) else {
            return Ok(());
        };
        let batchable: Vec<&UpdateQuery> = update_queries
            .queries
            .values()
            .filter(|q| q.eval_strategy == UpdateEvalStrategy::PgEval && q.pg_batchable)
            .collect();
        // A multi-row VALUES needs uniform arity; rows narrower than the
        // relation (e.g. truncated tuples) fall back to per-row eval by
        // staying out of `covered`.
        let full_width = table_metadata.columns.len();
        let batch_rows: Vec<EventRow<'_>> = rows
            .into_iter()
            .filter(|(_, row)| row.len() == full_width)
            .collect();
        if batchable.is_empty() || batch_rows.is_empty() {
            return Ok(());
        }

        // Mirror the per-row fresh/rest split: dirtyable-MV queries (Fresh or
        // Building) are always fully evaluated (every match must dirty-mark)…
        let (fresh, rest): (Vec<&UpdateQuery>, Vec<&UpdateQuery>) = batchable
            .into_iter()
            .partition(|q| core.mv_dirty_eval_required(q.fingerprint));

        let batch = membership.relation_batch(relation_oid);
        self.membership_chunks_eval(table_metadata, &fresh, &batch_rows, &mut batch.hits)
            .await?;

        // …while rest queries only decide the shared-table upsert, so they are
        // only worth evaluating for rows nothing else matched. Gating pre-pass:
        // a row with a LocalEval match or a fresh hit needs no rest eval — the
        // per-row path's `if !matched` short-circuit, which does zero PgEval
        // round-trips for locally-matched rows. (A gating miss is safe:
        // uncovered rows fall back to per-row `pg_eval_any`, itself guarded by
        // `if !matched` at decide time.)
        let rest_rows: Vec<EventRow<'_>> = if rest.is_empty() {
            Vec::new()
        } else {
            batch_rows
                .iter()
                .filter(|(event_idx, row)| {
                    !fresh_hit(&fresh, &batch.hits, *event_idx)
                        && !local_eval_match_any(core, update_queries, row)
                })
                .copied()
                .collect()
        };
        self.membership_chunks_eval(table_metadata, &rest, &rest_rows, &mut batch.hits)
            .await?;

        batch.coverage_record(&fresh, &rest, &batch_rows, &rest_rows);
        Ok(())
    }

    /// Evaluate `queries` against `rows` in `UNION ALL`-combined multi-row
    /// VALUES statements (`PG_EVAL_ROW_CHUNK` rows × `PG_EVAL_CHUNK` queries per
    /// statement), inserting `(event index, fingerprint)` matches into `hits`.
    /// No-op when either side is empty.
    async fn membership_chunks_eval(
        &mut self,
        table_metadata: &TableMetadata,
        queries: &[&UpdateQuery],
        rows: &[EventRow<'_>],
        hits: &mut HashSet<(usize, Fingerprint)>,
    ) -> CacheResult<()> {
        if queries.is_empty() {
            return Ok(());
        }
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

    /// Get-or-prepare the per-`(relation, query)` membership statement. Misses
    /// prepare sequentially (cold cost, amortized: steady state is all hits).
    async fn membership_statement(
        &mut self,
        table_metadata: &TableMetadata,
        update_query: &UpdateQuery,
    ) -> CacheResult<Statement> {
        let key = PreparedEvalKey {
            relation_oid: table_metadata.relation_oid,
            fingerprint: update_query.fingerprint,
        };
        if let Some(statement) = self.prepared_membership.get(&key) {
            crate::metrics::handles().cdc.prepared_hits.increment(1);
            return Ok(statement.clone());
        }
        crate::metrics::handles().cdc.prepared_misses.increment(1);
        let select = update_query
            .resolved
            .as_select()
            .ok_or(CacheError::InvalidQuery)?;
        let unnest_select = resolved_select_node_table_replace_with_unnest(select, table_metadata)
            .map_err(|e| e.context_transform(CacheError::from))?;
        self.pg_eval_buf.clear();
        Deparse::deparse(&unnest_select, &mut self.pg_eval_buf);
        let statement = self
            .cache_eval_conn
            .prepare(&self.pg_eval_buf)
            .await
            .map_into_report::<CacheError>()?;
        self.prepared_membership.put(key, statement.clone());
        Ok(statement)
    }

    /// Prepared per-query membership for one row chunk (PGC-241 stage 4): one
    /// prepared statement per `(relation, query)` — shape fixed regardless of
    /// row count — bound to shared array parameters and executed concurrently,
    /// which tokio-postgres pipelines on `cache_eval_conn` in one flush.
    async fn membership_chunk_prepared(
        &mut self,
        table_metadata: &TableMetadata,
        queries: &[&UpdateQuery],
        row_chunk: &[EventRow<'_>],
        hits: &mut HashSet<(usize, Fingerprint)>,
    ) -> CacheResult<()> {
        let (ordinals, column_arrays) =
            chunk_arrays_build(table_metadata, row_chunk, |&(_, row)| row);
        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(1 + column_arrays.len());
        params.push(&ordinals);
        for array in &column_arrays {
            params.push(array);
        }

        let mut statements: Vec<(Fingerprint, Statement)> = Vec::with_capacity(queries.len());
        for update_query in queries {
            let statement = self
                .membership_statement(table_metadata, update_query)
                .await?;
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
        row_chunk: &[EventRow<'_>],
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
}

impl RelationBatch {
    /// Record which events and fingerprints the membership batch covered, so
    /// the decide pass knows when to consult `hits`.
    fn coverage_record(
        &mut self,
        fresh: &[&UpdateQuery],
        rest: &[&UpdateQuery],
        batch_rows: &[EventRow<'_>],
        rest_rows: &[EventRow<'_>],
    ) {
        self.covered
            .extend(batch_rows.iter().map(|(event_idx, _)| *event_idx));
        self.rest_covered
            .extend(rest_rows.iter().map(|(event_idx, _)| *event_idx));
        self.fresh_fps = fresh.iter().map(|q| q.fingerprint).collect();
        self.rest_fps = rest.iter().map(|q| q.fingerprint).collect();
    }
}
