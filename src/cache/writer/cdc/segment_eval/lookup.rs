//! The batched row-change / old-image lookup (PGC-241 stage 3, PGC-255): per
//! relation, the queued update and delete tuples are joined against the cache
//! table by PK in one statement per row chunk, projecting changed-column
//! booleans and the rung-2 old-image values.

use std::collections::HashSet;
use std::fmt::Write;

use ecow::EcoString;
use postgres_protocol::escape;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, Row, SimpleQueryMessage, SimpleQueryRow, Statement};
use tracing::{error, warn};

use super::old_image::{
    OldImagePrepass, RelationFetchSpec, old_image_bool_normalize, old_image_expand,
};
use super::{RelationBatch, SegmentMembership, chunk_arrays_build};
use crate::cache::update_query::{RowChanges, UpdateQueries};
use crate::cache::writer::cdc::row_changes::{
    RowChangeProjection, relation_order_columns, row_change_column_fold,
};
use crate::cache::writer::cdc::{PG_EVAL_ROW_CHUNK, WriterCdc};
use crate::cache::writer::core::WriterCore;
use crate::cache::writer::frame::FrameRowEvent;
use crate::cache::{CacheError, CacheResult, MapIntoReport};
use crate::catalog::TableMetadata;
use crate::pg::protocol::ByteString;
use crate::query::evaluate::bool_wire_text_parse;
use crate::query::transform::BATCH_IDX_COLUMN;
use crate::result::error_chain_format;

/// Alias prefix for the old-image value projections (`o.<col>::text`) the
/// lookup SELECT emits for eval-index columns (PGC-255).
const OLD_VALUE_ALIAS_PREFIX: &str = "__pgc_ov_";

/// Byte cap on fetched old-image values for unbounded text columns
/// (`pg_column_size`, which reads the stored size without detoasting). An
/// over-cap value ships NULL → `None` in the expanded image → the per-column
/// `Unknown` wildcard — conservative, and it keeps `WHERE body = '…'`-style
/// constraints from dragging whole documents through every lookup chunk.
const OLD_VALUE_TEXT_FETCH_CAP: usize = 512;

/// One row of the per-relation row-change/old-image lookup batch. The join
/// row is the new tuple for updates and the delete tuple for deletes;
/// `wants_changes` marks update events on change-eval relations (their
/// `IS DISTINCT FROM` booleans are consumed), `wants_old_image` marks events
/// whose old image the rung-1 overlay didn't resolve and should be filled
/// from the lookup's `o.<col>` values (PGC-255).
pub(super) struct LookupRow<'a> {
    pub(super) event_idx: usize,
    pub(super) row: &'a [Option<ByteString>],
    pub(super) wants_changes: bool,
    pub(super) wants_old_image: bool,
}

/// One relation's lookup inputs: the connection it runs on, the table, its
/// fetch spec, and its registered queries (which fix the ORDER BY projections).
struct RelationLookup<'a> {
    db: &'a Client,
    table_metadata: &'a TableMetadata,
    spec: &'a RelationFetchSpec,
    update_queries: Option<&'a UpdateQueries>,
}

/// Append the row-change SELECT list — `SELECT v.<idx>, o.X IS DISTINCT FROM
/// v.X AS X, …` — via the projection shared with the per-row builder.
/// Limit-window ORDER BY key columns additionally project old-vs-new ordering
/// for the window-direction check (PGC-334).
fn row_change_select_into(
    buf: &mut String,
    table_metadata: &TableMetadata,
    order_columns: HashSet<&EcoString>,
    spec: &RelationFetchSpec,
) {
    buf.push_str("SELECT v.");
    buf.push_str(BATCH_IDX_COLUMN);
    if spec.needs_changes {
        let projection = RowChangeProjection {
            old_qualifier: "o.",
            order_columns,
        };
        for column_meta in &table_metadata.columns {
            let name = escape::escape_identifier(&column_meta.name);
            buf.push_str(", ");
            projection.column_into(buf, &column_meta.name, format_args!("v.{name}"));
        }
    }
    // Old-image values for the wire-canonical eval-index columns (PGC-255),
    // cast to text so both the typed (prepared) and simple-query parses read
    // them uniformly.
    for column in &spec.fetch_columns {
        let name = escape::escape_identifier(column);
        let alias = escape::escape_identifier(&format!("{OLD_VALUE_ALIAS_PREFIX}{column}"));
        let wide = table_metadata
            .columns
            .get(column.as_str())
            .is_some_and(|m| matches!(m.cache_type_name.as_str(), "text" | "varchar"));
        if wide {
            let _ = write!(
                buf,
                ", CASE WHEN pg_column_size(o.{name}) <= {OLD_VALUE_TEXT_FETCH_CAP}                  THEN o.{name}::text END AS {alias}"
            );
        } else {
            let _ = write!(buf, ", o.{name}::text AS {alias}");
        }
    }
}

/// Append the row-change PK join — ` JOIN <schema>.<table> o ON o.pk = v.pk…`
/// — shared by the prepared and inline row-change builders.
fn row_change_join_on_into(buf: &mut String, table_metadata: &TableMetadata) {
    let _ = write!(
        buf,
        " JOIN {}.{} o ON ",
        escape::escape_identifier(&table_metadata.schema),
        escape::escape_identifier(&table_metadata.name)
    );
    for (i, pk_column) in table_metadata.primary_key_columns.iter().enumerate() {
        if i > 0 {
            buf.push_str(" AND ");
        }
        let pk = escape::escape_identifier(pk_column);
        let _ = write!(buf, "o.{pk} = v.{pk}");
    }
}

/// Prepared form: the lookup tuples arrive as `unnest()`ed array parameters,
/// so the statement's shape is fixed regardless of row count.
fn row_change_prepared_sql_into(buf: &mut String, lookup: &RelationLookup<'_>) {
    let table_metadata = lookup.table_metadata;
    row_change_select_into(
        buf,
        table_metadata,
        relation_order_columns(lookup.update_queries),
        lookup.spec,
    );
    let _ = write!(
        buf,
        " FROM (SELECT unnest($1::int4[]) AS {BATCH_IDX_COLUMN}"
    );
    for (i, column_meta) in table_metadata.columns.iter().enumerate() {
        let _ = write!(
            buf,
            ", unnest(${}::text[])::{} AS {}",
            i + 2,
            column_meta.cache_type_name,
            escape::escape_identifier(&column_meta.name)
        );
    }
    buf.push_str(") AS v");
    row_change_join_on_into(buf, table_metadata);
}

/// Inline form: the lookup tuples are rendered as a `VALUES` list.
fn row_change_inline_sql_into(
    buf: &mut String,
    lookup: &RelationLookup<'_>,
    row_chunk: &[LookupRow<'_>],
) {
    let table_metadata = lookup.table_metadata;
    row_change_select_into(
        buf,
        table_metadata,
        relation_order_columns(lookup.update_queries),
        lookup.spec,
    );
    buf.push_str(" FROM (VALUES ");
    for (i, lookup_row) in row_chunk.iter().enumerate() {
        if i > 0 {
            buf.push_str(", ");
        }
        let _ = write!(buf, "({i}");
        for column_meta in &table_metadata.columns {
            let value = lookup_row
                .row
                .get(column_meta.index())
                .and_then(|v| v.as_deref())
                .map_or_else(|| "NULL".to_owned(), escape::escape_literal);
            let _ = write!(buf, ", {value}::{}", column_meta.cache_type_name);
        }
        buf.push(')');
    }
    let _ = write!(buf, ") AS v({BATCH_IDX_COLUMN}");
    for column_meta in &table_metadata.columns {
        let _ = write!(buf, ", {}", escape::escape_identifier(&column_meta.name));
    }
    buf.push(')');
    row_change_join_on_into(buf, table_metadata);
}

/// Cell access common to a typed (prepared) and a simple-query result row.
trait LookupCells {
    fn cell_count(&self) -> usize;
    fn column_name(&self, idx: usize) -> Option<&str>;
    /// An old-image value, cast to text by the query.
    fn text(&self, idx: usize) -> Option<&str>;
    /// A changed-column / ordering boolean.
    fn flag(&self, idx: usize) -> Option<bool>;
}

impl LookupCells for Row {
    fn cell_count(&self) -> usize {
        self.len()
    }

    fn column_name(&self, idx: usize) -> Option<&str> {
        self.columns().get(idx).map(|c| c.name())
    }

    fn text(&self, idx: usize) -> Option<&str> {
        self.try_get(idx).ok().flatten()
    }

    fn flag(&self, idx: usize) -> Option<bool> {
        self.try_get::<_, Option<bool>>(idx).ok().flatten()
    }
}

impl LookupCells for SimpleQueryRow {
    fn cell_count(&self) -> usize {
        self.len()
    }

    fn column_name(&self, idx: usize) -> Option<&str> {
        self.columns().get(idx).map(|c| c.name())
    }

    fn text(&self, idx: usize) -> Option<&str> {
        self.get(idx)
    }

    fn flag(&self, idx: usize) -> Option<bool> {
        self.get(idx).and_then(bool_wire_text_parse)
    }
}

/// Fold one lookup result row (cell 0 is the batch ordinal) into the matrix:
/// its changed-column booleans and/or its rung-2 old image.
fn lookup_result_apply(
    batch: &mut RelationBatch,
    table_metadata: &TableMetadata,
    lookup_row: &LookupRow<'_>,
    row: &impl LookupCells,
) {
    let mut changes = RowChanges::with_capacity(row.cell_count().saturating_sub(1));
    let mut old_values: Vec<(usize, Option<ByteString>)> = Vec::new();
    for idx in 1..row.cell_count() {
        let Some(name) = row.column_name(idx) else {
            continue;
        };
        match name.strip_prefix(OLD_VALUE_ALIAS_PREFIX) {
            Some(column) if lookup_row.wants_old_image => {
                if let Some(meta) = table_metadata.columns.get(column) {
                    let value = row
                        .text(idx)
                        .map(|v| ByteString::from(old_image_bool_normalize(meta, v)));
                    old_values.push((meta.index(), value));
                }
            }
            Some(_) => {}
            // Reserved-prefix relations never enter the batch.
            None => row_change_column_fold(&mut changes, name, row.flag(idx), true),
        }
    }
    if lookup_row.wants_changes {
        batch.row_changes.insert(lookup_row.event_idx, changes);
    }
    if lookup_row.wants_old_image {
        // Overlay resolutions were never queued as wanting old images, so
        // vacancy is the norm; keep them authoritative regardless.
        batch
            .old_images
            .entry(lookup_row.event_idx)
            .or_insert_with(|| old_image_expand(table_metadata.columns.len(), &old_values));
    }
}

impl WriterCdc {
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
    pub(super) async fn segment_row_changes_eval(
        &mut self,
        core: &mut WriterCore,
        events: &[FrameRowEvent],
        base_idx: usize,
        membership: &mut SegmentMembership,
    ) -> CacheResult<()> {
        let mut prepass = OldImagePrepass::new(core, membership);
        for (offset, event) in events.iter().enumerate() {
            prepass.event_record(base_idx + offset, event);
        }
        let (specs, lookups) = prepass.into_parts();

        let core = &*core;
        for (relation_oid, rows) in lookups {
            let (Some(table_metadata), Some(spec)) = (
                core.cache.tables.get1(&relation_oid),
                specs.get(&relation_oid),
            ) else {
                continue;
            };
            if table_metadata.primary_key_columns.is_empty() {
                continue;
            }
            let lookup = RelationLookup {
                db: &core.db_cache,
                table_metadata,
                spec,
                update_queries: core.cache.update_queries.get(&relation_oid),
            };
            let batch = membership.relation_batch(relation_oid);
            self.relation_lookup_eval(&lookup, rows, batch).await?;
        }
        Ok(())
    }

    async fn relation_lookup_eval(
        &mut self,
        lookup: &RelationLookup<'_>,
        rows: Vec<LookupRow<'_>>,
        batch: &mut RelationBatch,
    ) -> CacheResult<()> {
        // Uniform VALUES arity, as in the membership batch.
        let full_width = lookup.table_metadata.columns.len();
        let batch_rows: Vec<LookupRow<'_>> = rows
            .into_iter()
            .filter(|r| r.row.len() == full_width)
            .collect();

        for row_chunk in batch_rows.chunks(PG_EVAL_ROW_CHUNK) {
            // Prepared per-relation statement (PGC-241 stage 4); self-heal on
            // failure: drop the cached statement and run the inlined form for
            // this chunk (re-prepare on next use).
            if let Err(e) = self
                .row_change_chunk_prepared(lookup, row_chunk, batch)
                .await
            {
                warn!(
                    "prepared row-change eval failed; falling back to inline: {}",
                    error_chain_format(e.current_context()),
                );
                self.row_change_chunk_inline(lookup, row_chunk, batch)
                    .await?;
            }
        }

        batch.row_change_covered.extend(
            batch_rows
                .iter()
                .filter(|r| r.wants_changes)
                .map(|r| r.event_idx),
        );
        Ok(())
    }

    /// Get-or-prepare the relation's row-change statement. The projection list
    /// depends on the relation's registered queries (ORDER BY key columns,
    /// eval-index columns) — a stale statement would silently drop
    /// projections, so the epoch is part of the cache hit.
    async fn row_change_statement(
        &mut self,
        lookup: &RelationLookup<'_>,
    ) -> CacheResult<Statement> {
        let relation_oid = lookup.table_metadata.relation_oid;
        let epoch = lookup.spec.epoch;
        let cached = self
            .prepared_row_change
            .get(&relation_oid)
            .filter(|(cached_epoch, _)| *cached_epoch == epoch);
        if let Some((_, statement)) = cached {
            crate::metrics::handles().cdc.prepared_hits.increment(1);
            return Ok(statement.clone());
        }
        crate::metrics::handles().cdc.prepared_misses.increment(1);
        self.pg_eval_buf.clear();
        row_change_prepared_sql_into(&mut self.pg_eval_buf, lookup);
        let statement = lookup
            .db
            .prepare(&self.pg_eval_buf)
            .await
            .map_into_report::<CacheError>()?;
        self.prepared_row_change
            .put(relation_oid, (epoch, statement.clone()));
        Ok(statement)
    }

    /// Prepared row-change for one chunk: one statement per relation —
    /// `unnest()` array params, shape fixed regardless of row count — on
    /// `db_cache`, matching the per-row `query_row_changes` connection.
    async fn row_change_chunk_prepared(
        &mut self,
        lookup: &RelationLookup<'_>,
        row_chunk: &[LookupRow<'_>],
        batch: &mut RelationBatch,
    ) -> CacheResult<()> {
        let statement = self.row_change_statement(lookup).await?;
        let (ordinals, column_arrays) =
            chunk_arrays_build(lookup.table_metadata, row_chunk, |r| r.row);
        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(1 + column_arrays.len());
        params.push(&ordinals);
        for array in &column_arrays {
            params.push(array);
        }

        let result_rows = match lookup.db.query(&statement, &params).await {
            Ok(rows) => rows,
            Err(e) => {
                // Self-heal: drop the (likely stale) statement; the caller
                // falls back to inline for this chunk.
                self.prepared_row_change
                    .pop(&lookup.table_metadata.relation_oid);
                return Err(CacheError::PgError(e).into());
            }
        };
        for row in result_rows {
            let Ok(local_idx) = row.try_get::<_, i32>(0) else {
                continue;
            };
            #[allow(clippy::cast_sign_loss)] // ordinals are 0..chunk len
            if let Some(lookup_row) = row_chunk.get(local_idx as usize) {
                lookup_result_apply(batch, lookup.table_metadata, lookup_row, &row);
            }
        }
        Ok(())
    }

    /// Inlined-VALUES row-change for one chunk via `simple_query` — the
    /// fallback when the prepared path fails.
    async fn row_change_chunk_inline(
        &mut self,
        lookup: &RelationLookup<'_>,
        row_chunk: &[LookupRow<'_>],
        batch: &mut RelationBatch,
    ) -> CacheResult<()> {
        self.pg_eval_buf.clear();
        row_change_inline_sql_into(&mut self.pg_eval_buf, lookup, row_chunk);
        let msgs = match lookup.db.simple_query(&self.pg_eval_buf).await {
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
            let lookup_row = row
                .get(0)
                .and_then(|v| v.parse::<usize>().ok())
                .and_then(|local_idx| row_chunk.get(local_idx));
            if let Some(lookup_row) = lookup_row {
                lookup_result_apply(batch, lookup.table_metadata, lookup_row, &row);
            }
        }
        Ok(())
    }
}
