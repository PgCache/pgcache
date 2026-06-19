use crate::catalog::Oid;
use crate::query::Fingerprint;
use std::fmt::Write;

use postgres_protocol::escape;
use tokio_postgres::{Client, SimpleQueryMessage, SimpleQueryRow};
use tracing::{error, instrument, trace, warn};

use crate::catalog::TableMetadata;
use crate::pg::protocol::ByteString;

use crate::query::ast::Deparse;
use crate::query::resolved::ResolvedQueryExpr;
use crate::query::transform::resolved_select_node_table_replace_with_values;

use super::super::super::types::UpdateQuery;
use super::super::super::{CacheError, CacheResult};
use super::super::core::WriterCore;
use crate::result::error_chain_format;

use super::*;

/// Append the tail of an upsert SQL: either ` DO UPDATE SET <non-pk cols>` or
/// ` DO NOTHING` if the table has no non-PK columns. PG rejects `DO UPDATE SET`
/// with an empty SET list, so PK-only tables must use `DO NOTHING`.
///
/// Assumes the caller has already emitted `INSERT INTO ... ON CONFLICT (<pk>)`.
fn cdc_on_conflict_tail_append(
    sql: &mut String,
    table_metadata: &TableMetadata,
    row_data: &[Option<ByteString>],
) {
    let is_pk = |name: &str| {
        table_metadata
            .primary_key_columns
            .iter()
            .any(|pk| pk.as_str() == name)
    };
    let mut first = true;
    for column_meta in &table_metadata.columns {
        if row_data.get(column_meta.index()).is_none() || is_pk(column_meta.name.as_str()) {
            continue;
        }
        if first {
            sql.push_str(" DO UPDATE SET ");
        } else {
            sql.push_str(", ");
        }
        let col = column_meta.name.as_str();
        let _ = write!(sql, "{col} = EXCLUDED.{col}");
        first = false;
    }
    if first {
        sql.push_str(" DO NOTHING");
    }
}

impl WriterCdc {
    /// Build `TRUNCATE <cache table>, ...` for the relations' cache tables,
    /// or `None` if none of the oids map to a known cache table. Shared by
    /// `handle_truncate` and the `40P01` recovery path.
    pub(super) fn truncate_sql_build(
        core: &WriterCore,
        oids: impl Iterator<Item = Oid>,
    ) -> Option<String> {
        let mut sql = String::with_capacity(SQL_BUFFER_CAPACITY);
        sql.push_str("TRUNCATE ");
        let mut first = true;
        for oid in oids {
            if let Some(table_metadata) = core.cache.tables.get1(&oid) {
                if !first {
                    sql.push_str(", ");
                }
                let _ = write!(sql, "{}.{}", table_metadata.schema, table_metadata.name);
                first = false;
            }
        }
        if first { None } else { Some(sql) }
    }

    /// Evaluate each query's membership predicate against the CDC row and return
    /// the fingerprints that matched. Predicates are combined into a single
    /// `SELECT EXISTS (p1), EXISTS (p2), …` per `PG_EVAL_CHUNK`-sized chunk — one
    /// round-trip and one boolean column per query — instead of a `simple_query`
    /// per query. Every query is evaluated (no short-circuit) so each match is
    /// reported; callers that need per-query identity (Fresh-MV dirty-marking)
    /// use this. Use `pg_eval_any` when only "did anything match" is needed.
    pub(super) async fn pg_eval_matches(
        &mut self,
        queries: &[&UpdateQuery],
        table_metadata: &TableMetadata,
        row_data: &[Option<ByteString>],
    ) -> CacheResult<Vec<Fingerprint>> {
        if queries.is_empty() {
            return Ok(Vec::new());
        }
        let mut hits = Vec::new();
        for chunk in queries.chunks(PG_EVAL_CHUNK) {
            self.pg_eval_buf.clear();
            self.pg_eval_buf.push_str("SELECT ");
            for (i, uq) in chunk.iter().enumerate() {
                if i > 0 {
                    self.pg_eval_buf.push_str(", ");
                }
                self.pg_eval_buf.push_str("EXISTS (");
                Self::cache_predicate_into(
                    &mut self.pg_eval_buf,
                    &uq.resolved,
                    table_metadata,
                    row_data,
                )?;
                self.pg_eval_buf.push(')');
            }
            let Some(row) =
                Self::pg_eval_chunk_row(&self.cache_eval_conn, &self.pg_eval_buf).await?
            else {
                continue;
            };
            // One boolean column per query; column `i` ↔ `chunk[i]`.
            for (i, uq) in chunk.iter().enumerate() {
                if row.get(i) == Some("t") {
                    trace!(
                        "update_queries pg-eval matched fingerprint {}",
                        uq.fingerprint
                    );
                    hits.push(uq.fingerprint);
                }
            }
        }
        Ok(hits)
    }

    /// Whether the CDC row matches *any* of `queries` — for the membership-only
    /// (non-`Fresh`) set, where one match is enough to trigger the shared-table
    /// upsert and individual fingerprints are never needed. Predicates are
    /// OR-combined per `PG_EVAL_CHUNK`-sized chunk so Postgres short-circuits the
    /// chain server-side, and evaluation stops at the first chunk that hits.
    pub(super) async fn pg_eval_any(
        &mut self,
        queries: &[&UpdateQuery],
        table_metadata: &TableMetadata,
        row_data: &[Option<ByteString>],
    ) -> CacheResult<bool> {
        if queries.is_empty() {
            return Ok(false);
        }
        for chunk in queries.chunks(PG_EVAL_CHUNK) {
            self.pg_eval_buf.clear();
            self.pg_eval_buf.push_str("SELECT ");
            for (i, uq) in chunk.iter().enumerate() {
                if i > 0 {
                    self.pg_eval_buf.push_str(" OR ");
                }
                self.pg_eval_buf.push_str("EXISTS (");
                Self::cache_predicate_into(
                    &mut self.pg_eval_buf,
                    &uq.resolved,
                    table_metadata,
                    row_data,
                )?;
                self.pg_eval_buf.push(')');
            }
            let Some(row) =
                Self::pg_eval_chunk_row(&self.cache_eval_conn, &self.pg_eval_buf).await?
            else {
                continue;
            };
            if row.get(0) == Some("t") {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Run one combined predicate `SELECT` and return its single result row, or
    /// `None` if the result carried no row (impossible for a well-formed
    /// `SELECT EXISTS (...)`, treated as no-match). Shared by `pg_eval_matches`
    /// and `pg_eval_any`.
    async fn pg_eval_chunk_row(conn: &Client, sql: &str) -> CacheResult<Option<SimpleQueryRow>> {
        let msgs = match conn.simple_query(sql).await {
            Ok(m) => m,
            Err(e) => {
                error!("predicate eval error: {}", error_chain_format(&e));
                return Err(CacheError::PgError(e).into());
            }
        };
        Ok(msgs.into_iter().find_map(|m| {
            if let SimpleQueryMessage::Row(row) = m {
                Some(row)
            } else {
                None
            }
        }))
    }

    /// Append one cached query's membership predicate — the inner SELECT of a
    /// `SELECT EXISTS (...)`, with the CDC row's values substituted for the
    /// changed table — into `buf`. Read-only; evaluated against the
    /// pre-transaction snapshot. Caller wraps it in `EXISTS (...)`.
    pub(super) fn cache_predicate_into(
        buf: &mut String,
        resolved: &ResolvedQueryExpr,
        table_metadata: &TableMetadata,
        row_data: &[Option<ByteString>],
    ) -> CacheResult<()> {
        let resolved_select = resolved.as_select().ok_or(CacheError::InvalidQuery)?;
        let value_select = resolved_select_node_table_replace_with_values(
            resolved_select,
            table_metadata,
            row_data,
        )
        .map_err(|e| e.context_transform(CacheError::from))?;
        Deparse::deparse(&value_select, buf);
        Ok(())
    }

    /// Build an unconditional UPSERT for the row — `INSERT ... ON CONFLICT DO UPDATE`
    /// with no WHERE predicate. Used by the LocalEval fast path once the Rust
    /// evaluator has already decided the row belongs in cache.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    /// Append an unconditional upsert for `row_data` into `buf` (PGC-228:
    /// builders write into the reused frame buffer instead of allocating a
    /// per-statement `String`).
    pub(super) fn cache_upsert_unconditional_into(
        &self,
        buf: &mut String,
        table_metadata: &TableMetadata,
        row_data: &[Option<ByteString>],
    ) {
        // Columns with a value in `row_data` are emitted in three passes
        // (names, values, conflict tail) over the position-sorted column
        // store, writing straight into `buf` — no per-event Vec or String.
        let schema = &table_metadata.schema;
        let table = &table_metadata.name;

        let _ = write!(buf, "INSERT INTO {schema}.{table} (");
        let mut first = true;
        for column_meta in &table_metadata.columns {
            if row_data.get(column_meta.index()).is_none() {
                continue;
            }
            if !first {
                buf.push_str(", ");
            }
            buf.push_str(column_meta.name.as_str());
            first = false;
        }
        buf.push_str(") VALUES (");
        let mut first = true;
        for column_meta in &table_metadata.columns {
            let Some(row_value) = row_data.get(column_meta.index()) else {
                continue;
            };
            if !first {
                buf.push_str(", ");
            }
            match row_value.as_deref() {
                Some(value) => {
                    let _ = escape::escape_literal_into(value, buf);
                }
                None => buf.push_str("NULL"),
            }
            first = false;
        }
        buf.push_str(") ON CONFLICT (");
        for (i, pk) in table_metadata.primary_key_columns.iter().enumerate() {
            if i > 0 {
                buf.push_str(", ");
            }
            buf.push_str(pk);
        }
        buf.push(')');
        cdc_on_conflict_tail_append(buf, table_metadata, row_data);
    }

    // Trace level: at info/debug the fmt layer allocates per-span extensions,
    // which would put a heap allocation on every CDC event.
    #[instrument(skip_all, level = "trace")]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    /// Append a PK-qualified delete for `row_data` into `buf` (PGC-228).
    pub(super) fn cache_delete_into(
        &self,
        buf: &mut String,
        table_metadata: &TableMetadata,
        row_data: &[Option<ByteString>],
    ) -> CacheResult<()> {
        let _ = write!(
            buf,
            "DELETE FROM {}.{} WHERE ",
            table_metadata.schema, table_metadata.name
        );

        let mut has_pk = false;
        for pk_column in &table_metadata.primary_key_columns {
            if let Some(column_meta) = table_metadata.columns.get(pk_column.as_str()) {
                let position = column_meta.index();
                if let Some(row_value) = row_data.get(position) {
                    if has_pk {
                        buf.push_str(" AND ");
                    }
                    let _ = write!(buf, "{pk_column} = ");
                    match row_value.as_deref() {
                        Some(value) => {
                            let _ = escape::escape_literal_into(value, buf);
                        }
                        None => buf.push_str("NULL"),
                    }
                    has_pk = true;
                }
            }
        }

        if !has_pk {
            error!("Cannot build DELETE WHERE clause: no primary key values found");
            return Err(CacheError::NoPrimaryKey.into());
        }

        Ok(())
    }
}
