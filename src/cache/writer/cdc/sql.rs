use crate::oid::Oid;
use crate::query::Fingerprint;

use postgres_protocol::escape;
use tokio_postgres::{Client, SimpleQueryMessage, SimpleQueryRow};
use tracing::{error, instrument, trace, warn};

use crate::catalog::TableMetadata;
use crate::pg::identifier_quote_into;
use crate::pg::protocol::ByteString;

use crate::query::ast::Deparse;
use crate::query::transform::resolved_select_node_table_replace_with_values_all;

use super::super::super::update_query::UpdateQuery;
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
        identifier_quote_into(col, sql);
        sql.push_str(" = EXCLUDED.");
        identifier_quote_into(col, sql);
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
                identifier_quote_into(&table_metadata.schema, &mut sql);
                sql.push('.');
                identifier_quote_into(&table_metadata.name, &mut sql);
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
            for (i, update_query) in chunk.iter().enumerate() {
                if i > 0 {
                    self.pg_eval_buf.push_str(", ");
                }
                self.pg_eval_buf.push('(');
                Self::cache_predicate_into(
                    &mut self.pg_eval_buf,
                    update_query,
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
            for (i, update_query) in chunk.iter().enumerate() {
                if row.get(i) == Some("t") {
                    trace!(
                        "update_queries pg-eval matched fingerprint {}",
                        update_query.fingerprint
                    );
                    hits.push(update_query.fingerprint);
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
            for (i, update_query) in chunk.iter().enumerate() {
                if i > 0 {
                    self.pg_eval_buf.push_str(" OR ");
                }
                self.pg_eval_buf.push('(');
                Self::cache_predicate_into(
                    &mut self.pg_eval_buf,
                    update_query,
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

    /// Append one cached query's membership predicate into `buf` as a complete
    /// boolean expression, with the CDC row's values substituted for the changed
    /// table.
    ///
    /// The relation is substituted once per FROM occurrence and the results are
    /// OR'd: `EXISTS (…)`, or `EXISTS (…) OR EXISTS (…)` for a self-join. A row
    /// belongs to the result if it can stand in for **any** occurrence, so the
    /// disjunction *is* the membership test — substituting a single arm
    /// under-approximates it and evicts rows the other arm still needs
    /// (PGC-256). Read-only; evaluated against the pre-transaction snapshot.
    pub(super) fn cache_predicate_into(
        buf: &mut String,
        update_query: &UpdateQuery,
        table_metadata: &TableMetadata,
        row_data: &[Option<ByteString>],
    ) -> CacheResult<()> {
        // Fast path: render the row's literals into the precomputed template
        // (PGC-343), skipping the per-row clone + deparse. The template is only
        // built for single-occurrence relations, so one `EXISTS` is the whole
        // predicate. `render_into` declines short/partial rows, falling through
        // to the general path below.
        if let Some(template) = &update_query.pg_eval_template {
            let mark = buf.len();
            buf.push_str("EXISTS (");
            if template.render_into(buf, row_data) {
                buf.push(')');
                return Ok(());
            }
            buf.truncate(mark);
        }
        let resolved_select = update_query
            .resolved
            .as_select()
            .ok_or(CacheError::InvalidQuery)?;
        let value_selects = resolved_select_node_table_replace_with_values_all(
            resolved_select,
            table_metadata,
            row_data,
        )
        .map_err(|e| e.context_transform(CacheError::from))?;
        for (i, value_select) in value_selects.iter().enumerate() {
            if i > 0 {
                buf.push_str(" OR ");
            }
            buf.push_str("EXISTS (");
            Deparse::deparse(value_select, buf);
            buf.push(')');
        }
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
        buf: &mut String,
        table_metadata: &TableMetadata,
        row_data: &[Option<ByteString>],
    ) {
        // Columns with a value in `row_data` are emitted in three passes
        // (names, values, conflict tail) over the position-sorted column
        // store, writing straight into `buf` — no per-event Vec or String.
        buf.push_str("INSERT INTO ");
        identifier_quote_into(&table_metadata.schema, buf);
        buf.push('.');
        identifier_quote_into(&table_metadata.name, buf);
        buf.push_str(" (");
        let mut first = true;
        for column_meta in &table_metadata.columns {
            if row_data.get(column_meta.index()).is_none() {
                continue;
            }
            if !first {
                buf.push_str(", ");
            }
            identifier_quote_into(column_meta.name.as_str(), buf);
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
            identifier_quote_into(pk, buf);
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
        buf: &mut String,
        table_metadata: &TableMetadata,
        row_data: &[Option<ByteString>],
    ) -> CacheResult<()> {
        buf.push_str("DELETE FROM ");
        identifier_quote_into(&table_metadata.schema, buf);
        buf.push('.');
        identifier_quote_into(&table_metadata.name, buf);
        buf.push_str(" WHERE ");

        let mut has_pk = false;
        for pk_column in &table_metadata.primary_key_columns {
            if let Some(column_meta) = table_metadata.columns.get(pk_column.as_str()) {
                let position = column_meta.index();
                if let Some(row_value) = row_data.get(position) {
                    if has_pk {
                        buf.push_str(" AND ");
                    }
                    identifier_quote_into(pk_column, buf);
                    buf.push_str(" = ");
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

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use tokio_postgres::types::Type;

    use crate::catalog::{ColumnMetadata, ColumnStore};

    use super::*;

    /// A table exercising every identifier hazard: mixed-case name,
    /// reserved-word column (`user`), mixed-case column, embedded quote.
    fn quoted_table_metadata() -> TableMetadata {
        let column = |name: &str, position: i16, is_primary_key: bool| ColumnMetadata {
            name: name.into(),
            position,
            type_oid: 25,
            data_type: Type::TEXT,
            type_name: "text".into(),
            cache_type_name: "text".into(),
            is_primary_key,
        };
        TableMetadata {
            replica_identity_full: false,
            relation_oid: Oid::from_raw(4242),
            name: "Order".into(),
            schema: "public".into(),
            primary_key_columns: vec!["id".into()],
            columns: ColumnStore::new([
                column("id", 1, true),
                column("user", 2, false),
                column("camelCase", 3, false),
                column("we\"ird", 4, false),
            ]),
            indexes: Vec::new(),
        }
    }

    fn cell(value: &'static str) -> Option<ByteString> {
        Some(ByteString::from_utf8(Bytes::from_static(value.as_bytes())).expect("utf8 cell"))
    }

    #[test]
    fn test_upsert_quotes_identifiers() {
        let table = quoted_table_metadata();
        let row = vec![cell("1"), cell("alice"), cell("42"), None];
        let mut buf = String::new();
        WriterCdc::cache_upsert_unconditional_into(&mut buf, &table, &row);
        assert_eq!(
            buf,
            "INSERT INTO \"public\".\"Order\" (\"id\", \"user\", \"camelCase\", \"we\"\"ird\") \
             VALUES ('1', 'alice', '42', NULL) \
             ON CONFLICT (\"id\") \
             DO UPDATE SET \"user\" = EXCLUDED.\"user\", \
             \"camelCase\" = EXCLUDED.\"camelCase\", \
             \"we\"\"ird\" = EXCLUDED.\"we\"\"ird\""
        );
    }

    #[test]
    fn test_delete_quotes_identifiers() {
        let table = quoted_table_metadata();
        let row = vec![cell("1")];
        let mut buf = String::new();
        WriterCdc::cache_delete_into(&mut buf, &table, &row).expect("build delete");
        assert_eq!(buf, "DELETE FROM \"public\".\"Order\" WHERE \"id\" = '1'");
    }
}
