use postgres_protocol::escape;
use tokio_postgres::{Client, SimpleQueryMessage, SimpleQueryRow};
use tracing::{error, instrument, trace, warn};

use super::{PG_EVAL_CHUNK, SQL_BUFFER_CAPACITY, WriterCdc};
use crate::cache::update_query::UpdateQuery;
use crate::cache::writer::core::WriterCore;
use crate::cache::{CacheError, CacheResult};
use crate::catalog::TableMetadata;
use crate::oid::Oid;
use crate::pg::identifier_quote_into;
use crate::pg::protocol::ByteString;
use crate::query::Fingerprint;
use crate::query::ast::Deparse;
use crate::query::transform::resolved_select_node_table_replace_with_values_all;
use crate::result::error_chain_format;

/// Append `items` to `buf` separated by `sep`, each rendered by `render`.
/// Returns false when there were no items.
fn separated_into<T>(
    buf: &mut String,
    sep: &str,
    items: impl IntoIterator<Item = T>,
    mut render: impl FnMut(&mut String, T),
) -> bool {
    let mut any = false;
    for item in items {
        if any {
            buf.push_str(sep);
        }
        render(buf, item);
        any = true;
    }
    any
}

/// Append the table's quoted `"schema"."name"`.
fn table_name_quote_into(buf: &mut String, table_metadata: &TableMetadata) {
    identifier_quote_into(&table_metadata.schema, buf);
    buf.push('.');
    identifier_quote_into(&table_metadata.name, buf);
}

/// Append a column value as an escaped SQL literal, or `NULL`.
fn sql_literal_into(buf: &mut String, value: Option<&str>) {
    match value {
        Some(value) => {
            let _ = escape::escape_literal_into(value, buf);
        }
        None => buf.push_str("NULL"),
    }
}

/// The columns `row_data` carries a value slot for, in position order.
fn row_columns<'a>(
    table_metadata: &'a TableMetadata,
    row_data: &'a [Option<ByteString>],
) -> impl Iterator<Item = (&'a str, &'a Option<ByteString>)> {
    table_metadata.columns.iter().filter_map(|column_meta| {
        row_data
            .get(column_meta.index())
            .map(|value| (column_meta.name.as_str(), value))
    })
}

/// Append the tail of an upsert SQL: either ` DO UPDATE SET <non-pk cols>` or
/// ` DO NOTHING` if the row has no non-PK columns. PG rejects `DO UPDATE SET`
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
    let mark = sql.len();
    sql.push_str(" DO UPDATE SET ");
    let non_pk = row_columns(table_metadata, row_data).filter(|(name, _)| !is_pk(name));
    let any = separated_into(sql, ", ", non_pk, |sql, (name, _)| {
        identifier_quote_into(name, sql);
        sql.push_str(" = EXCLUDED.");
        identifier_quote_into(name, sql);
    });
    if !any {
        sql.truncate(mark);
        sql.push_str(" DO NOTHING");
    }
}

/// `TRUNCATE <cache table>, ...`, or `None` for no tables.
fn truncate_sql<'a>(tables: impl Iterator<Item = &'a TableMetadata>) -> Option<String> {
    let mut sql = String::with_capacity(SQL_BUFFER_CAPACITY);
    sql.push_str("TRUNCATE ");
    separated_into(&mut sql, ", ", tables, table_name_quote_into).then_some(sql)
}

/// Render `update_query`'s precomputed predicate template (PGC-343) with the
/// row's literals into `buf`, skipping the per-row clone + deparse. The
/// template is only built for single-occurrence relations, so one `EXISTS` is
/// the whole predicate. False (and `buf` untouched) when there is no template
/// or it declines a short/partial row.
fn pg_eval_template_render(
    buf: &mut String,
    update_query: &UpdateQuery,
    row_data: &[Option<ByteString>],
) -> bool {
    let Some(template) = &update_query.pg_eval_template else {
        return false;
    };
    let mark = buf.len();
    buf.push_str("EXISTS (");
    if template.render_into(buf, row_data) {
        buf.push(')');
        return true;
    }
    buf.truncate(mark);
    false
}

impl WriterCdc {
    /// Build `TRUNCATE <cache table>, ...` for the relations' cache tables,
    /// or `None` if none of the oids map to a known cache table. Shared by
    /// `handle_truncate` and the `40P01` recovery path.
    pub(super) fn truncate_sql_build(
        core: &WriterCore,
        oids: impl Iterator<Item = Oid>,
    ) -> Option<String> {
        truncate_sql(oids.filter_map(|oid| core.cache.tables.get1(&oid)))
    }

    /// Build one chunk's combined predicate `SELECT` into `pg_eval_buf`, the
    /// predicates joined by `sep`.
    fn pg_eval_chunk_sql(
        &mut self,
        chunk: &[&UpdateQuery],
        sep: &str,
        table_metadata: &TableMetadata,
        row_data: &[Option<ByteString>],
    ) -> CacheResult<()> {
        let buf = &mut self.pg_eval_buf;
        buf.clear();
        buf.push_str("SELECT ");
        for (i, update_query) in chunk.iter().enumerate() {
            if i > 0 {
                buf.push_str(sep);
            }
            buf.push('(');
            Self::cache_predicate_into(buf, update_query, table_metadata, row_data)?;
            buf.push(')');
        }
        Ok(())
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
        let mut hits = Vec::new();
        for chunk in queries.chunks(PG_EVAL_CHUNK) {
            self.pg_eval_chunk_sql(chunk, ", ", table_metadata, row_data)?;
            let Some(row) =
                Self::pg_eval_chunk_row(&self.cache_eval_conn, &self.pg_eval_buf).await?
            else {
                continue;
            };
            // One boolean column per query; column `i` ↔ `chunk[i]`.
            let matched = chunk
                .iter()
                .enumerate()
                .filter(|(i, _)| row.get(*i) == Some("t"))
                .map(|(_, update_query)| update_query.fingerprint);
            for fingerprint in matched {
                trace!("update_queries pg-eval matched fingerprint {fingerprint}");
                hits.push(fingerprint);
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
        for chunk in queries.chunks(PG_EVAL_CHUNK) {
            self.pg_eval_chunk_sql(chunk, " OR ", table_metadata, row_data)?;
            let row = Self::pg_eval_chunk_row(&self.cache_eval_conn, &self.pg_eval_buf).await?;
            if row.is_some_and(|row| row.get(0) == Some("t")) {
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
        if pg_eval_template_render(buf, update_query, row_data) {
            return Ok(());
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
        separated_into(buf, " OR ", &value_selects, |buf, value_select| {
            buf.push_str("EXISTS (");
            Deparse::deparse(value_select, buf);
            buf.push(')');
        });
        Ok(())
    }

    /// Append an unconditional upsert for `row_data` into `buf` —
    /// `INSERT ... ON CONFLICT DO UPDATE` with no WHERE predicate. Used by the
    /// LocalEval fast path once the Rust evaluator has already decided the row
    /// belongs in cache. Builders write into the reused frame buffer instead of
    /// allocating a per-statement `String` (PGC-228): the row's columns are
    /// emitted in passes over the position-sorted column store, with no
    /// per-event Vec or String.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) fn cache_upsert_unconditional_into(
        buf: &mut String,
        table_metadata: &TableMetadata,
        row_data: &[Option<ByteString>],
    ) {
        buf.push_str("INSERT INTO ");
        table_name_quote_into(buf, table_metadata);
        buf.push_str(" (");
        separated_into(
            buf,
            ", ",
            row_columns(table_metadata, row_data),
            |buf, (name, _)| {
                identifier_quote_into(name, buf);
            },
        );
        buf.push_str(") VALUES (");
        separated_into(
            buf,
            ", ",
            row_columns(table_metadata, row_data),
            |buf, (_, value)| {
                sql_literal_into(buf, value.as_deref());
            },
        );
        buf.push_str(") ON CONFLICT (");
        separated_into(buf, ", ", &table_metadata.primary_key_columns, |buf, pk| {
            identifier_quote_into(pk, buf);
        });
        buf.push(')');
        cdc_on_conflict_tail_append(buf, table_metadata, row_data);
    }

    /// Append a PK-qualified delete for `row_data` into `buf` (PGC-228).
    // Trace level: at info/debug the fmt layer allocates per-span extensions,
    // which would put a heap allocation on every CDC event.
    #[instrument(skip_all, level = "trace")]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) fn cache_delete_into(
        buf: &mut String,
        table_metadata: &TableMetadata,
        row_data: &[Option<ByteString>],
    ) -> CacheResult<()> {
        buf.push_str("DELETE FROM ");
        table_name_quote_into(buf, table_metadata);
        buf.push_str(" WHERE ");
        let pk_values = table_metadata
            .primary_key_columns
            .iter()
            .filter_map(|pk_column| {
                let column_meta = table_metadata.columns.get(pk_column.as_str())?;
                row_data
                    .get(column_meta.index())
                    .map(|value| (pk_column, value))
            });
        let has_pk = separated_into(buf, " AND ", pk_values, |buf, (pk_column, value)| {
            identifier_quote_into(pk_column, buf);
            buf.push_str(" = ");
            sql_literal_into(buf, value.as_deref());
        });
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

    use super::*;
    use crate::catalog::{ColumnMetadata, ColumnPosition, ColumnStore};
    use crate::oid::TypeOid;

    /// A table exercising every identifier hazard: mixed-case name,
    /// reserved-word column (`user`), mixed-case column, embedded quote.
    fn quoted_table_metadata() -> TableMetadata {
        let column = |name: &str, position: i16, is_primary_key: bool| ColumnMetadata {
            name: name.into(),
            position: ColumnPosition::from_raw(position),
            type_oid: TypeOid::from_raw(25),
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
    fn test_upsert_pk_only_row_does_nothing_on_conflict() {
        let table = quoted_table_metadata();
        let row = vec![cell("1")];
        let mut buf = String::new();
        WriterCdc::cache_upsert_unconditional_into(&mut buf, &table, &row);
        assert_eq!(
            buf,
            "INSERT INTO \"public\".\"Order\" (\"id\") VALUES ('1') \
             ON CONFLICT (\"id\") DO NOTHING"
        );
    }

    #[test]
    fn test_delete_without_pk_value_is_rejected() {
        let table = quoted_table_metadata();
        let mut buf = String::new();
        let result = WriterCdc::cache_delete_into(&mut buf, &table, &[]);
        assert!(matches!(
            result.map_err(|e| e.into_current_context()),
            Err(CacheError::NoPrimaryKey)
        ));
    }

    #[test]
    fn test_truncate_lists_quoted_tables() {
        let first = quoted_table_metadata();
        let mut second = quoted_table_metadata();
        second.name = "line".into();
        assert_eq!(
            truncate_sql([&first, &second].into_iter()).as_deref(),
            Some("TRUNCATE \"public\".\"Order\", \"public\".\"line\"")
        );
        assert_eq!(truncate_sql(std::iter::empty()), None);
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
