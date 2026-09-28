use std::collections::HashSet;
use std::fmt::{Display, Write};

use ecow::EcoString;
use postgres_protocol::escape;
use tokio_postgres::{SimpleQueryMessage, SimpleQueryRow};

use super::{SQL_BUFFER_CAPACITY, WriterCdc};
use crate::cache::update_query::{RowChanges, UpdateQueries};
use crate::cache::writer::core::WriterCore;
use crate::cache::{CacheError, CacheResult, MapIntoReport};
use crate::catalog::TableMetadata;
use crate::oid::Oid;
use crate::pg::protocol::ByteString;
use crate::query::evaluate::bool_wire_text_parse;

/// Alias prefixes for the ordering projections the row-change SQL emits for
/// limit-window ORDER BY columns (PGC-334), folded back into the base column's
/// `ColumnChange` by `row_change_column_fold`. If a >54-byte column name makes
/// PG truncate the alias, the fold misses and the ordering fields stay
/// unknown — degrading to the conservative always-invalidate path.
pub(super) const OLD_LESS_THAN_ALIAS_PREFIX: &str = "__pgc_lt_";
pub(super) const OLD_IS_NULL_ALIAS_PREFIX: &str = "__pgc_nl_";

/// Whether any of the relation's columns uses the reserved `__pgc_` prefix.
/// Such a column's own result alias would collide with the projection-alias
/// namespace and mis-route through the fold, so the ordering/old-image
/// projections are suppressed for the relation and the fold routes every
/// result column as plain (conservative: direction punts to invalidate, old
/// images stay wildcard).
pub(super) fn table_has_reserved_columns(table_metadata: &TableMetadata) -> bool {
    table_metadata
        .columns
        .iter()
        .any(|c| c.name.starts_with("__pgc_"))
}

/// The relation's limit-window ORDER BY key columns — the set the row-change
/// projection emits ordering for.
pub(super) fn relation_order_columns(
    update_queries: Option<&UpdateQueries>,
) -> HashSet<&EcoString> {
    update_queries
        .map(|uq| uq.limit_order_columns().collect())
        .unwrap_or_default()
}

/// Per-column projection shared by the per-row and batched row-change
/// builders, so the changed-column contract the fold parses can't drift
/// between them. The old value is the cache-table column (optionally
/// qualified); the new value is whatever the caller compares it against.
pub(super) struct RowChangeProjection<'a> {
    pub(super) old_qualifier: &'static str,
    pub(super) order_columns: HashSet<&'a EcoString>,
}

impl RowChangeProjection<'_> {
    /// Append `old IS DISTINCT FROM new AS col`, plus old-vs-new ordering
    /// (`old < new`, `old IS NULL`) for limit-window ORDER BY key columns.
    pub(super) fn column_into(
        &self,
        buf: &mut String,
        column_name: &EcoString,
        new_value: impl Display,
    ) {
        // Identifiers are quoted throughout: cache-table DDL quotes column
        // names, so reserved-word / mixed-case columns exist and unquoted
        // references would error (or case-fold) against them. Quoting the
        // aliases also makes result column names exact-case, keeping the fold
        // and its consumers keyed by the real column names.
        let name = escape::escape_identifier(column_name);
        let old = self.old_qualifier;
        let _ = write!(buf, "{old}{name} IS DISTINCT FROM {new_value} AS {name}");
        if self.order_columns.contains(column_name) {
            let less_than_alias =
                escape::escape_identifier(&format!("{OLD_LESS_THAN_ALIAS_PREFIX}{column_name}"));
            let is_null_alias =
                escape::escape_identifier(&format!("{OLD_IS_NULL_ALIAS_PREFIX}{column_name}"));
            let _ = write!(
                buf,
                ", {old}{name} < {new_value} AS {less_than_alias}, \
                 {old}{name} IS NULL AS {is_null_alias}",
            );
        }
    }
}

/// Fold one row-change result column into the per-column map: plain names
/// carry the `IS DISTINCT FROM` changed flag, prefixed aliases carry the
/// ordering projections. `None` (SQL NULL) leaves ordering unknown and, for
/// the changed flag, defensively counts as unchanged — matching the historic
/// text parse. `strip_prefixes` is false when the builder suppressed every
/// prefixed projection (`table_has_reserved_columns`): every result column is
/// then a real column, including `__pgc_`-named ones.
pub(super) fn row_change_column_fold(
    changes: &mut RowChanges,
    name: &str,
    value: Option<bool>,
    strip_prefixes: bool,
) {
    if strip_prefixes && let Some(col) = name.strip_prefix(OLD_LESS_THAN_ALIAS_PREFIX) {
        changes
            .entry(EcoString::from(col))
            .or_default()
            .old_less_than_new = value;
    } else if strip_prefixes && let Some(col) = name.strip_prefix(OLD_IS_NULL_ALIAS_PREFIX) {
        changes.entry(EcoString::from(col)).or_default().old_is_null = value == Some(true);
    } else {
        changes.entry(EcoString::from(name)).or_default().changed = value == Some(true);
    }
}

fn sql_literal(value: &Option<ByteString>) -> String {
    value
        .as_deref()
        .map_or_else(|| "NULL".to_owned(), escape::escape_literal)
}

/// `SELECT <per-column change projections> FROM <cache table> WHERE <pk> =
/// <row pk>`; errors when no PK column is present in `row_data`.
fn row_changes_sql_build(
    table_metadata: &TableMetadata,
    projection: &RowChangeProjection<'_>,
    row_data: &[Option<ByteString>],
) -> CacheResult<String> {
    let mut sql = String::with_capacity(SQL_BUFFER_CAPACITY);
    sql.push_str("SELECT ");
    let projected = table_metadata
        .columns
        .iter()
        .filter_map(|column_meta| row_data.get(column_meta.index()).map(|v| (column_meta, v)));
    for (i, (column_meta, row_value)) in projected.enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        projection.column_into(&mut sql, &column_meta.name, sql_literal(row_value));
    }

    let _ = write!(
        sql,
        " FROM {}.{} WHERE ",
        escape::escape_identifier(&table_metadata.schema),
        escape::escape_identifier(&table_metadata.name)
    );

    if !pk_where_append(&mut sql, table_metadata, row_data) {
        return Err(CacheError::NoPrimaryKey.into());
    }
    Ok(sql)
}

/// Append `pk1 = v1 AND …` for the PK columns present in `row_data`; false
/// when none are.
fn pk_where_append(
    sql: &mut String,
    table_metadata: &TableMetadata,
    row_data: &[Option<ByteString>],
) -> bool {
    let pk_values = table_metadata.primary_key_columns.iter().filter_map(|pk| {
        let column_meta = table_metadata.columns.get(pk.as_str())?;
        row_data.get(column_meta.index()).map(|v| (pk, v))
    });
    let mut has_pk = false;
    for (pk_column, row_value) in pk_values {
        if has_pk {
            sql.push_str(" AND ");
        }
        let _ = write!(
            sql,
            "{} = {}",
            escape::escape_identifier(pk_column),
            sql_literal(row_value)
        );
        has_pk = true;
    }
    has_pk
}

fn row_changes_from_row(row: &SimpleQueryRow, strip_prefixes: bool) -> RowChanges {
    let mut changes = RowChanges::with_capacity(row.len());
    for (idx, col) in row.columns().iter().enumerate() {
        row_change_column_fold(
            &mut changes,
            col.name(),
            row.get(idx).and_then(bool_wire_text_parse),
            strip_prefixes,
        );
    }
    changes
}

impl WriterCdc {
    /// SELECT one row from the cache, projecting a boolean per non-PK column
    /// that's true iff the cached value differs from the incoming `row_data`
    /// value. Used by CDC UPDATE handling to decide whether a column change
    /// actually shifts query membership. For the relation's limit-window ORDER
    /// BY key columns it additionally projects old-vs-new ordering
    /// (`col < new`, `col IS NULL`) so the window check can tell promotions
    /// from demotions (PGC-334). Returns `None` when the row isn't in the
    /// cache (no PK match).
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) async fn query_row_changes(
        &self,
        core: &WriterCore,
        relation_oid: Oid,
        row_data: &[Option<ByteString>],
    ) -> CacheResult<Option<RowChanges>> {
        let table_metadata =
            core.cache
                .tables
                .get1(&relation_oid)
                .ok_or(CacheError::UnknownTable {
                    oid: Some(relation_oid),
                    name: None,
                })?;

        let reserved_columns = table_has_reserved_columns(table_metadata);
        let projection = RowChangeProjection {
            old_qualifier: "",
            order_columns: if reserved_columns {
                HashSet::new()
            } else {
                relation_order_columns(core.cache.update_queries.get(&relation_oid))
            },
        };
        let sql = row_changes_sql_build(table_metadata, &projection, row_data)?;

        let msgs = core
            .db_cache
            .simple_query(&sql)
            .await
            .map_into_report::<CacheError>()?;

        Ok(msgs.iter().find_map(|msg| {
            if let SimpleQueryMessage::Row(row) = msg {
                Some(row_changes_from_row(row, !reserved_columns))
            } else {
                None
            }
        }))
    }
}
