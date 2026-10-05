//! The SQL a drain runs: the per-relation merge plan, the chunk and discard
//! statements, and reading their row counts back.

use ecow::EcoString;
use postgres_protocol::escape;
use rootcause::Report;
use tokio_postgres::SimpleQueryMessage;

use super::ChunkStatement;
use crate::cache::{CacheError, CacheResult, ReportExt};
use crate::catalog::TableMetadata;

/// Pre-rendered, owned SQL fragments for merging one relation's staging table
/// into its shared cache table. Built while borrowing `TableMetadata`, so the
/// borrow doesn't span the async DB calls.
pub(super) struct MergePlan {
    schema: EcoString,
    name: EcoString,
    /// `"c1","c2",...` — all columns, position order.
    columns_csv: String,
    /// `("p1","p2")` — primary-key columns.
    pub(super) pk_columns_paren: String,
    /// `ON CONFLICT ("p1") DO UPDATE SET "p1" = EXCLUDED."p1"` — re-stamps the
    /// generation of pre-existing rows without overwriting data (CDC owns it).
    conflict: String,
}

impl MergePlan {
    pub(super) fn build(table: &TableMetadata) -> Self {
        let columns_csv = table
            .columns
            .iter()
            .map(|c| escape::escape_identifier(&c.name))
            .collect::<Vec<_>>()
            .join(",");
        let pk_quoted: Vec<String> = table
            .primary_key_columns
            .iter()
            .map(|c| escape::escape_identifier(c))
            .collect();
        let pk_columns_paren = format!("({})", pk_quoted.join(","));
        let conflict_assign = pk_quoted
            .iter()
            .map(|c| format!("{c} = EXCLUDED.{c}"))
            .collect::<Vec<_>>()
            .join(", ");
        let conflict = format!("ON CONFLICT {pk_columns_paren} DO UPDATE SET {conflict_assign}");
        Self {
            schema: table.schema.clone(),
            name: table.name.clone(),
            columns_csv,
            pk_columns_paren,
            conflict,
        }
    }

    /// One merge chunk: drain every staging row in heap blocks `[lo, hi)` into
    /// the cache table and report the row count.
    ///
    /// Drains with `DELETE … RETURNING` rather than `SELECT` + a later
    /// `DROP TABLE` (PGC-293): the rows feed the upsert and the table is left
    /// empty for pooled reuse, so a population emits *no* DDL — and thus no
    /// relcache/plan-cache invalidation, the per-population cost that
    /// collapses write-mixed throughput at high query cardinality (PGC-294).
    /// The filter (CDC-removed keys) gates only which drained rows are
    /// reinserted, so the table still ends empty. DISTINCT ON the PK collapses
    /// duplicate keys a set-operation query can stage (same relation in
    /// multiple branches) — without it the upsert would error "ON CONFLICT
    /// cannot affect row a second time". A duplicate split across two chunks
    /// conflicts on the second and is re-stamped, which is the same outcome.
    ///
    /// A page window is complete under any plan: the planner picks a TID range
    /// scan for it even with stale stats, and a sequential-scan fallback is a
    /// filter over the window, slower but never skipping. `SET LOCAL` scopes
    /// the generation stamp to the implicit transaction of this
    /// multi-statement string, which also rolls back as a unit on error and
    /// leaves the connection outside any transaction.
    pub(super) fn chunk_sql(&self, statement: ChunkStatement<'_>, filter: Option<&str>) -> String {
        let ChunkStatement {
            staging,
            generation,
            lo_block,
            hi_block,
        } = statement;
        let where_clause = filter.map_or_else(String::new, |f| format!(" WHERE {f}"));
        format!(
            "SET LOCAL mem.query_generation = {generation}; \
             WITH d AS (DELETE FROM pgcache_stage.{staging} \
                 WHERE ctid >= '({lo_block},0)'::tid AND ctid < '({hi_block},0)'::tid \
                 RETURNING {cols}), \
             i AS (INSERT INTO {schema}.{name} ({cols}) \
                 SELECT DISTINCT ON {pk} {cols} FROM d{where_clause} {conflict}) \
             SELECT count(*) FROM d",
            schema = escape::escape_identifier(&self.schema),
            name = escape::escape_identifier(&self.name),
            cols = self.columns_csv,
            pk = self.pk_columns_paren,
            conflict = self.conflict,
        )
    }
}

/// One discard chunk: delete every staging row in heap blocks `[lo, hi)`.
/// Same window shape as an apply chunk, so it is complete under any plan.
pub(super) fn discard_sql(statement: ChunkStatement<'_>) -> String {
    let ChunkStatement {
        staging,
        lo_block,
        hi_block,
        ..
    } = statement;
    format!(
        "DELETE FROM pgcache_stage.{staging} \
         WHERE ctid >= '({lo_block},0)'::tid AND ctid < '({hi_block},0)'::tid"
    )
}

/// Read a discard statement's row count from its command tag.
pub(super) fn discard_result_parse(messages: &[SimpleQueryMessage]) -> u64 {
    messages
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::CommandComplete(rows) => Some(*rows),
            SimpleQueryMessage::Row(_) | SimpleQueryMessage::RowDescription(_) | _ => None,
        })
        .unwrap_or(0)
}

/// Read a chunk statement's row count.
pub(super) fn chunk_result_parse(messages: &[SimpleQueryMessage]) -> CacheResult<u64> {
    messages
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(row),
            SimpleQueryMessage::CommandComplete(_) | SimpleQueryMessage::RowDescription(_) | _ => {
                None
            }
        })
        .and_then(|row| row.get(0))
        .and_then(|c| c.parse::<u64>().ok())
        .ok_or_else(|| Report::from(CacheError::Other))
        .attach_loc("merge chunk returned no row count")
}

#[cfg(test)]
mod tests {

    use super::*;

    fn statement(
        staging: &str,
        generation: u64,
        lo_block: u32,
        hi_block: u32,
    ) -> ChunkStatement<'_> {
        ChunkStatement {
            staging,
            generation,
            lo_block,
            hi_block,
        }
    }

    fn plan() -> MergePlan {
        MergePlan {
            schema: "public".into(),
            name: "t".into(),
            columns_csv: "\"id\",\"v\"".to_owned(),
            pk_columns_paren: "(\"id\")".to_owned(),
            conflict: "ON CONFLICT (\"id\") DO UPDATE SET \"id\" = EXCLUDED.\"id\"".to_owned(),
        }
    }

    #[test]
    fn test_chunk_sql_drains_a_page_window_and_stamps_generation_locally() {
        let sql = plan().chunk_sql(statement("stage_1_0", 9, 3, 67), None);
        assert!(
            sql.starts_with("SET LOCAL mem.query_generation = 9;"),
            "{sql}"
        );
        assert!(
            sql.contains("WHERE ctid >= '(3,0)'::tid AND ctid < '(67,0)'::tid"),
            "{sql}"
        );
        assert!(
            !sql.contains("LIMIT"),
            "a row limit is plan-dependent: {sql}"
        );
        assert!(sql.contains("DELETE FROM pgcache_stage.stage_1_0"), "{sql}");
        assert!(sql.contains("RETURNING \"id\",\"v\""), "{sql}");
        assert!(
            sql.contains("INSERT INTO \"public\".\"t\" (\"id\",\"v\") SELECT DISTINCT ON (\"id\") \"id\",\"v\" FROM d ON CONFLICT"),
            "{sql}"
        );
        assert!(sql.ends_with("SELECT count(*) FROM d"), "{sql}");
        assert!(
            !sql.contains("BEGIN"),
            "explicit BEGIN would strand an aborted txn: {sql}"
        );
    }

    #[test]
    fn test_discard_sql_is_the_same_page_window_without_the_insert() {
        let sql = discard_sql(statement("stage_1_0", 0, 3, 67));
        assert_eq!(
            sql,
            "DELETE FROM pgcache_stage.stage_1_0 WHERE ctid >= '(3,0)'::tid AND ctid < '(67,0)'::tid"
        );
    }

    #[test]
    fn test_chunk_sql_applies_deleted_key_filter_to_insert_only() {
        let sql = plan().chunk_sql(statement("s", 1, 0, 10), Some("(\"id\") NOT IN ((4))"));
        assert!(
            sql.contains("FROM d WHERE (\"id\") NOT IN ((4)) ON CONFLICT"),
            "{sql}"
        );
        let (delete, _) = sql.split_once("RETURNING").expect("returning clause");
        assert!(
            !delete.contains("NOT IN"),
            "filter must not gate the drain: {sql}"
        );
    }
}
