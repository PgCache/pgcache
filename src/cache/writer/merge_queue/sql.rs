//! The SQL a drain runs: the per-relation merge plan, the chunk and discard
//! statements, and reading their row counts back.

use ecow::EcoString;
use postgres_protocol::escape;
use rootcause::Report;
use tokio_postgres::SimpleQueryMessage;

use super::ChunkStatement;
use crate::cache::{CacheError, CacheResult, ReportExt};
use crate::catalog::TableMetadata;

/// A relation's primary-key columns as the merge's key predicates render
/// them: the recorded-delete filter on a chunk and the toast-stale probe.
///
/// Both predicates test whether a row of the source aliased `d` has its key
/// in a set of recorded key tuples. A scalar key renders as `IN (...)`,
/// which PostgreSQL folds into one array operator however long the list. A
/// composite key cannot: the parser expands a row-valued `IN` list into one
/// row comparison per tuple chained by OR, and planning that recurses once
/// per tuple, so a few thousand tuples exceed `max_stack_depth` (PGC-564).
/// It renders instead as an (anti-)join against a `VALUES` list, whose depth
/// does not grow with the set. The key tuples are stored as escaped literal
/// bodies, so the `VALUES` columns resolve to text and are cast back to each
/// column's cache type — the coercion the `IN` form applies to its literals.
pub(crate) struct MergeKey {
    /// `("p1","p2")`.
    paren: String,
    /// Quoted column name and the cache table's type for it, key order.
    columns: Vec<(String, EcoString)>,
}

impl MergeKey {
    pub(crate) fn build(table: &TableMetadata) -> Self {
        let columns = table
            .primary_key_columns
            .iter()
            .map(|c| {
                let cache_type = table
                    .columns
                    .get(c.as_str())
                    .map(|m| m.cache_type_name.clone());
                debug_assert!(
                    cache_type.is_some(),
                    "primary-key column {c} missing from columns"
                );
                (
                    escape::escape_identifier(c),
                    cache_type.unwrap_or_else(|| EcoString::from("text")),
                )
            })
            .collect();
        Self::from_columns(columns)
    }

    fn from_columns(columns: Vec<(String, EcoString)>) -> Self {
        let paren = format!(
            "({})",
            columns
                .iter()
                .map(|(c, _)| c.as_str())
                .collect::<Vec<_>>()
                .join(",")
        );
        Self { paren, columns }
    }

    #[cfg(test)]
    pub(crate) fn for_test(columns: &[(&str, &str)]) -> Self {
        Self::from_columns(
            columns
                .iter()
                .map(|(name, cache_type)| {
                    (
                        escape::escape_identifier(name),
                        EcoString::from(*cache_type),
                    )
                })
                .collect(),
        )
    }

    /// `("p1","p2")`.
    pub(crate) fn paren(&self) -> &str {
        &self.paren
    }

    /// Whether the key of a row of `d` is (`negated`: is not) one of `tuples`,
    /// rendered key bodies (`'v1','v2'`). `None` for an empty set.
    pub(crate) fn membership<'a>(
        &self,
        tuples: impl Iterator<Item = &'a str>,
        negated: bool,
    ) -> Option<String> {
        let mut list = String::new();
        for body in tuples {
            if !list.is_empty() {
                list.push(',');
            }
            list.push('(');
            list.push_str(body);
            list.push(')');
        }
        if list.is_empty() {
            return None;
        }
        let not = if negated { "NOT " } else { "" };
        if let [(column, _)] = self.columns.as_slice() {
            return Some(format!("({column}) {not}IN ({list})"));
        }
        let names = self
            .columns
            .iter()
            .map(|(c, _)| c.as_str())
            .collect::<Vec<_>>()
            .join(",");
        let on = self
            .columns
            .iter()
            .map(|(c, cache_type)| format!("CAST(k.{c} AS {cache_type}) = d.{c}"))
            .collect::<Vec<_>>()
            .join(" AND ");
        Some(format!(
            "{not}EXISTS (SELECT 1 FROM (VALUES {list}) AS k({names}) WHERE {on})"
        ))
    }
}

/// Pre-rendered, owned SQL fragments for merging one relation's staging table
/// into its shared cache table. Built while borrowing `TableMetadata`, so the
/// borrow doesn't span the async DB calls.
pub(super) struct MergePlan {
    schema: EcoString,
    name: EcoString,
    /// `"c1","c2",...` — all columns, position order.
    columns_csv: String,
    pub(super) key_columns: MergeKey,
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
        let key_columns = MergeKey::build(table);
        let conflict_assign = key_columns
            .columns
            .iter()
            .map(|(c, _)| format!("{c} = EXCLUDED.{c}"))
            .collect::<Vec<_>>()
            .join(", ");
        let conflict = format!(
            "ON CONFLICT {} DO UPDATE SET {conflict_assign}",
            key_columns.paren()
        );
        Self {
            schema: table.schema.clone(),
            name: table.name.clone(),
            columns_csv,
            key_columns,
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
            pk = self.key_columns.paren(),
            conflict = self.conflict,
        )
    }
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
    use crate::cache::Generation;

    fn statement(
        staging: &str,
        generation: Generation,
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
            key_columns: MergeKey::for_test(&[("id", "integer")]),
            conflict: "ON CONFLICT (\"id\") DO UPDATE SET \"id\" = EXCLUDED.\"id\"".to_owned(),
        }
    }

    #[test]
    fn test_chunk_sql_drains_a_page_window_and_stamps_generation_locally() {
        let sql = plan().chunk_sql(statement("stage_1_0", Generation::from_raw(9), 3, 67), None);
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
    fn test_chunk_sql_applies_deleted_key_filter_to_insert_only() {
        let sql = plan().chunk_sql(
            statement("s", Generation::from_raw(1), 0, 10),
            Some("(\"id\") NOT IN ((4))"),
        );
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

    #[test]
    fn test_scalar_key_membership_renders_an_in_list() {
        let key = MergeKey::for_test(&[("id", "integer")]);
        assert_eq!(
            key.membership(["4", "5"].into_iter(), true).as_deref(),
            Some("(\"id\") NOT IN ((4),(5))")
        );
        assert_eq!(
            key.membership(["4"].into_iter(), false).as_deref(),
            Some("(\"id\") IN ((4))")
        );
    }

    /// A row-valued `IN` list recurses once per tuple in the parser, so the
    /// composite form is an anti-join against VALUES, cast back to the cache
    /// types and correlated on the row source `d`.
    #[test]
    fn test_composite_key_membership_renders_a_values_anti_join() {
        let key = MergeKey::for_test(&[("post_id", "integer"), ("tag", "text")]);
        assert_eq!(
            key.membership(["'1','a'", "'2','b'"].into_iter(), true)
                .as_deref(),
            Some(
                "NOT EXISTS (SELECT 1 FROM (VALUES ('1','a'),('2','b')) AS k(\"post_id\",\"tag\") \
                 WHERE CAST(k.\"post_id\" AS integer) = d.\"post_id\" AND CAST(k.\"tag\" AS text) = d.\"tag\")"
            )
        );
        assert!(
            key.membership(["'1','a'"].into_iter(), false)
                .is_some_and(|p| p.starts_with("EXISTS (SELECT 1 FROM (VALUES ('1','a'))"))
        );
    }

    #[test]
    fn test_membership_of_an_empty_set_renders_nothing() {
        let key = MergeKey::for_test(&[("post_id", "integer"), ("tag", "text")]);
        assert_eq!(key.membership(std::iter::empty(), true), None);
        assert_eq!(key.membership(std::iter::empty(), false), None);
    }

    #[test]
    fn test_chunk_sql_correlates_a_composite_filter_on_the_drained_rows() {
        let key = MergeKey::for_test(&[("post_id", "integer"), ("tag", "text")]);
        let filter = key
            .membership(["'1','a'"].into_iter(), true)
            .expect("filter present");
        let sql = plan().chunk_sql(
            statement("s", Generation::from_raw(1), 0, 10),
            Some(&filter),
        );
        assert!(
            sql.contains("FROM d WHERE NOT EXISTS (SELECT 1 FROM (VALUES ('1','a')) AS k("),
            "{sql}"
        );
        assert!(
            sql.contains("= d.\"post_id\" AND") && sql.contains(") ON CONFLICT"),
            "{sql}"
        );
    }
}
