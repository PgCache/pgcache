//! Classify a registered query's WHERE clause and columns: can CDC evaluate it
//! in Rust, can it be batched, and which columns does its predicate touch.

use std::collections::HashSet;

use ecow::EcoString;

use crate::oid::Oid;
use crate::query::ast::{AstNode, NullOrder, OrderDirection};
use crate::query::evaluate::resolved_where_expr_supported;
use crate::query::resolved::{
    ResolvedColumnNode, ResolvedQueryExpr, ResolvedScalarExpr, ResolvedSelectColumns,
    ResolvedTableNode,
};

use super::super::update_query::{OrderByKey, UpdateEvalStrategy, UpdateQuerySource};

/// Decide whether CDC can evaluate this update query's WHERE in Rust.
///
/// Conservative classifier: rejects anything the Rust evaluator can't decide
/// from a single CDC row. GROUP BY / HAVING are rejected because row-level
/// matching doesn't capture post-aggregation filtering. Non-FromClause sources
/// are rejected because their CDC semantics (subquery membership, outer join
/// null-padding cascade) aren't expressible as a row-level predicate.
pub(super) fn update_eval_strategy_classify(
    resolved: &ResolvedQueryExpr,
    source: UpdateQuerySource,
) -> UpdateEvalStrategy {
    if source != UpdateQuerySource::FromClause {
        return UpdateEvalStrategy::PgEval;
    }
    let Some(select) = resolved.as_select() else {
        return UpdateEvalStrategy::PgEval;
    };
    if !select.is_single_table() {
        return UpdateEvalStrategy::PgEval;
    }
    if !select.group_by.is_empty() || select.having.is_some() {
        return UpdateEvalStrategy::PgEval;
    }
    let Some(where_expr) = &select.where_clause else {
        return UpdateEvalStrategy::LocalEval;
    };
    if resolved_where_expr_supported(where_expr) {
        UpdateEvalStrategy::LocalEval
    } else {
        UpdateEvalStrategy::PgEval
    }
}

/// Whether a PgEval query's membership predicate stays per-row correct when
/// evaluated for many CDC rows in one multi-row VALUES statement (PGC-241).
///
/// Per-row membership substitutes the changed table with *only that row*; a
/// multi-row VALUES is equivalent iff each row's answer is independent of the
/// others. GROUP BY / HAVING / SELECT-list aggregates evaluate against the
/// substituted rows as a set, so they must stay on the per-row path. (LIMIT and
/// ORDER BY never reach the predicate — `cache_predicate_into` deparses the
/// select node only — and aggregates can't appear in WHERE.)
///
/// Relations appearing more than once (self-joins) are excluded: the VALUES
/// transform's alias rewrite clobbers every occurrence's references to one
/// alias, which can forward-reference a later FROM entry — invalid SQL. The
/// per-row path has the same defect but only reaches it on paths the decide
/// pass usually skips; batching pre-evaluates unfiltered, so it must not pick
/// these up (PGC-256 tracks the underlying transform defect).
pub(super) fn pg_batchable_classify(
    resolved: &ResolvedQueryExpr,
    relation_oid: Oid,
    aggregate_functions: &HashSet<EcoString>,
) -> bool {
    let Some(select) = resolved.as_select() else {
        return false;
    };
    if !select.group_by.is_empty() || select.having.is_some() {
        return false;
    }
    if resolved
        .nodes::<ResolvedTableNode>()
        .filter(|t| t.relation_oid == relation_oid)
        .count()
        > 1
    {
        return false;
    }
    match &select.columns {
        ResolvedSelectColumns::None => true,
        ResolvedSelectColumns::Columns(cols) => cols
            .iter()
            .all(|c| !c.expr.has_aggregate(aggregate_functions)),
    }
}

/// Collect column names on `table_name` that participate in the parent
/// query's LIMIT-window definition: top-level ORDER BY, WHERE, and HAVING.
///
/// PGC-94: used by row_cached_invalidation_check to decide whether a CDC
/// UPDATE on a cached row may shift the window such that an untracked
/// row needs to fill the gap. Returns an empty set when the query has
/// no window-defining references on this table.
///
/// Aliased ORDER BY (`ORDER BY count(*) DESC LIMIT 10`) carries no table
/// reference and naturally produces no entries here. Those shapes are
/// Measure queries that already use PgEval/MV invalidation paths.
pub(super) fn limit_window_columns_collect(
    resolved: &ResolvedQueryExpr,
    table_name: &str,
) -> HashSet<EcoString> {
    let mut cols = HashSet::new();
    let mut push_if_local = |col: &ResolvedColumnNode| {
        if col.table.as_str() == table_name {
            cols.insert(col.column.clone());
        }
    };

    let select = resolved.as_select();

    for clause in &resolved.order_by {
        for col in clause.expr.nodes::<ResolvedColumnNode>() {
            push_if_local(col);
        }
        // Aliased `ORDER BY value` resolves to `Identifier`; chase it through
        // the SELECT-list to recover the underlying base-table column refs.
        if let ResolvedScalarExpr::Identifier(name) = &clause.expr
            && let Some(select) = select
            && let ResolvedSelectColumns::Columns(select_cols) = &select.columns
        {
            for select_col in select_cols {
                if select_col.output_name() == Some(name) {
                    for col in select_col.expr.nodes::<ResolvedColumnNode>() {
                        push_if_local(col);
                    }
                }
            }
        }
    }

    if let Some(select) = select {
        if let Some(where_expr) = &select.where_clause {
            for col in where_expr.nodes::<ResolvedColumnNode>() {
                push_if_local(col);
            }
        }
        if let Some(having) = &select.having {
            for col in having.nodes::<ResolvedColumnNode>() {
                push_if_local(col);
            }
        }
    }

    cols
}

/// The ORDER BY key spec for direction-aware window invalidation (PGC-334):
/// `Some` only when the parent query is a plain SELECT (no set-op, GROUP BY,
/// HAVING, or DISTINCT — their windows aren't row-level) and every key
/// resolves to exactly one plain column of `table_name`, directly or through
/// a bare-column SELECT alias. Anything else returns `None` and the window
/// check stays direction-blind (always invalidates). `Default` null ordering
/// resolves per PG semantics — NULLs sort larger than every value, so
/// ASC → last, DESC → first.
pub(super) fn limit_order_keys_collect(
    resolved: &ResolvedQueryExpr,
    table_name: &str,
) -> Option<Box<[OrderByKey]>> {
    let select = resolved.as_select()?;
    if select.distinct || !select.group_by.is_empty() || select.having.is_some() {
        return None;
    }
    if resolved.order_by.is_empty() {
        return None;
    }
    let mut keys = Vec::with_capacity(resolved.order_by.len());
    for clause in &resolved.order_by {
        let column = match &clause.expr {
            ResolvedScalarExpr::Column(col) => col,
            // Aliased `ORDER BY value`: usable only when the aliased SELECT
            // expression is itself a bare column.
            ResolvedScalarExpr::Identifier(name) => {
                let ResolvedSelectColumns::Columns(select_cols) = &select.columns else {
                    return None;
                };
                let aliased = select_cols
                    .iter()
                    .find(|sc| sc.output_name() == Some(name))?;
                match &aliased.expr {
                    ResolvedScalarExpr::Column(col) => col,
                    ResolvedScalarExpr::Identifier(_)
                    | ResolvedScalarExpr::Function(_)
                    | ResolvedScalarExpr::Literal(_)
                    | ResolvedScalarExpr::Case(_)
                    | ResolvedScalarExpr::Arithmetic(_)
                    | ResolvedScalarExpr::Subquery(..)
                    | ResolvedScalarExpr::Array(_)
                    | ResolvedScalarExpr::TypeCast { .. } => return None,
                }
            }
            ResolvedScalarExpr::Function(_)
            | ResolvedScalarExpr::Literal(_)
            | ResolvedScalarExpr::Case(_)
            | ResolvedScalarExpr::Arithmetic(_)
            | ResolvedScalarExpr::Subquery(..)
            | ResolvedScalarExpr::Array(_)
            | ResolvedScalarExpr::TypeCast { .. } => return None,
        };
        if column.table.as_str() != table_name {
            return None;
        }
        let descending = matches!(clause.direction, OrderDirection::Desc);
        let nulls_first = match clause.null_order {
            NullOrder::NullsFirst => true,
            NullOrder::NullsLast => false,
            NullOrder::Default => descending,
        };
        keys.push(OrderByKey {
            column: column.column.clone(),
            descending,
            nulls_first,
        });
    }
    Some(keys.into())
}

/// Collect column names on `table_name` whose values CDC eval reads for this
/// update query: WHERE, from-source join predicates (including any nested
/// subquery internals — over-collection errs toward invalidation, the safe
/// direction), GROUP BY, and HAVING. The outer SELECT list is excluded: it
/// never feeds a membership or invalidation verdict (PGC-264).
pub(super) fn predicate_columns_collect(
    resolved: &ResolvedQueryExpr,
    table_name: &str,
) -> HashSet<EcoString> {
    let local_columns = |cols: &mut HashSet<EcoString>, node: &ResolvedColumnNode| {
        if node.table.as_str() == table_name {
            cols.insert(node.column.clone());
        }
    };
    let mut cols = HashSet::new();
    let Some(select) = resolved.as_select() else {
        // Not a plain select (set-op branch): over-collect every reference.
        for col in resolved.nodes::<ResolvedColumnNode>() {
            local_columns(&mut cols, col);
        }
        return cols;
    };
    for source in &select.from {
        for col in source.nodes::<ResolvedColumnNode>() {
            local_columns(&mut cols, col);
        }
    }
    if let Some(where_expr) = &select.where_clause {
        for col in where_expr.nodes::<ResolvedColumnNode>() {
            local_columns(&mut cols, col);
        }
    }
    for col in &select.group_by {
        local_columns(&mut cols, col);
    }
    if let Some(having) = &select.having {
        for col in having.nodes::<ResolvedColumnNode>() {
            local_columns(&mut cols, col);
        }
    }
    cols
}

#[cfg(test)]
mod classify_tests {

    use super::*;

    use std::collections::HashMap;

    use iddqd::BiHashMap;
    use tokio_postgres::types::Type;

    use crate::cache::query::CacheableQuery;
    use crate::catalog::{ColumnMetadata, ColumnStore, TableMetadata};
    use crate::query::ast::query_expr_parse;
    use crate::query::resolved::query_expr_resolve;

    fn make_table(name: &str, oid: Oid, columns: &[&str]) -> TableMetadata {
        let cols = ColumnStore::new(columns.iter().enumerate().map(|(i, c)| {
            let is_pk = i == 0;
            ColumnMetadata {
                name: (*c).into(),
                position: i16::try_from(i + 1).expect("column position fits in i16"),
                type_oid: if is_pk { 23 } else { 25 },
                data_type: if is_pk { Type::INT4 } else { Type::TEXT },
                type_name: if is_pk { "int4" } else { "text" }.into(),
                cache_type_name: if is_pk { "int4" } else { "text" }.into(),
                is_primary_key: is_pk,
            }
        }));
        TableMetadata {
            replica_identity_full: false,
            relation_oid: oid,
            name: name.into(),
            schema: "public".into(),
            primary_key_columns: vec![columns[0].into()],
            columns: cols,
            indexes: Vec::new(),
        }
    }

    fn resolve(sql: &str, tables: &BiHashMap<TableMetadata>) -> ResolvedQueryExpr {
        let query_expr = query_expr_parse(sql).expect("convert");
        let cacheable = CacheableQuery::try_new(query_expr, &HashMap::new()).expect("cacheable");
        query_expr_resolve(&cacheable.query, tables, &["public"]).expect("resolve")
    }

    fn classify_single_table(sql: &str) -> UpdateEvalStrategy {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(make_table(
            "t",
            Oid::from_raw(1),
            &["id", "name", "status", "age"],
        ));
        let resolved = resolve(sql, &tables);
        update_eval_strategy_classify(&resolved, UpdateQuerySource::FromClause)
    }

    #[test]
    fn simple_equality_is_local_eval() {
        assert_eq!(
            classify_single_table("SELECT * FROM t WHERE id = 5"),
            UpdateEvalStrategy::LocalEval
        );
    }

    #[test]
    fn no_where_is_local_eval() {
        assert_eq!(
            classify_single_table("SELECT * FROM t"),
            UpdateEvalStrategy::LocalEval
        );
    }

    #[test]
    fn and_or_with_comparisons_is_local_eval() {
        assert_eq!(
            classify_single_table("SELECT * FROM t WHERE (id = 1 OR id = 2) AND name IS NOT NULL"),
            UpdateEvalStrategy::LocalEval
        );
    }

    #[test]
    fn in_list_is_pg_eval() {
        // IN is a Multi op — not yet evaluable in Rust
        assert_eq!(
            classify_single_table("SELECT * FROM t WHERE id IN (1, 2, 3)"),
            UpdateEvalStrategy::PgEval
        );
    }

    #[test]
    fn like_is_pg_eval() {
        assert_eq!(
            classify_single_table("SELECT * FROM t WHERE name LIKE 'j%'"),
            UpdateEvalStrategy::PgEval
        );
    }

    #[test]
    fn group_by_is_pg_eval() {
        assert_eq!(
            classify_single_table("SELECT status, count(*) FROM t GROUP BY status"),
            UpdateEvalStrategy::PgEval
        );
    }

    #[test]
    fn multi_table_is_pg_eval() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(make_table("a", Oid::from_raw(1), &["id", "bid"]));
        tables.insert_overwrite(make_table("b", Oid::from_raw(2), &["id", "name"]));
        let resolved = resolve("SELECT * FROM a JOIN b ON a.bid = b.id", &tables);
        assert_eq!(
            update_eval_strategy_classify(&resolved, UpdateQuerySource::FromClause),
            UpdateEvalStrategy::PgEval
        );
    }

    #[test]
    fn non_fromclause_source_is_pg_eval() {
        use crate::cache::SubqueryKind;
        let resolved = resolve("SELECT * FROM t WHERE id = 5", &{
            let mut tables = BiHashMap::new();
            tables.insert_overwrite(make_table("t", Oid::from_raw(1), &["id", "name"]));
            tables
        });
        // Same query, but classified as a subquery-sourced update query
        assert_eq!(
            update_eval_strategy_classify(
                &resolved,
                UpdateQuerySource::Subquery(SubqueryKind::Inclusion),
            ),
            UpdateEvalStrategy::PgEval
        );
    }

    // PGC-264: predicate_columns_collect feeds the toast-fallback gate — a
    // collected column elided as unchanged-toast forces invalidation.

    #[test]
    fn test_predicate_columns_where_collected_select_list_excluded() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(make_table(
            "t",
            Oid::from_raw(1),
            &["id", "name", "status", "age"],
        ));
        let resolved = resolve(
            "SELECT name FROM t WHERE status = 'a' AND age > 'x'",
            &tables,
        );
        let cols = predicate_columns_collect(&resolved, "t");
        assert!(cols.contains("status"));
        assert!(cols.contains("age"));
        assert!(
            !cols.contains("name"),
            "SELECT-list-only column must be excluded"
        );
        assert!(!cols.contains("id"));
    }

    #[test]
    fn test_predicate_columns_join_columns_collected_per_table() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(make_table("t", Oid::from_raw(1), &["id", "name", "status"]));
        tables.insert_overwrite(make_table("u", Oid::from_raw(2), &["id", "t_id", "region"]));
        let resolved = resolve(
            "SELECT t.name FROM t JOIN u ON u.t_id = t.id WHERE u.region = 'x'",
            &tables,
        );
        let t_cols = predicate_columns_collect(&resolved, "t");
        assert!(t_cols.contains("id"), "join column collected");
        assert!(!t_cols.contains("name"));
        let u_cols = predicate_columns_collect(&resolved, "u");
        assert!(u_cols.contains("t_id"));
        assert!(u_cols.contains("region"));
        assert!(!u_cols.contains("id"));
    }

    #[test]
    fn test_predicate_columns_group_by_and_having_collected() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(make_table(
            "t",
            Oid::from_raw(1),
            &["id", "name", "status", "age"],
        ));
        let resolved = resolve(
            "SELECT status, count(id) FROM t GROUP BY status HAVING count(age) > 1",
            &tables,
        );
        let cols = predicate_columns_collect(&resolved, "t");
        assert!(cols.contains("status"), "GROUP BY column collected");
        assert!(cols.contains("age"), "HAVING column collected");
        assert!(
            !cols.contains("id"),
            "aggregate arg only in the SELECT list must be excluded"
        );
    }
}
