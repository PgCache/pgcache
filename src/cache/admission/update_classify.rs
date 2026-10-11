//! Registration-time classifiers over an update query and the query it
//! came from: how CDC evaluates its WHERE, whether rows batch, and which
//! columns define a LIMIT window.

use std::collections::HashSet;
use std::ops::ControlFlow;

use ecow::EcoString;

use crate::cache::update_query::{OrderByKey, UpdateEvalStrategy, UpdateQuerySource};
use crate::oid::Oid;
use crate::query::ast::{
    AstNode, ColumnNode, JoinNode, JoinQual, NullOrder, OrderByClause, OrderDirection, QueryExpr,
    ScalarExpr, SelectColumn, SelectColumns, SelectNode,
};
use crate::query::evaluate::where_expr_supported;
use crate::query::resolve::{ColumnBinding, ColumnTarget, ResolvedQuery};

/// Conservative classifier: rejects anything the Rust evaluator can't decide
/// from a single CDC row. GROUP BY / HAVING are rejected because row-level
/// matching doesn't capture post-aggregation filtering. Non-FromClause sources
/// are rejected because their CDC semantics (subquery membership, outer join
/// null-padding cascade) aren't expressible as a row-level predicate.
pub(super) fn update_eval_strategy_classify(
    bound: &ResolvedQuery,
    source: UpdateQuerySource,
) -> UpdateEvalStrategy {
    if source != UpdateQuerySource::FromClause {
        return UpdateEvalStrategy::PgEval;
    }
    let Some(select) = bound.select() else {
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
    if where_expr_supported(bound, where_expr) {
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
/// ORDER BY never reach the predicate, and aggregates can't appear in WHERE.)
///
/// Relations appearing more than once (self-joins) are excluded: a
/// single-occurrence substitution is not a complete membership test
/// (PGC-256), and batching pre-evaluates unfiltered.
pub(super) fn pg_batchable_classify(
    bound: &ResolvedQuery,
    relation_oid: Oid,
    aggregate_functions: &HashSet<EcoString>,
) -> bool {
    let Some(select) = bound.select() else {
        return false;
    };
    if !select.group_by.is_empty() || select.having.is_some() {
        return false;
    }
    if bound.relation_occurrences(relation_oid) > 1 {
        return false;
    }
    match &select.columns {
        SelectColumns::None => true,
        SelectColumns::Columns(columns) => columns.iter().all(|column| match column {
            SelectColumn::Expr { expr, .. } => !expr.has_aggregate(aggregate_functions),
            SelectColumn::Star(_) => true,
        }),
    }
}

/// Column names of one table, collected from column references anywhere
/// under the nodes added.
struct ReferencedColumns<'a> {
    bound: &'a ResolvedQuery,
    table: &'a str,
    columns: HashSet<EcoString>,
}

impl<'a> ReferencedColumns<'a> {
    fn new(bound: &'a ResolvedQuery, table: &'a str) -> Self {
        Self {
            bound,
            table,
            columns: HashSet::new(),
        }
    }

    fn add(&mut self, column: &ColumnNode) {
        if let Some(binding) = self.bound.column_binding(column) {
            self.add_binding(&binding);
        }
    }

    fn add_binding(&mut self, binding: &ColumnBinding) {
        let bindings = self.bound.bindings();
        if let (Some(slot), Some(column)) = (
            bindings.slot(binding.slot),
            bindings.binding_column(binding),
        ) && slot.table().name.as_str() == self.table
        {
            self.columns.insert(column.name.clone());
        }
    }

    /// Every column reference under `node` (zero-allocation walk), plus
    /// both sides of each column a USING or NATURAL join under it merges:
    /// the tree names those without a column reference.
    fn add_refs<N: AstNode>(&mut self, node: &N) {
        let _ = node.try_for_each_node::<ColumnNode, ()>(&mut |column| {
            self.add(column);
            ControlFlow::Continue(())
        });
        let _ = node.try_for_each_node::<JoinNode, ()>(&mut |join| {
            if matches!(join.qual, JoinQual::Using(_) | JoinQual::Natural) {
                for merged in self.bound.bindings().merged_columns() {
                    if merged.join.index() == join.id.index() {
                        self.add_binding(&merged.left);
                        self.add_binding(&merged.right);
                    }
                }
            }
            ControlFlow::Continue(())
        });
    }
}

/// The SELECT-list expressions an `ORDER BY alias` names (output-name match).
fn alias_exprs<'s>(
    bound: &'s ResolvedQuery,
    select: &'s SelectNode,
    name: &'s EcoString,
) -> impl Iterator<Item = &'s ScalarExpr> {
    let columns: &[SelectColumn] = match &select.columns {
        SelectColumns::Columns(columns) => columns,
        SelectColumns::None => &[],
    };
    columns.iter().filter_map(move |column| match column {
        SelectColumn::Expr { expr, alias } => {
            let output = alias.clone().or_else(|| bound.column_output_name(expr));
            (output.as_ref() == Some(name)).then_some(expr)
        }
        SelectColumn::Star(_) => None,
    })
}

/// The output an ORDER BY item names, when it names one.
fn order_by_output<'s>(bound: &ResolvedQuery, clause: &'s OrderByClause) -> Option<&'s EcoString> {
    let ScalarExpr::Column(column) = &clause.expr else {
        return None;
    };
    matches!(bound.column(column), Some(ColumnTarget::Output)).then_some(&column.column)
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
    bound: &ResolvedQuery,
    table_name: &str,
) -> HashSet<EcoString> {
    let mut cols = ReferencedColumns::new(bound, table_name);
    let query: &QueryExpr = bound.tree();
    let select = bound.select();
    for clause in &query.order_by {
        cols.add_refs(&clause.expr);
        // Aliased `ORDER BY value` names an output; chase it through the
        // SELECT list to recover the underlying base-table column refs.
        if let Some(name) = order_by_output(bound, clause)
            && let Some(select) = select
        {
            for expr in alias_exprs(bound, select, name) {
                cols.add_refs(expr);
            }
        }
    }
    if let Some(select) = select {
        if let Some(where_expr) = &select.where_clause {
            cols.add_refs(where_expr);
        }
        if let Some(having) = &select.having {
            cols.add_refs(having);
        }
    }
    cols.columns
}

/// The ORDER BY key spec for direction-aware window invalidation (PGC-334):
/// `Some` only when the parent query is a plain SELECT (no set-op, GROUP BY,
/// HAVING, or DISTINCT — their windows aren't row-level) and every key
/// resolves to exactly one plain column of `table_name`, directly or through
/// a bare-column SELECT alias. Anything else returns `None` and the window
/// check stays direction-blind (always invalidates).
pub(super) fn limit_order_keys_collect(
    bound: &ResolvedQuery,
    table_name: &str,
) -> Option<Box<[OrderByKey]>> {
    let select = bound.select()?;
    let query: &QueryExpr = bound.tree();
    if !window_is_row_level(query, select) {
        return None;
    }
    let mut keys = Vec::with_capacity(query.order_by.len());
    for clause in &query.order_by {
        let column = order_key(bound, clause, select)?;
        if bound.column_table(column)?.name.as_str() != table_name {
            return None;
        }
        keys.push(order_key_build(bound.column_name(column)?, clause));
    }
    Some(keys.into())
}

/// A window whose rows are the SELECT's own rows: ordered, and no DISTINCT,
/// GROUP BY or HAVING reshaping them.
fn window_is_row_level(query: &QueryExpr, select: &SelectNode) -> bool {
    let reshaped = select.distinct || !select.group_by.is_empty() || select.having.is_some();
    !reshaped && !query.order_by.is_empty()
}

/// The plain column an ORDER BY key sorts by: the key itself, or the bare
/// column an `ORDER BY alias` names.
fn order_key<'s>(
    bound: &'s ResolvedQuery,
    clause: &'s OrderByClause,
    select: &'s SelectNode,
) -> Option<&'s ColumnNode> {
    let target = match order_by_output(bound, clause) {
        Some(name) => alias_exprs(bound, select, name).next()?,
        None => &clause.expr,
    };
    if let ScalarExpr::Column(column) = target {
        Some(column)
    } else {
        None
    }
}

/// `Default` null ordering resolves per PG semantics — NULLs sort larger than
/// every value, so ASC → last, DESC → first.
fn order_key_build(column: &EcoString, clause: &OrderByClause) -> OrderByKey {
    let descending = matches!(clause.direction, OrderDirection::Desc);
    let nulls_first = match clause.null_order {
        NullOrder::NullsFirst => true,
        NullOrder::NullsLast => false,
        NullOrder::Default => descending,
    };
    OrderByKey {
        column: column.clone(),
        descending,
        nulls_first,
    }
}

/// Collect column names on `table_name` whose values CDC eval reads for this
/// update query: WHERE, from-source join predicates (including any nested
/// subquery internals — over-collection errs toward invalidation, the safe
/// direction), GROUP BY, and HAVING. The outer SELECT list is excluded: it
/// never feeds a membership or invalidation verdict (PGC-264).
pub(super) fn predicate_columns_collect(
    bound: &ResolvedQuery,
    table_name: &str,
) -> HashSet<EcoString> {
    let mut cols = ReferencedColumns::new(bound, table_name);
    let Some(select) = bound.select() else {
        // Not a plain select (set-op branch): over-collect every reference.
        cols.add_refs(bound.tree());
        return cols.columns;
    };
    for source in &select.from {
        cols.add_refs(source);
    }
    if let Some(where_expr) = &select.where_clause {
        cols.add_refs(where_expr);
    }
    for column in &select.group_by {
        cols.add(column);
    }
    if let Some(having) = &select.having {
        cols.add_refs(having);
    }
    cols.columns
}

#[cfg(test)]
mod classify_tests {

    use std::collections::HashMap;

    use iddqd::BiHashMap;
    use postgres_types::Type;

    use super::*;
    use crate::cache::query::CacheableQuery;
    use crate::catalog::{Catalog, ColumnMetadata, ColumnPosition, ColumnStore, TableMetadata};
    use crate::oid::TypeOid;
    use crate::query::ast::query_expr_parse;
    use crate::query::resolve::{ResolvedQuery, query_expr_resolve_bound};

    fn make_table(name: &str, oid: Oid, columns: &[&str]) -> TableMetadata {
        let cols = ColumnStore::new(columns.iter().enumerate().map(|(i, c)| {
            let is_pk = i == 0;
            ColumnMetadata {
                name: (*c).into(),
                position: ColumnPosition::from_index(i).expect("column position in range"),
                type_oid: TypeOid::from_raw(if is_pk { 23 } else { 25 }),
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

    fn resolve(sql: &str, tables: &Catalog) -> ResolvedQuery {
        let query_expr = query_expr_parse(sql).expect("convert");
        let cacheable = CacheableQuery::try_new(query_expr, &HashMap::new()).expect("cacheable");
        let query = std::sync::Arc::new(cacheable.query().clone());
        let bindings = query_expr_resolve_bound(&query, tables, &["public"]).expect("resolve");
        ResolvedQuery::new(query, bindings)
    }

    fn classify_single_table(sql: &str) -> UpdateEvalStrategy {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(
            make_table("t", Oid::from_raw(1), &["id", "name", "status", "age"]).into(),
        );
        let bound = resolve(sql, &tables);
        update_eval_strategy_classify(&bound, UpdateQuerySource::FromClause)
    }

    #[test]
    fn test_simple_equality_is_local_eval() {
        assert_eq!(
            classify_single_table("SELECT * FROM t WHERE id = 5"),
            UpdateEvalStrategy::LocalEval
        );
    }

    #[test]
    fn test_no_where_is_local_eval() {
        assert_eq!(
            classify_single_table("SELECT * FROM t"),
            UpdateEvalStrategy::LocalEval
        );
    }

    #[test]
    fn test_and_or_with_comparisons_is_local_eval() {
        assert_eq!(
            classify_single_table("SELECT * FROM t WHERE (id = 1 OR id = 2) AND name IS NOT NULL"),
            UpdateEvalStrategy::LocalEval
        );
    }

    #[test]
    fn test_in_list_is_pg_eval() {
        // IN is a Multi op — not yet evaluable in Rust
        assert_eq!(
            classify_single_table("SELECT * FROM t WHERE id IN (1, 2, 3)"),
            UpdateEvalStrategy::PgEval
        );
    }

    #[test]
    fn test_like_is_pg_eval() {
        assert_eq!(
            classify_single_table("SELECT * FROM t WHERE name LIKE 'j%'"),
            UpdateEvalStrategy::PgEval
        );
    }

    #[test]
    fn test_group_by_is_pg_eval() {
        assert_eq!(
            classify_single_table("SELECT status, count(*) FROM t GROUP BY status"),
            UpdateEvalStrategy::PgEval
        );
    }

    #[test]
    fn test_multi_table_is_pg_eval() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(make_table("a", Oid::from_raw(1), &["id", "bid"]).into());
        tables.insert_overwrite(make_table("b", Oid::from_raw(2), &["id", "name"]).into());
        let bound = resolve("SELECT * FROM a JOIN b ON a.bid = b.id", &tables);
        assert_eq!(
            update_eval_strategy_classify(&bound, UpdateQuerySource::FromClause),
            UpdateEvalStrategy::PgEval
        );
    }

    #[test]
    fn test_non_fromclause_source_is_pg_eval() {
        use crate::cache::SubqueryKind;
        let bound = resolve("SELECT * FROM t WHERE id = 5", &{
            let mut tables = BiHashMap::new();
            tables.insert_overwrite(make_table("t", Oid::from_raw(1), &["id", "name"]).into());
            tables
        });
        // Same query, but classified as a subquery-sourced update query
        assert_eq!(
            update_eval_strategy_classify(
                &bound,
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
        tables.insert_overwrite(
            make_table("t", Oid::from_raw(1), &["id", "name", "status", "age"]).into(),
        );
        let bound = resolve(
            "SELECT name FROM t WHERE status = 'a' AND age > 'x'",
            &tables,
        );
        let cols = predicate_columns_collect(&bound, "t");
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
        tables
            .insert_overwrite(make_table("t", Oid::from_raw(1), &["id", "name", "status"]).into());
        tables
            .insert_overwrite(make_table("u", Oid::from_raw(2), &["id", "t_id", "region"]).into());
        let bound = resolve(
            "SELECT t.name FROM t JOIN u ON u.t_id = t.id WHERE u.region = 'x'",
            &tables,
        );
        let t_cols = predicate_columns_collect(&bound, "t");
        assert!(t_cols.contains("id"), "join column collected");
        assert!(!t_cols.contains("name"));
        let u_cols = predicate_columns_collect(&bound, "u");
        assert!(u_cols.contains("t_id"));
        assert!(u_cols.contains("region"));
        assert!(!u_cols.contains("id"));
    }

    #[test]
    fn test_predicate_columns_group_by_and_having_collected() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(
            make_table("t", Oid::from_raw(1), &["id", "name", "status", "age"]).into(),
        );
        let bound = resolve(
            "SELECT status, count(id) FROM t GROUP BY status HAVING count(age) > 1",
            &tables,
        );
        let cols = predicate_columns_collect(&bound, "t");
        assert!(cols.contains("status"), "GROUP BY column collected");
        assert!(cols.contains("age"), "HAVING column collected");
        assert!(
            !cols.contains("id"),
            "aggregate arg only in the SELECT list must be excluded"
        );
    }
}
