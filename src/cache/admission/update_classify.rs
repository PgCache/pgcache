//! Registration-time classifiers over an update query and the query it
//! came from: how CDC evaluates its WHERE, whether rows batch, and which
//! columns define a LIMIT window.

use std::collections::HashSet;

use ecow::EcoString;

use crate::cache::update_query::{OrderByKey, UpdateEvalStrategy, UpdateQuerySource};
use crate::query::ast::rope::{
    Column, JoinQualKind, NodeKind, NodeRef, OrderBy, ScalarKind, Select, SelectItem, Source, View,
};
use crate::query::ast::{NodeId, NullOrder, OrderDirection};
use crate::query::evaluate::where_expr_supported;
use crate::query::resolve::{ColumnBinding, OrderBySource, QueryBranch, ResolvedQuery, StarItem};

/// How CDC decides a changed row's membership in a branch: locally, when
/// the branch reads one table and its WHERE is one the compiled predicate
/// decides; otherwise through the cache PG.
pub(super) fn update_eval_strategy_classify(
    branch: QueryBranch<'_>,
    source: UpdateQuerySource,
) -> UpdateEvalStrategy {
    if source != UpdateQuerySource::FromClause {
        return UpdateEvalStrategy::PgEval;
    }
    let select = branch.select;
    if !select_is_single_table(select) {
        return UpdateEvalStrategy::PgEval;
    }
    if select.group_by().len() > 0 || select.having().is_some() {
        return UpdateEvalStrategy::PgEval;
    }
    let Some(where_expr) = select.where_clause() else {
        return UpdateEvalStrategy::LocalEval;
    };
    if where_expr_supported(branch.bound, where_expr) {
        UpdateEvalStrategy::LocalEval
    } else {
        UpdateEvalStrategy::PgEval
    }
}

/// Whether FROM is exactly one table reference.
fn select_is_single_table(select: Select<'_>) -> bool {
    let mut from = select.from();
    matches!((from.next(), from.next()), (Some(Source::Table(_)), None))
}

/// Whether many CDC rows can be evaluated against the branch in one
/// multi-row VALUES (PGC-241): no GROUP BY, HAVING or aggregate. Only the
/// entry's own reference is substituted, so another reference to the
/// relation reads the cache table and each row's answer stays its own.
pub(super) fn pg_batchable_classify(
    branch: QueryBranch<'_>,
    aggregate_functions: &HashSet<EcoString>,
) -> bool {
    let select = branch.select;
    if select.group_by().len() > 0 || select.having().is_some() {
        return false;
    }
    match select.items() {
        None => true,
        Some(mut items) => items.all(|item| match item {
            SelectItem::Expr(expr) => !expr.expr().has_aggregate(aggregate_functions),
            SelectItem::Star(_) => true,
        }),
    }
}

/// Collects the columns of one table that a set of expressions reads.
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

    fn add(&mut self, column: Column<'_>) {
        if let Some(binding) = self.bound.column_binding_at(column.id()) {
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

    /// Every column reference under `node`, and both sides of every
    /// `USING`/`NATURAL` join under it.
    fn add_refs(&mut self, node: NodeRef<'_>) {
        for position in node.subtree() {
            if matches!(position.kind(), NodeKind::Column { .. }) {
                self.add(Column::from_ref(position));
            } else if let NodeKind::Join {
                qual: JoinQualKind::Using(_) | JoinQualKind::Natural,
                ..
            } = position.kind()
            {
                self.merged_join_add(position.id());
            }
        }
    }

    /// A star-expanded output's column, both sides of a merged one.
    fn star_item_add(&mut self, item: &StarItem) {
        match item {
            StarItem::Column(binding) => self.add_binding(binding),
            StarItem::Merged(id) => {
                let Some(merged) = self.bound.bindings().merged(*id) else {
                    return;
                };
                let (left, right) = (merged.left, merged.right);
                self.add_binding(&left);
                self.add_binding(&right);
            }
        }
    }

    /// Both sides of a `USING`/`NATURAL` join's merged columns.
    fn merged_join_add(&mut self, join: NodeId) {
        let merged: Vec<(ColumnBinding, ColumnBinding)> = self
            .bound
            .bindings()
            .merged_columns()
            .iter()
            .filter(|merged| merged.join == join)
            .map(|merged| (merged.left, merged.right))
            .collect();
        for (left, right) in merged {
            self.add_binding(&left);
            self.add_binding(&right);
        }
    }
}

/// The columns of `table_name` that decide a LIMIT window: ORDER BY keys
/// (through output names and positions), WHERE and HAVING.
pub(super) fn limit_window_columns_collect(
    bound: &ResolvedQuery,
    table_name: &str,
) -> HashSet<EcoString> {
    let mut cols = ReferencedColumns::new(bound, table_name);
    let query = bound.tree().root();
    let select = bound.select();
    for clause in query.order_by() {
        cols.add_refs(clause.expr().node_ref());
        match select.and_then(|select| bound.order_by_source(select, clause)) {
            Some(OrderBySource::Expr(expr)) => cols.add_refs(expr.node_ref()),
            Some(OrderBySource::Star(item)) => cols.star_item_add(item),
            None => {}
        }
    }
    if let Some(select) = select {
        if let Some(where_expr) = select.where_clause() {
            cols.add_refs(where_expr.node_ref());
        }
        if let Some(having) = select.having() {
            cols.add_refs(having.node_ref());
        }
    }
    cols.columns
}

/// The ORDER BY keys of a row-level LIMIT window as plain columns of
/// `table_name`, or `None` when any key is not one (PGC-334).
pub(super) fn limit_order_keys_collect(
    bound: &ResolvedQuery,
    table_name: &str,
) -> Option<Box<[OrderByKey]>> {
    let select = bound.select()?;
    let query = bound.tree().root();
    if !window_is_row_level(select) {
        return None;
    }
    let order_by = query.order_by();
    let mut keys = Vec::with_capacity(order_by.len());
    let bindings = bound.bindings();
    for clause in order_by {
        let binding = order_key(bound, clause, select)?;
        if bindings.slot(binding.slot)?.table().name.as_str() != table_name {
            return None;
        }
        keys.push(order_key_build(
            &bindings.binding_column(&binding)?.name,
            clause,
        ));
    }
    Some(keys.into())
}

/// A window over rows as they are: no DISTINCT, GROUP BY or HAVING, and an
/// ORDER BY to rank them by.
fn window_is_row_level(select: Select<'_>) -> bool {
    let reshaped = select.distinct() || select.group_by().len() > 0 || select.having().is_some();
    !reshaped && select.node_ref().tree().root().order_by().len() > 0
}

/// The column an ORDER BY item ranks by, through an output name or position.
fn order_key<'s>(
    bound: &'s ResolvedQuery,
    clause: OrderBy<'s>,
    select: Select<'s>,
) -> Option<ColumnBinding> {
    match bound.order_by_source(select, clause)? {
        OrderBySource::Expr(expr) => match expr.kind() {
            ScalarKind::Column(column) => bound.column_binding_at(column.id()),
            ScalarKind::Literal(_)
            | ScalarKind::Function(_)
            | ScalarKind::Case(_)
            | ScalarKind::Arithmetic(_)
            | ScalarKind::Subquery(_)
            | ScalarKind::Array(_)
            | ScalarKind::TypeCast(_) => None,
        },
        OrderBySource::Star(StarItem::Column(binding)) => Some(*binding),
        OrderBySource::Star(StarItem::Merged(_)) => None,
    }
}

fn order_key_build(column: &EcoString, clause: OrderBy<'_>) -> OrderByKey {
    let descending = matches!(clause.direction(), OrderDirection::Desc);
    let nulls_first = match clause.null_order() {
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

/// The columns of `table_name` whose values CDC evaluation reads: FROM
/// (join predicates), WHERE, GROUP BY and HAVING, not the select list.
pub(super) fn predicate_columns_collect(
    branch: QueryBranch<'_>,
    table_name: &str,
) -> HashSet<EcoString> {
    let mut cols = ReferencedColumns::new(branch.bound, table_name);
    let select = branch.select;
    for source in select.from() {
        cols.add_refs(source.node_ref());
    }
    if let Some(where_expr) = select.where_clause() {
        cols.add_refs(where_expr.node_ref());
    }
    for column in select.group_by() {
        cols.add(column);
    }
    if let Some(having) = select.having() {
        cols.add_refs(having.node_ref());
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
    use crate::catalog::{Catalog, ColumnMetadata, ColumnPosition, TableMetadata};
    use crate::oid::{Oid, TypeOid};
    use crate::query::ast::query_tree_parse;
    use crate::query::resolve::ResolvedQuery;

    fn make_table(name: &str, oid: Oid, columns: &[&str]) -> TableMetadata {
        let cols = columns.iter().enumerate().map(|(i, c)| {
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
        });
        TableMetadata::for_test(name, oid.get(), cols)
    }

    fn resolve(sql: &str, tables: &Catalog) -> ResolvedQuery {
        let tree = query_tree_parse(sql).expect("convert");
        let cacheable = CacheableQuery::try_new(tree, &HashMap::new()).expect("cacheable");
        let tree = cacheable.tree().clone();
        ResolvedQuery::resolve(tree, tables, &["public"]).expect("resolve")
    }

    fn branch(bound: &ResolvedQuery) -> QueryBranch<'_> {
        QueryBranch {
            bound,
            select: bound.select().expect("a select"),
        }
    }

    fn classify_single_table(sql: &str) -> UpdateEvalStrategy {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(
            make_table("t", Oid::from_raw(1), &["id", "name", "status", "age"]).into(),
        );
        let bound = resolve(sql, &tables);
        update_eval_strategy_classify(branch(&bound), UpdateQuerySource::FromClause)
    }

    /// A LIMIT window's sort keys resolve through output names and positions,
    /// stars expanded, to the table's columns; an expression key has no plain
    /// column to rank by (PGC-527).
    #[test]
    fn test_limit_window_columns_and_order_keys() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(
            make_table("t", Oid::from_raw(1), &["id", "name", "status", "age"]).into(),
        );
        for (sql, window, keys) in [
            (
                "SELECT name, age FROM t ORDER BY 2 DESC LIMIT 3",
                &["age"][..],
                Some(&["age desc"][..]),
            ),
            (
                "SELECT * FROM t ORDER BY 4 LIMIT 3",
                &["age"],
                Some(&["age asc"][..]),
            ),
            (
                "SELECT name AS n FROM t ORDER BY n LIMIT 3",
                &["name"],
                Some(&["name asc"][..]),
            ),
            (
                "SELECT name FROM t WHERE status = 'x' ORDER BY age LIMIT 3",
                &["age", "status"],
                Some(&["age asc"][..]),
            ),
            (
                "SELECT name FROM t ORDER BY age + 1 LIMIT 3",
                &["age"],
                None,
            ),
        ] {
            let bound = resolve(sql, &tables);
            let mut actual: Vec<String> = limit_window_columns_collect(&bound, "t")
                .into_iter()
                .map(|column| column.to_string())
                .collect();
            actual.sort();
            assert_eq!(actual, window, "{sql}");
            let actual_keys: Option<Vec<String>> =
                limit_order_keys_collect(&bound, "t").map(|keys| {
                    keys.iter()
                        .map(|key| {
                            format!(
                                "{} {}",
                                key.column,
                                if key.descending { "desc" } else { "asc" }
                            )
                        })
                        .collect()
                });
            assert_eq!(
                actual_keys,
                keys.map(|keys| keys.iter().map(ToString::to_string).collect()),
                "{sql}"
            );
        }
    }

    /// A relation's other references don't stop its rows batching: each entry
    /// substitutes only its own (PGC-521). Grouping and aggregates still do.
    #[test]
    fn test_pg_batchable_with_repeated_relation() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(
            make_table("t", Oid::from_raw(1), &["id", "name", "status", "age"]).into(),
        );
        let aggregate_functions = HashSet::from([EcoString::from("count")]);
        for (sql, batchable) in [
            ("SELECT * FROM t a JOIN t b ON b.id = a.id", true),
            (
                "SELECT * FROM t WHERE name IN (SELECT name FROM t WHERE status = 'x')",
                true,
            ),
            (
                "SELECT a.status, count(*) FROM t a JOIN t b ON b.id = a.id GROUP BY a.status",
                false,
            ),
        ] {
            let bound = resolve(sql, &tables);
            assert_eq!(
                pg_batchable_classify(branch(&bound), &aggregate_functions),
                batchable,
                "{sql}"
            );
        }
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
            update_eval_strategy_classify(branch(&bound), UpdateQuerySource::FromClause),
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
                branch(&bound),
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
        let cols = predicate_columns_collect(branch(&bound), "t");
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
        let t_cols = predicate_columns_collect(branch(&bound), "t");
        assert!(t_cols.contains("id"), "join column collected");
        assert!(!t_cols.contains("name"));
        let u_cols = predicate_columns_collect(branch(&bound), "u");
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
        let cols = predicate_columns_collect(branch(&bound), "t");
        assert!(cols.contains("status"), "GROUP BY column collected");
        assert!(cols.contains("age"), "HAVING column collected");
        assert!(
            !cols.contains("id"),
            "aggregate arg only in the SELECT list must be excluded"
        );
    }
}
