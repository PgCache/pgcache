#![allow(clippy::wildcard_enum_match_arm)]

use ecow::EcoString;
use iddqd::BiHashMap;
use postgres_types::Type;

use std::sync::Arc;

use super::*;
use crate::catalog::{Catalog, ColumnMetadata, ColumnPosition, TableMetadata};
use crate::oid::{Oid, TypeOid};
use crate::query::ast::rope::{
    Binary, Body, Column, Function, Join, JoinQual, NodeKind, ScalarKind, Select, SelectItem,
    Source, Table, Where, WhereKind,
};
use crate::query::ast::{
    BinaryOp, JoinType, LiteralValue, OrderDirection, SubLinkType, query_tree_parse,
};
use crate::query::resolve::ResolveError;
use crate::query::update::select_branches_with_source;

// ============================================================================
// Catalogs
// ============================================================================

/// `name(id int4, name text)` with `id` the key.
fn test_table_metadata(name: &str, relation_oid: Oid) -> TableMetadata {
    let columns = [
        ColumnMetadata {
            name: "id".into(),
            position: ColumnPosition::from_raw(1),
            type_oid: TypeOid::from_raw(23),
            data_type: Type::INT4,
            type_name: "int4".into(),
            cache_type_name: "int4".into(),
            is_primary_key: true,
        },
        ColumnMetadata {
            name: "name".into(),
            position: ColumnPosition::from_raw(2),
            type_oid: TypeOid::from_raw(25),
            data_type: Type::TEXT,
            type_name: "text".into(),
            cache_type_name: "text".into(),
            is_primary_key: false,
        },
    ];

    TableMetadata::for_test(name, relation_oid.get(), columns)
}

/// A catalog of `test_table_metadata` tables (`id int4`, `name text`), with
/// relation OIDs from 1001 in order.
pub(super) fn catalog(names: &[&str]) -> Catalog {
    let mut tables = BiHashMap::new();
    for (oid, name) in (1001..).zip(names) {
        tables.insert_overwrite(test_table_metadata(name, Oid::from_raw(oid)).into());
    }
    tables
}

/// Create test table metadata with custom column names (all text type, first is PK).
fn test_table_metadata_with_columns(
    name: &str,
    relation_oid: Oid,
    column_names: &[&str],
) -> TableMetadata {
    let columns = column_names
        .iter()
        .enumerate()
        .map(|(i, col_name)| ColumnMetadata {
            name: (*col_name).into(),
            position: ColumnPosition::from_index(i).expect("column position in range"),
            type_oid: TypeOid::from_raw(25),
            data_type: Type::TEXT,
            type_name: "text".into(),
            cache_type_name: "text".into(),
            is_primary_key: i == 0,
        });
    TableMetadata::for_test(name, relation_oid.get(), columns)
}

/// A catalog of all-text tables given as (name, columns), OIDs from 1001.
pub(super) fn catalog_with_columns(specs: &[(&str, &[&str])]) -> Catalog {
    let mut tables = BiHashMap::new();
    for (oid, (name, columns)) in (1001..).zip(specs) {
        tables.insert_overwrite(
            test_table_metadata_with_columns(name, Oid::from_raw(oid), columns).into(),
        );
    }
    tables
}

// ============================================================================
// Binding helpers
// ============================================================================

/// Parse and bind `sql` over `tables`.
fn bound(sql: &str, tables: &Catalog) -> ResolvedQuery {
    ResolvedQuery::parse_resolve(sql, tables, &["public"]).expect("resolve")
}

/// The error binding `sql` over `tables` ends in.
fn bind_error(sql: &str, tables: &Catalog) -> ResolveError {
    match ResolvedQuery::parse_resolve(sql, tables, &["public"]) {
        Ok(_) => panic!("expected {sql} not to resolve"),
        Err(e) => e.into_current_context(),
    }
}

fn select_of(bound: &ResolvedQuery) -> Select<'_> {
    bound.select().expect("a SELECT")
}

/// `(table, column)` a column reference is bound to.
fn column_ref(bound: &ResolvedQuery, column: Column<'_>) -> (String, String) {
    let table = bound.column_table_at(column.id()).expect("a bound column");
    let name = bound.column_name_at(column.id()).expect("a bound column");
    (table.name.to_string(), name.to_string())
}

fn as_binary(expr: Where<'_>) -> Binary<'_> {
    match expr.kind() {
        WhereKind::Binary(binary) => binary,
        _ => panic!("expected a binary expression"),
    }
}

fn as_column(expr: Where<'_>) -> Column<'_> {
    match expr.kind() {
        WhereKind::Scalar(scalar) => match scalar.kind() {
            ScalarKind::Column(column) => column,
            _ => panic!("expected a column"),
        },
        _ => panic!("expected a column"),
    }
}

fn only_table(select: Select<'_>) -> Table<'_> {
    let mut from = select.from();
    match (from.next(), from.next()) {
        (Some(Source::Table(table)), None) => table,
        _ => panic!("expected a single table source"),
    }
}

fn only_join(select: Select<'_>) -> Join<'_> {
    let mut from = select.from();
    match (from.next(), from.next()) {
        (Some(Source::Join(join)), None) => join,
        _ => panic!("expected a single join source"),
    }
}

fn as_table(source: Source<'_>) -> Table<'_> {
    match source {
        Source::Table(table) => table,
        _ => panic!("expected a table source"),
    }
}

fn join_on(join: Join<'_>) -> Where<'_> {
    match join.qual() {
        JoinQual::On(on) => on,
        _ => panic!("expected an ON join"),
    }
}

/// The function a select item holds.
fn item_function(item: SelectItem<'_>) -> Function<'_> {
    match item {
        SelectItem::Expr(expr) => match expr.expr().kind() {
            ScalarKind::Function(function) => function,
            _ => panic!("expected a function"),
        },
        SelectItem::Star(_) => panic!("expected an expression item"),
    }
}

/// The base slot a table node is bound to: `(schema, table, alias, oid)`.
fn table_slot(bound: &ResolvedQuery, table: Table<'_>) -> (String, String, Option<String>, u32) {
    let slot = bound.bindings().source(table.id()).expect("table bound");
    match bound.bindings().slot(slot).expect("slot") {
        TableSlot::Base { table, alias } => (
            table.schema.to_string(),
            table.name.to_string(),
            alias.as_ref().map(ToString::to_string),
            table.relation_oid.get(),
        ),
        TableSlot::Derived { .. } => panic!("table node bound to a derived slot"),
    }
}

/// `(table, column)` of every correlated reference, in tree order.
fn correlated_refs(bound: &ResolvedQuery) -> Vec<(String, String)> {
    let bindings = bound.bindings();
    bindings
        .correlated()
        .iter()
        .map(|id| {
            let Some(ColumnTarget::Column(binding)) = bindings.column_by_id(*id) else {
                panic!("correlated node without a column binding");
            };
            let slot = bindings.slot(binding.slot).expect("slot");
            let column = bindings.binding_column(binding).expect("column");
            (slot.table().name.to_string(), column.name.to_string())
        })
        .collect()
}

fn pair(table: &str, column: &str) -> (String, String) {
    (table.to_owned(), column.to_owned())
}

/// The name of every base table in the tree, in pre-order.
fn table_names(bound: &ResolvedQuery) -> Vec<&str> {
    bound
        .tree()
        .nodes()
        .filter_map(|node| match &node.kind {
            NodeKind::Table { name, .. } => Some(name.as_str()),
            _ => None,
        })
        .collect()
}

// ============================================================================
// Tables and columns
// ============================================================================

#[test]
fn test_table_and_column_bind_cases() {
    let tables = catalog(&["users"]);
    // (label, SQL, expected FROM alias, WHERE lhs column, its type name if checked)
    #[rustfmt::skip]
    let cases = [
        ("bare table, qualified column",   "SELECT * FROM users WHERE users.id = 1",          None,      "id",   Some("int4")),
        ("aliased table, aliased column",  "SELECT * FROM users u WHERE u.name = 'john'",     Some("u"), "name", Some("text")),
        ("bare table, unqualified column", "SELECT * FROM users WHERE id = 1",                None,      "id",   None),
    ];
    for (label, sql, alias, column, type_name) in cases {
        let query = bound(sql, &tables);
        let select = select_of(&query);
        let table = only_table(select);
        assert_eq!(
            table_slot(&query, table),
            ("public".into(), "users".into(), alias.map(Into::into), 1001),
            "{label}: table of {sql:?}"
        );
        let binary = as_binary(select.where_clause().expect("WHERE"));
        let col = as_column(binary.lexpr());
        assert_eq!(
            column_ref(&query, col),
            pair("users", column),
            "{label}: WHERE column of {sql:?}"
        );
        if let Some(type_name) = type_name {
            assert_eq!(
                query.column_type_at(col.id()).expect("column type").name(),
                type_name,
                "{label}: column type of {sql:?}"
            );
        }
    }
}

#[test]
fn test_bind_error_cases() {
    assert!(matches!(
        bind_error("SELECT * FROM users", &BiHashMap::new()),
        ResolveError::TableNotFound { .. }
    ));
    // Both tables have `id`: an unqualified reference is ambiguous.
    assert!(matches!(
        bind_error(
            "SELECT * FROM users, orders WHERE id = 1",
            &catalog(&["users", "orders"])
        ),
        ResolveError::AmbiguousColumn { .. }
    ));
    assert!(matches!(
        bind_error(
            "SELECT * FROM users ORDER BY nonexistent_column ASC",
            &catalog(&["users"])
        ),
        ResolveError::ColumnNotFound { .. }
    ));
    // A column in no scope of a subquery is not found, not correlated.
    assert!(matches!(
        bind_error(
            "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE nonexistent = 1)",
            &catalog_with_columns(&[("users", &["id", "name"]), ("orders", &["id", "total"])])
        ),
        ResolveError::ColumnNotFound { .. }
    ));
}

/// `*` expands through the bindings: a base table's columns in catalog
/// order, a derived table's output, and a USING/NATURAL join's merged
/// columns first (COALESCE'd on an outer join) with each side's remaining
/// columns after, in FROM order; a qualified `t.*` is that side verbatim.
#[test]
fn test_star_expansion_render_cases() {
    let users_orders = catalog(&["users", "orders"]);
    let t1_t2 = catalog_with_columns(&[("t1", &["id", "name", "x"]), ("t2", &["id", "name", "y"])]);
    let mut users_total = catalog(&["users"]);
    users_total.insert_overwrite(
        test_table_metadata_with_columns("orders", Oid::from_raw(1002), &["id", "total"]).into(),
    );
    #[rustfmt::skip]
    let cases: [(&str, &Catalog, &str, &str); 11] = [
        ("bare star",                 &users_orders, "SELECT * FROM users",
         "SELECT public.users.id, public.users.name FROM public.users"),
        ("explicit columns",          &users_orders, "SELECT id, name FROM users",
         "SELECT public.users.id, public.users.name FROM public.users"),
        ("star then a column",        &users_orders, "SELECT *, name FROM users",
         "SELECT public.users.id, public.users.name, public.users.name FROM public.users"),
        ("qualified star in a join",  &users_orders, "SELECT u.*, o.name FROM users u JOIN orders o ON o.id = u.id",
         "SELECT u.id, u.name, o.name FROM public.users u JOIN public.orders o ON o.id = u.id"),
        ("derived USING inner",       &users_orders, "SELECT * FROM (SELECT id, name FROM users) a JOIN (SELECT id, name FROM orders) b USING (id)",
         "SELECT a.id AS id, a.name, b.name FROM (SELECT public.users.id, public.users.name FROM public.users) a JOIN (SELECT public.orders.id, public.orders.name FROM public.orders) b USING (id)"),
        ("derived NATURAL LEFT",      &users_orders, "SELECT * FROM (SELECT name, id AS a_id FROM users) a NATURAL LEFT JOIN (SELECT name, id AS b_id FROM orders) b",
         "SELECT coalesce(a.name, b.name) AS name, a.a_id, b.b_id FROM (SELECT public.users.name, public.users.id AS a_id FROM public.users) a LEFT JOIN (SELECT public.orders.name, public.orders.id AS b_id FROM public.orders) b USING (name)"),
        ("single derived",            &users_orders, "SELECT * FROM (SELECT id, name FROM users) d",
         "SELECT d.id, d.name FROM (SELECT public.users.id, public.users.name FROM public.users) d"),
        ("multi-column USING",        &t1_t2,        "SELECT * FROM (SELECT id, name, x FROM t1) a JOIN (SELECT id, name, y FROM t2) b USING (id, name)",
         "SELECT a.id AS id, a.name AS name, a.x, b.y FROM (SELECT public.t1.id, public.t1.name, public.t1.x FROM public.t1) a JOIN (SELECT public.t2.id, public.t2.name, public.t2.y FROM public.t2) b USING (id, name)"),
        ("base table first",          &users_total,  "SELECT * FROM users u JOIN (SELECT id, total FROM orders) o USING (id)",
         "SELECT u.id AS id, u.name, o.total FROM public.users u JOIN (SELECT public.orders.id, public.orders.total FROM public.orders) o USING (id)"),
        ("derived table first",       &users_total,  "SELECT * FROM (SELECT id, total FROM orders) o JOIN users u USING (id)",
         "SELECT o.id AS id, o.total, u.name FROM (SELECT public.orders.id, public.orders.total FROM public.orders) o JOIN public.users u USING (id)"),
        ("qualified derived star",    &users_total,  "SELECT o.* FROM (SELECT id, total FROM orders) o JOIN users u USING (id)",
         "SELECT o.id, o.total FROM (SELECT public.orders.id, public.orders.total FROM public.orders) o JOIN public.users u USING (id)"),
    ];
    for (label, tables, sql, expected) in cases {
        assert_eq!(bound(sql, tables).sql(), expected, "{label}: {sql}");
    }
}

#[test]
fn test_join_bind_cases() {
    let tables = catalog(&["users", "orders"]);
    #[rustfmt::skip]
    let cases = [
        ("bare",    "SELECT * FROM users JOIN orders ON users.id = orders.id", None,      None),
        ("aliased", "SELECT * FROM users u JOIN orders o ON u.id = o.id",     Some("u"), Some("o")),
    ];
    for (label, sql, left_alias, right_alias) in cases {
        let query = bound(sql, &tables);
        let join = only_join(select_of(&query));
        assert_eq!(join.join_type(), JoinType::Inner, "{label}");
        let (left, right) = (as_table(join.left()), as_table(join.right()));
        assert_eq!(
            table_slot(&query, left),
            (
                "public".into(),
                "users".into(),
                left_alias.map(Into::into),
                1001
            ),
            "{label}: left side"
        );
        assert_eq!(
            table_slot(&query, right),
            (
                "public".into(),
                "orders".into(),
                right_alias.map(Into::into),
                1002
            ),
            "{label}: right side"
        );
        // The condition's columns bind to the tables whatever the spelling.
        let on = as_binary(join_on(join));
        assert_eq!(
            column_ref(&query, as_column(on.lexpr())),
            pair("users", "id"),
            "{label}"
        );
        assert_eq!(
            column_ref(&query, as_column(on.rexpr())),
            pair("orders", "id"),
            "{label}"
        );
    }
}

#[test]
fn test_where_conjunction_binds_each_side() {
    let query = bound(
        "SELECT * FROM users WHERE id = 1 AND name = 'john'",
        &catalog(&["users"]),
    );
    let and_expr = as_binary(select_of(&query).where_clause().expect("WHERE"));
    assert_eq!(and_expr.op(), BinaryOp::And);
    for (side, column) in [(and_expr.lexpr(), "id"), (and_expr.rexpr(), "name")] {
        let comparison = as_binary(side);
        assert_eq!(comparison.op(), BinaryOp::Equal);
        assert_eq!(
            column_ref(&query, as_column(comparison.lexpr())),
            pair("users", column)
        );
    }
}

// ============================================================================
// ORDER BY, GROUP BY, HAVING, LIMIT
// ============================================================================

/// What an ORDER BY item binds to.
#[derive(Debug, PartialEq)]
enum OrderTarget {
    Column(String, String),
    Output(String),
}

fn order_targets(bound: &ResolvedQuery) -> Vec<(OrderDirection, OrderTarget)> {
    bound
        .tree()
        .root()
        .order_by()
        .map(|clause| {
            let ScalarKind::Column(column) = clause.expr().kind() else {
                panic!("expected a column in ORDER BY");
            };
            let target = match bound.column_target_at(column.id()) {
                Some(ColumnTarget::Output) => OrderTarget::Output(column.column().to_string()),
                Some(ColumnTarget::Column(_) | ColumnTarget::Merged(_)) => {
                    let (table, name) = column_ref(bound, column);
                    OrderTarget::Column(table, name)
                }
                None => panic!("unbound ORDER BY column {}", column.column()),
            };
            (clause.direction(), target)
        })
        .collect()
}

/// An unqualified ORDER BY name matches an output name first (PG's
/// output-first rule); a qualified one always binds through the tables.
#[test]
fn test_order_by_bind_cases() {
    use OrderDirection::{Asc, Desc};
    let column = |table: &str, name: &str| OrderTarget::Column(table.into(), name.into());
    let output = |name: &str| OrderTarget::Output(name.into());
    let tables = catalog(&["users", "orders"]);
    #[rustfmt::skip]
    let cases = [
        ("qualified column",       "SELECT users.name FROM users ORDER BY users.name ASC",
         vec![(Asc, column("users", "name"))]),
        ("two columns",            "SELECT users.name, users.id FROM users ORDER BY users.name ASC, users.id DESC",
         vec![(Asc, column("users", "name")), (Desc, column("users", "id"))]),
        ("alias-qualified column", "SELECT * FROM users u ORDER BY u.name DESC",
         vec![(Desc, column("users", "name"))]),
        ("select alias",           "SELECT id, name AS display_name FROM users ORDER BY display_name DESC",
         vec![(Desc, output("display_name"))]),
        ("aggregate alias",        "SELECT id, SUM(id) AS total FROM orders GROUP BY id ORDER BY total DESC",
         vec![(Desc, output("total"))]),
        ("qualified beats alias",  "SELECT id, name AS display FROM users u ORDER BY u.name",
         vec![(Asc, column("users", "name"))]),
        ("across a join",          "SELECT * FROM users u JOIN orders o ON u.id = o.id ORDER BY u.name ASC, o.id DESC",
         vec![(Asc, column("users", "name")), (Desc, column("orders", "id"))]),
        ("unqualified non-output", "SELECT id FROM users ORDER BY name",
         vec![(Asc, column("users", "name"))]),
    ];
    for (label, sql, expected) in cases {
        assert_eq!(
            order_targets(&bound(sql, &tables)),
            expected,
            "{label}: {sql}"
        );
    }
}

#[test]
fn test_group_by_having_and_limit_bind() {
    let tables = catalog(&["users"]);
    let query = bound(
        "SELECT name FROM users GROUP BY name HAVING name != 'test' ORDER BY name LIMIT 10",
        &tables,
    );
    let select = select_of(&query);
    let group_by: Vec<_> = select.group_by().map(|c| column_ref(&query, c)).collect();
    assert_eq!(group_by, vec![pair("users", "name")]);
    let having = as_binary(select.having().expect("HAVING"));
    assert_eq!(
        column_ref(&query, as_column(having.lexpr())),
        pair("users", "name")
    );
    assert_eq!(query.tree().root().order_by().len(), 1);
    assert_eq!(
        query.tree().root().limit_count().map(|l| l.value().clone()),
        Some(LiteralValue::Integer(10))
    );

    let query = bound(
        "SELECT u.id, u.name FROM users u GROUP BY u.id, u.name",
        &tables,
    );
    let group_by: Vec<_> = select_of(&query)
        .group_by()
        .map(|c| column_ref(&query, c))
        .collect();
    assert_eq!(group_by, vec![pair("users", "id"), pair("users", "name")]);
}

#[test]
fn test_limit_cases() {
    use LiteralValue::{Integer, Parameter};
    // Parameterized values survive binding.
    #[rustfmt::skip]
    let cases = [
        ("count only",         "SELECT * FROM users LIMIT 10",            (Some(Integer(10)), None)),
        ("offset only",        "SELECT * FROM users OFFSET 5",            (None, Some(Integer(5)))),
        ("count and offset",   "SELECT * FROM users LIMIT 10 OFFSET 20",  (Some(Integer(10)), Some(Integer(20)))),
        ("parameterized",      "SELECT * FROM users LIMIT $1 OFFSET $2",  (Some(Parameter("$1".into())), Some(Parameter("$2".into())))),
        ("no LIMIT",           "SELECT * FROM users",                     (None, None)),
    ];
    let tables = catalog(&["users"]);
    for (label, sql, expected) in cases {
        let query = bound(sql, &tables);
        let root = query.tree().root();
        let actual = (
            root.limit_count().map(|l| l.value().clone()),
            root.limit_offset().map(|l| l.value().clone()),
        );
        assert_eq!(actual, expected, "{label}: (count, offset) of {sql}");
    }
}

/// A predicate over a HAVING aggregate.
type AggregateCheck = for<'t> fn(Function<'t>) -> bool;

/// FILTER, aggregate ORDER BY and DISTINCT on a HAVING aggregate survive
/// binding and render.
#[test]
fn test_having_aggregate_decorations_bind_cases() {
    #[rustfmt::skip]
    let cases: [(&str, &str, AggregateCheck); 3] = [
        ("count(*) FILTER",     "SELECT name FROM users GROUP BY name HAVING COUNT(*) FILTER (WHERE id > 0) > 5",
         |f| f.name() == "count" && f.agg_star() && f.filter().is_some()),
        ("string_agg ORDER BY", "SELECT id FROM users GROUP BY id HAVING string_agg(name, ',' ORDER BY name) <> ''",
         |f| f.name() == "string_agg" && f.agg_order().len() > 0),
        ("count DISTINCT",      "SELECT name FROM users GROUP BY name HAVING COUNT(DISTINCT id) > 1",
         |f| f.agg_distinct()),
    ];
    let tables = catalog(&["users"]);
    for (label, sql, check) in cases {
        let query = bound(sql, &tables);
        let having = as_binary(select_of(&query).having().expect("HAVING"));
        let WhereKind::Scalar(scalar) = having.lexpr().kind() else {
            panic!("{label}: expected a scalar on the left of HAVING");
        };
        let ScalarKind::Function(func) = scalar.kind() else {
            panic!("{label}: expected a function on the left of HAVING");
        };
        assert!(check(func), "{label}: HAVING aggregate of {sql}");
    }
    assert_eq!(
        bound(cases[0].1, &tables).sql(),
        "SELECT public.users.name FROM public.users GROUP BY public.users.name \
         HAVING count(*) FILTER (WHERE public.users.id > 0) > 5"
    );
}

#[test]
fn test_window_function_binds_and_renders() {
    let tables = catalog(&["users"]);
    let query = bound(
        "SELECT sum(id) OVER (PARTITION BY name ORDER BY id) FROM users",
        &tables,
    );
    let item = select_of(&query)
        .items()
        .expect("items")
        .next()
        .expect("an item");
    let func = item_function(item);
    assert_eq!(func.name(), "sum");
    let window = func.over().expect("an OVER clause");
    assert_eq!(
        (window.partition_by().len(), window.order_by().len()),
        (1, 1)
    );
    assert_eq!(
        bound(
            "SELECT sum(id) OVER (ORDER BY name DESC) FROM users",
            &tables
        )
        .sql(),
        "SELECT sum(public.users.id) OVER (ORDER BY public.users.name DESC) FROM public.users"
    );
}

// ============================================================================
// Subqueries and traversal
// ============================================================================

#[test]
fn test_select_nodes_cases() {
    // (label, SQL, the FROM table of each branch in order)
    #[rustfmt::skip]
    let cases: [(&str, &str, &[&str]); 3] = [
        ("plain SELECT",  "SELECT * FROM a",                                            &["a"]),
        ("UNION",         "SELECT * FROM a UNION SELECT * FROM b",                      &["a", "b"]),
        ("nested UNION",  "SELECT * FROM a UNION SELECT * FROM b UNION SELECT * FROM c", &["a", "b", "c"]),
    ];
    for (label, sql, expected) in cases {
        let tree = query_tree_parse(sql).expect("parse");
        let branches: Vec<&str> = select_branches_with_source(tree.root())
            .into_iter()
            .map(|(select, _)| select.direct_tables()[0].name().as_str())
            .collect();
        assert_eq!(branches, expected, "{label}");
    }
}

#[test]
fn test_subquery_bind_cases() {
    // IN keeps its test expression; the inner table has a slot of its own.
    let query = bound(
        "SELECT * FROM users WHERE id IN (SELECT id FROM active_users)",
        &catalog(&["users", "active_users"]),
    );
    let WhereKind::Subquery(subquery) = select_of(&query).where_clause().expect("WHERE").kind()
    else {
        panic!("expected a subquery predicate");
    };
    assert_eq!(
        (subquery.sublink_type(), subquery.test_expr().is_some()),
        (SubLinkType::Any, true)
    );
    let Body::Select(inner_select) = subquery.query().body() else {
        panic!("expected a SELECT subquery");
    };
    assert_eq!(
        table_slot(&query, only_table(inner_select)),
        ("public".into(), "active_users".into(), None, 1002)
    );

    // EXISTS has no test expression.
    let query = bound(
        "SELECT * FROM orders WHERE EXISTS (SELECT id FROM items)",
        &catalog(&["orders", "items"]),
    );
    let WhereKind::Subquery(subquery) = select_of(&query).where_clause().expect("WHERE").kind()
    else {
        panic!("expected a subquery predicate");
    };
    assert_eq!(
        (subquery.sublink_type(), subquery.test_expr().is_none()),
        (SubLinkType::Exists, true)
    );

    // A scalar subquery is the comparison's right operand.
    let query = bound(
        "SELECT * FROM users WHERE id > (SELECT id FROM users)",
        &catalog(&["users"]),
    );
    let binary = as_binary(select_of(&query).where_clause().expect("WHERE"));
    assert!(matches!(
        binary.rexpr().kind(),
        WhereKind::Subquery(subquery) if subquery.sublink_type() == SubLinkType::Expr
    ));

    // A derived table binds to a slot named by its alias.
    let query = bound(
        "SELECT * FROM (SELECT id FROM users) AS sub",
        &catalog(&["users"]),
    );
    let Some(Source::Subquery(sub)) = select_of(&query).from().next() else {
        panic!("expected a derived table");
    };
    assert_eq!(sub.alias().map(EcoString::as_str), Some("sub"));
    let slot = query
        .bindings()
        .source(sub.id())
        .expect("derived table bound");
    let TableSlot::Derived { table, .. } = query.bindings().slot(slot).expect("slot") else {
        panic!("expected a derived slot");
    };
    assert_eq!(table.name, "sub");
    assert_eq!(
        query.sql(),
        "SELECT sub.id FROM (SELECT public.users.id FROM public.users) sub"
    );
}

/// `direct_tables` is the FROM clause's own tables; the tree's nodes reach
/// every table, subqueries included.
#[test]
fn test_direct_tables_cases() {
    let users_active = catalog(&["users", "active_users"]);
    let users = catalog(&["users"]);
    let items = catalog_with_columns(&[
        ("items", &["id", "name", "category_id"]),
        ("inventory", &["id", "item_id", "quantity"]),
        ("categories", &["id", "name", "active"]),
    ]);
    // (label, catalog, SQL, every table reached, direct FROM tables)
    #[rustfmt::skip]
    let cases: [(&str, &Catalog, &str, usize, &[&str]); 3] = [
        ("WHERE subquery",            &users_active, "SELECT * FROM users WHERE id IN (SELECT id FROM active_users)", 2, &["users"]),
        ("join plus WHERE subquery",  &items,
         "SELECT i.name FROM items i JOIN inventory inv ON i.id = inv.item_id \
          WHERE i.category_id IN (SELECT c.id FROM categories c WHERE c.active = true) ORDER BY i.name", 3, &["items", "inventory"]),
        ("derived table",             &users,        "SELECT * FROM (SELECT id FROM users WHERE id = 1) AS sub",     1, &[]),
    ];
    for (label, tables, sql, all, direct) in cases {
        let query = bound(sql, tables);
        let reached = table_names(&query).len();
        let from: Vec<&str> = select_of(&query)
            .direct_tables()
            .iter()
            .map(|t| t.name().as_str())
            .collect();
        assert_eq!((reached, from), (all, direct.to_vec()), "{label}: {sql}");
    }
}

#[test]
fn test_table_nodes_reach_subqueries_cases() {
    // (label, SQL, tables reached, sorted)
    #[rustfmt::skip]
    let cases: [(&str, &str, &[&str]); 4] = [
        ("WHERE subquery",  "SELECT * FROM users WHERE id IN (SELECT id FROM active_users)",                                  &["active_users", "users"]),
        ("FROM subquery",   "SELECT * FROM (SELECT id FROM users WHERE id = 1) AS sub",                                       &["users"]),
        ("scalar subquery", "SELECT id, (SELECT COUNT(*) FROM users) AS user_count FROM orders WHERE id = 1",                 &["orders", "users"]),
        ("nested",          "SELECT * FROM a WHERE id IN (SELECT id FROM b WHERE id IN (SELECT id FROM c))",                  &["a", "b", "c"]),
    ];
    for (label, sql, expected) in cases {
        let tree = query_tree_parse(sql).expect("parse");
        let mut found: Vec<&str> = tree.table_names().map(|(_, name)| name).collect();
        found.sort_unstable();
        assert_eq!(found, expected, "{label}: tables reached in {sql}");
    }
}

/// Unqualified columns fall back to the outer scope only when the inner
/// scope lacks them; a column in both scopes binds to the inner one.
#[test]
fn test_correlated_reference_cases() {
    let users_orders = || catalog(&["users", "orders"]);
    #[rustfmt::skip]
    let cases = [
        ("EXISTS",                     catalog(&["orders", "items"]),
         "SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM items WHERE items.id = orders.id)",                   vec![pair("orders", "id")]),
        ("IN",                         users_orders(),
         "SELECT * FROM users WHERE id IN (SELECT id FROM orders WHERE orders.name = users.name)",               vec![pair("users", "name")]),
        ("SELECT-list scalar",         users_orders(),
         "SELECT id, (SELECT COUNT(*) FROM orders WHERE orders.id = users.id) FROM users",                       vec![pair("users", "id")]),
        ("outer table alias",          users_orders(),
         "SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders WHERE orders.id = u.id)",                     vec![pair("users", "id")]),
        ("unqualified, outer only",    catalog_with_columns(&[("users", &["id", "email"]), ("orders", &["id", "user_id", "total"])]),
         "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE email = 'test@example.com')",             vec![pair("users", "email")]),
        ("unqualified, in both",       catalog_with_columns(&[("users", &["id", "email"]), ("orders", &["id", "user_id"])]),
         "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE user_id = id)",                      vec![]),
        ("unqualified, scalar",        catalog_with_columns(&[("users", &["id", "status"]), ("orders", &["id", "amount"])]),
         "SELECT id, (SELECT COUNT(*) FROM orders WHERE status = 'active') FROM users",                          vec![pair("users", "status")]),
        ("not correlated",             catalog(&["users", "active_users"]),
         "SELECT * FROM users WHERE id IN (SELECT id FROM active_users)",                                         vec![]),
        ("mixed inner and outer",      catalog_with_columns(&[("departments", &["id", "region"]), ("employees", &["id", "dept_id", "region"])]),
         "SELECT d.id FROM departments d WHERE EXISTS (SELECT 1 FROM employees WHERE dept_id = d.id)",           vec![pair("departments", "id")]),
        ("doubly nested",              catalog_with_columns(&[("departments", &["id", "name"]), ("employees", &["id", "dept_id"]), ("projects", &["id", "employee_id"])]),
         "SELECT d.id FROM departments d WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id \
          AND EXISTS (SELECT 1 FROM projects WHERE employee_id = e.id))",                                        vec![pair("departments", "id"), pair("employees", "id")]),
    ];
    for (label, tables, sql, expected) in cases {
        assert_eq!(
            correlated_refs(&bound(sql, &tables)),
            expected,
            "{label}: correlated references of {sql}"
        );
    }
}

// ============================================================================
// Rendering
// ============================================================================

/// `*` expands to explicit columns; references are fully qualified unless
/// aliased.
#[test]
fn test_render_cases() {
    #[rustfmt::skip]
    let cases = [
        ("WHERE",          "SELECT * FROM users WHERE id = 1",
                           "SELECT public.users.id, public.users.name FROM public.users WHERE public.users.id = 1"),
        ("table alias",    "SELECT u.id, u.name FROM users u WHERE u.id = 1",
                           "SELECT u.id, u.name FROM public.users u WHERE u.id = 1"),
        ("join",           "SELECT u.id, o.name FROM users u JOIN orders o ON u.id = o.id WHERE u.id = 1",
                           "SELECT u.id, o.name FROM public.users u JOIN public.orders o ON u.id = o.id WHERE u.id = 1"),
        ("ORDER BY",       "SELECT id FROM users u ORDER BY name DESC",
                           "SELECT u.id FROM public.users u ORDER BY u.name DESC"),
        ("count(*)",       "SELECT COUNT(*) FROM users WHERE id = 1",
                           "SELECT count(*) FROM public.users WHERE public.users.id = 1"),
        ("count DISTINCT", "SELECT COUNT(DISTINCT name) FROM users WHERE id = 1",
                           "SELECT count(DISTINCT public.users.name) FROM public.users WHERE public.users.id = 1"),
        ("CASE",           "SELECT CASE WHEN name = 'admin' THEN 1 ELSE 0 END FROM users WHERE id = 1",
                           "SELECT CASE WHEN public.users.name = 'admin' THEN 1 ELSE 0 END FROM public.users WHERE public.users.id = 1"),
    ];
    let tables = catalog(&["users", "orders"]);
    for (label, sql, expected) in cases {
        assert_eq!(bound(sql, &tables).sql(), expected, "{label}: {sql}");
    }
}

/// PGC-262: identifiers render quoted when mixed-case, reserved or holding
/// a quote, which is doubled.
#[test]
fn test_identifier_quoting_render_cases() {
    let table = |schema: &str, name: &str, columns: &[&str], oid: u32| {
        let mut table = test_table_metadata_with_columns(name, Oid::from_raw(oid), columns);
        table.schema = schema.into();
        table
    };
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(table("Public", "Users", &["id", "firstName"], 1001).into());
    tables.insert_overwrite(table("public", "order", &["id", "user"], 1002).into());
    tables.insert_overwrite(table("public", "users", &["id", "we\"ird"], 1003).into());
    #[rustfmt::skip]
    let cases = [
        ("mixed case is quoted",   r#"SELECT "firstName" FROM "Public"."Users""#, r#"SELECT "Public"."Users"."firstName" FROM "Public"."Users""#),
        ("keywords are quoted",    r#"SELECT "user" FROM "order""#,               r#"SELECT public."order"."user" FROM public."order""#),
        ("embedded quote doubled", r#"SELECT "we""ird" FROM users"#,              r#"SELECT public.users."we""ird" FROM public.users"#),
        ("alias wins",             "SELECT u.id FROM users u",                    "SELECT u.id FROM public.users u"),
    ];
    for (label, sql, expected) in cases {
        assert_eq!(bound(sql, &tables).sql(), expected, "{label}: {sql}");
    }
}

// ============================================================================
// The binding corpus (ADR-057)
// ============================================================================

pub(super) fn binding_catalog() -> Catalog {
    catalog_with_columns(&[
        ("users", &["id", "name", "dept_id"]),
        ("departments", &["id", "name", "manager_id"]),
        ("orders", &["id", "user_id", "amount"]),
        ("items", &["id", "order_id", "sku"]),
    ])
}

pub(crate) const BINDING_CORPUS: &[&str] = &[
    "SELECT u.id, name FROM users u WHERE dept_id = 'x'",
    "SELECT u.id, d.name AS dname FROM users u JOIN departments d ON u.dept_id = d.id \
     WHERE d.name = 'a' ORDER BY dname, u.id DESC",
    "SELECT * FROM users JOIN departments USING (id)",
    "SELECT * FROM users LEFT JOIN departments USING (id) WHERE id = '1'",
    "SELECT id, count(*) FROM users JOIN departments USING (id) GROUP BY id",
    "SELECT * FROM users NATURAL JOIN departments",
    "SELECT d.name FROM departments d WHERE EXISTS (SELECT 1 FROM users u WHERE u.dept_id = d.id)",
    "SELECT v.total, v.uid FROM (SELECT user_id AS uid, amount AS total FROM orders) v \
     WHERE v.total = '5'",
    "WITH big AS (SELECT id, amount FROM orders WHERE amount = '9') \
     SELECT big.id FROM big JOIN items ON items.order_id = big.id",
    "SELECT id FROM users UNION SELECT id FROM departments ORDER BY id",
    "SELECT name, count(*) FROM users GROUP BY name HAVING count(*) > 1 ORDER BY count(*) DESC",
    "SELECT u.name, (SELECT max(o.amount) FROM orders o WHERE o.user_id = u.id) FROM users u \
     WHERE u.id IN (SELECT user_id FROM orders WHERE amount = '1')",
    "SELECT u.* FROM users u JOIN departments d ON d.id = u.dept_id \
     WHERE d.id = ANY(ARRAY['1', '2']) AND u.name LIKE 'a%'",
    "SELECT CASE WHEN amount = '0' THEN sku ELSE order_id END FROM orders o \
     JOIN items i ON i.order_id = o.id ORDER BY i.sku NULLS FIRST",
];

#[test]
fn test_bound_query_clone_shares_the_tree() {
    let catalog = binding_catalog();
    let resolved =
        ResolvedQuery::parse_resolve(BINDING_CORPUS[1], &catalog, &["public"]).expect("resolve");
    let copy = resolved.clone();
    assert_eq!(copy.tree(), resolved.tree());
    assert_eq!(copy.bindings().slots().len(), 2);
}

/// Shapes the binding corpus leaves out: stars over outer and `NATURAL`
/// joins, qualified stars, aliased and chained CTE references, set-op
/// and alias ordering, nested predicates, windows, filters and casts.
pub(crate) const RENDER_CORPUS: &[&str] = &[
    "SELECT * FROM users u LEFT JOIN departments d USING (id) ORDER BY id",
    "SELECT u.*, d.name FROM users u NATURAL JOIN departments d",
    "SELECT * FROM users NATURAL LEFT JOIN departments WHERE id = 1",
    "SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount > 5 \
     ORDER BY o.amount DESC NULLS LAST, u.id",
    "WITH t AS (SELECT id, name FROM users) SELECT x.name FROM t AS x WHERE x.id = 1",
    "WITH a AS (SELECT id FROM users), b AS (SELECT id FROM a WHERE id = 1) SELECT b.id FROM b",
    "SELECT id FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount = 1 LIMIT 5) \
     ORDER BY id LIMIT 3 OFFSET 1",
    "SELECT id FROM users UNION ALL SELECT id FROM departments ORDER BY id DESC",
    "SELECT id FROM users WHERE id > 1 INTERSECT SELECT id FROM departments WHERE id < 9",
    "SELECT name, count(*) AS n FROM users GROUP BY name HAVING count(*) > 1 ORDER BY n DESC, name",
    "SELECT rank() OVER (PARTITION BY dept_id ORDER BY id ROWS BETWEEN 1 PRECEDING AND \
     CURRENT ROW) FROM users",
    "SELECT count(*) FILTER (WHERE amount > 2), string_agg(sku, ',' ORDER BY items.id) \
     FROM orders JOIN items ON items.order_id = orders.id",
    "SELECT DISTINCT dept_id, CASE WHEN name = 'a' THEN 1 ELSE 0 END FROM users \
     WHERE id = ANY (ARRAY[1, 2]) AND name NOT IN ('x', 'y') AND id BETWEEN 1 AND 9",
    "SELECT (id)::text FROM users WHERE id IS NOT NULL AND NOT (id = 1 OR name = 'a')",
    "SELECT u.name, (SELECT max(o.amount) FROM orders o WHERE o.user_id = u.id) AS top \
     FROM users u ORDER BY top",
    "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) v",
    "SELECT u.id FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id \
     AND o.amount = 3) AND NOT EXISTS (SELECT 1 FROM items i WHERE i.id = u.id)",
    "SELECT a.id, b.id FROM users a CROSS JOIN departments b WHERE a.id = b.id",
    "SELECT (amount * 2) + 1 FROM orders WHERE amount <> ALL (ARRAY[1]) ORDER BY 1",
];

// ============================================================================
// Resolver expectations
// ============================================================================

/// What binding each query gives: its serve SQL, or `ERR` and the error.
#[rustfmt::skip]
const RESOLVE_EXPECTED: &[(&str, &str)] = &[
    ("SELECT id FROM missing",
     "ERR Table not found: missing"),
    ("SELECT u.id, name FROM users u WHERE dept_id = 'x'",
     "SELECT u.id, u.name FROM public.users u WHERE u.dept_id = 'x'"),
    ("SELECT nope FROM users",
     "ERR Column 'nope' not found in table '<unknown>'"),
    ("SELECT id FROM users JOIN departments ON users.dept_id = departments.id",
     "ERR Ambiguous column reference: 'id' could refer to multiple tables"),
    ("SELECT users.*, d.* FROM users JOIN departments d ON d.id = users.dept_id",
     "SELECT public.users.id, public.users.name, public.users.dept_id, d.id, d.name, d.manager_id FROM public.users JOIN public.departments d ON d.id = public.users.dept_id"),
    ("SELECT public.users.id FROM public.users",
     "SELECT public.users.id FROM public.users"),
    ("SELECT u.id, d.name AS dname FROM users u JOIN departments d ON u.dept_id = d.id WHERE d.name = 'a' ORDER BY dname, u.id DESC",
     "SELECT u.id, d.name AS dname FROM public.users u JOIN public.departments d ON u.dept_id = d.id WHERE d.name = 'a' ORDER BY dname ASC, u.id DESC"),
    ("SELECT u.id FROM users AS u (uid, uname, udept)",
     "SELECT u.id FROM public.users u(uid, uname, udept)"),
    ("SELECT name FROM users u LEFT JOIN departments d USING (name) GROUP BY name",
     "ERR Unsupported join qualifier (USING/NATURAL not yet cacheable)"),
    ("SELECT * FROM users JOIN departments USING (id)",
     "SELECT public.users.id AS id, public.users.name, public.users.dept_id, public.departments.name, public.departments.manager_id FROM public.users JOIN public.departments USING (id)"),
    ("SELECT * FROM (SELECT id FROM users)",
     "ERR Subquery alias '<missing>' not found"),
    ("SELECT id FROM users WHERE id IN (SELECT user_id FROM orders) AND name = 'a'",
     "SELECT public.users.id FROM public.users WHERE public.users.id IN (SELECT public.orders.user_id FROM public.orders) AND public.users.name = 'a'"),
    ("SELECT id FROM users u LEFT JOIN departments d USING (sku)",
     "ERR Unsupported join qualifier (USING/NATURAL not yet cacheable)"),
    ("SELECT x.id FROM users WHERE id = '1'",
     "ERR Table not found: x"),
    ("SELECT * FROM users LEFT JOIN departments USING (id) WHERE id = '1'",
     "SELECT coalesce(public.users.id, public.departments.id) AS id, public.users.name, public.users.dept_id, public.departments.name, public.departments.manager_id FROM public.users LEFT JOIN public.departments USING (id) WHERE coalesce(public.users.id, public.departments.id) = '1'"),
    ("SELECT id FROM users WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = users.id)",
     "SELECT public.users.id FROM public.users WHERE NOT EXISTS (SELECT 1 FROM public.orders o WHERE o.user_id = public.users.id)"),
    ("SELECT id, count(*) FROM users JOIN departments USING (id) GROUP BY id",
     "SELECT public.users.id, count(*) FROM public.users JOIN public.departments USING (id) GROUP BY public.users.id"),
    ("SELECT (SELECT count(*) FROM orders o WHERE o.user_id = u.id) AS n FROM users u ORDER BY n",
     "SELECT (SELECT count(*) FROM public.orders o WHERE o.user_id = u.id) AS n FROM public.users u ORDER BY n ASC"),
    ("SELECT * FROM users NATURAL JOIN departments",
     "SELECT public.users.id AS id, public.users.name AS name, public.users.dept_id, public.departments.manager_id FROM public.users JOIN public.departments USING (id, name)"),
    ("SELECT name FROM users u WHERE u.id = ANY (SELECT user_id FROM orders)",
     "SELECT u.name FROM public.users u WHERE u.id IN (SELECT public.orders.user_id FROM public.orders)"),
    ("SELECT d.name FROM departments d WHERE EXISTS (SELECT 1 FROM users u WHERE u.dept_id = d.id)",
     "SELECT d.name FROM public.departments d WHERE EXISTS (SELECT 1 FROM public.users u WHERE u.dept_id = d.id)"),
    ("SELECT name FROM users u WHERE u.id <> ALL (SELECT user_id FROM orders)",
     "SELECT u.name FROM public.users u WHERE u.id <> ALL (SELECT public.orders.user_id FROM public.orders)"),
    ("SELECT v.total, v.uid FROM (SELECT user_id AS uid, amount AS total FROM orders) v WHERE v.total = '5'",
     "SELECT v.total, v.uid FROM (SELECT public.orders.user_id AS uid, public.orders.amount AS total FROM public.orders) v WHERE v.total = '5'"),
    ("SELECT sum(amount) OVER (PARTITION BY user_id ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM orders",
     "SELECT sum(public.orders.amount) OVER (PARTITION BY public.orders.user_id ORDER BY public.orders.id ASC ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM public.orders"),
    ("SELECT string_agg(name, ',' ORDER BY name) FILTER (WHERE id = '1') FROM users",
     "SELECT string_agg(public.users.name, ',' ORDER BY public.users.name ASC) FILTER (WHERE public.users.id = '1') FROM public.users"),
    ("SELECT CASE id WHEN '1' THEN name ELSE 'x' END FROM users",
     "SELECT CASE public.users.id WHEN '1' THEN public.users.name ELSE 'x' END FROM public.users"),
    ("WITH big AS (SELECT id, amount FROM orders WHERE amount = '9') SELECT big.id FROM big JOIN items ON items.order_id = big.id",
     "SELECT big.id FROM (SELECT public.orders.id, public.orders.amount FROM public.orders WHERE public.orders.amount = '9') big JOIN public.items ON public.items.order_id = big.id"),
    ("SELECT id + dept_id, id::text, name::date FROM users",
     "SELECT (public.users.id + public.users.dept_id), (public.users.id)::text, (public.users.name)::date FROM public.users"),
    ("SELECT id FROM users UNION SELECT id FROM departments ORDER BY id",
     "SELECT public.users.id FROM public.users UNION SELECT public.departments.id FROM public.departments ORDER BY id ASC"),
    ("SELECT * FROM (VALUES ('1', '2')) AS v (a, b)",
     "SELECT FROM (VALUES ('1', '2')) v(a, b)"),
    ("SELECT name, count(*) FROM users GROUP BY name HAVING count(*) > 1 ORDER BY count(*) DESC",
     "SELECT public.users.name, count(*) FROM public.users GROUP BY public.users.name HAVING count(*) > 1 ORDER BY count(*) DESC"),
    ("SELECT a FROM (SELECT id AS a FROM users UNION ALL SELECT id FROM departments) s",
     "SELECT s.a FROM (SELECT public.users.id AS a FROM public.users UNION ALL SELECT public.departments.id FROM public.departments) s"),
    ("SELECT u.name, (SELECT max(o.amount) FROM orders o WHERE o.user_id = u.id) FROM users u WHERE u.id IN (SELECT user_id FROM orders WHERE amount = '1')",
     "SELECT u.name, (SELECT max(o.amount) FROM public.orders o WHERE o.user_id = u.id) FROM public.users u WHERE u.id IN (SELECT public.orders.user_id FROM public.orders WHERE public.orders.amount = '1')"),
    ("WITH u (uid) AS (SELECT id FROM users) SELECT uid FROM u",
     "ERR Column 'uid' not found in table '<unknown>'"),
    ("WITH u AS (SELECT id FROM users) SELECT x.id FROM u AS x",
     "SELECT x.id FROM (SELECT public.users.id FROM public.users) x"),
    ("SELECT u.* FROM users u JOIN departments d ON d.id = u.dept_id WHERE d.id = ANY(ARRAY['1', '2']) AND u.name LIKE 'a%'",
     "SELECT u.id, u.name, u.dept_id FROM public.users u JOIN public.departments d ON d.id = u.dept_id WHERE d.id = ANY (ARRAY['1', '2']) AND u.name LIKE 'a%'"),
    ("SELECT i.sku FROM items i NATURAL JOIN orders",
     "SELECT i.sku FROM public.items i JOIN public.orders USING (id)"),
    ("SELECT CASE WHEN amount = '0' THEN sku ELSE order_id END FROM orders o JOIN items i ON i.order_id = o.id ORDER BY i.sku NULLS FIRST",
     "SELECT CASE WHEN o.amount = '0' THEN i.sku ELSE i.order_id END FROM public.orders o JOIN public.items i ON i.order_id = o.id ORDER BY i.sku ASC NULLS FIRST"),
    ("SELECT * FROM users CROSS JOIN departments",
     "SELECT public.users.id, public.users.name, public.users.dept_id, public.departments.id, public.departments.name, public.departments.manager_id FROM public.users CROSS JOIN public.departments"),
    ("SELECT * FROM users u LEFT JOIN departments d USING (name)",
     "SELECT coalesce(u.name, d.name) AS name, u.id, u.dept_id, d.id, d.manager_id FROM public.users u LEFT JOIN public.departments d USING (name)"),
    ("SELECT id FROM users GROUP BY id HAVING count(*) > '1'",
     "SELECT public.users.id FROM public.users GROUP BY public.users.id HAVING count(*) > '1'"),
    ("SELECT name FROM users ORDER BY name DESC NULLS LAST LIMIT 3 OFFSET 1",
     "SELECT public.users.name FROM public.users ORDER BY name DESC NULLS LAST LIMIT 3 OFFSET 1"),
    ("SELECT id FROM users UNION SELECT id FROM departments ORDER BY id LIMIT 2",
     "SELECT public.users.id FROM public.users UNION SELECT public.departments.id FROM public.departments ORDER BY id ASC LIMIT 2"),
    ("SELECT u.name FROM users u WHERE u.dept_id BETWEEN '1' AND '2' AND u.name NOT IN ('a', 'b')",
     "SELECT u.name FROM public.users u WHERE u.dept_id BETWEEN '1' AND '2' AND u.name NOT IN ('a', 'b')"),
];

/// The binding corpus, every shape the resolver handles and every error it
/// reports bind to their expected renderings.
#[test]
fn test_resolver_expectations() {
    let tables = binding_catalog();
    for (sql, expected) in RESOLVE_EXPECTED {
        let actual = match ResolvedQuery::parse_resolve(sql, &tables, &["public"]) {
            Ok(bound) => bound.sql(),
            Err(e) => format!("ERR {}", e.current_context()),
        };
        assert_eq!(actual, *expected, "{sql}");
    }
}

#[test]
fn test_resolver_follows_the_search_path() {
    let tables = catalog(&["orders"]);
    let tree = query_tree_parse("SELECT id FROM orders WHERE name = 'x'").expect("parse");
    assert!(query_tree_resolve(&tree, &tables, &["public"]).is_ok());
    assert!(query_tree_resolve(&tree, &tables, &["other"]).is_err());
}

#[test]
fn test_privilege_needs_lists_referenced_columns_per_relation_and_functions() {
    let tables = catalog_with_columns(&[
        ("orders", &["id", "customer_id", "total"]),
        ("customers", &["id", "name"]),
    ]);
    let query = bound(
        "SELECT upper(c.name) FROM orders o JOIN customers c ON c.id = o.customer_id \
         WHERE o.total > 10 ORDER BY lower(c.name)",
        &tables,
    );
    let needs = Arc::clone(query.privilege_needs());
    let columns = |oid: u32| {
        let mut columns: Vec<&str> = needs.relations[&Oid::from_raw(oid)]
            .iter()
            .map(EcoString::as_str)
            .collect();
        columns.sort_unstable();
        columns
    };
    assert_eq!(
        columns(1001),
        ["customer_id", "total"],
        "orders: join and WHERE"
    );
    assert_eq!(
        columns(1002),
        ["id", "name"],
        "customers: join, select and ORDER BY"
    );
    assert_eq!(needs.functions, ["lower", "upper"]);

    let count = Arc::clone(bound("SELECT count(*) FROM orders", &tables).privilege_needs());
    assert!(
        count.relations[&Oid::from_raw(1001)].is_empty(),
        "no column referenced"
    );
    assert_eq!(count.functions, ["count"]);

    // A column read only by ORDER BY still decides the result's order.
    let ordered =
        Arc::clone(bound("SELECT id FROM orders ORDER BY total", &tables).privilege_needs());
    let mut ordered_columns: Vec<&str> = ordered.relations[&Oid::from_raw(1001)]
        .iter()
        .map(EcoString::as_str)
        .collect();
    ordered_columns.sort_unstable();
    assert_eq!(ordered_columns, ["id", "total"]);

    // A CTE or FROM subquery is no relation at origin: only the base
    // columns its body reads count.
    let cte = Arc::clone(
        bound(
            "WITH big AS (SELECT id, total FROM orders WHERE total > 10) SELECT total FROM big",
            &tables,
        )
        .privilege_needs(),
    );
    assert_eq!(cte.relations.len(), 1);
    let mut cte_columns: Vec<&str> = cte.relations[&Oid::from_raw(1001)]
        .iter()
        .map(EcoString::as_str)
        .collect();
    cte_columns.sort_unstable();
    assert_eq!(cte_columns, ["id", "total"]);
}
