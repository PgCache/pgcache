#![allow(clippy::wildcard_enum_match_arm)]

use std::sync::Arc;

use iddqd::BiHashMap;
use postgres_types::Type;

use super::*;
use crate::catalog::{Catalog, ColumnMetadata, ColumnPosition, ColumnStore, TableMetadata};
use crate::oid::{Oid, TypeOid};
use crate::query::ast::{
    AstNode, BinaryExpr, BinaryOp, ColumnNode, FunctionCall, JoinNode, JoinQual, JoinType,
    LiteralValue, OrderDirection, QueryBody, ScalarExpr, SelectColumn, SelectColumns, SelectNode,
    SubLinkType, TableNode, TableSource, WhereExpr, query_expr_parse,
};
use crate::query::resolve::ResolveError;

// ============================================================================
// Catalogs
// ============================================================================

/// `name(id int4, name text)` with `id` the key.
fn test_table_metadata(name: &str, relation_oid: Oid) -> TableMetadata {
    let columns = ColumnStore::new([
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
    ]);

    TableMetadata {
        replica_identity_full: false,
        relation_oid,
        name: name.into(),
        schema: "public".into(),
        primary_key_columns: vec!["id".into()],
        columns,
        indexes: Vec::new(),
    }
}

/// A catalog of `test_table_metadata` tables (`id int4`, `name text`), with
/// relation OIDs from 1001 in order.
fn catalog(names: &[&str]) -> Catalog {
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
    let columns =
        ColumnStore::new(
            column_names
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
                }),
        );
    TableMetadata {
        replica_identity_full: false,
        relation_oid,
        name: name.into(),
        schema: "public".into(),
        primary_key_columns: vec![column_names[0].into()],
        columns,
        indexes: Vec::new(),
    }
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
    let query = Arc::new(query_expr_parse(sql).expect("convert to QueryExpr"));
    let bindings = query_expr_resolve(&query, tables, &["public"]).expect("resolve");
    ResolvedQuery::new(query, bindings)
}

/// The error binding `sql` over `tables` ends in.
fn bind_error(sql: &str, tables: &Catalog) -> ResolveError {
    let query = query_expr_parse(sql).expect("convert to QueryExpr");
    match query_expr_resolve(&query, tables, &["public"]) {
        Ok(_) => panic!("expected {sql} not to resolve"),
        Err(e) => e.into_current_context(),
    }
}

fn select_of(bound: &ResolvedQuery) -> &SelectNode {
    bound.select().expect("a SELECT")
}

/// `(table, column)` a column reference is bound to.
fn column_ref(bound: &ResolvedQuery, column: &ColumnNode) -> (String, String) {
    let table = bound.column_table(column).expect("a bound column");
    let name = bound.column_name(column).expect("a bound column");
    (table.name.to_string(), name.to_string())
}

fn as_binary(expr: &WhereExpr) -> &BinaryExpr {
    let WhereExpr::Binary(binary) = expr else {
        panic!("expected a binary expression, got {expr:?}");
    };
    binary
}

fn as_column(expr: &WhereExpr) -> &ColumnNode {
    let WhereExpr::Scalar(ScalarExpr::Column(column)) = expr else {
        panic!("expected a column, got {expr:?}");
    };
    column
}

fn only_join(select: &SelectNode) -> &JoinNode {
    match select.from.as_slice() {
        [TableSource::Join(join)] => join,
        other => panic!("expected a single join source, got {other:?}"),
    }
}

fn as_table(source: &TableSource) -> &TableNode {
    let TableSource::Table(table) = source else {
        panic!("expected a table source, got {source:?}");
    };
    table
}

fn join_on(join: &JoinNode) -> &WhereExpr {
    let JoinQual::On(on) = &join.qual else {
        panic!("expected an ON join, got {:?}", join.qual);
    };
    on
}

/// The base slot a table node is bound to: `(schema, table, alias, oid)`.
fn table_slot(bound: &ResolvedQuery, table: &TableNode) -> (String, String, Option<String>, u32) {
    let slot = bound.bindings().source(table.id).expect("table bound");
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
        let [TableSource::Table(table)] = select.from.as_slice() else {
            panic!("{label}: expected a single table source in {sql:?}");
        };
        assert_eq!(
            table_slot(&query, table),
            ("public".into(), "users".into(), alias.map(Into::into), 1001),
            "{label}: table of {sql:?}"
        );
        let binary = as_binary(select.where_clause.as_ref().expect("WHERE"));
        let col = as_column(&binary.lexpr);
        assert_eq!(
            column_ref(&query, col),
            pair("users", column),
            "{label}: WHERE column of {sql:?}"
        );
        if let Some(type_name) = type_name {
            assert_eq!(
                query.column_type(col).expect("column type").name(),
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
        assert_eq!(join.join_type, JoinType::Inner, "{label}");
        let (left, right) = (as_table(&join.left), as_table(&join.right));
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
            column_ref(&query, as_column(&on.lexpr)),
            pair("users", "id"),
            "{label}"
        );
        assert_eq!(
            column_ref(&query, as_column(&on.rexpr)),
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
    let and_expr = as_binary(select_of(&query).where_clause.as_ref().expect("WHERE"));
    assert_eq!(and_expr.op, BinaryOp::And);
    for (side, column) in [(&and_expr.lexpr, "id"), (&and_expr.rexpr, "name")] {
        let comparison = as_binary(side);
        assert_eq!(comparison.op, BinaryOp::Equal);
        assert_eq!(
            column_ref(&query, as_column(&comparison.lexpr)),
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
        .order_by
        .iter()
        .map(|clause| {
            let ScalarExpr::Column(column) = &clause.expr else {
                panic!("expected a column in ORDER BY, got {:?}", clause.expr);
            };
            let target = match bound.column(column) {
                Some(ColumnTarget::Output) => OrderTarget::Output(column.column.to_string()),
                Some(ColumnTarget::Column(_) | ColumnTarget::Merged(_)) => {
                    let (table, name) = column_ref(bound, column);
                    OrderTarget::Column(table, name)
                }
                None => panic!("unbound ORDER BY column {column:?}"),
            };
            (clause.direction.clone(), target)
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
    let group_by: Vec<_> = select
        .group_by
        .iter()
        .map(|c| column_ref(&query, c))
        .collect();
    assert_eq!(group_by, vec![pair("users", "name")]);
    let having = as_binary(select.having.as_ref().expect("HAVING"));
    assert_eq!(
        column_ref(&query, as_column(&having.lexpr)),
        pair("users", "name")
    );
    assert_eq!(query.tree().order_by.len(), 1);
    assert_eq!(
        query.tree().limit.as_ref().and_then(|l| l.count.clone()),
        Some(LiteralValue::Integer(10))
    );

    let query = bound(
        "SELECT u.id, u.name FROM users u GROUP BY u.id, u.name",
        &tables,
    );
    let group_by: Vec<_> = select_of(&query)
        .group_by
        .iter()
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
        ("count only",         "SELECT * FROM users LIMIT 10",            Some((Some(Integer(10)), None))),
        ("offset only",        "SELECT * FROM users OFFSET 5",            Some((None, Some(Integer(5))))),
        ("count and offset",   "SELECT * FROM users LIMIT 10 OFFSET 20",  Some((Some(Integer(10)), Some(Integer(20))))),
        ("parameterized",      "SELECT * FROM users LIMIT $1 OFFSET $2",  Some((Some(Parameter("$1".into())), Some(Parameter("$2".into()))))),
        ("no LIMIT",           "SELECT * FROM users",                     None),
    ];
    let tables = catalog(&["users"]);
    for (label, sql, expected) in cases {
        let actual = bound(sql, &tables)
            .tree()
            .limit
            .as_ref()
            .map(|l| (l.count.clone(), l.offset.clone()));
        assert_eq!(actual, expected, "{label}: (count, offset) of {sql}");
    }
}

/// A predicate over a HAVING aggregate.
type AggregateCheck = fn(&FunctionCall) -> bool;

/// FILTER, aggregate ORDER BY and DISTINCT on a HAVING aggregate survive
/// binding and render.
#[test]
fn test_having_aggregate_decorations_bind_cases() {
    #[rustfmt::skip]
    let cases: [(&str, &str, AggregateCheck); 3] = [
        ("count(*) FILTER",     "SELECT name FROM users GROUP BY name HAVING COUNT(*) FILTER (WHERE id > 0) > 5",
         |f| f.name == "count" && f.agg_star && f.agg_filter.is_some()),
        ("string_agg ORDER BY", "SELECT id FROM users GROUP BY id HAVING string_agg(name, ',' ORDER BY name) <> ''",
         |f| f.name == "string_agg" && !f.agg_order.is_empty()),
        ("count DISTINCT",      "SELECT name FROM users GROUP BY name HAVING COUNT(DISTINCT id) > 1",
         |f| f.agg_distinct),
    ];
    let tables = catalog(&["users"]);
    for (label, sql, check) in cases {
        let query = bound(sql, &tables);
        let having = as_binary(select_of(&query).having.as_ref().expect("HAVING"));
        let WhereExpr::Scalar(ScalarExpr::Function(func)) = having.lexpr.as_ref() else {
            panic!(
                "{label}: expected a function on the left of HAVING, got {:?}",
                having.lexpr
            );
        };
        assert!(
            check(func),
            "{label}: HAVING aggregate of {sql} is {func:?}"
        );
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
    let SelectColumns::Columns(columns) = &select_of(&query).columns else {
        panic!("expected columns");
    };
    let SelectColumn::Expr {
        expr: ScalarExpr::Function(func),
        ..
    } = &columns[0]
    else {
        panic!("expected a function");
    };
    assert_eq!(func.name, "sum");
    let window = func.over.as_ref().expect("an OVER clause");
    assert_eq!((window.partition_by.len(), window.order_by.len()), (1, 1));
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
        let query = query_expr_parse(sql).expect("parse");
        let branches: Vec<&str> = query
            .select_nodes()
            .iter()
            .map(|select| select.direct_table_nodes()[0].name.as_str())
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
    let Some(WhereExpr::Subquery {
        sublink_type,
        test_expr,
        query: inner,
    }) = &select_of(&query).where_clause
    else {
        panic!("expected a subquery predicate");
    };
    assert_eq!(
        (*sublink_type, test_expr.is_some()),
        (SubLinkType::Any, true)
    );
    let QueryBody::Select(inner_select) = &inner.body else {
        panic!("expected a SELECT subquery");
    };
    let [TableSource::Table(inner_table)] = inner_select.from.as_slice() else {
        panic!("expected one inner table");
    };
    assert_eq!(
        table_slot(&query, inner_table),
        ("public".into(), "active_users".into(), None, 1002)
    );

    // EXISTS has no test expression.
    let query = bound(
        "SELECT * FROM orders WHERE EXISTS (SELECT id FROM items)",
        &catalog(&["orders", "items"]),
    );
    let Some(WhereExpr::Subquery {
        sublink_type,
        test_expr,
        ..
    }) = &select_of(&query).where_clause
    else {
        panic!("expected a subquery predicate");
    };
    assert_eq!(
        (*sublink_type, test_expr.is_none()),
        (SubLinkType::Exists, true)
    );

    // A scalar subquery is the comparison's right operand.
    let query = bound(
        "SELECT * FROM users WHERE id > (SELECT id FROM users)",
        &catalog(&["users"]),
    );
    let binary = as_binary(select_of(&query).where_clause.as_ref().expect("WHERE"));
    assert!(matches!(
        binary.rexpr.as_ref(),
        WhereExpr::Subquery {
            sublink_type: SubLinkType::Expr,
            ..
        }
    ));

    // A derived table binds to a slot named by its alias.
    let query = bound(
        "SELECT * FROM (SELECT id FROM users) AS sub",
        &catalog(&["users"]),
    );
    let [TableSource::Subquery(sub)] = select_of(&query).from.as_slice() else {
        panic!("expected a derived table");
    };
    assert_eq!(sub.alias.as_ref().map(|a| a.name.as_str()), Some("sub"));
    let slot = query
        .bindings()
        .source(sub.id)
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

/// `direct_table_nodes` is the FROM clause's own tables; `nodes` reaches
/// every table, subqueries included.
#[test]
fn test_direct_table_nodes_cases() {
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
        let reached = query.tree().nodes::<TableNode>().count();
        let from: Vec<&str> = select_of(&query)
            .direct_table_nodes()
            .iter()
            .map(|t| t.name.as_str())
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
        let query = query_expr_parse(sql).expect("parse");
        let mut found: Vec<&str> = query
            .nodes::<TableNode>()
            .map(|t| t.name.as_str())
            .collect();
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

pub(super) const BINDING_CORPUS: &[&str] = &[
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
    let resolved = bound(BINDING_CORPUS[1], &catalog);
    let copy = resolved.clone();
    assert!(std::ptr::eq(resolved.tree(), copy.tree()));
    let first_column = copy.tree().nodes::<ColumnNode>().next().expect("a column");
    assert!(matches!(
        copy.column(first_column),
        Some(ColumnTarget::Column(_))
    ));
    assert_eq!(copy.bindings().slots().len(), 2);
}
