//! The converter's tests, read through the rope's views: what each SQL
//! shape converts to, what it renders back to, and what is rejected.

#![allow(clippy::wildcard_enum_match_arm, clippy::type_complexity)]

use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

use ecow::EcoString;
use ordered_float::NotNan;

use crate::cache::{SubqueryKind, UpdateQuerySource};
use crate::query::ast::rope::{
    Body, Function, Join, JoinQual, NodeKind, QueryTree, Scalar, ScalarKind, Select, SelectItem,
    Source, Table, View, Where, WhereKind,
};
use crate::query::ast::{
    ArithmeticOp, BinaryOp, CteMaterialization, Deparse, JoinType, LiteralValue, MultiOp,
    OrderDirection, SetOpType, SubLinkType, UnaryOp, query_tree_parse,
};
use crate::query::cast::cast_target_deparse;
use crate::query::update::select_branches_with_source;

fn parse(sql: &str) -> QueryTree {
    query_tree_parse(sql).unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// The SELECT a plain query's body is.
fn select_of(tree: &QueryTree) -> Select<'_> {
    match tree.root().body() {
        Body::Select(select) => select,
        other => panic!("expected a SELECT body, got {:?}", other_kind(other)),
    }
}

fn other_kind(body: Body<'_>) -> &'static str {
    match body {
        Body::Select(_) => "a SELECT",
        Body::Values(_) => "VALUES",
        Body::SetOp(_) => "a set operation",
    }
}

/// The select list, which the test expects to be explicit items.
fn items(select: Select<'_>) -> Vec<SelectItem<'_>> {
    select.items().expect("explicit select items").collect()
}

/// The expression of a non-star select item.
fn expr_of(item: SelectItem<'_>) -> Scalar<'_> {
    match item {
        SelectItem::Expr(expr) => expr.expr(),
        SelectItem::Star(_) => panic!("expected an expression item, got a star"),
    }
}

fn alias_of(item: SelectItem<'_>) -> Option<&EcoString> {
    match item {
        SelectItem::Expr(expr) => expr.alias(),
        SelectItem::Star(_) => None,
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
        _ => panic!("expected a single join"),
    }
}

/// The (qualifier, column) of every select-list column reference.
fn column_refs(select: Select<'_>) -> Vec<(Option<&str>, &str)> {
    items(select)
        .into_iter()
        .map(|item| match expr_of(item).kind() {
            ScalarKind::Column(column) => (
                column.table().map(EcoString::as_str),
                column.column().as_str(),
            ),
            _ => panic!("expected a column reference"),
        })
        .collect()
}

/// The function call `sql` selects as its only item.
fn only_function(sql: &str) -> (QueryTree, impl Fn(&QueryTree) -> Function<'_>) {
    (parse(sql), |tree: &QueryTree| {
        let item_list = items(select_of(tree));
        let [item] = item_list.as_slice() else {
            panic!("expected exactly one item");
        };
        match expr_of(*item).kind() {
            ScalarKind::Function(function) => function,
            _ => panic!("expected a function"),
        }
    })
}

fn where_of(tree: &QueryTree) -> Where<'_> {
    select_of(tree).where_clause().expect("a WHERE clause")
}

/// Every node kind in pre-order.
fn kinds(tree: &QueryTree) -> impl Iterator<Item = &NodeKind> {
    tree.nodes().map(|node| &node.kind)
}

/// The (schema, name, alias) of every base table in pre-order.
fn tables(tree: &QueryTree) -> Vec<(Option<&str>, &str, Option<&str>)> {
    kinds(tree)
        .filter_map(|kind| match kind {
            NodeKind::Table {
                schema,
                name,
                alias,
                ..
            } => Some((
                (!schema.is_empty()).then_some(schema.as_str()),
                name.as_str(),
                (!alias.is_empty()).then_some(alias.as_str()),
            )),
            _ => None,
        })
        .collect()
}

/// The (qualifier, column) of every column reference under `view` in
/// pre-order.
fn columns_under<'t>(view: impl View<'t>) -> Vec<(Option<&'t str>, &'t str)> {
    view.node_ref()
        .subtree()
        .filter_map(|position| match position.kind() {
            NodeKind::Column { table, column } => {
                Some((table.as_ref().map(EcoString::as_str), column.as_str()))
            }
            _ => None,
        })
        .collect()
}

fn literals_under<'t>(view: impl View<'t>) -> Vec<&'t LiteralValue> {
    view.node_ref()
        .subtree()
        .filter_map(|position| match position.kind() {
            NodeKind::Literal(value) => Some(value),
            _ => None,
        })
        .collect()
}

/// Whether a query node sits under `select`.
fn has_subqueries(select: Select<'_>) -> bool {
    select
        .node_ref()
        .subtree()
        .any(|position| matches!(position.kind(), NodeKind::Query { .. }))
}

fn single_table(tree: &QueryTree) -> bool {
    let mut from = select_of(tree).from();
    matches!((from.next(), from.next()), (Some(Source::Table(_)), None))
}

fn session_hash(tree: &QueryTree) -> u64 {
    let mut hasher = DefaultHasher::new();
    tree.session_hash(&mut hasher);
    hasher.finish()
}

fn directions<'t>(
    clauses: impl Iterator<Item = crate::query::ast::rope::OrderBy<'t>>,
) -> Vec<OrderDirection> {
    clauses.map(|clause| clause.direction()).collect()
}

fn branch_count(tree: &QueryTree) -> usize {
    select_branches_with_source(tree.root()).len()
}

// ============================================================================
// Tables, aliases and columns
// ============================================================================

#[test]
fn test_simple_select() {
    let tree = parse("SELECT id, name FROM users WHERE id = 1");
    assert!(single_table(&tree));
    assert!(matches!(where_of(&tree).kind(), WhereKind::Binary(_)));
    assert_eq!(tables(&tree), [(None, "users", None)]);
    assert_eq!(
        column_refs(select_of(&tree)),
        [(None, "id"), (None, "name")]
    );
}

/// PGC-183: `SELECT $1 FROM …` must parse.
#[test]
fn test_select_paramref() {
    let tree = parse("SELECT $1 FROM users");
    let item_list = items(select_of(&tree));
    let [item] = item_list.as_slice() else {
        panic!("expected one item");
    };
    assert!(matches!(
        expr_of(*item).kind(),
        ScalarKind::Literal(literal) if literal.value() == &LiteralValue::Parameter("$1".into())
    ));
}

#[test]
fn test_select_star() {
    let tree = parse("SELECT * FROM products");
    assert!(single_table(&tree));
    assert!(select_of(&tree).where_clause().is_none());
    assert_eq!(tables(&tree), [(None, "products", None)]);
    let item_list = items(select_of(&tree));
    let [item] = item_list.as_slice() else {
        panic!("expected one item");
    };
    assert!(matches!(item, SelectItem::Star(star) if star.qualifier().is_none()));
}

#[test]
fn test_star_cases() {
    let tree = parse("SELECT *, col FROM test");
    let item_list = items(select_of(&tree));
    let [star, column] = item_list.as_slice() else {
        panic!("expected two items");
    };
    assert!(matches!(star, SelectItem::Star(star) if star.qualifier().is_none()));
    assert!(matches!(expr_of(*column).kind(), ScalarKind::Column(c) if c.column() == "col"));

    let tree = parse("SELECT t1.*, t2.col FROM test t1 JOIN test2 t2 ON t2.id = t1.id");
    let item_list = items(select_of(&tree));
    let [star, column] = item_list.as_slice() else {
        panic!("expected two items");
    };
    assert!(
        matches!(star, SelectItem::Star(star) if star.qualifier().map(EcoString::as_str) == Some("t1"))
    );
    assert!(matches!(
        expr_of(*column).kind(),
        ScalarKind::Column(c) if c.column() == "col" && c.table().map(EcoString::as_str) == Some("t2")
    ));

    assert_eq!(
        parse("SELECT * FROM products").sql(),
        "SELECT * FROM products"
    );
    assert_eq!(
        parse("SELECT t1.*, t2.name FROM a t1 JOIN b t2 ON t2.id = t1.id").sql(),
        "SELECT t1.*, t2.name FROM a t1 JOIN b t2 ON t2.id = t1.id"
    );
}

#[test]
fn test_table_schema_and_alias() {
    let tree = parse("SELECT id, name FROM test.users WHERE active = true");
    let table = only_table(select_of(&tree));
    assert_eq!(table.schema().map(EcoString::as_str), Some("test"));
    assert_eq!(table.name(), "users");
    assert_eq!(table.alias(), None);
    assert_eq!(
        column_refs(select_of(&tree)),
        [(None, "id"), (None, "name")]
    );

    let tree = parse("SELECT u.id, u.name FROM users u WHERE u.active = true");
    let table = only_table(select_of(&tree));
    assert_eq!(table.name(), "users");
    assert_eq!(table.alias().map(EcoString::as_str), Some("u"));
    assert_eq!(table.alias_columns().len(), 0);
    assert_eq!(
        column_refs(select_of(&tree)),
        [(Some("u"), "id"), (Some("u"), "name")]
    );

    let tree = parse("SELECT id, name FROM users");
    assert_eq!(only_table(select_of(&tree)).alias(), None);
    let aliases: Vec<_> = items(select_of(&tree)).into_iter().map(alias_of).collect();
    assert_eq!(aliases, [None, None]);
}

#[test]
fn test_column_alias() {
    let tree = parse("SELECT id as user_id, name as full_name FROM users");
    let items = items(select_of(&tree));
    assert_eq!(items.len(), 2);
    let described: Vec<(Option<&str>, &str)> = items
        .iter()
        .map(|item| {
            let ScalarKind::Column(column) = expr_of(*item).kind() else {
                panic!("expected a column");
            };
            (
                alias_of(*item).map(EcoString::as_str),
                column.column().as_str(),
            )
        })
        .collect();
    assert_eq!(
        described,
        [(Some("user_id"), "id"), (Some("full_name"), "name")]
    );
}

/// The base tables of a query in pre-order, aliases included: joins,
/// derived tables, scalar and predicate subqueries and CTE bodies are
/// all reached.
#[test]
fn test_tables_in_preorder_cases() {
    #[rustfmt::skip]
    let cases: [(&str, &str, Vec<(Option<&str>, &str, Option<&str>)>); 9] = [
        ("join with alias",       "SELECT * FROM invoice JOIN product p ON p.id = invoice.product_id",
         vec![(None, "invoice", None), (None, "product", Some("p"))]),
        ("three tables",          "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id WHERE a.id = 1",
         vec![(None, "a", None), (None, "b", None), (None, "c", None)]),
        ("four tables",           "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id JOIN d ON c.id = d.id",
         vec![(None, "a", None), (None, "b", None), (None, "c", None), (None, "d", None)]),
        ("schema and aliases",    "SELECT first_name FROM actor a JOIN public.film_actor fa ON a.actor_id = fa.actor_id WHERE a.actor_id = 1",
         vec![(None, "actor", Some("a")), (Some("public"), "film_actor", Some("fa"))]),
        ("qualified table",       "SELECT * FROM public.users",
         vec![(Some("public"), "users", None)]),
        ("derived table",         "SELECT * FROM (SELECT id FROM users) sub",
         vec![(None, "users", None)]),
        ("scalar subquery",       "SELECT id, (SELECT COUNT(*) FROM orders) FROM users",
         vec![(None, "orders", None), (None, "users", None)]),
        ("WHERE subquery",        "SELECT * FROM users WHERE id IN (SELECT user_id FROM active_users)",
         vec![(None, "users", None), (None, "active_users", None)]),
        ("CTE body",              "WITH x AS (SELECT id FROM users) SELECT * FROM x",
         vec![(None, "users", None), (None, "users", None)]),
    ];
    for (label, sql, expected) in cases {
        assert_eq!(tables(&parse(sql)), expected, "{label}: {sql}");
    }
}

/// The joins of a query in pre-order: the outermost first, so a chain of
/// joins lists the last written join type first.
#[test]
fn test_joins_in_preorder_cases() {
    use JoinType::{Inner, Left};
    #[rustfmt::skip]
    let cases: [(&str, &str, Vec<JoinType>); 4] = [
        ("one join",     "SELECT * FROM users u JOIN orders o ON u.id = o.user_id",                                   vec![Inner]),
        ("two joins",    "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id WHERE a.id = 1",                vec![Inner, Inner]),
        ("three joins",  "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id JOIN d ON c.id = d.id",         vec![Inner, Inner, Inner]),
        ("mixed types",  "SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id LEFT JOIN payments p ON o.id = p.order_id", vec![Left, Inner]),
    ];
    for (label, sql, expected) in cases {
        let tree = parse(sql);
        let joins: Vec<JoinType> = kinds(&tree)
            .filter_map(|kind| match kind {
                NodeKind::Join { join_type, .. } => Some(*join_type),
                _ => None,
            })
            .collect();
        assert_eq!(joins, expected, "{label}: {sql}");
    }
}

#[test]
fn test_join_condition_columns() {
    let tree = parse("SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id WHERE a.id = 1");
    let outer = only_join(select_of(&tree));
    let JoinQual::On(outer_condition) = outer.qual() else {
        panic!("expected ON");
    };
    assert_eq!(
        columns_under(outer_condition),
        [(Some("b"), "id"), (Some("c"), "id")]
    );
    let Source::Join(inner) = outer.left() else {
        panic!("expected a nested join");
    };
    let JoinQual::On(inner_condition) = inner.qual() else {
        panic!("expected ON");
    };
    assert_eq!(
        columns_under(inner_condition),
        [(Some("a"), "id"), (Some("b"), "id")]
    );

    let tree = parse("SELECT * FROM users u JOIN orders o ON u.id = o.user_id");
    let join = only_join(select_of(&tree));
    assert_eq!(join.join_type(), JoinType::Inner);
    let JoinQual::On(condition) = join.qual() else {
        panic!("expected ON");
    };
    assert_eq!(
        columns_under(condition),
        [(Some("u"), "id"), (Some("o"), "user_id")]
    );
}

/// Every column reference a clause holds, in pre-order.
#[test]
fn test_columns_in_preorder_cases() {
    #[rustfmt::skip]
    let cases: [(&str, &str, Vec<(Option<&str>, &str)>); 5] = [
        ("WHERE",            "SELECT * FROM users WHERE name = 'john' AND age > 25",                     vec![(None, "name"), (None, "age")]),
        ("nested WHERE",     "SELECT * FROM users WHERE (name = 'john' OR name = 'jane') AND age > 18",  vec![(None, "name"), (None, "name"), (None, "age")]),
        ("select list",      "SELECT id, name FROM users",                                               vec![(None, "id"), (None, "name")]),
        ("list and WHERE",   "SELECT id, name FROM users WHERE active = true",                           vec![(None, "id"), (None, "name"), (None, "active")]),
        ("function argument", "SELECT COUNT(id) FROM users",                                             vec![(None, "id")]),
    ];
    for (label, sql, expected) in cases {
        assert_eq!(columns_under(parse(sql).root()), expected, "{label}: {sql}");
    }
    let tree = parse("SELECT * FROM users ORDER BY u.name ASC, age DESC");
    assert_eq!(
        columns_under(tree.root()),
        [(Some("u"), "name"), (None, "age")]
    );
}

#[test]
fn test_where_nodes_in_preorder() {
    let tree = parse("SELECT * FROM users WHERE name = 'john' AND age > 25 AND active = true");
    assert_eq!(
        literals_under(where_of(&tree)),
        [
            &LiteralValue::String("john".into()),
            &LiteralValue::Integer(25),
            &LiteralValue::Boolean(true)
        ]
    );
    let tree = parse("SELECT * FROM users WHERE name = 'john' AND age > 25");
    let ops: Vec<BinaryOp> = where_of(&tree)
        .node_ref()
        .subtree()
        .filter_map(|position| match position.kind() {
            NodeKind::Binary { op } => Some(*op),
            _ => None,
        })
        .collect();
    assert_eq!(ops, [BinaryOp::And, BinaryOp::Equal, BinaryOp::GreaterThan]);
    // The comparison, its column and its literal.
    let tree = parse("SELECT * FROM users WHERE name = 'john'");
    assert_eq!(where_of(&tree).node_ref().subtree_len(), 3);
    let tree = parse("SELECT * FROM users WHERE NOT active");
    assert!(
        matches!(where_of(&tree).kind(), WhereKind::Unary(unary) if unary.op() == UnaryOp::Not)
    );
    let tree = parse("SELECT * FROM t WHERE id IN (1, 2)");
    let WhereKind::Multi(multi) = where_of(&tree).kind() else {
        panic!("expected IN");
    };
    assert_eq!(multi.op(), MultiOp::In);
    assert_eq!(columns_under(where_of(&tree)), [(None, "id")]);
    assert_eq!(multi.exprs().len(), 3);
}

// ============================================================================
// Subqueries, VALUES and set operations
// ============================================================================

#[test]
fn test_select_list_subquery() {
    let tree = parse("SELECT invoice.id, (SELECT x.data FROM x WHERE 1 = 1) as one FROM invoice");
    let item_list = items(select_of(&tree));
    let [first, second] = item_list.as_slice() else {
        panic!("expected two items");
    };
    assert!(matches!(
        expr_of(*first).kind(),
        ScalarKind::Column(c) if c.table().map(EcoString::as_str) == Some("invoice") && c.column() == "id"
    ));
    assert_eq!(alias_of(*first), None);
    assert!(matches!(expr_of(*second).kind(), ScalarKind::Subquery(_)));
    assert_eq!(alias_of(*second).map(EcoString::as_str), Some("one"));
}

#[test]
fn test_table_subquery() {
    let tree = parse("SELECT * FROM (SELECT * FROM invoice WHERE id = 2) inv");
    let mut from = select_of(&tree).from();
    let (Some(Source::Subquery(subquery)), None) = (from.next(), from.next()) else {
        panic!("expected a derived table");
    };
    assert!(!subquery.lateral());
    assert_eq!(subquery.alias().map(EcoString::as_str), Some("inv"));
    assert_eq!(subquery.alias_columns().len(), 0);
}

#[test]
fn test_values_in_from() {
    let tree = parse("SELECT * FROM (VALUES(1, 2, 'test'), (3, 4, 'a')) v");
    let mut from = select_of(&tree).from();
    let (Some(Source::Subquery(subquery)), None) = (from.next(), from.next()) else {
        panic!("expected a derived table");
    };
    assert!(!subquery.lateral());
    assert_eq!(subquery.alias().map(EcoString::as_str), Some("v"));
    let Body::Values(values) = subquery.query().body() else {
        panic!("expected VALUES");
    };
    let widths: Vec<usize> = values.rows().map(|row| row.literals().len()).collect();
    assert_eq!(widths, [3, 3]);
}

#[test]
fn test_values_query() {
    let tree = parse("VALUES (1, 'a'), (2, 'b')");
    let Body::Values(values) = tree.root().body() else {
        panic!("expected VALUES");
    };
    let rows: Vec<Vec<&LiteralValue>> = values
        .rows()
        .map(|row| row.literals().map(|literal| literal.value()).collect())
        .collect();
    assert_eq!(
        rows,
        [
            vec![&LiteralValue::Integer(1), &LiteralValue::String("a".into())],
            vec![&LiteralValue::Integer(2), &LiteralValue::String("b".into())]
        ]
    );
    assert_eq!(tree.sql(), "VALUES (1, 'a'), (2, 'b')");
}

#[test]
fn test_set_operation_cases() {
    #[rustfmt::skip]
    let cases = [
        ("UNION",     "SELECT a FROM t1 UNION SELECT b FROM t2",     SetOpType::Union,     false),
        ("UNION ALL", "SELECT a FROM t1 UNION ALL SELECT b FROM t2", SetOpType::Union,     true),
        ("INTERSECT", "SELECT a FROM t1 INTERSECT SELECT b FROM t2", SetOpType::Intersect, false),
        ("EXCEPT",    "SELECT a FROM t1 EXCEPT SELECT b FROM t2",    SetOpType::Except,    false),
    ];
    for (label, sql, op, all) in cases {
        let tree = parse(sql);
        let Body::SetOp(set_op) = tree.root().body() else {
            panic!("{label}: expected a set operation");
        };
        assert_eq!((set_op.op(), set_op.all()), (op, all), "{label}");
        assert!(matches!(set_op.left().body(), Body::Select(_)), "{label}");
        assert!(matches!(set_op.right().body(), Body::Select(_)), "{label}");
        assert_eq!(tree.sql(), sql, "{label}");
    }
}

#[test]
fn test_union_order_by_is_top_level() {
    let tree = parse("SELECT a FROM t1 UNION SELECT b FROM t2 ORDER BY 1");
    assert_eq!(tree.root().order_by().len(), 1);
    let Body::SetOp(set_op) = tree.root().body() else {
        panic!("expected a set operation");
    };
    assert_eq!(set_op.op(), SetOpType::Union);
    assert_eq!(set_op.left().order_by().len(), 0);
    assert_eq!(set_op.right().order_by().len(), 0);
}

#[test]
fn test_chained_union_nests_left() {
    let tree = parse("SELECT a FROM t1 UNION SELECT b FROM t2 UNION SELECT c FROM t3");
    let Body::SetOp(outer) = tree.root().body() else {
        panic!("expected a set operation");
    };
    assert_eq!(outer.op(), SetOpType::Union);
    let Body::SetOp(inner) = outer.left().body() else {
        panic!("expected a nested set operation");
    };
    assert_eq!(inner.op(), SetOpType::Union);
    assert!(matches!(outer.right().body(), Body::Select(_)));
}

#[test]
fn test_union_branches() {
    let tree = parse("SELECT id FROM users UNION SELECT id FROM admins");
    let branches: Vec<&str> = select_branches_with_source(tree.root())
        .into_iter()
        .map(|(select, _)| select.direct_tables()[0].name().as_str())
        .collect();
    assert_eq!(branches, ["users", "admins"]);
}

#[test]
fn test_where_subquery_cases() {
    let tree = parse("SELECT * FROM users WHERE id IN (SELECT user_id FROM active_users)");
    let WhereKind::Subquery(subquery) = where_of(&tree).kind() else {
        panic!("expected a subquery predicate");
    };
    assert_eq!(subquery.sublink_type(), SubLinkType::Any);
    assert!(subquery.test_expr().is_some());
    let Body::Select(inner) = subquery.query().body() else {
        panic!("expected an inner SELECT");
    };
    let inner_tables: Vec<&str> = inner
        .direct_tables()
        .iter()
        .map(|t| t.name().as_str())
        .collect();
    assert_eq!(inner_tables, ["active_users"]);

    let tree = parse(
        "SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM items WHERE items.order_id = orders.id)",
    );
    let WhereKind::Subquery(subquery) = where_of(&tree).kind() else {
        panic!("expected a subquery predicate");
    };
    assert_eq!(subquery.sublink_type(), SubLinkType::Exists);
    assert!(subquery.test_expr().is_none());

    let tree = parse("SELECT * FROM users WHERE age > (SELECT AVG(age) FROM users)");
    let WhereKind::Binary(binary) = where_of(&tree).kind() else {
        panic!("expected a comparison");
    };
    let WhereKind::Subquery(subquery) = binary.rexpr().kind() else {
        panic!("expected a scalar subquery on the right");
    };
    assert_eq!(subquery.sublink_type(), SubLinkType::Expr);
    assert!(subquery.test_expr().is_none());

    let tree = parse("SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM banned_users)");
    assert!(has_subqueries(select_of(&tree)));
}

/// PGC-361: only equality ANY sublinks are IN-equivalent; a quantified
/// range comparison is rejected, not converted with its operator dropped.
#[test]
fn test_where_subquery_any_operator_check() {
    let tree = parse("SELECT * FROM users WHERE id = ANY (SELECT user_id FROM active_users)");
    assert!(matches!(
        where_of(&tree).kind(),
        WhereKind::Subquery(subquery) if subquery.sublink_type() == SubLinkType::Any
    ));
    for sql in [
        "SELECT * FROM users WHERE id > ANY (SELECT user_id FROM active_users)",
        "SELECT * FROM users WHERE id <= ANY (SELECT user_id FROM active_users)",
        "SELECT * FROM users WHERE id <> ANY (SELECT user_id FROM active_users)",
        "SELECT * FROM users WHERE id < ALL (SELECT user_id FROM active_users)",
    ] {
        assert!(query_tree_parse(sql).is_err(), "should be rejected: {sql}");
    }
    let tree = parse("SELECT * FROM users WHERE id <> ALL (SELECT user_id FROM active_users)");
    assert!(matches!(
        where_of(&tree).kind(),
        WhereKind::Subquery(subquery) if subquery.sublink_type() == SubLinkType::All
    ));
}

#[test]
fn test_subquery_render_cases() {
    let sql = parse("SELECT * FROM users WHERE id IN (SELECT user_id FROM active_users)").sql();
    assert!(sql.contains("IN") && sql.contains("active_users"), "{sql}");
    let sql = parse("SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM items)").sql();
    assert!(sql.contains("EXISTS") && sql.contains("items"), "{sql}");
}

#[test]
fn test_has_subqueries_cases() {
    #[rustfmt::skip]
    let cases = [
        ("select list", "SELECT id, (SELECT x FROM other WHERE id = 1) as val FROM t",       true),
        ("FROM",        "SELECT * FROM (SELECT id FROM users) sub",                          true),
        ("join",        "SELECT * FROM a JOIN (SELECT id FROM b) sub ON a.id = sub.id",      true),
        ("WHERE",       "SELECT * FROM t WHERE id IN (SELECT id FROM other)",                true),
        ("CASE",        "SELECT CASE WHEN id IN (SELECT id FROM other) THEN 1 ELSE 0 END FROM items", true),
        ("none",        "SELECT id, name FROM users WHERE active = true",                    false),
    ];
    for (label, sql, expected) in cases {
        assert_eq!(
            has_subqueries(select_of(&parse(sql))),
            expected,
            "{label}: {sql}"
        );
    }
}

#[test]
fn test_select_branches_with_source_cases() {
    use UpdateQuerySource::{FromClause, Subquery};
    // The outer SELECT is FromClause; each subquery branch carries its kind.
    #[rustfmt::skip]
    let cases = [
        ("simple",            "SELECT id FROM users WHERE id = 1",                                                       vec![FromClause]),
        ("union",             "SELECT id FROM users UNION SELECT id FROM admins",                                        vec![FromClause, FromClause]),
        ("WHERE IN",          "SELECT * FROM users WHERE id IN (SELECT user_id FROM active_users)",                      vec![FromClause, Subquery(SubqueryKind::Inclusion)]),
        ("NOT IN",            "SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM banned_users)",                  vec![FromClause, Subquery(SubqueryKind::Exclusion)]),
        ("EXISTS",            "SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM items WHERE items.order_id = orders.id)", vec![FromClause, Subquery(SubqueryKind::Inclusion)]),
        ("NOT EXISTS",        "SELECT * FROM orders WHERE NOT EXISTS (SELECT 1 FROM items WHERE items.order_id = orders.id)", vec![FromClause, Subquery(SubqueryKind::Exclusion)]),
        ("scalar in WHERE",   "SELECT * FROM users WHERE age > (SELECT AVG(age) FROM users)",                            vec![FromClause, Subquery(SubqueryKind::Scalar)]),
        ("scalar in list",    "SELECT id, (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) FROM users",     vec![FromClause, Subquery(SubqueryKind::Scalar)]),
        ("FROM subquery",     "SELECT * FROM (SELECT id FROM users) sub",                                                vec![FromClause, Subquery(SubqueryKind::Inclusion)]),
        ("CTE reference",     "WITH active AS (SELECT id FROM users WHERE active = true) SELECT * FROM active",          vec![FromClause, Subquery(SubqueryKind::Inclusion)]),
    ];
    for (label, sql, expected) in cases {
        let tree = parse(sql);
        let sources: Vec<UpdateQuerySource> = select_branches_with_source(tree.root())
            .into_iter()
            .map(|(_, source)| source)
            .collect();
        assert_eq!(sources, expected, "{label}: {sql}");
    }
    let tree =
        parse("WITH active AS (SELECT id FROM users WHERE active = true) SELECT * FROM active");
    let branches = select_branches_with_source(tree.root());
    let inner_tables: Vec<&str> = branches[1]
        .0
        .direct_tables()
        .iter()
        .map(|t| t.name().as_str())
        .collect();
    assert_eq!(inner_tables, ["users"]);
}

/// Every SELECT a query contains: set-operation branches, FROM derived
/// tables, WHERE / SELECT-list subqueries and CTE bodies through their
/// references.
#[test]
fn test_branch_count_cases() {
    #[rustfmt::skip]
    let cases = [
        ("simple SELECT",        "SELECT id FROM users WHERE id = 1",                                                  1),
        ("nested UNION",         "SELECT id FROM a UNION SELECT id FROM b UNION SELECT id FROM c",                     3),
        ("FROM subquery",        "SELECT * FROM (SELECT id FROM users) sub",                                           2),
        ("WHERE IN subquery",    "SELECT * FROM users WHERE id IN (SELECT user_id FROM active)",                      2),
        ("EXISTS subquery",      "SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM items WHERE items.order_id = orders.id)", 2),
        ("SELECT-list subquery", "SELECT id, (SELECT name FROM users WHERE users.id = orders.user_id) FROM orders",    2),
        ("nested subqueries",    "SELECT * FROM (SELECT id FROM users WHERE id IN (SELECT user_id FROM active)) sub", 3),
        ("CTE reference",        "WITH x AS (SELECT id FROM users WHERE active = true) SELECT * FROM x",               2),
    ];
    for (label, sql, expected) in cases {
        assert_eq!(branch_count(&parse(sql)), expected, "{label}: {sql}");
    }
}

// ============================================================================
// CTEs
// ============================================================================

#[test]
fn test_cte_simple() {
    let tree = parse("WITH x AS (SELECT id FROM users) SELECT * FROM x");
    let ctes: Vec<_> = tree.root().ctes().collect();
    assert_eq!(ctes.len(), 1);
    assert_eq!(ctes[0].name(), "x");
    assert_eq!(ctes[0].materialization(), CteMaterialization::Default);
    let mut from = select_of(&tree).from();
    let (Some(Source::CteRef(cte_ref)), None) = (from.next(), from.next()) else {
        panic!("expected a CTE reference");
    };
    assert_eq!(cte_ref.cte_name(), "x");
    assert_eq!(cte_ref.alias(), None);
    let inner: Vec<_> = tables(&tree).into_iter().map(|(_, name, _)| name).collect();
    assert_eq!(inner, ["users", "users"]);

    let sql = tree.sql();
    assert!(sql.contains("WITH") && sql.contains("x AS"), "{sql}");
    assert!(sql.matches(" x").count() >= 2, "{sql}");
}

#[test]
fn test_cte_column_aliases_and_reference_alias() {
    let tree = parse("WITH x(a, b) AS (SELECT id, name FROM users) SELECT * FROM x");
    let cte = tree.root().ctes().next().expect("a CTE");
    let aliases: Vec<&str> = cte
        .column_aliases()
        .map(|name| name.text().as_str())
        .collect();
    assert_eq!(aliases, ["a", "b"]);
    let Some(Source::CteRef(cte_ref)) = select_of(&tree).from().next() else {
        panic!("expected a CTE reference");
    };
    let aliases: Vec<&str> = cte_ref
        .column_aliases()
        .map(|name| name.text().as_str())
        .collect();
    assert_eq!(aliases, ["a", "b"]);

    let tree = parse("WITH x AS (SELECT id FROM users) SELECT * FROM x AS y");
    let Some(Source::CteRef(cte_ref)) = select_of(&tree).from().next() else {
        panic!("expected a CTE reference");
    };
    assert_eq!(cte_ref.cte_name(), "x");
    assert_eq!(cte_ref.alias().map(EcoString::as_str), Some("y"));
}

#[test]
fn test_cte_reference_cases() {
    let tree = parse(
        "WITH active AS (SELECT id, name FROM users WHERE active = true) \
         SELECT u.id, a.name FROM users u JOIN active a ON u.id = a.id",
    );
    assert_eq!(tree.root().ctes().len(), 1);
    let join = only_join(select_of(&tree));
    assert!(matches!(join.left(), Source::Table(_)));
    assert!(matches!(join.right(), Source::CteRef(_)));

    let tree = parse("WITH x AS (SELECT id FROM users) SELECT * FROM x a JOIN x b ON a.id = b.id");
    let join = only_join(select_of(&tree));
    assert!(matches!(join.left(), Source::CteRef(_)));
    assert!(matches!(join.right(), Source::CteRef(_)));

    let tree = parse("WITH x AS (SELECT id FROM users) SELECT * FROM x UNION ALL SELECT * FROM x");
    assert_eq!(tree.root().ctes().len(), 1);
    let Body::SetOp(set_op) = tree.root().body() else {
        panic!("expected a set operation");
    };
    for branch in [set_op.left(), set_op.right()] {
        let Body::Select(select) = branch.body() else {
            panic!("expected a SELECT branch");
        };
        assert!(matches!(select.from().next(), Some(Source::CteRef(_))));
    }

    let tree = parse("WITH a AS (SELECT id FROM users), b AS (SELECT * FROM a) SELECT * FROM b");
    let names: Vec<&str> = tree.root().ctes().map(|cte| cte.name().as_str()).collect();
    assert_eq!(names, ["a", "b"]);
    let b = tree.root().ctes().nth(1).expect("CTE b");
    let Body::Select(b_select) = b.query().body() else {
        panic!("expected a SELECT");
    };
    assert!(matches!(
        b_select.from().next(),
        Some(Source::CteRef(cte_ref)) if cte_ref.cte_name() == "a"
    ));

    let tree = parse(
        "WITH a AS (SELECT id FROM users), b AS (SELECT id FROM products) \
         SELECT * FROM a JOIN b ON a.id = b.id",
    );
    let join = only_join(select_of(&tree));
    let (Source::CteRef(left), Source::CteRef(right)) = (join.left(), join.right()) else {
        panic!("expected two CTE references");
    };
    assert_eq!(
        (left.cte_name().as_str(), right.cte_name().as_str()),
        ("a", "b")
    );
}

#[test]
fn test_cte_name_resolution_cases() {
    // An unreferenced CTE is kept; its name does not capture other tables.
    let tree = parse("WITH x AS (SELECT 1) SELECT * FROM users");
    assert_eq!(tree.root().ctes().len(), 1);
    assert_eq!(only_table(select_of(&tree)).name(), "users");
    // A schema-qualified name is never a CTE reference.
    let tree = parse("WITH users AS (SELECT 1 AS id) SELECT * FROM public.users");
    let table = only_table(select_of(&tree));
    assert_eq!(table.schema().map(EcoString::as_str), Some("public"));
    assert_eq!(table.name(), "users");
}

#[test]
fn test_cte_materialization_cases() {
    let tree = parse("WITH x AS MATERIALIZED (SELECT id FROM users) SELECT * FROM x");
    assert_eq!(
        tree.root().ctes().next().expect("CTE").materialization(),
        CteMaterialization::Materialized
    );
    let Some(Source::CteRef(cte_ref)) = select_of(&tree).from().next() else {
        panic!("expected a CTE reference");
    };
    assert_eq!(cte_ref.materialization(), CteMaterialization::Materialized);
    assert!(tree.sql().contains("MATERIALIZED"));

    let tree = parse("WITH x AS NOT MATERIALIZED (SELECT id FROM users) SELECT * FROM x");
    assert_eq!(
        tree.root().ctes().next().expect("CTE").materialization(),
        CteMaterialization::NotMaterialized
    );
    assert!(tree.sql().contains("NOT MATERIALIZED"));
}

#[test]
fn test_cte_recursive_rejected() {
    let err = query_tree_parse("WITH RECURSIVE x AS (SELECT 1 UNION ALL SELECT 1) SELECT * FROM x")
        .expect_err("recursive CTE is rejected");
    assert!(err.to_string().contains("RECURSIVE"), "{err}");
}

// ============================================================================
// ORDER BY, GROUP BY, HAVING, LIMIT
// ============================================================================

#[test]
fn test_order_by_cases() {
    use OrderDirection::{Asc, Desc};
    #[rustfmt::skip]
    let cases: [(&str, &str, Vec<(Option<&str>, &str, OrderDirection)>); 5] = [
        ("two columns",   "SELECT * FROM users ORDER BY last_name ASC, first_name DESC", vec![(None, "last_name", Asc), (None, "first_name", Desc)]),
        ("qualified",     "SELECT * FROM users u ORDER BY u.name ASC",                   vec![(Some("u"), "name", Asc)]),
        ("with WHERE",    "SELECT * FROM users WHERE active = true ORDER BY name ASC",   vec![(None, "name", Asc)]),
        ("explicit DESC", "SELECT * FROM users ORDER BY age DESC",                       vec![(None, "age", Desc)]),
        ("default ASC",   "SELECT * FROM users ORDER BY name",                           vec![(None, "name", Asc)]),
    ];
    for (label, sql, expected) in cases {
        let tree = parse(sql);
        let actual: Vec<_> = tree
            .root()
            .order_by()
            .map(|clause| {
                let ScalarKind::Column(column) = clause.expr().kind() else {
                    panic!("{label}: expected a column");
                };
                (
                    column.table().map(EcoString::as_str),
                    column.column().as_str(),
                    clause.direction(),
                )
            })
            .collect();
        assert_eq!(actual, expected, "{label}: {sql}");
    }
}

#[test]
fn test_group_by_having_cases() {
    let tree = parse("SELECT status FROM orders GROUP BY status");
    let group_by: Vec<_> = select_of(&tree)
        .group_by()
        .map(|c| (c.table().cloned(), c.column().clone()))
        .collect();
    assert_eq!(group_by, [(None, EcoString::from("status"))]);

    let tree = parse("SELECT status, category FROM orders GROUP BY status, category");
    let group_by: Vec<&str> = select_of(&tree)
        .group_by()
        .map(|c| c.column().as_str())
        .collect();
    assert_eq!(group_by, ["status", "category"]);

    let tree = parse("SELECT o.status FROM orders o GROUP BY o.status");
    let group_by: Vec<_> = select_of(&tree)
        .group_by()
        .map(|c| (c.table().map(EcoString::as_str), c.column().as_str()))
        .collect();
    assert_eq!(group_by, [(Some("o"), "status")]);

    let tree = parse("SELECT status FROM orders GROUP BY status HAVING status = 'active'");
    assert!(select_of(&tree).having().is_some());
    let tree = parse(
        "SELECT category FROM sales GROUP BY category HAVING category = 'electronics' AND category != 'toys'",
    );
    assert_eq!(select_of(&tree).group_by().len(), 1);
    assert!(select_of(&tree).having().is_some());

    let tree = parse("SELECT * FROM users WHERE id = 1");
    assert_eq!(select_of(&tree).group_by().len(), 0);
    assert!(select_of(&tree).having().is_none());

    let tree = parse(
        "SELECT status FROM orders GROUP BY status HAVING status != 'cancelled' ORDER BY status DESC LIMIT 10",
    );
    assert_eq!(select_of(&tree).group_by().len(), 1);
    assert!(select_of(&tree).having().is_some());
    assert!(tree.root().has_limit());
    assert_eq!(tree.root().order_by().len(), 1);
}

#[test]
fn test_limit_cases() {
    use LiteralValue::{Integer, Parameter};
    #[rustfmt::skip]
    let cases = [
        ("count only",    "SELECT * FROM users LIMIT 10",            (Some(Integer(10)), None)),
        ("offset only",   "SELECT * FROM users OFFSET 20",           (None, Some(Integer(20)))),
        ("both",          "SELECT * FROM users LIMIT 10 OFFSET 20",  (Some(Integer(10)), Some(Integer(20)))),
        ("none",          "SELECT * FROM users",                     (None, None)),
        ("parameter",     "SELECT * FROM users LIMIT $1",            (Some(Parameter("$1".into())), None)),
        ("parameters",    "SELECT * FROM users LIMIT $1 OFFSET $2",  (Some(Parameter("$1".into())), Some(Parameter("$2".into())))),
    ];
    for (label, sql, expected) in cases {
        let tree = parse(sql);
        let actual = (
            tree.root().limit_count().map(|l| l.value().clone()),
            tree.root().limit_offset().map(|l| l.value().clone()),
        );
        assert_eq!(actual, expected, "{label}: {sql}");
        assert_eq!(
            tree.root().has_limit(),
            expected.0.is_some() || expected.1.is_some(),
            "{label}"
        );
    }
}

#[test]
fn test_limit_render_cases() {
    #[rustfmt::skip]
    let cases = [
        ("SELECT a FROM t ORDER BY a LIMIT 10",  "SELECT a FROM t ORDER BY a ASC LIMIT 10"),
        ("SELECT a FROM t LIMIT 10 OFFSET 5",    "SELECT a FROM t LIMIT 10 OFFSET 5"),
        ("SELECT a FROM t OFFSET 5",             "SELECT a FROM t OFFSET 5"),
    ];
    for (sql, expected) in cases {
        assert_eq!(parse(sql).sql(), expected, "{sql}");
    }
}

// ============================================================================
// Functions, CASE, casts and arithmetic
// ============================================================================

#[test]
fn test_function_in_select_list_cases() {
    let tree = parse("SELECT COUNT(*) as total FROM users");
    assert_eq!(
        alias_of(items(select_of(&tree))[0]).map(EcoString::as_str),
        Some("total")
    );

    let tree = parse("SELECT id, name, COUNT(*) as cnt FROM users GROUP BY id, name");
    let kinds: Vec<&str> = items(select_of(&tree))
        .into_iter()
        .map(|item| match expr_of(item).kind() {
            ScalarKind::Column(_) => "column",
            ScalarKind::Function(_) => "function",
            _ => "other",
        })
        .collect();
    assert_eq!(kinds, ["column", "column", "function"]);

    let tree = parse("SELECT 42 as answer, 'hello' as greeting FROM t");
    let values: Vec<&LiteralValue> = items(select_of(&tree))
        .into_iter()
        .map(|item| match expr_of(item).kind() {
            ScalarKind::Literal(literal) => literal.value(),
            _ => panic!("expected a literal"),
        })
        .collect();
    assert_eq!(
        values,
        [
            &LiteralValue::Integer(42),
            &LiteralValue::String("hello".into())
        ]
    );
}

/// Expected properties of a parsed function call; `None` fields are not
/// checked.
#[derive(Default)]
struct FunctionExpect {
    name: Option<&'static str>,
    args: Option<usize>,
    agg_star: Option<bool>,
    agg_distinct: Option<bool>,
    has_filter: Option<bool>,
    agg_order: Option<&'static [OrderDirection]>,
    agg_order_len: Option<usize>,
    /// Name of the function the first argument calls.
    first_arg_function: Option<&'static str>,
    window: Option<WindowExpect>,
}

/// Expected OVER clause; `None` fields are not checked.
#[derive(Default)]
struct WindowExpect {
    partitions: Option<usize>,
    order: Option<&'static [OrderDirection]>,
    order_len: Option<usize>,
}

fn field_check<T: PartialEq + std::fmt::Debug>(
    context: &str,
    field: &str,
    expected: Option<T>,
    actual: T,
) {
    if let Some(expected) = expected {
        assert_eq!(actual, expected, "{context}: {field}");
    }
}

impl FunctionExpect {
    fn check(&self, function: Function<'_>, context: &str) {
        field_check(context, "name", self.name, function.name().as_str());
        field_check(context, "argument count", self.args, function.args().len());
        field_check(context, "agg_star", self.agg_star, function.agg_star());
        field_check(
            context,
            "agg_distinct",
            self.agg_distinct,
            function.agg_distinct(),
        );
        field_check(
            context,
            "FILTER",
            self.has_filter,
            function.filter().is_some(),
        );
        field_check(
            context,
            "aggregate ORDER BY",
            self.agg_order.map(<[_]>::to_vec),
            directions(function.agg_order()),
        );
        field_check(
            context,
            "aggregate ORDER BY count",
            self.agg_order_len,
            function.agg_order().len(),
        );
        let first_arg_function = function
            .args()
            .next()
            .and_then(|arg| match arg.kind() {
                ScalarKind::Function(inner) => Some(inner.name().as_str()),
                _ => None,
            })
            .unwrap_or("");
        field_check(
            context,
            "first argument's function",
            self.first_arg_function,
            first_arg_function,
        );
        if let Some(window) = &self.window {
            let over = function
                .over()
                .unwrap_or_else(|| panic!("{context}: expected OVER"));
            field_check(
                context,
                "PARTITION BY count",
                window.partitions,
                over.partition_by().len(),
            );
            field_check(
                context,
                "window ORDER BY",
                window.order.map(<[_]>::to_vec),
                directions(over.order_by()),
            );
            field_check(
                context,
                "window ORDER BY count",
                window.order_len,
                over.order_by().len(),
            );
        }
    }
}

#[test]
fn test_select_function_cases() {
    use OrderDirection::{Asc, Desc};
    // COALESCE / GREATEST / LEAST / NULLIF parse as plain function calls.
    #[rustfmt::skip]
    let cases = [
        ("count(*)",                 "SELECT COUNT(*) FROM users",
         FunctionExpect { name: Some("count"), agg_star: Some(true), args: Some(0), ..Default::default() }),
        ("count(column)",            "SELECT COUNT(id) FROM users",
         FunctionExpect { name: Some("count"), agg_star: Some(false), args: Some(1), ..Default::default() }),
        ("count(DISTINCT)",          "SELECT COUNT(DISTINCT status) FROM orders",
         FunctionExpect { name: Some("count"), agg_distinct: Some(true), ..Default::default() }),
        ("sum",                      "SELECT SUM(amount) FROM orders WHERE tenant_id = 1",
         FunctionExpect { name: Some("sum"), ..Default::default() }),
        ("nested round(avg())",      "SELECT ROUND(AVG(value)) FROM data",
         FunctionExpect { name: Some("round"), args: Some(1), first_arg_function: Some("avg"), ..Default::default() }),
        ("coalesce",                 "SELECT COALESCE(name, 'unknown') FROM users",
         FunctionExpect { name: Some("coalesce"), args: Some(2), ..Default::default() }),
        ("coalesce(max(), 0)",       "SELECT COALESCE(MAX(value), 0) FROM data",
         FunctionExpect { name: Some("coalesce"), args: Some(2), first_arg_function: Some("max"), ..Default::default() }),
        ("greatest",                 "SELECT GREATEST(a, b, c) FROM t",
         FunctionExpect { name: Some("greatest"), args: Some(3), ..Default::default() }),
        ("least",                    "SELECT LEAST(a, b) FROM t",
         FunctionExpect { name: Some("least"), args: Some(2), ..Default::default() }),
        ("nullif",                   "SELECT NULLIF(status, 'deleted') FROM items",
         FunctionExpect { name: Some("nullif"), args: Some(2), ..Default::default() }),
        ("window ORDER BY",          "SELECT sum(amount) OVER (ORDER BY date) FROM orders",
         FunctionExpect { name: Some("sum"), window: Some(WindowExpect { partitions: Some(0), order_len: Some(1), ..Default::default() }), ..Default::default() }),
        ("window PARTITION BY",      "SELECT sum(amount) OVER (PARTITION BY category ORDER BY date) FROM orders",
         FunctionExpect { window: Some(WindowExpect { partitions: Some(1), order_len: Some(1), ..Default::default() }), ..Default::default() }),
        ("row_number()",             "SELECT row_number() OVER (ORDER BY id) FROM users",
         FunctionExpect { name: Some("row_number"), args: Some(0), window: Some(WindowExpect::default()), ..Default::default() }),
        ("window multi ORDER BY",    "SELECT sum(x) OVER (ORDER BY a ASC, b DESC) FROM t",
         FunctionExpect { window: Some(WindowExpect { order: Some(&[Asc, Desc]), ..Default::default() }), ..Default::default() }),
        ("aggregate ORDER BY",       "SELECT string_agg(name, ', ' ORDER BY name) FROM t",
         FunctionExpect { name: Some("string_agg"), agg_order: Some(&[Asc]), ..Default::default() }),
        ("DISTINCT with ORDER BY",   "SELECT string_agg(DISTINCT name, ', ' ORDER BY name) FROM t",
         FunctionExpect { agg_distinct: Some(true), agg_order_len: Some(1), ..Default::default() }),
        ("aggregate multi ORDER BY", "SELECT array_agg(x ORDER BY y ASC, z DESC) FROM t",
         FunctionExpect { name: Some("array_agg"), agg_order: Some(&[Asc, Desc]), ..Default::default() }),
        ("FILTER",                   "SELECT count(*) FILTER (WHERE x = 1) FROM t",
         FunctionExpect { agg_star: Some(true), has_filter: Some(true), ..Default::default() }),
    ];
    for (label, sql, expect) in cases {
        let (tree, function) = only_function(sql);
        expect.check(function(&tree), &format!("{label}: {sql}"));
    }
}

#[test]
fn test_function_schema_qualifier_is_kept() {
    let (tree, function) = only_function("SELECT pg_catalog.lower(name) FROM t");
    assert_eq!(
        function(&tree).schema().map(EcoString::as_str),
        Some("pg_catalog")
    );
    assert_eq!(function(&tree).name(), "lower");
    assert_eq!(tree.sql(), "SELECT pg_catalog.LOWER(name) FROM t");
    let (bare, function) = only_function("SELECT lower(name) FROM t");
    assert_eq!(function(&bare).schema(), None);
    assert_ne!(session_hash(&tree), session_hash(&bare));
}

#[test]
fn test_function_three_part_name_unsupported() {
    assert!(query_tree_parse("SELECT db.myschema.f(name) FROM t").is_err());
}

#[test]
fn test_function_render_cases() {
    #[rustfmt::skip]
    let cases = [
        ("count star",          "SELECT count(*) FROM t",                                       "SELECT COUNT(*) FROM t"),
        ("count distinct",      "SELECT count(DISTINCT status) FROM t",                         "SELECT COUNT(DISTINCT status) FROM t"),
        ("window",              "SELECT sum(amount) OVER (PARTITION BY category ORDER BY date) FROM t",
                                "SELECT SUM(amount) OVER (PARTITION BY category ORDER BY date ASC) FROM t"),
        ("aggregate ORDER BY",  "SELECT string_agg(name, ', ' ORDER BY name ASC) FROM t",       "SELECT STRING_AGG(name, ', ' ORDER BY name ASC) FROM t"),
    ];
    for (label, sql, expected) in cases {
        assert_eq!(parse(sql).sql(), expected, "{label}");
    }
}

/// Two FILTER aggregates with different predicates must render as
/// distinct function calls; they must not collapse to plain count(*).
#[test]
fn test_filter_aggregate_distinct_predicates_roundtrip() {
    let tree = parse(
        "SELECT count(*) FILTER (WHERE posttypeid = 1) AS questions, \
         count(*) FILTER (WHERE posttypeid = 2) AS answers FROM posts",
    );
    let items = items(select_of(&tree));
    assert_eq!(items.len(), 2);
    for (i, item) in items.iter().enumerate() {
        let ScalarKind::Function(function) = expr_of(*item).kind() else {
            panic!("expected a function in item {i}");
        };
        assert!(function.filter().is_some(), "item {i} keeps its FILTER");
    }
    let sql = tree.sql();
    assert_eq!(sql.matches("FILTER (WHERE").count(), 2, "{sql}");
    assert!(
        sql.contains("posttypeid = 1") && sql.contains("posttypeid = 2"),
        "{sql}"
    );
}

/// The OVER clause the only select-list function renders.
fn over_rendering(sql: &str) -> String {
    let rendered = parse(sql).sql();
    let (_, after_over) = rendered.split_once(" OVER ").expect("an OVER clause");
    let (over, _) = after_over.rsplit_once(" FROM ").expect("a FROM clause");
    over.to_owned()
}

#[test]
fn test_window_frame_render_cases() {
    #[rustfmt::skip]
    let cases = [
        // The single-bound shorthand normalizes to the explicit BETWEEN form.
        ("rows unbounded",     "SELECT sum(a) OVER (ORDER BY b ROWS UNBOUNDED PRECEDING) FROM t",
                               "(ORDER BY b ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"),
        ("rows between",       "SELECT sum(a) OVER (ORDER BY b ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t",
                               "(ORDER BY b ASC ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)"),
        ("range full",         "SELECT last_value(a) OVER (ORDER BY b RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t",
                               "(ORDER BY b ASC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"),
        ("groups exclude",     "SELECT count(*) OVER (ORDER BY b GROUPS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING EXCLUDE TIES) FROM t",
                               "(ORDER BY b ASC GROUPS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING EXCLUDE TIES)"),
        // No explicit frame: no frame text (PostgreSQL's default applies).
        ("default frame",      "SELECT sum(a) OVER (ORDER BY b) FROM t",
                               "(ORDER BY b ASC)"),
        // `OVER w` is inlined from the WINDOW clause (PGC-280).
        ("named window",       "SELECT sum(a) OVER w FROM t WINDOW w AS (PARTITION BY b ORDER BY c)",
                               "(PARTITION BY b ORDER BY c ASC)"),
        ("named with frame",   "SELECT last_value(a) OVER w FROM t WINDOW w AS (ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
                               "(ORDER BY b ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"),
    ];
    for (label, sql, expected) in cases {
        assert_eq!(over_rendering(sql), expected, "{label}");
    }
}

#[test]
fn test_undefined_window_reference_forwards() {
    // No WINDOW clause defines `w`: conversion fails so the query forwards
    // rather than silently rendering `OVER ()`.
    assert!(query_tree_parse("SELECT rank() OVER w FROM t").is_err());
}

#[test]
fn test_arithmetic_cases() {
    use ArithmeticOp::{Add, Divide, Multiply, Subtract};
    #[rustfmt::skip]
    let cases = [
        ("multiply", "SELECT amount * 2 FROM t",      Multiply),
        ("negative", "SELECT amount * -1 FROM t",     Multiply),
        ("add",      "SELECT price + tax FROM t",     Add),
        ("subtract", "SELECT total - discount FROM t", Subtract),
        ("divide",   "SELECT total / count FROM t",   Divide),
    ];
    for (label, sql, op) in cases {
        let tree = parse(sql);
        let ScalarKind::Arithmetic(arithmetic) = expr_of(items(select_of(&tree))[0]).kind() else {
            panic!("{label}: expected arithmetic");
        };
        assert_eq!(arithmetic.op(), op, "{label}");
    }
    let tree = parse("SELECT amount * -1 FROM t");
    let ScalarKind::Arithmetic(arithmetic) = expr_of(items(select_of(&tree))[0]).kind() else {
        panic!("expected arithmetic");
    };
    assert!(matches!(
        arithmetic.right().kind(),
        ScalarKind::Literal(literal) if literal.value() == &LiteralValue::Integer(-1)
    ));
    assert_eq!(tree.sql(), "SELECT (amount * -1) FROM t");

    let tree = parse("SELECT (a + b) * c FROM t");
    let ScalarKind::Arithmetic(outer) = expr_of(items(select_of(&tree))[0]).kind() else {
        panic!("expected arithmetic");
    };
    assert_eq!(outer.op(), Multiply);
    let ScalarKind::Arithmetic(inner) = outer.left().kind() else {
        panic!("expected nested arithmetic");
    };
    assert_eq!(inner.op(), Add);
}

/// The first select item as a `(inner, canonical target)` cast.
fn first_typecast(tree: &QueryTree) -> (Scalar<'_>, &str) {
    match expr_of(items(select_of(tree))[0]).kind() {
        ScalarKind::TypeCast(cast) => (cast.expr(), cast_target_deparse(cast.target())),
        _ => panic!("expected a type cast"),
    }
}

#[test]
fn test_type_cast_cases() {
    let tree = parse("SELECT COUNT(*)::INT FROM t");
    let (expr, target) = first_typecast(&tree);
    assert_eq!(target, "int4");
    assert!(matches!(expr.kind(), ScalarKind::Function(f) if f.name() == "count"));

    let tree = parse("SELECT col::text FROM t");
    let (expr, target) = first_typecast(&tree);
    assert_eq!(target, "text");
    assert!(matches!(expr.kind(), ScalarKind::Column(c) if c.column() == "col"));

    let tree = parse("SELECT (a + b)::numeric(10,2) FROM t");
    let (expr, target) = first_typecast(&tree);
    assert_eq!(target, "numeric(10,2)");
    assert!(matches!(expr.kind(), ScalarKind::Arithmetic(_)));

    let tree = parse("SELECT t.col::int FROM t");
    let (expr, target) = first_typecast(&tree);
    assert_eq!(target, "int4");
    assert!(matches!(
        expr.kind(),
        ScalarKind::Column(c) if c.table().map(EcoString::as_str) == Some("t") && c.column() == "col"
    ));

    let tree = parse("SELECT '2024-01-01'::DATE FROM t");
    let (expr, target) = first_typecast(&tree);
    assert_eq!(target, "date");
    assert!(
        matches!(expr.kind(), ScalarKind::Literal(l) if matches!(l.value(), LiteralValue::String(_)))
    );

    let tree = parse("SELECT COUNT(*)::INT AS n FROM t");
    assert_eq!(
        alias_of(items(select_of(&tree))[0]).map(EcoString::as_str),
        Some("n")
    );
    assert!(matches!(
        expr_of(items(select_of(&tree))[0]).kind(),
        ScalarKind::TypeCast(_)
    ));

    let sql = parse("SELECT COUNT(*)::INT, SUM(amount)::NUMERIC(18,2) FROM t").sql();
    assert!(sql.contains("(COUNT(*))::int4"), "{sql}");
    assert!(sql.contains("(SUM(amount))::numeric(18,2)"), "{sql}");
}

#[test]
fn test_case_shape_cases() {
    // Simple CASE carries the tested expression as `arg`; searched CASE has none.
    #[rustfmt::skip]
    let cases = [
        ("searched",     "SELECT CASE WHEN status = 'active' THEN 1 ELSE 0 END FROM items",                    false, 1, true),
        ("simple",       "SELECT CASE status WHEN 'active' THEN 1 WHEN 'pending' THEN 2 ELSE 0 END FROM items", true,  2, true),
        ("without ELSE", "SELECT CASE WHEN x > 0 THEN 'positive' END FROM items",                               false, 1, false),
    ];
    for (label, sql, has_arg, whens, has_default) in cases {
        let tree = parse(sql);
        let ScalarKind::Case(case) = expr_of(items(select_of(&tree))[0]).kind() else {
            panic!("{label}: expected a CASE in {sql}");
        };
        let actual = (
            case.arg().is_some(),
            case.whens().len(),
            case.default().is_some(),
        );
        assert_eq!(
            actual,
            (has_arg, whens, has_default),
            "{label}: (arg, WHENs, ELSE) of {sql}"
        );
    }
    assert_eq!(
        parse("SELECT CASE WHEN status = 'active' THEN 1 ELSE 0 END FROM items WHERE id = 1").sql(),
        "SELECT CASE WHEN status = 'active' THEN 1 ELSE 0 END FROM items WHERE id = 1"
    );
}

// ---------------------------------------------------------------
// PGC-123: HAVING aggregate metadata (DISTINCT / ORDER BY / FILTER)
// survives conversion and rendering.
// ---------------------------------------------------------------

/// The function on the left of a `f(...) <op> <rhs>` HAVING expression.
fn having_lhs_function(tree: &QueryTree) -> Function<'_> {
    let having = select_of(tree).having().expect("HAVING present");
    let WhereKind::Binary(binary) = having.kind() else {
        panic!("expected a binary HAVING");
    };
    match binary.lexpr().kind() {
        WhereKind::Scalar(scalar) => match scalar.kind() {
            ScalarKind::Function(function) => function,
            _ => panic!("expected a function on the left of HAVING"),
        },
        _ => panic!("expected a scalar on the left of HAVING"),
    }
}

#[test]
fn test_having_aggregate_decorations_cases() {
    let tree = parse("SELECT name FROM users GROUP BY name HAVING COUNT(DISTINCT id) > 1");
    let function = having_lhs_function(&tree);
    assert_eq!(function.name(), "count");
    assert!(function.agg_distinct());
    assert!(tree.sql().contains("COUNT(DISTINCT "), "{}", tree.sql());

    let tree =
        parse("SELECT name FROM users GROUP BY name HAVING COUNT(*) FILTER (WHERE id > 0) > 5");
    let function = having_lhs_function(&tree);
    assert_eq!(function.name(), "count");
    assert!(function.agg_star());
    assert!(function.filter().is_some());
    assert!(tree.sql().contains("FILTER (WHERE "), "{}", tree.sql());

    let tree =
        parse("SELECT id FROM users GROUP BY id HAVING string_agg(name, ',' ORDER BY name) <> ''");
    let function = having_lhs_function(&tree);
    assert_eq!(function.name(), "string_agg");
    assert_eq!(function.agg_order().len(), 1);
    assert!(tree.sql().contains("ORDER BY"), "{}", tree.sql());

    let tree = parse(
        "SELECT name FROM users GROUP BY name HAVING COUNT(DISTINCT id) FILTER (WHERE id > 0) > 1",
    );
    let function = having_lhs_function(&tree);
    assert!(function.agg_distinct() && function.filter().is_some());
    let sql = tree.sql();
    assert!(
        sql.contains("COUNT(DISTINCT ") && sql.contains("FILTER (WHERE "),
        "{sql}"
    );

    // Parse → render → parse → render converges.
    let first =
        parse("SELECT name FROM users GROUP BY name HAVING COUNT(*) FILTER (WHERE id > 0) > 5")
            .sql();
    assert_eq!(parse(&first).sql(), first);
}

// ============================================================================
// Rendering
// ============================================================================

#[test]
fn test_literal_value_deparse() {
    let text = |value: &LiteralValue| {
        let mut buf = String::new();
        value.deparse(&mut buf);
        buf
    };
    assert_eq!(text(&LiteralValue::String("hello".into())), "'hello'");
    assert_eq!(text(&LiteralValue::Integer(42)), "42");
    assert_eq!(
        text(&LiteralValue::Float(NotNan::new(3.25).expect("not NaN"))),
        "3.25"
    );
    assert_eq!(text(&LiteralValue::Boolean(true)), "true");
    assert_eq!(text(&LiteralValue::Boolean(false)), "false");
    assert_eq!(text(&LiteralValue::Null), "NULL");
    assert_eq!(text(&LiteralValue::Parameter("$1".into())), "$1");
    assert_eq!(text(&LiteralValue::String("".into())), "''");
    assert_eq!(
        text(&LiteralValue::String("test'quote".into())),
        "'test''quote'"
    );
    assert_eq!(
        text(&LiteralValue::String("test\\path".into())),
        "E'test\\\\path'"
    );
}

#[test]
fn test_operator_deparse() {
    fn text(value: &impl Deparse) -> String {
        let mut buf = String::new();
        value.deparse(&mut buf);
        buf
    }
    assert_eq!(text(&UnaryOp::Not), "NOT");
    assert_eq!(text(&UnaryOp::IsNull), "IS NULL");
    assert_eq!(text(&UnaryOp::IsNotNull), "IS NOT NULL");
    assert_eq!(text(&UnaryOp::IsTrue), "IS TRUE");
    assert_eq!(text(&UnaryOp::IsNotTrue), "IS NOT TRUE");
    assert_eq!(text(&UnaryOp::IsFalse), "IS FALSE");
    assert_eq!(text(&UnaryOp::IsNotFalse), "IS NOT FALSE");
    assert_eq!(text(&BinaryOp::Equal), "=");
    assert_eq!(text(&BinaryOp::NotEqual), "!=");
    assert_eq!(text(&BinaryOp::LessThan), "<");
    assert_eq!(text(&BinaryOp::GreaterThanOrEqual), ">=");
    assert_eq!(text(&BinaryOp::Like), "LIKE");
    assert_eq!(text(&BinaryOp::NotLike), "NOT LIKE");
    assert_eq!(text(&BinaryOp::And), "AND");
    assert_eq!(text(&BinaryOp::Or), "OR");
    assert_eq!(text(&MultiOp::In), "IN");
    assert_eq!(text(&MultiOp::NotIn), "NOT IN");
}

/// Identifiers that need quoting, predicates of every unary form, and
/// IN lists render as written.
#[test]
fn test_render_cases() {
    #[rustfmt::skip]
    let cases = [
        ("quoted table and alias",   r#"SELECT * FROM public."userAccount" "usrAcc""#),
        ("quoted column",            r#"SELECT "Users"."firstName" FROM "Users""#),
        ("quoted alias",             r#"SELECT id AS "Alias" FROM t"#),
        ("qualified table alias",    "SELECT * FROM public.users alias"),
        ("column alias",             "SELECT users.name AS alias FROM users"),
        ("equality",                 "SELECT * FROM t WHERE users.name = 'john'"),
        ("NOT",                      "SELECT * FROM t WHERE NOT active"),
        ("IS NULL",                  "SELECT * FROM t WHERE deleted_at IS NULL"),
        ("IS NOT NULL",              "SELECT * FROM t WHERE name IS NOT NULL"),
        ("IS TRUE",                  "SELECT * FROM t WHERE active IS TRUE"),
        ("IS NOT FALSE",             "SELECT * FROM t WHERE active IS NOT FALSE"),
        ("IN",                       "SELECT * FROM t WHERE status IN ('active', 'pending')"),
        ("NOT IN",                   "SELECT * FROM t WHERE id NOT IN (1, 2, 3)"),
        ("simple",                   "SELECT id, name FROM users"),
        ("with WHERE",               "SELECT * FROM users WHERE id = 1"),
        ("DISTINCT",                 "SELECT DISTINCT name FROM users"),
        ("comma join",               "SELECT * FROM users, orders"),
        ("schema qualified",         "SELECT * FROM public.users"),
        ("join",                     "SELECT first_name, last_name, film_id FROM actor a JOIN film_actor fa ON a.actor_id = fa.actor_id WHERE a.actor_id = 1"),
        ("left join",                "SELECT a.id, b.name FROM a LEFT JOIN b ON a.id = b.a_id WHERE a.id = 1"),
        ("right join",               "SELECT a.id, b.name FROM a RIGHT JOIN b ON a.id = b.a_id WHERE b.id = 1"),
        ("mixed joins",              "SELECT * FROM a JOIN b ON a.id = b.a_id LEFT JOIN c ON b.id = c.b_id WHERE a.id = 1"),
        ("VALUES with aliases",      "SELECT fa.actor_id FROM (VALUES ('1', '2'), ('3', '4')) fa(actor_id, film_id) WHERE a.actor_id = 1"),
        ("ORDER BY ASC",             "SELECT * FROM users ORDER BY name ASC"),
        ("ORDER BY DESC",            "SELECT * FROM users ORDER BY age DESC"),
        ("ORDER BY two",             "SELECT * FROM users ORDER BY last_name ASC, first_name DESC"),
        ("CTE",                      "WITH x AS (SELECT id FROM users) SELECT * FROM x"),
    ];
    for (label, sql) in cases {
        assert_eq!(parse(sql).sql(), sql, "{label}");
    }
}

/// Parse → render → parse gives the same tree.
#[test]
fn test_round_trip_cases() {
    for sql in [
        "SELECT id, name FROM users WHERE active = true",
        "SELECT id, name FROM users JOIN address a on a.is = users.address_id WHERE active = true",
        "SELECT id, name FROM (SELECT * FROM users) u WHERE active = true",
        "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id",
        "SELECT * FROM users ORDER BY name ASC",
        "SELECT * FROM users ORDER BY age DESC",
        "SELECT id, name FROM users WHERE active = true ORDER BY created_at DESC",
        "SELECT * FROM users ORDER BY last_name ASC, first_name ASC, id DESC",
    ] {
        let tree = parse(sql);
        assert_eq!(parse(&tree.sql()), tree, "round trip of {sql}");
    }
}

#[test]
fn test_render_keeps_meaningful_parentheses_cases() {
    // NOT binds tighter than AND/OR, and AND tighter than OR, so grouping
    // parentheses must survive or the predicate changes meaning; AND inside
    // OR needs none.
    #[rustfmt::skip]
    let cases = [
        ("NOT with AND",       "SELECT * FROM t WHERE NOT (x = 1 AND y = 2)",        "NOT (x = 1 AND y = 2)"),
        ("NOT with OR",        "SELECT * FROM t WHERE NOT (x = 1 OR y = 2)",         "NOT (x = 1 OR y = 2)"),
        ("OR inside AND",      "SELECT * FROM t WHERE (x = 1 OR y = 2) AND z = 3",   "(x = 1 OR y = 2)"),
    ];
    for (label, sql, fragment) in cases {
        let text = parse(sql).sql();
        assert!(
            text.contains(fragment),
            "{label}: expected {fragment:?} in {text:?}"
        );
    }
    assert_eq!(
        parse("SELECT * FROM t WHERE x = 1 OR (y = 2 AND z = 3)").sql(),
        "SELECT * FROM t WHERE x = 1 OR y = 2 AND z = 3"
    );
}

#[test]
fn test_parameterized_where_cases() {
    use LiteralValue::{Boolean, Parameter};
    // WHERE literals in order; each statement also renders back to itself.
    #[rustfmt::skip]
    let cases = [
        ("single parameter",       "SELECT * FROM users WHERE id = $1",                      vec![Parameter("$1".into())]),
        ("two parameters",         "SELECT * FROM users WHERE name = $1 AND age > $2",       vec![Parameter("$1".into()), Parameter("$2".into())]),
        ("parameter and literal",  "SELECT * FROM users WHERE name = $1 AND active = true",  vec![Parameter("$1".into()), Boolean(true)]),
    ];
    for (label, sql, expected) in cases {
        let tree = parse(sql);
        let literals: Vec<LiteralValue> = literals_under(where_of(&tree))
            .into_iter()
            .cloned()
            .collect();
        assert_eq!(literals, expected, "{label}: WHERE literals of {sql}");
        assert_eq!(tree.sql(), sql, "{label}: render");
    }
}

/// Converter errors name the construct that was met, not pg_query's numeric
/// node tag (PGC-409). One case per fall-through arm the workloads have hit.
#[test]
fn test_conversion_errors_name_the_construct() {
    let cases = [
        (
            "SELECT * FROM t TABLESAMPLE SYSTEM (10)",
            "TABLESAMPLE in FROM",
        ),
        (
            "SELECT a FROM t GROUP BY ROLLUP (a)",
            "GROUPING SETS / ROLLUP / CUBE in GROUP BY",
        ),
        (
            "SELECT arr[1] FROM t",
            "subscript or field access (x[1], (x).f) in a column expression",
        ),
        (
            "SELECT a AND b FROM t",
            "AND/OR/NOT expression in a column expression",
        ),
        (
            "SELECT a FROM t WHERE a IS DISTINCT FROM b",
            "IS DISTINCT FROM",
        ),
    ];
    for (sql, expected) in cases {
        let err = query_tree_parse(sql).expect_err(sql).to_string();
        assert!(err.contains(expected), "{sql}: {err}");
        assert!(
            !err.chars().last().is_some_and(|c| c.is_ascii_digit()),
            "{sql}: message ends in a raw number: {err}"
        );
    }
}
