//! The WHERE clause converter's tests: each predicate shape parses to
//! exactly the expected rope fragment.

#![allow(clippy::wildcard_enum_match_arm)]

use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

use ecow::EcoString;

use crate::query::ast::rope::{
    Body, Node, NodeKind, QueryTree, ScalarKind, SelectItem, View, Where, WhereKind,
};
use crate::query::ast::{
    ArithmeticOp, AstError, BinaryOp, LiteralValue, MultiOp, NodeId, UnaryOp, query_tree_parse,
};
use crate::query::cast::CastTarget;

fn parse(sql: &str) -> QueryTree {
    query_tree_parse(sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"))
}

/// The WHERE clause of `sql`'s SELECT, if any.
fn where_of(tree: &QueryTree) -> Option<Where<'_>> {
    match tree.root().body() {
        Body::Select(select) => select.where_clause(),
        _ => panic!("expected a SELECT"),
    }
}

/// The nodes of a view's subtree, as a fragment.
fn fragment<'t>(view: impl View<'t>) -> Vec<Node> {
    let node = view.node_ref();
    let start = u32::try_from(node.id().index()).expect("position fits");
    (start..node.end())
        .map(|position| node.tree().node(NodeId::from_index(position)).clone())
        .collect()
}

/// Parse SQL and return its WHERE clause as a fragment.
fn where_clause_parse(sql: &str) -> Result<Option<Vec<Node>>, AstError> {
    let tree = query_tree_parse(sql)?;
    Ok(where_of(&tree).map(fragment))
}

// ---------- The expected-fragment DSL ----------

fn node(kind: NodeKind, children: &[Vec<Node>]) -> Vec<Node> {
    let len = 1 + children.iter().map(|child| child.len()).sum::<usize>();
    let mut out = vec![Node {
        len: u32::try_from(len).expect("fragment fits"),
        kind,
    }];
    for child in children {
        out.extend(child.iter().cloned());
    }
    out
}

fn col(name: &str) -> Vec<Node> {
    node(
        NodeKind::Column {
            table: None,
            column: EcoString::from(name),
        },
        &[],
    )
}

fn qcol(table: &str, name: &str) -> Vec<Node> {
    node(
        NodeKind::Column {
            table: Some(EcoString::from(table)),
            column: EcoString::from(name),
        },
        &[],
    )
}

fn lit(value: LiteralValue) -> Vec<Node> {
    node(NodeKind::Literal(value), &[])
}

fn text(value: &str) -> Vec<Node> {
    lit(LiteralValue::String(value.into()))
}

fn int(value: i64) -> Vec<Node> {
    lit(LiteralValue::Integer(value))
}

fn param(name: &str) -> Vec<Node> {
    lit(LiteralValue::Parameter(name.into()))
}

fn boolean(value: bool) -> Vec<Node> {
    lit(LiteralValue::Boolean(value))
}

fn null() -> Vec<Node> {
    lit(LiteralValue::Null)
}

fn cmp(op: BinaryOp, lexpr: Vec<Node>, rexpr: Vec<Node>) -> Vec<Node> {
    node(NodeKind::Binary { op }, &[lexpr, rexpr])
}

fn and(lexpr: Vec<Node>, rexpr: Vec<Node>) -> Vec<Node> {
    cmp(BinaryOp::And, lexpr, rexpr)
}

fn or(lexpr: Vec<Node>, rexpr: Vec<Node>) -> Vec<Node> {
    cmp(BinaryOp::Or, lexpr, rexpr)
}

fn unary(op: UnaryOp, expr: Vec<Node>) -> Vec<Node> {
    node(NodeKind::Unary { op }, &[expr])
}

fn not(expr: Vec<Node>) -> Vec<Node> {
    unary(UnaryOp::Not, expr)
}

fn multi(op: MultiOp, exprs: Vec<Vec<Node>>) -> Vec<Node> {
    let count = u32::try_from(exprs.len()).expect("operand count fits");
    node(NodeKind::Multi { op, count }, &exprs)
}

fn array(elements: Vec<Vec<Node>>) -> Vec<Node> {
    let count = u32::try_from(elements.len()).expect("element count fits");
    node(NodeKind::Array { count }, &elements)
}

fn arith(op: ArithmeticOp, left: Vec<Node>, right: Vec<Node>) -> Vec<Node> {
    node(NodeKind::Arithmetic { op }, &[left, right])
}

/// Each case's WHERE clause must parse to exactly the expected fragment.
fn where_cases_check(cases: Vec<(&str, &str, Option<Vec<Node>>)>) {
    for (label, sql, expected) in cases {
        let actual = where_clause_parse(sql).unwrap_or_else(|e| panic!("{label}: {sql}: {e:?}"));
        assert_eq!(actual, expected, "{label}: {sql}");
    }
}

fn session_hash(tree: &QueryTree) -> u64 {
    let mut hasher = DefaultHasher::new();
    tree.session_hash(&mut hasher);
    hasher.finish()
}

#[test]
fn test_session_hash_differs_by_literal() {
    let q1 = parse("select id, str from test where str = 'hello'");
    let q2 = parse("select id, str from test where str = 'bye'");
    assert_ne!(session_hash(&q1), session_hash(&q2));
}

#[test]
fn test_select_columns() {
    let describe = |sql: &str| -> Vec<String> {
        let tree = parse(sql);
        let Body::Select(select) = tree.root().body() else {
            panic!("expected a SELECT");
        };
        select
            .items()
            .expect("explicit items")
            .map(|item| match item {
                SelectItem::Expr(expr) => match expr.expr().kind() {
                    ScalarKind::Column(column) => format!("column {}", column.column()),
                    ScalarKind::Function(function) => format!("function {}", function.name()),
                    _ => "other".to_owned(),
                },
                SelectItem::Star(_) => "star".to_owned(),
            })
            .collect()
    };
    assert_eq!(
        describe("select id, str from test where str = 'hello'"),
        ["column id", "column str"]
    );
    assert_eq!(
        describe("select count(id), str from test where str = 'hihi'"),
        ["function count", "column str"]
    );
}

// ------------------------------------------------------------------
// Arithmetic in WHERE (PGC-118 layer 1)
//
// Layer 2 (the constant fold) runs at the end of conversion, so
// pure-literal arithmetic is folded away before these assertions see
// the tree. Each test uses at least one non-literal operand (column or
// parameter) to keep the arithmetic node observable.
// ------------------------------------------------------------------

/// The RHS of `WHERE col = <rhs>` as a fragment.
fn where_clause_rhs(sql: &str) -> Vec<Node> {
    let tree = parse(sql);
    let WhereKind::Binary(binary) = where_of(&tree).expect("a WHERE").kind() else {
        panic!("expected a binary WHERE");
    };
    fragment(binary.rexpr())
}

#[test]
fn test_where_clause_arithmetic_render() {
    // Each arithmetic level is parenthesized, so nested `a % 10000 + 1`
    // renders as `((a % 10000) + 1)`.
    assert_eq!(
        parse("SELECT * FROM t WHERE x = a % 10000 + 1").sql(),
        "SELECT * FROM t WHERE x = ((a % 10000) + 1)"
    );
}

#[test]
fn test_where_clause_typecast_column() {
    // PGC-120: a column cast on the left of a comparison is a scalar cast,
    // not an unsupported pattern.
    let tree = parse("SELECT * FROM t WHERE col::text = 'foo'");
    let WhereKind::Binary(binary) = where_of(&tree).expect("a WHERE").kind() else {
        panic!("expected a comparison");
    };
    let WhereKind::Scalar(scalar) = binary.lexpr().kind() else {
        panic!("expected a scalar on the left");
    };
    let ScalarKind::TypeCast(cast) = scalar.kind() else {
        panic!("expected a cast on the left");
    };
    assert_eq!(*cast.target(), CastTarget::Text);
    assert!(matches!(cast.expr().kind(), ScalarKind::Column(c) if c.column() == "col"));
}

#[test]
fn test_where_clause_typecast_round_trips() {
    // PGC-120: common cast shapes parse → render → parse to the same tree.
    for sql in [
        "SELECT * FROM t WHERE col::text = 'foo'",
        "SELECT * FROM t WHERE created_at::date = '2024-01-01'",
        "SELECT * FROM t WHERE (a + b)::int > 10",
    ] {
        let tree = parse(sql);
        let rendered = tree.sql();
        assert_eq!(
            parse(&rendered),
            tree,
            "round trip of {sql} through {rendered}"
        );
    }
}

#[test]
fn test_where_comparison_cases() {
    #[rustfmt::skip]
    let cases: Vec<(&str, &str, Option<Vec<Node>>)> = vec![
        ("=, text", "SELECT id, str FROM test WHERE str = 'hello'", Some(cmp(BinaryOp::Equal, col("str"), text("hello")))),
        ("=, integer", "SELECT id FROM test WHERE id = 123", Some(cmp(BinaryOp::Equal, col("id"), int(123)))),
        ("=, boolean", "SELECT id FROM test WHERE active = true", Some(cmp(BinaryOp::Equal, col("active"), boolean(true)))),
        (">", "SELECT id FROM test WHERE cnt > 0", Some(cmp(BinaryOp::GreaterThan, col("cnt"), int(0)))),
        ("!=", "SELECT id FROM test WHERE id != 123", Some(cmp(BinaryOp::NotEqual, col("id"), int(123)))),
        ("<>", "SELECT id FROM test WHERE id <> 123", Some(cmp(BinaryOp::NotEqual, col("id"), int(123)))),
        ("<", "SELECT id FROM test WHERE id < 123", Some(cmp(BinaryOp::LessThan, col("id"), int(123)))),
        ("<=", "SELECT id FROM test WHERE id <= 123", Some(cmp(BinaryOp::LessThanOrEqual, col("id"), int(123)))),
        (">=", "SELECT id FROM test WHERE id >= 123", Some(cmp(BinaryOp::GreaterThanOrEqual, col("id"), int(123)))),
        ("qualified column", "SELECT id FROM test WHERE test.str = 'hello'", Some(cmp(BinaryOp::Equal, qcol("test", "str"), text("hello")))),
        ("= NULL", "SELECT id FROM test WHERE data = NULL", Some(cmp(BinaryOp::Equal, col("data"), null()))),
        ("no WHERE", "SELECT id FROM test", None),
    ];
    where_cases_check(cases);
}

#[test]
fn test_where_logical_cases() {
    // AND and OR chains associate to the left.
    #[rustfmt::skip]
    let cases: Vec<(&str, &str, Option<Vec<Node>>)> = vec![
        ("AND", "SELECT id FROM test WHERE str = 'hello' AND id = 123", Some(and(cmp(BinaryOp::Equal, col("str"), text("hello")), cmp(BinaryOp::Equal, col("id"), int(123))))),
        ("OR", "SELECT id FROM test WHERE str = 'hello' OR str = 'world'", Some(or(cmp(BinaryOp::Equal, col("str"), text("hello")), cmp(BinaryOp::Equal, col("str"), text("world"))))),
        ("NOT", "SELECT id FROM test WHERE NOT str = 'hello'", Some(not(cmp(BinaryOp::Equal, col("str"), text("hello"))))),
        ("chained AND (left-assoc)", "SELECT id FROM test WHERE name = 'john' AND age > 25 AND active = true", Some(and(and(cmp(BinaryOp::Equal, col("name"), text("john")), cmp(BinaryOp::GreaterThan, col("age"), int(25))), cmp(BinaryOp::Equal, col("active"), boolean(true))))),
        ("chained OR (left-assoc)", "SELECT id FROM test WHERE name = 'john' OR name = 'jane' OR name = 'bob'", Some(or(or(cmp(BinaryOp::Equal, col("name"), text("john")), cmp(BinaryOp::Equal, col("name"), text("jane"))), cmp(BinaryOp::Equal, col("name"), text("bob"))))),
    ];
    where_cases_check(cases);
}

#[test]
fn test_where_parameter_cases() {
    #[rustfmt::skip]
    let cases: Vec<(&str, &str, Option<Vec<Node>>)> = vec![
        ("single", "SELECT id FROM test WHERE id = $1", Some(cmp(BinaryOp::Equal, col("id"), param("$1")))),
        ("multiple", "SELECT id FROM test WHERE name = $1 AND age > $2", Some(and(cmp(BinaryOp::Equal, col("name"), param("$1")), cmp(BinaryOp::GreaterThan, col("age"), param("$2"))))),
        ("mixed with literals", "SELECT id FROM test WHERE name = $1 AND active = true", Some(and(cmp(BinaryOp::Equal, col("name"), param("$1")), cmp(BinaryOp::Equal, col("active"), boolean(true))))),
    ];
    where_cases_check(cases);
}

#[test]
fn test_where_in_list_cases() {
    // The tested column first, then the list values.
    #[rustfmt::skip]
    let cases: Vec<(&str, &str, Option<Vec<Node>>)> = vec![
        ("IN strings",       "SELECT * FROM t WHERE status IN ('active', 'pending', 'complete')",
                             Some(multi(MultiOp::In, vec![col("status"), text("active"), text("pending"), text("complete")]))),
        ("NOT IN",           "SELECT * FROM t WHERE id NOT IN (1, 2, 3)",
                             Some(multi(MultiOp::NotIn, vec![col("id"), int(1), int(2), int(3)]))),
        ("IN integers",      "SELECT * FROM t WHERE id IN (1, 2, 3)",
                             Some(multi(MultiOp::In, vec![col("id"), int(1), int(2), int(3)]))),
        ("IN under AND",     "SELECT * FROM t WHERE tenant_id = 1 AND status IN ('active', 'pending')",
                             Some(and(cmp(BinaryOp::Equal, col("tenant_id"), int(1)), multi(MultiOp::In, vec![col("status"), text("active"), text("pending")])))),
    ];
    where_cases_check(cases);
}

#[test]
fn test_where_is_test_cases() {
    #[rustfmt::skip]
    let cases: Vec<(&str, &str, Option<Vec<Node>>)> = vec![
        ("IS NULL",                  "SELECT id FROM test WHERE deleted_at IS NULL",  Some(unary(UnaryOp::IsNull, col("deleted_at")))),
        ("IS NOT NULL",              "SELECT id FROM test WHERE name IS NOT NULL",    Some(unary(UnaryOp::IsNotNull, col("name")))),
        ("IS TRUE",                  "SELECT id FROM test WHERE active IS TRUE",      Some(unary(UnaryOp::IsTrue, col("active")))),
        ("IS FALSE",                 "SELECT id FROM test WHERE active IS FALSE",     Some(unary(UnaryOp::IsFalse, col("active")))),
        ("IS NOT TRUE",              "SELECT id FROM test WHERE active IS NOT TRUE",  Some(unary(UnaryOp::IsNotTrue, col("active")))),
        ("IS NOT FALSE",             "SELECT id FROM test WHERE active IS NOT FALSE", Some(unary(UnaryOp::IsNotFalse, col("active")))),
        ("IS UNKNOWN is IS NULL",    "SELECT id FROM test WHERE active IS UNKNOWN",     Some(unary(UnaryOp::IsNull, col("active")))),
        ("IS NOT UNKNOWN is IS NOT NULL", "SELECT id FROM test WHERE active IS NOT UNKNOWN", Some(unary(UnaryOp::IsNotNull, col("active")))),
        ("IS TRUE under AND",        "SELECT * FROM t WHERE id = 1 AND active IS TRUE",
                                     Some(and(cmp(BinaryOp::Equal, col("id"), int(1)), unary(UnaryOp::IsTrue, col("active"))))),
        ("IS NULL under AND",        "SELECT * FROM t WHERE id = 1 AND deleted_at IS NULL",
                                     Some(and(cmp(BinaryOp::Equal, col("id"), int(1)), unary(UnaryOp::IsNull, col("deleted_at"))))),
    ];
    where_cases_check(cases);
}

#[test]
fn test_where_between_cases() {
    // The tested column, then the low and high bounds.
    #[rustfmt::skip]
    let cases: Vec<(&str, &str, Option<Vec<Node>>)> = vec![
        ("integers",              "SELECT * FROM t WHERE id BETWEEN 1 AND 10",
                                  Some(multi(MultiOp::Between, vec![col("id"), int(1), int(10)]))),
        ("NOT BETWEEN",           "SELECT * FROM t WHERE price NOT BETWEEN 100 AND 200",
                                  Some(multi(MultiOp::NotBetween, vec![col("price"), int(100), int(200)]))),
        ("parameters",            "SELECT * FROM t WHERE created_at BETWEEN $1 AND $2",
                                  Some(multi(MultiOp::Between, vec![col("created_at"), param("$1"), param("$2")]))),
        ("under AND",             "SELECT * FROM t WHERE tenant_id = 1 AND price BETWEEN 10 AND 50",
                                  Some(and(cmp(BinaryOp::Equal, col("tenant_id"), int(1)), multi(MultiOp::Between, vec![col("price"), int(10), int(50)])))),
        ("strings",               "SELECT * FROM t WHERE name BETWEEN 'alice' AND 'charlie'",
                                  Some(multi(MultiOp::Between, vec![col("name"), text("alice"), text("charlie")]))),
        // SYMMETRIC keeps the bounds in written order; evaluation swaps them.
        ("SYMMETRIC",             "SELECT * FROM t WHERE id BETWEEN SYMMETRIC 10 AND 1",
                                  Some(multi(MultiOp::BetweenSymmetric, vec![col("id"), int(10), int(1)]))),
        ("NOT BETWEEN SYMMETRIC", "SELECT * FROM t WHERE id NOT BETWEEN SYMMETRIC 10 AND 1",
                                  Some(multi(MultiOp::NotBetweenSymmetric, vec![col("id"), int(10), int(1)]))),
    ];
    where_cases_check(cases);
}

#[test]
fn test_where_any_all_cases() {
    // The tested column, then the array (or array parameter).
    #[rustfmt::skip]
    let cases: Vec<(&str, &str, Option<Vec<Node>>)> = vec![
        ("= ANY(ARRAY)",   "SELECT * FROM t WHERE id = ANY(ARRAY[1, 2, 3])",
                           Some(multi(MultiOp::Any { comparison: BinaryOp::Equal }, vec![col("id"), array(vec![int(1), int(2), int(3)])]))),
        ("= ANY($1)",      "SELECT * FROM t WHERE id = ANY($1)",
                           Some(multi(MultiOp::Any { comparison: BinaryOp::Equal }, vec![col("id"), param("$1")]))),
        ("> ALL(ARRAY)",   "SELECT * FROM t WHERE score > ALL(ARRAY[80, 90])",
                           Some(multi(MultiOp::All { comparison: BinaryOp::GreaterThan }, vec![col("score"), array(vec![int(80), int(90)])]))),
        ("<> ANY(ARRAY)",  "SELECT * FROM t WHERE status <> ANY(ARRAY['a', 'b'])",
                           Some(multi(MultiOp::Any { comparison: BinaryOp::NotEqual }, vec![col("status"), array(vec![text("a"), text("b")])]))),
        ("ANY under AND",  "SELECT * FROM t WHERE tenant_id = 1 AND id = ANY(ARRAY[10, 20])",
                           Some(and(cmp(BinaryOp::Equal, col("tenant_id"), int(1)), multi(MultiOp::Any { comparison: BinaryOp::Equal }, vec![col("id"), array(vec![int(10), int(20)])])))),
    ];
    where_cases_check(cases);
}

#[test]
fn test_where_like_cases() {
    #[rustfmt::skip]
    let cases: Vec<(&str, &str, Option<Vec<Node>>)> = vec![
        ("LIKE",            "SELECT id FROM test WHERE name LIKE 'test%'",    Some(cmp(BinaryOp::Like, col("name"), text("test%")))),
        ("NOT LIKE",        "SELECT * FROM t WHERE name NOT LIKE '%test%'",   Some(cmp(BinaryOp::NotLike, col("name"), text("%test%")))),
        ("ILIKE",           "SELECT * FROM t WHERE name ILIKE '%test%'",      Some(cmp(BinaryOp::ILike, col("name"), text("%test%")))),
        ("NOT ILIKE",       "SELECT * FROM t WHERE name NOT ILIKE '%test%'",  Some(cmp(BinaryOp::NotILike, col("name"), text("%test%")))),
        ("LIKE $1",         "SELECT * FROM t WHERE name LIKE $1",             Some(cmp(BinaryOp::Like, col("name"), param("$1")))),
        ("LIKE under AND",  "SELECT * FROM t WHERE tenant_id = 1 AND name LIKE 'test%'",
                            Some(and(cmp(BinaryOp::Equal, col("tenant_id"), int(1)), cmp(BinaryOp::Like, col("name"), text("test%"))))),
    ];
    where_cases_check(cases);
}

#[test]
fn test_where_arithmetic_rhs_cases() {
    #[rustfmt::skip]
    let cases: Vec<(&str, &str, Vec<Node>)> = vec![
        ("column + literal",     "SELECT * FROM t WHERE x = a + 1",              arith(ArithmeticOp::Add, col("a"), int(1))),
        ("-",                    "SELECT * FROM t WHERE x = a - 2",              arith(ArithmeticOp::Subtract, col("a"), int(2))),
        ("*",                    "SELECT * FROM t WHERE x = a * 2",              arith(ArithmeticOp::Multiply, col("a"), int(2))),
        ("/",                    "SELECT * FROM t WHERE x = a / 2",              arith(ArithmeticOp::Divide, col("a"), int(2))),
        ("%",                    "SELECT * FROM t WHERE x = a % 3",              arith(ArithmeticOp::Modulo, col("a"), int(3))),
        ("parameter % literal",  "SELECT * FROM t WHERE x = $1 % 10",            arith(ArithmeticOp::Modulo, param("$1"), int(10))),
        // `%` binds tighter than `+`.
        ("nested with parameter", "SELECT * FROM t WHERE user_id = $1 % 10000 + 1",
                                  arith(ArithmeticOp::Add, arith(ArithmeticOp::Modulo, param("$1"), int(10000)), int(1))),
        ("two columns",          "SELECT * FROM t WHERE x = a + b",              arith(ArithmeticOp::Add, col("a"), col("b"))),
    ];
    for (label, sql, expected) in cases {
        assert_eq!(where_clause_rhs(sql), expected, "{label}: {sql}");
    }
}
