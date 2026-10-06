#![allow(clippy::wildcard_enum_match_arm)]

use ecow::EcoString;

use crate::query::ast::*;

/// Parse SQL and extract the WHERE clause via the AST layer.
fn where_clause_parse(sql: &str) -> Result<Option<WhereExpr>, AstError> {
    let query_expr = query_expr_parse(sql)?;
    Ok(query_expr.where_clause().cloned())
}

fn col(name: &str) -> WhereExpr {
    WhereExpr::Scalar(s_col(name))
}

fn qcol(table: &str, name: &str) -> WhereExpr {
    WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
        table: Some(EcoString::from(table)),
        column: EcoString::from(name),
    }))
}

fn lit(value: LiteralValue) -> WhereExpr {
    WhereExpr::Scalar(ScalarExpr::Literal(value))
}

fn text(value: &str) -> WhereExpr {
    lit(LiteralValue::String(value.into()))
}

fn int(value: i64) -> WhereExpr {
    lit(LiteralValue::Integer(value))
}

fn param(name: &str) -> WhereExpr {
    lit(LiteralValue::Parameter(name.into()))
}

fn boolean(value: bool) -> WhereExpr {
    lit(LiteralValue::Boolean(value))
}

fn null() -> WhereExpr {
    lit(LiteralValue::Null)
}

fn cmp(op: BinaryOp, lexpr: WhereExpr, rexpr: WhereExpr) -> WhereExpr {
    WhereExpr::Binary(BinaryExpr {
        op,
        lexpr: Box::new(lexpr),
        rexpr: Box::new(rexpr),
    })
}

fn and(lexpr: WhereExpr, rexpr: WhereExpr) -> WhereExpr {
    cmp(BinaryOp::And, lexpr, rexpr)
}

fn or(lexpr: WhereExpr, rexpr: WhereExpr) -> WhereExpr {
    cmp(BinaryOp::Or, lexpr, rexpr)
}

fn unary(op: UnaryOp, expr: WhereExpr) -> WhereExpr {
    WhereExpr::Unary(UnaryExpr {
        op,
        expr: Box::new(expr),
    })
}

fn not(expr: WhereExpr) -> WhereExpr {
    unary(UnaryOp::Not, expr)
}

fn multi(op: MultiOp, exprs: Vec<WhereExpr>) -> WhereExpr {
    WhereExpr::Multi(MultiExpr { op, exprs })
}

fn array(elements: Vec<ScalarExpr>) -> WhereExpr {
    WhereExpr::Scalar(ScalarExpr::Array(elements))
}

fn s_col(name: &str) -> ScalarExpr {
    ScalarExpr::Column(ColumnNode {
        table: None,
        column: EcoString::from(name),
    })
}

fn s_int(value: i64) -> ScalarExpr {
    ScalarExpr::Literal(LiteralValue::Integer(value))
}

fn s_text(value: &str) -> ScalarExpr {
    ScalarExpr::Literal(LiteralValue::String(value.into()))
}

fn s_param(name: &str) -> ScalarExpr {
    ScalarExpr::Literal(LiteralValue::Parameter(name.into()))
}

fn arith(op: ArithmeticOp, left: ScalarExpr, right: ScalarExpr) -> ScalarExpr {
    ScalarExpr::Arithmetic(ArithmeticExpr {
        left: Box::new(left),
        op,
        right: Box::new(right),
    })
}

/// Each case's WHERE clause must parse to exactly the expected tree.
fn where_cases_check(cases: Vec<(&str, &str, Option<WhereExpr>)>) {
    for (label, sql, expected) in cases {
        let actual = where_clause_parse(sql).unwrap_or_else(|e| panic!("{label}: {sql}: {e:?}"));
        assert_eq!(actual, expected, "{label}: {sql}");
    }
}

#[test]
fn fingerprint_literals_differ() {
    let q1 = query_expr_parse("select id, str from test where str = 'hello'").unwrap();
    let q2 = query_expr_parse("select id, str from test where str = 'bye'").unwrap();

    assert_ne!(query_expr_fingerprint(&q1), query_expr_fingerprint(&q2));
}

#[test]
fn select_columns() {
    let q = query_expr_parse("select id, str from test where str = 'hello'").unwrap();
    let select = q.as_select().unwrap();
    let SelectColumns::Columns(cols) = &select.columns else {
        panic!("expected explicit columns");
    };
    assert_eq!(cols.len(), 2);
    assert!(matches!(&cols[0].expr().unwrap(), ScalarExpr::Column(c) if c.column == "id"));
    assert!(matches!(&cols[1].expr().unwrap(), ScalarExpr::Column(c) if c.column == "str"));

    let q = query_expr_parse("select count(id), str from test where str = 'hihi'").unwrap();
    let select = q.as_select().unwrap();
    let SelectColumns::Columns(cols) = &select.columns else {
        panic!("expected explicit columns");
    };
    assert_eq!(cols.len(), 2);
    assert!(matches!(&cols[0].expr().unwrap(), ScalarExpr::Function(f) if f.name == "count"));
    assert!(matches!(&cols[1].expr().unwrap(), ScalarExpr::Column(c) if c.column == "str"));
}

// ------------------------------------------------------------------
// Arithmetic in WHERE (PGC-118 layer 1)
//
// Layer 2 (constant_fold) runs at the end of `query_expr_convert_raw`, so
// pure-literal arithmetic is folded away before these assertions see
// the tree. Each test uses at least one non-literal operand (column or
// parameter) to keep the arithmetic node observable.
// ------------------------------------------------------------------

/// Pull the RHS of `WHERE col = <rhs>` so each arithmetic test can
/// assert on the scalar shape directly.
fn where_clause_rhs(sql: &str) -> ScalarExpr {
    let WhereExpr::Binary(binary) = where_clause_parse(sql).unwrap().unwrap() else {
        panic!("expected binary WHERE");
    };
    let WhereExpr::Scalar(scalar) = *binary.rexpr else {
        panic!("expected scalar RHS");
    };
    scalar
}

#[test]
fn where_clause_arithmetic_deparse() {
    // Round-trip a non-foldable arithmetic query to confirm Scalar wrapping deparses cleanly.
    let q = query_expr_parse("SELECT * FROM t WHERE x = a % 10000 + 1").unwrap();
    let mut buf = String::new();
    q.deparse(&mut buf);
    // ArithmeticExpr::deparse parenthesizes each level, so nested
    // `a % 10000 + 1` round-trips as `((a % 10000) + 1)`.
    assert_eq!(buf, "SELECT * FROM t WHERE x = ((a % 10000) + 1)");
}

#[test]
fn where_clause_typecast_column() {
    // PGC-120: column cast on the left of a comparison. Must parse as
    // WhereExpr::Scalar(ScalarExpr::TypeCast{...}), not UnsupportedPattern.
    let where_clause = where_clause_parse("SELECT * FROM t WHERE col::text = 'foo'")
        .unwrap()
        .unwrap();
    let WhereExpr::Binary(binary) = &where_clause else {
        panic!("expected Binary, got {where_clause:?}");
    };
    let WhereExpr::Scalar(ScalarExpr::TypeCast { expr, target }) = &*binary.lexpr else {
        panic!("expected Scalar(TypeCast), got {:?}", binary.lexpr);
    };
    assert_eq!(*target, crate::query::cast::CastTarget::Text);
    assert!(matches!(&**expr, ScalarExpr::Column(c) if c.column == "col"));
}

#[test]
fn where_clause_typecast_deparse() {
    // PGC-120: round-trip a few common cast shapes through Deparse.
    for sql in [
        "SELECT * FROM t WHERE col::text = 'foo'",
        "SELECT * FROM t WHERE created_at::date = '2024-01-01'",
        "SELECT * FROM t WHERE (a + b)::int > 10",
    ] {
        let q = query_expr_parse(sql).unwrap_or_else(|e| panic!("convert failed for {sql}: {e}"));
        let mut buf = String::new();
        q.deparse(&mut buf);
        // Re-parse the deparsed SQL to confirm semantic round-trip.
        let q2 =
            query_expr_parse(&buf).unwrap_or_else(|e| panic!("re-convert failed for {buf:?}: {e}"));
        assert_eq!(
            query_expr_fingerprint(&q),
            query_expr_fingerprint(&q2),
            "fingerprint mismatch after deparse round-trip\n  in:  {sql}\n  out: {buf}",
        );
    }
}

#[test]
fn test_where_comparison_cases() {
    #[rustfmt::skip]
    let cases: Vec<(&str, &str, Option<WhereExpr>)> = vec![
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
    let cases: Vec<(&str, &str, Option<WhereExpr>)> = vec![
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
    let cases: Vec<(&str, &str, Option<WhereExpr>)> = vec![
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
    let cases: Vec<(&str, &str, Option<WhereExpr>)> = vec![
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
    let cases: Vec<(&str, &str, Option<WhereExpr>)> = vec![
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
    let cases: Vec<(&str, &str, Option<WhereExpr>)> = vec![
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
    let cases: Vec<(&str, &str, Option<WhereExpr>)> = vec![
        ("= ANY(ARRAY)",   "SELECT * FROM t WHERE id = ANY(ARRAY[1, 2, 3])",
                           Some(multi(MultiOp::Any { comparison: BinaryOp::Equal }, vec![col("id"), array(vec![s_int(1), s_int(2), s_int(3)])]))),
        ("= ANY($1)",      "SELECT * FROM t WHERE id = ANY($1)",
                           Some(multi(MultiOp::Any { comparison: BinaryOp::Equal }, vec![col("id"), param("$1")]))),
        ("> ALL(ARRAY)",   "SELECT * FROM t WHERE score > ALL(ARRAY[80, 90])",
                           Some(multi(MultiOp::All { comparison: BinaryOp::GreaterThan }, vec![col("score"), array(vec![s_int(80), s_int(90)])]))),
        ("<> ANY(ARRAY)",  "SELECT * FROM t WHERE status <> ANY(ARRAY['a', 'b'])",
                           Some(multi(MultiOp::Any { comparison: BinaryOp::NotEqual }, vec![col("status"), array(vec![s_text("a"), s_text("b")])]))),
        ("ANY under AND",  "SELECT * FROM t WHERE tenant_id = 1 AND id = ANY(ARRAY[10, 20])",
                           Some(and(cmp(BinaryOp::Equal, col("tenant_id"), int(1)), multi(MultiOp::Any { comparison: BinaryOp::Equal }, vec![col("id"), array(vec![s_int(10), s_int(20)])])))),
    ];
    where_cases_check(cases);
}

#[test]
fn test_where_like_cases() {
    #[rustfmt::skip]
    let cases: Vec<(&str, &str, Option<WhereExpr>)> = vec![
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
    let cases: Vec<(&str, &str, ScalarExpr)> = vec![
        ("column + literal",     "SELECT * FROM t WHERE x = a + 1",              arith(ArithmeticOp::Add, s_col("a"), s_int(1))),
        ("-",                    "SELECT * FROM t WHERE x = a - 2",              arith(ArithmeticOp::Subtract, s_col("a"), s_int(2))),
        ("*",                    "SELECT * FROM t WHERE x = a * 2",              arith(ArithmeticOp::Multiply, s_col("a"), s_int(2))),
        ("/",                    "SELECT * FROM t WHERE x = a / 2",              arith(ArithmeticOp::Divide, s_col("a"), s_int(2))),
        ("%",                    "SELECT * FROM t WHERE x = a % 3",              arith(ArithmeticOp::Modulo, s_col("a"), s_int(3))),
        ("parameter % literal",  "SELECT * FROM t WHERE x = $1 % 10",            arith(ArithmeticOp::Modulo, s_param("$1"), s_int(10))),
        // `%` binds tighter than `+`.
        ("nested with parameter", "SELECT * FROM t WHERE user_id = $1 % 10000 + 1",
                                  arith(ArithmeticOp::Add, arith(ArithmeticOp::Modulo, s_param("$1"), s_int(10000)), s_int(1))),
        ("two columns",          "SELECT * FROM t WHERE x = a + b",              arith(ArithmeticOp::Add, s_col("a"), s_col("b"))),
    ];
    for (label, sql, expected) in cases {
        assert_eq!(where_clause_rhs(sql), expected, "{label}: {sql}");
    }
}
