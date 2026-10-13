//! Rendering: value types, identifiers, round trips, parentheses and
//! what the converter rejects.

#![allow(clippy::wildcard_enum_match_arm, clippy::type_complexity)]

use super::*;

// ============================================================================
// Rendering
// ============================================================================

#[test]
fn test_literal_value_deparse_cases() {
    #[rustfmt::skip]
    let cases = [
        (LiteralValue::String("hello".into()),                             "'hello'"),
        (LiteralValue::Integer(42),                                        "42"),
        (LiteralValue::Float(NotNan::new(3.25).expect("not NaN")),         "3.25"),
        (LiteralValue::Boolean(true),                                      "true"),
        (LiteralValue::Boolean(false),                                     "false"),
        (LiteralValue::Null,                                               "NULL"),
        (LiteralValue::Parameter("$1".into()),                             "$1"),
        (LiteralValue::String("".into()),                                  "''"),
        (LiteralValue::String("test'quote".into()),                        "'test''quote'"),
        (LiteralValue::String("test\\path".into()),                        "E'test\\\\path'"),
    ];
    for (value, expected) in cases {
        let mut buf = String::new();
        value.deparse(&mut buf);
        assert_eq!(buf, expected, "{value:?}");
    }
}

#[test]
fn test_operator_deparse_cases() {
    #[rustfmt::skip]
    let cases: [(&dyn Deparse, &str); 17] = [
        (&UnaryOp::Not, "NOT"),
        (&UnaryOp::IsNull, "IS NULL"),
        (&UnaryOp::IsNotNull, "IS NOT NULL"),
        (&UnaryOp::IsTrue, "IS TRUE"),
        (&UnaryOp::IsNotTrue, "IS NOT TRUE"),
        (&UnaryOp::IsFalse, "IS FALSE"),
        (&UnaryOp::IsNotFalse, "IS NOT FALSE"),
        (&BinaryOp::Equal, "="),
        (&BinaryOp::NotEqual, "!="),
        (&BinaryOp::LessThan, "<"),
        (&BinaryOp::GreaterThanOrEqual, ">="),
        (&BinaryOp::Like, "LIKE"),
        (&BinaryOp::NotLike, "NOT LIKE"),
        (&BinaryOp::And, "AND"),
        (&BinaryOp::Or, "OR"),
        (&MultiOp::In, "IN"),
        (&MultiOp::NotIn, "NOT IN"),
    ];
    for (op, expected) in cases {
        let mut buf = String::new();
        op.deparse(&mut buf);
        assert_eq!(buf, expected);
    }
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
