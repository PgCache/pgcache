//! Functions, windows, arithmetic, casts, CASE and HAVING aggregates.

#![allow(clippy::wildcard_enum_match_arm, clippy::type_complexity)]

use super::*;

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
    // (sql, function, agg_star, agg_distinct, FILTER, aggregate ORDER BY count, rendered fragments)
    #[rustfmt::skip]
    let cases: [(&str, &str, &str, bool, bool, bool, usize, &[&str]); 4] = [
        ("DISTINCT",           "SELECT name FROM users GROUP BY name HAVING COUNT(DISTINCT id) > 1",
         "count", false, true, false, 0, &["COUNT(DISTINCT "]),
        ("FILTER",             "SELECT name FROM users GROUP BY name HAVING COUNT(*) FILTER (WHERE id > 0) > 5",
         "count", true, false, true, 0, &["FILTER (WHERE "]),
        ("aggregate ORDER BY", "SELECT id FROM users GROUP BY id HAVING string_agg(name, ',' ORDER BY name) <> ''",
         "string_agg", false, false, false, 1, &["ORDER BY"]),
        ("DISTINCT + FILTER",  "SELECT name FROM users GROUP BY name HAVING COUNT(DISTINCT id) FILTER (WHERE id > 0) > 1",
         "count", false, true, true, 0, &["COUNT(DISTINCT ", "FILTER (WHERE "]),
    ];
    for (label, sql, name, star, distinct, filter, order_count, fragments) in cases {
        let tree = parse(sql);
        let function = having_lhs_function(&tree);
        let actual = (
            function.name().as_str(),
            function.agg_star(),
            function.agg_distinct(),
            function.filter().is_some(),
            function.agg_order().len(),
        );
        assert_eq!(
            actual,
            (name, star, distinct, filter, order_count),
            "{label}: {sql}"
        );
        let rendered = tree.sql();
        for fragment in fragments {
            assert!(
                rendered.contains(fragment),
                "{label}: {fragment:?} in {rendered}"
            );
        }
    }
    // Parse → render → parse → render converges.
    let first =
        parse("SELECT name FROM users GROUP BY name HAVING COUNT(*) FILTER (WHERE id > 0) > 5")
            .sql();
    assert_eq!(parse(&first).sql(), first);
}
