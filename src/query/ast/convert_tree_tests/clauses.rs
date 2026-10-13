//! ORDER BY, GROUP BY, HAVING and LIMIT.

#![allow(clippy::wildcard_enum_match_arm, clippy::type_complexity)]

use super::*;

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
    // (sql, GROUP BY columns as (qualifier, column), HAVING present)
    #[rustfmt::skip]
    let cases = [
        ("one column",         "SELECT status FROM orders GROUP BY status",                                     vec![(None, "status")],                        false),
        ("two columns",        "SELECT status, category FROM orders GROUP BY status, category",                 vec![(None, "status"), (None, "category")],    false),
        ("qualified",          "SELECT o.status FROM orders o GROUP BY o.status",                               vec![(Some("o"), "status")],                   false),
        ("HAVING",             "SELECT status FROM orders GROUP BY status HAVING status = 'active'",            vec![(None, "status")],                        true),
        ("HAVING with AND",    "SELECT category FROM sales GROUP BY category HAVING category = 'electronics' AND category != 'toys'", vec![(None, "category")], true),
        ("none",               "SELECT * FROM users WHERE id = 1",                                              vec![],                                        false),
    ];
    for (label, sql, group_by, has_having) in cases {
        let tree = parse(sql);
        let select = select_of(&tree);
        let actual: Vec<(Option<&str>, &str)> = select
            .group_by()
            .map(|c| (c.table().map(EcoString::as_str), c.column().as_str()))
            .collect();
        assert_eq!(
            (actual, select.having().is_some()),
            (group_by, has_having),
            "{label}: {sql}"
        );
    }
    let tree = parse(
        "SELECT status FROM orders GROUP BY status HAVING status != 'cancelled' ORDER BY status DESC LIMIT 10",
    );
    assert_eq!(
        (
            select_of(&tree).group_by().len(),
            select_of(&tree).having().is_some(),
            tree.root().has_limit(),
            tree.root().order_by().len()
        ),
        (1, true, true, 1)
    );
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
