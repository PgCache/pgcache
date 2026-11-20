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

/// Every LIMIT and OFFSET form converts (PGC-640, PGC-643): the ones
/// PostgreSQL reads as a plain bigint become that literal or drop out, and
/// the rest stay as written for the cacheability check to refuse.
#[test]
fn test_limit_forms_reduce_to_a_literal_or_stay_as_written() {
    #[rustfmt::skip]
    let cases = [
        ("all",                  "LIMIT ALL",                              ""),
        ("null",                 "LIMIT NULL OFFSET NULL",                 ""),
        ("null cast",            "LIMIT NULL::int",                        ""),
        ("untyped string",       "LIMIT '2' OFFSET ' 7 '",                 " LIMIT 2 OFFSET 7"),
        ("wide integer",         "LIMIT 3000000000",                       " LIMIT 3000000000"),
        ("integer cast",         "LIMIT 10::int OFFSET '5'::bigint",       " LIMIT 10 OFFSET 5"),
        ("smallint cast",        "LIMIT 10::smallint",                     " LIMIT 10"),
        ("placeholder bigint",   "LIMIT $1::bigint OFFSET $2",             " LIMIT $1 OFFSET $2"),
        ("fetch first only",     "FETCH FIRST 2 ROWS ONLY",                " LIMIT 2"),
        ("ties of no limit",     "FETCH FIRST (NULL) ROWS WITH TIES",      ""),
        ("constant arithmetic",  "LIMIT 1 + 1 OFFSET 2 * 5",               " LIMIT 2 OFFSET 10"),
        ("placeholder int",      "LIMIT $1::int",                          " LIMIT ($1)::int4"),
        ("cast out of range",    "LIMIT 3000000000::int",                  " LIMIT (3000000000)::int4"),
        ("numeric",              "LIMIT 2.5",                              " LIMIT 2.5"),
        ("word",                 "LIMIT 'ten'",                            " LIMIT 'ten'"),
        ("arithmetic",           "LIMIT $1 + 1 OFFSET ($1 - 1) * $2",      " LIMIT ($1 + 1) OFFSET (($1 - 1) * $2)"),
        ("function",             "LIMIT f(1)",                             " LIMIT F(1)"),
        ("with ties",            "FETCH FIRST 2 ROWS WITH TIES",           " FETCH FIRST (2) ROWS WITH TIES"),
        ("with ties and offset", "OFFSET 1 FETCH FIRST $1 ROWS WITH TIES", " FETCH FIRST ($1) ROWS WITH TIES OFFSET 1"),
    ];
    let base = "SELECT * FROM users ORDER BY id ASC";
    for (label, limit, expected) in cases {
        let tree = parse(&format!("{base} {limit}"));
        assert_eq!(tree.sql(), format!("{base}{expected}"), "{label}: {limit}");
        assert_eq!(
            tree.root().with_ties(),
            expected.contains("WITH TIES"),
            "{label}"
        );
    }
}

/// The session hash leaves out a root LIMIT that is a literal, the row
/// count the dispatch tracks apart. An expression or WITH TIES is no such
/// count, so a query with one never shares the key of its literal sibling.
#[test]
fn test_session_hash_leaves_out_only_a_literal_limit() {
    let hash = |limit: &str| session_hash(&parse(&format!("SELECT * FROM users {limit}")));
    assert_eq!(hash("LIMIT ALL"), hash(""));
    assert_eq!(hash("LIMIT '5' OFFSET $1"), hash("LIMIT 10 OFFSET 5"));
    assert_ne!(hash("LIMIT $1 + 1"), hash("LIMIT 10"));
    assert_ne!(hash("LIMIT $1 + 1"), hash("LIMIT $1 + 2"));
    assert_ne!(hash("LIMIT 10 OFFSET f(2)"), hash("LIMIT 10 OFFSET 5"));
    assert_ne!(hash("LIMIT $1::int"), hash("LIMIT $1"));
    assert_ne!(
        hash("ORDER BY id FETCH FIRST 2 ROWS WITH TIES"),
        hash("ORDER BY id LIMIT 2")
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
            tree.root().limit_count().and_then(|l| l.literal().cloned()),
            tree.root()
                .limit_offset()
                .and_then(|l| l.literal().cloned()),
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
