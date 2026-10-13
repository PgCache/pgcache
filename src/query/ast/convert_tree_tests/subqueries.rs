//! Subqueries, VALUES, set operations and CTEs.

#![allow(clippy::wildcard_enum_match_arm, clippy::type_complexity)]

use super::*;

// ============================================================================
// Subqueries, VALUES and set operations
// ============================================================================

#[test]
fn test_select_list_subquery() {
    let tree = parse("SELECT invoice.id, (SELECT x.data FROM x WHERE 1 = 1) as one FROM invoice");
    let items = items(select_of(&tree));
    let described: Vec<(Option<&str>, &str)> = items
        .iter()
        .map(|item| {
            let kind = match expr_of(*item).kind() {
                ScalarKind::Column(column)
                    if column.table().is_some_and(|t| t == "invoice")
                        && column.column() == "id" =>
                {
                    "invoice.id"
                }
                ScalarKind::Subquery(_) => "subquery",
                _ => "other",
            };
            (alias_of(*item).map(EcoString::as_str), kind)
        })
        .collect();
    assert_eq!(described, [(None, "invoice.id"), (Some("one"), "subquery")]);
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
        let branches_are_selects = matches!(set_op.left().body(), Body::Select(_))
            && matches!(set_op.right().body(), Body::Select(_));
        assert_eq!(
            (set_op.op(), set_op.all(), branches_are_selects, tree.sql()),
            (op, all, true, sql.to_owned()),
            "{label}"
        );
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
