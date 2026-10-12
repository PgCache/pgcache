#![allow(clippy::expect_used, clippy::unwrap_used)]

use super::query_tree_resolve;
use crate::catalog::Catalog;
use crate::query::ast::rope::QueryTree;
use crate::query::ast::{NodeId, query_expr_parse};
use crate::query::resolve::query_expr_resolve;
use crate::query::resolve::tests::{BINDING_CORPUS, binding_catalog, catalog};

/// The bindings the owned resolver produces for `sql`, with its node ids
/// mapped into the rope's id space, against the rope resolver's; an error
/// on one side must be the same error on the other.
fn oracle(sql: &str, tables: &Catalog) {
    let owned = query_expr_parse(sql).expect("parse query");
    let (tree, origins) = QueryTree::from_query_expr_with_origins(&owned);
    let expected = query_expr_resolve(&owned, tables, &["public"]);
    let actual = query_tree_resolve(&tree, tables, &["public"]);
    match (expected, actual) {
        (Ok(expected), Ok(actual)) => {
            let mut expected = expected.describe_with(|id: NodeId| {
                origins.new_id(id).map_or_else(
                    || format!("unmapped {}", id.index()),
                    |p| p.index().to_string(),
                )
            });
            let mut actual = actual.describe();
            expected.sort();
            actual.sort();
            assert_eq!(actual, expected, "{sql}");
        }
        (Err(expected), Err(actual)) => assert_eq!(
            actual.current_context().to_string(),
            expected.current_context().to_string(),
            "{sql}"
        ),
        (Ok(_), Err(actual)) => panic!("{sql}: the rope resolver failed: {actual}"),
        (Err(expected), Ok(_)) => {
            panic!("{sql}: the rope resolver bound a query the owned one rejects: {expected}")
        }
    }
}

#[test]
fn test_rope_bindings_match_the_owned_resolver_over_the_binding_corpus() {
    let tables = binding_catalog();
    for sql in BINDING_CORPUS {
        oracle(sql, &tables);
    }
}

#[test]
fn test_rope_bindings_match_the_owned_resolver_on_shapes() {
    let tables = binding_catalog();
    for sql in [
        "SELECT * FROM users",
        "SELECT users.*, d.* FROM users JOIN departments d ON d.id = users.dept_id",
        "SELECT public.users.id FROM public.users",
        "SELECT u.id FROM users AS u (uid, uname, udept)",
        "SELECT id FROM users WHERE id IN (SELECT user_id FROM orders) AND name = 'a'",
        "SELECT id FROM users WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = users.id)",
        "SELECT (SELECT count(*) FROM orders o WHERE o.user_id = u.id) AS n FROM users u ORDER BY n",
        "SELECT name FROM users u WHERE u.id = ANY (SELECT user_id FROM orders)",
        "SELECT name FROM users u WHERE u.id <> ALL (SELECT user_id FROM orders)",
        "SELECT sum(amount) OVER (PARTITION BY user_id ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) \
         FROM orders",
        "SELECT string_agg(name, ',' ORDER BY name) FILTER (WHERE id = '1') FROM users",
        "SELECT CASE id WHEN '1' THEN name ELSE 'x' END FROM users",
        "SELECT id + dept_id, id::text, name::date FROM users",
        "SELECT * FROM (VALUES ('1', '2')) AS v (a, b)",
        "SELECT a FROM (SELECT id AS a FROM users UNION ALL SELECT id FROM departments) s",
        "WITH u (uid) AS (SELECT id FROM users) SELECT uid FROM u",
        "WITH u AS (SELECT id FROM users) SELECT x.id FROM u AS x",
        "SELECT i.sku FROM items i NATURAL JOIN orders",
        "SELECT * FROM users CROSS JOIN departments",
        "SELECT * FROM users u LEFT JOIN departments d USING (name)",
        "SELECT id FROM users GROUP BY id HAVING count(*) > '1'",
        "SELECT name FROM users ORDER BY name DESC NULLS LAST LIMIT 3 OFFSET 1",
        "SELECT id FROM users UNION SELECT id FROM departments ORDER BY id LIMIT 2",
        "SELECT u.name FROM users u WHERE u.dept_id BETWEEN '1' AND '2' AND u.name NOT IN ('a', 'b')",
    ] {
        oracle(sql, &tables);
    }
}

#[test]
fn test_rope_resolver_reports_the_owned_resolver_errors() {
    let tables = binding_catalog();
    for sql in [
        "SELECT id FROM missing",
        "SELECT nope FROM users",
        "SELECT id FROM users JOIN departments ON users.dept_id = departments.id",
        "SELECT name FROM users u LEFT JOIN departments d USING (name) GROUP BY name",
        "SELECT * FROM (SELECT id FROM users)",
        "SELECT id FROM users u LEFT JOIN departments d USING (sku)",
        "SELECT x.id FROM users WHERE id = '1'",
    ] {
        oracle(sql, &tables);
    }
}

#[test]
fn test_rope_resolver_follows_the_search_path() {
    let tables = catalog(&["orders"]);
    let owned = query_expr_parse("SELECT id FROM orders WHERE name = 'x'").expect("parse");
    let tree = QueryTree::from_query_expr(&owned);
    assert!(query_tree_resolve(&tree, &tables, &["public"]).is_ok());
    assert!(query_tree_resolve(&tree, &tables, &["other"]).is_err());
}
