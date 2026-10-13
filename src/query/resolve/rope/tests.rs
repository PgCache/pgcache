#![allow(clippy::expect_used, clippy::unwrap_used)]

use super::query_tree_resolve;
use crate::query::ast::query_tree_parse;
use crate::query::resolve::ResolvedQuery;
use crate::query::resolve::tests::{binding_catalog, catalog};

/// What binding each query gives: its serve SQL, or `ERR` and the error.
#[rustfmt::skip]
const RESOLVE_EXPECTED: &[(&str, &str)] = &[
    ("SELECT id FROM missing",
     "ERR Table not found: missing"),
    ("SELECT u.id, name FROM users u WHERE dept_id = 'x'",
     "SELECT u.id, u.name FROM public.users u WHERE u.dept_id = 'x'"),
    ("SELECT nope FROM users",
     "ERR Column 'nope' not found in table '<unknown>'"),
    ("SELECT id FROM users JOIN departments ON users.dept_id = departments.id",
     "ERR Ambiguous column reference: 'id' could refer to multiple tables"),
    ("SELECT users.*, d.* FROM users JOIN departments d ON d.id = users.dept_id",
     "SELECT public.users.id, public.users.name, public.users.dept_id, d.id, d.name, d.manager_id FROM public.users JOIN public.departments d ON d.id = public.users.dept_id"),
    ("SELECT public.users.id FROM public.users",
     "SELECT public.users.id FROM public.users"),
    ("SELECT u.id, d.name AS dname FROM users u JOIN departments d ON u.dept_id = d.id WHERE d.name = 'a' ORDER BY dname, u.id DESC",
     "SELECT u.id, d.name AS dname FROM public.users u JOIN public.departments d ON u.dept_id = d.id WHERE d.name = 'a' ORDER BY dname ASC, u.id DESC"),
    ("SELECT u.id FROM users AS u (uid, uname, udept)",
     "SELECT u.id FROM public.users u(uid, uname, udept)"),
    ("SELECT name FROM users u LEFT JOIN departments d USING (name) GROUP BY name",
     "ERR Unsupported join qualifier (USING/NATURAL not yet cacheable)"),
    ("SELECT * FROM users JOIN departments USING (id)",
     "SELECT public.users.id AS id, public.users.name, public.users.dept_id, public.departments.name, public.departments.manager_id FROM public.users JOIN public.departments USING (id)"),
    ("SELECT * FROM (SELECT id FROM users)",
     "ERR Subquery alias '<missing>' not found"),
    ("SELECT id FROM users WHERE id IN (SELECT user_id FROM orders) AND name = 'a'",
     "SELECT public.users.id FROM public.users WHERE public.users.id IN (SELECT public.orders.user_id FROM public.orders) AND public.users.name = 'a'"),
    ("SELECT id FROM users u LEFT JOIN departments d USING (sku)",
     "ERR Unsupported join qualifier (USING/NATURAL not yet cacheable)"),
    ("SELECT x.id FROM users WHERE id = '1'",
     "ERR Table not found: x"),
    ("SELECT * FROM users LEFT JOIN departments USING (id) WHERE id = '1'",
     "SELECT coalesce(public.users.id, public.departments.id) AS id, public.users.name, public.users.dept_id, public.departments.name, public.departments.manager_id FROM public.users LEFT JOIN public.departments USING (id) WHERE coalesce(public.users.id, public.departments.id) = '1'"),
    ("SELECT id FROM users WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = users.id)",
     "SELECT public.users.id FROM public.users WHERE NOT EXISTS (SELECT 1 FROM public.orders o WHERE o.user_id = public.users.id)"),
    ("SELECT id, count(*) FROM users JOIN departments USING (id) GROUP BY id",
     "SELECT public.users.id, count(*) FROM public.users JOIN public.departments USING (id) GROUP BY public.users.id"),
    ("SELECT (SELECT count(*) FROM orders o WHERE o.user_id = u.id) AS n FROM users u ORDER BY n",
     "SELECT (SELECT count(*) FROM public.orders o WHERE o.user_id = u.id) AS n FROM public.users u ORDER BY n ASC"),
    ("SELECT * FROM users NATURAL JOIN departments",
     "SELECT public.users.id AS id, public.users.name AS name, public.users.dept_id, public.departments.manager_id FROM public.users JOIN public.departments USING (id, name)"),
    ("SELECT name FROM users u WHERE u.id = ANY (SELECT user_id FROM orders)",
     "SELECT u.name FROM public.users u WHERE u.id IN (SELECT public.orders.user_id FROM public.orders)"),
    ("SELECT d.name FROM departments d WHERE EXISTS (SELECT 1 FROM users u WHERE u.dept_id = d.id)",
     "SELECT d.name FROM public.departments d WHERE EXISTS (SELECT 1 FROM public.users u WHERE u.dept_id = d.id)"),
    ("SELECT name FROM users u WHERE u.id <> ALL (SELECT user_id FROM orders)",
     "SELECT u.name FROM public.users u WHERE u.id <> ALL (SELECT public.orders.user_id FROM public.orders)"),
    ("SELECT v.total, v.uid FROM (SELECT user_id AS uid, amount AS total FROM orders) v WHERE v.total = '5'",
     "SELECT v.total, v.uid FROM (SELECT public.orders.user_id AS uid, public.orders.amount AS total FROM public.orders) v WHERE v.total = '5'"),
    ("SELECT sum(amount) OVER (PARTITION BY user_id ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM orders",
     "SELECT sum(public.orders.amount) OVER (PARTITION BY public.orders.user_id ORDER BY public.orders.id ASC ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM public.orders"),
    ("SELECT string_agg(name, ',' ORDER BY name) FILTER (WHERE id = '1') FROM users",
     "SELECT string_agg(public.users.name, ',' ORDER BY public.users.name ASC) FILTER (WHERE public.users.id = '1') FROM public.users"),
    ("SELECT CASE id WHEN '1' THEN name ELSE 'x' END FROM users",
     "SELECT CASE public.users.id WHEN '1' THEN public.users.name ELSE 'x' END FROM public.users"),
    ("WITH big AS (SELECT id, amount FROM orders WHERE amount = '9') SELECT big.id FROM big JOIN items ON items.order_id = big.id",
     "SELECT big.id FROM (SELECT public.orders.id, public.orders.amount FROM public.orders WHERE public.orders.amount = '9') big JOIN public.items ON public.items.order_id = big.id"),
    ("SELECT id + dept_id, id::text, name::date FROM users",
     "SELECT (public.users.id + public.users.dept_id), (public.users.id)::text, (public.users.name)::date FROM public.users"),
    ("SELECT id FROM users UNION SELECT id FROM departments ORDER BY id",
     "SELECT public.users.id FROM public.users UNION SELECT public.departments.id FROM public.departments ORDER BY id ASC"),
    ("SELECT * FROM (VALUES ('1', '2')) AS v (a, b)",
     "SELECT FROM (VALUES ('1', '2')) v(a, b)"),
    ("SELECT name, count(*) FROM users GROUP BY name HAVING count(*) > 1 ORDER BY count(*) DESC",
     "SELECT public.users.name, count(*) FROM public.users GROUP BY public.users.name HAVING count(*) > 1 ORDER BY count(*) DESC"),
    ("SELECT a FROM (SELECT id AS a FROM users UNION ALL SELECT id FROM departments) s",
     "SELECT s.a FROM (SELECT public.users.id AS a FROM public.users UNION ALL SELECT public.departments.id FROM public.departments) s"),
    ("SELECT u.name, (SELECT max(o.amount) FROM orders o WHERE o.user_id = u.id) FROM users u WHERE u.id IN (SELECT user_id FROM orders WHERE amount = '1')",
     "SELECT u.name, (SELECT max(o.amount) FROM public.orders o WHERE o.user_id = u.id) FROM public.users u WHERE u.id IN (SELECT public.orders.user_id FROM public.orders WHERE public.orders.amount = '1')"),
    ("WITH u (uid) AS (SELECT id FROM users) SELECT uid FROM u",
     "ERR Column 'uid' not found in table '<unknown>'"),
    ("WITH u AS (SELECT id FROM users) SELECT x.id FROM u AS x",
     "SELECT x.id FROM (SELECT public.users.id FROM public.users) x"),
    ("SELECT u.* FROM users u JOIN departments d ON d.id = u.dept_id WHERE d.id = ANY(ARRAY['1', '2']) AND u.name LIKE 'a%'",
     "SELECT u.id, u.name, u.dept_id FROM public.users u JOIN public.departments d ON d.id = u.dept_id WHERE d.id = ANY (ARRAY['1', '2']) AND u.name LIKE 'a%'"),
    ("SELECT i.sku FROM items i NATURAL JOIN orders",
     "SELECT i.sku FROM public.items i JOIN public.orders USING (id)"),
    ("SELECT CASE WHEN amount = '0' THEN sku ELSE order_id END FROM orders o JOIN items i ON i.order_id = o.id ORDER BY i.sku NULLS FIRST",
     "SELECT CASE WHEN o.amount = '0' THEN i.sku ELSE i.order_id END FROM public.orders o JOIN public.items i ON i.order_id = o.id ORDER BY i.sku ASC NULLS FIRST"),
    ("SELECT * FROM users CROSS JOIN departments",
     "SELECT public.users.id, public.users.name, public.users.dept_id, public.departments.id, public.departments.name, public.departments.manager_id FROM public.users CROSS JOIN public.departments"),
    ("SELECT * FROM users u LEFT JOIN departments d USING (name)",
     "SELECT coalesce(u.name, d.name) AS name, u.id, u.dept_id, d.id, d.manager_id FROM public.users u LEFT JOIN public.departments d USING (name)"),
    ("SELECT id FROM users GROUP BY id HAVING count(*) > '1'",
     "SELECT public.users.id FROM public.users GROUP BY public.users.id HAVING count(*) > '1'"),
    ("SELECT name FROM users ORDER BY name DESC NULLS LAST LIMIT 3 OFFSET 1",
     "SELECT public.users.name FROM public.users ORDER BY name DESC NULLS LAST LIMIT 3 OFFSET 1"),
    ("SELECT id FROM users UNION SELECT id FROM departments ORDER BY id LIMIT 2",
     "SELECT public.users.id FROM public.users UNION SELECT public.departments.id FROM public.departments ORDER BY id ASC LIMIT 2"),
    ("SELECT u.name FROM users u WHERE u.dept_id BETWEEN '1' AND '2' AND u.name NOT IN ('a', 'b')",
     "SELECT u.name FROM public.users u WHERE u.dept_id BETWEEN '1' AND '2' AND u.name NOT IN ('a', 'b')"),
];

/// The binding corpus, every shape the resolver handles and every error it
/// reports bind to their expected renderings.
#[test]
fn test_resolver_expectations() {
    let tables = binding_catalog();
    for (sql, expected) in RESOLVE_EXPECTED {
        let actual = match ResolvedQuery::parse_resolve(sql, &tables, &["public"]) {
            Ok(bound) => bound.sql(),
            Err(e) => format!("ERR {}", e.current_context()),
        };
        assert_eq!(actual, *expected, "{sql}");
    }
}

#[test]
fn test_resolver_follows_the_search_path() {
    let tables = catalog(&["orders"]);
    let tree = query_tree_parse("SELECT id FROM orders WHERE name = 'x'").expect("parse");
    assert!(query_tree_resolve(&tree, &tables, &["public"]).is_ok());
    assert!(query_tree_resolve(&tree, &tables, &["other"]).is_err());
}
