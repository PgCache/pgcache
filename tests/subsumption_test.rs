use std::io::Error;

use crate::util::{
    TestContext, assert_cache_hit, assert_cache_miss, assert_not_subsumed, assert_row_at,
    assert_subsume_hit, metrics_delta,
};

mod util;

/// Test that an unconstrained cached query subsumes a constrained query on the same table.
/// SELECT * FROM t (no WHERE) covers SELECT * FROM t WHERE id = 1.
#[tokio::test]
async fn test_subsumption_unconstrained_covers_constrained() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_sub_uncon (id integer PRIMARY KEY, data text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_sub_uncon VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')",
        &[],
    )
    .await?;

    // Cache the unconstrained query
    let m = ctx.metrics().await?;
    let res = ctx
        .simple_query("SELECT id, data FROM test_sub_uncon")
        .await?;
    assert_eq!(res.len(), 5); // 3 rows + RowDescription + CommandComplete
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Constrained query should be subsumed — served from cache immediately
    let res = ctx
        .simple_query("SELECT id, data FROM test_sub_uncon WHERE id = 1")
        .await?;
    assert_eq!(res.len(), 3); // 1 row
    assert_row_at(&res, 1, &[("id", "1"), ("data", "alice")])?;
    let _m = assert_subsume_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test equality superset subsumption: cached WHERE tenant_id = 1 covers
/// WHERE tenant_id = 1 AND status = 'active'.
#[tokio::test]
async fn test_subsumption_equality_superset() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_sub_eq (id integer PRIMARY KEY, tenant_id integer, status text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_sub_eq VALUES \
         (1, 1, 'active'), (2, 1, 'inactive'), (3, 2, 'active')",
        &[],
    )
    .await?;

    // Cache with single equality constraint
    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM test_sub_eq WHERE tenant_id = 1")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Narrower query (superset of constraints) should be subsumed
    let res = ctx
        .simple_query("SELECT * FROM test_sub_eq WHERE tenant_id = 1 AND status = 'active'")
        .await?;
    assert_eq!(res.len(), 3); // 1 row
    assert_row_at(
        &res,
        1,
        &[("id", "1"), ("tenant_id", "1"), ("status", "active")],
    )?;
    let _m = assert_subsume_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test that different equality values are NOT subsumed.
/// Cached WHERE tenant_id = 1 does NOT cover WHERE tenant_id = 2.
#[tokio::test]
async fn test_subsumption_different_values_not_subsumed() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_sub_diff (id integer PRIMARY KEY, tenant_id integer)",
        &[],
    )
    .await?;
    ctx.query("INSERT INTO test_sub_diff VALUES (1, 1), (2, 2)", &[])
        .await?;

    // Cache tenant_id = 1
    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM test_sub_diff WHERE tenant_id = 1")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Query tenant_id = 2 — different value, should NOT be subsumed
    ctx.simple_query("SELECT * FROM test_sub_diff WHERE tenant_id = 2")
        .await?;
    let _m = assert_not_subsumed(&mut ctx, m).await?;

    Ok(())
}

/// Test that queries on different tables are NOT subsumed.
#[tokio::test]
async fn test_subsumption_different_table_not_subsumed() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_sub_t1 (id integer PRIMARY KEY, data text)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE TABLE test_sub_t2 (id integer PRIMARY KEY, data text)",
        &[],
    )
    .await?;
    ctx.query("INSERT INTO test_sub_t1 VALUES (1, 'a')", &[])
        .await?;
    ctx.query("INSERT INTO test_sub_t2 VALUES (1, 'b')", &[])
        .await?;

    // Cache t1
    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM test_sub_t1").await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Query t2 — different table, not subsumed
    ctx.simple_query("SELECT * FROM test_sub_t2").await?;
    let _m = assert_not_subsumed(&mut ctx, m).await?;

    Ok(())
}

/// Test that a narrower inequality IS subsumed by a wider one.
/// Cached WHERE value > 50 covers WHERE value > 100.
#[tokio::test]
async fn test_subsumption_inequality_subsumed() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_sub_ineq (id integer PRIMARY KEY, value integer)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_sub_ineq VALUES (1, 25), (2, 75), (3, 150)",
        &[],
    )
    .await?;

    // Cache with inequality
    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM test_sub_ineq WHERE value > 50")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Narrower inequality — subsumed (value > 100 implies value > 50)
    let rows = ctx
        .simple_query("SELECT * FROM test_sub_ineq WHERE value > 100")
        .await?;
    let _m = assert_subsume_hit(&mut ctx, m).await?;
    assert_eq!(rows.len(), 3); // 1 row + RowDescription + CommandComplete

    Ok(())
}

/// Test that a cached JOIN query does NOT subsume, even with same tables and
/// additional WHERE constraints. Multi-table cached queries are excluded from
/// subsumption candidates because join conditions act as implicit filters.
#[tokio::test]
async fn test_subsumption_join_not_subsumed() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_sub_orders (id integer PRIMARY KEY, customer_id integer, status text)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE TABLE test_sub_customers (id integer PRIMARY KEY, name text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_sub_orders VALUES (1, 1, 'pending'), (2, 2, 'active')",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_sub_customers VALUES (1, 'alice'), (2, 'bob')",
        &[],
    )
    .await?;

    // Cache the JOIN query
    let m = ctx.metrics().await?;
    ctx.simple_query(
        "SELECT o.id, c.name FROM test_sub_orders o \
         JOIN test_sub_customers c ON o.customer_id = c.id",
    )
    .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Same JOIN with additional WHERE — not subsumed (multi-table cached query)
    ctx.simple_query(
        "SELECT o.id, c.name FROM test_sub_orders o \
         JOIN test_sub_customers c ON o.customer_id = c.id \
         WHERE o.status = 'pending'",
    )
    .await?;
    let _m = assert_not_subsumed(&mut ctx, m).await?;

    Ok(())
}

/// Test that a single-table cached query does NOT subsume a JOIN query
/// that references an additional uncovered table.
#[tokio::test]
async fn test_subsumption_join_partial_coverage_not_subsumed() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_sub_orders2 (id integer PRIMARY KEY, customer_id integer, tenant_id integer)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE TABLE test_sub_customers2 (id integer PRIMARY KEY, name text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_sub_orders2 VALUES (1, 1, 1), (2, 2, 1)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_sub_customers2 VALUES (1, 'alice'), (2, 'bob')",
        &[],
    )
    .await?;

    // Cache single-table query on orders
    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM test_sub_orders2 WHERE tenant_id = 1")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // JOIN query — orders is covered but customers is not
    ctx.simple_query(
        "SELECT o.id, c.name FROM test_sub_orders2 o \
         JOIN test_sub_customers2 c ON o.customer_id = c.id \
         WHERE o.tenant_id = 1",
    )
    .await?;
    let _m = assert_not_subsumed(&mut ctx, m).await?;

    Ok(())
}

/// Test that a cached query with LIMIT does NOT subsume.
/// LIMIT restricts the number of rows cached, so coverage can't be guaranteed.
#[tokio::test]
async fn test_subsumption_with_limit_not_subsumed() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_sub_limit (id integer PRIMARY KEY, data text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_sub_limit VALUES (1, 'a'), (2, 'b'), (3, 'c')",
        &[],
    )
    .await?;

    // Cache with LIMIT
    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM test_sub_limit LIMIT 5")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Query without LIMIT — not subsumed (cached has LIMIT)
    ctx.simple_query("SELECT * FROM test_sub_limit WHERE id = 1")
        .await?;
    let _m = assert_not_subsumed(&mut ctx, m).await?;

    Ok(())
}

/// Test that two separate single-table cached queries can subsume a JOIN query
/// that references both tables. Each table is independently covered by its own
/// cached query, so the JOIN query's data is fully available in the cache DB.
#[tokio::test]
async fn test_subsumption_two_single_tables_cover_join() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_sub_items (id integer PRIMARY KEY, category_id integer, name text)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE TABLE test_sub_categories (id integer PRIMARY KEY, label text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_sub_items VALUES (1, 10, 'widget'), (2, 20, 'gadget'), (3, 10, 'thing')",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_sub_categories VALUES (10, 'tools'), (20, 'electronics')",
        &[],
    )
    .await?;

    // Cache both tables independently
    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM test_sub_items").await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    ctx.simple_query("SELECT * FROM test_sub_categories")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // JOIN query referencing both tables — each table covered by its own cached query
    let res = ctx
        .simple_query(
            "SELECT i.name, c.label FROM test_sub_items i \
             JOIN test_sub_categories c ON i.category_id = c.id \
             ORDER BY i.name",
        )
        .await?;
    assert_eq!(res.len(), 5); // 3 rows (widget+tools, gadget+electronics, thing+tools)
    assert_row_at(&res, 1, &[("name", "gadget"), ("label", "electronics")])?;
    assert_row_at(&res, 2, &[("name", "thing"), ("label", "tools")])?;
    assert_row_at(&res, 3, &[("name", "widget"), ("label", "tools")])?;
    let _m = assert_subsume_hit(&mut ctx, m).await?;

    Ok(())
}

/// A self-joined new query must not be subsumed by a parent that covers only
/// one arm (PGC-493). The new query's constraints are keyed by table name, so
/// `e.dept = 'x'` and `m.name = 'ceo'` collapse into one set for `emp` that
/// implies the parent's `dept = 'x'`. The `m` arm needs the ceo row, which is
/// `dept = 'exec'` and was never loaded.
#[tokio::test]
async fn test_subsumption_self_join_child_not_subsumed() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_sub_emp (id integer PRIMARY KEY, name text, manager_id integer, dept text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_sub_emp VALUES (1, 'ceo', NULL, 'exec'), (2, 'alice', 1, 'x'), (3, 'bob', 1, 'x')",
        &[],
    )
    .await?;

    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT id FROM test_sub_emp WHERE dept = 'x'")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    let res = ctx
        .simple_query(
            "SELECT e.id FROM test_sub_emp e \
             JOIN test_sub_emp m ON m.id = e.manager_id \
             WHERE e.dept = 'x' AND m.name = 'ceo' ORDER BY e.id",
        )
        .await?;
    assert_eq!(res.len(), 4, "alice and bob report to the ceo");
    assert_row_at(&res, 1, &[("id", "2")])?;
    assert_row_at(&res, 2, &[("id", "3")])?;
    let _m = assert_not_subsumed(&mut ctx, m).await?;

    Ok(())
}

/// A new query that reads a table in both the outer query and a subquery must
/// not be subsumed by a parent covering only the outer predicate (PGC-493).
/// The subquery's `y = 2` is not extracted, so the table's constraints are the
/// outer `x = 1` alone; the subquery needs row 1, which has `x = 0`.
#[tokio::test]
async fn test_subsumption_subquery_same_table_child_not_subsumed() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_sub_node (id integer PRIMARY KEY, parent_id integer, x integer, y integer)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_sub_node VALUES (1, NULL, 0, 2), (2, 1, 1, 0), (3, 1, 1, 0)",
        &[],
    )
    .await?;

    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT id FROM test_sub_node WHERE x = 1")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    let res = ctx
        .simple_query(
            "SELECT id FROM test_sub_node WHERE x = 1 \
             AND parent_id IN (SELECT id FROM test_sub_node WHERE y = 2) ORDER BY id",
        )
        .await?;
    assert_eq!(res.len(), 4, "rows 2 and 3 have parent 1");
    assert_row_at(&res, 1, &[("id", "2")])?;
    assert_row_at(&res, 2, &[("id", "3")])?;
    let _m = assert_not_subsumed(&mut ctx, m).await?;

    Ok(())
}

/// The CTE form of the subquery case (PGC-493): the table is read once in the
/// CTE body and once in the outer query.
#[tokio::test]
async fn test_subsumption_cte_same_table_child_not_subsumed() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_sub_node_cte (id integer PRIMARY KEY, parent_id integer, x integer, y integer)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_sub_node_cte VALUES (1, NULL, 0, 2), (2, 1, 1, 0), (3, 1, 1, 0)",
        &[],
    )
    .await?;

    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT id FROM test_sub_node_cte WHERE x = 1")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    let res = ctx
        .simple_query(
            "WITH c AS (SELECT id FROM test_sub_node_cte WHERE y = 2) \
             SELECT id FROM test_sub_node_cte WHERE x = 1 \
             AND parent_id IN (SELECT id FROM c) ORDER BY id",
        )
        .await?;
    assert_eq!(res.len(), 4, "rows 2 and 3 have parent 1");
    assert_row_at(&res, 1, &[("id", "2")])?;
    assert_row_at(&res, 2, &[("id", "3")])?;
    let _m = assert_not_subsumed(&mut ctx, m).await?;

    Ok(())
}

/// A full-table parent holds every row any occurrence could need, so it still
/// subsumes a self-joined new query (PGC-493 keeps this precision).
#[tokio::test]
async fn test_subsumption_full_table_covers_self_join_child() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_sub_emp_full (id integer PRIMARY KEY, name text, manager_id integer, dept text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_sub_emp_full VALUES (1, 'ceo', NULL, 'exec'), (2, 'alice', 1, 'x'), (3, 'bob', 1, 'x')",
        &[],
    )
    .await?;

    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM test_sub_emp_full").await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    let res = ctx
        .simple_query(
            "SELECT e.id FROM test_sub_emp_full e \
             JOIN test_sub_emp_full m ON m.id = e.manager_id \
             WHERE e.dept = 'x' AND m.name = 'ceo' ORDER BY e.id",
        )
        .await?;
    assert_eq!(res.len(), 4, "alice and bob report to the ceo");
    assert_row_at(&res, 1, &[("id", "2")])?;
    assert_row_at(&res, 2, &[("id", "3")])?;
    let _m = assert_subsume_hit(&mut ctx, m).await?;

    Ok(())
}

/// Two tables sharing a name across schemas must not share constraints
/// (PGC-493). Constraints are keyed by table name, so `a.x = 1` would also be
/// read as a constraint on `test_sub_schema_b.item`, which both parents then
/// appear to cover; the `b` side needs row 10, which has `x = 0`.
#[tokio::test]
async fn test_subsumption_same_name_across_schemas_not_subsumed() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    for schema in ["test_sub_schema_a", "test_sub_schema_b"] {
        ctx.query(&format!("CREATE SCHEMA {schema}"), &[]).await?;
        ctx.query(
            &format!(
                "CREATE TABLE {schema}.item (id integer PRIMARY KEY, ref_id integer, x integer)"
            ),
            &[],
        )
        .await?;
    }
    ctx.query(
        "INSERT INTO test_sub_schema_a.item VALUES (1, 10, 1), (2, 10, 1)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_sub_schema_b.item VALUES (10, NULL, 0), (11, NULL, 1)",
        &[],
    )
    .await?;

    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT id FROM test_sub_schema_a.item WHERE x = 1")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    ctx.simple_query("SELECT id FROM test_sub_schema_b.item WHERE x = 1")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    let res = ctx
        .simple_query(
            "SELECT a.id FROM test_sub_schema_a.item a \
             JOIN test_sub_schema_b.item b ON b.id = a.ref_id \
             WHERE a.x = 1 ORDER BY a.id",
        )
        .await?;
    assert_eq!(res.len(), 4, "rows 1 and 2 reference b row 10");
    assert_row_at(&res, 1, &[("id", "1")])?;
    assert_row_at(&res, 2, &[("id", "2")])?;
    let _m = assert_not_subsumed(&mut ctx, m).await?;

    Ok(())
}

/// A cached `a = b` holds only the rows where the two columns agree, so a
/// narrower query on another column must not be served from it.
#[tokio::test]
async fn test_subsumption_same_table_equality_not_full_table() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE sub_ceq (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
        &[],
    )
    .await?;
    ctx.query("INSERT INTO sub_ceq VALUES (1, 1, 1), (5, 1, 2)", &[])
        .await?;
    ctx.cdc_decode_settle().await?;

    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT id, a, b FROM sub_ceq WHERE a = b")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    let res = ctx
        .simple_query("SELECT id, a, b FROM sub_ceq WHERE id = 5")
        .await?;
    assert_eq!(res.len(), 3); // 1 row
    assert_row_at(&res, 1, &[("id", "5"), ("a", "1"), ("b", "2")])?;
    let _m = assert_not_subsumed(&mut ctx, m).await?;

    Ok(())
}

/// A cached `x BETWEEN 1 AND y` holds only the rows inside each row's own
/// bound, so a query on `x` alone must not be served from it.
#[tokio::test]
async fn test_subsumption_non_literal_between_not_full_table() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE sub_nlo (id INTEGER PRIMARY KEY, x INTEGER, y INTEGER)",
        &[],
    )
    .await?;
    ctx.query("INSERT INTO sub_nlo VALUES (1, 5, 9), (2, 5, 3)", &[])
        .await?;
    ctx.cdc_decode_settle().await?;

    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT id, x, y FROM sub_nlo WHERE x BETWEEN 1 AND y")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    let res = ctx
        .simple_query("SELECT id, x, y FROM sub_nlo WHERE x = 5 ORDER BY id")
        .await?;
    assert_eq!(res.len(), 4); // 2 rows
    assert_row_at(&res, 1, &[("id", "1"), ("x", "5"), ("y", "9")])?;
    assert_row_at(&res, 2, &[("id", "2"), ("x", "5"), ("y", "3")])?;
    let _m = assert_not_subsumed(&mut ctx, m).await?;

    Ok(())
}

/// The preserved reference of a same-branch LEFT self-join is loaded in full
/// for its filters, so it parents a narrower single-table query, which CDC
/// then keeps current on its own (PGC-516).
#[tokio::test]
async fn test_subsumption_self_join_preserved_reference_parent() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_sub_sjp (id integer PRIMARY KEY, manager_id integer, dept text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_sub_sjp VALUES (1, NULL, 'exec'), (2, 1, 'x'), (3, 1, 'x'), (4, NULL, 'x')",
        &[],
    )
    .await?;

    let m = ctx.metrics().await?;
    ctx.simple_query(
        "SELECT e.id, m.id FROM test_sub_sjp e \
         LEFT JOIN test_sub_sjp m ON m.id = e.manager_id WHERE e.dept = 'x'",
    )
    .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    let child = "SELECT id FROM test_sub_sjp WHERE dept = 'x' AND id > 2 ORDER BY id";
    let res = ctx.simple_query(child).await?;
    assert_eq!(res.len(), 4, "rows 3 and 4");
    assert_row_at(&res, 1, &[("id", "3")])?;
    assert_row_at(&res, 2, &[("id", "4")])?;
    let _m = assert_subsume_hit(&mut ctx, m).await?;

    ctx.query("INSERT INTO test_sub_sjp VALUES (5, NULL, 'x')", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    let (proxied, origin) = ctx.proxy_and_origin_rows(child).await?;
    assert_eq!(origin.len(), 3, "rows 3, 4 and 5");
    assert_eq!(proxied, origin);

    Ok(())
}

/// Each reference of a self-joined new query is covered on its own filters,
/// so one parent per reference covers it (PGC-516).
#[tokio::test]
async fn test_subsumption_self_join_child_covered_per_reference() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_sub_emp_ref (id integer PRIMARY KEY, name text, manager_id integer, dept text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_sub_emp_ref VALUES (1, 'ceo', NULL, 'exec'), (2, 'alice', 1, 'x'), (3, 'bob', 1, 'x')",
        &[],
    )
    .await?;

    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT id FROM test_sub_emp_ref WHERE dept = 'x'")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    ctx.simple_query("SELECT id FROM test_sub_emp_ref WHERE name = 'ceo'")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    let res = ctx
        .simple_query(
            "SELECT e.id FROM test_sub_emp_ref e \
             JOIN test_sub_emp_ref m ON m.id = e.manager_id \
             WHERE e.dept = 'x' AND m.name = 'ceo' ORDER BY e.id",
        )
        .await?;
    assert_eq!(res.len(), 4, "alice and bob report to the ceo");
    assert_row_at(&res, 1, &[("id", "2")])?;
    assert_row_at(&res, 2, &[("id", "3")])?;
    let _m = assert_subsume_hit(&mut ctx, m).await?;

    Ok(())
}

/// A table read in a subquery is covered on the subquery's own filters, so
/// parents on the outer and the subquery predicates together cover the query
/// that `test_subsumption_subquery_same_table_child_not_subsumed` refuses
/// with the outer parent alone (PGC-516).
#[tokio::test]
async fn test_subsumption_subquery_reference_covered_on_own_filters() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_sub_node_ref (id integer PRIMARY KEY, parent_id integer, x integer, y integer)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_sub_node_ref VALUES (1, NULL, 0, 2), (2, 1, 1, 0), (3, 1, 1, 0)",
        &[],
    )
    .await?;

    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT id FROM test_sub_node_ref WHERE x = 1")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    ctx.simple_query("SELECT id FROM test_sub_node_ref WHERE y = 2")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    let res = ctx
        .simple_query(
            "SELECT id FROM test_sub_node_ref WHERE x = 1 \
             AND parent_id IN (SELECT id FROM test_sub_node_ref WHERE y = 2) ORDER BY id",
        )
        .await?;
    assert_eq!(res.len(), 4, "rows 2 and 3 have parent 1");
    assert_row_at(&res, 1, &[("id", "2")])?;
    assert_row_at(&res, 2, &[("id", "3")])?;
    let _m = assert_subsume_hit(&mut ctx, m).await?;

    Ok(())
}

/// The CTE form: the CTE body's table is covered on the body's filters
/// (PGC-516).
#[tokio::test]
async fn test_subsumption_cte_reference_covered_on_own_filters() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_sub_node_cte_ref (id integer PRIMARY KEY, parent_id integer, x integer, y integer)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_sub_node_cte_ref VALUES (1, NULL, 0, 2), (2, 1, 1, 0), (3, 1, 1, 0)",
        &[],
    )
    .await?;

    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT id FROM test_sub_node_cte_ref WHERE x = 1")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    ctx.simple_query("SELECT id FROM test_sub_node_cte_ref WHERE y = 2")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    let res = ctx
        .simple_query(
            "WITH c AS (SELECT id FROM test_sub_node_cte_ref WHERE y = 2) \
             SELECT id FROM test_sub_node_cte_ref WHERE x = 1 \
             AND parent_id IN (SELECT id FROM c) ORDER BY id",
        )
        .await?;
    assert_eq!(res.len(), 4, "rows 2 and 3 have parent 1");
    assert_row_at(&res, 1, &[("id", "2")])?;
    assert_row_at(&res, 2, &[("id", "3")])?;
    let _m = assert_subsume_hit(&mut ctx, m).await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Set-operation children: covered when every arm is (PGC-495)
// ---------------------------------------------------------------------------

async fn setup_set_operation_tables(ctx: &mut TestContext) -> Result<(), Error> {
    for table in ["sub_setop_a", "sub_setop_b"] {
        ctx.query(
            &format!("CREATE TABLE {table} (id integer PRIMARY KEY, tenant_id integer)"),
            &[],
        )
        .await?;
    }
    ctx.query("INSERT INTO sub_setop_a VALUES (1, 1), (2, 1), (3, 2)", &[])
        .await?;
    ctx.query("INSERT INTO sub_setop_b VALUES (2, 1), (4, 1), (5, 2)", &[])
        .await?;
    ctx.cdc_decode_settle().await
}

/// Warm a single-table parent: a miss, then settled.
async fn parent_warm(ctx: &mut TestContext, sql: &str) -> Result<(), Error> {
    let m = ctx.metrics().await?;
    ctx.simple_query(sql).await?;
    assert_cache_miss(ctx, m).await?;
    ctx.cache_settle().await
}

/// UNION, INTERSECT and EXCEPT children are served from parents that cover
/// every arm, and keep following CDC afterwards.
#[tokio::test]
async fn test_subsumption_set_operation_covered_per_arm() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_set_operation_tables(&mut ctx).await?;
    parent_warm(&mut ctx, "SELECT * FROM sub_setop_a WHERE tenant_id = 1").await?;
    parent_warm(&mut ctx, "SELECT * FROM sub_setop_b WHERE tenant_id = 1").await?;

    let children = [
        "SELECT id FROM sub_setop_a WHERE tenant_id = 1 AND id > 1 \
         UNION SELECT id FROM sub_setop_b WHERE tenant_id = 1 ORDER BY id",
        "SELECT id FROM sub_setop_a WHERE tenant_id = 1 \
         INTERSECT SELECT id FROM sub_setop_b WHERE tenant_id = 1 ORDER BY id",
        "SELECT id FROM sub_setop_a WHERE tenant_id = 1 \
         EXCEPT SELECT id FROM sub_setop_b WHERE tenant_id = 1 ORDER BY id",
    ];
    // UNION {2, 4}, INTERSECT {2}, EXCEPT {1}.
    let expected_rows = [2, 1, 1];
    for (child, expected) in children.iter().zip(expected_rows) {
        let m = ctx.metrics().await?;
        let (proxied, origin) = ctx.proxy_and_origin_rows(child).await?;
        assert_eq!(origin.len(), expected, "{child}");
        assert_eq!(proxied, origin, "{child}");
        assert_subsume_hit(&mut ctx, m).await?;
    }

    // A tenant-1 row joining both tables shifts every arm's result in place.
    let m = ctx.metrics().await?;
    ctx.origin_query("INSERT INTO sub_setop_b VALUES (3, 1)", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    let (proxied, origin) = ctx.proxy_and_origin_rows(children[1]).await?;
    assert_eq!(origin.len(), 1, "origin intersect after insert");
    assert_eq!(proxied, origin, "intersect after insert");
    assert_cache_hit(&mut ctx, m).await?;
    Ok(())
}

/// One arm over a relation no parent covers leaves the whole set operation
/// uncovered.
#[tokio::test]
async fn test_subsumption_set_operation_uncovered_arm_not_subsumed() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_set_operation_tables(&mut ctx).await?;
    parent_warm(&mut ctx, "SELECT * FROM sub_setop_a WHERE tenant_id = 1").await?;

    let m = ctx.metrics().await?;
    ctx.simple_query(
        "SELECT id FROM sub_setop_a WHERE tenant_id = 1 \
         UNION SELECT id FROM sub_setop_b WHERE tenant_id = 1",
    )
    .await?;
    let _m = assert_not_subsumed(&mut ctx, m).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Same-relation parents: each exact reference is a parent (PGC-511)
// ---------------------------------------------------------------------------
/// Each arm of a same-table UNION ALL parent is loaded in full, so each is
/// a parent on its own (PGC-511): a child under one arm is subsumed, a child
/// spanning both arms is covered value by value (PGC-494), and a child
/// outside both is not. The covered child then follows CDC.
#[tokio::test]
async fn test_subsumption_same_table_union_arms_each_parent() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_set_operation_tables(&mut ctx).await?;
    parent_warm(
        &mut ctx,
        "SELECT id, tenant_id FROM sub_setop_a WHERE tenant_id = 1 \
         UNION ALL SELECT id, tenant_id FROM sub_setop_a WHERE tenant_id = 2",
    )
    .await?;

    let under_arm = "SELECT id FROM sub_setop_a WHERE tenant_id = 1 AND id > 1 ORDER BY id";
    let m = ctx.metrics().await?;
    let (proxied, origin) = ctx.proxy_and_origin_rows(under_arm).await?;
    assert_eq!(origin.len(), 1, "row 2");
    assert_eq!(proxied, origin);
    assert_subsume_hit(&mut ctx, m).await?;

    let spanning = "SELECT id FROM sub_setop_a WHERE tenant_id IN (1, 2) ORDER BY id";
    let m = ctx.metrics().await?;
    let (proxied, origin) = ctx.proxy_and_origin_rows(spanning).await?;
    assert_eq!(origin.len(), 3, "every row");
    assert_eq!(proxied, origin, "{spanning}");
    assert_subsume_hit(&mut ctx, m).await?;

    let outside = "SELECT id FROM sub_setop_a WHERE tenant_id = 3";
    let m = ctx.metrics().await?;
    let (proxied, origin) = ctx.proxy_and_origin_rows(outside).await?;
    assert_eq!(proxied, origin, "{outside}");
    ctx.cache_settle().await?;
    assert_not_subsumed(&mut ctx, m).await?;

    let m = ctx.metrics().await?;
    ctx.origin_query("INSERT INTO sub_setop_a VALUES (6, 1)", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    let (proxied, origin) = ctx.proxy_and_origin_rows(under_arm).await?;
    assert_eq!(origin.len(), 2, "rows 2 and 6");
    assert_eq!(proxied, origin, "after insert");
    assert_cache_hit(&mut ctx, m).await?;
    Ok(())
}

/// A UNION ALL arm that joins loads only the rows with a partner, so it is
/// no parent, while the single-table arm beside it is.
#[tokio::test]
async fn test_subsumption_union_join_arm_not_parent() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_set_operation_tables(&mut ctx).await?;
    parent_warm(
        &mut ctx,
        "SELECT id FROM sub_setop_a WHERE tenant_id = 1 \
         UNION ALL SELECT a.id FROM sub_setop_a a \
         JOIN sub_setop_b b ON b.id = a.id WHERE a.tenant_id = 2",
    )
    .await?;

    // Row 3 is tenant 2 with no partner in b, so the join arm never loaded it.
    let m = ctx.metrics().await?;
    let (proxied, origin) = ctx
        .proxy_and_origin_rows("SELECT id FROM sub_setop_a WHERE tenant_id = 2 ORDER BY id")
        .await?;
    assert_eq!(origin.len(), 1, "row 3");
    assert_eq!(proxied, origin);
    ctx.cache_settle().await?;
    assert_not_subsumed(&mut ctx, m).await?;

    let m = ctx.metrics().await?;
    let (proxied, origin) = ctx
        .proxy_and_origin_rows(
            "SELECT id FROM sub_setop_a WHERE tenant_id = 1 AND id > 1 ORDER BY id",
        )
        .await?;
    assert_eq!(origin.len(), 1, "row 2");
    assert_eq!(proxied, origin);
    assert_subsume_hit(&mut ctx, m).await?;
    Ok(())
}

/// A table read in an uncorrelated subquery is loaded in full on the
/// subquery's own filters, so that reference parents a child under it; the
/// outer reference, narrowed by the IN, does not. The child then follows CDC
/// on its own.
#[tokio::test]
async fn test_subsumption_subquery_reference_parent() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_set_operation_tables(&mut ctx).await?;
    parent_warm(
        &mut ctx,
        "SELECT id FROM sub_setop_a WHERE tenant_id = 1 \
         AND id IN (SELECT id FROM sub_setop_a WHERE tenant_id = 2)",
    )
    .await?;

    // No tenant-1 id is a tenant-2 id, so the outer reference loaded no row.
    let m = ctx.metrics().await?;
    let (proxied, origin) = ctx
        .proxy_and_origin_rows("SELECT id FROM sub_setop_a WHERE tenant_id = 1 ORDER BY id")
        .await?;
    assert_eq!(origin.len(), 2, "rows 1 and 2");
    assert_eq!(proxied, origin);
    ctx.cache_settle().await?;
    assert_not_subsumed(&mut ctx, m).await?;

    let under_subquery = "SELECT id FROM sub_setop_a WHERE tenant_id = 2 AND id > 0 ORDER BY id";
    let m = ctx.metrics().await?;
    let (proxied, origin) = ctx.proxy_and_origin_rows(under_subquery).await?;
    assert_eq!(origin.len(), 1, "row 3");
    assert_eq!(proxied, origin);
    assert_subsume_hit(&mut ctx, m).await?;

    let m = ctx.metrics().await?;
    ctx.origin_query("INSERT INTO sub_setop_a VALUES (7, 2)", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    let (proxied, origin) = ctx.proxy_and_origin_rows(under_subquery).await?;
    assert_eq!(origin.len(), 2, "rows 3 and 7");
    assert_eq!(proxied, origin, "after insert");
    assert_cache_hit(&mut ctx, m).await?;
    Ok(())
}

/// Point parents registered one literal at a time cover an IN-list child
/// value by value (PGC-494): the child is stamped from their rows, counted
/// as a split subsumption, and follows CDC afterwards; a value no parent
/// holds leaves the child to populate.
#[tokio::test]
async fn test_subsumption_point_parents_cover_in_set_child() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(
        "CREATE TABLE sub_points (id integer PRIMARY KEY, v integer)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO sub_points VALUES (1, 10), (2, 20), (3, 30), (4, 40)",
        &[],
    )
    .await?;
    ctx.cdc_decode_settle().await?;
    for id in 1..=3 {
        parent_warm(
            &mut ctx,
            &format!("SELECT id, v FROM sub_points WHERE id = {id}"),
        )
        .await?;
    }

    let child = "SELECT id, v FROM sub_points WHERE id IN (1, 2, 3) ORDER BY id";
    let m = ctx.metrics().await?;
    let (proxied, origin) = ctx.proxy_and_origin_rows(child).await?;
    assert_eq!(origin.len(), 3);
    assert_eq!(proxied, origin);
    let after = assert_subsume_hit(&mut ctx, m.clone()).await?;
    assert_eq!(
        metrics_delta(&m, &after).cache_subsumptions_split,
        1,
        "covered by splitting the IN list"
    );

    let uncovered = "SELECT id, v FROM sub_points WHERE id IN (1, 4) ORDER BY id";
    let m = ctx.metrics().await?;
    let (proxied, origin) = ctx.proxy_and_origin_rows(uncovered).await?;
    assert_eq!(proxied, origin, "{uncovered}");
    ctx.cache_settle().await?;
    assert_not_subsumed(&mut ctx, m).await?;

    let m = ctx.metrics().await?;
    ctx.origin_query("UPDATE sub_points SET v = 21 WHERE id = 2", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    let (proxied, origin) = ctx.proxy_and_origin_rows(child).await?;
    assert_eq!(proxied, origin, "after update");
    assert_eq!(proxied[1], [Some("2".to_owned()), Some("21".to_owned())]);
    assert_cache_hit(&mut ctx, m).await?;
    Ok(())
}
