use std::io::Error;
use std::time::Duration;

use crate::util::{
    MetricsSnapshot, TestContext, assert_cache_hit, assert_cache_miss, assert_not_subsumed,
    assert_row_at, assert_subsume_hit, metrics_delta, origin_change_invalidates,
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

/// Assert that the last query was subsumed by a cached join of the same
/// shape (PGC-417).
async fn assert_join_subsume_hit(
    ctx: &mut TestContext,
    before: MetricsSnapshot,
) -> Result<MetricsSnapshot, Error> {
    let after = assert_subsume_hit(ctx, before.clone()).await?;
    let delta = metrics_delta(&before, &after);
    assert_eq!(delta.cache_subsumptions_join, 1, "expected a join cover");
    Ok(after)
}

/// Orders and customers for the same-shape join tests: orders 1 and 3 are
/// alice's, order 2 bob's; order 1 is pending.
async fn setup_join_shape_tables(ctx: &mut TestContext) -> Result<(), Error> {
    ctx.query(
        "CREATE TABLE sj_orders (id integer PRIMARY KEY, customer_id integer, status text)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE TABLE sj_customers (id integer PRIMARY KEY, name text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO sj_orders VALUES (1, 1, 'pending'), (2, 2, 'active'), (3, 1, 'active')",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO sj_customers VALUES (1, 'alice'), (2, 'bob')",
        &[],
    )
    .await?;
    ctx.cdc_decode_settle().await?;
    Ok(())
}

const SJ_JOIN: &str =
    "SELECT o.id, c.name FROM sj_orders o JOIN sj_customers c ON o.customer_id = c.id";

/// A cached join subsumes the same join under a narrower WHERE (PGC-417):
/// the parent loaded every FROM row of the shape, so the child's rows are
/// there. A change that shrinks the child applies in place; one that grows
/// it invalidates and repopulates.
#[tokio::test]
async fn test_subsumption_same_shape_join_subsumed() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_join_shape_tables(&mut ctx).await?;

    let m = ctx.metrics().await?;
    ctx.simple_query(SJ_JOIN).await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    let child = format!("{SJ_JOIN} WHERE o.status = 'pending' ORDER BY o.id");
    let (proxied, origin) = ctx.proxy_and_origin_rows(&child).await?;
    assert_eq!(origin.len(), 1, "one pending order");
    assert_eq!(proxied, origin, "subsumed child");
    let m = assert_join_subsume_hit(&mut ctx, m).await?;

    ctx.origin_query("UPDATE sj_orders SET status = 'active' WHERE id = 1", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    let (proxied, origin) = ctx.proxy_and_origin_rows(&child).await?;
    assert!(origin.is_empty(), "no pending order after the update");
    assert_eq!(proxied, origin, "child after the shrinking update");
    let m = assert_cache_hit(&mut ctx, m).await?;

    origin_change_invalidates(
        &mut ctx,
        m,
        "INSERT INTO sj_orders VALUES (4, 2, 'pending')",
        &child,
    )
    .await?;

    Ok(())
}

/// A range and an IN list under a wider range parent are covered, as on a
/// single table.
#[tokio::test]
async fn test_subsumption_same_shape_join_narrower_range_subsumed() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_join_shape_tables(&mut ctx).await?;

    let m = ctx.metrics().await?;
    ctx.simple_query(&format!("{SJ_JOIN} WHERE o.customer_id > 0"))
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    let child =
        format!("{SJ_JOIN} WHERE o.customer_id > 1 AND c.name IN ('bob', 'carol') ORDER BY o.id");
    let (proxied, origin) = ctx.proxy_and_origin_rows(&child).await?;
    assert_eq!(origin.len(), 1, "bob's order");
    assert_eq!(proxied, origin, "subsumed child");
    let _m = assert_join_subsume_hit(&mut ctx, m).await?;

    Ok(())
}

/// A same-shape parent covers only a WHERE that implies its own: another
/// value, or no constraint on the parent's column, is not covered.
#[tokio::test]
async fn test_subsumption_same_shape_join_wider_where_not_subsumed() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_join_shape_tables(&mut ctx).await?;

    let m = ctx.metrics().await?;
    ctx.simple_query(&format!("{SJ_JOIN} WHERE o.status = 'pending'"))
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    for (child, expected) in [
        (
            format!("{SJ_JOIN} WHERE o.status = 'active' ORDER BY o.id"),
            2,
        ),
        (format!("{SJ_JOIN} WHERE c.name = 'alice' ORDER BY o.id"), 2),
    ] {
        let (proxied, origin) = ctx.proxy_and_origin_rows(&child).await?;
        assert_eq!(origin.len(), expected, "{child}");
        assert_eq!(proxied, origin, "{child}");
        assert_not_subsumed(&mut ctx, m.clone()).await?;
        ctx.cache_settle().await?;
    }

    Ok(())
}

/// The shape carries the join type and the ON clause, literals included:
/// a join that differs in either is no child of the parent.
#[tokio::test]
async fn test_subsumption_join_shape_differs_not_subsumed() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_join_shape_tables(&mut ctx).await?;

    let m = ctx.metrics().await?;
    ctx.simple_query(
        "SELECT o.id, c.name FROM sj_orders o JOIN sj_customers c \
         ON o.customer_id = c.id AND o.id > 0",
    )
    .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    for child in [
        "SELECT o.id, c.name FROM sj_orders o JOIN sj_customers c \
         ON o.customer_id = c.id AND o.id > 1 WHERE o.status = 'active' ORDER BY o.id",
        "SELECT o.id, c.name FROM sj_orders o LEFT JOIN sj_customers c \
         ON o.customer_id = c.id AND o.id > 0 WHERE o.status = 'active' ORDER BY o.id",
        "SELECT o.id, c.name FROM sj_customers c JOIN sj_orders o \
         ON o.customer_id = c.id AND o.id > 0 WHERE o.status = 'active' ORDER BY o.id",
    ] {
        let (proxied, origin) = ctx.proxy_and_origin_rows(child).await?;
        assert_eq!(origin.len(), 2, "{child}");
        assert_eq!(proxied, origin, "{child}");
        assert_not_subsumed(&mut ctx, m.clone()).await?;
        ctx.cache_settle().await?;
    }

    Ok(())
}

/// A self-join's WHERE constraints are kept apart by reference position: a
/// constraint the parent put on one reference does not cover the same
/// constraint on the other.
#[tokio::test]
async fn test_subsumption_same_shape_self_join_covered_by_position() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE sj_emp (id integer PRIMARY KEY, name text, manager_id integer, dept text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO sj_emp VALUES (1, 'ceo', NULL, 'exec'), (2, 'alice', 1, 'x'), \
         (3, 'bob', 1, 'x'), (4, 'carl', 2, 'x')",
        &[],
    )
    .await?;
    ctx.cdc_decode_settle().await?;

    const JOIN: &str = "SELECT e.id FROM sj_emp e JOIN sj_emp m ON m.id = e.manager_id";
    let m = ctx.metrics().await?;
    ctx.simple_query(&format!("{JOIN} WHERE e.dept = 'x'"))
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    let child = format!("{JOIN} WHERE e.dept = 'x' AND m.dept = 'exec' ORDER BY e.id");
    let (proxied, origin) = ctx.proxy_and_origin_rows(&child).await?;
    assert_eq!(origin.len(), 2, "alice and bob report to the ceo");
    assert_eq!(proxied, origin, "subsumed child");
    let m = assert_join_subsume_hit(&mut ctx, m).await?;

    let other = format!("{JOIN} WHERE m.dept = 'x' ORDER BY e.id");
    let (proxied, origin) = ctx.proxy_and_origin_rows(&other).await?;
    assert_eq!(origin.len(), 1, "carl reports to alice");
    assert_eq!(proxied, origin, "the other reference's constraint");
    let _m = assert_not_subsumed(&mut ctx, m).await?;

    Ok(())
}

/// A join whose WHERE the model can't read as single-column constraints
/// loaded rows no region describes, so it parents nothing.
#[tokio::test]
async fn test_subsumption_join_parent_opaque_where_not_parent() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_join_shape_tables(&mut ctx).await?;

    let m = ctx.metrics().await?;
    ctx.simple_query(&format!(
        "{SJ_JOIN} WHERE o.status = 'pending' OR c.name = 'bob'"
    ))
    .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    let child = format!("{SJ_JOIN} WHERE o.status = 'pending' AND c.name = 'bob' ORDER BY o.id");
    let (proxied, origin) = ctx.proxy_and_origin_rows(&child).await?;
    assert!(origin.is_empty(), "no pending order of bob's");
    assert_eq!(proxied, origin, "{child}");
    let _m = assert_not_subsumed(&mut ctx, m).await?;

    Ok(())
}

/// A WHERE on the nullable side of a LEFT JOIN is part of the parent's
/// region like any other: a child repeating it is covered, one without it
/// reads NULL-padded rows the parent never loaded. The same constraint in a
/// child's ON instead of its WHERE is another shape, so it is not covered
/// either, though the child's filters on the table would match.
#[tokio::test]
async fn test_subsumption_same_shape_left_join_nullable_where() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE sj_users (id integer PRIMARY KEY, name text)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE TABLE sj_user_orders (id integer PRIMARY KEY, user_id integer, total integer)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO sj_users VALUES (1, 'a'), (2, 'a'), (3, 'b')",
        &[],
    )
    .await?;
    ctx.query("INSERT INTO sj_user_orders VALUES (10, 1, 5)", &[])
        .await?;
    ctx.cdc_decode_settle().await?;

    const LEFT: &str =
        "SELECT u.id, o.total FROM sj_users u LEFT JOIN sj_user_orders o ON o.user_id = u.id";
    let m = ctx.metrics().await?;
    ctx.simple_query(&format!("{LEFT} WHERE o.total IS NULL"))
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    let child = format!("{LEFT} WHERE o.total IS NULL AND u.name = 'a' ORDER BY u.id");
    let (proxied, origin) = ctx.proxy_and_origin_rows(&child).await?;
    assert_eq!(origin.len(), 1, "user 2 has no order");
    assert_eq!(proxied, origin, "subsumed child");
    let m = assert_join_subsume_hit(&mut ctx, m).await?;

    let wider = format!("{LEFT} WHERE u.name = 'b' ORDER BY u.id");
    let (proxied, origin) = ctx.proxy_and_origin_rows(&wider).await?;
    assert_eq!(origin.len(), 1, "user 3, NULL-padded");
    assert_eq!(proxied, origin, "{wider}");
    let m = assert_not_subsumed(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    ctx.simple_query(&format!("{LEFT} WHERE o.total = 5"))
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    let on_child = "SELECT u.id, o.total FROM sj_users u LEFT JOIN sj_user_orders o \
                    ON o.user_id = u.id AND o.total = 5 WHERE u.name = 'a' ORDER BY u.id";
    let (proxied, origin) = ctx.proxy_and_origin_rows(on_child).await?;
    assert_eq!(origin.len(), 2, "both 'a' users, user 2 NULL-padded");
    assert_eq!(proxied, origin, "{on_child}");
    let _m = assert_not_subsumed(&mut ctx, m).await?;

    Ok(())
}

/// A LIMIT caps a plain join's population (ADR-060, PGC-570): the join
/// holds only the rows of its top N, so like any capped query it parents
/// nothing, and a same-shape child populates itself.
#[tokio::test]
async fn test_subsumption_join_limit_parent_does_not_parent() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_join_shape_tables(&mut ctx).await?;

    let m = ctx.metrics().await?;
    ctx.simple_query(&format!("{SJ_JOIN} ORDER BY o.id LIMIT 2"))
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    let child = format!("{SJ_JOIN} WHERE o.status = 'active' ORDER BY o.id");
    let (proxied, origin) = ctx.proxy_and_origin_rows(&child).await?;
    assert_eq!(origin.len(), 2, "{child}");
    assert_eq!(proxied, origin, "{child}");
    let _m = assert_not_subsumed(&mut ctx, m).await?;

    Ok(())
}

/// A growing change invalidates a join and its same-shape children alike;
/// once the parent has repopulated, a child is readmitted from it without
/// a population of its own (PGC-532). Under CLOCK an invalidated entry
/// stays for readmission.
#[tokio::test]
async fn test_subsumption_join_readmitted_from_same_shape_parent() -> Result<(), Error> {
    let mut ctx = TestContext::setup_clock(1).await?;
    setup_join_shape_tables(&mut ctx).await?;

    let parent = format!("{SJ_JOIN} WHERE o.customer_id > 0 ORDER BY o.id");
    let m = ctx.metrics().await?;
    ctx.simple_query(&parent).await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    let child = format!("{SJ_JOIN} WHERE o.customer_id > 0 AND o.status = 'pending' ORDER BY o.id");
    let (proxied, origin) = ctx.proxy_and_origin_rows(&child).await?;
    assert_eq!(origin.len(), 1, "{child}");
    assert_eq!(proxied, origin, "subsumed child");
    let m = assert_join_subsume_hit(&mut ctx, m).await?;

    let before = m.clone();
    origin_change_invalidates(
        &mut ctx,
        m,
        "INSERT INTO sj_orders VALUES (4, 2, 'pending')",
        &parent,
    )
    .await?;

    let m = ctx.metrics().await?;
    let delta = metrics_delta(&before, &m);
    assert_eq!(delta.cache_invalidations, 2, "parent and child invalidated");
    assert_eq!(delta.cache_readmissions, 1, "the parent repopulated");
    let (proxied, origin) = ctx.proxy_and_origin_rows(&child).await?;
    assert_eq!(origin.len(), 2, "{child}");
    assert_eq!(proxied, origin, "readmitted child");
    let after = ctx.metrics_after_outcomes(&m, 1).await?;
    let delta = metrics_delta(&m, &after);
    assert_eq!(delta.queries_cache_hit, 1, "served from the cache");
    assert_eq!(delta.cache_readmissions, 1, "the child readmitted");
    assert_eq!(
        delta.cache_readmission_subsumptions, 1,
        "readmitted from the parent"
    );
    assert_eq!(delta.cache_subsumptions_join, 1, "by its join shape");

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

// =============================================================================
// Waiting on a populating parent (PGC-496)
// =============================================================================

/// `sw_users` 1..8 and `sw_orders` 10 (user 1), 11 (user 6).
async fn setup_wait_tables(ctx: &mut TestContext) -> Result<(), Error> {
    ctx.query(
        "CREATE TABLE sw_users (id INTEGER PRIMARY KEY, name TEXT)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE TABLE sw_orders (id INTEGER PRIMARY KEY, user_id INTEGER)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO sw_users VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), \
         (5, 'e'), (6, 'f'), (7, 'g'), (8, 'h')",
        &[],
    )
    .await?;
    ctx.query("INSERT INTO sw_orders VALUES (10, 1), (11, 6)", &[])
        .await?;
    ctx.cdc_decode_settle().await
}

const SW_USERS_WIDE: &str = "SELECT id, name FROM sw_users WHERE id > 0 ORDER BY id";
const SW_USERS_NARROW: &str = "SELECT id, name FROM sw_users WHERE id > 5 ORDER BY id";
const SW_LEFT_JOIN_WIDE: &str = "SELECT u.id, o.id FROM sw_users u \
    LEFT JOIN sw_orders o ON o.user_id = u.id WHERE u.id > 0 ORDER BY u.id, o.id";
const SW_JOIN_WIDE: &str = "SELECT u.id, o.id FROM sw_users u \
    JOIN sw_orders o ON o.user_id = u.id WHERE u.id > 0 ORDER BY u.id, o.id";
const SW_JOIN_NARROW: &str = "SELECT u.id, o.id FROM sw_users u \
    JOIN sw_orders o ON o.user_id = u.id WHERE u.id > 5 ORDER BY u.id, o.id";

/// A query registered while the parent that covers it is still populating
/// waits for that parent and is stamped from its rows: one population serves
/// both.
#[tokio::test]
async fn test_subsumption_child_waits_on_populating_parent() -> Result<(), Error> {
    let mut ctx =
        TestContext::setup_fault(&[("PGCACHE_FAULT_POPULATION_DELAY_ONCE_MS", "1500")]).await?;
    setup_wait_tables(&mut ctx).await?;

    let m = ctx.metrics().await?;
    ctx.simple_query(SW_USERS_WIDE).await?;
    ctx.simple_query(SW_USERS_NARROW).await?;
    ctx.cache_settle_with_timeout(Duration::from_secs(10))
        .await?;

    let (proxied, origin) = ctx.proxy_and_origin_rows(SW_USERS_NARROW).await?;
    assert_eq!(proxied, origin, "the child serves origin's rows");
    let delta = metrics_delta(&m, &ctx.metrics_after_outcomes(&m, 3).await?);
    assert_eq!(delta.queries_cache_miss, 2, "both registrations miss");
    assert_eq!(delta.queries_cache_hit, 1, "the child's read is a hit");
    assert_eq!(delta.cache_subsumption_waits, 1, "the child waited");
    assert_eq!(delta.cache_subsumptions, 1, "the child was stamped");
    assert_eq!(delta.cache_subsumption_wait_fallbacks, 0);
    Ok(())
}

/// The parent is invalidated while the child waits on it: the child is
/// released to its own population. A users insert invalidates the LEFT JOIN
/// parent but applies in place to the single-table child.
#[tokio::test]
async fn test_subsumption_wait_parent_invalidated_falls_back() -> Result<(), Error> {
    let mut ctx =
        TestContext::setup_fault(&[("PGCACHE_FAULT_POPULATION_DELAY_ONCE_MS", "1500")]).await?;
    setup_wait_tables(&mut ctx).await?;

    let m = ctx.metrics().await?;
    ctx.simple_query(SW_LEFT_JOIN_WIDE).await?;
    ctx.simple_query(SW_USERS_NARROW).await?;
    ctx.origin_query("INSERT INTO sw_users VALUES (9, 'i')", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    ctx.cache_settle_with_timeout(Duration::from_secs(10))
        .await?;

    let (proxied, origin) = ctx.proxy_and_origin_rows(SW_USERS_NARROW).await?;
    assert_eq!(proxied, origin, "the child serves origin's rows");
    assert_eq!(origin.len(), 4, "users 6, 7, 8 and 9");
    let delta = metrics_delta(&m, &ctx.metrics_after_outcomes(&m, 3).await?);
    assert_eq!(delta.queries_cache_hit, 1, "the child's read is a hit");
    assert_eq!(delta.cache_subsumption_waits, 1, "the child waited");
    assert_eq!(
        delta.cache_subsumption_wait_fallbacks, 1,
        "the child populated itself"
    );
    assert_eq!(delta.cache_subsumptions, 0);
    Ok(())
}

/// A readmitted query waits on its parent's readmission too: after both
/// joins are invalidated, the child read behind the parent's repopulation is
/// stamped from it once it lands.
#[tokio::test]
async fn test_subsumption_wait_through_parent_readmission() -> Result<(), Error> {
    let mut ctx =
        TestContext::setup_fault_clock_admitting(&[("PGCACHE_FAULT_POPULATION_DELAY_MS", "1500")])
            .await?;
    setup_wait_tables(&mut ctx).await?;

    ctx.simple_query(SW_JOIN_WIDE).await?;
    ctx.cache_settle_with_timeout(Duration::from_secs(10))
        .await?;
    let m = ctx.metrics().await?;
    ctx.simple_query(SW_JOIN_NARROW).await?;
    let m = assert_subsume_hit(&mut ctx, m).await?;

    ctx.origin_query("INSERT INTO sw_orders VALUES (12, 7)", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    ctx.simple_query(SW_JOIN_WIDE).await?;
    ctx.simple_query(SW_JOIN_NARROW).await?;
    ctx.cache_settle_with_timeout(Duration::from_secs(10))
        .await?;

    let (proxied, origin) = ctx.proxy_and_origin_rows(SW_JOIN_NARROW).await?;
    assert_eq!(proxied, origin, "the child serves origin's rows");
    assert_eq!(origin.len(), 2, "orders 11 and 12");
    let delta = metrics_delta(&m, &ctx.metrics_after_outcomes(&m, 3).await?);
    assert_eq!(delta.queries_cache_hit, 1, "the child's read is a hit");
    assert_eq!(delta.cache_readmissions, 2, "both readmitted");
    assert_eq!(delta.cache_subsumption_waits, 1, "the child waited");
    assert_eq!(delta.cache_subsumptions, 1, "the child was stamped");
    assert_eq!(delta.cache_subsumption_wait_fallbacks, 0);
    Ok(())
}

/// A child invalidated while it waits is left alone at the release; its
/// next read readmits it, and by then both parents are Ready.
#[tokio::test]
async fn test_subsumption_waiting_child_invalidated_readmits() -> Result<(), Error> {
    let mut ctx =
        TestContext::setup_fault_clock_admitting(&[("PGCACHE_FAULT_POPULATION_DELAY_MS", "1500")])
            .await?;
    setup_wait_tables(&mut ctx).await?;

    ctx.simple_query("SELECT id, user_id FROM sw_orders")
        .await?;
    ctx.cache_settle_with_timeout(Duration::from_secs(10))
        .await?;

    let m = ctx.metrics().await?;
    ctx.simple_query(SW_USERS_WIDE).await?;
    ctx.simple_query(SW_JOIN_NARROW).await?;
    ctx.origin_query("INSERT INTO sw_orders VALUES (13, 8)", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    ctx.cache_settle_with_timeout(Duration::from_secs(10))
        .await?;

    let (proxied, origin) = ctx.proxy_and_origin_rows(SW_JOIN_NARROW).await?;
    assert_eq!(proxied, origin, "the child serves origin's rows");
    assert_eq!(origin.len(), 2, "orders 11 and 13");
    let delta = metrics_delta(&m, &ctx.metrics_after_outcomes(&m, 3).await?);
    assert_eq!(delta.queries_cache_hit, 1, "the child's read is a hit");
    assert_eq!(delta.cache_subsumption_waits, 1, "the child waited");
    assert_eq!(delta.cache_subsumption_wait_fallbacks, 0);
    assert_eq!(delta.cache_readmissions, 1, "the child readmitted");
    assert_eq!(
        delta.cache_readmission_subsumptions, 1,
        "the readmission was stamped"
    );
    Ok(())
}
