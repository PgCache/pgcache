use std::io::Error;

use crate::util::{TestContext, assert_cache_hit, assert_cache_miss, assert_row_at, metrics_delta};

mod util;

/// Test that a query with an immutable function (lower) in WHERE is cacheable.
/// `lower()` is immutable in PostgreSQL — same input always produces same output.
#[tokio::test]
async fn test_immutable_function_in_where_cacheable() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table test_imm_where (id integer primary key, name text)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_imm_where (id, name) values \
         (1, 'Alice'), (2, 'Bob'), (3, 'ALICE')",
        &[],
    )
    .await?;

    let query_str = "select id, name from test_imm_where where lower(name) = 'alice' order by id";

    // First query — cache miss
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query_str).await?;

    // RowDescription + 2 data rows + CommandComplete = 4
    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("id", "1"), ("name", "Alice")])?;
    assert_row_at(&res, 2, &[("id", "3"), ("name", "ALICE")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Second query — cache hit
    let res = ctx.simple_query(query_str).await?;

    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("id", "1"), ("name", "Alice")])?;
    assert_row_at(&res, 2, &[("id", "3"), ("name", "ALICE")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test that a query with a stable function (now) in WHERE is NOT cacheable.
/// `now()` is stable — same result within a transaction, but differs across transactions.
#[tokio::test]
async fn test_stable_function_in_where_not_cacheable() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table test_stable_where (id integer primary key, created_at timestamptz)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_stable_where (id, created_at) values \
         (1, '2020-01-01'), (2, '2099-01-01')",
        &[],
    )
    .await?;

    let query_str = "select id from test_stable_where where created_at < now() order by id";

    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query_str).await?;

    // Should return row 1 (past date) — result is correct via origin passthrough
    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1")])?;

    // Verify it was counted as uncacheable, not cacheable
    let after = ctx.metrics().await?;
    let delta = metrics_delta(&m, &after);
    assert_eq!(
        delta.queries_uncacheable, 1,
        "stable function in WHERE should be uncacheable"
    );
    assert_eq!(
        delta.queries_cacheable, 0,
        "should not be counted as cacheable"
    );

    Ok(())
}

/// Test that functions in SELECT list are always allowed regardless of volatility.
/// Even stable/volatile functions like now() in SELECT should not prevent caching
/// when the WHERE clause is otherwise cacheable.
#[tokio::test]
async fn test_function_in_select_always_allowed() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table test_fn_select (id integer primary key, name text)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_fn_select (id, name) values (1, 'alice'), (2, 'bob')",
        &[],
    )
    .await?;

    // upper() in SELECT, equality in WHERE — should be cacheable
    let query_str =
        "select id, upper(name) as uname from test_fn_select where name = 'alice' order by id";

    // First query — cache miss
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query_str).await?;

    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("uname", "ALICE")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Second query — cache hit
    let res = ctx.simple_query(query_str).await?;

    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("uname", "ALICE")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test that an immutable function in WHERE works correctly with CDC.
/// Cache a query with lower() in WHERE, then INSERT a matching row via origin
/// and verify it appears in subsequent results.
#[tokio::test]
async fn test_immutable_function_cdc_insert() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table test_imm_cdc (id integer primary key, name text, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_imm_cdc (id, name, data) values \
         (1, 'Alice', 'first'), (2, 'Bob', 'second')",
        &[],
    )
    .await?;

    let query_str =
        "select id, name, data from test_imm_cdc where lower(name) = 'alice' order by id";

    // Prime the cache — cache miss
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query_str).await?;

    assert_eq!(res.len(), 3);
    assert_row_at(
        &res,
        1,
        &[("id", "1"), ("name", "Alice"), ("data", "first")],
    )?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Verify cache hit
    let _ = ctx.simple_query(query_str).await?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    // Insert via origin — new row matching lower(name) = 'alice'
    ctx.origin_query(
        "insert into test_imm_cdc (id, name, data) values (3, 'ALICE', 'third')",
        &[],
    )
    .await?;

    ctx.cdc_apply_settle().await?;

    // Query should now include the new row
    let res = ctx.simple_query(query_str).await?;

    // RowDescription + 2 data rows + CommandComplete = 4
    assert_eq!(res.len(), 4);
    assert_row_at(
        &res,
        1,
        &[("id", "1"), ("name", "Alice"), ("data", "first")],
    )?;
    assert_row_at(
        &res,
        2,
        &[("id", "3"), ("name", "ALICE"), ("data", "third")],
    )?;

    Ok(())
}

/// Test immutable function in WHERE on a JOIN query with CDC.
/// Uses an equality constraint on the child table alongside the function
/// on the parent, so the CDC constraint system can extract the child constraint
/// and handle in-place mutations correctly.
#[tokio::test]
async fn test_immutable_function_join_cdc() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table imm_join_parent (id integer primary key, category text)",
        &[],
    )
    .await?;

    ctx.query(
        "create table imm_join_child (id serial primary key, parent_id integer, value integer)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into imm_join_parent (id, category) values (1, 'Active'), (2, 'Inactive')",
        &[],
    )
    .await?;

    ctx.query(
        "insert into imm_join_child (parent_id, value) values \
         (1, 100), (1, 200), (2, 300)",
        &[],
    )
    .await?;

    ctx.cdc_decode_settle().await?;

    // Equality constraint on child (parent_id = 1) gives CDC something to extract.
    // The immutable function on the parent is an additional filter.
    let query_str = "select c.id, c.parent_id, c.value, p.category \
                     from imm_join_child c \
                     join imm_join_parent p on c.parent_id = p.id \
                     where p.id = 1 and lower(p.category) = 'active' \
                     order by c.id";

    // Prime cache — parent 1 matches both constraints (2 child rows)
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query_str).await?;

    // RowDescription + 2 data rows + CommandComplete = 4
    assert_eq!(res.len(), 4);
    assert_row_at(
        &res,
        1,
        &[("parent_id", "1"), ("value", "100"), ("category", "Active")],
    )?;
    assert_row_at(
        &res,
        2,
        &[("parent_id", "1"), ("value", "200"), ("category", "Active")],
    )?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;
    ctx.cache_settle().await?;

    // Verify cache hit
    let _ = ctx.simple_query(query_str).await?;
    let m = assert_cache_hit(&mut ctx, m).await?;

    // Insert a child row for parent 2 — does not match parent_id = 1 constraint,
    // so cache should NOT be invalidated
    ctx.origin_query(
        "insert into imm_join_child (parent_id, value) values (2, 400)",
        &[],
    )
    .await?;

    ctx.cdc_apply_settle().await?;

    // Should still be a cache hit — row doesn't match child constraint
    let res = ctx.simple_query(query_str).await?;

    assert_eq!(res.len(), 4);
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test that nested immutable functions in WHERE are cacheable.
/// e.g., trim(lower(name)) = 'alice'
#[tokio::test]
async fn test_nested_immutable_functions_cacheable() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table test_nested_fn (id integer primary key, name text)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_nested_fn (id, name) values \
         (1, '  Alice  '), (2, 'Bob'), (3, 'charlie')",
        &[],
    )
    .await?;

    let query_str =
        "select id, name from test_nested_fn where trim(lower(name)) = 'alice' order by id";

    // First query — cache miss
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query_str).await?;

    // RowDescription + 1 data row + CommandComplete = 3
    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("name", "  Alice  ")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Second query — cache hit
    let res = ctx.simple_query(query_str).await?;

    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("name", "  Alice  ")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}
