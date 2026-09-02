use std::io::Error;

use crate::util::{TestContext, assert_cache_hit, assert_cache_miss, assert_row_at};

mod util;

/// Test that a query with a greater-than inequality in WHERE is cacheable.
/// Verifies cache miss on first run and cache hit on second.
#[tokio::test]
async fn test_inequality_greater_than_cacheable() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table test_gt (id integer primary key, value integer, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_gt (id, value, data) values \
         (1, 10, 'low'), (2, 50, 'mid'), (3, 90, 'high')",
        &[],
    )
    .await?;

    let query_str = "select id, value, data from test_gt where value > 20 order by id";

    // First query — cache miss
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query_str).await?;

    // RowDescription + 2 data rows + CommandComplete = 4
    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("id", "2"), ("value", "50"), ("data", "mid")])?;
    assert_row_at(&res, 2, &[("id", "3"), ("value", "90"), ("data", "high")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Second query — cache hit
    let res = ctx.simple_query(query_str).await?;

    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("id", "2"), ("value", "50"), ("data", "mid")])?;
    assert_row_at(&res, 2, &[("id", "3"), ("value", "90"), ("data", "high")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test that INSERT on a join table with a row matching the inequality constraint
/// properly invalidates the cache.
/// Cached query: score > 50 on the child table joined to parent.
/// Insert child with score=80 → should trigger invalidation and appear in results.
#[tokio::test]
async fn test_inequality_join_insert_matching() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table ij_ins_m_parent (id integer primary key, name text)",
        &[],
    )
    .await?;

    ctx.query(
        "create table ij_ins_m_child (id serial primary key, parent_id integer, score integer)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into ij_ins_m_parent (id, name) values (1, 'alice'), (2, 'bob')",
        &[],
    )
    .await?;

    ctx.query(
        "insert into ij_ins_m_child (parent_id, score) values (1, 80), (2, 30)",
        &[],
    )
    .await?;

    let query_str = "select c.id, c.parent_id, c.score, p.name \
                     from ij_ins_m_child c \
                     join ij_ins_m_parent p on c.parent_id = p.id \
                     where c.score > 50 \
                     order by c.id";

    // Prime cache — only row with score=80 matches
    let _ = ctx.simple_query(query_str).await?;

    // Insert child row that matches constraint (score=90 > 50)
    ctx.origin_query(
        "insert into ij_ins_m_child (parent_id, score) values (2, 90)",
        &[],
    )
    .await?;

    ctx.cdc_apply_settle().await?;

    // Query should now show both rows
    let res = ctx.simple_query(query_str).await?;

    // RowDescription + 2 data rows + CommandComplete = 4
    assert_eq!(res.len(), 4);
    assert_row_at(
        &res,
        1,
        &[("parent_id", "1"), ("score", "80"), ("name", "alice")],
    )?;
    assert_row_at(
        &res,
        2,
        &[("parent_id", "2"), ("score", "90"), ("name", "bob")],
    )?;

    Ok(())
}

/// Test that INSERT on a join table with a row NOT matching the inequality constraint
/// does not invalidate the cache (optimization).
/// Cached query: score > 50. Insert child with score=10 → should not invalidate.
#[tokio::test]
async fn test_inequality_join_insert_not_matching() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table ij_ins_nm_parent (id integer primary key, name text)",
        &[],
    )
    .await?;

    ctx.query(
        "create table ij_ins_nm_child (id serial primary key, parent_id integer, score integer)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into ij_ins_nm_parent (id, name) values (1, 'alice'), (2, 'bob')",
        &[],
    )
    .await?;

    ctx.query(
        "insert into ij_ins_nm_child (parent_id, score) values (1, 80), (1, 90)",
        &[],
    )
    .await?;

    ctx.cdc_decode_settle().await?;

    let query_str = "select c.id, c.parent_id, c.score, p.name \
                     from ij_ins_nm_child c \
                     join ij_ins_nm_parent p on c.parent_id = p.id \
                     where c.score > 50 \
                     order by c.id";

    // Prime cache
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query_str).await?;

    assert_eq!(res.len(), 4);
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;
    ctx.cache_settle().await?;

    // Verify cache hit
    let _ = ctx.simple_query(query_str).await?;
    let m = assert_cache_hit(&mut ctx, m).await?;

    // Insert child row that does NOT match constraint (score=10 <= 50)
    ctx.origin_query(
        "insert into ij_ins_nm_child (parent_id, score) values (2, 10)",
        &[],
    )
    .await?;

    ctx.cdc_apply_settle().await?;

    // Should still be a cache hit — row doesn't match inequality constraint
    let res = ctx.simple_query(query_str).await?;

    assert_eq!(res.len(), 4);
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test UPDATE on a join table where a row enters the inequality range.
/// Cached query: score > 50. Update child score from 10 to 80 → should invalidate.
#[tokio::test]
async fn test_inequality_join_update_entering_range() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table ij_upd_enter_parent (id integer primary key, name text)",
        &[],
    )
    .await?;

    ctx.query(
        "create table ij_upd_enter_child (id serial primary key, parent_id integer, score integer)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into ij_upd_enter_parent (id, name) values (1, 'alice'), (2, 'bob')",
        &[],
    )
    .await?;

    ctx.query(
        "insert into ij_upd_enter_child (parent_id, score) values \
         (1, 80), (2, 10)",
        &[],
    )
    .await?;

    let query_str = "select c.id, c.parent_id, c.score, p.name \
                     from ij_upd_enter_child c \
                     join ij_upd_enter_parent p on c.parent_id = p.id \
                     where c.score > 50 \
                     order by c.id";

    // Prime cache — only row with score=80 matches
    let _ = ctx.simple_query(query_str).await?;

    // Update child row score from 10 to 80 (entering the range)
    ctx.origin_query(
        "update ij_upd_enter_child set score = 80 where parent_id = 2",
        &[],
    )
    .await?;

    ctx.cdc_apply_settle().await?;

    // Query should now show both rows
    let res = ctx.simple_query(query_str).await?;

    // RowDescription + 2 data rows + CommandComplete = 4
    assert_eq!(res.len(), 4);
    assert_row_at(
        &res,
        1,
        &[("parent_id", "1"), ("score", "80"), ("name", "alice")],
    )?;
    assert_row_at(
        &res,
        2,
        &[("parent_id", "2"), ("score", "80"), ("name", "bob")],
    )?;

    Ok(())
}

/// Test UPDATE on a join table where a row leaves the inequality range.
/// Cached query: score > 50. Update child score from 80 to 10 → row should disappear.
#[tokio::test]
async fn test_inequality_join_update_leaving_range() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table ij_upd_leave_parent (id integer primary key, name text)",
        &[],
    )
    .await?;

    ctx.query(
        "create table ij_upd_leave_child (id serial primary key, parent_id integer, score integer)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into ij_upd_leave_parent (id, name) values (1, 'alice'), (2, 'bob')",
        &[],
    )
    .await?;

    ctx.query(
        "insert into ij_upd_leave_child (parent_id, score) values \
         (1, 80), (2, 90)",
        &[],
    )
    .await?;

    ctx.cdc_decode_settle().await?;

    let query_str = "select c.id, c.parent_id, c.score, p.name \
                     from ij_upd_leave_child c \
                     join ij_upd_leave_parent p on c.parent_id = p.id \
                     where c.score > 50 \
                     order by c.id";

    // Prime cache — both rows match
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query_str).await?;

    assert_eq!(res.len(), 4);
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;
    ctx.cache_settle().await?;

    // Verify cache hit
    let _ = ctx.simple_query(query_str).await?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    // Update child row score from 90 to 10 (leaving the range)
    ctx.origin_query(
        "update ij_upd_leave_child set score = 10 where parent_id = 2",
        &[],
    )
    .await?;

    ctx.cdc_apply_settle().await?;

    // Query should now show only 1 row
    let res = ctx.simple_query(query_str).await?;

    // RowDescription + 1 data row + CommandComplete = 3
    assert_eq!(res.len(), 3);
    assert_row_at(
        &res,
        1,
        &[("parent_id", "1"), ("score", "80"), ("name", "alice")],
    )?;

    Ok(())
}

/// Test BETWEEN on a join query — caching and CDC constraint optimization.
/// Cached query: score BETWEEN 30 AND 70. Insert inside and outside the range.
#[tokio::test]
async fn test_between_join_insert() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table ij_btw_parent (id integer primary key, name text)",
        &[],
    )
    .await?;

    ctx.query(
        "create table ij_btw_child (id serial primary key, parent_id integer, score integer)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into ij_btw_parent (id, name) values (1, 'alice'), (2, 'bob')",
        &[],
    )
    .await?;

    ctx.query(
        "insert into ij_btw_child (parent_id, score) values (1, 50), (2, 10)",
        &[],
    )
    .await?;

    ctx.cdc_decode_settle().await?;

    let query_str = "select c.id, c.parent_id, c.score, p.name \
                     from ij_btw_child c \
                     join ij_btw_parent p on c.parent_id = p.id \
                     where c.score between 30 and 70 \
                     order by c.id";

    // Prime cache — only row with score=50 matches
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query_str).await?;

    // RowDescription + 1 data row + CommandComplete = 3
    assert_eq!(res.len(), 3);
    assert_row_at(
        &res,
        1,
        &[("parent_id", "1"), ("score", "50"), ("name", "alice")],
    )?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;
    ctx.cache_settle().await?;

    // Verify cache hit
    let _ = ctx.simple_query(query_str).await?;
    let m = assert_cache_hit(&mut ctx, m).await?;

    // Insert row outside range (score=90, above 70) — should NOT invalidate
    ctx.origin_query(
        "insert into ij_btw_child (parent_id, score) values (2, 90)",
        &[],
    )
    .await?;

    ctx.cdc_apply_settle().await?;

    let res = ctx.simple_query(query_str).await?;

    assert_eq!(res.len(), 3);
    let m = assert_cache_hit(&mut ctx, m).await?;

    // Insert row inside range (score=60, between 30 and 70) — should invalidate
    ctx.origin_query(
        "insert into ij_btw_child (parent_id, score) values (2, 60)",
        &[],
    )
    .await?;

    ctx.cdc_apply_settle().await?;

    let res = ctx.simple_query(query_str).await?;

    // RowDescription + 2 data rows + CommandComplete = 4
    assert_eq!(res.len(), 4);
    assert_row_at(
        &res,
        1,
        &[("parent_id", "1"), ("score", "50"), ("name", "alice")],
    )?;
    assert_row_at(
        &res,
        2,
        &[("parent_id", "2"), ("score", "60"), ("name", "bob")],
    )?;

    // Ensure this last query wasn't served from stale cache
    let _m = assert_cache_miss(&mut ctx, m).await?;

    Ok(())
}
