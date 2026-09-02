use std::io::Error;

use crate::util::{TestContext, assert_cache_hit, assert_cache_miss, assert_row_at};

mod util;

/// Test admission gating with threshold=2.
/// First miss → Pending(1), second miss → Loading (population), third query → cache hit.
#[tokio::test]
async fn test_clock_admission_threshold_2() -> Result<(), Error> {
    let mut ctx = TestContext::setup_clock(2).await?;

    ctx.query(
        "CREATE TABLE test_admit (id INTEGER PRIMARY KEY, data TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO test_admit (id, data) VALUES (1, 'foo'), (2, 'bar')",
        &[],
    )
    .await?;

    let query_str = "SELECT id, data FROM test_admit WHERE data = 'foo'";

    // First query — cache miss, enters Pending(1) state
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query_str).await?;
    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("data", "foo")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    // Second query — still a cache miss, but promotes Pending(1) → Loading
    let res = ctx.simple_query(query_str).await?;
    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("data", "foo")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Third query — cache hit (population completed)
    let res = ctx.simple_query(query_str).await?;
    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("data", "foo")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test that threshold=1 admits immediately on first miss (like FIFO behavior).
#[tokio::test]
async fn test_clock_admission_threshold_1() -> Result<(), Error> {
    let mut ctx = TestContext::setup_clock(1).await?;

    ctx.query(
        "CREATE TABLE test_admit1 (id INTEGER PRIMARY KEY, data TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO test_admit1 (id, data) VALUES (1, 'foo'), (2, 'bar')",
        &[],
    )
    .await?;

    let query_str = "SELECT id, data FROM test_admit1 WHERE data = 'foo'";

    // First query — cache miss, admitted immediately (threshold=1)
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query_str).await?;
    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("data", "foo")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Second query — cache hit
    let res = ctx.simple_query(query_str).await?;
    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("data", "foo")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test fast readmission after CDC invalidation.
/// With CLOCK policy, an invalidated query is readmitted on the next miss
/// without re-proving demand (skips admission gate).
/// Uses a JOIN query because UPDATEs on join tables reliably trigger invalidation,
/// whereas direct (non-join) queries handle CDC events in place.
#[tokio::test]
async fn test_clock_fast_readmission() -> Result<(), Error> {
    let mut ctx = TestContext::setup_clock(2).await?;

    ctx.query(
        "CREATE TABLE test_readmit (id INTEGER PRIMARY KEY, data TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE test_readmit_map (id SERIAL PRIMARY KEY, test_id INTEGER, data TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO test_readmit (id, data) VALUES (1, 'foo'), (2, 'bar')",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO test_readmit_map (test_id, data) VALUES (1, 'alpha'), (1, 'beta')",
        &[],
    )
    .await?;

    ctx.cdc_decode_settle().await?;

    let query_str = "SELECT t.id, t.data, tm.data AS map_data \
        FROM test_readmit t JOIN test_readmit_map tm ON tm.test_id = t.id \
        WHERE t.id = 1 ORDER BY tm.id";

    // First miss → Pending(1)
    let m = ctx.metrics().await?;
    let _ = ctx.simple_query(query_str).await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    // Second miss → Loading (admitted)
    let _ = ctx.simple_query(query_str).await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Cache hit — query is Ready
    let _ = ctx.simple_query(query_str).await?;
    let m = assert_cache_hit(&mut ctx, m).await?;

    // CDC UPDATE on the join table's join column → invalidation
    // A row with test_id=2 enters the result set (WHERE t.id = 1 won't match,
    // but changing test_id from 2→1 makes it join into the cached query)
    ctx.origin_query(
        "INSERT INTO test_readmit_map (test_id, data) VALUES (2, 'gamma')",
        &[],
    )
    .await?;

    ctx.cdc_apply_settle().await?;

    // gamma (test_id=2) is not in the cached result — update it to enter
    ctx.origin_query(
        "UPDATE test_readmit_map SET test_id = 1 WHERE data = 'gamma'",
        &[],
    )
    .await?;

    ctx.cdc_apply_settle().await?;

    // Next query — cache miss, but triggers fast readmission (skips admission gate)
    let res = ctx.simple_query(query_str).await?;
    // 3 rows now: alpha, beta, gamma all with test_id=1
    assert_eq!(res.len(), 5);
    assert_row_at(
        &res,
        1,
        &[("id", "1"), ("data", "foo"), ("map_data", "alpha")],
    )?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Query after readmission population — cache hit
    let res = ctx.simple_query(query_str).await?;
    assert_eq!(res.len(), 5);
    assert_row_at(
        &res,
        1,
        &[("id", "1"), ("data", "foo"), ("map_data", "alpha")],
    )?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}
