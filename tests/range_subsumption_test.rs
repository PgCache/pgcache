use std::io::Error;

use crate::util::{
    TestContext, assert_cache_hit, assert_cache_miss, assert_not_subsumed, assert_row_at,
    assert_subsume_hit,
};

mod util;

/// Range subsumption: cached WHERE value > 50 covers WHERE value > 100.
/// Also verifies correct data is returned from the subsumed query.
#[tokio::test]
async fn test_range_subsumption_tighter_lower() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_rsub_lower (id integer PRIMARY KEY, value integer)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_rsub_lower VALUES (1, 25), (2, 75), (3, 150), (4, 200)",
        &[],
    )
    .await?;

    // Cache wide range
    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM test_rsub_lower WHERE value > 50")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Narrower range — subsumed
    let res = ctx
        .simple_query("SELECT id, value FROM test_rsub_lower WHERE value > 100 ORDER BY id")
        .await?;
    let m = assert_subsume_hit(&mut ctx, m).await?;
    assert_eq!(res.len(), 4); // RowDescription + 2 rows + CommandComplete
    assert_row_at(&res, 1, &[("id", "3"), ("value", "150")])?;
    assert_row_at(&res, 2, &[("id", "4"), ("value", "200")])?;

    // Verify cache hit on repeat
    ctx.simple_query("SELECT id, value FROM test_rsub_lower WHERE value > 100 ORDER BY id")
        .await?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Range subsumption: cached WHERE value BETWEEN 10 AND 200 covers WHERE value >= 50 AND value <= 150.
#[tokio::test]
async fn test_range_subsumption_between_containment() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_rsub_between (id integer PRIMARY KEY, value integer)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_rsub_between VALUES (1, 5), (2, 50), (3, 100), (4, 150), (5, 250)",
        &[],
    )
    .await?;

    // Cache wide range
    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM test_rsub_between WHERE value BETWEEN 10 AND 200")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Narrower range — subsumed
    let res = ctx
        .simple_query(
            "SELECT id, value FROM test_rsub_between WHERE value >= 50 AND value <= 150 ORDER BY id",
        )
        .await?;
    let m = assert_subsume_hit(&mut ctx, m).await?;
    assert_eq!(res.len(), 5); // RowDescription + 3 rows + CommandComplete
    assert_row_at(&res, 1, &[("id", "2"), ("value", "50")])?;
    assert_row_at(&res, 2, &[("id", "3"), ("value", "100")])?;
    assert_row_at(&res, 3, &[("id", "4"), ("value", "150")])?;

    // Wider range — NOT subsumed (goes below cached lower bound)
    ctx.simple_query("SELECT * FROM test_rsub_between WHERE value >= 1 AND value <= 150")
        .await?;
    let _m = assert_not_subsumed(&mut ctx, m).await?;

    Ok(())
}

/// Range subsumption: cached WHERE value > 50 covers WHERE value = 100 (point in range).
#[tokio::test]
async fn test_range_subsumption_point_in_range() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_rsub_point (id integer PRIMARY KEY, value integer)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_rsub_point VALUES (1, 25), (2, 75), (3, 100)",
        &[],
    )
    .await?;

    // Cache range
    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM test_rsub_point WHERE value > 50")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Equality within range — subsumed
    let res = ctx
        .simple_query("SELECT id, value FROM test_rsub_point WHERE value = 100")
        .await?;
    let _m = assert_subsume_hit(&mut ctx, m).await?;
    assert_eq!(res.len(), 3); // RowDescription + 1 row + CommandComplete
    assert_row_at(&res, 1, &[("id", "3"), ("value", "100")])?;

    Ok(())
}

/// Range subsumption with CDC: cached range query, INSERT a matching row via origin.
/// Cache should be invalidated and re-populated with the new row.
#[tokio::test]
async fn test_range_subsumption_cdc_insert() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_rsub_cdc (id integer PRIMARY KEY, value integer)",
        &[],
    )
    .await?;
    ctx.query("INSERT INTO test_rsub_cdc VALUES (1, 100), (2, 200)", &[])
        .await?;

    // Cache range
    let m = ctx.metrics().await?;
    let res = ctx
        .simple_query("SELECT id, value FROM test_rsub_cdc WHERE value > 50 ORDER BY id")
        .await?;
    assert_eq!(res.len(), 4); // 2 rows
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Cache hit
    ctx.simple_query("SELECT id, value FROM test_rsub_cdc WHERE value > 50 ORDER BY id")
        .await?;
    let m = assert_cache_hit(&mut ctx, m).await?;

    // INSERT a matching row via origin → CDC in-place update (single-table, never invalidated)
    ctx.origin_query("INSERT INTO test_rsub_cdc VALUES (3, 300)", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    // Should see all 3 rows — served from cache with in-place update
    let res = ctx
        .simple_query("SELECT id, value FROM test_rsub_cdc WHERE value > 50 ORDER BY id")
        .await?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    assert_eq!(res.len(), 5); // 3 rows
    assert_row_at(&res, 1, &[("id", "1"), ("value", "100")])?;
    assert_row_at(&res, 2, &[("id", "2"), ("value", "200")])?;
    assert_row_at(&res, 3, &[("id", "3"), ("value", "300")])?;

    Ok(())
}

/// IN-set subsumption: cached WHERE id IN (1,2,3,4,5) covers WHERE id IN (2,3).
#[tokio::test]
async fn test_in_set_subsumption_subset() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_insub (id integer PRIMARY KEY, data text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_insub VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')",
        &[],
    )
    .await?;

    // Cache wide IN-set
    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM test_insub WHERE id IN (1, 2, 3, 4, 5)")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Subset IN-set — subsumed
    let res = ctx
        .simple_query("SELECT id, data FROM test_insub WHERE id IN (2, 3) ORDER BY id")
        .await?;
    let m = assert_subsume_hit(&mut ctx, m).await?;
    assert_eq!(res.len(), 4); // 2 rows
    assert_row_at(&res, 1, &[("id", "2"), ("data", "b")])?;
    assert_row_at(&res, 2, &[("id", "3"), ("data", "c")])?;

    // Equality within set — also subsumed
    let res = ctx
        .simple_query("SELECT id, data FROM test_insub WHERE id = 4")
        .await?;
    let _m = assert_subsume_hit(&mut ctx, m).await?;
    assert_eq!(res.len(), 3); // 1 row
    assert_row_at(&res, 1, &[("id", "4"), ("data", "d")])?;

    Ok(())
}

/// IN-set NOT subsumed when new query has values outside cached set.
#[tokio::test]
async fn test_in_set_subsumption_not_subset() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_insub_no (id integer PRIMARY KEY, data text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_insub_no VALUES (1, 'a'), (2, 'b'), (3, 'c'), (10, 'x')",
        &[],
    )
    .await?;

    // Cache narrow IN-set
    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM test_insub_no WHERE id IN (1, 2, 3)")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // IN-set with value outside cached set — NOT subsumed
    ctx.simple_query("SELECT * FROM test_insub_no WHERE id IN (1, 10)")
        .await?;
    let _m = assert_not_subsumed(&mut ctx, m).await?;

    Ok(())
}

/// Unconstrained query subsumes IN-set query.
#[tokio::test]
async fn test_unconstrained_subsumes_in_set() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_uncon_in (id integer PRIMARY KEY, data text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_uncon_in VALUES (1, 'a'), (2, 'b'), (3, 'c')",
        &[],
    )
    .await?;

    // Cache full table
    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM test_uncon_in").await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // IN-set query — subsumed by full table
    let res = ctx
        .simple_query("SELECT id, data FROM test_uncon_in WHERE id IN (1, 3) ORDER BY id")
        .await?;
    let _m = assert_subsume_hit(&mut ctx, m).await?;
    assert_eq!(res.len(), 4); // 2 rows
    assert_row_at(&res, 1, &[("id", "1"), ("data", "a")])?;
    assert_row_at(&res, 2, &[("id", "3"), ("data", "c")])?;

    Ok(())
}

/// Range query subsumes IN-set when all IN values fall within the range.
#[tokio::test]
async fn test_range_subsumes_in_set() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_range_in (id integer PRIMARY KEY, value integer)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_range_in VALUES (1, 10), (2, 50), (3, 100), (4, 200)",
        &[],
    )
    .await?;

    // Cache range
    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM test_range_in WHERE value > 0")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // IN-set where all values > 0 — subsumed
    let res = ctx
        .simple_query("SELECT id, value FROM test_range_in WHERE value IN (50, 100) ORDER BY id")
        .await?;
    let _m = assert_subsume_hit(&mut ctx, m).await?;
    assert_eq!(res.len(), 4); // 2 rows
    assert_row_at(&res, 1, &[("id", "2"), ("value", "50")])?;
    assert_row_at(&res, 2, &[("id", "3"), ("value", "100")])?;

    Ok(())
}

/// IN-set with CDC: INSERT a row matching the IN-set → cache invalidated.
#[tokio::test]
async fn test_in_set_cdc_insert_matching() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_in_cdc (id integer PRIMARY KEY, status text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_in_cdc VALUES (1, 'active'), (2, 'pending'), (3, 'inactive')",
        &[],
    )
    .await?;

    // Cache IN-set query
    let m = ctx.metrics().await?;
    let res = ctx
        .simple_query(
            "SELECT id, status FROM test_in_cdc WHERE status IN ('active', 'pending') ORDER BY id",
        )
        .await?;
    assert_eq!(res.len(), 4); // 2 rows
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Cache hit
    ctx.simple_query(
        "SELECT id, status FROM test_in_cdc WHERE status IN ('active', 'pending') ORDER BY id",
    )
    .await?;
    let m = assert_cache_hit(&mut ctx, m).await?;

    // INSERT matching row via origin → CDC in-place update (single-table, never invalidated)
    ctx.origin_query("INSERT INTO test_in_cdc VALUES (4, 'active')", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    // Cache hit with in-place update — should see new row
    let res = ctx
        .simple_query(
            "SELECT id, status FROM test_in_cdc WHERE status IN ('active', 'pending') ORDER BY id",
        )
        .await?;
    let _m = assert_cache_hit(&mut ctx, m).await?;
    assert_eq!(res.len(), 5); // 3 rows
    assert_row_at(&res, 1, &[("id", "1"), ("status", "active")])?;
    assert_row_at(&res, 2, &[("id", "2"), ("status", "pending")])?;
    assert_row_at(&res, 3, &[("id", "4"), ("status", "active")])?;

    Ok(())
}

/// IN-set with CDC: INSERT a row NOT matching the IN-set → cache NOT invalidated.
#[tokio::test]
async fn test_in_set_cdc_insert_non_matching() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_in_cdc_no (id integer PRIMARY KEY, status text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_in_cdc_no VALUES (1, 'active'), (2, 'pending')",
        &[],
    )
    .await?;

    // Cache IN-set query
    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM test_in_cdc_no WHERE status IN ('active', 'pending')")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // INSERT non-matching row via origin
    ctx.origin_query("INSERT INTO test_in_cdc_no VALUES (3, 'inactive')", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    // Should still be a cache hit — 'inactive' not in the IN-set
    ctx.simple_query("SELECT * FROM test_in_cdc_no WHERE status IN ('active', 'pending')")
        .await?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}
