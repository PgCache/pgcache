use std::io::Error;
use std::time::{Duration, Instant};

use tokio::time::sleep;

#[cfg(feature = "fault-injection")]
use crate::util::http_put;
use crate::util::{
    MetricsSnapshot, TestContext, assert_cache_hit, assert_cache_miss, assert_row_at,
};

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

/// A miss below the admission threshold ends at the subsumption check: no
/// entry is created, so no memo slot is held for a query that may never be
/// admitted (PGC-545). Both gauges are published on the writer's 1s tick;
/// a second pending query makes the pending gauge prove a tick ran after the
/// writer handled the first miss, so the memo gauge read with it is current.
#[tokio::test]
async fn test_pending_miss_holds_no_memo_slot() -> Result<(), Error> {
    let mut ctx = TestContext::setup_clock(2).await?;
    let timeout = Duration::from_secs(5);

    ctx.query(
        "CREATE TABLE test_pending_memo (id INTEGER PRIMARY KEY, data TEXT)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_pending_memo (id, data) VALUES (1, 'foo')",
        &[],
    )
    .await?;
    let query_str = "SELECT id, data FROM test_pending_memo WHERE data = 'foo'";

    // First miss: Pending(1), checked for subsumption only. A miss returns
    // only after the writer replied, so the second query's Pending view is
    // inserted after the writer handled the first; a tick that publishes
    // pending == 2 ran after that, and its memo-slot count is current.
    ctx.simple_query(query_str).await?;
    ctx.simple_query("SELECT id, data FROM test_pending_memo WHERE data = 'bar'")
        .await?;
    let m = gauges_wait(&mut ctx, timeout, |m| m.cache_queries_pending == 2).await?;
    assert_eq!(
        m.cache_memo_slots, 0,
        "a pending query below threshold must not hold a memo slot"
    );

    // Second miss admits; the entry holds its slot once registered.
    ctx.simple_query(query_str).await?;
    ctx.cache_settle().await?;
    gauges_wait(&mut ctx, timeout, |m| m.cache_memo_slots == 1).await?;

    Ok(())
}

/// The Pending set is bounded without hits (PGC-547). A stream of distinct
/// one-off queries at threshold 2 never hits, so hit-only decay would hold
/// every one of them forever. The growth-keyed safety term purges an entry
/// once `divisor * credit` later inserts have arrived; with the divisor
/// forced to 1 and the 100-credit floor, fewer than 100 entries can survive
/// however many were inserted. The first 50 cannot drain a credit of 100,
/// so the gauge reading 50 proves the queries take the Pending path before
/// the bound is asserted.
#[cfg(feature = "fault-injection")]
#[tokio::test]
async fn test_pending_set_bounded_without_hits() -> Result<(), Error> {
    let mut ctx =
        TestContext::setup_fault_clock(&[("PGCACHE_FAULT_PENDING_SAFETY_DIVISOR", "1")]).await?;
    let (status, body) = http_put(
        ctx.metrics_port,
        "/config",
        r#"{"cache_policy": "clock", "admission_threshold": 2}"#,
    )
    .await?;
    assert_eq!(status, 200, "PUT /config failed: {body}");
    let timeout = Duration::from_secs(10);

    ctx.query(
        "CREATE TABLE test_pending_bound (id INTEGER PRIMARY KEY, data TEXT)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_pending_bound (id, data) VALUES (1, 'foo')",
        &[],
    )
    .await?;

    for id in 0..50 {
        ctx.simple_query(&format!(
            "SELECT id, data FROM test_pending_bound WHERE id = {id}"
        ))
        .await?;
    }
    gauges_wait(&mut ctx, timeout, |m| m.cache_queries_pending == 50).await?;

    for id in 50..400 {
        ctx.simple_query(&format!(
            "SELECT id, data FROM test_pending_bound WHERE id = {id}"
        ))
        .await?;
    }
    // The gauge is published on the writer's 1s tick: let one run after the
    // inserts so the read below is not phase 1's value of 50.
    sleep(Duration::from_millis(1500)).await;
    gauges_wait(&mut ctx, timeout, |m| m.cache_queries_pending < 100).await?;

    Ok(())
}

/// Poll the metrics endpoint until `ready` holds, returning that snapshot.
async fn gauges_wait(
    ctx: &mut TestContext,
    timeout: Duration,
    ready: impl Fn(&MetricsSnapshot) -> bool,
) -> Result<MetricsSnapshot, Error> {
    let start = Instant::now();
    loop {
        let m = ctx.metrics().await?;
        if ready(&m) {
            return Ok(m);
        }
        if start.elapsed() > timeout {
            return Err(Error::other(format!("gauges did not settle: {m:?}")));
        }
        sleep(Duration::from_millis(100)).await;
    }
}
