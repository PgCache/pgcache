//! Integration tests for the in-process result memo (PGC-236).
//!
//! These use the simple-query protocol throughout: it always carries a
//! RowDescription (so the capture path stores a full core) and is served with
//! the whole core, exercising both capture and inline serve without the
//! extended-protocol `Describe('S')` fall-through.
//!
//! The memo is captured on the worker *after* the response is written, so tests
//! never assume timing — they poll the metrics endpoint until the memo is live.

use std::io::Error;
use std::time::Duration;

use crate::util::{TestContext, assert_row_at, http_put};

mod util;

/// Issue `sql` repeatedly until it is served from the memo (a `memo_hits`
/// increment is observed), or fail after a bounded number of attempts. Captures
/// require `MEMO_CAPTURE_MIN_HITS` prior hits plus the post-serve `finish()`, so
/// a hot query becomes memo-served after a handful of iterations.
async fn warm_until_memoized(ctx: &mut TestContext, sql: &str) -> Result<(), Error> {
    for _ in 0..80 {
        ctx.simple_query(sql).await?;
        if ctx.metrics().await?.cache_memo_hits >= 1 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Err(Error::other(format!("query never served from memo: {sql}")))
}

/// Like [`warm_until_memoized`] but drives the query over the extended protocol
/// as a reused prepared statement (`Bind`/`Execute`, no `Describe`) — the demo's
/// exclusive access pattern. These serves carry no RowDescription, so they
/// exercise the `rd_len == 0` capture/serve path.
async fn warm_until_memoized_prepared(
    ctx: &mut TestContext,
    sql: &str,
    param: i32,
) -> Result<(), Error> {
    for _ in 0..80 {
        ctx.query(sql, &[&param]).await?;
        if ctx.metrics().await?.cache_memo_hits >= 1 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Err(Error::other(format!(
        "prepared query never served from memo: {sql}"
    )))
}

/// Poll until `cache_memo_evictions` exceeds `baseline`, or fail. Eviction
/// happens lazily on the next `get` or proactively on the writer's gc tick, so
/// either path satisfies this.
async fn wait_for_eviction(ctx: &mut TestContext, baseline: u64) -> Result<(), Error> {
    for _ in 0..80 {
        if ctx.metrics().await?.cache_memo_evictions > baseline {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Err(Error::other("memo entry was never evicted"))
}

/// A hot, MV-ineligible single-table query is captured and then served inline
/// from the memo.
#[tokio::test]
async fn test_memo_serves_hot_simple_query() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(
        "create table memo_serve (id integer primary key, val text)",
        &[],
    )
    .await?;
    ctx.query("insert into memo_serve values (1, 'a'), (2, 'b')", &[])
        .await?;

    warm_until_memoized(&mut ctx, "select val from memo_serve where id = 1").await?;

    // A subsequent serve is a memo hit and returns the correct row.
    let before = ctx.metrics().await?;
    let res = ctx
        .simple_query("select val from memo_serve where id = 1")
        .await?;
    assert_row_at(&res, 1, &[("val", "a")])?;
    let after = ctx.metrics().await?;
    assert!(
        after.cache_memo_hits > before.cache_memo_hits,
        "expected an inline memo hit"
    );
    assert!(
        after.cache_memo_captures >= 1,
        "memo should have been captured before it could be served"
    );
    Ok(())
}

/// No stale reads: an in-place CDC UPDATE to a memoized query's table must evict
/// the snapshot, so the next read reflects the new value. This is the headline
/// correctness test for the seqlock eviction.
#[tokio::test]
async fn test_memo_no_stale_on_inplace_update() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(
        "create table memo_stale (id integer primary key, val text)",
        &[],
    )
    .await?;
    ctx.query("insert into memo_stale values (1, 'old')", &[])
        .await?;

    warm_until_memoized(&mut ctx, "select val from memo_stale where id = 1").await?;
    let before = ctx.metrics().await?;

    // In-place UPDATE on the origin → CDC applies it to the cache and bumps the
    // relation seqlock, which must invalidate the 'old' snapshot.
    ctx.origin_query("update memo_stale set val = 'new' where id = 1", &[])
        .await?;
    ctx.cdc_settle().await?;

    // The next read must NOT serve the stale 'old' snapshot.
    let res = ctx
        .simple_query("select val from memo_stale where id = 1")
        .await?;
    assert_row_at(&res, 1, &[("val", "new")])?;

    // And the stale entry was actually evicted (proves the memo was live and the
    // seqlock busted it, not that the test passed via a never-captured query).
    wait_for_eviction(&mut ctx, before.cache_memo_evictions).await?;
    Ok(())
}

/// No stale reads on growth: an INSERT that adds a matching row (applied in-place
/// for a single-table query) must also evict the snapshot so the larger result
/// is returned.
#[tokio::test]
async fn test_memo_evicted_on_grow_insert() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(
        "create table memo_grow (id integer primary key, kind text)",
        &[],
    )
    .await?;
    ctx.query("insert into memo_grow values (1, 'x')", &[])
        .await?;

    warm_until_memoized(&mut ctx, "select id from memo_grow where kind = 'x'").await?;

    ctx.origin_query("insert into memo_grow values (2, 'x')", &[])
        .await?;
    ctx.cdc_settle().await?;

    // The memoized single-row snapshot must be evicted; the fresh read sees both.
    let res = ctx
        .simple_query("select id from memo_grow where kind = 'x' order by id")
        .await?;
    assert_row_at(&res, 1, &[("id", "1")])?;
    assert_row_at(&res, 2, &[("id", "2")])?;
    Ok(())
}

/// The demo path: a reused prepared statement (no RowDescription on the wire) is
/// captured (`rd_len == 0`) and served inline from the memo, byte-correct
/// (BindComplete + DataRow* + CommandComplete + ReadyForQuery).
#[tokio::test]
async fn test_memo_prepared_statement_serves() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(
        "create table memo_prep (id integer primary key, val text)",
        &[],
    )
    .await?;
    ctx.query("insert into memo_prep values (1, 'a'), (2, 'b')", &[])
        .await?;

    warm_until_memoized_prepared(&mut ctx, "select val from memo_prep where id = $1", 1).await?;

    let before = ctx.metrics().await?;
    let rows = ctx
        .query("select val from memo_prep where id = $1", &[&1i32])
        .await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>("val"), "a");
    let after = ctx.metrics().await?;
    assert!(
        after.cache_memo_hits > before.cache_memo_hits,
        "reused prepared statement should serve from the memo"
    );
    Ok(())
}

/// No stale reads over the prepared-statement path: warm a memo via reused
/// prepared executes, UPDATE in place on origin, and confirm the next prepared
/// execute reflects the new value.
#[tokio::test]
async fn test_memo_prepared_no_stale_on_update() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(
        "create table memo_prep_stale (id integer primary key, val text)",
        &[],
    )
    .await?;
    ctx.query("insert into memo_prep_stale values (1, 'old')", &[])
        .await?;

    warm_until_memoized_prepared(&mut ctx, "select val from memo_prep_stale where id = $1", 1)
        .await?;
    let before = ctx.metrics().await?;

    ctx.origin_query("update memo_prep_stale set val = 'new' where id = 1", &[])
        .await?;
    ctx.cdc_settle().await?;

    let rows = ctx
        .query("select val from memo_prep_stale where id = $1", &[&1i32])
        .await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get::<_, String>("val"),
        "new",
        "prepared execute must not serve the stale memo snapshot"
    );
    wait_for_eviction(&mut ctx, before.cache_memo_evictions).await?;
    Ok(())
}

/// No stale reads across a runtime disable: while memoization is toggled off via
/// the admin API, the seqlock must still bump on a committing change (the store
/// is non-empty), so re-enabling cannot resurrect a pre-disable snapshot.
#[tokio::test]
async fn test_memo_no_stale_across_runtime_disable() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(
        "create table memo_toggle (id integer primary key, val text)",
        &[],
    )
    .await?;
    ctx.query("insert into memo_toggle values (1, 'old')", &[])
        .await?;

    warm_until_memoized(&mut ctx, "select val from memo_toggle where id = 1").await?;

    // Disable memoization at runtime; existing entries stay in the store.
    let (status, body) = http_put(ctx.metrics_port, "/config", r#"{"memo_cache_size": 0}"#).await?;
    assert_eq!(status, 200, "disable PUT failed: {body}");

    // A write commits while disabled — the seqlock must still bump.
    ctx.origin_query("update memo_toggle set val = 'new' where id = 1", &[])
        .await?;
    ctx.cdc_settle().await?;

    // Re-enable memoization.
    let (status, body) = http_put(
        ctx.metrics_port,
        "/config",
        r#"{"memo_cache_size": 67108864}"#,
    )
    .await?;
    assert_eq!(status, 200, "re-enable PUT failed: {body}");

    // The pre-disable 'old' snapshot must not survive the re-enable.
    let res = ctx
        .simple_query("select val from memo_toggle where id = 1")
        .await?;
    assert_row_at(&res, 1, &[("val", "new")])?;
    Ok(())
}

/// With `memo_cache_size = 0` the feature is off: queries are still cache hits
/// (served by the worker) but never produce a memo hit.
#[tokio::test]
async fn test_memo_disabled_via_env() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&[("PGCACHE_MEMO_CACHE_SIZE", "0")]).await?;
    ctx.query(
        "create table memo_off (id integer primary key, val text)",
        &[],
    )
    .await?;
    ctx.query("insert into memo_off values (1, 'a')", &[])
        .await?;

    // Warm well past the capture threshold.
    for _ in 0..12 {
        let res = ctx
            .simple_query("select val from memo_off where id = 1")
            .await?;
        assert_row_at(&res, 1, &[("val", "a")])?;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let m = ctx.metrics().await?;
    assert_eq!(
        m.cache_memo_hits, 0,
        "memoization disabled: no memo hits expected"
    );
    assert_eq!(
        m.cache_memo_captures, 0,
        "memoization disabled: nothing should be captured"
    );
    assert!(
        m.queries_cache_hit > 0,
        "queries should still be cache hits via the worker"
    );
    Ok(())
}
