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

/// Rung 3b precision: an INSERT that does NOT match the memoized query's WHERE
/// must leave the snapshot intact (the headline win — unrelated high-cardinality
/// inserts stop busting hot memos). The next read is still a memo hit and the
/// result is unchanged.
#[tokio::test]
async fn test_memo_survives_unrelated_insert() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(
        "create table memo_unrel (id integer primary key, val integer)",
        &[],
    )
    .await?;
    ctx.query("insert into memo_unrel values (1, 42)", &[])
        .await?;

    warm_until_memoized(&mut ctx, "select id, val from memo_unrel where val = 42").await?;

    // An insert that does not match `val = 42` — the memo must survive.
    ctx.origin_query("insert into memo_unrel values (2, 7)", &[])
        .await?;
    ctx.cdc_settle().await?;

    let before = ctx.metrics().await?;
    let res = ctx
        .simple_query("select id, val from memo_unrel where val = 42")
        .await?;
    assert_row_at(&res, 1, &[("id", "1"), ("val", "42")])?;
    assert_eq!(res.len(), 3, "result unchanged: RowDescription + 1 row + CC");
    let after = ctx.metrics().await?;
    assert!(
        after.cache_memo_hits > before.cache_memo_hits,
        "unrelated insert must NOT evict the memo — it should still serve inline"
    );
    Ok(())
}

/// Rung 3b: an INSERT that DOES match the memoized query's WHERE must evict the
/// snapshot, so the next read reflects the grown result (not a stale memo hit).
#[tokio::test]
async fn test_memo_evicted_on_matching_insert() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(
        "create table memo_match (id integer primary key, val integer)",
        &[],
    )
    .await?;
    ctx.query("insert into memo_match values (1, 42)", &[])
        .await?;

    warm_until_memoized(&mut ctx, "select id, val from memo_match where val = 42").await?;

    ctx.origin_query("insert into memo_match values (3, 42)", &[])
        .await?;
    ctx.cdc_settle().await?;

    // Fresh result (both rows) proves the matching insert busted the one-row
    // snapshot — a surviving stale memo would return only id=1.
    let res = ctx
        .simple_query("select id, val from memo_match where val = 42 order by id")
        .await?;
    assert_row_at(&res, 1, &[("id", "1"), ("val", "42")])?;
    assert_row_at(&res, 2, &[("id", "3"), ("val", "42")])?;
    assert_eq!(res.len(), 4, "RowDescription + 2 rows + CommandComplete");
    Ok(())
}

/// Rung 3b: a matching single-table INSERT is maintained in-place (the query
/// stays Ready), so its memo is reachable and must be evicted. Warm and serve
/// the SAME query so the memo is actually exercised — the sibling
/// `test_memo_evicted_on_matching_insert` warms without `ORDER BY` but serves
/// with it, a different `MemoKey`, so it never tests the INSERT eviction arm.
#[tokio::test]
async fn test_memo_evicted_on_inplace_matching_insert() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(
        "create table memo_ins (id integer primary key, val integer)",
        &[],
    )
    .await?;
    ctx.query("insert into memo_ins values (1, 42)", &[])
        .await?;

    let sql = "select id, val from memo_ins where val = 42 order by id";
    warm_until_memoized(&mut ctx, sql).await?;

    ctx.origin_query("insert into memo_ins values (3, 42)", &[])
        .await?;
    ctx.cdc_settle().await?;

    // Same query (same MemoKey): a stale memo would still return only id=1.
    let res = ctx.simple_query(sql).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("val", "42")])?;
    assert_row_at(&res, 2, &[("id", "3"), ("val", "42")])?;
    assert_eq!(
        res.len(),
        4,
        "in-place matching insert must serve {{1,3}}, not the stale one-row memo"
    );
    Ok(())
}

/// Rung 3b removal guardrail: a DELETE carries only the PK (REPLICA IDENTITY
/// DEFAULT), so the non-PK WHERE can't be probed — eviction is conservative and
/// the memo is busted, never serving a stale (now-larger-than-origin) snapshot.
#[tokio::test]
async fn test_memo_evicted_on_delete() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(
        "create table memo_del (id integer primary key, val integer)",
        &[],
    )
    .await?;
    ctx.query("insert into memo_del values (1, 42), (2, 42)", &[])
        .await?;

    warm_until_memoized(&mut ctx, "select id from memo_del where val = 42 order by id").await?;

    ctx.origin_query("delete from memo_del where id = 2", &[])
        .await?;
    ctx.cdc_settle().await?;

    // Fresh result (deleted row gone) proves the delete busted the snapshot — a
    // surviving stale memo would still return id=2.
    let res = ctx
        .simple_query("select id from memo_del where val = 42 order by id")
        .await?;
    assert_row_at(&res, 1, &[("id", "1")])?;
    assert_eq!(res.len(), 3, "deleted row gone: RowDescription + 1 row + CommandComplete");
    Ok(())
}

/// A 40P01 deadlock makes the writer swallow the frame and `frame_recover`
/// invalidate + truncate the affected relations — and a serve afterward must
/// not return the pre-recovery snapshot from a memo. Recovery evicts the query
/// (removed from `cached_queries` → not Ready), so its orphan memo is
/// unreachable until the query re-registers and re-captures; this guards that
/// path against regression. A NON-matching insert is used so rung 3b's per-row
/// eviction isn't what clears the memo. Gated on `fault-injection` (the deadlock
/// hook).
#[cfg(feature = "fault-injection")]
#[tokio::test]
async fn test_memo_busted_by_deadlock_recovery() -> Result<(), Error> {
    // The one-shot deadlock fires on the first CDC frame, so the table starts
    // empty (no pre-test row frames) and the memo is captured over the empty
    // result before the triggering insert.
    let mut ctx = TestContext::setup_fault(&[("PGCACHE_FAULT_CDC_DEADLOCK_ONCE", "1")]).await?;
    ctx.query(
        "create table memo_recover (id integer primary key, val integer)",
        &[],
    )
    .await?;

    warm_until_memoized(&mut ctx, "select id, val from memo_recover where val = 42").await?;

    // First CDC frame: a non-matching insert. The fault deadlocks it → the
    // writer enters Recovering and `frame_recover` truncates + busts memos.
    ctx.origin_query("insert into memo_recover values (2, 7)", &[])
        .await?;
    ctx.cdc_settle().await?;

    let before = ctx.metrics().await?;
    let res = ctx
        .simple_query("select id, val from memo_recover where val = 42")
        .await?;
    assert_eq!(res.len(), 2, "empty result: RowDescription + CommandComplete");
    let after = ctx.metrics().await?;
    assert_eq!(
        after.cache_memo_hits, before.cache_memo_hits,
        "deadlock recovery must bust the memo — no stale inline serve"
    );
    Ok(())
}
