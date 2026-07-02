//! Correctness and precision guards for the precise old-image CDC probe
//! (PGC-255).
//!
//! The old-image candidate probe recovers the pre-event row (in-batch overlay
//! → batched cache-table lookup → PK-only wildcard fallback) and probes it
//! with real values. Precision tests assert the new behavior — a write to a
//! row *outside* a memoized query's predicate region no longer evicts its
//! memo. Correctness tests assert the never-under-return side: every write
//! whose committed result change matters still evicts/invalidates, across the
//! recovery ladder's edges (chained same-PK updates in one transaction,
//! deletes, PK changes, uncached-row fallback).

use std::io::Error;
use std::time::Duration;

use crate::util::{TestContext, assert_row_at};

mod util;

/// Issue `sql` repeatedly until it is served from the in-process memo (a
/// `memo_hits` increment), so a later write must evict that memo to stay fresh.
async fn warm_until_memoized(ctx: &mut TestContext, sql: &str) -> Result<(), Error> {
    let start = ctx.metrics().await?.cache_memo_hits;
    for _ in 0..80 {
        ctx.simple_query(sql).await?;
        if ctx.metrics().await?.cache_memo_hits > start {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Err(Error::other(format!("query never served from memo: {sql}")))
}

/// Two-owner setup: both owners' queries cached (so both rows are in the
/// cache table and the old-image lookup can resolve them), owner 10's also
/// memoized.
async fn setup_two_owners(ctx: &mut TestContext) -> Result<(&'static str, &'static str), Error> {
    ctx.query(
        "CREATE TABLE items (id INT PRIMARY KEY, owner INT, val INT)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO items (id, owner, val) VALUES (1, 10, 5), (2, 20, 7)",
        &[],
    )
    .await?;
    let q10 = "SELECT val FROM items WHERE owner = 10";
    let q20 = "SELECT val FROM items WHERE owner = 20";
    ctx.simple_query(q10).await?;
    ctx.simple_query(q20).await?;
    ctx.cache_settle().await?;
    warm_until_memoized(ctx, q10).await?;
    Ok((q10, q20))
}

/// Precision (the PGC-255 payoff): an UPDATE to a row that never matched a
/// memoized query's predicate must NOT evict its memo. The wildcard probe
/// made every non-PK-constrained query a candidate on every update; the
/// recovered old image (owner = 20, real value) excludes owner-10's query.
/// The counterpart write to the matching row must still evict and re-serve
/// fresh — the never-under-return side.
#[tokio::test]
async fn test_old_image_memo_survives_unrelated_row_update() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    let (q10, _q20) = setup_two_owners(&mut ctx).await?;

    // Unrelated write: owner 20's row changes value. Old image (owner = 20)
    // resolves from the cache table; owner-10's query is not a candidate.
    let evictions_before = ctx.metrics().await?.cache_memo_evictions;
    let hits_before = ctx.metrics().await?.cache_memo_hits;
    ctx.origin_query("UPDATE items SET val = 8 WHERE id = 2", &[])
        .await?;
    ctx.cdc_settle().await?;

    let m = ctx.metrics().await?;
    assert_eq!(
        m.cache_memo_evictions, evictions_before,
        "unrelated-row update must not evict the memo (precise old image)"
    );
    // Still memo-served, still correct.
    let res = ctx.simple_query(q10).await?;
    assert_row_at(&res, 1, &[("val", "5")])?;
    assert!(
        ctx.metrics().await?.cache_memo_hits > hits_before,
        "owner-10 query should still serve from its memo"
    );

    // Matching write: owner 10's row changes value → memo must die and the
    // fresh value must serve (an under-returning probe would serve stale 5).
    ctx.origin_query("UPDATE items SET val = 6 WHERE id = 1", &[])
        .await?;
    ctx.cdc_settle().await?;
    let res = ctx.simple_query(q10).await?;
    assert_row_at(&res, 1, &[("val", "6")])?;
    assert!(
        ctx.metrics().await?.cache_memo_evictions > evictions_before,
        "matching-row update must evict the memo"
    );

    Ok(())
}

/// Precision for DELETE: deleting a row outside the memoized predicate region
/// keeps the memo; deleting the matching row kills it and the fresh (empty)
/// result serves.
#[tokio::test]
async fn test_old_image_delete_precision() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    let (q10, _q20) = setup_two_owners(&mut ctx).await?;

    let evictions_before = ctx.metrics().await?.cache_memo_evictions;
    ctx.origin_query("DELETE FROM items WHERE id = 2", &[])
        .await?;
    ctx.cdc_settle().await?;
    assert_eq!(
        ctx.metrics().await?.cache_memo_evictions,
        evictions_before,
        "unrelated-row delete must not evict the memo (precise old image)"
    );
    let res = ctx.simple_query(q10).await?;
    assert_row_at(&res, 1, &[("val", "5")])?;

    ctx.origin_query("DELETE FROM items WHERE id = 1", &[])
        .await?;
    ctx.cdc_settle().await?;
    // RowDescription + CommandComplete only — serving stale [5] here means the
    // delete's old image under-returned.
    let res = ctx.simple_query(q10).await?;
    assert_eq!(res.len(), 2, "owner-10 result must be empty, got {res:?}");

    Ok(())
}

/// Chained same-PK updates in one transaction: the row moves owner
/// 10 → 20 → 30 atomically. Committed results change for owner 10 (loses the
/// row) and owner 30 (gains it); owner 20 only held it transiently and its
/// committed (empty) result is unchanged. All three must serve fresh, correct
/// results afterwards — this exercises the in-batch overlay (event 2's old
/// image is event 1's new image, not the pre-batch row).
#[tokio::test]
async fn test_old_image_chained_same_pk_updates_one_txn() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(
        "CREATE TABLE items (id INT PRIMARY KEY, owner INT, val INT)",
        &[],
    )
    .await?;
    ctx.query("INSERT INTO items (id, owner, val) VALUES (1, 10, 5)", &[])
        .await?;
    let q10 = "SELECT val FROM items WHERE owner = 10";
    let q20 = "SELECT val FROM items WHERE owner = 20";
    let q30 = "SELECT val FROM items WHERE owner = 30";
    ctx.simple_query(q10).await?;
    ctx.simple_query(q20).await?;
    ctx.simple_query(q30).await?;
    ctx.cache_settle().await?;
    warm_until_memoized(&mut ctx, q10).await?;

    ctx.origin
        .batch_execute(
            "BEGIN; UPDATE items SET owner = 20 WHERE id = 1; \
             UPDATE items SET owner = 30 WHERE id = 1; COMMIT",
        )
        .await
        .map_err(Error::other)?;
    ctx.cdc_settle().await?;

    // Owner 10 lost the row: stale [5] here means the first event's old image
    // under-returned.
    let res = ctx.simple_query(q10).await?;
    assert_eq!(res.len(), 2, "owner-10 result must be empty, got {res:?}");
    // Owner 30 gained it.
    let res = ctx.simple_query(q30).await?;
    assert_row_at(&res, 1, &[("val", "5")])?;
    // Owner 20 held it only transiently: still empty.
    let res = ctx.simple_query(q20).await?;
    assert_eq!(res.len(), 2, "owner-20 result must be empty, got {res:?}");

    Ok(())
}

/// PK-change updates skip the batched lookup (the join key would be the new
/// PK, the old image lives under the old one) and fall back conservatively.
/// Correctness: the row's owner page must serve the row under its new PK.
#[tokio::test]
async fn test_old_image_pk_change_conservative() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(
        "CREATE TABLE items (id INT PRIMARY KEY, owner INT, val INT)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO items (id, owner, val) VALUES (1, 10, 5), (2, 20, 7)",
        &[],
    )
    .await?;
    let q10 = "SELECT id, val FROM items WHERE owner = 10 ORDER BY id";
    ctx.simple_query(q10).await?;
    ctx.cache_settle().await?;

    ctx.origin_query("UPDATE items SET id = 3 WHERE id = 1", &[])
        .await?;
    ctx.cdc_settle().await?;

    let res = ctx.simple_query(q10).await?;
    assert_row_at(&res, 1, &[("id", "3"), ("val", "5")])?;
    assert_eq!(
        res.len(),
        3,
        "expected exactly one owner-10 row, got {res:?}"
    );

    Ok(())
}

/// Uncached-row fallback: after an update-out (the row left every cached
/// predicate and was dropped from the cache table), a later update to that
/// row finds no old image on any rung and must fall back to the wildcard —
/// results for the original owner page stay correct throughout.
#[tokio::test]
async fn test_old_image_uncached_row_falls_back() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(
        "CREATE TABLE items (id INT PRIMARY KEY, owner INT, val INT)",
        &[],
    )
    .await?;
    ctx.query("INSERT INTO items (id, owner, val) VALUES (1, 10, 5)", &[])
        .await?;
    let q10 = "SELECT val FROM items WHERE owner = 10";
    ctx.simple_query(q10).await?;
    ctx.cache_settle().await?;

    // Update-out: owner 10 → 99 (no cached query matches owner 99).
    ctx.origin_query("UPDATE items SET owner = 99 WHERE id = 1", &[])
        .await?;
    ctx.cdc_settle().await?;
    let res = ctx.simple_query(q10).await?;
    assert_eq!(res.len(), 2, "owner-10 result must be empty, got {res:?}");

    // The row is now uncached; this update's old image resolves on no rung.
    ctx.origin_query("UPDATE items SET val = 9 WHERE id = 1", &[])
        .await?;
    ctx.cdc_settle().await?;

    // Coming back into the predicate region must serve the fresh value.
    let m = ctx.metrics().await?;
    ctx.origin_query("UPDATE items SET owner = 10 WHERE id = 1", &[])
        .await?;
    ctx.cdc_settle().await?;
    let res = ctx.simple_query(q10).await?;
    assert_row_at(&res, 1, &[("val", "9")])?;
    let _ = m;

    Ok(())
}
