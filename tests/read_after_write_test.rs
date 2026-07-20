//! Per-connection read-after-write (PGC-124). The gate tests force CDC apply
//! lag via the fault-injection sentinel, and the harness disables the feature by
//! default, so all tests here re-enable it and build only with the feature.
#![cfg(feature = "fault-injection")]

use std::io::Error;

use tokio_postgres::SimpleQueryMessage;

use crate::util::{TestContext, assert_cache_hit, assert_cache_miss, assert_row_at, metrics_delta};

mod util;

/// Enable per-connection read-after-write, which the shared test harness turns
/// off by default (most tests write and read on one connection and would then
/// forward instead of cache-hit). These tests exercise the feature itself.
const RAW_ON: (&str, &str) = ("PGCACHE_READ_YOUR_WRITES", "on");

fn row_count(msgs: &[SimpleQueryMessage]) -> usize {
    msgs.iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .count()
}

/// PGC-366: writes a connection forwards are recorded into its per-connection
/// log (observed via the `pgcache.raw.writes_recorded` counter), while pure
/// reads record nothing.
#[tokio::test]
async fn test_writes_recorded_into_raw_log() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&[RAW_ON]).await?;

    ctx.query(
        "CREATE TABLE raw_items (id int primary key, data text)",
        &[],
    )
    .await?;

    // Baseline after setup/DDL so we measure only the statements below.
    let before = ctx.metrics().await?;

    ctx.simple_query("INSERT INTO raw_items VALUES (1, 'a')")
        .await?;
    ctx.simple_query("UPDATE raw_items SET data = 'b' WHERE id = 1")
        .await?;
    ctx.simple_query("DELETE FROM raw_items WHERE id = 1")
        .await?;

    let after_writes = metrics_delta(&before, &ctx.metrics().await?);
    assert!(
        after_writes.raw_writes_recorded >= 3,
        "expected the INSERT/UPDATE/DELETE to be recorded, got {}",
        after_writes.raw_writes_recorded
    );

    // A pure read records nothing.
    let mid = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM raw_items WHERE id = 1")
        .await?;
    let after_read = metrics_delta(&mid, &ctx.metrics().await?);
    assert_eq!(
        after_read.raw_writes_recorded, 0,
        "a read must not record a write"
    );

    Ok(())
}

/// PGC-367: after a connection forwards a write, a subsequent quiescent
/// ReadyForQuery injects a commit-LSN probe (observed via `pgcache.raw.probes`),
/// and normal query results are unaffected by the swallowed probe round-trip.
#[tokio::test]
async fn test_commit_lsn_probe_fires_and_preserves_results() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&[RAW_ON]).await?;

    let before = ctx.metrics().await?;

    ctx.query("CREATE TABLE raw_probe (id int primary key, v int)", &[])
        .await?;
    ctx.simple_query("INSERT INTO raw_probe VALUES (1, 10)")
        .await?;

    // Results are correct despite the injected probe being swallowed on the
    // origin socket.
    let res = ctx
        .simple_query("SELECT v FROM raw_probe WHERE id = 1")
        .await?;
    assert_row_at(&res, 1, &[("v", "10")])?;

    // Give the injected probe(s) time to round-trip on the origin socket.
    ctx.cdc_apply_settle().await?;

    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert!(
        delta.raw_probes >= 1,
        "expected at least one commit-LSN probe, got {}",
        delta.raw_probes
    );

    Ok(())
}

/// PGC-368: under CDC apply lag, a connection's own INSERT must be visible to
/// its immediately-following cacheable read — the gate forwards the read to
/// origin rather than serving the pre-insert cached result.
#[tokio::test]
async fn test_gate_forwards_read_after_write_under_cdc_lag() -> Result<(), Error> {
    let mut ctx =
        TestContext::setup_fault(&[RAW_ON, ("PGCACHE_FAULT_CDC_APPLY_LAG_MS", "1500")]).await?;
    ctx.query("CREATE TABLE g (id int primary key, v int)", &[])
        .await?;
    ctx.query("INSERT INTO g VALUES (1, 10), (2, 20)", &[])
        .await?;
    // Let the setup writes drain so they don't gate the prime below.
    ctx.cdc_apply_settle().await?;

    // Prime the baseline query and confirm it becomes a cache hit.
    let m = ctx.metrics().await?;
    let _ = ctx.simple_query("SELECT id, v FROM g").await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    let res = ctx.simple_query("SELECT id, v FROM g").await?;
    assert_eq!(row_count(&res), 2);
    let _ = assert_cache_hit(&mut ctx, m).await?;

    // Write on this connection, then read immediately. Under 1.5s CDC lag the
    // cache still holds 2 rows, so only the gate (forwarding to origin) can
    // return all 3 — a stale cache hit would return 2.
    ctx.simple_query("INSERT INTO g VALUES (3, 30)").await?;
    let res = ctx.simple_query("SELECT id, v FROM g").await?;
    assert_eq!(
        row_count(&res),
        3,
        "read-after-write must see the connection's own insert"
    );

    // Once CDC catches up, the pending write clears and the query cache-hits
    // again (now with the applied row).
    ctx.cdc_apply_settle().await?;
    let m = ctx.metrics().await?;
    let res = ctx.simple_query("SELECT id, v FROM g").await?;
    assert_eq!(row_count(&res), 3);
    let _ = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// PGC-368: the gate is table-scoped — a pending write to one table does not
/// force reads of an unrelated table to forward; those still serve from cache.
#[tokio::test]
async fn test_gate_does_not_over_forward_unrelated_tables() -> Result<(), Error> {
    let mut ctx =
        TestContext::setup_fault(&[RAW_ON, ("PGCACHE_FAULT_CDC_APPLY_LAG_MS", "1500")]).await?;
    ctx.query("CREATE TABLE a (id int primary key)", &[])
        .await?;
    ctx.query("CREATE TABLE b (id int primary key)", &[])
        .await?;
    ctx.query("INSERT INTO a VALUES (1)", &[]).await?;
    ctx.query("INSERT INTO b VALUES (1)", &[]).await?;
    ctx.cdc_apply_settle().await?;

    // Prime a read of table `b`.
    let m = ctx.metrics().await?;
    let _ = ctx.simple_query("SELECT id FROM b").await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    let _ = ctx.simple_query("SELECT id FROM b").await?;
    let m = assert_cache_hit(&mut ctx, m).await?;

    // Write to `a` (not `b`), then read `b` immediately — `b` has no pending
    // write, so it still serves from cache during the lag window.
    ctx.simple_query("INSERT INTO a VALUES (2)").await?;
    let _ = ctx.simple_query("SELECT id FROM b").await?;
    let _ = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// PGC-369: row-level INSERT precision. A cacheable read provably disjoint from
/// a pending INSERT still serves from cache, while a read the insert could match
/// forwards and sees the new row — both on the same connection, under CDC lag.
#[tokio::test]
async fn test_gate_row_level_insert_precision() -> Result<(), Error> {
    let mut ctx =
        TestContext::setup_fault(&[RAW_ON, ("PGCACHE_FAULT_CDC_APPLY_LAG_MS", "1500")]).await?;
    ctx.query("CREATE TABLE t (id int primary key, v int)", &[])
        .await?;
    ctx.query("INSERT INTO t VALUES (1, 10), (2, 20)", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    // Register two point reads: id = 1 (present) and id = 3 (absent for now).
    for sql in [
        "SELECT v FROM t WHERE id = 1",
        "SELECT v FROM t WHERE id = 3",
    ] {
        let m0 = ctx.metrics().await?;
        let _ = ctx.simple_query(sql).await?;
        let _ = assert_cache_miss(&mut ctx, m0).await?;
        ctx.cache_settle().await?;
    }

    // A write this connection just made, disjoint from `id = 1`. The explicit
    // column list keeps it row-enumerable (an omitted list degrades to
    // table-level for want of catalog column order).
    ctx.simple_query("INSERT INTO t (id, v) VALUES (3, 30)")
        .await?;

    // `id = 1` is provably disjoint from the inserted id = 3 → still a cache hit,
    // and the row-precision counter records the proof.
    let m = ctx.metrics().await?;
    let res = ctx.simple_query("SELECT v FROM t WHERE id = 1").await?;
    assert_row_at(&res, 1, &[("v", "10")])?;
    let after = ctx.metrics().await?;
    let delta = metrics_delta(&m, &after);
    assert_eq!(
        delta.queries_cache_hit, 1,
        "disjoint read must serve from cache"
    );
    assert!(
        delta.raw_insert_disjoint >= 1,
        "row-level disjointness proof expected"
    );

    // `id = 3` is matched by the pending insert → forwarded, and sees v = 30
    // (a stale cache hit would return no rows).
    let res = ctx.simple_query("SELECT v FROM t WHERE id = 3").await?;
    assert_row_at(&res, 1, &[("v", "30")])?;

    Ok(())
}

/// PGC-124: the row-level disjointness proof compares by numeric value, not
/// `LiteralValue` spelling. A read `WHERE k = 10` (integer literal) against a
/// pending `INSERT ... (10.0)` (float literal) is numerically a match and must
/// forward — before the numeric-aware fix the two spellings compared unequal,
/// so the insert was wrongly proven disjoint and the read served a stale cache
/// result that omitted the new row.
#[tokio::test]
async fn test_gate_int_float_numeric_precision() -> Result<(), Error> {
    let mut ctx =
        TestContext::setup_fault(&[RAW_ON, ("PGCACHE_FAULT_CDC_APPLY_LAG_MS", "1500")]).await?;
    ctx.query("CREATE TABLE nf (id int primary key, k int)", &[])
        .await?;
    ctx.query("INSERT INTO nf VALUES (1, 10)", &[]).await?;
    ctx.cdc_apply_settle().await?;

    // Register both reads up front (a read can only serve disjoint from a
    // pending insert once it has a resolved cache entry): `k = 10` returns the
    // seed row, `k = 99` returns none.
    for sql in [
        "SELECT id FROM nf WHERE k = 10",
        "SELECT id FROM nf WHERE k = 99",
    ] {
        let m0 = ctx.metrics().await?;
        let _ = ctx.simple_query(sql).await?;
        let _ = assert_cache_miss(&mut ctx, m0).await?;
        ctx.cache_settle().await?;
    }

    // Insert a second `k = 10` row spelled as a float (`10.0` → k = 10). Under
    // CDC lag the cache still holds only the seed row.
    ctx.simple_query("INSERT INTO nf (id, k) VALUES (2, 10.0)")
        .await?;

    // `k = 10` numerically matches the pending `10.0` → forward → sees both rows
    // (a stale cache hit would return only the seed row).
    let res = ctx.simple_query("SELECT id FROM nf WHERE k = 10").await?;
    assert_eq!(
        row_count(&res),
        2,
        "int/float numeric match must forward, not serve stale"
    );

    // `k = 99` is provably disjoint from the float insert → still a cache hit,
    // recorded by the row-precision counter.
    let m = ctx.metrics().await?;
    let res = ctx.simple_query("SELECT id FROM nf WHERE k = 99").await?;
    assert_eq!(row_count(&res), 0);
    let after = ctx.metrics().await?;
    let delta = metrics_delta(&m, &after);
    assert_eq!(
        delta.queries_cache_hit, 1,
        "disjoint read must serve from cache"
    );
    assert!(
        delta.raw_insert_disjoint >= 1,
        "row-level disjointness proof expected"
    );

    Ok(())
}
