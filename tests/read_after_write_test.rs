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

/// Register a simple-protocol read into the cache and confirm it reaches a
/// steady cache hit (miss → settle → hit), the baseline the shape tests write
/// against.
async fn register(ctx: &mut TestContext, sql: &str) -> Result<(), Error> {
    let m = ctx.metrics().await?;
    let _ = ctx.simple_query(sql).await?;
    let m = assert_cache_miss(ctx, m).await?;
    ctx.cache_settle().await?;
    let _ = ctx.simple_query(sql).await?;
    let _ = assert_cache_hit(ctx, m).await?;
    Ok(())
}

/// Environment for the shape tests: read-after-write on, 1.5s CDC apply lag so a
/// pending write stays visible across the forward/drain window.
const RAW_LAG: [(&str, &str); 2] = [RAW_ON, ("PGCACHE_FAULT_CDC_APPLY_LAG_MS", "1500")];

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
        delta.raw_serve_disjoint_insert >= 1,
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
        delta.raw_serve_disjoint_insert >= 1,
        "row-level disjointness proof expected"
    );

    Ok(())
}

/// PGC-370: an extended-protocol *parameterized* INSERT (`VALUES ($1, $2)`) gets
/// the same row-level precision as a literal one — its bind values are
/// substituted at record time, so a disjoint read still serves from cache while
/// a matching read forwards and sees the new row.
#[tokio::test]
async fn test_gate_parameterized_insert_precision() -> Result<(), Error> {
    let mut ctx =
        TestContext::setup_fault(&[RAW_ON, ("PGCACHE_FAULT_CDC_APPLY_LAG_MS", "1500")]).await?;
    ctx.query("CREATE TABLE t (id int primary key, v int)", &[])
        .await?;
    ctx.query("INSERT INTO t VALUES (1, 10), (2, 20)", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    // Register two literal point reads up front (isolates the write-side
    // substitution): id = 1 (present) and id = 3 (absent for now).
    for sql in [
        "SELECT v FROM t WHERE id = 1",
        "SELECT v FROM t WHERE id = 3",
    ] {
        let m0 = ctx.metrics().await?;
        let _ = ctx.simple_query(sql).await?;
        let _ = assert_cache_miss(&mut ctx, m0).await?;
        ctx.cache_settle().await?;
    }

    // Parameterized INSERT over the extended protocol (tokio_postgres binds the
    // int4 params in binary): disjoint from id = 1.
    ctx.query("INSERT INTO t (id, v) VALUES ($1, $2)", &[&3i32, &30i32])
        .await?;

    // id = 1 is provably disjoint from the substituted inserted id = 3 → cache
    // hit, and the row-precision counter records the proof (which only fires if
    // the `$1`/`$2` cells were substituted to concrete values).
    let m = ctx.metrics().await?;
    let res = ctx.simple_query("SELECT v FROM t WHERE id = 1").await?;
    assert_row_at(&res, 1, &[("v", "10")])?;
    let delta = metrics_delta(&m, &ctx.metrics().await?);
    assert_eq!(
        delta.queries_cache_hit, 1,
        "disjoint read must serve from cache"
    );
    assert!(
        delta.raw_serve_disjoint_insert >= 1,
        "parameterized insert must be substituted for the row-level proof"
    );

    // id = 3 is matched by the pending parameterized insert → forward, sees v = 30.
    let res = ctx.simple_query("SELECT v FROM t WHERE id = 3").await?;
    assert_row_at(&res, 1, &[("v", "30")])?;

    Ok(())
}

// ============================================================================
// #5 — the gate is correct and behavior-neutral across common query shapes.
// Each test: register a shaped read, write to a table it references, confirm the
// read forwards fresh data during the lag window, then — once CDC drains — the
// read cache-hits again.
// ============================================================================

/// A JOIN read: a write to a joined table grows the result, so the read must
/// forward during lag; after CDC applies (and re-populates the grown join) it
/// serves from cache again.
#[tokio::test]
async fn test_gate_join_shape() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&RAW_LAG).await?;
    ctx.query("CREATE TABLE ja (id int primary key, bid int)", &[])
        .await?;
    ctx.query("CREATE TABLE jb (id int primary key, label text)", &[])
        .await?;
    ctx.query("INSERT INTO ja VALUES (1, 10)", &[]).await?;
    ctx.query("INSERT INTO jb VALUES (10, 'x')", &[]).await?;
    ctx.cdc_apply_settle().await?;

    let q = "SELECT ja.id FROM ja JOIN jb ON ja.bid = jb.id";
    register(&mut ctx, q).await?;

    // A new ja row joining jb(10) grows the result to two rows.
    ctx.simple_query("INSERT INTO ja VALUES (2, 10)").await?;
    let res = ctx.simple_query(q).await?;
    assert_eq!(
        row_count(&res),
        2,
        "join read must forward the grown result"
    );

    // The growing insert invalidates the cached join; once CDC drains, the next
    // request re-populates it, and the one after that cache-hits.
    ctx.cdc_apply_settle().await?;
    let res = ctx.simple_query(q).await?;
    assert_eq!(row_count(&res), 2);
    ctx.cache_settle().await?;
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(q).await?;
    assert_eq!(row_count(&res), 2);
    let _ = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// An IN-subquery read: a write to the subquery's table changes membership, so
/// the read (which references that table inside the subquery) forwards during
/// lag, then serves from cache once CDC drains.
#[tokio::test]
async fn test_gate_subquery_shape() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&RAW_LAG).await?;
    ctx.query("CREATE TABLE su (id int primary key, k int)", &[])
        .await?;
    ctx.query("CREATE TABLE sv (id int primary key)", &[])
        .await?;
    ctx.query("INSERT INTO su VALUES (1, 10), (2, 20)", &[])
        .await?;
    ctx.query("INSERT INTO sv VALUES (10)", &[]).await?;
    ctx.cdc_apply_settle().await?;

    let q = "SELECT id FROM su WHERE k IN (SELECT id FROM sv) ORDER BY id";
    register(&mut ctx, q).await?;

    // Admitting sv=20 makes su(2) qualify → the result grows to two rows.
    ctx.simple_query("INSERT INTO sv VALUES (20)").await?;
    let res = ctx.simple_query(q).await?;
    assert_eq!(
        row_count(&res),
        2,
        "subquery read must forward the grown membership"
    );

    // The growing membership invalidates the cached query; re-populate after the
    // drain, then confirm the following read cache-hits.
    ctx.cdc_apply_settle().await?;
    let res = ctx.simple_query(q).await?;
    assert_eq!(row_count(&res), 2);
    ctx.cache_settle().await?;
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(q).await?;
    assert_eq!(row_count(&res), 2);
    let _ = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// A LIMIT read: an in-place UPDATE to a row inside the limited window changes
/// the served value, so the read forwards during lag; single-table CDC applies
/// in place, so it cache-hits (with the new value) once drained.
#[tokio::test]
async fn test_gate_limit_shape() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&RAW_LAG).await?;
    ctx.query("CREATE TABLE lt (id int primary key, v int)", &[])
        .await?;
    ctx.query("INSERT INTO lt VALUES (1, 10), (2, 20), (3, 30)", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    let q = "SELECT id, v FROM lt ORDER BY id LIMIT 2";
    register(&mut ctx, q).await?;

    ctx.simple_query("UPDATE lt SET v = 99 WHERE id = 1")
        .await?;
    let res = ctx.simple_query(q).await?;
    assert_row_at(&res, 1, &[("v", "99")])?;

    ctx.cdc_apply_settle().await?;
    ctx.cache_settle().await?;
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(q).await?;
    assert_row_at(&res, 1, &[("v", "99")])?;
    let _ = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// A read with a typecast comparison (`col::text = …`): the cast column is
/// dropped from the read's ranges, so a pending write forwards conservatively;
/// behavior is neutral once CDC drains.
#[tokio::test]
async fn test_gate_typecast_shape() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&RAW_LAG).await?;
    ctx.query("CREATE TABLE tc (id int primary key, v int)", &[])
        .await?;
    ctx.query("INSERT INTO tc VALUES (1, 10), (2, 20)", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    let q = "SELECT id, v FROM tc WHERE id::text = '1'";
    register(&mut ctx, q).await?;

    ctx.simple_query("UPDATE tc SET v = 77 WHERE id = 1")
        .await?;
    let res = ctx.simple_query(q).await?;
    assert_row_at(&res, 1, &[("v", "77")])?;

    ctx.cdc_apply_settle().await?;
    ctx.cache_settle().await?;
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(q).await?;
    assert_row_at(&res, 1, &[("v", "77")])?;
    let _ = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// An extended-protocol parameterized read (`WHERE id = $1`) gets the same
/// row-level precision as a literal one: its bind value is substituted before
/// the gate derives the read's ranges, so a read disjoint from a pending
/// (literal) UPDATE serves from cache instead of forwarding.
#[tokio::test]
async fn test_gate_prepared_read_disjoint_precision() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&RAW_LAG).await?;
    ctx.query("CREATE TABLE pr (id int primary key, v int)", &[])
        .await?;
    ctx.query("INSERT INTO pr VALUES (1, 10), (2, 20)", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    // Register the prepared read for id = 2 (miss, then populate).
    let read = "SELECT v FROM pr WHERE id = $1";
    let m0 = ctx.metrics().await?;
    let _ = ctx.query(read, &[&2i32]).await?;
    let _ = assert_cache_miss(&mut ctx, m0).await?;
    ctx.cache_settle().await?;

    // Literal UPDATE of id = 1 on the same connection (concrete write predicate).
    ctx.simple_query("UPDATE pr SET v = 88 WHERE id = 1")
        .await?;

    // The prepared read of id = 2 is disjoint from the pending update → cache
    // hit, recorded by the update row-precision counter (which fires only if the
    // read's `$1` was substituted to a concrete value before the gate).
    let m = ctx.metrics().await?;
    let rows = ctx.query(read, &[&2i32]).await?;
    assert_eq!(rows[0].get::<_, i32>(0), 20);
    let delta = metrics_delta(&m, &ctx.metrics().await?);
    assert_eq!(
        delta.queries_cache_hit, 1,
        "disjoint prepared read must serve from cache"
    );
    assert!(
        delta.raw_serve_disjoint_update >= 1,
        "prepared read must be substituted for the row-level proof"
    );

    Ok(())
}

/// An extended-protocol parameterized read (`WHERE id = $1`) matching a pending
/// write on the same connection: the read overlaps the write, so it forwards
/// during lag and cache-hits again with the new value once CDC drains.
#[tokio::test]
async fn test_gate_prepared_read_shape() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&RAW_LAG).await?;
    ctx.query("CREATE TABLE pt (id int primary key, v int)", &[])
        .await?;
    ctx.query("INSERT INTO pt VALUES (1, 10), (2, 20)", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    let read = "SELECT v FROM pt WHERE id = $1";
    // Register the prepared read and confirm a baseline hit.
    let m = ctx.metrics().await?;
    let _ = ctx.query(read, &[&1i32]).await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    let _ = ctx.query(read, &[&1i32]).await?;
    let _ = assert_cache_hit(&mut ctx, m).await?;

    // Write on the same connection, then read (prepared) during lag → forward,
    // sees the new value.
    ctx.simple_query("UPDATE pt SET v = 88 WHERE id = 1")
        .await?;
    let rows = ctx.query(read, &[&1i32]).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 88, "prepared read must forward");

    // Drain: single-table UPDATE applies in place, so the prepared read hits.
    ctx.cdc_apply_settle().await?;
    ctx.cache_settle().await?;
    let m = ctx.metrics().await?;
    let rows = ctx.query(read, &[&1i32]).await?;
    assert_eq!(rows[0].get::<_, i32>(0), 88);
    let _ = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// PGC-386: an extended-protocol *parameterized* UPDATE (`... WHERE id = $1`)
/// records a concrete predicate — its bind value is substituted at record time,
/// so a disjoint parameterized read still serves from cache. Exercises both the
/// write-side substitution and the read-side gate substitution together.
#[tokio::test]
async fn test_gate_parameterized_update_precision() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&RAW_LAG).await?;
    ctx.query("CREATE TABLE pu (id int primary key, v int)", &[])
        .await?;
    ctx.query("INSERT INTO pu VALUES (1, 10), (2, 20)", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    // Register the prepared read for id = 2 (miss, then populate).
    let read = "SELECT v FROM pu WHERE id = $1";
    let m0 = ctx.metrics().await?;
    let _ = ctx.query(read, &[&2i32]).await?;
    let _ = assert_cache_miss(&mut ctx, m0).await?;
    ctx.cache_settle().await?;

    // Parameterized UPDATE of id = 1 over the extended protocol.
    ctx.query("UPDATE pu SET v = 88 WHERE id = $1", &[&1i32])
        .await?;

    // The read of id = 2 is disjoint from the substituted update predicate (id =
    // 1) → cache hit, recorded by the update row-precision counter (which fires
    // only if both the write predicate and the read's `$1` were substituted).
    let m = ctx.metrics().await?;
    let rows = ctx.query(read, &[&2i32]).await?;
    assert_eq!(rows[0].get::<_, i32>(0), 20);
    let delta = metrics_delta(&m, &ctx.metrics().await?);
    assert_eq!(
        delta.queries_cache_hit, 1,
        "disjoint read must serve from cache"
    );
    assert!(
        delta.raw_serve_disjoint_update >= 1,
        "parameterized update predicate must be substituted for the row-level proof"
    );

    // A read of id = 1 overlaps the substituted predicate → forward, sees v = 88.
    let rows = ctx.query(read, &[&1i32]).await?;
    assert_eq!(rows[0].get::<_, i32>(0), 88);

    Ok(())
}

/// PGC-381: row-level DELETE precision. A read provably disjoint from a pending
/// DELETE's predicate still serves from cache, while a read the delete removes
/// forwards and sees the deletion — both on the same connection, under CDC lag.
#[tokio::test]
async fn test_gate_delete_row_level_precision() -> Result<(), Error> {
    let mut ctx =
        TestContext::setup_fault(&[RAW_ON, ("PGCACHE_FAULT_CDC_APPLY_LAG_MS", "1500")]).await?;
    ctx.query("CREATE TABLE d (id int primary key, v int)", &[])
        .await?;
    ctx.query("INSERT INTO d VALUES (1, 10), (2, 20), (3, 30)", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    for sql in [
        "SELECT v FROM d WHERE id = 1",
        "SELECT v FROM d WHERE id = 3",
    ] {
        let m0 = ctx.metrics().await?;
        let _ = ctx.simple_query(sql).await?;
        let _ = assert_cache_miss(&mut ctx, m0).await?;
        ctx.cache_settle().await?;
    }

    // Delete id = 1 on this connection; id = 3 is disjoint from the delete.
    ctx.simple_query("DELETE FROM d WHERE id = 1").await?;

    // id = 3 is provably disjoint from the pending delete → still a cache hit,
    // recorded by the row-precision counter.
    let m = ctx.metrics().await?;
    let res = ctx.simple_query("SELECT v FROM d WHERE id = 3").await?;
    assert_row_at(&res, 1, &[("v", "30")])?;
    let delta = metrics_delta(&m, &ctx.metrics().await?);
    assert_eq!(
        delta.queries_cache_hit, 1,
        "disjoint read must serve from cache"
    );
    assert!(
        delta.raw_serve_disjoint_delete >= 1,
        "row-level disjointness proof expected"
    );

    // id = 1 is removed by the pending delete → forwarded, sees zero rows (a
    // stale cache hit would still return v = 10).
    let res = ctx.simple_query("SELECT v FROM d WHERE id = 1").await?;
    assert_eq!(
        row_count(&res),
        0,
        "deleted row must not be served from cache"
    );

    // Once CDC applies the single-table delete in place, the read cache-hits
    // again (now empty).
    ctx.cdc_apply_settle().await?;
    let m = ctx.metrics().await?;
    let res = ctx.simple_query("SELECT v FROM d WHERE id = 1").await?;
    assert_eq!(row_count(&res), 0);
    let _ = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// PGC-382: row-level UPDATE precision. A read disjoint from both the update's
/// WHERE and its post-update image still serves; a read a row grows *into* (via
/// the SET image) forwards and sees the change — both under CDC lag.
#[tokio::test]
async fn test_gate_update_row_level_precision() -> Result<(), Error> {
    let mut ctx =
        TestContext::setup_fault(&[RAW_ON, ("PGCACHE_FAULT_CDC_APPLY_LAG_MS", "1500")]).await?;
    ctx.query("CREATE TABLE u (id int primary key, v int)", &[])
        .await?;
    ctx.query("INSERT INTO u VALUES (1, 10), (2, 20), (3, 30)", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    // id = 1 (present) and v = 99 (absent for now — the grow target).
    for sql in [
        "SELECT v FROM u WHERE id = 1",
        "SELECT id FROM u WHERE v = 99",
    ] {
        let m0 = ctx.metrics().await?;
        let _ = ctx.simple_query(sql).await?;
        let _ = assert_cache_miss(&mut ctx, m0).await?;
        ctx.cache_settle().await?;
    }

    // Update id = 2's v to 99 on this connection.
    ctx.simple_query("UPDATE u SET v = 99 WHERE id = 2").await?;

    // id = 1 is disjoint from the update (WHERE id = 2, image {id=2, v=99}) →
    // cache hit, recorded by the row-precision counter.
    let m = ctx.metrics().await?;
    let res = ctx.simple_query("SELECT v FROM u WHERE id = 1").await?;
    assert_row_at(&res, 1, &[("v", "10")])?;
    let delta = metrics_delta(&m, &ctx.metrics().await?);
    assert_eq!(
        delta.queries_cache_hit, 1,
        "disjoint read must serve from cache"
    );
    assert!(
        delta.raw_serve_disjoint_update >= 1,
        "row-level disjointness proof expected"
    );

    // `v = 99` grows to include the updated id = 2 → forwarded, sees the row
    // (a stale cache hit would return no rows).
    let res = ctx.simple_query("SELECT id FROM u WHERE v = 99").await?;
    assert_row_at(&res, 1, &[("id", "2")])?;

    // After CDC applies the single-table update in place, the read cache-hits.
    ctx.cdc_apply_settle().await?;
    let m = ctx.metrics().await?;
    let res = ctx.simple_query("SELECT id FROM u WHERE v = 99").await?;
    assert_eq!(row_count(&res), 1);
    let _ = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// PGC-384: DELETE across a JOIN shape. A delete to a joined table shrinks the
/// result; the read forwards during lag and cache-hits once the (in-place)
/// shrink is applied.
#[tokio::test]
async fn test_gate_delete_join_shape() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&RAW_LAG).await?;
    ctx.query("CREATE TABLE dja (id int primary key, bid int)", &[])
        .await?;
    ctx.query("CREATE TABLE djb (id int primary key, label text)", &[])
        .await?;
    ctx.query("INSERT INTO dja VALUES (1, 10), (2, 10)", &[])
        .await?;
    ctx.query("INSERT INTO djb VALUES (10, 'x')", &[]).await?;
    ctx.cdc_apply_settle().await?;

    let q = "SELECT dja.id FROM dja JOIN djb ON dja.bid = djb.id ORDER BY dja.id";
    register(&mut ctx, q).await?;

    // Deleting dja(1) shrinks the join to a single row.
    ctx.simple_query("DELETE FROM dja WHERE id = 1").await?;
    let res = ctx.simple_query(q).await?;
    assert_eq!(
        row_count(&res),
        1,
        "join read must forward the shrunk result"
    );

    // A delete only shrinks, so CDC applies it in place — the read cache-hits.
    ctx.cdc_apply_settle().await?;
    ctx.cache_settle().await?;
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(q).await?;
    assert_eq!(row_count(&res), 1);
    let _ = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// PGC-384: UPDATE across an IN-subquery shape. Updating a row out of the
/// subquery's match set shrinks membership; the read forwards during lag and
/// cache-hits once applied.
#[tokio::test]
async fn test_gate_update_subquery_shape() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&RAW_LAG).await?;
    ctx.query("CREATE TABLE usu (id int primary key, k int)", &[])
        .await?;
    ctx.query("CREATE TABLE usv (id int primary key)", &[])
        .await?;
    ctx.query("INSERT INTO usu VALUES (1, 10), (2, 20)", &[])
        .await?;
    ctx.query("INSERT INTO usv VALUES (10)", &[]).await?;
    ctx.cdc_apply_settle().await?;

    let q = "SELECT id FROM usu WHERE k IN (SELECT id FROM usv) ORDER BY id";
    register(&mut ctx, q).await?;

    // Move usu(1) out of the k = 10 match set → the result shrinks to empty.
    ctx.simple_query("UPDATE usu SET k = 99 WHERE id = 1")
        .await?;
    let res = ctx.simple_query(q).await?;
    assert_eq!(
        row_count(&res),
        0,
        "subquery read must forward the shrunk result"
    );

    ctx.cdc_apply_settle().await?;
    ctx.cache_settle().await?;
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(q).await?;
    assert_eq!(row_count(&res), 0);
    let _ = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// PGC-445: a multi-execute batch that rebinds the unnamed portal must record
/// each execute's own bind values — resolving portals at Sync time would alias
/// every entry to the last Bind, and a read matching an earlier execute's row
/// would be wrongly proven disjoint and served from a cache that predates the
/// batch's own committed writes.
#[tokio::test]
async fn test_gate_batch_portal_rebind_logs_each_execute() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&RAW_LAG).await?;
    ctx.query("CREATE TABLE raw_batch (id int primary key, v int)", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    // Register the read so a stale serve is possible at all.
    let q = "SELECT id, v FROM raw_batch WHERE id = 1";
    register(&mut ctx, q).await?;

    // One pgproto connection: INSERT $1 bound to 1 then rebound to 2 in a
    // single Sync batch, then the read of id = 1 while the writes are
    // unsettled (1.5s injected apply lag). The read must see the row.
    let m = ctx.metrics().await?;
    let output =
        crate::util::pgproto_run(ctx.cache_port, "tests/data/pgproto/raw_batch_rebind.data");
    let rows = output.matches("<= BE DataRow").count();
    assert_eq!(
        rows, 1,
        "read of the first execute's row must forward, not serve stale:\n{output}"
    );
    // Both executes must be logged, and the gated read must have forwarded —
    // a stale serve would have counted a cache hit instead.
    let after = metrics_delta(&m, &ctx.metrics().await?);
    assert_eq!(
        after.raw_writes_recorded, 2,
        "each execute must record its own write"
    );
    assert_eq!(
        after.queries_cache_hit, 0,
        "the gated read must forward, not serve from cache"
    );
    Ok(())
}

/// PGC-446: a pending text write must never be proven disjoint from a read by
/// byte *ordering* — byte order disagrees with collation order ('a' > 'B' in
/// bytes, 'a' < 'B' under en_US). A pending `DELETE ... WHERE name < 'B'`
/// must gate an equality read of 'a': byte order would "prove" 'a' outside
/// the deleted range and serve the cached (pre-delete) row.
#[tokio::test]
async fn test_gate_string_ordering_never_proves_disjoint() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&RAW_LAG).await?;
    ctx.query("CREATE TABLE raw_text (id int primary key, name text)", &[])
        .await?;
    ctx.query("INSERT INTO raw_text VALUES (1, 'a')", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    let q = "SELECT id, name FROM raw_text WHERE name = 'a'";
    register(&mut ctx, q).await?;

    // Pending range delete on the same connection removes 'a' at origin.
    let m = ctx.metrics().await?;
    ctx.simple_query("DELETE FROM raw_text WHERE name < 'B'")
        .await?;
    let res = ctx.simple_query(q).await?;
    assert_eq!(
        row_count(&res),
        0,
        "the read must forward and observe its own pending delete"
    );
    let after = metrics_delta(&m, &ctx.metrics().await?);
    assert_eq!(
        after.queries_cache_hit, 0,
        "text ordering must not prove disjointness"
    );
    Ok(())
}

/// PGC-448: an Execute of a statement pg_query cannot parse must record
/// conservatively at connection scope (parser version skew could make it a
/// genuine write at origin), mirroring the simple path's failure direction.
#[tokio::test]
async fn test_gate_parse_error_execute_records_connection_scope() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&[RAW_ON]).await?;
    let m = ctx.metrics().await?;
    let _ = crate::util::pgproto_run(
        ctx.cache_port,
        "tests/data/pgproto/parse_error_execute.data",
    );
    let after = metrics_delta(&m, &ctx.metrics().await?);
    assert!(
        after.raw_writes_recorded >= 1,
        "parse-error execute must be recorded, delta: {}",
        after.raw_writes_recorded
    );
    Ok(())
}

/// PGC-449: a SELECT whose AST conversion fails has no tree to scan for
/// volatile (writing) functions, so it must record conservatively at
/// connection scope — the same failure direction as a parse failure.
#[tokio::test]
async fn test_gate_conversion_failed_select_records_connection_scope() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&RAW_LAG).await?;
    ctx.query("CREATE TABLE raw_conv (id int primary key)", &[])
        .await?;
    ctx.query("INSERT INTO raw_conv VALUES (1)", &[]).await?;
    ctx.cdc_apply_settle().await?;
    let q = "SELECT id FROM raw_conv WHERE id = 1";
    register(&mut ctx, q).await?;

    // WITH RECURSIVE fails conversion; it could hide a volatile writing
    // function, so it must poison the connection scope.
    let m = ctx.metrics().await?;
    let _ = ctx
        .simple_query("WITH RECURSIVE r AS (SELECT 1) SELECT * FROM r")
        .await?;
    let res = ctx.simple_query(q).await?;
    assert_eq!(row_count(&res), 1);
    let after = metrics_delta(&m, &ctx.metrics().await?);
    assert!(
        after.raw_writes_recorded >= 1,
        "conversion-failed select must be recorded"
    );
    assert_eq!(
        after.queries_cache_hit, 0,
        "reads after an unconvertible select must forward until settle"
    );
    Ok(())
}
