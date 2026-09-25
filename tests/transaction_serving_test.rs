//! In-transaction cache serving under read-after-write (PGC-387). The gate
//! depends on the per-connection write log, which the shared harness turns off
//! through its fault hook, so every test here re-enables it and builds only
//! with the feature.
#![cfg(feature = "fault-injection")]

use std::io::Error;

use crate::util::{
    TestContext, WireClient, assert_cache_hit, assert_cache_miss, assert_row_at, metrics_delta,
};

mod util;

/// Re-enable per-connection read-after-write (the harness disables it by
/// default so write-then-read tests get deterministic hits).
const RAW_ON: (&str, &str) = ("PGCACHE_FAULT_READ_YOUR_WRITES_OFF", "0");

const TABLE_SQL: &str = "CREATE TABLE txn_items (id int primary key, v int)";
const SEED_SQL: &str = "INSERT INTO txn_items VALUES (1, 10), (2, 20), (3, 30)";
const READ_ONE: &str = "SELECT v FROM txn_items WHERE id = 1";
const READ_TWO: &str = "SELECT v FROM txn_items WHERE id = 2";

/// Create and seed the table, then register `sql` into a steady cache hit.
async fn seed_and_register(ctx: &mut TestContext, sql: &str) -> Result<(), Error> {
    ctx.query(TABLE_SQL, &[]).await?;
    ctx.simple_query(SEED_SQL).await?;
    ctx.cache_settle().await?;
    register(ctx, sql).await
}

/// Register a simple-protocol read into the cache and confirm it reaches a
/// steady cache hit (miss → settle → hit).
async fn register(ctx: &mut TestContext, sql: &str) -> Result<(), Error> {
    let m = ctx.metrics().await?;
    let _ = ctx.simple_query(sql).await?;
    let m = assert_cache_miss(ctx, m).await?;
    ctx.cache_settle().await?;
    let _ = ctx.simple_query(sql).await?;
    let _ = assert_cache_hit(ctx, m).await?;
    Ok(())
}

/// A read inside a READ COMMITTED block is served from cache, and the served
/// response ends with an in-transaction (`T`) ReadyForQuery so the client's
/// transaction state stays correct.
#[tokio::test]
async fn test_in_transaction_read_served_with_transaction_status() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&[RAW_ON]).await?;
    seed_and_register(&mut ctx, READ_ONE).await?;

    let mut wire = WireClient::connect(ctx.cache_port).await?;
    let begin = wire.query("BEGIN").await?;
    assert_eq!(begin.ready_status(), b'T');

    let before = ctx.metrics().await?;
    let read = wire.query(READ_ONE).await?;
    assert_eq!(read.data_row_count(), 1);
    assert_eq!(
        read.ready_status(),
        b'T',
        "a cache-served read inside a block must report in-transaction status"
    );
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.queries_cache_hit, 1, "expected an in-txn cache hit");
    assert_eq!(delta.txn_served, 1);

    // The block is still open on origin: a write inside it commits with it.
    wire.query("UPDATE txn_items SET v = 11 WHERE id = 3")
        .await?;
    let commit = wire.query("COMMIT").await?;
    assert_eq!(commit.ready_status(), b'I');
    let rows = ctx
        .origin_query("SELECT v FROM txn_items WHERE id = 3", &[])
        .await?;
    assert_eq!(rows[0].get::<_, i32>(0), 11);
    Ok(())
}

/// The block's own uncommitted write is visible to its reads: an intersecting
/// read forwards (and sees the write), a disjoint one still serves from cache,
/// and after ROLLBACK nothing of the write survives.
#[tokio::test]
async fn test_in_transaction_own_write_intersection_forwards_disjoint_serves() -> Result<(), Error>
{
    let mut ctx = TestContext::setup_fault(&[RAW_ON]).await?;
    seed_and_register(&mut ctx, READ_ONE).await?;
    register(&mut ctx, READ_TWO).await?;

    ctx.simple_query("BEGIN").await?;
    ctx.simple_query("UPDATE txn_items SET v = 99 WHERE id = 1")
        .await?;

    let before = ctx.metrics().await?;
    let res = ctx.simple_query(READ_ONE).await?;
    assert_row_at(&res, 1, &[("v", "99")])?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.queries_cache_hit, 0, "intersecting read must forward");
    assert_eq!(delta.txn_forward_pending_write, 1);

    let before = ctx.metrics().await?;
    let res = ctx.simple_query(READ_TWO).await?;
    assert_row_at(&res, 1, &[("v", "20")])?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.queries_cache_hit, 1, "disjoint read must serve");
    assert_eq!(delta.txn_served, 1);

    ctx.simple_query("ROLLBACK").await?;
    ctx.cache_settle().await?;
    let res = ctx.simple_query(READ_ONE).await?;
    assert_row_at(&res, 1, &[("v", "10")])?;
    Ok(())
}

/// In a failed block origin rejects every statement; a cacheable read must
/// return that error rather than rows from cache.
#[tokio::test]
async fn test_failed_transaction_read_forwards_aborted_error() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&[RAW_ON]).await?;
    seed_and_register(&mut ctx, READ_ONE).await?;

    let mut wire = WireClient::connect(ctx.cache_port).await?;
    wire.query("BEGIN").await?;
    let failed = wire.query("SELECT 1/0").await?;
    assert_eq!(failed.ready_status(), b'E');

    let before = ctx.metrics().await?;
    let read = wire.query(READ_ONE).await?;
    assert_eq!(
        read.error_sqlstate().as_deref(),
        Some("25P02"),
        "expected in_failed_sql_transaction"
    );
    assert_eq!(read.data_row_count(), 0);
    assert_eq!(read.ready_status(), b'E');
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.queries_cache_hit, 0);
    assert_eq!(delta.txn_forward_failed, 1);

    let rollback = wire.query("ROLLBACK").await?;
    assert_eq!(rollback.ready_status(), b'I');
    Ok(())
}

/// Stricter isolation levels forward, whether set on the BEGIN itself, by SET
/// TRANSACTION, or as the session default; switching the default back to READ
/// COMMITTED is confirmed by a re-probe and serving resumes.
#[tokio::test]
async fn test_strict_isolation_forwards_read_committed_serves() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&[RAW_ON]).await?;
    seed_and_register(&mut ctx, READ_ONE).await?;

    for begin in [
        "BEGIN ISOLATION LEVEL REPEATABLE READ",
        "START TRANSACTION ISOLATION LEVEL SERIALIZABLE",
    ] {
        ctx.simple_query(begin).await?;
        let before = ctx.metrics().await?;
        let res = ctx.simple_query(READ_ONE).await?;
        assert_row_at(&res, 1, &[("v", "10")])?;
        let delta = metrics_delta(&before, &ctx.metrics().await?);
        assert_eq!(delta.queries_cache_hit, 0, "{begin}: must forward");
        assert_eq!(delta.txn_forward_isolation_strict, 1, "{begin}");
        ctx.simple_query("COMMIT").await?;
    }

    ctx.simple_query("BEGIN").await?;
    ctx.simple_query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .await?;
    let before = ctx.metrics().await?;
    ctx.simple_query(READ_ONE).await?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.txn_forward_isolation_strict, 1, "SET TRANSACTION");
    ctx.simple_query("COMMIT").await?;

    ctx.simple_query("SET default_transaction_isolation = 'serializable'")
        .await?;
    ctx.simple_query("BEGIN").await?;
    let before = ctx.metrics().await?;
    ctx.simple_query(READ_ONE).await?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.txn_forward_isolation_strict, 1, "session default");
    ctx.simple_query("COMMIT").await?;

    // Back to READ COMMITTED: the SET marks the default unknown, the probe
    // injected at its ReadyForQuery confirms it before the next block's read.
    let before = ctx.metrics().await?;
    ctx.simple_query("SET default_transaction_isolation = 'read committed'")
        .await?;
    ctx.simple_query("BEGIN").await?;
    ctx.simple_query(READ_ONE).await?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert!(delta.txn_isolation_probes >= 1, "expected a re-probe");
    assert_eq!(delta.queries_cache_hit, 1, "READ COMMITTED serves again");
    assert_eq!(delta.txn_served, 1);
    ctx.simple_query("COMMIT").await?;
    Ok(())
}

/// A stricter default configured on the role — invisible on the wire — is
/// discovered by the connect-time probe, so a fresh connection forwards.
#[tokio::test]
async fn test_role_default_isolation_discovered_at_connect() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&[RAW_ON]).await?;
    seed_and_register(&mut ctx, READ_ONE).await?;

    ctx.origin_query(
        "ALTER ROLE postgres SET default_transaction_isolation = 'repeatable read'",
        &[],
    )
    .await?;

    let strict = ctx.proxy_client_connect().await?;
    strict.simple_query("BEGIN").await.map_err(Error::other)?;
    let before = ctx.metrics().await?;
    strict.simple_query(READ_ONE).await.map_err(Error::other)?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.queries_cache_hit, 0, "role default must forward");
    assert_eq!(delta.txn_forward_isolation_strict, 1);
    strict.simple_query("COMMIT").await.map_err(Error::other)?;

    // The pre-existing connection keeps its probed READ COMMITTED default.
    ctx.simple_query("BEGIN").await?;
    let before = ctx.metrics().await?;
    ctx.simple_query(READ_ONE).await?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.txn_served, 1);
    ctx.simple_query("COMMIT").await?;
    Ok(())
}

/// `SET LOCAL default_transaction_isolation` inside a block changes the
/// session default (until the block ends), not the open block: its reads keep
/// serving at the level the block started with, and the default is re-probed
/// after COMMIT so the next block is classified afresh.
#[tokio::test]
async fn test_set_local_isolation_rediscovered_after_commit() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&[RAW_ON]).await?;
    seed_and_register(&mut ctx, READ_ONE).await?;

    ctx.simple_query("BEGIN").await?;
    ctx.simple_query("SET LOCAL default_transaction_isolation = 'serializable'")
        .await?;
    let before = ctx.metrics().await?;
    ctx.simple_query(READ_ONE).await?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.txn_served, 1, "the open block stays READ COMMITTED");
    assert_eq!(delta.txn_isolation_probes, 0, "no probe inside a block");
    // The txn-end re-probe is injected at COMMIT's ReadyForQuery.
    let before = ctx.metrics().await?;
    ctx.simple_query("COMMIT").await?;
    ctx.simple_query("BEGIN").await?;
    ctx.simple_query(READ_ONE).await?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert!(
        delta.txn_isolation_probes >= 1,
        "expected a txn-end re-probe"
    );
    assert_eq!(delta.txn_served, 1, "default reverted to READ COMMITTED");
    ctx.simple_query("COMMIT").await?;
    Ok(())
}

/// Extended-protocol reads inside a block (tokio-postgres `transaction()`)
/// serve from cache too.
#[tokio::test]
async fn test_extended_protocol_read_in_transaction_served() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&[RAW_ON]).await?;
    ctx.query(TABLE_SQL, &[]).await?;
    ctx.simple_query(SEED_SQL).await?;
    ctx.cache_settle().await?;

    let mut client = ctx.proxy_client_connect().await?;
    let stmt = client.prepare(READ_ONE).await.map_err(Error::other)?;
    let m = ctx.metrics().await?;
    client.query(&stmt, &[]).await.map_err(Error::other)?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    client.query(&stmt, &[]).await.map_err(Error::other)?;
    assert_cache_hit(&mut ctx, m).await?;

    let txn = client.transaction().await.map_err(Error::other)?;
    let before = ctx.metrics().await?;
    let rows = txn.query(&stmt, &[]).await.map_err(Error::other)?;
    assert_eq!(rows[0].get::<_, i32>(0), 10);
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.queries_cache_hit, 1);
    assert_eq!(delta.txn_served, 1);
    txn.execute("UPDATE txn_items SET v = 12 WHERE id = 2", &[])
        .await
        .map_err(Error::other)?;
    txn.commit().await.map_err(Error::other)?;

    let rows = ctx
        .origin_query("SELECT v FROM txn_items WHERE id = 2", &[])
        .await?;
    assert_eq!(rows[0].get::<_, i32>(0), 12, "the block committed");
    Ok(())
}

/// A read pipelined right behind BEGIN (sent before BEGIN's ReadyForQuery is
/// back) is gated on the state current when its slot reaches the head: it is
/// served with in-transaction status, not idle.
#[tokio::test]
async fn test_pipelined_begin_then_read_reports_transaction_status() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&[RAW_ON]).await?;
    seed_and_register(&mut ctx, READ_ONE).await?;

    let mut wire = WireClient::connect(ctx.cache_port).await?;
    let before = ctx.metrics().await?;
    wire.query_send("BEGIN").await?;
    wire.query_send(READ_ONE).await?;
    let begin = wire.response_read().await?;
    assert_eq!(begin.ready_status(), b'T');
    let read = wire.response_read().await?;
    assert_eq!(read.data_row_count(), 1);
    assert_eq!(read.ready_status(), b'T');
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.queries_cache_hit, 1);
    assert_eq!(delta.txn_served, 1);
    let rollback = wire.query("ROLLBACK").await?;
    assert_eq!(rollback.ready_status(), b'I');
    Ok(())
}

// --- Review findings: block-scoped isolation under pipelining and mid-block changes ---

/// A block's level is fixed when it starts. Changing the session default
/// inside a strict block must not loosen the block; the new default applies
/// to the next block (confirmed by the re-probe at block end).
#[tokio::test]
async fn test_session_default_change_inside_block_keeps_block_level() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&[RAW_ON]).await?;
    seed_and_register(&mut ctx, READ_ONE).await?;
    ctx.origin_query(
        "ALTER ROLE postgres SET default_transaction_isolation = 'repeatable read'",
        &[],
    )
    .await?;

    let client = ctx.proxy_client_connect().await?;
    client.simple_query("BEGIN").await.map_err(Error::other)?;
    client
        .simple_query("SET default_transaction_isolation = 'read committed'")
        .await
        .map_err(Error::other)?;
    let before = ctx.metrics().await?;
    client.simple_query(READ_ONE).await.map_err(Error::other)?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(
        delta.queries_cache_hit, 0,
        "the block is still REPEATABLE READ"
    );
    assert_eq!(delta.txn_forward_isolation_strict, 1);

    // The re-probe is injected at COMMIT's ReadyForQuery.
    let before = ctx.metrics().await?;
    client.simple_query("COMMIT").await.map_err(Error::other)?;
    client.simple_query("BEGIN").await.map_err(Error::other)?;
    client.simple_query(READ_ONE).await.map_err(Error::other)?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert!(
        delta.txn_isolation_probes >= 1,
        "default re-probed at block end"
    );
    assert_eq!(delta.txn_served, 1, "the next block starts READ COMMITTED");
    client.simple_query("COMMIT").await.map_err(Error::other)?;
    Ok(())
}

/// Pipelined `COMMIT; BEGIN ISOLATION LEVEL ...; read`: the new block's
/// clause must survive the previous block's end and gate the read.
#[tokio::test]
async fn test_pipelined_commit_begin_isolation_read_forwards() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&[RAW_ON]).await?;
    seed_and_register(&mut ctx, READ_ONE).await?;

    let mut wire = WireClient::connect(ctx.cache_port).await?;
    wire.query("BEGIN").await?;
    let before = ctx.metrics().await?;
    wire.query_send("COMMIT").await?;
    wire.query_send("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await?;
    wire.query_send(READ_ONE).await?;
    assert_eq!(wire.response_read().await?.ready_status(), b'I');
    assert_eq!(wire.response_read().await?.ready_status(), b'T');
    let read = wire.response_read().await?;
    assert_eq!(read.data_row_count(), 1);
    assert_eq!(read.ready_status(), b'T');
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(
        delta.queries_cache_hit, 0,
        "SERIALIZABLE block must forward"
    );
    assert_eq!(delta.txn_forward_isolation_strict, 1);
    wire.query("ROLLBACK").await?;
    Ok(())
}

/// `SET TRANSACTION` outside a block is a no-op at origin (a warning); it must
/// not install a level for the next block.
#[tokio::test]
async fn test_set_transaction_outside_block_is_ignored() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&[RAW_ON]).await?;
    seed_and_register(&mut ctx, READ_ONE).await?;

    ctx.simple_query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        .await?;
    ctx.simple_query("BEGIN").await?;
    let before = ctx.metrics().await?;
    ctx.simple_query(READ_ONE).await?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.txn_served, 1, "the block starts READ COMMITTED");
    ctx.simple_query("COMMIT").await?;
    Ok(())
}

/// Pipelined `BEGIN; SET default_transaction_isolation; ROLLBACK`: the SET is
/// forwarded before BEGIN's ReadyForQuery arrives, yet it belongs to the block
/// and reverts with it, so the default must be re-probed.
#[tokio::test]
async fn test_pipelined_begin_set_default_rollback_reprobes() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&[RAW_ON]).await?;
    seed_and_register(&mut ctx, READ_ONE).await?;

    let mut wire = WireClient::connect(ctx.cache_port).await?;
    let before = ctx.metrics().await?;
    wire.query_send("BEGIN").await?;
    wire.query_send("SET default_transaction_isolation = 'serializable'")
        .await?;
    wire.query_send("ROLLBACK").await?;
    for _ in 0..3 {
        wire.response_read().await?;
    }
    wire.query("BEGIN").await?;
    let read = wire.query(READ_ONE).await?;
    assert_eq!(read.data_row_count(), 1);
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert!(
        delta.txn_isolation_probes >= 1,
        "default re-probed after rollback"
    );
    assert_eq!(
        delta.txn_served, 1,
        "the reverted default is READ COMMITTED"
    );
    wire.query("COMMIT").await?;
    Ok(())
}

/// No isolation probe is injected while a block is open, so a failed block
/// whose default became unknown (a `set_config` call) cannot loop probes
/// against origin; the probe fires once the block ends.
#[tokio::test]
async fn test_failed_block_does_not_probe_repeatedly() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&[RAW_ON]).await?;
    seed_and_register(&mut ctx, READ_ONE).await?;

    ctx.simple_query("BEGIN").await?;
    ctx.simple_query("SELECT set_config('app.tenant', 'x', false)")
        .await?;
    let _ = ctx.simple_query("SELECT 1/0").await;
    let before = ctx.metrics().await?;
    tokio::time::sleep(std::time::Duration::from_millis(700)).await;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.txn_isolation_probes, 0, "no probe inside a block");

    ctx.simple_query("ROLLBACK").await?;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert!(
        delta.txn_isolation_probes <= 1,
        "one probe after the block ends, got {}",
        delta.txn_isolation_probes
    );
    Ok(())
}

/// A serve that fails after bytes reached the client inside a block cannot
/// be reconciled with a ReadyForQuery: the connection closes and origin
/// rolls the block back, so the block's earlier write never commits.
#[tokio::test]
async fn test_serve_failure_inside_block_closes_connection_and_rolls_back() -> Result<(), Error> {
    let mut ctx =
        TestContext::setup_fault(&[RAW_ON, ("PGCACHE_FAULT_MIDSTREAM_SERVES", "1")]).await?;
    ctx.query(TABLE_SQL, &[]).await?;
    ctx.simple_query(SEED_SQL).await?;
    ctx.cache_settle().await?;
    // Register without a hit: the fault must strike the in-block serve.
    let _ = ctx.simple_query(READ_ONE).await?;
    ctx.cache_settle().await?;

    let mut wire = WireClient::connect(ctx.cache_port).await?;
    wire.query("BEGIN").await?;
    wire.query("UPDATE txn_items SET v = 99 WHERE id = 2")
        .await?;
    let before = ctx.metrics().await?;
    wire.query_send(READ_ONE).await?;
    assert!(
        wire.response_read().await.is_err(),
        "the connection must close without a ReadyForQuery"
    );
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.queries_cache_error, 1);

    let rows = ctx
        .origin_query("SELECT v FROM txn_items WHERE id = 2", &[])
        .await?;
    assert_eq!(
        rows[0].get::<_, i32>(0),
        20,
        "the block must have rolled back"
    );
    Ok(())
}
