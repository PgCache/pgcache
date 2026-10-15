//! Probe replies must never be mistaken for a client statement's response.
//!
//! pgcache injects probes (the batched session-settings sample and
//! `pg_current_wal_insert_lsn()`) on the origin socket and swallows their
//! replies. A probe is injected at an origin
//! `ReadyForQuery`, and a client statement may already be in flight behind
//! that point. Before the fix the probe's intercept swallowed whatever came
//! back next — the client's real response — and the probe's own rows were
//! relayed in its place: a `SELECT` or `UPDATE` answered with "read committed"
//! (0.7.0, the first statement of most fresh connections on PG 17 and earlier).
//! Probes now hold a position in the egress queue's origin reply order.
//!
//! Pipelining puts a client statement in flight at the injection point
//! deterministically. A real client waits for the startup `ReadyForQuery`
//! before its first statement; the pipelined startup here is a synthetic
//! trigger for the same condition the pre-PG18 chained probe and ordinary
//! extended-protocol pipelining reach legitimately.

use std::io::Error;

use crate::util::{TestContext, WireClient, assert_cache_hit, assert_cache_miss, metrics_delta};

mod util;

const TABLE_SQL: &str = "CREATE TABLE startup_items (id int primary key, v int)";
const SEED_SQL: &str = "INSERT INTO startup_items VALUES (1, 10), (2, 20)";
const READ_ONE: &str = "SELECT v FROM startup_items WHERE id = 1";

/// Open a fresh connection with `sql` pipelined behind the startup message and
/// return that statement's response.
async fn first_statement(
    ctx: &TestContext,
    sql: &str,
) -> Result<(WireClient, util::WireResponse), Error> {
    let (client, response) = WireClient::connect_pipelined(ctx.cache_port, Some(sql)).await?;
    response
        .map(|r| (client, r))
        .ok_or_else(|| Error::other("no response to the pipelined first query"))
}

/// A forwarded read pipelined behind startup is answered with its own rows,
/// and the connection is healthy afterwards.
#[tokio::test]
async fn test_first_statement_pipelined_behind_startup_gets_its_own_result() -> Result<(), Error> {
    let ctx = TestContext::setup().await?;

    let (mut client, response) = first_statement(&ctx, "SELECT 42 AS answer").await?;
    assert_eq!(response.command_tags(), ["SELECT 1"]);
    assert_eq!(response.first_value().as_deref(), Some("42"));
    assert_eq!(response.ready_status(), b'I');

    let next = client.query("SELECT 7").await?;
    assert_eq!(next.first_value().as_deref(), Some("7"));
    Ok(())
}

/// An uncacheable read (`now()`) as the first statement.
#[tokio::test]
async fn test_first_statement_uncacheable_select_pipelined_behind_startup() -> Result<(), Error> {
    let ctx = TestContext::setup().await?;

    let (_client, response) = first_statement(&ctx, "SELECT extract(year FROM now())::int").await?;
    assert_eq!(response.command_tags(), ["SELECT 1"]);
    let year: i32 = response
        .first_value()
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| {
            Error::other(format!("expected a year, got {:?}", response.first_value()))
        })?;
    assert!(year >= 2026, "year {year}");
    Ok(())
}

/// A write as the first statement: it runs on origin, and the client must see
/// its command tag rather than a probe's rows.
#[tokio::test]
async fn test_first_statement_write_pipelined_behind_startup_reports_command_tag()
-> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(TABLE_SQL, &[]).await?;
    ctx.simple_query(SEED_SQL).await?;

    let (_client, response) =
        first_statement(&ctx, "UPDATE startup_items SET v = 11 WHERE id = 1").await?;
    assert_eq!(response.command_tags(), ["UPDATE 1"]);
    assert_eq!(response.data_row_count(), 0);
    assert_eq!(response.ready_status(), b'I');

    let rows = ctx
        .origin_query("SELECT v FROM startup_items WHERE id = 1", &[])
        .await?;
    assert_eq!(rows[0].get::<_, i32>(0), 11);
    Ok(())
}

/// A cached read as the first statement: served from cache, after the startup
/// response and with the right rows.
#[tokio::test]
async fn test_first_statement_cached_read_pipelined_behind_startup() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(TABLE_SQL, &[]).await?;
    ctx.simple_query(SEED_SQL).await?;
    ctx.cache_settle().await?;

    let m = ctx.metrics().await?;
    ctx.simple_query(READ_ONE).await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    ctx.simple_query(READ_ONE).await?;
    let m = assert_cache_hit(&mut ctx, m).await?;

    let (mut client, response) = first_statement(&ctx, READ_ONE).await?;
    assert_eq!(response.command_tags(), ["SELECT 1"]);
    assert_eq!(response.first_value().as_deref(), Some("10"));
    let _ = assert_cache_hit(&mut ctx, m).await?;

    let next = client.query(READ_ONE).await?;
    assert_eq!(next.first_value().as_deref(), Some("10"));
    Ok(())
}

/// The isolation default is re-probed at the ReadyForQuery of a statement that
/// makes it unknown. A statement pipelined behind that one is in flight when
/// the probe is injected and must still get its own response.
#[tokio::test]
async fn test_isolation_reprobe_with_pipelined_statement_behind_it() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    let mut client = WireClient::connect(ctx.cache_port).await?;
    let before = ctx.metrics().await?;
    client
        .query_send("SET default_transaction_isolation = 'read committed'")
        .await?;
    client.query_send("SELECT 42 AS answer").await?;

    let set = client.response_read().await?;
    assert_eq!(set.command_tags(), ["SET"]);
    let select = client.response_read().await?;
    assert_eq!(select.command_tags(), ["SELECT 1"]);
    assert_eq!(select.first_value().as_deref(), Some("42"));

    // The re-probe did fire (and its reply went nowhere the client could see).
    let next = client.query("SELECT 7").await?;
    assert_eq!(next.first_value().as_deref(), Some("7"));
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert!(
        delta.conn_settings_probes >= 1,
        "expected a re-probe after SET default_transaction_isolation, got {}",
        delta.conn_settings_probes
    );
    Ok(())
}
