//! Per-connection session-settings tracking (PGC-287, ADR-056): one state for
//! every tracked GUC, fed by `ParameterStatus` and one batched probe, with
//! block-scoped values for `SET LOCAL`.

use std::io::Error;

use crate::util::{
    TestContext, WireClient, assert_cache_hit, assert_cache_miss, assert_row_at, metrics_delta,
};

mod util;

const RAW_ON: (&str, &str) = ("PGCACHE_FAULT_READ_YOUR_WRITES_OFF", "0");
const TABLE_SQL: &str = "CREATE TABLE settings_items (id int primary key, v int)";
const SEED_SQL: &str = "INSERT INTO settings_items VALUES (1, 10), (2, 20)";
const READ_ONE: &str = "SELECT v FROM settings_items WHERE id = 1";

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

async fn config_table_create(
    ctx: &mut TestContext,
    schema: &str,
    value: &str,
) -> Result<(), Error> {
    ctx.origin_query(&format!("CREATE SCHEMA {schema}"), &[])
        .await?;
    ctx.origin_query(
        &format!("CREATE TABLE {schema}.config (id integer primary key, value text)"),
        &[],
    )
    .await?;
    ctx.origin_query(
        &format!("INSERT INTO {schema}.config (id, value) VALUES (1, '{value}')"),
        &[],
    )
    .await?;
    Ok(())
}

/// A `SET` of a setting origin does not report marks it unknown; the probe
/// injected at the SET's ReadyForQuery re-reads it, and the next cacheable
/// read is served once the value is known again.
#[tokio::test]
async fn test_set_unreported_setting_probes_then_serves() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(TABLE_SQL, &[]).await?;
    ctx.simple_query(SEED_SQL).await?;
    ctx.cache_settle().await?;
    register(&mut ctx, READ_ONE).await?;

    let before = ctx.metrics().await?;
    ctx.simple_query("SET extra_float_digits = 2").await?;
    let res = ctx.simple_query(READ_ONE).await?;
    assert_row_at(&res, 1, &[("v", "10")])?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert!(delta.conn_settings_probes >= 1, "expected a settings probe");
    assert_eq!(delta.queries_cache_hit, 1, "served once the value is known");
    Ok(())
}

/// A transaction that touches no setting needs no probe when it ends, and
/// the read after it is still a hit.
#[tokio::test]
async fn test_untouched_transaction_needs_no_probe() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(TABLE_SQL, &[]).await?;
    ctx.simple_query(SEED_SQL).await?;
    ctx.cache_settle().await?;
    register(&mut ctx, READ_ONE).await?;

    let before = ctx.metrics().await?;
    ctx.simple_query("BEGIN").await?;
    ctx.simple_query("SELECT 1").await?;
    ctx.simple_query("COMMIT").await?;
    let _ = ctx.simple_query(READ_ONE).await?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.conn_settings_probes, 0, "nothing to re-discover");
    assert_eq!(delta.queries_cache_hit, 1);
    Ok(())
}

/// `SET LOCAL search_path` inside a block keys the block's reads under the
/// block's schema; after COMMIT the session's schema is back, with no probe.
#[tokio::test]
async fn test_set_local_search_path_scopes_block_reads() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&[RAW_ON]).await?;
    ctx.origin_query(
        "CREATE TABLE public.config (id integer primary key, value text)",
        &[],
    )
    .await?;
    ctx.origin_query(
        "INSERT INTO public.config (id, value) VALUES (1, 'from_public')",
        &[],
    )
    .await?;
    config_table_create(&mut ctx, "blk", "from_blk").await?;
    ctx.cache_settle().await?;
    let sql = "SELECT id, value FROM config WHERE id = 1";
    register(&mut ctx, sql).await?;

    ctx.simple_query("BEGIN").await?;
    ctx.simple_query("SET LOCAL search_path TO blk").await?;
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(sql).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("value", "from_blk")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    let res = ctx.simple_query(sql).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("value", "from_blk")])?;
    let m = assert_cache_hit(&mut ctx, m).await?;
    ctx.simple_query("COMMIT").await?;

    let res = ctx.simple_query(sql).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("value", "from_public")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;
    Ok(())
}

/// A `SET search_path` with a cacheable read pipelined behind it: the read
/// resolves against the new path and the SET's own response is untouched
/// (no query rewriting on the client's connection).
#[tokio::test]
async fn test_pipelined_set_search_path_then_read() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    config_table_create(&mut ctx, "pipe_a", "from_a").await?;
    config_table_create(&mut ctx, "pipe_b", "from_b").await?;
    let sql = "SELECT value FROM config WHERE id = 1";

    let mut client = WireClient::connect(ctx.cache_port).await?;
    for (schema, expected) in [
        ("pipe_a", "from_a"),
        ("pipe_b", "from_b"),
        ("pipe_a", "from_a"),
    ] {
        client
            .query_send(&format!("SET search_path TO {schema}"))
            .await?;
        client.query_send(sql).await?;
        let set = client.response_read().await?;
        assert_eq!(set.command_tags(), ["SET"]);
        let read = client.response_read().await?;
        assert_eq!(read.command_tags(), ["SELECT 1"]);
        assert_eq!(read.first_value().as_deref(), Some(expected));
        ctx.cache_settle().await?;
    }
    Ok(())
}
