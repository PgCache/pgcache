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

/// A setting that changes how origin parses statements forwards the session
/// while it is set (`standard_conforming_strings` off: pg_query parses as if
/// on), and caching resumes once it is reset.
#[tokio::test]
async fn test_parsing_incompatible_setting_forwards_until_reset() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(TABLE_SQL, &[]).await?;
    ctx.simple_query(SEED_SQL).await?;
    ctx.cache_settle().await?;
    register(&mut ctx, READ_ONE).await?;

    ctx.simple_query("SET standard_conforming_strings = off")
        .await?;
    let before = ctx.metrics().await?;
    let res = ctx.simple_query(READ_ONE).await?;
    assert_row_at(&res, 1, &[("v", "10")])?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(
        delta.queries_uncacheable, 1,
        "forwarded while parsing differs"
    );
    assert_eq!(delta.queries_cache_hit, 0);

    ctx.simple_query("RESET standard_conforming_strings")
        .await?;
    let before = ctx.metrics().await?;
    let _ = ctx.simple_query(READ_ONE).await?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.queries_cache_hit, 1, "served again once reset");
    Ok(())
}

/// A block-scoped `SET` inside a failed block leaves a block value unknown,
/// but origin rejects every statement there, the probe included: nothing is
/// probed until the block ends.
#[tokio::test]
async fn test_failed_block_is_not_probed() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(TABLE_SQL, &[]).await?;
    ctx.simple_query(SEED_SQL).await?;
    ctx.cache_settle().await?;
    register(&mut ctx, READ_ONE).await?;

    ctx.simple_query("BEGIN").await?;
    assert!(ctx.simple_query("SELECT 1/0").await.is_err());
    // Rejected by origin (failed block), but forwarded and so tracked.
    assert!(
        ctx.simple_query("SET LOCAL bytea_output = 'escape'")
            .await
            .is_err()
    );
    let before = ctx.metrics().await?;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.conn_settings_probes, 0, "no probe can succeed here");

    ctx.simple_query("ROLLBACK").await?;
    let before = ctx.metrics().await?;
    let res = ctx.simple_query(READ_ONE).await?;
    assert_row_at(&res, 1, &[("v", "10")])?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.queries_cache_hit, 1, "served once the block is gone");
    Ok(())
}

/// A probe injected before a pipelined `BEGIN; SET LOCAL …` answers with an
/// idle sample that predates the block: it must not erase the block mark, so
/// the read inside the block waits for an in-block sample and renders as
/// origin would.
#[tokio::test]
async fn test_idle_sample_does_not_erase_a_later_set_local() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(
        "CREATE TABLE bytes_items (id int primary key, v bytea)",
        &[],
    )
    .await?;
    ctx.simple_query("INSERT INTO bytes_items VALUES (1, '\\x01')")
        .await?;
    ctx.cache_settle().await?;
    let sql = "SELECT v FROM bytes_items WHERE id = 1";
    register(&mut ctx, sql).await?;

    let mut client = WireClient::connect(ctx.cache_port).await?;
    // The SET's probe queues on origin behind the sleep, so it is still in
    // flight when the block below is forwarded.
    client.query_send("SET bytea_output = 'hex'").await?;
    client.query_send("SELECT pg_sleep(0.3)").await?;
    assert_eq!(client.response_read().await?.command_tags(), ["SET"]);
    client.query_send("BEGIN").await?;
    client
        .query_send("SET LOCAL bytea_output = 'escape'")
        .await?;
    client.query_send(sql).await?;
    let before = ctx.metrics().await?;
    assert_eq!(client.response_read().await?.command_tags(), ["SELECT 1"]);
    assert_eq!(client.response_read().await?.command_tags(), ["BEGIN"]);
    assert_eq!(client.response_read().await?.command_tags(), ["SET"]);
    let read = client.response_read().await?;
    assert_eq!(read.command_tags(), ["SELECT 1"]);
    assert_eq!(
        read.first_value().as_deref(),
        Some("\\001"),
        "rendered under the block's escape setting"
    );
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.queries_cache_hit, 1, "served from cache, not origin");
    client.query("COMMIT").await?;
    Ok(())
}

/// `$user` on the search_path follows a `SET LOCAL ROLE` inside the block.
#[tokio::test]
async fn test_set_local_role_resolves_user_schema() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    for sql in [
        "CREATE ROLE role_bob",
        "CREATE SCHEMA postgres",
        "CREATE SCHEMA role_bob",
        "CREATE TABLE postgres.t (id int primary key, value text)",
        "CREATE TABLE role_bob.t (id int primary key, value text)",
        "INSERT INTO postgres.t VALUES (1, 'from_postgres')",
        "INSERT INTO role_bob.t VALUES (1, 'from_bob')",
        "GRANT USAGE ON SCHEMA role_bob TO role_bob",
        "GRANT SELECT ON role_bob.t TO role_bob",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }
    ctx.cache_settle().await?;
    ctx.simple_query(r#"SET search_path TO "$user", public"#)
        .await?;
    let sql = "SELECT value FROM t WHERE id = 1";
    register(&mut ctx, sql).await?;

    ctx.simple_query("BEGIN").await?;
    ctx.simple_query("SET LOCAL ROLE role_bob").await?;
    let before = ctx.metrics().await?;
    let res = ctx.simple_query(sql).await?;
    assert_row_at(&res, 1, &[("value", "from_bob")])?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(
        delta.queries_cache_hit, 0,
        "the postgres entry must not serve bob"
    );
    ctx.simple_query("COMMIT").await?;

    let before = ctx.metrics().await?;
    let res = ctx.simple_query(sql).await?;
    assert_row_at(&res, 1, &[("value", "from_postgres")])?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.queries_cache_hit, 1);
    Ok(())
}
