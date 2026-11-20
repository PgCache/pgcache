use std::io::Error;
use std::time::Duration;

use crate::util::{
    TestContext, assert_cache_hit, assert_cache_miss, assert_row_at, connect_cache_db, http_put,
    metrics_delta, query_warm,
};

mod util;

/// Allowlisted table queries are cached normally.
#[tokio::test]
async fn test_allowlist_allows_configured_table() -> Result<(), Error> {
    let mut ctx = TestContext::setup_allowlist("allowed_table").await?;

    ctx.query(
        "CREATE TABLE allowed_table (id int primary key, data text)",
        &[],
    )
    .await?;
    ctx.query("INSERT INTO allowed_table VALUES (1, 'hello')", &[])
        .await?;

    // First query — cache miss (query admitted to cache)
    let m = ctx.metrics().await?;
    let res = ctx
        .simple_query("SELECT * FROM allowed_table WHERE id = 1")
        .await?;
    assert_row_at(&res, 1, &[("id", "1"), ("data", "hello")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Second query — cache hit
    let res = ctx
        .simple_query("SELECT * FROM allowed_table WHERE id = 1")
        .await?;
    assert_row_at(&res, 1, &[("id", "1"), ("data", "hello")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Non-allowlisted table queries are forwarded and the allowlist_skipped metric increments.
#[tokio::test]
async fn test_allowlist_rejects_non_configured_table() -> Result<(), Error> {
    let mut ctx = TestContext::setup_allowlist("allowed_table").await?;

    ctx.query(
        "CREATE TABLE allowed_table (id int primary key, data text)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE TABLE blocked_table (id int primary key, data text)",
        &[],
    )
    .await?;
    ctx.query("INSERT INTO blocked_table VALUES (1, 'secret')", &[])
        .await?;

    // Query against non-allowlisted table — forwarded, never cached
    let m = ctx.metrics().await?;
    let res = ctx
        .simple_query("SELECT * FROM blocked_table WHERE id = 1")
        .await?;
    assert_row_at(&res, 1, &[("id", "1"), ("data", "secret")])?;
    let after = ctx.metrics().await?;
    let delta = metrics_delta(&m, &after);
    assert_eq!(
        delta.queries_allowlist_skipped, 1,
        "expected allowlist skip"
    );
    assert_eq!(delta.queries_cache_hit, 0, "unexpected cache hit");

    ctx.cache_settle().await?;

    // Repeat — still forwarded, never becomes a cache hit
    let m2 = ctx.metrics().await?;
    let res = ctx
        .simple_query("SELECT * FROM blocked_table WHERE id = 1")
        .await?;
    assert_row_at(&res, 1, &[("id", "1"), ("data", "secret")])?;
    let after2 = ctx.metrics().await?;
    let delta2 = metrics_delta(&m2, &after2);
    assert_eq!(
        delta2.queries_allowlist_skipped, 1,
        "expected allowlist skip on repeat"
    );
    assert_eq!(
        delta2.queries_cache_hit, 0,
        "unexpected cache hit on repeat"
    );

    Ok(())
}

/// A join between a allowlisted and non-allowlisted table is forwarded.
#[tokio::test]
async fn test_allowlist_rejects_mixed_join() -> Result<(), Error> {
    let mut ctx = TestContext::setup_allowlist("allowed_table").await?;

    ctx.query(
        "CREATE TABLE allowed_table (id int primary key, data text)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE TABLE blocked_table (id int primary key, ref_id int)",
        &[],
    )
    .await?;
    ctx.query("INSERT INTO allowed_table VALUES (1, 'hello')", &[])
        .await?;
    ctx.query("INSERT INTO blocked_table VALUES (1, 1)", &[])
        .await?;

    let m = ctx.metrics().await?;
    let res = ctx
        .simple_query("SELECT a.data FROM allowed_table a JOIN blocked_table b ON a.id = b.ref_id")
        .await?;
    assert_row_at(&res, 1, &[("data", "hello")])?;
    let after = ctx.metrics().await?;
    let delta = metrics_delta(&m, &after);
    assert_eq!(
        delta.queries_allowlist_skipped, 1,
        "expected allowlist skip for mixed join"
    );

    Ok(())
}

/// Multiple tables can be allowlisted via comma-separated config.
#[tokio::test]
async fn test_allowlist_multiple_tables() -> Result<(), Error> {
    let mut ctx = TestContext::setup_allowlist("table_a,table_b").await?;

    ctx.query("CREATE TABLE table_a (id int primary key, data text)", &[])
        .await?;
    ctx.query("CREATE TABLE table_b (id int primary key, ref_id int)", &[])
        .await?;
    ctx.query("CREATE TABLE table_c (id int primary key)", &[])
        .await?;
    ctx.query("INSERT INTO table_a VALUES (1, 'a')", &[])
        .await?;
    ctx.query("INSERT INTO table_b VALUES (1, 1)", &[]).await?;
    ctx.query("INSERT INTO table_c VALUES (1)", &[]).await?;

    // Join of two allowlisted tables — cacheable
    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT a.data FROM table_a a JOIN table_b b ON a.id = b.ref_id")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    ctx.simple_query("SELECT a.data FROM table_a a JOIN table_b b ON a.id = b.ref_id")
        .await?;
    let m = assert_cache_hit(&mut ctx, m).await?;

    // Query against non-allowlisted table_c — forwarded
    ctx.simple_query("SELECT * FROM table_c WHERE id = 1")
        .await?;
    let after = ctx.metrics().await?;
    let delta = metrics_delta(&m, &after);
    assert_eq!(
        delta.queries_allowlist_skipped, 1,
        "expected allowlist skip for table_c"
    );

    Ok(())
}

/// Schema-qualified allowlist entry only matches the specified schema.
#[tokio::test]
async fn test_allowlist_schema_qualified() -> Result<(), Error> {
    let mut ctx = TestContext::setup_allowlist("public.target_table").await?;

    ctx.query(
        "CREATE TABLE public.target_table (id int primary key, data text)",
        &[],
    )
    .await?;
    ctx.query("INSERT INTO public.target_table VALUES (1, 'yes')", &[])
        .await?;

    // Explicit public.target_table — matches allowlist
    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM public.target_table WHERE id = 1")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    ctx.simple_query("SELECT * FROM public.target_table WHERE id = 1")
        .await?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Unqualified allowlist entry matches the table regardless of schema in the query.
#[tokio::test]
async fn test_allowlist_unqualified_matches_any_schema() -> Result<(), Error> {
    let mut ctx = TestContext::setup_allowlist("my_table").await?;

    ctx.query("CREATE TABLE my_table (id int primary key, data text)", &[])
        .await?;
    ctx.query("INSERT INTO my_table VALUES (1, 'val')", &[])
        .await?;

    // Query with explicit public schema — unqualified allowlist should match
    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM public.my_table WHERE id = 1")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    ctx.simple_query("SELECT * FROM public.my_table WHERE id = 1")
        .await?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Subquery referencing a non-allowlisted table causes the whole query to be forwarded.
#[tokio::test]
async fn test_allowlist_rejects_subquery_with_non_allowlisted_table() -> Result<(), Error> {
    let mut ctx = TestContext::setup_allowlist("allowed_table").await?;

    ctx.query(
        "CREATE TABLE allowed_table (id int primary key, data text)",
        &[],
    )
    .await?;
    ctx.query("CREATE TABLE blocked_table (id int primary key)", &[])
        .await?;
    ctx.query("INSERT INTO allowed_table VALUES (1, 'hello')", &[])
        .await?;
    ctx.query("INSERT INTO blocked_table VALUES (1)", &[])
        .await?;

    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM allowed_table WHERE id IN (SELECT id FROM blocked_table)")
        .await?;
    let after = ctx.metrics().await?;
    let delta = metrics_delta(&m, &after);
    assert_eq!(
        delta.queries_allowlist_skipped, 1,
        "expected allowlist skip for subquery with non-allowlisted table"
    );

    Ok(())
}

/// No allowlist configured (default) — all tables are cacheable.
#[tokio::test]
async fn test_no_allowlist_allows_all_tables() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE any_table (id int primary key, data text)",
        &[],
    )
    .await?;
    ctx.query("INSERT INTO any_table VALUES (1, 'anything')", &[])
        .await?;

    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM any_table WHERE id = 1")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    ctx.simple_query("SELECT * FROM any_table WHERE id = 1")
        .await?;
    let m = assert_cache_hit(&mut ctx, m).await?;

    // Confirm no allowlist skips occurred
    let after = ctx.metrics().await?;
    assert_eq!(
        after.queries_allowlist_skipped, 0,
        "no allowlist skips expected when allowlist is disabled"
    );
    let _ = m;

    Ok(())
}

/// Whether the cache database holds a table of this name.
async fn cache_table_exists(ctx: &TestContext, name: &str) -> Result<bool, Error> {
    let cache_db = connect_cache_db(&ctx.dbs).await?;
    let row = cache_db
        .query_one(
            "SELECT count(*) FROM pg_tables WHERE tablename = $1",
            &[&name],
        )
        .await
        .map_err(Error::other)?;
    let count: i64 = row.try_get(0).map_err(Error::other)?;
    Ok(count > 0)
}

/// A pinned query on a table outside the allowlist is skipped at startup:
/// it would be populated and maintained without ever being served.
#[tokio::test]
async fn test_allowlist_pinned_table_outside_it_is_not_cached() -> Result<(), Error> {
    let pinned = "SELECT id, name FROM pin_other";
    let mut ctx = TestContext::setup_before_start_with_args(
        |origin| async move {
            for sql in [
                "CREATE TABLE pin_other (id int primary key, name text)",
                "INSERT INTO pin_other VALUES (1, 'ann')",
            ] {
                origin.execute(sql, &[]).await.map_err(Error::other)?;
            }
            Ok(origin)
        },
        &[
            "--pinned_queries",
            pinned,
            "--allowed_tables",
            "allowed_table",
        ],
    )
    .await?;
    ctx.cache_settle().await?;

    assert!(!cache_table_exists(&ctx, "pin_other").await?);
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(pinned).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("name", "ann")])?;
    let after = ctx.metrics_after_outcomes(&m, 1).await?;
    let delta = metrics_delta(&m, &after);
    assert_eq!(delta.queries_allowlist_skipped, 1);
    assert_eq!(delta.queries_cache_hit, 0);
    Ok(())
}

/// Narrowing the allowlist at runtime forwards the reads of a table that
/// left it at once and evicts the queries cached on it; widening it again
/// caches the table from origin's current rows.
#[tokio::test]
async fn test_allowlist_narrowed_at_runtime_evicts_cached_queries() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    let timeout = Duration::from_secs(10);
    for sql in [
        "CREATE TABLE sessions (id int primary key, state text)",
        "CREATE TABLE users (id int primary key, name text)",
        "INSERT INTO sessions VALUES (1, 'open')",
        "INSERT INTO users VALUES (1, 'ann')",
    ] {
        ctx.query(sql, &[]).await?;
    }
    let sessions_read = "SELECT * FROM sessions WHERE id = 1";
    query_warm(&mut ctx, sessions_read).await?;
    query_warm(&mut ctx, "SELECT * FROM users WHERE id = 1").await?;

    let (status, body) = http_put(
        ctx.metrics_port,
        "/config",
        r#"{"allowed_tables": ["users"]}"#,
    )
    .await?;
    assert_eq!(status, 200, "body: {body}");

    let m = ctx.metrics().await?;
    ctx.simple_query(sessions_read).await?;
    let after = ctx.metrics_after_outcomes(&m, 1).await?;
    let delta = metrics_delta(&m, &after);
    assert_eq!(delta.queries_allowlist_skipped, 1);
    assert_eq!(delta.queries_cache_hit, 0);
    // The writer evicts the entry on its next tick; the allowed table's stays.
    ctx.queries_registered_wait(1, timeout).await?;
    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM users WHERE id = 1").await?;
    assert_cache_hit(&mut ctx, m).await?;

    // A write the cache no longer follows.
    ctx.query("UPDATE sessions SET state = 'closed' WHERE id = 1", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    let (status, body) =
        http_put(ctx.metrics_port, "/config", r#"{"allowed_tables": null}"#).await?;
    assert_eq!(status, 200, "body: {body}");

    let m = ctx.metrics().await?;
    let res = ctx.simple_query(sessions_read).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("state", "closed")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    let res = ctx.simple_query(sessions_read).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("state", "closed")])?;
    assert_cache_hit(&mut ctx, m).await?;
    Ok(())
}
