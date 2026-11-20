//! `excluded_tables`: a query that references an excluded table is forwarded
//! and the table never gets a cache table, whichever way the query reaches
//! it. The policy route (a policy's own tables) is in `rls_test`.

use std::io::Error;
use std::time::Duration;

use crate::util::{
    TestContext, assert_cache_hit, assert_cache_miss, assert_row_at, connect_cache_db, http_get,
    http_put, metrics_delta, query_warm,
};

mod util;

/// One read of `sql` forwards as excluded: no hit, no allowlist skip.
async fn excluded_forward_assert(ctx: &mut TestContext, sql: &str) -> Result<(), Error> {
    let before = ctx.metrics().await?;
    ctx.simple_query(sql).await?;
    let after = ctx.metrics_after_outcomes(&before, 1).await?;
    let delta = metrics_delta(&before, &after);
    assert_eq!(delta.queries_excluded_skipped, 1, "excluded skip: {sql}");
    assert_eq!(delta.queries_allowlist_skipped, 0, "allowlist skip: {sql}");
    assert_eq!(delta.queries_cache_hit, 0, "served from cache: {sql}");
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

/// Whether pgcache's publication on origin carries a table of this name.
async fn publication_has_table(ctx: &mut TestContext, name: &str) -> Result<bool, Error> {
    let rows = ctx
        .origin_query(
            "SELECT 1 FROM pg_publication_tables WHERE tablename = $1",
            &[&name],
        )
        .await?;
    Ok(!rows.is_empty())
}

/// An excluded table is forwarded on every read and gets no cache table;
/// the other tables are cached as usual.
#[tokio::test]
async fn test_excluded_table_is_forwarded() -> Result<(), Error> {
    let mut ctx = TestContext::setup_with_args(&["--excluded_tables", "audit_logs"]).await?;
    for sql in [
        "CREATE TABLE users (id int primary key, name text)",
        "CREATE TABLE audit_logs (id int primary key, entry text)",
        "INSERT INTO users VALUES (1, 'ann')",
        "INSERT INTO audit_logs VALUES (1, 'login')",
    ] {
        ctx.query(sql, &[]).await?;
    }

    query_warm(&mut ctx, "SELECT * FROM users WHERE id = 1").await?;

    let res = ctx
        .simple_query("SELECT * FROM audit_logs WHERE id = 1")
        .await?;
    assert_row_at(&res, 1, &[("id", "1"), ("entry", "login")])?;
    excluded_forward_assert(&mut ctx, "SELECT * FROM audit_logs WHERE id = 1").await?;
    excluded_forward_assert(&mut ctx, "SELECT * FROM public.audit_logs WHERE id = 1").await?;
    excluded_forward_assert(&mut ctx, "SELECT * FROM AUDIT_LOGS WHERE id = 1").await?;

    assert!(cache_table_exists(&ctx, "users").await?);
    assert!(!cache_table_exists(&ctx, "audit_logs").await?);
    assert!(!publication_has_table(&mut ctx, "audit_logs").await?);
    Ok(())
}

/// One excluded table forwards the whole query: a join, a subquery, a CTE.
#[tokio::test]
async fn test_excluded_table_forwards_queries_that_reach_it() -> Result<(), Error> {
    let mut ctx = TestContext::setup_with_args(&["--excluded_tables", "audit_logs"]).await?;
    for sql in [
        "CREATE TABLE users (id int primary key, name text)",
        "CREATE TABLE audit_logs (id int primary key, user_id int, entry text)",
        "INSERT INTO users VALUES (1, 'ann')",
        "INSERT INTO audit_logs VALUES (1, 1, 'login')",
    ] {
        ctx.query(sql, &[]).await?;
    }

    for sql in [
        "SELECT u.name, a.entry FROM users u JOIN audit_logs a ON a.user_id = u.id",
        "SELECT * FROM users WHERE id IN (SELECT user_id FROM audit_logs)",
        "WITH recent AS (SELECT user_id FROM audit_logs) \
         SELECT u.name FROM users u JOIN recent r ON r.user_id = u.id",
    ] {
        excluded_forward_assert(&mut ctx, sql).await?;
    }
    assert!(!cache_table_exists(&ctx, "audit_logs").await?);
    Ok(())
}

/// A schema-qualified entry excludes that schema's table and leaves the
/// same name in another schema cacheable.
#[tokio::test]
async fn test_excluded_schema_qualified_entry() -> Result<(), Error> {
    let mut ctx = TestContext::setup_with_args(&["--excluded_tables", "analytics.events"]).await?;
    for sql in [
        "CREATE SCHEMA analytics",
        "CREATE TABLE analytics.events (id int primary key, kind text)",
        "CREATE TABLE public.events (id int primary key, kind text)",
        "INSERT INTO analytics.events VALUES (1, 'click')",
        "INSERT INTO public.events VALUES (1, 'signup')",
    ] {
        ctx.query(sql, &[]).await?;
    }

    excluded_forward_assert(&mut ctx, "SELECT * FROM analytics.events WHERE id = 1").await?;
    query_warm(&mut ctx, "SELECT * FROM public.events WHERE id = 1").await?;
    Ok(())
}

/// A bare name that the session's `search_path` resolves to the excluded
/// schema is forwarded: the entry names a schema, the query does not.
#[tokio::test]
async fn test_excluded_schema_qualified_entry_reached_through_search_path() -> Result<(), Error> {
    let mut ctx = TestContext::setup_with_args(&["--excluded_tables", "analytics.events"]).await?;
    for sql in [
        "CREATE SCHEMA analytics",
        "CREATE TABLE analytics.events (id int primary key, kind text)",
        "INSERT INTO analytics.events VALUES (1, 'click')",
    ] {
        ctx.query(sql, &[]).await?;
    }
    ctx.simple_query("SET search_path = analytics, public")
        .await?;

    let res = ctx
        .simple_query("SELECT * FROM events WHERE id = 1")
        .await?;
    assert_row_at(&res, 1, &[("id", "1"), ("kind", "click")])?;
    excluded_forward_assert(&mut ctx, "SELECT * FROM events WHERE id = 1").await?;
    assert!(!cache_table_exists(&ctx, "events").await?);
    Ok(())
}

/// The cost of the rule above: a bare name is forwarded even where it
/// resolves to another schema's table of that name.
#[tokio::test]
async fn test_excluded_schema_qualified_entry_forwards_a_bare_name_elsewhere() -> Result<(), Error>
{
    let mut ctx = TestContext::setup_with_args(&["--excluded_tables", "analytics.events"]).await?;
    for sql in [
        "CREATE TABLE public.events (id int primary key, kind text)",
        "INSERT INTO public.events VALUES (1, 'signup')",
    ] {
        ctx.query(sql, &[]).await?;
    }

    excluded_forward_assert(&mut ctx, "SELECT * FROM events WHERE id = 1").await?;
    query_warm(&mut ctx, "SELECT * FROM public.events WHERE id = 1").await?;
    Ok(())
}

/// With both lists set a table must be allowed and not excluded, and an
/// excluded table counts as excluded whatever the allowlist says.
#[tokio::test]
async fn test_excluded_wins_over_allowed() -> Result<(), Error> {
    let mut ctx = TestContext::setup_with_args(&[
        "--allowed_tables",
        "table_a,table_b",
        "--excluded_tables",
        "table_b",
    ])
    .await?;
    for table in ["table_a", "table_b", "table_c"] {
        ctx.query(
            &format!("CREATE TABLE {table} (id int primary key, data text)"),
            &[],
        )
        .await?;
        ctx.query(&format!("INSERT INTO {table} VALUES (1, 'x')"), &[])
            .await?;
    }

    query_warm(&mut ctx, "SELECT * FROM table_a WHERE id = 1").await?;
    excluded_forward_assert(&mut ctx, "SELECT * FROM table_b WHERE id = 1").await?;
    excluded_forward_assert(
        &mut ctx,
        "SELECT a.data FROM table_a a JOIN table_b b ON b.id = a.id",
    )
    .await?;

    let before = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM table_c WHERE id = 1")
        .await?;
    let after = ctx.metrics_after_outcomes(&before, 1).await?;
    let delta = metrics_delta(&before, &after);
    assert_eq!(delta.queries_allowlist_skipped, 1);
    assert_eq!(delta.queries_excluded_skipped, 0);
    Ok(())
}

/// A pinned query on an excluded table is skipped at startup: nothing is
/// populated and reads of it forward.
#[tokio::test]
async fn test_excluded_pinned_table_is_not_cached() -> Result<(), Error> {
    let pinned = "SELECT id, name FROM pin_excluded";
    let mut ctx = TestContext::setup_before_start_with_args(
        |origin| async move {
            for sql in [
                "CREATE TABLE pin_excluded (id int primary key, name text)",
                "INSERT INTO pin_excluded VALUES (1, 'ann')",
            ] {
                origin.execute(sql, &[]).await.map_err(Error::other)?;
            }
            Ok(origin)
        },
        &[
            "--pinned_queries",
            pinned,
            "--excluded_tables",
            "pin_excluded",
        ],
    )
    .await?;
    ctx.cache_settle().await?;

    assert!(!cache_table_exists(&ctx, "pin_excluded").await?);
    assert!(!publication_has_table(&mut ctx, "pin_excluded").await?);
    let res = ctx.simple_query(pinned).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("name", "ann")])?;
    excluded_forward_assert(&mut ctx, pinned).await?;
    Ok(())
}

/// Excluding a table at runtime forwards its reads at once and evicts the
/// queries cached on it, so the table leaves the publication; lifting the
/// exclusion caches it again from origin's current rows.
#[tokio::test]
async fn test_excluded_at_runtime_evicts_cached_queries() -> Result<(), Error> {
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
    assert!(publication_has_table(&mut ctx, "sessions").await?);

    let (status, body) = http_put(
        ctx.metrics_port,
        "/config",
        r#"{"excluded_tables": ["sessions"]}"#,
    )
    .await?;
    assert_eq!(status, 200, "body: {body}");
    let (_, body) = http_get(ctx.metrics_port, "/config").await?;
    let json: serde_json::Value = serde_json::from_str(&body).map_err(Error::other)?;
    assert_eq!(
        json["dynamic"]["excluded_tables"],
        serde_json::json!(["sessions"])
    );

    excluded_forward_assert(&mut ctx, sessions_read).await?;
    // The writer evicts the entry on its next tick; the other table's stays.
    ctx.queries_registered_wait(1, timeout).await?;
    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM users WHERE id = 1").await?;
    assert_cache_hit(&mut ctx, m).await?;

    // A write the cache no longer follows.
    ctx.query("UPDATE sessions SET state = 'closed' WHERE id = 1", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    assert!(!publication_has_table(&mut ctx, "sessions").await?);

    let (status, body) =
        http_put(ctx.metrics_port, "/config", r#"{"excluded_tables": null}"#).await?;
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

/// The number of queries the writer reports registered.
async fn queries_registered(ctx: &TestContext) -> Result<u64, Error> {
    let (status, body) = http_get(ctx.metrics_port, "/status").await?;
    if status != 200 {
        return Err(Error::other(format!("/status returned {status}: {body}")));
    }
    let json: serde_json::Value = serde_json::from_str(&body).map_err(Error::other)?;
    json.get("cache")
        .and_then(|cache| cache.get("queries_registered"))
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| Error::other("cache.queries_registered missing"))
}

/// A table with more cached queries than the writer evicts in one tick is
/// cleared over several, and only then leaves the publication.
#[tokio::test]
async fn test_excluded_at_runtime_evicts_more_queries_than_one_tick_takes() -> Result<(), Error> {
    // Above the writer's per-tick eviction bound (512).
    const QUERIES: u64 = 600;
    let mut ctx = TestContext::setup().await?;
    let timeout = Duration::from_secs(30);
    ctx.query("CREATE TABLE jobs (id int primary key, state text)", &[])
        .await?;
    ctx.query(
        "INSERT INTO jobs SELECT n, 'queued' FROM generate_series(1, 600) n",
        &[],
    )
    .await?;

    let deadline = std::time::Instant::now() + timeout;
    let mut passes = 0;
    while queries_registered(&ctx).await? < QUERIES {
        // A registration the rate cap forwarded is asked again on the next pass.
        for id in 1..=QUERIES {
            ctx.simple_query(&format!("SELECT * FROM jobs WHERE id = {id}"))
                .await?;
        }
        passes += 1;
        if std::time::Instant::now() >= deadline {
            return Err(Error::other(format!(
                "{} of {QUERIES} queries registered after {passes} passes",
                queries_registered(&ctx).await?
            )));
        }
    }
    ctx.cache_settle_with_timeout(timeout).await?;
    assert!(publication_has_table(&mut ctx, "jobs").await?);

    let (status, body) = http_put(
        ctx.metrics_port,
        "/config",
        r#"{"excluded_tables": ["jobs"]}"#,
    )
    .await?;
    assert_eq!(status, 200, "body: {body}");

    ctx.queries_registered_wait(0, timeout).await?;
    excluded_forward_assert(&mut ctx, "SELECT * FROM jobs WHERE id = 1").await?;
    ctx.query("UPDATE jobs SET state = 'done' WHERE id = 1", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    assert!(!publication_has_table(&mut ctx, "jobs").await?);
    assert!(!cache_table_exists(&ctx, "jobs").await?);
    Ok(())
}
