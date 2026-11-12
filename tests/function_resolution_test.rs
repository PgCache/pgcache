//! Function resolution at registration (PGC-578): a call is resolved on the
//! session's search_path; PostgreSQL's own functions are served and
//! evaluated on the cache PG, a user function only when the cache database
//! holds it with origin's body, and otherwise the read forwards cleanly
//! instead of erroring on every hit (PGC-474). A function redefined or
//! dropped on origin evicts the entries calling it.

use std::io::Error;

use tokio_postgres::Client;
use tokio_postgres::error::SqlState;

use crate::util::{
    MetricsSnapshot, TestContext, assert_cache_hit, assert_proxy_rows_match_origin,
    connect_cache_db, metrics_delta, query_warm, result_rows,
};

mod util;

const ITEMS: &[&str] = &[
    "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, n INTEGER)",
    "INSERT INTO items VALUES (1, 'Ann', 10), (2, 'Bob', 20), (3, NULL, 30)",
];

// plpgsql bodies: a sql body would be inlined (PGC-579) and never reach
// the presence check these tests are about.
const DOUBLE_IT: &str = "CREATE FUNCTION double_it(x INTEGER) RETURNS INTEGER \
     LANGUAGE plpgsql IMMUTABLE AS $$ begin return x * 2; end $$";
const TRIPLE_IT: &str = "CREATE OR REPLACE FUNCTION double_it(x INTEGER) RETURNS INTEGER \
     LANGUAGE plpgsql IMMUTABLE AS $$ begin return x * 3; end $$";
const DOUBLE_READ: &str = "SELECT id FROM items WHERE double_it(n) = 20 ORDER BY id";
const APP_DOUBLE_IT: &str = "CREATE FUNCTION app.double_it(x INTEGER) RETURNS INTEGER \
     LANGUAGE plpgsql IMMUTABLE AS $$ begin return x * 2; end $$";
const SHADOW_LOWER: &str = "CREATE FUNCTION app.lower(t TEXT) RETURNS TEXT \
     LANGUAGE sql IMMUTABLE AS $$ SELECT 'shadow' $$";

/// Origin with the items table and `statements` in place before pgcache
/// starts, so its function catalog knows them from the start.
async fn setup_with(statements: &'static [&'static str]) -> Result<TestContext, Error> {
    TestContext::setup_before_start(|origin| async move {
        for sql in ITEMS.iter().chain(statements) {
            origin.simple_query(sql).await.map_err(Error::other)?;
        }
        Ok(origin)
    })
    .await
}

async fn set_search_path(client: &Client, path: &str) -> Result<(), Error> {
    client
        .simple_query(&format!("SET search_path = {path}"))
        .await
        .map_err(Error::other)?;
    Ok(())
}

async fn rows(client: &Client, sql: &str) -> Result<Vec<Vec<Option<String>>>, Error> {
    Ok(result_rows(
        &client.simple_query(sql).await.map_err(Error::other)?,
    ))
}

/// Run `sql` on the cache database, where pgcache's own role installs
/// nothing: what an operator does to make a user function servable.
async fn cache_db_run(ctx: &TestContext, sql: &str) -> Result<(), Error> {
    connect_cache_db(&ctx.dbs)
        .await?
        .simple_query(sql)
        .await
        .map_err(Error::other)?;
    Ok(())
}

/// One read of `sql` through `client` forwards for the function `reason`,
/// with the rows `origin` returns and no error on the cache PG; a repeat
/// read still never hits.
async fn forward_assert(
    ctx: &mut TestContext,
    client: &Client,
    origin: &Client,
    sql: &str,
    reason: impl Fn(&MetricsSnapshot) -> u64,
) -> Result<(), Error> {
    let before = ctx.metrics().await?;
    let proxied = rows(client, sql).await?;
    let after = ctx.metrics_after_outcomes(&before, 1).await?;
    let delta = metrics_delta(&before, &after);
    assert_eq!(
        reason(&delta),
        1,
        "forwarded on the function: {sql}\n{delta:#?}"
    );
    assert_eq!(delta.queries_cache_hit, 0, "never served from cache: {sql}");
    assert_eq!(
        delta.queries_cache_error, 0,
        "no error on the cache PG: {sql}"
    );
    assert_eq!(proxied, rows(origin, sql).await?, "{sql}");
    ctx.cache_settle().await?;
    let before = ctx.metrics().await?;
    client.simple_query(sql).await.map_err(Error::other)?;
    let after = ctx.metrics_after_outcomes(&before, 1).await?;
    assert_eq!(
        metrics_delta(&before, &after).queries_cache_hit,
        0,
        "still forwarding: {sql}"
    );
    Ok(())
}

/// A user function the cache database lacks is refused at registration, in
/// the SELECT list as in WHERE, so the read forwards with origin's rows
/// instead of erroring on the cache PG.
#[tokio::test]
async fn test_user_function_absent_from_the_cache_database_forwards() -> Result<(), Error> {
    let mut ctx = setup_with(&[DOUBLE_IT]).await?;
    let client = ctx.proxy_client_connect().await?;
    let origin = ctx.origin_connect_as("postgres").await?;
    for sql in [
        "SELECT id, double_it(n) AS twice FROM items WHERE id = 1",
        DOUBLE_READ,
    ] {
        forward_assert(&mut ctx, &client, &origin, sql, |d| {
            d.queries_function_forward_not_on_cache
        })
        .await?;
    }
    Ok(())
}

/// A user function with no immutable overload and no body to inline is
/// refused before dispatch.
#[tokio::test]
async fn test_stable_user_function_in_where_is_uncacheable() -> Result<(), Error> {
    let mut ctx = setup_with(&["CREATE FUNCTION clock_n(x INTEGER) RETURNS INTEGER \
         LANGUAGE plpgsql STABLE AS $$ begin return x; end $$"])
    .await?;
    let sql = "SELECT id FROM items WHERE clock_n(n) = 20";
    let before = ctx.metrics().await?;
    assert_proxy_rows_match_origin(&mut ctx, sql).await?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.queries_uncacheable, 1, "refused at the proxy");
    assert_eq!(delta.queries_cache_error, 0);
    Ok(())
}

/// A function created after pgcache started reaches the catalog through
/// the function-change signal, so the read is judged by it rather than
/// forwarded as unknown.
#[tokio::test]
async fn test_function_created_after_start_is_known() -> Result<(), Error> {
    let mut ctx = setup_with(&[]).await?;
    ctx.origin_query(DOUBLE_IT, &[]).await?;
    ctx.cache_settle().await?;
    let client = ctx.proxy_client_connect().await?;
    let origin = ctx.origin_connect_as("postgres").await?;
    forward_assert(
        &mut ctx,
        &client,
        &origin,
        "SELECT id, double_it(n) AS twice FROM items WHERE id = 1",
        |d| d.queries_function_forward_not_on_cache,
    )
    .await
}

/// A user function the cache database holds with origin's body is cached;
/// a new body on origin evicts the entry and, until the cache database has
/// the same body, the read forwards with origin's rows.
#[tokio::test]
async fn test_function_on_both_databases_caches_until_redefined() -> Result<(), Error> {
    let mut ctx = setup_with(&[DOUBLE_IT]).await?;
    cache_db_run(&ctx, DOUBLE_IT).await?;
    query_warm(&mut ctx, DOUBLE_READ).await?;
    query_warm(
        &mut ctx,
        "SELECT id, double_it(n) AS twice FROM items ORDER BY id",
    )
    .await?;

    ctx.origin_query(TRIPLE_IT, &[]).await?;
    ctx.cache_settle().await?;
    let client = ctx.proxy_client_connect().await?;
    let origin = ctx.origin_connect_as("postgres").await?;
    assert!(
        rows(&origin, DOUBLE_READ).await?.is_empty(),
        "origin now triples"
    );
    forward_assert(&mut ctx, &client, &origin, DOUBLE_READ, |d| {
        d.queries_function_forward_not_on_cache
    })
    .await?;

    cache_db_run(&ctx, TRIPLE_IT).await?;
    query_warm(&mut ctx, DOUBLE_READ).await?;
    Ok(())
}

/// Dropping a function on origin evicts the entries calling it; the next
/// read forwards and gets origin's own error.
#[tokio::test]
async fn test_dropping_a_function_evicts_its_queries() -> Result<(), Error> {
    let mut ctx = setup_with(&[DOUBLE_IT]).await?;
    cache_db_run(&ctx, DOUBLE_IT).await?;
    query_warm(&mut ctx, DOUBLE_READ).await?;

    ctx.origin_query("DROP FUNCTION double_it(INTEGER)", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    let client = ctx.proxy_client_connect().await?;
    let before = ctx.metrics().await?;
    let error = client
        .simple_query(DOUBLE_READ)
        .await
        .expect_err("origin has no such function");
    assert_eq!(
        error.code(),
        Some(&SqlState::UNDEFINED_FUNCTION),
        "origin's own error: {error}"
    );
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.queries_cache_hit, 0, "never served the evicted entry");
    assert_eq!(delta.queries_cache_error, 0);
    Ok(())
}

/// A user overload of a builtin's name on the search_path may be the one
/// origin picks (its argument types decide), so that session forwards and
/// gets origin's rows; a session whose path has no such overload resolves
/// the builtin and is cached.
#[tokio::test]
async fn test_user_overload_on_the_search_path_forwards() -> Result<(), Error> {
    let mut ctx = setup_with(&["CREATE SCHEMA app", SHADOW_LOWER]).await?;
    let sql = "SELECT id, lower(name) AS low FROM items WHERE lower(name) = 'ann' ORDER BY id";
    query_warm(&mut ctx, sql).await?;

    let shadowed = ctx.proxy_client_connect().await?;
    let origin = ctx.origin_connect_as("postgres").await?;
    for client in [&shadowed, &origin] {
        set_search_path(client, "app, public").await?;
    }
    forward_assert(&mut ctx, &shadowed, &origin, sql, |d| {
        d.queries_function_forward_not_on_cache
    })
    .await
}

/// A call qualified with a user schema is judged by that schema's function
/// and cached once the cache database holds it.
#[tokio::test]
async fn test_schema_qualified_user_function_caches() -> Result<(), Error> {
    let mut ctx = setup_with(&["CREATE SCHEMA app", APP_DOUBLE_IT]).await?;
    cache_db_run(&ctx, "CREATE SCHEMA app").await?;
    cache_db_run(&ctx, APP_DOUBLE_IT).await?;
    query_warm(
        &mut ctx,
        "SELECT id FROM items WHERE app.double_it(n) = 20 ORDER BY id",
    )
    .await?;
    Ok(())
}

/// A SQL-standard body (`BEGIN ATOMIC`) leaves `prosrc` empty, so the
/// digest covers the body PostgreSQL stores parsed: a cache database holding
/// the same identity with another body does not pass as present.
#[tokio::test]
async fn test_standard_body_differing_on_the_cache_database_forwards() -> Result<(), Error> {
    let mut ctx = setup_with(&[
        "CREATE FUNCTION double_it(x INTEGER) RETURNS INTEGER IMMUTABLE \
         BEGIN ATOMIC SELECT x * 2; END",
    ])
    .await?;
    cache_db_run(
        &ctx,
        "CREATE FUNCTION double_it(x INTEGER) RETURNS INTEGER IMMUTABLE \
         BEGIN ATOMIC SELECT x * 3; END",
    )
    .await?;
    let client = ctx.proxy_client_connect().await?;
    let origin = ctx.origin_connect_as("postgres").await?;
    forward_assert(&mut ctx, &client, &origin, DOUBLE_READ, |d| {
        d.queries_function_forward_not_on_cache
    })
    .await
}

/// A function created after a call was cached has an oid no entry recorded.
/// The reload clears the session-key aliases instead: a session whose path
/// now reaches the new function resolves afresh and forwards, one whose path
/// does not keeps the entry (after the one read that re-asks the privilege
/// verdicts the DDL forgot, ADR-061).
#[tokio::test]
async fn test_function_created_on_a_path_makes_that_path_resolve_afresh() -> Result<(), Error> {
    let mut ctx = setup_with(&["CREATE SCHEMA app"]).await?;
    let sql = "SELECT id FROM items WHERE lower(name) = 'ann' ORDER BY id";
    query_warm(&mut ctx, sql).await?;
    let shadowed = ctx.proxy_client_connect().await?;
    let origin = ctx.origin_connect_as("postgres").await?;
    for client in [&shadowed, &origin] {
        set_search_path(client, "app, public").await?;
    }
    let before = ctx.metrics().await?;
    shadowed.simple_query(sql).await.map_err(Error::other)?;
    assert_cache_hit(&mut ctx, before).await?;

    ctx.origin_query(SHADOW_LOWER, &[]).await?;
    ctx.cache_settle().await?;
    forward_assert(&mut ctx, &shadowed, &origin, sql, |d| {
        d.queries_function_forward_not_on_cache
    })
    .await?;
    assert_proxy_rows_match_origin(&mut ctx, sql).await?;
    let before = ctx.metrics().await?;
    assert_proxy_rows_match_origin(&mut ctx, sql).await?;
    assert_cache_hit(&mut ctx, before).await?;
    Ok(())
}

/// A `pg_catalog`-qualified builtin in WHERE is cached like the bare name;
/// the parser's own forms, aggregates and window functions resolve too.
#[tokio::test]
async fn test_builtins_and_sql_forms_are_cached() -> Result<(), Error> {
    let mut ctx = setup_with(&[]).await?;
    for sql in [
        "SELECT id FROM items WHERE pg_catalog.lower(name) = 'bob' ORDER BY id",
        "SELECT id FROM items WHERE COALESCE(name, 'none') = 'none' ORDER BY id",
        "SELECT id FROM items WHERE NULLIF(n, 10) = 20 AND GREATEST(n, 5) = LEAST(n, 50)",
        "SELECT id, row_number() OVER (ORDER BY id) AS r FROM items WHERE n > 15 ORDER BY id",
        "SELECT count(*) AS c, string_agg(name, ',' ORDER BY id) AS names FROM items WHERE n > 0",
    ] {
        let before = ctx.metrics().await?;
        ctx.simple_query(sql).await?;
        let after = ctx.metrics_after_outcomes(&before, 1).await?;
        let delta = metrics_delta(&before, &after);
        assert_eq!(delta.queries_cache_miss, 1, "registers: {sql}\n{delta:#?}");
        ctx.cache_settle().await?;
        assert_proxy_rows_match_origin(&mut ctx, sql).await?;
        let before = ctx.metrics().await?;
        ctx.simple_query(sql).await?;
        let after = ctx.metrics_after_outcomes(&before, 1).await?;
        let delta = metrics_delta(&before, &after);
        assert_eq!(delta.queries_cache_hit, 1, "hits: {sql}\n{delta:#?}");
    }
    Ok(())
}

/// `ALTER EXTENSION ... SET SCHEMA` reports the extension, not its
/// functions. The catalog is reloaded all the same, and the entries calling
/// a function whose identity changed are evicted: the bare call no longer
/// resolves on origin, so the next read forwards and gets origin's error.
#[tokio::test]
async fn test_extension_moved_to_another_schema_evicts_its_callers() -> Result<(), Error> {
    let mut ctx = setup_with(&["CREATE EXTENSION pg_trgm", "CREATE SCHEMA app"]).await?;
    cache_db_run(&ctx, "CREATE EXTENSION pg_trgm").await?;
    let sql = "SELECT id FROM items WHERE similarity(name, 'Ann') > 0.5 ORDER BY id";
    query_warm(&mut ctx, sql).await?;

    ctx.origin_query("ALTER EXTENSION pg_trgm SET SCHEMA app", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    let client = ctx.proxy_client_connect().await?;
    let before = ctx.metrics().await?;
    let error = client
        .simple_query(sql)
        .await
        .expect_err("origin no longer resolves the bare name");
    assert_eq!(
        error.code(),
        Some(&SqlState::UNDEFINED_FUNCTION),
        "origin's own error: {error}"
    );
    let after = ctx.metrics_after_outcomes(&before, 1).await?;
    let delta = metrics_delta(&before, &after);
    assert_eq!(delta.queries_cache_hit, 0, "never served the evicted entry");
    assert_eq!(delta.queries_function_forward_unknown, 1, "{delta:#?}");
    assert_eq!(delta.queries_cache_error, 0);
    Ok(())
}
