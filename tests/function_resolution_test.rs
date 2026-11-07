//! Function resolution at registration (PGC-578): a call is resolved on the
//! session's search_path to PostgreSQL's own functions, which the cache PG
//! can serve and evaluate with; a user function, which it lacks, forwards
//! the read cleanly instead of erroring on every hit (PGC-474).

use std::io::Error;

use tokio_postgres::Client;

use crate::util::{
    MetricsSnapshot, TestContext, assert_proxy_rows_match_origin, metrics_delta, query_warm,
    result_rows,
};

mod util;

const ITEMS: &[&str] = &[
    "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, n INTEGER)",
    "INSERT INTO items VALUES (1, 'Ann', 10), (2, 'Bob', 20), (3, NULL, 30)",
];

const DOUBLE_IT: &str = "CREATE FUNCTION double_it(x INTEGER) RETURNS INTEGER \
     LANGUAGE sql IMMUTABLE AS $$ SELECT x * 2 $$";

/// Origin with the items table and `statements` in place before pgcache
/// starts, so its function catalog knows them.
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

/// A user function in the SELECT list is refused at registration, so the
/// read forwards with origin's rows instead of erroring on the cache PG.
#[tokio::test]
async fn test_user_function_in_select_list_forwards_cleanly() -> Result<(), Error> {
    let mut ctx = setup_with(&[DOUBLE_IT]).await?;
    let client = ctx.proxy_client_connect().await?;
    let origin = ctx.origin_connect_as("postgres").await?;
    forward_assert(
        &mut ctx,
        &client,
        &origin,
        "SELECT id, double_it(n) AS twice FROM items WHERE id = 1",
        |d| d.queries_function_forward_not_builtin,
    )
    .await
}

/// A function created after pgcache started forwards too: as unknown when
/// the writer's catalog was read before the CREATE, as a non-builtin when
/// after (the two race under load). The function-change signal will bring
/// the catalog up to date; until then either verdict forwards the read.
#[tokio::test]
async fn test_function_created_after_start_forwards() -> Result<(), Error> {
    let mut ctx = setup_with(&[]).await?;
    ctx.origin_query(DOUBLE_IT, &[]).await?;
    let client = ctx.proxy_client_connect().await?;
    let origin = ctx.origin_connect_as("postgres").await?;
    forward_assert(
        &mut ctx,
        &client,
        &origin,
        "SELECT id, double_it(n) AS twice FROM items WHERE id = 1",
        |d| d.queries_function_forward_unknown + d.queries_function_forward_not_builtin,
    )
    .await
}

/// A user function in WHERE is refused before dispatch: it is not a builtin
/// the cache PG could evaluate, immutable or not.
#[tokio::test]
async fn test_user_function_in_where_is_uncacheable() -> Result<(), Error> {
    let mut ctx = setup_with(&[DOUBLE_IT]).await?;
    let sql = "SELECT id FROM items WHERE double_it(n) = 20";
    let before = ctx.metrics().await?;
    assert_proxy_rows_match_origin(&mut ctx, sql).await?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.queries_uncacheable, 1, "refused at the proxy");
    assert_eq!(delta.queries_cache_error, 0);
    Ok(())
}

/// A user overload of a builtin's name on the search_path may be the one
/// origin picks (its argument types decide), so that session forwards and
/// gets origin's rows; a session whose path has no such overload resolves
/// the builtin and is cached.
#[tokio::test]
async fn test_user_overload_on_the_search_path_forwards() -> Result<(), Error> {
    let mut ctx = setup_with(&[
        "CREATE SCHEMA app",
        "CREATE FUNCTION app.lower(t TEXT) RETURNS TEXT \
         LANGUAGE sql IMMUTABLE AS $$ SELECT 'shadow' $$",
    ])
    .await?;
    let sql = "SELECT id, lower(name) AS low FROM items WHERE lower(name) = 'ann' ORDER BY id";
    query_warm(&mut ctx, sql).await?;

    let shadowed = ctx.proxy_client_connect().await?;
    let origin = ctx.origin_connect_as("postgres").await?;
    for client in [&shadowed, &origin] {
        set_search_path(client, "app, public").await?;
    }
    forward_assert(&mut ctx, &shadowed, &origin, sql, |d| {
        d.queries_function_forward_not_builtin
    })
    .await
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
