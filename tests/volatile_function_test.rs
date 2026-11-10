//! Function volatility at admission (PGC-589): a volatile call forwards
//! wherever it stands, since its result may change per call and its side
//! effects belong on origin, in the session that asked; a stable builtin on
//! the allowlist is cached in the SELECT list and recomputed on every hit,
//! never frozen by a materialized result or a memoized response; a stable
//! call outside the allowlist forwards.

use std::io::Error;
use std::time::Duration;

use tokio_postgres::Client;

use crate::util::{
    TestContext, assert_cache_hit, assert_cache_miss, assert_proxy_rows_match_origin,
    connect_cache_db, metrics_delta, query_warm, result_rows,
};

mod util;

const ITEMS: &[&str] = &[
    "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, n INTEGER)",
    "INSERT INTO items VALUES (1, 'Ann', 10), (2, 'Bob', 20), (3, NULL, 30)",
];

async fn setup() -> Result<TestContext, Error> {
    TestContext::setup_before_start(|origin| async move {
        for sql in ITEMS {
            origin.simple_query(sql).await.map_err(Error::other)?;
        }
        Ok(origin)
    })
    .await
}

async fn rows(client: &Client, sql: &str) -> Result<Vec<Vec<Option<String>>>, Error> {
    Ok(result_rows(
        &client.simple_query(sql).await.map_err(Error::other)?,
    ))
}

/// One read of `sql` through `client` is refused at the proxy as uncacheable
/// and never served from cache; returns its rows.
async fn uncacheable_read(
    ctx: &mut TestContext,
    client: &Client,
    sql: &str,
) -> Result<Vec<Vec<Option<String>>>, Error> {
    let before = ctx.metrics().await?;
    let served = rows(client, sql).await?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.queries_uncacheable, 1, "refused at the proxy: {sql}");
    assert_eq!(delta.queries_cache_hit, 0, "never served from cache: {sql}");
    assert_eq!(delta.queries_cache_error, 0, "{sql}");
    Ok(served)
}

/// A volatile call in the SELECT list forwards every time, so its value
/// changes per read as it does on origin.
#[tokio::test]
async fn test_volatile_call_in_select_list_forwards_every_read() -> Result<(), Error> {
    let mut ctx = setup().await?;
    let client = ctx.proxy_client_connect().await?;
    let sql = "SELECT random() AS r, id FROM items WHERE id = 1";
    let first = uncacheable_read(&mut ctx, &client, sql).await?;
    let second = uncacheable_read(&mut ctx, &client, sql).await?;
    assert_ne!(first[0][0], second[0][0], "random() answered afresh");
    Ok(())
}

/// `pg_try_advisory_lock` runs on origin, where the lock lives: while
/// another session holds it, the proxied read says so, every time.
#[tokio::test]
async fn test_advisory_lock_in_select_list_is_origins_answer() -> Result<(), Error> {
    let mut ctx = setup().await?;
    let holder = ctx.origin_connect().await?;
    assert_eq!(
        rows(&holder, "SELECT pg_try_advisory_lock(4242)").await?,
        vec![vec![Some("t".to_owned())]]
    );
    let client = ctx.proxy_client_connect().await?;
    let sql = "SELECT pg_try_advisory_lock(4242) AS got, id FROM items WHERE id = 1";
    for _ in 0..2 {
        let served = uncacheable_read(&mut ctx, &client, sql).await?;
        assert_eq!(
            served,
            vec![vec![Some("f".to_owned()), Some("1".to_owned())]]
        );
    }
    holder
        .simple_query("SELECT pg_advisory_unlock(4242)")
        .await
        .map_err(Error::other)?;
    Ok(())
}

/// `set_config` runs in the session that asked, on origin: the setting is
/// visible to that session afterwards and to no other.
#[tokio::test]
async fn test_set_config_in_select_list_reaches_the_session() -> Result<(), Error> {
    let mut ctx = setup().await?;
    let setter = ctx.proxy_client_connect().await?;
    let other = ctx.proxy_client_connect().await?;
    let sql = "SELECT set_config('app.who', 'x', false) AS who FROM items WHERE id = 1";
    uncacheable_read(&mut ctx, &setter, sql).await?;
    assert_eq!(
        rows(&setter, "SELECT current_setting('app.who', true)").await?,
        vec![vec![Some("x".to_owned())]]
    );
    assert_eq!(
        rows(&other, "SELECT current_setting('app.who', true)").await?,
        vec![vec![None]],
        "another session never sees it"
    );
    Ok(())
}

/// `ORDER BY random()` forwards: the order is origin's to draw.
#[tokio::test]
async fn test_order_by_volatile_forwards() -> Result<(), Error> {
    let mut ctx = setup().await?;
    let client = ctx.proxy_client_connect().await?;
    let sql = "SELECT id FROM items ORDER BY random()";
    for _ in 0..2 {
        let mut served = uncacheable_read(&mut ctx, &client, sql).await?;
        served.sort();
        assert_eq!(
            served,
            vec![
                vec![Some("1".to_owned())],
                vec![Some("2".to_owned())],
                vec![Some("3".to_owned())]
            ]
        );
    }
    Ok(())
}

/// A stable builtin on the allowlist is cached in the SELECT list and
/// recomputed by every hit: `now()` advances from hit to hit, in a plain
/// projection and in an aggregate shape alike, and no materialized result
/// ever serves it.
#[tokio::test]
async fn test_allowlisted_stable_call_is_recomputed_per_hit() -> Result<(), Error> {
    let mut ctx = setup().await?;
    for sql in [
        "SELECT id, now() AS at FROM items WHERE id = 1",
        "SELECT count(*) AS c, now() AS at FROM items WHERE n > 0",
    ] {
        let m = ctx.metrics().await?;
        ctx.simple_query(sql).await?;
        let mut m = assert_cache_miss(&mut ctx, m).await?;
        ctx.cache_settle().await?;
        let mut seen: Vec<Option<String>> = Vec::new();
        for _ in 0..3 {
            tokio::time::sleep(Duration::from_millis(5)).await;
            let served = result_rows(&ctx.simple_query(sql).await?);
            m = assert_cache_hit(&mut ctx, m).await?;
            seen.push(served[0].last().cloned().flatten());
        }
        seen.dedup();
        assert_eq!(
            seen.len(),
            3,
            "now() recomputed on each hit: {sql}\n{seen:?}"
        );
        assert_eq!(
            m.cache_mv_hits, 0,
            "never served from a materialized result: {sql}"
        );
    }
    let mv_tables: i64 = connect_cache_db(&ctx.dbs)
        .await?
        .query_one(
            "SELECT count(*) FROM pg_tables WHERE schemaname = 'pgcache_mv'",
            &[],
        )
        .await
        .map_err(Error::other)?
        .get(0);
    assert_eq!(
        mv_tables, 0,
        "no materialized result is built for an entry recomputed per hit"
    );
    Ok(())
}

/// `to_char` renders under the session's settings on every hit.
#[tokio::test]
async fn test_allowlisted_stable_formatting_call_is_cached() -> Result<(), Error> {
    let mut ctx = setup().await?;
    let sql = "SELECT to_char(n, 'FM0000') AS n, id FROM items WHERE id = 2";
    let m = query_warm(&mut ctx, sql).await?;
    assert_proxy_rows_match_origin(&mut ctx, sql).await?;
    assert_cache_hit(&mut ctx, m).await?;
    Ok(())
}

/// `current_user` in a SELECT list is a session term, keyed on the role
/// (PGC-580): each role has its own cached entry with its own name, never
/// the cache connection's.
#[tokio::test]
async fn test_current_user_in_select_list_is_keyed_on_the_role() -> Result<(), Error> {
    let mut ctx = setup().await?;
    for sql in [
        "CREATE ROLE ann LOGIN",
        "CREATE ROLE bob LOGIN",
        "GRANT SELECT ON items TO ann, bob",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }
    ctx.cdc_apply_settle().await?;
    let sql = "SELECT current_user AS who, id FROM items WHERE id = 1";
    for user in ["ann", "bob"] {
        // The role's first reads may forward while its privilege verdicts
        // are looked up; the entry serves once they are in.
        let client = ctx.proxy_client_connect_as(user).await?;
        let mut served = None;
        for _ in 0..40 {
            let m = ctx.metrics().await?;
            let read = rows(&client, sql).await?;
            let delta = metrics_delta(&m, &ctx.metrics_after_outcomes(&m, 1).await?);
            if delta.queries_cache_hit == 1 {
                served = Some(read);
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(
            served,
            Some(vec![vec![Some(user.to_owned()), Some("1".to_owned())]]),
            "{user} is served its own name from cache"
        );
    }
    Ok(())
}

/// A stable call outside the allowlist forwards even in the SELECT list: it
/// reads state the cache connection would answer for wrongly.
#[tokio::test]
async fn test_stable_call_outside_the_allowlist_forwards() -> Result<(), Error> {
    let mut ctx = setup().await?;
    let client = ctx.proxy_client_connect().await?;
    let origin = ctx.origin_connect().await?;
    for sql in [
        "SELECT current_database() AS db, id FROM items WHERE id = 1",
        "SELECT pg_backend_pid() > 0 AS alive, id FROM items WHERE id = 1",
    ] {
        let served = uncacheable_read(&mut ctx, &client, sql).await?;
        assert_eq!(served, rows(&origin, sql).await?, "{sql}");
    }
    Ok(())
}
