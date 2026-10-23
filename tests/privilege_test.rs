//! Table, column, schema and function privileges on the cache hit path
//! (PGC-463, ADR-061): a cached result is served only to a role origin would
//! let run the read; any other role is forwarded, so origin raises its own
//! error. A role's verdicts are fetched at registration for the registering
//! role and on the first hit for any other, and flushed by every DDL.

use std::io::Error;
use std::time::Duration;

use tokio_postgres::{Client, SimpleQueryMessage, error::SqlState};

use crate::util::{
    MetricsSnapshot, TestContext, assert_cache_hit, assert_cache_miss, metrics_delta, result_rows,
};

mod util;

const READ_BOTH: &str = "SELECT id, v FROM secret WHERE id = 1";
const READ_ID: &str = "SELECT id FROM secret WHERE id = 1";
const READ_COUNT: &str = "SELECT count(*) FROM secret";

/// A `secret` table readable in full by `reader` and not at all by
/// `outsider`; both can log in.
async fn roles_create(ctx: &mut TestContext) -> Result<(), Error> {
    for sql in [
        "CREATE TABLE secret (id int primary key, v text)",
        "INSERT INTO secret VALUES (1, 'classified')",
        "CREATE ROLE reader LOGIN",
        "CREATE ROLE outsider LOGIN",
        "GRANT SELECT ON secret TO reader",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }
    ctx.cache_settle().await
}

/// Register `sql` through `client` into a steady cache hit: the registering
/// role's verdicts are fetched before Ready, so its second read is a hit.
async fn register(ctx: &mut TestContext, client: &Client, sql: &str) -> Result<(), Error> {
    let m = ctx.metrics().await?;
    client.simple_query(sql).await.map_err(Error::other)?;
    let m = assert_cache_miss(ctx, m).await?;
    ctx.cache_settle().await?;
    client.simple_query(sql).await.map_err(Error::other)?;
    assert_cache_hit(ctx, m).await?;
    Ok(())
}

fn assert_permission_denied(
    result: Result<Vec<SimpleQueryMessage>, tokio_postgres::Error>,
    what: &str,
) {
    let rows = result.as_ref().ok().map(|messages| result_rows(messages));
    let err = result.expect_err(&format!("{what}: permission denied, not rows {rows:?}"));
    assert_eq!(
        err.code(),
        Some(&SqlState::INSUFFICIENT_PRIVILEGE),
        "{what}: expected permission denied, got {err}"
    );
}

/// Read `sql` through `client` until one read's metrics delta satisfies
/// `done`, returning that read's rows. A role's first hit after a verdict
/// gap forwards while the writer looks the verdict up; the next serves.
async fn read_until(
    ctx: &mut TestContext,
    client: &Client,
    sql: &str,
    done: impl Fn(&MetricsSnapshot) -> bool,
) -> Result<Vec<Vec<Option<String>>>, Error> {
    for _ in 0..40 {
        let before = ctx.metrics().await?;
        let res = client.simple_query(sql).await.map_err(Error::other)?;
        let after = ctx.metrics_after_outcomes(&before, 1).await?;
        if done(&metrics_delta(&before, &after)) {
            return Ok(result_rows(&res));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(Error::other(format!(
        "no read of `{sql}` reached the expected outcome"
    )))
}

/// `sql` through `client` becomes a cache hit.
async fn hit_wait(
    ctx: &mut TestContext,
    client: &Client,
    sql: &str,
) -> Result<Vec<Vec<Option<String>>>, Error> {
    read_until(ctx, client, sql, |delta| delta.queries_cache_hit == 1).await
}

/// `sql` through `client` is denied on a recorded verdict (not merely
/// forwarded while one is pending), and origin refuses it.
async fn denied_wait(
    ctx: &mut TestContext,
    client: &Client,
    sql: &str,
    what: &str,
) -> Result<(), Error> {
    for _ in 0..40 {
        let before = ctx.metrics().await?;
        assert_permission_denied(client.simple_query(sql).await, what);
        let after = ctx.metrics_after_outcomes(&before, 1).await?;
        if metrics_delta(&before, &after).queries_privilege_forward_denied == 1 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(Error::other(format!(
        "{what}: never denied on a recorded verdict"
    )))
}

/// A role without `SELECT` on the table must not be served the cached rows
/// another role registered; its reads forward to origin's error.
#[tokio::test]
async fn test_role_without_table_grant_is_denied_cached_rows() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    roles_create(&mut ctx).await?;

    let reader = ctx.proxy_client_connect_as("reader").await?;
    register(&mut ctx, &reader, READ_BOTH).await?;

    let outsider = ctx.proxy_client_connect_as("outsider").await?;
    denied_wait(&mut ctx, &outsider, READ_BOTH, "outsider reads secret").await?;
    // The reader is unaffected.
    let m = ctx.metrics().await?;
    reader.simple_query(READ_BOTH).await.map_err(Error::other)?;
    assert_cache_hit(&mut ctx, m).await?;
    Ok(())
}

/// A role granted `SELECT` on some columns only reads those: the cached
/// two-column result is denied, while a read of the granted column alone,
/// subsumed by that same entry, is served.
#[tokio::test]
async fn test_role_with_column_grant_reads_only_granted_columns() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    roles_create(&mut ctx).await?;
    ctx.origin_query("GRANT SELECT (id) ON secret TO outsider", &[])
        .await?;

    let reader = ctx.proxy_client_connect_as("reader").await?;
    register(&mut ctx, &reader, READ_BOTH).await?;

    let outsider = ctx.proxy_client_connect_as("outsider").await?;
    denied_wait(
        &mut ctx,
        &outsider,
        READ_BOTH,
        "outsider reads an ungranted column",
    )
    .await?;
    let rows = hit_wait(&mut ctx, &outsider, READ_ID).await?;
    assert_eq!(rows, vec![vec![Some("1".to_owned())]]);
    Ok(())
}

/// A read referencing no column needs `SELECT` on the table or on any one
/// column: a column grant serves it, no grant at all denies it.
#[tokio::test]
async fn test_zero_column_read_needs_any_granted_column() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    roles_create(&mut ctx).await?;
    for sql in [
        "CREATE ROLE nobody LOGIN",
        "GRANT SELECT (id) ON secret TO outsider",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }

    let reader = ctx.proxy_client_connect_as("reader").await?;
    register(&mut ctx, &reader, READ_COUNT).await?;

    let outsider = ctx.proxy_client_connect_as("outsider").await?;
    let rows = hit_wait(&mut ctx, &outsider, READ_COUNT).await?;
    assert_eq!(rows, vec![vec![Some("1".to_owned())]]);
    let nobody = ctx.proxy_client_connect_as("nobody").await?;
    denied_wait(&mut ctx, &nobody, READ_COUNT, "nobody counts secret").await?;
    Ok(())
}

/// `SET ROLE` changes the identity a hit is checked against, and `RESET
/// ROLE` changes it back.
#[tokio::test]
async fn test_set_role_switches_the_checked_identity() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    roles_create(&mut ctx).await?;
    for sql in [
        "CREATE ROLE switcher LOGIN NOINHERIT",
        "GRANT reader TO switcher",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }

    let reader = ctx.proxy_client_connect_as("reader").await?;
    register(&mut ctx, &reader, READ_BOTH).await?;

    let switcher = ctx.proxy_client_connect_as("switcher").await?;
    denied_wait(&mut ctx, &switcher, READ_BOTH, "switcher as itself").await?;
    switcher
        .simple_query("SET ROLE reader")
        .await
        .map_err(Error::other)?;
    let rows = hit_wait(&mut ctx, &switcher, READ_BOTH).await?;
    assert_eq!(
        rows,
        vec![vec![Some("1".to_owned()), Some("classified".to_owned())]]
    );
    switcher
        .simple_query("RESET ROLE")
        .await
        .map_err(Error::other)?;
    denied_wait(&mut ctx, &switcher, READ_BOTH, "switcher after RESET ROLE").await?;
    Ok(())
}

/// `SET SESSION AUTHORIZATION` (a superuser's) is the identity checked.
#[tokio::test]
async fn test_set_session_authorization_is_checked() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    roles_create(&mut ctx).await?;
    let m = ctx.metrics().await?;
    ctx.simple_query(READ_BOTH).await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    ctx.simple_query(READ_BOTH).await?;
    assert_cache_hit(&mut ctx, m).await?;

    ctx.simple_query("SET SESSION AUTHORIZATION outsider")
        .await?;
    let superuser = ctx.proxy_client_connect().await?;
    let impersonating = std::mem::replace(&mut ctx.cache, superuser);
    denied_wait(&mut ctx, &impersonating, READ_BOTH, "superuser as outsider").await?;
    impersonating
        .simple_query("RESET SESSION AUTHORIZATION")
        .await
        .map_err(Error::other)?;
    hit_wait(&mut ctx, &impersonating, READ_BOTH).await?;
    Ok(())
}

/// A `REVOKE` on origin reaches the cache as a schema-change signal: the
/// verdicts are flushed, the next read forwards and origin refuses it. A
/// `GRANT` back serves again.
#[tokio::test]
async fn test_revoke_mid_run_forwards_to_origin_error() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    roles_create(&mut ctx).await?;

    let reader = ctx.proxy_client_connect_as("reader").await?;
    register(&mut ctx, &reader, READ_BOTH).await?;

    ctx.origin_query("REVOKE SELECT ON secret FROM reader", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    denied_wait(&mut ctx, &reader, READ_BOTH, "reader after REVOKE").await?;

    ctx.origin_query("GRANT SELECT ON secret TO reader", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    hit_wait(&mut ctx, &reader, READ_BOTH).await?;
    Ok(())
}

/// `EXECUTE` on a function the query calls is part of the check. A
/// built-in function: a user-defined one has no copy on the cache
/// database to serve from.
#[tokio::test]
async fn test_function_execute_privilege_is_checked() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    roles_create(&mut ctx).await?;
    for sql in [
        "REVOKE EXECUTE ON FUNCTION pg_catalog.upper(text) FROM PUBLIC",
        "GRANT EXECUTE ON FUNCTION pg_catalog.upper(text) TO reader",
        "GRANT SELECT ON secret TO outsider",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }
    ctx.cache_settle().await?;
    let sql = "SELECT upper(v) FROM secret WHERE id = 1";

    let reader = ctx.proxy_client_connect_as("reader").await?;
    register(&mut ctx, &reader, sql).await?;

    let outsider = ctx.proxy_client_connect_as("outsider").await?;
    denied_wait(&mut ctx, &outsider, sql, "outsider calls upper").await?;
    Ok(())
}

/// `USAGE` on the table's schema is part of the check: without it origin
/// refuses the read even with `SELECT` on the table.
#[tokio::test]
async fn test_schema_usage_is_checked() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    roles_create(&mut ctx).await?;
    for sql in [
        "CREATE SCHEMA vault",
        "CREATE TABLE vault.t (id int primary key, v text)",
        "INSERT INTO vault.t VALUES (1, 'locked')",
        "GRANT USAGE ON SCHEMA vault TO reader",
        "GRANT SELECT ON vault.t TO reader",
        "GRANT SELECT ON vault.t TO outsider",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }
    ctx.cache_settle().await?;
    let sql = "SELECT id, v FROM vault.t WHERE id = 1";

    let reader = ctx.proxy_client_connect_as("reader").await?;
    register(&mut ctx, &reader, sql).await?;

    let outsider = ctx.proxy_client_connect_as("outsider").await?;
    denied_wait(&mut ctx, &outsider, sql, "outsider without USAGE on vault").await?;
    Ok(())
}

/// A coalesced waiter is gated at the drain like a direct hit: a waiter
/// whose role may not read is forwarded, not handed the group's rows.
#[tokio::test]
async fn test_coalesced_waiter_of_denied_role_forwards() -> Result<(), Error> {
    // The population delay must end inside the waiter's 200ms cold coalesce
    // deadline, or the waiter forwards on the deadline instead of draining.
    let mut ctx = TestContext::setup_fault(&[("PGCACHE_FAULT_POPULATION_DELAY_MS", "150")]).await?;
    roles_create(&mut ctx).await?;

    let reader = ctx.proxy_client_connect_as("reader").await?;
    let outsider = ctx.proxy_client_connect_as("outsider").await?;
    let before = ctx.metrics().await?;
    let loader = tokio::spawn(async move { reader.simple_query(READ_BOTH).await });
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_permission_denied(
        outsider.simple_query(READ_BOTH).await,
        "outsider waiting on the reader's population",
    );
    loader.await.map_err(Error::other)?.map_err(Error::other)?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(
        delta.cache_coalesce_deadline_forward, 0,
        "the waiter must reach the Ready drain, not the deadline"
    );
    assert_eq!(
        delta.queries_privilege_forward_pending + delta.queries_privilege_forward_denied,
        1,
        "the waiter was forwarded on its role"
    );
    Ok(())
}
