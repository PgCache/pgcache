//! Table and column privileges on the cache hit path (PGC-463): a cached
//! result is served only to a role origin would let read every referenced
//! column; any other role is forwarded, so origin raises its own error.

use std::io::Error;

use tokio_postgres::{Client, error::SqlState};

use crate::util::{TestContext, assert_cache_hit, assert_cache_miss, result_rows};

mod util;

const READ_BOTH: &str = "SELECT id, v FROM secret WHERE id = 1";
const READ_ID: &str = "SELECT id FROM secret WHERE id = 1";

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

/// Register `sql` through `client` into a steady cache hit.
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
    result: Result<Vec<tokio_postgres::SimpleQueryMessage>, tokio_postgres::Error>,
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

/// A role without `SELECT` on the table must not be served the cached rows
/// another role registered.
#[tokio::test]
async fn test_role_without_table_grant_is_denied_cached_rows() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    roles_create(&mut ctx).await?;

    let reader = ctx.proxy_client_connect_as("reader").await?;
    register(&mut ctx, &reader, READ_BOTH).await?;

    let outsider = ctx.proxy_client_connect_as("outsider").await?;
    assert_permission_denied(
        outsider.simple_query(READ_BOTH).await,
        "outsider reads secret",
    );
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
    assert_permission_denied(
        outsider.simple_query(READ_BOTH).await,
        "outsider reads an ungranted column",
    );
    let rows = outsider.simple_query(READ_ID).await.map_err(Error::other)?;
    assert_eq!(result_rows(&rows), vec![vec![Some("1".to_owned())]]);
    Ok(())
}
