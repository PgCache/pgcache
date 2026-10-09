//! Cache hits render text results under the client's own session settings
//! (PGC-287 class 3, ADR-056): the serve pool applies them on the cache-DB
//! connection before the SELECT, and memo entries are keyed by them.

use std::io::Error;
use std::time::Duration;

use crate::util::{TestContext, assert_cache_hit, assert_cache_miss, assert_row_at, metrics_delta};

mod util;

const TABLE_SQL: &str =
    "CREATE TABLE render_items (id int primary key, at timestamptz, f float8, b bytea)";
const SEED_SQL: &str =
    "INSERT INTO render_items VALUES (1, '2024-01-01 00:00:00+00', 0.1, '\\x0102')";
const READ_AT: &str = "SELECT id, at FROM render_items WHERE id = 1";
const READ_F_B: &str = "SELECT id, f, b FROM render_items WHERE id = 1";

async fn seed(ctx: &mut TestContext) -> Result<(), Error> {
    ctx.query(TABLE_SQL, &[]).await?;
    ctx.simple_query(SEED_SQL).await?;
    ctx.cache_settle().await?;
    Ok(())
}

/// The miss (rendered by origin) and the hit (rendered by the cache DB) show
/// the same text under a non-default `TimeZone`.
#[tokio::test]
async fn test_timezone_rendering_matches_origin_on_hit() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    seed(&mut ctx).await?;
    ctx.simple_query("SET TimeZone = 'Asia/Tokyo'").await?;

    let m = ctx.metrics().await?;
    let res = ctx.simple_query(READ_AT).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("at", "2024-01-01 09:00:00+09")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    let res = ctx.simple_query(READ_AT).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("at", "2024-01-01 09:00:00+09")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;
    Ok(())
}

/// `extra_float_digits` and `bytea_output` are applied the same way.
#[tokio::test]
async fn test_float_and_bytea_rendering_match_origin_on_hit() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    seed(&mut ctx).await?;
    ctx.simple_query("SET extra_float_digits = 0").await?;
    ctx.simple_query("SET bytea_output = 'escape'").await?;

    let m = ctx.metrics().await?;
    let res = ctx.simple_query(READ_F_B).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("f", "0.1"), ("b", "\\001\\002")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    let res = ctx.simple_query(READ_F_B).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("f", "0.1"), ("b", "\\001\\002")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;
    Ok(())
}

/// Two clients on different zones each get their own rendering from the same
/// cache entry, and the first client's memoized bytes are not handed to the
/// second.
#[tokio::test]
async fn test_each_client_gets_its_own_rendering() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    seed(&mut ctx).await?;
    ctx.simple_query("SET TimeZone = 'Asia/Tokyo'").await?;
    ctx.simple_query(READ_AT).await?;
    ctx.cache_settle().await?;
    // Hot enough to be memoized for the Tokyo client.
    let mut memoized = false;
    for _ in 0..80 {
        ctx.simple_query(READ_AT).await?;
        if ctx.metrics().await?.cache_memo_hits >= 1 {
            memoized = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(memoized, "query never served from memo");

    let other = ctx.proxy_client_connect().await?;
    other
        .simple_query("SET TimeZone = 'UTC'")
        .await
        .map_err(Error::other)?;
    let before = ctx.metrics().await?;
    let res = other.simple_query(READ_AT).await.map_err(Error::other)?;
    assert_row_at(&res, 1, &[("id", "1"), ("at", "2024-01-01 00:00:00+00")])?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.queries_cache_hit, 1, "served from the shared entry");
    assert_eq!(delta.cache_memo_hits, 0, "not from the other client's memo");

    // The first client still sees Tokyo.
    let res = ctx.simple_query(READ_AT).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("at", "2024-01-01 09:00:00+09")])?;
    Ok(())
}

/// A serve connection remembers the settings it applied: repeated hits from
/// one client apply them at most once per pooled connection.
#[tokio::test]
async fn test_settings_applied_once_per_connection() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    seed(&mut ctx).await?;
    ctx.simple_query("SET TimeZone = 'Asia/Tokyo'").await?;
    ctx.simple_query(READ_AT).await?;
    ctx.cache_settle().await?;
    ctx.simple_query(READ_AT).await?;

    let before = ctx.metrics().await?;
    for _ in 0..20 {
        ctx.simple_query(READ_AT).await?;
    }
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert!(
        delta.cache_serve_settings_applied < 20,
        "settings re-applied on every hit: {}",
        delta.cache_serve_settings_applied
    );
    Ok(())
}
