//! Cache hits render text results under the client's own session settings
//! (PGC-287 class 3, ADR-056): the serve pool applies them on the cache-DB
//! connection before the SELECT, and memo entries are keyed by them.

use std::io::Error;
use std::time::Duration;

use tokio_postgres::SimpleQueryMessage;

use crate::util::{
    TestContext, assert_cache_hit, assert_cache_miss, assert_row_at, extract_row, metrics_delta,
};

mod util;

const TABLE_SQL: &str =
    "CREATE TABLE render_items (id int primary key, at timestamptz, f float8, b bytea)";
const SEED_SQL: &str =
    "INSERT INTO render_items VALUES (1, '2024-01-01 00:00:00+00', 0.1, '\\x0102')";
const READ_AT: &str = "SELECT id, at FROM render_items WHERE id = 1";
const READ_F_B: &str = "SELECT id, f, b FROM render_items WHERE id = 1";

async fn seed(ctx: &mut TestContext) -> Result<(), Error> {
    table_seed(ctx, TABLE_SQL, SEED_SQL).await
}

async fn table_seed(ctx: &mut TestContext, create: &str, insert: &str) -> Result<(), Error> {
    ctx.query(create, &[]).await?;
    ctx.simple_query(insert).await?;
    ctx.cache_settle().await?;
    Ok(())
}

/// The first row's values for `columns`, in order.
fn row_values(
    messages: &[SimpleQueryMessage],
    columns: &[&str],
) -> Result<Vec<Option<String>>, Error> {
    let row = extract_row(messages, 1)?;
    Ok(columns
        .iter()
        .map(|column| row.get::<&str>(column).map(str::to_owned))
        .collect())
}

/// Apply `sets` on the client and on a direct origin session, then register
/// `read` and check its hit renders `columns` as origin does. Each column
/// must render differently from origin's defaults, so a setting the serve
/// ignored cannot pass. Returns origin's rendering.
async fn rendering_matches_origin(
    ctx: &mut TestContext,
    sets: &[&str],
    read: &str,
    columns: &[&str],
) -> Result<Vec<Option<String>>, Error> {
    let origin = ctx.origin_connect().await?;
    let defaults = row_values(
        &origin.simple_query(read).await.map_err(Error::other)?,
        columns,
    )?;
    for sql in sets {
        origin.simple_query(sql).await.map_err(Error::other)?;
        ctx.simple_query(sql).await?;
    }
    let expected = row_values(
        &origin.simple_query(read).await.map_err(Error::other)?,
        columns,
    )?;
    for (column, (default, value)) in columns.iter().zip(defaults.iter().zip(&expected)) {
        assert_ne!(
            default, value,
            "{column} renders the same under the defaults"
        );
    }

    let m = ctx.metrics().await?;
    let _ = ctx.simple_query(read).await?;
    let m = assert_cache_miss(ctx, m).await?;
    ctx.cache_settle().await?;
    let res = ctx.simple_query(read).await?;
    let _m = assert_cache_hit(ctx, m).await?;
    assert_eq!(
        row_values(&res, columns)?,
        expected,
        "the hit renders as origin"
    );
    Ok(expected)
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

/// `lc_monetary`, `lc_numeric` and `lc_time` are applied on the serve
/// connection: a `money` column and locale-aware `to_char` patterns render
/// on a hit as origin renders them.
#[tokio::test]
async fn test_locale_rendering_matches_origin_on_hit() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    table_seed(
        &mut ctx,
        "CREATE TABLE locale_items (id int primary key, m money, n numeric, at timestamptz)",
        "INSERT INTO locale_items VALUES (1, 1234.56, 1234.5, '2024-01-15 12:00:00+00')",
    )
    .await?;
    let rendered = rendering_matches_origin(
        &mut ctx,
        &[
            "SET lc_monetary = 'de_DE.UTF-8'",
            "SET lc_numeric = 'de_DE.UTF-8'",
            "SET lc_time = 'de_DE.UTF-8'",
        ],
        "SELECT m, to_char(n, 'FM9G999D99') AS n, to_char(at, 'TMMonth') AS month \
         FROM locale_items WHERE id = 1",
        &["m", "n", "month"],
    )
    .await?;
    assert_eq!(rendered[1].as_deref(), Some("1.234,5"));
    assert_eq!(rendered[2].as_deref(), Some("Januar"));
    Ok(())
}

/// `IntervalStyle` is applied on the serve connection.
#[tokio::test]
async fn test_interval_style_rendering_matches_origin_on_hit() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    table_seed(
        &mut ctx,
        "CREATE TABLE interval_items (id int primary key, d interval)",
        "INSERT INTO interval_items VALUES (1, '1 year 2 mons 3 days 04:05:06')",
    )
    .await?;
    let rendered = rendering_matches_origin(
        &mut ctx,
        &["SET IntervalStyle = 'sql_standard'"],
        "SELECT d FROM interval_items WHERE id = 1",
        &["d"],
    )
    .await?;
    assert_eq!(rendered[0].as_deref(), Some("+1-2 +3 +4:05:06"));
    Ok(())
}
