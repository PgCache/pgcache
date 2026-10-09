//! CDC membership for predicates whose literal is spelled differently from
//! the row's wire text (PGC-478): the local evaluator and the eval index
//! compare by column type, so an inserted or updated row that belongs to the
//! cached result is applied in place whatever the spelling.

use std::io::Error;

use crate::util::{TestContext, assert_cache_hit, assert_cache_miss, assert_row_at};

mod util;

async fn warm(ctx: &mut TestContext, sql: &str) -> Result<(), Error> {
    let m = ctx.metrics().await?;
    ctx.simple_query(sql).await?;
    let m = assert_cache_miss(ctx, m).await?;
    ctx.cache_settle().await?;
    ctx.simple_query(sql).await?;
    let _ = assert_cache_hit(ctx, m).await?;
    Ok(())
}

/// A `timestamp` equality written without seconds matches the wire spelling
/// `… 10:00:00` once the row arrives over CDC.
#[tokio::test]
async fn test_timestamp_literal_without_seconds_matches_inserted_row() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(
        "create table ts_items (id integer primary key, at timestamp)",
        &[],
    )
    .await?;
    ctx.query(
        "insert into ts_items values (1, '2024-01-15 10:00:00')",
        &[],
    )
    .await?;
    let sql = "select id from ts_items where at = '2024-01-15 10:00' order by id";
    warm(&mut ctx, sql).await?;

    ctx.origin_query(
        "insert into ts_items values (2, '2024-01-15 10:00:00')",
        &[],
    )
    .await?;
    ctx.cdc_apply_settle().await?;
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(sql).await?;
    assert_eq!(
        res.len(),
        4,
        "two rows plus RowDescription and CommandComplete"
    );
    assert_row_at(&res, 1, &[("id", "1")])?;
    assert_row_at(&res, 2, &[("id", "2")])?;
    let _ = assert_cache_hit(&mut ctx, m).await?;
    Ok(())
}

/// A `timestamptz` range bound with an explicit offset admits a row whose
/// wire text carries a different offset for the same instant.
#[tokio::test]
async fn test_timestamptz_literal_with_offset_matches_inserted_row() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(
        "create table tstz_items (id integer primary key, at timestamptz)",
        &[],
    )
    .await?;
    ctx.query(
        "insert into tstz_items values (1, '2024-01-15 12:00:00+00')",
        &[],
    )
    .await?;
    let sql = "select id from tstz_items where at >= '2024-01-15T10:00:00+05:00' order by id";
    warm(&mut ctx, sql).await?;

    // 05:30 UTC: after the bound (05:00 UTC). Written with its own offset.
    ctx.origin_query(
        "insert into tstz_items values (2, '2024-01-15 11:00:00+05:30')",
        &[],
    )
    .await?;
    // 04:00 UTC: before the bound; must not appear.
    ctx.origin_query(
        "insert into tstz_items values (3, '2024-01-15 04:00:00+00')",
        &[],
    )
    .await?;
    ctx.cdc_apply_settle().await?;
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(sql).await?;
    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("id", "1")])?;
    assert_row_at(&res, 2, &[("id", "2")])?;
    let _ = assert_cache_hit(&mut ctx, m).await?;
    Ok(())
}

/// A `numeric` literal with trailing zeros matches the wire `10.5`.
#[tokio::test]
async fn test_numeric_literal_spelling_matches_inserted_row() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(
        "create table price_items (id integer primary key, price numeric(10,2))",
        &[],
    )
    .await?;
    ctx.query("insert into price_items values (1, 10.50)", &[])
        .await?;
    let sql = "select id from price_items where price = '10.500' order by id";
    warm(&mut ctx, sql).await?;

    ctx.origin_query("insert into price_items values (2, 10.5), (3, 9.99)", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(sql).await?;
    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("id", "1")])?;
    assert_row_at(&res, 2, &[("id", "2")])?;
    let _ = assert_cache_hit(&mut ctx, m).await?;
    Ok(())
}

/// A date spelled with time of day matches the `date` column's wire text,
/// and an update moving a row into the predicate is applied in place.
#[tokio::test]
async fn test_date_literal_with_time_matches_updated_row() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(
        "create table day_items (id integer primary key, day date)",
        &[],
    )
    .await?;
    ctx.query(
        "insert into day_items values (1, '2024-01-15'), (2, '2024-01-16')",
        &[],
    )
    .await?;
    let sql = "select id from day_items where day = '2024-01-15 00:00' order by id";
    warm(&mut ctx, sql).await?;

    ctx.origin_query("update day_items set day = '2024-01-15' where id = 2", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(sql).await?;
    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("id", "1")])?;
    assert_row_at(&res, 2, &[("id", "2")])?;
    let _ = assert_cache_hit(&mut ctx, m).await?;
    Ok(())
}
