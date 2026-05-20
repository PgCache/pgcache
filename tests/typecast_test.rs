//! Integration tests for `TypeCast` nodes in WHERE clauses.

use std::io::Error;

use crate::util::{TestContext, assert_cache_hit, assert_cache_miss, assert_row_at};

mod util;

/// `WHERE col::text = 'literal'` — column cast against a text literal.
/// The canonical ORM pattern (PGC-120 cites `WHERE col::text = 'foo'`).
#[tokio::test]
async fn test_typecast_column_to_text_cacheable() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table tc_text (id integer primary key, val integer)",
        &[],
    )
    .await?;
    ctx.query(
        "insert into tc_text (id, val) values (1, 5), (2, 42), (3, 99)",
        &[],
    )
    .await?;

    let sql = "select id, val from tc_text where val::text = '42' order by id";

    let m = ctx.metrics().await?;
    let res = ctx.simple_query(sql).await?;
    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "2"), ("val", "42")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    let res = ctx.simple_query(sql).await?;
    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "2"), ("val", "42")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// `WHERE (a + b)::int > 10` — arithmetic-result cast.
#[tokio::test]
async fn test_typecast_arithmetic_result_cacheable() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table tc_arith (id integer primary key, a integer, b integer)",
        &[],
    )
    .await?;
    ctx.query(
        "insert into tc_arith (id, a, b) values (1, 1, 2), (2, 5, 7), (3, 100, 200)",
        &[],
    )
    .await?;

    let sql = "select id from tc_arith where (a + b)::int > 10 order by id";

    let m = ctx.metrics().await?;
    let res = ctx.simple_query(sql).await?;
    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("id", "2")])?;
    assert_row_at(&res, 2, &[("id", "3")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    let res = ctx.simple_query(sql).await?;
    assert_eq!(res.len(), 4);
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// `WHERE col::date = 'YYYY-MM-DD'` — date cast against a date literal.
#[tokio::test]
async fn test_typecast_timestamp_to_date_cacheable() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table tc_date (id integer primary key, created_at timestamp)",
        &[],
    )
    .await?;
    ctx.query(
        "insert into tc_date (id, created_at) values \
         (1, '2024-01-01 09:00:00'), \
         (2, '2024-01-01 23:59:59'), \
         (3, '2024-01-02 00:00:00')",
        &[],
    )
    .await?;

    let sql = "select id from tc_date where created_at::date = '2024-01-01' order by id";

    // ids 1 and 2 fall on 2024-01-01; id 3 rolls into 2024-01-02.
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(sql).await?;
    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("id", "1")])?;
    assert_row_at(&res, 2, &[("id", "2")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    let res = ctx.simple_query(sql).await?;
    assert_eq!(res.len(), 4);
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}
