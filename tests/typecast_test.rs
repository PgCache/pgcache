//! Integration tests for `TypeCast` nodes in WHERE clauses.

use std::io::Error;

use crate::util::{TestContext, assert_cache_hit, assert_cache_miss, assert_row_at, metrics_delta};

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

/// PGC-149: `WHERE col::text = 'literal'` on a text column must take the
/// **CDC LocalEval** fast path on subsequent writes — the identity cast is
/// stripped, so per-row predicate evaluation runs in Rust rather than
/// round-tripping to origin via PgEval.
#[tokio::test]
async fn test_typecast_text_column_uses_local_eval_on_cdc() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table tc_text_col (id integer primary key, name text)",
        &[],
    )
    .await?;
    ctx.query(
        "insert into tc_text_col (id, name) values (1, 'alice'), (2, 'bob')",
        &[],
    )
    .await?;

    let sql = "select id, name from tc_text_col where name::text = 'alice' order by id";

    // Populate the cache.
    let res = ctx.simple_query(sql).await?;
    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("name", "alice")])?;
    ctx.cache_settle().await?;

    // Issue a CDC-triggering write. The new row matches the predicate
    // (`name = 'alice'`) so the CDC handler must decide whether to insert
    // it into the cache. With identity-cast strip, that decision happens
    // locally (LocalEval); without it, the writer would punt to PgEval.
    let m_before = ctx.metrics().await?;
    ctx.query(
        "insert into tc_text_col (id, name) values (3, 'alice')",
        &[],
    )
    .await?;
    ctx.cdc_settle().await?;
    let m_after = ctx.metrics().await?;

    let delta = metrics_delta(&m_before, &m_after);
    assert!(
        delta.cache_cdc_local_eval_hits >= 1,
        "expected LocalEval CDC hit, got delta {delta:?}"
    );
    assert_eq!(
        delta.cache_cdc_pg_eval_hits, 0,
        "no PgEval round-trip should be needed for an identity ::text cast"
    );

    // And the result reflects the new row from cache.
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(sql).await?;
    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("id", "1"), ("name", "alice")])?;
    assert_row_at(&res, 2, &[("id", "3"), ("name", "alice")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// PGC-177: `WHERE col::text = '42'` on an **int** column must also take
/// the CDC LocalEval fast path — int wire-text matches canonical int→text
/// byte-for-byte, so the cast is identity even though the column isn't text.
#[tokio::test]
async fn test_typecast_int_column_uses_local_eval_on_cdc() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table tc_int_col (id integer primary key, val integer)",
        &[],
    )
    .await?;
    ctx.query(
        "insert into tc_int_col (id, val) values (1, 42), (2, 7)",
        &[],
    )
    .await?;

    let sql = "select id, val from tc_int_col where val::text = '42' order by id";

    let res = ctx.simple_query(sql).await?;
    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("val", "42")])?;
    ctx.cache_settle().await?;

    let m_before = ctx.metrics().await?;
    ctx.query("insert into tc_int_col (id, val) values (3, 42)", &[])
        .await?;
    ctx.cdc_settle().await?;
    let m_after = ctx.metrics().await?;

    let delta = metrics_delta(&m_before, &m_after);
    assert!(
        delta.cache_cdc_local_eval_hits >= 1,
        "expected LocalEval CDC hit for int::text identity cast, got delta {delta:?}"
    );
    assert_eq!(
        delta.cache_cdc_pg_eval_hits, 0,
        "no PgEval round-trip should be needed for an identity ::text cast on an int column"
    );

    let m = ctx.metrics().await?;
    let res = ctx.simple_query(sql).await?;
    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("id", "1"), ("val", "42")])?;
    assert_row_at(&res, 2, &[("id", "3"), ("val", "42")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// PGC-178: `WHERE text_col::int = N` must take CDC LocalEval — the row's
/// stored text is coerced to int locally and compared against the literal
/// without round-tripping to origin.
#[tokio::test]
async fn test_typecast_text_to_int4_uses_local_eval_on_cdc() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table tc_text_int (id integer primary key, code text)",
        &[],
    )
    .await?;
    ctx.query(
        "insert into tc_text_int (id, code) values (1, '42'), (2, '7')",
        &[],
    )
    .await?;

    let sql = "select id, code from tc_text_int where code::int = 42 order by id";

    let res = ctx.simple_query(sql).await?;
    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("code", "42")])?;
    ctx.cache_settle().await?;

    let m_before = ctx.metrics().await?;
    ctx.query("insert into tc_text_int (id, code) values (3, '42')", &[])
        .await?;
    ctx.cdc_settle().await?;
    let m_after = ctx.metrics().await?;

    let delta = metrics_delta(&m_before, &m_after);
    assert!(
        delta.cache_cdc_local_eval_hits >= 1,
        "expected LocalEval CDC hit for text::int coercion, got delta {delta:?}"
    );
    assert_eq!(
        delta.cache_cdc_pg_eval_hits, 0,
        "no PgEval round-trip should be needed for a whitelisted text::int coercion"
    );

    let m = ctx.metrics().await?;
    let res = ctx.simple_query(sql).await?;
    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("id", "1"), ("code", "42")])?;
    assert_row_at(&res, 2, &[("id", "3"), ("code", "42")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// PGC-181: `WHERE text_col::bool = true` must take CDC LocalEval — the
/// stored text is parsed locally per postgres bool rules and compared
/// against the literal without round-tripping to origin.
#[tokio::test]
async fn test_typecast_text_to_bool_uses_local_eval_on_cdc() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table tc_text_bool (id integer primary key, flag text)",
        &[],
    )
    .await?;
    ctx.query(
        "insert into tc_text_bool (id, flag) values (1, 'true'), (2, 'false')",
        &[],
    )
    .await?;

    let sql = "select id, flag from tc_text_bool where flag::bool = true order by id";

    let res = ctx.simple_query(sql).await?;
    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("flag", "true")])?;
    ctx.cache_settle().await?;

    let m_before = ctx.metrics().await?;
    ctx.query("insert into tc_text_bool (id, flag) values (3, 't')", &[])
        .await?;
    ctx.cdc_settle().await?;
    let m_after = ctx.metrics().await?;

    let delta = metrics_delta(&m_before, &m_after);
    assert!(
        delta.cache_cdc_local_eval_hits >= 1,
        "expected LocalEval CDC hit for text::bool coercion, got delta {delta:?}"
    );
    assert_eq!(
        delta.cache_cdc_pg_eval_hits, 0,
        "no PgEval round-trip should be needed for text::bool"
    );

    let m = ctx.metrics().await?;
    let res = ctx.simple_query(sql).await?;
    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("id", "1"), ("flag", "true")])?;
    assert_row_at(&res, 2, &[("id", "3"), ("flag", "t")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// PGC-180: `WHERE timestamp_col::date = '2024-01-15'` on a plain
/// `timestamp` column must take CDC LocalEval — the date prefix of the
/// timestamp wire-text is extracted locally and compared lexicographically.
#[tokio::test]
async fn test_typecast_timestamp_to_date_uses_local_eval_on_cdc() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table tc_ts_date (id integer primary key, created_at timestamp)",
        &[],
    )
    .await?;
    ctx.query(
        "insert into tc_ts_date (id, created_at) values \
         (1, '2024-01-15 09:00:00'), \
         (2, '2024-01-16 23:59:59')",
        &[],
    )
    .await?;

    let sql = "select id from tc_ts_date where created_at::date = '2024-01-15' order by id";

    let res = ctx.simple_query(sql).await?;
    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1")])?;
    ctx.cache_settle().await?;

    let m_before = ctx.metrics().await?;
    ctx.query(
        "insert into tc_ts_date (id, created_at) values (3, '2024-01-15 23:45:00')",
        &[],
    )
    .await?;
    ctx.cdc_settle().await?;
    let m_after = ctx.metrics().await?;

    let delta = metrics_delta(&m_before, &m_after);
    assert!(
        delta.cache_cdc_local_eval_hits >= 1,
        "expected LocalEval CDC hit for timestamp::date coercion, got delta {delta:?}"
    );
    assert_eq!(
        delta.cache_cdc_pg_eval_hits, 0,
        "no PgEval round-trip should be needed for timestamp::date"
    );

    let m = ctx.metrics().await?;
    let res = ctx.simple_query(sql).await?;
    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("id", "1")])?;
    assert_row_at(&res, 2, &[("id", "3")])?;
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
