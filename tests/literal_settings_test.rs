//! Date-time literals are canonicalised at registration under the session's
//! literal settings (PGC-287 class 2, ADR-056): sessions whose literals mean
//! different instants get different entries, sessions whose literals mean the
//! same instant share one, and a literal the canonicaliser cannot handle is
//! served only to sessions on the reference settings.

use std::io::Error;

use crate::util::{TestContext, assert_cache_hit, assert_cache_miss, assert_row_at, metrics_delta};

mod util;

/// Three instants: 00:30, 06:30 and 12:00 UTC on 2024-01-02.
async fn instants_table_create(ctx: &mut TestContext) -> Result<(), Error> {
    ctx.query(
        "create table instants (id integer primary key, at timestamptz)",
        &[],
    )
    .await?;
    ctx.query(
        "insert into instants values (1, '2024-01-02 00:30:00+00'), \
         (2, '2024-01-02 06:30:00+00'), (3, '2024-01-02 12:00:00+00')",
        &[],
    )
    .await?;
    ctx.cache_settle().await
}

fn ids(res: &[tokio_postgres::SimpleQueryMessage]) -> Result<Vec<String>, Error> {
    let mut ids = Vec::new();
    for message in res {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = message {
            ids.push(row.get(0).unwrap_or_default().to_owned());
        }
    }
    Ok(ids)
}

/// A zone-less `timestamptz` literal means a different instant under each
/// session zone, so the same text lands on two entries, each with its rows.
#[tokio::test]
async fn test_zoneless_timestamptz_literal_keys_per_session_zone() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    instants_table_create(&mut ctx).await?;
    let sql = "select id from instants where at >= '2024-01-02 10:00' order by id";

    // Tokyo: 10:00 is 01:00 UTC, admitting the 06:30 and 12:00 rows.
    ctx.simple_query("SET TimeZone = 'Asia/Tokyo'").await?;
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(sql).await?;
    assert_eq!(ids(&res)?, ["2", "3"]);
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    let res = ctx.simple_query(sql).await?;
    assert_eq!(ids(&res)?, ["2", "3"]);
    let m = assert_cache_hit(&mut ctx, m).await?;

    // UTC: 10:00 admits only the 12:00 row. A second entry, never the first.
    ctx.simple_query("SET TimeZone = 'UTC'").await?;
    let res = ctx.simple_query(sql).await?;
    assert_eq!(ids(&res)?, ["3"]);
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    let res = ctx.simple_query(sql).await?;
    assert_eq!(ids(&res)?, ["3"]);
    let m = assert_cache_hit(&mut ctx, m).await?;

    // Back to Tokyo: its entry is still there.
    ctx.simple_query("SET TimeZone = 'Asia/Tokyo'").await?;
    let res = ctx.simple_query(sql).await?;
    assert_eq!(ids(&res)?, ["2", "3"]);
    let _ = assert_cache_hit(&mut ctx, m).await?;
    Ok(())
}

/// A literal with an explicit offset means one instant under any zone, so
/// sessions on different zones share the entry.
#[tokio::test]
async fn test_offset_timestamptz_literal_shares_entry_across_zones() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    instants_table_create(&mut ctx).await?;
    let sql = "select id from instants where at >= '2024-01-02 10:00+09' order by id";

    ctx.simple_query("SET TimeZone = 'Asia/Tokyo'").await?;
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(sql).await?;
    assert_eq!(ids(&res)?, ["2", "3"]);
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    ctx.simple_query("SET TimeZone = 'UTC'").await?;
    let res = ctx.simple_query(sql).await?;
    assert_eq!(ids(&res)?, ["2", "3"]);
    let _ = assert_cache_hit(&mut ctx, m).await?;
    Ok(())
}

/// `DateStyle`'s order half decides which field of `01/02/2024` is the month;
/// each order gets its own entry with the right row.
#[tokio::test]
async fn test_date_order_resolves_ambiguous_date_per_session() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query("create table days (id integer primary key, day date)", &[])
        .await?;
    ctx.query(
        "insert into days values (1, '2024-01-02'), (2, '2024-02-01')",
        &[],
    )
    .await?;
    ctx.cache_settle().await?;
    let sql = "select id from days where day = '01/02/2024'";

    ctx.simple_query("SET DateStyle = 'ISO, DMY'").await?;
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(sql).await?;
    assert_row_at(&res, 1, &[("id", "2")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    let res = ctx.simple_query(sql).await?;
    assert_row_at(&res, 1, &[("id", "2")])?;
    let m = assert_cache_hit(&mut ctx, m).await?;

    ctx.simple_query("SET DateStyle = 'ISO, MDY'").await?;
    let res = ctx.simple_query(sql).await?;
    assert_row_at(&res, 1, &[("id", "1")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    let res = ctx.simple_query(sql).await?;
    assert_row_at(&res, 1, &[("id", "1")])?;
    let _ = assert_cache_hit(&mut ctx, m).await?;
    Ok(())
}

/// A predicate canonicalised through the session zone is evaluated locally
/// under CDC: an inserted row past the bound is applied in place.
#[tokio::test]
async fn test_session_zone_predicate_applies_cdc_locally() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    instants_table_create(&mut ctx).await?;
    let sql = "select id from instants where at >= '2024-01-02 10:00' order by id";

    ctx.simple_query("SET TimeZone = 'Asia/Tokyo'").await?;
    let m = ctx.metrics().await?;
    let _ = ctx.simple_query(sql).await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    let _ = ctx.simple_query(sql).await?;
    let _ = assert_cache_hit(&mut ctx, m).await?;

    // 03:00 UTC: past the 01:00 UTC bound. 00:45 UTC: before it.
    let before = ctx.metrics().await?;
    ctx.origin_query(
        "insert into instants values (4, '2024-01-02 03:00:00+00'), \
         (5, '2024-01-02 00:45:00+00')",
        &[],
    )
    .await?;
    ctx.cdc_apply_settle().await?;
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(sql).await?;
    assert_eq!(ids(&res)?, ["2", "3", "4"]);
    let _ = assert_cache_hit(&mut ctx, m).await?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert!(
        delta.cache_cdc_local_eval_hits >= 1,
        "the canonical literal keeps the predicate local"
    );
    assert_eq!(delta.cache_cdc_pg_eval_hits, 0);
    Ok(())
}

/// A spelling the canonicaliser does not handle is populated under the
/// reference settings and served only to a session on them; a session on
/// another zone forwards until it returns to the reference.
#[tokio::test]
async fn test_uncanonicalisable_literal_serves_only_reference_sessions() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query("create table days (id integer primary key, day date)", &[])
        .await?;
    ctx.query("insert into days values (1, '2024-01-02')", &[])
        .await?;
    ctx.cache_settle().await?;
    let sql = "select id from days where day = 'Jan 2 2024'";

    let m = ctx.metrics().await?;
    let res = ctx.simple_query(sql).await?;
    assert_row_at(&res, 1, &[("id", "1")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    let res = ctx.simple_query(sql).await?;
    assert_row_at(&res, 1, &[("id", "1")])?;
    let _ = assert_cache_hit(&mut ctx, m).await?;

    ctx.simple_query("SET TimeZone = 'Asia/Tokyo'").await?;
    let before = ctx.metrics().await?;
    let res = ctx.simple_query(sql).await?;
    assert_row_at(&res, 1, &[("id", "1")])?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.cache_reference_only_forwards, 1);
    assert_eq!(delta.queries_cache_hit, 0);

    ctx.simple_query("RESET TimeZone").await?;
    let before = ctx.metrics().await?;
    let res = ctx.simple_query(sql).await?;
    assert_row_at(&res, 1, &[("id", "1")])?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.queries_cache_hit, 1, "served again on the reference");
    Ok(())
}
