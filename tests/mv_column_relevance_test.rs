//! Column-aware MV dirty-marking (PGC-404): an in-place UPDATE that changes
//! no column a materialized query reads leaves its result as it was, so the
//! MV stays Fresh and keeps serving. A change to an aggregate input, a
//! predicate, a GROUP BY column, a join partner's ON column, or the PK still
//! dirties it, and the rebuilt result matches origin.

use std::io::Error;
use std::time::Duration;

use crate::util::{TestContext, assert_proxy_rows_match_origin, metrics_delta};

mod util;

/// Read `sql` until it is served from its MV, each read matching origin.
/// A dirtied MV is rebuilt on the hit that schedules it, after any
/// discard-backoff cooldown a quick succession of dirties has armed
/// (ADR-047), so the wait is bounded well past the first cooldowns.
async fn mv_hit_wait(ctx: &mut TestContext, sql: &str) -> Result<(), Error> {
    for _ in 0..120 {
        let before = ctx.metrics().await?;
        assert_proxy_rows_match_origin(ctx, sql).await?;
        if metrics_delta(&before, &ctx.metrics().await?).cache_mv_hits >= 1 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(Error::other(format!(
        "query never served from its MV: {sql}"
    )))
}

/// Warm `sql` into a Fresh MV: miss, settle, then reads until an MV hit.
async fn mv_warm(ctx: &mut TestContext, sql: &str) -> Result<(), Error> {
    ctx.simple_query(sql).await?;
    ctx.cache_settle().await?;
    mv_hit_wait(ctx, sql).await
}

/// Apply `change` on origin and read `sql` once it is applied: the MV stayed
/// Fresh (an MV hit, no rebuild, a skip counted) and rows match origin.
async fn assert_mv_kept(ctx: &mut TestContext, change: &str, sql: &str) -> Result<(), Error> {
    let before = ctx.metrics().await?;
    ctx.origin_query(change, &[]).await?;
    ctx.cdc_apply_settle().await?;
    assert_proxy_rows_match_origin(ctx, sql).await?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.cache_mv_hits, 1, "MV dirtied by {change}");
    assert_eq!(delta.cache_mv_rebuilds, 0, "MV rebuilt after {change}");
    assert!(
        delta.cache_mv_dirty_skipped >= 1,
        "skip counted for {change}"
    );
    Ok(())
}

/// Apply `change` on origin and read `sql` once it is applied: the MV was
/// dirtied (the read falls through and schedules a rebuild), nothing was
/// skipped, rows match origin, and after the rebuild the MV serves again.
async fn assert_mv_dirtied(ctx: &mut TestContext, change: &str, sql: &str) -> Result<(), Error> {
    let before = ctx.metrics().await?;
    ctx.origin_query(change, &[]).await?;
    ctx.cdc_apply_settle().await?;
    assert_proxy_rows_match_origin(ctx, sql).await?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.cache_mv_hits, 0, "MV still Fresh after {change}");
    assert_eq!(
        delta.cache_mv_fallthrough, 1,
        "no fallthrough after {change}"
    );
    assert_eq!(delta.cache_mv_dirty_skipped, 0, "skip counted for {change}");
    mv_hit_wait(ctx, sql).await
}

/// `sales(id, region, amount, note, seen)`: 60 rows over three regions, so
/// a per-region aggregate passes the Measure size gate.
async fn sales_create(ctx: &mut TestContext) -> Result<(), Error> {
    ctx.query(
        "CREATE TABLE sales (id int primary key, region text, amount int, note text, seen int)",
        &[],
    )
    .await?;
    let regions = ["east", "west", "north"].iter().cycle();
    for (i, region) in (0_i32..60).zip(regions) {
        ctx.query(
            "INSERT INTO sales VALUES ($1, $2, $3, 'n', 0)",
            &[&i, region, &i],
        )
        .await?;
    }
    Ok(())
}

/// `sum(amount) GROUP BY region`: `note` and `seen` churn keeps the MV;
/// `amount`, `region` and the PK dirty it.
#[tokio::test]
async fn test_mv_kept_unless_a_read_column_changes() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    sales_create(&mut ctx).await?;
    let sql = "SELECT region, sum(amount) FROM sales GROUP BY region ORDER BY region";
    mv_warm(&mut ctx, sql).await?;

    assert_mv_kept(
        &mut ctx,
        "UPDATE sales SET note = 'edited' WHERE id = 1",
        sql,
    )
    .await?;
    assert_mv_kept(&mut ctx, "UPDATE sales SET seen = seen + 1", sql).await?;
    assert_mv_dirtied(&mut ctx, "UPDATE sales SET amount = 1000 WHERE id = 1", sql).await?;
    assert_mv_dirtied(
        &mut ctx,
        "UPDATE sales SET region = 'west' WHERE id = 3",
        sql,
    )
    .await?;
    assert_mv_dirtied(&mut ctx, "UPDATE sales SET id = 600 WHERE id = 6", sql).await?;
    Ok(())
}

/// `count(*)` under a predicate reads only the predicate column: every other
/// in-place change keeps the MV, the predicate column dirties it.
#[tokio::test]
async fn test_mv_count_reads_only_its_predicate() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    sales_create(&mut ctx).await?;
    let sql = "SELECT count(*) FROM sales WHERE region = 'east'";
    mv_warm(&mut ctx, sql).await?;

    assert_mv_kept(
        &mut ctx,
        "UPDATE sales SET amount = amount + 1, note = 'x', seen = 9 WHERE id = 3",
        sql,
    )
    .await?;
    assert_mv_dirtied(
        &mut ctx,
        "UPDATE sales SET region = 'west' WHERE id = 3",
        sql,
    )
    .await?;
    Ok(())
}

/// A join aggregate keeps across a partner's unread column and dirties on
/// the partner's ON column.
#[tokio::test]
async fn test_mv_join_partner_columns() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    sales_create(&mut ctx).await?;
    for sql in [
        "CREATE TABLE regions (name text primary key, manager text, phone text)",
        "INSERT INTO regions VALUES ('east', 'ann', '1'), ('west', 'bob', '2'), \
         ('north', 'cy', '3')",
    ] {
        ctx.query(sql, &[]).await?;
    }
    let sql = "SELECT r.manager, sum(s.amount) FROM sales s JOIN regions r ON r.name = s.region \
               GROUP BY r.manager ORDER BY r.manager";
    mv_warm(&mut ctx, sql).await?;

    assert_mv_kept(
        &mut ctx,
        "UPDATE regions SET phone = '9' WHERE name = 'east'",
        sql,
    )
    .await?;
    assert_mv_kept(&mut ctx, "UPDATE sales SET note = 'z' WHERE id = 1", sql).await?;
    assert_mv_dirtied(
        &mut ctx,
        "UPDATE regions SET manager = 'ANN' WHERE name = 'east'",
        sql,
    )
    .await?;
    Ok(())
}
