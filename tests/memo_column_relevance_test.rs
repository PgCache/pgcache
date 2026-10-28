//! Column-aware memo eviction (PGC-247): an in-place UPDATE that changes no
//! column a memoized query reads leaves its result byte-identical, so the
//! entry keeps serving. Every change a query can observe — a select-list,
//! predicate or ORDER BY column, a star, a PK change, a join partner's ON
//! column — still evicts, and the re-served rows match origin.

use std::io::Error;
use std::time::Duration;

use crate::util::{TestContext, assert_proxy_rows_match_origin, metrics_delta};

mod util;

/// Issue `sql` until it is served from the memo (a `memo_hits` increment).
async fn warm_until_memoized(ctx: &mut TestContext, sql: &str) -> Result<(), Error> {
    let start = ctx.metrics().await?.cache_memo_hits;
    for _ in 0..80 {
        ctx.simple_query(sql).await?;
        if ctx.metrics().await?.cache_memo_hits > start {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Err(Error::other(format!("query never served from memo: {sql}")))
}

/// Apply `change` on origin and read `sql` once it is applied: the memo
/// entry survived (no eviction, a memo hit, at least one skip counted) and
/// the rows still match origin.
async fn assert_memo_kept(ctx: &mut TestContext, change: &str, sql: &str) -> Result<(), Error> {
    let before = ctx.metrics().await?;
    ctx.origin_query(change, &[]).await?;
    ctx.cdc_apply_settle().await?;
    assert_proxy_rows_match_origin(ctx, sql).await?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.cache_memo_evictions, 0, "memo evicted by {change}");
    assert!(
        delta.cache_memo_hits >= 1,
        "served from memo after {change}"
    );
    assert!(
        delta.cache_memo_evictions_skipped >= 1,
        "skip counted for {change}"
    );
    Ok(())
}

/// Apply `change` on origin and read `sql` once it is applied: the read is
/// not served from the memo (the entry is stale, or the query was
/// invalidated and the read forwarded), nothing was skipped, and the rows
/// match origin. Re-warms the memo for the next case.
async fn assert_memo_evicted(ctx: &mut TestContext, change: &str, sql: &str) -> Result<(), Error> {
    let before = ctx.metrics().await?;
    ctx.origin_query(change, &[]).await?;
    ctx.cdc_apply_settle().await?;
    assert_proxy_rows_match_origin(ctx, sql).await?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.cache_memo_hits, 0, "memo served across {change}");
    assert_eq!(
        delta.cache_memo_evictions_skipped, 0,
        "skip counted for {change}"
    );
    warm_until_memoized(ctx, sql).await
}

/// `docs(id, tenant, title, body, rank, touched_at)` with two tenants.
async fn docs_create(ctx: &mut TestContext) -> Result<(), Error> {
    for sql in [
        "CREATE TABLE docs (id int primary key, tenant int, title text, body text, \
         rank int, touched_at int)",
        "INSERT INTO docs VALUES (1, 1, 'a', 'body a', 2, 0), (2, 1, 'b', 'body b', 1, 0), \
         (3, 2, 'c', 'body c', 3, 0)",
    ] {
        ctx.query(sql, &[]).await?;
    }
    Ok(())
}

/// A single-table memo over `title` filtered on `tenant` and ordered by
/// `rank`: `body` and `touched_at` churn keeps it; `title` (select list),
/// `rank` (ORDER BY only), `tenant` (predicate) and the PK evict it.
#[tokio::test]
async fn test_memo_kept_unless_a_read_column_changes() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    docs_create(&mut ctx).await?;
    let sql = "SELECT id, title FROM docs WHERE tenant = 1 ORDER BY rank";
    warm_until_memoized(&mut ctx, sql).await?;

    assert_memo_kept(
        &mut ctx,
        "UPDATE docs SET body = 'edited', touched_at = touched_at + 1 WHERE id = 1",
        sql,
    )
    .await?;
    assert_memo_kept(
        &mut ctx,
        "UPDATE docs SET touched_at = touched_at + 1 WHERE tenant = 1",
        sql,
    )
    .await?;

    assert_memo_evicted(&mut ctx, "UPDATE docs SET title = 'A' WHERE id = 1", sql).await?;
    assert_memo_evicted(&mut ctx, "UPDATE docs SET rank = 0 WHERE id = 1", sql).await?;
    assert_memo_evicted(&mut ctx, "UPDATE docs SET tenant = 2 WHERE id = 2", sql).await?;
    assert_memo_evicted(&mut ctx, "UPDATE docs SET id = 20 WHERE id = 1", sql).await?;
    Ok(())
}

/// A star reads every column, so any in-place change evicts.
#[tokio::test]
async fn test_memo_star_evicts_on_any_column() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    docs_create(&mut ctx).await?;
    let sql = "SELECT * FROM docs WHERE tenant = 1 ORDER BY id";
    warm_until_memoized(&mut ctx, sql).await?;
    assert_memo_evicted(
        &mut ctx,
        "UPDATE docs SET touched_at = touched_at + 1 WHERE id = 1",
        sql,
    )
    .await
}

/// A join memo keeps across a partner's unread column and evicts on the
/// partner's ON column or a selected partner column.
#[tokio::test]
async fn test_memo_join_partner_columns() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    docs_create(&mut ctx).await?;
    for sql in [
        "CREATE TABLE authors (id int primary key, doc_id int, name text, bio text)",
        "INSERT INTO authors VALUES (10, 1, 'ann', 'x'), (11, 2, 'bob', 'y')",
    ] {
        ctx.query(sql, &[]).await?;
    }
    let sql = "SELECT d.id, a.name FROM docs d JOIN authors a ON a.doc_id = d.id \
               WHERE d.tenant = 1 ORDER BY d.id";
    warm_until_memoized(&mut ctx, sql).await?;

    assert_memo_kept(&mut ctx, "UPDATE authors SET bio = 'z' WHERE id = 10", sql).await?;
    assert_memo_kept(&mut ctx, "UPDATE docs SET body = 'z' WHERE id = 1", sql).await?;
    assert_memo_evicted(
        &mut ctx,
        "UPDATE authors SET name = 'ANN' WHERE id = 10",
        sql,
    )
    .await?;
    assert_memo_evicted(&mut ctx, "UPDATE authors SET doc_id = 3 WHERE id = 11", sql).await?;
    Ok(())
}

/// Under REPLICA IDENTITY FULL the old tuple in the message decides, with
/// the same outcomes.
#[tokio::test]
async fn test_memo_replica_identity_full() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    docs_create(&mut ctx).await?;
    ctx.origin_query("ALTER TABLE docs REPLICA IDENTITY FULL", &[])
        .await?;
    let sql = "SELECT id, title FROM docs WHERE tenant = 1 ORDER BY rank";
    warm_until_memoized(&mut ctx, sql).await?;

    assert_memo_kept(
        &mut ctx,
        "UPDATE docs SET body = 'edited' WHERE id = 1",
        sql,
    )
    .await?;
    assert_memo_evicted(&mut ctx, "UPDATE docs SET title = 'A' WHERE id = 1", sql).await?;
    assert_memo_evicted(&mut ctx, "UPDATE docs SET id = 20 WHERE id = 1", sql).await?;
    Ok(())
}
