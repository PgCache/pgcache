//! Regression tests for PGC-266: tables with origin-only column types
//! (enums, domains) must cache normally — pre-fix, every CDC Relation
//! message read as a schema change (the decoder's TEXT fallback vs the
//! registration catalog read), recreating the cache table and evicting
//! every query. Also covers the enum order-dependence admission gate:
//! enums are stored as text in the cache, so order-dependent enum usage
//! must forward rather than serve text-ordered results.

use std::io::Error;

use tokio_postgres::{SimpleQueryMessage, SimpleQueryRow};

use crate::util::{TestContext, assert_cache_hit, assert_cache_miss};

mod util;

fn rows_of(messages: &[SimpleQueryMessage]) -> Vec<&SimpleQueryRow> {
    messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r),
            SimpleQueryMessage::CommandComplete(_) | SimpleQueryMessage::RowDescription(_) | _ => {
                None
            }
        })
        .collect()
}

/// Equality query over an enum-columned table stays cached through CDC
/// insert, update, and delete. Pre-fix, each origin write's Relation
/// message invalidated the query, so every post-write read missed.
#[tokio::test]
async fn test_enum_table_stays_cached_through_cdc() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.origin_query("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')", &[])
        .await?;
    ctx.simple_query("CREATE TABLE moods (id int primary key, m mood, note text)")
        .await?;
    ctx.simple_query(
        "INSERT INTO moods (id, m, note) VALUES \
         (1, 'ok', 'a'), (2, 'happy', 'b'), (3, 'ok', 'c'), (4, 'sad', 'd')",
    )
    .await?;
    ctx.cdc_settle().await?;

    let q = "SELECT id, note FROM moods WHERE m = 'ok' ORDER BY id";

    let m = ctx.metrics().await?;
    let res = ctx.simple_query(q).await?;
    let rows = rows_of(&res);
    assert_eq!(rows.len(), 2);
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    let res = ctx.simple_query(q).await?;
    let rows = rows_of(&res);
    assert_eq!(rows.len(), 2);
    let m = assert_cache_hit(&mut ctx, m).await?;

    // CDC INSERT: a matching row enters the cached result; the Relation
    // message preceding it must not invalidate the query.
    ctx.origin_query("INSERT INTO moods (id, m, note) VALUES (5, 'ok', 'e')", &[])
        .await?;
    ctx.cdc_settle().await?;
    let res = ctx.simple_query(q).await?;
    let rows = rows_of(&res);
    assert_eq!(rows.len(), 3, "CDC INSERT landed in cache");
    let m = assert_cache_hit(&mut ctx, m).await?;

    // CDC UPDATE: one row leaves the result set, another enters.
    ctx.origin_query("UPDATE moods SET m = 'happy' WHERE id = 1", &[])
        .await?;
    ctx.origin_query("UPDATE moods SET m = 'ok' WHERE id = 4", &[])
        .await?;
    ctx.cdc_settle().await?;
    let res = ctx.simple_query(q).await?;
    let rows = rows_of(&res);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get("id"), Some("3"));
    assert_eq!(rows[1].get("id"), Some("4"));
    assert_eq!(rows[2].get("id"), Some("5"));
    let m = assert_cache_hit(&mut ctx, m).await?;

    // CDC DELETE.
    ctx.origin_query("DELETE FROM moods WHERE id = 3", &[])
        .await?;
    ctx.cdc_settle().await?;
    let res = ctx.simple_query(q).await?;
    let rows = rows_of(&res);
    assert_eq!(rows.len(), 2, "CDC DELETE removed the row");
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Order-dependent enum usage is gated from admission (text storage does
/// not preserve enum order), while equality queries on the same table
/// still cache.
#[tokio::test]
async fn test_enum_order_dependent_queries_forward() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.origin_query(
        "CREATE TYPE severity AS ENUM ('low', 'medium', 'high', 'critical')",
        &[],
    )
    .await?;
    ctx.simple_query("CREATE TABLE alerts (id int primary key, sev severity)")
        .await?;
    // 'high' < 'low' < 'medium' lexicographically but medium < high in enum
    // order — text-ordered serving would visibly diverge.
    ctx.simple_query(
        "INSERT INTO alerts (id, sev) VALUES (1, 'low'), (2, 'medium'), (3, 'high'), (4, 'critical')",
    )
    .await?;
    ctx.cdc_settle().await?;

    // Order-dependent shapes: repeatedly executed past any admission
    // threshold, with settles between — they must never serve from cache.
    for q in [
        "SELECT id FROM alerts ORDER BY sev",
        "SELECT id FROM alerts WHERE sev > 'medium' ORDER BY id",
        "SELECT max(sev) FROM alerts",
    ] {
        for _ in 0..3 {
            let m = ctx.metrics().await?;
            let proxy_res = ctx.simple_query(q).await?;
            let origin_res = ctx.origin.simple_query(q).await.map_err(Error::other)?;
            let via_proxy = rows_of(&proxy_res);
            let via_origin = rows_of(&origin_res);
            let proxy_vals: Vec<_> = via_proxy.iter().map(|r| r.get(0)).collect();
            let origin_vals: Vec<_> = via_origin.iter().map(|r| r.get(0)).collect();
            assert_eq!(
                proxy_vals, origin_vals,
                "forwarded result matches origin: {q}"
            );
            let _m = assert_cache_miss(&mut ctx, m).await?;
            ctx.cache_settle().await?;
        }
    }

    // Equality on the same table still caches — the gate is per-query.
    let eq = "SELECT id FROM alerts WHERE sev = 'high'";
    let m = ctx.metrics().await?;
    ctx.simple_query(eq).await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    let res = ctx.simple_query(eq).await?;
    let rows = rows_of(&res);
    assert_eq!(rows.len(), 1);
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}
