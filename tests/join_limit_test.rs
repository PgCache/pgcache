//! A plain-projection join with a LIMIT populates each table from the join's
//! top N rows rather than from every row of the join (PGC-570, ADR-060).
use std::io::Error;

use crate::util::{
    TestContext, assert_cache_hit, assert_limit_order_decides, http_get, origin_change_invalidates,
    query_warm,
};

mod util;

const ROWS_B: i32 = 400;

/// `ja (id, k)` holds 1..=50 with `k = id % 5`; `jb (id, a_id, v)` holds
/// 1..=400 inserted in id order with `a_id = id % 50 + 1` and
/// `v = id * 37 % 400`, so a read that ignored `ORDER BY v` would load the
/// wrong rows.
async fn setup(ctx: &mut TestContext) -> Result<(), Error> {
    ctx.query("CREATE TABLE ja (id INTEGER PRIMARY KEY, k INTEGER)", &[])
        .await?;
    ctx.query(
        "CREATE TABLE jb (id INTEGER PRIMARY KEY, a_id INTEGER, v INTEGER)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO ja SELECT g, g % 5 FROM generate_series(1, 50) g",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO jb SELECT g, g % 50 + 1, g * 37 % 400 FROM generate_series(1, 400) g",
        &[],
    )
    .await?;
    ctx.cdc_decode_settle().await
}

const QUERY: &str = "SELECT b.id, b.v, a.k FROM jb b JOIN ja a ON a.id = b.a_id \
                     WHERE a.k = 1 ORDER BY b.v DESC LIMIT 3";

/// The `jb` ids of `QUERY`'s result, best first, from the setup's formulas.
fn top_ids() -> Vec<i32> {
    top_ids_where(|id| (id % 50 + 1) % 5 == 1)
}

/// The `jb` ids of a top-3 by `v DESC` over the rows `keep` admits.
fn top_ids_where(keep: impl Fn(i32) -> bool) -> Vec<i32> {
    let mut rows: Vec<(i32, i32)> = (1..=ROWS_B)
        .filter(|id| keep(*id))
        .map(|id| (id * 37 % 400, id))
        .collect();
    rows.sort_by(|a, b| b.cmp(a));
    rows.into_iter().take(3).map(|(_, id)| id).collect()
}

/// Rows the cache's populations loaded, summed over its registered queries.
async fn population_rows(ctx: &TestContext) -> Result<u64, Error> {
    let (_, body) = http_get(ctx.metrics_port, "/status").await?;
    let json: serde_json::Value = serde_json::from_str(&body)
        .map_err(|e| Error::other(format!("invalid status JSON: {e}")))?;
    let queries = json
        .get("queries")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| Error::other("queries missing from status"))?;
    Ok(queries
        .iter()
        .filter_map(|q| {
            q.get("population_row_count")
                .and_then(serde_json::Value::as_u64)
        })
        .sum())
}

/// The capped join loads three `jb` rows and their `ja` partners, not the
/// eighty rows the join has for `k = 1`.
#[tokio::test]
async fn test_capped_join_loads_only_the_top_rows() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup(&mut ctx).await?;
    assert_limit_order_decides(&ctx, QUERY).await?;

    query_warm(&mut ctx, QUERY).await?;
    let rows = population_rows(&ctx).await?;
    assert!((4..=6).contains(&rows), "population loaded {rows} rows");
    Ok(())
}

/// A row entering the window is growth for a join: the query is invalidated
/// and the repopulation holds the new row.
#[tokio::test]
async fn test_capped_join_insert_entering_the_window_repopulates() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup(&mut ctx).await?;

    let m = query_warm(&mut ctx, QUERY).await?;
    origin_change_invalidates(
        &mut ctx,
        m,
        "INSERT INTO jb (id, a_id, v) VALUES (1000, 1, 999)",
        QUERY,
    )
    .await
}

/// Demoting a cached row out of the window opens a gap only origin can fill.
#[tokio::test]
async fn test_capped_join_sort_key_demotion_invalidates() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup(&mut ctx).await?;

    let m = query_warm(&mut ctx, QUERY).await?;
    let top = top_ids()[0];
    origin_change_invalidates(
        &mut ctx,
        m,
        &format!("UPDATE jb SET v = 0 WHERE id = {top}"),
        QUERY,
    )
    .await
}

/// A promotion within the window invalidates a join, unlike one table: the
/// window can cut a row's fan-out, so the promoted row may need partners
/// the population never loaded. The direction check stays single-table.
#[tokio::test]
async fn test_capped_join_sort_key_promotion_invalidates() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup(&mut ctx).await?;

    let m = query_warm(&mut ctx, QUERY).await?;
    let third = top_ids()[2];
    origin_change_invalidates(
        &mut ctx,
        m,
        &format!("UPDATE jb SET v = 1000 WHERE id = {third}"),
        QUERY,
    )
    .await
}

/// A delete leaves the window short by one; the replacement was never loaded.
#[tokio::test]
async fn test_capped_join_delete_invalidates() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup(&mut ctx).await?;

    let m = query_warm(&mut ctx, QUERY).await?;
    let top = top_ids()[0];
    origin_change_invalidates(
        &mut ctx,
        m,
        &format!("DELETE FROM jb WHERE id = {top}"),
        QUERY,
    )
    .await
}

/// An outer join stays uncapped: the NULL-filled side is read widened to
/// every partner of every preserved row (PGC-556).
#[tokio::test]
async fn test_outer_join_populates_whole() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup(&mut ctx).await?;

    let query = "SELECT a.id, b.v FROM ja a LEFT JOIN jb b ON b.a_id = a.id \
                 WHERE a.k = 1 ORDER BY a.id, b.v LIMIT 3";
    query_warm(&mut ctx, query).await?;
    let rows = population_rows(&ctx).await?;
    assert!(
        rows > 6,
        "an outer join's population loaded only {rows} rows"
    );
    Ok(())
}

/// Two capped joins filter the same table on different partners. A DELETE
/// of a row the second holds invalidates the second alone: the recovered
/// image fails the first's filter, so the row never sat in its window.
#[tokio::test]
async fn test_capped_join_delete_spares_other_predicate() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup(&mut ctx).await?;

    let query_7 = "SELECT b.id, b.v, a.k FROM jb b JOIN ja a ON a.id = b.a_id \
                   WHERE b.a_id = 7 ORDER BY b.v DESC LIMIT 3";
    let query_8 = "SELECT b.id, b.v, a.k FROM jb b JOIN ja a ON a.id = b.a_id \
                   WHERE b.a_id = 8 ORDER BY b.v DESC LIMIT 3";
    query_warm(&mut ctx, query_7).await?;
    let m = query_warm(&mut ctx, query_8).await?;

    let top_8 = top_ids_where(|id| id % 50 + 1 == 8)[0];
    origin_change_invalidates(
        &mut ctx,
        m,
        &format!("DELETE FROM jb WHERE id = {top_8}"),
        query_8,
    )
    .await?;

    let m = ctx.metrics().await?;
    ctx.simple_query(query_7).await?;
    assert_cache_hit(&mut ctx, m).await?;
    Ok(())
}
