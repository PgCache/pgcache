// SimpleQueryMessage is #[non_exhaustive], so a wildcard arm is unavoidable.
#![allow(clippy::wildcard_enum_match_arm)]

//! Integration tests for the materialized-results feature.
//!
//! Each test consolidates subtests that share the same `TestContext::setup()`,
//! per the project's integration-test pattern (see memory: "Testing Patterns").
//!
//! Flow note (unified MV build): MV first-build and rebuild share one
//! state machine and one writer handler. The coordinator flips
//! `Pending { has_table }` → `Scheduled { has_table }` on a cache hit and
//! sends `MvBuild`; the writer's `has_table` bit selects `CREATE TABLE AS`
//! (first build, runs Measure gate) or `TRUNCATE + INSERT` (rebuild, no gate).
//! The first hit after `state = Ready` falls through to source-row eval; the
//! *next* hit after the build completes gets the MV fast path. Tests reflect
//! this: one trigger hit + wait, then assert MV hit on the subsequent query.
//!
//! ORDER BY at serve time: emitted positionally (`ORDER BY 2 DESC`). The
//! classifier downgrades queries whose ORDER BY expressions aren't in the
//! SELECT list (e.g. `GROUP BY x ORDER BY sum(y)` where sum isn't selected)
//! to Skip, since the MV can't preserve sort columns it doesn't store.

use std::io::Error;

use crate::util::{TestContext, connect_cache_db, metrics_delta};

mod util;

/// Count tables in the `pgcache_mv` schema of the cache DB.
async fn mv_table_count(dbs: &crate::util::TempDBs) -> Result<i64, Error> {
    let client = connect_cache_db(dbs).await?;
    let row = client
        .query_one(
            "SELECT count(*) FROM pg_tables WHERE schemaname = 'pgcache_mv'",
            &[],
        )
        .await
        .map_err(Error::other)?;
    Ok(row.get(0))
}

/// End-to-end MV lifecycle for a bare `count(*)` query:
///   register → first-pop → MV fast-path hit → CDC invalidate → fallthrough
///   + rebuild → MV fast-path hit again.
///
/// Exercises the Measure shape (aggregate in SELECT list).
#[tokio::test]
async fn test_mv_count_lifecycle() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE mv_count (id integer primary key, val text)",
        &[],
    )
    .await?;
    for i in 0..20 {
        ctx.query(
            "INSERT INTO mv_count VALUES ($1, $2)",
            &[&i, &format!("v{i}")],
        )
        .await?;
    }

    // --- First query: cache miss, forwarded to origin. MV doesn't exist yet.
    let row = ctx.query_one("SELECT count(*) FROM mv_count", &[]).await?;
    assert_eq!(row.get::<_, i64>(0), 20);

    // Wait for source-row population. MV is still MeasurePending.
    ctx.cache_settle().await?;
    assert_eq!(
        mv_table_count(&ctx.dbs).await?,
        0,
        "MV not built until first cache hit triggers MvFirstPop"
    );

    // --- Second query: cache hit on MeasurePending → schedules first-pop and
    // falls through to source-row eval.
    let m1 = ctx.metrics().await?;
    let row = ctx.query_one("SELECT count(*) FROM mv_count", &[]).await?;
    assert_eq!(row.get::<_, i64>(0), 20);
    let m2 = ctx.metrics().await?;
    let d = metrics_delta(&m1, &m2);
    assert_eq!(d.queries_cache_hit, 1, "expected cache hit (fallthrough)");
    assert_eq!(d.cache_mv_hits, 0, "MeasurePending doesn't hit MV");
    assert_eq!(d.cache_mv_fallthrough, 1);

    // Wait for the writer to build the MV.
    ctx.cache_settle().await?;
    assert_eq!(
        mv_table_count(&ctx.dbs).await?,
        1,
        "MV table should be built after MvFirstPop completes"
    );

    // --- Third query: MV fast-path hit.
    let m2b = ctx.metrics().await?;
    let row = ctx.query_one("SELECT count(*) FROM mv_count", &[]).await?;
    assert_eq!(row.get::<_, i64>(0), 20);
    let m2c = ctx.metrics().await?;
    let d = metrics_delta(&m2b, &m2c);
    assert_eq!(d.cache_mv_hits, 1, "expected MV fast-path hit");
    assert_eq!(d.cache_mv_fallthrough, 0);

    // --- CDC INSERT: MV becomes Dirty. Next hit falls through + schedules rebuild.
    ctx.origin_query("INSERT INTO mv_count VALUES (100, 'new')", &[])
        .await?;
    ctx.cdc_settle().await?;

    let m3 = ctx.metrics().await?;
    let row = ctx.query_one("SELECT count(*) FROM mv_count", &[]).await?;
    assert_eq!(
        row.get::<_, i64>(0),
        21,
        "fallthrough serve must reflect new row"
    );
    let m4 = ctx.metrics().await?;
    let d = metrics_delta(&m3, &m4);
    assert_eq!(d.queries_cache_hit, 1, "expected cache hit (fallthrough)");
    assert_eq!(d.cache_mv_hits, 0, "MV should have been Dirty, not Fresh");
    assert_eq!(d.cache_mv_fallthrough, 1);

    // Rebuild runs on writer task; wait for it.
    ctx.cache_settle().await?;

    let m5 = ctx.metrics().await?;
    assert!(
        m5.cache_mv_rebuilds > m3.cache_mv_rebuilds,
        "expected at least one rebuild (got delta {})",
        m5.cache_mv_rebuilds - m3.cache_mv_rebuilds
    );

    // --- Third query: MV should be Fresh again.
    let row = ctx.query_one("SELECT count(*) FROM mv_count", &[]).await?;
    assert_eq!(row.get::<_, i64>(0), 21);
    let m6 = ctx.metrics().await?;
    let d = metrics_delta(&m5, &m6);
    assert_eq!(
        d.cache_mv_hits, 1,
        "expected MV hit after rebuild (got delta {:?})",
        d
    );

    Ok(())
}

/// `GROUP BY col` (no LIMIT) — Measure shape: source-row cache holds all rows,
/// MV stores one row per group. Verifies first-pop + serve hit the MV and that
/// results are correct.
#[tokio::test]
async fn test_mv_groupby_measure_hits() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE mv_gb (id serial primary key, category text not null)",
        &[],
    )
    .await?;
    // Size so the Measure gate passes: result_rows (6) × mv_size_ratio (10) ≤ source_rows.
    for (cat, n) in &[
        ("a", 100),
        ("b", 70),
        ("c", 50),
        ("d", 30),
        ("e", 20),
        ("f", 10),
    ] {
        for _ in 0..*n {
            ctx.query("INSERT INTO mv_gb (category) VALUES ($1)", &[cat])
                .await?;
        }
    }

    // First query — cache miss, source-row population runs.
    // (No outer ORDER BY — see "ORDER BY deparse limitation" note at the top
    // of this file for why we test unordered.)
    let res = ctx
        .simple_query("SELECT category, count(*) FROM mv_gb GROUP BY category")
        .await?;
    let data_rows: Vec<_> = res
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .collect();
    assert_eq!(data_rows.len(), 6, "first query should return 6 groups");

    ctx.cache_settle().await?;
    assert_eq!(
        mv_table_count(&ctx.dbs).await?,
        0,
        "MV not built until first cache hit triggers MvFirstPop"
    );

    // Second query — cache hit on MeasurePending → schedules MvFirstPop, falls through.
    let res = ctx
        .simple_query("SELECT category, count(*) FROM mv_gb GROUP BY category")
        .await?;
    let data_rows: Vec<_> = res
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .collect();
    assert_eq!(data_rows.len(), 6);

    ctx.cache_settle().await?;
    assert_eq!(
        mv_table_count(&ctx.dbs).await?,
        1,
        "Measure query should produce one MV table after MvFirstPop"
    );

    // Third query — MV fast-path hit.
    let m1 = ctx.metrics().await?;
    let res = ctx
        .simple_query("SELECT category, count(*) FROM mv_gb GROUP BY category")
        .await?;
    let data_rows: Vec<_> = res
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .collect();
    assert_eq!(data_rows.len(), 6);

    let m2 = ctx.metrics().await?;
    let d = metrics_delta(&m1, &m2);
    assert_eq!(d.cache_mv_hits, 1, "expected MV hit after first-pop");
    assert_eq!(d.cache_mv_fallthrough, 0);

    Ok(())
}

/// Plain filter/projection — shape classifier returns Skip, so no MV table
/// is ever created and cache hits never increment MV metrics beyond fallthrough.
#[tokio::test]
async fn test_mv_skip_shape_no_mv_table() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE mv_skip (id integer primary key, val text)",
        &[],
    )
    .await?;
    for i in 0..10 {
        ctx.query(
            "INSERT INTO mv_skip VALUES ($1, $2)",
            &[&i, &format!("v{i}")],
        )
        .await?;
    }

    // Register + hit a plain SELECT (Skip shape).
    let _ = ctx
        .query("SELECT id, val FROM mv_skip WHERE val = 'v1'", &[])
        .await?;
    ctx.cache_settle().await?;

    let _ = ctx
        .query("SELECT id, val FROM mv_skip WHERE val = 'v1'", &[])
        .await?;

    // No MV should ever have been created for a Skip-classified query.
    assert_eq!(
        mv_table_count(&ctx.dbs).await?,
        0,
        "Skip shape must not create an MV table"
    );

    let m = ctx.metrics().await?;
    assert_eq!(m.cache_mv_hits, 0, "Skip shape never hits MV fast path");
    // fallthrough may be >0 because every cache hit on a non-Fresh entry increments
    // (see design doc discussion of the diluted denominator).
    assert!(m.cache_mv_rebuilds == 0, "Skip shape never rebuilds");

    Ok(())
}

/// Measure shape with outer `ORDER BY count(*) DESC LIMIT N`. Verifies:
///   1. MV is built (classifier keeps the query as Measure because count(*)
///      is in the SELECT list).
///   2. Serve-time positional ORDER BY returns rows in the correct order.
///   3. A user LIMIT smaller than the registered one still returns the correct
///      top-M (LIMIT is excluded from the fingerprint, so it shares the MV).
///
/// Registering with an outer LIMIT is correct: reducer shapes (Measure /
/// Materialize) force `max_limit = None` at `query_resolve` time, so
/// source-row population isn't truncated.
#[tokio::test]
async fn test_mv_order_by_top_n() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE mv_topn (id serial primary key, category text not null)",
        &[],
    )
    .await?;
    for (cat, n) in &[
        ("a", 100),
        ("b", 70),
        ("c", 50),
        ("d", 30),
        ("e", 20),
        ("f", 10),
    ] {
        for _ in 0..*n {
            ctx.query("INSERT INTO mv_topn (category) VALUES ($1)", &[cat])
                .await?;
        }
    }

    let sql = "SELECT category, count(*) FROM mv_topn GROUP BY category \
               ORDER BY count(*) DESC LIMIT 3";

    // First query — cache miss, population runs.
    let rows = ctx.simple_query(sql).await?;
    let data_rows: Vec<(String, i64)> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some((
                r.get(0).unwrap().to_owned(),
                r.get(1).unwrap().parse().unwrap(),
            )),
            _ => None,
        })
        .collect();
    assert_eq!(
        data_rows,
        vec![
            ("a".to_owned(), 100),
            ("b".to_owned(), 70),
            ("c".to_owned(), 50)
        ],
        "first query (origin): top 3 groups by count DESC"
    );

    ctx.cache_settle().await?;

    // Second query — triggers MvFirstPop, falls through to source-row eval.
    let _ = ctx.simple_query(sql).await?;
    ctx.cache_settle().await?;

    // Third query — MV fast path.
    let m1 = ctx.metrics().await?;
    let rows = ctx.simple_query(sql).await?;
    let data_rows: Vec<(String, i64)> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some((
                r.get(0).unwrap().to_owned(),
                r.get(1).unwrap().parse().unwrap(),
            )),
            _ => None,
        })
        .collect();
    assert_eq!(
        data_rows,
        vec![
            ("a".to_owned(), 100),
            ("b".to_owned(), 70),
            ("c".to_owned(), 50)
        ],
        "MV fast path must return same top-3 in same order"
    );
    let m2 = ctx.metrics().await?;
    let d = metrics_delta(&m1, &m2);
    assert_eq!(d.cache_mv_hits, 1, "expected MV hit");
    assert_eq!(d.cache_mv_fallthrough, 0);

    // User LIMIT smaller than the registered LIMIT — top-2 subset.
    let sql_small = "SELECT category, count(*) FROM mv_topn GROUP BY category \
                     ORDER BY count(*) DESC LIMIT 2";
    let rows = ctx.simple_query(sql_small).await?;
    let data_rows: Vec<(String, i64)> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some((
                r.get(0).unwrap().to_owned(),
                r.get(1).unwrap().parse().unwrap(),
            )),
            _ => None,
        })
        .collect();
    assert_eq!(
        data_rows,
        vec![("a".to_owned(), 100), ("b".to_owned(), 70)],
        "user LIMIT 2 must return global top-2, not an arbitrary subset"
    );

    Ok(())
}

/// Regression for PGC-98: `ORDER BY <select_alias>` must register successfully
/// and serve from cache (and from the MV). Pre-fix, the resolver rejected the
/// alias reference as `ColumnNotFound`, leaving the per-fingerprint state stuck
/// in `Loading` and hanging every subsequent client.
#[tokio::test]
async fn test_mv_order_by_alias() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE mv_alias (id serial primary key, category text not null)",
        &[],
    )
    .await?;
    for (cat, n) in &[("a", 50), ("b", 30), ("c", 10)] {
        for _ in 0..*n {
            ctx.query("INSERT INTO mv_alias (category) VALUES ($1)", &[cat])
                .await?;
        }
    }

    // Alias referenced in ORDER BY — the case that used to fail resolve.
    let sql = "SELECT category, count(*) AS total FROM mv_alias \
               GROUP BY category ORDER BY total DESC";

    // First query — cache miss, forwarded to origin.
    let rows = ctx.simple_query(sql).await?;
    let data_rows: Vec<(String, i64)> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some((
                r.get(0).unwrap().to_owned(),
                r.get(1).unwrap().parse().unwrap(),
            )),
            _ => None,
        })
        .collect();
    assert_eq!(
        data_rows,
        vec![
            ("a".to_owned(), 50),
            ("b".to_owned(), 30),
            ("c".to_owned(), 10)
        ],
        "first query (origin): ordered by alias DESC"
    );

    ctx.cache_settle().await?;

    // Second query — triggers MvFirstPop, falls through to source-row eval.
    let _ = ctx.simple_query(sql).await?;
    ctx.cache_settle().await?;

    // Third query — MV fast path must honor ORDER BY through positional rewrite.
    let m1 = ctx.metrics().await?;
    let rows = ctx.simple_query(sql).await?;
    let data_rows: Vec<(String, i64)> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some((
                r.get(0).unwrap().to_owned(),
                r.get(1).unwrap().parse().unwrap(),
            )),
            _ => None,
        })
        .collect();
    assert_eq!(
        data_rows,
        vec![
            ("a".to_owned(), 50),
            ("b".to_owned(), 30),
            ("c".to_owned(), 10)
        ],
        "MV fast path must preserve alias-ordered results"
    );
    let m2 = ctx.metrics().await?;
    let d = metrics_delta(&m1, &m2);
    assert_eq!(
        d.cache_mv_hits, 1,
        "expected MV hit for alias-ordered query"
    );
    assert_eq!(d.cache_mv_fallthrough, 0);

    Ok(())
}

/// Regression: plain aggregate + LIMIT served from the source-row cache (no
/// MV involved) must return the correct count. Previously, `max_limit` was
/// applied to origin population even for aggregate shapes, truncating the
/// source input and returning a wildly wrong count on cache hit.
#[tokio::test]
async fn test_aggregate_limit_source_row_cache_correctness() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE agg_limit (id integer primary key, v text)",
        &[],
    )
    .await?;
    for i in 0..50 {
        ctx.query(
            "INSERT INTO agg_limit VALUES ($1, $2)",
            &[&i, &format!("v{i}")],
        )
        .await?;
    }

    // Register the query with an outer LIMIT that's a no-op on the aggregate
    // result (count(*) is always 1 row) but would historically have truncated
    // the source-row cache to 3 rows.
    let sql = "SELECT count(*) FROM agg_limit LIMIT 3";

    let row = ctx.query_one(sql, &[]).await?;
    assert_eq!(
        row.get::<_, i64>(0),
        50,
        "first query (origin): count is 50"
    );

    ctx.cache_settle().await?;

    // Second query — cache hit, re-evaluated against the source-row cache.
    // Pre-fix this returned 3 (the truncated source-row count) because the
    // cache only held 3 rows of agg_limit.
    let row = ctx.query_one(sql, &[]).await?;
    assert_eq!(
        row.get::<_, i64>(0),
        50,
        "cache hit must not truncate source rows for aggregate queries"
    );

    Ok(())
}

/// Measure shape whose ORDER BY expression is NOT in the SELECT list — the
/// classifier must downgrade to Skip, so no MV table is ever created.
#[tokio::test]
async fn test_mv_order_by_not_in_select_list_skips() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE mv_no_mv (id serial primary key, category text not null, value integer not null)",
        &[],
    )
    .await?;
    for (cat, v) in &[("a", 1), ("a", 2), ("b", 3), ("b", 4), ("c", 5)] {
        ctx.query(
            "INSERT INTO mv_no_mv (category, value) VALUES ($1, $2)",
            &[cat, v],
        )
        .await?;
    }

    // count(*) is in SELECT, sum(value) is NOT — classifier downgrades to Skip.
    let sql = "SELECT category, count(*) FROM mv_no_mv GROUP BY category \
               ORDER BY sum(value) DESC";

    let _ = ctx.simple_query(sql).await?;
    ctx.cache_settle().await?;
    let _ = ctx.simple_query(sql).await?;
    ctx.cache_settle().await?;

    assert_eq!(
        mv_table_count(&ctx.dbs).await?,
        0,
        "ORDER BY with expression not in SELECT list must downgrade to Skip"
    );

    Ok(())
}

/// Set operation MV lifecycle. Exercises:
///   - `UNION` (dedup): MV stores deduplicated result, serves correctly.
///   - `INTERSECT`: MV stores the overlap, serves correctly.
///   - `UNION ALL`: classifier downgrades to Skip, no MV created.
///
/// Uses a small-domain `tag` column so UNION/INTERSECT on `tag` gives enough
/// reduction to pass the default Measure size gate
/// (`result × mv_size_ratio ≤ source_rows`, default ratio 10).
#[tokio::test]
async fn test_mv_setop_lifecycle() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE mv_setop_a (id serial primary key, tag text not null)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE TABLE mv_setop_b (id serial primary key, tag text not null)",
        &[],
    )
    .await?;

    // a tags: {x, y, z}, b tags: {y, z, w}. Sized so the size gate passes.
    //   a: 300 rows total (100 of each tag)
    //   b: 300 rows total (100 of each tag)
    //   UNION distinct tags: {x, y, z, w} = 4 rows (gate 40 ≤ 600 ✓)
    //   INTERSECT tags:     {y, z} = 2 rows         (gate 20 ≤ 600 ✓)
    for tag in &["x", "y", "z"] {
        for _ in 0..100 {
            ctx.query("INSERT INTO mv_setop_a (tag) VALUES ($1)", &[tag])
                .await?;
        }
    }
    for tag in &["y", "z", "w"] {
        for _ in 0..100 {
            ctx.query("INSERT INTO mv_setop_b (tag) VALUES ($1)", &[tag])
                .await?;
        }
    }

    // ---- UNION (dedup) — Measure candidate.
    let union_sql = "SELECT tag FROM mv_setop_a UNION SELECT tag FROM mv_setop_b";

    let rows = ctx.simple_query(union_sql).await?;
    let n = rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(n, 4, "UNION (dedup) from origin: 4 distinct tags");

    ctx.cache_settle().await?;
    // Second query — triggers MvFirstPop, falls through.
    let _ = ctx.simple_query(union_sql).await?;
    ctx.cache_settle().await?;
    assert_eq!(
        mv_table_count(&ctx.dbs).await?,
        1,
        "UNION (dedup) must produce an MV table"
    );

    // Third query — MV fast path.
    let m1 = ctx.metrics().await?;
    let rows = ctx.simple_query(union_sql).await?;
    let n = rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(n, 4, "MV fast path: 4 deduped tags");
    let m2 = ctx.metrics().await?;
    let d = metrics_delta(&m1, &m2);
    assert_eq!(d.cache_mv_hits, 1, "expected UNION MV hit");

    // ---- INTERSECT — Measure candidate.
    let intersect_sql = "SELECT tag FROM mv_setop_a INTERSECT SELECT tag FROM mv_setop_b";

    let rows = ctx.simple_query(intersect_sql).await?;
    let n = rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(n, 2, "INTERSECT from origin: 2 overlapping tags (y, z)");

    ctx.cache_settle().await?;
    let _ = ctx.simple_query(intersect_sql).await?;
    ctx.cache_settle().await?;
    assert_eq!(
        mv_table_count(&ctx.dbs).await?,
        2,
        "INTERSECT must produce its own MV table (distinct fingerprint from UNION)"
    );

    let m3 = ctx.metrics().await?;
    let rows = ctx.simple_query(intersect_sql).await?;
    let n = rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(n, 2);
    let m4 = ctx.metrics().await?;
    let d = metrics_delta(&m3, &m4);
    assert_eq!(d.cache_mv_hits, 1, "expected INTERSECT MV hit");

    // ---- UNION ALL — must NOT create an MV (classifier Skip).
    let union_all_sql = "SELECT tag FROM mv_setop_a UNION ALL SELECT tag FROM mv_setop_b";
    let _ = ctx.simple_query(union_all_sql).await?;
    ctx.cache_settle().await?;
    let _ = ctx.simple_query(union_all_sql).await?;
    ctx.cache_settle().await?;
    assert_eq!(
        mv_table_count(&ctx.dbs).await?,
        2,
        "UNION ALL must not create an MV (stays at 2 from UNION + INTERSECT)"
    );

    Ok(())
}

/// PGC-136: a query whose output columns derive the same name (two
/// unaliased `count(*) FILTER (...)`) must still build an MV. The MV's
/// physical columns are positional; serve aliases them back, so the
/// client sees two columns both named `count`. Before the fix the
/// `CREATE TABLE AS` failed (`column "count" specified more than once`),
/// the MV never built, and the failure was only logged.
#[tokio::test]
async fn test_mv_duplicate_derived_column_names() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE pgc136 (id integer primary key, val text)",
        &[],
    )
    .await?;
    for i in 0..10 {
        ctx.query("INSERT INTO pgc136 VALUES ($1, 'q')", &[&i])
            .await?;
    }
    for i in 10..13 {
        ctx.query("INSERT INTO pgc136 VALUES ($1, 'a')", &[&i])
            .await?;
    }

    let sql = "SELECT count(*) FILTER (WHERE val = 'q'), \
               count(*) FILTER (WHERE val = 'a') FROM pgc136";

    // Miss → forward; settle source-row population.
    let row = ctx.query_one(sql, &[]).await?;
    assert_eq!(row.get::<_, i64>(0), 10);
    assert_eq!(row.get::<_, i64>(1), 3);
    ctx.cache_settle().await?;

    // Hit on MeasurePending → schedules first-pop, falls through.
    let row = ctx.query_one(sql, &[]).await?;
    assert_eq!(row.get::<_, i64>(0), 10);
    ctx.cache_settle().await?;

    // The regression assertion: the MV must have been built despite the
    // duplicate derived name.
    assert_eq!(
        mv_table_count(&ctx.dbs).await?,
        1,
        "MV must build despite duplicate derived column names (PGC-136)"
    );

    // MV fast-path hit: correct values and both columns named `count`.
    let m1 = ctx.metrics().await?;
    let row = ctx.query_one(sql, &[]).await?;
    let m2 = ctx.metrics().await?;
    assert_eq!(metrics_delta(&m1, &m2).cache_mv_hits, 1, "expected MV hit");
    assert_eq!(row.get::<_, i64>(0), 10);
    assert_eq!(row.get::<_, i64>(1), 3);
    let names: Vec<&str> = row.columns().iter().map(|c| c.name()).collect();
    assert_eq!(
        names,
        ["count", "count"],
        "duplicate output names preserved"
    );

    Ok(())
}

/// PGC-188: the demo's tag-page shape — `post_tags ⋈ posts` filtered to one
/// tag, `ORDER BY score DESC LIMIT N`. With a popular tag, the unlimited join
/// is large but the MV is LIMIT-capped, so `result_rows × mv_size_ratio (10)
/// ≤ source_rows` becomes `min(full_count, 10) × 10 ≤ source` → trivially
/// satisfied for a realistic tag page. Pre-fix the gate counted the full
/// unlimited join (limit was nulled for all reducer shapes), so the gate
/// failed and no MV was ever built.
#[tokio::test]
async fn test_mv_join_lifecycle() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE posts (id integer primary key, title text not null, score integer not null)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE TABLE post_tags (id serial primary key, post_id integer not null, tag text not null)",
        &[],
    )
    .await?;

    // 300 posts; 300 post_tags. 'rust' is the popular tag — 200 posts (ids
    // 100..299, score = id). With LIMIT 10 the MV stores top-10 rows; the
    // gate sees min(200, 10) × 10 = 100 ≤ source-row cache (≈400) → passes.
    // Pre-fix it saw 200 × 10 = 2000 ≤ 400 → failed.
    for id in 0..300 {
        ctx.query(
            "INSERT INTO posts VALUES ($1, $2, $3)",
            &[&id, &format!("post {id}"), &id],
        )
        .await?;
        let tag = if id >= 100 { "rust" } else { "other" };
        ctx.query(
            "INSERT INTO post_tags (post_id, tag) VALUES ($1, $2)",
            &[&id, &tag],
        )
        .await?;
    }

    let sql = "SELECT p.id, p.title, p.score FROM post_tags pt \
               JOIN posts p ON pt.post_id = p.id \
               WHERE pt.tag = 'rust' ORDER BY p.score DESC LIMIT 10";

    let expected_top10: Vec<i32> = (290..300).rev().collect();

    fn ids(res: &[tokio_postgres::SimpleQueryMessage]) -> Vec<i32> {
        res.iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(r) => {
                    Some(r.get(0).unwrap().parse().unwrap())
                }
                _ => None,
            })
            .collect()
    }

    // First query — cache miss, forwarded to origin; source-row population runs.
    let res = ctx.simple_query(sql).await?;
    assert_eq!(ids(&res), expected_top10, "origin: top-10 by score DESC");

    ctx.cache_settle().await?;
    assert_eq!(
        mv_table_count(&ctx.dbs).await?,
        0,
        "MV not built until first cache hit triggers MvFirstPop"
    );

    // Second query — cache hit on Pending → schedules first-pop, falls through.
    let res = ctx.simple_query(sql).await?;
    assert_eq!(ids(&res), expected_top10);

    ctx.cache_settle().await?;
    assert_eq!(
        mv_table_count(&ctx.dbs).await?,
        1,
        "LIMIT-capped join must produce an MV table after MvFirstPop"
    );

    // Third query — MV fast-path hit.
    let m1 = ctx.metrics().await?;
    let res = ctx.simple_query(sql).await?;
    assert_eq!(
        ids(&res),
        expected_top10,
        "MV fast path must return the same top-10"
    );
    let m2 = ctx.metrics().await?;
    let d = metrics_delta(&m1, &m2);
    assert_eq!(d.cache_mv_hits, 1, "expected join MV fast-path hit");
    assert_eq!(d.cache_mv_fallthrough, 0);

    // A smaller user LIMIT (3) over the same MV — top-3 subset, served from MV.
    let sql_small = "SELECT p.id, p.title, p.score FROM post_tags pt \
                     JOIN posts p ON pt.post_id = p.id \
                     WHERE pt.tag = 'rust' ORDER BY p.score DESC LIMIT 3";
    let m3 = ctx.metrics().await?;
    let res = ctx.simple_query(sql_small).await?;
    assert_eq!(ids(&res), vec![299, 298, 297]);
    let m4 = ctx.metrics().await?;
    assert_eq!(
        metrics_delta(&m3, &m4).cache_mv_hits,
        1,
        "smaller LIMIT must be served from the same MV"
    );

    Ok(())
}

/// PGC-138: a relation carrying a Fresh-MV query *and* a separate
/// non-MV cached query. The CDC short-circuit must still (a) dirty-mark
/// the Fresh MV when a row matches it and (b) upsert the shared cache
/// table for the non-MV query — skipping neither.
#[tokio::test]
async fn test_cdc_short_circuit_preserves_fresh_and_upsert() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(
        "CREATE TABLE pgc138 (id integer primary key, grp integer not null)",
        &[],
    )
    .await?;
    // >=10 grp=1 rows so the count(*) MV passes the size gate.
    for id in 1..=12 {
        ctx.query("INSERT INTO pgc138 VALUES ($1, 1)", &[&id])
            .await?;
    }
    for id in 13..=15 {
        ctx.query("INSERT INTO pgc138 VALUES ($1, 2)", &[&id])
            .await?;
    }

    let agg = "SELECT count(*) FROM pgc138 WHERE grp = 1";
    let rows = "SELECT id FROM pgc138 WHERE grp = 1 ORDER BY id";

    async fn rows_count(ctx: &mut TestContext, sql: &str) -> Result<usize, Error> {
        let r = ctx.simple_query(sql).await?;
        Ok(r.iter()
            .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
            .count())
    }

    // Prime both queries; materialize the aggregate MV.
    assert_eq!(ctx.query_one(agg, &[]).await?.get::<_, i64>(0), 12);
    ctx.simple_query(rows).await?;
    ctx.cache_settle().await?;
    ctx.query_one(agg, &[]).await?; // first-pop trigger
    ctx.cache_settle().await?;
    assert!(
        mv_table_count(&ctx.dbs).await? >= 1,
        "aggregate MV should be built"
    );

    // CDC INSERT a grp=1 row — matches the Fresh agg AND the rows query.
    ctx.origin_query("INSERT INTO pgc138 VALUES (16, 1)", &[])
        .await?;
    ctx.cdc_settle().await?;
    ctx.cache_settle().await?;
    assert_eq!(
        ctx.query_one(agg, &[]).await?.get::<_, i64>(0),
        13,
        "Fresh MV must reflect the CDC row (was dirty-marked)"
    );
    assert_eq!(
        rows_count(&mut ctx, rows).await?,
        13,
        "non-MV query must reflect the CDC row (shared table upserted)"
    );

    // CDC INSERT a grp=2 row — matches neither WHERE grp=1 query.
    ctx.origin_query("INSERT INTO pgc138 VALUES (17, 2)", &[])
        .await?;
    ctx.cdc_settle().await?;
    ctx.cache_settle().await?;
    assert_eq!(
        ctx.query_one(agg, &[]).await?.get::<_, i64>(0),
        13,
        "grp=2 row must not affect the grp=1 aggregate"
    );
    assert_eq!(
        rows_count(&mut ctx, rows).await?,
        13,
        "grp=2 row must not affect the grp=1 rows query"
    );

    Ok(())
}

/// A `Gated` query that becomes Ready via *subsumption* (not its own
/// population) must still materialize. Subsumed queries don't populate, so they
/// have no population row-count; the gate has to fall back to counting the cache
/// or it would reject every subsumed MV candidate. Regression for the
/// subsumption blind spot in the population-row-count gate (PGC-330).
#[tokio::test]
async fn test_subsumed_query_still_materializes() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE sub_mv (id integer primary key, owner integer not null)",
        &[],
    )
    .await?;
    for id in 1..=100 {
        ctx.query("INSERT INTO sub_mv VALUES ($1, 8)", &[&id])
            .await?;
    }
    ctx.cdc_settle().await?;

    // Broad single-table query caches all of sub_mv (Ready, non-limited) — this
    // makes the narrower aggregate below a subsumption candidate.
    let _ = ctx.query_one("SELECT count(*) FROM sub_mv", &[]).await?;
    ctx.cache_settle().await?;

    // Narrower Gated aggregate over the same relation → subsumed, so it has no
    // population row-count. First hit schedules the MV build; the gate must fall
    // back to a cache count (100) and admit (1 × ratio ≤ 100).
    let narrow = "SELECT count(*) FROM sub_mv WHERE owner = 8";
    let row = ctx.query_one(narrow, &[]).await?;
    assert_eq!(row.get::<_, i64>(0), 100);
    ctx.cache_settle().await?;

    // Next hit must be served from the MV — proving it built despite subsumption.
    let m_before = ctx.metrics().await?;
    let row = ctx.query_one(narrow, &[]).await?;
    assert_eq!(row.get::<_, i64>(0), 100);
    let m_after = ctx.metrics().await?;
    let d = metrics_delta(&m_before, &m_after);
    assert!(
        d.cache_mv_hits >= 1,
        "subsumed Gated query must still materialize via the cache-count fallback (delta {d:?})"
    );

    Ok(())
}
