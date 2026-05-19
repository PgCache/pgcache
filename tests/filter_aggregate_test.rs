//! Integration test for `count(*) FILTER (WHERE ...)` aggregates: each
//! FILTER predicate must survive AST round-trip so distinct columns return
//! distinct counts.

use std::io::Error;

use crate::util::{TestContext, metrics_delta};

mod util;

/// Two FILTER predicates on the same table must produce distinct counts
/// through pgcache; verifies the result both before and after a CDC rebuild.
#[tokio::test]
async fn test_filter_aggregate_two_predicates() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE posts (\
            id integer primary key,\
            owneruserid integer not null,\
            posttypeid integer not null\
         )",
        &[],
    )
    .await?;

    // owneruserid=8 has 10 questions and 1 answer; user 99 has 9 noise rows
    // so the WHERE clause actually narrows.
    for id in 1..=10 {
        ctx.query(
            "INSERT INTO posts (id, owneruserid, posttypeid) VALUES ($1, 8, 1)",
            &[&id],
        )
        .await?;
    }
    ctx.query(
        "INSERT INTO posts (id, owneruserid, posttypeid) VALUES (11, 8, 2)",
        &[],
    )
    .await?;
    for id in 12..=20 {
        ctx.query(
            "INSERT INTO posts (id, owneruserid, posttypeid) VALUES ($1, 99, 1)",
            &[&id],
        )
        .await?;
    }

    let sql = "SELECT count(*) FILTER (WHERE posttypeid = 1) AS questions, \
               count(*) FILTER (WHERE posttypeid = 2) AS answers \
               FROM posts WHERE owneruserid = 8";

    // Ground truth from origin.
    let origin = ctx.origin.query_one(sql, &[]).await.map_err(Error::other)?;
    assert_eq!(origin.get::<_, i64>("questions"), 10);
    assert_eq!(origin.get::<_, i64>("answers"), 1);

    // First pgcache query — miss; forwards to origin while population runs.
    let row = ctx.query_one(sql, &[]).await?;
    assert_eq!(row.get::<_, i64>("questions"), 10);
    assert_eq!(row.get::<_, i64>("answers"), 1);

    ctx.cache_settle().await?;

    // Second query — cache hit via source-row eval.
    let m1 = ctx.metrics().await?;
    let row = ctx.query_one(sql, &[]).await?;
    assert_eq!(
        row.get::<_, i64>("questions"),
        10,
        "FILTER predicate (posttypeid=1) must survive AST round-trip"
    );
    assert_eq!(
        row.get::<_, i64>("answers"),
        1,
        "FILTER predicate (posttypeid=2) must survive AST round-trip"
    );
    let m2 = ctx.metrics().await?;
    let d = metrics_delta(&m1, &m2);
    assert_eq!(d.queries_cache_hit, 1, "expected cache hit on second query");

    // Trigger MV first-build, then wait for it to land.
    let _ = ctx.query_one(sql, &[]).await?;
    ctx.cache_settle().await?;

    let m3 = ctx.metrics().await?;
    let row = ctx.query_one(sql, &[]).await?;
    assert_eq!(row.get::<_, i64>("questions"), 10);
    assert_eq!(row.get::<_, i64>("answers"), 1);
    let m4 = ctx.metrics().await?;
    let d = metrics_delta(&m3, &m4);
    assert!(
        d.cache_mv_hits >= 1 || d.cache_mv_fallthrough >= 1,
        "fourth query must be served from the cache (delta: {d:?})"
    );

    ctx.origin_query(
        "INSERT INTO posts (id, owneruserid, posttypeid) VALUES (21, 8, 1)",
        &[],
    )
    .await?;
    ctx.cdc_settle().await?;
    ctx.cache_settle().await?;

    let row = ctx.query_one(sql, &[]).await?;
    assert_eq!(
        row.get::<_, i64>("questions"),
        11,
        "CDC INSERT should advance the FILTER (posttypeid=1) count"
    );
    assert_eq!(
        row.get::<_, i64>("answers"),
        1,
        "CDC INSERT on a posttypeid=1 row must not affect the answers count"
    );

    Ok(())
}

/// A subquery inside a FILTER predicate references a separate table.
/// CDC events on that table must invalidate the cached fingerprint —
/// otherwise the subquery_nodes_collect traversal misses the dependency
/// and the cache silently serves stale data.
#[tokio::test]
async fn test_filter_subquery_cdc_invalidation() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE posts (id integer primary key, owneruserid integer not null)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE TABLE allowed_types (type_id integer primary key)",
        &[],
    )
    .await?;

    // 100 posts so the MV size gate (result_rows × ratio ≤ source_rows,
    // ratio default 10, result_rows = 1) passes and the MV actually builds.
    for id in 1..=100 {
        ctx.query("INSERT INTO posts VALUES ($1, 8)", &[&id])
            .await?;
    }
    // allowed_types starts empty; we INSERT after the MV is built so the
    // EXISTS predicate flips false → true and forces invalidation.

    // Let CDC register both tables with the writer before the first SELECT.
    ctx.cdc_settle().await?;

    // Touch each table once so the writer's catalog learns about them.
    let _ = ctx.simple_query("SELECT count(*) FROM posts").await?;
    let _ = ctx
        .simple_query("SELECT count(*) FROM allowed_types")
        .await?;
    ctx.cache_settle().await?;

    // FILTER predicate is a non-correlated EXISTS against allowed_types.
    // While allowed_types is empty the predicate is false for every input
    // row → count = 0. After we INSERT a row, EXISTS becomes true and
    // count must jump to 100.
    let sql = "SELECT count(*) FILTER (WHERE EXISTS \
                   (SELECT 1 FROM allowed_types WHERE type_id = 1)) AS visible \
               FROM posts WHERE owneruserid = 8";

    let row = ctx.origin.query_one(sql, &[]).await.map_err(Error::other)?;
    assert_eq!(row.get::<_, i64>("visible"), 0);

    // Q1: cache miss, triggers source-row population.
    let row = ctx.query_one(sql, &[]).await?;
    assert_eq!(row.get::<_, i64>("visible"), 0);
    ctx.cache_settle().await?;

    // Q2: cache hit on source-row eval; schedules MV first-build.
    let row = ctx.query_one(sql, &[]).await?;
    assert_eq!(row.get::<_, i64>("visible"), 0);
    ctx.cache_settle().await?;

    // Q3: should be served from the MV (the snapshot path). Asserting
    // cache_mv_hits proves the MV is what serves subsequent reads — and
    // therefore that an MV staleness bug would actually surface.
    let m_before_mv = ctx.metrics().await?;
    let row = ctx.query_one(sql, &[]).await?;
    assert_eq!(row.get::<_, i64>("visible"), 0);
    let m_after_mv = ctx.metrics().await?;
    let d = metrics_delta(&m_before_mv, &m_after_mv);
    assert!(
        d.cache_mv_hits >= 1,
        "expected MV fast-path hit before the invalidation test (delta {d:?})"
    );

    // INSERT a row into allowed_types. The cached query depends on this
    // table only through the FILTER+EXISTS subquery; if
    // subquery_nodes_collect doesn't walk agg_filter, no update query is
    // registered for allowed_types and the MV stays Fresh after the
    // insert — serving stale data.
    ctx.origin_query("INSERT INTO allowed_types VALUES (1)", &[])
        .await?;
    ctx.cdc_settle().await?;
    ctx.cache_settle().await?;

    let row = ctx.query_one(sql, &[]).await?;
    assert_eq!(
        row.get::<_, i64>("visible"),
        100,
        "INSERT into allowed_types must invalidate the FILTER+EXISTS query's MV"
    );

    Ok(())
}

/// DELETE-direction counterpart of `test_filter_subquery_cdc_invalidation`
/// (PGC-107). Builds the MV while `allowed_types` is populated and EXISTS
/// is true, then DELETEs the inner row. The cached aggregate must drop
/// 100 → 0; the bug surfaces as the stale value 100.
///
/// Root cause: `row_uncached_invalidation_check` (src/cache/writer/cdc.rs)
/// classifies the FILTER+EXISTS subquery as `SubqueryKind::Inclusion` and
/// skips invalidation on `CdcOperation::Delete`. Sound for WHERE-clause
/// EXISTS (extra cached rows re-filter at serve time); unsound for an
/// EXISTS inside an aggregate FILTER, where the predicate truth value
/// feeds the aggregate per row.
#[tokio::test]
async fn test_filter_subquery_cdc_invalidation_delete() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE posts (id integer primary key, owneruserid integer not null)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE TABLE allowed_types (type_id integer primary key)",
        &[],
    )
    .await?;

    for id in 1..=100 {
        ctx.query("INSERT INTO posts VALUES ($1, 8)", &[&id])
            .await?;
    }
    // allowed_types starts populated; MV is built while EXISTS is true so
    // the subsequent DELETE must flip the predicate true → false and force
    // invalidation.
    ctx.query("INSERT INTO allowed_types VALUES (1)", &[])
        .await?;

    ctx.cdc_settle().await?;

    let _ = ctx.simple_query("SELECT count(*) FROM posts").await?;
    let _ = ctx
        .simple_query("SELECT count(*) FROM allowed_types")
        .await?;
    ctx.cache_settle().await?;

    let sql = "SELECT count(*) FILTER (WHERE EXISTS \
                   (SELECT 1 FROM allowed_types WHERE type_id = 1)) AS visible \
               FROM posts WHERE owneruserid = 8";

    let row = ctx.origin.query_one(sql, &[]).await.map_err(Error::other)?;
    assert_eq!(row.get::<_, i64>("visible"), 100);

    // Q1: cache miss → population.
    let row = ctx.query_one(sql, &[]).await?;
    assert_eq!(row.get::<_, i64>("visible"), 100);
    ctx.cache_settle().await?;

    // Q2: source-row hit; schedules MV first-build.
    let row = ctx.query_one(sql, &[]).await?;
    assert_eq!(row.get::<_, i64>("visible"), 100);
    ctx.cache_settle().await?;

    // Q3: served from the MV snapshot — assert this so a staleness bug
    // on the MV path actually surfaces.
    let m_before_mv = ctx.metrics().await?;
    let row = ctx.query_one(sql, &[]).await?;
    assert_eq!(row.get::<_, i64>("visible"), 100);
    let m_after_mv = ctx.metrics().await?;
    let d = metrics_delta(&m_before_mv, &m_after_mv);
    assert!(
        d.cache_mv_hits >= 1,
        "expected MV fast-path hit before the invalidation test (delta {d:?})"
    );

    // DELETE the only row in allowed_types. Origin's aggregate drops to 0.
    // pgcache must invalidate the MV; otherwise it serves stale 100.
    ctx.origin_query("DELETE FROM allowed_types WHERE type_id = 1", &[])
        .await?;
    ctx.cdc_settle().await?;
    ctx.cache_settle().await?;

    let row = ctx.query_one(sql, &[]).await?;
    assert_eq!(
        row.get::<_, i64>("visible"),
        0,
        "DELETE from allowed_types must invalidate the FILTER+EXISTS query's MV"
    );

    Ok(())
}

/// CASE-WHEN counterpart of `test_filter_subquery_cdc_invalidation_delete`
/// (PGC-107). The fix retags subqueries inside both `agg_filter` and
/// `Case` WHEN conditions — these hit different walker arms, so assert
/// the CASE path independently to catch a future walker rewrite that
/// re-introduces the bug only on the CASE side.
#[tokio::test]
async fn test_case_when_subquery_cdc_invalidation_delete() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE posts (id integer primary key, owneruserid integer not null)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE TABLE allowed_types (type_id integer primary key)",
        &[],
    )
    .await?;

    for id in 1..=100 {
        ctx.query("INSERT INTO posts VALUES ($1, 8)", &[&id])
            .await?;
    }
    ctx.query("INSERT INTO allowed_types VALUES (1)", &[])
        .await?;

    ctx.cdc_settle().await?;

    let _ = ctx.simple_query("SELECT count(*) FROM posts").await?;
    let _ = ctx
        .simple_query("SELECT count(*) FROM allowed_types")
        .await?;
    ctx.cache_settle().await?;

    // EXISTS lives inside a CASE WHEN condition (Case-arm walker path,
    // distinct from agg_filter). Truth value flips THEN/ELSE per row, so
    // DELETE on the inner table must invalidate the cached aggregate.
    let sql = "SELECT SUM(CASE WHEN EXISTS \
                   (SELECT 1 FROM allowed_types WHERE type_id = 1) \
                   THEN 1 ELSE 0 END)::bigint AS visible \
               FROM posts WHERE owneruserid = 8";

    let row = ctx.origin.query_one(sql, &[]).await.map_err(Error::other)?;
    assert_eq!(row.get::<_, i64>("visible"), 100);

    // Q1: cache miss → population.
    let row = ctx.query_one(sql, &[]).await?;
    assert_eq!(row.get::<_, i64>("visible"), 100);
    ctx.cache_settle().await?;

    // Q2: source-row hit; schedules MV first-build.
    let row = ctx.query_one(sql, &[]).await?;
    assert_eq!(row.get::<_, i64>("visible"), 100);
    ctx.cache_settle().await?;

    // Q3: must be served from the MV — assert this so a staleness bug
    // on the MV path actually surfaces.
    let m_before_mv = ctx.metrics().await?;
    let row = ctx.query_one(sql, &[]).await?;
    assert_eq!(row.get::<_, i64>("visible"), 100);
    let m_after_mv = ctx.metrics().await?;
    let d = metrics_delta(&m_before_mv, &m_after_mv);
    assert!(
        d.cache_mv_hits >= 1,
        "expected MV fast-path hit before the invalidation test (delta {d:?})"
    );

    ctx.origin_query("DELETE FROM allowed_types WHERE type_id = 1", &[])
        .await?;
    ctx.cdc_settle().await?;
    ctx.cache_settle().await?;

    let row = ctx.query_one(sql, &[]).await?;
    assert_eq!(
        row.get::<_, i64>("visible"),
        0,
        "DELETE from allowed_types must invalidate the CASE-WHEN+EXISTS query's MV"
    );

    Ok(())
}
