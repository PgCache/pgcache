use std::io::Error;

use crate::util::{
    TestContext, assert_cache_hit, assert_proxy_rows_match_origin, assert_subsume_hit,
    metrics_delta, query_warm,
};

mod util;

/// Readmission of an invalidated query runs the subsumption check: a join
/// child whose references two Ready single-table parents cover is stamped and
/// served on readmit instead of repopulated from origin (PGC-532).
#[tokio::test]
async fn test_readmit_subsumed_by_ready_parents() -> Result<(), Error> {
    let mut ctx = TestContext::setup_clock(1).await?;

    ctx.query(
        "CREATE TABLE rsub_parent (id INTEGER PRIMARY KEY, grp INTEGER, name TEXT)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE TABLE rsub_detail (id INTEGER PRIMARY KEY, parent_id INTEGER, grp INTEGER, detail TEXT)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO rsub_parent VALUES (1, 1, 'alice'), (2, 1, 'bob'), (3, 2, 'carol')",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO rsub_detail VALUES (1, 1, 1, 'a1'), (2, 2, 1, 'b1'), (3, 3, 2, 'c1')",
        &[],
    )
    .await?;
    ctx.cdc_decode_settle().await?;

    // Single-table parents: never invalidated, maintained in place by CDC.
    query_warm(&mut ctx, "SELECT * FROM rsub_parent WHERE grp = 1").await?;
    let m = query_warm(&mut ctx, "SELECT * FROM rsub_detail WHERE grp = 1").await?;

    // The child is covered on first registration.
    let child = "SELECT p.id, p.name, d.detail \
        FROM rsub_parent p JOIN rsub_detail d ON d.parent_id = p.id \
        WHERE p.grp = 1 AND d.grp = 1 ORDER BY d.id";
    ctx.simple_query(child).await?;
    let m = assert_subsume_hit(&mut ctx, m).await?;

    // A detail row joining in grows the child's result: invalidated. The
    // detail parent absorbs the row in place and stays Ready.
    ctx.origin_query("INSERT INTO rsub_detail VALUES (4, 1, 1, 'a2')", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    let m_invalidated = ctx.metrics().await?;
    let delta = metrics_delta(&m, &m_invalidated);
    assert_eq!(
        delta.cache_invalidations, 1,
        "expected the child invalidated"
    );

    // Readmit: covered by the still-Ready parents, so it serves immediately
    // with the new row and no population.
    assert_proxy_rows_match_origin(&mut ctx, child).await?;
    let m_readmitted = ctx.metrics_after_outcomes(&m_invalidated, 1).await?;
    let delta = metrics_delta(&m_invalidated, &m_readmitted);
    assert_eq!(
        delta.queries_cache_hit, 1,
        "expected the readmit served from cache"
    );
    assert_eq!(delta.queries_cache_miss, 0, "unexpected cache miss");
    assert_eq!(delta.cache_readmissions, 1, "expected one readmission");
    assert_eq!(delta.cache_subsumptions, 1, "expected the readmit subsumed");
    assert_eq!(
        delta.cache_readmission_subsumptions, 1,
        "expected the readmit counted as a readmission subsumption"
    );

    // Steady state: a plain hit.
    assert_proxy_rows_match_origin(&mut ctx, child).await?;
    assert_cache_hit(&mut ctx, m_readmitted).await?;

    Ok(())
}

/// A pinned query's automatic readmit after CDC invalidation also runs the
/// subsumption check: with Ready parents covering it, no population is
/// dispatched and the next request is a hit (PGC-532).
#[tokio::test]
async fn test_pinned_readmit_subsumed_by_ready_parents() -> Result<(), Error> {
    let pinned = "SELECT p.id, p.name, d.detail \
        FROM rsub_pin_parent p JOIN rsub_pin_detail d ON d.parent_id = p.id \
        WHERE p.grp = 1 AND d.grp = 1 ORDER BY d.id";

    let mut ctx = TestContext::setup_pinned(pinned, |origin| async move {
        for sql in [
            "CREATE TABLE rsub_pin_parent (id INTEGER PRIMARY KEY, grp INTEGER, name TEXT)",
            "CREATE TABLE rsub_pin_detail (id INTEGER PRIMARY KEY, parent_id INTEGER, grp INTEGER, detail TEXT)",
            "INSERT INTO rsub_pin_parent VALUES (1, 1, 'alice'), (2, 1, 'bob'), (3, 2, 'carol')",
            "INSERT INTO rsub_pin_detail VALUES (1, 1, 1, 'a1'), (2, 2, 1, 'b1'), (3, 3, 2, 'c1')",
        ] {
            origin.execute(sql, &[]).await.map_err(Error::other)?;
        }
        Ok(origin)
    })
    .await?;
    ctx.cache_settle().await?;

    // Parents registered after the pinned child populated.
    query_warm(&mut ctx, "SELECT * FROM rsub_pin_parent WHERE grp = 1").await?;
    let m = query_warm(&mut ctx, "SELECT * FROM rsub_pin_detail WHERE grp = 1").await?;

    // Invalidate the pinned child; its auto-readmit is covered by the parents.
    ctx.origin_query("INSERT INTO rsub_pin_detail VALUES (4, 1, 1, 'a2')", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    ctx.cache_settle().await?;

    let m_readmitted = ctx.metrics().await?;
    let delta = metrics_delta(&m, &m_readmitted);
    assert_eq!(
        delta.cache_invalidations, 1,
        "expected the pinned child invalidated"
    );
    assert_eq!(delta.cache_readmissions, 1, "expected one readmission");
    assert_eq!(
        delta.cache_readmission_subsumptions, 1,
        "expected the pinned readmit subsumed"
    );

    // Served from cache with the new row, no population in between.
    assert_proxy_rows_match_origin(&mut ctx, pinned).await?;
    assert_cache_hit(&mut ctx, m_readmitted).await?;

    Ok(())
}
