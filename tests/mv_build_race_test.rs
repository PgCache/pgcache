//! Off-thread MV build races, made deterministic with fault-injection holds
//! (`PGCACHE_FAULT_MV_BUILD_HOLD_MS` widens the in-flight window):
//!
//! - dirtied mid-build: the result must be discarded (`Building →
//!   BuildingDirty → Pending`), never flipped Fresh — flipping would serve an
//!   MV missing the concurrent change (a stale read).
//! - evicted mid-build (`PGCACHE_FAULT_MV_EVICT_ON_BUILD`): the re-registered
//!   incarnation's build defers behind the in-flight guard; the stale
//!   completion is discarded, its table dropped, the deferred build
//!   dispatched.

#![cfg(feature = "fault-injection")]

use std::io::Error;
use std::time::Duration;

use crate::util::{TestContext, metrics_delta};

mod util;

/// How long each MV build task is held before running its SQL. Long enough
/// for an origin write + CDC apply to land mid-build with margin.
const BUILD_HOLD_MS: &str = "1000";

/// Settle timeout covering held builds (each build adds ~1s).
const SETTLE: Duration = Duration::from_secs(20);

/// Count tables in the `pgcache_mv` schema of the cache DB.
async fn mv_table_count(dbs: &crate::util::TempDBs) -> Result<i64, Error> {
    let client = crate::util::connect_cache_db(dbs).await?;
    let row = client
        .query_one(
            "SELECT count(*) FROM pg_tables WHERE schemaname = 'pgcache_mv'",
            &[],
        )
        .await
        .map_err(Error::other)?;
    Ok(row.get(0))
}

/// Eviction while a build task is in flight: the entry's MV-table drop is
/// deferred (the build holds locks), the re-registered incarnation's build is
/// deferred behind the in-flight guard, and the stale completion must discard
/// its result, drop the stale table, and dispatch the deferred build — ending
/// with exactly one MV table and a working fast path.
#[tokio::test]
async fn test_mv_build_evicted_mid_build_recovers() -> Result<(), Error> {
    // Longer hold than the dirtied-in-flight test: build B must be scheduled
    // while build A is still in flight for the dispatch to take the deferred
    // path, and registration + settle of the re-registered incarnation has to
    // fit inside A's hold window.
    let mut ctx = TestContext::setup_fault(&[
        ("PGCACHE_FAULT_MV_BUILD_HOLD_MS", "2500"),
        ("PGCACHE_FAULT_MV_EVICT_ON_BUILD", "1"),
    ])
    .await?;

    ctx.simple_query("CREATE TABLE mv_evict (id integer primary key, val text)")
        .await?;
    for i in 0..20 {
        ctx.simple_query(&format!("INSERT INTO mv_evict VALUES ({i}, 'v{i}')"))
            .await?;
    }

    let q = "SELECT count(*) FROM mv_evict";

    // Register + populate the source-row cache.
    let row = ctx.query_one(q, &[]).await?;
    assert_eq!(row.get::<_, i64>(0), 20);
    ctx.cache_settle_with_timeout(SETTLE).await?;

    // Trigger hit: schedules build A. The writer dispatches it (task held
    // ~2.5s) and the fault hook evicts the entry immediately after — the MV
    // drop is deferred because the build holds the table.
    let row = ctx.query_one(q, &[]).await?;
    assert_eq!(row.get::<_, i64>(0), 20);

    // Give the writer time to process the MvBuild command + fault eviction
    // (microseconds of work; the margin is for command-queue latency).
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The entry is gone: this query must MISS and re-register.
    let m1 = ctx.metrics().await?;
    let row = ctx.query_one(q, &[]).await?;
    assert_eq!(row.get::<_, i64>(0), 20);
    let m2 = ctx.metrics().await?;
    assert_eq!(
        metrics_delta(&m1, &m2).queries_cache_miss,
        1,
        "expected a miss after the fault eviction (hook did not fire?)"
    );
    ctx.cache_settle_with_timeout(SETTLE).await?;

    // Hit the new incarnation: schedules build B, whose dispatch is deferred
    // behind build A's in-flight guard. Build A then completes into the
    // Scheduled state: its result is discarded, its table dropped, and B is
    // dispatched (held ~2.5s again) and lands Fresh.
    let row = ctx.query_one(q, &[]).await?;
    assert_eq!(row.get::<_, i64>(0), 20);
    ctx.cache_settle_with_timeout(SETTLE).await?;

    // Fast path serves from the rebuilt MV; exactly one table on disk.
    let m3 = ctx.metrics().await?;
    let row = ctx.query_one(q, &[]).await?;
    assert_eq!(row.get::<_, i64>(0), 20);
    let m4 = ctx.metrics().await?;
    let d = metrics_delta(&m3, &m4);
    assert_eq!(
        d.cache_mv_hits, 1,
        "expected MV hit after recovery (got {d:?})"
    );
    assert_eq!(
        mv_table_count(&ctx.dbs).await?,
        1,
        "expected exactly one MV table (stale build's table must be dropped)"
    );

    Ok(())
}

#[tokio::test]
async fn test_mv_build_dirtied_in_flight_is_discarded() -> Result<(), Error> {
    let mut ctx =
        TestContext::setup_fault(&[("PGCACHE_FAULT_MV_BUILD_HOLD_MS", BUILD_HOLD_MS)]).await?;

    ctx.simple_query("CREATE TABLE mv_race (id integer primary key, val text)")
        .await?;
    for i in 0..20 {
        ctx.simple_query(&format!("INSERT INTO mv_race VALUES ({i}, 'v{i}')"))
            .await?;
    }

    let q = "SELECT count(*) FROM mv_race";

    // Register + populate the source-row cache.
    let row = ctx.query_one(q, &[]).await?;
    assert_eq!(row.get::<_, i64>(0), 20);
    ctx.cache_settle_with_timeout(SETTLE).await?;

    // First cache hit schedules the first build (held ~1s); settle waits
    // through Scheduled/Building via /status mv_state.
    let row = ctx.query_one(q, &[]).await?;
    assert_eq!(row.get::<_, i64>(0), 20);
    ctx.cache_settle_with_timeout(SETTLE).await?;

    // MV is Fresh now: confirm the fast path works before the race.
    let m1 = ctx.metrics().await?;
    let row = ctx.query_one(q, &[]).await?;
    assert_eq!(row.get::<_, i64>(0), 20);
    let m2 = ctx.metrics().await?;
    assert_eq!(
        metrics_delta(&m1, &m2).cache_mv_hits,
        1,
        "expected a Fresh MV fast-path hit before the race"
    );

    // Dirty the MV (insert matches the aggregate unconditionally).
    ctx.origin_query("INSERT INTO mv_race VALUES (100, 'first')", &[])
        .await?;
    ctx.cdc_settle().await?;

    // Trigger the rebuild: fallthrough serve + `Pending → Scheduled` +
    // dispatch. The build task is now held in `Building` for ~1s.
    let row = ctx.query_one(q, &[]).await?;
    assert_eq!(row.get::<_, i64>(0), 21, "fallthrough must see the insert");

    // Land a second insert while the build is in flight. CDC apply is no
    // longer blocked behind the build (the point of off-thread builds), so
    // this commits to the cache and dirty-marks `Building → BuildingDirty`
    // well inside the hold window.
    ctx.origin_query("INSERT INTO mv_race VALUES (101, 'mid-build')", &[])
        .await?;
    ctx.cdc_settle().await?;

    // Wait for the held build to complete and the writer to process the
    // completion (discard).
    let m3 = ctx.metrics().await?;
    ctx.cache_settle_with_timeout(SETTLE).await?;

    // The discarded build must NOT serve: this read must fall through to
    // source rows and see both inserts. A broken discard (Fresh flip with
    // pre-insert contents) would serve 21 from the MV here.
    let m4 = ctx.metrics().await?;
    let row = ctx.query_one(q, &[]).await?;
    assert_eq!(
        row.get::<_, i64>(0),
        22,
        "stale MV served: build dirtied in flight was not discarded"
    );
    let m5 = ctx.metrics().await?;
    let d = metrics_delta(&m4, &m5);
    assert_eq!(
        d.cache_mv_hits, 0,
        "MV must not be Fresh right after a discarded build"
    );
    assert_eq!(d.cache_mv_fallthrough, 1);
    assert!(
        m4.cache_mv_skipped_rebuilds > m3.cache_mv_skipped_rebuilds,
        "expected the in-flight build to be counted as discarded"
    );

    // The fallthrough hit above scheduled a clean rebuild (no concurrent
    // writes this time): it must land Fresh and serve the full count.
    ctx.cache_settle_with_timeout(SETTLE).await?;
    let m6 = ctx.metrics().await?;
    let row = ctx.query_one(q, &[]).await?;
    assert_eq!(row.get::<_, i64>(0), 22);
    let m7 = ctx.metrics().await?;
    assert_eq!(
        metrics_delta(&m6, &m7).cache_mv_hits,
        1,
        "expected MV hit after the clean rebuild"
    );

    Ok(())
}
