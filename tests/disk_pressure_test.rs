//! PGC-276 disk-pressure throttle + escalating reclaim. Pressure is forced via
//! the fault-injection sentinel file (`PGCACHE_FAULT_DISK_PRESSURE` names a path;
//! pressure = file exists), so the test toggles it after queries are cached —
//! impossible with a static env. Builds only with `--features fault-injection`.
#![cfg(feature = "fault-injection")]

use std::io::Error;
use std::time::{Duration, Instant, SystemTime};

use tokio::time::sleep;

use crate::util::TestContext;

mod util;

/// Run a query and return (cache_hit_delta, cache_miss_delta).
async fn probe(ctx: &mut TestContext, sql: &str) -> Result<(u64, u64), Error> {
    let before = ctx.metrics().await?;
    ctx.simple_query(sql).await?;
    let after = ctx.metrics().await?;
    Ok((
        after.queries_cache_hit - before.queries_cache_hit,
        after.queries_cache_miss - before.queries_cache_miss,
    ))
}

#[tokio::test]
async fn test_disk_pressure_throttles_and_drops_fewest_queries_table() -> Result<(), Error> {
    // Unique sentinel path per test run; absent → no pressure.
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let sentinel = std::env::temp_dir().join(format!("pgc_disk_pressure_{nanos}"));
    let _ = std::fs::remove_file(&sentinel);

    let mut ctx =
        TestContext::setup_fault(&[("PGCACHE_FAULT_DISK_PRESSURE", sentinel.to_str().unwrap())])
            .await?;

    // Two source tables: dp_small touched by 1 cached query, dp_big by 3.
    for t in &["dp_small", "dp_big"] {
        ctx.simple_query(&format!(
            "create table {t} (id int primary key, data text not null)"
        ))
        .await?;
        ctx.simple_query(&format!(
            "insert into {t} select g, repeat('x', 200) from generate_series(1, 300) g"
        ))
        .await?;
    }
    ctx.cdc_settle().await?;

    let q_small = "select data from dp_small where id = 1";
    let q_big1 = "select data from dp_big where id = 1";
    let q_big2 = "select data from dp_big where id = 2";
    let q_big3 = "select data from dp_big where id = 3";
    for q in [q_small, q_big1, q_big2, q_big3] {
        ctx.simple_query(q).await?;
    }
    ctx.cache_settle().await?;

    // All cached: hits, no misses.
    assert_eq!(probe(&mut ctx, q_small).await?, (1, 0), "dp_small cached");
    assert_eq!(probe(&mut ctx, q_big1).await?, (1, 0), "dp_big cached");

    // Engage disk pressure.
    std::fs::write(&sentinel, b"").map_err(Error::other)?;

    // After a tick the disk throttle is set: a brand-new query is forwarded, not
    // cached, so issuing it twice yields two misses (no hit).
    sleep(Duration::from_millis(1500)).await;
    let (h1, m1) = probe(&mut ctx, "select data from dp_big where id = 50").await?;
    let (h2, m2) = probe(&mut ctx, "select data from dp_big where id = 50").await?;
    assert_eq!(
        (h1, m1, h2, m2),
        (0, 1, 0, 1),
        "under disk throttle a new query is forwarded both times, never cached"
    );

    // Escalating reclaim drops the fewest-queries table (dp_small) first. Poll
    // until dp_small is evicted (its query now misses), and check dp_big is still
    // cached at that moment — confirming fewest-queries targeting.
    let deadline = Instant::now() + Duration::from_secs(12);
    loop {
        let (sh, sm) = probe(&mut ctx, q_small).await?;
        if (sh, sm) == (0, 1) {
            // dp_small dropped. dp_big (more queries) should still be cached.
            let (bh, _bm) = probe(&mut ctx, q_big1).await?;
            assert_eq!(bh, 1, "dp_big (more queries) should outlive dp_small");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "dp_small was never dropped under sustained disk pressure"
        );
        sleep(Duration::from_millis(200)).await;
    }

    // Clear pressure; after a tick the throttle releases and new queries cache again.
    std::fs::remove_file(&sentinel).map_err(Error::other)?;
    sleep(Duration::from_millis(1500)).await;
    let (h1, _m1) = probe(&mut ctx, "select data from dp_big where id = 100").await?;
    let (h2, _m2) = probe(&mut ctx, "select data from dp_big where id = 100").await?;
    assert_eq!(
        (h1, h2),
        (0, 1),
        "after pressure clears, a new query caches (miss then hit)"
    );

    Ok(())
}
