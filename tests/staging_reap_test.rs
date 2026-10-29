//! Eager tombstone reap (PGC-553): a population staged behind a busy merge
//! slot whose query is invalidated while it waits releases its staging the
//! moment the invalidation lands, not when its dead entry surfaces at the
//! heap head — and exactly once. Builds only with `--features fault-injection`
//! (the merge chunk faults hold the slot).
#![cfg(feature = "fault-injection")]

use std::io::Error;
use std::time::{Duration, Instant};

use crate::util::{MetricsSnapshot, TestContext};

mod util;

/// Heap blocks per chunk and the delay after each: with `POPULATED_ROWS` the
/// first merge spans several seconds and holds the slot.
const CHUNK_BLOCKS: &str = "2";
const CHUNK_DELAY_MS: &str = "250";
const POPULATED_ROWS: u64 = 10_000;

async fn metrics_wait(
    ctx: &mut TestContext,
    what: &str,
    secs: u64,
    ready: impl Fn(&MetricsSnapshot) -> bool,
) -> Result<MetricsSnapshot, Error> {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let m = ctx.metrics().await?;
        if ready(&m) {
            return Ok(m);
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {what}: {m:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn test_invalidated_staged_population_releases_staging_before_its_turn() -> Result<(), Error>
{
    let mut ctx = TestContext::setup_fault(&[
        ("PGCACHE_FAULT_MERGE_CHUNK_BLOCKS", CHUNK_BLOCKS),
        ("PGCACHE_FAULT_MERGE_CHUNK_DELAY_MS", CHUNK_DELAY_MS),
    ])
    .await?;
    ctx.simple_query(&format!(
        "create table chunked (id int primary key, grp int not null, v int not null); \
         insert into chunked (id, grp, v) values (1, 1, 1), (3, 3, 3); \
         insert into chunked (id, grp, v) select i, 2, i from generate_series(1000, {}) i; \
         create table other (id int primary key, c_id int not null); \
         insert into other values (1, 3);",
        999 + POPULATED_ROWS
    ))
    .await?;
    ctx.cdc_decode_settle().await?;

    // The slot holder: a merge of several seconds.
    let before = ctx.metrics().await?;
    ctx.simple_query("select count(*) from chunked where grp = 2")
        .await?;
    // The waiter: a small join whose population stages at once and then
    // waits in the heap behind the busy slot.
    let q_wait = "select o.id from other o join chunked c on c.id = o.c_id \
                  where c.grp = 3 order by o.id";
    ctx.simple_query(q_wait).await?;
    metrics_wait(&mut ctx, "the join to stage behind the merge", 15, |m| {
        m.cache_merge_pending_depth >= 1 && m.cache_merge_applied == before.cache_merge_applied
    })
    .await?;

    // A growing insert invalidates the waiting join. Its staging is released
    // at once: superseded and discarded while its entry still sits in the
    // heap and the first merge is still applying.
    ctx.origin
        .batch_execute("insert into other values (2, 3)")
        .await
        .map_err(Error::other)?;
    let dropped = metrics_wait(&mut ctx, "the eager drop", 10, |m| {
        m.cache_merge_superseded > before.cache_merge_superseded
    })
    .await?;
    assert_eq!(
        dropped.cache_merge_discards - before.cache_merge_discards,
        1,
        "the staging was sent for discard with the drop"
    );
    assert_eq!(
        dropped.cache_merge_applied, before.cache_merge_applied,
        "the slot holder is still merging"
    );
    assert!(
        dropped.cache_merge_pending_depth >= 1,
        "the dead entry stays in the heap until it surfaces"
    );
    let returned = metrics_wait(&mut ctx, "the worker's ack", 10, |m| {
        m.cache_merge_discards_pending == 0
    })
    .await?;
    assert_eq!(
        returned.cache_merge_applied, before.cache_merge_applied,
        "staging came back while the slot holder was still merging"
    );

    // Once the slot holder finishes the tombstone surfaces and is popped
    // without a second discard; the join readmits on its next read.
    ctx.cache_settle_with_timeout(Duration::from_secs(60))
        .await?;
    let settled = metrics_wait(&mut ctx, "the heap to drain", 30, |m| {
        m.cache_merge_pending_depth == 0
    })
    .await?;
    assert_eq!(
        settled.cache_merge_discards - before.cache_merge_discards,
        1,
        "the tombstone pop did not discard again"
    );
    assert_eq!(
        settled.cache_merge_superseded - before.cache_merge_superseded,
        1,
        "counted once"
    );
    ctx.simple_query(q_wait).await?;
    ctx.cache_settle_with_timeout(Duration::from_secs(30))
        .await?;
    let before = ctx.metrics().await?;
    ctx.simple_query(q_wait).await?;
    let after = ctx.metrics_after_outcomes(&before, 1).await?;
    assert_eq!(after.queries_cache_hit - before.queries_cache_hit, 1);
    Ok(())
}
