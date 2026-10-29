//! PGC-553: a readmission is deferred while the oldest pending merge has
//! waited longer than the query's last Ready period lasted — a population now
//! would be invalidated again before its merge landed, holding staging for
//! nothing. The merge head is held back with the CDC delivery-delay fault
//! behind a burst of origin writes: the apply watermark trails the snapshot of
//! any population started after the burst, so its merge waits at the head.
//! Builds only with `--features fault-injection`.
#![cfg(feature = "fault-injection")]

use std::io::Error;
use std::time::{Duration, Instant};

use tokio::time::sleep;

use crate::util::TestContext;

mod util;

/// Run a query and return (cache_hit_delta, cache_miss_delta).
async fn probe(ctx: &mut TestContext, sql: &str) -> Result<(u64, u64), Error> {
    let before = ctx.metrics().await?;
    ctx.simple_query(sql).await?;
    let after = ctx.metrics_after_outcomes(&before, 1).await?;
    Ok((
        after.queries_cache_hit - before.queries_cache_hit,
        after.queries_cache_miss - before.queries_cache_miss,
    ))
}

/// Read `sql` until it hits: the moment it is Ready, without waiting on the
/// delayed CDC stream the way `cache_settle` does.
async fn ready_wait(ctx: &mut TestContext, sql: &str) -> Result<(), Error> {
    let deadline = Instant::now() + Duration::from_secs(15);
    while probe(ctx, sql).await? != (1, 0) {
        assert!(Instant::now() < deadline, "query never became Ready");
        sleep(Duration::from_millis(50)).await;
    }
    Ok(())
}

/// Wait for the writer to process one more CDC invalidation than `before` saw.
async fn invalidation_wait(ctx: &mut TestContext, before: u64) -> Result<(), Error> {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if ctx.metrics().await?.cache_invalidations > before {
            return Ok(());
        }
        assert!(Instant::now() < deadline, "invalidation never arrived");
        sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn test_readmission_deferred_while_merge_head_outwaits_query_lifetime() -> Result<(), Error> {
    let mut ctx =
        TestContext::setup_fault_clock(&[("PGCACHE_FAULT_CDC_DELIVER_DELAY_MS", "250")]).await?;

    // Three independent join pairs, so a write to one never touches another.
    for pair in ["rd_cold", "rd_hot", "rd_block"] {
        ctx.simple_query(&format!(
            "create table {pair}_a (id int primary key, v int not null)"
        ))
        .await?;
        ctx.simple_query(&format!(
            "create table {pair}_b (id int primary key, a_id int not null, v int not null)"
        ))
        .await?;
        ctx.simple_query(&format!("insert into {pair}_a values (1, 10), (2, 20)"))
            .await?;
        ctx.simple_query(&format!(
            "insert into {pair}_b values (1, 1, 100), (2, 2, 200)"
        ))
        .await?;
    }
    ctx.cdc_decode_settle().await?;

    let join = |pair: &str, id: u32| {
        format!(
            "select a.id, b.v from {pair}_a a join {pair}_b b on b.a_id = a.id \
             where a.id = {id} order by b.v"
        )
    };
    let q_cold = join("rd_cold", 1);
    let q_hot = join("rd_hot", 1);

    // q_cold becomes Ready first and lives for seconds. A query over the block
    // pair is cached now so the pair is published: only delivered messages are
    // delayed, and a write to an unpublished relation is never delivered.
    ctx.simple_query(&q_cold).await?;
    ctx.simple_query(&join("rd_block", 1)).await?;
    ctx.cache_settle().await?;

    // q_hot is invalidated the moment it becomes Ready. Its invalidation is
    // itself delivered through the delayed stream, behind the keepalives the
    // merge gate requested while q_hot's own merge waited, so its last
    // lifetime is a few seconds, not milliseconds.
    let before = ctx.metrics().await?;
    ready_wait(&mut ctx, &q_hot).await?;
    ctx.simple_query("insert into rd_hot_b values (3, 1, 300)")
        .await?;
    invalidation_wait(&mut ctx, before.cache_invalidations).await?;

    // q_cold is invalidated after many seconds of life, before the burst
    // below so its message is not queued behind it.
    sleep(Duration::from_secs(8)).await;
    let before = ctx.metrics().await?;
    ctx.simple_query("insert into rd_cold_b values (50, 1, 500)")
        .await?;
    invalidation_wait(&mut ctx, before.cache_invalidations).await?;

    // Congest the merge queue: a hundred delayed messages (about 25 s of
    // delivery) ahead of a population whose snapshot postdates them, so its
    // merge waits on the watermark at the head of the heap. Probe once the
    // head has waited longer than q_hot lived and shorter than q_cold did.
    ctx.simple_query("insert into rd_block_b select g, 2, g from generate_series(10, 109) g")
        .await?;
    ctx.simple_query(&join("rd_block", 2)).await?;
    sleep(Duration::from_secs(8)).await;

    // q_hot: the head has waited longer than q_hot's last lifetime, so both
    // reads forward and neither readmits.
    let before = ctx.metrics().await?;
    assert_eq!(
        probe(&mut ctx, &q_hot).await?,
        (0, 1),
        "deferred read forwards"
    );
    assert_eq!(probe(&mut ctx, &q_hot).await?, (0, 1), "still deferred");
    let after = ctx.metrics().await?;
    assert_eq!(
        after.cache_readmissions_deferred - before.cache_readmissions_deferred,
        2,
        "both reads counted as deferred"
    );
    assert_eq!(
        after.cache_readmissions, before.cache_readmissions,
        "a deferred read is not a readmission"
    );

    // q_cold readmits under the same held head: its last lifetime outlasts
    // the head's age. The read itself forwards while its population waits.
    let before = ctx.metrics().await?;
    assert_eq!(probe(&mut ctx, &q_cold).await?, (0, 1), "readmit forwards");
    let after = ctx.metrics().await?;
    assert_eq!(
        after.cache_readmissions - before.cache_readmissions,
        1,
        "long-lived query readmits"
    );
    assert_eq!(
        after.cache_readmissions_deferred, before.cache_readmissions_deferred,
        "long-lived query is not deferred"
    );

    // Once CDC catches up the head drains, and q_hot readmits and caches.
    ctx.cdc_apply_settle_with_timeout(Duration::from_secs(60))
        .await?;
    ctx.simple_query(&q_hot).await?;
    ctx.cache_settle_with_timeout(Duration::from_secs(60))
        .await?;
    assert_eq!(
        probe(&mut ctx, &q_hot).await?,
        (1, 0),
        "cached again after the queue drains"
    );

    Ok(())
}
