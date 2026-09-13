//! Eviction's generation bump must not strand a query whose population is in
//! flight (PGC-418 review). Under a query-count cap, the candidate at the
//! minimum generation is bumped to a new generation if it is pinned (or
//! CLOCK-referenced). The bump re-keys the query, and a population or merge
//! keyed by the old generation is then abandoned by `population_is_current`
//! with nothing to finalize or readmit it — the query sits in Loading forever.
//!
//! Construction: a pinned query's population is held open past the writer's
//! 1 s eviction tick by the one-shot population delay, while two client
//! queries push the count over a cap of one. Without the fix the eviction tick
//! bumps the Loading pinned query and it never becomes Ready.
//!
//! Fault-dependent — gated like `population_merge_gate_test.rs`.
#![cfg(feature = "fault-injection")]

use std::io::Error;
use std::time::Duration;

use crate::util::TestContext;

mod util;

/// Longer than the writer's 1 s eviction tick, so the tick runs while the
/// pinned population is still in flight.
const PINNED_POPULATION_DELAY_MS: &str = "3000";

#[tokio::test]
async fn test_eviction_does_not_strand_a_pinned_query_mid_population() -> Result<(), Error> {
    let pinned = "select id, data from pin_slow where id > 0 order by id";
    let mut ctx = TestContext::setup_pinned_fault(
        pinned,
        &[
            ("PGCACHE_FAULT_EVICTION_COUNT_CAP", "1"),
            // First population = the pinned query, registered at startup.
            (
                "PGCACHE_FAULT_POPULATION_DELAY_ONCE_MS",
                PINNED_POPULATION_DELAY_MS,
            ),
        ],
        |origin| async move {
            origin
                .batch_execute(
                    "create table pin_slow (id int primary key, data text); \
                     insert into pin_slow select i, 'p' || i from generate_series(1, 200) i; \
                     create table fodder_a (id int primary key); insert into fodder_a values (1); \
                     create table fodder_b (id int primary key); insert into fodder_b values (1);",
                )
                .await
                .map_err(Error::other)?;
            Ok(origin)
        },
    )
    .await?;

    // Two registrations over the cap of one while the pinned population is
    // still delayed: the eviction tick finds the pinned query at the minimum
    // generation.
    ctx.simple_query("select id from fodder_a where id = 1")
        .await?;
    ctx.simple_query("select id from fodder_b where id = 1")
        .await?;
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // The pinned query must still reach Ready once its population completes.
    ctx.cache_settle_with_timeout(Duration::from_secs(20))
        .await
        .map_err(|e| {
            Error::other(format!(
                "pinned query never settled after an eviction tick during its population: {e}"
            ))
        })?;

    let before = ctx.metrics().await?;
    ctx.simple_query(pinned).await?;
    let after = ctx.metrics().await?;
    assert_eq!(
        after.queries_cache_hit - before.queries_cache_hit,
        1,
        "pinned query is not served from cache after its population"
    );
    Ok(())
}
