// The whole suite needs the fault-injection hook to drive poisons; without the
// feature it would compile to unused imports/consts, so gate the entire file.
#![cfg(feature = "fault-injection")]

use std::io::Error;
use std::time::{Duration, Instant};

use crate::util::TestContext;

mod util;

/// Poisons to induce. The test pool is `num_workers * 2 = 4`, so this forces
/// several full deplete/replenish cycles — without replenishment the pool would
/// be empty after 4 discards and the next serve would hang forever.
const POISONS: u64 = 10;

/// Verify the cache-DB serve pool is self-healing (PGC-238): a poisoned
/// connection is discarded *and* a fresh one reconnected, so repeated poisons
/// can't permanently shrink the pool or wedge the worker loop.
///
/// With `--features fault-injection` armed via `PGCACHE_FAULT_POISON_SERVES=N`,
/// the first N cache-hit serves are poisoned (connection discarded, request
/// falls through to origin). Each discard signals the replenish task to
/// reconnect a replacement. We assert `pool_replenished` reaches N — which can
/// only happen if every poisoned serve got a connection (i.e. the pool never
/// stayed empty) — and that results stay correct throughout.
///
/// Built without the feature the poison is inert, so `pool_replenished` never
/// advances and the wait times out — hence the test is gated to fault-injection.
#[tokio::test]
async fn test_serve_pool_replenishes_after_poison() -> Result<(), Error> {
    let mut ctx =
        TestContext::setup_fault(&[("PGCACHE_FAULT_POISON_SERVES", &POISONS.to_string())]).await?;

    ctx.query(
        "CREATE TABLE pool_t (id INTEGER PRIMARY KEY, data TEXT)",
        &[],
    )
    .await?;
    let values: Vec<String> = (1..=10).map(|i| format!("({i}, 'row_{i}')")).collect();
    ctx.query(
        &format!("INSERT INTO pool_t VALUES {}", values.join(", ")) as &str,
        &[],
    )
    .await?;
    ctx.cdc_settle().await?;

    let q = "SELECT id, data FROM pool_t WHERE id <= 5";

    // Register + populate. The miss/population path does not go through the serve
    // pool, so it isn't poisoned.
    ctx.query(q, &[]).await?;
    ctx.cache_settle().await?;

    // Drive cache-hit serves: each is poisoned (discard → forward to origin →
    // replenish) until the budget is spent. Results must stay correct, and the
    // pool must replace every discarded connection.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        assert_eq!(
            ctx.query(q, &[]).await?.len(),
            5,
            "correct rows under poison churn"
        );
        let replenished = ctx.metrics().await?.cache_pool_replenished;
        if replenished >= POISONS {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "serve pool did not replenish (only {replenished}/{POISONS}) — likely wedged"
        );
    }

    // Poison budget spent: further serves come from cache and trigger no more
    // replenishment, confirming the pool is whole and serving again.
    let before = ctx.metrics().await?;
    for _ in 0..5 {
        assert_eq!(
            ctx.query(q, &[]).await?.len(),
            5,
            "correct rows after healing"
        );
    }
    let after = ctx.metrics().await?;
    assert_eq!(
        after.cache_pool_replenished, before.cache_pool_replenished,
        "no further replenishment once the cache serves from the pool again"
    );

    Ok(())
}
