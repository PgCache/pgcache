use std::io::Error;
use std::time::Duration;

use tokio_postgres::SimpleQueryMessage;

use crate::util::TestContext;

mod util;

/// Stress the coalesce enqueue/drain race.
///
/// Many concurrent connections request the same uncached fingerprint. The first
/// admits the query (state `Loading`) and triggers population; the rest observe
/// `Loading` and enqueue as coalesced waiters. With `--features fault-injection`
/// armed via `PGCACHE_FAULT_COALESCE_DELAY`, a delay is inserted between the
/// `Loading` observation and the enqueue, so population completes and the
/// `Ready` notify drains the waiting queue *before* the waiters enqueue.
///
/// Without the re-check-under-lock fix, such a late waiter is orphaned — its
/// query never completes and the request hangs (caught here as a timeout). With
/// the fix, the waiter observes `Ready` under the lock and serves itself.
///
/// Built without the feature this is a plain concurrent-coalescing smoke test.
#[tokio::test]
async fn test_coalesce_enqueue_drain_race() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&[("PGCACHE_FAULT_COALESCE_DELAY", "1")]).await?;

    ctx.query(
        "CREATE TABLE race_test (id INTEGER PRIMARY KEY, data TEXT)",
        &[],
    )
    .await?;
    let values: Vec<String> = (1..=500).map(|i| format!("({i}, 'row_{i}')")).collect();
    ctx.query(
        &format!(
            "INSERT INTO race_test (id, data) VALUES {}",
            values.join(", ")
        ) as &str,
        &[],
    )
    .await?;
    ctx.cdc_settle().await?;

    // Each round uses a distinct literal → a distinct (cold) fingerprint, so
    // every round exercises a fresh Loading → coalesce → drain cycle.
    for round in 0..3u32 {
        let bound = 50 + round;
        let query = format!("SELECT id, data FROM race_test WHERE id <= {bound}");
        let expected = bound as usize;

        let num_clients = 8;
        let mut clients = Vec::with_capacity(num_clients);
        for _ in 0..num_clients {
            clients.push(ctx.proxy_client_connect().await?);
        }

        let mut handles = Vec::with_capacity(num_clients);
        for client in clients {
            let q = query.clone();
            handles.push(tokio::spawn(async move {
                tokio::time::timeout(Duration::from_secs(15), client.simple_query(&q)).await
            }));
        }

        for (i, handle) in handles.into_iter().enumerate() {
            let timed = handle.await.expect("client task panicked");
            let result = timed.unwrap_or_else(|_| {
                panic!("round {round} client {i}: query hung — orphaned coalesce waiter")
            });
            let messages = result.expect("query failed");
            let row_count = messages
                .iter()
                .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
                .count();
            assert_eq!(
                row_count, expected,
                "round {round} client {i}: wrong row count"
            );
        }
    }

    Ok(())
}
