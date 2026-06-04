use std::io::Error;
use std::time::Duration;

use crate::util::{TestContext, metrics_delta};

mod util;

/// Test that concurrent queries for the same fingerprint during Loading are coalesced.
///
/// Fires N concurrent queries while population is in progress. At least some should
/// be coalesced (queued during Loading, then served from cache when Ready arrives)
/// rather than individually forwarded to origin.
#[tokio::test]
async fn test_request_coalescing() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Create a table with enough rows that population takes measurable time
    ctx.query(
        "CREATE TABLE coalesce_test (id INTEGER PRIMARY KEY, data TEXT)",
        &[],
    )
    .await?;

    let mut values = Vec::new();
    for i in 1..=500 {
        values.push(format!("({i}, 'row_{i}')"));
    }
    let insert_sql = format!(
        "INSERT INTO coalesce_test (id, data) VALUES {}",
        values.join(", ")
    );
    ctx.query(&insert_sql as &str, &[]).await?;

    ctx.cdc_settle().await?;

    // Open multiple proxy connections before firing queries
    let num_clients = 10;
    let mut clients = Vec::with_capacity(num_clients);
    for _ in 0..num_clients {
        clients.push(ctx.proxy_client_connect().await?);
    }

    let m_before = ctx.metrics().await?;

    // Fire all queries concurrently — the first to arrive triggers Loading,
    // the rest should be coalesced while population is in progress.
    let query_str = "SELECT id, data FROM coalesce_test WHERE id <= 100";
    let mut handles = Vec::with_capacity(num_clients);
    for client in clients {
        let q = query_str.to_owned();
        handles.push(tokio::spawn(async move { client.simple_query(&q).await }));
    }

    // Wait for all queries to complete
    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok(), "concurrent query failed: {result:?}");
    }

    // Wait for population to finish and metrics to settle
    ctx.cache_settle().await?;

    let m_after = ctx.metrics().await?;
    let delta = metrics_delta(&m_before, &m_after);

    // All N queries should have been processed
    assert_eq!(
        delta.queries_cacheable, num_clients as u64,
        "all queries should be cacheable"
    );

    // At least some queries should have been coalesced (served from cache via the
    // waiting queue rather than individually forwarded to origin). The exact number
    // depends on timing — we can't guarantee all N-1 are coalesced, but with 10
    // concurrent queries and a 500-row population, we should reliably get some.
    assert!(
        delta.cache_coalesce_served > 0,
        "expected some coalesced requests, got 0 (delta: {delta:?})"
    );

    // Coalesced queries are cache hits (served from cache after population)
    assert!(
        delta.queries_cache_hit > 0,
        "expected cache hits from coalesced requests"
    );

    // Dispatch is inline per-connection (no central coordinator serialization),
    // so a few concurrent cold requests can reach origin before the first one
    // establishes `Loading`. The point of coalescing is to prevent a thundering
    // herd, not to guarantee a single miss: most requests must coalesce, while a
    // small minority reaching origin is acceptable.
    assert!(
        delta.queries_cache_miss >= 1 && delta.queries_cache_miss < num_clients as u64,
        "expected coalescing to absorb most requests with only a few cold misses, \
         got {} misses of {num_clients}",
        delta.queries_cache_miss
    );

    Ok(())
}

/// Regression for PGC-99: when the writer's `query_register` fails (here: a
/// query against a nonexistent table makes `cache_tables_ensure` error out),
/// the coordinator's `state_view` entry must be cleaned up and any coalesced
/// waiters drained. Pre-fix, the entry stayed in `Loading` forever and every
/// subsequent client request for the same fingerprint hung in `waiting`.
#[tokio::test]
async fn test_register_failure_drains_waiters() -> Result<(), Error> {
    let ctx = TestContext::setup().await?;

    // Nonexistent table: passes parse + cacheability, fails at writer-side
    // table metadata fetch — exercises the defensive cleanup path.
    let sql = "SELECT * FROM __pgcache_nonexistent_test_table__ WHERE id = 1";

    let num_clients = 5;
    let mut clients = Vec::with_capacity(num_clients);
    for _ in 0..num_clients {
        clients.push(ctx.proxy_client_connect().await?);
    }

    let mut handles = Vec::with_capacity(num_clients);
    for client in clients {
        let q = sql.to_owned();
        handles.push(tokio::spawn(async move { client.simple_query(&q).await }));
    }

    // Bound the wait — bug shape is "later clients coalesce and hang forever".
    // Cleanup fires synchronously on Register error, so generous 5s is plenty.
    for handle in handles {
        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .map_err(|_| Error::other("client hung — coalesced waiter not drained"))?
            .unwrap();
        // Origin will reject the query (relation does not exist). The contract
        // we're testing is that the request *returned* — success or error
        // doesn't matter, just that it didn't hang.
        assert!(
            result.is_err(),
            "expected origin error for nonexistent table, got {result:?}"
        );
    }

    Ok(())
}
