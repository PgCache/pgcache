//! Serve must not block on a slow population (PGC-335 fix A).
//!
//! When a query is `Loading`, concurrent requests coalesce onto the in-flight
//! population. A fast population still coalesces (thundering-herd protection),
//! but a slow one must not hold its waiters hostage: once a waiter passes the
//! forward deadline it degrades to origin while the population completes in the
//! background. These tests arm `PGCACHE_FAULT_POPULATION_DELAY_MS` to make
//! population deterministically fast or slow and assert which path each waiter
//! takes via the coalesce metrics.
#![cfg(feature = "fault-injection")]

use std::io::Error;
use std::time::{Duration, Instant};

use crate::util::{TestContext, metrics_delta};

mod util;

const FANOUT: usize = 8;

/// Fire `FANOUT` concurrent identical reads; return how long the slowest took.
async fn concurrent_reads(ctx: &TestContext, query: &str) -> Result<Duration, Error> {
    let mut clients = Vec::with_capacity(FANOUT);
    for _ in 0..FANOUT {
        clients.push(ctx.proxy_client_connect().await?);
    }
    let started = Instant::now();
    let mut handles = Vec::with_capacity(FANOUT);
    for client in clients {
        let q = query.to_owned();
        handles.push(tokio::spawn(async move {
            tokio::time::timeout(Duration::from_secs(20), client.simple_query(&q)).await
        }));
    }
    for handle in handles {
        // Outer `?`: task join. Middle: a timeout means a waiter hung past the
        // deadline (the bug under test). Inner: the query itself failed.
        handle
            .await
            .map_err(Error::other)?
            .map_err(|_| Error::other("query hung past the forward deadline"))?
            .map_err(Error::other)?;
    }
    Ok(started.elapsed())
}

/// A fast population (under the cold deadline) is waited out and served from
/// cache — coalescing still protects the origin from a thundering herd, and no
/// waiter is prematurely forwarded.
#[tokio::test]
async fn test_fast_population_serves_coalesced() -> Result<(), Error> {
    // 50 ms population: enough of a Loading window for followers to coalesce,
    // with wide headroom under the 200 ms cold forward deadline so real
    // fetch+stage overhead can't push a waiter past it and forward spuriously.
    let mut ctx = TestContext::setup_fault(&[("PGCACHE_FAULT_POPULATION_DELAY_MS", "50")]).await?;
    ctx.query("CREATE TABLE fast_pop (id INT PRIMARY KEY, v TEXT)", &[])
        .await?;
    let vals: Vec<String> = (1..=100).map(|i| format!("({i}, 'v{i}')")).collect();
    ctx.query(
        &format!("INSERT INTO fast_pop (id, v) VALUES {}", vals.join(", ")) as &str,
        &[],
    )
    .await?;
    ctx.cdc_settle().await?;

    let before = ctx.metrics().await?;
    concurrent_reads(&ctx, "SELECT id, v FROM fast_pop WHERE id <= 50").await?;
    ctx.cache_settle().await?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);

    assert!(
        delta.cache_coalesce_served > 0,
        "fast population should serve coalesced waiters from cache, got {delta:?}"
    );
    assert_eq!(
        delta.cache_coalesce_deadline_forward, 0,
        "fast population must not forward any waiter on the deadline, got {delta:?}"
    );
    Ok(())
}

/// A slow cold population forwards its coalesced waiters to origin at the cold
/// deadline rather than blocking for the full population — serve latency is
/// decoupled from population latency.
#[tokio::test]
async fn test_slow_cold_population_forwards_on_deadline() -> Result<(), Error> {
    // 3 s population vs 200 ms cold deadline: waiters forward ~14× sooner.
    let mut ctx =
        TestContext::setup_fault(&[("PGCACHE_FAULT_POPULATION_DELAY_MS", "3000")]).await?;
    ctx.query("CREATE TABLE slow_cold (id INT PRIMARY KEY, v TEXT)", &[])
        .await?;
    let vals: Vec<String> = (1..=100).map(|i| format!("({i}, 'v{i}')")).collect();
    ctx.query(
        &format!("INSERT INTO slow_cold (id, v) VALUES {}", vals.join(", ")) as &str,
        &[],
    )
    .await?;
    ctx.cdc_settle().await?;

    let before = ctx.metrics().await?;
    let elapsed = concurrent_reads(&ctx, "SELECT id, v FROM slow_cold WHERE id <= 50").await?;

    // The waiters must not have blocked for the 3 s population.
    assert!(
        elapsed < Duration::from_millis(2000),
        "reads blocked on the slow population ({elapsed:?}); serve is still coupled to populate"
    );

    ctx.cache_settle_with_timeout(Duration::from_secs(10))
        .await?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);

    assert!(
        delta.cache_coalesce_deadline_forward > 0,
        "slow cold population should forward coalesced waiters on the deadline, got {delta:?}"
    );
    assert_eq!(
        delta.cache_coalesce_served, 0,
        "no waiter should have been served from cache before the deadline, got {delta:?}"
    );
    Ok(())
}

/// A slow re-population (after invalidation) also forwards on the deadline. The
/// deadline here is derived from the query's observed fetch+stage estimate (the
/// re-pop path), distinct from the cold fixed deadline — but the observable
/// contract is the same: waiters degrade to origin instead of blocking.
#[tokio::test]
async fn test_slow_repopulation_forwards_on_deadline() -> Result<(), Error> {
    let mut ctx =
        TestContext::setup_fault(&[("PGCACHE_FAULT_POPULATION_DELAY_MS", "3000")]).await?;
    ctx.query("CREATE TABLE authors (id INT PRIMARY KEY, name TEXT)", &[])
        .await?;
    ctx.query(
        "CREATE TABLE books (id INT PRIMARY KEY, author_id INT, title TEXT)",
        &[],
    )
    .await?;
    let authors: Vec<String> = (1..=10).map(|i| format!("({i}, 'a{i}')")).collect();
    ctx.query(
        &format!(
            "INSERT INTO authors (id, name) VALUES {}",
            authors.join(", ")
        ) as &str,
        &[],
    )
    .await?;
    let books: Vec<String> = (1..=20)
        .map(|i| format!("({i}, {}, 't{i}')", (i % 10) + 1))
        .collect();
    ctx.query(
        &format!(
            "INSERT INTO books (id, author_id, title) VALUES {}",
            books.join(", ")
        ) as &str,
        &[],
    )
    .await?;
    ctx.cdc_settle().await?;

    let query = "SELECT b.id, b.title, a.name FROM books b JOIN authors a ON b.author_id = a.id WHERE a.id <= 5";

    // Warm the join to Ready (the cold population records the fetch+stage
    // estimate that will set the re-pop deadline).
    {
        let client = ctx.proxy_client_connect().await?;
        let _ = client.simple_query(query).await.map_err(Error::other)?;
    }
    ctx.cache_settle_with_timeout(Duration::from_secs(10))
        .await?;

    // Invalidate by inserting a book that grows the join result.
    ctx.query(
        "INSERT INTO books (id, author_id, title) VALUES (21, 3, 'grow')",
        &[],
    )
    .await?;
    ctx.cdc_settle().await?;

    let before = ctx.metrics().await?;
    concurrent_reads(&ctx, query).await?;
    ctx.cache_settle_with_timeout(Duration::from_secs(10))
        .await?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);

    assert!(
        delta.cache_coalesce_deadline_forward > 0,
        "slow re-population should forward coalesced waiters on the deadline, got {delta:?}"
    );
    Ok(())
}
