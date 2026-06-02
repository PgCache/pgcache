//! Measurement driver — NOT a correctness test.
//!
//! Verifies the Postgres-side benefit of named prepared statements on the binary
//! serve path: the cache DB should PLAN the served SELECT roughly once (per pool
//! connection), not on every execution. Enables `pg_stat_statements.track_planning`
//! on the cache DB, runs a binary cache-hit workload through the proxy, then
//! reports `plans` vs `calls` for the served SELECT.
//!
//! Expectation: current (prepared) → plans ≪ calls; unnamed baseline → plans ≈ calls.
//!
//! Run (same on baseline and current):
//!   HITS=3000 cargo test --test prepared_stmt_planning -- --ignored --nocapture --test-threads=1

use std::io::Error;
use std::time::Duration;

use crate::util::{TestContext, connect_cache_db};

mod util;

#[tokio::test]
#[ignore = "measurement driver; run explicitly"]
async fn prepared_stmt_planning_measure() -> Result<(), Error> {
    let hits: usize = std::env::var("HITS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3000);

    let mut ctx = TestContext::setup().await?;
    ctx.query(
        "CREATE TABLE bench (id INTEGER PRIMARY KEY, val TEXT, n INTEGER)",
        &[],
    )
    .await?;
    for i in 0..1000i32 {
        ctx.query(
            "INSERT INTO bench (id, val, n) VALUES ($1, $2, $3)",
            &[&i, &format!("val-{i}"), &(i % 10)],
        )
        .await?;
    }

    // Enable planning tracking cluster-wide on the cache DB and reload so the
    // already-connected worker pool backends pick it up.
    let cache = connect_cache_db(&ctx.dbs).await?;
    // Single-statement simple queries: ALTER SYSTEM can't run in a transaction
    // block (which a multi-statement batch would form).
    cache
        .simple_query("ALTER SYSTEM SET pg_stat_statements.track_planning = 'on'")
        .await
        .map_err(Error::other)?;
    cache
        .simple_query("SELECT pg_reload_conf()")
        .await
        .map_err(Error::other)?;

    let q = "SELECT id, val FROM bench WHERE n = 3 ORDER BY id";

    // Populate (miss → origin) + settle so the loop below is all cache hits.
    ctx.query(q, &[]).await?;
    ctx.cache_settle().await?;
    // Let the config reload reach the pool backends, then isolate the hit phase.
    tokio::time::sleep(Duration::from_millis(500)).await;
    cache
        .execute("SELECT pg_stat_statements_reset()", &[])
        .await
        .map_err(Error::other)?;

    // MODE=simple drives the text serve path (simple 'Q'); default exercises the
    // binary/extended path. Both now route through the unified named-statement
    // serve, so both should amortize planning.
    let simple = std::env::var("MODE").is_ok_and(|m| m == "simple");
    for _ in 0..hits {
        if simple {
            let _ = ctx.simple_query(q).await?;
        } else {
            let _ = ctx.query(q, &[]).await?;
        }
    }

    let rows = cache
        .query(
            "SELECT calls, plans, total_plan_time, total_exec_time, left(query, 90) AS query \
             FROM pg_stat_statements \
             WHERE query ILIKE '%bench%' \
             ORDER BY calls DESC LIMIT 10",
            &[],
        )
        .await
        .map_err(Error::other)?;

    let mode_label = if simple {
        "simple/text"
    } else {
        "binary/extended"
    };
    println!("\n=== cache-DB pg_stat_statements after {hits} {mode_label} cache hits ===");
    for r in &rows {
        let calls: i64 = r.get("calls");
        let plans: i64 = r.get("plans");
        let total_plan_ms: f64 = r.get("total_plan_time");
        let total_exec_ms: f64 = r.get("total_exec_time");
        let query: String = r.get("query");
        println!(
            "calls={calls} plans={plans} total_plan_time={total_plan_ms:.2}ms total_exec_time={total_exec_ms:.2}ms\n  query: {query}"
        );
    }
    println!("=== end ===\n");

    Ok(())
}
