//! dhat allocation-profiling driver for the CDC apply path — NOT a correctness test.
//!
//! Registers single-table cached queries, then issues a fixed mix of
//! INSERT/UPDATE/DELETE directly against the ORIGIN (bypassing the proxy) so
//! the profiled binary's heap traffic is dominated by per-CDC-event decode and
//! in-place cache apply rather than the client serve path. Each statement is
//! its own origin transaction, so the per-source-txn BEGIN/COMMIT framing is
//! included in the per-event cost. Single-table queries are maintained
//! in-place (never invalidated), so every event takes the apply path.
//!
//! Build with the `dhat-heap` feature so the spawned pgcache binary uses
//! `dhat::Alloc` and writes `dhat-heap.json` on a clean (SIGINT) exit.
//!
//! Run:
//!   DHAT_CDC_EVENTS=30000 cargo test --features dhat-heap --test dhat_cdc_profile \
//!     -- --ignored --nocapture --test-threads=1
//! then collect pgcache/dhat-heap.json. The event count is rounded down to a
//! multiple of 3 (each round = one INSERT + one UPDATE + one DELETE); sites
//! with ~N or ~N/3 blocks are per-event allocations.

use std::io::Error;
use std::process::Command;
use std::time::Duration;

use crate::util::TestContext;

mod util;

#[tokio::test]
#[ignore = "profiling driver; run explicitly with --features dhat-heap"]
async fn dhat_cdc_apply_workload() -> Result<(), Error> {
    let events: usize = std::env::var("DHAT_CDC_EVENTS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30_000);
    let rounds = events / 3;

    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE cdc_bench (id INTEGER PRIMARY KEY, val TEXT, n INTEGER)",
        &[],
    )
    .await?;
    ctx.origin_query(
        "INSERT INTO cdc_bench (id, val, n) SELECT g, 'val-' || g, g % 10 FROM generate_series(0, 999) g",
        &[],
    )
    .await?;

    // Register a cached single-table query so cdc_bench is tracked and every
    // subsequent CDC event is applied in-place to the cache copy.
    let _ = ctx
        .query("SELECT id, val FROM cdc_bench WHERE n = 3 ORDER BY id", &[])
        .await?;
    ctx.cache_settle().await?;

    // Steady-state CDC events: each round inserts a fresh row matching the
    // cached predicate, updates it, then deletes it — table size stays
    // bounded and insert/update/delete events arrive in equal proportion.
    for round in 0..rounds {
        let id = 1_000_000 + i32::try_from(round % 1000).expect("round id fits in i32");
        ctx.origin_query(
            "INSERT INTO cdc_bench (id, val, n) VALUES ($1, $2, 3)",
            &[&id, &format!("ins-{round}")],
        )
        .await?;
        ctx.origin_query(
            "UPDATE cdc_bench SET val = $2 WHERE id = $1",
            &[&id, &format!("upd-{round}")],
        )
        .await?;
        ctx.origin_query("DELETE FROM cdc_bench WHERE id = $1", &[&id])
            .await?;
    }

    // Let the writer drain the replication stream so every event's apply cost
    // lands in the heap profile. Non-fatal: a settle timeout must not skip the
    // SIGINT-triggered dhat flush.
    let _ = ctx.cdc_settle_with_timeout(Duration::from_secs(240)).await;

    // Sanity-check the workload: raw CDC counters from the metrics endpoint,
    // so per-event allocation counts in the profile have a trusted denominator.
    // NOTE: this scrape drains the run's buffered histogram samples into the
    // Prometheus summary sketches in one go — the resulting sketches_ddsketch
    // allocations in the heap profile are scrape cost, not apply-path cost.
    if let Ok((_, body)) = util::http_get(ctx.metrics_port, "/metrics").await {
        for line in body.lines() {
            if line.starts_with("pgcache_cdc_") && !line.starts_with('#') {
                eprintln!("{line}");
            }
        }
    }

    eprintln!(
        "applied ~{} CDC events; sending SIGINT for dhat flush",
        rounds * 3
    );
    let pid = ctx.pgcache.id();
    Command::new("kill")
        .arg("-INT")
        .arg(pid.to_string())
        .status()
        .expect("send SIGINT to pgcache");
    ctx.pgcache
        .wait()
        .expect("wait for pgcache graceful exit (dhat flush)");

    Ok(())
}
