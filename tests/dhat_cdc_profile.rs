//! dhat allocation-profiling driver for the CDC apply path — NOT a correctness test.
//!
//! Registers cached queries over `cdc_bench`, then issues a fixed mix of
//! INSERT/UPDATE/DELETE directly against the ORIGIN (bypassing the proxy) so
//! the profiled binary's heap traffic is dominated by per-CDC-event decode and
//! in-place cache apply rather than the client serve path. Each statement is
//! its own origin transaction, so the per-source-txn BEGIN/COMMIT framing is
//! included in the per-event cost. Single-table queries are maintained
//! in-place (never invalidated), so every event takes the apply path.
//!
//! `DHAT_CDC_QUERIES` (default 0) also registers that many point queries of
//! a join between `cdc_bench` and a partner table, each keyed on the
//! partner's id. A join's membership is evaluated on the cache DB, and a
//! point on the partner says nothing about a `cdc_bench` row, so every one
//! of them is a candidate on every event: the per-event apply cost then
//! scales with the count, as it does under a workload with many cached
//! joins on a relation.
//!
//! Build with the `dhat-heap` feature so the spawned pgcache binary uses
//! `dhat::Alloc` and writes `dhat-heap.json` on a clean (SIGINT) exit.
//!
//! Run:
//!   DHAT_CDC_EVENTS=30000 DHAT_CDC_QUERIES=100 cargo test --features dhat-heap \
//!     --test dhat_cdc_profile -- --ignored --nocapture --test-threads=1
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
async fn test_dhat_cdc_apply_workload() -> Result<(), Error> {
    let events: usize = std::env::var("DHAT_CDC_EVENTS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30_000);
    let rounds = events / 3;
    let join_queries: usize = std::env::var("DHAT_CDC_QUERIES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

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

    if join_queries > 0 {
        ctx.query(
            "CREATE TABLE cdc_partner (id INTEGER PRIMARY KEY, cdc_id INTEGER)",
            &[],
        )
        .await?;
        // Partners point at the ids the rounds below write, so the joins
        // match and the hit side of the apply path runs too.
        ctx.origin_query(
            "INSERT INTO cdc_partner (id, cdc_id) \
             SELECT g, 1000000 + (g % 1000) FROM generate_series(1, $1) g",
            &[&i32::try_from(join_queries).expect("query count fits in i32")],
        )
        .await?;
        // Two passes: the registration-rate gate forwards part of a burst of
        // brand-new queries, and the second pass registers what it forwarded.
        for _ in 0..2 {
            for k in 1..=join_queries {
                let sql = format!(
                    "SELECT b.id, b.val, p.id FROM cdc_bench b \
                     JOIN cdc_partner p ON p.cdc_id = b.id WHERE p.id = {k}"
                );
                let _ = ctx.query(&sql, &[]).await?;
            }
            ctx.cache_settle_with_timeout(Duration::from_secs(120))
                .await?;
        }
    }

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
    let _ = ctx
        .cdc_apply_settle_with_timeout(Duration::from_secs(240))
        .await;

    // Sanity-check the workload: raw CDC counters from the metrics endpoint,
    // so per-event allocation counts in the profile have a trusted denominator.
    // NOTE: this scrape drains the run's buffered histogram samples into the
    // Prometheus summary sketches in one go — the resulting sketches_ddsketch
    // allocations in the heap profile are scrape cost, not apply-path cost.
    if let Ok((_, body)) = util::http_get(ctx.metrics_port, "/metrics").await {
        for line in body.lines() {
            if (line.starts_with("pgcache_cdc_") || line.starts_with("pgcache_cache_cdc_"))
                && !line.starts_with('#')
            {
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
