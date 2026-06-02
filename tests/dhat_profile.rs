//! dhat allocation-profiling driver — NOT a correctness test.
//!
//! Build with the `dhat-heap` feature so the spawned pgcache binary uses
//! `dhat::Alloc` and writes `dhat-heap.json` on a clean exit. This driver runs a
//! fixed binary cache-hit workload (so steady-state serve-path allocations
//! dominate startup noise), then gracefully stops pgcache with SIGINT (the
//! harness `Drop` SIGKILLs, which would skip dhat's flush) and waits for it to
//! write the json into the package dir.
//!
//! Run (same invocation on baseline and current commits):
//!   DHAT_HITS=30000 cargo test --features dhat-heap --test dhat_profile \
//!     -- --ignored --nocapture --test-threads=1
//! then collect pgcache/dhat-heap.json.

use std::io::Error;
use std::process::Command;

use crate::util::TestContext;

mod util;

#[tokio::test]
#[ignore = "profiling driver; run explicitly with --features dhat-heap"]
async fn dhat_binary_serve_workload() -> Result<(), Error> {
    let hits: usize = std::env::var("DHAT_HITS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30_000);

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

    // Two cacheable queries served over the binary/extended protocol: one
    // unlimited, one with LIMIT/OFFSET (exercises the parameterized-limit path).
    let q_unlimited = "SELECT id, val FROM bench WHERE n = 3 ORDER BY id";
    let q_limited = "SELECT id, val FROM bench WHERE n = 7 ORDER BY id LIMIT 20 OFFSET 5";

    // Populate (miss) + settle so the loop below is all cache hits.
    ctx.query(q_unlimited, &[]).await?;
    ctx.query(q_limited, &[]).await?;
    ctx.cache_settle().await?;

    // Steady-state binary cache hits, alternating limited/unlimited.
    for i in 0..hits {
        let q = if i % 2 == 0 { q_unlimited } else { q_limited };
        let _ = ctx.query(q, &[]).await?;
    }

    // Graceful shutdown so dhat flushes dhat-heap.json: SIGINT makes pgcache
    // cancel and return from main, dropping the dhat profiler.
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
