//! dhat allocation-profiling driver for high-cardinality query REGISTRATION.
//!
//! Reproduces the per-distinct-literal in-process memory growth that drives
//! pgcache RSS toward OOM under workloads like YCSB point reads, where every
//! distinct literal (`... WHERE key = '<lit>'`) registers as its own query and
//! accumulates per-query state (state-view entry, metrics histogram,
//! subsumption-index membership, and the admitted CachedQuery).
//!
//! Unlike `dhat_profile.rs` (which profiles the steady-state serve/hit path),
//! this driver profiles the REGISTRATION path: it issues N distinct single-
//! literal point queries once each, so the heap snapshot is dominated by the
//! per-registered-query structures that grow without bound.
//!
//! Build with the `dhat-heap` feature so the spawned pgcache binary uses
//! `dhat::Alloc` and writes `dhat-heap.json` on a clean (SIGINT) exit.
//!
//! Run:
//!   REG_QUERIES=3000 cargo test --features dhat-heap --test dhat_registration_profile \
//!     -- --ignored --nocapture --test-threads=1
//! then collect pgcache/dhat-heap.json. Divide the heap total by REG_QUERIES for
//! the per-registered-query coefficient.

use std::io::Error;
use std::process::Command;
use std::time::Duration;

use crate::util::TestContext;

mod util;

#[tokio::test]
#[ignore = "profiling driver; run explicitly with --features dhat-heap"]
async fn dhat_registration_workload() -> Result<(), Error> {
    let n: i32 = std::env::var("REG_QUERIES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3000);

    let mut ctx = TestContext::setup().await?;

    ctx.query("CREATE TABLE bench (id INTEGER PRIMARY KEY, val TEXT)", &[])
        .await?;
    // Bulk-load N rows on origin so each point read returns one row.
    ctx.origin_query(
        "INSERT INTO bench (id, val) SELECT g, 'val-' || g FROM generate_series(0, $1) g",
        &[&n],
    )
    .await?;

    // Register N distinct single-literal point queries. Each unique literal is a
    // distinct fingerprint → first miss → registration, accumulating the
    // per-query in-process state under profile.
    for i in 0..n {
        let sql = format!("SELECT id, val FROM bench WHERE id = {i}");
        let _ = ctx.query(&sql, &[]).await?;
    }

    // Let the writer finish processing all registrations/populations so the
    // steady-state in-process footprint is realized before the heap snapshot.
    // Non-fatal: a settle timeout must not skip the SIGINT-triggered dhat flush.
    let _ = ctx
        .cache_settle_with_timeout(Duration::from_secs(240))
        .await;

    eprintln!("registered ~{n} distinct queries; sending SIGINT for dhat flush");
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
