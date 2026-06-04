// The whole suite needs the fault-injection hook to kill the writer; without the
// feature it would compile to unused imports/consts, so gate the entire file.
#![cfg(feature = "fault-injection")]

use std::io::Error;
use std::time::{Duration, Instant};

use crate::util::TestContext;

mod util;

/// Must match `cache::writer::core::fault::WRITER_DIE_SENTINEL`. A CDC insert
/// carrying this value in any column trips the one-shot writer-death fault.
const WRITER_DIE_SENTINEL: &str = "__PGCACHE_WRITER_DIE__";

/// Exercise the cache restart supervisor (PGC-239 #5) end to end: a fatal writer
/// failure must tear the cache subsystem down and the supervisor must rebuild it,
/// after which the cache serves again.
///
/// With `--features fault-injection` armed via `PGCACHE_FAULT_WRITER_DIE`, the
/// writer exits the first time it sees a CDC insert carrying the sentinel value.
/// That death propagates (writer notify channel closes → subsystem cancel), the
/// supervisor reaps the dead generation and rebuilds it. We assert the rebuild
/// happened (`cache_restarts_total`) and that the rebuilt cache serves hits.
///
/// Built without the feature the sentinel insert is inert, so the writer never
/// dies and the "did it restart" wait times out — hence the test is gated to the
/// fault-injection build.
#[tokio::test]
async fn test_cache_restart_recovers_after_writer_death() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&[("PGCACHE_FAULT_WRITER_DIE", "1")]).await?;

    ctx.query(
        "CREATE TABLE restart_t (id INTEGER PRIMARY KEY, data TEXT)",
        &[],
    )
    .await?;
    let values: Vec<String> = (1..=20).map(|i| format!("({i}, 'row_{i}')")).collect();
    ctx.query(
        &format!("INSERT INTO restart_t VALUES {}", values.join(", ")) as &str,
        &[],
    )
    .await?;
    ctx.cdc_settle().await?;

    // The query range excludes the sentinel row (id 999), so its result is
    // identical before and after the restart.
    let q = "SELECT id, data FROM restart_t WHERE id <= 10";

    // Warm the cache and confirm it serves a hit in generation 1. Registering the
    // query also adds restart_t to the publication, so the sentinel insert below
    // produces a CDC insert that reaches the writer.
    ctx.query(q, &[]).await?;
    ctx.cache_settle().await?;
    let before = ctx.metrics().await?;
    let rows = ctx.query(q, &[]).await?;
    assert_eq!(rows.len(), 10, "query returns 10 rows");
    let after = ctx.metrics().await?;
    assert!(
        after.queries_cache_hit > before.queries_cache_hit,
        "query is served from cache before the restart"
    );
    assert_eq!(after.cache_restarts_total, 0, "no restart has happened yet");

    // Trigger a fatal writer failure via a sentinel CDC insert.
    ctx.query(
        &format!("INSERT INTO restart_t VALUES (999, '{WRITER_DIE_SENTINEL}')") as &str,
        &[],
    )
    .await?;

    // The supervisor must rebuild the cache subsystem.
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if ctx.metrics().await?.cache_restarts_total >= 1 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "cache subsystem did not restart after writer death"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // The rebuilt cache starts cold. Confirm it repopulates and serves the query
    // from cache again — a permanently-degraded cache would forward to origin
    // forever and never register a hit.
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let before = ctx.metrics().await?;
        assert_eq!(ctx.query(q, &[]).await?.len(), 10, "correct rows after restart");
        ctx.cache_settle().await?;
        assert_eq!(ctx.query(q, &[]).await?.len(), 10, "correct rows after restart");
        if ctx.metrics().await?.queries_cache_hit > before.queries_cache_hit {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "cache did not serve hits again after restart"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Ok(())
}
