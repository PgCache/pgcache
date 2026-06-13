// The whole suite needs the fault-injection hook to kill the writer; without the
// feature it would compile to unused imports/consts, so gate the entire file.
#![cfg(feature = "fault-injection")]

use std::io::Error;
use std::time::{Duration, Instant};

use crate::util::{TestContext, http_get, pgcache_client_connect};

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
        assert_eq!(
            ctx.query(q, &[]).await?.len(),
            10,
            "correct rows after restart"
        );
        ctx.cache_settle().await?;
        assert_eq!(
            ctx.query(q, &[]).await?.len(),
            10,
            "correct rows after restart"
        );
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

/// A connection that dispatches a cacheable query while the cache is down must
/// survive the cache's recovery. The dispatch-unavailable fallback discards an
/// armed `ReplySender`, leaving a stale permit in the connection's reusable
/// reply slot; the first query dispatched after recovery wakes on that permit
/// and must read it as `Empty` and keep waiting — not misclassify it as
/// cache-died-in-flight and drop a healthy client connection.
///
/// The post-recovery dispatch must be a worker-served HIT: misses and
/// registrations are replied inline during dispatch, before the wait first
/// polls, so the already-delivered reply masks the stale permit. A second
/// connection re-warms the rebuilt cache so the planted connection's next
/// dispatch goes to the worker asynchronously.
#[tokio::test]
async fn test_connection_survives_query_during_restart_window() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&[("PGCACHE_FAULT_WRITER_DIE", "1")]).await?;

    ctx.simple_query("create table restart_w (id int primary key, data text)")
        .await?;
    ctx.simple_query("insert into restart_w values (1, 'a'), (2, 'b')")
        .await?;
    ctx.cdc_settle().await?;

    let q = "select id, data from restart_w where id <= 2";
    ctx.simple_query(q).await?;
    ctx.cache_settle().await?;

    // Kill the writer via the sentinel insert.
    ctx.simple_query(&format!(
        "insert into restart_w values (999, '{WRITER_DIE_SENTINEL}')"
    ))
    .await?;

    // Wait for the death to be observable (/status 503s once the writer's
    // status channel closes), then give the supervisor a beat to clear the
    // dispatch. The down window stays open well past this: the supervisor
    // sleeps its 500ms restart backoff before even starting the rebuild.
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let (status, _) = http_get(ctx.metrics_port, "/status").await?;
        if status == 503 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "/status never reported the cache down"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(250)).await;

    // Down-window reads on the SAME connection: each takes the degraded
    // forward (discarding an armed reply sender — the stale-permit plant) and
    // must still serve correct rows from origin.
    let before_window = ctx.metrics().await?;
    for _ in 0..3 {
        let served = ctx.simple_query(q).await?;
        let rows = served
            .iter()
            .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
            .count();
        assert_eq!(rows, 2, "wrong rows from the degraded forward");
    }
    let after_window = ctx.metrics().await?;
    assert_eq!(
        after_window.queries_cacheable - before_window.queries_cacheable,
        3,
        "down-window reads were not classified cacheable"
    );
    assert_eq!(
        (after_window.queries_cache_hit
            + after_window.queries_cache_miss
            + after_window.queries_cache_error)
            - (before_window.queries_cache_hit
                + before_window.queries_cache_miss
                + before_window.queries_cache_error),
        0,
        "a down-window read dispatched into the cache; the scenario did not \
         exercise the degraded forward"
    );

    // Wait for the supervisor to rebuild.
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

    // Re-warm the rebuilt (cold) cache from a second connection until the
    // query is a hit again. The planted connection stays idle so nothing
    // absorbs its stale permit.
    let warm = pgcache_client_connect(ctx.cache_port).await?;
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let before = ctx.metrics().await?;
        warm.simple_query(q).await.map_err(Error::other)?;
        if ctx.metrics().await?.queries_cache_hit > before.queries_cache_hit {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "cache did not serve hits again after restart"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // The planted connection's next dispatch is now an async worker hit. Its
    // wait wakes first on the stale permit; reading it as `Empty` it re-arms
    // and serves the hit. Under the bug the wait concludes
    // cache-died-in-flight and tears the connection down — the worker has
    // already written the rows to the leased client socket, so THIS query
    // still returns fine; the death surfaces as an uncounted hit (the
    // connection dies before `handle_cache_outcome`) and a dead connection
    // for the follow-up query.
    let before = ctx.metrics().await?;
    let served = ctx.simple_query(q).await?;
    let rows = served
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(rows, 2, "wrong rows after recovery");
    let after = ctx.metrics().await?;
    assert_eq!(
        after.queries_cache_hit - before.queries_cache_hit,
        1,
        "post-recovery hit went uncounted: the connection died on the stale \
         reply-slot permit (or the scenario no longer dispatches an async hit)"
    );
    ctx.simple_query(q)
        .await
        .expect("planted connection alive after the post-recovery hit");

    Ok(())
}
