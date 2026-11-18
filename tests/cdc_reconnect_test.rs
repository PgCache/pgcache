//! A dropped replication connection: pgcache reattaches to its slot and
//! resumes the stream with the cache intact, instead of restarting cold.

use std::io::Error;
use std::time::{Duration, Instant};

use tokio_postgres::Client;

use crate::util::TestContext;

mod util;

/// Slot name from `tests/data/default_config.toml`.
const SLOT: &str = "slot_test";

const READ: &str = "SELECT id, data FROM reconnect_t WHERE id <= 100";

/// A proxy serving `READ` from cache, with two rows in the table.
async fn serving_context() -> Result<TestContext, Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(
        "CREATE TABLE reconnect_t (id INTEGER PRIMARY KEY, data TEXT)",
        &[],
    )
    .await?;
    ctx.query("INSERT INTO reconnect_t VALUES (1, 'a'), (2, 'b')", &[])
        .await?;
    ctx.cdc_decode_settle().await?;
    ctx.query(READ, &[]).await?;
    ctx.cache_settle().await?;
    let before = ctx.metrics().await?;
    ctx.query(READ, &[]).await?;
    let after = ctx.metrics_after_outcomes(&before, 1).await?;
    assert!(
        after.queries_cache_hit > before.queries_cache_hit,
        "the read is served from cache before the stream drops"
    );
    Ok(ctx)
}

/// Pid of the backend streaming from pgcache's slot, if one is attached.
async fn walsender_pid(origin: &Client) -> Result<Option<i32>, Error> {
    let row = origin
        .query_opt(
            "SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1",
            &[&SLOT],
        )
        .await
        .map_err(Error::other)?;
    Ok(row.and_then(|row| row.get(0)))
}

/// Terminate the backend streaming from the slot and wait for pgcache to
/// attach a new one.
async fn walsender_kill(origin: &Client) -> Result<(), Error> {
    let killed = walsender_pid(origin)
        .await?
        .ok_or_else(|| Error::other("no backend is streaming from the slot"))?;
    origin
        .execute("SELECT pg_terminate_backend($1)", &[&killed])
        .await
        .map_err(Error::other)?;

    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if walsender_pid(origin)
            .await?
            .is_some_and(|pid| pid != killed)
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(Error::other("pgcache did not reattach to the slot"));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The stream resumed with the cache intact: a row written on origin after
/// the drop reaches the cached result, the read is still a hit, and the
/// cache subsystem was not restarted.
async fn stream_resumed_assert(
    ctx: &mut TestContext,
    restarts_before: u64,
    rows_expected: usize,
) -> Result<(), Error> {
    let id = i32::try_from(rows_expected).map_err(Error::other)?;
    ctx.origin_query("INSERT INTO reconnect_t VALUES ($1, 'new')", &[&id])
        .await?;
    ctx.cdc_apply_settle().await?;

    let before = ctx.metrics().await?;
    let rows = ctx.query(READ, &[]).await?;
    let after = ctx.metrics_after_outcomes(&before, 1).await?;

    assert_eq!(
        after.cache_restarts_total, restarts_before,
        "the cache subsystem was not restarted"
    );
    assert!(
        after.queries_cache_hit > before.queries_cache_hit,
        "the read is still served from cache"
    );
    assert_eq!(rows.len(), rows_expected, "the new row reached the cache");
    Ok(())
}

#[tokio::test]
async fn test_replication_drop_resumes_the_stream() -> Result<(), Error> {
    let mut ctx = serving_context().await?;
    let restarts = ctx.metrics().await?.cache_restarts_total;

    walsender_kill(&ctx.origin).await?;

    stream_resumed_assert(&mut ctx, restarts, 3).await
}

/// The second drop lands before the reconnected stream has acknowledged
/// anything, which is when a flapping connection drops again (PGC-627).
#[tokio::test]
async fn test_replication_drop_right_after_reconnect_resumes_the_stream() -> Result<(), Error> {
    let mut ctx = serving_context().await?;
    let restarts = ctx.metrics().await?.cache_restarts_total;

    walsender_kill(&ctx.origin).await?;
    walsender_kill(&ctx.origin).await?;

    stream_resumed_assert(&mut ctx, restarts, 3).await
}
