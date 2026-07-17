use std::io::Error;

use crate::util::{TestContext, assert_row_at, metrics_delta};

mod util;

/// PGC-366 smoke test: with read-after-write tracking on by default, writes a
/// connection forwards are recorded into its per-connection log (observed via
/// the `pgcache.raw.writes_recorded` counter), while pure reads record nothing.
/// The log is not yet consulted (that lands in PGC-368), so behavior is
/// unchanged — this only confirms the recording pipeline is wired end to end.
#[tokio::test]
async fn test_writes_recorded_into_raw_log() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE raw_items (id int primary key, data text)",
        &[],
    )
    .await?;

    // Baseline after setup/DDL so we measure only the statements below.
    let before = ctx.metrics().await?;

    ctx.simple_query("INSERT INTO raw_items VALUES (1, 'a')")
        .await?;
    ctx.simple_query("UPDATE raw_items SET data = 'b' WHERE id = 1")
        .await?;
    ctx.simple_query("DELETE FROM raw_items WHERE id = 1")
        .await?;

    let after_writes = metrics_delta(&before, &ctx.metrics().await?);
    assert!(
        after_writes.raw_writes_recorded >= 3,
        "expected the INSERT/UPDATE/DELETE to be recorded, got {}",
        after_writes.raw_writes_recorded
    );

    // A pure read records nothing.
    let mid = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM raw_items WHERE id = 1")
        .await?;
    let after_read = metrics_delta(&mid, &ctx.metrics().await?);
    assert_eq!(
        after_read.raw_writes_recorded, 0,
        "a read must not record a write"
    );

    Ok(())
}

/// PGC-367: after a connection forwards a write, a subsequent quiescent
/// ReadyForQuery injects a commit-LSN probe (observed via `pgcache.raw.probes`),
/// and normal query results are unaffected by the swallowed probe round-trip.
#[tokio::test]
async fn test_commit_lsn_probe_fires_and_preserves_results() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    let before = ctx.metrics().await?;

    ctx.query("CREATE TABLE raw_probe (id int primary key, v int)", &[])
        .await?;
    ctx.simple_query("INSERT INTO raw_probe VALUES (1, 10)")
        .await?;

    // Results are correct despite the injected probe being swallowed on the
    // origin socket.
    let res = ctx
        .simple_query("SELECT v FROM raw_probe WHERE id = 1")
        .await?;
    assert_row_at(&res, 1, &[("v", "10")])?;

    // Give the injected probe(s) time to round-trip on the origin socket.
    ctx.cdc_apply_settle().await?;

    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert!(
        delta.raw_probes >= 1,
        "expected at least one commit-LSN probe, got {}",
        delta.raw_probes
    );

    Ok(())
}
