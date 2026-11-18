//! Leaving origin clean: on a shutdown signal, and on a startup that fails
//! after provisioning, pgcache drops its replication slot and publication
//! before it exits, so a stopped instance holds no WAL there.

use std::io::Error;
use std::process::{Command, ExitStatus, Output};
use std::time::{Duration, Instant};

use tokio_postgres::Client;

use crate::util::{
    TempDBs, TestContext, find_available_port, pgcache_run_to_exit, start_databases,
};

mod util;

/// `docker stop` waits this long after its stop signal before it sends SIGKILL.
const STOP_GRACE: Duration = Duration::from_secs(10);

/// Slot and publication names from `tests/data/default_config.toml`.
const SLOT: &str = "slot_test";
const PUBLICATION: &str = "pub_test";

const SLOTS: &str = "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1";
const SLOTS_ACTIVE: &str =
    "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1 AND active";
const PUBLICATIONS: &str = "SELECT count(*) FROM pg_publication WHERE pubname = $1";

/// A proxy with one cached query, so the replication stream is attached to
/// the slot and the publication holds a table when the signal arrives.
async fn serving_context() -> Result<TestContext, Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(
        "CREATE TABLE shutdown_t (id INTEGER PRIMARY KEY, data TEXT)",
        &[],
    )
    .await?;
    ctx.query("INSERT INTO shutdown_t VALUES (1, 'a'), (2, 'b')", &[])
        .await?;
    ctx.cdc_decode_settle().await?;
    let read = "SELECT id, data FROM shutdown_t WHERE id <= 10";
    ctx.query(read, &[]).await?;
    ctx.cache_settle().await?;
    let before = ctx.metrics().await?;
    ctx.query(read, &[]).await?;
    let after = ctx.metrics_after_outcomes(&before, 1).await?;
    assert!(
        after.queries_cache_hit > before.queries_cache_hit,
        "the read is served from cache before the signal"
    );
    Ok(ctx)
}

fn signal_send(ctx: &TestContext, signal: &str) -> Result<(), Error> {
    let sent = Command::new("kill")
        .arg(format!("-{signal}"))
        .arg(ctx.pgcache.id().to_string())
        .status()?;
    assert!(sent.success(), "send SIG{signal} to pgcache");
    Ok(())
}

/// Wait for pgcache to exit within [`STOP_GRACE`] of a signal.
fn exit_wait(ctx: &mut TestContext, signal: &str) -> Result<ExitStatus, Error> {
    let started = Instant::now();
    loop {
        if let Some(status) = ctx.pgcache.try_wait()? {
            return Ok(status);
        }
        if started.elapsed() > STOP_GRACE {
            return Err(Error::other(format!(
                "pgcache still running {STOP_GRACE:?} after SIG{signal}"
            )));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

async fn origin_count(origin: &Client, sql: &str, name: &str) -> Result<i64, Error> {
    let row = origin
        .query_one(sql, &[&name])
        .await
        .map_err(Error::other)?;
    Ok(row.get(0))
}

async fn shutdown_leaves_origin_clean(signal: &str) -> Result<(), Error> {
    let mut ctx = serving_context().await?;
    assert_eq!(
        origin_count(&ctx.origin, SLOTS, SLOT).await?,
        1,
        "slot while running"
    );

    signal_send(&ctx, signal)?;
    let status = exit_wait(&mut ctx, signal)?;

    assert!(status.success(), "exit status after SIG{signal}: {status}");
    assert_eq!(
        origin_count(&ctx.origin, SLOTS, SLOT).await?,
        0,
        "no replication slot left on origin after SIG{signal}"
    );
    assert_eq!(
        origin_count(&ctx.origin, PUBLICATIONS, PUBLICATION).await?,
        0,
        "no publication left on origin after SIG{signal}"
    );
    Ok(())
}

/// Start a pgcache whose cache database is unreachable: provisioning on
/// origin succeeds, then setup fails and the process exits.
fn startup_refused_run(dbs: &TempDBs) -> Result<Output, Error> {
    let closed_port = find_available_port()?.to_string();
    pgcache_run_to_exit(
        dbs,
        &["--cache_port", &closed_port],
        &[],
        Duration::from_secs(30),
    )
}

/// What systemd sends, and a container runtime unless the image declares another.
#[tokio::test]
async fn test_sigterm_drops_the_slot_and_publication() -> Result<(), Error> {
    shutdown_leaves_origin_clean("TERM").await
}

/// Ctrl-C, the Docker image's declared stop signal, and what `try.sh stop` sends.
#[tokio::test]
async fn test_sigint_drops_the_slot_and_publication() -> Result<(), Error> {
    shutdown_leaves_origin_clean("INT").await
}

#[tokio::test]
async fn test_sighup_drops_the_slot_and_publication() -> Result<(), Error> {
    shutdown_leaves_origin_clean("HUP").await
}

#[tokio::test]
async fn test_sigquit_drops_the_slot_and_publication() -> Result<(), Error> {
    shutdown_leaves_origin_clean("QUIT").await
}

/// A second signal while the first shutdown is still running exits at once
/// with the conventional 128 + signal status, so a cleanup that cannot
/// finish does not hold the process.
#[tokio::test]
async fn test_second_signal_exits_without_waiting_for_cleanup() -> Result<(), Error> {
    let mut ctx = serving_context().await?;

    // Two different signals: a repeat of one still pending would be merged.
    signal_send(&ctx, "TERM")?;
    signal_send(&ctx, "INT")?;
    let status = exit_wait(&mut ctx, "INT")?;

    assert_eq!(status.code(), Some(130), "exit status: {status}");
    Ok(())
}

/// A startup that fails after provisioning (here the cache database is
/// unreachable) removes the slot and publication it had just created.
#[tokio::test]
async fn test_failed_startup_drops_the_slot_and_publication() -> Result<(), Error> {
    let (dbs, origin) = start_databases().await?;

    let output = startup_refused_run(&dbs)?;

    assert!(!output.status.success(), "startup is refused");
    assert_eq!(
        origin_count(&origin, SLOTS, SLOT).await?,
        0,
        "no replication slot left on origin after a failed startup"
    );
    assert_eq!(
        origin_count(&origin, PUBLICATIONS, PUBLICATION).await?,
        0,
        "no publication left on origin after a failed startup"
    );
    Ok(())
}

/// A second instance configured with the same slot name fails startup while
/// the first is streaming: it exits without waiting on, or dropping, the slot
/// the first holds.
#[tokio::test]
async fn test_failed_startup_leaves_a_slot_another_instance_holds() -> Result<(), Error> {
    let ctx = serving_context().await?;

    let output = startup_refused_run(&ctx.dbs)?;

    assert!(!output.status.success(), "startup is refused");
    let log = String::from_utf8_lossy(&output.stdout);
    assert!(
        log.contains("held by another connection"),
        "the second instance saw the slot in use and left it\n{log}"
    );
    assert_eq!(
        origin_count(&ctx.origin, SLOTS_ACTIVE, SLOT).await?,
        1,
        "the running instance still holds its slot"
    );
    Ok(())
}
