//! `pgcache --cleanup`: removing from origin what an instance left there,
//! run as the real binary.

use std::io::Error;
use std::process::Command;
use std::time::{Duration, Instant};

use pgtemp::PgTempDB;
use tokio_postgres::Client;

use crate::util::{TestContext, start_databases};

mod util;

/// Slot and publication names from `tests/data/default_config.toml`.
const SLOT: &str = "slot_test";
const PUBLICATION: &str = "pub_test";

const SLOTS: &str = "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1";
const SLOTS_ACTIVE: &str =
    "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1 AND active";
const PUBLICATIONS: &str = "SELECT count(*) FROM pg_publication WHERE pubname = $1";
const SIGNAL_OBJECTS: &str = "SELECT (SELECT count(*) FROM pg_event_trigger WHERE evtname LIKE $1) \
     + (SELECT count(*) FROM pg_namespace WHERE nspname = 'pgcache')";

/// Run `pgcache --cleanup` against `origin` and return (exit ok, stdout).
fn cleanup_run(origin: &PgTempDB, extra_args: &[&str]) -> Result<(bool, String), Error> {
    let output = Command::new(env!("CARGO_BIN_EXE_pgcache"))
        .arg("--cleanup")
        .arg("--origin_host")
        .arg("127.0.0.1")
        .arg("--origin_port")
        .arg(origin.db_port().to_string())
        .arg("--origin_user")
        .arg(origin.db_user())
        .arg("--origin_database")
        .arg(origin.db_name())
        .arg("--cdc_slot_name")
        .arg(SLOT)
        .arg("--cdc_publication_name")
        .arg(PUBLICATION)
        .args(extra_args)
        .output()?;
    Ok((
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
    ))
}

async fn origin_count(origin: &Client, sql: &str, name: &str) -> Result<i64, Error> {
    let row = origin
        .query_one(sql, &[&name])
        .await
        .map_err(Error::other)?;
    Ok(row.get(0))
}

/// Kill pgcache without letting it clean up, and wait for origin to release
/// the slot its stream held.
async fn pgcache_kill(ctx: &mut TestContext) -> Result<(), Error> {
    ctx.pgcache.kill()?;
    ctx.pgcache.wait()?;
    let started = Instant::now();
    while origin_count(&ctx.origin, SLOTS_ACTIVE, SLOT).await? != 0 {
        if started.elapsed() > Duration::from_secs(10) {
            return Err(Error::other("origin still holds the killed stream's slot"));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Ok(())
}

/// A killed instance leaves its slot, publication and the shared signal
/// objects. `--cleanup` drops the first two and keeps the shared ones;
/// `--purge` then drops those.
#[tokio::test]
async fn test_cleanup_drops_what_a_killed_instance_left() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    pgcache_kill(&mut ctx).await?;
    assert_eq!(origin_count(&ctx.origin, SLOTS, SLOT).await?, 1);
    assert_eq!(
        origin_count(&ctx.origin, PUBLICATIONS, PUBLICATION).await?,
        1
    );

    let (ok, report) = cleanup_run(&ctx.dbs.origin, &[])?;

    assert!(ok, "{report}");
    assert!(report.starts_with("pgcache cleanup: origin "), "{report}");
    assert!(
        report.contains("  dropped  replication slot slot_test\n"),
        "{report}"
    );
    assert!(
        report.contains("  dropped  publication pub_test\n"),
        "{report}"
    );
    assert!(
        report.contains("  kept     schema-change event triggers and schema pgcache"),
        "{report}"
    );
    assert_eq!(origin_count(&ctx.origin, SLOTS, SLOT).await?, 0);
    assert_eq!(
        origin_count(&ctx.origin, PUBLICATIONS, PUBLICATION).await?,
        0
    );
    assert_eq!(
        origin_count(&ctx.origin, SIGNAL_OBJECTS, "pgcache_%").await?,
        3,
        "two event triggers and the schema stay without --purge"
    );

    let (ok, report) = cleanup_run(&ctx.dbs.origin, &["--purge"])?;

    assert!(ok, "{report}");
    assert!(
        report.contains("  absent   replication slot slot_test\n"),
        "{report}"
    );
    assert!(
        report.contains("  dropped  schema-change event triggers and schema pgcache\n"),
        "{report}"
    );
    assert_eq!(
        origin_count(&ctx.origin, SIGNAL_OBJECTS, "pgcache_%").await?,
        0
    );
    Ok(())
}

/// While a pgcache is streaming from the slot, cleanup removes nothing,
/// `--purge` included, and exits non-zero.
#[tokio::test]
async fn test_cleanup_refuses_a_slot_in_use() -> Result<(), Error> {
    let ctx = TestContext::setup().await?;

    let (ok, report) = cleanup_run(&ctx.dbs.origin, &["--purge"])?;

    assert!(!ok, "{report}");
    assert!(
        report.contains("  in use   replication slot slot_test (backend "),
        "{report}"
    );
    assert_eq!(origin_count(&ctx.origin, SLOTS_ACTIVE, SLOT).await?, 1);
    assert_eq!(
        origin_count(&ctx.origin, PUBLICATIONS, PUBLICATION).await?,
        1
    );
    assert_eq!(
        origin_count(&ctx.origin, SIGNAL_OBJECTS, "pgcache_%").await?,
        3
    );
    Ok(())
}

#[tokio::test]
async fn test_cleanup_on_an_untouched_origin_reports_absent() -> Result<(), Error> {
    let (dbs, _origin) = start_databases().await?;

    let (ok, report) = cleanup_run(&dbs.origin, &["--purge"])?;

    assert!(ok, "{report}");
    assert_eq!(report.matches("  absent   ").count(), 3, "{report}");
    Ok(())
}

/// `--purge` only means something to `--cleanup`.
#[test]
fn test_purge_without_cleanup_is_refused() -> Result<(), Error> {
    let output = Command::new(env!("CARGO_BIN_EXE_pgcache"))
        .arg("--purge")
        .output()?;
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("cleanup"),
        "names the flag it needs"
    );
    Ok(())
}
