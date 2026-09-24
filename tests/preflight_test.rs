//! `pgcache --check`: the origin readiness report, run as the real binary
//! against a prepared and an unprepared origin.

use std::io::Error;
use std::process::Command;

use pgtemp::{PgTempDB, PgTempDBBuilder};
use tokio_postgres::{Client, Config, NoTls};

mod util;

async fn origin_start(wal_level: &str) -> Result<(PgTempDB, Client), Error> {
    let db = PgTempDBBuilder::new()
        .with_dbname("preflight_test")
        .with_config_param("wal_level", wal_level)
        .with_config_param("log_destination", "stderr")
        .with_config_param("logging_collector", "on")
        .with_config_param("log_directory", "/tmp/")
        .start_async()
        .await;
    let (client, connection) = Config::new()
        .host("127.0.0.1")
        .port(db.db_port())
        .user(db.db_user())
        .dbname(db.db_name())
        .connect(NoTls)
        .await
        .map_err(Error::other)?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("origin connection error: {e}");
        }
    });
    Ok((db, client))
}

/// Run `pgcache --check` against `db` and return (exit ok, stdout).
fn check_run(db: &PgTempDB, extra_args: &[&str]) -> Result<(bool, String), Error> {
    let output = Command::new(env!("CARGO_BIN_EXE_pgcache"))
        .arg("--check")
        .arg("--origin_host")
        .arg("127.0.0.1")
        .arg("--origin_port")
        .arg(db.db_port().to_string())
        .arg("--origin_user")
        .arg(db.db_user())
        .arg("--origin_database")
        .arg(db.db_name())
        .args(extra_args)
        .output()?;
    Ok((
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
    ))
}

#[tokio::test]
async fn test_check_passes_on_prepared_origin() -> Result<(), Error> {
    let (db, client) = origin_start("logical").await?;
    client
        .execute("CREATE TABLE users (id int PRIMARY KEY, name text)", &[])
        .await
        .map_err(Error::other)?;

    let (ok, report) = check_run(&db, &[])?;
    assert!(ok, "expected exit 0:\n{report}");
    assert!(report.contains("  PASS  wal_level"), "{report}");
    assert!(
        report.contains("  PASS  replication connection"),
        "{report}"
    );
    assert!(report.contains("  PASS  role privileges"), "{report}");
    assert!(
        report.contains("all 1 tables have a primary key"),
        "{report}"
    );
    assert!(report.contains("  PASS  views"), "{report}");
    assert!(report.contains("  PASS  row-level security"), "{report}");
    assert!(
        report.contains("0 failed\nOrigin is ready for pgcache."),
        "{report}"
    );
    Ok(())
}

#[tokio::test]
async fn test_check_reports_every_defect_on_unprepared_origin() -> Result<(), Error> {
    let (db, client) = origin_start("replica").await?;
    for statement in [
        "CREATE TABLE logs (id int, line text)",
        "CREATE TABLE tenants (id int PRIMARY KEY, name text)",
        "ALTER TABLE tenants ENABLE ROW LEVEL SECURITY",
        "CREATE VIEW recent_logs AS SELECT * FROM logs",
    ] {
        client.execute(statement, &[]).await.map_err(Error::other)?;
    }

    let (ok, report) = check_run(&db, &[])?;
    assert!(!ok, "expected exit 1:\n{report}");
    assert!(report.contains("  FAIL  wal_level"), "{report}");
    assert!(
        report.contains("ALTER SYSTEM SET wal_level = logical"),
        "{report}"
    );
    assert!(report.contains("  WARN  primary keys"), "{report}");
    assert!(report.contains("public.logs"), "{report}");
    assert!(report.contains("  WARN  views"), "{report}");
    assert!(report.contains("public.recent_logs"), "{report}");
    assert!(report.contains("  FAIL  row-level security"), "{report}");
    assert!(report.contains("public.tenants"), "{report}");
    assert!(
        report.contains("Origin is not ready for pgcache."),
        "{report}"
    );

    // The allowlist scopes the table checks: with only `logs` allowed, the
    // RLS table and the view are out of scope and no longer reported.
    let (ok, report) = check_run(&db, &["--allowed_tables", "logs"])?;
    assert!(!ok, "wal_level still fails:\n{report}");
    assert!(report.contains("  PASS  row-level security"), "{report}");
    assert!(report.contains("  PASS  views"), "{report}");
    assert!(report.contains("  WARN  primary keys"), "{report}");
    Ok(())
}

#[tokio::test]
async fn test_check_reports_connection_failure_and_stops() -> Result<(), Error> {
    let output = Command::new(env!("CARGO_BIN_EXE_pgcache"))
        .args([
            "--check",
            "--origin_host",
            "127.0.0.1",
            "--origin_port",
            "1",
            "--origin_user",
            "postgres",
            "--origin_database",
            "nowhere",
        ])
        .output()?;
    let report = String::from_utf8_lossy(&output.stdout);
    assert!(!output.status.success(), "{report}");
    assert!(report.contains("  FAIL  connection"), "{report}");
    assert!(report.contains("Nothing is listening"), "{report}");
    assert!(!report.contains("wal_level"), "{report}");
    assert!(
        report.contains("0 passed, 0 warnings, 1 failed"),
        "{report}"
    );
    Ok(())
}
