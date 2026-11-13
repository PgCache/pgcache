//! The schema-change event triggers are a requirement, not an option:
//! pgcache refuses to start without them at its own version, and a cache
//! that loses them while running is torn down and rebuilt.

use std::io::Error;
use std::time::Duration;

use crate::util::{
    TestContext, assert_cache_hit, assert_cache_miss, assert_proxy_rows_match_origin,
    connect_pgcache_args, pgcache_run_to_exit, start_databases,
};

mod util;

/// A role that owns its database's objects but may not create event
/// triggers, as on a managed origin.
const APP_ROLE_SQL: &str = "CREATE ROLE app LOGIN REPLICATION; \
     GRANT CREATE ON DATABASE origin_test TO app; \
     GRANT pg_read_all_data TO app;";

/// The version marker as a script other than this binary's would leave it.
const STALE_VERSION_SQL: &str = "CREATE OR REPLACE FUNCTION pgcache.signal_version() \
     RETURNS integer LANGUAGE sql IMMUTABLE AS 'SELECT 0'";

/// Without the triggers and without the privilege to install them, startup
/// is refused. Once an administrator's install is in place the same role
/// starts, and is refused again when that install is another version's.
#[tokio::test]
async fn test_startup_requires_event_triggers() -> Result<(), Error> {
    let (dbs, origin) = start_databases().await?;
    origin
        .batch_execute(APP_ROLE_SQL)
        .await
        .map_err(Error::other)?;

    let output = pgcache_run_to_exit(
        &dbs,
        &["--cache_policy", "fifo", "--origin_user", "app"],
        &[],
        Duration::from_secs(60),
    )?;
    assert!(!output.status.success(), "startup refused");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        text.contains("event triggers on origin are not installed"),
        "names the missing signal: {text}"
    );

    // A superuser's pgcache installs them; they outlive it. Its publication
    // would too, owned by the superuser.
    drop(connect_pgcache_args(&dbs, &[]).await?);
    origin
        .batch_execute("DROP PUBLICATION IF EXISTS pub_test")
        .await
        .map_err(Error::other)?;

    let (pgcache, _listen_port, _metrics_port, client) =
        connect_pgcache_args(&dbs, &["--origin_user", "app"]).await?;
    client
        .simple_query("select 1")
        .await
        .map_err(Error::other)?;
    drop(client);
    drop(pgcache);

    // An install some other pgcache version left: the triggers fire, but
    // their script is not the one this binary carries, and the role cannot
    // replace it.
    origin
        .batch_execute(STALE_VERSION_SQL)
        .await
        .map_err(Error::other)?;
    let output = pgcache_run_to_exit(
        &dbs,
        &["--cache_policy", "fifo", "--origin_user", "app"],
        &[],
        Duration::from_secs(60),
    )?;
    assert!(!output.status.success(), "startup refused");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        text.contains("event triggers on origin are installed by another pgcache version"),
        "names the stale signal: {text}"
    );

    Ok(())
}

/// A trigger disabled on origin while pgcache runs: the poll notices, the
/// cache is rebuilt (reinstalling the trigger, where the role may), and
/// reads are correct throughout.
#[tokio::test]
async fn test_trigger_disabled_while_running_rebuilds_cache() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query("create table sig (id integer primary key, val text)", &[])
        .await?;
    ctx.query("insert into sig (id, val) values (1, 'one')", &[])
        .await?;

    let q = "select id, val from sig order by id";
    let m = ctx.metrics().await?;
    ctx.simple_query(q).await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    ctx.simple_query(q).await?;
    let m = assert_cache_hit(&mut ctx, m).await?;
    assert_eq!(m.cache_restarts_total, 0);

    ctx.query("alter event trigger pgcache_sql_drop disable", &[])
        .await?;

    let mut restarted = false;
    for _ in 0..120 {
        assert_proxy_rows_match_origin(&mut ctx, q).await?;
        if ctx.metrics().await?.cache_restarts_total > 0 {
            restarted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(restarted, "the poll tears the cache down");

    let enabled = ctx
        .query(
            "select evtenabled::text from pg_event_trigger where evtname = 'pgcache_sql_drop'",
            &[],
        )
        .await?;
    assert_eq!(
        enabled
            .first()
            .map(|row| row.get::<_, String>(0))
            .as_deref(),
        Some("A"),
        "the rebuild reinstalls the trigger"
    );

    ctx.verdict_warm(q).await?;
    assert_proxy_rows_match_origin(&mut ctx, q).await?;

    Ok(())
}

/// The version marker replaced while pgcache runs, as another pgcache
/// version starting against the same origin would: the poll notices, and the
/// rebuild installs this binary's script again.
#[tokio::test]
async fn test_stale_version_while_running_rebuilds_cache() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query("create table ver (id integer primary key, val text)", &[])
        .await?;
    ctx.query("insert into ver (id, val) values (1, 'one')", &[])
        .await?;

    let q = "select id, val from ver order by id";
    let m = ctx.metrics().await?;
    ctx.simple_query(q).await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    ctx.simple_query(q).await?;
    let m = assert_cache_hit(&mut ctx, m).await?;
    assert_eq!(m.cache_restarts_total, 0);

    ctx.origin
        .batch_execute(STALE_VERSION_SQL)
        .await
        .map_err(Error::other)?;

    let mut restarted = false;
    for _ in 0..120 {
        assert_proxy_rows_match_origin(&mut ctx, q).await?;
        if ctx.metrics().await?.cache_restarts_total > 0 {
            restarted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(restarted, "the poll tears the cache down");

    ctx.verdict_warm(q).await?;
    assert_proxy_rows_match_origin(&mut ctx, q).await?;
    let version = ctx
        .query(
            "select prosrc from pg_proc where proname = 'signal_version'",
            &[],
        )
        .await?;
    assert_ne!(
        version
            .first()
            .map(|row| row.get::<_, String>(0))
            .as_deref(),
        Some("SELECT 0"),
        "the rebuild reinstalls the current script"
    );

    Ok(())
}
