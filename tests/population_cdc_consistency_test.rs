//! Consistency between query population and the CDC stream (PGC-250).
//!
//! Population reads origin at a snapshot while the CDC stream concurrently
//! applies changes to the shared cache tables. If a row a population read is
//! removed at origin (DELETE, or an UPDATE that moves it out of the query
//! predicate) *during* population, the CDC removal lands on a cache table that
//! doesn't hold the row yet (a no-op) and population then inserts it. No future
//! CDC event ever references that row again, so it becomes a permanent ghost.
//!
//! These tests use the `PGCACHE_FAULT_POPULATION_DELAY_MS` fault hook (feature
//! `fault-injection`) to sleep between population's snapshot read and its cache
//! insert, making the race deterministic. They are expected to FAIL until the
//! consistency work lands.
//!
//! Entirely fault-dependent: without the feature the population delay compiles
//! out, the race can't be provoked, and the tests would pass inertly — so gate
//! the whole file (matching `cache_pool_replenish_test.rs`).
#![cfg(feature = "fault-injection")]

use std::io::Error;
use std::time::Duration;

use tokio_postgres::SimpleQueryMessage;

use crate::util::{TestContext, assert_cache_hit};

mod util;

/// Population delay long enough that the test can read its snapshot, mutate
/// origin, and let CDC apply the mutation, all before population inserts.
const POPULATION_DELAY_MS: &str = "2500";

/// Margin after the triggering miss before mutating origin. The population
/// snapshot is fixed at query execution (sub-millisecond after the worker
/// picks up the work), so this only needs to clear worker pickup — it sits
/// comfortably inside `POPULATION_DELAY_MS`.
const SNAPSHOT_SETTLE: Duration = Duration::from_millis(800);

fn row_count(msgs: &[SimpleQueryMessage]) -> usize {
    msgs.iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .count()
}

/// A DELETE applied during population must not leave a ghost row in the cache.
#[tokio::test]
async fn test_delete_during_population_no_ghost_row() -> Result<(), Error> {
    let mut ctx =
        TestContext::setup_fault(&[("PGCACHE_FAULT_POPULATION_DELAY_MS", POPULATION_DELAY_MS)])
            .await?;

    ctx.simple_query("create table ghost_del (id int primary key, v text)")
        .await?;
    ctx.simple_query("insert into ghost_del (id, v) values (1, 'a')")
        .await?;
    ctx.cdc_settle().await?;

    // Cache miss: served from origin pass-through, triggers the background
    // population which blocks (fault delay) after reading its snapshot.
    let initial = ctx
        .simple_query("select id, v from ghost_del where id = 1")
        .await?;
    assert_eq!(row_count(&initial), 1, "row exists when population reads it");

    // Population has its snapshot; delete the row at origin and let CDC apply
    // the delete before population reaches its insert.
    tokio::time::sleep(SNAPSHOT_SETTLE).await;
    ctx.origin_query("delete from ghost_del where id = 1", &[])
        .await?;
    ctx.cdc_settle().await?;

    // Let the delayed population finish (it inserts its snapshot row), then
    // drain any trailing CDC.
    ctx.cache_settle().await?;
    ctx.cdc_settle().await?;

    let origin = ctx
        .origin_query("select id, v from ghost_del where id = 1", &[])
        .await?;
    assert_eq!(origin.len(), 0, "row was deleted at origin");

    let before = ctx.metrics().await?;
    let cached = ctx
        .simple_query("select id, v from ghost_del where id = 1")
        .await?;
    assert_cache_hit(&mut ctx, before).await?;
    assert_eq!(
        row_count(&cached),
        0,
        "ghost row: population resurrected a row CDC had already deleted"
    );

    Ok(())
}

/// An UPDATE that moves a row out of the query predicate during population must
/// not leave the stale (now non-matching) row in the cache.
#[tokio::test]
async fn test_update_out_of_predicate_during_population_no_ghost_row() -> Result<(), Error> {
    let mut ctx =
        TestContext::setup_fault(&[("PGCACHE_FAULT_POPULATION_DELAY_MS", POPULATION_DELAY_MS)])
            .await?;

    ctx.simple_query("create table ghost_upd (id int primary key, v text, status text)")
        .await?;
    ctx.simple_query("insert into ghost_upd (id, v, status) values (1, 'x', 'active')")
        .await?;
    ctx.cdc_settle().await?;

    let initial = ctx
        .simple_query("select id, v from ghost_upd where status = 'active'")
        .await?;
    assert_eq!(
        row_count(&initial),
        1,
        "row matches predicate when population reads it"
    );

    // Move the row out of the predicate at origin during population.
    tokio::time::sleep(SNAPSHOT_SETTLE).await;
    ctx.origin_query("update ghost_upd set status = 'inactive' where id = 1", &[])
        .await?;
    ctx.cdc_settle().await?;

    ctx.cache_settle().await?;
    ctx.cdc_settle().await?;

    let origin = ctx
        .origin_query("select id, v from ghost_upd where status = 'active'", &[])
        .await?;
    assert_eq!(origin.len(), 0, "row no longer matches the predicate");

    let before = ctx.metrics().await?;
    let cached = ctx
        .simple_query("select id, v from ghost_upd where status = 'active'")
        .await?;
    assert_cache_hit(&mut ctx, before).await?;
    assert_eq!(
        row_count(&cached),
        0,
        "ghost row: population resurrected a row that no longer matches the predicate"
    );

    Ok(())
}
