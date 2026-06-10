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
//! insert, making the race deterministic. The Slice-A staging + deleted-key
//! merge (PGC-250) is what keeps these green.
//!
//! Entirely fault-dependent: without the feature the population delay compiles
//! out, the race can't be provoked, and the tests would pass inertly — so gate
//! the whole file (matching `cache_pool_replenish_test.rs`).
#![cfg(feature = "fault-injection")]

use std::io::Error;
use std::time::{Duration, Instant};

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

fn first_value(msgs: &[SimpleQueryMessage]) -> Option<String> {
    msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
        SimpleQueryMessage::CommandComplete(_) | SimpleQueryMessage::RowDescription(_) | _ => None,
    })
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
    assert_eq!(
        row_count(&initial),
        1,
        "row exists when population reads it"
    );

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

/// A TRUNCATE during population must not leave the pre-truncate snapshot rows
/// in the shared cache table.
///
/// The truncate invalidates the populating query, so its *own* reads forward to
/// origin and would hide a bad merge. But the merge's orphan rows survive in the
/// shared cache table (a population is insert-only — repopulation never removes
/// them) and resurface once the query repopulates and serves from cache again.
/// So this repopulates and asserts on a cache *hit*. The abort watermark makes
/// the merge abort instead of inserting the orphans. (Without the abort paths
/// this test fails with the 3 pre-truncate rows resurrected — verified.)
#[tokio::test]
async fn test_truncate_during_population_no_resurrected_rows() -> Result<(), Error> {
    let mut ctx =
        TestContext::setup_fault(&[("PGCACHE_FAULT_POPULATION_DELAY_MS", "1200")]).await?;

    ctx.simple_query("create table trunc_t (id int primary key, v text)")
        .await?;
    ctx.simple_query("insert into trunc_t (id, v) values (1, 'a'), (2, 'b'), (3, 'c')")
        .await?;
    ctx.cdc_settle().await?;

    // Miss: population reads the 3 rows, then blocks (population delay).
    let initial = ctx.simple_query("select id, v from trunc_t").await?;
    assert_eq!(
        row_count(&initial),
        3,
        "rows exist when population reads them"
    );

    // Truncate during the population window: origin is now empty.
    tokio::time::sleep(Duration::from_millis(400)).await;
    ctx.origin_query("truncate trunc_t", &[]).await?;
    ctx.cdc_settle().await?;

    // Let the delayed population finish (its merge must abort, not orphan the 3
    // pre-truncate rows into the shared cache table).
    ctx.cache_settle_with_timeout(Duration::from_secs(15))
        .await?;
    ctx.cdc_settle().await?;

    // Repopulate and read a cache hit — a forward would mask orphan rows.
    let _ = ctx.simple_query("select id, v from trunc_t").await?;
    ctx.cache_settle_with_timeout(Duration::from_secs(15))
        .await?;
    let before = ctx.metrics().await?;
    let cached = ctx.simple_query("select id, v from trunc_t").await?;
    assert_cache_hit(&mut ctx, before).await?;
    assert_eq!(
        row_count(&cached),
        0,
        "pre-truncate rows resurrected as orphans in the shared cache table"
    );

    Ok(())
}

/// Population delay for the coalesce-drain test: long enough that "drained at
/// invalidation" (the fix) and "drained only when population finally completes"
/// (no fix) are cleanly separable in wall-clock time.
const DRAIN_POPULATION_DELAY_MS: &str = "5000";

/// A coalesced waiter parked on a population must be drained (forwarded to
/// origin) at *invalidation*, not left parked until the population eventually
/// completes — under sustained invalidation churn a successor Ready may never
/// come, so a still-parked waiter hangs forever (found by the two-table
/// consistency stress harness, PGC-252). The drain covers both the CDC
/// invalidate path and `cache_query_evict`.
///
/// Distinguisher is timing, not just completion: the populating query is held
/// open for 5s, a second connection parks as a coalesced waiter, then an insert
/// that grows the join invalidates the query. With the drain the waiter returns
/// at invalidation (well under 5s); without it the waiter is only freed when the
/// 5s population finishes (or never). The assertion is that it returns in a
/// fraction of the population delay.
#[tokio::test]
async fn test_invalidation_drains_coalesced_waiters() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&[(
        "PGCACHE_FAULT_POPULATION_DELAY_MS",
        DRAIN_POPULATION_DELAY_MS,
    )])
    .await?;

    ctx.simple_query("create table coal_g (id int primary key, v int)")
        .await?;
    ctx.simple_query("create table coal_i (id int primary key, gid int)")
        .await?;
    ctx.simple_query("insert into coal_g (id, v) values (1, 10)")
        .await?;
    ctx.simple_query("insert into coal_i (id, gid) values (1, 1), (2, 1)")
        .await?;
    ctx.cdc_settle().await?;

    // Miss: registers the join query; population reads its snapshot, then blocks
    // for 5s (fault delay) — the query sits in Loading the whole time.
    let query = "select i.id, g.v from coal_i i join coal_g g on i.gid = g.id where g.id = 1";
    let initial = ctx.simple_query(query).await?;
    assert_eq!(row_count(&initial), 2, "join rows exist at registration");

    // Park a second connection's request as a coalesced waiter on the Loading
    // query.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let waiter_client = ctx.proxy_client_connect().await?;
    let parked_at = Instant::now();
    let waiter = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(15), waiter_client.simple_query(query)).await
    });

    // Deterministically wait until the waiter is actually parked (coalesce-queue
    // gauge >= 1) before invalidating. A waiter that parks *after* invalidation
    // would be forwarded via the enqueue-Err re-dispatch and never exercise the
    // drain — the test would then pass without testing anything.
    let mut parked = false;
    for _ in 0..100 {
        if ctx.metrics().await?.cache_coalesce_waiting >= 1 {
            parked = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        parked,
        "waiter never parked in the coalesce queue within 5s"
    );

    // Grow the join at origin: invalidates the still-populating query, which
    // must drain the parked waiter to origin now — not 5s later when population
    // completes.
    ctx.origin_query("insert into coal_i (id, gid) values (3, 1)", &[])
        .await?;

    let timed = waiter.await.expect("waiter task panicked");
    let elapsed = parked_at.elapsed();
    let messages = timed
        .unwrap_or_else(|_| panic!("parked waiter hung past 15s: invalidation did not drain it"))
        .expect("waiter query failed");

    // The drain must arrive at invalidation (~hundreds of ms), far short of the
    // 5s population delay. A waiter freed only at population completion would
    // land near 5s.
    assert!(
        elapsed < Duration::from_millis(2500),
        "parked waiter returned after {elapsed:?}; invalidation did not drain the coalesce \
         queue (it was freed only when the 5s population completed)"
    );
    assert_eq!(
        row_count(&messages),
        3,
        "drained waiter serves origin's post-insert rows"
    );

    Ok(())
}

/// Slice B deferred-Ready gate: a population must not be served while CDC is
/// behind the snapshot, or it would expose a transiently-stale cache during
/// catch-up.
///
/// Construction: population reads `v=10`; during the population window the row
/// is updated to `v=20` at origin with CDC delivery delayed, so the merge
/// stages the stale `v=10`. Without the gate the query goes Ready immediately
/// and `cache_settle` returns at once with the stale value; with the gate
/// `cache_settle` returns only after the watermark reaches the snapshot LSN —
/// by which point CDC has applied the update — so the cache hit yields `20`.
/// The single value distinguishes the two.
#[tokio::test]
async fn test_deferred_ready_gate_serves_caught_up_value() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&[
        ("PGCACHE_FAULT_POPULATION_DELAY_MS", "800"),
        ("PGCACHE_FAULT_CDC_DELIVER_DELAY_MS", "300"),
    ])
    .await?;

    ctx.simple_query("create table gate_t (id int primary key, v int)")
        .await?;
    ctx.simple_query("insert into gate_t (id, v) values (1, 10)")
        .await?;
    // No cached query yet, so the CDC delivery delay is inactive here.
    ctx.cdc_settle().await?;

    // Miss: population reads v=10, then blocks (population delay). Registering
    // the query tracks the relation, which activates the CDC delivery delay.
    let initial = ctx
        .simple_query("select v from gate_t where id = 1")
        .await?;
    assert_eq!(first_value(&initial).as_deref(), Some("10"));

    // Update during the population window — its CDC event is delayed, so the
    // merge stages the stale v=10 while origin is already v=20.
    tokio::time::sleep(Duration::from_millis(200)).await;
    ctx.origin_query("update gate_t set v = 20 where id = 1", &[])
        .await?;

    // The gate holds the query non-Ready until the watermark catches up; allow a
    // generous timeout for the delayed CDC stream to drain.
    ctx.cache_settle_with_timeout(Duration::from_secs(20))
        .await?;

    let before = ctx.metrics().await?;
    let served = ctx
        .simple_query("select v from gate_t where id = 1")
        .await?;
    assert_cache_hit(&mut ctx, before).await?;
    assert_eq!(
        first_value(&served).as_deref(),
        Some("20"),
        "gate served a stale value: query went Ready before CDC caught up to the snapshot"
    );

    Ok(())
}

/// A PK deleted and then reinserted at origin must not be omitted from a later
/// population's result (PGC-260). The delete is tracked while a guard
/// population holds the deleted-key set open; the reinsert matches no live
/// query at apply time, so without the fix the row is absent from the shared
/// table AND the tracked key filters it out of the later population's merge —
/// a persistent omission of a live row.
#[tokio::test]
async fn test_reinsert_during_population_included() -> Result<(), Error> {
    let mut ctx =
        TestContext::setup_fault(&[("PGCACHE_FAULT_POPULATION_DELAY_ONCE_MS", "4000")]).await?;

    ctx.simple_query("create table reins (id int primary key, flag text not null, v int not null)")
        .await?;
    ctx.simple_query("insert into reins (id, flag, v) values (1, 'guard', 1), (2, 'target', 10)")
        .await?;
    ctx.cdc_settle().await?;

    // Guard query: its (one-shot-delayed) population keeps deleted-key
    // tracking active for the relation across the whole scenario.
    ctx.simple_query("select id, v from reins where flag = 'guard'")
        .await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Delete then reinsert the target row in separate source transactions.
    // At apply time nothing live matches flag='target', so without the
    // cancel + force-upsert the reinsert writes nothing.
    ctx.origin_query("delete from reins where id = 2", &[])
        .await?;
    ctx.cdc_settle().await?;
    ctx.origin_query(
        "insert into reins (id, flag, v) values (2, 'target', 20)",
        &[],
    )
    .await?;
    ctx.cdc_settle().await?;

    // Now register the query that matches the reinserted row. Its population
    // runs undelayed, merging while the guard is still in flight (the tracked
    // key is not yet pruned).
    let q = "select id, v from reins where flag = 'target'";
    ctx.simple_query(q).await?;
    ctx.cache_settle().await?;

    let before = ctx.metrics().await?;
    let served = ctx.simple_query(q).await?;
    assert_cache_hit(&mut ctx, before).await?;
    assert_eq!(
        row_count(&served),
        1,
        "reinserted live row omitted from the population result (PGC-260)"
    );
    assert_eq!(
        first_value(&served).as_deref(),
        Some("2"),
        "served row should be id=2"
    );

    Ok(())
}

/// Same as above with the delete + reinsert in ONE source transaction: the
/// frame-pending deleted key must net out against the same frame's reinsert
/// (event order), recording nothing at commit.
#[tokio::test]
async fn test_reinsert_same_frame_during_population_included() -> Result<(), Error> {
    let mut ctx =
        TestContext::setup_fault(&[("PGCACHE_FAULT_POPULATION_DELAY_ONCE_MS", "4000")]).await?;

    ctx.simple_query(
        "create table reins_f (id int primary key, flag text not null, v int not null)",
    )
    .await?;
    ctx.simple_query("insert into reins_f (id, flag, v) values (1, 'guard', 1), (2, 'target', 10)")
        .await?;
    ctx.cdc_settle().await?;

    ctx.simple_query("select id, v from reins_f where flag = 'guard'")
        .await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    ctx.origin
        .batch_execute(
            "BEGIN; \
             delete from reins_f where id = 2; \
             insert into reins_f (id, flag, v) values (2, 'target', 20); \
             COMMIT;",
        )
        .await
        .map_err(Error::other)?;
    ctx.cdc_settle().await?;

    let q = "select id, v from reins_f where flag = 'target'";
    ctx.simple_query(q).await?;
    ctx.cache_settle().await?;

    let before = ctx.metrics().await?;
    let served = ctx.simple_query(q).await?;
    assert_cache_hit(&mut ctx, before).await?;
    assert_eq!(
        row_count(&served),
        1,
        "reinserted live row omitted after same-frame delete+reinsert (PGC-260)"
    );

    Ok(())
}
