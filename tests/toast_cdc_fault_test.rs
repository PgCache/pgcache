//! Unchanged-toast under PGC-261's tracking-active update-out path (PGC-264).
//!
//! While a population holds deleted-key tracking open, an update-out upserts
//! the row's new version instead of deleting it. With an unchanged-toast
//! column that upsert must carry the repaired value: a NULL-holed row in the
//! shared table is unrepairable (population merges never overwrite existing
//! rows), so a later query over the new version would serve NULL forever.
//!
//! Fault-dependent (population delay keeps tracking open deterministically) —
//! gated like `population_cdc_consistency_test.rs`.
#![cfg(feature = "fault-injection")]

use std::io::Error;
use std::time::Duration;

use tokio_postgres::SimpleQueryMessage;

use crate::util::{TestContext, assert_cache_hit, metrics_delta};

mod util;

const TOAST_LEN: usize = 8000;

fn first_value(msgs: &[SimpleQueryMessage]) -> Option<String> {
    msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
        SimpleQueryMessage::CommandComplete(_) | SimpleQueryMessage::RowDescription(_) | _ => None,
    })
}

fn row_count(msgs: &[SimpleQueryMessage]) -> usize {
    msgs.iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .count()
}

#[tokio::test]
async fn test_unchanged_toast_update_out_during_tracking_preserved() -> Result<(), Error> {
    let mut ctx =
        TestContext::setup_fault(&[("PGCACHE_FAULT_POPULATION_DELAY_ONCE_MS", "4000")]).await?;

    let big: String = std::iter::repeat_n('x', TOAST_LEN).collect();
    ctx.simple_query("create table toast_uo (id int primary key, big text, status text not null)")
        .await?;
    ctx.simple_query("alter table toast_uo alter column big set storage external")
        .await?;
    ctx.simple_query(&format!(
        "insert into toast_uo (id, big, status) values (1, '', 'guard'), (2, '{big}', 'active')"
    ))
    .await?;
    ctx.cdc_decode_settle().await?;

    // Guard query: its (one-shot-delayed) population keeps deleted-key
    // tracking active for the relation across the whole scenario.
    ctx.simple_query("select id from toast_uo where status = 'guard'")
        .await?;
    // The active-row query caches id=2 (with its TOAST value) undelayed.
    // Read until it's a cache hit so the row is provably in the cache table
    // before the update — that pins the repair (not the not-found fallback)
    // path, which is the one under test. Must resolve well inside the guard's
    // 4s tracking window.
    let qa = "select big from toast_uo where status = 'active'";
    ctx.simple_query(qa).await?;
    let mut qa_cached = false;
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let before = ctx.metrics().await?;
        ctx.simple_query(qa).await?;
        let after = ctx.metrics().await?;
        if after.queries_cache_hit > before.queries_cache_hit {
            qa_cached = true;
            break;
        }
    }
    assert!(qa_cached, "active-row query never cached within 3s");
    ctx.cdc_decode_settle().await?;

    // Update-out with `big` unchanged → elided. Tracking is active, so the
    // PGC-261 branch upserts the new version — it must carry the repaired
    // TOAST value into the shared table.
    ctx.origin_query("update toast_uo set status = 'archived' where id = 2", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    // A later query over the new version populates while the guard is still
    // in flight; its merge never overwrites the upserted row, so a NULL hole
    // written by the update-out would be frozen here. The settle must outlast
    // the guard's injected 4s population delay, which the 5s default doesn't
    // under parallel-suite load.
    let q2 = "select big from toast_uo where status = 'archived'";
    ctx.simple_query(q2).await?;
    ctx.cache_settle_with_timeout(Duration::from_secs(20))
        .await?;

    let before = ctx.metrics().await?;
    let served = ctx.simple_query(q2).await?;
    assert_cache_hit(&mut ctx, before).await?;
    assert_eq!(
        first_value(&served).as_deref(),
        Some(big.as_str()),
        "update-out under tracking froze a corrupted TOAST value into the shared table"
    );

    // Control: the old version must not be served to the original query.
    // No hit assertion — if timing pushed the update onto the conservative
    // fallback (row not yet cached at apply time), `qa` was legitimately
    // invalidated and this read forwards; serving the stale row is the only
    // failure.
    let served = ctx.simple_query(qa).await?;
    assert_eq!(
        row_count(&served),
        0,
        "stale old version served after update-out"
    );

    Ok(())
}

// --- PGC-464: a toast fallback must not invalidate unrelated queries ---

/// Every population sleeps this long between its origin read and its staging
/// insert, so a population started last is provably in flight — recording,
/// with its rows staged pre-event — when the scenario's UPDATE lands.
const STALE_DELAY: (&str, &str) = ("PGCACHE_FAULT_POPULATION_DELAY_MS", "3000");

/// Rows in four groups; `big` is TOASTed and never written by the scenarios,
/// so every UPDATE arrives with it elided.
async fn stale_setup(ctx: &mut TestContext, big: &str) -> Result<(), Error> {
    ctx.simple_query(
        "create table toast_stale (id int primary key, grp int not null, big text, v int not null)",
    )
    .await?;
    ctx.simple_query("alter table toast_stale alter column big set storage external")
        .await?;
    ctx.simple_query(&format!(
        "insert into toast_stale (id, grp, big, v) values \
         (1, 1, '{big}', 0), (2, 2, '{big}', 0), (3, 3, '{big}', 0), (4, 4, '{big}', 0)"
    ))
    .await?;
    ctx.cdc_decode_settle().await?;
    Ok(())
}

const GUARD: &str = "select v, big from toast_stale where grp = 3";

fn group_query(grp: i32) -> String {
    format!("select v, big from toast_stale where grp = {grp}")
}

/// Read `sql` until it is a cache hit (registered, populated, Ready).
async fn read_until_hit(ctx: &mut TestContext, sql: &str) -> Result<(), Error> {
    ctx.simple_query(sql).await?;
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let before = ctx.metrics().await?;
        ctx.simple_query(sql).await?;
        let after = ctx.metrics().await?;
        if after.queries_cache_hit > before.queries_cache_hit {
            return Ok(());
        }
    }
    Err(Error::other(format!("{sql:?} never became a cache hit")))
}

/// Start the guard population last, and give it time to take its origin
/// snapshot before the scenario's UPDATE (its injected delay then holds it in
/// flight, recording, for seconds).
async fn guard_start(ctx: &mut TestContext) -> Result<(), Error> {
    ctx.simple_query(GUARD).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    Ok(())
}

/// Read the guard until it is a cache hit serving `rows` rows (first value
/// `v` when there is one).
async fn guard_converges(ctx: &mut TestContext, v: &str, rows: usize) -> Result<(), Error> {
    ctx.cache_settle_with_timeout(Duration::from_secs(20))
        .await?;
    read_until_hit(ctx, GUARD).await?;
    let served = ctx.simple_query(GUARD).await?;
    assert_eq!(row_count(&served), rows);
    if rows > 0 {
        assert_eq!(first_value(&served).as_deref(), Some(v));
    }
    Ok(())
}

async fn assert_group_hit(ctx: &mut TestContext, grp: i32, v: &str) -> Result<(), Error> {
    let before = ctx.metrics().await?;
    let served = ctx.simple_query(&group_query(grp)).await?;
    assert_cache_hit(ctx, before).await?;
    assert_eq!(first_value(&served).as_deref(), Some(v));
    Ok(())
}

/// A toasted UPDATE of a row no query has cached falls back, but under
/// recording it must leave the Ready queries alone and let the in-flight
/// guard (which never staged the row) merge cleanly. Also covers a PK change.
#[tokio::test]
async fn test_toast_fallback_on_uncached_row_leaves_other_queries_ready() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&[STALE_DELAY]).await?;
    let big: String = std::iter::repeat_n('x', TOAST_LEN).collect();
    stale_setup(&mut ctx, &big).await?;

    read_until_hit(&mut ctx, &group_query(1)).await?;
    read_until_hit(&mut ctx, &group_query(2)).await?;
    guard_start(&mut ctx).await?;

    let before = ctx.metrics().await?;
    ctx.origin_query("update toast_stale set v = 1 where id = 4", &[])
        .await?;
    ctx.origin_query("update toast_stale set id = 40 where id = 4", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert!(
        delta.cache_cdc_toast_fallbacks >= 1,
        "expected the uncached-row fallback"
    );
    assert_eq!(
        delta.cache_invalidations, 0,
        "unrelated queries were invalidated"
    );

    assert_group_hit(&mut ctx, 1, "0").await?;
    assert_group_hit(&mut ctx, 2, "0").await?;

    // The guard merges once its delay elapses: it never staged the row.
    guard_converges(&mut ctx, "0", 1).await?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(
        delta.cache_cdc_toast_stale_aborts, 0,
        "guard must not abort"
    );
    Ok(())
}

/// A toasted UPDATE of a row the in-flight guard staged, whose post-image
/// still matches the guard's predicate: membership evaluation invalidates the
/// guard (the row could grow its result and can't be upserted) and nothing
/// else; the guard repopulates with the post-update row, and the Ready query
/// is untouched.
#[tokio::test]
async fn test_toast_fallback_matching_inflight_predicate_invalidates_only_it() -> Result<(), Error>
{
    let mut ctx = TestContext::setup_fault(&[STALE_DELAY]).await?;
    let big: String = std::iter::repeat_n('x', TOAST_LEN).collect();
    stale_setup(&mut ctx, &big).await?;

    read_until_hit(&mut ctx, &group_query(1)).await?;
    guard_start(&mut ctx).await?;

    let before = ctx.metrics().await?;
    ctx.origin_query("update toast_stale set v = 1 where id = 3", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert!(
        delta.cache_cdc_toast_fallbacks >= 1,
        "expected the uncached-row fallback"
    );
    assert_eq!(
        delta.cache_invalidations, 1,
        "only the guard is invalidated"
    );

    assert_group_hit(&mut ctx, 1, "0").await?;
    guard_converges(&mut ctx, "1", 1).await?;
    let rows = ctx
        .query("select big from toast_stale where grp = 3", &[])
        .await?;
    assert_eq!(
        rows[0].get::<_, String>(0),
        big,
        "TOAST value intact after repopulation"
    );
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.cache_cdc_toast_stale_aborts, 0);
    Ok(())
}

/// A toasted UPDATE that moves a staged row *out* of the guard's predicate:
/// the post-image matches no query, so membership evaluation invalidates
/// nothing — only the toast-stale probe knows the guard staged a copy it can
/// no longer trust, and aborts exactly that merge. The guard repopulates
/// without the row; the Ready query is untouched.
#[tokio::test]
async fn test_toast_fallback_on_staged_row_leaving_predicate_aborts_only_that_merge()
-> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&[STALE_DELAY]).await?;
    let big: String = std::iter::repeat_n('x', TOAST_LEN).collect();
    stale_setup(&mut ctx, &big).await?;

    read_until_hit(&mut ctx, &group_query(1)).await?;
    guard_start(&mut ctx).await?;

    let before = ctx.metrics().await?;
    ctx.origin_query("update toast_stale set grp = 9 where id = 3", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert!(
        delta.cache_cdc_toast_fallbacks >= 1,
        "expected the uncached-row fallback"
    );
    assert_eq!(
        delta.cache_invalidations, 0,
        "no query matches the post-image"
    );

    ctx.cache_settle_with_timeout(Duration::from_secs(20))
        .await?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(
        delta.cache_cdc_toast_stale_aborts, 1,
        "the guard's merge must abort exactly once"
    );

    let served = ctx.simple_query(GUARD).await?;
    assert_eq!(row_count(&served), 0, "the moved row must not be served");
    guard_converges(&mut ctx, "", 0).await?;
    assert_group_hit(&mut ctx, 1, "0").await?;
    Ok(())
}

/// Regression guard: a toasted UPDATE of a cached row is repaired in place
/// under recording — no fallback, no invalidation, no abort.
#[tokio::test]
async fn test_toast_update_of_cached_row_repairs_under_recording() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&[STALE_DELAY]).await?;
    let big: String = std::iter::repeat_n('x', TOAST_LEN).collect();
    stale_setup(&mut ctx, &big).await?;

    read_until_hit(&mut ctx, &group_query(1)).await?;
    guard_start(&mut ctx).await?;

    let before = ctx.metrics().await?;
    ctx.origin_query("update toast_stale set v = 1 where id = 1", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert!(
        delta.cache_cdc_toast_repairs >= 1,
        "expected an in-place repair"
    );
    assert_eq!(delta.cache_cdc_toast_fallbacks, 0);
    assert_eq!(delta.cache_invalidations, 0);

    assert_group_hit(&mut ctx, 1, "1").await?;
    let rows = ctx
        .query("select big from toast_stale where grp = 1", &[])
        .await?;
    assert_eq!(rows[0].get::<_, String>(0), big);

    guard_converges(&mut ctx, "0", 1).await?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.cache_cdc_toast_stale_aborts, 0);
    Ok(())
}
