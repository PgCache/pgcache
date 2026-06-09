//! PGC-254 root-cause probe: a CDC DELETE (or PK-change) of a row that a
//! per-group JOIN query's materialized view (MV) holds leaves a ghost row in
//! the MV, because the DELETE path never dirty-marks the MV.
//!
//! `mv_dirty_mark` is only called from `update_queries_execute_batch` (the CDC
//! INSERT/UPDATE upsert path). `handle_delete` / `frame_cache_delete` remove the
//! row from the *source* cache table but never flip the MV `Fresh →
//! Pending`, so the MV is never rebuilt and keeps the deleted row. The
//! source-row-served full-table query stays clean; the MV-served per-group
//! query serves the ghost.

use std::io::Error;

use tokio_postgres::SimpleQueryMessage;

use crate::util::{TestContext, connect_cache_db};

mod util;

fn row_count(msgs: &[SimpleQueryMessage]) -> usize {
    msgs.iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .count()
}

/// Warm a join query until its MV is Fresh, then DELETE one of its rows via CDC.
/// The served cache hit must drop the deleted row; the MV must not retain it.
#[tokio::test]
async fn test_cdc_delete_during_fresh_mv_leaves_no_ghost() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.simple_query("create table mvg_groups (group_id int primary key, version int not null)")
        .await?;
    ctx.simple_query(
        "create table mvg_items (id int primary key, group_id int not null, data int not null)",
    )
    .await?;
    // Many groups so the source table is large (the MV size gate is
    // `result_rows × ratio ≤ source_rows`, ratio=10; the per-group result is 2
    // rows, so the source needs ≥ 20 rows for the join MV to be built — exactly
    // the geometry of the stress harness).
    ctx.simple_query(
        "insert into mvg_groups (group_id, version) \
         select g, 0 from generate_series(1, 50) g",
    )
    .await?;
    ctx.simple_query(
        "insert into mvg_items (id, group_id, data) \
         select 1000 + (g*2) + r, g, g*100 + r \
         from generate_series(1, 50) g, generate_series(0, 1) r",
    )
    .await?;
    // The two items of group 1 get ids 1002 and 1003.
    ctx.cdc_settle().await?;

    let group_query = |g: i32| {
        format!(
            "select i.id, g.version, i.data from mvg_items i \
             join mvg_groups g on i.group_id = g.group_id where i.group_id = {g}"
        )
    };
    let query = group_query(1);

    // Warm every per-group query so the *shared* mvg_items source cache table
    // accumulates all groups' rows (~100). The MV size gate is
    // `result_rows × ratio ≤ source_rows` against that shared table, so a
    // per-group result of 2 rows needs the shared table large to pass — exactly
    // why the 100-group stress harness builds MVs but a single small query
    // doesn't.
    for g in 1..=50 {
        let _ = ctx.simple_query(&group_query(g)).await?;
    }
    ctx.cache_settle().await?;

    let cache_db = connect_cache_db(&ctx.dbs).await?;

    // Now drive group 1: first hit schedules the MV build, the build runs on the
    // writer. Poll until the MV table exists (it serves the join from there).
    let mut mv_built = false;
    for _ in 0..30 {
        let _ = ctx.simple_query(&query).await?;
        ctx.cache_settle().await?;
        let n: i64 = cache_db
            .query_one(
                "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace \
                 WHERE n.nspname='pgcache_mv' AND c.relkind='r'",
                &[],
            )
            .await
            .map_err(Error::other)?
            .get(0);
        if n >= 1 {
            mv_built = true;
            break;
        }
    }
    assert!(mv_built, "per-group join MV never built");

    // DELETE one item at origin; CDC removes it from the source cache table.
    ctx.origin_query("delete from mvg_items where id = 1002", &[])
        .await?;
    ctx.cdc_settle().await?;

    // Source cache table reflects the delete (sanity: the delete path works).
    let src_count: i64 = cache_db
        .query_one("SELECT count(*) FROM mvg_items WHERE group_id = 1", &[])
        .await
        .map_err(Error::other)?
        .get(0);
    assert_eq!(src_count, 1, "source cache table still has the deleted row");

    // Correctness is the *served* result. The DELETE must dirty-mark the
    // per-group MV (PGC-254): with the MV `Pending` the join serves from the
    // (clean) source rows, so the CDC-deleted row must not appear. Without the
    // dirty-mark the MV stays Fresh and serves the ghost (row_count == 2).
    // (A `Pending` MV table may physically retain the row — that's fine, it
    // isn't served; correctness lives in the served result, not the MV bytes.)
    let served = ctx.simple_query(&query).await?;
    assert_eq!(
        row_count(&served),
        1,
        "ghost row: the join still returns the CDC-deleted row 1002 \
         (DELETE path must dirty-mark the MV)"
    );

    Ok(())
}

/// Control: a CDC INSERT/UPDATE that touches the group *does* dirty-mark the MV,
/// so the ghost self-heals. This isolates the DELETE path as the sole gap — the
/// same reason the stress harness leaves ghosts only in groups not subsequently
/// written, and the source-row full-table query stays clean.
#[tokio::test]
async fn test_cdc_version_bump_after_delete_heals_mv() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.simple_query("create table mvh_groups (group_id int primary key, version int not null)")
        .await?;
    ctx.simple_query(
        "create table mvh_items (id int primary key, group_id int not null, data int not null)",
    )
    .await?;
    ctx.simple_query(
        "insert into mvh_groups (group_id, version) select g, 0 from generate_series(1, 50) g",
    )
    .await?;
    ctx.simple_query(
        "insert into mvh_items (id, group_id, data) \
         select 1000 + (g*2) + r, g, g*100 + r \
         from generate_series(1, 50) g, generate_series(0, 1) r",
    )
    .await?;
    ctx.cdc_settle().await?;

    let group_query = |g: i32| {
        format!(
            "select i.id, g.version, i.data from mvh_items i \
             join mvh_groups g on i.group_id = g.group_id where i.group_id = {g}"
        )
    };
    let query = group_query(1);

    for g in 1..=50 {
        let _ = ctx.simple_query(&group_query(g)).await?;
    }
    ctx.cache_settle().await?;

    let cache_db = connect_cache_db(&ctx.dbs).await?;
    let mut mv_built = false;
    for _ in 0..30 {
        let _ = ctx.simple_query(&query).await?;
        ctx.cache_settle().await?;
        let n: i64 = cache_db
            .query_one(
                "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace \
                 WHERE n.nspname='pgcache_mv' AND c.relkind='r'",
                &[],
            )
            .await
            .map_err(Error::other)?
            .get(0);
        if n >= 1 {
            mv_built = true;
            break;
        }
    }
    assert!(mv_built, "per-group join MV never built");

    // Delete the item, then bump the group's version. The bump's UPDATE on
    // mvh_groups matches the join and dirty-marks the MV, so the next hit
    // rebuilds it against the (now clean) source — the ghost heals.
    ctx.origin_query("delete from mvh_items where id = 1002", &[])
        .await?;
    ctx.cdc_settle().await?;
    ctx.origin_query(
        "update mvh_groups set version = version + 1 where group_id = 1",
        &[],
    )
    .await?;
    ctx.cdc_settle().await?;

    let mut served_ids: Vec<i32> = Vec::new();
    for _ in 0..10 {
        let served = ctx.simple_query(&query).await?;
        ctx.cache_settle().await?;
        served_ids = served
            .iter()
            .filter_map(|m| match m {
                SimpleQueryMessage::Row(r) => r.get(0).and_then(|s| s.parse::<i32>().ok()),
                SimpleQueryMessage::CommandComplete(_)
                | SimpleQueryMessage::RowDescription(_)
                | _ => None,
            })
            .collect();
        served_ids.sort_unstable();
        if served_ids == vec![1003] {
            break;
        }
    }
    assert_eq!(
        served_ids,
        vec![1003],
        "a post-delete write to the group should have healed the MV"
    );

    Ok(())
}

/// Control: a PK-change (`UPDATE ... SET id = <new>`) self-heals, so it is NOT a
/// persistent ghost source. The new-PK upsert goes through
/// `update_queries_execute_batch`, which DOES dirty-mark the MV; a later hit
/// rebuilds it correctly. (The old-PK delete-half alone would not — see the
/// DELETE test — but the upsert half compensates.) This passes, narrowing the
/// persistent bug to the pure-DELETE path where no compensating upsert fires.
#[tokio::test]
async fn test_cdc_pk_change_during_fresh_mv_leaves_no_ghost() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.simple_query("create table mvp_groups (group_id int primary key, version int not null)")
        .await?;
    ctx.simple_query(
        "create table mvp_items (id int primary key, group_id int not null, data int not null)",
    )
    .await?;
    ctx.simple_query(
        "insert into mvp_groups (group_id, version) \
         select g, 0 from generate_series(1, 50) g",
    )
    .await?;
    ctx.simple_query(
        "insert into mvp_items (id, group_id, data) \
         select 1000 + (g*2) + r, g, g*100 + r \
         from generate_series(1, 50) g, generate_series(0, 1) r",
    )
    .await?;
    ctx.cdc_settle().await?;

    let group_query = |g: i32| {
        format!(
            "select i.id, g.version, i.data from mvp_items i \
             join mvp_groups g on i.group_id = g.group_id where i.group_id = {g}"
        )
    };
    let query = group_query(1);

    for g in 1..=50 {
        let _ = ctx.simple_query(&group_query(g)).await?;
    }
    ctx.cache_settle().await?;

    let cache_db = connect_cache_db(&ctx.dbs).await?;

    // Drive group 1 until its MV is built (the build runs on the writer after a
    // hit flips Pending → Scheduled).
    let mut mv_built = false;
    for _ in 0..30 {
        let _ = ctx.simple_query(&query).await?;
        ctx.cache_settle().await?;
        let n: i64 = cache_db
            .query_one(
                "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace \
                 WHERE n.nspname='pgcache_mv' AND c.relkind='r'",
                &[],
            )
            .await
            .map_err(Error::other)?
            .get(0);
        if n >= 1 {
            mv_built = true;
            break;
        }
    }
    assert!(mv_built, "per-group join MV never built");

    // Capture the MV table name while it's known to exist.
    let mv_table: String = cache_db
        .query_one(
            "SELECT n.nspname||'.'||c.relname FROM pg_class c \
             JOIN pg_namespace n ON n.oid=c.relnamespace \
             WHERE n.nspname='pgcache_mv' AND c.relkind='r' LIMIT 1",
            &[],
        )
        .await
        .map_err(Error::other)?
        .get(0);
    let ids_before: Vec<i32> = cache_db
        .query(&format!("SELECT c0 FROM {mv_table} ORDER BY c0"), &[])
        .await
        .map_err(Error::other)?
        .iter()
        .map(|r| r.get(0))
        .collect();
    eprintln!("MV {mv_table} ids before pk-change: {ids_before:?}");

    // Change item 1002's PK to 9999 at origin.
    ctx.origin_query("update mvp_items set id = 9999 where id = 1002", &[])
        .await?;
    ctx.cdc_settle().await?;

    // The MV table may have been dropped/rebuilt; read it if it still exists.
    let mv_ids: Vec<i32> = cache_db
        .query(&format!("SELECT c0 FROM {mv_table} ORDER BY c0"), &[])
        .await
        .map(|rows| rows.iter().map(|r| r.get(0)).collect())
        .unwrap_or_default();
    eprintln!("MV {mv_table} ids after pk-change: {mv_ids:?}");

    // The served per-group join must match origin: exactly {1003, 9999}.
    // Drive several hits + settles to give any rebuild every chance to run.
    let mut served_ids: Vec<i32> = Vec::new();
    for _ in 0..10 {
        let served = ctx.simple_query(&query).await?;
        ctx.cache_settle().await?;
        served_ids = served
            .iter()
            .filter_map(|m| match m {
                SimpleQueryMessage::Row(r) => r.get(0).and_then(|s| s.parse::<i32>().ok()),
                SimpleQueryMessage::CommandComplete(_)
                | SimpleQueryMessage::RowDescription(_)
                | _ => None,
            })
            .collect();
        served_ids.sort_unstable();
        if served_ids == vec![1003, 9999] {
            break;
        }
    }
    assert_eq!(
        served_ids,
        vec![1003, 9999],
        "PK-change ghost: served per-group join diverged from origin \
         (the MV is not rebuilt to reflect the new PK / drop the old)"
    );

    Ok(())
}
