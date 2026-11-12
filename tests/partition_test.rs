//! Partitioned tables read at more than one level. A row change streams
//! under the leaf partition that holds the row, and the writer applies it to
//! every cached table whose partition tree holds that leaf: the root, a
//! partition in between, and the leaf itself.

use std::io::Error;

use crate::util::{
    TestContext, assert_cache_hit, assert_cache_miss, assert_proxy_rows_match_origin, rows_of,
};

mod util;

const PC_SETUP: [&str; 4] = [
    "create table pc (id int, k int, v int, primary key (id, k)) partition by range (k)",
    "create table pc1 partition of pc for values from (0) to (10)",
    "create table pc2 partition of pc for values from (10) to (20)",
    "insert into pc values (1, 15, 1), (2, 5, 1)",
];

const PC_ROOT: &str = "select id, k, v from pc order by id";
const PC_LOW: &str = "select id, k, v from pc1 order by id";
const PC_HIGH: &str = "select id, k, v from pc2 order by id";

/// Register `q`: a miss, then a hit once populated.
async fn warm(ctx: &mut TestContext, q: &str) -> Result<(), Error> {
    let m = ctx.metrics().await?;
    ctx.simple_query(q).await?;
    let m = assert_cache_miss(ctx, m).await?;
    ctx.cache_settle().await?;
    ctx.simple_query(q).await?;
    assert_cache_hit(ctx, m).await?;
    Ok(())
}

/// `q` is served from the cache and returns what origin returns.
async fn assert_hit_matches_origin(ctx: &mut TestContext, q: &str) -> Result<(), Error> {
    ctx.verdict_warm(q).await?;
    let m = ctx.metrics().await?;
    assert_proxy_rows_match_origin(ctx, q).await?;
    let m = assert_cache_hit(ctx, m).await?;
    assert_eq!(m.cache_restarts_total, 0);
    Ok(())
}

/// The root cached first, then a partition: a write through the root
/// reaches both.
#[tokio::test]
async fn test_partition_read_follows_writes_after_root_read() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    for statement in PC_SETUP {
        ctx.query(statement, &[]).await?;
    }

    warm(&mut ctx, PC_ROOT).await?;
    warm(&mut ctx, PC_HIGH).await?;

    ctx.query("update pc set v = 9 where id = 1", &[]).await?;
    ctx.cdc_apply_settle().await?;
    assert_hit_matches_origin(&mut ctx, PC_ROOT).await?;
    assert_hit_matches_origin(&mut ctx, PC_HIGH).await?;

    ctx.query("insert into pc2 values (3, 12, 3)", &[]).await?;
    ctx.query("delete from pc where id = 1", &[]).await?;
    ctx.cdc_apply_settle().await?;
    assert_hit_matches_origin(&mut ctx, PC_ROOT).await?;
    assert_hit_matches_origin(&mut ctx, PC_HIGH).await?;

    Ok(())
}

/// The partition cached first, then the root.
#[tokio::test]
async fn test_partition_read_follows_writes_before_root_read() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    for statement in PC_SETUP {
        ctx.query(statement, &[]).await?;
    }

    warm(&mut ctx, PC_HIGH).await?;
    ctx.query("update pc set v = 4 where id = 1", &[]).await?;
    ctx.cdc_apply_settle().await?;
    assert_hit_matches_origin(&mut ctx, PC_HIGH).await?;

    warm(&mut ctx, PC_ROOT).await?;
    ctx.query("update pc set v = 9 where id = 1", &[]).await?;
    ctx.cdc_apply_settle().await?;
    assert_hit_matches_origin(&mut ctx, PC_ROOT).await?;
    assert_hit_matches_origin(&mut ctx, PC_HIGH).await?;

    Ok(())
}

/// An update of the partition key moves the row: it leaves one partition's
/// reads and joins the other's.
#[tokio::test]
async fn test_partition_row_move_reaches_both_partitions() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    for statement in PC_SETUP {
        ctx.query(statement, &[]).await?;
    }

    warm(&mut ctx, PC_ROOT).await?;
    warm(&mut ctx, PC_LOW).await?;
    warm(&mut ctx, PC_HIGH).await?;

    ctx.query("update pc set k = 6 where id = 1", &[]).await?;
    ctx.cdc_apply_settle().await?;
    assert_hit_matches_origin(&mut ctx, PC_ROOT).await?;
    assert_hit_matches_origin(&mut ctx, PC_LOW).await?;
    assert_hit_matches_origin(&mut ctx, PC_HIGH).await?;
    let res = ctx.simple_query(PC_HIGH).await?;
    assert_eq!(rows_of(&res).len(), 0, "the row left the partition");

    Ok(())
}

/// A partition attached with its columns in another order than the root's:
/// its rows stream in its own layout and land in the root's by name.
#[tokio::test]
async fn test_partition_with_reordered_columns_applies_by_name() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    for statement in [
        "create table po (id int, k int, v text, primary key (id, k)) partition by range (k)",
        "create table po1 partition of po for values from (0) to (10)",
        "create table po2 (v text, extra int, k int not null, id int not null)",
        "alter table po2 drop column extra",
        "alter table po2 add primary key (id, k)",
        "alter table po attach partition po2 for values from (10) to (20)",
        "insert into po values (1, 15, 'one'), (2, 5, 'two')",
    ] {
        ctx.query(statement, &[]).await?;
    }

    let root = "select id, k, v from po order by id";
    let leaf = "select id, k, v from po2 order by id";
    warm(&mut ctx, root).await?;
    warm(&mut ctx, leaf).await?;

    ctx.query("update po set v = 'uno' where id = 1", &[])
        .await?;
    ctx.query("insert into po values (3, 16, 'three')", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    assert_hit_matches_origin(&mut ctx, root).await?;
    assert_hit_matches_origin(&mut ctx, leaf).await?;

    ctx.query("delete from po where id = 3", &[]).await?;
    ctx.query("update po set k = 17 where id = 1", &[]).await?;
    ctx.cdc_apply_settle().await?;
    assert_hit_matches_origin(&mut ctx, root).await?;
    assert_hit_matches_origin(&mut ctx, leaf).await?;

    Ok(())
}

/// A partition created and written in one transaction under a cached root.
#[tokio::test]
async fn test_partition_created_and_written_in_one_transaction() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    for statement in PC_SETUP {
        ctx.query(statement, &[]).await?;
    }

    warm(&mut ctx, PC_ROOT).await?;

    ctx.origin
        .batch_execute(
            "BEGIN; \
             create table pc3 partition of pc for values from (20) to (30); \
             insert into pc values (7, 25, 7); \
             COMMIT;",
        )
        .await
        .map_err(Error::other)?;
    ctx.query("update pc set v = 8 where id = 7", &[]).await?;
    ctx.cdc_apply_settle().await?;
    assert_hit_matches_origin(&mut ctx, PC_ROOT).await?;
    let res = ctx.simple_query(PC_ROOT).await?;
    assert_eq!(rows_of(&res).len(), 3);

    Ok(())
}

/// `TRUNCATE` of a partition cached on its own and under its root: both
/// reads lose its rows.
#[tokio::test]
async fn test_partition_truncate_reaches_partition_and_root() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    for statement in PC_SETUP {
        ctx.query(statement, &[]).await?;
    }

    warm(&mut ctx, PC_ROOT).await?;
    warm(&mut ctx, PC_HIGH).await?;

    ctx.query("truncate pc2", &[]).await?;
    ctx.cdc_apply_settle().await?;
    assert_proxy_rows_match_origin(&mut ctx, PC_ROOT).await?;
    assert_proxy_rows_match_origin(&mut ctx, PC_HIGH).await?;
    let res = ctx.simple_query(PC_HIGH).await?;
    assert_eq!(rows_of(&res).len(), 0);

    ctx.query("insert into pc values (4, 14, 4)", &[]).await?;
    ctx.cdc_apply_settle().await?;
    assert_proxy_rows_match_origin(&mut ctx, PC_ROOT).await?;
    assert_proxy_rows_match_origin(&mut ctx, PC_HIGH).await?;
    let m = ctx.metrics().await?;
    assert_eq!(m.cache_restarts_total, 0);

    Ok(())
}

/// A sub-partitioned tree read at the root, the middle and the leaf.
#[tokio::test]
async fn test_sub_partition_write_reaches_every_level() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    for statement in [
        "create table ps (id int, k int, v int, primary key (id, k)) partition by range (k)",
        "create table ps1 partition of ps for values from (0) to (10)",
        "create table ps2 partition of ps for values from (10) to (20) partition by range (k)",
        "create table ps2a partition of ps2 for values from (10) to (15)",
        "create table ps2b partition of ps2 for values from (15) to (20)",
        "insert into ps values (1, 12, 1), (2, 17, 1), (3, 5, 1)",
    ] {
        ctx.query(statement, &[]).await?;
    }

    let root = "select id, k, v from ps order by id";
    let middle = "select id, k, v from ps2 order by id";
    let leaf = "select id, k, v from ps2a order by id";
    warm(&mut ctx, root).await?;
    warm(&mut ctx, middle).await?;
    warm(&mut ctx, leaf).await?;

    ctx.query("update ps set v = 9 where id = 1", &[]).await?;
    ctx.query("update ps set k = 13 where id = 2", &[]).await?;
    ctx.query("insert into ps2a values (4, 11, 4)", &[]).await?;
    ctx.cdc_apply_settle().await?;
    assert_hit_matches_origin(&mut ctx, root).await?;
    assert_hit_matches_origin(&mut ctx, middle).await?;
    assert_hit_matches_origin(&mut ctx, leaf).await?;

    Ok(())
}
