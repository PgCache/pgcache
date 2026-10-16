//! Origin schema changes that the replication stream does not report by
//! itself (PGC-458): `DROP TABLE`, partition attach/detach, and DDL in the
//! middle of a transaction. pgcache's event triggers emit each as a
//! transactional logical message, and the writer evicts the affected
//! relation's queries at that point in the stream.

use std::io::Error;

use tokio_postgres::error::SqlState;

use crate::util::{TestContext, assert_cache_hit, assert_cache_miss, assert_row_at, rows_of};

mod util;

/// `DROP TABLE` on a cached relation (PGC-71): the cached result must not
/// outlive the table, and a table recreated under the same name (a new OID)
/// must cache again without the writer tripping over the dropped OID in its
/// publication bookkeeping (a restart would show in `cache_restarts_total`).
#[tokio::test]
async fn test_drop_table_stops_serving_and_recreated_table_caches() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table dropped_t (id integer primary key, val text)",
        &[],
    )
    .await?;
    ctx.query("insert into dropped_t (id, val) values (1, 'one')", &[])
        .await?;

    let q = "select id, val from dropped_t where id = 1";
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(q).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("val", "one")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    let res = ctx.simple_query(q).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("val", "one")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    ctx.query("drop table dropped_t", &[]).await?;
    ctx.cdc_apply_settle().await?;

    // No table on origin, nothing in the cache: the read fails like it would
    // against origin, rather than serving the row that was cached.
    let err = ctx
        .simple_query(q)
        .await
        .expect_err("read of a dropped table fails");
    let code = err
        .get_ref()
        .and_then(|e| e.downcast_ref::<tokio_postgres::Error>())
        .and_then(tokio_postgres::Error::as_db_error)
        .map(|db| db.code().clone());
    assert_eq!(
        code,
        Some(SqlState::UNDEFINED_TABLE),
        "unexpected error: {err}"
    );

    ctx.query(
        "create table dropped_t (id integer primary key, val text)",
        &[],
    )
    .await?;
    ctx.query("insert into dropped_t (id, val) values (1, 'two')", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    let m = ctx.metrics().await?;
    let res = ctx.simple_query(q).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("val", "two")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    let res = ctx.simple_query(q).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("val", "two")])?;
    let m = assert_cache_hit(&mut ctx, m).await?;
    assert_eq!(m.cache_restarts_total, 0, "writer must survive the drop");

    Ok(())
}

/// Attaching a non-empty partition under `publish_via_partition_root`
/// (PGC-70): the partition's existing rows never arrive as inserts, so the
/// signal must repopulate the parent's queries. Detaching removes them again.
#[tokio::test]
async fn test_partition_attach_and_detach_repopulate_parent() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    for statement in [
        "create table events (id integer, day integer, primary key (id, day)) \
         partition by range (day)",
        "create table events_p1 partition of events for values from (1) to (10)",
        "insert into events (id, day) values (1, 1)",
        "create table events_p2 (id integer, day integer, primary key (id, day))",
        "insert into events_p2 (id, day) values (2, 15)",
    ] {
        ctx.query(statement, &[]).await?;
    }

    let q = "select id, day from events where day < 100 order by id";
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(q).await?;
    assert_eq!(rows_of(&res).len(), 1);
    assert_row_at(&res, 1, &[("id", "1"), ("day", "1")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    let res = ctx.simple_query(q).await?;
    assert_eq!(rows_of(&res).len(), 1);
    let _m = assert_cache_hit(&mut ctx, m).await?;

    ctx.query(
        "alter table events attach partition events_p2 for values from (10) to (20)",
        &[],
    )
    .await?;
    ctx.cdc_apply_settle().await?;

    let m = ctx.metrics().await?;
    let res = ctx.simple_query(q).await?;
    assert_eq!(rows_of(&res).len(), 2, "attached partition's rows visible");
    assert_row_at(&res, 2, &[("id", "2"), ("day", "15")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    let res = ctx.simple_query(q).await?;
    assert_eq!(rows_of(&res).len(), 2);
    let _m = assert_cache_hit(&mut ctx, m).await?;

    ctx.query("alter table events detach partition events_p2", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    let m = ctx.metrics().await?;
    let res = ctx.simple_query(q).await?;
    assert_eq!(rows_of(&res).len(), 1, "detached partition's rows gone");
    assert_row_at(&res, 1, &[("id", "1"), ("day", "1")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    let res = ctx.simple_query(q).await?;
    assert_eq!(rows_of(&res).len(), 1);
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// DDL between two writes in one source transaction: the signal lands between
/// them in the frame. The first write was captured under the old shape and is
/// discarded with the eviction; the repopulation reads both from origin.
#[tokio::test]
async fn test_ddl_between_writes_in_one_transaction() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table mid_ddl (id integer primary key, val text)",
        &[],
    )
    .await?;
    ctx.query("insert into mid_ddl (id, val) values (1, 'one')", &[])
        .await?;

    let q = "select id, val from mid_ddl order by id";
    let m = ctx.metrics().await?;
    ctx.simple_query(q).await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    ctx.simple_query(q).await?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    ctx.origin
        .batch_execute(
            "BEGIN; \
             insert into mid_ddl (id, val) values (2, 'two'); \
             alter table mid_ddl add column extra text; \
             insert into mid_ddl (id, val, extra) values (3, 'three', 'e'); \
             COMMIT;",
        )
        .await
        .map_err(Error::other)?;
    ctx.cdc_apply_settle().await?;

    let m = ctx.metrics().await?;
    let res = ctx.simple_query(q).await?;
    assert_eq!(rows_of(&res).len(), 3);
    assert_row_at(&res, 2, &[("id", "2"), ("val", "two")])?;
    assert_row_at(&res, 3, &[("id", "3"), ("val", "three")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    let res = ctx.simple_query(q).await?;
    assert_eq!(rows_of(&res).len(), 3);
    let m = assert_cache_hit(&mut ctx, m).await?;
    assert_eq!(m.cache_restarts_total, 0);

    Ok(())
}
