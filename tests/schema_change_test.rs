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

/// Register `q`, expecting `expected_ids` in order: a miss, then a hit once
/// populated. Returns the metrics after the hit.
async fn assert_repopulated(
    ctx: &mut TestContext,
    q: &str,
    expected_ids: &[&str],
) -> Result<(), Error> {
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(q).await?;
    let rows = rows_of(&res);
    assert_eq!(rows.len(), expected_ids.len(), "row count after the signal");
    for (index, id) in expected_ids.iter().enumerate() {
        assert_row_at(&res, index + 1, &[("id", id)])?;
    }
    let m = assert_cache_miss(ctx, m).await?;
    ctx.cache_settle().await?;
    let res = ctx.simple_query(q).await?;
    assert_eq!(rows_of(&res).len(), expected_ids.len());
    let m = assert_cache_hit(ctx, m).await?;
    assert_eq!(m.cache_restarts_total, 0, "writer must survive the signal");
    Ok(())
}

/// `TRUNCATE` of a partition under `publish_via_partition_root` (PGC-70):
/// pgoutput sends nothing for it, so the per-partition truncate trigger must
/// signal it and the parent's queries repopulate without the partition's rows.
#[tokio::test]
async fn test_partition_truncate_repopulates_parent() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    for statement in [
        "create table events (id integer, day integer, primary key (id, day)) \
         partition by range (day)",
        "create table events_p1 partition of events for values from (1) to (10)",
        "create table events_p2 partition of events for values from (10) to (20)",
        "insert into events (id, day) values (1, 1), (2, 15)",
    ] {
        ctx.query(statement, &[]).await?;
    }

    let q = "select id, day from events where day < 100 order by id";
    assert_repopulated(&mut ctx, q, &["1", "2"]).await?;

    ctx.query("truncate events_p2", &[]).await?;
    ctx.cdc_apply_settle().await?;
    assert_repopulated(&mut ctx, q, &["1"]).await?;

    Ok(())
}

/// `DROP TABLE` of a leaf under a sub-partitioned partition: `sql_drop`
/// names only the leaf, and pgcache resolves it to the cached root through
/// the partition list it loaded at registration.
#[tokio::test]
async fn test_partition_drop_resolves_through_sub_partition() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    for statement in [
        "create table events (id integer, day integer, primary key (id, day)) \
         partition by range (day)",
        "create table events_p1 partition of events for values from (1) to (10)",
        "create table events_p2 partition of events for values from (10) to (20) \
         partition by range (day)",
        "create table events_p2a partition of events_p2 for values from (10) to (15)",
        "insert into events (id, day) values (1, 1), (2, 12)",
    ] {
        ctx.query(statement, &[]).await?;
    }

    let q = "select id, day from events where day < 100 order by id";
    assert_repopulated(&mut ctx, q, &["1", "2"]).await?;

    ctx.query("drop table events_p2a", &[]).await?;
    ctx.cdc_apply_settle().await?;
    assert_repopulated(&mut ctx, q, &["1"]).await?;

    Ok(())
}

/// A partition created after the parent was registered: the create signal
/// extends the parent's partition list and installs the truncate trigger on
/// the new partition, so its later truncate and drop both resolve to the
/// parent. Row changes through it apply in place as usual.
#[tokio::test]
async fn test_partition_created_after_registration_is_tracked() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    for statement in [
        "create table events (id integer, day integer, primary key (id, day)) \
         partition by range (day)",
        "create table events_p1 partition of events for values from (1) to (10)",
        "insert into events (id, day) values (1, 1)",
    ] {
        ctx.query(statement, &[]).await?;
    }

    let q = "select id, day from events where day < 100 order by id";
    assert_repopulated(&mut ctx, q, &["1"]).await?;

    ctx.query(
        "create table events_p2 partition of events for values from (10) to (20)",
        &[],
    )
    .await?;
    ctx.query("insert into events (id, day) values (2, 15)", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    // The partition's DDL postdates the role's verdict (PGC-463).
    ctx.verdict_warm(q).await?;
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(q).await?;
    assert_eq!(
        rows_of(&res).len(),
        2,
        "insert through the new partition applied in place"
    );
    let _m = assert_cache_hit(&mut ctx, m).await?;

    ctx.query("truncate events_p2", &[]).await?;
    ctx.cdc_apply_settle().await?;
    assert_repopulated(&mut ctx, q, &["1"]).await?;

    ctx.query("insert into events (id, day) values (3, 16)", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(q).await?;
    assert_eq!(rows_of(&res).len(), 2);
    let _m = assert_cache_hit(&mut ctx, m).await?;

    ctx.query("drop table events_p2", &[]).await?;
    ctx.cdc_apply_settle().await?;
    assert_repopulated(&mut ctx, q, &["1"]).await?;

    Ok(())
}

/// `TRUNCATE` of the partitioned root: pgoutput reports it as a truncate of
/// the root, and every partition's trigger signals too. The signals resolve
/// to a table already forgotten by the first, and the writer survives.
#[tokio::test]
async fn test_partitioned_root_truncate_empties_cache() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    for statement in [
        "create table events (id integer, day integer, primary key (id, day)) \
         partition by range (day)",
        "create table events_p1 partition of events for values from (1) to (10)",
        "create table events_p2 partition of events for values from (10) to (20)",
        "insert into events (id, day) values (1, 1), (2, 15)",
    ] {
        ctx.query(statement, &[]).await?;
    }

    let q = "select id, day from events where day < 100 order by id";
    assert_repopulated(&mut ctx, q, &["1", "2"]).await?;

    ctx.query("truncate events", &[]).await?;
    ctx.cdc_apply_settle().await?;
    assert_repopulated(&mut ctx, q, &[]).await?;

    Ok(())
}

/// `ALTER TABLE` on a partitioned root reaches every partition, so a query
/// cached directly on a partition must repopulate too: the signal names only
/// the root, and pgcache resolves it downward through the ancestor list it
/// loaded at registration.
#[tokio::test]
async fn test_alter_root_repopulates_directly_cached_partition() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    for statement in [
        "create table events (id integer, day integer, primary key (id, day)) \
         partition by range (day)",
        "create table events_p1 partition of events for values from (1) to (10)",
        "insert into events (id, day) values (1, 1)",
    ] {
        ctx.query(statement, &[]).await?;
    }

    let q = "select id, day from events_p1 where day < 100 order by id";
    assert_repopulated(&mut ctx, q, &["1"]).await?;

    ctx.query("alter table events add column note text", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    assert_repopulated(&mut ctx, q, &["1"]).await?;

    Ok(())
}

/// Renaming a cached table: the signal forgets it under its old name, and
/// the publication drain that follows must name it as origin now knows it,
/// or the `ALTER PUBLICATION` fails and restarts the writer.
#[tokio::test]
async fn test_rename_of_cached_table_survives_publication_drain() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table renamed_t (id integer primary key, val text)",
        &[],
    )
    .await?;
    ctx.query("insert into renamed_t (id, val) values (1, 'one')", &[])
        .await?;

    let q = "select id, val from renamed_t where id = 1";
    assert_repopulated(&mut ctx, q, &["1"]).await?;

    ctx.query("alter table renamed_t rename to renamed_t2", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    let q = "select id, val from renamed_t2 where id = 1";
    assert_repopulated(&mut ctx, q, &["1"]).await?;

    Ok(())
}

/// A legacy inheritance child with two parents: the DDL trigger's parent
/// lookup runs inside the user's statement and must not fail it (a scalar
/// subquery over `pg_inherits` did). The stream carries on afterwards.
#[tokio::test]
async fn test_multi_parent_inheritance_child_does_not_fail_ddl() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    for statement in [
        "create table parent_a (id integer primary key, val text)",
        "create table parent_b (id integer primary key, extra text)",
        "create table child_ab (note text) inherits (parent_a, parent_b)",
        "insert into parent_a (id, val) values (1, 'one')",
        "drop table child_ab",
    ] {
        ctx.query(statement, &[]).await?;
    }
    ctx.cdc_apply_settle().await?;

    let q = "select id, val from parent_a where id = 1";
    assert_repopulated(&mut ctx, q, &["1"]).await?;

    Ok(())
}
