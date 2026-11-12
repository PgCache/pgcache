//! Table inheritance and `ONLY`. A read of an inheritance parent returns its
//! children's rows, which stream under the children's own identity and may
//! repeat the parent's primary key, so one cache table cannot hold the tree:
//! such a read is forwarded. `ONLY` narrows a read to the named table and is
//! forwarded too, never served from the plain read's entry.

use std::io::Error;

use crate::util::{
    TestContext, assert_cache_hit, assert_cache_miss, assert_proxy_rows_match_origin, rows_of,
};

mod util;

/// Read `q` twice with a settle between, so a cacheable read is registered
/// and populated before the test writes.
async fn warm(ctx: &mut TestContext, q: &str) -> Result<(), Error> {
    ctx.simple_query(q).await?;
    ctx.cache_settle().await?;
    ctx.simple_query(q).await?;
    Ok(())
}

/// A read of the parent follows an update of a child row, an insert into the
/// child, and a child row repeating the parent's primary key.
#[tokio::test]
async fn test_inheritance_parent_read_sees_child_writes() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    for statement in [
        "create table d2 (id int primary key, v int)",
        "create table k2 (primary key (id)) inherits (d2)",
        "insert into d2 values (1, 1)",
        "insert into k2 values (2, 2)",
    ] {
        ctx.query(statement, &[]).await?;
    }

    let q = "select id, v from d2 order by id, v";
    warm(&mut ctx, q).await?;

    ctx.query("update k2 set v = 5 where id = 2", &[]).await?;
    ctx.cdc_apply_settle().await?;
    assert_proxy_rows_match_origin(&mut ctx, q).await?;

    ctx.query("insert into k2 values (3, 3), (1, 99)", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    assert_proxy_rows_match_origin(&mut ctx, q).await?;
    let res = ctx.simple_query(q).await?;
    assert_eq!(rows_of(&res).len(), 4, "parent row and three child rows");

    let m = ctx.metrics().await?;
    assert_eq!(m.cache_restarts_total, 0);

    Ok(())
}

/// A child read by its own name is an ordinary table: cached, and kept
/// current by its own stream.
#[tokio::test]
async fn test_inheritance_child_read_is_cached() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    for statement in [
        "create table d3 (id int primary key, v int)",
        "create table k3 (primary key (id)) inherits (d3)",
        "insert into k3 values (2, 2)",
    ] {
        ctx.query(statement, &[]).await?;
    }

    let q = "select id, v from k3 order by id";
    let m = ctx.metrics().await?;
    ctx.simple_query(q).await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    ctx.simple_query(q).await?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    ctx.query("update k3 set v = 7 where id = 2", &[]).await?;
    ctx.cdc_apply_settle().await?;
    let m = ctx.metrics().await?;
    assert_proxy_rows_match_origin(&mut ctx, q).await?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// A table cached while it had no children gains one: its cached reads are
/// evicted at the `CREATE TABLE … INHERITS`, so the child's rows show.
#[tokio::test]
async fn test_inheritance_child_created_after_parent_cached() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query("create table d4 (id int primary key, v int)", &[])
        .await?;
    ctx.query("insert into d4 values (1, 1)", &[]).await?;

    let q = "select id, v from d4 order by id, v";
    let m = ctx.metrics().await?;
    ctx.simple_query(q).await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    ctx.simple_query(q).await?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    ctx.query("create table k4 (primary key (id)) inherits (d4)", &[])
        .await?;
    ctx.query("insert into k4 values (2, 2)", &[]).await?;
    ctx.cdc_apply_settle().await?;
    assert_proxy_rows_match_origin(&mut ctx, q).await?;

    ctx.query("update k4 set v = 8 where id = 2", &[]).await?;
    ctx.cdc_apply_settle().await?;
    assert_proxy_rows_match_origin(&mut ctx, q).await?;

    let m = ctx.metrics().await?;
    assert_eq!(m.cache_restarts_total, 0);

    Ok(())
}

/// The same through `ALTER TABLE … INHERIT`: an existing table with rows
/// becomes a child of a cached one.
#[tokio::test]
async fn test_inheritance_child_attached_after_parent_cached() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    for statement in [
        "create table d5 (id int primary key, v int)",
        "create table k5 (id int primary key, v int)",
        "insert into d5 values (1, 1)",
        "insert into k5 values (2, 2)",
    ] {
        ctx.query(statement, &[]).await?;
    }

    let q = "select id, v from d5 order by id, v";
    let m = ctx.metrics().await?;
    ctx.simple_query(q).await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    ctx.simple_query(q).await?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    ctx.query("alter table k5 inherit d5", &[]).await?;
    ctx.cdc_apply_settle().await?;
    assert_proxy_rows_match_origin(&mut ctx, q).await?;

    ctx.query("update k5 set v = 9 where id = 2", &[]).await?;
    ctx.cdc_apply_settle().await?;
    assert_proxy_rows_match_origin(&mut ctx, q).await?;

    let m = ctx.metrics().await?;
    assert_eq!(m.cache_restarts_total, 0);

    Ok(())
}

/// `ONLY` on an inheritance parent leaves the children out.
#[tokio::test]
async fn test_only_read_of_inheritance_parent_excludes_children() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    for statement in [
        "create table d6 (id int primary key, v int)",
        "create table k6 (primary key (id)) inherits (d6)",
        "insert into d6 values (1, 1)",
        "insert into k6 values (2, 2)",
    ] {
        ctx.query(statement, &[]).await?;
    }

    warm(&mut ctx, "select id, v from d6 order by id").await?;

    let only = "select id, v from only d6 order by id";
    assert_proxy_rows_match_origin(&mut ctx, only).await?;
    let res = ctx.simple_query(only).await?;
    assert_eq!(rows_of(&res).len(), 1, "the parent's own row");

    Ok(())
}

/// `ONLY` on a partitioned root reads no rows; the plain read of the same
/// table is cached and must not answer it.
#[tokio::test]
async fn test_only_read_is_not_served_from_the_plain_read() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    for statement in [
        "create table po (id int, k int, primary key (id, k)) partition by range (k)",
        "create table po1 partition of po for values from (0) to (10)",
        "insert into po values (1, 5)",
    ] {
        ctx.query(statement, &[]).await?;
    }

    let plain = "select id, k from po order by id";
    let m = ctx.metrics().await?;
    ctx.simple_query(plain).await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    let res = ctx.simple_query(plain).await?;
    assert_eq!(rows_of(&res).len(), 1);
    let m = assert_cache_hit(&mut ctx, m).await?;

    let only = "select id, k from only po order by id";
    let res = ctx.simple_query(only).await?;
    assert_eq!(rows_of(&res).len(), 0, "a partitioned root holds no rows");
    assert_proxy_rows_match_origin(&mut ctx, only).await?;
    let after = ctx.metrics().await?;
    assert_eq!(
        after.queries_cache_hit, m.queries_cache_hit,
        "an ONLY read is forwarded"
    );

    Ok(())
}
