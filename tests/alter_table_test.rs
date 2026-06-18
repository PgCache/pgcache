use std::io::Error;

use crate::util::{TestContext, assert_cache_hit, assert_cache_miss, assert_row_at};

mod util;

/// An origin `ALTER TABLE ADD COLUMN` on a cached relation must let the query
/// repopulate after the schema change.
///
/// Regression test for the staging-table pool (PGC-293): pooled staging tables
/// are `CREATE (LIKE cache_table)` once and reused across populations. A schema
/// change recreates the cache table with a new shape and the merge then names
/// the new column set (`INSERT INTO cache(id,val,extra) SELECT id,val,extra FROM
/// staging`). If the pool weren't purged, the next population would reuse an
/// old-shape staging table that lacks the new column, the merge would error, and
/// the query would never re-cache. The per-relation epoch purge drops the stale
/// pooled tables so the population mints a fresh-shape one and the query caches
/// again — observed here as the post-ALTER query becoming a cache hit.
///
/// Uses explicit columns (not `SELECT *`) so the test exercises the staging-pool
/// shape path specifically, independent of star re-expansion.
#[tokio::test]
async fn test_alter_table_add_column_repopulates() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table alter_t (id integer primary key, val text)",
        &[],
    )
    .await?;
    ctx.query("insert into alter_t (id, val) values (1, 'one')", &[])
        .await?;

    // Populate the query — this creates a staging table for the relation in the
    // pool, freed back to the pool after the merge.
    let m = ctx.metrics().await?;
    let res = ctx
        .simple_query("select id, val from alter_t where id = 1")
        .await?;
    assert_row_at(&res, 1, &[("id", "1"), ("val", "one")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;
    let res = ctx
        .simple_query("select id, val from alter_t where id = 1")
        .await?;
    assert_row_at(&res, 1, &[("id", "1"), ("val", "one")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    // Change the relation's shape, then write to it again so logical replication
    // resends the relation with its new shape; `cdc_settle` then waits for
    // pgcache to apply that relation message (schema change → invalidate +
    // recreate cache table + staging-pool purge) before the next read.
    ctx.query("alter table alter_t add column extra text", &[])
        .await?;
    ctx.query("update alter_t set extra = 'e1' where id = 1", &[])
        .await?;
    ctx.cdc_settle().await?;

    // The query was invalidated by the schema change → miss, then repopulates
    // against the new shape via a freshly minted staging table. Without the pool
    // purge the repopulation's merge would fail on a stale-shape staging table
    // and this would never become a cache hit.
    let m = ctx.metrics().await?;
    let res = ctx
        .simple_query("select id, val from alter_t where id = 1")
        .await?;
    assert_row_at(&res, 1, &[("id", "1"), ("val", "one")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;
    let res = ctx
        .simple_query("select id, val from alter_t where id = 1")
        .await?;
    assert_row_at(&res, 1, &[("id", "1"), ("val", "one")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}
