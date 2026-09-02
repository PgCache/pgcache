//! Regression test for PGC-96: source-row cache generated
//! `INSERT ... ON CONFLICT (<pk>) DO UPDATE SET ` with an empty SET list for
//! tables whose only columns are primary keys. PG rejects the SQL syntax and
//! population silently failed with the cache entry stuck at `Loading`.
//!
//! Covers three code paths that generate upsert SQL:
//!   1. `insert_statement_build` in writer/population.rs (initial population) —
//!      emits `DO UPDATE SET <pk> = <pk>` to fire the tracking trigger.
//!   2. `handle_pg_eval_cdc_sql` in writer/cdc.rs (CDC PG-eval upsert) —
//!      emits `DO NOTHING` (no non-PK values to apply).
//!   3. `cache_upsert_unconditional_sql` in writer/cdc.rs (CDC LocalEval fast
//!      path) — emits `DO NOTHING`.
//!
//! All three avoid the empty-SET syntax error.

use std::io::Error;

use crate::util::{TestContext, assert_cache_hit, assert_cache_miss, connect_cache_db};

mod util;

/// PK-only table: verifies initial population and CDC-driven in-place updates
/// both succeed.
#[tokio::test]
async fn test_pk_only_table_caching() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query("CREATE TABLE pk_only (id integer primary key)", &[])
        .await?;
    ctx.query("INSERT INTO pk_only (id) VALUES (1), (2), (3)", &[])
        .await?;

    // First query — cache miss, exercises insert_statement_build (site 1).
    let m = ctx.metrics().await?;
    let res = ctx.simple_query("SELECT id FROM pk_only").await?;
    let rows: Vec<_> = res
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .collect();
    assert_eq!(rows.len(), 3, "first query returns 3 rows");
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Second query — cache hit. Proves population (site 1) succeeded.
    let res = ctx.simple_query("SELECT id FROM pk_only").await?;
    let rows: Vec<_> = res
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .collect();
    assert_eq!(rows.len(), 3);
    let m = assert_cache_hit(&mut ctx, m).await?;

    // CDC INSERT — single-table unconditional upsert path (site 3,
    // cache_upsert_unconditional_sql in LocalEval).
    ctx.origin_query("INSERT INTO pk_only (id) VALUES (4)", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    let res = ctx.simple_query("SELECT id FROM pk_only").await?;
    let rows: Vec<_> = res
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .collect();
    assert_eq!(rows.len(), 4, "CDC INSERT landed in cache");
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Generation-tracker regression: population must link pre-existing cache rows
/// (inserted via CDC before the query was cached) to the new query's
/// generation. Before the `DO UPDATE SET <pk> = <pk>` change, population's
/// `DO NOTHING` on conflict skipped the tracking trigger for PK-only tables,
/// leaving those rows unlinked from any generation and subject to premature
/// purge.
#[tokio::test]
async fn test_pk_only_population_stamps_generation() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query("CREATE TABLE pk_only_gen (id integer primary key)", &[])
        .await?;

    // Insert at origin so the rows land in the cache via CDC — NOT via
    // population. Wait for CDC to apply them before triggering the cache
    // query. Without this wait, population races CDC and may insert the
    // rows fresh (no conflict) instead of exercising the conflict path.
    ctx.origin_query("INSERT INTO pk_only_gen (id) VALUES (10), (20), (30)", &[])
        .await?;
    ctx.cdc_decode_settle().await?;

    // First cached query — population runs against a cache DB that already
    // contains the three rows, so every row hits the ON CONFLICT branch.
    let res = ctx.simple_query("SELECT id FROM pk_only_gen").await?;
    let rows: Vec<_> = res
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .collect();
    assert_eq!(rows.len(), 3);

    ctx.cache_settle().await?;

    // Dump the generation tracker and count entries for this table at gen > 0.
    // The tracker is keyed by pk_hash, computed from the row's ctid in the
    // cache DB (same helper the tracker trigger uses).
    let cache_db = connect_cache_db(&ctx.dbs).await?;
    let entries = cache_db
        .query(
            "SELECT d.generation \
             FROM pk_only_gen t \
             JOIN pgcache_generation_dump() d \
               ON d.table_oid = 'pk_only_gen'::regclass \
              AND d.pk_hash = pgcache_pk_hash(d.table_oid, t.ctid) \
             WHERE d.generation > 0",
            &[],
        )
        .await
        .map_err(Error::other)?;

    assert_eq!(
        entries.len(),
        3,
        "expected one tracker entry per row after population fires the trigger \
         on conflict; got {}",
        entries.len()
    );

    Ok(())
}
