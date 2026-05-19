//! Integration tests for CDC source-transaction framing (PGC-108).
//!
//! Each origin `BEGIN … COMMIT` becomes one cache-DB transaction on the
//! writer's dedicated write connection, spanning every CDC message of that
//! source transaction and committed at `CommitMark`. These tests exercise the
//! observable guarantees of that frame: a multi-message source transaction is
//! applied as a correct unit, intra-transaction ordering on the same row is
//! preserved, TRUNCATE is applied in-frame *and* invalidates dependent cached
//! queries, and the frame-deferred maintenance paths (publication drain /
//! eviction / generation purge) no longer deadlock against the open frame.
//!
//! `cdc_settle()` polls `/status` until `last_applied_lsn` reaches the origin
//! WAL position. Because the watermark only advances at `CommitMark` (after
//! `frame_commit`), a broken or stalled frame surfaces here as a settle
//! timeout, not just a wrong result.
//!
//! Multi-statement origin transactions use `ctx.origin.batch_execute("BEGIN;
//! …; COMMIT")` — a single PG transaction → a single pgoutput
//! BEGIN→messages→COMMIT → a single frame.

use std::io::Error;

use tokio_postgres::SimpleQueryMessage;

use crate::util::{TestContext, assert_row_at};
#[cfg(feature = "fault-injection")]
use crate::util::{assert_cache_hit, assert_cache_miss};

mod util;

/// First data cell of a single-row `SELECT count(*)` result.
fn scalar_of(msgs: &[SimpleQueryMessage]) -> Option<String> {
    msgs.iter().find_map(|m| {
        if let SimpleQueryMessage::Row(r) = m {
            Some(r.get(0).unwrap_or_default().to_owned())
        } else {
            None
        }
    })
}

/// A multi-statement source transaction (insert + update + delete on a
/// single-table Direct query) is applied as one correct unit. Single-table
/// changes are maintained in place, so this validates the frame accumulates
/// every message and commits the combined effect.
#[tokio::test]
async fn test_multi_statement_source_txn_atomic() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table sf_multi (id integer primary key, data text)",
        &[],
    )
    .await?;
    ctx.query(
        "insert into sf_multi (id, data) values (1, 'foo'), (2, 'bar'), (3, 'foo')",
        &[],
    )
    .await?;
    ctx.cdc_settle().await?;

    let q = "select id, data from sf_multi where data = 'foo' order by id";

    // Populate cache, then confirm a hit before CDC.
    ctx.simple_query(q).await?;
    ctx.cache_settle().await?;
    let res = ctx.simple_query(q).await?;
    assert_eq!(res.len(), 4, "expected 2 rows (ids 1,3) + desc + complete");

    // One source transaction: add a matching row, flip a non-matching row to
    // matching, and delete a matching row. Final `data='foo'` set = {2, 3}.
    ctx.origin
        .batch_execute(
            "BEGIN; \
             insert into sf_multi (id, data) values (4, 'foo'); \
             update sf_multi set data = 'foo' where id = 2; \
             delete from sf_multi where id = 1; \
             update sf_multi set data = 'bar' where id = 4; \
             COMMIT;",
        )
        .await
        .map_err(Error::other)?;
    ctx.cdc_settle().await?;

    let res = ctx.simple_query(q).await?;
    assert_eq!(res.len(), 4, "final data='foo' set should be ids 2 and 3");
    assert_row_at(&res, 1, &[("id", "2"), ("data", "foo")])?;
    assert_row_at(&res, 2, &[("id", "3"), ("data", "foo")])?;
    Ok(())
}

/// DELETE then re-INSERT of the same primary key within one source
/// transaction. Both land on the single write connection in the same
/// transaction, so their order is preserved and the cache ends with the
/// re-inserted row — not the (later-arriving) delete winning.
#[tokio::test]
async fn test_same_row_delete_then_reinsert_in_txn() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table sf_reinsert (id integer primary key, data text)",
        &[],
    )
    .await?;
    ctx.query(
        "insert into sf_reinsert (id, data) values (1, 'old'), (2, 'keep')",
        &[],
    )
    .await?;
    ctx.cdc_settle().await?;

    let q = "select id, data from sf_reinsert order by id";
    ctx.simple_query(q).await?;
    ctx.cache_settle().await?;

    ctx.origin
        .batch_execute(
            "BEGIN; \
             delete from sf_reinsert where id = 1; \
             insert into sf_reinsert (id, data) values (1, 'new'); \
             COMMIT;",
        )
        .await
        .map_err(Error::other)?;
    ctx.cdc_settle().await?;

    let res = ctx.simple_query(q).await?;
    assert_eq!(res.len(), 4, "expected ids 1 and 2");
    assert_row_at(&res, 1, &[("id", "1"), ("data", "new")])?;
    assert_row_at(&res, 2, &[("id", "2"), ("data", "keep")])?;
    Ok(())
}

/// TRUNCATE in a source transaction empties the single-table cache in-frame
/// and the cached query reflects the empty table after settle.
#[tokio::test]
async fn test_truncate_single_table_in_frame() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table sf_trunc (id integer primary key, data text)",
        &[],
    )
    .await?;
    ctx.query(
        "insert into sf_trunc (id, data) values (1, 'a'), (2, 'b'), (3, 'c')",
        &[],
    )
    .await?;
    ctx.cdc_settle().await?;

    let q = "select id, data from sf_trunc order by id";
    ctx.simple_query(q).await?;
    ctx.cache_settle().await?;
    let res = ctx.simple_query(q).await?;
    assert_eq!(res.len(), 5, "3 rows + desc + complete before truncate");

    ctx.origin
        .batch_execute("truncate sf_trunc")
        .await
        .map_err(Error::other)?;
    ctx.cdc_settle().await?;

    let res = ctx.simple_query(q).await?;
    assert_eq!(res.len(), 2, "0 rows + desc + complete after truncate");
    Ok(())
}

/// TRUNCATE then INSERT in the same source transaction: the cache reflects
/// only the post-truncate rows (the truncate and the inserts are one frame,
/// applied as a unit and dependent cached queries repopulate correctly).
#[tokio::test]
async fn test_truncate_then_insert_same_txn() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table sf_trunc_ins (id integer primary key, data text)",
        &[],
    )
    .await?;
    ctx.query(
        "insert into sf_trunc_ins (id, data) values (1, 'old'), (2, 'old')",
        &[],
    )
    .await?;
    ctx.cdc_settle().await?;

    let q = "select id, data from sf_trunc_ins order by id";
    ctx.simple_query(q).await?;
    ctx.cache_settle().await?;

    ctx.origin
        .batch_execute(
            "BEGIN; \
             truncate sf_trunc_ins; \
             insert into sf_trunc_ins (id, data) values (10, 'new'), (11, 'new'); \
             COMMIT;",
        )
        .await
        .map_err(Error::other)?;
    ctx.cdc_settle().await?;

    let res = ctx.simple_query(q).await?;
    assert_eq!(res.len(), 4, "only the two post-truncate rows");
    assert_row_at(&res, 1, &[("id", "10"), ("data", "new")])?;
    assert_row_at(&res, 2, &[("id", "11"), ("data", "new")])?;
    Ok(())
}

/// TRUNCATE invalidates a dependent multi-table (join) cached query: the
/// query's result rebuilds from origin and reflects the now-empty table.
/// Exercises `handle_truncate`'s mass invalidation, not just the in-frame
/// physical TRUNCATE.
#[tokio::test]
async fn test_truncate_invalidates_join_query() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table sf_j_parent (id integer primary key, name text)",
        &[],
    )
    .await?;
    ctx.query(
        "create table sf_j_child (id serial primary key, parent_id integer, val text)",
        &[],
    )
    .await?;
    ctx.query(
        "insert into sf_j_parent (id, name) values (1, 'p1'), (2, 'p2')",
        &[],
    )
    .await?;
    ctx.query(
        "insert into sf_j_child (parent_id, val) values (1, 'x'), (1, 'y'), (2, 'z')",
        &[],
    )
    .await?;
    ctx.cdc_settle().await?;

    let q = "select p.id, p.name, c.val from sf_j_parent p \
             join sf_j_child c on c.parent_id = p.id where p.id = 1 order by c.id";
    ctx.simple_query(q).await?;
    ctx.cache_settle().await?;
    let res = ctx.simple_query(q).await?;
    assert_eq!(
        res.len(),
        4,
        "2 joined rows + desc + complete before truncate"
    );

    ctx.origin
        .batch_execute("truncate sf_j_child")
        .await
        .map_err(Error::other)?;
    ctx.cdc_settle().await?;

    // The join query referenced sf_j_child → invalidated → repopulated from
    // the now-empty child table.
    let res = ctx.simple_query(q).await?;
    assert_eq!(res.len(), 2, "no joined rows after child truncate");
    Ok(())
}

/// Regression for the frame-vs-`db_cache` deadlock: a small cache (forces
/// eviction) plus a join query plus a source transaction whose changes drive
/// invalidation/eviction while the frame is open. Before the
/// frame-deferred-maintenance fix this deadlocked the writer and `cdc_settle`
/// timed out; it must now settle and return correct data.
#[tokio::test]
async fn test_txn_invalidation_under_small_cache_no_deadlock() -> Result<(), Error> {
    // ~256 KiB cache: small enough that population + CDC churn exercises the
    // eviction path while a frame is open.
    let mut ctx = TestContext::setup_small_cache(256 * 1024).await?;

    ctx.query(
        "create table sf_dl_parent (id integer primary key, name text)",
        &[],
    )
    .await?;
    ctx.query(
        "create table sf_dl_child (id serial primary key, parent_id integer, val text)",
        &[],
    )
    .await?;
    ctx.query(
        "insert into sf_dl_parent (id, name) values (1, 'p1'), (2, 'p2'), (3, 'p3')",
        &[],
    )
    .await?;
    ctx.query(
        "insert into sf_dl_child (parent_id, val) values \
         (1, 'a'), (1, 'b'), (2, 'c'), (3, 'd')",
        &[],
    )
    .await?;
    ctx.cdc_settle().await?;

    let q = "select p.id, p.name, c.val from sf_dl_parent p \
             join sf_dl_child c on c.parent_id = p.id where p.id = 1 order by c.id";
    ctx.simple_query(q).await?;
    ctx.cache_settle().await?;

    // One source transaction mixing inserts/updates/deletes across both joined
    // tables: the child insert is a growing cross-table change → invalidation
    // of the join query, whose physical cleanup runs on db_cache while the
    // frame holds locks. This is the deadlock scenario.
    ctx.origin
        .batch_execute(
            "BEGIN; \
             insert into sf_dl_child (parent_id, val) values (1, 'e'); \
             update sf_dl_parent set name = 'p1b' where id = 1; \
             delete from sf_dl_child where val = 'd'; \
             insert into sf_dl_child (parent_id, val) values (2, 'f'); \
             COMMIT;",
        )
        .await
        .map_err(Error::other)?;

    // The assertion that matters most: this returns (no settle timeout =
    // no deadlock).
    ctx.cdc_settle().await?;

    let res = ctx.simple_query(q).await?;
    // id=1 now joins child rows a, b, e (d was on parent 3; f on parent 2).
    assert_eq!(res.len(), 5, "3 joined rows for parent 1 + desc + complete");
    assert_row_at(&res, 1, &[("id", "1"), ("name", "p1b"), ("val", "a")])?;
    assert_row_at(&res, 2, &[("id", "1"), ("name", "p1b"), ("val", "b")])?;
    assert_row_at(&res, 3, &[("id", "1"), ("name", "p1b"), ("val", "e")])?;
    Ok(())
}

/// PGC-147: under population↔CDC-frame contention on one table's shared
/// source cache table, the cache stays consistent with origin and never
/// resets — `cdc_settle` always succeeds (a writer reset surfaces as 503 /
/// timeout). One-directional: red iff the bug regresses, never flaky.
/// Doesn't deterministically provoke the writer `40P01` (unreproducible
/// probabilistically — see `test_cdc_frame_deadlock_recovery`); guards the
/// contention + deferred-invalidation path.
#[tokio::test]
async fn test_cdc_frame_population_contention_consistent() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table dl_t (id integer primary key, k integer, v integer)",
        &[],
    )
    .await?;
    ctx.query(
        "insert into dl_t select g, g % 50, g % 7 from generate_series(1, 800) g",
        &[],
    )
    .await?;
    ctx.cdc_settle().await?;

    // Equality-correlated subqueries (decorrelatable per ADR-014/016/015):
    // each materializes a *different* subset of dl_t into dl_t's shared
    // source cache table, so concurrent populations break PGC-133's
    // same-row-set serialization and contend on the PK index.
    let queries = [
        "SELECT count(*) FROM dl_t o \
         WHERE EXISTS (SELECT 1 FROM dl_t i WHERE i.k = o.k AND i.v = 0)",
        "SELECT count(*) FROM dl_t o \
         WHERE o.id IN (SELECT i.id FROM dl_t i WHERE i.k = o.k AND i.v = 1)",
        "SELECT count(*) FROM dl_t o \
         WHERE o.v = (SELECT max(i.v) FROM dl_t i WHERE i.k = o.k)",
    ];

    for iter in 0..5 {
        // Trigger/refresh population of every subset, WITHOUT settling — these
        // run on the worker pool concurrently with the CDC frame below.
        for q in &queries {
            ctx.simple_query(q).await?;
        }

        // Overlapping CDC source transaction on dl_t: new rows + an update
        // touching ~1/3 of existing rows → the frame upserts rows the
        // in-flight populations are also writing.
        let lo = 801 + iter * 400;
        let hi = lo + 399;
        ctx.origin
            .batch_execute(&format!(
                "BEGIN; \
                 insert into dl_t select g, g % 50, g % 7 \
                   from generate_series({lo}, {hi}) g; \
                 update dl_t set v = (v + 1) % 7 where id % 3 = 0; \
                 COMMIT;"
            ))
            .await
            .map_err(Error::other)?;

        // Invariant: the frame committed or recovered — never reset.
        ctx.cdc_settle().await?;

        for q in &queries {
            let via_cache = ctx.simple_query(q).await?;
            let via_origin = ctx.origin.simple_query(q).await.map_err(Error::other)?;
            assert_eq!(
                scalar_of(&via_cache),
                scalar_of(&via_origin),
                "iter {iter}: pgcache disagrees with origin for `{q}`"
            );
        }
    }

    Ok(())
}

/// PGC-147 deterministic recovery test (`--features fault-injection`,
/// `--test-threads=1`). The writer `40P01`-victim path is unreproducible
/// probabilistically, so it's forced via the env one-shot
/// `PGCACHE_FAULT_CDC_DEADLOCK_ONCE`. Asserts the full recovery contract: no
/// cache reset (`cdc_settle` succeeds), the dependent query is invalidated
/// then repopulates cleanly (cache table was truncated, not left stale),
/// results correct throughout.
#[cfg(feature = "fault-injection")]
#[tokio::test]
async fn test_cdc_frame_deadlock_recovery() -> Result<(), Error> {
    // Armed at writer startup (inherited by the spawned pgcache); removed
    // immediately after so no later spawn sees it. Race-free under
    // --test-threads=1 (the harness already requires bounded threads).
    unsafe { std::env::set_var("PGCACHE_FAULT_CDC_DEADLOCK_ONCE", "1") };
    let mut ctx = TestContext::setup().await?;
    unsafe { std::env::remove_var("PGCACHE_FAULT_CDC_DEADLOCK_ONCE") };

    ctx.query(
        "create table rec_t (id integer primary key, k integer)",
        &[],
    )
    .await?;
    ctx.query(
        "insert into rec_t select g, g % 4 from generate_series(1, 40) g",
        &[],
    )
    .await?;
    ctx.cdc_settle().await?;

    let q = "SELECT count(*) FROM rec_t WHERE k = 0";

    // Cache it: miss → populate, then a confirming hit. cached_queries is now
    // non-empty, so the next CDC insert trips the injected deadlock.
    let m = ctx.metrics().await?;
    let r0 = ctx.simple_query(q).await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    let r1 = ctx.simple_query(q).await?;
    let m = assert_cache_hit(&mut ctx, m).await?;
    assert_eq!(scalar_of(&r0), scalar_of(&r1), "cached value pre-insert");

    // First CDC insert with a query cached → injected 40P01 →
    // frame_recover_enter → Recovering → CommitMark frame_recover: evict
    // rec_t's queries + truncate its cache table.
    ctx.origin
        .batch_execute("insert into rec_t (id, k) values (10001, 0)")
        .await
        .map_err(Error::other)?;

    // Primary invariant: recovery did not reset the cache.
    ctx.cdc_settle().await?;

    // Invalidated by recovery → next request is a miss, forwarded to origin,
    // returning the correct incremented count.
    let expected = ctx.origin.simple_query(q).await.map_err(Error::other)?;
    let after = ctx.simple_query(q).await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    assert_eq!(
        scalar_of(&after),
        scalar_of(&expected),
        "recovered query must match origin (includes the new row)"
    );

    // Repopulates cleanly from the truncated cache table → correct cache hit.
    ctx.cache_settle().await?;
    let rehit = ctx.simple_query(q).await?;
    assert_cache_hit(&mut ctx, m).await?;
    assert_eq!(
        scalar_of(&rehit),
        scalar_of(&expected),
        "repopulated query stays correct after recovery truncate"
    );

    Ok(())
}
