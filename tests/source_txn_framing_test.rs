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
use std::time::Duration;

use tokio_postgres::SimpleQueryMessage;

use crate::util::{TestContext, assert_row_at};
#[cfg(feature = "fault-injection")]
use crate::util::{assert_cache_hit, assert_cache_miss};

mod util;

/// First data cell of a single-row `SELECT count(*)` result.
#[cfg(feature = "fault-injection")]
fn row_count(msgs: &[SimpleQueryMessage]) -> usize {
    msgs.iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .count()
}

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

/// Regression for the frame-vs-`db_cache` deadlock: a join query plus a source
/// transaction whose changes invalidate it while the frame is open, so the
/// invalidation's physical cleanup (generation purge) would run on `db_cache`
/// against the frame's locks. Before the frame-deferred-maintenance fix this
/// deadlocked the writer and `cdc_settle` timed out; the cleanup must now defer
/// to `CommitMark`, so this settles and returns correct data.
#[tokio::test]
async fn test_txn_invalidation_under_small_cache_no_deadlock() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

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

/// One fat source transaction updating ~70 rows of a relation referenced by
/// several join (PgEval) queries — exercises the batched per-segment
/// membership eval (PGC-241) across the `PG_EVAL_ROW_CHUNK` (64) boundary and
/// across multiple queries in one chunk, mixed with deletes and a join-key
/// move so the ordered decide/emit pass runs every path. Correctness oracle:
/// every per-group join query serves exactly what origin serves.
#[tokio::test]
async fn test_fat_frame_batched_membership_maintains_joins() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.simple_query("create table bm_groups (id int primary key, v int not null)")
        .await?;
    ctx.simple_query(
        "create table bm_items (id int primary key, gid int not null, data int not null)",
    )
    .await?;
    ctx.simple_query("insert into bm_groups (id, v) values (1, 10), (2, 20), (3, 30)")
        .await?;
    // 72 items, 24 per group: gid cycles 1,2,3.
    ctx.simple_query(
        "insert into bm_items (id, gid, data) \
         select i, (i % 3) + 1, i from generate_series(1, 72) i",
    )
    .await?;
    ctx.cdc_settle().await?;

    let group_query = |g: i32| {
        format!(
            "select i.id, i.data, g.v from bm_items i \
             join bm_groups g on i.gid = g.id where g.id = {g} order by i.id"
        )
    };

    // Register + populate the three per-group join queries (PgEval, batchable).
    for g in 1..=3 {
        ctx.simple_query(&group_query(g)).await?;
    }
    ctx.cache_settle().await?;

    // One source transaction: 70 data-only updates (in-place maintenance via
    // batched membership), two deletes, and one join-key move (invalidates the
    // affected groups). 73 row events in one frame.
    let mut txn = String::from("BEGIN; ");
    for id in 1..=70 {
        txn.push_str(&format!(
            "update bm_items set data = data + 1000 where id = {id}; "
        ));
    }
    txn.push_str("delete from bm_items where id = 5; ");
    txn.push_str("delete from bm_items where id = 40; ");
    txn.push_str("update bm_items set gid = 2 where id = 10; ");
    txn.push_str("COMMIT;");
    ctx.origin.batch_execute(&txn).await.map_err(Error::other)?;
    ctx.cdc_settle().await?;
    ctx.cache_settle().await?;

    // Every per-group join result must match origin exactly.
    for g in 1..=3 {
        let q = group_query(g);
        let served = ctx.simple_query(&q).await?;
        let origin_rows = ctx.origin_query(&q as &str, &[]).await?;
        let served_rows: Vec<(String, String, String)> = served
            .iter()
            .filter_map(|m| match m {
                SimpleQueryMessage::Row(row) => Some((
                    row.get(0).unwrap_or_default().to_owned(),
                    row.get(1).unwrap_or_default().to_owned(),
                    row.get(2).unwrap_or_default().to_owned(),
                )),
                SimpleQueryMessage::CommandComplete(_)
                | SimpleQueryMessage::RowDescription(_)
                | _ => None,
            })
            .collect();
        let expected: Vec<(String, String, String)> = origin_rows
            .iter()
            .map(|row| {
                (
                    row.get::<_, i32>(0).to_string(),
                    row.get::<_, i32>(1).to_string(),
                    row.get::<_, i32>(2).to_string(),
                )
            })
            .collect();
        assert_eq!(
            served_rows, expected,
            "group {g} join result must match origin after the fat frame"
        );
    }

    Ok(())
}

/// A row matched by a LocalEval query must not trigger any PgEval round-trip
/// for the relation's non-Fresh PgEval queries — the per-row path's
/// `if !matched` short-circuit, preserved through the batched segment eval
/// (PGC-241). Regression test for the pgbench mixed-workload slowdown where
/// the unconditional batch evaluated every PgEval query per write event.
#[tokio::test]
async fn test_local_match_short_circuits_batched_pg_eval() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.simple_query(
        "create table sc_main (id int primary key, grp int not null, data int not null)",
    )
    .await?;
    ctx.simple_query("create table sc_other (id int primary key, ref int not null)")
        .await?;
    ctx.simple_query(
        "insert into sc_main (id, grp, data) values (1, 1, 10), (2, 2, 20), (3, 1, 30)",
    )
    .await?;
    ctx.simple_query("insert into sc_other (id, ref) values (1, 1), (2, 2)")
        .await?;
    ctx.cdc_settle().await?;

    // LocalEval over sc_main (single table, simple predicate)…
    ctx.simple_query("select id, data from sc_main where grp = 1")
        .await?;
    // …and a PgEval join over the same relation (non-Fresh: no MV at this size).
    ctx.simple_query(
        "select m.id, o.id from sc_main m join sc_other o on o.ref = m.grp where m.grp = 1",
    )
    .await?;
    ctx.cache_settle().await?;

    // A data-only update to a row the LocalEval query matches (grp=1): the
    // local match decides the upsert, so the join's membership must not be
    // evaluated at all — zero PgEval statements.
    let before = ctx.metrics().await?;
    ctx.origin_query("update sc_main set data = data + 1 where id = 1", &[])
        .await?;
    ctx.cdc_settle().await?;
    let after = ctx.metrics().await?;
    assert_eq!(
        after.cache_cdc_pg_eval_hits - before.cache_cdc_pg_eval_hits,
        0,
        "locally-matched row must not round-trip PgEval membership"
    );

    // Control: a row no LocalEval query matches (grp=2 is outside the local
    // query's predicate) must evaluate the rest set — at least one statement.
    let before = ctx.metrics().await?;
    ctx.origin_query("update sc_main set data = data + 1 where id = 2", &[])
        .await?;
    ctx.cdc_settle().await?;
    let after = ctx.metrics().await?;
    assert!(
        after.cache_cdc_pg_eval_hits > before.cache_cdc_pg_eval_hits,
        "unmatched row still evaluates the rest set"
    );

    Ok(())
}

/// Canary for intra-txn DDL handling: a mid-transaction Relation message
/// must not let buffered row events replay against the NEW table metadata
/// (position-based lookups misalign old-shape tuples). The fix discards the
/// relation's buffered events and lets the recreate + abort watermark rebuild
/// from origin.
///
/// Note: the end state asserted here also survives WITHOUT the event drop in
/// most orderings (the recreate's query eviction masks the misaligned replay)
/// — the drop is defense in depth for the racy windows (a query registering
/// between the DDL and CommitMark, or a recording population with async
/// deactivation). This test's teeth are against regressions that break the
/// flow outright (it caught a replay-before-register variant whose buffered
/// old-shape SQL flushed against the recreated table).
#[tokio::test]
async fn test_intra_txn_ddl_does_not_misalign_buffered_events() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.simple_query(
        "create table sf_ddl (id int primary key, a int not null, b int not null, c int not null)",
    )
    .await?;
    ctx.simple_query("insert into sf_ddl (id, a, b, c) values (1, 10, 20, 30), (2, 11, 21, 31)")
        .await?;
    ctx.cdc_settle().await?;

    let q = "select id, a, c from sf_ddl order by id";
    ctx.simple_query(q).await?;
    ctx.cache_settle().await?;

    // One source transaction: an update captured under the 4-column layout,
    // then DDL (Relation message mid-frame), then an update under the new
    // 3-column layout.
    ctx.origin
        .batch_execute(
            "BEGIN; \
             update sf_ddl set a = 100 where id = 1; \
             alter table sf_ddl drop column b; \
             update sf_ddl set c = 300 where id = 2; \
             COMMIT;",
        )
        .await
        .map_err(Error::other)?;
    ctx.cdc_settle().await?;
    // The schema change invalidates and repopulates the query.
    ctx.cache_settle().await?;

    let served = ctx.simple_query(q).await?;
    assert_row_at(&served, 1, &[("id", "1"), ("a", "100"), ("c", "30")])?;
    assert_row_at(&served, 2, &[("id", "2"), ("a", "11"), ("c", "300")])?;

    Ok(())
}

/// Companion to the above for the case where a partial replay
/// (FRAME_ROWS_CAPACITY) has already moved the DDL'd relation's writes into the
/// frame buffer before the schema change arrives. Discarding `frame_rows`
/// can't retract those buffered/executed old-layout writes, so at COMMIT they
/// would run against the recreated table and abort the writer (a full cache
/// reset). The writer must instead recover the frame in-band — roll the cache
/// txn back and repopulate the frame's relations from post-DDL origin —
/// keeping the rest of the cache warm.
#[tokio::test]
async fn test_intra_txn_ddl_after_partial_replay_recovers_in_band() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.simple_query(
        "create table sf_ddlp (id int primary key, drop_me int not null, keep int not null, n int not null)",
    )
    .await?;
    ctx.simple_query("insert into sf_ddlp (id, drop_me, keep, n) values (1, 9, 20, 1)")
        .await?;
    ctx.simple_query("create table sf_ddlp_filler (id int primary key, v int not null)")
        .await?;
    // Enough rows that one bulk update overflows FRAME_ROWS_CAPACITY (4096),
    // forcing a partial replay before the DDL arrives.
    ctx.simple_query("insert into sf_ddlp_filler select g, 0 from generate_series(1, 4200) g")
        .await?;
    ctx.cdc_settle().await?;

    let q = "select id, keep from sf_ddlp where id = 1";
    ctx.simple_query(q).await?;
    // Track the filler so its bulk update reaches the writer and splits the frame.
    let qf = "select v from sf_ddlp_filler where id = 1";
    ctx.simple_query(qf).await?;
    ctx.cache_settle().await?;

    let restarts_before = ctx.metrics().await?.cache_restarts_total;

    // One transaction: an sf_ddlp update captured under the 4-column layout
    // (replayed into the frame buffer when the filler overflows frame_rows),
    // then DROP COLUMN, then another sf_ddlp update under the new layout.
    ctx.origin
        .batch_execute(
            "BEGIN; \
             update sf_ddlp set keep = 200, drop_me = 99 where id = 1; \
             update sf_ddlp_filler set v = v + 1; \
             alter table sf_ddlp drop column drop_me; \
             update sf_ddlp set n = 2 where id = 1; \
             COMMIT;",
        )
        .await
        .map_err(Error::other)?;
    ctx.cdc_settle_with_timeout(Duration::from_secs(30)).await?;
    ctx.cache_settle().await?;

    // In-band recovery: no writer death / cache reset.
    assert_eq!(
        ctx.metrics().await?.cache_restarts_total,
        restarts_before,
        "mid-frame DDL after a partial replay reset the whole cache instead of \
         recovering the frame in-band"
    );
    // The DDL'd relation repopulates from post-DDL origin.
    let served = ctx.simple_query(q).await?;
    assert_row_at(&served, 1, &[("id", "1"), ("keep", "200")])?;
    // Whole-frame recovery must not lose the other relation's committed change.
    assert_eq!(
        scalar_of(&ctx.simple_query(qf).await?).as_deref(),
        Some("1"),
        "filler relation lost its in-frame update after recovery"
    );

    Ok(())
}

/// PGC-242: while frames accumulate (fault-held batch), the applied watermark
/// must not advance and the cache must keep serving the pre-batch state; the
/// flush applies all batched frames atomically.
#[cfg(feature = "fault-injection")]
#[tokio::test]
async fn test_batch_holds_watermark_until_flush() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&[("PGCACHE_FAULT_CDC_HOLD_FLUSH_FRAMES", "3")]).await?;

    ctx.simple_query("create table bt_w (id int primary key, v int not null)")
        .await?;
    ctx.simple_query("insert into bt_w (id, v) values (1, 10)")
        .await?;
    ctx.cdc_settle().await?;

    let q = "select id, v from bt_w order by id";
    ctx.simple_query(q).await?;
    ctx.cache_settle().await?;

    // Two frames accumulate (hold = 3): the watermark must NOT advance and
    // the cached query must keep serving the pre-batch state.
    ctx.origin_query("update bt_w set v = 11 where id = 1", &[])
        .await?;
    ctx.origin_query("insert into bt_w (id, v) values (2, 20)", &[])
        .await?;
    assert!(
        ctx.cdc_settle_with_timeout(Duration::from_secs(2))
            .await
            .is_err(),
        "watermark advanced past unflushed batched frames"
    );
    let before = ctx.metrics().await?;
    let served = ctx.simple_query(q).await?;
    assert_cache_hit(&mut ctx, before).await?;
    assert_eq!(row_count(&served), 1, "batched frames visible before flush");
    assert_row_at(&served, 1, &[("id", "1"), ("v", "10")])?;

    // The third frame reaches the hold threshold: flush applies all three
    // atomically and the watermark advances.
    ctx.origin_query("insert into bt_w (id, v) values (3, 30)", &[])
        .await?;
    ctx.cdc_settle().await?;
    let before = ctx.metrics().await?;
    let served = ctx.simple_query(q).await?;
    assert_cache_hit(&mut ctx, before).await?;
    assert_eq!(
        row_count(&served),
        3,
        "all batched frames visible after flush"
    );
    assert_row_at(&served, 1, &[("id", "1"), ("v", "11")])?;

    Ok(())
}

/// PGC-242: same-PK sequences across batched frames apply in arrival order —
/// insert-then-delete nets to absent, delete-then-reinsert nets to the new
/// row (the cross-frame extension of the within-frame ordering tests).
#[cfg(feature = "fault-injection")]
#[tokio::test]
async fn test_batch_cross_frame_same_pk_sequences() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&[("PGCACHE_FAULT_CDC_HOLD_FLUSH_FRAMES", "4")]).await?;

    ctx.simple_query("create table bt_pk (id int primary key, v text not null)")
        .await?;
    ctx.simple_query("insert into bt_pk (id, v) values (1, 'keep'), (2, 'old')")
        .await?;
    ctx.cdc_settle().await?;

    let q = "select id, v from bt_pk order by id";
    ctx.simple_query(q).await?;
    ctx.cache_settle().await?;

    // Four frames in one batch: insert 10 then delete it (→ absent); delete 2
    // then reinsert it with a new value (→ present, new value).
    ctx.origin_query("insert into bt_pk (id, v) values (10, 'transient')", &[])
        .await?;
    ctx.origin_query("delete from bt_pk where id = 10", &[])
        .await?;
    ctx.origin_query("delete from bt_pk where id = 2", &[])
        .await?;
    ctx.origin_query("insert into bt_pk (id, v) values (2, 'new')", &[])
        .await?;
    ctx.cdc_settle().await?;
    ctx.cache_settle().await?;

    let served = ctx.simple_query(q).await?;
    assert_eq!(
        row_count(&served),
        2,
        "insert+delete nets out; reinsert survives"
    );
    assert_row_at(&served, 1, &[("id", "1"), ("v", "keep")])?;
    assert_row_at(&served, 2, &[("id", "2"), ("v", "new")])?;

    Ok(())
}

/// PGC-242: a population's watermark gate must not deadlock against a held
/// batch — the deferred-Ready nudge requests a keepalive, and KeepAliveMark
/// forces a flush before advancing the watermark.
#[cfg(feature = "fault-injection")]
#[tokio::test]
async fn test_batch_keepalive_forces_flush() -> Result<(), Error> {
    let mut ctx =
        TestContext::setup_fault(&[("PGCACHE_FAULT_CDC_HOLD_FLUSH_FRAMES", "100")]).await?;

    ctx.simple_query("create table bt_ka (id int primary key, v int not null)")
        .await?;
    ctx.simple_query("insert into bt_ka (id, v) values (1, 10)")
        .await?;
    ctx.cdc_settle().await?;

    // Open a batch the hold will never release on its own.
    ctx.origin_query("update bt_ka set v = 11 where id = 1", &[])
        .await?;
    ctx.origin_query("insert into bt_ka (id, v) values (2, 20)", &[])
        .await?;

    // A new query populates; its deferred-Ready gate waits for the watermark,
    // nudging a keepalive — which must flush the held batch.
    let q = "select id, v from bt_ka order by id";
    ctx.simple_query(q).await?;
    ctx.cache_settle().await?;

    let before = ctx.metrics().await?;
    let served = ctx.simple_query(q).await?;
    assert_cache_hit(&mut ctx, before).await?;
    assert_eq!(row_count(&served), 2);
    assert_row_at(&served, 1, &[("id", "1"), ("v", "11")])?;
    assert_row_at(&served, 2, &[("id", "2"), ("v", "20")])?;

    Ok(())
}

/// PGC-242 + review-named case: an UPDATE that reverts a value across batched
/// frames (A→B→A) — frame 2's diff against the pre-batch baseline sees "no
/// change", but frame 1's flagged it; the union must invalidate/maintain
/// correctly and the final served state must match origin.
#[cfg(feature = "fault-injection")]
#[tokio::test]
async fn test_batch_value_revert_across_frames() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&[("PGCACHE_FAULT_CDC_HOLD_FLUSH_FRAMES", "3")]).await?;

    ctx.simple_query("create table bt_rev (id int primary key, v int not null)")
        .await?;
    ctx.simple_query("insert into bt_rev (id, v) values (1, 5), (2, 7)")
        .await?;
    ctx.cdc_settle().await?;

    // A LIMIT query makes v a window column (change-dependent invalidation).
    let q = "select id, v from bt_rev where v > 0 order by v, id limit 10";
    ctx.simple_query(q).await?;
    ctx.cache_settle().await?;

    ctx.origin_query("update bt_rev set v = 99 where id = 1", &[])
        .await?;
    ctx.origin_query("update bt_rev set v = 5 where id = 1", &[])
        .await?;
    ctx.origin_query("insert into bt_rev (id, v) values (3, 8)", &[])
        .await?;
    ctx.cdc_settle().await?;
    ctx.cache_settle().await?;

    let served = ctx.simple_query(q).await?;
    assert_eq!(row_count(&served), 3);
    assert_row_at(&served, 1, &[("id", "1"), ("v", "5")])?;
    assert_row_at(&served, 2, &[("id", "2"), ("v", "7")])?;
    assert_row_at(&served, 3, &[("id", "3"), ("v", "8")])?;

    Ok(())
}

/// PGC-242: a 40P01 mid-batch recovers every relation any batched frame
/// touched and advances the watermark past the whole batch — composing the
/// PGC-147 recovery one-shot with a fault-held multi-frame batch.
#[cfg(feature = "fault-injection")]
#[tokio::test]
async fn test_batch_deadlock_recovery_covers_all_frames() -> Result<(), Error> {
    unsafe { std::env::set_var("PGCACHE_FAULT_CDC_DEADLOCK_ONCE", "1") };
    let mut ctx = TestContext::setup_fault(&[("PGCACHE_FAULT_CDC_HOLD_FLUSH_FRAMES", "3")]).await?;
    unsafe { std::env::remove_var("PGCACHE_FAULT_CDC_DEADLOCK_ONCE") };

    ctx.simple_query("create table bt_rec_a (id int primary key, v int not null)")
        .await?;
    ctx.simple_query("create table bt_rec_b (id int primary key, v int not null)")
        .await?;
    ctx.simple_query("insert into bt_rec_a (id, v) values (1, 10)")
        .await?;
    ctx.simple_query("insert into bt_rec_b (id, v) values (1, 100)")
        .await?;
    ctx.cdc_settle().await?;

    let qa = "select id, v from bt_rec_a order by id";
    let qb = "select id, v from bt_rec_b order by id";
    ctx.simple_query(qa).await?;
    ctx.simple_query(qb).await?;
    ctx.cache_settle().await?;

    // Frame 1 touches bt_rec_a; frame 2's insert into bt_rec_b trips the
    // injected deadlock (cached queries exist) → Recovering mid-batch; the
    // frame's CommitMark force-flushes into recovery over BOTH relations.
    ctx.origin_query("update bt_rec_a set v = 11 where id = 1", &[])
        .await?;
    ctx.origin_query("insert into bt_rec_b (id, v) values (2, 200)", &[])
        .await?;
    ctx.cdc_settle().await?;
    // Recovery evicted the queries; repopulation restores correct state.
    ctx.cache_settle().await?;

    let served = ctx.simple_query(qa).await?;
    assert_row_at(&served, 1, &[("id", "1"), ("v", "11")])?;
    let served = ctx.simple_query(qb).await?;
    assert_eq!(
        row_count(&served),
        2,
        "recovered relation reflects the batch"
    );
    assert_row_at(&served, 2, &[("id", "2"), ("v", "200")])?;

    Ok(())
}
