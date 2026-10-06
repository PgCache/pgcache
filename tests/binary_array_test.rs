//! End-to-end regression test for **PGC-103**.
//!
//! Tokio-postgres encodes `Vec<i32>` bindings as binary `int4[]` by
//! default, and the previous catch-all UTF-8 fallback in
//! `binary_parameter_to_literal` silently corrupted the deparsed SQL
//! whenever the array header bytes (mostly NUL) happened to be valid
//! UTF-8. The fix decodes binary arrays into a proper PG text array
//! literal (`'{1,3}'::int4[]`) so the query is fully cacheable.
//!
//! This test exercises the full proxy path: tokio-postgres on the client
//! side → pgcache parameter substitution → origin → cache population →
//! second-call cache hit. The unit-level companions live in
//! `src/query/transform/parameters.rs::tests::test_binary_int4_array_*`.

use std::io::Error;

use crate::util::{TestContext, assert_cache_hit, assert_cache_miss};

mod util;

/// `WHERE id = ANY($1)` with a binary `int4[]` parameter — the exact path
/// PGC-103 was about. First call populates, second call hits cache.
/// Before the fix this either errored with "error encoding message to
/// server" or returned wrong rows; both are visible end-to-end in the
/// assertions below.
#[tokio::test]
async fn test_binary_int4_array_in_any_clause() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table widgets (id integer primary key, name text)",
        &[],
    )
    .await?;
    ctx.query(
        "insert into widgets (id, name) \
         values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')",
        &[],
    )
    .await?;

    let stmt = ctx
        .prepare("select id, name from widgets where id = any($1) order by id")
        .await?;

    // tokio-postgres binds Vec<i32> as binary int4[] (oid 1007).
    let ids: Vec<i32> = vec![1, 3];

    let m = ctx.metrics().await?;
    let rows = ctx.query(&stmt, &[&ids]).await?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>("id"), 1);
    assert_eq!(rows[0].get::<_, &str>("name"), "a");
    assert_eq!(rows[1].get::<_, i32>("id"), 3);
    assert_eq!(rows[1].get::<_, &str>("name"), "c");
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Second execution with the same array — same fingerprint, cache hit.
    let rows = ctx.query(&stmt, &[&ids]).await?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>("id"), 1);
    assert_eq!(rows[1].get::<_, i32>("id"), 3);
    let _ = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// PGC-106 (option C): a narrower binary `int4[]` array hits the cached
/// entry of a wider one via subsumption — the cache holds rows
/// matching `[1, 2, 3]`, the new query asks for `[1]`, the worker runs
/// the new SQL against the cache and returns `[1]`. Before the option-C
/// constraint extraction, the analyzer didn't recognize `MultiOp::Any`
/// and option-B's safety gate refused the subsumption (every distinct
/// array got its own miss).
#[tokio::test]
async fn test_any_subsumes_narrower_any() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query("create table widgets (id integer primary key)", &[])
        .await?;
    ctx.query(
        "insert into widgets (id) values (1), (2), (3), (4), (5)",
        &[],
    )
    .await?;

    let stmt = ctx
        .prepare("select id from widgets where id = any($1) order by id")
        .await?;

    // Cache the wider set first.
    let m = ctx.metrics().await?;
    let r1 = ctx.query(&stmt, &[&vec![1i32, 2, 3]]).await?;
    assert_eq!(
        r1.iter().map(|r| r.get::<_, i32>("id")).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    // Narrower subset: subsumption hit, returns the right row.
    let r2 = ctx.query(&stmt, &[&vec![1i32]]).await?;
    assert_eq!(
        r2.iter().map(|r| r.get::<_, i32>("id")).collect::<Vec<_>>(),
        vec![1]
    );
    let _ = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// PGC-106 regression: same prepared statement, two different binary
/// `int4[]` parameter values must each get their own cache entry and
/// return the right rows.
///
/// Before the constraint-analysis fix in `src/query/constraints.rs`, the
/// second array was incorrectly considered "subsumed" by the first call's
/// cache (because `MultiOp::Any` produced no extracted constraints, and
/// the absence of constraints was conflated with "full table scan, all
/// rows loaded"). pgcache then ran the new query against the first
/// array's cached rows — empty intersection, returning `[]` instead of
/// the correct rows.
#[tokio::test]
async fn test_distinct_arrays_get_distinct_cache_entries() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query("create table widgets (id integer primary key)", &[])
        .await?;
    ctx.query(
        "insert into widgets (id) values (1), (2), (3), (4), (5)",
        &[],
    )
    .await?;

    let stmt = ctx
        .prepare("select id from widgets where id = any($1) order by id")
        .await?;

    // First array: cache miss, populates entry for `[1, 2]`.
    let m = ctx.metrics().await?;
    let r1 = ctx.query(&stmt, &[&vec![1i32, 2]]).await?;
    assert_eq!(r1.len(), 2);
    assert_eq!(r1[0].get::<_, i32>("id"), 1);
    assert_eq!(r1[1].get::<_, i32>("id"), 2);
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    // Same array → cache hit on the same entry.
    let _ = ctx.query(&stmt, &[&vec![1i32, 2]]).await?;
    let m = assert_cache_hit(&mut ctx, m).await?;

    // Different array — must NOT be subsumed by the previous entry.
    // Before PGC-106 fix: returned `[]` and reported a hit.
    let r2 = ctx.query(&stmt, &[&vec![3i32, 4, 5]]).await?;
    assert_eq!(
        r2.iter().map(|r| r.get::<_, i32>("id")).collect::<Vec<_>>(),
        vec![3, 4, 5]
    );
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    // Same different array — hits its own newly-populated entry.
    let _ = ctx.query(&stmt, &[&vec![3i32, 4, 5]]).await?;
    let _ = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}
