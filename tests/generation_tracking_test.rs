use std::io::Error;

use crate::util::{TestContext, connect_cache_db};

mod util;

/// Test that generation tracking records row access with correct generation numbers
#[tokio::test]
async fn test_generation_tracking_basic() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Create test table
    ctx.query("create table test (id integer primary key, data text)", &[])
        .await?;

    ctx.query(
        "insert into test (id, data) values (1, 'foo'), (2, 'bar'), (3, 'baz')",
        &[],
    )
    .await?;

    // Wait for CDC to process the inserts before caching
    ctx.cdc_decode_settle().await?;

    // First cached query - should get generation 1
    let _res = ctx
        .simple_query("select id, data from test where id = 1")
        .await?;

    ctx.cache_settle().await?;

    // Execute the cached query again (cache hit) - this records row access
    let _res = ctx
        .simple_query("select id, data from test where id = 1")
        .await?;

    // Connect directly to cache db to check generation tracking
    let cache_db = connect_cache_db(&ctx.dbs).await?;

    // Dump all generation entries to see what was tracked
    let entries = cache_db
        .query(
            "SELECT t.id, t.ctid, d.generation FROM test t \
            JOIN pgcache_generation_dump() d
            ON d.table_oid = 'test'::regclass \
                AND d.pk_hash = pgcache_pk_hash(d.table_oid, t.ctid) \
            WHERE t.id = 1 AND d.generation = 1",
            &[],
        )
        .await
        .map_err(Error::other)?;

    // Should have one entry for the row we accessed
    assert_eq!(
        entries.len(),
        1,
        "Should have generation tracking entries after cache hit"
    );

    // Verify the generation is 1 (first cached query)
    let generation: i64 = entries[0].get("generation");
    assert_eq!(generation, 1, "First cached query should have generation 1");

    Ok(())
}

/// Test that multiple cached queries get incrementing generation numbers
#[tokio::test]
async fn test_generation_tracking_multiple_queries() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Create test table
    ctx.query("create table test (id integer primary key, data text)", &[])
        .await?;

    ctx.query(
        "insert into test (id, data) values (1, 'foo'), (2, 'bar'), (3, 'baz')",
        &[],
    )
    .await?;

    // Wait for CDC to process the inserts before caching
    ctx.cdc_decode_settle().await?;

    // First cached query - generation 1
    let _res = ctx
        .simple_query("select id, data from test where id = 1")
        .await?;

    ctx.cache_settle().await?;

    // Second cached query (different WHERE) - generation 2
    let _res = ctx
        .simple_query("select id, data from test where id = 2")
        .await?;

    ctx.cache_settle().await?;

    // Execute both cached queries to record row access
    let _res = ctx
        .simple_query("select id, data from test where id = 1")
        .await?;
    let _res = ctx
        .simple_query("select id, data from test where id = 2")
        .await?;

    // Connect directly to cache db
    let cache_db = connect_cache_db(&ctx.dbs).await?;

    // Check generation for row id=1 (should be generation 1)
    let entries_id1 = cache_db
        .query(
            "SELECT t.id, t.ctid, d.generation FROM test t \
            JOIN pgcache_generation_dump() d
            ON d.table_oid = 'test'::regclass \
                AND d.pk_hash = pgcache_pk_hash(d.table_oid, t.ctid) \
            WHERE t.id = 1 AND d.generation = 1",
            &[],
        )
        .await
        .map_err(Error::other)?;

    assert_eq!(
        entries_id1.len(),
        1,
        "Should have generation 1 entry for id=1"
    );

    // Check generation for row id=2 (should be generation 2)
    let entries_id2 = cache_db
        .query(
            "SELECT t.id, t.ctid, d.generation FROM test t \
            JOIN pgcache_generation_dump() d
            ON d.table_oid = 'test'::regclass \
                AND d.pk_hash = pgcache_pk_hash(d.table_oid, t.ctid) \
            WHERE t.id = 2 AND d.generation = 2",
            &[],
        )
        .await
        .map_err(Error::other)?;

    assert_eq!(
        entries_id2.len(),
        1,
        "Should have generation 2 entry for id=2"
    );

    Ok(())
}

/// Test that generation purge removes entries correctly
#[tokio::test]
async fn test_generation_purge() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Create test table
    ctx.query("create table test (id integer primary key, data text)", &[])
        .await?;

    ctx.query(
        "insert into test (id, data) values (1, 'foo'), (2, 'bar'), (3, 'baz')",
        &[],
    )
    .await?;

    // Wait for CDC to process the inserts before caching
    ctx.cdc_decode_settle().await?;

    // Create two cached queries
    let _res = ctx
        .simple_query("select id, data from test where id = 1")
        .await?;

    ctx.cache_settle().await?;

    let _res = ctx
        .simple_query("select id, data from test where id = 2")
        .await?;

    ctx.cache_settle().await?;

    // Execute both to record row access
    let _res = ctx
        .simple_query("select id, data from test where id = 1")
        .await?;
    let _res = ctx
        .simple_query("select id, data from test where id = 2")
        .await?;

    // Connect directly to cache db
    let cache_db = connect_cache_db(&ctx.dbs).await?;

    // Check entries before purge - row id=1 should have generation 1
    let entries_id1_before = cache_db
        .query(
            "SELECT t.id, t.ctid, d.generation FROM test t \
            JOIN pgcache_generation_dump() d
            ON d.table_oid = 'test'::regclass \
                AND d.pk_hash = pgcache_pk_hash(d.table_oid, t.ctid) \
            WHERE t.id = 1 AND d.generation = 1",
            &[],
        )
        .await
        .map_err(Error::other)?;

    assert_eq!(
        entries_id1_before.len(),
        1,
        "Should have generation 1 entry for id=1 before purge"
    );

    // Row id=2 should have generation 2
    let entries_id2_before = cache_db
        .query(
            "SELECT t.id, t.ctid, d.generation FROM test t \
            JOIN pgcache_generation_dump() d
            ON d.table_oid = 'test'::regclass \
                AND d.pk_hash = pgcache_pk_hash(d.table_oid, t.ctid) \
            WHERE t.id = 2 AND d.generation = 2",
            &[],
        )
        .await
        .map_err(Error::other)?;

    assert_eq!(
        entries_id2_before.len(),
        1,
        "Should have generation 2 entry for id=2 before purge"
    );

    // Purge generation 1 and below
    let _purged: i64 = cache_db
        .query_one("SELECT pgcache_generation_purge_all(1)", &[])
        .await
        .map_err(Error::other)?
        .get(0);

    // Check entries after purge - row id=1 should have no generation 1 entry
    let entries_id1_after = cache_db
        .query(
            "SELECT t.id, t.ctid, d.generation FROM test t \
            JOIN pgcache_generation_dump() d
            ON d.table_oid = 'test'::regclass \
                AND d.pk_hash = pgcache_pk_hash(d.table_oid, t.ctid) \
            WHERE t.id = 1 AND d.generation = 1",
            &[],
        )
        .await
        .map_err(Error::other)?;

    assert!(
        entries_id1_after.is_empty(),
        "Generation 1 entry for id=1 should be purged"
    );

    // Row id=2 should still have generation 2
    let entries_id2_after = cache_db
        .query(
            "SELECT t.id, t.ctid, d.generation FROM test t \
            JOIN pgcache_generation_dump() d
            ON d.table_oid = 'test'::regclass \
                AND d.pk_hash = pgcache_pk_hash(d.table_oid, t.ctid) \
            WHERE t.id = 2 AND d.generation = 2",
            &[],
        )
        .await
        .map_err(Error::other)?;

    assert_eq!(
        entries_id2_after.len(),
        1,
        "Generation 2 entry for id=2 should remain after purge"
    );

    Ok(())
}

/// Test that generation-0 entries are promoted (not purged) by pgcache_generation_zero_promote.
///
/// Gen-0 entries represent rows tracked before a generation was assigned. The promote
/// function moves them to a real generation so they survive the current purge cycle
/// and become purgeable in future cycles.
#[tokio::test]
async fn test_generation_zero_promote() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Create test table and populate
    ctx.query("create table test (id integer primary key, data text)", &[])
        .await?;

    ctx.query(
        "insert into test (id, data) values (1, 'foo'), (2, 'bar'), (3, 'baz')",
        &[],
    )
    .await?;

    ctx.cdc_decode_settle().await?;

    // Cache a query so the table exists in the cache DB (generation 1)
    let _res = ctx
        .simple_query("select id, data from test where id = 1")
        .await?;

    ctx.cache_settle().await?;

    // Execute cached query to record row access at generation 1
    let _res = ctx
        .simple_query("select id, data from test where id = 1")
        .await?;

    // Connect directly to cache DB
    let cache_db = connect_cache_db(&ctx.dbs).await?;

    // Verify normal gen-1 entry exists for id=1
    let gen1_entries = cache_db
        .query(
            "SELECT d.generation FROM pgcache_generation_dump() d \
            WHERE d.generation = 1",
            &[],
        )
        .await
        .map_err(Error::other)?;

    assert!(
        !gen1_entries.is_empty(),
        "Should have generation 1 entries from cache hit"
    );

    // Inject gen-0 entries directly — simulates rows tracked before a generation was assigned
    let table_oid: u32 = cache_db
        .query_one("SELECT 'test'::regclass::oid", &[])
        .await
        .map_err(Error::other)?
        .get(0);

    cache_db
        .execute(
            "SELECT pgcache_generation_record($1, 9001, 0)",
            &[&table_oid],
        )
        .await
        .map_err(Error::other)?;

    cache_db
        .execute(
            "SELECT pgcache_generation_record($1, 9002, 0)",
            &[&table_oid],
        )
        .await
        .map_err(Error::other)?;

    // Verify gen-0 entries exist
    let gen0_before = cache_db
        .query(
            "SELECT d.pk_hash FROM pgcache_generation_dump() d WHERE d.generation = 0",
            &[],
        )
        .await
        .map_err(Error::other)?;

    assert_eq!(gen0_before.len(), 2, "Should have 2 gen-0 entries");

    // Promote gen-0 entries to generation 5
    let promoted: i64 = cache_db
        .query_one("SELECT pgcache_generation_zero_promote(5)", &[])
        .await
        .map_err(Error::other)?
        .get(0);

    assert_eq!(promoted, 2, "Should have promoted exactly 2 entries");

    // Gen-0 entries should be gone
    let gen0_after = cache_db
        .query(
            "SELECT d.pk_hash FROM pgcache_generation_dump() d WHERE d.generation = 0",
            &[],
        )
        .await
        .map_err(Error::other)?;

    assert!(gen0_after.is_empty(), "No gen-0 entries should remain");

    // Entries should now be at generation 5
    let gen5 = cache_db
        .query(
            "SELECT d.pk_hash FROM pgcache_generation_dump() d WHERE d.generation = 5",
            &[],
        )
        .await
        .map_err(Error::other)?;

    assert_eq!(gen5.len(), 2, "Both entries should now be at generation 5");

    // Purge at threshold 4 — promoted entries at gen 5 should survive
    let purged_low: i64 = cache_db
        .query_one("SELECT pgcache_generation_purge_all(4)", &[])
        .await
        .map_err(Error::other)?
        .get(0);

    // Gen-1 entries should be purged, but gen-5 entries survive
    assert!(purged_low >= 0, "Purge should succeed");

    let gen5_after_low_purge = cache_db
        .query(
            "SELECT d.pk_hash FROM pgcache_generation_dump() d WHERE d.generation = 5",
            &[],
        )
        .await
        .map_err(Error::other)?;

    assert_eq!(
        gen5_after_low_purge.len(),
        2,
        "Gen-5 entries should survive purge at threshold 4"
    );

    // Purge at threshold 5 — promoted entries should now be removed
    let purged_high: i64 = cache_db
        .query_one("SELECT pgcache_generation_purge_all(5)", &[])
        .await
        .map_err(Error::other)?
        .get(0);

    assert_eq!(purged_high, 2, "Should purge the 2 promoted entries");

    let gen5_after_high_purge = cache_db
        .query(
            "SELECT d.pk_hash FROM pgcache_generation_dump() d WHERE d.generation = 5",
            &[],
        )
        .await
        .map_err(Error::other)?;

    assert!(
        gen5_after_high_purge.is_empty(),
        "Gen-5 entries should be purged at threshold 5"
    );

    Ok(())
}

/// Test that generation-0 promote is a no-op when no gen-0 entries exist
#[tokio::test]
async fn test_generation_zero_promote_noop() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query("create table test (id integer primary key, data text)", &[])
        .await?;

    ctx.query("insert into test (id, data) values (1, 'foo')", &[])
        .await?;

    ctx.cdc_decode_settle().await?;

    // Cache and hit a query to create gen-1 entries
    let _res = ctx
        .simple_query("select id, data from test where id = 1")
        .await?;

    ctx.cache_settle().await?;

    let _res = ctx
        .simple_query("select id, data from test where id = 1")
        .await?;

    let cache_db = connect_cache_db(&ctx.dbs).await?;

    // No gen-0 entries exist — promote should return 0
    let promoted: i64 = cache_db
        .query_one("SELECT pgcache_generation_zero_promote(10)", &[])
        .await
        .map_err(Error::other)?
        .get(0);

    assert_eq!(promoted, 0, "No entries to promote when none are gen-0");

    // Existing gen-1 entries should be unaffected
    let gen1 = cache_db
        .query(
            "SELECT d.pk_hash FROM pgcache_generation_dump() d WHERE d.generation = 1",
            &[],
        )
        .await
        .map_err(Error::other)?;

    assert!(
        !gen1.is_empty(),
        "Existing gen-1 entries should be unaffected by promote"
    );

    Ok(())
}

/// Test that generation tracking works with JOIN queries
#[tokio::test]
async fn test_generation_tracking_join() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Create test tables
    ctx.query("create table test (id integer primary key, data text)", &[])
        .await?;

    ctx.query(
        "create table test_map (id serial primary key, test_id integer, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test (id, data) values (1, 'foo'), (2, 'bar')",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_map (test_id, data) values (1, 'alpha'), (1, 'beta'), (2, 'gamma')",
        &[],
    )
    .await?;

    // Wait for CDC to process the inserts before caching
    ctx.cdc_decode_settle().await?;

    // Cached JOIN query
    let _res = ctx
        .simple_query(
            "select t.id, tm.data from test t join test_map tm on tm.test_id = t.id where t.id = 1",
        )
        .await?;

    ctx.cache_settle().await?;

    // Execute to record row access
    let _res = ctx
        .simple_query(
            "select t.id, tm.data from test t join test_map tm on tm.test_id = t.id where t.id = 1",
        )
        .await?;

    // Connect directly to cache db
    let cache_db = connect_cache_db(&ctx.dbs).await?;

    // Check generation entry for test table row id=1
    let entries_test = cache_db
        .query(
            "SELECT t.id, t.ctid, d.generation FROM test t \
            JOIN pgcache_generation_dump() d
            ON d.table_oid = 'test'::regclass \
                AND d.pk_hash = pgcache_pk_hash(d.table_oid, t.ctid) \
            WHERE t.id = 1 AND d.generation = 1",
            &[],
        )
        .await
        .map_err(Error::other)?;

    assert_eq!(
        entries_test.len(),
        1,
        "Should have generation 1 entry for test.id=1"
    );

    // Check generation entries for test_map table rows with test_id=1
    let entries_test_map = cache_db
        .query(
            "SELECT tm.id, tm.ctid, d.generation FROM test_map tm \
            JOIN pgcache_generation_dump() d
            ON d.table_oid = 'test_map'::regclass \
                AND d.pk_hash = pgcache_pk_hash(d.table_oid, tm.ctid) \
            WHERE tm.test_id = 1 AND d.generation = 1",
            &[],
        )
        .await
        .map_err(Error::other)?;

    // Should have entries for both test_map rows with test_id=1 (alpha, beta)
    assert_eq!(
        entries_test_map.len(),
        2,
        "Should have generation 1 entries for both test_map rows with test_id=1"
    );

    Ok(())
}
