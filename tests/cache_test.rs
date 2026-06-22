use std::io::Error;

use crate::util::{
    TestContext, assert_cache_hit, assert_cache_miss, assert_row_at, cache_settle_at,
    connect_cache_db, connect_pgcache_tls, metrics_http_get, start_databases,
};

mod util;

/// Test basic caching with simple queries
#[tokio::test]
async fn test_cache_simple() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table test_simple (id integer primary key, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_simple (id, data) values (1, 'foo'), (2, 'bar')",
        &[],
    )
    .await?;

    // First query — cache miss
    let m = ctx.metrics().await?;
    let res = ctx
        .simple_query("select id, data from test_simple where data = 'foo'")
        .await?;

    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("data", "foo")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Second query — cache hit
    let res = ctx
        .simple_query("select id, data from test_simple where data = 'foo'")
        .await?;

    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("data", "foo")])?;
    let m = assert_cache_hit(&mut ctx, m).await?;

    // CDC INSERT on Direct table adds rows to cache in place → no invalidation
    ctx.origin_query(
        "insert into test_simple (id, data) values (3, 'foo'), (4, 'bar')",
        &[],
    )
    .await?;

    ctx.cdc_settle().await?;

    // Third query — cache hit (Direct + INSERT, row added in place)
    let res = ctx
        .simple_query("select id, data from test_simple where data = 'foo'")
        .await?;

    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("id", "1"), ("data", "foo")])?;
    assert_row_at(&res, 2, &[("id", "3"), ("data", "foo")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test caching with join queries
#[tokio::test]
async fn test_cache_join() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table test_join (id integer primary key, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "create table test_map_join (id serial primary key, test_id integer, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_join (id, data) values (1, 'foo'), (2, 'bar')",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_map_join (test_id, data) values \
        (1, 'foo'), \
        (1, 'bar'), \
        (1, 'baz'), \
        (2, 'foo'), \
        (2, 'bar'), \
        (2, 'baz')",
        &[],
    )
    .await?;

    ctx.cdc_settle().await?;

    let query_str = "select t.id, t.data as test_data, tm.test_id, tm.data as map_data \
        from test_join t join test_map_join tm on tm.test_id = t.id where t.id = 1 \
        order by tm.id;";

    // First query — cache miss, populates cache
    let m = ctx.metrics().await?;
    let _ = ctx.simple_query(query_str).await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Second query — cache hit
    let res = ctx.simple_query(query_str).await?;

    assert_eq!(res.len(), 5);
    assert_row_at(
        &res,
        1,
        &[
            ("id", "1"),
            ("test_data", "foo"),
            ("test_id", "1"),
            ("map_data", "foo"),
        ],
    )?;
    assert_row_at(
        &res,
        2,
        &[
            ("id", "1"),
            ("test_data", "foo"),
            ("test_id", "1"),
            ("map_data", "bar"),
        ],
    )?;
    assert_row_at(
        &res,
        3,
        &[
            ("id", "1"),
            ("test_data", "foo"),
            ("test_id", "1"),
            ("map_data", "baz"),
        ],
    )?;
    let m = assert_cache_hit(&mut ctx, m).await?;

    // Trigger CDC events by modifying the test table
    ctx.origin_query("update test_join set id = 10 where id = 1", &[])
        .await?;

    ctx.origin_query("update test_join set id = 1 where id = 10", &[])
        .await?;

    ctx.cdc_settle().await?;

    // Query after CDC — cache miss (UPDATE invalidates)
    let res = ctx.simple_query(query_str).await?;
    assert_eq!(res.len(), 5);
    assert_row_at(
        &res,
        1,
        &[
            ("id", "1"),
            ("test_data", "foo"),
            ("test_id", "1"),
            ("map_data", "foo"),
        ],
    )?;
    assert_row_at(
        &res,
        2,
        &[
            ("id", "1"),
            ("test_data", "foo"),
            ("test_id", "1"),
            ("map_data", "bar"),
        ],
    )?;
    assert_row_at(
        &res,
        3,
        &[
            ("id", "1"),
            ("test_data", "foo"),
            ("test_id", "1"),
            ("map_data", "baz"),
        ],
    )?;
    let _m = assert_cache_miss(&mut ctx, m).await?;

    Ok(())
}

/// Test caching with self-join queries where the same table appears multiple
/// times with different aliases. Exercises a bug where population would always
/// use the first alias for a table, producing wrong data for subsequent instances.
#[tokio::test]
async fn test_cache_self_join() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table test_self (id integer primary key, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "create table test_map_self (id serial primary key, test_id integer, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_self (id, data) values (1, 'foo'), (2, 'bar'), (3, 'bar')",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_map_self (test_id, data) values (1, 'foo'), (2, 'foo')",
        &[],
    )
    .await?;

    ctx.cdc_settle().await?;

    // Self-join: test_self appears as both t0 and t1, joined on data.
    // t0 is filtered via test_map_self, t1 fans out on matching data.
    // Before the fix, population used alias t0 for both instances of test_self,
    // so the cache table for t1 would be populated with t0's rows instead.
    let query_str = "\
        select t0.id as t0_id, t0.data as t0_data, \
        tm.id as tm_id, tm.test_id, tm.data as tm_data, \
        t1.id as t1_id, t1.data as t1_data \
        from test_self t0 \
        inner join test_map_self tm on tm.test_id = t0.id \
        inner join test_self t1 on t1.data = t0.data \
        where tm.data = 'foo' \
        order by t0.id, t1.id";

    // Expected results:
    //   t0(1,'foo') + tm(1,1,'foo') + t1(1,'foo')  -- only t1 with data='foo' is id=1
    //   t0(2,'bar') + tm(2,2,'foo') + t1(2,'bar')  -- t1 with data='bar': id=2
    //   t0(2,'bar') + tm(2,2,'foo') + t1(3,'bar')  -- t1 with data='bar': id=3

    // First query — cache miss
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query_str).await?;
    assert_eq!(res.len(), 5); // 3 rows + RowDescription + CommandComplete
    assert_row_at(
        &res,
        1,
        &[
            ("t0_id", "1"),
            ("t0_data", "foo"),
            ("t1_id", "1"),
            ("t1_data", "foo"),
        ],
    )?;
    assert_row_at(
        &res,
        2,
        &[
            ("t0_id", "2"),
            ("t0_data", "bar"),
            ("t1_id", "2"),
            ("t1_data", "bar"),
        ],
    )?;
    assert_row_at(
        &res,
        3,
        &[
            ("t0_id", "2"),
            ("t0_data", "bar"),
            ("t1_id", "3"),
            ("t1_data", "bar"),
        ],
    )?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Second query — cache hit with identical data
    let res = ctx.simple_query(query_str).await?;
    assert_eq!(res.len(), 5);
    assert_row_at(
        &res,
        1,
        &[
            ("t0_id", "1"),
            ("t0_data", "foo"),
            ("t1_id", "1"),
            ("t1_data", "foo"),
        ],
    )?;
    assert_row_at(
        &res,
        2,
        &[
            ("t0_id", "2"),
            ("t0_data", "bar"),
            ("t1_id", "2"),
            ("t1_data", "bar"),
        ],
    )?;
    assert_row_at(
        &res,
        3,
        &[
            ("t0_id", "2"),
            ("t0_data", "bar"),
            ("t1_id", "3"),
            ("t1_data", "bar"),
        ],
    )?;
    let m = assert_cache_hit(&mut ctx, m).await?;

    // CDC: insert a new row that joins into the self-join via data='bar'
    ctx.origin_query("insert into test_self (id, data) values (4, 'bar')", &[])
        .await?;

    ctx.cdc_settle().await?;

    // Query after CDC — cache miss, now t1 has an additional match for data='bar'
    let res = ctx.simple_query(query_str).await?;
    assert_eq!(res.len(), 6); // 4 rows now
    assert_row_at(&res, 1, &[("t0_id", "1"), ("t1_id", "1")])?;
    assert_row_at(&res, 2, &[("t0_id", "2"), ("t1_id", "2")])?;
    assert_row_at(&res, 3, &[("t0_id", "2"), ("t1_id", "3")])?;
    assert_row_at(
        &res,
        4,
        &[("t0_id", "2"), ("t1_id", "4"), ("t1_data", "bar")],
    )?;
    let _m = assert_cache_miss(&mut ctx, m).await?;

    Ok(())
}

/// Test that indexes from the origin table are created on the cache table
#[tokio::test]
async fn test_cache_index_creation() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Create table with multiple indexes on origin
    ctx.query(
        "CREATE TABLE test_indexed (
            id INTEGER PRIMARY KEY,
            email TEXT,
            name TEXT,
            created_at TIMESTAMP
        )",
        &[],
    )
    .await?;

    // Create various index types
    ctx.query("CREATE INDEX idx_email ON test_indexed (email)", &[])
        .await?;
    ctx.query("CREATE UNIQUE INDEX idx_name ON test_indexed (name)", &[])
        .await?;
    ctx.query(
        "CREATE INDEX idx_composite ON test_indexed (email, created_at)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE INDEX idx_email_hash ON test_indexed USING hash (email)",
        &[],
    )
    .await?;

    // Insert some data
    ctx.query(
        "INSERT INTO test_indexed (id, email, name, created_at) VALUES
         (1, 'alice@example.com', 'Alice', '2024-01-01'),
         (2, 'bob@example.com', 'Bob', '2024-01-02')",
        &[],
    )
    .await?;

    // Execute a cacheable query to trigger cache table creation
    let _ = ctx
        .simple_query("SELECT * FROM test_indexed WHERE id = 1")
        .await?;

    ctx.cache_settle().await?;

    // Connect directly to the cache database and verify the replicated indexes
    // via their canonical definitions. Cache indexes are created anonymously, so
    // we assert on the definition shape (UNIQUE flag, access method, columns)
    // rather than the origin index names.
    let cache_db = connect_cache_db(&ctx.dbs).await?;
    let rows = cache_db
        .query(
            r"
            SELECT pg_get_indexdef(ix.indexrelid) AS definition
            FROM pg_index ix
            JOIN pg_class t ON t.oid = ix.indrelid
            WHERE t.relname = 'test_indexed' AND NOT ix.indisprimary
            ",
            &[],
        )
        .await
        .map_err(Error::other)?;

    let defs: Vec<String> = rows.iter().map(|r| r.get("definition")).collect();
    assert_eq!(
        defs.len(),
        4,
        "Expected 4 non-primary-key indexes, found: {defs:?}"
    );

    let has = |needle: &str| defs.iter().any(|d| d.contains(needle));

    assert!(
        defs.iter()
            .any(|d| d.contains("UNIQUE") && d.contains("USING btree (name)")),
        "Missing unique btree index on (name): {defs:?}"
    );
    assert!(
        has("USING btree (email)"),
        "Missing btree index on (email): {defs:?}"
    );
    assert!(
        has("USING btree (email, created_at)"),
        "Missing composite btree index on (email, created_at): {defs:?}"
    );
    assert!(
        has("USING hash (email)"),
        "Missing hash index on (email): {defs:?}"
    );

    Ok(())
}

/// Partial and `DESC` indexes must be replicated faithfully onto the cache
/// table, not silently dropped (PGC-332).
#[tokio::test]
async fn test_cache_partial_index_creation() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE posts (
            id INTEGER PRIMARY KEY,
            owneruserid INTEGER,
            lastactivitydate TIMESTAMP
        )",
        &[],
    )
    .await?;

    // Partial index (the case that PGC-332 was dropping) and a DESC index.
    ctx.query(
        "CREATE INDEX posts_owneruserid_idx ON posts (owneruserid) WHERE owneruserid IS NOT NULL",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE INDEX posts_lastactivitydate_idx ON posts (lastactivitydate DESC)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO posts (id, owneruserid, lastactivitydate) VALUES
         (1, 10, '2024-01-01'), (2, NULL, '2024-01-02')",
        &[],
    )
    .await?;

    // Trigger cache table creation.
    let _ = ctx.simple_query("SELECT * FROM posts WHERE id = 1").await?;
    ctx.cache_settle().await?;

    let cache_db = connect_cache_db(&ctx.dbs).await?;
    let rows = cache_db
        .query(
            r"
            SELECT pg_get_indexdef(ix.indexrelid) AS definition
            FROM pg_index ix
            JOIN pg_class t ON t.oid = ix.indrelid
            WHERE t.relname = 'posts' AND NOT ix.indisprimary
            ",
            &[],
        )
        .await
        .map_err(Error::other)?;

    let defs: Vec<String> = rows.iter().map(|r| r.get("definition")).collect();

    let partial = defs
        .iter()
        .find(|d| d.contains("(owneruserid)"))
        .expect("partial index on owneruserid present in cache");
    assert!(
        partial.to_lowercase().contains("where")
            && partial.to_lowercase().contains("owneruserid is not null"),
        "partial index lost its predicate: {partial}"
    );

    let desc = defs
        .iter()
        .find(|d| d.contains("lastactivitydate"))
        .expect("DESC index on lastactivitydate present in cache");
    assert!(
        desc.to_uppercase().contains("DESC"),
        "DESC index lost its ordering: {desc}"
    );

    Ok(())
}

/// Test that client TLS connections work correctly with caching
#[tokio::test]
async fn test_client_tls() -> Result<(), Error> {
    let (dbs, _origin) = start_databases().await?;

    // Connect to pgcache with TLS
    let (_pgcache, _port, metrics_port, client) = connect_pgcache_tls(&dbs).await?;

    // Create a table
    client
        .execute(
            "CREATE TABLE tls_test (id INTEGER PRIMARY KEY, data TEXT)",
            &[],
        )
        .await
        .map_err(Error::other)?;

    // Insert some data
    client
        .execute(
            "INSERT INTO tls_test (id, data) VALUES (1, 'encrypted')",
            &[],
        )
        .await
        .map_err(Error::other)?;

    // First query - should be a cache miss
    let rows = client
        .query("SELECT id, data FROM tls_test WHERE id = $1", &[&1i32])
        .await
        .map_err(Error::other)?;

    assert_eq!(rows.len(), 1);
    let id: i32 = rows[0].get("id");
    let data: &str = rows[0].get("data");
    assert_eq!(id, 1);
    assert_eq!(data, "encrypted");

    // Wait for cache to load
    cache_settle_at(metrics_port, std::time::Duration::from_secs(5)).await?;

    // Second query - should be a cache hit
    let rows = client
        .query("SELECT id, data FROM tls_test WHERE id = $1", &[&1i32])
        .await
        .map_err(Error::other)?;

    assert_eq!(rows.len(), 1);
    let id: i32 = rows[0].get("id");
    let data: &str = rows[0].get("data");
    assert_eq!(id, 1);
    assert_eq!(data, "encrypted");

    // Verify metrics show 1 cache hit
    let metrics = metrics_http_get(metrics_port).await?;
    assert_eq!(metrics.queries_cache_hit, 1, "Expected 1 cache hit");
    assert_eq!(metrics.queries_cache_miss, 1, "Expected 1 cache miss");

    Ok(())
}
