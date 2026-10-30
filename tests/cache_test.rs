use std::io::Error;

use crate::util::{
    TestContext, assert_cache_hit, assert_cache_miss, assert_proxy_rows_match_origin,
    assert_row_at, cache_settle_at, connect_cache_db, connect_pgcache_tls, metrics_after_outcomes,
    metrics_delta, metrics_http_get, query_warm, start_databases,
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

    ctx.cdc_apply_settle().await?;

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

    ctx.cdc_decode_settle().await?;

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

    ctx.cdc_apply_settle().await?;

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

    ctx.cdc_decode_settle().await?;

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

    ctx.cdc_apply_settle().await?;

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
    let before_hit = metrics_http_get(metrics_port).await?;
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
    let metrics = metrics_after_outcomes(metrics_port, &before_hit, 1).await?;
    assert_eq!(metrics.queries_cache_hit, 1, "Expected 1 cache hit");
    assert_eq!(metrics.queries_cache_miss, 1, "Expected 1 cache miss");

    Ok(())
}

/// A CDC change to a self-joined relation must not corrupt the cached result.
///
/// `emp e JOIN emp m ON m.id = e.manager_id WHERE e.dept = 'x'` — a row can
/// participate as the employee side, the manager side, or both. `payload` is
/// projected but is not a join or WHERE column, so updating it can neither grow
/// nor shrink the result: the cached rows must simply carry the new value.
///
/// Membership is decided by substituting the CDC row into the query. With a
/// self-join it must hold for *any* occurrence — the manager row (`ceo`) fails
/// the employee-side predicate `e.dept = 'x'` yet is still required as `m`.
/// Testing a single occurrence evicts it and empties the result (PGC-256).
#[tokio::test]
async fn test_cache_self_join_noninvalidating_update() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table emp (id int primary key, name text, manager_id int, dept text, payload text)",
        &[],
    )
    .await?;
    ctx.query(
        "insert into emp values (1,'ceo',null,'exec','P1'), (2,'alice',1,'x','P2'), \
         (3,'bob',1,'x','P3'), (4,'carol',2,'y','P4')",
        &[],
    )
    .await?;
    ctx.cdc_decode_settle().await?;

    let query_str = "\
        select e.id as e_id, e.payload as e_payload, m.id as m_id, m.payload as m_payload \
        from emp e join emp m on m.id = e.manager_id \
        where e.dept = 'x' order by e.id";

    // Populate: alice and bob both report to ceo.
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query_str).await?;
    assert_eq!(res.len(), 4); // 2 rows + RowDescription + CommandComplete
    let _m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    // `ceo` appears only on the manager side — it never satisfies `e.dept = 'x'`.
    ctx.query("update emp set payload = 'P1x' where id = 1", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    // `alice` appears only on the employee side (carol reports to her, but is
    // filtered out by dept = 'y').
    ctx.query("update emp set payload = 'P2x' where id = 2", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query_str).await?;
    assert_row_at(
        &res,
        1,
        &[
            ("e_id", "2"),
            ("e_payload", "P2x"),
            ("m_id", "1"),
            ("m_payload", "P1x"),
        ],
    )?;
    assert_row_at(
        &res,
        2,
        &[
            ("e_id", "3"),
            ("e_payload", "P3"),
            ("m_id", "1"),
            ("m_payload", "P1x"),
        ],
    )?;
    assert_eq!(res.len(), 4, "both rows must survive the updates");
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// A self-joined cached query must not be treated as a subsumption parent.
///
/// `subsumption_check` only admits *single-table* parents — but it tests
/// `relation_oids.len() == 1`, and a self-join has exactly one relation, so it
/// sails through. Its extracted constraints then claim more than it caches:
/// `emp e JOIN emp m … WHERE e.dept = 'x'` records `dept = 'x'` for `emp`, yet
/// only caches emp rows the *join* reaches. `dave` is dept 'x' with no manager,
/// so the join never sees him and he is never cached.
///
/// A later `WHERE dept = 'x'` query is therefore implied by the parent's claimed
/// constraints, and would be marked Ready and served from the parent's rows —
/// silently dropping `dave`. Self-joined queries must stay out of the
/// subsumption index (PGC-256).
#[tokio::test]
async fn test_cache_self_join_is_not_a_subsumption_parent() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table emp (id int primary key, name text, manager_id int, dept text)",
        &[],
    )
    .await?;
    ctx.query(
        "insert into emp values (1,'ceo',null,'exec'), (2,'alice',1,'x'), \
         (3,'bob',1,'x'), (5,'dave',null,'x')",
        &[],
    )
    .await?;
    ctx.cdc_decode_settle().await?;

    // The join reaches ceo/alice/bob. `dave` has no manager, so he is not cached.
    let self_join = "\
        select e.id as e_id, m.id as m_id from emp e \
        join emp m on m.id = e.manager_id where e.dept = 'x' order by e.id";
    let res = ctx.simple_query(self_join).await?;
    assert_eq!(res.len(), 4, "self-join returns alice and bob");
    ctx.cache_settle().await?;

    // Implied by the parent's *claimed* constraints (dept = 'x'), but not by what
    // it actually caches. If subsumed, `dave` silently disappears.
    let dept_x = "select id, name from emp where dept = 'x' order by id";
    let res = ctx.simple_query(dept_x).await?;
    assert_row_at(&res, 1, &[("id", "2"), ("name", "alice")])?;
    assert_row_at(&res, 2, &[("id", "3"), ("name", "bob")])?;
    assert_row_at(&res, 3, &[("id", "5"), ("name", "dave")])?;
    assert_eq!(res.len(), 5, "dave must not be subsumed away");
    ctx.cache_settle().await?;

    // Still correct once served from its own cache entry.
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(dept_x).await?;
    assert_row_at(&res, 3, &[("id", "5"), ("name", "dave")])?;
    assert_eq!(res.len(), 5);
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// A WHERE literal on one occurrence of a self-join does not filter rows
/// standing in for the other: an inserted manager outside the filter still
/// attaches to an employee the join had dropped.
#[tokio::test]
async fn test_cache_self_join_other_occurrence_cdc_insert() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table emp_ins (id int primary key, manager_id int, dept text)",
        &[],
    )
    .await?;
    ctx.query(
        "insert into emp_ins values (1, null, 'exec'), (2, 1, 'x'), (3, 5, 'x')",
        &[],
    )
    .await?;
    ctx.cdc_decode_settle().await?;

    let query = "\
        select e.id as e_id, m.id as m_id from emp_ins e \
        join emp_ins m on m.id = e.manager_id where e.dept = 'x' order by e.id";
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 3, "self-join returns employee 2");
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 3, "cached self-join returns employee 2");
    let _m = assert_cache_hit(&mut ctx, m).await?;

    ctx.query("insert into emp_ins values (5, null, 'exec')", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    let res = ctx.simple_query(query).await?;
    assert_row_at(&res, 2, &[("e_id", "3"), ("m_id", "5")])?;
    assert_eq!(res.len(), 4, "the new manager attaches employee 3");

    Ok(())
}

/// A WHERE literal on one occurrence of a self-join does not hold for the
/// other: an employee moved to an uncached manager meeting the literal must
/// invalidate rather than drop out of the result.
#[tokio::test]
async fn test_cache_self_join_cached_update_uncached_partner() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table emp_upd (id int primary key, manager_id int, dept text)",
        &[],
    )
    .await?;
    ctx.query(
        "insert into emp_upd values (1, null, 'exec'), (2, 1, 'x'), (4, null, 'exec')",
        &[],
    )
    .await?;
    ctx.cdc_decode_settle().await?;

    let query = "\
        select e.id as e_id, m.id as m_id from emp_upd e \
        join emp_upd m on m.id = e.manager_id where m.dept = 'exec' order by e.id";
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 3, "self-join returns employee 2");
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 3, "cached self-join returns employee 2");
    let _m = assert_cache_hit(&mut ctx, m).await?;

    ctx.query("update emp_upd set manager_id = 4 where id = 2", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    let res = ctx.simple_query(query).await?;
    assert_row_at(&res, 1, &[("e_id", "2"), ("m_id", "4")])?;
    assert_eq!(res.len(), 3, "employee 2 reports to manager 4");

    Ok(())
}

/// A table named both in FROM and in a subquery has a change relevant to
/// each occurrence: an inserted row meeting only the outer predicate must
/// reach the cached result.
#[tokio::test]
async fn test_cache_repeated_table_outer_occurrence_cdc_insert() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table emp_sub (id int primary key, name text, manager_id int, dept text)",
        &[],
    )
    .await?;
    ctx.query(
        "insert into emp_sub values (1,'ceo',null,'exec'), (2,'alice',1,'x')",
        &[],
    )
    .await?;
    ctx.cdc_decode_settle().await?;

    let query = "\
        select id, name from emp_sub where dept = 'x' \
        and manager_id in (select id from emp_sub where dept = 'exec') order by id";
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 3, "query returns alice");
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 3, "cached query returns alice");
    let _m = assert_cache_hit(&mut ctx, m).await?;

    ctx.query("insert into emp_sub values (3,'bob',1,'x')", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    let res = ctx.simple_query(query).await?;
    assert_row_at(&res, 2, &[("id", "3"), ("name", "bob")])?;
    assert_eq!(res.len(), 4, "bob reaches the result");

    Ok(())
}

/// Two tables of one name in different schemas are separate relations: a
/// literal on one must not filter CDC rows of the other.
#[tokio::test]
async fn test_cache_same_name_other_schema_cdc_insert() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query("create schema other_items", &[]).await?;
    ctx.query(
        "create table items_dup (id int primary key, kind text)",
        &[],
    )
    .await?;
    ctx.query(
        "create table other_items.items_dup (id int primary key, kind text)",
        &[],
    )
    .await?;
    ctx.query("insert into items_dup values (1, 'x'), (2, 'x')", &[])
        .await?;
    ctx.query("insert into other_items.items_dup values (1, 'y')", &[])
        .await?;
    ctx.cdc_decode_settle().await?;

    let query = "\
        select p.id as p_id, o.id as o_id from public.items_dup p \
        join other_items.items_dup o on o.id = p.id where p.kind = 'x' order by p.id";
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 3, "join returns id 1");
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 3, "cached join returns id 1");
    let _m = assert_cache_hit(&mut ctx, m).await?;

    ctx.query("insert into other_items.items_dup values (2, 'y')", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    let res = ctx.simple_query(query).await?;
    assert_row_at(&res, 2, &[("p_id", "2"), ("o_id", "2")])?;
    assert_eq!(res.len(), 4, "the other schema's row joins id 2");

    Ok(())
}

/// Each reference of a self-join filters on its own: a CDC row is matched
/// against the reference it can stand in for. A change that only one
/// reference's filter admits is applied, and one neither admits leaves the
/// cached result alone (PGC-517).
#[tokio::test]
async fn test_cache_self_join_filters_per_reference_cdc() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table sjf (id int primary key, kind text, partner int)",
        &[],
    )
    .await?;
    ctx.query(
        "insert into sjf values (1, 'x', 2), (2, 'y', null), (3, 'x', 4), (4, 'z', null)",
        &[],
    )
    .await?;
    ctx.cdc_decode_settle().await?;

    let query = "\
        select a.id as a_id, b.id as b_id from sjf a \
        join sjf b on b.id = a.partner where a.kind = 'x' and b.kind = 'y' order by a.id";
    let m = ctx.metrics().await?;
    ctx.simple_query(query).await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    // Matches neither reference's filter: the cached result stands.
    ctx.query("insert into sjf values (6, 'z', null)", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 3, "only (1, 2)");
    assert_row_at(&res, 1, &[("a_id", "1"), ("b_id", "2")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    // Row 4 now passes only `b`'s filter, which attaches row 3.
    ctx.query("update sjf set kind = 'y' where id = 4", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    let (proxied, origin) = ctx.proxy_and_origin_rows(query).await?;
    assert_eq!(origin.len(), 2, "(1, 2) and (3, 4)");
    assert_eq!(proxied, origin);

    // Row 2 leaves `b`'s filter, which detaches row 1.
    ctx.query("update sjf set kind = 'z' where id = 2", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    let (proxied, origin) = ctx.proxy_and_origin_rows(query).await?;
    assert_eq!(origin.len(), 1, "(3, 4)");
    assert_eq!(proxied, origin);

    Ok(())
}

/// A multi-row transaction into a self-joined relation is batch-evaluated per
/// reference: rows matching only the employee side or only the manager side
/// both stay cached, and each reference costs one evaluation, not one per row
/// (PGC-521).
#[tokio::test]
async fn test_self_join_rows_batch_per_reference() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(
        "CREATE TABLE emp (id INTEGER PRIMARY KEY, name TEXT, dept TEXT, manager_id INTEGER)",
        &[],
    )
    .await?;
    // Managers 1 and 5 sit outside dept x, so they match only the manager
    // reference; 2, 3, 4 and 6 match only the employee reference.
    ctx.query(
        "INSERT INTO emp (id, name, dept, manager_id) VALUES \
         (1, 'bob', 'y', NULL), (5, 'eve', 'y', NULL), \
         (2, 'ann', 'x', 1), (3, 'cat', 'x', 1), (4, 'dan', 'x', 1), (6, 'fay', 'x', 5)",
        &[],
    )
    .await?;
    ctx.cdc_decode_settle().await?;

    let query = "SELECT e.id, e.name, m.name AS manager FROM emp e \
                 JOIN emp m ON m.id = e.manager_id WHERE e.dept = 'x' ORDER BY e.id";
    query_warm(&mut ctx, query).await?;

    let before = ctx.metrics().await?;
    ctx.origin_query("UPDATE emp SET name = name || '2'", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    let after = ctx.metrics().await?;
    let evaluations = metrics_delta(&before, &after).cache_cdc_pg_eval_hits;
    assert!(
        (1..=2).contains(&evaluations),
        "six rows batch into one evaluation per reference, got {evaluations}"
    );

    assert_proxy_rows_match_origin(&mut ctx, query).await?;
    assert_cache_hit(&mut ctx, after).await?;
    Ok(())
}

/// A registration reuses the binding the Resolve command made for its
/// session key: the Register command binds nothing itself, for a query
/// that resolves on the search path as for one naming its schema.
#[tokio::test]
async fn test_register_reuses_resolve_binding() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(
        "create table bind_once (id integer primary key, data text)",
        &[],
    )
    .await?;
    ctx.query(
        "insert into bind_once (id, data) select g, 'row-' || g from generate_series(1, 20) g",
        &[],
    )
    .await?;

    for i in 1..=10 {
        ctx.query(
            &format!("select id, data from bind_once where id = {i}"),
            &[],
        )
        .await?;
    }
    for i in 11..=20 {
        ctx.query(
            &format!("select id, data from public.bind_once where id = {i}"),
            &[],
        )
        .await?;
    }
    ctx.cache_settle().await?;

    let m = ctx.metrics().await?;
    assert_eq!(
        m.queries_cache_miss, 20,
        "every distinct literal registered"
    );
    assert_eq!(
        m.cache_writer_register_binds, 0,
        "the Register command bound a query the Resolve command had bound"
    );
    Ok(())
}
