use std::io::Error;

use crate::util::{TestContext, assert_cache_hit, assert_cache_miss, assert_row_at};

mod util;

// TODO: Add mechanism to verify if cache was invalidated or not
// Currently tests verify correctness (results are accurate) but not optimization
// (whether invalidation occurred). Would be useful to expose invalidation metrics
// or events for testing the optimization behavior directly.

/// Test that INSERT with matching constraints properly caches the row
#[tokio::test]
async fn test_insert_matching_constraint() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table test_match (id integer primary key, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "create table test_map_match (id serial primary key, test_id integer, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_match (id, data) values (1, 'foo'), (2, 'bar'), (3, 'baz')",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_map_match (test_id, data) values (1, 'alpha'), (1, 'beta'), (2, 'gamma')",
        &[],
    )
    .await?;

    // Prime the cache with a query that has constraint test_map_match.test_id = 1
    // Base data has: (1, 1, 'alpha'), (2, 1, 'beta')
    let _ = ctx
        .simple_query(
            "select tm.id, tm.test_id, tm.data \
            from test_match t join test_map_match tm on tm.test_id = t.id where t.id = 1 \
            order by tm.id",
        )
        .await?;

    // Insert row that matches constraint (test_id = 1)
    ctx.origin_query(
        "insert into test_map_match (test_id, data) values (1, 'delta')",
        &[],
    )
    .await?;

    ctx.cdc_settle().await?;

    // Query should show all 3 rows with test_id = 1
    let res = ctx
        .simple_query(
            "select tm.id, tm.test_id, tm.data \
            from test_match t join test_map_match tm on tm.test_id = t.id where t.id = 1 \
            order by tm.id",
        )
        .await?;

    // RowDescription + 3 data rows + CommandComplete = 5
    assert_eq!(res.len(), 5);
    assert_row_at(&res, 1, &[("id", "1"), ("test_id", "1"), ("data", "alpha")])?;
    assert_row_at(&res, 2, &[("id", "2"), ("test_id", "1"), ("data", "beta")])?;
    assert_row_at(&res, 3, &[("id", "4"), ("test_id", "1"), ("data", "delta")])?;

    Ok(())
}

/// Test that INSERT with non-matching constraints is optimized (no invalidation)
#[tokio::test]
async fn test_insert_non_matching_constraint() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table test_nonmatch (id integer primary key, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "create table test_map_nonmatch (id serial primary key, test_id integer, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_nonmatch (id, data) values (1, 'foo'), (2, 'bar'), (3, 'baz')",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_map_nonmatch (test_id, data) values (1, 'alpha'), (1, 'beta'), (2, 'gamma')",
        &[],
    )
    .await?;

    // Prime the cache with a query that has constraint test_map_nonmatch.test_id = 1
    let _ = ctx
        .simple_query(
            "select tm.id, tm.test_id, tm.data \
            from test_nonmatch t join test_map_nonmatch tm on tm.test_id = t.id where t.id = 1 \
            order by tm.id",
        )
        .await?;

    // Insert row that does NOT match constraint (test_id = 5, not 1)
    // This should be optimized - no invalidation because row won't appear in results
    ctx.origin_query(
        "insert into test_map_nonmatch (test_id, data) values (5, 'no_match')",
        &[],
    )
    .await?;

    ctx.cdc_settle().await?;

    // Query should still return only the original 2 rows (cache not invalidated, new row doesn't match)
    let res = ctx
        .simple_query(
            "select tm.id, tm.test_id, tm.data \
            from test_nonmatch t join test_map_nonmatch tm on tm.test_id = t.id where t.id = 1 \
            order by tm.id",
        )
        .await?;

    // RowDescription + 2 data rows + CommandComplete = 4
    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("id", "1"), ("test_id", "1"), ("data", "alpha")])?;
    assert_row_at(&res, 2, &[("id", "2"), ("test_id", "1"), ("data", "beta")])?;

    Ok(())
}

/// Test UPDATE where JOIN column changes from non-matching to matching value
/// This should invalidate because row is entering the result set
#[tokio::test]
async fn test_update_entering_result_set() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table test_enter (id integer primary key, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "create table test_map_enter (id serial primary key, test_id integer, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_enter (id, data) values (1, 'foo'), (2, 'bar'), (3, 'baz')",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_map_enter (test_id, data) values (1, 'alpha'), (1, 'beta'), (2, 'gamma')",
        &[],
    )
    .await?;

    // Prime the cache - query has constraint test_map_enter.test_id = 1
    // Base data: gamma has test_id = 2, so only alpha and beta match
    let _ = ctx
        .simple_query(
            "select tm.id, tm.test_id, tm.data \
            from test_enter t join test_map_enter tm on tm.test_id = t.id where t.id = 1 \
            order by tm.id",
        )
        .await?;

    // UPDATE: Change gamma's test_id from 2 to 1 (entering result set)
    ctx.origin_query(
        "update test_map_enter set test_id = 1 where data = 'gamma'",
        &[],
    )
    .await?;

    ctx.cdc_settle().await?;

    // Query should now show 3 rows including gamma
    let res = ctx
        .simple_query(
            "select tm.id, tm.test_id, tm.data \
            from test_enter t join test_map_enter tm on tm.test_id = t.id where t.id = 1 \
            order by tm.id",
        )
        .await?;

    // RowDescription + 3 data rows + CommandComplete = 5
    assert_eq!(res.len(), 5);
    assert_row_at(&res, 1, &[("id", "1"), ("test_id", "1"), ("data", "alpha")])?;
    assert_row_at(&res, 2, &[("id", "2"), ("test_id", "1"), ("data", "beta")])?;
    assert_row_at(&res, 3, &[("id", "3"), ("test_id", "1"), ("data", "gamma")])?;

    Ok(())
}

/// Test UPDATE where JOIN column changes from matching to non-matching value
/// This should NOT invalidate because row is leaving the result set (UPDATE handles removal)
#[tokio::test]
async fn test_update_leaving_result_set() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table test_leave (id integer primary key, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "create table test_map_leave (id serial primary key, test_id integer, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_leave (id, data) values (1, 'foo'), (2, 'bar'), (3, 'baz')",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_map_leave (test_id, data) values (1, 'alpha'), (1, 'beta'), (2, 'gamma')",
        &[],
    )
    .await?;

    // Prime the cache - query has constraint test_map_leave.test_id = 1
    let _ = ctx
        .simple_query(
            "select tm.id, tm.test_id, tm.data \
            from test_leave t join test_map_leave tm on tm.test_id = t.id where t.id = 1 \
            order by tm.id",
        )
        .await?;

    // UPDATE: Change alpha's test_id from 1 to 5 (leaving result set)
    // Optimization: no invalidation, UPDATE mechanism removes row from cache
    ctx.origin_query(
        "update test_map_leave set test_id = 5 where data = 'alpha'",
        &[],
    )
    .await?;

    ctx.cdc_settle().await?;

    // Query should now return 1 row (only beta)
    let res = ctx
        .simple_query(
            "select tm.id, tm.test_id, tm.data \
            from test_leave t join test_map_leave tm on tm.test_id = t.id where t.id = 1 \
            order by tm.id",
        )
        .await?;

    // RowDescription + 1 data row + CommandComplete = 3
    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "2"), ("test_id", "1"), ("data", "beta")])?;

    Ok(())
}

/// Test UPDATE where non-JOIN column changes (data field)
/// This should NOT invalidate because JOIN key is unchanged
#[tokio::test]
async fn test_update_non_join_column() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table test_nonjoin (id integer primary key, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "create table test_map_nonjoin (id serial primary key, test_id integer, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_nonjoin (id, data) values (1, 'foo'), (2, 'bar'), (3, 'baz')",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_map_nonjoin (test_id, data) values (1, 'alpha'), (1, 'beta'), (2, 'gamma')",
        &[],
    )
    .await?;

    // Prime the cache
    let _ = ctx
        .simple_query(
            "select tm.id, tm.test_id, tm.data \
            from test_nonjoin t join test_map_nonjoin tm on tm.test_id = t.id where t.id = 1 \
            order by tm.id",
        )
        .await?;

    // UPDATE: Change data field only (not JOIN column)
    // This should NOT invalidate
    ctx.origin_query(
        "update test_map_nonjoin set data = 'alpha_updated' where data = 'alpha'",
        &[],
    )
    .await?;

    ctx.cdc_settle().await?;

    // Query should show updated data
    let res = ctx
        .simple_query(
            "select tm.id, tm.test_id, tm.data \
            from test_nonjoin t join test_map_nonjoin tm on tm.test_id = t.id where t.id = 1 \
            order by tm.id",
        )
        .await?;

    // RowDescription + 2 data rows + CommandComplete = 4
    assert_eq!(res.len(), 4);
    assert_row_at(
        &res,
        1,
        &[("id", "1"), ("test_id", "1"), ("data", "alpha_updated")],
    )?;
    assert_row_at(&res, 2, &[("id", "2"), ("test_id", "1"), ("data", "beta")])?;

    Ok(())
}

/// Test UPDATE where a WHERE column (not join column) changes to match the constraint.
/// This should invalidate because a row is entering the result set via WHERE clause.
///
/// Scenario: SELECT * FROM orders JOIN users ON orders.user_id = users.id WHERE users.status = 'active'
/// If users.status changes from 'inactive' to 'active', the user's orders should now appear.
#[tokio::test]
async fn test_update_where_column_entering_result_set() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table users_where (id integer primary key, status text)",
        &[],
    )
    .await?;

    ctx.query(
        "create table orders_where (id serial primary key, user_id integer, amount integer)",
        &[],
    )
    .await?;

    // Insert users: user 1 is active, user 2 is inactive
    ctx.query(
        "insert into users_where (id, status) values (1, 'active'), (2, 'inactive')",
        &[],
    )
    .await?;

    // Insert orders for both users
    ctx.query(
        "insert into orders_where (user_id, amount) values (1, 100), (1, 200), (2, 300), (2, 400)",
        &[],
    )
    .await?;

    // Drain setup INSERT CDC events before priming so they don't race the
    // query registration that follows.
    ctx.cdc_settle().await?;

    // Prime the cache with join query filtering by users.status = 'active'
    // Should only return orders for user 1 (amounts 100, 200)
    let res = ctx
        .simple_query(
            "select o.id, o.user_id, o.amount, u.status \
            from orders_where o join users_where u on o.user_id = u.id \
            where u.status = 'active' \
            order by o.id",
        )
        .await?;

    // RowDescription + 2 data rows + CommandComplete = 4
    assert_eq!(res.len(), 4);
    assert_row_at(
        &res,
        1,
        &[("user_id", "1"), ("amount", "100"), ("status", "active")],
    )?;
    assert_row_at(
        &res,
        2,
        &[("user_id", "1"), ("amount", "200"), ("status", "active")],
    )?;

    // Wait for the query to be registered + populated before the UPDATE,
    // otherwise the UPDATE's CDC event can arrive at the writer before the
    // query exists to invalidate.
    ctx.cache_settle().await?;

    // UPDATE: Change user 2's status from 'inactive' to 'active'
    // This should invalidate the cache because user 2's orders should now appear
    ctx.origin_query("update users_where set status = 'active' where id = 2", &[])
        .await?;

    ctx.cdc_settle().await?;

    // Query should now return 4 orders (both users are active)
    let res = ctx
        .simple_query(
            "select o.id, o.user_id, o.amount, u.status \
            from orders_where o join users_where u on o.user_id = u.id \
            where u.status = 'active' \
            order by o.id",
        )
        .await?;

    // RowDescription + 4 data rows + CommandComplete = 6
    assert_eq!(res.len(), 6);
    assert_row_at(
        &res,
        1,
        &[("user_id", "1"), ("amount", "100"), ("status", "active")],
    )?;
    assert_row_at(
        &res,
        2,
        &[("user_id", "1"), ("amount", "200"), ("status", "active")],
    )?;
    assert_row_at(
        &res,
        3,
        &[("user_id", "2"), ("amount", "300"), ("status", "active")],
    )?;
    assert_row_at(
        &res,
        4,
        &[("user_id", "2"), ("amount", "400"), ("status", "active")],
    )?;

    Ok(())
}

/// Test UPDATE where a WHERE column (not join column) changes causing row to LEAVE result set.
/// This tests the row_changes.is_some() branch - the row IS in the cache initially.
///
/// Scenario: SELECT * FROM orders JOIN users ON orders.user_id = users.id WHERE users.status = 'active'
/// If users.status changes from 'active' to 'inactive', the user's orders should no longer appear.
#[tokio::test]
async fn test_update_where_column_leaving_result_set() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table users_leave (id integer primary key, status text)",
        &[],
    )
    .await?;

    ctx.query(
        "create table orders_leave (id serial primary key, user_id integer, amount integer)",
        &[],
    )
    .await?;

    // Insert users: both are active initially
    ctx.query(
        "insert into users_leave (id, status) values (1, 'active'), (2, 'active')",
        &[],
    )
    .await?;

    // Insert orders for both users
    ctx.query(
        "insert into orders_leave (user_id, amount) values (1, 100), (1, 200), (2, 300), (2, 400)",
        &[],
    )
    .await?;

    // Wait for setup CDC events to be processed before caching —
    // INSERT events on cached tables would trigger invalidation
    ctx.cdc_settle().await?;

    let query_str = "select o.id, o.user_id, o.amount, u.status \
            from orders_leave o join users_leave u on o.user_id = u.id \
            where u.status = 'active' \
            order by o.id";

    // Prime the cache with join query filtering by users.status = 'active'
    // Should return all 4 orders (both users are active)
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query_str).await?;

    // RowDescription + 4 data rows + CommandComplete = 6
    assert_eq!(res.len(), 6);
    assert_row_at(
        &res,
        1,
        &[("user_id", "1"), ("amount", "100"), ("status", "active")],
    )?;
    assert_row_at(
        &res,
        2,
        &[("user_id", "1"), ("amount", "200"), ("status", "active")],
    )?;
    assert_row_at(
        &res,
        3,
        &[("user_id", "2"), ("amount", "300"), ("status", "active")],
    )?;
    assert_row_at(
        &res,
        4,
        &[("user_id", "2"), ("amount", "400"), ("status", "active")],
    )?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    // Wait for cache to be populated
    ctx.cache_settle().await?;

    // Run same query again to verify cache hit
    let _ = ctx.simple_query(query_str).await?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    // UPDATE: Change user 1's status from 'active' to 'inactive'
    // User 1's row IS in the cache, so row_changes will be Some
    // This should invalidate the cache because user 1's orders should no longer appear
    ctx.origin_query(
        "update users_leave set status = 'inactive' where id = 1",
        &[],
    )
    .await?;

    ctx.cdc_settle().await?;

    // Query should now return only 2 orders (only user 2 is active)
    let res = ctx.simple_query(query_str).await?;

    // RowDescription + 2 data rows + CommandComplete = 4
    assert_eq!(res.len(), 4);
    assert_row_at(
        &res,
        1,
        &[("user_id", "2"), ("amount", "300"), ("status", "active")],
    )?;
    assert_row_at(
        &res,
        2,
        &[("user_id", "2"), ("amount", "400"), ("status", "active")],
    )?;

    Ok(())
}

/// Test UPDATE of non-PK column on an unconstrained table where the row is NOT in cache.
/// This tests the key_data optimization in row_changes.is_none() branch.
///
/// Scenario: SELECT * FROM film_actor fa JOIN actor a ON fa.actor_id = a.actor_id WHERE fa.film_id = 19
/// - film_actor has WHERE constraint (film_id = 19)
/// - actor has NO WHERE constraints, joined on actor_id (which is the PK)
/// - Update a non-PK column on an actor NOT in the cached result (not in film 19)
/// - Since: row not in cache, no WHERE constraints on actor, join col = PK, PK unchanged
/// - Optimization: skip invalidation, cache should still hit
#[tokio::test]
async fn test_update_non_pk_column_unconstrained_table_not_in_cache() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Create actor table with actor_id as PK
    ctx.query(
        "create table actor_opt (actor_id integer primary key, first_name text, last_name text)",
        &[],
    )
    .await?;

    // Create film_actor as a mapping table with serial PK (simpler than composite)
    ctx.query(
        "create table film_actor_opt (id serial primary key, film_id integer, actor_id integer)",
        &[],
    )
    .await?;

    // Insert actors
    ctx.query(
        "insert into actor_opt (actor_id, first_name, last_name) \
        values (1, 'John', 'Doe'), (2, 'Jane', 'Smith'), (3, 'Bob', 'Wilson')",
        &[],
    )
    .await?;

    // Film 19 has actors 1, 2; Film 20 has actor 3
    ctx.query(
        "insert into film_actor_opt (film_id, actor_id) \
        values (19, 1), (19, 2), (20, 3)",
        &[],
    )
    .await?;

    // Wait for initial data to settle
    ctx.cdc_settle().await?;

    let query_str = "select fa.film_id, a.actor_id, a.first_name, a.last_name from film_actor_opt fa join actor_opt a on fa.actor_id = a.actor_id where fa.film_id = 19 order by a.actor_id";

    // Prime cache: query for film 19 (actors 1, 2)
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query_str).await?;

    // RowDescription + 2 data rows + CommandComplete = 4
    assert_eq!(res.len(), 4);
    assert_row_at(
        &res,
        1,
        &[("film_id", "19"), ("actor_id", "1"), ("first_name", "John")],
    )?;
    assert_row_at(
        &res,
        2,
        &[("film_id", "19"), ("actor_id", "2"), ("first_name", "Jane")],
    )?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    // Wait for cache to load (use longer wait for reliability)
    ctx.cache_settle().await?;
    ctx.cache_settle().await?;

    // Verify cache hit
    let _ = ctx.simple_query(query_str).await?;
    let m = assert_cache_hit(&mut ctx, m).await?;

    // UPDATE: Change actor 3's last_name (actor NOT in cached result - not in film 19)
    // Optimization should kick in:
    // - row_changes.is_none() (actor 3 not in cache)
    // - actor table has no WHERE constraints
    // - join column (actor_id) is the PK
    // - key_data is empty (PK didn't change)
    // => Skip invalidation
    ctx.origin_query(
        "update actor_opt set last_name = 'Updated' where actor_id = 3",
        &[],
    )
    .await?;

    ctx.cdc_settle().await?;

    // Query again - should still be a cache hit (not invalidated)
    let res = ctx.simple_query(query_str).await?;

    // Verify still 2 rows (correct result)
    assert_eq!(res.len(), 4);
    assert_row_at(
        &res,
        1,
        &[("film_id", "19"), ("actor_id", "1"), ("first_name", "John")],
    )?;
    assert_row_at(
        &res,
        2,
        &[("film_id", "19"), ("actor_id", "2"), ("first_name", "Jane")],
    )?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test UPDATE of inclusive subquery of non-PK column on an unconstrained table where
/// the row is NOT in cache.
#[tokio::test]
async fn test_update_inclusive_subquery_non_pk_column_unconstrained_table_not_in_cache()
-> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Create actor table with actor_id as PK
    ctx.query(
        "create table actor (id integer primary key, first_name text, last_name text)",
        &[],
    )
    .await?;

    // Create film_actor as a mapping table with serial PK
    ctx.query(
        "create table film_actor (id serial primary key, film_id integer, actor_id integer)",
        &[],
    )
    .await?;

    ctx.query(
        "create table film (film_id integer primary key, title text)",
        &[],
    )
    .await?;

    // Insert actors
    ctx.query(
        "insert into actor (id, first_name, last_name) \
        values (1, 'John', 'Doe'), (2, 'Jane', 'Smith'), (3, 'Bob', 'Wilson')",
        &[],
    )
    .await?;

    // Film 19 has actors 1, 2; Film 20 has actor 3
    ctx.query(
        "insert into film_actor (film_id, actor_id) \
        values (19, 1), (19, 2), (20, 3)",
        &[],
    )
    .await?;

    // Film titles
    ctx.query(
        "insert into film (film_id, title) \
        values (19, 'nineteen'), (20, 'twenty')",
        &[],
    )
    .await?;

    // Wait for initial data to settle
    ctx.cdc_settle().await?;

    let query_str = "select film_id, title from film where film_id in ( \
                select fa.film_id \
                from film_actor fa join actor a on fa.actor_id = a.id \
                where a.id in (1, 3) \
            ) \
            order by film_id";

    // Prime cache: query for film titles
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query_str).await?;

    // RowDescription + 2 data rows + CommandComplete = 4
    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("film_id", "19"), ("title", "nineteen")])?;
    assert_row_at(&res, 2, &[("film_id", "20"), ("title", "twenty")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    // Wait for cache to load (use longer wait for reliability)
    ctx.cache_settle().await?;
    ctx.cache_settle().await?;

    // Verify cache hit
    let _ = ctx.simple_query(query_str).await?;
    let m = assert_cache_hit(&mut ctx, m).await?;

    // UPDATE: Change actor 2's last_name
    // Optimization should kick in:
    // - row_changes.is_none() (actor 2 not in cache)
    // - row is filtered out be constraints on actor
    // => Skip invalidation
    ctx.origin_query("update actor set last_name = 'Updated' where id = 2", &[])
        .await?;

    ctx.cdc_settle().await?;

    // Query again - should still be a cache hit (not invalidated)
    let res = ctx.simple_query(query_str).await?;

    // Verify still 2 rows (correct result)
    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("film_id", "19"), ("title", "nineteen")])?;
    assert_row_at(&res, 2, &[("film_id", "20"), ("title", "twenty")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// PGC-227 regression: an UPDATE to an uncached row that newly satisfies an
/// inner join must invalidate, even when the join condition is an *expression*
/// equi-join (`a.id = b.parent_id + 1`) whose equivalence the constraint
/// analyzer does not record as a column-to-column join.
///
/// The PGC-227 `change_dependent` optimization derives "this query can never
/// have its UPDATE invalidation depend on the changed row" from
/// `table_join_columns`, which only sees recorded `col = col` equivalences.
/// An expression join records none, so `change_dependent` is false and
/// `handle_update` skips the invalidation check. But the join is real: the
/// updated `b` row newly matches an `a` row that is NOT in the cache (it was
/// never part of the result), so the in-place upsert cannot add it — the only
/// correct response is invalidation. Skipping it serves a stale, under-
/// populated result.
#[tokio::test]
async fn test_expression_join_uncached_update_invalidates() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table exprjoin_a (id integer primary key, label text)",
        &[],
    )
    .await?;
    ctx.query(
        "create table exprjoin_b (id integer primary key, parent_id integer, note text)",
        &[],
    )
    .await?;

    // a row 8 ('eight') is intentionally NOT joined by any b row initially, so
    // it never enters the cache.
    ctx.query(
        "insert into exprjoin_a (id, label) values (5, 'five'), (8, 'eight')",
        &[],
    )
    .await?;
    // b 100: parent_id 4 -> 4+1 = 5 -> matches a 5 (cached).
    // b 200: parent_id 99 -> 99+1 = 100 -> matches no a (uncached).
    ctx.query(
        "insert into exprjoin_b (id, parent_id, note) \
        values (100, 4, 'first'), (200, 99, 'second')",
        &[],
    )
    .await?;

    ctx.cdc_settle().await?;

    let query_str = "select a.label, b.note \
        from exprjoin_a a join exprjoin_b b on a.id = b.parent_id + 1 \
        order by b.id";

    // Prime the cache (only the a5/b100 pair matches initially).
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query_str).await?;
    assert_eq!(res.len(), 3, "expected 1 data row initially");
    assert_row_at(&res, 1, &[("label", "five"), ("note", "first")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;
    ctx.cache_settle().await?;

    // Confirm the expression join is actually cached (else the test proves
    // nothing — a non-cacheable query always forwards to origin).
    let _ = ctx.simple_query(query_str).await?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    // UPDATE b 200 so 7+1 = 8 now matches a 8 — an a row that is NOT cached.
    // The row was uncached before the update, so the in-place upsert cannot
    // materialize the new (a8, b200) pair; correctness requires invalidation.
    ctx.origin_query("update exprjoin_b set parent_id = 7 where id = 200", &[])
        .await?;

    ctx.cdc_settle().await?;

    // Correct result is now both pairs: (a5, b100) and (a8, b200).
    let res = ctx.simple_query(query_str).await?;
    assert_eq!(res.len(), 4, "expected 2 data rows after the update");
    assert_row_at(&res, 1, &[("label", "five"), ("note", "first")])?;
    assert_row_at(&res, 2, &[("label", "eight"), ("note", "second")])?;

    Ok(())
}

/// PGC-227 regression (CROSS JOIN variant): an UPDATE that turns an empty
/// cached result non-empty must invalidate. A cross-joined table has no
/// recorded join columns, so `change_dependent` is false and `handle_update`
/// skips invalidation. The in-place upsert cannot rescue it here: the prior
/// result was EMPTY (no `c` row matched the constraint), so the partner table
/// `a` has nothing in cache for the new row to cross with.
#[tokio::test]
async fn test_cross_join_uncached_update_invalidates() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table crossj_a (id integer primary key, label text)",
        &[],
    )
    .await?;
    ctx.query(
        "create table crossj_c (id integer primary key, val integer, note text)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into crossj_a (id, label) values (1, 'A'), (2, 'B')",
        &[],
    )
    .await?;
    // No c row matches val = 7 initially -> the cached result is empty.
    ctx.query(
        "insert into crossj_c (id, val, note) values (10, 3, 'ten')",
        &[],
    )
    .await?;

    ctx.cdc_settle().await?;

    let query_str = "select a.label, c.note \
        from crossj_a a cross join crossj_c c where c.val = 7 \
        order by a.id";

    // Prime the cache with an empty result.
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query_str).await?;
    assert_eq!(res.len(), 2, "expected 0 data rows initially");
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;
    ctx.cache_settle().await?;

    let _ = ctx.simple_query(query_str).await?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    // UPDATE c 10 so val = 7 now matches -> the result becomes (A, ten), (B, ten).
    ctx.origin_query("update crossj_c set val = 7 where id = 10", &[])
        .await?;

    ctx.cdc_settle().await?;

    let res = ctx.simple_query(query_str).await?;
    assert_eq!(res.len(), 4, "expected 2 data rows after the update");
    assert_row_at(&res, 1, &[("label", "A"), ("note", "ten")])?;
    assert_row_at(&res, 2, &[("label", "B"), ("note", "ten")])?;

    Ok(())
}
