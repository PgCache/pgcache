//! LEFT and RIGHT JOIN caching tests.
//!
//! pgcache stores per-table data, not materialized join results. JOINs are
//! re-executed at retrieval time against cached per-table data. These tests
//! verify that outer joins work correctly for caching, cache hits, and CDC
//! invalidation.
//!
//! Terminal vs Non-terminal:
//! - Terminal optional-side tables: columns don't appear in WHERE or other
//!   join conditions. CDC uses row-level updates (same as INNER JOIN).
//! - Non-terminal optional-side tables: columns appear in WHERE or downstream
//!   joins. CDC triggers full query invalidation (conservative).

use std::io::Error;

use crate::util::{TestContext, assert_cache_hit, assert_cache_miss, assert_row_at};

mod util;

// =============================================================================
// Terminal LEFT JOIN
// =============================================================================

/// Terminal LEFT JOIN: basic caching and cache hit.
///
/// The optional-side table (details) is terminal — its columns only appear in
/// its own ON clause and the SELECT list. CDC on the optional side uses
/// row-level updates.
#[tokio::test]
async fn test_left_join_terminal_cache_hit() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE lj_orders (id INTEGER PRIMARY KEY, customer TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE lj_details (id INTEGER PRIMARY KEY, order_id INTEGER, item TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO lj_orders (id, customer) VALUES (1, 'Alice'), (2, 'Bob')",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO lj_details (id, order_id, item) VALUES \
         (10, 1, 'Widget'), (11, 1, 'Gizmo'), (12, 2, 'Thing')",
        &[],
    )
    .await?;

    ctx.cdc_decode_settle().await?;

    let query = "SELECT o.id, o.customer, d.item \
                 FROM lj_orders o LEFT JOIN lj_details d ON o.id = d.order_id \
                 WHERE o.id = 1 ORDER BY d.id";

    // First query — cache miss
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4); // RowDescription + 2 rows + CommandComplete
    assert_row_at(
        &res,
        1,
        &[("id", "1"), ("customer", "Alice"), ("item", "Widget")],
    )?;
    assert_row_at(
        &res,
        2,
        &[("id", "1"), ("customer", "Alice"), ("item", "Gizmo")],
    )?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Second query — cache hit
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4);
    assert_row_at(
        &res,
        1,
        &[("id", "1"), ("customer", "Alice"), ("item", "Widget")],
    )?;
    assert_row_at(
        &res,
        2,
        &[("id", "1"), ("customer", "Alice"), ("item", "Gizmo")],
    )?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Terminal LEFT JOIN: CDC INSERT on optional side adds rows in place.
///
/// When a new row is inserted into the terminal optional-side table and it
/// matches the cached query, the row is added to the cache table. The LEFT
/// JOIN at retrieval picks it up — no invalidation needed.
#[tokio::test]
async fn test_left_join_terminal_cdc_insert_optional_side() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE ljins_orders (id INTEGER PRIMARY KEY, customer TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE ljins_details (id INTEGER PRIMARY KEY, order_id INTEGER, item TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO ljins_orders (id, customer) VALUES (1, 'Alice')",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO ljins_details (id, order_id, item) VALUES (10, 1, 'Widget')",
        &[],
    )
    .await?;

    ctx.cdc_decode_settle().await?;

    let query = "SELECT o.id, o.customer, d.item \
                 FROM ljins_orders o LEFT JOIN ljins_details d ON o.id = d.order_id \
                 WHERE o.id = 1 ORDER BY d.id";

    // Cache miss, populates cache
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 3); // 1 row
    assert_row_at(
        &res,
        1,
        &[("id", "1"), ("customer", "Alice"), ("item", "Widget")],
    )?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // INSERT on optional side — should be added to cache in place
    ctx.origin_query(
        "INSERT INTO ljins_details (id, order_id, item) VALUES (11, 1, 'Gizmo')",
        &[],
    )
    .await?;

    ctx.cdc_apply_settle().await?;

    // Cache hit — new row appears via LEFT JOIN at retrieval
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4); // 2 rows now
    assert_row_at(
        &res,
        1,
        &[("id", "1"), ("customer", "Alice"), ("item", "Widget")],
    )?;
    assert_row_at(
        &res,
        2,
        &[("id", "1"), ("customer", "Alice"), ("item", "Gizmo")],
    )?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Terminal LEFT JOIN: NULL padding when no optional-side match.
///
/// When the optional side has no matching row, the LEFT JOIN produces NULLs.
/// After inserting a matching row on the optional side, the NULLs are replaced.
#[tokio::test]
async fn test_left_join_null_padding() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE ljnull_orders (id INTEGER PRIMARY KEY, customer TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE ljnull_details (id INTEGER PRIMARY KEY, order_id INTEGER, item TEXT)",
        &[],
    )
    .await?;

    // Order 1 has no details initially
    ctx.query(
        "INSERT INTO ljnull_orders (id, customer) VALUES (1, 'Alice')",
        &[],
    )
    .await?;

    ctx.cdc_decode_settle().await?;

    let query = "SELECT o.id, o.customer, d.item \
                 FROM ljnull_orders o LEFT JOIN ljnull_details d ON o.id = d.order_id \
                 WHERE o.id = 1 ORDER BY d.id";

    // Cache miss — no details, so item is NULL
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 3); // 1 row (NULL-padded)
    assert_row_at(&res, 1, &[("id", "1"), ("customer", "Alice")])?;
    // item should be NULL (get returns None)
    let row = util::extract_row(&res, 1)?;
    assert_eq!(row.get::<&str>("item"), None, "item should be NULL");
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // INSERT a matching detail on the optional side
    ctx.origin_query(
        "INSERT INTO ljnull_details (id, order_id, item) VALUES (10, 1, 'Widget')",
        &[],
    )
    .await?;

    ctx.cdc_apply_settle().await?;

    // Cache hit — NULLs replaced by the new row
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 3); // 1 row (now with data)
    assert_row_at(
        &res,
        1,
        &[("id", "1"), ("customer", "Alice"), ("item", "Widget")],
    )?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Terminal LEFT JOIN: CDC DELETE on optional side removes rows in place.
#[tokio::test]
async fn test_left_join_terminal_cdc_delete_optional_side() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE ljdel_orders (id INTEGER PRIMARY KEY, customer TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE ljdel_details (id INTEGER PRIMARY KEY, order_id INTEGER, item TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO ljdel_orders (id, customer) VALUES (1, 'Alice')",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO ljdel_details (id, order_id, item) VALUES \
         (10, 1, 'Widget'), (11, 1, 'Gizmo')",
        &[],
    )
    .await?;

    ctx.cdc_decode_settle().await?;

    let query = "SELECT o.id, o.customer, d.item \
                 FROM ljdel_orders o LEFT JOIN ljdel_details d ON o.id = d.order_id \
                 WHERE o.id = 1 ORDER BY d.id";

    // Cache miss
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4); // 2 rows
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // DELETE one detail — row removed from cache in place
    ctx.origin_query("DELETE FROM ljdel_details WHERE id = 11", &[])
        .await?;

    ctx.cdc_apply_settle().await?;

    // Cache hit — only one detail remains
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 3); // 1 row
    assert_row_at(
        &res,
        1,
        &[("id", "1"), ("customer", "Alice"), ("item", "Widget")],
    )?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

// =============================================================================
// Terminal RIGHT JOIN
// =============================================================================

/// Terminal RIGHT JOIN: basic caching and cache hit.
///
/// RIGHT JOIN flips the preserved/optional sides: the right side is preserved,
/// the left side is optional. The same terminal CDC logic applies.
#[tokio::test]
async fn test_right_join_terminal_cache_hit() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE rj_details (id INTEGER PRIMARY KEY, order_id INTEGER, item TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE rj_orders (id INTEGER PRIMARY KEY, customer TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO rj_orders (id, customer) VALUES (1, 'Alice'), (2, 'Bob')",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO rj_details (id, order_id, item) VALUES \
         (10, 1, 'Widget'), (11, 1, 'Gizmo')",
        &[],
    )
    .await?;

    ctx.cdc_decode_settle().await?;

    // RIGHT JOIN: orders is preserved, details is optional
    let query = "SELECT d.item, o.id, o.customer \
                 FROM rj_details d RIGHT JOIN rj_orders o ON d.order_id = o.id \
                 WHERE o.id = 1 ORDER BY d.id";

    // Cache miss
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4); // 2 rows
    assert_row_at(
        &res,
        1,
        &[("item", "Widget"), ("id", "1"), ("customer", "Alice")],
    )?;
    assert_row_at(
        &res,
        2,
        &[("item", "Gizmo"), ("id", "1"), ("customer", "Alice")],
    )?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Cache hit
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4);
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

// =============================================================================
// Non-terminal LEFT JOIN (conservative CDC invalidation)
// =============================================================================

/// Non-terminal LEFT JOIN: optional-side column in WHERE clause.
///
/// When the optional-side table's columns appear in the WHERE clause, the table
/// is non-terminal. CDC events on the optional side trigger full query
/// invalidation (cache miss after CDC).
#[tokio::test]
async fn test_left_join_non_terminal_cdc_invalidation() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE ljnt_orders (id INTEGER PRIMARY KEY, customer TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE ljnt_details (id INTEGER PRIMARY KEY, order_id INTEGER, status TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO ljnt_orders (id, customer) VALUES (1, 'Alice'), (2, 'Bob')",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO ljnt_details (id, order_id, status) VALUES \
         (10, 1, 'shipped'), (11, 2, 'pending')",
        &[],
    )
    .await?;

    ctx.cdc_decode_settle().await?;

    // Non-terminal: d.status appears in WHERE (optional-side column in WHERE)
    let query = "SELECT o.id, o.customer, d.status \
                 FROM ljnt_orders o LEFT JOIN ljnt_details d ON o.id = d.order_id \
                 WHERE d.status = 'shipped' ORDER BY o.id";

    // Cache miss
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 3); // 1 row (only Alice's shipped order)
    assert_row_at(
        &res,
        1,
        &[("id", "1"), ("customer", "Alice"), ("status", "shipped")],
    )?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Cache hit
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 3);
    let m = assert_cache_hit(&mut ctx, m).await?;

    // UPDATE on non-terminal optional side — triggers full invalidation
    ctx.origin_query(
        "UPDATE ljnt_details SET status = 'shipped' WHERE id = 11",
        &[],
    )
    .await?;

    ctx.cdc_apply_settle().await?;

    // Cache miss — query was invalidated by non-terminal CDC
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4); // 2 rows now (both shipped)
    assert_row_at(
        &res,
        1,
        &[("id", "1"), ("customer", "Alice"), ("status", "shipped")],
    )?;
    assert_row_at(
        &res,
        2,
        &[("id", "2"), ("customer", "Bob"), ("status", "shipped")],
    )?;
    let _m = assert_cache_miss(&mut ctx, m).await?;

    Ok(())
}

// =============================================================================
// CDC on preserved side
// =============================================================================

/// LEFT JOIN: CDC INSERT on preserved side triggers invalidation.
///
/// The preserved side is Direct (not OuterJoinTerminal). INSERT on a Direct
/// multi-table query triggers invalidation because optional-side rows matching
/// the new preserved-side row may not be in cache. After invalidation, the
/// query is re-registered with the correct data on the next request.
#[tokio::test]
async fn test_left_join_cdc_insert_preserved_side() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE ljpres_orders (id INTEGER PRIMARY KEY, customer TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE ljpres_details (id INTEGER PRIMARY KEY, order_id INTEGER, item TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO ljpres_orders (id, customer) VALUES (1, 'Alice')",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO ljpres_details (id, order_id, item) VALUES (10, 1, 'Widget')",
        &[],
    )
    .await?;

    ctx.cdc_decode_settle().await?;

    // WHERE on preserved side only — details is terminal
    let query = "SELECT o.id, o.customer, d.item \
                 FROM ljpres_orders o LEFT JOIN ljpres_details d ON o.id = d.order_id \
                 WHERE o.customer = 'Alice' ORDER BY o.id, d.id";

    // Cache miss
    let m = ctx.metrics().await?;
    let _ = ctx.simple_query(query).await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Cache hit
    let _ = ctx.simple_query(query).await?;
    let m = assert_cache_hit(&mut ctx, m).await?;

    // INSERT on preserved side — triggers invalidation (Direct multi-table)
    ctx.origin_query(
        "INSERT INTO ljpres_orders (id, customer) VALUES (2, 'Alice')",
        &[],
    )
    .await?;

    ctx.cdc_apply_settle().await?;

    // Cache miss — query was invalidated, re-registers with updated data
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4); // 2 rows now
    assert_row_at(
        &res,
        1,
        &[("id", "1"), ("customer", "Alice"), ("item", "Widget")],
    )?;
    // Second row: order 2 has no details → NULL item
    let row2 = util::extract_row(&res, 2)?;
    assert_eq!(row2.get::<&str>("id"), Some("2"));
    assert_eq!(row2.get::<&str>("customer"), Some("Alice"));
    assert_eq!(
        row2.get::<&str>("item"),
        None,
        "new order should have NULL item"
    );
    let _m = assert_cache_miss(&mut ctx, m).await?;

    Ok(())
}
