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

use crate::util::{
    TestContext, assert_cache_hit, assert_cache_miss, assert_not_subsumed, assert_row_at,
    assert_subsume_hit, origin_change_applied_in_place, origin_change_invalidates, query_warm,
};

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

// =============================================================================
// Literal predicates in an outer join's ON clause
// =============================================================================

/// A literal on the preserved side in a LEFT JOIN's ON clause filters
/// nothing: every order is returned, matched or NULL-padded. A preserved-side
/// INSERT that fails the ON literal must still appear after CDC.
#[tokio::test]
async fn test_left_join_on_preserved_literal_cdc_insert() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE lonp_orders (id INTEGER PRIMARY KEY, status TEXT, customer_id INTEGER)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE TABLE lonp_customers (id INTEGER PRIMARY KEY, name TEXT)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO lonp_orders VALUES (1, 'x', 10), (2, 'y', 10)",
        &[],
    )
    .await?;
    ctx.query("INSERT INTO lonp_customers VALUES (10, 'Acme')", &[])
        .await?;
    ctx.cdc_decode_settle().await?;

    let query = "SELECT o.id, o.status, c.name \
                 FROM lonp_orders o LEFT JOIN lonp_customers c \
                 ON o.customer_id = c.id AND o.status = 'x' \
                 ORDER BY o.id";

    let m = ctx.metrics().await?;
    let (proxied, origin) = ctx.proxy_and_origin_rows(query).await?;
    assert_eq!(proxied, origin, "initial result");
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    let (proxied, origin) = ctx.proxy_and_origin_rows(query).await?;
    assert_eq!(proxied, origin, "cached result");
    let _m = assert_cache_hit(&mut ctx, m).await?;

    ctx.origin_query("INSERT INTO lonp_orders VALUES (3, 'y', 10)", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    let (proxied, origin) = ctx.proxy_and_origin_rows(query).await?;
    assert_eq!(origin.len(), 3, "origin returns every order");
    assert_eq!(proxied, origin, "result after preserved-side insert");

    Ok(())
}

/// A literal on the nullable side of a LEFT JOIN's ON clause, equated to a
/// preserved-side column, still filters no preserved rows. An order whose
/// customer_id differs from that literal must still appear after CDC.
#[tokio::test]
async fn test_left_join_on_nullable_literal_cdc_insert() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE lonn_orders (id INTEGER PRIMARY KEY, status TEXT, customer_id INTEGER)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE TABLE lonn_customers (id INTEGER PRIMARY KEY, name TEXT)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO lonn_orders VALUES (1, 'x', 10), (2, 'x', 20)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO lonn_customers VALUES (10, 'Acme'), (20, 'Globex')",
        &[],
    )
    .await?;
    ctx.cdc_decode_settle().await?;

    let query = "SELECT o.id, o.status, c.name \
                 FROM lonn_orders o LEFT JOIN lonn_customers c \
                 ON o.customer_id = c.id AND c.id = 10 \
                 ORDER BY o.id";

    let m = ctx.metrics().await?;
    let (proxied, origin) = ctx.proxy_and_origin_rows(query).await?;
    assert_eq!(proxied, origin, "initial result");
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    let (proxied, origin) = ctx.proxy_and_origin_rows(query).await?;
    assert_eq!(proxied, origin, "cached result");
    let _m = assert_cache_hit(&mut ctx, m).await?;

    ctx.origin_query("INSERT INTO lonn_orders VALUES (3, 'x', 20)", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    let (proxied, origin) = ctx.proxy_and_origin_rows(query).await?;
    assert_eq!(origin.len(), 3, "origin returns every order");
    assert_eq!(proxied, origin, "result after preserved-side insert");

    Ok(())
}

/// A LEFT JOIN with a preserved-side literal in its ON clause needs every
/// order, so a cached parent holding only the orders matching that literal
/// cannot cover it.
#[tokio::test]
async fn test_left_join_on_preserved_literal_not_subsumed() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE lons_orders (id INTEGER PRIMARY KEY, status TEXT, customer_id INTEGER)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE TABLE lons_customers (id INTEGER PRIMARY KEY, name TEXT)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO lons_orders VALUES (1, 'x', 10), (2, 'y', 10)",
        &[],
    )
    .await?;
    ctx.query("INSERT INTO lons_customers VALUES (10, 'Acme')", &[])
        .await?;
    ctx.cdc_decode_settle().await?;

    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT id, status, customer_id FROM lons_orders WHERE status = 'x'")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    ctx.simple_query("SELECT id, name FROM lons_customers")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    let query = "SELECT o.id, o.status, c.name \
                 FROM lons_orders o LEFT JOIN lons_customers c \
                 ON o.customer_id = c.id AND o.status = 'x' \
                 ORDER BY o.id";
    let (proxied, origin) = ctx.proxy_and_origin_rows(query).await?;
    assert_eq!(origin.len(), 2, "origin returns every order");
    assert_eq!(proxied, origin, "join result");
    let _m = assert_not_subsumed(&mut ctx, m).await?;

    Ok(())
}

/// A preserved-side column in a LEFT JOIN's ON clause decides which
/// nullable-side row attaches. Updating it can attach a customer the cache
/// never loaded, so the order must not be patched in place.
#[tokio::test]
async fn test_left_join_on_preserved_literal_cdc_update() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE lonu_orders (id INTEGER PRIMARY KEY, status TEXT, customer_id INTEGER)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE TABLE lonu_customers (id INTEGER PRIMARY KEY, name TEXT)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO lonu_orders VALUES (1, 'x', 10), (2, 'y', 20)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO lonu_customers VALUES (10, 'Acme'), (20, 'Globex')",
        &[],
    )
    .await?;
    ctx.cdc_decode_settle().await?;

    let query = "SELECT o.id, o.status, c.name \
                 FROM lonu_orders o LEFT JOIN lonu_customers c \
                 ON o.customer_id = c.id AND o.status = 'x' \
                 ORDER BY o.id";

    let m = ctx.metrics().await?;
    let (proxied, origin) = ctx.proxy_and_origin_rows(query).await?;
    assert_eq!(proxied, origin, "initial result");
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    let (proxied, origin) = ctx.proxy_and_origin_rows(query).await?;
    assert_eq!(proxied, origin, "cached result");
    let _m = assert_cache_hit(&mut ctx, m).await?;

    ctx.origin_query("UPDATE lonu_orders SET status = 'x' WHERE id = 2", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    let (proxied, origin) = ctx.proxy_and_origin_rows(query).await?;
    assert_eq!(
        origin.get(1).and_then(|row| row.get(2)).cloned().flatten(),
        Some("Globex".to_owned()),
        "origin attaches the newly matching customer"
    );
    assert_eq!(proxied, origin, "result after preserved-side update");

    Ok(())
}

/// A LEFT self-join's preserved occurrence is not its optional side: an
/// inserted node that attaches a tag the join had dropped must reach the
/// cached result.
#[tokio::test]
async fn test_left_self_join_preserved_occurrence_cdc_insert() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE lsj_nodes (id INTEGER PRIMARY KEY, parent_id INTEGER)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE TABLE lsj_tags (id INTEGER PRIMARY KEY, node_id INTEGER)",
        &[],
    )
    .await?;
    ctx.query("INSERT INTO lsj_nodes VALUES (1, NULL), (2, 1)", &[])
        .await?;
    ctx.query("INSERT INTO lsj_tags VALUES (10, 1), (30, 3)", &[])
        .await?;
    ctx.cdc_decode_settle().await?;

    let query = "SELECT a.id, b.id, g.id \
                 FROM lsj_nodes a LEFT JOIN lsj_nodes b ON b.parent_id = a.id \
                 JOIN lsj_tags g ON g.node_id = a.id \
                 ORDER BY a.id, b.id";

    let m = ctx.metrics().await?;
    let (proxied, origin) = ctx.proxy_and_origin_rows(query).await?;
    assert_eq!(proxied, origin, "initial result");
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    let (proxied, origin) = ctx.proxy_and_origin_rows(query).await?;
    assert_eq!(proxied, origin, "cached result");
    let _m = assert_cache_hit(&mut ctx, m).await?;

    ctx.origin_query("INSERT INTO lsj_nodes VALUES (3, NULL)", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    let (proxied, origin) = ctx.proxy_and_origin_rows(query).await?;
    assert_eq!(origin.len(), 2, "origin attaches tag 30 to node 3");
    assert_eq!(proxied, origin, "result after preserved-occurrence insert");

    Ok(())
}

/// A LEFT JOIN keeps every preserved-side row its WHERE admits, whatever the
/// ON clause says, so that table serves a single-table query, users without
/// an order included. The child then follows CDC on its own.
#[tokio::test]
async fn test_left_join_preserved_parent_subsumes() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE ljps_users (id INTEGER PRIMARY KEY, name TEXT)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE TABLE ljps_orders (id INTEGER PRIMARY KEY, user_id INTEGER, total INTEGER)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO ljps_users VALUES (1, 'a'), (2, 'a'), (3, 'b')",
        &[],
    )
    .await?;
    ctx.query("INSERT INTO ljps_orders VALUES (10, 1, 5)", &[])
        .await?;
    ctx.cdc_decode_settle().await?;

    let m = ctx.metrics().await?;
    ctx.simple_query(
        "SELECT u.id, u.name, o.total FROM ljps_users u LEFT JOIN ljps_orders o \
         ON o.user_id = u.id AND o.total = u.id + 4 WHERE u.name = 'a'",
    )
    .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    let child = "SELECT id, name FROM ljps_users WHERE name = 'a' ORDER BY id";
    let (proxied, origin) = ctx.proxy_and_origin_rows(child).await?;
    assert_eq!(origin.len(), 2, "origin returns both 'a' users");
    assert_eq!(proxied, origin, "subsumed child");
    let m = assert_subsume_hit(&mut ctx, m).await?;

    ctx.query("INSERT INTO ljps_users VALUES (4, 'a')", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    let (proxied, origin) = ctx.proxy_and_origin_rows(child).await?;
    assert_eq!(origin.len(), 3, "origin returns the inserted user");
    assert_eq!(proxied, origin, "child after CDC insert");
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// A join parent that didn't load a table in full can't serve it: the
/// nullable side holds only matched rows, an inner join drops unmatched
/// rows, and a WHERE on the nullable side drops preserved rows.
#[tokio::test]
async fn test_join_parent_incomplete_table_not_subsumed() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE ljpn_users (id INTEGER PRIMARY KEY, name TEXT)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE TABLE ljpn_orders (id INTEGER PRIMARY KEY, user_id INTEGER, total INTEGER)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO ljpn_users VALUES (1, 'a'), (2, 'a'), (3, 'b'), (4, 'b'), (5, 'c'), (6, 'c')",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO ljpn_orders VALUES (10, 1, 5), (11, 3, 5), (12, 5, 7), (13, 99, 1)",
        &[],
    )
    .await?;
    ctx.cdc_decode_settle().await?;

    let cases = [
        (
            "SELECT u.id, o.total FROM ljpn_users u LEFT JOIN ljpn_orders o \
             ON o.user_id = u.id WHERE u.name = 'a'",
            "SELECT id, user_id, total FROM ljpn_orders WHERE user_id = 99",
            1,
        ),
        (
            "SELECT u.id, o.total FROM ljpn_users u JOIN ljpn_orders o \
             ON o.user_id = u.id WHERE u.name = 'b'",
            "SELECT id, name FROM ljpn_users WHERE name = 'b' ORDER BY id",
            2,
        ),
        (
            "SELECT u.id, o.total FROM ljpn_users u LEFT JOIN ljpn_orders o \
             ON o.user_id = u.id WHERE u.name = 'c' AND o.total = 7",
            "SELECT id, name FROM ljpn_users WHERE name = 'c' ORDER BY id",
            2,
        ),
    ];
    for (parent, child, expected) in cases {
        let m = ctx.metrics().await?;
        ctx.simple_query(parent).await?;
        let m = assert_cache_miss(&mut ctx, m).await?;
        ctx.cache_settle().await?;

        let (proxied, origin) = ctx.proxy_and_origin_rows(child).await?;
        assert_eq!(origin.len(), expected, "{child}");
        assert_eq!(proxied, origin, "{child}");
        let _m = assert_not_subsumed(&mut ctx, m).await?;
    }

    Ok(())
}

/// A preserved reference sharing its table with a terminal one is not
/// terminal itself: an UPDATE to one of its uncached rows can meet a partner
/// that was never loaded, so it must invalidate (PGC-518).
#[tokio::test]
async fn test_preserved_reference_named_like_terminal_uncached_update() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE pnt_t (id INTEGER PRIMARY KEY, c_id INTEGER, x INTEGER)",
        &[],
    )
    .await?;
    ctx.query("CREATE TABLE pnt_c (id INTEGER PRIMARY KEY)", &[])
        .await?;
    ctx.query("INSERT INTO pnt_c VALUES (1), (5)", &[]).await?;
    ctx.query("INSERT INTO pnt_t VALUES (1, 1, NULL), (2, 9, NULL)", &[])
        .await?;
    ctx.cdc_decode_settle().await?;

    let query = "SELECT a.id, b.id FROM pnt_t a JOIN pnt_c c ON c.id = a.c_id \
                 LEFT JOIN pnt_t b ON b.id = a.x ORDER BY a.id";
    let m = ctx.metrics().await?;
    ctx.simple_query(query).await?;
    let _m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    ctx.query("UPDATE pnt_t SET c_id = 5 WHERE id = 2", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    let (proxied, origin) = ctx.proxy_and_origin_rows(query).await?;
    assert_eq!(origin.len(), 2, "row 2 now meets c 5");
    assert_eq!(proxied, origin);

    Ok(())
}

/// Each reference of a LEFT self-join has its own role: WHERE reads only the
/// preserved one, so the NULL-filled one is terminal and an INSERT that only
/// it can take is applied in place (PGC-518).
#[tokio::test]
async fn test_left_self_join_cdc_insert_applied_in_place() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE lsi_t (id INTEGER PRIMARY KEY, x INTEGER, y INTEGER)",
        &[],
    )
    .await?;
    ctx.query("INSERT INTO lsi_t VALUES (1, 10, 1), (2, 20, 1)", &[])
        .await?;
    ctx.cdc_decode_settle().await?;

    let query = "SELECT a.id, b.id FROM lsi_t a LEFT JOIN lsi_t b ON b.id = a.x \
                 WHERE a.y = 1 ORDER BY a.id";
    let m = ctx.metrics().await?;
    ctx.simple_query(query).await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    // Row 10 fails `a.y = 1`, so it can only stand in for `b`.
    ctx.query("INSERT INTO lsi_t VALUES (10, NULL, 0)", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    let (proxied, origin) = ctx.proxy_and_origin_rows(query).await?;
    assert_eq!(origin.len(), 2, "row 1 attaches row 10");
    assert_eq!(proxied, origin);
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// A reference sharing the NULL-filled side of an outer join with another is
/// no leaf: an INSERT there can attach a partner that was never loaded, so it
/// must not be applied as terminal.
#[tokio::test]
async fn test_nested_null_side_partner_cdc_insert() -> Result<(), Error> {
    for (label, inner_join) in [("left", "LEFT JOIN"), ("inner", "JOIN")] {
        let mut ctx = TestContext::setup().await?;
        ctx.query("CREATE TABLE nn_a (id INTEGER PRIMARY KEY)", &[])
            .await?;
        ctx.query(
            "CREATE TABLE nn_b (id INTEGER PRIMARY KEY, aid INTEGER)",
            &[],
        )
        .await?;
        ctx.query(
            "CREATE TABLE nn_c (id INTEGER PRIMARY KEY, bid INTEGER)",
            &[],
        )
        .await?;
        ctx.query("INSERT INTO nn_a VALUES (1)", &[]).await?;
        ctx.query("INSERT INTO nn_c VALUES (7, 6)", &[]).await?;
        ctx.cdc_decode_settle().await?;

        let query = format!(
            "SELECT a.id, b.id, c.id FROM nn_a a \
             LEFT JOIN (nn_b b {inner_join} nn_c c ON c.bid = b.id) ON b.aid = a.id \
             ORDER BY a.id"
        );
        let m = ctx.metrics().await?;
        ctx.simple_query(&query).await?;
        let _m = assert_cache_miss(&mut ctx, m).await?;
        ctx.cache_settle().await?;

        ctx.query("INSERT INTO nn_b VALUES (6, 1)", &[]).await?;
        ctx.cdc_apply_settle().await?;

        let (proxied, origin) = ctx.proxy_and_origin_rows(&query).await?;
        assert_eq!(origin.len(), 1, "{label}: one row");
        assert_eq!(proxied, origin, "{label}: b 6 attaches c 7");
    }
    Ok(())
}

// =============================================================================
// Terminal and optional link columns (PGC-520)
// =============================================================================

/// `authors` 1 and 2 (NZ) and 3 (AU); `bios` 100 (author 1) and 101
/// (author 2).
async fn setup_authors_bios(ctx: &mut TestContext) -> Result<(), Error> {
    ctx.query(
        "CREATE TABLE authors (id INTEGER PRIMARY KEY, country TEXT)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE TABLE bios (id INTEGER PRIMARY KEY, author_id INTEGER, note TEXT)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO authors (id, country) VALUES (1, 'NZ'), (2, 'NZ'), (3, 'AU')",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO bios (id, author_id, note) VALUES (100, 1, 'first'), (101, 2, 'second')",
        &[],
    )
    .await?;
    ctx.cdc_decode_settle().await
}

/// A terminal side's join column moves its row to another preserved row in
/// place: every preserved row is in the result, so it's cached (PGC-520).
#[tokio::test]
async fn test_terminal_join_column_origin_change_applied_in_place() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_authors_bios(&mut ctx).await?;

    let query = "SELECT a.id, b.id AS bio FROM authors a \
                 LEFT JOIN bios b ON b.author_id = a.id ORDER BY a.id";
    let m = query_warm(&mut ctx, query).await?;
    origin_change_applied_in_place(
        &mut ctx,
        m,
        "UPDATE bios SET author_id = 3 WHERE id = 101",
        query,
    )
    .await?;
    Ok(())
}

/// The same through an expression ON, which the model can't read as an
/// equality (PGC-520).
#[tokio::test]
async fn test_terminal_expression_join_origin_change_applied_in_place() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(
        "CREATE TABLE people (id INTEGER PRIMARY KEY, email TEXT)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE TABLE profiles (id INTEGER PRIMARY KEY, email TEXT)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO people (id, email) VALUES (1, 'ann@x'), (2, 'bob@x'), (3, 'cy@x')",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO profiles (id, email) VALUES (100, 'ANN@x')",
        &[],
    )
    .await?;
    ctx.cdc_decode_settle().await?;

    let query = "SELECT p.id, f.id AS profile FROM people p \
                 LEFT JOIN profiles f ON lower(f.email) = lower(p.email) ORDER BY p.id";
    let m = query_warm(&mut ctx, query).await?;
    origin_change_applied_in_place(
        &mut ctx,
        m,
        "UPDATE profiles SET email = 'CY@x' WHERE id = 100",
        query,
    )
    .await?;
    Ok(())
}

/// A terminal row moving to a preserved row the WHERE excludes needs nothing
/// uncached: that row is out of the result whatever the terminal side does
/// (PGC-520).
#[tokio::test]
async fn test_terminal_update_to_filtered_preserved_row_applied_in_place() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_authors_bios(&mut ctx).await?;

    let query = "SELECT a.id, b.id AS bio FROM authors a \
                 LEFT JOIN bios b ON b.author_id = a.id WHERE a.country = 'NZ' ORDER BY a.id";
    let m = query_warm(&mut ctx, query).await?;
    origin_change_applied_in_place(
        &mut ctx,
        m,
        "UPDATE bios SET author_id = 3 WHERE id = 101",
        query,
    )
    .await?;
    Ok(())
}

/// A terminal side whose preserved partner is inner-joined, so not exact,
/// still re-attaches in place: the preserved rows in the result are cached,
/// and one the inner join drops adds no row (PGC-520).
#[tokio::test]
async fn test_terminal_update_with_inner_joined_partner_applied_in_place() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query("CREATE TABLE members (id INTEGER PRIMARY KEY)", &[])
        .await?;
    ctx.query(
        "CREATE TABLE posts (id INTEGER PRIMARY KEY, member_id INTEGER)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE TABLE likes (id INTEGER PRIMARY KEY, post_id INTEGER, member_id INTEGER)",
        &[],
    )
    .await?;
    ctx.query("INSERT INTO members (id) VALUES (1), (2)", &[])
        .await?;
    // Post 12's member doesn't exist, so the inner join drops it.
    ctx.query(
        "INSERT INTO posts (id, member_id) VALUES (10, 1), (11, 2), (12, 9)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO likes (id, post_id, member_id) VALUES (100, 10, 1)",
        &[],
    )
    .await?;
    ctx.cdc_decode_settle().await?;

    let query = "SELECT p.id, l.id AS like_id FROM posts p JOIN members m ON m.id = p.member_id \
                 LEFT JOIN likes l ON l.post_id = p.id AND l.member_id = m.id ORDER BY p.id";
    let mut m = query_warm(&mut ctx, query).await?;
    for update in [
        "UPDATE likes SET post_id = 11, member_id = 2 WHERE id = 100",
        "UPDATE likes SET post_id = 12, member_id = 9 WHERE id = 100",
    ] {
        m = origin_change_applied_in_place(&mut ctx, m, update, query).await?;
    }
    Ok(())
}

/// Contrast: an anti-join's WHERE mentions the outer side, so it is optional.
/// Moving a bio exposes an author that was never loaded, and the UPDATE must
/// invalidate.
#[tokio::test]
async fn test_anti_join_optional_side_origin_change_invalidates() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_authors_bios(&mut ctx).await?;

    let query = "SELECT a.id FROM authors a \
                 LEFT JOIN bios b ON b.author_id = a.id WHERE b.id IS NULL ORDER BY a.id";
    let m = query_warm(&mut ctx, query).await?;
    origin_change_invalidates(
        &mut ctx,
        m,
        "UPDATE bios SET author_id = 3 WHERE id = 101",
        query,
    )
    .await
}

/// Contrast: a NULL-filled side with a partner beside it is optional. Moving a
/// bio to a photo no result row used needs that photo, which was never loaded,
/// and the UPDATE must invalidate.
#[tokio::test]
async fn test_null_side_with_partner_origin_change_invalidates() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query("CREATE TABLE writers (id INTEGER PRIMARY KEY)", &[])
        .await?;
    ctx.query(
        "CREATE TABLE profiles (id INTEGER PRIMARY KEY, writer_id INTEGER, photo_id INTEGER)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE TABLE photos (id INTEGER PRIMARY KEY, url TEXT)",
        &[],
    )
    .await?;
    ctx.query("INSERT INTO writers (id) VALUES (1), (2)", &[])
        .await?;
    // Profile 101's writer doesn't exist, so photo 501 is in no result row.
    ctx.query(
        "INSERT INTO profiles (id, writer_id, photo_id) VALUES (100, 1, 500), (101, 9, 501)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO photos (id, url) VALUES (500, 'a.png'), (501, 'b.png')",
        &[],
    )
    .await?;
    ctx.cdc_decode_settle().await?;

    let query = "SELECT w.id, ph.url FROM writers w \
                 LEFT JOIN (profiles p JOIN photos ph ON ph.id = p.photo_id) ON p.writer_id = w.id \
                 ORDER BY w.id";
    let m = query_warm(&mut ctx, query).await?;
    origin_change_invalidates(
        &mut ctx,
        m,
        "UPDATE profiles SET photo_id = 501 WHERE id = 100",
        query,
    )
    .await
}

/// An anti-join whose ON filters the optional side on a non-key column: a
/// DELETE of an attached row exposes a preserved row, so it invalidates
/// though the PK-only DELETE tuple carries no `flag`. The anti-join's WHERE
/// keeps attached rows out of population, so even an uncached row can decide
/// the result (PGC-528).
#[tokio::test]
async fn test_anti_join_filtered_optional_delete_invalidates() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_authors_bios(&mut ctx).await?;
    ctx.query("ALTER TABLE bios ADD COLUMN flag INTEGER DEFAULT 1", &[])
        .await?;
    ctx.cdc_decode_settle().await?;

    let query = "SELECT a.id FROM authors a LEFT JOIN bios b \
                 ON b.author_id = a.id AND b.flag = 1 WHERE b.id IS NULL ORDER BY a.id";
    let m = query_warm(&mut ctx, query).await?;
    origin_change_invalidates(&mut ctx, m, "DELETE FROM bios WHERE id = 101", query).await
}

// =============================================================================
// Null tests in an outer join's ON clause (the soft-delete idiom, PGC-510)
// =============================================================================

/// Orders of two tenants with refunds, one of them soft-deleted, and an
/// archived order.
async fn setup_orders_refunds(ctx: &mut TestContext) -> Result<(), Error> {
    ctx.query(
        "CREATE TABLE sd_orders (id INTEGER PRIMARY KEY, tenant_id INTEGER, archived_at TIMESTAMPTZ)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE TABLE sd_refunds (id INTEGER PRIMARY KEY, order_id INTEGER, amount INTEGER, \
         deleted_at TIMESTAMPTZ)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO sd_orders VALUES (1, 1, NULL), (2, 1, '2026-01-01'), (3, 2, NULL)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO sd_refunds VALUES (10, 1, 5, NULL), (11, 2, 7, '2026-01-02'), (12, 3, 9, NULL)",
        &[],
    )
    .await?;
    ctx.cdc_decode_settle().await
}

const SOFT_DELETE_QUERY: &str = "SELECT o.id, r.id AS refund, r.amount \
    FROM sd_orders o LEFT JOIN sd_refunds r \
    ON r.order_id = o.id AND r.deleted_at IS NULL \
    WHERE o.tenant_id = 1 ORDER BY o.id, r.id";

/// A refund inserted already soft-deleted fails the ON null test: its order
/// stays NULL-padded, in place.
#[tokio::test]
async fn test_soft_delete_join_insert_deleted_refund_applied_in_place() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_orders_refunds(&mut ctx).await?;

    let m = query_warm(&mut ctx, SOFT_DELETE_QUERY).await?;
    origin_change_applied_in_place(
        &mut ctx,
        m,
        "INSERT INTO sd_refunds VALUES (13, 2, 11, '2026-01-03')",
        SOFT_DELETE_QUERY,
    )
    .await?;
    Ok(())
}

/// A live refund inserted for a NULL-padded order attaches to it, in place.
#[tokio::test]
async fn test_soft_delete_join_insert_live_refund_applied_in_place() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_orders_refunds(&mut ctx).await?;

    let m = query_warm(&mut ctx, SOFT_DELETE_QUERY).await?;
    origin_change_applied_in_place(
        &mut ctx,
        m,
        "INSERT INTO sd_refunds VALUES (13, 2, 11, NULL)",
        SOFT_DELETE_QUERY,
    )
    .await?;
    Ok(())
}

/// Soft-deleting an attached refund detaches it: the order stays, NULL-padded,
/// in place.
#[tokio::test]
async fn test_soft_delete_join_set_deleted_at_applied_in_place() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_orders_refunds(&mut ctx).await?;

    let m = query_warm(&mut ctx, SOFT_DELETE_QUERY).await?;
    origin_change_applied_in_place(
        &mut ctx,
        m,
        "UPDATE sd_refunds SET deleted_at = '2026-01-04' WHERE id = 10",
        SOFT_DELETE_QUERY,
    )
    .await?;
    Ok(())
}

/// Restoring a soft-deleted refund, which population never loaded, attaches
/// it, in place.
#[tokio::test]
async fn test_soft_delete_join_clear_deleted_at_applied_in_place() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_orders_refunds(&mut ctx).await?;

    let m = query_warm(&mut ctx, SOFT_DELETE_QUERY).await?;
    origin_change_applied_in_place(
        &mut ctx,
        m,
        "UPDATE sd_refunds SET deleted_at = NULL WHERE id = 11",
        SOFT_DELETE_QUERY,
    )
    .await?;
    Ok(())
}

/// Deleting an order drops its row, refund and all, in place.
#[tokio::test]
async fn test_soft_delete_join_order_delete_applied_in_place() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_orders_refunds(&mut ctx).await?;

    let m = query_warm(&mut ctx, SOFT_DELETE_QUERY).await?;
    origin_change_applied_in_place(
        &mut ctx,
        m,
        "DELETE FROM sd_orders WHERE id = 1",
        SOFT_DELETE_QUERY,
    )
    .await?;
    Ok(())
}

/// The same join on `IS NOT NULL` attaches only soft-deleted refunds: setting
/// `deleted_at` attaches a refund and clearing it detaches one, in place.
#[tokio::test]
async fn test_soft_delete_join_is_not_null_applied_in_place() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_orders_refunds(&mut ctx).await?;

    let query = "SELECT o.id, r.id AS refund FROM sd_orders o LEFT JOIN sd_refunds r \
                 ON r.order_id = o.id AND r.deleted_at IS NOT NULL \
                 WHERE o.tenant_id = 1 ORDER BY o.id, r.id";
    let m = query_warm(&mut ctx, query).await?;
    let m = origin_change_applied_in_place(
        &mut ctx,
        m,
        "UPDATE sd_refunds SET deleted_at = '2026-01-04' WHERE id = 10",
        query,
    )
    .await?;
    origin_change_applied_in_place(
        &mut ctx,
        m,
        "UPDATE sd_refunds SET deleted_at = NULL WHERE id = 11",
        query,
    )
    .await?;
    Ok(())
}

/// A null test on the preserved side of a LEFT JOIN's ON filters no order:
/// archived orders are returned NULL-padded, an inserted one included.
#[tokio::test]
async fn test_left_join_on_preserved_null_test_returns_every_order() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_orders_refunds(&mut ctx).await?;

    let query = "SELECT o.id, r.id AS refund FROM sd_orders o LEFT JOIN sd_refunds r \
                 ON r.order_id = o.id AND o.archived_at IS NULL \
                 WHERE o.tenant_id = 1 ORDER BY o.id, r.id";
    query_warm(&mut ctx, query).await?;

    ctx.origin_query("INSERT INTO sd_orders VALUES (4, 1, '2026-01-05')", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    let (proxied, origin) = ctx.proxy_and_origin_rows(query).await?;
    assert_eq!(origin.len(), 3, "origin returns every tenant-1 order");
    assert_eq!(proxied, origin, "result after preserved-side insert");
    Ok(())
}

/// The preserved-side column of an ON null test decides which refund
/// attaches: archiving an order detaches a refund the cache may hold, and
/// unarchiving one attaches a refund it may not, so the change invalidates.
#[tokio::test]
async fn test_left_join_on_preserved_null_test_update_invalidates() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_orders_refunds(&mut ctx).await?;

    let query = "SELECT o.id, r.id AS refund FROM sd_orders o LEFT JOIN sd_refunds r \
                 ON r.order_id = o.id AND o.archived_at IS NULL \
                 WHERE o.tenant_id = 1 ORDER BY o.id, r.id";
    let m = query_warm(&mut ctx, query).await?;
    origin_change_invalidates(
        &mut ctx,
        m,
        "UPDATE sd_orders SET archived_at = NULL WHERE id = 2",
        query,
    )
    .await
}

/// In an inner join the ON null test filters as a WHERE would: a refund
/// inserted already soft-deleted can never join, so it applies in place,
/// while a live one can grow the result and invalidates (PGC-533).
#[tokio::test]
async fn test_inner_join_on_null_test_filters_like_where() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_orders_refunds(&mut ctx).await?;

    let query = "SELECT o.id, r.id AS refund FROM sd_orders o JOIN sd_refunds r \
                 ON r.order_id = o.id AND r.deleted_at IS NULL \
                 WHERE o.tenant_id = 1 ORDER BY o.id, r.id";
    let m = query_warm(&mut ctx, query).await?;
    let m = origin_change_applied_in_place(
        &mut ctx,
        m,
        "INSERT INTO sd_refunds VALUES (13, 2, 11, '2026-01-03')",
        query,
    )
    .await?;
    let (_, origin) = ctx.proxy_and_origin_rows(query).await?;
    assert_eq!(origin.len(), 1, "only the live refund joins");
    let m = assert_cache_hit(&mut ctx, m).await?;

    origin_change_invalidates(
        &mut ctx,
        m,
        "INSERT INTO sd_refunds VALUES (14, 2, 12, NULL)",
        query,
    )
    .await?;
    let (proxied, origin) = ctx.proxy_and_origin_rows(query).await?;
    assert_eq!(origin.len(), 2, "the live refund joins");
    assert_eq!(proxied, origin, "result after live insert");
    Ok(())
}

/// `WHERE r.deleted_at IS NOT NULL` rejects the NULL padding, so the LEFT
/// JOIN is maintained as an inner join: a live refund never matches and
/// applies in place, un-deleting one shrinks the result in place, and a
/// soft-deleted insert grows it and invalidates (PGC-533).
#[tokio::test]
async fn test_left_join_where_not_null_maintained_as_inner() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_orders_refunds(&mut ctx).await?;

    let query = "SELECT o.id, r.id AS refund FROM sd_orders o LEFT JOIN sd_refunds r \
                 ON r.order_id = o.id \
                 WHERE r.deleted_at IS NOT NULL AND o.tenant_id = 1 ORDER BY o.id, r.id";
    let m = query_warm(&mut ctx, query).await?;
    let (_, origin) = ctx.proxy_and_origin_rows(query).await?;
    assert_eq!(origin.len(), 1, "only the soft-deleted refund is selected");
    let m = assert_cache_hit(&mut ctx, m).await?;

    let m = origin_change_applied_in_place(
        &mut ctx,
        m,
        "INSERT INTO sd_refunds VALUES (13, 1, 11, NULL)",
        query,
    )
    .await?;
    let m = origin_change_applied_in_place(
        &mut ctx,
        m,
        "UPDATE sd_refunds SET deleted_at = NULL WHERE id = 11",
        query,
    )
    .await?;
    let (_, origin) = ctx.proxy_and_origin_rows(query).await?;
    assert_eq!(origin.len(), 0, "un-deleting the refund empties the result");
    let m = assert_cache_hit(&mut ctx, m).await?;

    origin_change_invalidates(
        &mut ctx,
        m,
        "INSERT INTO sd_refunds VALUES (14, 2, 12, '2026-01-03')",
        query,
    )
    .await?;
    let (proxied, origin) = ctx.proxy_and_origin_rows(query).await?;
    assert_eq!(origin.len(), 1, "the soft-deleted insert is selected");
    assert_eq!(proxied, origin, "result after soft-deleted insert");
    Ok(())
}

/// A single-table soft-delete query describes a region, so it covers a
/// child with the same test and a further filter, which then follows CDC;
/// it does not cover the opposite test (PGC-533).
#[tokio::test]
async fn test_soft_delete_parent_subsumes_same_test_child() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_orders_refunds(&mut ctx).await?;

    let parent = "SELECT id, order_id FROM sd_refunds WHERE deleted_at IS NULL ORDER BY id";
    let m = query_warm(&mut ctx, parent).await?;

    let child = "SELECT id, order_id FROM sd_refunds \
                 WHERE deleted_at IS NULL AND order_id = 1 ORDER BY id";
    let (proxied, origin) = ctx.proxy_and_origin_rows(child).await?;
    assert_eq!(origin.len(), 1, "origin returns the live refund of order 1");
    assert_eq!(proxied, origin, "subsumed child");
    let m = assert_subsume_hit(&mut ctx, m).await?;

    let m = origin_change_applied_in_place(
        &mut ctx,
        m,
        "INSERT INTO sd_refunds VALUES (13, 1, 11, NULL)",
        child,
    )
    .await?;
    let (_, origin) = ctx.proxy_and_origin_rows(child).await?;
    assert_eq!(origin.len(), 2, "the child sees the inserted live refund");
    let m = assert_cache_hit(&mut ctx, m).await?;

    let opposite = "SELECT id, order_id FROM sd_refunds WHERE deleted_at IS NOT NULL ORDER BY id";
    let (proxied, origin) = ctx.proxy_and_origin_rows(opposite).await?;
    assert_eq!(proxied, origin, "opposite test from origin");
    assert_cache_miss(&mut ctx, m).await?;
    Ok(())
}

/// A soft-delete LEFT JOIN loads every order its WHERE admits, so its
/// preserved side covers a single-table query on the same filter, and the
/// child then follows CDC.
#[tokio::test]
async fn test_soft_delete_join_preserved_parent_subsumes() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_orders_refunds(&mut ctx).await?;

    let m = query_warm(&mut ctx, SOFT_DELETE_QUERY).await?;

    let child = "SELECT id, tenant_id FROM sd_orders WHERE tenant_id = 1 ORDER BY id";
    let (proxied, origin) = ctx.proxy_and_origin_rows(child).await?;
    assert_eq!(origin.len(), 2, "origin returns both tenant-1 orders");
    assert_eq!(proxied, origin, "subsumed child");
    let m = assert_subsume_hit(&mut ctx, m).await?;

    ctx.origin_query("INSERT INTO sd_orders VALUES (4, 1, NULL)", &[])
        .await?;
    ctx.cdc_apply_settle().await?;

    let (proxied, origin) = ctx.proxy_and_origin_rows(child).await?;
    assert_eq!(origin.len(), 3, "origin returns the inserted order");
    assert_eq!(proxied, origin, "child after CDC insert");
    let _m = assert_cache_hit(&mut ctx, m).await?;
    Ok(())
}
