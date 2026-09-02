use std::io::Error;

use crate::util::{TestContext, assert_cache_hit, assert_cache_miss, assert_row_at};

mod util;

/// Test UNION query caching with metrics verification and CDC updates
#[tokio::test]
async fn test_set_op_union() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // -- Setup --

    ctx.query(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, tenant_id INTEGER)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE admins (id INTEGER PRIMARY KEY, name TEXT, tenant_id INTEGER)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO users (id, name, tenant_id) VALUES (1, 'Alice', 1), (2, 'Bob', 1), (3, 'Charlie', 2)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO admins (id, name, tenant_id) VALUES (10, 'Admin1', 1), (11, 'Admin2', 2)",
        &[],
    )
    .await?;

    // UNION query - combines users and admins for tenant 1
    let query = "SELECT id, name FROM users WHERE tenant_id = 1 \
                 UNION \
                 SELECT id, name FROM admins WHERE tenant_id = 1 \
                 ORDER BY id";

    // First query - cache miss, populates cache
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;

    // Should have 3 results: Alice (1), Bob (2), Admin1 (10)
    assert_eq!(res.len(), 5); // 3 rows + RowDescription + CommandComplete
    assert_row_at(&res, 1, &[("id", "1"), ("name", "Alice")])?;
    assert_row_at(&res, 2, &[("id", "2"), ("name", "Bob")])?;
    assert_row_at(&res, 3, &[("id", "10"), ("name", "Admin1")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Second query - should hit cache
    let res = ctx.simple_query(query).await?;

    assert_eq!(res.len(), 5);
    assert_row_at(&res, 1, &[("id", "1"), ("name", "Alice")])?;
    assert_row_at(&res, 2, &[("id", "2"), ("name", "Bob")])?;
    assert_row_at(&res, 3, &[("id", "10"), ("name", "Admin1")])?;
    let m = assert_cache_hit(&mut ctx, m).await?;

    // -- CDC updates --

    // Insert a new user directly on origin (simulating external change)
    ctx.origin_query(
        "INSERT INTO users (id, name, tenant_id) VALUES (4, 'Dave', 1)",
        &[],
    )
    .await?;

    ctx.cdc_decode_settle().await?;

    // CDC updates cache in place → cache hit, Dave now included
    let res = ctx.simple_query(query).await?;

    assert_eq!(res.len(), 6); // 4 rows now
    assert_row_at(&res, 1, &[("id", "1"), ("name", "Alice")])?;
    assert_row_at(&res, 2, &[("id", "2"), ("name", "Bob")])?;
    assert_row_at(&res, 3, &[("id", "4"), ("name", "Dave")])?;
    assert_row_at(&res, 4, &[("id", "10"), ("name", "Admin1")])?;
    let m = assert_cache_hit(&mut ctx, m).await?;

    // Insert a new admin on origin
    ctx.origin_query(
        "INSERT INTO admins (id, name, tenant_id) VALUES (12, 'Admin3', 1)",
        &[],
    )
    .await?;

    ctx.cdc_apply_settle().await?;

    // CDC updates cache in place → cache hit, Admin3 now included
    let res = ctx.simple_query(query).await?;

    assert_eq!(res.len(), 7); // 5 rows now
    assert_row_at(&res, 5, &[("id", "12"), ("name", "Admin3")])?;
    let m = assert_cache_hit(&mut ctx, m).await?;

    // Delete a user
    ctx.origin_query("DELETE FROM users WHERE id = 4", &[])
        .await?;

    ctx.cdc_apply_settle().await?;

    // CDC DELETE updates cache in place → cache hit, Dave gone
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 6); // back to 4 rows
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test INTERSECT query caching
#[tokio::test]
async fn test_set_op_intersect() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Create tables for intersection test
    ctx.query(
        "CREATE TABLE products_a (id INTEGER PRIMARY KEY, sku TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE products_b (id INTEGER PRIMARY KEY, sku TEXT)",
        &[],
    )
    .await?;

    // Insert products - some overlap
    ctx.query(
        "INSERT INTO products_a (id, sku) VALUES (1, 'SKU-001'), (2, 'SKU-002'), (3, 'SKU-003')",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO products_b (id, sku) VALUES (10, 'SKU-002'), (11, 'SKU-003'), (12, 'SKU-004')",
        &[],
    )
    .await?;

    // INTERSECT - only SKUs present in both tables
    let query = "SELECT sku FROM products_a \
                 INTERSECT \
                 SELECT sku FROM products_b \
                 ORDER BY sku";

    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;

    // Should have SKU-002 and SKU-003 (the overlap)
    assert_eq!(res.len(), 4); // 2 rows + RowDescription + CommandComplete
    assert_row_at(&res, 1, &[("sku", "SKU-002")])?;
    assert_row_at(&res, 2, &[("sku", "SKU-003")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Cache hit
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4);
    let m = assert_cache_hit(&mut ctx, m).await?;

    // Add a new overlapping product via CDC
    ctx.origin_query(
        "INSERT INTO products_a (id, sku) VALUES (4, 'SKU-004')",
        &[],
    )
    .await?;

    ctx.cdc_apply_settle().await?;

    // CDC updates cache in place → cache hit, SKU-004 now in intersection
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 5); // 3 rows now
    assert_row_at(&res, 3, &[("sku", "SKU-004")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test EXCEPT query caching
#[tokio::test]
async fn test_set_op_except() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Create tables for except test
    ctx.query(
        "CREATE TABLE all_items (id INTEGER PRIMARY KEY, item TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE excluded_items (id INTEGER PRIMARY KEY, item TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO all_items (id, item) VALUES (1, 'Apple'), (2, 'Banana'), (3, 'Cherry'), (4, 'Date')",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO excluded_items (id, item) VALUES (10, 'Banana'), (11, 'Date')",
        &[],
    )
    .await?;

    // EXCEPT - items in all_items but not in excluded_items
    let query = "SELECT item FROM all_items \
                 EXCEPT \
                 SELECT item FROM excluded_items \
                 ORDER BY item";

    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;

    // Should have Apple and Cherry (not Banana or Date)
    assert_eq!(res.len(), 4); // 2 rows + RowDescription + CommandComplete
    assert_row_at(&res, 1, &[("item", "Apple")])?;
    assert_row_at(&res, 2, &[("item", "Cherry")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Cache hit
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4);
    let m = assert_cache_hit(&mut ctx, m).await?;

    // Add Cherry to excluded via CDC - should disappear from result
    ctx.origin_query(
        "INSERT INTO excluded_items (id, item) VALUES (12, 'Cherry')",
        &[],
    )
    .await?;

    ctx.cdc_apply_settle().await?;

    // CDC updates cache in place → cache hit, Cherry removed
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 3); // 1 row
    assert_row_at(&res, 1, &[("item", "Apple")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test UNION ALL (no deduplication) vs UNION (with deduplication)
#[tokio::test]
async fn test_set_op_union_all() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Create tables with duplicate values across them
    ctx.query(
        "CREATE TABLE list_a (id INTEGER PRIMARY KEY, value TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE list_b (id INTEGER PRIMARY KEY, value TEXT)",
        &[],
    )
    .await?;

    // Insert values with duplicates
    ctx.query(
        "INSERT INTO list_a (id, value) VALUES (1, 'X'), (2, 'Y')",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO list_b (id, value) VALUES (10, 'Y'), (11, 'Z')",
        &[],
    )
    .await?;

    // UNION (deduplicated) - should have X, Y, Z
    let query_union = "SELECT value FROM list_a \
                       UNION \
                       SELECT value FROM list_b \
                       ORDER BY value";

    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query_union).await?;
    assert_eq!(res.len(), 5); // 3 rows (deduplicated)
    assert_row_at(&res, 1, &[("value", "X")])?;
    assert_row_at(&res, 2, &[("value", "Y")])?;
    assert_row_at(&res, 3, &[("value", "Z")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // UNION ALL (not deduplicated) - should have X, Y, Y, Z
    let query_union_all = "SELECT value FROM list_a \
                           UNION ALL \
                           SELECT value FROM list_b \
                           ORDER BY value";

    let res = ctx.simple_query(query_union_all).await?;
    assert_eq!(res.len(), 6); // 4 rows (not deduplicated)
    assert_row_at(&res, 1, &[("value", "X")])?;
    assert_row_at(&res, 2, &[("value", "Y")])?;
    assert_row_at(&res, 3, &[("value", "Y")])?; // duplicate
    assert_row_at(&res, 4, &[("value", "Z")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Verify both queries hit cache on subsequent runs
    let res = ctx.simple_query(query_union).await?;
    assert_eq!(res.len(), 5);
    let m = assert_cache_hit(&mut ctx, m).await?;

    let res = ctx.simple_query(query_union_all).await?;
    assert_eq!(res.len(), 6);
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test per-branch constraint filtering for UNION with JOINed branches.
///
/// Single-table UNION branches skip constraint checks via is_single_table(),
/// so JOINs are needed to create multi-table branches that exercise the
/// constraint checking code path.
///
/// With per-UpdateQuery constraints, each UNION branch's WHERE clause is
/// analyzed independently. For Direct source multi-table branches,
/// row_uncached_invalidation_check checks has_table_constraints and calls
/// row_constraints_match before deciding on invalidation.
///
/// Before per-UpdateQuery constraints: UNION queries had empty constraints
/// (as_select() returned None). Multi-table branches with
/// has_table_constraints = false always invalidated.
/// After: each branch gets its own constraints, and non-matching CDC events
/// on constrained tables are filtered.
#[tokio::test]
async fn test_set_op_union_join_constraint_filter() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Branch 1 tables: orders joined with customers, filtered by region
    ctx.query(
        "CREATE TABLE union_customers (id INTEGER PRIMARY KEY, name TEXT, region TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE union_orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount INTEGER)",
        &[],
    )
    .await?;

    // Branch 2 tables: refunds joined with suppliers, filtered by region
    ctx.query(
        "CREATE TABLE union_suppliers (id INTEGER PRIMARY KEY, name TEXT, region TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE union_refunds (id INTEGER PRIMARY KEY, supplier_id INTEGER, amount INTEGER)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO union_customers (id, name, region) VALUES \
         (1, 'Acme', 'east'), (2, 'Globex', 'west')",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO union_orders (id, customer_id, amount) VALUES \
         (10, 1, 500), (11, 2, 300)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO union_suppliers (id, name, region) VALUES \
         (1, 'SupplyA', 'east'), (2, 'SupplyB', 'west')",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO union_refunds (id, supplier_id, amount) VALUES \
         (20, 1, 100), (21, 2, 200)",
        &[],
    )
    .await?;

    // Wait for setup CDC events to settle
    ctx.cdc_decode_settle().await?;

    // UNION with JOINs in each branch, both filtered by region = 'east'
    //
    // Branch 1 constraints: {union_customers.region = 'east'}
    // Branch 2 constraints: {union_suppliers.region = 'east'}
    let query = "SELECT o.amount FROM union_orders o \
                 JOIN union_customers c ON o.customer_id = c.id \
                 WHERE c.region = 'east' \
                 UNION \
                 SELECT r.amount FROM union_refunds r \
                 JOIN union_suppliers s ON r.supplier_id = s.id \
                 WHERE s.region = 'east' \
                 ORDER BY amount";

    // First query — cache miss
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4); // 2 rows (100, 500) + RowDesc + CommandComplete
    assert_row_at(&res, 1, &[("amount", "100")])?;
    assert_row_at(&res, 2, &[("amount", "500")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Second query — cache hit
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4);
    let m = assert_cache_hit(&mut ctx, m).await?;

    // --- Non-matching INSERT on constrained table: region = 'west' ---
    // union_customers.region = 'west' does NOT match constraint 'east'
    // row_constraints_match returns false → no invalidation
    ctx.origin_query(
        "INSERT INTO union_customers (id, name, region) VALUES (3, 'Wayne', 'west')",
        &[],
    )
    .await?;

    ctx.cdc_apply_settle().await?;

    // Cache hit — non-matching INSERT on constrained table was filtered
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4); // still 100, 500
    let m = assert_cache_hit(&mut ctx, m).await?;

    // --- Matching INSERT on constrained table: region = 'east' ---
    // union_customers.region = 'east' DOES match constraint
    // has_table_constraints = true → row_constraints_match → true → invalidate
    ctx.origin_query(
        "INSERT INTO union_customers (id, name, region) VALUES (4, 'Stark', 'east')",
        &[],
    )
    .await?;

    ctx.cdc_apply_settle().await?;

    // Cache miss — matching INSERT triggered invalidation
    // Result unchanged (Stark has no orders) but invalidation occurred
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4); // still 100, 500 (Stark has no orders)
    let _m = assert_cache_miss(&mut ctx, m).await?;

    Ok(())
}
