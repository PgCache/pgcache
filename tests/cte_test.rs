//! CTE (Common Table Expression) caching tests.
//!
//! These tests verify that queries using CTEs are correctly cached
//! and invalidated via CDC.
//!
//! CTE patterns covered:
//! - Simple CTE: WITH x AS (SELECT ...) SELECT * FROM x
//! - CTE with JOIN: CTE joined with a regular table
//! - Multiple CTEs: WITH a AS (...), b AS (...) SELECT ...
//! - Materialized CTE: WITH x AS MATERIALIZED (SELECT ...)
//! - CTE with CDC invalidation

use std::io::Error;

use crate::util::{TestContext, assert_cache_hit, assert_cache_miss, assert_row_at};

mod util;

/// Test simple CTE caching (cache miss then hit) and CDC invalidation.
///
/// Pattern: WITH active AS (SELECT ... WHERE ...) SELECT * FROM active
///
/// Verifies:
/// - First query is a cache miss, second is a cache hit
/// - CDC UPDATE invalidates the cache
/// - CDC DELETE does not invalidate (row removed in place)
#[tokio::test]
async fn test_cte_simple() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, department TEXT, active BOOLEAN)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO employees (id, name, department, active) VALUES \
         (1, 'Alice', 'eng', true), \
         (2, 'Bob', 'eng', false), \
         (3, 'Charlie', 'sales', true), \
         (4, 'Diana', 'sales', true)",
        &[],
    )
    .await?;

    // Wait for setup CDC events to be processed before caching —
    // INSERT events on subquery/CTE tables would trigger invalidation
    ctx.cdc_decode_settle().await?;

    let query = "WITH active_emp AS (SELECT id, name, department FROM employees WHERE active = true) \
                 SELECT name, department FROM active_emp ORDER BY name";

    // First query — cache miss, populates cache
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;

    assert_eq!(res.len(), 5); // 3 rows + RowDescription + CommandComplete
    assert_row_at(&res, 1, &[("name", "Alice"), ("department", "eng")])?;
    assert_row_at(&res, 2, &[("name", "Charlie"), ("department", "sales")])?;
    assert_row_at(&res, 3, &[("name", "Diana"), ("department", "sales")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Second query — should hit cache
    let res = ctx.simple_query(query).await?;

    assert_eq!(res.len(), 5);
    assert_row_at(&res, 1, &[("name", "Alice"), ("department", "eng")])?;
    let m = assert_cache_hit(&mut ctx, m).await?;

    // -- CDC invalidation --

    // Verify query is cached
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 5); // 3 rows: Alice, Charlie, Diana
    let m = assert_cache_hit(&mut ctx, m).await?;

    // Activate Bob via origin (CDC)
    ctx.origin_query("UPDATE employees SET active = true WHERE id = 2", &[])
        .await?;

    ctx.cdc_apply_settle().await?;

    // CDC UPDATE on CTE table invalidates → cache miss
    // Query should now include Bob
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 6); // 4 rows now
    assert_row_at(&res, 1, &[("name", "Alice"), ("department", "eng")])?;
    assert_row_at(&res, 2, &[("name", "Bob"), ("department", "eng")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Delete an employee via CDC
    ctx.origin_query("DELETE FROM employees WHERE id = 4", &[])
        .await?;

    ctx.cdc_apply_settle().await?;

    // CDC DELETE on CTE table (Inclusion) does NOT invalidate → cache hit
    // Row removed from cache table, query re-evaluates correctly
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 5); // back to 3 rows
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test CTE joined with a regular table.
///
/// Pattern: WITH cte AS (SELECT ...) SELECT ... FROM cte JOIN table ON ...
#[tokio::test]
async fn test_cte_with_join() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, department TEXT, active BOOLEAN)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO employees (id, name, department, active) VALUES \
         (1, 'Alice', 'eng', true), \
         (2, 'Bob', 'eng', true), \
         (3, 'Charlie', 'sales', true), \
         (4, 'Diana', 'sales', true)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE projects (id INTEGER PRIMARY KEY, name TEXT, lead_id INTEGER)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO projects (id, name, lead_id) VALUES \
         (1, 'Alpha', 1), \
         (2, 'Beta', 3)",
        &[],
    )
    .await?;

    // Wait for setup CDC events to be processed before caching
    ctx.cdc_decode_settle().await?;

    // CTE selects active employees, then join with projects
    let query = "WITH active_emp AS (SELECT id, name FROM employees WHERE active = true) \
                 SELECT p.name AS project, ae.name AS lead \
                 FROM projects p \
                 JOIN active_emp ae ON ae.id = p.lead_id \
                 ORDER BY p.name";

    // First query — cache miss
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;

    // Alpha led by Alice(1), Beta led by Charlie(3)
    assert_eq!(res.len(), 4); // 2 rows + RowDescription + CommandComplete
    assert_row_at(&res, 1, &[("project", "Alpha"), ("lead", "Alice")])?;
    assert_row_at(&res, 2, &[("project", "Beta"), ("lead", "Charlie")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Second query — cache hit
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4);
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test multiple CTE definitions in a single query.
///
/// Pattern: WITH a AS (...), b AS (...) SELECT ... FROM a JOIN b ON ...
#[tokio::test]
async fn test_cte_multiple_tables() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, department TEXT, active BOOLEAN)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO employees (id, name, department, active) VALUES \
         (1, 'Alice', 'eng', true), \
         (2, 'Bob', 'eng', true), \
         (3, 'Charlie', 'sales', true)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE sales (id INTEGER PRIMARY KEY, employee_id INTEGER, amount INTEGER)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO sales (id, employee_id, amount) VALUES \
         (1, 1, 500), (2, 1, 300), (3, 3, 200)",
        &[],
    )
    .await?;

    // Wait for setup CDC events
    ctx.cdc_decode_settle().await?;

    // Two CTEs: active employees and their sales
    let query = "WITH active_emp AS (SELECT id, name FROM employees WHERE active = true), \
                 emp_sales AS (SELECT employee_id, SUM(amount) AS total FROM sales GROUP BY employee_id) \
                 SELECT ae.name, es.total \
                 FROM active_emp ae \
                 JOIN emp_sales es ON es.employee_id = ae.id \
                 ORDER BY ae.name";

    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;

    // Alice has sales(500+300=800), Charlie has sales(200)
    // Bob is active but has no sales
    assert_eq!(res.len(), 4); // 2 rows + RowDescription + CommandComplete
    assert_row_at(&res, 1, &[("name", "Alice"), ("total", "800")])?;
    assert_row_at(&res, 2, &[("name", "Charlie"), ("total", "200")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Cache hit
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4);
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test MATERIALIZED CTE caching.
///
/// Pattern: WITH x AS MATERIALIZED (SELECT ...) SELECT * FROM x
///
/// MATERIALIZED CTEs are evaluated once and their results are reused.
/// The cache should handle these correctly.
#[tokio::test]
async fn test_cte_materialized() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, department TEXT, active BOOLEAN)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO employees (id, name, department, active) VALUES \
         (1, 'Alice', 'eng', true), \
         (2, 'Bob', 'eng', true), \
         (3, 'Charlie', 'sales', true)",
        &[],
    )
    .await?;

    // Wait for setup CDC events
    ctx.cdc_decode_settle().await?;

    let query = "WITH eng AS MATERIALIZED (SELECT id, name FROM employees WHERE department = 'eng') \
                 SELECT name FROM eng ORDER BY name";

    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;

    // Alice and Bob are in eng
    assert_eq!(res.len(), 4); // 2 rows + RowDescription + CommandComplete
    assert_row_at(&res, 1, &[("name", "Alice")])?;
    assert_row_at(&res, 2, &[("name", "Bob")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Cache hit
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4);
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}
