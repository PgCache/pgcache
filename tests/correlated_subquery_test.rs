//! Correlated subquery caching tests (EXISTS, NOT EXISTS, IN, NOT IN, scalar).
//!
//! Correlated subqueries reference columns from the outer query. pgcache
//! decorrelates them into JOINs for update query generation so that CDC
//! invalidation can track dependencies on the inner tables.
//!
//! Decorrelation transforms:
//! - EXISTS → INNER JOIN + DISTINCT (semi-join)
//! - NOT EXISTS → LEFT JOIN + IS NULL (anti-join)
//! - IN → INNER JOIN + DISTINCT (semi-join, with IN predicate in ON)
//! - NOT IN → LEFT JOIN + IS NULL (anti-join, with NOT IN predicate in ON)
//! - Scalar subquery → LEFT JOIN + derived table (aggregate or lookup)
//!
//! These tests verify:
//! - Correlated queries are cached and served correctly
//! - CDC events on inner tables trigger appropriate invalidation
//! - CDC events on outer tables trigger appropriate invalidation
//! - EXISTS with GROUP BY/HAVING in inner query (stripped for update queries)
//! - Residual predicates on inner tables filter CDC events correctly
//! - Scalar subqueries in SELECT list and WHERE clause
//! - IN/NOT IN with correlation predicates and residual filtering

use std::io::Error;

use crate::util::{
    MetricsSnapshot, TestContext, assert_cache_hit, assert_cache_miss, assert_row_at,
};

mod util;

/// A row's expected columns, as `(column, text value)` pairs.
type ExpectedRow<'a> = &'a [(&'a str, &'a str)];

/// How the cache answers a read after an origin write reached it.
#[derive(Clone, Copy)]
enum Serve {
    /// The write was applied in place, or filtered out; the read is a hit.
    Hit,
    /// The write invalidated the query; the read misses and re-populates.
    Miss,
}

/// Runs the DDL and seed rows through pgcache, then waits for CDC to decode
/// them so the tables are known before the first cached read.
async fn seed(ctx: &mut TestContext, statements: &[&str]) -> Result<(), Error> {
    for statement in statements {
        ctx.query(*statement, &[]).await?;
    }
    ctx.cdc_decode_settle().await
}

/// Reads `query` and checks that its data rows are exactly `expected`, in
/// order.
async fn rows_expect(
    ctx: &mut TestContext,
    query: &str,
    expected: &[ExpectedRow<'_>],
) -> Result<(), Error> {
    let res = ctx.simple_query(query).await?;
    // RowDescription + data rows + CommandComplete.
    assert_eq!(res.len(), expected.len() + 2, "{query}");
    for (index, row) in expected.iter().enumerate() {
        assert_row_at(&res, index + 1, row)?;
    }
    Ok(())
}

/// A query warmed into the cache, with the metrics as of its last read.
struct CachedQuery<'a> {
    ctx: &'a mut TestContext,
    query: &'a str,
    m: MetricsSnapshot,
}

impl<'a> CachedQuery<'a> {
    /// The first read of `query` misses and populates the cache; once
    /// settled, the same read is a hit.
    async fn warm(
        ctx: &'a mut TestContext,
        query: &'a str,
        expected: &[ExpectedRow<'_>],
    ) -> Result<Self, Error> {
        let m = ctx.metrics().await?;
        rows_expect(ctx, query, expected).await?;
        let m = assert_cache_miss(ctx, m).await?;
        ctx.cache_settle().await?;
        rows_expect(ctx, query, expected).await?;
        let m = assert_cache_hit(ctx, m).await?;
        Ok(Self { ctx, query, m })
    }

    /// Applies `write` on origin, waits for CDC to apply it, and reads the
    /// query expecting `expected` rows served as `serve` says. After a miss
    /// the cache is settled again, ready for the next write.
    async fn origin_write(
        &mut self,
        write: &str,
        expected: &[ExpectedRow<'_>],
        serve: Serve,
    ) -> Result<(), Error> {
        self.ctx.origin_query(write, &[]).await?;
        self.ctx.cdc_apply_settle().await?;
        rows_expect(self.ctx, self.query, expected).await?;
        let m = std::mem::take(&mut self.m);
        self.m = match serve {
            Serve::Hit => assert_cache_hit(self.ctx, m).await?,
            Serve::Miss => {
                let m = assert_cache_miss(self.ctx, m).await?;
                self.ctx.cache_settle().await?;
                m
            }
        };
        Ok(())
    }
}

// =============================================================================
// EXISTS — Basic Caching and CDC Invalidation
// =============================================================================

/// Test correlated EXISTS subquery caching and CDC invalidation.
///
/// Pattern: SELECT ... FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.ref = t1.ref)
///
/// Decorrelated form (for update queries):
///   SELECT DISTINCT t1.* FROM t1 JOIN t2 ON t2.ref = t1.ref
///
/// CDC behavior after decorrelation:
/// - INSERT into inner table → invalidates (new correlation match may appear)
/// - DELETE from inner table → in-place removal, cache hit (INNER JOIN DELETE
///   can only shrink the result, serve-time EXISTS re-evaluates)
/// - UPDATE on inner table correlation column → invalidates
/// - INSERT into outer table → does NOT invalidate (new row evaluated at serve time)
#[tokio::test]
async fn test_correlated_exists_basic() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    seed(
        &mut ctx,
        &[
            "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)",
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, emp_id INTEGER, status TEXT)",
            "INSERT INTO employees (id, name, dept_id) VALUES \
             (1, 'Alice', 10), (2, 'Bob', 20), (3, 'Charlie', 10)",
            // Only Alice and Charlie have orders
            "INSERT INTO orders (id, emp_id, status) VALUES \
             (100, 1, 'shipped'), (101, 3, 'pending')",
        ],
    )
    .await?;

    // Employees who have at least one order
    let query = "SELECT e.name FROM employees e \
                 WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id) \
                 ORDER BY e.name";
    let mut cached = CachedQuery::warm(
        &mut ctx,
        query,
        &[&[("name", "Alice")], &[("name", "Charlie")]],
    )
    .await?;

    // An order for Bob: he appears, so the insert invalidates.
    cached
        .origin_write(
            "INSERT INTO orders (id, emp_id, status) VALUES (102, 2, 'shipped')",
            &[
                &[("name", "Alice")],
                &[("name", "Bob")],
                &[("name", "Charlie")],
            ],
            Serve::Miss,
        )
        .await?;

    // Direct source: the row is removed from the cache table and serve-time
    // EXISTS re-evaluates; an INNER JOIN DELETE can only shrink the result.
    cached
        .origin_write(
            "DELETE FROM orders WHERE id = 101",
            &[&[("name", "Alice")], &[("name", "Bob")]],
            Serve::Hit,
        )
        .await?;
    Ok(())
}

// =============================================================================
// NOT EXISTS — Basic Caching and CDC Invalidation
// =============================================================================

/// Test correlated NOT EXISTS subquery caching and CDC invalidation.
///
/// Pattern: SELECT ... FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.ref = t1.ref)
///
/// Decorrelated form (for update queries):
///   SELECT t1.* FROM t1 LEFT JOIN t2 ON t2.ref = t1.ref WHERE t2.ref IS NULL
///
/// CDC behavior after decorrelation:
/// - INSERT into inner table → invalidates (match may appear, removing outer row from result)
/// - DELETE from inner table → invalidates (match may disappear, adding outer row to result)
/// - UPDATE on inner table → invalidates
#[tokio::test]
async fn test_correlated_not_exists_basic() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    seed(
        &mut ctx,
        &[
            "CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)",
            "INSERT INTO departments (id, name) VALUES \
             (1, 'Engineering'), (2, 'Marketing'), (3, 'Sales')",
            // Engineering and Sales have employees; Marketing is empty
            "INSERT INTO employees (id, name, dept_id) VALUES \
             (10, 'Alice', 1), (11, 'Bob', 3)",
        ],
    )
    .await?;

    // Departments with no employees
    let query = "SELECT d.name FROM departments d \
                 WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id) \
                 ORDER BY d.name";
    let mut cached = CachedQuery::warm(&mut ctx, query, &[&[("name", "Marketing")]]).await?;

    // An employee in Marketing: it disappears from the result.
    cached
        .origin_write(
            "INSERT INTO employees (id, name, dept_id) VALUES (12, 'Carol', 2)",
            &[],
            Serve::Miss,
        )
        .await?;

    // Bob leaves Sales: Sales reappears.
    cached
        .origin_write(
            "DELETE FROM employees WHERE id = 11",
            &[&[("name", "Sales")]],
            Serve::Miss,
        )
        .await?;
    Ok(())
}

// =============================================================================
// EXISTS with Residual Predicates — Constraint Filtering
// =============================================================================

/// Test EXISTS with residual predicates (non-correlation WHERE conditions).
///
/// Pattern: SELECT ... FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.ref = t1.ref AND t2.col = 'val')
///
/// The residual predicate (t2.col = 'val') produces constraints on the inner
/// table's update query. CDC events that don't match the constraint should NOT
/// trigger invalidation.
#[tokio::test]
async fn test_correlated_exists_residual_constraint_filter() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    seed(
        &mut ctx,
        &[
            "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, emp_id INTEGER, status TEXT)",
            "INSERT INTO employees (id, name) VALUES \
             (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
            // Only Alice has a 'shipped' order
            "INSERT INTO orders (id, emp_id, status) VALUES \
             (100, 1, 'shipped'), (101, 2, 'pending')",
        ],
    )
    .await?;

    // Employees with at least one shipped order
    let query = "SELECT e.name FROM employees e \
                 WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id AND o.status = 'shipped') \
                 ORDER BY e.name";
    let mut cached = CachedQuery::warm(&mut ctx, query, &[&[("name", "Alice")]]).await?;

    // 'pending' doesn't match the 'shipped' constraint: no invalidation.
    cached
        .origin_write(
            "INSERT INTO orders (id, emp_id, status) VALUES (102, 3, 'pending')",
            &[&[("name", "Alice")]],
            Serve::Hit,
        )
        .await?;

    // 'shipped' matches the constraint: invalidation.
    cached
        .origin_write(
            "INSERT INTO orders (id, emp_id, status) VALUES (103, 3, 'shipped')",
            &[&[("name", "Alice")], &[("name", "Charlie")]],
            Serve::Miss,
        )
        .await?;
    Ok(())
}

// =============================================================================
// NOT EXISTS with Residual Predicates
// =============================================================================

/// Test NOT EXISTS with residual predicates — residual goes into JOIN ON clause.
///
/// Pattern: SELECT ... FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.ref = t1.ref AND t2.col = 'val')
///
/// For NOT EXISTS, residual predicates go in the ON clause (not outer WHERE)
/// to preserve LEFT JOIN semantics. A department is included if no employees
/// match the full ON condition (correlation + residual).
#[tokio::test]
async fn test_correlated_not_exists_residual_predicates() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    seed(
        &mut ctx,
        &[
            "CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, status TEXT)",
            "INSERT INTO departments (id, name) VALUES \
             (1, 'Engineering'), (2, 'Marketing'), (3, 'Sales')",
            // Engineering has an active employee; Marketing has an inactive one;
            // Sales has none
            "INSERT INTO employees (id, name, dept_id, status) VALUES \
             (10, 'Alice', 1, 'active'), (11, 'Bob', 2, 'inactive')",
        ],
    )
    .await?;

    // Departments with no ACTIVE employees
    let query = "SELECT d.name FROM departments d \
                 WHERE NOT EXISTS ( \
                     SELECT 1 FROM employees e \
                     WHERE e.dept_id = d.id AND e.status = 'active' \
                 ) \
                 ORDER BY d.name";
    let mut cached = CachedQuery::warm(
        &mut ctx,
        query,
        &[&[("name", "Marketing")], &[("name", "Sales")]],
    )
    .await?;

    // An active employee in Sales: Sales disappears.
    cached
        .origin_write(
            "INSERT INTO employees (id, name, dept_id, status) VALUES (12, 'Carol', 3, 'active')",
            &[&[("name", "Marketing")]],
            Serve::Miss,
        )
        .await?;
    Ok(())
}

// =============================================================================
// EXISTS with Inner GROUP BY — Stripped for Update Queries
// =============================================================================

/// Test correlated EXISTS with GROUP BY in the inner subquery.
///
/// GROUP BY/HAVING are stripped from the inner subquery for update query
/// generation (conservative, over-invalidation is safe for EXISTS).
/// The cached query still returns correct results because population uses
/// the original query.
///
/// Pattern: SELECT ... FROM t1
///          WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.ref = t1.ref GROUP BY t2.col HAVING count(*) > 1)
#[tokio::test]
async fn test_correlated_exists_with_group_by() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    seed(
        &mut ctx,
        &[
            "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, emp_id INTEGER, status TEXT)",
            "INSERT INTO employees (id, name) VALUES \
             (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
            // Alice has 2 orders, Bob has 1, Charlie has 0
            "INSERT INTO orders (id, emp_id, status) VALUES \
             (100, 1, 'shipped'), (101, 1, 'pending'), (102, 2, 'shipped')",
        ],
    )
    .await?;

    // Employees with more than 1 order
    let query = "SELECT e.name FROM employees e \
                 WHERE EXISTS ( \
                     SELECT 1 FROM orders o \
                     WHERE o.emp_id = e.id \
                     GROUP BY o.emp_id \
                     HAVING count(*) > 1 \
                 ) \
                 ORDER BY e.name";
    let mut cached = CachedQuery::warm(&mut ctx, query, &[&[("name", "Alice")]]).await?;

    // A second order for Bob. GROUP BY is stripped for update queries, so any
    // orders INSERT invalidates (conservative); re-population applies HAVING.
    cached
        .origin_write(
            "INSERT INTO orders (id, emp_id, status) VALUES (103, 2, 'pending')",
            &[&[("name", "Alice")], &[("name", "Bob")]],
            Serve::Miss,
        )
        .await?;
    Ok(())
}

// =============================================================================
// EXISTS — CDC on Outer Table
// =============================================================================

/// Test that CDC events on the outer table correctly affect cached results.
///
/// After decorrelation, the outer table is the left side of the JOIN (Direct source).
/// For Direct source, CDC UPDATEs are applied in-place to cached rows — the cache
/// self-corrects without full invalidation, so the next query is a cache HIT.
#[tokio::test]
async fn test_correlated_exists_outer_table_cdc() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    seed(
        &mut ctx,
        &[
            "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, active INTEGER)",
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, emp_id INTEGER)",
            "INSERT INTO employees (id, name, active) VALUES \
             (1, 'Alice', 1), (2, 'Bob', 1), (3, 'Charlie', 0)",
            // Alice and Bob have orders
            "INSERT INTO orders (id, emp_id) VALUES (100, 1), (101, 2)",
        ],
    )
    .await?;

    // Active employees who have orders
    let query = "SELECT e.name FROM employees e \
                 WHERE e.active = 1 \
                 AND EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id) \
                 ORDER BY e.name";
    let mut cached =
        CachedQuery::warm(&mut ctx, query, &[&[("name", "Alice")], &[("name", "Bob")]]).await?;

    // Deactivating Bob updates the outer row in place: no invalidation.
    cached
        .origin_write(
            "UPDATE employees SET active = 0 WHERE id = 2",
            &[&[("name", "Alice")]],
            Serve::Hit,
        )
        .await?;
    Ok(())
}

// =============================================================================
// EXISTS with Multiple Tables in Inner Subquery
// =============================================================================

/// Test EXISTS where the inner subquery joins multiple tables.
///
/// Pattern: SELECT ... FROM t1
///          WHERE EXISTS (SELECT 1 FROM t2 JOIN t3 ON ... WHERE t2.ref = t1.ref)
///
/// CDC should track dependencies on all inner tables (t2 and t3).
#[tokio::test]
async fn test_correlated_exists_inner_join() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    seed(
        &mut ctx,
        &[
            "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, emp_id INTEGER, customer_id INTEGER)",
            "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, tier TEXT)",
            "INSERT INTO employees (id, name) VALUES (1, 'Alice'), (2, 'Bob')",
            "INSERT INTO customers (id, name, tier) VALUES \
             (10, 'Acme', 'gold'), (11, 'Beta', 'silver')",
            // Alice has an order with a gold customer, Bob one with silver
            "INSERT INTO orders (id, emp_id, customer_id) VALUES (100, 1, 10), (101, 2, 11)",
        ],
    )
    .await?;

    // Employees who have orders from gold-tier customers
    let query = "SELECT e.name FROM employees e \
                 WHERE EXISTS ( \
                     SELECT 1 FROM orders o \
                     JOIN customers c ON c.id = o.customer_id \
                     WHERE o.emp_id = e.id AND c.tier = 'gold' \
                 ) \
                 ORDER BY e.name";
    let mut cached = CachedQuery::warm(&mut ctx, query, &[&[("name", "Alice")]]).await?;

    // Beta upgraded to gold on the inner joined table: Bob appears.
    cached
        .origin_write(
            "UPDATE customers SET tier = 'gold' WHERE id = 11",
            &[&[("name", "Alice")], &[("name", "Bob")]],
            Serve::Miss,
        )
        .await?;
    Ok(())
}

// =============================================================================
// IN — Basic Caching and CDC Invalidation
// =============================================================================

/// Test correlated IN subquery caching and CDC invalidation.
///
/// Pattern: SELECT ... FROM t1 WHERE t1.col IN (SELECT t2.col FROM t2 WHERE t2.ref = t1.ref)
///
/// Decorrelated form (for update queries):
///   SELECT DISTINCT t1.* FROM t1 JOIN t2 ON t1.col = t2.col AND t2.ref = t1.ref
///
/// IN is a semi-join, same as EXISTS, with the IN predicate added to the
/// JOIN ON condition.
///
/// CDC behavior after decorrelation:
/// - INSERT into inner table → invalidates (new match may add outer row to result)
/// - DELETE from inner table → in-place removal, cache hit (INNER JOIN DELETE
///   can only shrink the result, serve-time IN re-evaluates)
#[tokio::test]
async fn test_correlated_in_basic() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    seed(
        &mut ctx,
        &[
            "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)",
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, emp_id INTEGER, dept_id INTEGER)",
            "INSERT INTO employees (id, name, dept_id) VALUES \
             (1, 'Alice', 10), (2, 'Bob', 20), (3, 'Charlie', 10)",
            // Alice and Charlie have orders in their department (dept 10)
            "INSERT INTO orders (id, emp_id, dept_id) VALUES \
             (100, 1, 10), (101, 3, 10)",
        ],
    )
    .await?;

    // Employees who have orders in their own department
    // Correlation: o.dept_id = e.dept_id, IN predicate: e.id = o.emp_id
    let query = "SELECT e.name FROM employees e \
                 WHERE e.id IN ( \
                     SELECT o.emp_id FROM orders o WHERE o.dept_id = e.dept_id \
                 ) \
                 ORDER BY e.name";
    let mut cached = CachedQuery::warm(
        &mut ctx,
        query,
        &[&[("name", "Alice")], &[("name", "Charlie")]],
    )
    .await?;

    // An order for Bob in dept 20: Bob appears.
    cached
        .origin_write(
            "INSERT INTO orders (id, emp_id, dept_id) VALUES (102, 2, 20)",
            &[
                &[("name", "Alice")],
                &[("name", "Bob")],
                &[("name", "Charlie")],
            ],
            Serve::Miss,
        )
        .await?;

    // Direct source: the row is removed from the cache table and serve-time
    // IN re-evaluates; an INNER JOIN DELETE can only shrink the result.
    cached
        .origin_write(
            "DELETE FROM orders WHERE id = 101",
            &[&[("name", "Alice")], &[("name", "Bob")]],
            Serve::Hit,
        )
        .await?;
    Ok(())
}

// =============================================================================
// IN with Correlated Predicate — Semi-Join
// =============================================================================

/// Test correlated IN subquery where the inner WHERE has a correlation predicate.
///
/// Pattern: SELECT ... FROM employees e
///          WHERE e.dept_id IN (SELECT d.id FROM departments d WHERE d.location = e.location)
///
/// Both the IN predicate (e.dept_id = d.id) and the correlation predicate
/// (d.location = e.location) appear in the JOIN ON condition.
///
/// CDC behavior: INSERT/DELETE/UPDATE on departments → invalidates
#[tokio::test]
async fn test_correlated_in_with_correlation() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    seed(
        &mut ctx,
        &[
            "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, location TEXT)",
            "CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT, location TEXT)",
            "INSERT INTO employees (id, name, dept_id, location) VALUES \
             (1, 'Alice', 10, 'NYC'), (2, 'Bob', 20, 'SF'), (3, 'Charlie', 30, 'NYC')",
            // Dept 10 in NYC, dept 20 in SF: Alice's and Bob's departments match
            // their locations. Charlie's dept 30 doesn't exist yet.
            "INSERT INTO departments (id, name, location) VALUES \
             (10, 'Engineering', 'NYC'), (20, 'Marketing', 'SF')",
        ],
    )
    .await?;

    // Employees whose dept_id matches a department in the same location
    let query = "SELECT e.name FROM employees e \
                 WHERE e.dept_id IN ( \
                     SELECT d.id FROM departments d WHERE d.location = e.location \
                 ) \
                 ORDER BY e.name";
    let mut cached =
        CachedQuery::warm(&mut ctx, query, &[&[("name", "Alice")], &[("name", "Bob")]]).await?;

    // Dept 30 in NYC: Charlie appears.
    cached
        .origin_write(
            "INSERT INTO departments (id, name, location) VALUES (30, 'Sales', 'NYC')",
            &[
                &[("name", "Alice")],
                &[("name", "Bob")],
                &[("name", "Charlie")],
            ],
            Serve::Miss,
        )
        .await?;
    Ok(())
}

// =============================================================================
// IN with Residual Predicates — Constraint Filtering
// =============================================================================

/// Test IN with residual predicates that create CDC constraints.
///
/// Pattern: SELECT ... FROM employees e
///          WHERE e.dept_id IN (SELECT d.id FROM departments d WHERE d.id = e.dept_id AND d.status = 'active')
///
/// The residual predicate (d.status = 'active') produces constraints on the
/// inner table's update query. CDC events that don't match should NOT
/// trigger invalidation.
#[tokio::test]
async fn test_correlated_in_residual_constraint_filter() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    seed(
        &mut ctx,
        &[
            "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)",
            "CREATE TABLE departments (id INTEGER PRIMARY KEY, dept_id INTEGER, status TEXT)",
            "INSERT INTO employees (id, name, dept_id) VALUES \
             (1, 'Alice', 10), (2, 'Bob', 20), (3, 'Charlie', 30)",
            // Dept 10 is active, dept 20 is inactive
            "INSERT INTO departments (id, dept_id, status) VALUES \
             (100, 10, 'active'), (101, 20, 'inactive')",
        ],
    )
    .await?;

    // Employees in active departments
    let query = "SELECT e.name FROM employees e \
                 WHERE e.dept_id IN ( \
                     SELECT d.dept_id FROM departments d \
                     WHERE d.dept_id = e.dept_id AND d.status = 'active' \
                 ) \
                 ORDER BY e.name";
    let mut cached = CachedQuery::warm(&mut ctx, query, &[&[("name", "Alice")]]).await?;

    // 'inactive' doesn't match the 'active' constraint: no invalidation.
    cached
        .origin_write(
            "INSERT INTO departments (id, dept_id, status) VALUES (102, 30, 'inactive')",
            &[&[("name", "Alice")]],
            Serve::Hit,
        )
        .await?;

    // 'active' matches the constraint: invalidation.
    cached
        .origin_write(
            "INSERT INTO departments (id, dept_id, status) VALUES (103, 30, 'active')",
            &[&[("name", "Alice")], &[("name", "Charlie")]],
            Serve::Miss,
        )
        .await?;
    Ok(())
}

// =============================================================================
// NOT IN — Basic Caching and CDC Invalidation
// =============================================================================

/// Test correlated NOT IN subquery caching and CDC invalidation.
///
/// Pattern: SELECT ... FROM t1 WHERE t1.col NOT IN (SELECT t2.col FROM t2 WHERE t2.ref = t1.ref)
///
/// Decorrelated form (for update queries):
///   SELECT t1.* FROM t1 LEFT JOIN t2 ON t1.col = t2.col AND t2.ref = t1.ref
///   WHERE t2.col IS NULL
///
/// NOT IN is an anti-join, same as NOT EXISTS, with the NOT IN predicate
/// added to the JOIN ON condition and an IS NULL check in the outer WHERE.
///
/// CDC behavior after decorrelation:
/// - INSERT into inner table → invalidates (new match may exclude outer row)
/// - DELETE from inner table → invalidates (removed match may include outer row)
#[tokio::test]
async fn test_correlated_not_in_basic() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    seed(
        &mut ctx,
        &[
            "CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)",
            "INSERT INTO departments (id, name) VALUES \
             (1, 'Engineering'), (2, 'Marketing'), (3, 'Sales')",
            // Engineering and Sales have employees; Marketing is empty
            "INSERT INTO employees (id, name, dept_id) VALUES \
             (10, 'Alice', 1), (11, 'Bob', 3)",
        ],
    )
    .await?;

    // Departments whose id does NOT appear among employee dept_ids
    // Correlation: e.dept_id = d.id, NOT IN predicate: d.id = e.dept_id
    let query = "SELECT d.name FROM departments d \
                 WHERE d.id NOT IN ( \
                     SELECT e.dept_id FROM employees e WHERE e.dept_id = d.id \
                 ) \
                 ORDER BY d.name";
    let mut cached = CachedQuery::warm(&mut ctx, query, &[&[("name", "Marketing")]]).await?;

    // An employee in Marketing: it disappears from the result.
    cached
        .origin_write(
            "INSERT INTO employees (id, name, dept_id) VALUES (12, 'Carol', 2)",
            &[],
            Serve::Miss,
        )
        .await?;

    // Bob leaves Sales: Sales reappears.
    cached
        .origin_write(
            "DELETE FROM employees WHERE id = 11",
            &[&[("name", "Sales")]],
            Serve::Miss,
        )
        .await?;
    Ok(())
}

// =============================================================================
// NOT IN with Correlated Predicate — Anti-Join
// =============================================================================

/// Test correlated NOT IN where the inner WHERE has a correlation predicate.
///
/// Pattern: SELECT ... FROM employees e
///          WHERE e.id NOT IN (SELECT o.emp_id FROM orders o WHERE o.status = e.status)
///
/// Both the NOT IN predicate (e.id = o.emp_id) and the correlation predicate
/// (o.status = e.status) appear in the LEFT JOIN ON condition.
/// The IS NULL check on the inner output column completes the anti-join.
#[tokio::test]
async fn test_correlated_not_in_with_correlation() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    seed(
        &mut ctx,
        &[
            "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, status TEXT)",
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, emp_id INTEGER, status TEXT)",
            "INSERT INTO employees (id, name, status) VALUES \
             (1, 'Alice', 'active'), (2, 'Bob', 'active'), (3, 'Charlie', 'inactive')",
            // Alice has an active order and Charlie an inactive one; Bob has
            // no order matching his status
            "INSERT INTO orders (id, emp_id, status) VALUES \
             (100, 1, 'active'), (101, 3, 'inactive')",
        ],
    )
    .await?;

    // Employees whose id does NOT appear among orders with a matching status:
    // Alice and Charlie have matching orders, Bob doesn't.
    let query = "SELECT e.name FROM employees e \
                 WHERE e.id NOT IN ( \
                     SELECT o.emp_id FROM orders o WHERE o.status = e.status \
                 ) \
                 ORDER BY e.name";
    let mut cached = CachedQuery::warm(&mut ctx, query, &[&[("name", "Bob")]]).await?;

    // An active order for Bob: everyone has a matching order.
    cached
        .origin_write(
            "INSERT INTO orders (id, emp_id, status) VALUES (102, 2, 'active')",
            &[],
            Serve::Miss,
        )
        .await?;

    // Alice's order removed: Alice reappears.
    cached
        .origin_write(
            "DELETE FROM orders WHERE id = 100",
            &[&[("name", "Alice")]],
            Serve::Miss,
        )
        .await?;
    Ok(())
}

// =============================================================================
// NOT IN with Residual Predicates — Constraint Filtering
// =============================================================================

/// Test NOT IN with residual predicates that create CDC constraints.
///
/// For NOT IN, residual predicates go in the ON clause (not outer WHERE)
/// to preserve LEFT JOIN semantics, same as NOT EXISTS. The residual
/// still produces constraints on the inner table's update query.
#[tokio::test]
async fn test_correlated_not_in_residual_constraint_filter() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    seed(
        &mut ctx,
        &[
            "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)",
            "CREATE TABLE blocked_assignments (id INTEGER PRIMARY KEY, dept_id INTEGER, severity TEXT)",
            "INSERT INTO employees (id, name, dept_id) VALUES \
             (1, 'Alice', 10), (2, 'Bob', 20), (3, 'Charlie', 30)",
            // Dept 10 has a critical block, dept 20 a minor one
            "INSERT INTO blocked_assignments (id, dept_id, severity) VALUES \
             (100, 10, 'critical'), (101, 20, 'minor')",
        ],
    )
    .await?;

    // Employees not in critically blocked departments: Alice excluded (dept
    // 10 critical), Bob included (dept 20 minor), Charlie included (no block).
    let query = "SELECT e.name FROM employees e \
                 WHERE e.dept_id NOT IN ( \
                     SELECT ba.dept_id FROM blocked_assignments ba \
                     WHERE ba.dept_id = e.dept_id AND ba.severity = 'critical' \
                 ) \
                 ORDER BY e.name";
    let mut cached = CachedQuery::warm(
        &mut ctx,
        query,
        &[&[("name", "Bob")], &[("name", "Charlie")]],
    )
    .await?;

    // 'minor' doesn't match the 'critical' constraint: no invalidation.
    cached
        .origin_write(
            "INSERT INTO blocked_assignments (id, dept_id, severity) VALUES (102, 30, 'minor')",
            &[&[("name", "Bob")], &[("name", "Charlie")]],
            Serve::Hit,
        )
        .await?;

    // 'critical' matches the constraint: invalidation.
    cached
        .origin_write(
            "INSERT INTO blocked_assignments (id, dept_id, severity) VALUES (103, 30, 'critical')",
            &[&[("name", "Bob")]],
            Serve::Miss,
        )
        .await?;
    Ok(())
}

// =============================================================================
// Mixed IN, NOT IN, EXISTS — Multiple Decorrelation Types
// =============================================================================

/// Test a query combining IN, NOT IN, and EXISTS decorrelation.
///
/// All three decorrelation types coexist in the same query:
/// - IN → INNER JOIN + DISTINCT (semi-join)
/// - NOT IN → LEFT JOIN + IS NULL (anti-join)
/// - EXISTS → INNER JOIN + DISTINCT (semi-join)
#[tokio::test]
async fn test_correlated_in_not_in_exists_combined() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    seed(
        &mut ctx,
        &[
            "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)",
            "CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT, active INTEGER)",
            "CREATE TABLE projects (id INTEGER PRIMARY KEY, dept_id INTEGER)",
            "CREATE TABLE suspensions (id INTEGER PRIMARY KEY, emp_id INTEGER)",
            "INSERT INTO employees (id, name, dept_id) VALUES \
             (1, 'Alice', 10), (2, 'Bob', 20), (3, 'Charlie', 10), (4, 'Diana', 30)",
            // Dept 10 and 20 are active
            "INSERT INTO departments (id, name, active) VALUES \
             (10, 'Engineering', 1), (20, 'Marketing', 1), (30, 'Sales', 0)",
            // Dept 10 has projects
            "INSERT INTO projects (id, dept_id) VALUES (100, 10)",
            // Bob is suspended
            "INSERT INTO suspensions (id, emp_id) VALUES (200, 2)",
        ],
    )
    .await?;

    // Employees who:
    // 1. Are in an active department (correlated IN: d.id = e.dept_id is both correlation and IN predicate)
    // 2. Are NOT suspended (correlated NOT IN: s.emp_id = e.id is both correlation and NOT IN predicate)
    // 3. Have projects in their department (correlated EXISTS: p.dept_id = e.dept_id)
    // Alice and Charlie qualify; Bob is suspended; Diana's dept is inactive.
    let query = "SELECT e.name FROM employees e \
                 WHERE e.dept_id IN (SELECT d.id FROM departments d WHERE d.id = e.dept_id AND d.active = 1) \
                 AND e.id NOT IN (SELECT s.emp_id FROM suspensions s WHERE s.emp_id = e.id) \
                 AND EXISTS (SELECT 1 FROM projects p WHERE p.dept_id = e.dept_id) \
                 ORDER BY e.name";
    let mut cached = CachedQuery::warm(
        &mut ctx,
        query,
        &[&[("name", "Alice")], &[("name", "Charlie")]],
    )
    .await?;

    // Bob unsuspended, but dept 20 has no projects: still excluded.
    cached
        .origin_write(
            "DELETE FROM suspensions WHERE id = 200",
            &[&[("name", "Alice")], &[("name", "Charlie")]],
            Serve::Miss,
        )
        .await?;

    // A project for dept 20: Bob appears.
    cached
        .origin_write(
            "INSERT INTO projects (id, dept_id) VALUES (101, 20)",
            &[
                &[("name", "Alice")],
                &[("name", "Bob")],
                &[("name", "Charlie")],
            ],
            Serve::Miss,
        )
        .await?;
    Ok(())
}

// =============================================================================
// Scalar Subquery in SELECT List — Aggregate (COUNT)
// =============================================================================

/// Test correlated scalar subquery with aggregate in SELECT list.
///
/// Pattern: SELECT e.name, (SELECT COUNT(*) FROM orders o WHERE o.emp_id = e.id) FROM employees e
///
/// Decorrelated form (transient, for update queries / population branches):
///   SELECT e.name, _dc1._ds1
///   FROM employees e
///   LEFT JOIN (SELECT emp_id, COUNT(*) AS _ds1 FROM orders GROUP BY emp_id) _dc1
///     ON _dc1.emp_id = e.id
///
/// The original (correlated) form is stored for serving — PostgreSQL evaluates
/// COUNT(*) against the cache database, returning 0 (not NULL) for employees
/// with no orders.
///
/// CDC behavior:
/// - INSERT/DELETE on orders → invalidates (count changes)
/// - UPDATE on outer table → applied in-place (Direct source)
#[tokio::test]
async fn test_correlated_scalar_select_count() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    seed(
        &mut ctx,
        &[
            "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, emp_id INTEGER, status TEXT)",
            "INSERT INTO employees (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
            // Alice has 2 orders, Bob has 1, Charlie has 0
            "INSERT INTO orders (id, emp_id, status) VALUES \
             (100, 1, 'shipped'), (101, 1, 'pending'), (102, 2, 'shipped')",
        ],
    )
    .await?;

    let query = "SELECT e.name, \
                     (SELECT count(*) FROM orders o WHERE o.emp_id = e.id) AS order_count \
                 FROM employees e \
                 ORDER BY e.name";
    let mut cached = CachedQuery::warm(
        &mut ctx,
        query,
        &[
            &[("name", "Alice"), ("order_count", "2")],
            &[("name", "Bob"), ("order_count", "1")],
            &[("name", "Charlie"), ("order_count", "0")],
        ],
    )
    .await?;

    // An order for Charlie: his count changes.
    cached
        .origin_write(
            "INSERT INTO orders (id, emp_id, status) VALUES (103, 3, 'pending')",
            &[
                &[("name", "Alice"), ("order_count", "2")],
                &[("name", "Bob"), ("order_count", "1")],
                &[("name", "Charlie"), ("order_count", "1")],
            ],
            Serve::Miss,
        )
        .await?;

    // One of Alice's orders removed: her count changes.
    cached
        .origin_write(
            "DELETE FROM orders WHERE id = 100",
            &[
                &[("name", "Alice"), ("order_count", "1")],
                &[("name", "Bob"), ("order_count", "1")],
                &[("name", "Charlie"), ("order_count", "1")],
            ],
            Serve::Miss,
        )
        .await?;
    Ok(())
}

// =============================================================================
// Scalar Subquery in SELECT List — Non-Aggregate Lookup
// =============================================================================

/// Test correlated scalar subquery without aggregate (direct lookup).
///
/// Pattern: SELECT e.name, (SELECT d.name FROM departments d WHERE d.id = e.dept_id) FROM employees e
///
/// No GROUP BY in the derived table since there's no aggregate.
/// CDC on departments should invalidate.
#[tokio::test]
async fn test_correlated_scalar_select_lookup() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    seed(
        &mut ctx,
        &[
            "CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)",
            "INSERT INTO departments (id, name) VALUES (1, 'Engineering'), (2, 'Marketing')",
            "INSERT INTO employees (id, name, dept_id) VALUES \
             (10, 'Alice', 1), (11, 'Bob', 2)",
        ],
    )
    .await?;

    let query = "SELECT e.name, \
                     (SELECT d.name FROM departments d WHERE d.id = e.dept_id) AS dept_name \
                 FROM employees e \
                 ORDER BY e.name";
    let mut cached = CachedQuery::warm(
        &mut ctx,
        query,
        &[
            &[("name", "Alice"), ("dept_name", "Engineering")],
            &[("name", "Bob"), ("dept_name", "Marketing")],
        ],
    )
    .await?;

    // A rename on the inner table invalidates.
    cached
        .origin_write(
            "UPDATE departments SET name = 'Eng' WHERE id = 1",
            &[
                &[("name", "Alice"), ("dept_name", "Eng")],
                &[("name", "Bob"), ("dept_name", "Marketing")],
            ],
            Serve::Miss,
        )
        .await?;
    Ok(())
}

// =============================================================================
// Scalar Subquery in WHERE Clause — Aggregate Comparison
// =============================================================================

/// Test correlated scalar subquery in WHERE clause (comparison with aggregate).
///
/// Pattern: SELECT ... FROM departments d
///          WHERE d.budget > (SELECT avg(d2.budget) FROM departments d2 WHERE d2.location = d.location)
///
/// Self-join: same table referenced in both inner and outer with different aliases.
/// CDC on departments should invalidate (affects both sides of the comparison).
#[tokio::test]
async fn test_correlated_scalar_where_aggregate() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    seed(
        &mut ctx,
        &[
            "CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT, location TEXT, budget INTEGER)",
            "INSERT INTO departments (id, name, location, budget) VALUES \
             (1, 'Engineering', 'NYC', 100), \
             (2, 'Marketing', 'NYC', 60), \
             (3, 'Sales', 'SF', 90), \
             (4, 'Support', 'SF', 50)",
        ],
    )
    .await?;

    // Departments with budget above their location's average
    // NYC avg = 80: Engineering (100) above, Marketing (60) below
    // SF avg = 70: Sales (90) above, Support (50) below
    let query = "SELECT d.name FROM departments d \
                 WHERE d.budget > (SELECT avg(d2.budget) FROM departments d2 WHERE d2.location = d.location) \
                 ORDER BY d.name";
    let mut cached = CachedQuery::warm(
        &mut ctx,
        query,
        &[&[("name", "Engineering")], &[("name", "Sales")]],
    )
    .await?;

    // Marketing's budget raised to 120: NYC avg becomes 110, so Engineering
    // (100) drops below and Marketing rises above.
    cached
        .origin_write(
            "UPDATE departments SET budget = 120 WHERE id = 2",
            &[&[("name", "Marketing")], &[("name", "Sales")]],
            Serve::Miss,
        )
        .await?;
    Ok(())
}

// =============================================================================
// Scalar Subquery in WHERE Clause — Non-Aggregate Lookup
// =============================================================================

/// Test correlated scalar subquery in WHERE clause without aggregate.
///
/// Pattern: SELECT ... FROM employees e
///          WHERE e.name = (SELECT d.name FROM departments d WHERE d.id = e.dept_id)
///
/// Non-aggregate lookup in WHERE: finds employees whose name matches
/// their department name.
#[tokio::test]
async fn test_correlated_scalar_where_lookup() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    seed(
        &mut ctx,
        &[
            "CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)",
            "INSERT INTO departments (id, name) VALUES (1, 'Alice'), (2, 'Marketing')",
            // Alice's name matches her department; Bob's does not
            "INSERT INTO employees (id, name, dept_id) VALUES \
             (10, 'Alice', 1), (11, 'Bob', 2)",
        ],
    )
    .await?;

    let query = "SELECT e.name FROM employees e \
                 WHERE e.name = (SELECT d.name FROM departments d WHERE d.id = e.dept_id) \
                 ORDER BY e.name";
    let mut cached = CachedQuery::warm(&mut ctx, query, &[&[("name", "Alice")]]).await?;

    // Department 2 renamed to 'Bob': Bob now matches.
    cached
        .origin_write(
            "UPDATE departments SET name = 'Bob' WHERE id = 2",
            &[&[("name", "Alice")], &[("name", "Bob")]],
            Serve::Miss,
        )
        .await?;
    Ok(())
}

// =============================================================================
// Scalar Subquery — Mixed with EXISTS
// =============================================================================

/// Test a query with both a scalar subquery in SELECT and a correlated EXISTS in WHERE.
///
/// Both decorrelation types coexist in the same query: scalar → LEFT JOIN with
/// derived table, EXISTS → INNER JOIN + DISTINCT.
#[tokio::test]
async fn test_correlated_scalar_and_exists_combined() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    seed(
        &mut ctx,
        &[
            "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)",
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, emp_id INTEGER)",
            "CREATE TABLE projects (id INTEGER PRIMARY KEY, dept_id INTEGER)",
            "INSERT INTO employees (id, name, dept_id) VALUES \
             (1, 'Alice', 10), (2, 'Bob', 20), (3, 'Charlie', 10)",
            // Alice has 2 orders, Charlie has 1, Bob has 0
            "INSERT INTO orders (id, emp_id) VALUES (100, 1), (101, 1), (102, 3)",
            // Dept 10 has a project, dept 20 does not
            "INSERT INTO projects (id, dept_id) VALUES (200, 10)",
        ],
    )
    .await?;

    // Employees in departments that have projects, with their order count:
    // dept 10 has Alice and Charlie; Bob's dept 20 has no projects.
    let query = "SELECT e.name, \
                     (SELECT count(*) FROM orders o WHERE o.emp_id = e.id) AS order_count \
                 FROM employees e \
                 WHERE EXISTS (SELECT 1 FROM projects p WHERE p.dept_id = e.dept_id) \
                 ORDER BY e.name";
    let mut cached = CachedQuery::warm(
        &mut ctx,
        query,
        &[
            &[("name", "Alice"), ("order_count", "2")],
            &[("name", "Charlie"), ("order_count", "1")],
        ],
    )
    .await?;

    // A project for dept 20: Bob appears.
    cached
        .origin_write(
            "INSERT INTO projects (id, dept_id) VALUES (201, 20)",
            &[
                &[("name", "Alice"), ("order_count", "2")],
                &[("name", "Bob"), ("order_count", "0")],
                &[("name", "Charlie"), ("order_count", "1")],
            ],
            Serve::Miss,
        )
        .await?;
    Ok(())
}

/// Kind a nodes without a kind b child, by a self-correlated NOT EXISTS: the
/// table is referenced in FROM and in the subquery, and an insert relevant to
/// either reference alone must reach the result.
#[tokio::test]
async fn test_correlated_not_exists_self_reference_cdc_insert() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    seed(
        &mut ctx,
        &[
            "CREATE TABLE leaf_nodes (id INTEGER PRIMARY KEY, parent_id INTEGER, kind TEXT)",
            "INSERT INTO leaf_nodes VALUES (1, NULL, 'a'), (2, 1, 'a'), (3, 1, 'b')",
        ],
    )
    .await?;

    let query = "SELECT e.id FROM leaf_nodes e WHERE e.kind = 'a' AND NOT EXISTS \
                 (SELECT 1 FROM leaf_nodes n WHERE n.parent_id = e.id AND n.kind = 'b') ORDER BY e.id";
    // Node 2 is the one kind a node without a kind b child.
    CachedQuery::warm(&mut ctx, query, &[&[("id", "2")]]).await?;

    // The inserted node reaches the result.
    ctx.query("INSERT INTO leaf_nodes VALUES (4, NULL, 'a')", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    rows_expect(&mut ctx, query, &[&[("id", "2")], &[("id", "4")]]).await?;

    // A kind b child removes node 2.
    ctx.query("INSERT INTO leaf_nodes VALUES (5, 2, 'b')", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    rows_expect(&mut ctx, query, &[&[("id", "4")]]).await?;
    Ok(())
}
