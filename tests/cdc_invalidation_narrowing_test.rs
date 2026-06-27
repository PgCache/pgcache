//! Correctness guards for ADR-045 (candidate-narrowed CDC invalidation).
//!
//! ADR-045 will narrow `update_queries_check_invalidate` / memo eviction from a
//! full per-relation scan to a per-operation narrowed set
//! (`candidates ∪ carve-outs`). A narrowing that drops a query that *should*
//! invalidate would leave it Ready with stale cached rows — a stale read. Each
//! test caches MULTIPLE queries on one relation, performs a write that must
//! change one query's result, and asserts that query is invalidated (next read
//! is a miss) and re-serves FRESH rows, while unrelated queries stay cached.
//! These pass on the current full-scan code and must keep passing after
//! narrowing; a single-query test would not catch a narrowing miss.
//!
//! One case per carve-out branch in ADR-045's correctness argument:
//! - subquery source, UPDATE (the `always_check` set)
//! - predicate-column change that makes a row leave a LIMIT window
//!   (the `limit_predicate_columns` expansion)
//! - DELETE on a LIMIT query (the `has_limit_fromclause` set)
//! - multi-table join INSERT that grows a result (`candidates(new)`)
//! - unrelated single-table UPDATE that must stay cached + correct (the skip)

use std::io::Error;
use std::time::Duration;

use crate::util::{TestContext, assert_cache_hit, assert_cache_miss, assert_row_at};

mod util;

/// Issue `sql` repeatedly until it is served from the in-process memo (a
/// `memo_hits` increment), so a later write must evict that memo to stay fresh.
async fn warm_until_memoized(ctx: &mut TestContext, sql: &str) -> Result<(), Error> {
    for _ in 0..80 {
        ctx.simple_query(sql).await?;
        if ctx.metrics().await?.cache_memo_hits >= 1 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Err(Error::other(format!("query never served from memo: {sql}")))
}

/// `always_check` carve-out: an UPDATE on a subquery table must invalidate the
/// subquery query even when the changed row no longer matches the subquery's
/// own predicate (so it is NOT in `candidates(new)`), while single-table queries
/// on that same table stay cached.
#[tokio::test]
async fn test_narrowing_subquery_update_shrinks_amid_single_table() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(
        "CREATE TABLE customers (id INT PRIMARY KEY, name TEXT)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO customers (id, name) VALUES (1,'alice'),(2,'bob'),(3,'carol')",
        &[],
    )
    .await?;
    // Customers 1 and 2 have an order with amount >= 100; customer 3 does not.
    ctx.query(
        "INSERT INTO orders (id, customer_id, amount) VALUES (10,1,200),(11,2,150),(12,3,80)",
        &[],
    )
    .await?;

    let q_sub = "SELECT name FROM customers \
                 WHERE id IN (SELECT customer_id FROM orders WHERE amount >= 100) ORDER BY name";
    let q_o3 = "SELECT amount FROM orders WHERE customer_id = 3";

    // Warm both into cache.
    ctx.simple_query(q_sub).await?;
    ctx.simple_query(q_o3).await?;
    ctx.cache_settle().await?;

    // Order 10 (customer 1) drops below the threshold → customer 1 leaves the IN
    // set. The new row (amount 50) does NOT match `amount >= 100`, so it is not a
    // candidate; only the unconditional subquery invalidation catches it.
    ctx.origin_query("UPDATE orders SET amount = 50 WHERE id = 10", &[])
        .await?;
    ctx.cdc_settle().await?;

    // Subquery query must be invalidated and re-serve the shrunk result [bob].
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(q_sub).await?;
    assert_row_at(&res, 1, &[("name", "bob")])?;
    assert_eq!(
        res.len(),
        3,
        "expected exactly 1 data row (bob), got {res:?}"
    );
    assert_cache_miss(&mut ctx, m).await?;

    // Unrelated single-table query on the same table stays cached + correct.
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(q_o3).await?;
    assert_row_at(&res, 1, &[("amount", "80")])?;
    assert_cache_hit(&mut ctx, m).await?;
    Ok(())
}

/// `limit_predicate_columns` carve-out: an UPDATE that changes a PREDICATE
/// column so a row leaves a `WHERE owner = K ORDER BY ... LIMIT` window must
/// invalidate owner K's page (the row's new owner makes it not a candidate for
/// K) and surface the replacement row that was outside the cached window. Other
/// owners' pages are untouched.
#[tokio::test]
async fn test_narrowing_predicate_change_leaves_limit_window() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(
        "CREATE TABLE posts (id INT PRIMARY KEY, owner INT, score INT)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO posts (id, owner, score) VALUES \
         (1,10,50),(2,10,40),(3,10,30),(4,10,20), \
         (5,20,55),(6,20,45),(7,20,35)",
        &[],
    )
    .await?;
    let page = |k: i32| {
        format!("SELECT id, score FROM posts WHERE owner = {k} ORDER BY score DESC LIMIT 2")
    };

    ctx.simple_query(&page(10)).await?;
    ctx.simple_query(&page(20)).await?;
    ctx.cache_settle().await?;

    // Move post 1 (owner 10's cached top row) to a brand-new owner 999. Owner
    // 10's window must drop it and surface post 3 (score 30), outside the cached
    // top-2 → must repopulate.
    ctx.origin_query("UPDATE posts SET owner = 999 WHERE id = 1", &[])
        .await?;
    ctx.cdc_settle().await?;

    let m = ctx.metrics().await?;
    let res = ctx.simple_query(&page(10)).await?;
    assert_row_at(&res, 1, &[("id", "2"), ("score", "40")])?;
    assert_row_at(&res, 2, &[("id", "3"), ("score", "30")])?;
    assert_cache_miss(&mut ctx, m).await?;

    // Owner 20's data stays correct. (Today an `owner` change conservatively
    // flags every owner page, since `predicate_changed` is a per-row signal;
    // the narrowing's `limit_predicate_columns` expansion preserves that, so
    // this asserts correctness rather than a specific hit/miss.)
    let res = ctx.simple_query(&page(20)).await?;
    assert_row_at(&res, 1, &[("id", "5"), ("score", "55")])?;
    assert_row_at(&res, 2, &[("id", "6"), ("score", "45")])?;
    Ok(())
}

/// `has_limit_fromclause` carve-out: a DELETE must invalidate `has_limit`
/// queries unconditionally (a delete can drop a row from any LIMIT window whose
/// replacement is uncached). Other owners stay cached.
#[tokio::test]
async fn test_narrowing_delete_invalidates_limit_query() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(
        "CREATE TABLE posts (id INT PRIMARY KEY, owner INT, score INT)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO posts (id, owner, score) VALUES \
         (1,10,50),(2,10,40),(3,10,30),(4,10,20), \
         (5,20,55),(6,20,45)",
        &[],
    )
    .await?;
    let page = |k: i32| {
        format!("SELECT id, score FROM posts WHERE owner = {k} ORDER BY score DESC LIMIT 2")
    };

    ctx.simple_query(&page(10)).await?;
    ctx.simple_query(&page(20)).await?;
    ctx.cache_settle().await?;

    // Delete owner 10's top row. The window must drop post 1 and surface post 3
    // (score 30), outside the cached top-2.
    ctx.origin_query("DELETE FROM posts WHERE id = 1", &[])
        .await?;
    ctx.cdc_settle().await?;

    let m = ctx.metrics().await?;
    let res = ctx.simple_query(&page(10)).await?;
    assert_row_at(&res, 1, &[("id", "2"), ("score", "40")])?;
    assert_row_at(&res, 2, &[("id", "3"), ("score", "30")])?;
    assert_cache_miss(&mut ctx, m).await?;

    // Owner 20's data stays correct. (Today a DELETE conservatively flags every
    // has_limit query on the relation regardless of whether the deleted row was
    // in its window; the `has_limit_fromclause` carve-out preserves that.)
    let res = ctx.simple_query(&page(20)).await?;
    assert_row_at(&res, 1, &[("id", "5"), ("score", "55")])?;
    assert_row_at(&res, 2, &[("id", "6"), ("score", "45")])?;
    Ok(())
}

/// `candidates(new)` for multi-table FromClause: an INSERT that grows a join
/// result must invalidate the affected join query while a different-tag join
/// query stays cached. Models the demo tag page (`post_tags ⋈ posts`).
#[tokio::test]
async fn test_narrowing_join_insert_grows_result() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query("CREATE TABLE posts (id INT PRIMARY KEY, title TEXT)", &[])
        .await?;
    ctx.query(
        "CREATE TABLE post_tags (post_id INT, tag TEXT, PRIMARY KEY (post_id, tag))",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO posts (id, title) VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO post_tags (post_id, tag) VALUES (1,'rust'),(2,'rust'),(3,'go')",
        &[],
    )
    .await?;
    let tag_page = |t: &str| {
        format!(
            "SELECT p.id, p.title FROM post_tags pt JOIN posts p ON p.id = pt.post_id \
             WHERE pt.tag = '{t}' ORDER BY p.id DESC LIMIT 5"
        )
    };

    ctx.simple_query(&tag_page("rust")).await?;
    ctx.simple_query(&tag_page("go")).await?;
    ctx.cache_settle().await?;

    // Tag post 4 'rust' → the 'rust' page grows.
    ctx.origin_query(
        "INSERT INTO post_tags (post_id, tag) VALUES (4,'rust')",
        &[],
    )
    .await?;
    ctx.cdc_settle().await?;

    let m = ctx.metrics().await?;
    let res = ctx.simple_query(&tag_page("rust")).await?;
    assert_row_at(&res, 1, &[("id", "4"), ("title", "d")])?;
    assert_row_at(&res, 2, &[("id", "2"), ("title", "b")])?;
    assert_row_at(&res, 3, &[("id", "1"), ("title", "a")])?;
    assert_cache_miss(&mut ctx, m).await?;

    let m = ctx.metrics().await?;
    let res = ctx.simple_query(&tag_page("go")).await?;
    assert_row_at(&res, 1, &[("id", "3"), ("title", "c")])?;
    assert_eq!(res.len(), 3, "go page should still have exactly 1 row");
    assert_cache_hit(&mut ctx, m).await?;
    Ok(())
}

/// The skip case: a single-table non-window UPDATE applies in place — the
/// touched query stays cached (a HIT, not a miss) and reflects the new value,
/// and unrelated queries are untouched. Guards against narrowing accidentally
/// invalidating (or failing to in-place-apply) the bulk single-table queries.
#[tokio::test]
async fn test_narrowing_unrelated_single_table_update_stays_cached() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query(
        "CREATE TABLE items (id INT PRIMARY KEY, owner INT, label TEXT)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO items (id, owner, label) VALUES (1,10,'x'),(2,20,'y'),(3,30,'z')",
        &[],
    )
    .await?;
    let by_owner = |k: i32| format!("SELECT id, label FROM items WHERE owner = {k}");

    ctx.simple_query(&by_owner(10)).await?;
    ctx.simple_query(&by_owner(20)).await?;
    ctx.cache_settle().await?;

    // In-place label update on owner 10's row (non-predicate, non-window).
    ctx.origin_query("UPDATE items SET label = 'X2' WHERE id = 1", &[])
        .await?;
    ctx.cdc_settle().await?;

    // Owner 10 reflects the new label (applied in place) and stays a cache hit.
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(&by_owner(10)).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("label", "X2")])?;
    assert_cache_hit(&mut ctx, m).await?;

    // Unrelated owner untouched.
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(&by_owner(20)).await?;
    assert_row_at(&res, 1, &[("id", "2"), ("label", "y")])?;
    assert_cache_hit(&mut ctx, m).await?;
    Ok(())
}

/// Memo symmetry (the part the invalidation-shaped narrowing would miss): a
/// memoized non-limit single-table query whose row LEAVES via a predicate
/// change must drop the stale memo. The row's new owner makes it not a
/// `candidates(new)` member, so only the symmetric `candidates(old-image)` probe
/// (ADR-045) catches it — invalidation's asymmetric set would not.
#[tokio::test]
async fn test_narrowing_memo_evicted_on_predicate_leave_shrink() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.query("CREATE TABLE items (id INT PRIMARY KEY, owner INT)", &[])
        .await?;
    ctx.query(
        "INSERT INTO items (id, owner) VALUES (1,10),(2,10),(3,20)",
        &[],
    )
    .await?;
    let q = "SELECT id FROM items WHERE owner = 10 ORDER BY id";

    ctx.simple_query(q).await?;
    ctx.cache_settle().await?;
    warm_until_memoized(&mut ctx, q).await?;

    // Move id=1 out of owner 10 (predicate change → the row leaves owner 10's
    // result). owner 10's query is not a candidate for the new owner (20).
    ctx.origin_query("UPDATE items SET owner = 20 WHERE id = 1", &[])
        .await?;
    ctx.cdc_settle().await?;

    // Must serve the fresh shrunk result [2], not the stale memo [1, 2].
    let res = ctx.simple_query(q).await?;
    assert_row_at(&res, 1, &[("id", "2")])?;
    assert_eq!(res.len(), 3, "expected exactly 1 row (id=2), got {res:?}");
    Ok(())
}
