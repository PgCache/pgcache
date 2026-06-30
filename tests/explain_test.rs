use std::io::Error;

use tokio_postgres::SimpleQueryMessage;

use crate::util::{TestContext, assert_cache_miss, http_get};

mod util;

/// Collect the `QUERY PLAN` column of every data row into one string. A
/// successful `pgcache_explain` response is a single `QUERY PLAN` column, so this
/// reassembles the plan (or the single status line) for substring assertions.
fn query_plan_text(messages: &[SimpleQueryMessage]) -> String {
    messages
        .iter()
        .filter_map(|message| {
            if let SimpleQueryMessage::Row(row) = message {
                row.get::<&str>("QUERY PLAN").map(str::to_owned)
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Warm a single-table query into the cache, then `pgcache_explain('<sql>')`
/// returns a `QUERY PLAN` for the cache-side execution. The call succeeding at
/// all proves the proxy intercepted it: an un-intercepted call would forward to
/// origin, which has no such function and would error.
#[tokio::test]
async fn test_pgcache_explain_source_row_returns_plan() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table explain_items (id integer primary key, name text)",
        &[],
    )
    .await?;
    ctx.query(
        "insert into explain_items (id, name) values (1, 'widget'), (2, 'gadget')",
        &[],
    )
    .await?;

    let query = "select id, name from explain_items where name = 'widget'";
    let m = ctx.metrics().await?;
    ctx.simple_query(query).await?;
    assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    // Dollar-quote the argument: the query itself contains single quotes
    // (`'widget'`), so a plain single-quoted literal would need them doubled.
    let res = ctx
        .simple_query(&format!("select pgcache_explain($q${query}$q$)"))
        .await?;

    let row_count = res
        .iter()
        .filter(|message| matches!(message, SimpleQueryMessage::Row(_)))
        .count();
    // A default-format EXPLAIN returns one row per plan line; the WHERE clause
    // forces a Filter line under the scan, so a correct capture is multi-row.
    // Guards against the codec batching all DataRow messages into one frame and
    // only the first line surviving (PGC-345 review).
    assert!(
        row_count >= 2,
        "expected a multi-line plan, got {row_count} row(s): {res:?}"
    );

    let plan = query_plan_text(&res);
    // A real EXPLAIN plan (cost estimates) against the cached relation — not an
    // origin error and not the "not cached" status line.
    assert!(plan.contains("cost="), "not an EXPLAIN plan: {plan}");
    assert!(
        plan.contains("explain_items"),
        "plan does not scan the cached relation: {plan}"
    );
    assert!(
        plan.contains("Filter:"),
        "plan is missing the Filter line — later rows were dropped: {plan}"
    );

    Ok(())
}

/// The fingerprint form (`pgcache_explain('<fingerprint>')`, as printed by
/// `/status`) resolves the same cached query and returns its plan.
#[tokio::test]
async fn test_pgcache_explain_by_fingerprint_returns_plan() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table explain_fp (id integer primary key, name text)",
        &[],
    )
    .await?;
    ctx.query("insert into explain_fp (id, name) values (1, 'a')", &[])
        .await?;

    let query = "select id, name from explain_fp where name = 'a'";
    let m = ctx.metrics().await?;
    ctx.simple_query(query).await?;
    assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    let (_status, body) = http_get(ctx.metrics_port, "/status").await?;
    let json: serde_json::Value = serde_json::from_str(&body).map_err(Error::other)?;
    let fingerprint = json["queries"][0]["fingerprint"]
        .as_u64()
        .expect("status reports the warmed query's fingerprint");

    let res = ctx
        .simple_query(&format!("select pgcache_explain('{fingerprint}')"))
        .await?;

    let plan = query_plan_text(&res);
    assert!(
        plan.contains("cost=") && plan.contains("explain_fp"),
        "fingerprint-mode explain did not return the cached plan: {plan}"
    );

    Ok(())
}

/// A query that was never cached reports a status line rather than a plan, and
/// is not forwarded to origin.
#[tokio::test]
async fn test_pgcache_explain_uncached_reports_status() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table explain_uncached (id integer primary key, name text)",
        &[],
    )
    .await?;

    // Never warmed, so this fingerprint is absent from the cache.
    let res = ctx
        .simple_query("select pgcache_explain('select id from explain_uncached where id = 999')")
        .await?;

    let text = query_plan_text(&res);
    assert!(
        text.contains("not cached"),
        "expected a not-cached status line, got: {text}"
    );

    Ok(())
}
