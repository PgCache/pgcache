//! Cache hits must not generate per-query traffic to origin.
//!
//! Regression test for PGC-195. `test_cache_hits_do_not_forward_to_origin`
//! is the headline assertion; the smaller variants below bisect the trigger.

use std::io::Error;
use std::sync::Arc;

use crate::util::TestContext;

mod util;

const Q1: &str = "SELECT val FROM t WHERE id = $1";
const Q2: &str = "SELECT count(*) FROM t WHERE id <= $1";
const Q3: &str = "SELECT id, val FROM t WHERE id = $1";
const Q4: &str = "SELECT id FROM t WHERE id < $1 ORDER BY id DESC LIMIT 5";

async fn setup_table(ctx: &mut TestContext) -> Result<(), Error> {
    ctx.query("create table t (id integer primary key, val text)", &[])
        .await?;
    ctx.query(
        "insert into t select i, md5(i::text) from generate_series(1, 100) i",
        &[],
    )
    .await?;
    Ok(())
}

async fn run_queries(
    client: &tokio_postgres::Client,
    pages: i32,
    sqls: &[&str],
) -> Result<(), Error> {
    for i in 0..pages {
        let id = i % 100;
        for sql in sqls {
            client.query(*sql, &[&id]).await.map_err(Error::other)?;
        }
    }
    Ok(())
}

async fn origin_xact_commit(ctx: &mut TestContext) -> Result<i64, Error> {
    Ok(ctx
        .origin
        .query_one(
            "SELECT xact_commit FROM pg_stat_database WHERE datname = current_database()",
            &[],
        )
        .await
        .map_err(Error::other)?
        .get(0))
}

/// Run a workload and report (cache_hits, cache_misses, origin_commits).
async fn measure(
    ctx: &mut TestContext,
    concurrency: usize,
    pages_per_worker: i32,
    sqls: &[&str],
) -> Result<(u64, u64, i64), Error> {
    setup_table(ctx).await?;

    // Warmup: 1 client, populate all fingerprints.
    {
        let client = ctx.proxy_client_connect().await?;
        run_queries(&client, pages_per_worker, sqls).await?;
    }
    ctx.cache_settle().await?;

    ctx.origin
        .execute("SELECT pg_stat_reset()", &[])
        .await
        .map_err(Error::other)?;

    let m_pre = ctx.metrics().await?;
    let commit_pre = origin_xact_commit(ctx).await?;

    let sqls_owned: Arc<Vec<String>> = Arc::new(sqls.iter().map(|s| s.to_string()).collect());
    let mut handles = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let client = Arc::new(ctx.proxy_client_connect().await?);
        let sqls_owned = Arc::clone(&sqls_owned);
        handles.push(tokio::spawn(async move {
            let sqls_ref: Vec<&str> = sqls_owned.iter().map(String::as_str).collect();
            run_queries(&client, pages_per_worker, &sqls_ref).await
        }));
    }
    for h in handles {
        h.await.map_err(Error::other)??;
    }

    let m_post = ctx.metrics().await?;
    let commit_post = origin_xact_commit(ctx).await?;

    Ok((
        m_post.queries_cache_hit - m_pre.queries_cache_hit,
        m_post.queries_cache_miss - m_pre.queries_cache_miss,
        commit_post - commit_pre,
    ))
}

/// Concurrent multi-SQL via `client.query(&str, ...)` (re-prepared each call).
/// Before the fix this produced one origin commit per query (1:1); the fix
/// must drive that to near zero.
#[tokio::test]
async fn test_cache_hits_do_not_forward_to_origin() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    let (hits, misses, commits) = measure(&mut ctx, 8, 100, &[Q1, Q2, Q3, Q4]).await?;
    let total = 8i64 * 100 * 4;
    eprintln!("hits={hits} misses={misses} commits={commits} total={total}");
    assert!(
        hits >= u64::try_from(total).expect("non-negative total") * 95 / 100,
        "expected ≥95% cache hits, got {hits}/{total} (misses={misses})"
    );
    // 1:1 is the bug; the fix produces ~5 commits per (conn × distinct SQL).
    // Allow a generous ceiling for background activity, well below 1:1.
    assert!(
        commits < 200,
        "origin saw {commits} commits for {hits} cache hits — PGC-195 regression"
    );
    Ok(())
}

/// Same workload, single client and single SQL — the simplest reproduction
/// of the pattern. Asserts the same near-zero commit invariant.
#[tokio::test]
async fn test_cache_hits_seq_single_sql() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    let (hits, misses, commits) = measure(&mut ctx, 1, 400, &[Q1]).await?;
    eprintln!("[seq+single] hits={hits} misses={misses} commits={commits}");
    assert_eq!(misses, 0, "expected 0 cache misses");
    assert!(
        commits < 50,
        "origin saw {commits} commits — PGC-195 regression"
    );
    Ok(())
}
