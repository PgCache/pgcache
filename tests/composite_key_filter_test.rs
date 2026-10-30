//! Composite-key deleted-key filters (PGC-564): while a population is in
//! flight the writer records the primary keys CDC removes and the merge
//! excludes them from every chunk. For a row-valued key the exclusion used to
//! render as `(c1,c2) NOT IN ((…),(…),…)`, which PostgreSQL expands into one
//! row comparison per tuple chained by OR, so a few thousand recorded deletes
//! exceeded `max_stack_depth` and the chunk — and with it the population —
//! failed. The filter now renders as an anti-join against a VALUES list,
//! which has no per-tuple depth.
//!
//! Fault-dependent — gated like `population_merge_chunk_test.rs`.
#![cfg(feature = "fault-injection")]

use std::io::Error;
use std::time::{Duration, Instant};

use tokio_postgres::SimpleQueryMessage;

use crate::util::{TestContext, http_get};

mod util;

/// Heap blocks per chunk; the rows below pack ~100 per block, so the table
/// spans ~100 blocks → ~50 chunks.
const CHUNK_BLOCKS: &str = "2";
/// Delay after each chunk; with `POPULATED_ROWS` this stretches the merge to
/// over ten seconds, long enough for the bulk delete to stream in.
const CHUNK_DELAY_MS: &str = "250";
const POPULATED_ROWS: u64 = 10_000;
/// Keys removed while the merge runs: well past the few thousand at which the
/// `NOT IN` tuple list exceeded the default 2 MB `max_stack_depth`.
const DELETED_ROWS: u64 = 8_000;

fn first_cell(msgs: &[SimpleQueryMessage]) -> Option<String> {
    msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
        SimpleQueryMessage::CommandComplete(_) | SimpleQueryMessage::RowDescription(_) | _ => None,
    })
}

async fn merge_chunks_total(metrics_port: u16) -> Result<f64, Error> {
    let (status, body) = http_get(metrics_port, "/metrics").await?;
    if status != 200 {
        return Err(Error::other(format!("/metrics returned {status}")));
    }
    Ok(body
        .lines()
        .find_map(|l| l.strip_prefix("pgcache_cache_merge_chunks_total "))
        .and_then(|v| v.trim().parse::<f64>().ok())
        .unwrap_or(0.0))
}

async fn merge_in_progress_wait(ctx: &TestContext) -> Result<(), Error> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if merge_chunks_total(ctx.metrics_port).await? > 0.0 {
            return Ok(());
        }
        assert!(Instant::now() < deadline, "populating merge never started");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Thousands of rows of a composite-key relation deleted at origin while its
/// population's merge is under way: every later chunk must exclude them, the
/// merge must complete, and the query must serve the post-delete count from
/// the cache.
#[tokio::test]
async fn test_composite_key_merge_excludes_thousands_of_deleted_keys() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&[
        ("PGCACHE_FAULT_MERGE_CHUNK_BLOCKS", CHUNK_BLOCKS),
        ("PGCACHE_FAULT_MERGE_CHUNK_DELAY_MS", CHUNK_DELAY_MS),
    ])
    .await?;
    ctx.simple_query("create table post_tags (post_id int, tag text, primary key (post_id, tag))")
        .await?;
    ctx.simple_query(&format!(
        "insert into post_tags (post_id, tag) select i, 't' || i from generate_series(1, {POPULATED_ROWS}) i"
    ))
    .await?;
    ctx.cdc_decode_settle().await?;

    let query = "select count(*) from post_tags where post_id > 0";
    ctx.simple_query(query).await?;
    merge_in_progress_wait(&ctx).await?;

    ctx.origin
        .batch_execute(&format!(
            "delete from post_tags where post_id <= {DELETED_ROWS}"
        ))
        .await
        .map_err(Error::other)?;

    ctx.cache_settle_with_timeout(Duration::from_secs(60))
        .await?;
    ctx.cdc_apply_settle().await?;
    let before = ctx.metrics().await?;
    assert!(
        before.cache_merge_applied >= 1,
        "the merge never completed: {before:?}"
    );
    let count = first_cell(&ctx.simple_query(query).await?).unwrap_or_default();
    let after = ctx.metrics_after_outcomes(&before, 1).await?;
    assert_eq!(
        after.queries_cache_hit - before.queries_cache_hit,
        1,
        "query was not served from the cache after the merge"
    );
    assert_eq!(
        count,
        (POPULATED_ROWS - DELETED_ROWS).to_string(),
        "rows deleted mid-merge were reinserted or the merge dropped live rows"
    );
    Ok(())
}
