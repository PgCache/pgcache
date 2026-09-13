//! Chunked population merges (PGC-418): a population's staging→cache merge
//! used to run as one statement over the whole staging table on the writer
//! thread, so no CDC frame applied until it returned — every Ready query over
//! the relation served pre-write state for the duration (12 s per 1M rows in
//! the external 16M-row benchmark). The merge now drains staging in bounded
//! chunks and the writer loop interleaves CDC frames between them.
//!
//! Construction: fault-injection pins a small chunk window and adds a per-chunk
//! delay, so a modest population spans a merge of several seconds. A Ready
//! bystander query over the same relation must see an origin insert within a
//! fraction of that while the populating query is still Loading; a delete
//! that lands between chunks must not be resurrected by a later chunk.
//!
//! Fault-dependent — gated like `population_merge_gate_test.rs`.
#![cfg(feature = "fault-injection")]

use std::io::Error;
use std::time::{Duration, Instant};

use tokio_postgres::SimpleQueryMessage;

use crate::util::{TestContext, http_get};

/// Discard chunks applied (staging of an abandoned/superseded population).
async fn discard_chunks_total(metrics_port: u16) -> Result<f64, Error> {
    let (status, body) = http_get(metrics_port, "/metrics").await?;
    if status != 200 {
        return Err(Error::other(format!("/metrics returned {status}")));
    }
    Ok(body
        .lines()
        .find_map(|l| l.strip_prefix("pgcache_cache_merge_discard_chunks_total "))
        .and_then(|v| v.trim().parse::<f64>().ok())
        .unwrap_or(0.0))
}

mod util;

/// Heap blocks per chunk under test. The 12-byte rows below pack ~200 per
/// block, so `POPULATED_ROWS` spans ~50 blocks → ~25 chunks.
const CHUNK_BLOCKS: &str = "2";
/// Delay after each chunk; with `POPULATED_ROWS` this stretches the merge to
/// several seconds.
const CHUNK_DELAY_MS: &str = "250";
const POPULATED_ROWS: u64 = 10_000;

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

async fn any_query_loading(metrics_port: u16) -> Result<bool, Error> {
    let (_, body) = http_get(metrics_port, "/status").await?;
    let json: serde_json::Value = serde_json::from_str(&body).map_err(Error::other)?;
    Ok(json
        .get("queries")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|qs| {
            qs.iter()
                .any(|q| q.get("state").and_then(serde_json::Value::as_str) == Some("Loading"))
        }))
}

async fn setup() -> Result<TestContext, Error> {
    let mut ctx = TestContext::setup_fault(&[
        ("PGCACHE_FAULT_MERGE_CHUNK_BLOCKS", CHUNK_BLOCKS),
        ("PGCACHE_FAULT_MERGE_CHUNK_DELAY_MS", CHUNK_DELAY_MS),
    ])
    .await?;
    ctx.simple_query("create table chunked (id int primary key, grp int not null, v int not null)")
        .await?;
    ctx.simple_query("insert into chunked (id, grp, v) values (1, 1, 1)")
        .await?;
    ctx.simple_query(&format!(
        "insert into chunked (id, grp, v) select i, 2, i from generate_series(1000, {}) i",
        999 + POPULATED_ROWS
    ))
    .await?;
    ctx.cdc_decode_settle().await?;
    Ok(ctx)
}

/// Wait until the populating query's merge is under way: its first chunk has
/// landed (the bystander's own one-chunk merge is `baseline`).
async fn merge_in_progress_wait(ctx: &TestContext, baseline: f64) -> Result<(), Error> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if merge_chunks_total(ctx.metrics_port).await? > baseline {
            return Ok(());
        }
        assert!(Instant::now() < deadline, "populating merge never started");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn test_cdc_applies_between_merge_chunks() -> Result<(), Error> {
    let mut ctx = setup().await?;

    // Bystander: Ready over grp 1 (its own merge is a single chunk).
    let q1 = "select id, v from chunked where grp = 1 order by id";
    ctx.simple_query(q1).await?;
    ctx.cache_settle_with_timeout(Duration::from_secs(15))
        .await?;
    let baseline = merge_chunks_total(ctx.metrics_port).await?;

    // Populating query: 10k rows → ~25 chunks × ≥250 ms ≈ 6 s of merge.
    let q2 = "select count(*) from chunked where grp = 2";
    ctx.simple_query(q2).await?;
    merge_in_progress_wait(&ctx, baseline).await?;

    // An origin write to the bystander's group while the merge is in progress.
    let written_at = Instant::now();
    ctx.origin
        .batch_execute("insert into chunked (id, grp, v) values (2, 1, 2)")
        .await
        .map_err(Error::other)?;

    // The bystander must serve it (from cache) long before the merge ends,
    // while the populating query is still Loading — the CDC frame applied
    // between chunks rather than after the whole merge.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let before = ctx.metrics().await?;
        let served = ctx.simple_query(q1).await?;
        let after = ctx.metrics().await?;
        let ids: Vec<String> = served
            .iter()
            .filter_map(|m| match m {
                SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
                SimpleQueryMessage::CommandComplete(_)
                | SimpleQueryMessage::RowDescription(_)
                | _ => None,
            })
            .collect();
        if ids.iter().any(|id| id == "2") {
            let seen_after = written_at.elapsed();
            assert_eq!(
                after.queries_cache_hit - before.queries_cache_hit,
                1,
                "the bystander read was not a cache hit; it no longer exercises apply-during-merge"
            );
            assert!(
                any_query_loading(ctx.metrics_port).await?,
                "populating query already Ready when the insert became visible ({seen_after:?}); \
                 the merge did not span the write, widen POPULATED_ROWS / CHUNK_DELAY_MS"
            );
            assert!(
                seen_after < Duration::from_millis(2_000),
                "insert took {seen_after:?} to reach the bystander: CDC apply waited on the merge"
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "insert never reached the bystander (CDC apply stalled behind the merge)"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // The populating query completes normally with the full row count, served
    // from cache.
    ctx.cache_settle_with_timeout(Duration::from_secs(30))
        .await?;
    let before = ctx.metrics().await?;
    let count = first_cell(&ctx.simple_query(q2).await?).unwrap_or_default();
    let after = ctx.metrics().await?;
    assert_eq!(count, POPULATED_ROWS.to_string());
    assert_eq!(after.queries_cache_hit - before.queries_cache_hit, 1);
    Ok(())
}

#[tokio::test]
async fn test_delete_between_chunks_is_not_resurrected() -> Result<(), Error> {
    let mut ctx = setup().await?;

    let q2 = "select count(*) from chunked where grp = 2";
    ctx.simple_query(q2).await?;
    merge_in_progress_wait(&ctx, 0.0).await?;

    // Remove a row from the tail of the staging order (last inserted → last
    // chunk) while the merge is still on its first chunks. Its removal streams
    // before its chunk lands: the deleted-key filter, rebuilt per chunk, must
    // keep the later chunk from reinserting it.
    let last_id = 999 + POPULATED_ROWS;
    ctx.origin
        .batch_execute(&format!("delete from chunked where id = {last_id}"))
        .await
        .map_err(Error::other)?;

    ctx.cache_settle_with_timeout(Duration::from_secs(30))
        .await?;
    ctx.cdc_apply_settle().await?;
    let before = ctx.metrics().await?;
    let count = first_cell(&ctx.simple_query(q2).await?).unwrap_or_default();
    let after = ctx.metrics().await?;
    assert_eq!(
        count,
        (POPULATED_ROWS - 1).to_string(),
        "a row deleted mid-merge was resurrected by a later chunk"
    );
    assert_eq!(after.queries_cache_hit - before.queries_cache_hit, 1);
    Ok(())
}

/// A merge abandoned part-way (here: its query evicted under a count cap)
/// must empty its remaining staging in chunks, not with one DELETE on the
/// writer — a Ready bystander keeps seeing CDC while the discard runs.
#[tokio::test]
async fn test_abandoned_merge_discards_staging_in_chunks() -> Result<(), Error> {
    // The bystander is pinned so the count-cap eviction takes the populating
    // query (the oldest unpinned generation) and not the observer.
    let q1 = "select id, v from chunked where grp = 1 order by id";
    let mut ctx = TestContext::setup_pinned_fault(
        q1,
        &[
            ("PGCACHE_FAULT_MERGE_CHUNK_BLOCKS", CHUNK_BLOCKS),
            ("PGCACHE_FAULT_MERGE_CHUNK_DELAY_MS", CHUNK_DELAY_MS),
            ("PGCACHE_FAULT_EVICTION_COUNT_CAP", "1"),
        ],
        |origin| async move {
            origin
                .batch_execute(&format!(
                    "create table chunked (id int primary key, grp int not null, v int not null); \
                     insert into chunked (id, grp, v) values (1, 1, 1); \
                     insert into chunked (id, grp, v) select i, 2, i from generate_series(1000, {}) i; \
                     create table trigger_t (id int primary key); insert into trigger_t values (1);",
                    999 + POPULATED_ROWS
                ))
                .await
                .map_err(Error::other)?;
            Ok(origin)
        },
    )
    .await?;
    ctx.cache_settle_with_timeout(Duration::from_secs(15))
        .await?;
    let baseline = merge_chunks_total(ctx.metrics_port).await?;

    // Populating query: its merge spans several seconds.
    let q2 = "select count(*) from chunked where grp = 2";
    ctx.simple_query(q2).await?;
    merge_in_progress_wait(&ctx, baseline).await?;

    // A third registration puts the count over the cap of one; the eviction
    // tick evicts q2 mid-merge, which abandons the merge and discards its
    // remaining staging in chunks.
    ctx.simple_query("select id from trigger_t where id = 1")
        .await?;
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if discard_chunks_total(ctx.metrics_port).await? >= 1.0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the abandoned merge never started discarding"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // An origin write to the bystander's group while the discard is in
    // progress must reach it long before the discard ends.
    let written_at = Instant::now();
    ctx.origin
        .batch_execute("insert into chunked (id, grp, v) values (2, 1, 2)")
        .await
        .map_err(Error::other)?;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let before = ctx.metrics().await?;
        let served = ctx.simple_query(q1).await?;
        let after = ctx.metrics().await?;
        let ids: Vec<String> = served
            .iter()
            .filter_map(|m| match m {
                SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
                SimpleQueryMessage::CommandComplete(_)
                | SimpleQueryMessage::RowDescription(_)
                | _ => None,
            })
            .collect();
        if ids.iter().any(|id| id == "2") {
            let seen_after = written_at.elapsed();
            assert_eq!(after.queries_cache_hit - before.queries_cache_hit, 1);
            assert!(
                seen_after < Duration::from_millis(2_000),
                "insert took {seen_after:?} to reach the bystander: CDC apply waited on the discard"
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "insert never reached the bystander during the discard"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // The discard drains the whole remainder in chunks (many, given the pinned
    // chunk size), never as one statement.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if discard_chunks_total(ctx.metrics_port).await? >= 5.0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "discard did not proceed in chunks"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(())
}
