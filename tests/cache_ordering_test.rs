use std::io::Error;

use crate::util::{TestContext, pgproto_run};

mod util;

/// A cacheable query pipelined after a slow, uncacheable statement must not have
/// its (fast) cache-hit response jump ahead of the uncacheable statement's
/// still-in-flight origin response.
///
/// The cache worker writes the response directly to the client socket; if it is
/// dispatched while a prior origin response is still in flight, the responses
/// reach the client out of order (and, with concurrent writes, can interleave at
/// the byte level). See PGC-213.
///
/// Repro: a `DO` block that sleeps 0.3s (forwarded to origin) is pipelined
/// immediately before a cacheable `SELECT` that is already a cache hit. Correct
/// ordering requires the DO's `CommandComplete` (which has no `DataRow`) to
/// precede the SELECT's `DataRow`.
#[tokio::test]
async fn test_pipelined_cache_hit_preserves_response_order() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE proto_pipeline (id integer PRIMARY KEY, name text)",
        &[],
    )
    .await?;
    ctx.query("INSERT INTO proto_pipeline VALUES (1, 'alice')", &[])
        .await?;

    // Populate the cache so the pipelined SELECT is served as a hit by the worker.
    let _ = ctx
        .simple_query("SELECT id, name FROM proto_pipeline WHERE id = 1")
        .await?;
    ctx.cache_settle().await?;

    let output = pgproto_run(ctx.cache_port, "tests/data/pgproto/pipelined_ordering.data");

    let data_row_idx = output
        .find("<= BE DataRow")
        .expect("a DataRow in the response");
    let command_complete_idx = output
        .find("<= BE CommandComplete")
        .expect("a CommandComplete in the response");

    assert!(
        command_complete_idx < data_row_idx,
        "response reordering: the cacheable SELECT's DataRow appeared before the \
         uncacheable DO's CommandComplete — the cache hit jumped ahead of the \
         in-flight origin response.\n{output}",
    );

    Ok(())
}
