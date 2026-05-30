use std::io::Error;

use crate::util::{TestContext, assert_cache_hit, assert_cache_miss, metrics_delta, pgproto_run};

mod util;

/// Assert pgproto output contains expected backend response messages.
/// pgproto debug output shows `<= BE MessageType` lines.
fn assert_pgproto_select(output: &str, expected_rows: usize) {
    assert!(
        output.contains("ReadyForQuery"),
        "expected ReadyForQuery in output:\n{output}",
    );
    let row_count = output.matches("<= BE DataRow").count();
    assert!(
        row_count >= expected_rows,
        "expected at least {expected_rows} DataRow messages, got {row_count}:\n{output}",
    );
}

/// Integration tests for extended protocol variations using pgproto.
///
/// These tests exercise wire-level protocol sequences that tokio-postgres cannot
/// produce, such as Flush-before-Sync (JDBC pattern), multi-Execute pipelines,
/// named statement reuse without re-Parse, and named portals.
#[tokio::test]
async fn test_pgproto_extended_protocol() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Create test table and seed data
    ctx.query(
        "CREATE TABLE proto_test (id integer PRIMARY KEY, name text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO proto_test VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')",
        &[],
    )
    .await?;

    // --- Scenario 1: Parse/Bind/Execute/Sync (standard cacheable) ---
    // First run: cache miss
    let m = ctx.metrics().await?;
    let output = pgproto_run(
        ctx.cache_port,
        "tests/data/pgproto/parse_bind_execute_sync.data",
    );
    assert_pgproto_select(&output, 3);
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Second run: cache hit (same query, new connection)
    let output = pgproto_run(
        ctx.cache_port,
        "tests/data/pgproto/parse_bind_execute_sync.data",
    );
    assert_pgproto_select(&output, 3);
    let _m = assert_cache_hit(&mut ctx, m).await?;

    // --- Scenario 2: Named statement reuse (Bind-only after prior Parse) ---
    // Parse once with Bind/Execute/Sync, then Bind-only/Execute/Sync.
    // The proxy proactively forwards Parse to origin on cache hit,
    // so the Bind-only second cycle works without re-Parse.
    let m = ctx.metrics().await?;
    let output = pgproto_run(ctx.cache_port, "tests/data/pgproto/bind_reuse_sync.data");
    // Two Sync boundaries should each return 3 rows = 6 DataRows total
    assert_pgproto_select(&output, 6);
    let after = ctx.metrics().await?;
    let delta = metrics_delta(&m, &after);
    assert!(
        delta.queries_total >= 2,
        "expected at least 2 queries from bind reuse, got {}",
        delta.queries_total,
    );

    // --- Scenario 3: Named portal ---
    // Bind to a named portal "P1", then Execute referencing that portal.
    // Verifies the proxy correctly tracks named portals and resolves them
    // during Execute for cacheability routing.
    let m = ctx.metrics().await?;
    let output = pgproto_run(ctx.cache_port, "tests/data/pgproto/named_portal.data");
    assert_pgproto_select(&output, 3);
    let after = ctx.metrics().await?;
    let delta = metrics_delta(&m, &after);
    assert!(
        delta.queries_total >= 1,
        "expected at least 1 query from named portal, got {}",
        delta.queries_total,
    );

    // --- Scenario 4: JDBC Describe/Flush pattern ---
    // Flush sends Parse/Bind/Describe to origin immediately.
    // Execute/Sync follows separately.
    let m = ctx.metrics().await?;
    let output = pgproto_run(
        ctx.cache_port,
        "tests/data/pgproto/jdbc_describe_flush.data",
    );
    assert_pgproto_select(&output, 3);
    // Flush-then-Sync pattern should still process the query
    let after = ctx.metrics().await?;
    let delta = metrics_delta(&m, &after);
    assert!(
        delta.queries_total >= 1,
        "expected at least 1 query from JDBC pattern, got {}",
        delta.queries_total,
    );

    // --- Scenario 5: Multiple Executes before one Sync (PGC-217) ---
    // Two Parse/Bind/Execute pairs before a single Sync. Both are cacheable
    // reads; the batch returns all rows and exactly one ReadyForQuery.
    let output = pgproto_run(ctx.cache_port, "tests/data/pgproto/multi_execute.data");
    assert_pgproto_select(&output, 3); // first SELECT returns 3 rows, second 1
    assert_eq!(
        output.matches("ReadyForQuery").count(),
        1,
        "expected exactly one RFQ for the multi-execute batch:\n{output}"
    );

    // --- Scenario 6: Non-cacheable INSERT forwarded at Sync ---
    let m = ctx.metrics().await?;
    let output = pgproto_run(ctx.cache_port, "tests/data/pgproto/non_cacheable_sync.data");
    assert!(
        output.contains("ReadyForQuery"),
        "expected ReadyForQuery for INSERT:\n{output}",
    );
    let after = ctx.metrics().await?;
    let delta = metrics_delta(&m, &after);
    assert!(
        delta.queries_unsupported >= 1,
        "expected unsupported increment for INSERT, got {}",
        delta.queries_unsupported,
    );

    // Wait for CDC to invalidate the cached SELECT * FROM proto_test
    // so the verification query isn't served from stale cache via subsumption
    ctx.cdc_settle().await?;

    // Verify the INSERT actually executed on origin
    let rows = ctx
        .query("SELECT name FROM proto_test WHERE id = 99", &[])
        .await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>("name"), "pgproto");

    Ok(())
}

/// PGC-195: a second `Parse + Describe('S') + Sync` for the same SQL on a
/// fresh statement name must be served from the per-connection Describe
/// cache without forwarding to origin. The wire response is identical to
/// origin's first-time reply: `ParseComplete + ParameterDescription +
/// RowDescription + ReadyForQuery`.
#[tokio::test]
async fn test_pgproto_describe_cache_synth() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE proto_test (id integer PRIMARY KEY, name text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO proto_test VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')",
        &[],
    )
    .await?;

    let m = ctx.metrics().await?;
    let output = pgproto_run(
        ctx.cache_port,
        "tests/data/pgproto/describe_cache_synth.data",
    );

    // Two Sync responses — first from origin, second synthesized.
    assert_eq!(
        output.matches("ParseComplete").count(),
        2,
        "expected two ParseComplete:\n{output}"
    );
    assert_eq!(
        output.matches("ParameterDescription").count(),
        2,
        "expected two ParameterDescription:\n{output}"
    );
    assert_eq!(
        output.matches("RowDescription").count(),
        2,
        "expected two RowDescription:\n{output}"
    );
    assert_eq!(
        output.matches("ReadyForQuery").count(),
        2,
        "expected two ReadyForQuery:\n{output}"
    );

    let delta = metrics_delta(&m, &ctx.metrics().await?);
    assert_eq!(
        delta.protocol_describe_cache_misses, 1,
        "expected 1 describe-cache miss (the first Parse), got {}",
        delta.protocol_describe_cache_misses
    );
    assert_eq!(
        delta.protocol_describe_cache_hits, 1,
        "expected 1 describe-cache hit (the second Parse), got {}",
        delta.protocol_describe_cache_hits
    );

    Ok(())
}

/// PGC-195: when a synthesized Parse is followed by a Bind+Execute that
/// misses the data cache, pgcache must prepend the captured Parse bytes
/// before forwarding so origin can resolve the Bind. The intercepted
/// ParseComplete must not leak to the client.
#[tokio::test]
async fn test_pgproto_lazy_parse_on_forward() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE proto_test (id integer PRIMARY KEY, name text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO proto_test VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')",
        &[],
    )
    .await?;

    let m = ctx.metrics().await?;
    let output = pgproto_run(
        ctx.cache_port,
        "tests/data/pgproto/lazy_parse_on_forward.data",
    );

    // Both rows should come back (one per Sync boundary).
    assert!(
        output.contains("ReadyForQuery"),
        "expected ReadyForQuery:\n{output}"
    );
    assert_eq!(
        output.matches("<= BE DataRow").count(),
        2,
        "expected one DataRow per Sync (id=1, id=2):\n{output}"
    );
    // ParseComplete count should match the two client Parse messages —
    // the proxy's lazy Parse intercepts the *extra* ParseComplete that
    // origin emits in response to the prepended Parse, so the client
    // only sees the responses it expected.
    assert_eq!(
        output.matches("ParseComplete").count(),
        2,
        "expected exactly 2 ParseComplete reaching the client:\n{output}"
    );

    let delta = metrics_delta(&m, &ctx.metrics().await?);
    assert!(
        delta.protocol_describe_cache_hits >= 1,
        "expected at least 1 describe-cache hit, got {}",
        delta.protocol_describe_cache_hits
    );
    assert!(
        delta.protocol_lazy_parse_forwarded >= 1,
        "expected at least 1 lazy-Parse forward, got {}",
        delta.protocol_lazy_parse_forwarded
    );

    Ok(())
}

/// PGC-217: an extended batch that carries more than one statement's worth of
/// prep around a single Execute must not be served from cache. The cache path
/// synthesizes exactly one ParseComplete/BindComplete; serving such a batch
/// from cache would under-respond and desync the connection. Both the
/// duplicate-prep shape (`P/B/P/B/E`) and the trailing-prep shape (`P/B/E/P/B`)
/// must forward to origin so the client receives a ParseComplete/BindComplete
/// for every statement it sent.
#[tokio::test]
async fn test_pgproto_multi_prep_forwards() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE proto_test (id integer PRIMARY KEY, name text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO proto_test VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')",
        &[],
    )
    .await?;

    // Warm the cache for `SELECT ... ORDER BY id` — the query both batches
    // Execute. This makes the cache path reachable, so the test exercises the
    // mis-synthesis-on-hit bug, not just the drop-on-miss regression.
    pgproto_run(
        ctx.cache_port,
        "tests/data/pgproto/parse_bind_execute_sync.data",
    );
    ctx.cache_settle().await?;

    for data in [
        "tests/data/pgproto/multi_prep_single_execute.data",
        "tests/data/pgproto/trailing_prep_after_execute.data",
    ] {
        let output = pgproto_run(ctx.cache_port, data);
        assert!(
            output.contains("ReadyForQuery"),
            "expected ReadyForQuery for {data}:\n{output}"
        );
        assert_eq!(
            output.matches("ParseComplete").count(),
            2,
            "expected 2 ParseComplete (one per client Parse) for {data}:\n{output}"
        );
        assert_eq!(
            output.matches("BindComplete").count(),
            2,
            "expected 2 BindComplete (one per client Bind) for {data}:\n{output}"
        );
        assert!(
            output.matches("<= BE DataRow").count() >= 1,
            "expected at least one DataRow for {data}:\n{output}"
        );
    }

    Ok(())
}

/// PGC-217: a batch of independently-cacheable reads before a single Sync is
/// served from cache (one cache hit per Execute, one trailing ReadyForQuery).
/// A batch containing a write is forwarded whole so the read observes the write
/// (the batch is one implicit transaction).
#[tokio::test]
async fn test_pgproto_multi_execute() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE proto_test (id integer PRIMARY KEY, name text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO proto_test VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')",
        &[],
    )
    .await?;

    // Warm both queries the batch executes so each Execute hits.
    ctx.query("SELECT id, name FROM proto_test ORDER BY id", &[])
        .await?;
    ctx.query("SELECT id, name FROM proto_test WHERE id = 1", &[])
        .await?;
    ctx.cache_settle().await?;

    let m = ctx.metrics().await?;
    let output = pgproto_run(ctx.cache_port, "tests/data/pgproto/multi_execute.data");
    assert_eq!(
        output.matches("ReadyForQuery").count(),
        1,
        "expected one RFQ for the batch:\n{output}"
    );
    let delta = metrics_delta(&m, &ctx.metrics().await?);
    assert_eq!(
        delta.queries_cache_hit, 2,
        "expected both executes served from cache, got {}",
        delta.queries_cache_hit
    );

    // A write+read batch forwards whole; the SELECT observes the INSERT.
    let output = pgproto_run(
        ctx.cache_port,
        "tests/data/pgproto/multi_execute_write_read.data",
    );
    assert_eq!(
        output.matches("ReadyForQuery").count(),
        1,
        "expected one RFQ for the write+read batch:\n{output}"
    );
    assert!(
        output.contains("fifty"),
        "expected the SELECT to observe the in-batch INSERT:\n{output}"
    );

    Ok(())
}

/// PGC-218: forwarding a batch with two named Parses must mark BOTH statements
/// `origin_prepared` (one per ParseComplete, in order), not just the last.
/// Otherwise a later Bind-only reuse of the first statement prepends a spurious
/// lazy Parse and origin errors with "already exists".
#[tokio::test]
async fn test_pgproto_multi_parse_origin_prepared() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE proto_test (id integer PRIMARY KEY, name text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO proto_test VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')",
        &[],
    )
    .await?;

    let output = pgproto_run(ctx.cache_port, "tests/data/pgproto/multi_parse_reuse.data");
    assert!(
        !output.contains("already exists") && !output.contains("ErrorResponse"),
        "Bind-only reuse of the first forwarded statement must not error:\n{output}"
    );
    assert!(
        output.matches("<= BE DataRow").count() >= 1,
        "expected the reused statement to return its row:\n{output}"
    );

    Ok(())
}
