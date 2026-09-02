use std::io::Error;

use crate::util::{TestContext, http_get, lsn_parse};

mod util;

/// /healthz always returns 200 OK
#[tokio::test]
async fn test_healthz_returns_ok() -> Result<(), Error> {
    let ctx = TestContext::setup().await?;

    let (status, body) = http_get(ctx.metrics_port, "/healthz").await?;
    assert_eq!(status, 200);
    assert_eq!(body, "OK");

    Ok(())
}

/// /readyz returns 200 when the proxy is in normal mode
#[tokio::test]
async fn test_readyz_returns_ok_when_healthy() -> Result<(), Error> {
    let ctx = TestContext::setup().await?;

    let (status, body) = http_get(ctx.metrics_port, "/readyz").await?;
    assert_eq!(status, 200);
    assert_eq!(body, "OK");

    Ok(())
}

/// /metrics returns 200 with Prometheus text format
#[tokio::test]
async fn test_metrics_returns_prometheus_format() -> Result<(), Error> {
    let ctx = TestContext::setup().await?;

    let (status, body) = http_get(ctx.metrics_port, "/metrics").await?;
    assert_eq!(status, 200);
    // Prometheus text format contains TYPE/HELP comment lines
    assert!(
        body.contains("# TYPE") || body.contains("# HELP") || body.contains("pgcache"),
        "expected Prometheus text format, got: {body}"
    );

    Ok(())
}

/// /status returns 200 with valid JSON containing cache, cdc, and queries fields
#[tokio::test]
async fn test_status_returns_json() -> Result<(), Error> {
    let ctx = TestContext::setup().await?;

    let (status, body) = http_get(ctx.metrics_port, "/status").await?;
    assert_eq!(status, 200, "status body: {body}");

    let json: serde_json::Value = serde_json::from_str(&body)
        .map_err(|e| Error::other(format!("invalid JSON: {e}\nbody: {body}")))?;

    assert!(json.get("cache").is_some(), "missing 'cache' field");
    assert!(json.get("cdc").is_some(), "missing 'cdc' field");
    assert!(json.get("queries").is_some(), "missing 'queries' field");

    Ok(())
}

/// /status reflects cached queries after they are loaded
#[tokio::test]
async fn test_status_shows_cached_queries() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE status_test (id integer PRIMARY KEY, name text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO status_test (id, name) VALUES (1, 'alice')",
        &[],
    )
    .await?;

    // Cache miss — registers the query
    ctx.simple_query("SELECT id, name FROM status_test WHERE id = 1")
        .await?;
    ctx.cache_settle().await?;

    let (status, body) = http_get(ctx.metrics_port, "/status").await?;
    assert_eq!(status, 200, "status body: {body}");

    let json: serde_json::Value = serde_json::from_str(&body)
        .map_err(|e| Error::other(format!("invalid JSON: {e}\nbody: {body}")))?;

    let queries = json["queries"]
        .as_array()
        .expect("queries should be an array");
    assert!(
        !queries.is_empty(),
        "expected at least one cached query in status"
    );

    // Verify query entry has expected fields
    let q = &queries[0];
    assert!(q.get("fingerprint").is_some(), "missing fingerprint");
    assert!(q.get("sql_preview").is_some(), "missing sql_preview");
    assert!(q.get("tables").is_some(), "missing tables");
    assert!(q.get("state").is_some(), "missing state");

    // Verify per-query metrics fields are present
    assert!(q.get("hit_count").is_some(), "missing hit_count");
    assert!(q.get("miss_count").is_some(), "missing miss_count");
    assert!(
        q.get("invalidation_count").is_some(),
        "missing invalidation_count"
    );
    assert!(
        q.get("readmission_count").is_some(),
        "missing readmission_count"
    );
    assert!(q.get("eviction_count").is_some(), "missing eviction_count");
    assert!(
        q.get("subsumption_count").is_some(),
        "missing subsumption_count"
    );
    assert!(
        q.get("population_count").is_some(),
        "missing population_count"
    );
    assert!(
        q.get("total_bytes_served").is_some(),
        "missing total_bytes_served"
    );
    assert!(
        q.get("population_row_count").is_some(),
        "missing population_row_count"
    );

    // After one miss + population, expect miss_count=1, population_count=1
    assert_eq!(q["miss_count"].as_u64(), Some(1), "expected 1 miss");
    assert_eq!(
        q["population_count"].as_u64(),
        Some(1),
        "expected 1 population"
    );
    assert_eq!(
        q["hit_count"].as_u64(),
        Some(0),
        "expected 0 hits before cache hit"
    );

    // Duration fields should be present after population
    assert!(
        q.get("registered_duration_ms").is_some(),
        "missing registered_duration_ms"
    );
    assert!(
        q.get("cached_duration_ms").is_some(),
        "missing cached_duration_ms"
    );
    assert!(
        q.get("last_population_duration_ms").is_some(),
        "missing last_population_duration_ms"
    );

    // idle_duration_ms should be null (no hits yet)
    assert!(
        q["idle_duration_ms"].is_null(),
        "idle_duration_ms should be null before any hit"
    );

    // Cache hit — serve from cache
    ctx.simple_query("SELECT id, name FROM status_test WHERE id = 1")
        .await?;

    let (status, body) = http_get(ctx.metrics_port, "/status").await?;
    assert_eq!(status, 200);
    let json: serde_json::Value = serde_json::from_str(&body)
        .map_err(|e| Error::other(format!("invalid JSON: {e}\nbody: {body}")))?;
    let q = &json["queries"].as_array().unwrap()[0];

    assert_eq!(
        q["hit_count"].as_u64(),
        Some(1),
        "expected 1 hit after cache hit"
    );
    assert!(
        q["idle_duration_ms"].is_number(),
        "idle_duration_ms should be set after hit"
    );
    assert!(
        q["total_bytes_served"].as_u64().unwrap() > 0,
        "expected bytes served after hit"
    );

    Ok(())
}

/// Unknown paths return 404
#[tokio::test]
async fn test_unknown_path_returns_404() -> Result<(), Error> {
    let ctx = TestContext::setup().await?;

    let (status, body) = http_get(ctx.metrics_port, "/nonexistent").await?;
    assert_eq!(status, 404);
    assert_eq!(body, "Not Found");

    Ok(())
}

/// /status cache field contains expected structure
#[tokio::test]
async fn test_status_cache_fields() -> Result<(), Error> {
    let ctx = TestContext::setup().await?;

    let (status, body) = http_get(ctx.metrics_port, "/status").await?;
    assert_eq!(status, 200);

    let json: serde_json::Value = serde_json::from_str(&body)
        .map_err(|e| Error::other(format!("invalid JSON: {e}\nbody: {body}")))?;

    let cache = &json["cache"];
    assert!(
        cache.get("size_bytes").is_some(),
        "missing cache.size_bytes"
    );
    assert!(
        cache.get("generation").is_some(),
        "missing cache.generation"
    );
    assert!(
        cache.get("tables_tracked").is_some(),
        "missing cache.tables_tracked"
    );
    assert!(cache.get("policy").is_some(), "missing cache.policy");
    assert!(
        cache.get("queries_registered").is_some(),
        "missing cache.queries_registered"
    );
    assert!(cache.get("uptime_ms").is_some(), "missing cache.uptime_ms");
    assert!(
        cache.get("cache_hits").is_some(),
        "missing cache.cache_hits"
    );
    assert!(
        cache.get("cache_misses").is_some(),
        "missing cache.cache_misses"
    );

    // uptime should be > 0
    assert!(
        cache["uptime_ms"].as_u64().unwrap() > 0,
        "uptime_ms should be > 0"
    );

    Ok(())
}

/// /status cdc field contains expected structure
#[tokio::test]
async fn test_status_cdc_fields() -> Result<(), Error> {
    let ctx = TestContext::setup().await?;

    let (status, body) = http_get(ctx.metrics_port, "/status").await?;
    assert_eq!(status, 200);

    let json: serde_json::Value = serde_json::from_str(&body)
        .map_err(|e| Error::other(format!("invalid JSON: {e}\nbody: {body}")))?;

    let cdc = &json["cdc"];
    assert!(
        cdc.get("last_applied_lsn").is_some(),
        "missing cdc.last_applied_lsn"
    );

    Ok(())
}

/// `cdc.last_applied_lsn` advances past `pg_current_wal_lsn()` captured after
/// a committed origin transaction. This is the watermark the conformance
/// harness will poll on between DML and the next SELECT.
#[tokio::test]
async fn test_status_applied_lsn_advances_after_write() -> Result<(), Error> {
    use std::time::{Duration, Instant};

    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE applied_lsn_test (id integer PRIMARY KEY, name text)",
        &[],
    )
    .await?;

    // Register a query against the table so its OID enters the publication.
    // Without this, INSERTs are filtered out by pgoutput on the origin and
    // the writer never sees a CommitMark for them.
    ctx.simple_query("SELECT id, name FROM applied_lsn_test WHERE id = 1")
        .await?;
    ctx.cache_settle().await?;

    // Commit a transaction on origin and capture its post-commit WAL position.
    ctx.origin_query(
        "INSERT INTO applied_lsn_test (id, name) VALUES (1, 'alice')",
        &[],
    )
    .await?;
    // `pg_current_wal_insert_lsn` (not `pg_current_wal_lsn`) — see the
    // doc comment on `TestContext::cdc_apply_settle_with_timeout`.
    let captured_lsn_str: String = ctx
        .origin
        .query_one("SELECT pg_current_wal_insert_lsn()::text", &[])
        .await
        .map_err(Error::other)?
        .get(0);
    let captured_lsn = lsn_parse(&captured_lsn_str)?;

    // Poll /status until the writer has applied past the captured LSN.
    let deadline = Instant::now() + Duration::from_secs(10);
    let applied_lsn = loop {
        let (status, body) = http_get(ctx.metrics_port, "/status").await?;
        assert_eq!(status, 200, "status body: {body}");
        let json: serde_json::Value = serde_json::from_str(&body)
            .map_err(|e| Error::other(format!("invalid JSON: {e}\nbody: {body}")))?;
        let applied = json["cdc"]["last_applied_lsn"]
            .as_u64()
            .ok_or_else(|| Error::other("cdc.last_applied_lsn not a u64"))?;
        if applied >= captured_lsn {
            break applied;
        }
        if Instant::now() >= deadline {
            return Err(Error::other(format!(
                "timed out waiting for cdc.last_applied_lsn ({applied}) to reach captured LSN ({captured_lsn} / {captured_lsn_str})"
            )));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    assert!(
        applied_lsn >= captured_lsn,
        "applied {applied_lsn} should be >= captured {captured_lsn}"
    );

    Ok(())
}
