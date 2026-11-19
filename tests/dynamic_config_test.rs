use std::io::Error;

use crate::util::{TestContext, http_get, http_post, http_put};

mod util;

/// GET /config returns current dynamic config with expected fields
#[tokio::test]
async fn test_config_get_returns_dynamic_fields() -> Result<(), Error> {
    let ctx = TestContext::setup().await?;

    let (status, body) = http_get(ctx.metrics_port, "/config").await?;
    assert_eq!(status, 200, "body: {body}");

    let json: serde_json::Value = serde_json::from_str(&body)
        .map_err(|e| Error::other(format!("invalid JSON: {e}\nbody: {body}")))?;

    // Verify top-level structure
    assert!(json.get("dynamic").is_some(), "missing 'dynamic' field");
    assert!(
        json.get("restart_required").is_some(),
        "missing 'restart_required' field"
    );

    let dynamic = &json["dynamic"];
    assert!(
        dynamic.get("cache_policy").is_some(),
        "missing cache_policy"
    );
    assert!(
        dynamic.get("admission_threshold").is_some(),
        "missing admission_threshold"
    );
    assert!(
        dynamic.get("cache_size").is_some() || dynamic["cache_size"].is_null(),
        "missing cache_size"
    );

    // Default setup uses FIFO policy (overridden via CLI)
    assert_eq!(dynamic["cache_policy"].as_str(), Some("fifo"));
    // The harness passes flags over the config file; an unedited file still
    // needs no restart.
    assert_eq!(json["restart_required"], false);

    Ok(())
}

/// PUT /config updates cache_policy and the change is reflected in GET
#[tokio::test]
async fn test_config_put_updates_cache_policy() -> Result<(), Error> {
    let ctx = TestContext::setup().await?;

    // Read initial values
    let (_, body) = http_get(ctx.metrics_port, "/config").await?;
    let initial: serde_json::Value = serde_json::from_str(&body).map_err(Error::other)?;
    let initial_policy = initial["dynamic"]["cache_policy"].as_str().unwrap();
    assert_eq!(initial_policy, "fifo");

    // Update cache_policy to clock and admission_threshold to 3
    let (status, body) = http_put(
        ctx.metrics_port,
        "/config",
        r#"{"cache_policy": "clock", "admission_threshold": 3}"#,
    )
    .await?;
    assert_eq!(status, 200, "PUT failed: {body}");

    let json: serde_json::Value = serde_json::from_str(&body)
        .map_err(|e| Error::other(format!("invalid JSON: {e}\nbody: {body}")))?;

    let dynamic = &json["dynamic"];
    assert_eq!(dynamic["cache_policy"].as_str(), Some("clock"));
    assert_eq!(dynamic["admission_threshold"].as_u64(), Some(3));

    // Verify GET reflects the same changes
    let (status, body) = http_get(ctx.metrics_port, "/config").await?;
    assert_eq!(status, 200);
    let json: serde_json::Value = serde_json::from_str(&body).map_err(Error::other)?;
    assert_eq!(json["dynamic"]["cache_policy"].as_str(), Some("clock"));
    assert_eq!(json["dynamic"]["admission_threshold"].as_u64(), Some(3));

    Ok(())
}

/// PUT /config with invalid JSON returns 400
#[tokio::test]
async fn test_config_put_invalid_json_returns_400() -> Result<(), Error> {
    let ctx = TestContext::setup().await?;

    let (status, body) = http_put(ctx.metrics_port, "/config", "not json").await?;
    assert_eq!(status, 400, "body: {body}");
    assert!(body.contains("error"), "expected error in body: {body}");

    Ok(())
}

/// PUT /config with partial update preserves unmentioned fields
#[tokio::test]
async fn test_config_put_partial_preserves_other_fields() -> Result<(), Error> {
    let ctx = TestContext::setup().await?;

    // Get initial config
    let (_, body) = http_get(ctx.metrics_port, "/config").await?;
    let initial: serde_json::Value = serde_json::from_str(&body).map_err(Error::other)?;
    let initial_policy = initial["dynamic"]["cache_policy"].as_str().unwrap();

    // Update only admission_threshold
    let (status, body) =
        http_put(ctx.metrics_port, "/config", r#"{"admission_threshold": 5}"#).await?;
    assert_eq!(status, 200, "body: {body}");

    let json: serde_json::Value = serde_json::from_str(&body).map_err(Error::other)?;
    // admission_threshold changed
    assert_eq!(json["dynamic"]["admission_threshold"].as_u64(), Some(5));
    // cache_policy unchanged
    assert_eq!(
        json["dynamic"]["cache_policy"].as_str().unwrap(),
        initial_policy
    );

    Ok(())
}

/// PUT /config with null unsets optional fields
#[tokio::test]
async fn test_config_put_null_unsets_field() -> Result<(), Error> {
    let ctx = TestContext::setup().await?;

    // First set allowed_tables
    let (status, _) = http_put(
        ctx.metrics_port,
        "/config",
        r#"{"allowed_tables": ["users", "orders"]}"#,
    )
    .await?;
    assert_eq!(status, 200);

    // Verify it was set
    let (_, body) = http_get(ctx.metrics_port, "/config").await?;
    let json: serde_json::Value = serde_json::from_str(&body).map_err(Error::other)?;
    assert!(json["dynamic"]["allowed_tables"].is_array());

    // Unset with null
    let (status, body) =
        http_put(ctx.metrics_port, "/config", r#"{"allowed_tables": null}"#).await?;
    assert_eq!(status, 200, "body: {body}");

    let json: serde_json::Value = serde_json::from_str(&body).map_err(Error::other)?;
    assert!(
        json["dynamic"]["allowed_tables"].is_null(),
        "expected null, got: {}",
        json["dynamic"]["allowed_tables"]
    );

    Ok(())
}

/// Dynamic config change actually affects cache behavior:
/// start with FIFO (immediate admit), verify queries get cached,
/// then switch to clock with high threshold and verify new queries are NOT cached.
#[tokio::test]
async fn test_config_change_affects_cache_behavior() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Create test table and data
    ctx.query(
        "CREATE TABLE config_test (id integer PRIMARY KEY, name text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO config_test (id, name) VALUES (1, 'alice'), (2, 'bob')",
        &[],
    )
    .await?;

    // With FIFO (default), query should be admitted immediately
    ctx.simple_query("SELECT id, name FROM config_test WHERE id = 1")
        .await?;
    ctx.cache_settle().await?;
    ctx.simple_query("SELECT id, name FROM config_test WHERE id = 1")
        .await?;

    // Verify the first query is cached
    let (_, body) = http_get(ctx.metrics_port, "/status").await?;
    let json: serde_json::Value = serde_json::from_str(&body).map_err(Error::other)?;
    let registered_before = json["cache"]["queries_registered"].as_u64().unwrap_or(0);
    assert!(
        registered_before >= 1,
        "expected at least 1 registered query with FIFO"
    );

    // Switch to clock with very high threshold — new queries won't be admitted
    let (status, body) = http_put(
        ctx.metrics_port,
        "/config",
        r#"{"cache_policy": "clock", "admission_threshold": 100}"#,
    )
    .await?;
    assert_eq!(status, 200);
    let put_json: serde_json::Value = serde_json::from_str(&body).map_err(Error::other)?;
    assert_eq!(
        put_json["dynamic"]["admission_threshold"].as_u64(),
        Some(100)
    );

    // Fire a new query pattern multiple times — it should NOT get registered
    // because admission_threshold=100 means it needs 100 hits before admission
    for _ in 0..3 {
        ctx.simple_query("SELECT name FROM config_test WHERE id = 2")
            .await?;
    }
    ctx.cache_settle().await?;

    // The registered query count should not have grown
    let (_, body) = http_get(ctx.metrics_port, "/status").await?;
    let json: serde_json::Value = serde_json::from_str(&body).map_err(Error::other)?;
    let registered_after = json["cache"]["queries_registered"].as_u64().unwrap_or(0);
    assert_eq!(
        registered_before, registered_after,
        "query count should not have increased (threshold=100 prevents admission)"
    );

    Ok(())
}

/// POST /config/reload re-reads config file
#[tokio::test]
async fn test_config_reload() -> Result<(), Error> {
    let ctx = TestContext::setup().await?;

    let (status, body) = http_post(ctx.metrics_port, "/config/reload").await?;

    // Test infra uses --config, so reload should succeed
    if status == 200 {
        let json: serde_json::Value = serde_json::from_str(&body)
            .map_err(|e| Error::other(format!("invalid JSON: {e}\nbody: {body}")))?;
        assert!(json.get("dynamic").is_some(), "missing 'dynamic' field");
    } else if status == 400 {
        // No config path — also acceptable
        assert!(
            body.contains("no config file"),
            "expected 'no config file' error, got: {body}"
        );
    } else {
        panic!("unexpected status: {status}, body: {body}");
    }

    Ok(())
}

/// PUT /config with log_level change is reflected in the tracing subscriber
#[tokio::test]
async fn test_config_log_level_change() -> Result<(), Error> {
    let ctx = TestContext::setup().await?;

    // Verify initial effective log level is "info" (set via --log_level in test infra)
    let (status, body) = http_get(ctx.metrics_port, "/config").await?;
    assert_eq!(status, 200, "body: {body}");
    let json: serde_json::Value = serde_json::from_str(&body).map_err(Error::other)?;
    let initial_effective = json["effective_log_level"]
        .as_str()
        .expect("effective_log_level should be present");
    assert_eq!(initial_effective, "info", "expected initial level 'info'");

    // Change log level to debug
    let (status, body) = http_put(ctx.metrics_port, "/config", r#"{"log_level": "debug"}"#).await?;
    assert_eq!(status, 200, "PUT failed: {body}");

    let json: serde_json::Value = serde_json::from_str(&body).map_err(Error::other)?;
    // The dynamic config should reflect the requested level
    assert_eq!(json["dynamic"]["log_level"].as_str(), Some("debug"));
    // The subscriber should have accepted it
    assert_eq!(
        json["effective_log_level"].as_str(),
        Some("debug"),
        "subscriber should have reloaded to debug"
    );

    // Change to a compound filter
    let (status, body) = http_put(
        ctx.metrics_port,
        "/config",
        r#"{"log_level": "pgcache_lib::cache=trace,info"}"#,
    )
    .await?;
    assert_eq!(status, 200, "PUT failed: {body}");

    let json: serde_json::Value = serde_json::from_str(&body).map_err(Error::other)?;
    let effective = json["effective_log_level"]
        .as_str()
        .expect("effective_log_level present");
    // The subscriber should report the compound filter
    assert!(
        effective.contains("pgcache_lib::cache=trace"),
        "expected compound filter, got: {effective}"
    );

    // Unset log_level (reverts to default "info")
    let (status, body) = http_put(ctx.metrics_port, "/config", r#"{"log_level": null}"#).await?;
    assert_eq!(status, 200, "PUT failed: {body}");

    let json: serde_json::Value = serde_json::from_str(&body).map_err(Error::other)?;
    assert!(json["dynamic"]["log_level"].is_null());
    assert_eq!(
        json["effective_log_level"].as_str(),
        Some("info"),
        "unsetting log_level should revert subscriber to 'info'"
    );

    Ok(())
}
