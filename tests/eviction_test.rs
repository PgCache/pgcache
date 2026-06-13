//! Eviction is forced via the fault-injection count cap
//! (`PGCACHE_FAULT_EVICTION_COUNT_CAP`), so this suite only builds with the
//! `fault-injection` feature (PGC-276 removed the disk byte-cap that previously
//! drove deterministic eviction in tests).
#![cfg(feature = "fault-injection")]

use std::io::Error;
use std::time::Duration;

use tokio::time::sleep;

use crate::util::TestContext;

mod util;

/// Helper: query pg_publication_tables on the origin to get the set of table names
/// currently in the publication.
async fn publication_tables_get(ctx: &mut TestContext) -> Result<Vec<String>, Error> {
    let rows = ctx
        .origin_query(
            "SELECT schemaname || '.' || tablename AS full_name \
             FROM pg_publication_tables \
             WHERE pubname = 'pub_test' \
             ORDER BY tablename",
            &[],
        )
        .await?;
    Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
}

/// Poll until the publication contains a table matching `needle`, or timeout.
async fn publication_table_wait(
    ctx: &mut TestContext,
    needle: &str,
    timeout: Duration,
) -> Result<Vec<String>, Error> {
    let start = std::time::Instant::now();
    loop {
        let tables = publication_tables_get(ctx).await?;
        if tables.iter().any(|t| t.contains(needle)) {
            return Ok(tables);
        }
        if start.elapsed() > timeout {
            return Err(Error::other(format!(
                "timed out waiting for '{needle}' in publication, got: {tables:?}"
            )));
        }
        sleep(Duration::from_millis(100)).await;
    }
}

/// Poll until the publication no longer contains a table matching `needle`, or timeout.
async fn publication_table_removed_wait(
    ctx: &mut TestContext,
    needle: &str,
    timeout: Duration,
) -> Result<Vec<String>, Error> {
    let start = std::time::Instant::now();
    loop {
        let tables = publication_tables_get(ctx).await?;
        if !tables.iter().any(|t| t.contains(needle)) {
            return Ok(tables);
        }
        if start.elapsed() > timeout {
            return Err(Error::other(format!(
                "timed out waiting for '{needle}' to be removed from publication, got: {tables:?}"
            )));
        }
        sleep(Duration::from_millis(100)).await;
    }
}

/// Test that tables are added to and removed from the publication
/// as queries are registered and evicted.
///
/// Uses a small cache_size to force FIFO eviction. Registers queries on
/// three tables sequentially. The cache is sized to hold roughly two queries,
/// so the third registration triggers eviction of the first. After eviction,
/// only the surviving queries' tables should remain in the publication.
#[tokio::test]
async fn test_eviction_removes_table_from_publication() -> Result<(), Error> {
    // Count cap of 2 — holds 2 queries, so the 3rd registration evicts the oldest.
    let mut ctx = TestContext::setup_small_cache(2).await?;
    let timeout = Duration::from_secs(5);

    // Create three tables with enough data to fill the cache.
    // 500 rows × ~70 bytes/row ≈ 35KB raw data per table,
    // but PostgreSQL disk footprint (pages + indexes) is much larger.
    for table in &["evict_a", "evict_b", "evict_c"] {
        ctx.query(
            &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, data TEXT)"),
            &[],
        )
        .await?;

        for i in 0..500 {
            ctx.query(
                &format!("INSERT INTO {table} (id, data) VALUES ($1, $2)"),
                &[&i, &format!("payload-{table}-{i:060}")],
            )
            .await?;
        }
    }

    // No publication tables yet — no cacheable queries registered
    let tables = publication_tables_get(&mut ctx).await?;
    assert!(
        tables.is_empty(),
        "expected empty publication, got: {tables:?}"
    );

    // Register query on table_a (cache miss → population)
    ctx.simple_query("SELECT id, data FROM evict_a").await?;
    let tables = publication_table_wait(&mut ctx, "evict_a", timeout).await?;
    assert!(
        tables.iter().any(|t| t.contains("evict_a")),
        "expected evict_a in publication"
    );

    // Register query on table_b (cache miss → population)
    ctx.simple_query("SELECT id, data FROM evict_b").await?;
    let tables = publication_table_wait(&mut ctx, "evict_b", timeout).await?;
    assert!(
        tables.iter().any(|t| t.contains("evict_a")),
        "expected evict_a still in publication, got: {tables:?}"
    );
    assert!(
        tables.iter().any(|t| t.contains("evict_b")),
        "expected evict_b in publication"
    );

    // Register query on table_c — this should push cache over the 200KB limit
    // and evict the oldest query (evict_a) via FIFO.
    ctx.simple_query("SELECT id, data FROM evict_c").await?;
    ctx.cache_settle().await?;

    // After eviction, evict_a should be removed from the publication.
    let tables = publication_table_removed_wait(&mut ctx, "evict_a", timeout).await?;
    assert!(
        !tables.iter().any(|t| t.contains("evict_a")),
        "expected evict_a removed from publication after eviction, got: {tables:?}"
    );

    // At least evict_c should be in the publication (the most recently registered)
    assert!(
        tables.iter().any(|t| t.contains("evict_c")),
        "expected evict_c in publication, got: {tables:?}"
    );

    Ok(())
}
