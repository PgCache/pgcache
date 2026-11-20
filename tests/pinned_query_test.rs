use std::io::Error;

use crate::util::{TestContext, assert_cache_hit, assert_cache_miss, assert_row_at, metrics_delta};

mod util;

/// Test 1: Pinned query is pre-populated at startup and serves a cache hit
/// on the very first client request.
///
/// Setup creates the table and data before pgcache starts, so the pinned
/// query can resolve and populate during cache initialization.
#[tokio::test]
async fn test_pinned_query_cache_hit_on_first_request() -> Result<(), Error> {
    let mut ctx =
        TestContext::setup_pinned("SELECT id, name FROM pin_table", |origin| async move {
            origin
                .execute(
                    "CREATE TABLE pin_table (id INTEGER PRIMARY KEY, name TEXT)",
                    &[],
                )
                .await
                .map_err(Error::other)?;
            origin
                .execute(
                    "INSERT INTO pin_table (id, name) VALUES (1, 'alice'), (2, 'bob')",
                    &[],
                )
                .await
                .map_err(Error::other)?;
            Ok(origin)
        })
        .await?;

    // Wait for pinned query to finish populating
    ctx.cache_settle().await?;

    // The first client request after the verdict is a cache hit (pinned
    // query already populated).
    ctx.verdict_warm("SELECT id, name FROM pin_table").await?;
    let m = ctx.metrics().await?;
    let res = ctx.simple_query("SELECT id, name FROM pin_table").await?;
    assert_row_at(&res, 1, &[("id", "1"), ("name", "alice")])?;
    assert_row_at(&res, 2, &[("id", "2"), ("name", "bob")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test 2: Pinned query auto-readmits after CDC invalidation.
///
/// Uses a JOIN query because single-table CDC updates are handled in place
/// without invalidation. A join-column change triggers full invalidation,
/// and the pinned flag causes automatic readmission.
#[tokio::test]
async fn test_pinned_query_auto_readmit_after_cdc() -> Result<(), Error> {
    let pinned_sql = "SELECT p.id, p.name, d.detail \
        FROM pin_parent p JOIN pin_detail d ON d.parent_id = p.id \
        ORDER BY d.detail";

    let mut ctx = TestContext::setup_pinned(pinned_sql, |origin| async move {
        origin
            .execute(
                "CREATE TABLE pin_parent (id INTEGER PRIMARY KEY, name TEXT)",
                &[],
            )
            .await
            .map_err(Error::other)?;
        origin
            .execute(
                "CREATE TABLE pin_detail (id SERIAL PRIMARY KEY, parent_id INTEGER, detail TEXT)",
                &[],
            )
            .await
            .map_err(Error::other)?;
        origin
            .execute(
                "INSERT INTO pin_parent (id, name) VALUES (1, 'alice'), (2, 'bob')",
                &[],
            )
            .await
            .map_err(Error::other)?;
        origin
            .execute(
                "INSERT INTO pin_detail (parent_id, detail) VALUES (1, 'original')",
                &[],
            )
            .await
            .map_err(Error::other)?;
        Ok(origin)
    })
    .await?;

    // Wait for pinned query population
    ctx.cache_settle().await?;

    // First request after the verdict: cache hit from pinned pre-population
    ctx.verdict_warm(pinned_sql).await?;
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(pinned_sql).await?;
    assert_row_at(
        &res,
        1,
        &[("id", "1"), ("name", "alice"), ("detail", "original")],
    )?;
    let m = assert_cache_hit(&mut ctx, m).await?;

    // CDC: insert a new detail row that joins into the result set.
    // This triggers invalidation on the join table.
    ctx.origin_query(
        "INSERT INTO pin_detail (parent_id, detail) VALUES (1, 'added')",
        &[],
    )
    .await?;

    // Wait for CDC invalidation + auto-readmit + re-population
    ctx.cdc_apply_settle().await?;
    ctx.cache_settle().await?;

    // Verify invalidation and readmission happened
    let m_after_cdc = ctx.metrics().await?;
    let delta = metrics_delta(&m, &m_after_cdc);
    assert!(
        delta.cache_invalidations >= 1,
        "expected at least 1 cache invalidation from CDC, got {}",
        delta.cache_invalidations
    );
    assert_eq!(
        delta.cache_readmissions, 1,
        "expected 1 readmission for pinned query"
    );

    // After auto-readmit, the query should be a cache hit with updated data
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(pinned_sql).await?;
    assert_row_at(
        &res,
        1,
        &[("id", "1"), ("name", "alice"), ("detail", "added")],
    )?;
    assert_row_at(
        &res,
        2,
        &[("id", "1"), ("name", "alice"), ("detail", "original")],
    )?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test 3: Pinned query survives eviction when the cache is full.
///
/// A count cap of 2 means registering additional non-pinned queries triggers
/// eviction; the pinned query is bumped (never evicted) while others are.
/// Eviction is forced via the fault-injection count cap (PGC-276).
#[cfg(feature = "fault-injection")]
#[tokio::test]
async fn test_pinned_query_survives_eviction() -> Result<(), Error> {
    let pinned_sql = "SELECT id, data FROM pin_survive";

    let mut ctx = TestContext::setup_pinned_small_cache(
        pinned_sql,
        2, // count cap: holds 2 queries, so further registrations evict
        |origin| async move {
            // Create the pinned table
            origin
                .execute(
                    "CREATE TABLE pin_survive (id INTEGER PRIMARY KEY, data TEXT)",
                    &[],
                )
                .await
                .map_err(Error::other)?;
            for i in 0..500 {
                origin
                    .execute(
                        "INSERT INTO pin_survive (id, data) VALUES ($1, $2)",
                        &[&i, &format!("pinned-{i:060}")],
                    )
                    .await
                    .map_err(Error::other)?;
            }

            // Create eviction-fodder tables
            for table in &["evict_x", "evict_y", "evict_z"] {
                origin
                    .execute(
                        &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, data TEXT)"),
                        &[],
                    )
                    .await
                    .map_err(Error::other)?;
                for i in 0..500 {
                    origin
                        .execute(
                            &format!("INSERT INTO {table} (id, data) VALUES ($1, $2)"),
                            &[&i, &format!("payload-{table}-{i:060}")],
                        )
                        .await
                        .map_err(Error::other)?;
                }
            }

            Ok(origin)
        },
    )
    .await?;

    // Wait for pinned query population
    ctx.cache_settle().await?;

    // Verify pinned query is serving hits
    ctx.verdict_warm(pinned_sql).await?;
    let m = ctx.metrics().await?;
    ctx.simple_query(pinned_sql).await?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    // Register non-pinned queries to fill the cache and trigger eviction
    ctx.simple_query("SELECT id, data FROM evict_x").await?;
    ctx.cache_settle().await?;
    ctx.simple_query("SELECT id, data FROM evict_y").await?;
    ctx.cache_settle().await?;
    ctx.simple_query("SELECT id, data FROM evict_z").await?;
    ctx.cache_settle().await?;

    // Eviction runs on the ~1s writer tick now (not per Ready); wait for it.
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

    // Take fresh metrics snapshot after eviction-fodder queries
    let m = ctx.metrics().await?;

    // Pinned query should still be a cache hit after eviction
    let res = ctx.simple_query(pinned_sql).await?;
    assert_row_at(&res, 1, &[]).expect("at least one data row from pinned query");
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test 4: Invalid pinned query is skipped gracefully — pgcache starts
/// normally and serves regular queries without issue.
#[tokio::test]
async fn test_pinned_query_invalid_sql_skipped() -> Result<(), Error> {
    let mut ctx = TestContext::setup_pinned("NOT VALID SQL AT ALL", |origin| async move {
        origin
            .execute(
                "CREATE TABLE normal_table (id INTEGER PRIMARY KEY, data TEXT)",
                &[],
            )
            .await
            .map_err(Error::other)?;
        origin
            .execute(
                "INSERT INTO normal_table (id, data) VALUES (1, 'works')",
                &[],
            )
            .await
            .map_err(Error::other)?;
        Ok(origin)
    })
    .await?;

    // pgcache started successfully despite invalid pinned query
    // Normal queries work fine — first request is a cache miss (pass-through)
    let m = ctx.metrics().await?;
    let res = ctx
        .simple_query("SELECT id, data FROM normal_table")
        .await?;
    assert_row_at(&res, 1, &[("id", "1"), ("data", "works")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    let res = ctx
        .simple_query("SELECT id, data FROM normal_table")
        .await?;
    assert_row_at(&res, 1, &[("id", "1"), ("data", "works")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// The number of generations the writer holds for cached queries.
async fn generations_active(ctx: &TestContext) -> Result<u64, Error> {
    let (status, body) = crate::util::http_get(ctx.metrics_port, "/status").await?;
    if status != 200 {
        return Err(Error::other(format!("/status returned {status}: {body}")));
    }
    let json: serde_json::Value = serde_json::from_str(&body).map_err(Error::other)?;
    json.get("cache")
        .and_then(|cache| cache.get("generations_active"))
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| Error::other("cache.generations_active missing"))
}

const READMIT_PINNED: &str = "SELECT p.id, p.name, d.detail \
    FROM gen_parent p JOIN gen_detail d ON d.parent_id = p.id \
    ORDER BY d.detail";

/// Origin tables for a pinned join query and one ordinary single-table query.
async fn readmit_tables_create(
    origin: tokio_postgres::Client,
) -> Result<tokio_postgres::Client, Error> {
    for sql in [
        "CREATE TABLE gen_parent (id INTEGER PRIMARY KEY, name TEXT)",
        "CREATE TABLE gen_detail (id SERIAL PRIMARY KEY, parent_id INTEGER, detail TEXT)",
        "CREATE TABLE gen_other (id INTEGER PRIMARY KEY, name TEXT)",
        "INSERT INTO gen_parent (id, name) VALUES (1, 'alice'), (2, 'bob')",
        "INSERT INTO gen_detail (parent_id, detail) VALUES (1, 'original')",
        "INSERT INTO gen_other VALUES (1, 'x')",
    ] {
        origin.execute(sql, &[]).await.map_err(Error::other)?;
    }
    Ok(origin)
}

/// A pinned query's readmission gives up the generation it held: one left
/// in the active set belongs to no query and holds the purge threshold
/// below it for the life of the cache (PGC-646).
#[tokio::test]
async fn test_pinned_readmit_releases_its_previous_generation() -> Result<(), Error> {
    let mut ctx = TestContext::setup_pinned(READMIT_PINNED, readmit_tables_create).await?;
    ctx.cache_settle().await?;
    ctx.simple_query("SELECT * FROM gen_other WHERE id = 1")
        .await?;
    ctx.cache_settle().await?;
    assert_eq!(generations_active(&ctx).await?, 2);

    for round in 0..3 {
        let m = ctx.metrics().await?;
        ctx.origin_query(
            &format!("INSERT INTO gen_detail (parent_id, detail) VALUES (1, 'added{round}')"),
            &[],
        )
        .await?;
        ctx.cdc_apply_settle().await?;
        ctx.cache_settle().await?;
        let delta = metrics_delta(&m, &ctx.metrics().await?);
        assert_eq!(delta.cache_readmissions, 1, "round {round}");
        assert_eq!(
            generations_active(&ctx).await?,
            2,
            "round {round}: a readmit left a generation no query holds"
        );
    }
    Ok(())
}

/// A readmit that finds its query gone allocates no generation. A deadlock
/// at the frame's commit is one way there: the frame's invalidations are
/// flushed first, queueing the pinned query's readmit, and the recovery
/// then evicts every query over the frame's relations (PGC-646).
#[cfg(feature = "fault-injection")]
#[tokio::test]
async fn test_readmit_of_an_evicted_pinned_query_allocates_no_generation() -> Result<(), Error> {
    let mut ctx = TestContext::setup_pinned_fault(
        READMIT_PINNED,
        &[("PGCACHE_FAULT_CDC_COMMIT_DEADLOCK_ONCE", "1")],
        readmit_tables_create,
    )
    .await?;
    ctx.cache_settle().await?;
    ctx.simple_query("SELECT * FROM gen_other WHERE id = 1")
        .await?;
    ctx.cache_settle().await?;
    assert_eq!(generations_active(&ctx).await?, 2);

    // One transaction: a write applied in place, so the frame commits, and
    // an insert that grows the join, so the pinned query is invalidated.
    for sql in [
        "BEGIN",
        "UPDATE gen_other SET name = 'y' WHERE id = 1",
        "INSERT INTO gen_detail (parent_id, detail) VALUES (1, 'added')",
        "COMMIT",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }
    ctx.cdc_apply_settle().await?;
    // The recovery evicted both queries; the queued readmit ran after.
    ctx.queries_registered_wait(0, std::time::Duration::from_secs(10))
        .await?;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert_eq!(
        generations_active(&ctx).await?,
        0,
        "the readmit of the evicted query left a generation no query holds"
    );
    Ok(())
}
