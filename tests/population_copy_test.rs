//! Population loads staging by piping origin's COPY text into the cache's COPY.
//! These tests check that values whose text form is hard to carry — quotes,
//! backslashes, control characters, the `\N` and `\.` markers, NULLs, arrays,
//! binary data and non-finite floats — come back from a cache hit exactly as
//! origin renders them, and that NULL-padded outer-join rows never reach staging.

use std::io::Error;

use tokio_postgres::SimpleQueryMessage;

use crate::util::{TestContext, assert_cache_hit, assert_cache_miss};

mod util;

/// Every data row of a simple-query response as optional column texts.
fn rows_text(messages: &[SimpleQueryMessage]) -> Vec<Vec<Option<String>>> {
    messages
        .iter()
        .filter_map(|message| {
            let SimpleQueryMessage::Row(row) = message else {
                return None;
            };
            Some(
                (0..row.len())
                    .map(|i| row.get(i).map(str::to_owned))
                    .collect(),
            )
        })
        .collect()
}

/// Values whose COPY text needs escaping, or that only round-trip if both
/// sessions agree on rendering, are served from cache byte-identical to origin.
#[tokio::test]
async fn test_population_copy_round_trips_edge_values() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.origin_query(
        "CREATE TYPE pc_mood AS ENUM ('happy', 'it''s complicated')",
        &[],
    )
    .await?;
    ctx.origin_query(
        "CREATE TABLE pc_values (\
             id INTEGER PRIMARY KEY, t TEXT, b BYTEA, ta TEXT[], ia INTEGER[], j JSONB, \
             ts TIMESTAMPTZ, iv INTERVAL, n NUMERIC, f FLOAT8, e pc_mood, d DATE)",
        &[],
    )
    .await?;
    ctx.origin_query(
        "INSERT INTO pc_values VALUES \
         (1, 'o''brien', '\\x00ff5c27'::bytea, ARRAY['a,b', 'quote\"d', NULL, 'back\\slash', ''], \
          ARRAY[1, NULL, 3], '{\"k\": \"v\\\"q\", \"n\": null, \"u\": \"☕\"}', \
          '2024-02-29 23:59:59.123456+05:30', '1 year 2 mons 3 days 04:05:06.789', \
          12345678901234567890.0123456789, 'NaN', 'it''s complicated', '0001-01-01 BC'), \
         (2, E'tab\\there\\nnewline\\rreturn', '\\x'::bytea, '{}', '{}', '[]', \
          'infinity', '-1 days', 0, 'Infinity', 'happy', 'infinity'), \
         (3, E'back\\\\slash and \\\\. and \\\\N', NULL, NULL, NULL, NULL, \
          NULL, NULL, NULL, '-Infinity', NULL, NULL), \
         (4, '\\N', NULL, ARRAY[NULL]::text[], NULL, 'null', NULL, NULL, -0.5, 0.30000000000000004, NULL, NULL), \
         (5, '', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL), \
         (6, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL), \
         (7, 'café ☕ 日本語', NULL, NULL, NULL, NULL, NULL, NULL, NULL, 1e-300, NULL, NULL)",
        &[],
    )
    .await?;
    ctx.cdc_decode_settle().await?;

    let query = "SELECT * FROM pc_values WHERE id > 0 ORDER BY id";
    let expected = rows_text(&ctx.origin.simple_query(query).await.map_err(Error::other)?);
    assert_eq!(expected.len(), 7);

    let m = ctx.metrics().await?;
    let miss = rows_text(&ctx.simple_query(query).await?);
    assert_eq!(miss, expected, "forwarded result differs from origin");
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    let hit = rows_text(&ctx.simple_query(query).await?);
    assert_cache_hit(&mut ctx, m).await?;
    assert_eq!(hit, expected, "cached result differs from origin");

    Ok(())
}

/// A population large enough to stream as many COPY chunks loads every row.
#[tokio::test]
async fn test_population_copy_streams_many_rows() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.origin_query(
        "CREATE TABLE pc_many (id INTEGER PRIMARY KEY, body TEXT, grp INTEGER)",
        &[],
    )
    .await?;
    ctx.origin_query(
        "INSERT INTO pc_many SELECT i, repeat('it''s \\ row ' || i || E'\\t', 20), i % 3 \
         FROM generate_series(1, 5000) i",
        &[],
    )
    .await?;
    ctx.cdc_decode_settle().await?;

    let query = "SELECT id, body FROM pc_many WHERE grp = 1 ORDER BY id";
    let expected = rows_text(&ctx.origin.simple_query(query).await.map_err(Error::other)?);
    assert_eq!(expected.len(), 1667);

    let m = ctx.metrics().await?;
    ctx.simple_query(query).await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    let hit = rows_text(&ctx.simple_query(query).await?);
    assert_cache_hit(&mut ctx, m).await?;
    assert_eq!(hit, expected, "cached result differs from origin");

    Ok(())
}

/// A LEFT JOIN whose optional side has a composite key and no match yields a
/// NULL-padded population row for that side. It must be filtered before the
/// staging COPY (staging keeps the key's NOT NULL), or the population fails and
/// the query never becomes servable.
#[tokio::test]
async fn test_population_copy_drops_null_padded_composite_key_rows() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.origin_query(
        "CREATE TABLE pc_orders (id INTEGER PRIMARY KEY, customer TEXT)",
        &[],
    )
    .await?;
    ctx.origin_query(
        "CREATE TABLE pc_lines (order_id INTEGER, line INTEGER, item TEXT, \
         PRIMARY KEY (order_id, line))",
        &[],
    )
    .await?;
    ctx.origin_query("INSERT INTO pc_orders VALUES (1, 'Alice'), (2, 'Bob')", &[])
        .await?;
    ctx.origin_query("INSERT INTO pc_lines VALUES (1, 1, 'Widget')", &[])
        .await?;
    ctx.cdc_decode_settle().await?;

    let query = "SELECT o.id, o.customer, l.line, l.item \
                 FROM pc_orders o LEFT JOIN pc_lines l ON l.order_id = o.id \
                 WHERE o.id > 0 ORDER BY o.id, l.line";
    let expected = rows_text(&ctx.origin.simple_query(query).await.map_err(Error::other)?);
    assert_eq!(expected.len(), 2);

    let m = ctx.metrics().await?;
    ctx.simple_query(query).await?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    let hit = rows_text(&ctx.simple_query(query).await?);
    assert_cache_hit(&mut ctx, m).await?;
    assert_eq!(hit, expected, "cached result differs from origin");

    Ok(())
}
