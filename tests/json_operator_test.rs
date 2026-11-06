//! The json/jsonb operators as opaque scalar operators (PGC-577): a predicate
//! over `->>`, `@>`, `?` and the rest caches, serves origin's rows, and CDC
//! keeps it in place by evaluating membership on the cache PG; `||`, which
//! has STABLE overloads, follows the SELECT-list rule for stable functions.

use std::io::Error;

use crate::util::{
    TestContext, assert_cache_hit, assert_cache_miss, assert_proxy_rows_match_origin,
    connect_cache_db, metrics_delta, origin_change_applied_in_place, query_warm,
};

mod util;

async fn docs_table_create(ctx: &mut TestContext) -> Result<(), Error> {
    ctx.query(
        "CREATE TABLE docs (id INTEGER PRIMARY KEY, data JSONB)",
        &[],
    )
    .await?;
    ctx.query(
        r#"INSERT INTO docs (id, data) VALUES
            (1, '{"kind": "invoice", "n": 10, "tags": ["red", "big"], "who": {"name": "ann"}}'),
            (2, '{"kind": "receipt", "n": 20, "tags": ["blue"], "who": {"name": "bob"}}'),
            (3, '{"kind": "invoice", "n": 30, "tags": ["red"], "2024": "yes"}'),
            (4, '{"kind": "memo", "n": 40}')"#,
        &[],
    )
    .await?;
    Ok(())
}

/// `data ->> 'k' = 'v'` is served from cache with origin's rows.
#[tokio::test]
async fn test_jsonb_key_equality_is_served_from_cache() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    docs_table_create(&mut ctx).await?;
    query_warm(
        &mut ctx,
        "SELECT id FROM docs WHERE data ->> 'kind' = 'invoice' ORDER BY id",
    )
    .await?;
    Ok(())
}

/// `data @> '{...}'` stands alone as a boolean predicate and is served from
/// cache with origin's rows.
#[tokio::test]
async fn test_jsonb_containment_is_served_from_cache() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    docs_table_create(&mut ctx).await?;
    query_warm(
        &mut ctx,
        r#"SELECT id FROM docs WHERE data @> '{"tags": ["red"]}' ORDER BY id"#,
    )
    .await?;
    Ok(())
}

/// Every operator of the family caches, in WHERE and in the SELECT list.
#[tokio::test]
async fn test_each_json_operator_caches() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    docs_table_create(&mut ctx).await?;
    for sql in [
        "SELECT id, data -> 'who' AS who FROM docs WHERE data -> 'who' IS NOT NULL ORDER BY id",
        "SELECT id FROM docs WHERE data #> '{who,name}' IS NOT NULL ORDER BY id",
        "SELECT id FROM docs WHERE data #>> '{who,name}' = 'ann' ORDER BY id",
        r#"SELECT id FROM docs WHERE data <@ '{"kind": "memo", "n": 40, "x": 1}' ORDER BY id"#,
        "SELECT id FROM docs WHERE data ? 'tags' ORDER BY id",
        "SELECT id FROM docs WHERE data ?| array['tags', 'who'] ORDER BY id",
        "SELECT id FROM docs WHERE data ?& array['tags', 'who'] ORDER BY id",
        "SELECT id FROM docs WHERE data @? '$.tags[*] ? (@ == \"red\")' ORDER BY id",
        "SELECT id, data #- '{who,name}' AS trimmed FROM docs WHERE id = 1",
        "SELECT id FROM docs WHERE (data ->> 'n')::int > 15 ORDER BY id",
    ] {
        query_warm(&mut ctx, sql).await?;
    }
    Ok(())
}

/// Insert, update and delete on the table reach the cached predicate in
/// place: the conjunct is opaque, so every change to the table is relevant
/// and membership is decided on the cache PG, never by invalidation.
#[tokio::test]
async fn test_cdc_changes_on_a_json_predicate_apply_in_place() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    docs_table_create(&mut ctx).await?;
    let sql = "SELECT id FROM docs WHERE data ->> 'kind' = 'invoice' ORDER BY id";
    let start = query_warm(&mut ctx, sql).await?;
    let mut m = start.clone();
    for change in [
        r#"INSERT INTO docs VALUES (5, '{"kind": "invoice"}'), (6, '{"kind": "memo"}')"#,
        r#"UPDATE docs SET data = data || '{"kind": "invoice"}' WHERE id = 2"#,
        r#"UPDATE docs SET data = data - 'kind' WHERE id = 1"#,
        "DELETE FROM docs WHERE id = 3",
    ] {
        m = origin_change_applied_in_place(&mut ctx, m, change, sql).await?;
    }
    let delta = metrics_delta(&start, &m);
    assert_eq!(delta.cache_invalidations, 0, "applied in place");
    assert!(
        delta.cache_cdc_pg_eval_hits >= 1,
        "membership is evaluated on the cache PG"
    );
    Ok(())
}

/// Two predicates on different keys are different entries: neither is an
/// exact description of its reference, so neither subsumes the other.
#[tokio::test]
async fn test_predicates_on_different_keys_each_register() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    docs_table_create(&mut ctx).await?;
    let m = query_warm(
        &mut ctx,
        "SELECT id FROM docs WHERE data ->> 'kind' = 'invoice' ORDER BY id",
    )
    .await?;
    let other = "SELECT id FROM docs WHERE data ->> 'who' = 'invoice' ORDER BY id";
    ctx.simple_query(other).await?;
    let after = assert_cache_miss(&mut ctx, m.clone()).await?;
    assert_eq!(metrics_delta(&m, &after).cache_subsumptions, 0);
    ctx.cache_settle().await?;
    assert_proxy_rows_match_origin(&mut ctx, other).await?;
    assert_cache_hit(&mut ctx, after).await?;
    Ok(())
}

/// `||` has STABLE overloads, so it follows the rule for stable functions:
/// allowed in the SELECT list, refused in WHERE.
#[tokio::test]
async fn test_concatenation_follows_the_stable_function_rule() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    docs_table_create(&mut ctx).await?;
    query_warm(
        &mut ctx,
        r#"SELECT id, data || '{"x": 1}' AS merged FROM docs WHERE id = 1"#,
    )
    .await?;

    let refused = r#"SELECT id FROM docs WHERE (data || '{"x": 1}') @> '{"x": 1}' ORDER BY id"#;
    let m = ctx.metrics().await?;
    assert_proxy_rows_match_origin(&mut ctx, refused).await?;
    let delta = metrics_delta(&m, &ctx.metrics().await?);
    assert_eq!(
        delta.queries_uncacheable, 1,
        "stable operator in WHERE forwards"
    );
    assert_eq!(delta.queries_cacheable, 0);
    Ok(())
}

/// A key that carries digits is text, not a date-time literal, so the entry
/// serves sessions on any date-time settings.
#[tokio::test]
async fn test_json_key_with_digits_serves_every_session() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    docs_table_create(&mut ctx).await?;
    let sql = "SELECT id FROM docs WHERE data ->> '2024' = 'yes' ORDER BY id";
    let m = query_warm(&mut ctx, sql).await?;
    ctx.simple_query("SET TimeZone = 'Asia/Tokyo'").await?;
    assert_proxy_rows_match_origin(&mut ctx, sql).await?;
    let after = assert_cache_hit(&mut ctx, m.clone()).await?;
    assert_eq!(metrics_delta(&m, &after).cache_reference_only_forwards, 0);
    Ok(())
}

/// A GIN index with a jsonb opclass on origin is recreated on the cache
/// table, so the opaque predicate runs against an index there too.
#[tokio::test]
async fn test_gin_index_is_mirrored_onto_the_cache_table() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    docs_table_create(&mut ctx).await?;
    ctx.query(
        "CREATE INDEX docs_data_idx ON docs USING gin (data jsonb_path_ops)",
        &[],
    )
    .await?;
    query_warm(
        &mut ctx,
        r#"SELECT id FROM docs WHERE data @> '{"kind": "memo"}' ORDER BY id"#,
    )
    .await?;

    let cache_db = connect_cache_db(&ctx.dbs).await?;
    let rows = cache_db
        .query(
            "SELECT pg_get_indexdef(ix.indexrelid) AS definition
             FROM pg_index ix JOIN pg_class t ON t.oid = ix.indrelid
             WHERE t.relname = 'docs' AND NOT ix.indisprimary",
            &[],
        )
        .await
        .map_err(Error::other)?;
    let definitions: Vec<String> = rows.iter().map(|row| row.get("definition")).collect();
    assert!(
        definitions
            .iter()
            .any(|definition| definition.contains("USING gin (data jsonb_path_ops)")),
        "gin index mirrored: {definitions:?}"
    );
    Ok(())
}
