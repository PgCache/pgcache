use std::io::Error;

use crate::util::{TestContext, assert_cache_hit, assert_cache_miss, assert_row_at, metrics_delta};

mod util;

/// Test that tables in custom schemas are correctly resolved via search_path.
/// Creates a table in a non-public schema, sets search_path, and verifies
/// unqualified queries resolve to the correct schema.
#[tokio::test]
async fn test_search_path_custom_schema() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Must be done BEFORE creating the schema/table so pgcache picks it up
    ctx.query("SET search_path TO myapp_custom, public", &[])
        .await?;

    ctx.query("CREATE SCHEMA IF NOT EXISTS myapp_custom", &[])
        .await?;
    ctx.query(
        "CREATE TABLE myapp_custom.users (id integer primary key, name text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO myapp_custom.users (id, name) VALUES (1, 'Alice'), (2, 'Bob')",
        &[],
    )
    .await?;

    // First query with unqualified table name - should resolve to myapp_custom.users via search_path
    let m = ctx.metrics().await?;
    let res = ctx
        .simple_query("SELECT id, name FROM users WHERE id = 1")
        .await?;

    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("name", "Alice")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Second query - same query, should hit cache
    let res = ctx
        .simple_query("SELECT id, name FROM users WHERE id = 1")
        .await?;

    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("name", "Alice")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test that same table name in different schemas resolves correctly based on search_path order.
/// Creates tables with same name in two schemas, verifies search_path priority.
#[tokio::test]
async fn test_search_path_schema_priority() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Create two schemas with same table name but different data
    ctx.origin_query("CREATE SCHEMA schema_a_priority", &[])
        .await?;
    ctx.origin_query("CREATE SCHEMA schema_b_priority", &[])
        .await?;

    ctx.origin_query(
        "CREATE TABLE schema_a_priority.config (id integer primary key, value text)",
        &[],
    )
    .await?;
    ctx.origin_query(
        "CREATE TABLE schema_b_priority.config (id integer primary key, value text)",
        &[],
    )
    .await?;

    ctx.origin_query(
        "INSERT INTO schema_a_priority.config (id, value) VALUES (1, 'from_a')",
        &[],
    )
    .await?;
    ctx.origin_query(
        "INSERT INTO schema_b_priority.config (id, value) VALUES (1, 'from_b')",
        &[],
    )
    .await?;

    // Set search_path with schema_a_priority first
    ctx.query(
        "SET search_path TO schema_a_priority, schema_b_priority",
        &[],
    )
    .await?;

    // Query should resolve to schema_a_priority.config (first in search_path)
    let m = ctx.metrics().await?;
    let res = ctx
        .simple_query("SELECT id, value FROM config WHERE id = 1")
        .await?;

    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("value", "from_a")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Cache hit should return same result
    let res = ctx
        .simple_query("SELECT id, value FROM config WHERE id = 1")
        .await?;

    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("value", "from_a")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test that schema-qualified queries work regardless of search_path.
/// Even with search_path set to one schema, explicit qualification accesses other schema.
#[tokio::test]
async fn test_search_path_explicit_schema() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Create schema and table
    ctx.origin_query("CREATE SCHEMA hidden_explicit", &[])
        .await?;
    ctx.origin_query(
        "CREATE TABLE hidden_explicit.secrets (id integer primary key, data text)",
        &[],
    )
    .await?;
    ctx.origin_query(
        "INSERT INTO hidden_explicit.secrets (id, data) VALUES (1, 'secret_value')",
        &[],
    )
    .await?;

    // Set search_path to only public (hidden_explicit not included)
    ctx.query("SET search_path TO public", &[]).await?;

    // Schema-qualified query should still work
    let m = ctx.metrics().await?;
    let res = ctx
        .simple_query("SELECT id, data FROM hidden_explicit.secrets WHERE id = 1")
        .await?;

    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("data", "secret_value")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Cache hit with explicit schema
    let res = ctx
        .simple_query("SELECT id, data FROM hidden_explicit.secrets WHERE id = 1")
        .await?;

    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("data", "secret_value")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test that the default search_path ("$user", public) works correctly.
/// With default search_path, tables in public schema should be accessible.
#[tokio::test]
async fn test_search_path_default() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Create table in public schema (default)
    ctx.origin_query(
        "CREATE TABLE items_default (id integer primary key, name text)",
        &[],
    )
    .await?;
    ctx.origin_query(
        "INSERT INTO items_default (id, name) VALUES (1, 'widget'), (2, 'gadget')",
        &[],
    )
    .await?;

    // Default search_path should include public
    let m = ctx.metrics().await?;
    let res = ctx
        .simple_query("SELECT id, name FROM items_default WHERE id = 1")
        .await?;

    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("name", "widget")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Cache hit
    let res = ctx
        .simple_query("SELECT id, name FROM items_default WHERE id = 1")
        .await?;

    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("name", "widget")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test cache invalidation works correctly with custom schemas.
/// CDC events from tables in non-public schemas should invalidate cache.
#[tokio::test]
async fn test_search_path_cache_invalidation() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Create schema and table
    ctx.origin_query("CREATE SCHEMA app_invalidation", &[])
        .await?;
    ctx.origin_query(
        "CREATE TABLE app_invalidation.products (id integer primary key, price integer)",
        &[],
    )
    .await?;
    ctx.origin_query(
        "INSERT INTO app_invalidation.products (id, price) VALUES (1, 100), (2, 200)",
        &[],
    )
    .await?;

    ctx.query("SET search_path TO app_invalidation, public", &[])
        .await?;

    // Initial query
    let res = ctx
        .simple_query("SELECT id, price FROM products WHERE id = 1")
        .await?;

    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("price", "100")])?;

    ctx.cache_settle().await?;

    // Update via origin (simulating external change)
    ctx.origin_query(
        "UPDATE app_invalidation.products SET price = 150 WHERE id = 1",
        &[],
    )
    .await?;

    ctx.cdc_apply_settle().await?;

    // Query should return updated value (cache invalidated by CDC)
    let res = ctx
        .simple_query("SELECT id, price FROM products WHERE id = 1")
        .await?;

    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("price", "150")])?;

    Ok(())
}

/// Smoke test: a mid-connection `SET search_path` via simple-query marks the
/// path unknown; the settings probe injected at the SET's ReadyForQuery
/// re-reads it before the next cacheable query is keyed (on PG18 the
/// ParameterStatus does the same without a probe). The client sees a normal
/// SET response and subsequent queries work against the new schema.
#[tokio::test]
async fn test_search_path_dynamic_simple_query() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.origin_query("CREATE SCHEMA dyn_simple_a", &[]).await?;
    ctx.origin_query("CREATE SCHEMA dyn_simple_b", &[]).await?;
    ctx.origin_query(
        "CREATE TABLE dyn_simple_a.widgets (id integer primary key, label text)",
        &[],
    )
    .await?;
    ctx.origin_query(
        "CREATE TABLE dyn_simple_b.gadgets (id integer primary key, label text)",
        &[],
    )
    .await?;
    ctx.origin_query(
        "INSERT INTO dyn_simple_a.widgets (id, label) VALUES (1, 'widget_one')",
        &[],
    )
    .await?;
    ctx.origin_query(
        "INSERT INTO dyn_simple_b.gadgets (id, label) VALUES (1, 'gadget_one')",
        &[],
    )
    .await?;

    // Phase 1: search_path points at schema A; query the A-only table.
    ctx.simple_query("SET search_path TO dyn_simple_a").await?;
    let res = ctx
        .simple_query("SELECT id, label FROM widgets WHERE id = 1")
        .await?;
    assert_row_at(&res, 1, &[("id", "1"), ("label", "widget_one")])?;

    // Phase 2: the SET's response reaches the client unchanged, and the
    // B-only table resolves against the re-read path.
    ctx.simple_query("SET search_path TO dyn_simple_b").await?;
    let res = ctx
        .simple_query("SELECT id, label FROM gadgets WHERE id = 1")
        .await?;
    assert_row_at(&res, 1, &[("id", "1"), ("label", "gadget_one")])?;

    Ok(())
}

/// Same smoke test but the `SET search_path` goes through the extended
/// protocol (tokio-postgres `query`).
#[tokio::test]
async fn test_search_path_dynamic_extended_protocol() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.origin_query("CREATE SCHEMA dyn_ext_a", &[]).await?;
    ctx.origin_query("CREATE SCHEMA dyn_ext_b", &[]).await?;
    ctx.origin_query(
        "CREATE TABLE dyn_ext_a.widgets (id integer primary key, label text)",
        &[],
    )
    .await?;
    ctx.origin_query(
        "CREATE TABLE dyn_ext_b.gadgets (id integer primary key, label text)",
        &[],
    )
    .await?;
    ctx.origin_query(
        "INSERT INTO dyn_ext_a.widgets (id, label) VALUES (1, 'widget_one')",
        &[],
    )
    .await?;
    ctx.origin_query(
        "INSERT INTO dyn_ext_b.gadgets (id, label) VALUES (1, 'gadget_one')",
        &[],
    )
    .await?;

    ctx.query("SET search_path TO dyn_ext_a", &[]).await?;
    let res = ctx
        .simple_query("SELECT id, label FROM widgets WHERE id = 1")
        .await?;
    assert_row_at(&res, 1, &[("id", "1"), ("label", "widget_one")])?;

    ctx.query("SET search_path TO dyn_ext_b", &[]).await?;
    let res = ctx
        .simple_query("SELECT id, label FROM gadgets WHERE id = 1")
        .await?;
    assert_row_at(&res, 1, &[("id", "1"), ("label", "gadget_one")])?;

    Ok(())
}

/// `SET LOCAL search_path` inside a transaction reverts on ROLLBACK. The
/// proxy must re-sync search_path on txn end so post-rollback queries resolve
/// against the original search_path.
#[tokio::test]
async fn test_search_path_rollback_reverts_set_local() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.origin_query("CREATE SCHEMA txn_schema", &[]).await?;
    ctx.origin_query(
        "CREATE TABLE txn_schema.items (id integer primary key, label text)",
        &[],
    )
    .await?;
    ctx.origin_query(
        "CREATE TABLE public.items_txn_revert (id integer primary key, label text)",
        &[],
    )
    .await?;
    ctx.origin_query(
        "INSERT INTO txn_schema.items (id, label) VALUES (1, 'in_txn_schema')",
        &[],
    )
    .await?;
    ctx.origin_query(
        "INSERT INTO public.items_txn_revert (id, label) VALUES (1, 'in_public')",
        &[],
    )
    .await?;

    // Baseline: public is on the default search_path.
    let res = ctx
        .simple_query("SELECT id, label FROM items_txn_revert WHERE id = 1")
        .await?;
    assert_row_at(&res, 1, &[("id", "1"), ("label", "in_public")])?;

    // Inside a txn, SET LOCAL swaps the schema; query sees the overlay.
    ctx.simple_query("BEGIN").await?;
    ctx.simple_query("SET LOCAL search_path TO txn_schema")
        .await?;
    let res = ctx
        .simple_query("SELECT id, label FROM items WHERE id = 1")
        .await?;
    assert_row_at(&res, 1, &[("id", "1"), ("label", "in_txn_schema")])?;
    ctx.simple_query("ROLLBACK").await?;

    // After ROLLBACK, SET LOCAL has reverted. The original unqualified query
    // should again resolve against public.
    let res = ctx
        .simple_query("SELECT id, label FROM items_txn_revert WHERE id = 1")
        .await?;
    assert_row_at(&res, 1, &[("id", "1"), ("label", "in_public")])?;

    Ok(())
}

/// `DISCARD ALL` resets session state including search_path. The proxy must
/// drop its cached value and re-resolve.
#[tokio::test]
async fn test_search_path_discard_all_resets() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.origin_query("CREATE SCHEMA discard_schema", &[])
        .await?;
    ctx.origin_query(
        "CREATE TABLE discard_schema.items (id integer primary key, label text)",
        &[],
    )
    .await?;
    ctx.origin_query(
        "CREATE TABLE public.items_discard (id integer primary key, label text)",
        &[],
    )
    .await?;
    ctx.origin_query(
        "INSERT INTO discard_schema.items (id, label) VALUES (1, 'from_schema')",
        &[],
    )
    .await?;
    ctx.origin_query(
        "INSERT INTO public.items_discard (id, label) VALUES (1, 'from_public')",
        &[],
    )
    .await?;

    ctx.simple_query("SET search_path TO discard_schema")
        .await?;
    let res = ctx
        .simple_query("SELECT id, label FROM items WHERE id = 1")
        .await?;
    assert_row_at(&res, 1, &[("id", "1"), ("label", "from_schema")])?;

    // DISCARD ALL reverts session-level SET; search_path returns to default.
    ctx.simple_query("DISCARD ALL").await?;

    let res = ctx
        .simple_query("SELECT id, label FROM items_discard WHERE id = 1")
        .await?;
    assert_row_at(&res, 1, &[("id", "1"), ("label", "from_public")])?;

    Ok(())
}

/// Test join queries across tables in custom schemas.
#[tokio::test]
async fn test_search_path_join() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Create schema with related tables
    ctx.origin_query("CREATE SCHEMA store_join", &[]).await?;
    ctx.origin_query(
        "CREATE TABLE store_join.customers (id integer primary key, name text)",
        &[],
    )
    .await?;
    ctx.origin_query(
        "CREATE TABLE store_join.orders (id serial primary key, customer_id integer, total integer)",
        &[],
    )
    .await?;

    ctx.origin_query(
        "INSERT INTO store_join.customers (id, name) VALUES (1, 'Alice'), (2, 'Bob')",
        &[],
    )
    .await?;
    ctx.origin_query(
        "INSERT INTO store_join.orders (customer_id, total) VALUES (1, 100), (1, 200), (2, 150)",
        &[],
    )
    .await?;

    ctx.query("SET search_path TO store_join, public", &[])
        .await?;

    ctx.cdc_decode_settle().await?;

    // Join query with unqualified table names
    let m = ctx.metrics().await?;
    let res = ctx
        .simple_query(
            "SELECT c.id, c.name, o.total FROM customers c \
             JOIN orders o ON o.customer_id = c.id \
             WHERE c.id = 1 ORDER BY o.id",
        )
        .await?;

    assert_eq!(res.len(), 4); // 2 rows + CommandComplete + ReadyForQuery
    assert_row_at(&res, 1, &[("id", "1"), ("name", "Alice"), ("total", "100")])?;
    assert_row_at(&res, 2, &[("id", "1"), ("name", "Alice"), ("total", "200")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Cache hit
    let res = ctx
        .simple_query(
            "SELECT c.id, c.name, o.total FROM customers c \
             JOIN orders o ON o.customer_id = c.id \
             WHERE c.id = 1 ORDER BY o.id",
        )
        .await?;

    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("id", "1"), ("name", "Alice"), ("total", "100")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Create `<schema>.config (id, value)` with a single row `(1, <value>)`.
async fn config_table_create(
    ctx: &mut TestContext,
    schema: &str,
    value: &str,
) -> Result<(), Error> {
    ctx.origin_query(&format!("CREATE SCHEMA {schema}"), &[])
        .await?;
    ctx.origin_query(
        &format!("CREATE TABLE {schema}.config (id integer primary key, value text)"),
        &[],
    )
    .await?;
    ctx.origin_query(
        &format!("INSERT INTO {schema}.config (id, value) VALUES (1, '{value}')"),
        &[],
    )
    .await?;
    Ok(())
}

/// The same unqualified SQL under two search_paths names two different
/// tables, so it must land on two cache entries. Served from one entry it
/// would return the first schema's rows to the second session (PGC-405).
#[tokio::test]
async fn test_search_path_flip_serves_each_schema() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    config_table_create(&mut ctx, "flip_a", "from_a").await?;
    config_table_create(&mut ctx, "flip_b", "from_b").await?;
    let sql = "SELECT id, value FROM config WHERE id = 1";

    ctx.simple_query("SET search_path TO flip_a").await?;
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(sql).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("value", "from_a")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    let res = ctx.simple_query(sql).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("value", "from_a")])?;
    let m = assert_cache_hit(&mut ctx, m).await?;

    // Same text, other schema: a miss that registers a second entry, never a
    // hit on the first one.
    ctx.simple_query("SET search_path TO flip_b").await?;
    let res = ctx.simple_query(sql).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("value", "from_b")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    let res = ctx.simple_query(sql).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("value", "from_b")])?;
    let m = assert_cache_hit(&mut ctx, m).await?;

    // Back to the first schema: its entry is still there.
    ctx.simple_query("SET search_path TO flip_a").await?;
    let res = ctx.simple_query(sql).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("value", "from_a")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Two search_paths that resolve the table to the same schema share one
/// cache entry: the second path's first request is served from the entry
/// the first path populated (the alias is established by a writer resolve,
/// not a registration).
#[tokio::test]
async fn test_search_path_equivalent_paths_share_entry() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    config_table_create(&mut ctx, "shared_a", "from_a").await?;
    let sql = "SELECT id, value FROM config WHERE id = 1";

    ctx.simple_query("SET search_path TO shared_a").await?;
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(sql).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("value", "from_a")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    ctx.simple_query("SET search_path TO shared_a, public")
        .await?;
    let res = ctx.simple_query(sql).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("value", "from_a")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// A schema earlier on the path shadows a later one even when only the later
/// one's table is already cached: resolution must consult the catalog, not
/// the set of cache tables.
#[tokio::test]
async fn test_search_path_earlier_schema_shadows_cached_table() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.origin_query(
        "CREATE TABLE public.config (id integer primary key, value text)",
        &[],
    )
    .await?;
    ctx.origin_query(
        "INSERT INTO public.config (id, value) VALUES (1, 'from_public')",
        &[],
    )
    .await?;
    config_table_create(&mut ctx, "shadow_s", "from_s").await?;
    let sql = "SELECT id, value FROM config WHERE id = 1";

    ctx.simple_query("SET search_path TO public").await?;
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(sql).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("value", "from_public")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    ctx.simple_query("SET search_path TO shadow_s, public")
        .await?;
    let res = ctx.simple_query(sql).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("value", "from_s")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    let res = ctx.simple_query(sql).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("value", "from_s")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// A schema-qualified query does not depend on search_path, so it keeps one
/// entry across paths.
#[tokio::test]
async fn test_search_path_qualified_query_shares_entry_across_paths() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    config_table_create(&mut ctx, "qual_a", "from_a").await?;
    config_table_create(&mut ctx, "qual_b", "from_b").await?;
    let sql = "SELECT id, value FROM qual_a.config WHERE id = 1";

    ctx.simple_query("SET search_path TO qual_a").await?;
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(sql).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("value", "from_a")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;

    ctx.simple_query("SET search_path TO qual_b").await?;
    let res = ctx.simple_query(sql).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("value", "from_a")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// A path that lists `pg_catalog` after another schema lets a user object
/// shadow a builtin on origin, which the cache DB cannot reproduce. Such a
/// session forwards; moving `pg_catalog` first restores caching.
#[tokio::test]
async fn test_search_path_pg_catalog_after_schema_forwards() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    config_table_create(&mut ctx, "late_cat", "from_late").await?;
    let sql = "SELECT id, value FROM config WHERE id = 1";

    ctx.simple_query("SET search_path TO late_cat, pg_catalog")
        .await?;
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(sql).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("value", "from_late")])?;
    let after = ctx.metrics().await?;
    let delta = metrics_delta(&m, &after);
    assert_eq!(delta.queries_uncacheable, 1, "expected a forward");
    assert_eq!(delta.queries_cacheable, 0, "must not be counted cacheable");

    ctx.simple_query("SET search_path TO pg_catalog, late_cat")
        .await?;
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(sql).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("value", "from_late")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    let res = ctx.simple_query(sql).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("value", "from_late")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// The default path `"$user", public` spells the token quoted, and
/// PostgreSQL still expands it, so a schema named after the role shadows
/// `public`. The test role is `postgres`.
#[tokio::test]
async fn test_search_path_default_user_schema_precedes_public() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    ctx.origin_query(
        "CREATE TABLE public.config (id integer primary key, value text)",
        &[],
    )
    .await?;
    ctx.origin_query(
        "INSERT INTO public.config (id, value) VALUES (1, 'from_public')",
        &[],
    )
    .await?;
    config_table_create(&mut ctx, "postgres", "from_user").await?;
    let sql = "SELECT id, value FROM config WHERE id = 1";

    ctx.simple_query(r#"SET search_path TO "$user", public"#)
        .await?;
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(sql).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("value", "from_user")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    let res = ctx.simple_query(sql).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("value", "from_user")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}
