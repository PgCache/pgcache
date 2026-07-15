//! Regression tests for PGC-262: identifiers that need quoting — mixed-case
//! names, lowercase reserved keywords, embedded quotes — must survive the full
//! cache pipeline: cache-table DDL, population (staging `LIKE` + merge),
//! serve-path deparse, and the CDC upsert/delete/truncate builders.

use std::io::Error;

use crate::util::{TestContext, assert_cache_hit, assert_cache_miss};

mod util;

/// Mixed-case table and column plus a reserved-word column, driven through
/// population and every CDC write kind.
#[tokio::test]
async fn test_mixed_case_identifiers_cdc_round_trip() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // `we""ird` (SQL for the identifier we"ird) exercises embedded-quote
    // doubling through DDL, population staging, and the CDC builders.
    ctx.query(
        "CREATE TABLE \"Order\" (id integer primary key, \"user\" text, \"camelCase\" integer, \"we\"\"ird\" text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO \"Order\" (id, \"user\", \"camelCase\", \"we\"\"ird\") VALUES (1, 'alice', 10, 'x'), (2, 'bob', 20, 'y')",
        &[],
    )
    .await?;

    let sql = "SELECT id, \"user\", \"camelCase\", \"we\"\"ird\" FROM \"Order\" ORDER BY id";

    // Cache miss — registration + population (staging LIKE, merge upsert).
    let m = ctx.metrics().await?;
    let rows = ctx.query(sql, &[]).await?;
    assert_eq!(rows.len(), 2);
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    // Cache hit — population succeeded despite the quoted identifiers.
    let rows = ctx.query(sql, &[]).await?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>("user"), "alice");
    let m = assert_cache_hit(&mut ctx, m).await?;

    // CDC INSERT — unconditional upsert builder.
    ctx.origin_query(
        "INSERT INTO \"Order\" (id, \"user\", \"camelCase\", \"we\"\"ird\") VALUES (3, 'carol', 30, 'z')",
        &[],
    )
    .await?;
    ctx.cdc_settle().await?;
    let rows = ctx.query(sql, &[]).await?;
    assert_eq!(rows.len(), 3, "CDC INSERT landed in cache");
    let m = assert_cache_hit(&mut ctx, m).await?;

    // CDC UPDATE — ON CONFLICT DO UPDATE SET with quoted column names.
    ctx.origin_query(
        "UPDATE \"Order\" SET \"user\" = 'carla', \"camelCase\" = 33 WHERE id = 3",
        &[],
    )
    .await?;
    ctx.cdc_settle().await?;
    let rows = ctx.query(sql, &[]).await?;
    assert_eq!(rows[2].get::<_, String>("user"), "carla");
    assert_eq!(rows[2].get::<_, i32>("camelCase"), 33);
    let m = assert_cache_hit(&mut ctx, m).await?;

    // CDC DELETE — PK-qualified delete builder.
    ctx.origin_query("DELETE FROM \"Order\" WHERE id = 2", &[])
        .await?;
    ctx.cdc_settle().await?;
    let rows = ctx.query(sql, &[]).await?;
    assert_eq!(rows.len(), 2, "CDC DELETE removed the row");
    let _m = assert_cache_hit(&mut ctx, m).await?;

    // CDC TRUNCATE — truncate_sql_build.
    ctx.origin_query("TRUNCATE \"Order\"", &[]).await?;
    ctx.cdc_settle().await?;
    let rows = ctx.query(sql, &[]).await?;
    assert_eq!(rows.len(), 0, "CDC TRUNCATE emptied the cached result");

    Ok(())
}

/// All-lowercase reserved keywords as table and column names — these pass
/// every character-class check and exercise the deparser's keyword list.
#[tokio::test]
async fn test_reserved_keyword_identifiers_round_trip() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE \"order\" (id integer primary key, \"select\" text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO \"order\" (id, \"select\") VALUES (1, 'x')",
        &[],
    )
    .await?;

    let sql = "SELECT id, \"select\" FROM \"order\" WHERE id = 1";

    let m = ctx.metrics().await?;
    let rows = ctx.query(sql, &[]).await?;
    assert_eq!(rows.len(), 1);
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    let rows = ctx.query(sql, &[]).await?;
    assert_eq!(rows[0].get::<_, String>("select"), "x");
    let m = assert_cache_hit(&mut ctx, m).await?;

    // CDC UPDATE with a reserved-word column in the SET list.
    ctx.origin_query("UPDATE \"order\" SET \"select\" = 'y' WHERE id = 1", &[])
        .await?;
    ctx.cdc_settle().await?;
    let rows = ctx.query(sql, &[]).await?;
    assert_eq!(rows[0].get::<_, String>("select"), "y");
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}
