//! The pattern and null-safe operators as opaque conjuncts (PGC-606): a
//! predicate over the regex operators (`~`, `~*`, `!~`, `!~*`), `SIMILAR TO`
//! and `IS [NOT] DISTINCT FROM` caches, serves origin's rows, and CDC keeps it
//! in place by evaluating membership on the cache PG, as `LIKE` already did.

use std::io::Error;

use crate::util::{TestContext, metrics_delta, origin_change_applied_in_place, query_warm};

mod util;

async fn notes_table_create(ctx: &mut TestContext) -> Result<(), Error> {
    ctx.query(
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT, tag TEXT)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO notes (id, body, tag) VALUES
            (1, 'alpha', 'hidden'),
            (2, 'Beta', NULL),
            (3, 'alpine', 'shown'),
            (4, 'gamma', 'shown')",
        &[],
    )
    .await?;
    Ok(())
}

/// Every operator of the family caches with origin's rows.
#[tokio::test]
async fn test_each_pattern_operator_caches() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    notes_table_create(&mut ctx).await?;
    for sql in [
        "SELECT id FROM notes WHERE body ~ '^al' ORDER BY id",
        "SELECT id FROM notes WHERE body ~* '^B' ORDER BY id",
        "SELECT id FROM notes WHERE body !~ '^al' ORDER BY id",
        "SELECT id FROM notes WHERE body !~* '^b' ORDER BY id",
        "SELECT id FROM notes WHERE body SIMILAR TO 'al%' ORDER BY id",
        "SELECT id FROM notes WHERE body NOT SIMILAR TO 'al%' ORDER BY id",
        "SELECT id FROM notes WHERE tag IS DISTINCT FROM 'hidden' ORDER BY id",
        "SELECT id FROM notes WHERE tag IS NOT DISTINCT FROM NULL ORDER BY id",
        "SELECT id FROM notes WHERE tag = 'shown' AND body ~ '^g' ORDER BY id",
    ] {
        query_warm(&mut ctx, sql).await?;
    }
    Ok(())
}

/// Insert, update and delete reach the cached regex predicate in place: the
/// conjunct is opaque, so membership is decided on the cache PG, never by
/// invalidation.
#[tokio::test]
async fn test_cdc_changes_on_a_regex_predicate_apply_in_place() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    notes_table_create(&mut ctx).await?;
    let sql = "SELECT id FROM notes WHERE body ~ '^al' ORDER BY id";
    let start = query_warm(&mut ctx, sql).await?;
    let mut m = start.clone();
    for change in [
        "INSERT INTO notes VALUES (5, 'alto', NULL), (6, 'delta', NULL)",
        "UPDATE notes SET body = 'album' WHERE id = 4",
        "UPDATE notes SET body = 'omega' WHERE id = 1",
        "DELETE FROM notes WHERE id = 3",
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

/// `IS DISTINCT FROM` treats NULL as a value: a NULL tag is distinct from
/// 'hidden' and stays in the result, and a row whose tag becomes NULL joins
/// it in place.
#[tokio::test]
async fn test_distinct_from_keeps_null_rows_in_place() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    notes_table_create(&mut ctx).await?;
    let sql = "SELECT id FROM notes WHERE tag IS DISTINCT FROM 'hidden' ORDER BY id";
    let start = query_warm(&mut ctx, sql).await?;
    let mut m = start.clone();
    for change in [
        "UPDATE notes SET tag = NULL WHERE id = 1",
        "UPDATE notes SET tag = 'hidden' WHERE id = 4",
    ] {
        m = origin_change_applied_in_place(&mut ctx, m, change, sql).await?;
    }
    assert_eq!(metrics_delta(&start, &m).cache_invalidations, 0);
    Ok(())
}
