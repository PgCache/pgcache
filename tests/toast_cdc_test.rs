//! Unchanged-toast CDC handling (PGC-264).
//!
//! Under `REPLICA IDENTITY DEFAULT`, pgoutput elides unchanged TOASTed column
//! values from UPDATE new-row images (an `UnchangedToast` marker instead of
//! the bytes). pgcache must complete the image from the cache-table row
//! (repair) or conservatively invalidate — never write the hole as NULL into
//! the shared cache table, where population merges (which never overwrite)
//! would freeze the corruption.
//!
//! The TOAST setup: `STORAGE EXTERNAL` disables compression so any value past
//! the ~2KB TOAST threshold is stored out-of-line deterministically.

use std::io::Error;
use std::time::Duration;

use tokio_postgres::SimpleQueryMessage;

use crate::util::{TestContext, assert_cache_hit};

mod util;

/// Comfortably past the ~2KB TOAST threshold; EXTERNAL storage skips
/// compression so the value is out-of-line regardless of content.
const TOAST_LEN: usize = 8000;

fn big_value(fill: char) -> String {
    std::iter::repeat_n(fill, TOAST_LEN).collect()
}

fn row_count(msgs: &[SimpleQueryMessage]) -> usize {
    msgs.iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .count()
}

/// First row's first column value.
fn first_value(msgs: &[SimpleQueryMessage]) -> Option<String> {
    msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
        SimpleQueryMessage::CommandComplete(_) | SimpleQueryMessage::RowDescription(_) | _ => None,
    })
}

/// First row's first two column values.
fn first_two_values(msgs: &[SimpleQueryMessage]) -> Option<(String, String)> {
    msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(row) => Some((
            row.get(0).unwrap_or_default().to_owned(),
            row.get(1).unwrap_or_default().to_owned(),
        )),
        SimpleQueryMessage::CommandComplete(_) | SimpleQueryMessage::RowDescription(_) | _ => None,
    })
}

/// `a` and `b` are both out-of-line; the dual-column shape exercises repairs
/// where one toasted update inline-rewrites a column the next one elides.
async fn toast_two_column_table_create(ctx: &mut TestContext, table: &str) -> Result<(), Error> {
    ctx.simple_query(&format!(
        "create table {table} (id int primary key, a text, b text, n int)"
    ))
    .await?;
    ctx.simple_query(&format!(
        "alter table {table} alter column a set storage external, \
         alter column b set storage external"
    ))
    .await?;
    Ok(())
}

async fn toast_table_create(ctx: &mut TestContext, table: &str) -> Result<(), Error> {
    ctx.simple_query(&format!(
        "create table {table} (id int primary key, big text, n int, status text)"
    ))
    .await?;
    ctx.simple_query(&format!(
        "alter table {table} alter column big set storage external"
    ))
    .await?;
    Ok(())
}

/// The core data-loss case: an UPDATE of a non-TOAST column must not overwrite
/// the cached TOAST value with NULL. The cached row is the repair source, so
/// the query stays cached (no invalidation) and serves the intact value.
#[tokio::test]
async fn test_unchanged_toast_preserved_on_update() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    toast_table_create(&mut ctx, "toast_t").await?;

    let big = big_value('x');
    ctx.simple_query(&format!(
        "insert into toast_t (id, big, n, status) values (1, '{big}', 1, 'active')"
    ))
    .await?;
    ctx.cdc_settle().await?;

    let q = "select big, n from toast_t where id = 1";
    ctx.simple_query(q).await?;
    ctx.cache_settle().await?;

    // The toast column is unchanged → elided from the CDC new-row image.
    ctx.origin_query("update toast_t set n = 2 where id = 1", &[])
        .await?;
    ctx.cdc_settle().await?;

    // No cache_settle between the update and the read: an invalidation would
    // surface as a miss here, so the hit assertion also proves the repair
    // path maintained the query in place.
    let before = ctx.metrics().await?;
    let served = ctx.simple_query(q).await?;
    assert_cache_hit(&mut ctx, before).await?;
    assert_eq!(
        first_value(&served).as_deref(),
        Some(big.as_str()),
        "cached TOAST value corrupted by an unchanged-toast UPDATE"
    );

    Ok(())
}

/// An UPDATE that flips an uncached row into a query's result set while
/// carrying an unchanged-toast column has nothing to repair from — the
/// incomplete row must never reach the cache; the query invalidates and
/// repopulates with the real value.
#[tokio::test]
async fn test_unchanged_toast_uncached_row_entering_predicate() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    toast_table_create(&mut ctx, "toast_e").await?;

    let big = big_value('x');
    ctx.simple_query(&format!(
        "insert into toast_e (id, big, n, status) values (1, '{big}', 1, 'inactive')"
    ))
    .await?;
    ctx.cdc_settle().await?;

    // Caches zero rows: id=1 is 'inactive', so it is absent from the cache table.
    let q = "select big from toast_e where status = 'active'";
    let initial = ctx.simple_query(q).await?;
    assert_eq!(row_count(&initial), 0);
    ctx.cache_settle().await?;

    // Row enters the predicate; `big` is unchanged → elided → unrepairable.
    ctx.origin_query("update toast_e set status = 'active' where id = 1", &[])
        .await?;
    ctx.cdc_settle().await?;

    // Forward-or-hit, either must carry the real value.
    let served = ctx.simple_query(q).await?;
    assert_eq!(first_value(&served).as_deref(), Some(big.as_str()));

    // After repopulation the cache hit must serve the real value too.
    ctx.cache_settle().await?;
    let before = ctx.metrics().await?;
    let served = ctx.simple_query(q).await?;
    assert_cache_hit(&mut ctx, before).await?;
    assert_eq!(
        first_value(&served).as_deref(),
        Some(big.as_str()),
        "repopulated cache serves a corrupted TOAST value"
    );

    Ok(())
}

/// A predicate over the TOASTed column itself: with the value elided the
/// predicate can't be evaluated, so the query must invalidate rather than
/// evaluate against NULL. Results must stay correct throughout.
#[tokio::test]
async fn test_unchanged_toast_predicate_on_toast_column() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    toast_table_create(&mut ctx, "toast_p").await?;

    let big = big_value('x');
    ctx.simple_query(&format!(
        "insert into toast_p (id, big, n, status) values (1, '{big}', 1, 'inactive')"
    ))
    .await?;
    ctx.cdc_settle().await?;

    let q = "select id from toast_p where big like 'x%' and status = 'active'";
    let initial = ctx.simple_query(q).await?;
    assert_eq!(row_count(&initial), 0);
    ctx.cache_settle().await?;

    ctx.origin_query("update toast_p set status = 'active' where id = 1", &[])
        .await?;
    ctx.cdc_settle().await?;

    let served = ctx.simple_query(q).await?;
    assert_eq!(
        row_count(&served),
        1,
        "row entering the predicate lost (toast-column predicate evaluated against NULL)"
    );

    ctx.cache_settle().await?;
    let served = ctx.simple_query(q).await?;
    assert_eq!(row_count(&served), 1);

    Ok(())
}

/// In-batch staleness guard: when one source transaction rewrites the TOAST
/// value and then updates another column, the second event's unchanged-toast
/// marker refers to the NEW value — repairing it from the pre-batch committed
/// cache image would resurrect the old one. The dirty-PK guard must route the
/// second event to invalidation instead.
#[tokio::test]
async fn test_unchanged_toast_same_transaction_rewrite() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    toast_table_create(&mut ctx, "toast_b").await?;

    let old = big_value('x');
    let new = big_value('y');
    ctx.simple_query(&format!(
        "insert into toast_b (id, big, n, status) values (1, '{old}', 1, 'active')"
    ))
    .await?;
    ctx.cdc_settle().await?;

    let q = "select big from toast_b where id = 1";
    ctx.simple_query(q).await?;
    ctx.cache_settle().await?;

    ctx.origin
        .batch_execute(&format!(
            "BEGIN; \
             update toast_b set big = '{new}' where id = 1; \
             update toast_b set n = 2 where id = 1; \
             COMMIT;"
        ))
        .await
        .map_err(Error::other)?;
    ctx.cdc_settle().await?;

    let served = ctx.simple_query(q).await?;
    assert_eq!(
        first_value(&served).as_deref(),
        Some(new.as_str()),
        "second in-transaction update repaired from the stale pre-transaction image"
    );

    ctx.cache_settle().await?;
    let before = ctx.metrics().await?;
    let served = ctx.simple_query(q).await?;
    assert_cache_hit(&mut ctx, before).await?;
    assert_eq!(first_value(&served).as_deref(), Some(new.as_str()));

    Ok(())
}

/// Same guard for relation-level churn: a TRUNCATE + reinsert + update in one
/// source transaction leaves no trustworthy pre-batch image at all.
#[tokio::test]
async fn test_unchanged_toast_truncate_insert_update_same_transaction() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    toast_table_create(&mut ctx, "toast_tr").await?;

    let old = big_value('x');
    let new = big_value('z');
    ctx.simple_query(&format!(
        "insert into toast_tr (id, big, n, status) values (1, '{old}', 1, 'active')"
    ))
    .await?;
    ctx.cdc_settle().await?;

    let q = "select big from toast_tr where id = 1";
    ctx.simple_query(q).await?;
    ctx.cache_settle().await?;

    ctx.origin
        .batch_execute(&format!(
            "BEGIN; \
             truncate toast_tr; \
             insert into toast_tr (id, big, n, status) values (1, '{new}', 1, 'active'); \
             update toast_tr set n = 2 where id = 1; \
             COMMIT;"
        ))
        .await
        .map_err(Error::other)?;
    ctx.cdc_settle().await?;

    let served = ctx.simple_query(q).await?;
    assert_eq!(
        first_value(&served).as_deref(),
        Some(new.as_str()),
        "post-truncate update repaired from the pre-truncate image"
    );

    Ok(())
}

/// Two toasted updates of the same row in one source transaction: the first
/// rewrites `a` inline while `b` is elided; the second elides both. Neither
/// has an overlay entry when it queues, so the second's repair must chain
/// through the first's post-image — repairing it from the pre-batch image
/// would resurrect the old `a`.
#[tokio::test]
async fn test_unchanged_toast_two_toasted_updates_same_transaction() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    toast_two_column_table_create(&mut ctx, "toast_q").await?;

    let a_old = big_value('x');
    let a_new = big_value('y');
    let b = big_value('z');
    ctx.simple_query(&format!(
        "insert into toast_q (id, a, b, n) values (1, '{a_old}', '{b}', 1)"
    ))
    .await?;
    ctx.cdc_settle().await?;

    let q = "select a, b from toast_q where id = 1";
    ctx.simple_query(q).await?;
    ctx.cache_settle().await?;

    let before = ctx.metrics().await?;
    ctx.origin
        .batch_execute(&format!(
            "BEGIN; \
             update toast_q set a = '{a_new}' where id = 1; \
             update toast_q set n = 2 where id = 1; \
             COMMIT;"
        ))
        .await
        .map_err(Error::other)?;
    ctx.cdc_settle().await?;

    let read_before = ctx.metrics().await?;
    let served = ctx.simple_query(q).await?;
    let after = assert_cache_hit(&mut ctx, read_before).await?;
    let (a_served, b_served) = first_two_values(&served).expect("row served");
    assert_eq!(
        a_served, a_new,
        "second toasted update repaired `a` from the pre-batch image instead of \
         the first update's post-image"
    );
    assert_eq!(b_served, b, "unchanged TOAST column corrupted");
    assert_eq!(
        after.cache_invalidations, before.cache_invalidations,
        "repair chain fell back to invalidation"
    );

    Ok(())
}

/// Cross-replay-chunk variant: a filler relation's bulk update forces a
/// mid-frame replay split (FRAME_ROWS_CAPACITY) between two toasted updates
/// of the same row, so the second repairs from the batch overlay entry the
/// first replay recorded. That entry must hold the first update's repaired
/// post-image, not the pre-batch committed image.
#[tokio::test]
async fn test_unchanged_toast_repair_across_replay_chunks() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    toast_two_column_table_create(&mut ctx, "toast_x").await?;
    ctx.simple_query("create table toast_filler (id int primary key, v int not null)")
        .await?;
    // Enough rows that one bulk update overflows FRAME_ROWS_CAPACITY (4096),
    // splitting the frame into multiple replays.
    ctx.simple_query("insert into toast_filler select g, 0 from generate_series(1, 4200) g")
        .await?;

    let a_old = big_value('x');
    let a_new = big_value('y');
    let b = big_value('z');
    ctx.simple_query(&format!(
        "insert into toast_x (id, a, b, n) values (1, '{a_old}', '{b}', 1)"
    ))
    .await?;
    ctx.cdc_settle().await?;

    let q = "select a, b from toast_x where id = 1";
    ctx.simple_query(q).await?;
    // Track the filler relation so its events flow through the writer.
    ctx.simple_query("select v from toast_filler where id = 1")
        .await?;
    ctx.cache_settle().await?;

    let before = ctx.metrics().await?;
    ctx.origin
        .batch_execute(&format!(
            "BEGIN; \
             update toast_x set a = '{a_new}' where id = 1; \
             update toast_filler set v = v + 1; \
             update toast_x set n = 2 where id = 1; \
             COMMIT;"
        ))
        .await
        .map_err(Error::other)?;
    ctx.cdc_settle_with_timeout(Duration::from_secs(30)).await?;

    let read_before = ctx.metrics().await?;
    let served = ctx.simple_query(q).await?;
    let after = assert_cache_hit(&mut ctx, read_before).await?;
    let (a_served, b_served) = first_two_values(&served).expect("row served");
    assert_eq!(
        a_served, a_new,
        "overlay entry recorded the pre-batch image; later replay chunk \
         repaired from stale values"
    );
    assert_eq!(b_served, b, "unchanged TOAST column corrupted");
    assert_eq!(
        after.cache_invalidations, before.cache_invalidations,
        "repair chain fell back to invalidation"
    );

    Ok(())
}

/// A PK-changing toasted update followed by a toasted update of the new PK,
/// in one transaction. The second can only repair through the pass-2 chain:
/// the new PK has no pre-batch row and no overlay entry. Falling back would
/// needlessly invalidate the query.
#[tokio::test]
async fn test_unchanged_toast_pk_change_then_toasted_update() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    toast_table_create(&mut ctx, "toast_pc").await?;

    let big = big_value('x');
    ctx.simple_query(&format!(
        "insert into toast_pc (id, big, n, status) values (1, '{big}', 1, 'active')"
    ))
    .await?;
    ctx.cdc_settle().await?;

    let q = "select big from toast_pc where status = 'active'";
    ctx.simple_query(q).await?;
    ctx.cache_settle().await?;

    let before = ctx.metrics().await?;
    ctx.origin
        .batch_execute(
            "BEGIN; \
             update toast_pc set id = 2 where id = 1; \
             update toast_pc set n = 2 where id = 2; \
             COMMIT;",
        )
        .await
        .map_err(Error::other)?;
    ctx.cdc_settle().await?;

    let read_before = ctx.metrics().await?;
    let served = ctx.simple_query(q).await?;
    let after = assert_cache_hit(&mut ctx, read_before).await?;
    assert_eq!(
        first_value(&served).as_deref(),
        Some(big.as_str()),
        "TOAST value lost across the PK-change repair chain"
    );
    assert_eq!(
        after.cache_invalidations, before.cache_invalidations,
        "repair chain fell back to invalidation"
    );

    Ok(())
}
