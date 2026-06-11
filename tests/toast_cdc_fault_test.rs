//! Unchanged-toast under PGC-261's tracking-active update-out path (PGC-264).
//!
//! While a population holds deleted-key tracking open, an update-out upserts
//! the row's new version instead of deleting it. With an unchanged-toast
//! column that upsert must carry the repaired value: a NULL-holed row in the
//! shared table is unrepairable (population merges never overwrite existing
//! rows), so a later query over the new version would serve NULL forever.
//!
//! Fault-dependent (population delay keeps tracking open deterministically) —
//! gated like `population_cdc_consistency_test.rs`.
#![cfg(feature = "fault-injection")]

use std::io::Error;
use std::time::Duration;

use tokio_postgres::SimpleQueryMessage;

use crate::util::{TestContext, assert_cache_hit};

mod util;

const TOAST_LEN: usize = 8000;

fn first_value(msgs: &[SimpleQueryMessage]) -> Option<String> {
    msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
        SimpleQueryMessage::CommandComplete(_) | SimpleQueryMessage::RowDescription(_) | _ => None,
    })
}

fn row_count(msgs: &[SimpleQueryMessage]) -> usize {
    msgs.iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .count()
}

#[tokio::test]
async fn test_unchanged_toast_update_out_during_tracking_preserved() -> Result<(), Error> {
    let mut ctx =
        TestContext::setup_fault(&[("PGCACHE_FAULT_POPULATION_DELAY_ONCE_MS", "4000")]).await?;

    let big: String = std::iter::repeat_n('x', TOAST_LEN).collect();
    ctx.simple_query("create table toast_uo (id int primary key, big text, status text not null)")
        .await?;
    ctx.simple_query("alter table toast_uo alter column big set storage external")
        .await?;
    ctx.simple_query(&format!(
        "insert into toast_uo (id, big, status) values (1, '', 'guard'), (2, '{big}', 'active')"
    ))
    .await?;
    ctx.cdc_settle().await?;

    // Guard query: its (one-shot-delayed) population keeps deleted-key
    // tracking active for the relation across the whole scenario.
    ctx.simple_query("select id from toast_uo where status = 'guard'")
        .await?;
    // The active-row query caches id=2 (with its TOAST value) undelayed.
    // Read until it's a cache hit so the row is provably in the cache table
    // before the update — that pins the repair (not the not-found fallback)
    // path, which is the one under test. Must resolve well inside the guard's
    // 4s tracking window.
    let qa = "select big from toast_uo where status = 'active'";
    ctx.simple_query(qa).await?;
    let mut qa_cached = false;
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let before = ctx.metrics().await?;
        ctx.simple_query(qa).await?;
        let after = ctx.metrics().await?;
        if after.queries_cache_hit > before.queries_cache_hit {
            qa_cached = true;
            break;
        }
    }
    assert!(qa_cached, "active-row query never cached within 3s");
    ctx.cdc_settle().await?;

    // Update-out with `big` unchanged → elided. Tracking is active, so the
    // PGC-261 branch upserts the new version — it must carry the repaired
    // TOAST value into the shared table.
    ctx.origin_query("update toast_uo set status = 'archived' where id = 2", &[])
        .await?;
    ctx.cdc_settle().await?;

    // A later query over the new version populates while the guard is still
    // in flight; its merge never overwrites the upserted row, so a NULL hole
    // written by the update-out would be frozen here. The settle must outlast
    // the guard's injected 4s population delay, which the 5s default doesn't
    // under parallel-suite load.
    let q2 = "select big from toast_uo where status = 'archived'";
    ctx.simple_query(q2).await?;
    ctx.cache_settle_with_timeout(Duration::from_secs(20))
        .await?;

    let before = ctx.metrics().await?;
    let served = ctx.simple_query(q2).await?;
    assert_cache_hit(&mut ctx, before).await?;
    assert_eq!(
        first_value(&served).as_deref(),
        Some(big.as_str()),
        "update-out under tracking froze a corrupted TOAST value into the shared table"
    );

    // Control: the old version must not be served to the original query.
    // No hit assertion — if timing pushed the update onto the conservative
    // fallback (row not yet cached at apply time), `qa` was legitimately
    // invalidated and this read forwards; serving the stale row is the only
    // failure.
    let served = ctx.simple_query(qa).await?;
    assert_eq!(
        row_count(&served),
        0,
        "stale old version served after update-out"
    );

    Ok(())
}
