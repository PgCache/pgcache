//! The future-merge race (PGC-272, found during PGC-264 validation):
//! PGC-250 Slice B gates a populated query's *Ready* on the CDC
//! apply watermark reaching its snapshot LSN, but the population's *merge*
//! applies to the shared cache table immediately. A merged row that was
//! created at origin after the watermark (its INSERT/PK-update CDC event
//! still queued) is therefore visible to every OTHER Ready query over the
//! relation the moment it lands — mixing snapshot-time state into results
//! that are otherwise watermark-consistent, and breaking source-transaction
//! atomicity in the served view.
//!
//! Construction: a fault-injected per-message CDC delivery delay holds the
//! watermark several seconds behind origin. One origin transaction updates
//! row A and inserts row B; a second query's population (snapshotted after
//! the transaction) merges B into the shared table inside that window. The
//! first query — Ready, never invalidated — must not serve B alongside the
//! pre-transaction A.
//!
//! Fault-dependent — gated like `population_cdc_consistency_test.rs`.
#![cfg(feature = "fault-injection")]

use std::io::Error;
use std::time::{Duration, Instant};

use tokio_postgres::SimpleQueryMessage;

use crate::util::TestContext;

mod util;

/// Per-message CDC delivery delay. The origin transaction below decodes as
/// several replication messages (Begin/Update/Insert/Commit), so the apply
/// watermark trails origin by a multiple of this — the window in which the
/// future merge must surface to a Ready query.
const CDC_DELAY_MS: &str = "1000";

/// `(id, v)` pairs of a served result.
fn id_v_rows(msgs: &[SimpleQueryMessage]) -> Vec<(String, String)> {
    msgs.iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some((
                row.get(0).unwrap_or_default().to_owned(),
                row.get(1).unwrap_or_default().to_owned(),
            )),
            SimpleQueryMessage::CommandComplete(_) | SimpleQueryMessage::RowDescription(_) | _ => {
                None
            }
        })
        .collect()
}

#[tokio::test]
async fn test_population_merge_does_not_expose_future_rows() -> Result<(), Error> {
    let mut ctx =
        TestContext::setup_fault(&[("PGCACHE_FAULT_CDC_DELIVER_DELAY_MS", CDC_DELAY_MS)]).await?;

    ctx.simple_query(
        "create table merge_race (id int primary key, grp int not null, v int not null)",
    )
    .await?;
    ctx.simple_query("insert into merge_race (id, grp, v) values (1, 1, 1)")
        .await?;
    // The delivery delay is inactive until a cached query tracks the relation.
    ctx.cdc_settle().await?;

    // Q1: cached and Ready. Registering it arms the CDC delivery delay.
    let q1 = "select id, v from merge_race where grp = 1 order by id";
    ctx.simple_query(q1).await?;
    ctx.cache_settle_with_timeout(Duration::from_secs(15))
        .await?;

    // One origin transaction: update row A, create row B. Its CDC frame is
    // delayed several seconds; until it applies, the watermark-consistent
    // state is {A@v1}.
    ctx.origin
        .batch_execute(
            "BEGIN; \
             update merge_race set v = 2 where id = 1; \
             insert into merge_race (id, grp, v) values (2, 1, 2); \
             COMMIT;",
        )
        .await
        .map_err(Error::other)?;

    // Q2 over the new row: its population snapshots post-transaction origin
    // and merges row B into the shared table within ~hundreds of ms. Q2
    // itself is Ready-gated on the watermark — but Q1 is not.
    ctx.simple_query("select v from merge_race where id = 2")
        .await?;

    // Poll Q1 until row B appears in its served result, then hold it to
    // source-transaction atomicity: any result containing B must also show
    // A at v=2. With the ungated merge, B appears within the delay window
    // next to the pre-transaction A@v1 — the torn serve under test. Once the
    // merge is watermark-gated, B first appears only after the frame applied,
    // by which time A is v=2 in the same result.
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let before = ctx.metrics().await?;
        let served = ctx.simple_query(q1).await?;
        let after = ctx.metrics().await?;
        let rows = id_v_rows(&served);
        if rows.iter().any(|(id, _)| id == "2") {
            let a = rows
                .iter()
                .find(|(id, _)| id == "1")
                .expect("row A present in the group result");
            assert_eq!(
                a.1, "2",
                "torn serve: merged future row B visible while A still shows the \
                 pre-transaction value (source-transaction atomicity broken in a \
                 Ready query's served view)"
            );
            // The torn-or-atomic read must have come from cache — a forwarded
            // read is origin-consistent by construction and would make this
            // test pass without exercising the merge at all.
            assert_eq!(
                after.queries_cache_hit - before.queries_cache_hit,
                1,
                "the observed read was not a cache hit; Q1 lost Ready and the \
                 scenario no longer exercises the shared-table merge"
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "row B never appeared in Q1's served result"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Ok(())
}
