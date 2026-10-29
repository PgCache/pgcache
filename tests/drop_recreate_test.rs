//! Dropping and recreating a joined table under the same name while cached
//! join queries over it are in flight. The drop reaches the writer as an
//! event-trigger message at its position in the source transaction
//! (ADR-058); the recreated table is a new OID that pgcache knows nothing
//! about until a registration reloads it from the origin catalog.
#![cfg(feature = "fault-injection")]

use std::collections::BTreeMap;
use std::io::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use tokio_postgres::error::SqlState;

use crate::util::{TestContext, assert_proxy_rows_match_origin, metrics_delta, result_rows};

mod util;

const JOIN: &str = "select b.id, a.name, b.title from authors a \
                    join books b on b.author_id = a.id where a.id <= 10 order by b.id";
// The `b.id > 0` in the ON gives this join a shape of its own, so the
// cached JOIN does not subsume it and its registration populates.
const JOIN_AUTHOR_2: &str = "select b.id, a.name, b.title from authors a \
                             join books b on b.author_id = a.id and b.id > 0 \
                             where a.id = 2 order by b.id";

const LAG: Duration = Duration::from_millis(1500);
const LAG_ENV: (&str, &str) = ("PGCACHE_FAULT_CDC_APPLY_LAG_MS", "1500");

const BOOKS_CREATE: &str =
    "create table books (id integer primary key, author_id integer, title text)";

async fn tables_create(ctx: &mut TestContext) -> Result<(), Error> {
    for statement in [
        "create table authors (id integer primary key, name text)",
        BOOKS_CREATE,
        "insert into authors (id, name) values (1, 'ann'), (2, 'bob')",
        "insert into books (id, author_id, title) values (1, 1, 'old-1'), (2, 2, 'old-2')",
    ] {
        ctx.query(statement, &[]).await?;
    }
    ctx.cdc_apply_settle().await
}

/// Miss, settle, then a hit that matches origin.
async fn join_cache(ctx: &mut TestContext, query: &str) -> Result<(), Error> {
    ctx.simple_query(query).await?;
    ctx.cache_settle().await?;
    let m = ctx.metrics().await?;
    assert_proxy_rows_match_origin(ctx, query).await?;
    let delta = metrics_delta(&m, &ctx.metrics().await?);
    assert_eq!(delta.queries_cache_hit, 1, "{query} cached before the drop");
    Ok(())
}

/// After the signal has applied and repopulation finished, every read
/// matches origin and the writer never restarted.
async fn converged_assert(ctx: &mut TestContext, queries: &[&str]) -> Result<(), Error> {
    ctx.cdc_apply_settle_with_timeout(Duration::from_secs(10))
        .await?;
    for query in queries {
        assert_proxy_rows_match_origin(ctx, query).await?;
    }
    ctx.cache_settle_with_timeout(Duration::from_secs(10))
        .await?;
    for query in queries {
        let m = ctx.metrics().await?;
        assert_proxy_rows_match_origin(ctx, query).await?;
        let delta = metrics_delta(&m, &ctx.metrics().await?);
        assert_eq!(delta.queries_cache_hit, 1, "{query} cached again");
    }
    assert_eq!(
        ctx.metrics().await?.cache_restarts_total,
        0,
        "writer must survive the drop and recreate"
    );
    Ok(())
}

/// Read `query` repeatedly while the drop signal is still held by the CDC
/// lag. Its registration and population run against the writer's pre-drop
/// catalog entry, so every read must match origin, whether it was served
/// from cache or forwarded.
async fn lag_window_reads_assert(ctx: &mut TestContext, query: &str) -> Result<(), Error> {
    let m = ctx.metrics().await?;
    let window_end = Instant::now() + LAG / 4 * 3;
    while Instant::now() < window_end {
        assert_proxy_rows_match_origin(ctx, query).await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // The scenario is a population under the dropped table's metadata; a
    // cover from the lagging parent would skip it.
    let delta = metrics_delta(&m, &ctx.metrics().await?);
    assert_eq!(
        delta.cache_subsumptions, 0,
        "{query} populated, not covered"
    );
    Ok(())
}

/// The join's first population has read origin and is held before loading
/// the cache when `books` is dropped and recreated with different rows. The
/// population's snapshot predates the drop, so it must not land.
#[tokio::test]
async fn test_drop_recreate_during_join_population() -> Result<(), Error> {
    let mut ctx =
        TestContext::setup_fault(&[("PGCACHE_FAULT_POPULATION_DELAY_ONCE_MS", "1500")]).await?;
    tables_create(&mut ctx).await?;

    ctx.simple_query(JOIN).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let started = Instant::now();
    ctx.origin
        .batch_execute(&format!(
            "begin; drop table books; {BOOKS_CREATE}; \
             insert into books (id, author_id, title) values (1, 1, 'new-1'), (3, 2, 'new-3'); \
             commit;"
        ))
        .await
        .map_err(Error::other)?;
    eprintln!("drop/recreate committed after {:?}", started.elapsed());

    converged_assert(&mut ctx, &[JOIN]).await
}

/// The drop signal is held back by CDC lag while a second join over `books`
/// registers: the writer's catalog still holds the dropped table's metadata,
/// and the population reads the recreated table. Every read in the lag
/// window must still match origin; nothing may serve a result built from a
/// mix of the old and new table.
#[tokio::test]
async fn test_drop_recreate_with_registration_in_lag_window() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&[LAG_ENV]).await?;
    tables_create(&mut ctx).await?;
    join_cache(&mut ctx, JOIN).await?;

    ctx.origin
        .batch_execute(&format!(
            "begin; drop table books; {BOOKS_CREATE}; \
             insert into books (id, author_id, title) values (1, 2, 'new-1'), (3, 2, 'new-3'); \
             commit;"
        ))
        .await
        .map_err(Error::other)?;

    lag_window_reads_assert(&mut ctx, JOIN_AUTHOR_2).await?;

    converged_assert(&mut ctx, &[JOIN, JOIN_AUTHOR_2]).await
}

/// As above, but the recreated `books` has a different layout: reordered and
/// added columns, a widened type, and a composite primary key under which
/// two rows share `id`. A population run under the dropped table's metadata
/// (keyed on `id` alone) would collapse them.
#[tokio::test]
async fn test_drop_recreate_with_new_layout_in_lag_window() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&[LAG_ENV]).await?;
    tables_create(&mut ctx).await?;
    join_cache(&mut ctx, JOIN).await?;

    ctx.origin
        .batch_execute(
            "begin; drop table books; \
             create table books (extra integer default 7, title text, author_id bigint, \
                                 id integer, primary key (author_id, id)); \
             insert into books (id, author_id, title) values (1, 1, 'new-1a'), (1, 2, 'new-1b'), (3, 2, 'new-3'); \
             commit;",
        )
        .await
        .map_err(Error::other)?;

    lag_window_reads_assert(&mut ctx, JOIN_AUTHOR_2).await?;

    converged_assert(&mut ctx, &[JOIN, JOIN_AUTHOR_2]).await
}

/// The session that drops and recreates the table reads the join straight
/// after, under CDC lag: the read-after-write gate must forward it rather
/// than serve the cached result over the dropped table.
#[tokio::test]
async fn test_drop_recreate_read_after_write_same_session() -> Result<(), Error> {
    let mut ctx =
        TestContext::setup_fault(&[("PGCACHE_FAULT_READ_YOUR_WRITES_OFF", "0"), LAG_ENV]).await?;
    tables_create(&mut ctx).await?;
    join_cache(&mut ctx, JOIN).await?;

    ctx.simple_query(&format!(
        "begin; drop table books; {BOOKS_CREATE}; \
         insert into books (id, author_id, title) values (1, 1, 'new-1'), (3, 2, 'new-3'); \
         commit;"
    ))
    .await?;
    assert_proxy_rows_match_origin(&mut ctx, JOIN).await?;

    converged_assert(&mut ctx, &[JOIN]).await
}

const STRESS_READERS: usize = 4;
const STRESS_ROUNDS: usize = 20;

/// Round `round` holds books `1..=books_in_round(round)`, every title
/// tagged with the round.
fn books_in_round(round: usize) -> usize {
    round % 3 + 2
}

/// A result is consistent when it is exactly one round's books.
fn stress_result_check(rows: &[Vec<Option<String>>]) -> Result<usize, String> {
    let rounds: Vec<usize> = rows
        .iter()
        .map(|row| {
            row.get(2)
                .and_then(Option::as_deref)
                .and_then(|title| title.split('-').next())
                .and_then(|tag| tag.strip_prefix('r'))
                .and_then(|n| n.parse().ok())
                .ok_or_else(|| format!("unexpected row {row:?}"))
        })
        .collect::<Result<_, _>>()?;
    let Some(&round) = rounds.first() else {
        return Err("empty result".to_owned());
    };
    if rounds.iter().any(|r| *r != round) || rows.len() != books_in_round(round) {
        return Err(format!("torn result {rows:?}"));
    }
    Ok(round)
}

/// How a round replaces `books` on origin.
#[derive(Clone, Copy, PartialEq, Eq)]
enum RoundShape {
    /// Drop, recreate and refill in one transaction: origin never shows the
    /// table missing.
    OneTransaction,
    /// The drop commits on its own, then the recreate and refill commit
    /// together: between them origin reports the table missing.
    SeparateTransactions,
}

fn books_round_fill_sql(round: usize) -> String {
    let values: Vec<String> = (1..=books_in_round(round))
        .map(|id| format!("({id}, {}, 'r{round}-{id}')", id % 2 + 1))
        .collect();
    format!(
        "{BOOKS_CREATE}; insert into books (id, author_id, title) values {};",
        values.join(", ")
    )
}

/// The origin batches for one round, in order.
fn stress_round_sql(round: usize, shape: RoundShape) -> Vec<String> {
    let fill = books_round_fill_sql(round);
    match shape {
        RoundShape::OneTransaction => vec![format!("begin; drop table books; {fill} commit;")],
        RoundShape::SeparateTransactions => {
            vec![
                "drop table books".to_owned(),
                format!("begin; {fill} commit;"),
            ]
        }
    }
}

#[derive(Default, Debug)]
struct ReaderStats {
    reads: usize,
    rounds_seen: BTreeMap<usize, usize>,
    undefined_table: usize,
    errors: Vec<String>,
}

/// Readers hammer the join from their own sessions while the table is
/// dropped and recreated round after round. No read may fail or return a
/// result that mixes rounds, and once the stream settles the cache must
/// converge on the last round.
#[tokio::test]
async fn test_drop_recreate_stress_with_concurrent_join_readers() -> Result<(), Error> {
    drop_recreate_stress(&[], RoundShape::OneTransaction).await
}

/// The same under CDC lag: readers are served rounds behind origin while
/// several drop signals queue up behind each other.
#[tokio::test]
async fn test_drop_recreate_stress_under_cdc_lag() -> Result<(), Error> {
    drop_recreate_stress(&[LAG_ENV], RoundShape::OneTransaction).await
}

/// The drop and the recreate commit separately, so the drop signal arrives
/// in a frame of its own and readers race a window where the table does not
/// exist on origin. The only failure allowed is origin's own `42P01` for that
/// window; afterwards the join must cache again, not stay stuck on the
/// failed registrations from the missing-table window.
#[tokio::test]
async fn test_drop_recreate_stress_separate_transactions() -> Result<(), Error> {
    drop_recreate_stress(&[], RoundShape::SeparateTransactions).await
}

/// The same under CDC lag: drop-only and recreate frames queue up behind
/// each other and apply as a backlog.
#[tokio::test]
async fn test_drop_recreate_stress_separate_transactions_under_cdc_lag() -> Result<(), Error> {
    drop_recreate_stress(&[LAG_ENV], RoundShape::SeparateTransactions).await
}

async fn drop_recreate_stress(env: &[(&str, &str)], shape: RoundShape) -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(env).await?;
    ctx.origin
        .batch_execute(
            "create table authors (id integer primary key, name text); \
             insert into authors (id, name) values (1, 'ann'), (2, 'bob');",
        )
        .await
        .map_err(Error::other)?;
    ctx.origin
        .batch_execute(&books_round_fill_sql(0))
        .await
        .map_err(Error::other)?;
    ctx.cdc_apply_settle().await?;
    join_cache(&mut ctx, JOIN).await?;

    let before = ctx.metrics().await?;
    let stop = Arc::new(AtomicBool::new(false));
    let mut readers = Vec::with_capacity(STRESS_READERS);
    for _ in 0..STRESS_READERS {
        let client = ctx.proxy_client_connect().await?;
        let stop = Arc::clone(&stop);
        readers.push(tokio::spawn(async move {
            let mut stats = ReaderStats::default();
            while !stop.load(Ordering::Relaxed) {
                stats.reads += 1;
                match client.simple_query(JOIN).await {
                    Ok(messages) => match stress_result_check(&result_rows(&messages)) {
                        Ok(round) => *stats.rounds_seen.entry(round).or_default() += 1,
                        Err(e) => stats.errors.push(e),
                    },
                    Err(e)
                        if shape == RoundShape::SeparateTransactions
                            && e.code() == Some(&SqlState::UNDEFINED_TABLE) =>
                    {
                        stats.undefined_table += 1;
                    }
                    Err(e) => stats.errors.push(format!(
                        "{:?}: {e}",
                        e.as_db_error().map(|db| db.code().code().to_owned())
                    )),
                }
            }
            stats
        }));
    }

    for round in 1..=STRESS_ROUNDS {
        for batch in stress_round_sql(round, shape) {
            ctx.origin
                .batch_execute(&batch)
                .await
                .map_err(Error::other)?;
            if shape == RoundShape::SeparateTransactions {
                tokio::time::sleep(Duration::from_millis(15)).await;
            }
        }
        tokio::time::sleep(Duration::from_millis((round as u64 * 37) % 120)).await;
    }
    // Under lag the drop signals are still queued here: keep reading while
    // they apply.
    ctx.cdc_apply_settle_with_timeout(Duration::from_secs(10))
        .await?;
    ctx.cache_settle_with_timeout(Duration::from_secs(10))
        .await?;
    stop.store(true, Ordering::Relaxed);

    let mut errors = Vec::new();
    for reader in readers {
        let stats = reader.await.map_err(Error::other)?;
        eprintln!(
            "reader: {} reads, rounds seen {:?}, {} undefined_table, {} errors",
            stats.reads,
            stats.rounds_seen,
            stats.undefined_table,
            stats.errors.len()
        );
        errors.extend(stats.errors);
    }
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    eprintln!(
        "stress: hits={} misses={} cache_errors={} invalidations={}",
        delta.queries_cache_hit,
        delta.queries_cache_miss,
        delta.queries_cache_error,
        delta.cache_invalidations
    );
    errors.dedup();
    assert!(errors.is_empty(), "reader failures: {errors:#?}");

    converged_assert(&mut ctx, &[JOIN]).await?;
    let res = result_rows(&ctx.simple_query(JOIN).await?);
    assert_eq!(stress_result_check(&res), Ok(STRESS_ROUNDS));
    Ok(())
}
