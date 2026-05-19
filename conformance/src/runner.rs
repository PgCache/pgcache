//! Top-level orchestration: resolve the pgcache source, load fixtures,
//! parse `.slt` suites, and drive the per-statement
//! compare/route/cdc/log checks.
//!
//! Origin is the oracle. Inlined sqllogictest expectations are ignored;
//! every statement's behavior is defined by origin and pgcache is
//! required to match it, route as annotated, and log no swallowed error.

use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use sqllogictest::{DefaultColumnType, QueryExpect, Record, SortMode, parse_file};
use tokio::time::{Instant, sleep};

use crate::annotation::{self, Routing};
use crate::cdc_settling;
use crate::cli::{CacheSource, Cli};
use crate::compare::{SortStrategy, results_match};
use crate::drivers::{RunOutcome, SqlDriver};
use crate::fixtures;
use crate::log_tail::LogTailer;
use crate::report::{Bucket, Outcome, Report};
use crate::spawn::{SpawnOptions, SpawnedPgcache};
use crate::status_client::StatusClient;

/// The endpoints a run targets, regardless of how they were obtained.
struct ResolvedEndpoints {
    origin_url: String,
    cache_url: String,
    status_url: String,
    logs_file: Option<PathBuf>,
    publication: Option<String>,
}

pub async fn run(cli: Cli) -> Result<()> {
    let mut spawned: Option<SpawnedPgcache> = None;
    let ep = match cli.cache_source()? {
        CacheSource::External {
            origin_url,
            cache_url,
            status_url,
            logs_file,
            ..
        } => ResolvedEndpoints {
            origin_url,
            cache_url,
            status_url,
            logs_file,
            publication: cli.publication.clone(),
        },
        CacheSource::Ephemeral => {
            let sp = SpawnedPgcache::launch(SpawnOptions {
                pgcache_bin: cli.pgcache_bin.clone(),
                workers: cli.pgcache_workers,
                log_level: cli.pgcache_log_level.clone(),
                ready_timeout: Duration::from_secs(cli.spawn_timeout_secs),
            })
            .await
            .context("launching ephemeral pgcache")?;
            tracing::info!(
                origin = %sp.origin_url,
                cache = %sp.cache_url,
                status = %sp.status_url,
                logs = %sp.logs_file.display(),
                "ephemeral stack ready"
            );
            let ep = ResolvedEndpoints {
                origin_url: sp.origin_url.clone(),
                cache_url: sp.cache_url.clone(),
                status_url: sp.status_url.clone(),
                logs_file: Some(sp.logs_file.clone()),
                publication: Some(sp.publication.clone()),
            };
            spawned = Some(sp);
            ep
        }
    };

    let origin = SqlDriver::connect(&ep.origin_url, "origin").await?;
    let pgcache = SqlDriver::connect(&ep.cache_url, "pgcache").await?;
    origin.ping().await?;
    pgcache.ping().await?;
    tracing::info!("preflight ok: origin and pgcache reachable");

    fixtures::onek_load(origin.client(), ep.publication.as_deref())
        .await
        .context("loading onek fixture")?;
    fixtures::join_tables_load(origin.client(), ep.publication.as_deref())
        .await
        .context("loading join fixtures")?;
    fixtures::select_tables_load(origin.client(), ep.publication.as_deref())
        .await
        .context("loading select fixtures")?;

    let status = StatusClient::new(ep.status_url);
    let mut log_tailer = match &ep.logs_file {
        Some(p) => Some(LogTailer::open(p)?),
        None => {
            tracing::info!("no log source; swallowed-error detection (PGC-102 check) is disabled");
            None
        }
    };
    let cdc_timeout = Duration::from_secs(cli.cdc_timeout_secs);

    // The fixture COPY must reach pgcache before the first query, or it
    // diverges against an empty cache. Only meaningful when replication
    // is wired up (publication given).
    if ep.publication.is_some() {
        let target = origin
            .current_wal_lsn()
            .await
            .context("WAL LSN after fixture load")?;
        cdc_settling::settle(&status, target, cdc_timeout)
            .await
            .context("waiting for onek fixture to replicate to pgcache")?;
        tracing::info!("onek fixture replicated to pgcache");
    }

    let mut report = Report::new();
    let mut unhealthy_streak = 0u32;

    // No `--tests` → run every bundled suite.
    let tests_path = cli
        .tests
        .clone()
        .unwrap_or_else(|| Path::new(env!("CARGO_MANIFEST_DIR")).join("suites"));
    for file in suite_files(&tests_path)? {
        let control = run_file(
            &file,
            &origin,
            &pgcache,
            &status,
            log_tailer.as_mut(),
            cdc_timeout,
            &mut report,
            &mut unhealthy_streak,
        )
        .await
        .with_context(|| format!("running suite {}", file.display()))?;
        if control.is_break() {
            tracing::error!("aborting remaining suites: cache subsystem persistently unavailable");
            break;
        }
    }

    println!("{}", report.summary());
    if let Some(j) = &cli.junit {
        report.junit_write(j)?;
        tracing::info!(path = %j.display(), "wrote JUnit XML");
    }

    if let Some(sp) = spawned {
        sp.shutdown().await;
    }

    if report.failed() {
        bail!(
            "conformance failed: {} of {} statements",
            report.failures(),
            report.total()
        );
    }
    Ok(())
}

fn suite_files(path: &Path) -> Result<Vec<PathBuf>> {
    let meta = std::fs::metadata(path).with_context(|| format!("stat {}", path.display()))?;
    if meta.is_file() {
        return Ok(vec![path.to_path_buf()]);
    }
    let mut files: Vec<PathBuf> = std::fs::read_dir(path)
        .with_context(|| format!("read dir {}", path.display()))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "slt"))
        .collect();
    files.sort();
    Ok(files)
}

/// Consecutive cache-health failures (status unreachable, CDC- or
/// cache-settle timeout) before the run aborts. A pass or a plain
/// result/routing diff on a reachable cache resets the streak, so a
/// failing-but-up cache never trips this.
const UNHEALTHY_ABORT_THRESHOLD: u32 = 3;

/// Backoff between asserted-run retries while waiting for a `cached`
/// query's deferred registration/population to land (PGC-148).
const ROUTING_RETRY_BACKOFF: Duration = Duration::from_millis(50);

/// Count one more consecutive unhealthy query. Trips the breaker once
/// the cache has been unavailable for `UNHEALTHY_ABORT_THRESHOLD`
/// queries in a row: records a single `CacheUnavailable` marker and
/// signals the run to abort (so it still gets a summary + JUnit + clean
/// pgcache shutdown, rather than a raw `?` bail or per-query spinning).
fn breaker_bump(streak: &mut u32, report: &mut Report, suite: &str) -> ControlFlow<()> {
    *streak += 1;
    if *streak < UNHEALTHY_ABORT_THRESHOLD {
        return ControlFlow::Continue(());
    }
    report.record(Outcome {
        suite: suite.to_string(),
        statement: format!("<circuit breaker after {streak} unhealthy queries>"),
        bucket: Some(Bucket::CacheUnavailable),
        detail: "cache subsystem persistently unavailable; run aborted".to_string(),
    });
    ControlFlow::Break(())
}

/// A `/status` snapshot, or — when the cache is unreachable — a recorded
/// `CacheUnavailable` outcome plus the breaker's verdict (`Break` =
/// abort the run, `Continue` = skip the rest of this query).
fn snapshot_checked(
    snap: Result<crate::status_client::StatusSnapshot>,
    when: &str,
    suite: &str,
    sql: &str,
    report: &mut Report,
    streak: &mut u32,
) -> Result<crate::status_client::StatusSnapshot, ControlFlow<()>> {
    match snap {
        Ok(s) => Ok(s),
        Err(e) => {
            report.record(Outcome {
                suite: suite.to_string(),
                statement: sql.to_string(),
                bucket: Some(Bucket::CacheUnavailable),
                detail: format!("status {when}: {e:#}"),
            });
            Err(breaker_bump(streak, report, suite))
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_file(
    file: &Path,
    origin: &SqlDriver,
    pgcache: &SqlDriver,
    status: &StatusClient,
    mut log_tailer: Option<&mut LogTailer>,
    cdc_timeout: Duration,
    report: &mut Report,
    unhealthy_streak: &mut u32,
) -> Result<ControlFlow<()>> {
    let suite = file
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown")
        .to_string();
    let records: Vec<Record<DefaultColumnType>> =
        parse_file(file).with_context(|| format!("parsing {}", file.display()))?;

    let mut routing = Routing::Any;
    // LSN captured after the last mutation; the next query must settle to
    // it before pgcache is queried.
    let mut pending_lsn: Option<u64> = None;

    'records: for record in records {
        match record {
            Record::Comment(lines) => {
                routing = annotation::scan(lines.iter().map(String::as_str))
                    .with_context(|| format!("in {suite}"))?;
            }
            Record::Halt { .. } => break,
            Record::Statement { sql, .. } => {
                let o = origin.run(&sql).await;
                let c = pgcache.run(&sql).await;
                let (bucket, detail) = compare_statement(&o, &c);
                report.record(Outcome {
                    suite: suite.clone(),
                    statement: sql.clone(),
                    bucket,
                    detail,
                });
                // A DDL/DML statement produces WAL the next query depends
                // on; settle to here before reading through pgcache.
                pending_lsn = Some(
                    origin
                        .current_wal_lsn()
                        .await
                        .context("capturing WAL LSN after a statement")?,
                );
                routing = Routing::Any;
            }
            Record::Query { sql, expected, .. } => {
                if let Some(target) = pending_lsn.take()
                    && let Err(e) = cdc_settling::settle(status, target, cdc_timeout).await
                {
                    report.record(Outcome {
                        suite: suite.clone(),
                        statement: sql.clone(),
                        bucket: Some(Bucket::CdcTimeout),
                        detail: e.to_string(),
                    });
                    routing = Routing::Any;
                    if breaker_bump(unhealthy_streak, report, &suite).is_break() {
                        return Ok(ControlFlow::Break(()));
                    }
                    continue;
                }

                let oracle = origin.run(&sql).await;

                // `/status` must be reachable to verify routing and to
                // gate the cache-hit attempt on population. Persistent
                // unreachability is the dead / restart-looping-cache
                // signal the breaker trips on, instead of `?`-bailing
                // the whole run past the summary and JUnit output.
                let before = match snapshot_checked(
                    status.snapshot().await,
                    "before",
                    &suite,
                    &sql,
                    report,
                    unhealthy_streak,
                ) {
                    Ok(s) => s,
                    Err(cf) => {
                        routing = Routing::Any;
                        if cf.is_break() {
                            return Ok(ControlFlow::Break(()));
                        }
                        continue;
                    }
                };
                if let Some(t) = log_tailer.as_deref_mut() {
                    t.mark()?;
                }
                // First execution registers/populates the query; population
                // is async, so wait for it to settle before the cache-hit
                // attempt — otherwise a cached query races a cold-start
                // populate and looks like a routing miss.
                let _ = pgcache.run(&sql).await;
                let settle_failed = status.cache_settle(cdc_timeout).await.is_err();
                if settle_failed {
                    tracing::warn!("cache did not settle before hit attempt");
                }
                let mut actual = pgcache.run(&sql).await;
                let mut after = match snapshot_checked(
                    status.snapshot().await,
                    "after",
                    &suite,
                    &sql,
                    report,
                    unhealthy_streak,
                ) {
                    Ok(s) => s,
                    Err(cf) => {
                        routing = Routing::Any;
                        if cf.is_break() {
                            return Ok(ControlFlow::Break(()));
                        }
                        continue;
                    }
                };

                // `cache_settle` only sees registered queries, so a just-seen
                // `cached` query whose registration hasn't reached the writer
                // yet looks "settled" and the asserted run races population
                // (PGC-148). Re-run until the asserted hit lands, bounded by
                // the timeout; normally the first run already hit so this
                // never loops.
                if routing == Routing::Cached {
                    let deadline = Instant::now() + cdc_timeout;
                    while crate::routing_check::assert_routing(routing, &before, &after).is_err()
                        && Instant::now() < deadline
                    {
                        sleep(ROUTING_RETRY_BACKOFF).await;
                        actual = pgcache.run(&sql).await;
                        // `snapshot_checked`, not a silent break: a cache that
                        // goes unreachable mid-retry must feed the health
                        // breaker, not skip it and then hit the streak-reset
                        // below on a stale `after`.
                        after = match snapshot_checked(
                            status.snapshot().await,
                            "retry",
                            &suite,
                            &sql,
                            report,
                            unhealthy_streak,
                        ) {
                            Ok(s) => s,
                            Err(cf) => {
                                routing = Routing::Any;
                                if cf.is_break() {
                                    return Ok(ControlFlow::Break(()));
                                }
                                continue 'records;
                            }
                        };
                    }
                }

                let strategy = sort_strategy(&sql, &expected);
                let offending = match log_tailer.as_deref_mut() {
                    Some(t) => t.offending_since_mark()?,
                    None => Vec::new(),
                };
                let (bucket, detail) = evaluate_query(
                    &oracle, &actual, strategy, routing, &before, &after, &offending,
                );
                report.record(Outcome {
                    suite: suite.clone(),
                    statement: sql.clone(),
                    bucket,
                    detail,
                });
                routing = Routing::Any;

                // A timed-out cache-settle (cache reachable but a query
                // stuck mid-population, e.g. a restart-looping writer) is
                // a health signal even if the result happened to match. A
                // reachable cache that merely produced a wrong/forwarded
                // result is not — resetting the streak keeps genuine
                // diffs from masquerading as unavailability.
                if settle_failed {
                    if breaker_bump(unhealthy_streak, report, &suite).is_break() {
                        return Ok(ControlFlow::Break(()));
                    }
                } else {
                    *unhealthy_streak = 0;
                }
            }
            _ => {}
        }
    }
    Ok(ControlFlow::Continue(()))
}

/// Compare a non-result statement (DDL/DML) across the two engines.
/// Compare origin (oracle) vs pgcache outcomes. `Ok(())` = match;
/// `Err(detail)` = a result-diff. `strategy` applies only when both
/// sides returned a result set.
fn outcomes_compare(o: &RunOutcome, c: &RunOutcome, strategy: SortStrategy) -> Result<(), String> {
    match (o, c) {
        (RunOutcome::Error(a), RunOutcome::Error(b)) if a == b => Ok(()),
        (RunOutcome::Error(a), RunOutcome::Error(b)) => {
            Err(format!("error mismatch: origin {a}, pgcache {b}"))
        }
        (RunOutcome::Error(a), _) => Err(format!("origin errored ({a}); pgcache did not")),
        (_, RunOutcome::Error(b)) => Err(format!("pgcache errored ({b}); origin did not")),
        (RunOutcome::Query(o), RunOutcome::Query(c)) => results_match(o, c, strategy),
        (RunOutcome::Statement { .. }, RunOutcome::Statement { .. }) => Ok(()),
        _ => Err("origin/pgcache returned different result kinds".to_string()),
    }
}

fn compare_statement(o: &RunOutcome, c: &RunOutcome) -> (Option<Bucket>, String) {
    match outcomes_compare(o, c, SortStrategy::Rows) {
        Ok(()) => (None, "ok".to_string()),
        Err(detail) => (Some(Bucket::ResultDiff), detail),
    }
}

/// Evaluate a query: result diff (vs origin), swallowed log error, then
/// routing. A swallowed error fails even when the result matched — the
/// PGC-102 case.
fn evaluate_query(
    oracle: &RunOutcome,
    actual: &RunOutcome,
    strategy: SortStrategy,
    routing: Routing,
    before: &crate::status_client::StatusSnapshot,
    after: &crate::status_client::StatusSnapshot,
    offending_log: &[String],
) -> (Option<Bucket>, String) {
    let result = outcomes_compare(oracle, actual, strategy);
    if let Err(detail) = result {
        return (Some(Bucket::ResultDiff), detail);
    }

    if !offending_log.is_empty() {
        return (
            Some(Bucket::SwallowedError),
            format!(
                "result matched origin but pgcache logged: {}",
                offending_log.join(" | ")
            ),
        );
    }

    if let Err(e) = crate::routing_check::assert_routing(routing, before, after) {
        return (Some(Bucket::RoutingMismatch), e.to_string());
    }

    (None, "ok".to_string())
}

/// Pick the comparison strategy. Honor an explicit sqllogictest sort
/// mode; otherwise a query with `ORDER BY` is compared row-wise and one
/// without falls back to a sorted multiset (hash-mode equivalent).
fn sort_strategy(sql: &str, expected: &QueryExpect<DefaultColumnType>) -> SortStrategy {
    let sort_mode = match expected {
        QueryExpect::Results { sort_mode, .. } => *sort_mode,
        QueryExpect::Error(_) => None,
    };
    match sort_mode {
        Some(SortMode::RowSort) => SortStrategy::Rows,
        Some(SortMode::ValueSort) => SortStrategy::Values,
        Some(SortMode::NoSort) => SortStrategy::None,
        _ => {
            if sql.to_lowercase().contains("order by") {
                SortStrategy::None
            } else {
                SortStrategy::Rows
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_breaker_trips_only_after_threshold() {
        let mut report = Report::new();
        let mut streak = 0u32;
        for _ in 0..UNHEALTHY_ABORT_THRESHOLD - 1 {
            assert!(breaker_bump(&mut streak, &mut report, "s").is_continue());
        }
        assert_eq!(report.total(), 0, "no marker recorded before the threshold");

        assert!(breaker_bump(&mut streak, &mut report, "s").is_break());
        assert_eq!(streak, UNHEALTHY_ABORT_THRESHOLD);
        assert_eq!(report.failures(), 1, "one CacheUnavailable marker on trip");
    }
}
