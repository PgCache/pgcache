//! Top-level orchestration: resolve the pgcache source, load fixtures,
//! parse `.slt` suites, and drive the per-statement
//! compare/route/cdc/log checks.
//!
//! Origin is the oracle. Inlined sqllogictest expectations are ignored;
//! every statement's behavior is defined by origin and pgcache is
//! required to match it, route as annotated, and log no swallowed error.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use sqllogictest::{DefaultColumnType, QueryExpect, Record, SortMode, parse_file};

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

    for file in suite_files(&cli.tests)? {
        run_file(
            &file,
            &origin,
            &pgcache,
            &status,
            log_tailer.as_mut(),
            cdc_timeout,
            &mut report,
        )
        .await
        .with_context(|| format!("running suite {}", file.display()))?;
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

#[allow(clippy::too_many_arguments)]
async fn run_file(
    file: &Path,
    origin: &SqlDriver,
    pgcache: &SqlDriver,
    status: &StatusClient,
    mut log_tailer: Option<&mut LogTailer>,
    cdc_timeout: Duration,
    report: &mut Report,
) -> Result<()> {
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

    for record in records {
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
                    continue;
                }

                let oracle = origin.run(&sql).await;
                let before = status.snapshot().await.context("status before")?;
                if let Some(t) = log_tailer.as_deref_mut() {
                    t.mark()?;
                }
                // First execution registers/populates the query; population
                // is async, so wait for it to settle before the cache-hit
                // attempt — otherwise a cached query races a cold-start
                // populate and looks like a routing miss.
                let _ = pgcache.run(&sql).await;
                if let Err(e) = status.cache_settle(cdc_timeout).await {
                    tracing::warn!(error = %e, "cache did not settle before hit attempt");
                }
                let actual = pgcache.run(&sql).await;
                let after = status.snapshot().await.context("status after")?;

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
            }
            _ => {}
        }
    }
    Ok(())
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
