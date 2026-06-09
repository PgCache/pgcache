//! Orchestration: spawn the stack, seed, drive load, snapshot-and-check, then
//! verify cache == origin once the dust settles.

use std::ops::ControlFlow;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use pgcache_conformance::cdc_settling::{lsn_parse, settle};
use pgcache_conformance::poll::poll_until;
use pgcache_conformance::spawn::{SpawnOptions, SpawnedPgcache};
use pgcache_conformance::status_client::StatusClient;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_postgres::Client;

use crate::cli::Cli;
use crate::db;
use crate::invariants::{MonotonicTracker, intra_snapshot_reduce, pair_check};
use crate::scenario::Scenario;
use crate::snapshot;
use crate::workload::{OpCounts, reader_task, writer_task};

const SETTLE_TIMEOUT: Duration = Duration::from_secs(60);
const READY_TIMEOUT: Duration = Duration::from_secs(60);
/// How long the final cache-vs-origin check polls for convergence. Transient
/// lag (CDC mid-apply, writer queues draining, pending merge/ready flush)
/// resolves in well under a second; a persistent divergence is a real bug.
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(20);

pub async fn run(cli: Cli) -> Result<()> {
    if cli.groups < 1 {
        bail!("--groups must be at least 1");
    }
    if cli.pairs < 1 {
        bail!("--pairs must be at least 1");
    }

    let seed = cli.seed.unwrap_or_else(rand::random::<u64>);
    let scenario = Scenario::new(cli.scenario, cli.groups, cli.pairs);
    tracing::info!(
        seed,
        scenario = ?cli.scenario,
        groups = cli.groups,
        pairs = cli.pairs,
        rows_per_group = cli.rows_per_group,
        writers = cli.writers,
        readers = cli.readers,
        duration_secs = cli.duration_secs,
        "starting consistency stress run (use --seed {seed} to reproduce)"
    );

    let stack = SpawnedPgcache::launch(SpawnOptions {
        pgcache_bin: cli.pgcache_bin.clone(),
        workers: cli.workers,
        log_level: cli.log_level.clone(),
        ready_timeout: READY_TIMEOUT,
    })
    .await
    .context("launching pgcache stack")?;

    let status = StatusClient::new(stack.status_url.clone());
    let origin = db::connect(&stack.origin_url).await?;
    let proxy = db::connect(&stack.cache_url).await?;

    scenario
        .provision(&origin, cli.rows_per_group)
        .await
        .context("provisioning schema")?;

    // The SQL each read uses, resolved once for the active scenario.
    let per_group_sql = scenario.per_group_select();
    let pair_sql = scenario.pair_select();
    let cross_sql = scenario.cross_group_select(snapshot::PROBE_DATA_HI);
    let full_sql = scenario.full_table_select();
    let all_groups = scenario.model.all_groups();

    // Let CDC catch up with the seed, then warm the snapshot queries so the
    // loop reads cache hits.
    settle(&status, origin_lsn(&origin).await?, SETTLE_TIMEOUT)
        .await
        .context("settling after seed")?;
    // Deliberately do NOT warm the per-group queries: they must populate
    // *during* load so their population races the concurrent removals — that
    // race is the bug under test. Warming them would make them cache hits
    // before any write lands and mask it. Only the maintained-in-place probe
    // is warmed.
    snapshot::cross_group_probe(&proxy, &cross_sql).await?;
    status
        .cache_settle(SETTLE_TIMEOUT)
        .await
        .context("warming probe query")?;

    // Pool connects only now: it prepares the per-group query, which provision
    // created the tables for above.
    let reader =
        snapshot::GroupReader::connect(&stack.cache_url, cli.snapshot_conns, &per_group_sql).await?;

    let deadline = Instant::now() + Duration::from_secs(cli.duration_secs);

    let mut writer_handles: Vec<JoinHandle<Result<OpCounts>>> = Vec::new();
    for i in 0..cli.writers {
        writer_handles.push(tokio::spawn(writer_task(
            stack.cache_url.clone(),
            scenario.clone(),
            seed ^ (0x1000 + i as u64),
            cli.write_think_ms,
            cli.bump_groups,
            deadline,
        )));
    }
    let mut reader_handles: Vec<JoinHandle<Result<u64>>> = Vec::new();
    for i in 0..cli.readers {
        reader_handles.push(tokio::spawn(reader_task(
            stack.cache_url.clone(),
            scenario.clone(),
            seed ^ (0x2000 + i as u64),
            deadline,
        )));
    }

    let loop_result = snapshot_loop(
        &reader,
        &proxy,
        &scenario,
        &all_groups,
        &pair_sql,
        &cross_sql,
        deadline,
        cli.snapshot_interval_ms,
    )
    .await;

    if loop_result.is_err() {
        for h in &writer_handles {
            h.abort();
        }
        for h in &reader_handles {
            h.abort();
        }
    }

    // Task errors only count when the loop succeeded: a loop failure aborts
    // the tasks, so their cancellation errors are expected noise.
    let mut task_error: Option<anyhow::Error> = None;
    let mut counts = OpCounts::default();
    for h in writer_handles {
        match h.await {
            Ok(Ok(c)) => counts.merge(c),
            Ok(Err(e)) if loop_result.is_ok() => {
                task_error.get_or_insert(e.context("writer task"));
            }
            Ok(Err(_)) | Err(_) => {}
        }
    }
    let mut reads = 0u64;
    for h in reader_handles {
        match h.await {
            Ok(Ok(n)) => reads += n,
            Ok(Err(e)) if loop_result.is_ok() => {
                task_error.get_or_insert(e.context("reader task"));
            }
            Ok(Err(_)) | Err(_) => {}
        }
    }

    let snapshots = loop_result?;
    if let Some(e) = task_error {
        stack.shutdown().await;
        return Err(e);
    }

    // Final convergence: cache must equal origin once everything drains.
    let check = EqualityCheck {
        metrics_url: &stack.metrics_url,
        status: &status,
        origin: &origin,
        proxy: &proxy,
        reader: &reader,
        per_group_sql: &per_group_sql,
        full_sql: &full_sql,
        all_groups: &all_groups,
    };
    let final_rows = match equality_converge(&check).await {
        Ok(n) => n,
        Err(e) => {
            stack.shutdown().await;
            return Err(e);
        }
    };

    tracing::info!(
        snapshots,
        reads,
        writes = counts.total(),
        version_bumps = counts.version_bump,
        cross_group_txns = counts.cross_group_txn,
        pk_updates = counts.pk_update,
        inserts = counts.insert,
        deletes = counts.delete,
        final_rows,
        "consistency stress run passed"
    );

    stack.shutdown().await;
    Ok(())
}

/// Snapshot-and-check until `deadline`, returning the number of snapshots taken
/// or the first invariant violation.
#[allow(clippy::too_many_arguments)]
async fn snapshot_loop(
    reader: &snapshot::GroupReader,
    proxy: &Client,
    scenario: &Scenario,
    groups: &[i32],
    pair_sql: &str,
    cross_sql: &str,
    deadline: Instant,
    interval_ms: u64,
) -> Result<u64> {
    let interval = Duration::from_millis(interval_ms);
    let pair_ids: Vec<i32> = scenario.model.pairs.iter().flat_map(|&(a, b)| [a, b]).collect();
    let mut tracker = MonotonicTracker::new();
    let mut snapshots = 0u64;

    while Instant::now() < deadline {
        // Per-group reads (one query per group, each atomic), fanned across the
        // connection pool: intra-group atomicity and per-group monotonicity.
        let rows = reader.read(groups).await?;
        let group_versions = snapshot::group_versions(&rows);
        let group_versions = match intra_snapshot_reduce(&group_versions) {
            Ok(map) => map,
            Err(v) => return violation_fail(v),
        };
        if let Some(v) = tracker.observe(&group_versions) {
            return violation_fail(v);
        }

        // Paired groups read in one atomic query: cross-group frame atomicity.
        let pair_rows = snapshot::pair_versions(proxy, pair_sql, &pair_ids).await?;
        let pair_map = match intra_snapshot_reduce(&pair_rows) {
            Ok(map) => map,
            Err(v) => return violation_fail(v),
        };
        if let Some(v) = pair_check(&pair_map, &scenario.model.pairs) {
            return violation_fail(v);
        }

        // Predicated cached query: its result must also be internally atomic.
        let probe = snapshot::cross_group_probe(proxy, cross_sql).await?;
        if let Err(v) = intra_snapshot_reduce(&probe) {
            return violation_fail(v);
        }

        snapshots += 1;
        tokio::time::sleep(interval).await;
    }

    Ok(snapshots)
}

/// Log the violation and fail the run.
fn violation_fail(violation: crate::invariants::Violation) -> Result<u64> {
    tracing::error!(%violation, "consistency violation in served view");
    bail!("consistency violation: {violation}");
}

/// Everything the final cache-vs-origin check needs.
struct EqualityCheck<'a> {
    metrics_url: &'a str,
    status: &'a StatusClient,
    origin: &'a Client,
    proxy: &'a Client,
    reader: &'a snapshot::GroupReader,
    per_group_sql: &'a str,
    full_sql: &'a str,
    all_groups: &'a [i32],
}

/// Poll the final cache-vs-origin equality until it converges or
/// [`CONVERGE_TIMEOUT`] elapses. Each round first drains everything that can
/// legitimately lag — the CDC watermark, in-flight populations, and the writer
/// channel queues — so a surviving mismatch is a genuine, persistent divergence
/// (a ghost row or lost update), not transient catch-up. Returns the converged
/// row count.
async fn equality_converge(c: &EqualityCheck<'_>) -> Result<usize> {
    let http = reqwest::Client::new();
    let deadline = Instant::now() + CONVERGE_TIMEOUT;
    loop {
        settle(c.status, origin_lsn(c.origin).await?, SETTLE_TIMEOUT)
            .await
            .context("final CDC settle")?;
        c.status
            .cache_settle(SETTLE_TIMEOUT)
            .await
            .context("final cache settle")?;
        writer_queues_drain(&http, c.metrics_url, SETTLE_TIMEOUT)
            .await
            .context("final writer-queue drain")?;

        let cache_pg = c.reader.read(c.all_groups).await?;
        let origin_pg = snapshot::per_group_rows(c.origin, c.per_group_sql, c.all_groups).await?;
        let cache_ft = snapshot::full_table(c.proxy, c.full_sql).await?;
        let origin_ft = snapshot::full_table(c.origin, c.full_sql).await?;

        let per_group = snapshot::equality_diff(&cache_pg, &origin_pg);
        let full_table = snapshot::equality_diff(&cache_ft, &origin_ft);
        if per_group.is_none() && full_table.is_none() {
            return Ok(cache_ft.len());
        }
        if Instant::now() >= deadline {
            bail!(
                "final cache/origin mismatch persisted for {CONVERGE_TIMEOUT:?} after settle + \
                 queue drain (transient lag would have converged; a persistent divergence is a \
                 real consistency bug)\n  per-group: {}\n  full-table: {}",
                per_group.as_deref().unwrap_or("ok"),
                full_table.as_deref().unwrap_or("ok"),
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Block until the writer's query/CDC/internal channel queues are all empty, so
/// the final comparison doesn't race work the writer has accepted but not yet
/// applied. These depths are only on `/metrics`, not `/status`.
async fn writer_queues_drain(
    http: &reqwest::Client,
    metrics_url: &str,
    timeout: Duration,
) -> Result<()> {
    poll_until(timeout, Duration::from_millis(50), || async {
        let depth = writer_queue_depth(http, metrics_url).await?;
        if depth <= 0.0 {
            Ok(ControlFlow::Break(()))
        } else {
            Ok(ControlFlow::Continue(format!(
                "writer queues not drained (depth {depth})"
            )))
        }
    })
    .await
}

/// Sum of the three writer channel-queue gauges scraped from `/metrics`.
async fn writer_queue_depth(http: &reqwest::Client, metrics_url: &str) -> Result<f64> {
    const NAMES: [&str; 3] = [
        "pgcache_cache_writer_query_queue",
        "pgcache_cache_writer_cdc_queue",
        "pgcache_cache_writer_internal_queue",
    ];
    let body = http
        .get(metrics_url)
        .send()
        .await
        .context("GET /metrics")?
        .error_for_status()
        .context("/metrics status")?
        .text()
        .await
        .context("/metrics body")?;

    let mut sum = 0.0;
    for line in body.lines() {
        if line.starts_with('#') {
            continue;
        }
        for name in NAMES {
            if let Some(rest) = line.strip_prefix(name)
                && matches!(rest.chars().next(), Some(' ') | Some('{'))
                && let Some(value) = rest.rsplit(' ').next().and_then(|v| v.trim().parse::<f64>().ok())
            {
                sum += value;
            }
        }
    }
    Ok(sum)
}

async fn origin_lsn(client: &Client) -> Result<u64> {
    let row = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .context("reading origin LSN")?;
    lsn_parse(row.get(0))
}
