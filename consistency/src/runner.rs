//! Orchestration: spawn the stack, seed, drive load, snapshot-and-check, then
//! verify cache == origin once the dust settles.

use std::ops::ControlFlow;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use pgcache_conformance::cdc_settling::{lsn_parse, settle, settle_while_progressing};
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
        cdc_lag_ms = cli.cdc_lag_ms,
        population_delay_ms = cli.population_delay_ms,
        "starting consistency stress run (use --seed {seed} to reproduce)"
    );

    let mut env = Vec::new();
    if let Some(ms) = cli.cdc_lag_ms.filter(|ms| *ms > 0) {
        env.push(("PGCACHE_FAULT_CDC_APPLY_LAG_MS".to_owned(), ms.to_string()));
    }
    if let Some(ms) = cli.population_delay_ms.filter(|ms| *ms > 0) {
        env.push((
            "PGCACHE_FAULT_POPULATION_DELAY_MS".to_owned(),
            ms.to_string(),
        ));
    }
    let faults_requested = !env.is_empty();
    // Injected delays stretch every settle/converge phase; pad their budgets
    // so a long configured lag isn't misreported as a hang.
    let fault_slack =
        Duration::from_millis(cli.cdc_lag_ms.unwrap_or(0) + cli.population_delay_ms.unwrap_or(0));
    let settle_timeout = SETTLE_TIMEOUT + fault_slack;
    let converge_timeout = CONVERGE_TIMEOUT + fault_slack;

    let stack = SpawnedPgcache::launch(SpawnOptions {
        pgcache_bin: cli.pgcache_bin.clone(),
        workers: cli.workers,
        log_level: cli.log_level.clone(),
        ready_timeout: READY_TIMEOUT,
        env,
    })
    .await
    .context("launching pgcache stack")?;

    let status = StatusClient::new(stack.status_url.clone());
    if faults_requested
        && !status
            .fault_injection()
            .await
            .context("checking /status for fault-injection support")?
    {
        bail!(
            "--cdc-lag-ms / --population-delay-ms need fault hooks, but the spawned pgcache \
             was built without them; rebuild with `cargo build --features fault-injection` \
             (or point --pgcache-bin at a fault-enabled build)"
        );
    }
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
    settle(&status, origin_lsn(&origin).await?, settle_timeout)
        .await
        .context("settling after seed")?;
    // Deliberately do NOT warm the per-group queries: they must populate
    // *during* load so their population races the concurrent removals — that
    // race is the bug under test. Warming them would make them cache hits
    // before any write lands and mask it. Only the maintained-in-place probe
    // is warmed.
    snapshot::cross_group_probe(&proxy, &cross_sql).await?;
    status
        .cache_settle(settle_timeout)
        .await
        .context("warming probe query")?;

    // Pool connects only now: it prepares the per-group query, which provision
    // created the tables for above.
    let reader =
        snapshot::GroupReader::connect(&stack.cache_url, cli.snapshot_conns, &per_group_sql)
            .await?;

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

    if loop_result.is_err() || task_error.is_some() {
        failure_diagnostics(&stack.metrics_url, &stack.origin_url, &stack.cache_db_url).await;
    }
    let snapshots = loop_result?;
    if let Some(e) = task_error {
        db::teardown_begin();
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
        settle_timeout,
        converge_timeout,
    };
    let final_rows = match equality_converge(&check).await {
        Ok(n) => n,
        Err(e) => {
            failure_diagnostics(&stack.metrics_url, &stack.origin_url, &stack.cache_db_url).await;
            db::teardown_begin();
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

    db::teardown_begin();
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
    let pair_ids: Vec<i32> = scenario
        .model
        .pairs
        .iter()
        .flat_map(|&(a, b)| [a, b])
        .collect();
    let mut tracker = MonotonicTracker::new();
    let mut snapshots = 0u64;

    while Instant::now() < deadline {
        // Per-group reads (one query per group, each atomic), fanned across the
        // connection pool: intra-group atomicity and per-group monotonicity.
        let rows = reader.read(groups).await?;
        let group_versions = snapshot::group_versions(&rows);
        let group_versions = match intra_snapshot_reduce(&group_versions) {
            Ok(map) => map,
            Err(v) => {
                // Forensics: the torn group's full served rows identify which
                // rows are stale (e.g. a pk_update predecessor frozen at an
                // old version next to its successor).
                if let crate::invariants::Violation::Intra { group, .. } = &v {
                    let torn: Vec<_> = rows.iter().filter(|r| r.1 == *group).collect();
                    tracing::error!(
                        ?torn,
                        "torn group served rows (id, group_id, version, data, payload_len)"
                    );
                }
                return violation_fail(v);
            }
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
    /// Settle/converge budgets, padded by any injected fault delays.
    settle_timeout: Duration,
    converge_timeout: Duration,
}

/// Poll the final cache-vs-origin equality until it converges or the
/// converge budget elapses. Each round first drains everything that can
/// legitimately lag — the CDC watermark, in-flight populations, and the writer
/// channel queues — so a surviving mismatch is a genuine, persistent divergence
/// (a ghost row or lost update), not transient catch-up. Returns the converged
/// row count.
async fn equality_converge(c: &EqualityCheck<'_>) -> Result<usize> {
    let http = reqwest::Client::new();
    let deadline = Instant::now() + c.converge_timeout;
    loop {
        // Progress-based: the run can legitimately bank a deep backlog
        // (cross-frame batching drains it in bulk after load stops), so only
        // a STALLED watermark is a failure — not a long healthy drain.
        settle_while_progressing(c.status, origin_lsn(c.origin).await?, c.settle_timeout)
            .await
            .context("final CDC settle")?;
        c.status
            .cache_settle(c.settle_timeout)
            .await
            .context("final cache settle")?;
        writer_queues_drain(&http, c.metrics_url, c.settle_timeout)
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
                "final cache/origin mismatch persisted for {:?} after settle + \
                 queue drain (transient lag would have converged; a persistent divergence is a \
                 real consistency bug)\n  per-group: {}\n  full-table: {}",
                c.converge_timeout,
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
/// Best-effort failure diagnostics: dump pgcache's key gauges and both
/// databases' non-idle backends (with wait events) so an intermittent stall
/// leaves evidence instead of just an op-timeout message.
async fn failure_diagnostics(metrics_url: &str, origin_url: &str, cache_db_url: &str) {
    // Per-query states: which fingerprints are non-Ready (and might hold
    // parked coalesce waiters).
    if let Ok(resp) = reqwest::get(metrics_url.replace("/metrics", "/status")).await
        && let Ok(body) = resp.text().await
    {
        let head: String = body.chars().take(4000).collect();
        tracing::error!("diagnostics /status: {head}");
    }
    if let Ok(resp) = reqwest::get(metrics_url).await
        && let Ok(body) = resp.text().await
    {
        for line in body.lines() {
            if line.starts_with("pgcache_cache_writer_cdc_queue")
                || line.starts_with("pgcache_cdc_applied_lsn")
                || line.starts_with("pgcache_cdc_received_lsn")
                || line.starts_with("pgcache_cache_queries_loading")
                || line.starts_with("pgcache_cache_coalesce_waiting")
                || line.starts_with("pgcache_connections_active")
            {
                tracing::error!("diagnostics: {line}");
            }
        }
    }
    for (label, url) in [("origin", origin_url), ("cache-db", cache_db_url)] {
        let Ok(client) = db::connect(url).await else {
            continue;
        };
        let Ok(rows) = client
            .query(
                "select pid::text, coalesce(state,'-'),                  coalesce(wait_event_type,'-') || '/' || coalesce(wait_event,'-'),                  coalesce(round(extract(epoch from now()-query_start))::text,'-'),                  left(coalesce(query,''),120)                  from pg_stat_activity                  where state <> 'idle' and pid <> pg_backend_pid()",
                &[],
            )
            .await
        else {
            continue;
        };
        for row in rows {
            let (pid, state, wait, dur, q): (String, String, String, String, String) =
                (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4));
            tracing::error!(
                "diagnostics {label}: pid={pid} state={state} wait={wait} dur={dur}s q={q}"
            );
        }
    }
}

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
                && let Some(value) = rest
                    .rsplit(' ')
                    .next()
                    .and_then(|v| v.trim().parse::<f64>().ok())
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
