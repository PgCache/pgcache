//! Off-thread MV build execution.
//!
//! The writer dispatches builds here (`MvBuild` handler: snapshot context,
//! flip to `Building`, spawn) and applies the resulting state transition
//! (`MvBuildComplete` handler). This module owns the SQL in between: the
//! Measure size gate, the output-column describe, and the build batch, all
//! run on the shared multi-thread runtime against a small dedicated
//! connection pool so the writer's event loop never blocks on build SQL.
//!
//! The task performs no `MvState` transitions — completion is reported back
//! through the writer's internal channel, which serializes the `Fresh` flip
//! against CDC dirty-marking (a build raced by a relevant change is observed
//! as `BuildingDirty` and discarded).

use std::sync::Arc;
use std::time::Instant;

use ecow::EcoString;
use tokio::runtime::Handle;
use tokio::sync::Semaphore;
use tokio::sync::mpsc::UnboundedSender;
use tokio_postgres::{Client, SimpleQueryMessage};
use tokio_stream::StreamExt;
use tracing::{error, trace};

use crate::pg;
use crate::query::ast::Deparse;
use crate::result::error_chain_format;
use crate::settings::PgSettings;

use super::super::{
    CacheError, CacheResult, MapIntoReport, ReportExt,
    messages::{MvBuildOutcome, QueryCommand},
    mv::{ShapeGate, mv_table_name},
    types::SharedResolved,
};

/// Connections dedicated to MV builds — also the build concurrency limit.
/// Deliberately separate from the serve pool: a build is a multi-statement
/// transaction holding an exclusive lock on its MV table, and a backlog of
/// builds must never consume serve capacity.
const MV_BUILD_CONNECTIONS: usize = 2;

/// Snapshot of everything a build task needs, taken on the writer (which can
/// read `core.cache`) so the task touches only the cache DB.
pub(super) struct MvBuildContext {
    pub fingerprint: u64,
    /// Build path: `false` = `CREATE UNLOGGED TABLE AS` (first build, gate may
    /// run), `true` = `BEGIN; TRUNCATE; INSERT; COMMIT` (rebuild).
    pub has_table: bool,
    pub shape_gate: ShapeGate,
    /// LIMIT cap for the MV body (joins only).
    pub max_limit: Option<u64>,
    pub generation: u64,
    pub resolved: SharedResolved,
    /// Captured by a previous build; `None` means describe before building.
    pub output_columns: Option<Arc<[EcoString]>>,
    /// `(schema, name)` of the referenced cache tables — the Measure gate's
    /// denominator. Populated only when the gate will run.
    pub gate_tables: Vec<(EcoString, EcoString)>,
    /// `mv_size_ratio` read from dynamic config at dispatch time.
    pub mv_size_ratio: u64,
}

/// Lazily-connected pool of cache-DB connections for build tasks. The
/// semaphore is the concurrency limit; clients reconnect on checkout when a
/// prior build poisoned them. Dropped wholesale on cache-generation teardown.
pub(crate) struct MvBuildPool {
    settings: PgSettings,
    permits: Semaphore,
    idle: std::sync::Mutex<Vec<Client>>,
}

impl MvBuildPool {
    pub(super) fn new(settings: PgSettings) -> Self {
        Self {
            settings,
            permits: Semaphore::new(MV_BUILD_CONNECTIONS),
            idle: std::sync::Mutex::new(Vec::with_capacity(MV_BUILD_CONNECTIONS)),
        }
    }

    fn client_take(&self) -> Option<Client> {
        self.idle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop()
    }

    fn client_return(&self, client: Client) {
        if !client.is_closed() {
            self.idle
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(client);
        }
    }
}

/// Spawn one MV build onto the shared runtime. The task reports back via
/// `MvBuildComplete` on the writer's internal channel; a send failure means
/// the writer is gone (cache teardown) and the result is moot.
pub(super) fn mv_build_spawn(
    runtime: &Handle,
    pool: &Arc<MvBuildPool>,
    ctx: MvBuildContext,
    query_tx: UnboundedSender<QueryCommand>,
) {
    let pool = Arc::clone(pool);
    runtime.spawn(async move {
        let fingerprint = ctx.fingerprint;
        let outcome = mv_build_run(&pool, ctx).await;
        let _ = query_tx.send(QueryCommand::MvBuildComplete {
            fingerprint,
            outcome,
        });
    });
}

/// Acquire a build slot + connection, run the build, return the connection.
async fn mv_build_run(pool: &MvBuildPool, ctx: MvBuildContext) -> MvBuildOutcome {
    fault_build_hold().await;

    let Ok(_permit) = pool.permits.acquire().await else {
        // Semaphore is never closed; unreachable in practice.
        return MvBuildOutcome::Failed {
            has_table: ctx.has_table,
        };
    };

    let client = match pool.client_take() {
        Some(c) if !c.is_closed() => c,
        _ => match pg::connect(&pool.settings, "mv build").await {
            Ok(c) => c,
            Err(e) => {
                error!("mv build connect failed for {}: {e}", ctx.fingerprint);
                return MvBuildOutcome::Failed {
                    has_table: ctx.has_table,
                };
            }
        },
    };

    let outcome = mv_build_execute(&client, &ctx).await;
    pool.client_return(client);
    outcome
}

/// The build itself: Measure gate (first build only), output-column describe,
/// then the build batch. SQL only — no `MvState` transitions.
async fn mv_build_execute(client: &Client, ctx: &MvBuildContext) -> MvBuildOutcome {
    let fingerprint = ctx.fingerprint;

    if !ctx.has_table {
        match mv_gate_passes(client, ctx).await {
            Ok(true) => {}
            Ok(false) => {
                trace!("mv build: size gate failed for {fingerprint}");
                return MvBuildOutcome::Ineligible;
            }
            Err(e) => {
                error!(
                    "mv build failed for {fingerprint}: size gate: {}",
                    error_chain_format(e.current_context()),
                );
                return MvBuildOutcome::Failed {
                    has_table: ctx.has_table,
                };
            }
        }
    }

    // Captured once, reused on rebuild. Failure aborts the build so a
    // Fresh MV always has names.
    let names = match &ctx.output_columns {
        Some(cols) => Arc::clone(cols),
        None => match mv_output_columns(client, ctx).await {
            Ok(n) if !n.is_empty() => n,
            Ok(_) => {
                error!("mv build failed for {fingerprint}: query describe returned no columns");
                return MvBuildOutcome::Failed {
                    has_table: ctx.has_table,
                };
            }
            Err(e) => {
                error!(
                    "mv build failed for {fingerprint}: output-column describe: {}",
                    error_chain_format(e.current_context()),
                );
                return MvBuildOutcome::Failed {
                    has_table: ctx.has_table,
                };
            }
        },
    };

    let start = Instant::now();
    let mv_table = mv_table_name(fingerprint);
    let batch = mv_build_batch(&mv_table, ctx, ctx.has_table, names.len());

    if let Err(e) = client
        .batch_execute(&batch)
        .await
        .map_into_report::<CacheError>()
        .attach_loc(if ctx.has_table {
            "mv rebuild transaction"
        } else {
            "creating MV table on first build"
        })
    {
        error!(
            "mv build failed for {fingerprint}: {}",
            error_chain_format(e.current_context()),
        );
        let cleanup = if ctx.has_table {
            "ROLLBACK; SET mem.query_generation = 0;".to_owned()
        } else {
            format!("SET mem.query_generation = 0; DROP TABLE IF EXISTS {mv_table};")
        };
        let _ = client.batch_execute(&cleanup).await;
        return MvBuildOutcome::Failed {
            has_table: ctx.has_table,
        };
    }

    let elapsed = start.elapsed();
    let mv = &crate::metrics::handles().mv;
    let build_handle = if ctx.has_table {
        &mv.build_rebuild
    } else {
        &mv.build_first_pop
    };
    build_handle.record(elapsed.as_secs_f64());
    trace!(
        "mv build ({}): built for {fingerprint} in {elapsed:?}",
        if ctx.has_table {
            "rebuild"
        } else {
            "first_pop"
        }
    );

    MvBuildOutcome::Built {
        output_columns: names,
        was_first_build: !ctx.has_table,
    }
}

/// PostgreSQL's output column names for the query, captured by describing
/// `<resolved> LIMIT 0` (same query as source-row serve).
async fn mv_output_columns(client: &Client, ctx: &MvBuildContext) -> CacheResult<Arc<[EcoString]>> {
    let mut sql = String::with_capacity(256);
    ctx.resolved.deparse(&mut sql);
    sql.push_str(" LIMIT 0");

    let stream = client
        .simple_query_raw(&sql)
        .await
        .map_into_report::<CacheError>()?;
    tokio::pin!(stream);
    match stream.next().await {
        Some(Ok(SimpleQueryMessage::RowDescription(cols))) => {
            Ok(cols.iter().map(|c| EcoString::from(c.name())).collect())
        }
        Some(Ok(_)) => Err(CacheError::InvalidMessage.into()),
        Some(Err(e)) => Err(CacheError::from(e).into()),
        None => Err(CacheError::InvalidMessage.into()),
    }
}

/// Run the Measure size gate (no-op for Materialize / Skip defensively).
/// Called only before a first build — rebuilds inherit the sticky gate
/// result via classification not re-running.
async fn mv_gate_passes(client: &Client, ctx: &MvBuildContext) -> CacheResult<bool> {
    match ctx.shape_gate {
        ShapeGate::Materialize => Ok(true),
        ShapeGate::Skip => Ok(false),
        ShapeGate::Measure => {
            let source_rows = mv_source_rows_count(client, ctx).await?;
            mv_size_gate_passes(client, ctx, source_rows).await
        }
    }
}

/// Sum `count(*)` across the cache tables referenced by the fingerprint
/// (snapshotted into `ctx.gate_tables` at dispatch). Used as the denominator
/// of the Measure size gate. An empty table list yields 0 — causes the gate
/// to fail, which is the safe default.
async fn mv_source_rows_count(client: &Client, ctx: &MvBuildContext) -> CacheResult<u64> {
    let mut total: u64 = 0;
    for (schema, name) in &ctx.gate_tables {
        let sql = format!("SELECT count(*) FROM \"{schema}\".\"{name}\"");
        let row = client
            .query_one(&sql, &[])
            .await
            .map_into_report::<CacheError>()
            .attach_loc("counting cache table rows for MV size gate")?;
        total = total.saturating_add(u64::try_from(row.get::<_, i64>(0)).unwrap_or(0));
    }
    Ok(total)
}

/// Measure-gate: `result_rows × mv_size_ratio ≤ source_rows`.
///
/// Runs `SELECT count(*) FROM (<query> LIMIT max_limit) x` against the
/// source-row cache with the query's current generation set so the count
/// sees the consistent snapshot. The LIMIT keeps the gate consistent with
/// what would actually be stored (MV is capped at `max_limit`).
async fn mv_size_gate_passes(
    client: &Client,
    ctx: &MvBuildContext,
    source_rows: u64,
) -> CacheResult<bool> {
    let set_gen = format!("SET mem.query_generation = {}", ctx.generation);
    client
        .batch_execute(&set_gen)
        .await
        .map_into_report::<CacheError>()
        .attach_loc("setting query generation for MV size gate")?;

    let count_sql = mv_count_sql(ctx);

    let result = client.query_one(&count_sql, &[]).await;

    // Always reset generation, even on failure.
    let _ = client.batch_execute("SET mem.query_generation = 0").await;

    let row = result
        .map_into_report::<CacheError>()
        .attach_loc("executing MV size gate count")?;
    let result_rows = u64::try_from(row.get::<_, i64>(0)).unwrap_or(0);

    Ok(result_rows.saturating_mul(ctx.mv_size_ratio) <= source_rows)
}

/// Append the resolved query body (including any ORDER BY) and the MV's
/// `max_limit` cap. Used anywhere we need the SELECT body that would populate
/// the MV table.
fn mv_body_append(buf: &mut String, ctx: &MvBuildContext) {
    use std::fmt::Write;
    ctx.resolved.deparse(buf);
    if let Some(limit) = ctx.max_limit {
        let _ = write!(buf, " LIMIT {limit}");
    }
}

/// Build the complete batch for an MV build. First-pop wraps `CREATE UNLOGGED
/// TABLE AS <body>` with SET/RESET of the query generation. Rebuild uses a
/// `BEGIN; TRUNCATE; INSERT; COMMIT;` transaction so concurrent reads are never
/// exposed to an empty intermediate state.
fn mv_build_batch(mv_table: &str, ctx: &MvBuildContext, has_table: bool, arity: usize) -> String {
    use std::fmt::Write;
    let mut sql = String::with_capacity(512);
    let generation = ctx.generation;
    let cols = mv_columns_list(arity);
    if has_table {
        let _ = write!(
            &mut sql,
            "BEGIN; SET mem.query_generation = {generation}; \
             TRUNCATE {mv_table}; INSERT INTO {mv_table} {cols} "
        );
        mv_body_append(&mut sql, ctx);
        sql.push_str("; COMMIT; SET mem.query_generation = 0;");
    } else {
        let _ = write!(
            &mut sql,
            "SET mem.query_generation = {generation}; \
             CREATE UNLOGGED TABLE {mv_table} {cols} AS "
        );
        mv_body_append(&mut sql, ctx);
        sql.push_str("; SET mem.query_generation = 0;");
    }
    sql
}

/// Positional MV column list `(c0, c1, …, c{n-1})` — lets the table hold
/// otherwise-colliding output names; `mv_serve_sql` aliases them back.
fn mv_columns_list(arity: usize) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(arity * 5 + 2);
    s.push('(');
    for i in 0..arity {
        if i > 0 {
            s.push_str(", ");
        }
        let _ = write!(s, "c{i}");
    }
    s.push(')');
    s
}

/// `SELECT count(*) FROM (<deparsed resolved> LIMIT max_limit) _mv_gate_src`.
/// The LIMIT keeps the gate consistent with what would be stored — an MV capped
/// at `max_limit` can never have more than `max_limit` rows, so counting past
/// the cap would make the ratio over-report.
fn mv_count_sql(ctx: &MvBuildContext) -> String {
    let mut sql = String::with_capacity(512);
    sql.push_str("SELECT count(*) FROM (");
    mv_body_append(&mut sql, ctx);
    sql.push_str(") _mv_gate_src");
    sql
}

/// Test-only build delay (`PGCACHE_FAULT_MV_BUILD_HOLD_MS`): widens the
/// in-flight window so tests can deterministically land a CDC change between
/// dispatch and completion and assert the `BuildingDirty` discard.
#[cfg(feature = "fault-injection")]
async fn fault_build_hold() {
    use std::sync::OnceLock;
    static HOLD_MS: OnceLock<Option<u64>> = OnceLock::new();
    let hold = *HOLD_MS.get_or_init(|| {
        std::env::var("PGCACHE_FAULT_MV_BUILD_HOLD_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|n| *n > 0)
    });
    if let Some(ms) = hold {
        tracing::debug!("fault injection: holding mv build for {ms}ms");
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
    }
}
#[cfg(not(feature = "fault-injection"))]
async fn fault_build_hold() {}
