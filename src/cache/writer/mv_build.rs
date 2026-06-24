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

use crate::query::Fingerprint;
use std::sync::Arc;
use std::time::Instant;

use ecow::EcoString;
use tokio::runtime::Handle;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{Receiver, Sender, UnboundedSender, channel};
use tokio_postgres::{Client, SimpleQueryMessage};
use tokio_stream::StreamExt;
use tracing::{error, trace};

use crate::pg;
use crate::query::ast::{Deparse, LiteralValue, SetOpType};
use crate::query::resolved::{
    ResolvedQueryBody, ResolvedQueryExpr, ResolvedScalarExpr, ResolvedSelectColumn,
    ResolvedSelectColumns, ResolvedSelectNode, ResolvedSetOpNode,
};
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
    pub fingerprint: Fingerprint,
    /// Build path: `false` = `CREATE UNLOGGED TABLE AS` (first build, gate may
    /// run), `true` = `BEGIN; DELETE; INSERT; COMMIT` (rebuild).
    pub has_table: bool,
    pub shape_gate: ShapeGate,
    /// LIMIT cap for the MV body (joins only).
    pub max_limit: Option<u64>,
    pub generation: u64,
    pub resolved: SharedResolved,
    /// Captured by a previous build; `None` means describe before building.
    pub output_columns: Option<Arc<[EcoString]>>,
    /// Real source-row count from the origin population, when the query
    /// self-populated. `None` when it became Ready without populating (subsumed,
    /// or other non-population paths) — the gate then counts the cache itself.
    pub populated_rows: Option<u64>,
    /// `mv_size_ratio` read from dynamic config at dispatch time.
    pub mv_size_ratio: u64,
    /// ComputeAvoid row threshold from dynamic config at dispatch time.
    pub mv_compute_min_rows: u64,
}

/// Pool of cache-DB connections for build tasks, using the codebase's bounded
/// mpsc pool pattern (see `connection_pool_create` / serve pool): checkout is
/// `recv()`, return is `send()`, and the channel capacity is the concurrency
/// limit. Two adaptations for the build case: tasks are independent consumers
/// (no single dispatcher), so the receiver sits behind a `Mutex`; and the
/// channel carries `Option<Client>` slots pre-filled with `None` so
/// connections open lazily on the shared runtime (eager creation in writer
/// init would put the tokio-postgres driver tasks on the writer's runtime).
/// A slot whose connection died returns as `None` and reconnects at the next
/// checkout — slot count is conserved, so no replenish task is needed.
pub(crate) struct MvBuildPool {
    settings: PgSettings,
    slot_tx: Sender<Option<Client>>,
    slot_rx: Mutex<Receiver<Option<Client>>>,
}

impl MvBuildPool {
    pub(super) fn new(settings: PgSettings) -> Self {
        let (slot_tx, slot_rx) = channel(MV_BUILD_CONNECTIONS);
        for _ in 0..MV_BUILD_CONNECTIONS {
            let _ = slot_tx.try_send(None);
        }
        Self {
            settings,
            slot_tx,
            slot_rx: Mutex::new(slot_rx),
        }
    }

    /// Wait for a slot, wrapped in a guard that returns it on drop. The inner
    /// `None` (never-connected or discarded slot) and a closed-channel `None`
    /// (unreachable — `self` holds a sender) both mean "no usable client":
    /// the caller connects.
    async fn slot_acquire(&self) -> SlotGuard {
        let content = self.slot_rx.lock().await.recv().await.flatten();
        SlotGuard {
            slot_tx: self.slot_tx.clone(),
            content,
        }
    }
}

/// Returns its slot to the pool on drop, so a panic or cancellation anywhere
/// between checkout and return cannot shrink the pool (the slot comes back as
/// `None` and reconnects at the next checkout). Mirrors the serve pool's
/// `ConnectionGuard` safety property.
struct SlotGuard {
    slot_tx: Sender<Option<Client>>,
    content: Option<Client>,
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        // try_send cannot fail: capacity equals the number of slots out.
        let _ = self.slot_tx.try_send(self.content.take());
    }
}

/// Sends `MvBuildComplete` on drop, so the writer hears about every build —
/// including one that panicked. The writer's in-flight guard blocks all
/// future builds for the fingerprint until a completion arrives, so a lost
/// completion would wedge the MV permanently.
struct CompletionGuard {
    query_tx: UnboundedSender<QueryCommand>,
    fingerprint: Fingerprint,
    /// `has_table` for the fallback `Failed` outcome when the task died
    /// before producing one.
    has_table: bool,
    outcome: Option<MvBuildOutcome>,
}

impl Drop for CompletionGuard {
    fn drop(&mut self) {
        let outcome = self.outcome.take().unwrap_or(MvBuildOutcome::Failed {
            has_table: self.has_table,
        });
        // Send failure means the writer is gone (cache teardown); moot.
        let _ = self.query_tx.send(QueryCommand::MvBuildComplete {
            fingerprint: self.fingerprint,
            outcome,
        });
    }
}

/// Spawn one MV build onto the shared runtime. The completion guard reports
/// back via `MvBuildComplete` on the writer's internal channel even if the
/// build panics.
pub(super) fn mv_build_spawn(
    runtime: &Handle,
    pool: &Arc<MvBuildPool>,
    ctx: MvBuildContext,
    query_tx: UnboundedSender<QueryCommand>,
) {
    let pool = Arc::clone(pool);
    runtime.spawn(async move {
        let mut completion = CompletionGuard {
            query_tx,
            fingerprint: ctx.fingerprint,
            has_table: ctx.has_table,
            outcome: None,
        };
        completion.outcome = Some(mv_build_run(&pool, &ctx).await);
    });
}

/// Acquire a build slot + connection, run the build; the guard returns the
/// slot on every exit path.
async fn mv_build_run(pool: &MvBuildPool, ctx: &MvBuildContext) -> MvBuildOutcome {
    fault_build_hold().await;

    // Queue gauge brackets the slot wait: per-fingerprint exclusivity makes
    // this the number of distinct MVs waiting for a build connection.
    let queue = &crate::metrics::handles().mv.build_queue;
    queue.increment(1.0);
    let mut slot = pool.slot_acquire().await;
    queue.decrement(1.0);

    let client = match slot.content.take() {
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

    let outcome = mv_build_execute(&client, ctx).await;
    if !client.is_closed() {
        slot.content = Some(client);
    }
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

/// First-build gate. `Skip` never materializes; a `Gated` shape materializes if
/// EITHER admission test passes (PGC-330):
///
/// - **compute avoidance**: `populated_rows >= mv_compute_min_rows` — the input
///   is large enough that re-running the join/window/aggregate per serve is worth
///   avoiding, regardless of how much it reduces.
/// - **row reduction**: `result_rows * mv_size_ratio <= populated_rows` — the
///   result is a large enough reduction of its input to be worth storing.
///
/// The source-row count is the origin population count when the query
/// self-populated (free, no query); otherwise — subsumed, or any path that
/// reached Ready without populating — it falls back to a bounded predicate-scoped
/// `count(*)` against the cache (off the serve path). The reduction test also
/// needs the result count, so it's only run when compute avoidance hasn't
/// already admitted the build.
///
/// Called only before a first build — rebuilds inherit the sticky gate result.
async fn mv_gate_passes(client: &Client, ctx: &MvBuildContext) -> CacheResult<bool> {
    if ctx.shape_gate == ShapeGate::Skip {
        return Ok(false);
    }
    let source_rows = match ctx.populated_rows {
        Some(n) => n,
        None => mv_source_rows_count(client, ctx).await?,
    };
    let admit = if source_rows >= ctx.mv_compute_min_rows {
        true
    } else {
        let result_rows = mv_result_count(client, ctx).await?;
        result_rows.saturating_mul(ctx.mv_size_ratio) <= source_rows
    };
    let mv = &crate::metrics::handles().mv;
    if admit {
        mv.gate_admit.increment(1);
    } else {
        mv.gate_reject.increment(1);
    }
    Ok(admit)
}

/// Result-row count — the reduction test's numerator: `count(*)` over the MV
/// body, capped by `max_limit`. `mem.query_generation` is set/reset around it
/// because source rows are generation-filtered (a GC mechanism, not
/// read-consistency).
async fn mv_result_count(client: &Client, ctx: &MvBuildContext) -> CacheResult<u64> {
    let set_gen = format!("SET mem.query_generation = {}", ctx.generation);
    client
        .batch_execute(&set_gen)
        .await
        .map_into_report::<CacheError>()
        .attach_loc("setting query generation for MV gate")?;

    let count_sql = mv_count_sql(ctx);
    let result = client.query_one(&count_sql, &[]).await;

    // Always reset generation, even on failure.
    let _ = client.batch_execute("SET mem.query_generation = 0").await;

    let row = result
        .map_into_report::<CacheError>()
        .attach_loc("executing MV gate result count")?;
    Ok(u64::try_from(row.get::<_, i64>(0)).unwrap_or(0))
}
/// Row source whose count is the Measure-gate denominator: the source query's
/// *input* rows — the rows that feed the result — NOT the result itself.
///
/// - **SELECT**: strip the projection/aggregation (GROUP BY/HAVING) and outer
///   ORDER BY/LIMIT, keeping FROM + WHERE + JOINs, so the count is the
///   predicate-scoped scanned rows (`SELECT 1 FROM posts WHERE owneruserid=$1`
///   counts predicate-matching rows, not all of `posts`). Counting the
///   un-stripped aggregate would yield its single result row and defeat the gate.
/// - **Set-op**: recurse into both branches and `UNION ALL` their input sources,
///   so the count is the *sum of branch inputs* — never the deduped/intersected
///   result. This keeps the gate measuring reduction for UNION/INTERSECT/EXCEPT
///   the way it does for an aggregate's pre-grouping rows (PGC-329).
/// - **VALUES**: the literal rows themselves.
fn mv_gate_input_query(resolved: &ResolvedQueryExpr) -> ResolvedQueryExpr {
    let body = match &resolved.body {
        ResolvedQueryBody::Select(select) => {
            // Constant projection, no DISTINCT: counts scanned rows (including
            // join fan-out) — the work the query does.
            ResolvedQueryBody::Select(Box::new(ResolvedSelectNode {
                distinct: false,
                columns: ResolvedSelectColumns::Columns(vec![ResolvedSelectColumn {
                    expr: ResolvedScalarExpr::Literal(LiteralValue::Integer(1)),
                    alias: None,
                }]),
                from: select.from.clone(),
                where_clause: select.where_clause.clone(),
                group_by: vec![],
                having: None,
            }))
        }
        // `UNION ALL` (not the original op) so no branch rows are deduped or
        // dropped — the denominator is the total input the set-op scans.
        ResolvedQueryBody::SetOp(setop) => ResolvedQueryBody::SetOp(ResolvedSetOpNode {
            op: SetOpType::Union,
            all: true,
            left: Box::new(mv_gate_input_query(&setop.left)),
            right: Box::new(mv_gate_input_query(&setop.right)),
        }),
        ResolvedQueryBody::Values(_) => resolved.body.clone(),
    };
    ResolvedQueryExpr {
        body,
        order_by: vec![],
        limit: None,
    }
}

/// SQL for the Measure-gate denominator: `count(*)` over the source query's
/// input rows (see `mv_gate_input_query`).
fn mv_source_count_sql(resolved: &ResolvedQueryExpr) -> String {
    let inner = mv_gate_input_query(resolved);
    let mut sql = String::with_capacity(256);
    sql.push_str("SELECT count(*) FROM (");
    inner.deparse(&mut sql);
    sql.push_str(") _mv_gate_denom");
    sql
}

/// Denominator of the Measure size gate: the source query's predicate-scoped
/// input row count (see `mv_source_count_sql`).
async fn mv_source_rows_count(client: &Client, ctx: &MvBuildContext) -> CacheResult<u64> {
    let sql = mv_source_count_sql(&ctx.resolved);
    let row = client
        .query_one(&sql, &[])
        .await
        .map_into_report::<CacheError>()
        .attach_loc("counting MV size-gate source rows")?;
    Ok(u64::try_from(row.get::<_, i64>(0)).unwrap_or(0))
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
/// `BEGIN; DELETE; INSERT; COMMIT;` transaction so concurrent reads are never
/// exposed to an empty intermediate state.
///
/// Rebuild clears with `DELETE`, not `TRUNCATE`: TRUNCATE churns the relfilenode
/// (the old one is unlinked at commit → a full shared-buffer scan in
/// `DropRelationsAllBuffers` plus a relcache-invalidation broadcast every backend
/// processes) and takes ACCESS EXCLUSIVE, which blocks readers for the whole
/// rebuild — together collapsing the cache PG under an invalidation storm
/// (PGC-335). DELETE reuses the relfilenode and takes only ROW EXCLUSIVE, so
/// readers proceed on their MVCC snapshot; autovacuum reclaims the dead tuples.
fn mv_build_batch(mv_table: &str, ctx: &MvBuildContext, has_table: bool, arity: usize) -> String {
    use std::fmt::Write;
    let mut sql = String::with_capacity(512);
    let generation = ctx.generation;
    let cols = mv_columns_list(arity);
    if has_table {
        let _ = write!(
            &mut sql,
            "BEGIN; SET mem.query_generation = {generation}; \
             DELETE FROM {mv_table}; INSERT INTO {mv_table} {cols} "
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

#[cfg(test)]
mod tests {
    use iddqd::BiHashMap;
    use tokio_postgres::types::Type;

    use super::*;
    use crate::catalog::{ColumnMetadata, ColumnStore, Oid, TableMetadata};
    use crate::query::ast::{
        AstNode, ColumnNode, FunctionCall, QueryExpr, SelectNode, SetOpNode, SetOpType, TableNode,
        query_expr_parse,
    };
    use crate::query::resolved::query_expr_resolve;

    fn test_table(name: &str, oid: Oid, cols: &[&str]) -> TableMetadata {
        let columns = ColumnStore::new(cols.iter().enumerate().map(|(i, c)| {
            let is_pk = i == 0;
            ColumnMetadata {
                name: (*c).into(),
                position: i16::try_from(i + 1).expect("column position fits in i16"),
                type_oid: if is_pk { 23 } else { 25 },
                data_type: if is_pk { Type::INT4 } else { Type::TEXT },
                type_name: if is_pk { "int4" } else { "text" }.into(),
                cache_type_name: if is_pk { "int4" } else { "text" }.into(),
                is_primary_key: is_pk,
            }
        }));
        TableMetadata {
            relation_oid: oid,
            name: name.into(),
            schema: "public".into(),
            primary_key_columns: vec![cols[0].into()],
            columns,
            indexes: Vec::new(),
        }
    }

    fn test_tables() -> BiHashMap<TableMetadata> {
        let mut t = BiHashMap::new();
        t.insert_overwrite(test_table(
            "orders",
            Oid::from_raw(1),
            &["id", "status", "total"],
        ));
        t.insert_overwrite(test_table(
            "users",
            Oid::from_raw(2),
            &["id", "name", "email"],
        ));
        t
    }

    /// Build the Measure-gate denominator for `sql` and re-parse it into an AST
    /// so tests assert on nodes, not raw text.
    fn denom_ast(sql: &str) -> QueryExpr {
        let ast = query_expr_parse(sql).expect("parse source");
        let resolved =
            query_expr_resolve(&ast, &test_tables(), &["public"]).expect("resolve source");
        let denom = mv_source_count_sql(&resolved);
        query_expr_parse(&denom).expect("denominator SQL re-parses")
    }

    /// Count of `count()` aggregate calls anywhere in the AST.
    fn count_aggs(ast: &QueryExpr) -> usize {
        ast.nodes::<FunctionCall>()
            .filter(|f| f.name.eq_ignore_ascii_case("count"))
            .count()
    }

    /// The WHERE predicate survives (the bug dropped it) and the inner aggregate
    /// is gone — only the wrapper `count(*)` remains.
    #[test]
    fn test_denominator_keeps_predicate_drops_aggregate() {
        let ast = denom_ast("SELECT count(*) FROM orders WHERE status = 'open'");
        assert!(
            ast.nodes::<ColumnNode>().any(|c| c.column == "status"),
            "predicate column `status` must survive in the denominator"
        );
        assert!(
            ast.nodes::<TableNode>().any(|t| t.name == "orders"),
            "source table must survive"
        );
        assert_eq!(count_aggs(&ast), 1, "only the wrapper count() remains");
    }

    /// GROUP BY is stripped so the count is over scanned input rows, not groups.
    #[test]
    fn test_denominator_strips_group_by() {
        let ast =
            denom_ast("SELECT status, count(*) FROM orders WHERE status = 'open' GROUP BY status");
        assert!(
            ast.nodes::<SelectNode>().all(|s| s.group_by.is_empty()),
            "no SELECT node may carry GROUP BY"
        );
        assert!(ast.nodes::<ColumnNode>().any(|c| c.column == "status"));
        assert_eq!(count_aggs(&ast), 1);
    }

    /// Outer ORDER BY / LIMIT are stripped from the denominator (the inner
    /// row-source query carries neither).
    #[test]
    fn test_denominator_strips_order_and_limit() {
        let ast = denom_ast("SELECT count(*) FROM orders WHERE status = 'open' LIMIT 5");
        assert!(
            ast.nodes::<QueryExpr>()
                .all(|q| q.limit.is_none() && q.order_by.is_empty()),
            "no query node may carry ORDER BY / LIMIT"
        );
    }

    /// Set-op denominator is branch-aware: each branch is stripped to its input
    /// row source and the branches are `UNION ALL`'d, so the count is the sum of
    /// branch inputs (not the deduped result). Outer LIMIT dropped.
    #[test]
    fn test_denominator_sums_branch_inputs() {
        let ast = denom_ast("SELECT id FROM orders UNION SELECT id FROM users");
        // Both branch source tables survive.
        assert!(ast.nodes::<TableNode>().any(|t| t.name == "orders"));
        assert!(ast.nodes::<TableNode>().any(|t| t.name == "users"));
        // Each branch is stripped to a constant projection — no `id` column ref,
        // so the count is over scanned rows, not the projected/deduped result.
        assert!(
            ast.nodes::<ColumnNode>().all(|c| c.column != "id"),
            "branch projections must be stripped to a constant"
        );
        // Branches are combined with UNION ALL so the denominator is the *sum*
        // of branch inputs, never the deduped UNION result.
        assert!(
            ast.nodes::<SetOpNode>()
                .any(|s| s.op == SetOpType::Union && s.all),
            "branches must be UNION ALL'd (sum of inputs, no dedup)"
        );
        assert_eq!(count_aggs(&ast), 1, "only the wrapper count()");
        assert!(
            ast.nodes::<QueryExpr>().all(|q| q.limit.is_none()),
            "outer LIMIT dropped"
        );
    }

    /// More than two branches: the binary set-op tree is recursed fully, so every
    /// branch's table survives and each pairwise node becomes UNION ALL (a
    /// 3-branch query has two such nodes). Confirms the recursion isn't limited to
    /// two branches.
    #[test]
    fn test_denominator_three_branch_setop() {
        let ast = denom_ast(
            "SELECT id FROM orders UNION SELECT id FROM users UNION SELECT status FROM orders",
        );
        assert!(ast.nodes::<TableNode>().any(|t| t.name == "orders"));
        assert!(ast.nodes::<TableNode>().any(|t| t.name == "users"));
        assert_eq!(
            ast.nodes::<SetOpNode>()
                .filter(|s| s.op == SetOpType::Union && s.all)
                .count(),
            2,
            "three branches → two UNION ALL nodes"
        );
        assert!(
            ast.nodes::<ColumnNode>()
                .all(|c| c.column != "id" && c.column != "status"),
            "every branch projection stripped to a constant"
        );
        assert_eq!(count_aggs(&ast), 1, "only the wrapper count()");
    }

    /// PGC-329's example: a set-op whose branches aggregate. Each branch's
    /// `count(*)` is stripped (the branch is reduced to its input row source), so
    /// the denominator counts the rows feeding the aggregates — only the wrapper
    /// `count()` survives, not the branch aggregates.
    #[test]
    fn test_denominator_setop_aggregate_branches() {
        let ast = denom_ast("SELECT count(*) FROM orders UNION SELECT count(*) FROM users");
        assert!(ast.nodes::<TableNode>().any(|t| t.name == "orders"));
        assert!(ast.nodes::<TableNode>().any(|t| t.name == "users"));
        assert!(
            ast.nodes::<SetOpNode>()
                .any(|s| s.op == SetOpType::Union && s.all),
            "branches UNION ALL'd"
        );
        assert_eq!(
            count_aggs(&ast),
            1,
            "branch count() aggregates stripped — only the wrapper count() remains"
        );
    }
}
