//! Writer-side MV operations: build (first-pop or rebuild), dirty-marking,
//! eviction helpers.
//!
//! First-pop and rebuild share a single state-machine and a single handler —
//! the `has_table` bit carried by `Pending` / `Scheduled` tells the writer
//! which SQL variant to run (`CREATE UNLOGGED TABLE AS` for first-pop,
//! `BEGIN; TRUNCATE; INSERT; COMMIT;` for rebuild) and whether a Measure gate
//! is still owed (only before the first successful build).
//!
//! All MV state transitions and SQL execution against the cache DB happen on
//! the writer task (v1 execution model — see design doc), so CDC event
//! handling is naturally serialized with MV builds. No race detection needed
//! for v1. If long builds cause replication slot lag, the follow-up is to move
//! builds off-thread with `Building` / `BuildingDirty` states.

use std::sync::Arc;
use std::time::Instant;

use ecow::EcoString;
use tokio_postgres::SimpleQueryMessage;
use tokio_stream::StreamExt;
use tracing::{debug, error, trace};

use crate::metrics::names;
use crate::query::ast::Deparse;
use crate::result::error_chain_format;

use super::super::{
    CacheError, CacheResult, MapIntoReport, ReportExt,
    messages::QueryCommand,
    mv::{MvState, ShapeGate, mv_table_name},
    types::{CachedQueryState, SharedResolved},
};
use super::core::WriterCore;

/// Snapshot of the state we need for an MV build, taken atomically up front so
/// the long-running SQL doesn't hold a DashMap guard across awaits.
struct MvBuildContext {
    shape_gate: ShapeGate,
    max_limit: Option<u64>,
    generation: u64,
    resolved: SharedResolved,
}

impl WriterCore {
    /// Pinned queries bypass the coordinator-driven "first hit triggers MV
    /// build" flow so they stay warm across startup and readmits. Called from
    /// the Ready handler. Performs the same check-and-transition the
    /// coordinator would do on first hit and self-sends an `MvBuild` command.
    pub(super) fn mv_pinned_bootstrap(&self, fingerprint: u64) {
        let is_pinned = self
            .cache
            .cached_queries
            .get1(&fingerprint)
            .is_some_and(|q| q.pinned);
        if !is_pinned {
            return;
        }
        let Some(mut view) = self.state_view.cached_queries.get_mut(&fingerprint) else {
            return;
        };
        let MvState::Pending { has_table } = view.mv.state else {
            return;
        };
        view.mv.state = MvState::Scheduled { has_table };
        drop(view);
        let _ = self.query_tx.send(QueryCommand::MvBuild { fingerprint });
    }

    /// Handle `QueryCommand::MvBuild`. Precondition: `mv_state ==
    /// Scheduled { .. }` (set by the coordinator or pinned bootstrap before
    /// the command was enqueued) and source-row state `Ready`.
    ///
    /// Branches on `has_table`:
    /// - `false` (first build): for Measure, compute source_rows and run the
    ///   size gate; for Materialize, skip. Run `CREATE UNLOGGED TABLE AS`.
    ///   Re-check source-row state post-build; abort drops the partial table.
    /// - `true` (rebuild): run `BEGIN; TRUNCATE; INSERT; COMMIT;` — no gate
    ///   (classification is sticky after the first successful build).
    ///
    /// Terminal: `Fresh` on success, `Ineligible` on size-gate fail. Abort
    /// transitions back to `Pending { has_table }` (preserving existence) so
    /// the next hit retries.
    pub(super) async fn mv_build(&mut self, fingerprint: u64) -> CacheResult<()> {
        let Some((ctx, has_table)) = self.mv_context_snapshot(fingerprint) else {
            trace!("mv build: precondition not met for {fingerprint}");
            metrics::counter!(names::CACHE_MV_SKIPPED_REBUILDS).increment(1);
            return Ok(());
        };

        if !has_table && !self.mv_gate_passes(&ctx, fingerprint).await? {
            self.mv_state_transition(fingerprint, MvState::Ineligible);
            trace!("mv build: size gate failed for {fingerprint}");
            return Ok(());
        }

        // Captured once, reused on rebuild. Failure aborts the build so a
        // Fresh MV always has names.
        let names = match self.mv_output_columns(fingerprint, &ctx).await {
            Ok(n) if !n.is_empty() => n,
            Ok(_) => {
                error!("mv build failed for {fingerprint}: query describe returned no columns");
                self.mv_state_transition(fingerprint, MvState::Pending { has_table });
                return Ok(());
            }
            Err(e) => {
                error!(
                    "mv build failed for {fingerprint}: output-column describe: {}",
                    error_chain_format(e.current_context()),
                );
                self.mv_state_transition(fingerprint, MvState::Pending { has_table });
                return Ok(());
            }
        };

        let start = Instant::now();
        let mv_table = mv_table_name(fingerprint);
        let batch = mv_build_batch(&mv_table, &ctx, has_table, names.len());

        if let Err(e) = self
            .db_cache
            .batch_execute(&batch)
            .await
            .map_into_report::<CacheError>()
            .attach_loc(if has_table {
                "mv rebuild transaction"
            } else {
                "creating MV table on first build"
            })
        {
            error!(
                "mv build failed for {fingerprint}: {}",
                error_chain_format(e.current_context()),
            );
            let cleanup = if has_table {
                "ROLLBACK; SET mem.query_generation = 0;".to_owned()
            } else {
                format!("SET mem.query_generation = 0; DROP TABLE IF EXISTS {mv_table};")
            };
            let _ = self.db_cache.batch_execute(&cleanup).await;
            self.mv_state_transition(fingerprint, MvState::Pending { has_table });
            return Ok(());
        }

        let elapsed = start.elapsed();
        let kind = if has_table { "rebuild" } else { "first_pop" };
        metrics::histogram!(names::CACHE_MV_BUILD_DURATION_SECONDS, "kind" => kind)
            .record(elapsed.as_secs_f64());

        // State re-check only matters for first-pop: CREATE TABLE AS is atomic
        // but not transactional with any follow-up — if source-row state flipped
        // during the async SQL, drop the (now stale) table and retry. Rebuild
        // is fully wrapped in BEGIN/COMMIT and takes the same snapshot-consistent
        // "overlapping read" accommodation as the existing design.
        if !has_table && !self.source_row_state_is_ready(fingerprint) {
            debug!(
                "mv build: source-row state changed during first build for {fingerprint}, \
                 dropping and resetting"
            );
            let _ = self
                .db_cache
                .batch_execute(&format!("DROP TABLE IF EXISTS {mv_table}"))
                .await;
            self.mv_state_transition(fingerprint, MvState::Pending { has_table: false });
            return Ok(());
        }

        // Only flip to Fresh if nobody raced us (e.g. CDC invalidation during the
        // build would have moved us to Pending { has_table: true }).
        if let Some(mut view) = self.state_view.cached_queries.get_mut(&fingerprint)
            && matches!(view.mv.state, MvState::Scheduled { .. })
        {
            view.mv.output_columns = Some(Arc::clone(&names));
            view.mv.state = MvState::Fresh;
        }

        metrics::counter!(names::CACHE_MV_REBUILDS).increment(1);
        trace!(
            "mv build ({kind}): fresh for {fingerprint} in {:?}",
            elapsed
        );
        Ok(())
    }

    /// PostgreSQL's output column names for the query. Reused from the
    /// view on rebuild; otherwise captured by describing `<resolved>
    /// LIMIT 0` against the cache DB (same query as source-row serve).
    async fn mv_output_columns(
        &self,
        fingerprint: u64,
        ctx: &MvBuildContext,
    ) -> CacheResult<Arc<[EcoString]>> {
        if let Some(v) = self.state_view.cached_queries.get(&fingerprint)
            && let Some(cols) = &v.mv.output_columns
        {
            return Ok(Arc::clone(cols));
        }

        let mut sql = String::with_capacity(256);
        ctx.resolved.deparse(&mut sql);
        sql.push_str(" LIMIT 0");

        let stream = self
            .db_cache
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
    async fn mv_gate_passes(&self, ctx: &MvBuildContext, fingerprint: u64) -> CacheResult<bool> {
        match ctx.shape_gate {
            ShapeGate::Materialize => Ok(true),
            ShapeGate::Skip => Ok(false),
            ShapeGate::Measure => {
                let source_rows = self.mv_source_rows_count(fingerprint).await?;
                self.mv_size_gate_passes(ctx, source_rows).await
            }
        }
    }

    /// Sum `count(*)` across the cache tables referenced by the fingerprint.
    /// Used as the denominator of the Measure size gate under the coordinator-
    /// driven first-pop flow (no population `row_count` available here).
    /// Returns 0 when relations cannot be resolved — causes the gate to fail,
    /// which is the safe default.
    async fn mv_source_rows_count(&self, fingerprint: u64) -> CacheResult<u64> {
        let relation_oids: Vec<u32> = match self.cache.cached_queries.get1(&fingerprint) {
            Some(q) => q.relation_oids.clone(),
            None => return Ok(0),
        };
        let tables: Vec<(String, String)> = relation_oids
            .iter()
            .filter_map(|oid| {
                self.cache
                    .tables
                    .get1(oid)
                    .map(|t| (t.schema.to_string(), t.name.to_string()))
            })
            .collect();

        let mut total: u64 = 0;
        for (schema, name) in tables {
            let sql = format!("SELECT count(*) FROM \"{schema}\".\"{name}\"");
            let row = self
                .db_cache
                .query_one(&sql, &[])
                .await
                .map_into_report::<CacheError>()
                .attach_loc("counting cache table rows for MV size gate")?;
            total = total.saturating_add(u64::try_from(row.get::<_, i64>(0)).unwrap_or(0));
        }
        Ok(total)
    }

    /// Snapshot the fields needed for an MV build from the state_view entry,
    /// plus the `has_table` bit from `Scheduled`. Returns None when the entry
    /// is missing, `mv_state` isn't `Scheduled { .. }`, or the source-row
    /// state isn't `Ready` (races resolved at the call site).
    fn mv_context_snapshot(&self, fingerprint: u64) -> Option<(MvBuildContext, bool)> {
        let view = self.state_view.cached_queries.get(&fingerprint)?;
        let MvState::Scheduled { has_table } = view.mv.state else {
            return None;
        };
        if view.state != CachedQueryState::Ready {
            return None;
        }
        let resolved = view.resolved.as_ref().map(Arc::clone)?;
        Some((
            MvBuildContext {
                shape_gate: view.mv.shape_gate,
                // MV body uses its own cap (joins only); independent of
                // the source-row population cap `view.max_limit`.
                max_limit: view.mv.limit,
                generation: view.generation,
                resolved,
            },
            has_table,
        ))
    }

    fn source_row_state_is_ready(&self, fingerprint: u64) -> bool {
        self.state_view
            .cached_queries
            .get(&fingerprint)
            .is_some_and(|v| v.state == CachedQueryState::Ready)
    }

    /// Transition `Fresh → Pending { has_table: true }` on the MV state for
    /// this fingerprint. No-op for any other state — dirty-marking a non-Fresh
    /// entry has no meaningful effect.
    ///
    /// Takes `&self` (mutation is via DashMap interior mutability) so callers
    /// holding `&self` in CDC paths don't need to become `&mut self`.
    pub(super) fn mv_dirty_mark(&self, fingerprint: u64) {
        if let Some(mut view) = self.state_view.cached_queries.get_mut(&fingerprint)
            && view.mv.state == MvState::Fresh
        {
            view.mv.state = MvState::Pending { has_table: true };
        }
    }

    /// Whether this query currently has a `Fresh` MV. Only `Fresh` queries
    /// need full CDC evaluation (so `mv_dirty_mark` can fire on a match);
    /// non-`Fresh` queries are short-circuitable in the membership check.
    pub(super) fn mv_is_fresh(&self, fingerprint: u64) -> bool {
        self.state_view
            .cached_queries
            .get(&fingerprint)
            .is_some_and(|v| v.mv.state == MvState::Fresh)
    }

    /// Eviction pre-sweep: truncate every MV in `Pending { has_table: true }`
    /// so the bytes are reclaimed before we start evicting live cache entries.
    ///
    /// These entries hold rows that will never be served — dead weight until
    /// the next hit rebuilds or eviction drops the table. Reclaiming them first
    /// means size pressure preferentially removes dead weight rather than
    /// evicting cache entries that might still be useful. The table persists
    /// (empty) so the next rebuild's `BEGIN; TRUNCATE; INSERT; COMMIT` still
    /// hits an existing table; state stays `Pending { has_table: true }` so
    /// coordinators keep falling through.
    ///
    /// Does **not** touch `Scheduled { .. }` (writer's own queue has a build
    /// pending; truncating would churn) or `Fresh` (still serving the fast
    /// path). Collects fingerprints into a Vec first so we don't hold a
    /// DashMap guard across awaits.
    pub(super) async fn mv_dirty_sweep(&self) -> CacheResult<()> {
        let dirty: Vec<u64> = self
            .state_view
            .cached_queries
            .iter()
            .filter(|entry| entry.mv.state == MvState::Pending { has_table: true })
            .map(|entry| *entry.key())
            .collect();

        if dirty.is_empty() {
            return Ok(());
        }

        for fingerprint in dirty {
            let mv_table = mv_table_name(fingerprint);
            if let Err(e) = self
                .db_cache
                .batch_execute(&format!("TRUNCATE {mv_table}"))
                .await
                .map_into_report::<CacheError>()
                .attach_loc("truncating dirty MV in eviction sweep")
            {
                error!(
                    "mv dirty-sweep truncate failed for {fingerprint}: {}",
                    error_chain_format(e.current_context()),
                );
            } else {
                metrics::counter!(names::CACHE_MV_DIRTY_TRUNCATES).increment(1);
            }
        }

        Ok(())
    }

    /// Drop the MV table for a fingerprint if its state indicates an on-disk
    /// table exists. Called from `cache_query_evict` before the `CachedQueryView`
    /// entry is removed. The caller is expected to pass the current `mv_state`
    /// read just before the evict (post-read the state_view entry will be gone).
    pub(super) async fn mv_drop(&self, fingerprint: u64, mv_state: MvState) -> CacheResult<()> {
        if !mv_state.has_table() {
            return Ok(());
        }
        let mv_table = mv_table_name(fingerprint);
        self.db_cache
            .batch_execute(&format!("DROP TABLE IF EXISTS {mv_table}"))
            .await
            .map_into_report::<CacheError>()
            .attach_loc("dropping MV table on eviction")?;
        Ok(())
    }

    /// Mutate `mv_state` on the state_view entry. No-op when the entry is gone
    /// (evicted during the build).
    fn mv_state_transition(&self, fingerprint: u64, new_state: MvState) {
        if let Some(mut view) = self.state_view.cached_queries.get_mut(&fingerprint) {
            view.mv.state = new_state;
        }
    }

    /// Revert `Scheduled { has_table }` → `Pending { has_table }` after an
    /// outer-level `mv_build` error (e.g. `mv_gate_passes` failed). The cache
    /// itself is intact and serving Ready; the next hit retries the build.
    pub(super) fn mv_build_failed_reset(&self, fingerprint: u64) {
        let Some(mut view) = self.state_view.cached_queries.get_mut(&fingerprint) else {
            return;
        };
        if let MvState::Scheduled { has_table } = view.mv.state {
            view.mv.state = MvState::Pending { has_table };
        }
    }

    /// Measure-gate: `result_rows × mv_size_ratio ≤ source_rows`.
    ///
    /// Runs `SELECT count(*) FROM (<query> LIMIT max_limit) x` against the
    /// source-row cache with the query's current generation set so the count
    /// sees the consistent snapshot. The LIMIT keeps the gate consistent with
    /// what would actually be stored (MV is capped at `max_limit`).
    async fn mv_size_gate_passes(
        &self,
        ctx: &MvBuildContext,
        source_rows: u64,
    ) -> CacheResult<bool> {
        let ratio = u64::from(self.cache.dynamic.load().mv_size_ratio);

        // Set query generation on cache DB for consistent snapshot filtering.
        let set_gen = format!("SET mem.query_generation = {}", ctx.generation);
        self.db_cache
            .batch_execute(&set_gen)
            .await
            .map_into_report::<CacheError>()
            .attach_loc("setting query generation for MV size gate")?;

        let count_sql = mv_count_sql(ctx);

        let result = self.db_cache.query_one(&count_sql, &[]).await;

        // Always reset generation, even on failure.
        let _ = self
            .db_cache
            .batch_execute("SET mem.query_generation = 0")
            .await;

        let row = result
            .map_into_report::<CacheError>()
            .attach_loc("executing MV size gate count")?;
        let result_rows = u64::try_from(row.get::<_, i64>(0)).unwrap_or(0);

        Ok(result_rows.saturating_mul(ratio) <= source_rows)
    }
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
