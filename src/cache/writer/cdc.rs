use std::collections::HashMap;
use std::fmt::Write;
use std::rc::Rc;
use std::time::Instant;

use ecow::EcoString;
use futures_util::stream::FuturesUnordered;
use postgres_protocol::escape;
use tokio_postgres::{Client, SimpleQueryMessage};
use tokio_stream::StreamExt;
use tracing::{debug, error, info, instrument, trace};

use crate::catalog::TableMetadata;
use crate::metrics::names;

use crate::query::ast::{BinaryOp, Deparse};
use crate::query::constraints::{QueryConstraints, TableConstraint};
use crate::query::evaluate::where_value_compare_string;
use crate::query::resolved::ResolvedQueryExpr;
use crate::query::transform::resolved_select_node_table_replace_with_values;

use crate::settings::{CachePolicy, Settings};

use crate::query::evaluate::where_expr_evaluate;

use super::super::messages::{CdcCommand, QueryCommand};
use super::super::mv::MvState;
use super::super::types::{
    CachedQueryState, SubqueryKind, UpdateEvalStrategy, UpdateQuery, UpdateQuerySource,
};
use super::super::{CacheError, CacheResult, MapIntoReport, ReportExt};
use super::core::{FrameState, WriterCore};
use super::deadlock::{SQLSTATE_DEADLOCK, cache_error_sqlstate};
use crate::pg;
use crate::result::error_chain_format;

/// Default capacity for dynamically built SQL strings.
const SQL_BUFFER_CAPACITY: usize = 1024;

/// Minimum number of connections in the cache pool for concurrent CDC updates.
const MIN_CACHE_POOL_SIZE: usize = 2;

/// Test-only deterministic fault injection (PGC-147). Compiled out entirely
/// unless built with `--features fault-injection`; the writer-side CDC `40P01`
/// is a timing race that cannot be provoked probabilistically, so the recovery
/// path is exercised by forcing it here.
#[cfg(feature = "fault-injection")]
mod fault {
    use std::sync::atomic::{AtomicBool, Ordering};

    static CDC_DEADLOCK_ONCE: AtomicBool = AtomicBool::new(false);

    /// Arm the one-shot from the environment (read once at writer startup).
    pub(super) fn init() {
        if std::env::var_os("PGCACHE_FAULT_CDC_DEADLOCK_ONCE").is_some() {
            CDC_DEADLOCK_ONCE.store(true, Ordering::Relaxed);
        }
    }

    /// True exactly once if armed — consumes the one-shot.
    pub(super) fn cdc_deadlock_take() -> bool {
        CDC_DEADLOCK_ONCE.swap(false, Ordering::Relaxed)
    }
}

/// Whether to simulate a CDC-frame `40P01` for the current insert. Always
/// `false` (and `core` untouched) unless built with `fault-injection`. The
/// one-shot is consumed only once a query is cached, so fixture-load inserts
/// (which precede any cached query) don't trip it — the injected deadlock
/// lands on a frame that actually has a relation to recover.
#[cfg(feature = "fault-injection")]
fn fault_cdc_deadlock_should_inject(core: &WriterCore) -> bool {
    core.cache.cached_queries.iter().next().is_some() && fault::cdc_deadlock_take()
}
#[cfg(not(feature = "fault-injection"))]
fn fault_cdc_deadlock_should_inject(_core: &WriterCore) -> bool {
    false
}

/// Rows-affected count from a single-statement `simple_query` result. Returns
/// the first `CommandComplete` count; 0 if none is present (e.g. statement
/// affected nothing or returned only rows).
fn simple_query_rows_affected(msgs: &[SimpleQueryMessage]) -> u64 {
    msgs.iter()
        .find_map(|m| {
            if let SimpleQueryMessage::CommandComplete(n) = m {
                Some(*n)
            } else {
                None
            }
        })
        .unwrap_or(0)
}

/// Boolean from a single-row `SELECT EXISTS (...)` `simple_query` result.
/// Postgres returns the boolean in text format; any non-`"t"` (including no
/// row) is treated as false.
fn simple_query_exists(msgs: &[SimpleQueryMessage]) -> bool {
    msgs.iter()
        .any(|m| matches!(m, SimpleQueryMessage::Row(r) if r.get(0) == Some("t")))
}

/// Distinguishes INSERT from DELETE so that subquery invalidation logic
/// can flip Inclusion/Exclusion semantics correctly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CdcOperation {
    Upsert,
    Delete,
}

/// Owns the CDC apply path: consumes `CdcCommand`s and applies mutations /
/// invalidations to the shared `WriterCore`. Holds the connection pool used
/// for concurrent CDC update execution and the applied-LSN watermark.
pub(super) struct WriterCdc {
    /// Read/eval pool: parallel predicate evaluation only (`SELECT EXISTS`).
    /// Read-only, autocommit — sees the pre-source-transaction snapshot, never
    /// the in-flight frame's uncommitted writes.
    pub(super) cache_pool: Vec<Rc<Client>>,
    /// Single dedicated write connection. All cache mutations for a source
    /// transaction are applied here inside one `BEGIN…COMMIT` spanning every
    /// message of that transaction, so cache readers observe the source
    /// transaction atomically. Distinct from `cache_pool` and from
    /// `WriterCore.db_cache`.
    pub(super) cdc_write_conn: Client,
    /// Highest LSN whose effects (cache mutations and invalidations) have been
    /// applied by this writer. Advances on `CommitMark` and `KeepAliveMark`,
    /// guaranteed transaction-aligned by mpsc ordering.
    pub(super) last_applied_lsn: u64,
}

/// Whether `pg_eval_matches` evaluates every query or stops at the first
/// match (membership-only / non-`Fresh` set).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PgEvalMode {
    All,
    FirstMatch,
}

impl WriterCdc {
    pub async fn new(settings: &Settings) -> CacheResult<Self> {
        // Create cache connection pool for concurrent CDC updates
        let cache_pool_size = (settings.num_workers / 2).max(MIN_CACHE_POOL_SIZE);
        let mut cache_pool = Vec::with_capacity(cache_pool_size);
        for i in 0..cache_pool_size {
            let pool_conn = pg::connect(&settings.cache, &format!("cache pool {i}"))
                .await
                .map_into_report::<CacheError>()?;
            cache_pool.push(Rc::new(pool_conn));
        }

        let cdc_write_conn = pg::connect(&settings.cache, "cdc write")
            .await
            .map_into_report::<CacheError>()?;

        #[cfg(feature = "fault-injection")]
        fault::init();

        Ok(Self {
            cache_pool,
            cdc_write_conn,
            last_applied_lsn: 0,
        })
    }

    /// `BEGIN` on the first cache-table write of the frame (`Active →
    /// TxnOpen`); idempotent for later writes. A write while `Idle` (no
    /// preceding `Begin`) is a bug.
    async fn frame_ensure(&mut self, core: &mut WriterCore) -> CacheResult<()> {
        debug_assert!(
            !matches!(core.frame_state, FrameState::Idle),
            "cache-table write before Begin (frame not entered)"
        );
        if core.frame_state == FrameState::Active {
            self.cdc_write_conn
                .batch_execute("BEGIN")
                .await
                .map_into_report::<CacheError>()?;
            core.frame_state = FrameState::TxnOpen;
        }
        Ok(())
    }

    /// Commit the frame txn. A write-less frame stayed `Active` and commits
    /// nothing (its invalidations flush separately at `CommitMark`).
    async fn frame_commit(&mut self, core: &mut WriterCore) -> CacheResult<()> {
        if core.frame_state == FrameState::TxnOpen {
            self.cdc_write_conn
                .batch_execute("COMMIT")
                .await
                .map_into_report::<CacheError>()?;
            core.frame_state = FrameState::Idle;
        }
        Ok(())
    }

    /// `40P01` aborted the whole frame txn: roll back (if a `BEGIN` was open)
    /// and enter `Recovering` (recovery happens at `CommitMark`, PGC-147).
    async fn frame_recover_enter(&mut self, core: &mut WriterCore) -> CacheResult<()> {
        if core.frame_state == FrameState::TxnOpen {
            self.cdc_write_conn
                .batch_execute("ROLLBACK")
                .await
                .map_into_report::<CacheError>()?;
        }
        core.frame_state = FrameState::Recovering;
        Ok(())
    }

    /// Apply the frame's deferred invalidations just before `COMMIT` — atomic
    /// with the maintenance, past the last deadlock-retriable point (a bare
    /// `COMMIT` under READ COMMITTED can't `40P01`).
    async fn frame_invalidations_flush(&mut self, core: &mut WriterCore) -> CacheResult<()> {
        if core.frame_invalidations.is_empty() {
            return Ok(());
        }
        // Collected out: `cache_query_cdc_invalidate` needs `&mut core`, so we
        // can't hold a borrow of `core.frame_invalidations` across the loop.
        let fps: Vec<u64> = core.frame_invalidations.iter().copied().collect();
        let count = fps.len() as u64;
        for fp in fps {
            self.cache_query_cdc_invalidate(core, fp)
                .await
                .attach_loc("flushing deferred invalidation")?;
        }
        metrics::counter!(names::CACHE_INVALIDATIONS).increment(count);
        core.state_gauges_update();
        Ok(())
    }

    /// Recover an aborted (`40P01`) frame: evict every query over the affected
    /// relations, then truncate those cache tables in a dedicated txn — a
    /// skipped Delete/Truncate may have left rows origin no longer has, and
    /// the queries repopulate from origin anyway.
    async fn frame_recover(&mut self, core: &mut WriterCore) -> CacheResult<()> {
        // Collected out: `cache_table_invalidate` needs `&mut core`.
        let oids: Vec<u32> = core.frame_relation_oids.iter().copied().collect();
        info!(
            relations = oids.len(),
            "cdc frame recovery: invalidating + truncating affected relations"
        );
        // Evict first so no query can read a mid-truncate cache table.
        for oid in &oids {
            core.cache_table_invalidate(*oid)
                .await
                .attach_loc("recover: invalidating affected relation")?;
        }
        if let Some(truncate) = Self::truncate_sql_build(core, oids.into_iter()) {
            self.cdc_write_conn
                .batch_execute(&format!("BEGIN; {truncate}; COMMIT"))
                .await
                .map_into_report::<CacheError>()
                .attach_loc("recover: truncating affected cache tables")?;
        }
        Ok(())
    }

    /// `40P01` from a DML handler → enter `Recovering` and swallow (PGC-147);
    /// any other error propagates (cache subsystem reset, as before).
    async fn frame_dml_result(
        &mut self,
        core: &mut WriterCore,
        r: CacheResult<()>,
    ) -> CacheResult<()> {
        let Err(e) = r else { return Ok(()) };
        if core.frame_state != FrameState::Recovering
            && cache_error_sqlstate(e.current_context()) == Some(SQLSTATE_DEADLOCK)
        {
            info!(
                relations = core.frame_relation_oids.len(),
                "cdc frame deadlocked (40P01); recovering affected relations"
            );
            self.frame_recover_enter(core).await?;
            return Ok(());
        }
        Err(e)
    }

    /// Handle a CDC command, dispatching to the appropriate method.
    pub async fn cdc_command_handle(
        &mut self,
        core: &mut WriterCore,
        cmd: CdcCommand,
    ) -> CacheResult<()> {
        let cmd_label = match &cmd {
            CdcCommand::Begin { .. } => "cdc_begin",
            CdcCommand::TableRegister(_) => "cdc_table_register",
            CdcCommand::Insert { .. } => "cdc_insert",
            CdcCommand::Update { .. } => "cdc_update",
            CdcCommand::Delete { .. } => "cdc_delete",
            CdcCommand::Truncate { .. } => "cdc_truncate",
            CdcCommand::CommitMark { .. } => "cdc_commit_mark",
            CdcCommand::KeepAliveMark { .. } => "cdc_keepalive_mark",
        };
        let handle_start = Instant::now();
        // Relation OIDs are recorded from frame start so a mid-frame 40P01 can
        // recover every relation the frame touched (pre-deadlock writes rolled
        // back too).
        match cmd {
            CdcCommand::Begin { xid } => {
                debug_assert!(
                    matches!(core.frame_state, FrameState::Idle),
                    "Begin within an active source-transaction frame"
                );
                trace!(xid, "cdc frame begin");
                core.frame_state = FrameState::Active;
            }
            CdcCommand::TableRegister(table_metadata) => {
                core.frame_relation_oids.insert(table_metadata.relation_oid);
                core.cache_table_register(table_metadata)
                    .await
                    .attach_loc("cdc table register")?;
            }
            CdcCommand::Insert {
                relation_oid,
                row_data,
            } => {
                core.frame_relation_oids.insert(relation_oid);
                if core.frame_state != FrameState::Recovering {
                    if fault_cdc_deadlock_should_inject(core) {
                        // Behave exactly as a real 40P01 victim (PGC-147).
                        self.frame_recover_enter(core)
                            .await
                            .attach_loc("fault: injected cdc deadlock")?;
                    } else {
                        let r = self.handle_insert(core, relation_oid, row_data).await;
                        self.frame_dml_result(core, r)
                            .await
                            .attach_loc("cdc insert")?;
                    }
                }
            }
            CdcCommand::Update {
                relation_oid,
                key_data,
                row_data,
            } => {
                core.frame_relation_oids.insert(relation_oid);
                if core.frame_state != FrameState::Recovering {
                    let r = self
                        .handle_update(core, relation_oid, key_data, row_data)
                        .await;
                    self.frame_dml_result(core, r)
                        .await
                        .attach_loc("cdc update")?;
                }
            }
            CdcCommand::Delete {
                relation_oid,
                row_data,
            } => {
                core.frame_relation_oids.insert(relation_oid);
                if core.frame_state != FrameState::Recovering {
                    let r = self.handle_delete(core, relation_oid, row_data).await;
                    self.frame_dml_result(core, r)
                        .await
                        .attach_loc("cdc delete")?;
                }
            }
            CdcCommand::Truncate { relation_oids } => {
                core.frame_relation_oids
                    .extend(relation_oids.iter().copied());
                if core.frame_state != FrameState::Recovering {
                    let r = self.handle_truncate(core, &relation_oids).await;
                    self.frame_dml_result(core, r)
                        .await
                        .attach_loc("cdc truncate")?;
                }
            }
            CdcCommand::CommitMark { lsn } => {
                match core.frame_state {
                    FrameState::TxnOpen => {
                        self.frame_invalidations_flush(core).await?;
                        self.frame_commit(core)
                            .await
                            .attach_loc("cdc commit frame")?;
                    }
                    // A frame can flag queries for invalidation without any
                    // in-place write (e.g. a growing-join insert excluded from
                    // in-place maintenance) — no `BEGIN` was opened, but the
                    // flagged queries must still be invalidated here.
                    FrameState::Active => {
                        self.frame_invalidations_flush(core).await?;
                    }
                    FrameState::Recovering => {
                        // Relation-level recovery evicts every query over the
                        // affected relations — a superset of the selectively
                        // flagged fps, so the fp flush is subsumed.
                        self.frame_recover(core)
                            .await
                            .attach_loc("cdc recover frame")?;
                    }
                    // CommitMark without a preceding Begin: pgoutput always
                    // pairs them for published txns, so this is unreachable.
                    FrameState::Idle => {
                        debug_assert!(false, "CommitMark without a preceding Begin");
                    }
                }
                core.frame_state = FrameState::Idle;
                core.frame_invalidations.clear();
                core.frame_relation_oids.clear();
                self.applied_lsn_advance(lsn);
                // Frame is closed; flush maintenance that was deferred while it
                // was open (it would have deadlocked on the frame's locks).
                if core.purge_pending {
                    let threshold = core.cache.generation_purge_threshold();
                    core.generation_purge(threshold)
                        .await
                        .attach_loc("deferred generation purge")?;
                    core.purge_pending = false;
                }
            }
            CdcCommand::KeepAliveMark { lsn } => {
                // Keepalives only arrive between source transactions, so the
                // frame must be Idle. The guard keeps the watermark from
                // advancing past an active frame if that ever breaks.
                debug_assert!(
                    core.frame_state == FrameState::Idle,
                    "keepalive received with an active source-transaction frame"
                );
                if core.frame_state == FrameState::Idle {
                    self.applied_lsn_advance(lsn);
                }
            }
        }
        // Self-defers while the frame is open; flushes here at CommitMark
        // (frame just committed) and KeepAlive (no frame).
        core.publication_dirty_drain().await?;
        metrics::histogram!(names::CACHE_WRITER_COMMAND_HANDLE_SECONDS, "cmd" => cmd_label)
            .record(handle_start.elapsed().as_secs_f64());
        Ok(())
    }

    /// Advance `last_applied_lsn` forward to `lsn`, updating the Prometheus
    /// gauge. No-op if `lsn` does not advance the watermark.
    fn applied_lsn_advance(&mut self, lsn: u64) {
        if lsn > self.last_applied_lsn {
            self.last_applied_lsn = lsn;
            // LSNs past 2^53 lose precision in f64 (~9 PB of WAL — irrelevant).
            #[allow(clippy::cast_precision_loss)]
            metrics::gauge!(names::CDC_APPLIED_LSN).set(lsn as f64);
        }
    }

    /// Handle INSERT operation.
    #[instrument(skip_all)]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn handle_insert(
        &mut self,
        core: &mut WriterCore,
        relation_oid: u32,
        row_data: Vec<Option<String>>,
    ) -> CacheResult<()> {
        let start = Instant::now();
        metrics::counter!(names::CACHE_HANDLE_INSERTS).increment(1);

        // CDC event for a relation we don't cache (never cached, or its
        // queries were evicted) is a benign no-op — not a frame-consistency
        // failure. Skip without erroring so it doesn't trip the reset path.
        if !core.cache.tables.contains_key1(&relation_oid) {
            return Ok(());
        }

        let fp_list = self
            .update_queries_check_invalidate(
                core,
                relation_oid,
                None,
                &row_data,
                None,
                CdcOperation::Upsert,
            )
            .attach_loc("checking for query invalidations")?;

        // Defer the actual invalidation to just before the frame COMMIT
        // (frame_invalidations_flush) so it is atomic with the maintenance
        // it accompanies rather than visible mid-frame.
        let invalidation_count = fp_list.len() as u64;
        core.frame_invalidations.extend(fp_list);

        let matched = self
            .update_queries_execute_concurrent(core, relation_oid, &row_data)
            .await?;

        if matched {
            let total = core
                .cache
                .update_queries
                .get(&relation_oid)
                .map_or(0, |q| q.queries.len() as u64);
            let freshness_count = total.saturating_sub(invalidation_count);
            if freshness_count > 0 {
                metrics::counter!(names::CACHE_FRESHNESS_HITS).increment(freshness_count);
            }
        }

        metrics::histogram!(names::CACHE_HANDLE_INSERT_SECONDS)
            .record(start.elapsed().as_secs_f64());
        Ok(())
    }

    /// Handle UPDATE operation.
    #[instrument(skip_all)]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn handle_update(
        &mut self,
        core: &mut WriterCore,
        relation_oid: u32,
        key_data: Vec<Option<String>>,
        new_row_data: Vec<Option<String>>,
    ) -> CacheResult<()> {
        let start = Instant::now();
        metrics::counter!(names::CACHE_HANDLE_UPDATES).increment(1);

        // See handle_insert: an untracked relation's CDC is a benign skip.
        if !core.cache.tables.contains_key1(&relation_oid) {
            return Ok(());
        }

        let row_changes = self
            .query_row_changes(core, relation_oid, &new_row_data)
            .await?;
        trace!("row_changes {:?}", row_changes);

        let fp_list = self.update_queries_check_invalidate(
            core,
            relation_oid,
            row_changes.as_ref(),
            &new_row_data,
            Some(&key_data),
            CdcOperation::Upsert,
        )?;
        let invalidation_count = fp_list.len() as u64;
        trace!("invalidation_count {}", invalidation_count);
        // Deferred to frame_invalidations_flush (see handle_insert).
        core.frame_invalidations.extend(fp_list);

        let matched = self
            .update_queries_execute_concurrent(core, relation_oid, &new_row_data)
            .await?;

        if matched {
            let total = core
                .cache
                .update_queries
                .get(&relation_oid)
                .map_or(0, |q| q.queries.len() as u64);
            let freshness_count = total.saturating_sub(invalidation_count);
            if freshness_count > 0 {
                metrics::counter!(names::CACHE_FRESHNESS_HITS).increment(freshness_count);
            }
        }

        if !matched {
            let Some(table_metadata) = core.cache.tables.get1(&relation_oid) else {
                error!("No table metadata found for relation_oid: {}", relation_oid);
                return Err(CacheError::UnknownTable {
                    oid: Some(relation_oid),
                    name: None,
                }
                .into());
            };

            let delete_sql = self.cache_delete_sql(table_metadata, &new_row_data)?;
            self.frame_ensure(core).await?;
            self.cdc_write_conn
                .batch_execute(delete_sql.as_str())
                .await
                .map_into_report::<CacheError>()?;
        }

        if !key_data.is_empty() {
            let Some(table_metadata) = core.cache.tables.get1(&relation_oid) else {
                error!("No table metadata found for relation_oid: {}", relation_oid);
                return Err(CacheError::UnknownTable {
                    oid: Some(relation_oid),
                    name: None,
                }
                .into());
            };

            let delete_sql = self.cache_delete_sql(table_metadata, &key_data)?;
            self.frame_ensure(core).await?;
            self.cdc_write_conn
                .batch_execute(delete_sql.as_str())
                .await
                .map_into_report::<CacheError>()?;
        }

        metrics::histogram!(names::CACHE_HANDLE_UPDATE_SECONDS)
            .record(start.elapsed().as_secs_f64());
        Ok(())
    }

    /// Handle DELETE operation.
    ///
    /// Deletes the row from cache tables and checks for subquery invalidations.
    /// For Exclusion subquery tables (NOT IN, NOT EXISTS), a DELETE shrinks the
    /// exclusion set, which grows the outer result set — requiring invalidation.
    #[instrument(skip_all)]
    pub async fn handle_delete(
        &mut self,
        core: &mut WriterCore,
        relation_oid: u32,
        row_data: Vec<Option<String>>,
    ) -> CacheResult<()> {
        let start = Instant::now();
        metrics::counter!(names::CACHE_HANDLE_DELETES).increment(1);

        let table_metadata = match core.cache.tables.get1(&relation_oid) {
            Some(metadata) => metadata,
            None => {
                error!("No table metadata found for relation_oid: {}", relation_oid);
                metrics::histogram!(names::CACHE_HANDLE_DELETE_SECONDS)
                    .record(start.elapsed().as_secs_f64());
                return Ok(());
            }
        };

        let delete_sql = self.cache_delete_sql(table_metadata, &row_data)?;
        self.frame_ensure(core).await?;
        let rows_deleted = simple_query_rows_affected(
            &self
                .cdc_write_conn
                .simple_query(delete_sql.as_str())
                .await
                .map_into_report::<CacheError>()?,
        );

        // Check for subquery invalidations — removing a row can expand the
        // final result set for Exclusion/Scalar subquery tables
        let mut invalidation_count = 0u64;
        if core.cache.update_queries.contains_key(&relation_oid) {
            let fp_list = self
                .update_queries_check_invalidate(
                    core,
                    relation_oid,
                    None,
                    &row_data,
                    None,
                    CdcOperation::Delete,
                )
                .attach_loc("checking delete invalidations")?;

            invalidation_count = fp_list.len() as u64;
            // Deferred to frame_invalidations_flush (see handle_insert).
            core.frame_invalidations.extend(fp_list);
        }

        if rows_deleted > 0 {
            let total = core
                .cache
                .update_queries
                .get(&relation_oid)
                .map_or(0, |q| q.queries.len() as u64);
            let freshness_count = total.saturating_sub(invalidation_count);
            if freshness_count > 0 {
                metrics::counter!(names::CACHE_FRESHNESS_HITS).increment(freshness_count);
            }
        }

        metrics::histogram!(names::CACHE_HANDLE_DELETE_SECONDS)
            .record(start.elapsed().as_secs_f64());
        Ok(())
    }

    /// Handle TRUNCATE operation.
    ///
    /// The physical `TRUNCATE` of the source tables' cache tables runs in-frame
    /// on `cdc_write_conn` (atomic with the rest of the source transaction).
    /// Additionally, every cached query referencing a truncated relation is
    /// invalidated: a table-wide empty can change derived/multi-table results
    /// in ways the in-place model can't track, so those queries repopulate
    /// from origin.
    #[instrument(skip_all)]
    pub async fn handle_truncate(
        &mut self,
        core: &mut WriterCore,
        relation_oids: &[u32],
    ) -> CacheResult<()> {
        if let Some(sql) = Self::truncate_sql_build(core, relation_oids.iter().copied()) {
            self.frame_ensure(core).await?;
            self.cdc_write_conn
                .batch_execute(sql.as_str())
                .await
                .map_into_report::<CacheError>()?;
        }

        for oid in relation_oids {
            core.cache_table_invalidate(*oid)
                .await
                .attach_loc("invalidating queries on truncate")?;
        }

        Ok(())
    }

    /// Build `TRUNCATE <cache table>, ...` for the relations' cache tables,
    /// or `None` if none of the oids map to a known cache table. Shared by
    /// `handle_truncate` and the `40P01` recovery path.
    fn truncate_sql_build(core: &WriterCore, oids: impl Iterator<Item = u32>) -> Option<String> {
        let mut sql = String::with_capacity(SQL_BUFFER_CAPACITY);
        sql.push_str("TRUNCATE ");
        let mut first = true;
        for oid in oids {
            if let Some(table_metadata) = core.cache.tables.get1(&oid) {
                if !first {
                    sql.push_str(", ");
                }
                let _ = write!(sql, "{}.{}", table_metadata.schema, table_metadata.name);
                first = false;
            }
        }
        if first { None } else { Some(sql) }
    }

    /// CDC-triggered invalidation of a cached query.
    /// For FIFO: delegates to full eviction.
    /// For CLOCK: marks the entry as Invalidated, keeping metadata for fast readmission.
    /// Removes from generations BTreeSet and purges stale rows, but preserves
    /// cached_queries entry and update_queries for reuse on readmission.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) async fn cache_query_cdc_invalidate(
        &self,
        core: &mut WriterCore,
        fingerprint: u64,
    ) -> CacheResult<()> {
        // Pinned queries: defer readmission to the writer event loop
        if core
            .cache
            .cached_queries
            .get1(&fingerprint)
            .is_some_and(|q| q.pinned)
        {
            debug!("pinned query invalidated, deferring readmit {fingerprint}");
            let _ = core.query_tx.send(QueryCommand::Readmit { fingerprint });
            return Ok(());
        }

        let cfg = core.cache.dynamic.load();

        if cfg.cache_policy == CachePolicy::Fifo {
            return core.cache_query_evict(fingerprint).await;
        }

        let Some(query) = core.cache.cached_queries.get1(&fingerprint) else {
            return Ok(());
        };

        // Already invalidated — nothing to do
        if query.invalidated {
            return Ok(());
        }

        let generation = query.generation;
        debug!("cdc invalidating query {fingerprint}");
        if let Some(mut m) = core.state_view.metrics.get_mut(&fingerprint) {
            m.invalidation_count += 1;
            m.cached_since_ns = None;
        }

        let prev_generation_threshold = core.cache.generation_purge_threshold();

        // Remove from active generations (no longer serving cached results)
        core.cache.generations.remove(&generation);

        // Mark as invalidated (keep entry for metadata reuse on readmission)
        if let Some(mut query) = core.cache.cached_queries.get1_mut(&fingerprint) {
            query.invalidated = true;
        }

        // Update state view to Invalidated. Fold the MV dirty transition into
        // the same get_mut block so coordinators observe both transitions
        // atomically — a reader that sees state=Invalidated never sees the MV
        // in a stale-Fresh state.
        if let Some(mut entry) = core.state_view.cached_queries.get_mut(&fingerprint) {
            entry.state = CachedQueryState::Invalidated;
            entry.referenced = false;
            if entry.mv.state == MvState::Fresh {
                entry.mv.state = MvState::Pending { has_table: true };
            }
        }

        // Purge stale rows if generation threshold moved
        let new_threshold = core.cache.generation_purge_threshold();
        if new_threshold > prev_generation_threshold {
            let mut current_size = core.cache_size_load().await?;

            if cfg.cache_size.is_some_and(|s| current_size > s) {
                core.generation_purge(new_threshold).await?;
                current_size = core.cache_size_load().await?;
            }

            core.cache.current_size = current_size as usize;
        }

        Ok(())
    }

    /// SELECT one row from the cache, projecting a boolean per non-PK column
    /// that's true iff the cached value differs from the incoming `row_data`
    /// value. Used by CDC UPDATE handling to decide whether a column change
    /// actually shifts query membership. Returns `None` when the row isn't in
    /// the cache (no PK match).
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) async fn query_row_changes(
        &self,
        core: &WriterCore,
        relation_oid: u32,
        row_data: &[Option<String>],
    ) -> CacheResult<Option<HashMap<EcoString, bool>>> {
        let table_metadata =
            core.cache
                .tables
                .get1(&relation_oid)
                .ok_or(CacheError::UnknownTable {
                    oid: Some(relation_oid),
                    name: None,
                })?;

        // Build SELECT ... FROM ... WHERE ... in a single String.
        // Comparison columns go in the SELECT list, PK conditions in the WHERE clause.
        let mut sql = String::with_capacity(SQL_BUFFER_CAPACITY);
        sql.push_str("SELECT ");

        let mut first_col = true;
        for column_meta in &table_metadata.columns {
            let position = column_meta.index();
            if let Some(row_value) = row_data.get(position) {
                let value = row_value
                    .as_deref()
                    .map_or_else(|| "NULL".to_owned(), escape::escape_literal);
                if !first_col {
                    sql.push_str(", ");
                }
                let _ = write!(
                    sql,
                    "{} IS DISTINCT FROM {} AS {}",
                    column_meta.name, value, column_meta.name
                );
                first_col = false;
            }
        }

        let _ = write!(
            sql,
            " FROM {}.{} WHERE ",
            table_metadata.schema, table_metadata.name
        );

        let mut has_pk = false;
        for pk_column in &table_metadata.primary_key_columns {
            if let Some(column_meta) = table_metadata.columns.get(pk_column.as_str()) {
                let position = column_meta.index();
                if let Some(row_value) = row_data.get(position) {
                    let value = row_value
                        .as_deref()
                        .map_or_else(|| "NULL".to_owned(), escape::escape_literal);
                    if has_pk {
                        sql.push_str(" AND ");
                    }
                    let _ = write!(sql, "{pk_column} = {value}");
                    has_pk = true;
                }
            }
        }

        if !has_pk {
            return Err(CacheError::NoPrimaryKey.into());
        }

        let msgs = core
            .db_cache
            .simple_query(&sql)
            .await
            .map_into_report::<CacheError>()?;

        for msg in msgs {
            if let SimpleQueryMessage::Row(row) = msg {
                let mut changes = HashMap::with_capacity(row.len());
                for (idx, col) in row.columns().iter().enumerate() {
                    // PG boolean text format: "t" = true, "f" = false. NULL
                    // shouldn't occur (IS DISTINCT FROM always returns t/f),
                    // but treat any non-"t" as false defensively.
                    let changed = row.get(idx) == Some("t");
                    changes.insert(EcoString::from(col.name()), changed);
                }
                return Ok(Some(changes));
            }
        }
        Ok(None)
    }

    /// Check if all WHERE constraints for a table match the given row values.
    /// Returns true if all constraints match (or no constraints exist for this table).
    fn row_constraints_match(
        &self,
        constraints: &QueryConstraints,
        table_metadata: &TableMetadata,
        row_data: &[Option<String>],
    ) -> bool {
        let Some(constraints) = constraints
            .table_constraints
            .get(table_metadata.name.as_str())
        else {
            return true;
        };

        for constraint in constraints {
            let column_name = match constraint {
                TableConstraint::Comparison(col, ..) | TableConstraint::AnyOf(col, ..) => {
                    col.as_str()
                }
            };

            if let Some(column_meta) = table_metadata.columns.get(column_name) {
                let position = column_meta.index();
                if let Some(row_value) = row_data.get(position) {
                    let matches = match row_value {
                        Some(row_str) => match constraint {
                            TableConstraint::Comparison(_, op, val) => {
                                where_value_compare_string(val, row_str, *op)
                            }
                            TableConstraint::AnyOf(_, values) => values
                                .iter()
                                .any(|v| where_value_compare_string(v, row_str, BinaryOp::Equal)),
                        },
                        // NULL never matches comparison operators
                        None => false,
                    };
                    if !matches {
                        return false;
                    }
                }
            }
        }

        true
    }

    /// Determine if a query should be invalidated when the row is not currently cached.
    /// Returns true if the query should be invalidated.
    fn row_uncached_invalidation_check(
        &self,
        update_query: &UpdateQuery,
        table_metadata: &TableMetadata,
        row_data: &[Option<String>],
        key_data: Option<&[Option<String>]>,
        operation: CdcOperation,
    ) -> bool {
        match update_query.source {
            UpdateQuerySource::FromClause => {
                // DELETE on a limited query's table: cached result may have fewer
                // rows than the LIMIT window. Invalidate to trigger re-population.
                if update_query.has_limit && operation == CdcOperation::Delete {
                    return true;
                }

                // DELETE on FromClause source: the row is already removed from the cache
                // table. For INNER JOIN (the only join type that gets FromClause source),
                // removing a row can only shrink the result set, never expand it.
                // Serve-time re-evaluation handles correctness.
                if operation == CdcOperation::Delete {
                    return false;
                }

                // Single-table queries don't need invalidation for uncached rows
                if update_query.resolved.is_single_table() {
                    return false;
                }

                let has_table_constraints = update_query
                    .constraints
                    .table_constraints
                    .contains_key(table_metadata.name.as_str());

                // If key_data is empty, PK didn't change. If all join columns are PK columns
                // and there are no WHERE constraints for this table, the row's membership
                // in the result set is unchanged - skip invalidation.
                if !has_table_constraints {
                    !join_membership_unchanged(update_query, table_metadata, key_data)
                } else {
                    // Check if row matches table constraints - invalidate only if it matches
                    self.row_constraints_match(&update_query.constraints, table_metadata, row_data)
                }
            }
            UpdateQuerySource::Subquery(kind) => {
                let has_table_constraints = update_query
                    .constraints
                    .table_constraints
                    .contains_key(table_metadata.name.as_str());

                // If key_data is empty, PK didn't change. If all join columns are PK columns
                // and there are no WHERE constraints for this table, the row's membership
                // in the result set is unchanged - skip invalidation.
                let row_added = if !has_table_constraints {
                    !join_membership_unchanged(update_query, table_metadata, key_data)
                } else {
                    self.row_constraints_match(&update_query.constraints, table_metadata, row_data)
                };

                // Check constraints — if row doesn't match constraints for this
                // table, it's not relevant to the cached query
                if !row_added {
                    return false;
                }

                // Only invalidate when the change can expand the final result set.
                // Changes that can only contract it are safe to skip (extra cached
                // rows are acceptable, missing rows are not).
                //
                // INSERT + Inclusion: grows IN set → expands result → invalidate.
                // INSERT + Exclusion: grows exclusion set → contracts result → skip.
                // DELETE + Inclusion: shrinks IN set → contracts result → skip.
                // DELETE + Exclusion: shrinks exclusion set → expands result → invalidate.
                // Scalar: any change can shift the value → always invalidate.
                match (kind, operation) {
                    (SubqueryKind::Scalar, _) => true,
                    (SubqueryKind::Inclusion, CdcOperation::Upsert) => true,
                    (SubqueryKind::Inclusion, CdcOperation::Delete) => false,
                    (SubqueryKind::Exclusion, CdcOperation::Upsert) => false,
                    (SubqueryKind::Exclusion, CdcOperation::Delete) => true,
                }
            }
            UpdateQuerySource::OuterJoinTerminal => {
                // Terminal optional side of an outer join. Changes here only
                // affect NULL-padded columns — the preserved side already has
                // the row. No cross-table dependencies, so the update query
                // execution handles it (upsert into cache table).
                false
            }
            UpdateQuerySource::OuterJoinOptional => {
                // Non-terminal optional side of an outer join. Changes here can
                // cascade to affect other tables' result set membership (e.g. a
                // new match may activate a downstream join path that was previously
                // NULL-padded). Invalidate if the row is relevant to this query.
                self.row_constraints_match(&update_query.constraints, table_metadata, row_data)
            }
        }
    }

    /// Determine if a query should be invalidated when the row exists in cache.
    /// Returns true if the query should be invalidated.
    fn row_cached_invalidation_check(
        &self,
        update_query: &UpdateQuery,
        table_metadata: &TableMetadata,
        row_data: &[Option<String>],
        row_changes: &HashMap<EcoString, bool>,
    ) -> bool {
        // Subquery and non-terminal outer join tables: always invalidate on
        // UPDATE — column changes could shift set membership or cascade to
        // affect downstream joins/predicates
        if matches!(
            update_query.source,
            UpdateQuerySource::Subquery(_) | UpdateQuerySource::OuterJoinOptional
        ) {
            return true;
        }

        for column in update_query
            .constraints
            .table_join_columns(&table_metadata.name)
        {
            // Missing column would mean query constraints reference a column
            // that wasn't projected — a builder invariant violation, not a
            // runtime condition. Silent `false` would risk missed
            // invalidations and stale reads, so panic instead.
            let column_changed = *row_changes
                .get(column)
                .expect("column present in row_changes");

            if !column_changed {
                continue;
            }

            // Check constraints - skip if row doesn't match
            if !self.row_constraints_match(&update_query.constraints, table_metadata, row_data) {
                continue;
            }

            return true;
        }

        false
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) fn update_queries_check_invalidate(
        &self,
        core: &WriterCore,
        relation_oid: u32,
        row_changes: Option<&HashMap<EcoString, bool>>,
        row_data: &[Option<String>],
        key_data: Option<&[Option<String>]>,
        operation: CdcOperation,
    ) -> CacheResult<Vec<u64>> {
        // No cached query references this relation (never registered, or all
        // its queries were evicted) → nothing to invalidate. Not an error.
        let Some(update_queries) = core.cache.update_queries.get(&relation_oid) else {
            return Ok(Vec::new());
        };
        let Some(table_metadata) = core.cache.tables.get1(&relation_oid) else {
            return Ok(Vec::new());
        };

        let mut fp_list = Vec::new();
        for update_query in update_queries.iter_complexity_ordered() {
            // Guard clause: handle uncached rows (INSERT or UPDATE where row not in cache)
            if row_changes.is_none() {
                if self.row_uncached_invalidation_check(
                    update_query,
                    table_metadata,
                    row_data,
                    key_data,
                    operation,
                ) {
                    fp_list.push(update_query.fingerprint);
                }
                continue;
            }

            // Main path: handle cached rows (UPDATE where row exists in cache)
            // row_changes is guaranteed to be Some here due to the guard clause above
            if let Some(row_changes) = row_changes
                && self.row_cached_invalidation_check(
                    update_query,
                    table_metadata,
                    row_data,
                    row_changes,
                )
            {
                fp_list.push(update_query.fingerprint);
            }
        }

        Ok(fp_list)
    }

    /// Decide whether a CDC row belongs in cache and, if so, upsert it once.
    ///
    /// Phase A (parallel, read-only): determine which cached queries the row
    /// matches. LocalEval queries are evaluated in Rust; PgEval queries run
    /// `SELECT EXISTS (<query>)` fanned across `cache_pool` — autocommit, so
    /// they observe the pre-source-transaction snapshot, never the in-flight
    /// frame's uncommitted writes.
    ///
    /// Phase B (serialized, in-frame): if any query matched, a single
    /// unconditional upsert into the source table's cache table on
    /// `cdc_write_conn`. The shared cache table holds the row iff some cached
    /// query needs it, so one upsert suffices regardless of how many matched.
    ///
    /// Returns true if the row matched any cached query.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) async fn update_queries_execute_concurrent(
        &mut self,
        core: &mut WriterCore,
        relation_oid: u32,
        row_data: &[Option<String>],
    ) -> CacheResult<bool> {
        // Phase A: membership evaluation. The `core` borrow is confined to
        // this block so the in-frame write below doesn't hold it.
        let (matched, upsert_sql) = {
            // No cached query references this relation → nothing to upsert.
            // Not an error (the relation simply isn't maintained in place).
            let Some(update_queries) = core.cache.update_queries.get(&relation_oid) else {
                return Ok(false);
            };
            let Some(table_metadata) = core.cache.tables.get1(&relation_oid) else {
                return Ok(false);
            };

            let total_queries = update_queries.queries.len();
            trace!("update_queries_execute_concurrent start [{total_queries}]");
            if total_queries == 0 {
                return Ok(false);
            }

            // Fresh-MV queries must be fully evaluated so every match is
            // dirty-marked (else a Fresh MV silently goes stale). Non-Fresh
            // queries only decide whether the row belongs in the shared
            // cache table — one match triggers the single upsert, so they
            // short-circuit, exactly as before MV existed.
            let mut matched = false;

            // LocalEval: Rust evaluation, complexity-ordered (simplest
            // first). Cheap, so always evaluated in full; `mv_dirty_mark`
            // self-gates on `Fresh`.
            let mut local_hit = false;
            for update_query in update_queries.iter_complexity_ordered() {
                // Flagged for invalidation this frame → not maintained in
                // place (it forwards to origin and repopulates). Matches the
                // pre-deferral ordering, where the inline invalidate ran
                // before this executor so such queries were already excluded.
                if core.frame_invalidations.contains(&update_query.fingerprint) {
                    continue;
                }
                if update_query.eval_strategy != UpdateEvalStrategy::LocalEval {
                    continue;
                }
                if !update_query_matches_locally(update_query, table_metadata, row_data) {
                    continue;
                }
                trace!(
                    "update_queries local-eval matched fingerprint {}",
                    update_query.fingerprint
                );
                core.mv_dirty_mark(update_query.fingerprint);
                matched = true;
                local_hit = true;
            }
            if local_hit {
                metrics::counter!(names::CACHE_CDC_LOCAL_EVAL_HITS).increment(1);
            }

            // PgEval (the expensive set): only Fresh-MV queries need full
            // evaluation (to dirty-mark matches); the rest short-circuit.
            let pg_eval: Vec<&UpdateQuery> = update_queries
                .iter_complexity_ordered()
                .filter(|q| {
                    q.eval_strategy == UpdateEvalStrategy::PgEval
                        && !core.frame_invalidations.contains(&q.fingerprint)
                })
                .collect();

            if !pg_eval.is_empty() {
                let (fresh_pg, rest_pg): (Vec<&UpdateQuery>, Vec<&UpdateQuery>) = pg_eval
                    .into_iter()
                    .partition(|q| core.mv_is_fresh(q.fingerprint));

                let fresh_hits = self
                    .pg_eval_matches(&fresh_pg, table_metadata, row_data, PgEvalMode::All)
                    .await?;
                for &fingerprint in &fresh_hits {
                    core.mv_dirty_mark(fingerprint);
                }
                let mut pg_hit = !fresh_hits.is_empty();
                matched |= pg_hit;

                // Non-Fresh queries only decide the upsert: skip the whole
                // set once anything matched, else stop at the first match.
                if !matched {
                    let rest_hits = self
                        .pg_eval_matches(&rest_pg, table_metadata, row_data, PgEvalMode::FirstMatch)
                        .await?;
                    if !rest_hits.is_empty() {
                        matched = true;
                        pg_hit = true;
                    }
                }

                if pg_hit {
                    metrics::counter!(names::CACHE_CDC_PG_EVAL_HITS).increment(1);
                }
            }

            let upsert_sql =
                matched.then(|| self.cache_upsert_unconditional_sql(table_metadata, row_data));
            (matched, upsert_sql)
        };

        // Phase B: single in-frame write.
        if let Some(sql) = upsert_sql {
            self.frame_ensure(core).await?;
            self.cdc_write_conn
                .batch_execute(sql.as_str())
                .await
                .map_into_report::<CacheError>()?;
        }

        Ok(matched)
    }

    /// Run `SELECT EXISTS (<predicate>)` for each query in parallel across
    /// the read pool (pool-sized batches) and return the fingerprints that
    /// matched. `FirstMatch` stops launching further batches once any match
    /// is seen — for the membership-only (non-`Fresh`) set where one match
    /// is enough to trigger the shared-table upsert.
    async fn pg_eval_matches(
        &self,
        queries: &[&UpdateQuery],
        table_metadata: &TableMetadata,
        row_data: &[Option<String>],
        mode: PgEvalMode,
    ) -> CacheResult<Vec<u64>> {
        let mut hits = Vec::new();
        if queries.is_empty() {
            return Ok(hits);
        }
        let pool_size = self.cache_pool.len();
        let mut iter = queries.iter();
        loop {
            let mut futures = FuturesUnordered::new();
            for (idx, uq) in iter.by_ref().take(pool_size).enumerate() {
                let fingerprint = uq.fingerprint;
                let sql =
                    self.cache_predicate_exists_sql(&uq.resolved, table_metadata, row_data)?;
                let conn = Rc::clone(
                    self.cache_pool
                        .get(idx % pool_size)
                        .ok_or(CacheError::Other)?,
                );
                futures.push(async move {
                    let r = conn
                        .simple_query(&sql)
                        .await
                        .map(|msgs| simple_query_exists(&msgs));
                    (fingerprint, r)
                });
            }
            if futures.is_empty() {
                break;
            }
            while let Some((fingerprint, r)) = futures.next().await {
                match r {
                    Ok(true) => {
                        trace!("update_queries pg-eval matched fingerprint {fingerprint}");
                        hits.push(fingerprint);
                    }
                    Ok(false) => {}
                    Err(e) => {
                        error!(
                            "predicate eval error for fingerprint {fingerprint}: {}",
                            error_chain_format(&e),
                        );
                        return Err(CacheError::PgError(e).into());
                    }
                }
            }
            if mode == PgEvalMode::FirstMatch && !hits.is_empty() {
                break;
            }
        }
        Ok(hits)
    }

    /// Build `SELECT EXISTS (<cached query with the CDC row's values
    /// substituted>)` — the membership predicate for one cached query,
    /// evaluated read-only on the pool against the pre-transaction snapshot.
    pub(super) fn cache_predicate_exists_sql(
        &self,
        resolved: &ResolvedQueryExpr,
        table_metadata: &TableMetadata,
        row_data: &[Option<String>],
    ) -> CacheResult<String> {
        let resolved_select = resolved.as_select().ok_or(CacheError::InvalidQuery)?;
        let value_select = resolved_select_node_table_replace_with_values(
            resolved_select,
            table_metadata,
            row_data,
        )
        .map_err(|e| e.context_transform(CacheError::from))?;
        let mut select = String::with_capacity(SQL_BUFFER_CAPACITY);
        Deparse::deparse(&value_select, &mut select);
        Ok(format!("SELECT EXISTS ({select})"))
    }

    /// Build an unconditional UPSERT for the row — `INSERT ... ON CONFLICT DO UPDATE`
    /// with no WHERE predicate. Used by the LocalEval fast path once the Rust
    /// evaluator has already decided the row belongs in cache.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) fn cache_upsert_unconditional_sql(
        &self,
        table_metadata: &TableMetadata,
        row_data: &[Option<String>],
    ) -> String {
        let mut column_names = Vec::with_capacity(table_metadata.columns.len());
        let mut values = Vec::with_capacity(table_metadata.columns.len());

        for column_meta in &table_metadata.columns {
            let position = column_meta.index();
            if let Some(row_value) = row_data.get(position) {
                let value = row_value
                    .as_deref()
                    .map_or_else(|| "NULL".to_owned(), escape::escape_literal);
                column_names.push(column_meta.name.as_str());
                values.push(value);
            }
        }

        let schema = &table_metadata.schema;
        let table = &table_metadata.name;

        let mut sql = String::with_capacity(SQL_BUFFER_CAPACITY);
        let _ = write!(sql, "INSERT INTO {schema}.{table} (");
        for (i, col) in column_names.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(col);
        }
        sql.push_str(") VALUES (");
        for (i, val) in values.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(val);
        }
        sql.push_str(") ON CONFLICT (");
        for (i, pk) in table_metadata.primary_key_columns.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(pk);
        }
        sql.push(')');
        cdc_on_conflict_tail_append(&mut sql, &column_names, &table_metadata.primary_key_columns);

        sql
    }

    #[instrument(skip_all)]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) fn cache_delete_sql(
        &self,
        table_metadata: &TableMetadata,
        row_data: &[Option<String>],
    ) -> CacheResult<String> {
        let mut sql = String::with_capacity(SQL_BUFFER_CAPACITY);
        let _ = write!(
            sql,
            "DELETE FROM {}.{} WHERE ",
            table_metadata.schema, table_metadata.name
        );

        let mut has_pk = false;
        for pk_column in &table_metadata.primary_key_columns {
            if let Some(column_meta) = table_metadata.columns.get(pk_column.as_str()) {
                let position = column_meta.index();
                if let Some(row_value) = row_data.get(position) {
                    let value = row_value
                        .as_deref()
                        .map_or_else(|| "NULL".to_owned(), escape::escape_literal);
                    if has_pk {
                        sql.push_str(" AND ");
                    }
                    let _ = write!(sql, "{pk_column} = {value}");
                    has_pk = true;
                }
            }
        }

        if !has_pk {
            error!("Cannot build DELETE WHERE clause: no primary key values found");
            return Err(CacheError::NoPrimaryKey.into());
        }

        Ok(sql)
    }
}

impl WriterCore {
    /// Invalidate all cached queries that reference a table.
    pub(super) async fn cache_table_invalidate(&mut self, relation_oid: u32) -> CacheResult<()> {
        let fingerprints: Vec<u64> = self
            .cache
            .cached_queries
            .iter()
            .filter(|q| q.relation_oids.contains(&relation_oid))
            .map(|q| q.fingerprint)
            .collect();

        for fp in fingerprints {
            self.cache_query_evict(fp).await?;
        }
        Ok(())
    }

    /// Fully evict a cached query: remove from all data structures and purge rows.
    /// Used by the eviction loop and schema-change (table) invalidation.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) async fn cache_query_evict(&mut self, fingerprint: u64) -> CacheResult<()> {
        let Some(query) = self.cache.cached_queries.remove1(&fingerprint) else {
            trace!(fingerprint, "cache_query_evict: not found, skipping");
            return Ok(());
        };

        debug!(
            fingerprint,
            generation = query.generation,
            relation_oids = ?query.relation_oids,
            "cache_query_evict entry"
        );
        if let Some(mut m) = self.state_view.metrics.get_mut(&fingerprint) {
            m.eviction_count += 1;
            m.cached_since_ns = None;
        }
        // Removal paths defer publication sync to the end-of-command drain
        // (publication_dirty_drain) — stale subscriptions to the dropped
        // oid are filtered out by the writer ignoring its CDC events.
        self.active_relations_release(&query.relation_oids);

        let prev_generation_threshold = self.cache.generation_purge_threshold();

        // Remove generation from tracking
        self.cache.generations.remove(&query.generation);

        // Drop the MV table (if any) before removing the state_view entry so we
        // can read the mv_state. Errors are logged but don't abort the eviction.
        // Unlike the other db_cache maintenance, this is NOT frame-deferred:
        // MV tables (pgcache_mv schema) are never written by the frame, which
        // only touches source cache tables — so a DROP here can't deadlock on
        // the frame's locks even when reached in-frame (e.g. via truncate
        // invalidation).
        let mv_state = self
            .state_view
            .cached_queries
            .get(&fingerprint)
            .map(|v| v.mv.state);
        if let Some(mv_state) = mv_state
            && let Err(e) = self.mv_drop(fingerprint, mv_state).await
        {
            error!(
                "mv drop on eviction failed for {fingerprint}: {}",
                error_chain_format(e.current_context()),
            );
        }

        // Remove from state view
        self.state_view.cached_queries.remove(&fingerprint);

        self.cache
            .update_queries_remove_fingerprint(fingerprint, &query.relation_oids);

        // Purge generations based on new threshold
        let new_threshold = self.cache.generation_purge_threshold();
        if new_threshold > prev_generation_threshold {
            let cache_size = self.cache.dynamic.load().cache_size;
            let mut current_size = self.cache_size_load().await?;

            if cache_size.is_some_and(|s| current_size > s) {
                self.generation_purge(new_threshold).await?;
                current_size = self.cache_size_load().await?;
            }

            self.cache.current_size = current_size as usize;
        }

        Ok(())
    }
}

/// Append the tail of an upsert SQL: either ` DO UPDATE SET <non-pk cols>` or
/// ` DO NOTHING` if the table has no non-PK columns. PG rejects `DO UPDATE SET`
/// with an empty SET list, so PK-only tables must use `DO NOTHING`.
///
/// Assumes the caller has already emitted `INSERT INTO ... ON CONFLICT (<pk>)`.
fn cdc_on_conflict_tail_append(sql: &mut String, column_names: &[&str], pkey_columns: &[String]) {
    let is_pk = |name: &str| pkey_columns.iter().any(|pk| pk.as_str() == name);
    let has_non_pk = column_names.iter().any(|c| !is_pk(c));
    if !has_non_pk {
        sql.push_str(" DO NOTHING");
        return;
    }
    sql.push_str(" DO UPDATE SET ");
    let mut first = true;
    for col in column_names {
        if is_pk(col) {
            continue;
        }
        if !first {
            sql.push_str(", ");
        }
        let _ = write!(sql, "{col} = EXCLUDED.{col}");
        first = false;
    }
}

/// Evaluate a LocalEval update query's WHERE against the CDC row.
///
/// Must only be called when `update_query.eval_strategy == LocalEval` — the
/// classifier has already ensured the query is single-table, FromClause, with
/// no GROUP BY / HAVING and a supported WHERE shape. A WHERE of `None` means
/// the query loads every row, so the match is unconditional.
fn update_query_matches_locally(
    update_query: &UpdateQuery,
    table_metadata: &TableMetadata,
    row_data: &[Option<String>],
) -> bool {
    let Some(select) = update_query.resolved.as_select() else {
        return false;
    };
    match &select.where_clause {
        None => true,
        Some(expr) => where_expr_evaluate(expr, row_data, table_metadata.name.as_str()),
    }
}

/// Check if a row's membership in a joined result set is unchanged.
/// Returns true when the primary key didn't change and all join columns
/// are primary key columns — meaning the row's join relationships are stable.
fn join_membership_unchanged(
    update_query: &UpdateQuery,
    table_metadata: &TableMetadata,
    key_data: Option<&[Option<String>]>,
) -> bool {
    let Some(key) = key_data else {
        return false;
    };

    if !key.is_empty() {
        return false;
    }

    let join_columns: Vec<&str> = update_query
        .constraints
        .table_join_columns(&table_metadata.name)
        .collect();

    !join_columns.is_empty()
        && join_columns.iter().all(|col| {
            table_metadata
                .primary_key_columns
                .iter()
                .any(|pk| pk == col)
        })
}
