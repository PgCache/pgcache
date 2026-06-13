use crate::query::Fingerprint;
use std::collections::{HashMap, HashSet};
use std::fmt::Write;
use std::num::NonZeroUsize;
use std::time::Instant;

use ecow::EcoString;
use futures_util::future;
use lru::LruCache;
use postgres_protocol::escape;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, SimpleQueryMessage, SimpleQueryRow, Statement};
use tracing::{debug, error, info, instrument, trace, warn};

use crate::catalog::TableMetadata;
use crate::pg::protocol::ByteString;

use crate::query::ast::{BinaryOp, Deparse};
use crate::query::cast::cast_target_coerce_text;
use crate::query::constraints::{QueryConstraints, TableConstraint};
use crate::query::evaluate::{literal_compare, where_value_compare_string};
use crate::query::resolved::ResolvedQueryExpr;
use crate::query::transform::{
    BATCH_IDX_COLUMN, resolved_select_node_table_replace_with_unnest,
    resolved_select_node_table_replace_with_values,
    resolved_select_node_table_replace_with_values_batch,
};

use crate::settings::{CachePolicy, Settings};

use crate::query::evaluate::where_expr_evaluate;

use super::super::memo::SlotKey;
use super::super::messages::{CdcCommand, QueryCommand};
use super::super::types::{
    CachedQueryState, SubqueryKind, UpdateEvalStrategy, UpdateQuery, UpdateQuerySource,
};
use super::super::{CacheError, CacheResult, MapIntoReport, ReportExt};
use super::core::{
    FRAME_BUF_CAPACITY, FRAME_ROWS_CAPACITY, FrameRowEvent, FrameState, ToastOverlayEntry,
    WriterCore,
};
use super::deadlock::{SQLSTATE_DEADLOCK, cache_error_sqlstate};
use super::staging::pk_body_render;
use crate::pg;
use crate::result::error_chain_format;

/// Default capacity for dynamically built SQL strings.
const SQL_BUFFER_CAPACITY: usize = 1024;

/// One queued toast repair awaiting the batched pre-batch-image lookup
/// (PGC-264).
struct PendingRepairSlot {
    event_idx: usize,
    /// Rendered source PK, for overlay bookkeeping.
    overlay_key: EcoString,
    /// Raw source-PK column values, for matching lookup result rows.
    raw_pk: Vec<ByteString>,
}

/// Pass-1 outcome for one toasted update (PGC-264).
enum ToastResolution {
    /// Overlay hit: the toasted positions' values to substitute.
    Repaired(Vec<(usize, Option<ByteString>)>),
    /// No in-batch state: queue for the batched lookup, keyed by these raw
    /// source-PK values.
    Queue(Vec<ByteString>),
    Fallback,
}

/// Max membership predicates combined into one `pg_eval_matches` query, bounding
/// the combined query's parse/plan cost for relations with many PgEval queries.
const PG_EVAL_CHUNK: usize = 32;

/// Max CDC rows per batched membership statement (PGC-241). With inlined
/// VALUES the per-statement SQL is ~`PG_EVAL_CHUNK × PG_EVAL_ROW_CHUNK ×
/// row_width`, so this bounds statement size the way `FRAME_BUF_CAPACITY`
/// bounds the frame buffer; round-trips collapse `K → ⌈K/64⌉` per query chunk.
const PG_EVAL_ROW_CHUNK: usize = 64;

/// Prepared-eval statement cache bound (PGC-241 stage 4). Per-literal
/// registration can produce thousands of live fingerprints; the LRU keeps the
/// hot working set prepared and ages the rest out (which also closes them
/// server-side). If `cdc_prepared_misses` tracks executions, the working set
/// exceeds this and prepare-per-use is thrashing — raise it or gate on
/// second use (until shape-parameterized update queries, PGC-257, collapse
/// the cardinality).
// Compile-time evaluated: cannot panic at runtime.
const PREPARED_EVAL_CACHE_CAPACITY: NonZeroUsize = match NonZeroUsize::new(512) {
    Some(capacity) => capacity,
    None => unreachable!(),
};

/// Shared array params for a prepared eval statement over one row chunk:
/// `$1` = row ordinals, `$2..` = one `text[]` per column in
/// `table_metadata.columns` order. The unnest transform's parameter numbering
/// and the prepared row-change SQL builder both follow the same column order —
/// this is the single place the binding contract is produced.
fn chunk_arrays_build<'a>(
    table_metadata: &TableMetadata,
    row_chunk: &[(usize, &'a [Option<ByteString>])],
) -> (Vec<i32>, Vec<Vec<Option<&'a str>>>) {
    // Chunk length is bounded by PG_EVAL_ROW_CHUNK (64): never wraps.
    #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
    let ordinals: Vec<i32> = (0..row_chunk.len() as i32).collect();
    let column_arrays = table_metadata
        .columns
        .iter()
        .map(|column_meta| {
            row_chunk
                .iter()
                .map(|(_, row)| row.get(column_meta.index()).and_then(|v| v.as_deref()))
                .collect()
        })
        .collect();
    (ordinals, column_arrays)
}

/// Append the row-change SELECT list — `SELECT v.<idx>, o.X IS DISTINCT FROM
/// v.X AS X, …` — shared by the prepared and inline row-change builders so the
/// changed-column contract can't drift between them.
fn row_change_select_into(buf: &mut String, table_metadata: &TableMetadata) {
    buf.push_str("SELECT v.");
    buf.push_str(BATCH_IDX_COLUMN);
    for column_meta in &table_metadata.columns {
        let _ = write!(
            buf,
            ", o.{name} IS DISTINCT FROM v.{name} AS {name}",
            name = column_meta.name
        );
    }
}

/// Append the row-change PK join — ` JOIN <schema>.<table> o ON o.pk = v.pk…`
/// — shared by the prepared and inline row-change builders.
fn row_change_join_on_into(buf: &mut String, table_metadata: &TableMetadata) {
    let _ = write!(
        buf,
        " JOIN {}.{} o ON ",
        table_metadata.schema, table_metadata.name
    );
    for (i, pk_column) in table_metadata.primary_key_columns.iter().enumerate() {
        if i > 0 {
            buf.push_str(" AND ");
        }
        let _ = write!(buf, "o.{pk_column} = v.{pk_column}");
    }
}

/// Statement-cache key for prepared membership eval. Deliberately a named
/// type: when update queries become shape-parameterized (PGC-257) this swaps
/// to `(relation, shape)` here, and the cache machinery carries over.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct PreparedEvalKey {
    relation_oid: u32,
    fingerprint: Fingerprint,
}

/// Per-relation membership-eval rows of one segment: `(event index, row)`.
type SegmentRows<'a> = HashMap<u32, Vec<(usize, &'a [Option<ByteString>])>>;

/// Max complete source frames accumulated per batch (PGC-242). The event cap
/// (`FRAME_ROWS_CAPACITY`) usually triggers first for fat frames; this bounds
/// the memo-bracket window and the 40P01 recovery blast radius for streams of
/// tiny frames.
const BATCH_FRAMES_MAX: usize = 256;

/// Batched PgEval membership + row-change results for one segment of frame
/// row events (PGC-241). Built by `segment_membership_eval` /
/// `segment_row_changes_eval`, consumed per event by the decide pass.
#[derive(Default)]
struct SegmentMembership {
    relations: HashMap<u32, RelationBatch>,
}

/// One relation's batched-eval results within a segment. Every event belongs
/// to exactly one relation, so the coverage invariants (which fingerprints a
/// covered event may consult) live inside one entry instead of across
/// parallel collections.
#[derive(Default)]
struct RelationBatch {
    /// Batchable Fresh-MV fingerprints, evaluated for every `covered` event
    /// (Fresh queries are always fully evaluated so every match dirty-marks —
    /// same as the per-row path).
    fresh_fps: HashSet<Fingerprint>,
    /// Batchable non-Fresh fingerprints, evaluated only for `rest_covered`
    /// events (rows with no LocalEval match and no fresh hit) — mirroring the
    /// per-row path's `if !matched` short-circuit, which does zero PgEval
    /// round-trips for locally-matched rows.
    rest_fps: HashSet<Fingerprint>,
    /// Event indexes whose row was in the fresh membership batch (rows of
    /// unexpected arity stay out and fall back to per-row eval).
    covered: HashSet<usize>,
    /// Event indexes whose row was in the rest membership batch.
    rest_covered: HashSet<usize>,
    /// `(event index, fingerprint)` membership hits.
    hits: HashSet<(usize, Fingerprint)>,
    /// Update events whose row-change SELECT was batched. Covered-but-absent
    /// from `row_changes` ⇒ the row isn't in the cache table (the per-row
    /// `None` case).
    row_change_covered: HashSet<usize>,
    /// Per covered update event: column → changed (`IS DISTINCT FROM`).
    row_changes: HashMap<usize, HashMap<EcoString, bool>>,
}

impl SegmentMembership {
    /// The matrix view for one event, or `None` if neither the membership nor
    /// the row-change batch covered it.
    fn view(&self, relation_oid: u32, event_idx: usize) -> Option<BatchEvalView<'_>> {
        let batch = self.relations.get(&relation_oid)?;
        let fresh_fps = batch
            .covered
            .contains(&event_idx)
            .then_some(&batch.fresh_fps);
        let rest_fps = batch
            .rest_covered
            .contains(&event_idx)
            .then_some(&batch.rest_fps);
        let row_change = batch
            .row_change_covered
            .contains(&event_idx)
            .then(|| batch.row_changes.get(&event_idx));
        if fresh_fps.is_none() && rest_fps.is_none() && row_change.is_none() {
            return None;
        }
        Some(BatchEvalView {
            fresh_fps,
            rest_fps,
            hits: &batch.hits,
            event_idx,
            row_change,
        })
    }
}

/// One event's window into a [`SegmentMembership`] matrix.
pub(super) struct BatchEvalView<'a> {
    fresh_fps: Option<&'a HashSet<Fingerprint>>,
    rest_fps: Option<&'a HashSet<Fingerprint>>,
    hits: &'a HashSet<(usize, Fingerprint)>,
    event_idx: usize,
    /// Outer `None` = row-change not batched for this event (fall back to the
    /// per-row SELECT); `Some(inner)` mirrors `query_row_changes`' return.
    row_change: Option<Option<&'a HashMap<EcoString, bool>>>,
}

impl BatchEvalView<'_> {
    /// Whether `fingerprint` was batch-evaluated (consult `hit` instead of a
    /// per-row round-trip). A rest query outside both covered sets falls back
    /// to the per-row path, whose `if !matched` guard skips it exactly as the
    /// pre-batch flow did.
    fn covers(&self, fingerprint: Fingerprint) -> bool {
        self.fresh_fps.is_some_and(|fps| fps.contains(&fingerprint))
            || self.rest_fps.is_some_and(|fps| fps.contains(&fingerprint))
    }

    /// Whether this event's row matched `fingerprint`'s predicate.
    fn hit(&self, fingerprint: Fingerprint) -> bool {
        self.hits.contains(&(self.event_idx, fingerprint))
    }
}

/// Test-only deterministic fault injection (PGC-147). Compiled out entirely
/// unless built with `--features fault-injection`; the writer-side CDC `40P01`
/// is a timing race that cannot be provoked probabilistically, so the recovery
/// path is exercised by forcing it here.
#[cfg(feature = "fault-injection")]
mod fault {
    use std::sync::atomic::{AtomicBool, Ordering};

    static CDC_DEADLOCK_ONCE: AtomicBool = AtomicBool::new(false);

    /// Minimum batch size before the queue-empty flush trigger fires
    /// (`PGCACHE_FAULT_CDC_HOLD_FLUSH_FRAMES`), read once.
    pub(super) fn hold_flush_frames() -> Option<usize> {
        use std::sync::OnceLock;
        static HOLD: OnceLock<Option<usize>> = OnceLock::new();
        *HOLD.get_or_init(|| {
            std::env::var("PGCACHE_FAULT_CDC_HOLD_FLUSH_FRAMES")
                .ok()
                .and_then(|s| s.parse().ok())
                .filter(|n| *n > 0)
        })
    }

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

/// Test-only flush hold (PGC-242): with `PGCACHE_FAULT_CDC_HOLD_FLUSH_FRAMES=N`
/// the queue-empty trigger is suppressed until N frames have accumulated, so
/// tests can provoke deterministic multi-frame batches without real queue
/// pressure. Size caps and `Recovering` still force a flush.
#[cfg(feature = "fault-injection")]
fn fault_cdc_hold_flush(batch_frames: usize) -> bool {
    fault::hold_flush_frames().is_some_and(|n| batch_frames < n)
}
#[cfg(not(feature = "fault-injection"))]
fn fault_cdc_hold_flush(_batch_frames: usize) -> bool {
    false
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
    /// Dedicated read connection for membership evaluation (`SELECT EXISTS`).
    /// Read-only, autocommit — sees the pre-source-transaction snapshot, never
    /// the in-flight frame's uncommitted writes. One connection suffices:
    /// predicates are batched into combined `SELECT`s and the writer evaluates
    /// one CDC row at a time.
    pub(super) cache_eval_conn: Client,
    /// Single dedicated write connection. All cache mutations for a source
    /// transaction are applied here inside one `BEGIN…COMMIT` spanning every
    /// message of that transaction, so cache readers observe the source
    /// transaction atomically. Distinct from `cache_eval_conn` and from
    /// `WriterCore.db_cache`.
    pub(super) cdc_write_conn: Client,
    /// Highest LSN whose effects (cache mutations and invalidations) have been
    /// applied by this writer. Advances on `CommitMark` and `KeepAliveMark`,
    /// guaranteed transaction-aligned by mpsc ordering.
    pub(super) last_applied_lsn: u64,
    /// Reused scratch buffer for the combined predicate `SELECT` built per CDC
    /// row in `pg_eval_matches`/`pg_eval_any`. Lives for the writer's lifetime
    /// so steady-state membership evaluation allocates no SQL string.
    pg_eval_buf: String,
    /// Prepared membership statements per (relation, query), LRU-bounded
    /// (PGC-241 stage 4). Stale entries — evicted queries — age out on their
    /// own (they are never selected for execution again), and dropping a
    /// `Statement` closes it server-side, so the bound also caps the cache-PG
    /// backend's plancache memory. Cleared per relation on `TableRegister`
    /// (schema change); an execution error drops the entry and the call falls
    /// back to the inlined-VALUES path (self-healing).
    prepared_membership: LruCache<PreparedEvalKey, Statement>,
    /// Prepared row-change statements per relation (same lifecycle). These run
    /// on `WriterCore.db_cache`, matching the per-row `query_row_changes`.
    prepared_row_change: LruCache<u32, Statement>,
}

/// Check that every WHERE constraint for `table_metadata` matches `row_data`.
/// Returns true when there are no constraints for this table (full-scan
/// cached query), or when every constraint evaluates to true on the row.
///
/// CastComparison constraints coerce the row's wire-text via
/// `cast_target_coerce_text` and compare via `literal_compare`; a coercion
/// failure is treated as non-match (the row would have errored at origin).
pub(super) fn row_constraints_match(
    constraints: &QueryConstraints,
    table_metadata: &TableMetadata,
    row_data: &[Option<ByteString>],
) -> bool {
    let Some(constraints) = constraints
        .table_constraints
        .get(table_metadata.name.as_str())
    else {
        return true;
    };

    for constraint in constraints {
        let column_name = match constraint {
            TableConstraint::Comparison(col, ..)
            | TableConstraint::AnyOf(col, ..)
            | TableConstraint::CastComparison(col, ..) => col.as_str(),
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
                        TableConstraint::CastComparison(_, cast, op, val) => {
                            match cast_target_coerce_text(cast, row_str) {
                                Some(coerced) => literal_compare(&coerced, *op, val),
                                // Coercion failure (e.g. `'abc'::int4`): the
                                // row would error at origin and never match
                                // — safe to treat as non-matching here.
                                None => false,
                            }
                        }
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

impl WriterCdc {
    pub async fn new(settings: &Settings) -> CacheResult<Self> {
        let cache_eval_conn = pg::connect(&settings.cache, "cache eval")
            .await
            .map_into_report::<CacheError>()?;

        let cdc_write_conn = pg::connect(&settings.cache, "cdc write")
            .await
            .map_into_report::<CacheError>()?;

        #[cfg(feature = "fault-injection")]
        fault::init();

        Ok(Self {
            cache_eval_conn,
            cdc_write_conn,
            last_applied_lsn: 0,
            pg_eval_buf: String::with_capacity(SQL_BUFFER_CAPACITY),
            prepared_membership: LruCache::new(PREPARED_EVAL_CACHE_CAPACITY),
            prepared_row_change: LruCache::new(PREPARED_EVAL_CACHE_CAPACITY),
        })
    }

    /// Finish a buffered statement: append the separator, then chunk-flush if
    /// `frame_buf` has grown past `FRAME_BUF_CAPACITY` (bounds memory for large
    /// source transactions). The chunk goes out inside the open frame txn
    /// (`BEGIN` was buffered as the first write), which stays open server-side
    /// until the `COMMIT` at `frame_commit`. The flag is set before the send so
    /// `40P01` recovery knows a `BEGIN` reached the server.
    async fn frame_write_finish(&mut self, core: &mut WriterCore) -> CacheResult<()> {
        core.frame_buf.push_str("; ");
        if core.frame_buf.len() >= FRAME_BUF_CAPACITY {
            core.frame_chunk_flushed = true;
            self.cdc_write_conn
                .batch_execute(&core.frame_buf)
                .await
                .map_into_report::<CacheError>()?;
            core.frame_buf.clear();
        }
        Ok(())
    }

    /// Flush the frame's buffered writes as a single `BEGIN; …; COMMIT`
    /// round-trip (PGC-228). A write-less frame stayed `Active` and flushes
    /// nothing (its invalidations flush separately at `CommitMark`). If chunks
    /// were already flushed, `frame_buf` holds only the tail + `COMMIT`.
    async fn frame_commit(&mut self, core: &mut WriterCore) -> CacheResult<()> {
        if core.frame_state == FrameState::TxnOpen {
            core.frame_buf.push_str("COMMIT");
            self.cdc_write_conn
                .batch_execute(&core.frame_buf)
                .await
                .map_into_report::<CacheError>()?;
            core.frame_buf.clear();
            core.frame_buf_relations.clear();
            core.frame_state = FrameState::Idle;
        }
        Ok(())
    }

    /// `40P01` aborted the frame: discard buffered writes, roll back the
    /// server-side txn if a chunk had already opened one, and enter `Recovering`
    /// (relation-level recovery happens at `CommitMark`, PGC-147).
    async fn frame_recover_enter(&mut self, core: &mut WriterCore) -> CacheResult<()> {
        // A fully-buffered frame never sent anything (a failed `BEGIN; …; COMMIT`
        // batch auto-rolls-back), so only an already-flushed chunk leaves a live
        // aborted txn block that needs an explicit ROLLBACK.
        if core.frame_chunk_flushed {
            self.cdc_write_conn
                .batch_execute("ROLLBACK")
                .await
                .map_into_report::<CacheError>()?;
        }
        core.frame_buf.clear();
        core.frame_buf_relations.clear();
        // Unreplayed row events are dropped — relation-level recovery
        // (evict + truncate) supersedes whatever they would have applied.
        core.frame_rows.clear();
        // Rolled-back deletes/truncates must not affect population merges — the
        // rows may still exist (PGC-250). The recovery's own truncate re-adds the
        // affected relations in `frame_recover`.
        core.frame_deleted_keys.clear();
        core.frame_truncated_relations.clear();
        core.batch_deleted_pks.clear();
        core.toast_overlay_reset();
        core.batch_toast_guard_oids.clear();
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
        let fps: Vec<Fingerprint> = core.frame_invalidations.iter().copied().collect();
        let count = fps.len() as u64;
        for fp in fps {
            self.cache_query_cdc_invalidate(core, fp)
                .await
                .attach_loc("flushing deferred invalidation")?;
        }
        crate::metrics::handles().cdc.invalidations.increment(count);
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
        // Like an explicit TRUNCATE, abort in-flight populations over these
        // relations whose snapshot predates the recovery (PGC-250); stamped with
        // the commit LSN at CommitMark.
        core.batch_truncated_relations.extend(oids.iter().copied());
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

    /// Replay the frame's buffered row events in arrival order (PGC-241:
    /// collect at arrival, evaluate + emit at the flush boundary). Arrival
    /// order is what makes the deferral pure: same-key sequences (an INSERT
    /// then DELETE of one PK) and TRUNCATE-vs-row interleavings emit exactly
    /// as per-arrival handling did.
    ///
    /// Events run in segments split at Truncate boundaries (a truncate evicts
    /// queries over its relations, changing the update-query set). Each
    /// segment's PgEval membership is batch-evaluated up front — one statement
    /// per relation per row/query chunk instead of per row — and the ordered
    /// decide/emit pass consumes the precomputed matrix. Handler errors route
    /// through `frame_dml_result` (`40P01` → `Recovering`); once `Recovering`,
    /// the remaining events are dropped, matching the per-arrival path where
    /// post-deadlock commands skip handling.
    async fn frame_rows_replay(&mut self, core: &mut WriterCore) -> CacheResult<()> {
        let mut events = std::mem::take(&mut core.frame_rows);
        // Resolve unchanged-toast images first (PGC-264): segment eval and
        // the decide pass below must only ever see complete row images.
        Self::toast_repair_events(core, &mut events).await;
        // Segments end at (and include) each Truncate: a truncate evicts
        // queries over its relations, changing the update-query set the batch
        // eval is built from, so eval never spans one. `base` keeps the global
        // event index — the identity the eval matrix is keyed by.
        let mut base = 0;
        'segments: for segment in
            events.split_inclusive(|e| matches!(e, FrameRowEvent::Truncate { .. }))
        {
            if core.frame_state == FrameState::Recovering {
                break;
            }
            let (rows, trailing_truncate) = match segment.split_last() {
                Some((FrameRowEvent::Truncate { relation_oids }, rows)) => {
                    (rows, Some(relation_oids))
                }
                _ => (segment, None),
            };

            // Batched membership + row-change for the segment's rows
            // (PGC-241), then the ordered decide/emit pass over the same
            // events.
            let membership = if rows.is_empty() {
                SegmentMembership::default()
            } else {
                match self.segment_eval(core, rows, base).await {
                    Ok(m) => m,
                    Err(e) => {
                        self.frame_dml_result(core, Err(e))
                            .await
                            .attach_loc("cdc segment batch eval")?;
                        // Swallowed 40P01 → Recovering; the loop exits above.
                        base += segment.len();
                        continue;
                    }
                }
            };

            for (offset, event) in rows.iter().enumerate() {
                if core.frame_state == FrameState::Recovering {
                    break 'segments;
                }
                let i = base + offset;
                let (r, loc) = match event {
                    FrameRowEvent::Insert {
                        relation_oid,
                        row_data,
                    } => (
                        self.handle_insert(
                            core,
                            *relation_oid,
                            row_data,
                            membership.view(*relation_oid, i),
                        )
                        .await,
                        "cdc replay insert",
                    ),
                    FrameRowEvent::Update {
                        relation_oid,
                        key_data,
                        new_row_data,
                    } => (
                        self.handle_update(
                            core,
                            *relation_oid,
                            key_data,
                            new_row_data,
                            membership.view(*relation_oid, i),
                        )
                        .await,
                        "cdc replay update",
                    ),
                    FrameRowEvent::UpdateToastFallback {
                        relation_oid,
                        key_data,
                        new_row_data,
                        toasted_columns,
                    } => (
                        self.handle_update_toast_fallback(
                            core,
                            *relation_oid,
                            key_data,
                            new_row_data,
                            toasted_columns,
                        )
                        .await,
                        "cdc replay update toast fallback",
                    ),
                    // Unreachable by construction (`toast_repair_events`
                    // resolved every one before the segment loop); degrade to
                    // the conservative fallback rather than panicking.
                    FrameRowEvent::UpdateToasted {
                        relation_oid,
                        key_data,
                        new_row_data,
                        toasted,
                    } => {
                        debug_assert!(false, "UpdateToasted survived the repair pre-pass");
                        error!(relation_oid, "unrepaired toasted update at decide time");
                        let toasted_columns: Vec<EcoString> = core
                            .cache
                            .tables
                            .get1(relation_oid)
                            .map(|t| {
                                t.columns
                                    .iter()
                                    .filter(|c| toasted.contains(&c.index()))
                                    .map(|c| c.name.clone())
                                    .collect()
                            })
                            .unwrap_or_default();
                        (
                            self.handle_update_toast_fallback(
                                core,
                                *relation_oid,
                                key_data,
                                new_row_data,
                                &toasted_columns,
                            )
                            .await,
                            "cdc replay unrepaired toasted update",
                        )
                    }
                    FrameRowEvent::Delete {
                        relation_oid,
                        row_data,
                    } => (
                        self.handle_delete(core, *relation_oid, row_data).await,
                        "cdc replay delete",
                    ),
                    // Unreachable by construction (`split_last` separated the
                    // trailing Truncate); a no-op keeps this panic-free.
                    FrameRowEvent::Truncate { .. } => (Ok(()), "cdc replay truncate"),
                    // Frame commit boundary (PGC-242): stamp the bookkeeping
                    // this frame's replay produced with its commit LSN.
                    FrameRowEvent::Boundary { commit_lsn } => {
                        let frame_deletes = std::mem::take(&mut core.frame_deleted_keys);
                        for (rel, key) in frame_deletes {
                            core.population_deleted_keys.record(rel, key, *commit_lsn);
                        }
                        let frame_truncated = std::mem::take(&mut core.frame_truncated_relations);
                        for rel in frame_truncated {
                            core.population_deleted_keys.abort_below(rel, *commit_lsn);
                        }
                        (Ok(()), "cdc replay boundary")
                    }
                };
                self.frame_dml_result(core, r).await.attach_loc(loc)?;
            }

            if let Some(relation_oids) = trailing_truncate
                && core.frame_state != FrameState::Recovering
            {
                let r = self.handle_truncate(core, relation_oids).await;
                self.frame_dml_result(core, r)
                    .await
                    .attach_loc("cdc frame replay truncate")?;
            }
            base += segment.len();
        }
        // Hand the cleared buffer back so its capacity is reused across
        // frames, recycling each event's row Vecs into the pool on the way.
        let mut events = events;
        for event in events.drain(..) {
            core.row_vecs_recycle(event);
        }
        core.frame_rows = events;
        Ok(())
    }

    /// Run both batch passes for a segment: PgEval membership, then row-change
    /// detection, into one [`SegmentMembership`] matrix (PGC-241).
    async fn segment_eval(
        &mut self,
        core: &WriterCore,
        events: &[FrameRowEvent],
        base_idx: usize,
    ) -> CacheResult<SegmentMembership> {
        let mut membership = self.segment_membership_eval(core, events, base_idx).await?;
        self.segment_row_changes_eval(core, events, base_idx, &mut membership)
            .await?;
        Ok(membership)
    }

    /// Batch-evaluate PgEval membership for one segment of row events: per
    /// relation, every batchable query (`UpdateQuery::pg_batchable`) is
    /// evaluated against all the segment's rows in `UNION ALL`-combined
    /// multi-row VALUES statements — `⌈rows/PG_EVAL_ROW_CHUNK⌉ ×
    /// ⌈queries/PG_EVAL_CHUNK⌉` round-trips instead of one per row (PGC-241).
    ///
    /// The matrix is built unfiltered (no `frame_invalidations` / Fresh-MV
    /// partition); the decide pass applies those — they evolve as earlier
    /// events in the segment are decided. Reads run on `cache_eval_conn`'s
    /// pre-transaction snapshot, identical to the per-row path.
    async fn segment_membership_eval(
        &mut self,
        core: &WriterCore,
        events: &[FrameRowEvent],
        base_idx: usize,
    ) -> CacheResult<SegmentMembership> {
        let mut membership = SegmentMembership::default();

        // Bucket membership-eval rows per relation: inserts and updates (new
        // row image); deletes carry no membership question.
        let mut rows_by_relation: SegmentRows<'_> = HashMap::new();
        for (offset, event) in events.iter().enumerate() {
            let (relation_oid, row): (u32, &[Option<ByteString>]) = match event {
                FrameRowEvent::Insert {
                    relation_oid,
                    row_data,
                } => (*relation_oid, row_data),
                FrameRowEvent::Update {
                    relation_oid,
                    new_row_data,
                    ..
                } => (*relation_oid, new_row_data),
                // UpdateToastFallback is excluded by design: its row image is
                // incomplete, so the decide pass invalidates instead of
                // evaluating membership from it. UpdateToasted no longer
                // exists at eval time (resolved by the repair pre-pass)
                // (PGC-264).
                FrameRowEvent::UpdateToasted { .. }
                | FrameRowEvent::UpdateToastFallback { .. }
                | FrameRowEvent::Delete { .. }
                | FrameRowEvent::Truncate { .. }
                | FrameRowEvent::Boundary { .. } => continue,
            };
            rows_by_relation
                .entry(relation_oid)
                .or_default()
                .push((base_idx + offset, row));
        }

        for (relation_oid, rows) in rows_by_relation {
            let Some(update_queries) = core.cache.update_queries.get(&relation_oid) else {
                continue;
            };
            let Some(table_metadata) = core.cache.tables.get1(&relation_oid) else {
                continue;
            };
            let batchable: Vec<&UpdateQuery> = update_queries
                .iter_complexity_ordered()
                .filter(|q| q.eval_strategy == UpdateEvalStrategy::PgEval && q.pg_batchable)
                .collect();
            if batchable.is_empty() {
                continue;
            }

            // A multi-row VALUES needs uniform arity; rows narrower than the
            // relation (e.g. truncated tuples) fall back to per-row eval by
            // staying out of `covered`.
            let full_width = table_metadata.columns.len();
            let batch_rows: Vec<(usize, &[Option<ByteString>])> = rows
                .into_iter()
                .filter(|(_, row)| row.len() == full_width)
                .collect();
            if batch_rows.is_empty() {
                continue;
            }

            // Mirror the per-row fresh/rest split: dirtyable-MV queries
            // (Fresh or Building) are always fully evaluated (every match
            // must dirty-mark)…
            let (fresh, rest): (Vec<&UpdateQuery>, Vec<&UpdateQuery>) = batchable
                .into_iter()
                .partition(|q| core.mv_dirty_eval_required(q.fingerprint));

            let batch = membership.relations.entry(relation_oid).or_default();
            if !fresh.is_empty() {
                self.membership_chunks_eval(table_metadata, &fresh, &batch_rows, &mut batch.hits)
                    .await?;
            }

            // …while rest queries only decide the shared-table upsert, so they
            // are only worth evaluating for rows nothing else matched. Gating
            // pre-pass: a row with a LocalEval match or a fresh hit needs no
            // rest eval — the per-row path's `if !matched` short-circuit, which
            // does zero PgEval round-trips for locally-matched rows. (A gating
            // miss is safe: uncovered rows fall back to per-row `pg_eval_any`,
            // itself guarded by `if !matched` at decide time.)
            let rest_rows: Vec<(usize, &[Option<ByteString>])> = if rest.is_empty() {
                Vec::new()
            } else {
                batch_rows
                    .iter()
                    .filter(|(event_idx, row)| {
                        if fresh
                            .iter()
                            .any(|q| batch.hits.contains(&(*event_idx, q.fingerprint)))
                        {
                            return false;
                        }
                        !update_queries.iter_complexity_ordered().any(|q| {
                            q.eval_strategy == UpdateEvalStrategy::LocalEval
                                && !core.frame_invalidations.contains(&q.fingerprint)
                                && update_query_matches_locally(q, table_metadata, row)
                        })
                    })
                    .copied()
                    .collect()
            };
            if !rest.is_empty() && !rest_rows.is_empty() {
                self.membership_chunks_eval(table_metadata, &rest, &rest_rows, &mut batch.hits)
                    .await?;
            }

            batch
                .covered
                .extend(batch_rows.iter().map(|(event_idx, _)| *event_idx));
            batch
                .rest_covered
                .extend(rest_rows.iter().map(|(event_idx, _)| *event_idx));
            batch.fresh_fps = fresh.iter().map(|q| q.fingerprint).collect();
            batch.rest_fps = rest.iter().map(|q| q.fingerprint).collect();
        }

        Ok(membership)
    }

    /// Evaluate `queries` against `rows` in `UNION ALL`-combined multi-row
    /// VALUES statements (`PG_EVAL_ROW_CHUNK` rows × `PG_EVAL_CHUNK` queries per
    /// statement), inserting `(event index, fingerprint)` matches into `hits`.
    async fn membership_chunks_eval(
        &mut self,
        table_metadata: &TableMetadata,
        queries: &[&UpdateQuery],
        rows: &[(usize, &[Option<ByteString>])],
        hits: &mut HashSet<(usize, Fingerprint)>,
    ) -> CacheResult<()> {
        for row_chunk in rows.chunks(PG_EVAL_ROW_CHUNK) {
            // Prepared per-query statements, pipelined (PGC-241 stage 4);
            // self-heal on failure: drop the cached statements and run the
            // inlined-VALUES form for this chunk (re-prepare on next use).
            if let Err(e) = self
                .membership_chunk_prepared(table_metadata, queries, row_chunk, hits)
                .await
            {
                warn!(
                    "prepared membership eval failed; falling back to inline: {}",
                    error_chain_format(e.current_context()),
                );
                self.membership_chunk_inline(table_metadata, queries, row_chunk, hits)
                    .await?;
            }
        }
        Ok(())
    }

    /// Prepared per-query membership for one row chunk (PGC-241 stage 4): one
    /// prepared statement per `(relation, query)` — shape fixed regardless of
    /// row count — bound to shared array parameters and executed concurrently,
    /// which tokio-postgres pipelines on `cache_eval_conn` in one flush.
    async fn membership_chunk_prepared(
        &mut self,
        table_metadata: &TableMetadata,
        queries: &[&UpdateQuery],
        row_chunk: &[(usize, &[Option<ByteString>])],
        hits: &mut HashSet<(usize, Fingerprint)>,
    ) -> CacheResult<()> {
        let (ordinals, column_arrays) = chunk_arrays_build(table_metadata, row_chunk);
        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(1 + column_arrays.len());
        params.push(&ordinals);
        for array in &column_arrays {
            params.push(array);
        }

        // Get-or-prepare. Misses prepare sequentially (cold cost, amortized:
        // steady state is all hits).
        let mut statements: Vec<(Fingerprint, Statement)> = Vec::with_capacity(queries.len());
        for update_query in queries {
            let key = PreparedEvalKey {
                relation_oid: table_metadata.relation_oid,
                fingerprint: update_query.fingerprint,
            };
            let statement = if let Some(statement) = self.prepared_membership.get(&key) {
                crate::metrics::handles().cdc.prepared_hits.increment(1);
                statement.clone()
            } else {
                crate::metrics::handles().cdc.prepared_misses.increment(1);
                let select = update_query
                    .resolved
                    .as_select()
                    .ok_or(CacheError::InvalidQuery)?;
                let unnest_select =
                    resolved_select_node_table_replace_with_unnest(select, table_metadata)
                        .map_err(|e| e.context_transform(CacheError::from))?;
                self.pg_eval_buf.clear();
                Deparse::deparse(&unnest_select, &mut self.pg_eval_buf);
                let statement = self
                    .cache_eval_conn
                    .prepare(&self.pg_eval_buf)
                    .await
                    .map_into_report::<CacheError>()?;
                self.prepared_membership.put(key, statement.clone());
                statement
            };
            statements.push((update_query.fingerprint, statement));
        }

        // Concurrent execution = pipelined on the single connection.
        let executions = future::join_all(
            statements
                .iter()
                .map(|(_, statement)| self.cache_eval_conn.query(statement, &params)),
        )
        .await;

        // Counted per successful execution only: failed statements re-run via
        // the inline fallback, which does its own counting.
        let mut executed = 0u64;
        let mut first_error = None;
        for ((fingerprint, _), execution) in statements.iter().zip(executions) {
            let result_rows = match execution {
                Ok(rows) => rows,
                Err(e) => {
                    // Self-heal: drop the (likely stale) statement; the caller
                    // falls back to inline for this chunk.
                    self.prepared_membership.pop(&PreparedEvalKey {
                        relation_oid: table_metadata.relation_oid,
                        fingerprint: *fingerprint,
                    });
                    first_error.get_or_insert(CacheError::PgError(e));
                    continue;
                }
            };
            executed += 1;
            for row in result_rows {
                let Ok(local_idx) = row.try_get::<_, i32>(0) else {
                    continue;
                };
                #[allow(clippy::cast_sign_loss)] // ordinals are 0..chunk len
                if let Some(&(event_idx, _)) = row_chunk.get(local_idx as usize) {
                    hits.insert((event_idx, *fingerprint));
                }
            }
        }
        crate::metrics::handles()
            .cdc
            .pg_eval_hits
            .increment(executed);
        match first_error {
            Some(e) => Err(e.into()),
            None => Ok(()),
        }
    }

    /// Inlined-VALUES membership for one row chunk: `UNION ALL`-combined arms
    /// per `PG_EVAL_CHUNK` queries via `simple_query`. The fallback when the
    /// prepared path fails (statement invalidated by DDL, etc.).
    async fn membership_chunk_inline(
        &mut self,
        table_metadata: &TableMetadata,
        queries: &[&UpdateQuery],
        row_chunk: &[(usize, &[Option<ByteString>])],
        hits: &mut HashSet<(usize, Fingerprint)>,
    ) -> CacheResult<()> {
        let chunk_rows: Vec<&[Option<ByteString>]> =
            row_chunk.iter().map(|(_, row)| *row).collect();
        for query_chunk in queries.chunks(PG_EVAL_CHUNK) {
            self.pg_eval_buf.clear();
            for (ordinal, update_query) in query_chunk.iter().enumerate() {
                if ordinal > 0 {
                    self.pg_eval_buf.push_str(" UNION ALL ");
                }
                let select = update_query
                    .resolved
                    .as_select()
                    .ok_or(CacheError::InvalidQuery)?;
                // Ordinal is bounded by PG_EVAL_CHUNK (32): never wraps.
                #[allow(clippy::cast_possible_wrap)]
                let batch_select = resolved_select_node_table_replace_with_values_batch(
                    select,
                    table_metadata,
                    &chunk_rows,
                    ordinal as i64,
                )
                .map_err(|e| e.context_transform(CacheError::from))?;
                Deparse::deparse(&batch_select, &mut self.pg_eval_buf);
            }

            let msgs = match self.cache_eval_conn.simple_query(&self.pg_eval_buf).await {
                Ok(m) => m,
                Err(e) => {
                    error!("batched predicate eval error: {}", error_chain_format(&e));
                    return Err(CacheError::PgError(e).into());
                }
            };
            crate::metrics::handles().cdc.pg_eval_hits.increment(1);
            for msg in msgs {
                let SimpleQueryMessage::Row(row) = msg else {
                    continue;
                };
                let (Some(ordinal), Some(local_idx)) = (
                    row.get(0).and_then(|v| v.parse::<usize>().ok()),
                    row.get(1).and_then(|v| v.parse::<usize>().ok()),
                ) else {
                    continue;
                };
                if let (Some(update_query), Some(&(event_idx, _))) =
                    (query_chunk.get(ordinal), row_chunk.get(local_idx))
                {
                    hits.insert((event_idx, update_query.fingerprint));
                }
            }
        }
        Ok(())
    }

    /// Batch the segment's row-change SELECTs (PGC-241 stage 3): per relation,
    /// every update event's `col IS DISTINCT FROM <new>` comparison runs in one
    /// statement per row chunk, joining the new tuples against the cache table
    /// by PK — `⌈K/PG_EVAL_ROW_CHUNK⌉` round-trips instead of one per row. A
    /// tuple absent from the join is the per-row "row not cached" (`None`)
    /// case: covered-but-absent in the matrix.
    ///
    /// Runs on `db_cache` like the per-row `query_row_changes`; all of a
    /// frame's reads see the same pre-frame committed state either way (the
    /// frame's own writes are buffered, or sit uncommitted on
    /// `cdc_write_conn`), so batching up front is snapshot-equivalent.
    async fn segment_row_changes_eval(
        &mut self,
        core: &WriterCore,
        events: &[FrameRowEvent],
        base_idx: usize,
        membership: &mut SegmentMembership,
    ) -> CacheResult<()> {
        let mut rows_by_relation: SegmentRows<'_> = HashMap::new();
        for (offset, event) in events.iter().enumerate() {
            let FrameRowEvent::Update {
                relation_oid,
                new_row_data,
                ..
            } = event
            else {
                continue;
            };
            // PGC-227: skip relations where no query's UPDATE invalidation
            // depends on changed columns — handle_update never reads changes.
            if !core
                .cache
                .update_queries
                .get(relation_oid)
                .is_some_and(|q| q.needs_change_eval())
            {
                continue;
            }
            rows_by_relation
                .entry(*relation_oid)
                .or_default()
                .push((base_idx + offset, new_row_data));
        }

        for (relation_oid, rows) in rows_by_relation {
            let Some(table_metadata) = core.cache.tables.get1(&relation_oid) else {
                continue;
            };
            if table_metadata.primary_key_columns.is_empty() {
                continue;
            }
            // Uniform VALUES arity, as in the membership batch.
            let full_width = table_metadata.columns.len();
            let batch_rows: Vec<(usize, &[Option<ByteString>])> = rows
                .into_iter()
                .filter(|(_, row)| row.len() == full_width)
                .collect();

            let batch = membership.relations.entry(relation_oid).or_default();
            for row_chunk in batch_rows.chunks(PG_EVAL_ROW_CHUNK) {
                // Prepared per-relation statement (PGC-241 stage 4); self-heal
                // on failure: drop the cached statement and run the inlined
                // form for this chunk (re-prepare on next use).
                if let Err(e) = self
                    .row_change_chunk_prepared(core, table_metadata, row_chunk, batch)
                    .await
                {
                    warn!(
                        "prepared row-change eval failed; falling back to inline: {}",
                        error_chain_format(e.current_context()),
                    );
                    self.row_change_chunk_inline(core, table_metadata, row_chunk, batch)
                        .await?;
                }
            }

            batch
                .row_change_covered
                .extend(batch_rows.iter().map(|(event_idx, _)| *event_idx));
        }

        Ok(())
    }

    /// Prepared row-change for one chunk: one statement per relation —
    /// `unnest()` array params, shape fixed regardless of row count — on
    /// `db_cache`, matching the per-row `query_row_changes` connection.
    async fn row_change_chunk_prepared(
        &mut self,
        core: &WriterCore,
        table_metadata: &TableMetadata,
        row_chunk: &[(usize, &[Option<ByteString>])],
        batch: &mut RelationBatch,
    ) -> CacheResult<()> {
        let relation_oid = table_metadata.relation_oid;
        let statement = if let Some(statement) = self.prepared_row_change.get(&relation_oid) {
            crate::metrics::handles().cdc.prepared_hits.increment(1);
            statement.clone()
        } else {
            crate::metrics::handles().cdc.prepared_misses.increment(1);
            self.pg_eval_buf.clear();
            row_change_select_into(&mut self.pg_eval_buf, table_metadata);
            let _ = write!(
                self.pg_eval_buf,
                " FROM (SELECT unnest($1::int4[]) AS {BATCH_IDX_COLUMN}"
            );
            for (i, column_meta) in table_metadata.columns.iter().enumerate() {
                let _ = write!(
                    self.pg_eval_buf,
                    ", unnest(${}::text[])::{} AS {}",
                    i + 2,
                    column_meta.cache_type_name,
                    column_meta.name
                );
            }
            self.pg_eval_buf.push_str(") AS v");
            row_change_join_on_into(&mut self.pg_eval_buf, table_metadata);
            let statement = core
                .db_cache
                .prepare(&self.pg_eval_buf)
                .await
                .map_into_report::<CacheError>()?;
            self.prepared_row_change
                .put(relation_oid, statement.clone());
            statement
        };

        let (ordinals, column_arrays) = chunk_arrays_build(table_metadata, row_chunk);
        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(1 + column_arrays.len());
        params.push(&ordinals);
        for array in &column_arrays {
            params.push(array);
        }

        let result_rows = match core.db_cache.query(&statement, &params).await {
            Ok(rows) => rows,
            Err(e) => {
                // Self-heal: drop the (likely stale) statement; the caller
                // falls back to inline for this chunk.
                self.prepared_row_change.pop(&relation_oid);
                return Err(CacheError::PgError(e).into());
            }
        };
        for row in result_rows {
            let Ok(local_idx) = row.try_get::<_, i32>(0) else {
                continue;
            };
            #[allow(clippy::cast_sign_loss)] // ordinals are 0..chunk len
            let Some(&(event_idx, _)) = row_chunk.get(local_idx as usize) else {
                continue;
            };
            let mut changes = HashMap::with_capacity(row.len().saturating_sub(1));
            for (col_idx, col) in row.columns().iter().enumerate().skip(1) {
                // NULL can't occur (IS DISTINCT FROM is total); default false
                // defensively, matching the per-row parse.
                changes.insert(
                    EcoString::from(col.name()),
                    row.try_get::<_, bool>(col_idx).unwrap_or(false),
                );
            }
            batch.row_changes.insert(event_idx, changes);
        }
        Ok(())
    }

    /// Inlined-VALUES row-change for one chunk via `simple_query` — the
    /// fallback when the prepared path fails.
    async fn row_change_chunk_inline(
        &mut self,
        core: &WriterCore,
        table_metadata: &TableMetadata,
        row_chunk: &[(usize, &[Option<ByteString>])],
        batch: &mut RelationBatch,
    ) -> CacheResult<()> {
        self.pg_eval_buf.clear();
        row_change_select_into(&mut self.pg_eval_buf, table_metadata);
        self.pg_eval_buf.push_str(" FROM (VALUES ");
        for (i, (_, row)) in row_chunk.iter().enumerate() {
            if i > 0 {
                self.pg_eval_buf.push_str(", ");
            }
            let _ = write!(self.pg_eval_buf, "({i}");
            for column_meta in &table_metadata.columns {
                let value = row
                    .get(column_meta.index())
                    .and_then(|v| v.as_deref())
                    .map_or_else(|| "NULL".to_owned(), escape::escape_literal);
                let _ = write!(
                    self.pg_eval_buf,
                    ", {value}::{}",
                    column_meta.cache_type_name
                );
            }
            self.pg_eval_buf.push(')');
        }
        let _ = write!(self.pg_eval_buf, ") AS v({BATCH_IDX_COLUMN}");
        for column_meta in &table_metadata.columns {
            let _ = write!(self.pg_eval_buf, ", {}", column_meta.name);
        }
        self.pg_eval_buf.push(')');
        row_change_join_on_into(&mut self.pg_eval_buf, table_metadata);

        let msgs = match core.db_cache.simple_query(&self.pg_eval_buf).await {
            Ok(m) => m,
            Err(e) => {
                error!("batched row-change eval error: {}", error_chain_format(&e));
                return Err(CacheError::PgError(e).into());
            }
        };
        for msg in msgs {
            let SimpleQueryMessage::Row(row) = msg else {
                continue;
            };
            let Some(local_idx) = row.get(0).and_then(|v| v.parse::<usize>().ok()) else {
                continue;
            };
            let Some(&(event_idx, _)) = row_chunk.get(local_idx) else {
                continue;
            };
            let mut changes = HashMap::with_capacity(row.len().saturating_sub(1));
            for (col_idx, col) in row.columns().iter().enumerate().skip(1) {
                // PG boolean text: "t"/"f"; treat non-"t" as false,
                // matching the per-row parse.
                changes.insert(EcoString::from(col.name()), row.get(col_idx) == Some("t"));
            }
            batch.row_changes.insert(event_idx, changes);
        }
        Ok(())
    }

    /// Mid-frame partial replay once the event log reaches its cap, bounding
    /// frame memory the way `frame_buf`'s chunk flush does.
    async fn frame_rows_replay_if_full(&mut self, core: &mut WriterCore) -> CacheResult<()> {
        if core.frame_rows.len() >= FRAME_ROWS_CAPACITY {
            self.frame_rows_replay(core)
                .await
                .attach_loc("cdc mid-frame replay")?;
        }
        Ok(())
    }

    /// Flush the accumulated batch (PGC-242): memo-bracket the union of the
    /// batched frames' relations, replay + commit the whole event log as one
    /// cache transaction, advance the watermark to `lsn` (the last batched
    /// frame's commit), and run the deferred bookkeeping. With an empty queue
    /// every CommitMark flushes its own frame — the pre-batching behavior.
    async fn batch_flush(&mut self, core: &mut WriterCore, lsn: u64) -> CacheResult<()> {
        // In-process memo seqlock (PGC-236, rung 1): bracket the batch's
        // visibility with begin (even→odd) before and end (odd→even) after,
        // over every relation the batched frames touched, so any memo over
        // them is invalidated atomically with the batch becoming visible.
        // Conceptually the relation-granularity twin of the per-query MV
        // dirty hooks in `frame_finalize`.
        //
        // Gated on the store being non-empty rather than on `enabled()`:
        // when no memo exists there is nothing to bust, and (unlike an
        // `enabled()` gate) this keeps the seqlock authoritative even
        // while memoization is toggled off at runtime — a change landing
        // during the disabled window still bumps the slot, so a later
        // re-enable cannot serve a snapshot that predates it.
        let memo_active = !core.state_view.memo.is_empty();
        if memo_active {
            for &oid in &core.frame_relation_oids {
                core.state_view
                    .memo
                    .slot_dirty_begin(SlotKey::Relation(oid));
            }
        }

        // Always publish the post-commit version (end the seqlock) before
        // propagating any finalize error: a slot left odd would permanently
        // disable memo serving for that relation on a live subsystem.
        let finalize = self.frame_finalize(core).await;
        if memo_active {
            for &oid in &core.frame_relation_oids {
                core.state_view.memo.slot_dirty_end(SlotKey::Relation(oid));
            }
        }
        finalize?;

        core.frame_state = FrameState::Idle;
        core.frame_invalidations.clear();
        core.frame_relation_oids.clear();
        core.frame_buf.clear();
        core.frame_buf_relations.clear();
        core.frame_rows.clear();
        core.frame_chunk_flushed = false;
        core.batch_frames = 0;
        core.batch_events = 0;
        core.batch_deleted_pks.clear();
        core.toast_overlay_reset();
        core.batch_toast_guard_oids.clear();
        self.applied_lsn_advance(lsn);
        core.last_applied_lsn = self.last_applied_lsn;
        // Per-frame deletes/truncates were stamped by Boundary events during
        // replay; these drains catch what replay didn't reach — pending
        // entries from a frame that entered `Recovering` before its boundary
        // (no-ops in the normal path, where the lists are already empty).
        let frame_deletes = std::mem::take(&mut core.frame_deleted_keys);
        for (relation_oid, key) in frame_deletes {
            core.population_deleted_keys.record(relation_oid, key, lsn);
        }
        let frame_truncated = std::mem::take(&mut core.frame_truncated_relations);
        for relation_oid in frame_truncated {
            core.population_deleted_keys.abort_below(relation_oid, lsn);
        }
        // Bulk invalidations recorded outside replay (mid-batch DDL drops,
        // 40P01 recovery) — flush-LSN-stamped by design (an upper bound on
        // the triggering frame's commit; over-aborts, never under-aborts).
        let batch_truncated = std::mem::take(&mut core.batch_truncated_relations);
        for relation_oid in batch_truncated {
            core.population_deleted_keys.abort_below(relation_oid, lsn);
        }
        // The batch is closed; flush maintenance that was deferred while it
        // was open (it would have deadlocked on the frame's locks).
        if core.purge_pending {
            let threshold = core.cache.generation_purge_threshold();
            core.generation_purge(threshold)
                .await
                .attach_loc("deferred generation purge")?;
            core.purge_pending = false;
        }
        Ok(())
    }

    /// Apply a `CommitMark`: flush the frame's deferred invalidations and commit
    /// (or recover) per its state. Extracted from the `CommitMark` handler so the
    /// memo seqlock bracket can publish the post-commit version on every exit
    /// path — including an error return from here.
    async fn frame_finalize(&mut self, core: &mut WriterCore) -> CacheResult<()> {
        // Evaluate + emit the frame's buffered row events first (PGC-241): the
        // replay is what can open the txn (`Active → TxnOpen`) or enter
        // `Recovering`, so the state match below must run after it.
        self.frame_rows_replay(core).await?;
        match core.frame_state {
            FrameState::TxnOpen => {
                self.frame_invalidations_flush(core).await?;
                // The buffered writes flush as one BEGIN; …; COMMIT here
                // (PGC-228). A 40P01 now surfaces at flush, not per statement:
                // route it through frame_dml_result so the frame enters
                // Recovering, then run relation-level recovery (same as a
                // mid-frame deadlock would).
                let r = self.frame_commit(core).await;
                self.frame_dml_result(core, r)
                    .await
                    .attach_loc("cdc commit frame")?;
                if core.frame_state == FrameState::Recovering {
                    self.frame_recover(core)
                        .await
                        .attach_loc("cdc recover frame at commit")?;
                }
            }
            // A frame can flag queries for invalidation without any in-place
            // write (e.g. a growing-join insert excluded from in-place
            // maintenance) — no `BEGIN` was opened, but the flagged queries must
            // still be invalidated here.
            FrameState::Active => {
                self.frame_invalidations_flush(core).await?;
            }
            FrameState::Recovering => {
                // Relation-level recovery evicts every query over the affected
                // relations — a superset of the selectively flagged fps, so the
                // fp flush is subsumed.
                self.frame_recover(core)
                    .await
                    .attach_loc("cdc recover frame")?;
            }
            // CommitMark without a preceding Begin: pgoutput always pairs them
            // for published txns, so this is unreachable.
            FrameState::Idle => {
                debug_assert!(false, "CommitMark without a preceding Begin");
            }
        }
        Ok(())
    }

    /// Handle a CDC command, dispatching to the appropriate method.
    pub async fn cdc_command_handle(
        &mut self,
        core: &mut WriterCore,
        cmd: CdcCommand,
        queued: usize,
    ) -> CacheResult<()> {
        let m = crate::metrics::handles();
        let cmd_handle = match &cmd {
            CdcCommand::Begin { .. } => &m.cdc.cmd_begin,
            CdcCommand::TableRegister(_) => &m.cdc.cmd_table_register,
            CdcCommand::Insert { .. } => &m.cdc.cmd_insert,
            CdcCommand::Update { .. } => &m.cdc.cmd_update,
            CdcCommand::Delete { .. } => &m.cdc.cmd_delete,
            CdcCommand::Truncate { .. } => &m.cdc.cmd_truncate,
            CdcCommand::CommitMark { .. } => &m.cdc.cmd_commit_mark,
            CdcCommand::KeepAliveMark { .. } => &m.cdc.cmd_keepalive_mark,
        };
        let handle_start = Instant::now();
        // Relation OIDs are recorded from frame start so a mid-frame 40P01 can
        // recover every relation the frame touched (pre-deadlock writes rolled
        // back too).
        match cmd {
            CdcCommand::Begin { xid } => {
                debug_assert!(
                    !core.frame_open,
                    "Begin within an open source-transaction frame"
                );
                trace!(xid, "cdc frame begin");
                core.frame_open = true;
                // Reset only at a fresh batch — when accumulating (PGC-242)
                // the log, buffers, and pending bookkeeping span frames.
                if core.frame_state == FrameState::Idle {
                    core.frame_state = FrameState::Active;
                    core.frame_buf.clear();
                    core.frame_buf_relations.clear();
                    core.frame_rows.clear();
                    core.frame_chunk_flushed = false;
                    core.frame_deleted_keys.clear();
                    core.frame_truncated_relations.clear();
                    core.batch_deleted_pks.clear();
                    core.toast_overlay_reset();
                    core.batch_toast_guard_oids.clear();
                }
            }
            CdcCommand::TableRegister(table_metadata) => {
                core.frame_relation_oids.insert(table_metadata.relation_oid);
                let relation_oid = table_metadata.relation_oid;
                // A mid-frame Relation message whose metadata CHANGED (intra-txn
                // DDL): the relation's buffered events were captured under the
                // old column layout, so replaying them after the recreate
                // misaligns position-based lookups and references dropped
                // columns. (An identical re-sent Relation — e.g. after a
                // publication change — leaves everything in place.)
                let metadata_changed = core
                    .cache
                    .tables
                    .get1(&relation_oid)
                    .is_none_or(|current| !current.schema_eq(&table_metadata));
                if metadata_changed {
                    if core.frame_buf_relations.contains(&relation_oid) {
                        // The relation's cache-table writes (naming the old
                        // columns) are already committed to `frame_buf` or
                        // executed in the open cache txn — a partial replay
                        // moved them out of `frame_rows`, so discarding events
                        // can't retract them, and at COMMIT they would run
                        // against the recreated table and fail. Escalate to
                        // frame recovery: roll the cache txn back and let
                        // CommitMark invalidate + repopulate every relation the
                        // frame touched from post-DDL origin (PGC-264).
                        self.frame_recover_enter(core)
                            .await
                            .attach_loc("mid-frame DDL on a buffered relation")?;
                    } else {
                        // No buffered writes yet: the relation's events are all
                        // still in `frame_rows`. Discard them and purge its
                        // toast overlay (a different relation's partial replay
                        // may have recorded a stale entry under the old layout);
                        // the recreate evicts its queries and empties the table,
                        // so it rebuilds from origin (PGC-264).
                        core.toast_overlay_relation_invalidate(relation_oid);
                        let before = core.frame_rows.len();
                        core.frame_rows.retain(|event| match event {
                            FrameRowEvent::Insert {
                                relation_oid: r, ..
                            }
                            | FrameRowEvent::Update {
                                relation_oid: r, ..
                            }
                            | FrameRowEvent::UpdateToasted {
                                relation_oid: r, ..
                            }
                            | FrameRowEvent::UpdateToastFallback {
                                relation_oid: r, ..
                            }
                            | FrameRowEvent::Delete {
                                relation_oid: r, ..
                            } => *r != relation_oid,
                            FrameRowEvent::Truncate { .. } | FrameRowEvent::Boundary { .. } => true,
                        });
                        if core.frame_rows.len() != before {
                            core.batch_truncated_relations.push(relation_oid);
                        }
                    }
                }
                // Schema change: prepared eval SQL embeds the column list —
                // drop the relation's cached statements so the next use
                // re-prepares against the new shape.
                self.prepared_row_change.pop(&relation_oid);
                let stale: Vec<PreparedEvalKey> = self
                    .prepared_membership
                    .iter()
                    .map(|(key, _)| *key)
                    .filter(|key| key.relation_oid == relation_oid)
                    .collect();
                for key in stale {
                    self.prepared_membership.pop(&key);
                }
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
                        let (row_data, toasted) = core.row_convert(row_data);
                        // pgoutput never elides toast from INSERT images;
                        // dropping the event keeps the NULL-holed row out of
                        // the shared table (handle_insert's tracked-key upsert
                        // would write it, and merges never overwrite).
                        if !toasted.is_empty() {
                            Self::toast_unexpected_invalidate(core, relation_oid, "insert");
                        } else {
                            core.frame_rows.push(FrameRowEvent::Insert {
                                relation_oid,
                                row_data,
                            });
                            core.batch_events += 1;
                            self.frame_rows_replay_if_full(core).await?;
                        }
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
                    let (key_data, key_toasted) = core.row_convert(key_data);
                    let (new_row_data, toasted) = core.row_convert(row_data);
                    // Key tuples carry real values under every replica
                    // identity; a toasted one can't even key the old-PK delete.
                    if !key_toasted.is_empty() {
                        Self::toast_unexpected_invalidate(core, relation_oid, "update key tuple");
                    } else {
                        // Toasted images are resolved by the replay pre-pass
                        // (`toast_repair_events`) — batched there instead of a
                        // per-event lookup here.
                        let event = if toasted.is_empty() {
                            FrameRowEvent::Update {
                                relation_oid,
                                key_data,
                                new_row_data,
                            }
                        } else {
                            FrameRowEvent::UpdateToasted {
                                relation_oid,
                                key_data,
                                new_row_data,
                                toasted,
                            }
                        };
                        core.frame_rows.push(event);
                        core.batch_events += 1;
                        self.frame_rows_replay_if_full(core).await?;
                    }
                }
            }
            CdcCommand::Delete {
                relation_oid,
                row_data,
            } => {
                core.frame_relation_oids.insert(relation_oid);
                if core.frame_state != FrameState::Recovering {
                    let (row_data, toasted) = core.row_convert(row_data);
                    // Delete images are key/old tuples — same reasoning as the
                    // update key tuple above.
                    if !toasted.is_empty() {
                        Self::toast_unexpected_invalidate(core, relation_oid, "delete");
                    } else {
                        core.frame_rows.push(FrameRowEvent::Delete {
                            relation_oid,
                            row_data,
                        });
                        core.batch_events += 1;
                        self.frame_rows_replay_if_full(core).await?;
                    }
                }
            }
            CdcCommand::Truncate { relation_oids } => {
                core.frame_relation_oids
                    .extend(relation_oids.iter().copied());
                if core.frame_state != FrameState::Recovering {
                    // Toast-repair guarding happens at the event's replay
                    // position (PGC-264): pre-truncate events may still trust
                    // the pre-batch image.
                    core.frame_rows
                        .push(FrameRowEvent::Truncate { relation_oids });
                    core.batch_events += 1;
                    self.frame_rows_replay_if_full(core).await?;
                }
            }
            CdcCommand::CommitMark { lsn } => {
                // The frame's commit boundary rides in the event log (PGC-242):
                // deleted keys and truncate watermarks are produced *during
                // replay* (`frame_cache_delete` runs in the decide pass), so
                // the per-frame LSN context must travel with the events for
                // logs that span multiple frames.
                core.frame_rows
                    .push(FrameRowEvent::Boundary { commit_lsn: lsn });
                core.batch_frames += 1;
                core.batch_last_lsn = lsn;
                core.frame_open = false;

                // Flush decision (PGC-242): an empty queue flushes immediately
                // (caught up — today's per-frame behavior, zero added
                // latency); a backlog accumulates, amortizing eval and commit
                // round-trips over the frames that would otherwise wait in the
                // queue anyway. `Recovering` flushes now (recovery semantics
                // are batch-terminal), and the size caps bound memory, the
                // memo-bracket window, and the recovery blast radius — they
                // override a fault-injected hold; the queue-empty trigger
                // respects it.
                let flush = core.frame_state == FrameState::Recovering
                    || core.batch_events >= FRAME_ROWS_CAPACITY
                    || core.batch_frames >= BATCH_FRAMES_MAX
                    || (queued == 0 && !fault_cdc_hold_flush(core.batch_frames));
                if flush {
                    self.batch_flush(core, lsn).await?;
                }
            }
            CdcCommand::KeepAliveMark { lsn } => {
                // Keepalives only arrive between source transactions, so no
                // frame may be open. The guard keeps the watermark from
                // advancing past an open frame if that ever breaks.
                debug_assert!(
                    !core.frame_open,
                    "keepalive received with an open source-transaction frame"
                );
                if !core.frame_open {
                    // The keepalive LSN is past every accumulated frame:
                    // flush first so the watermark never claims unapplied
                    // events (PGC-242).
                    if core.batch_frames > 0 {
                        let up_to = core.batch_last_lsn;
                        self.batch_flush(core, up_to).await?;
                    }
                    self.applied_lsn_advance(lsn);
                    core.last_applied_lsn = self.last_applied_lsn;
                }
            }
        }
        // Self-defers while the frame is open; flushes here at CommitMark
        // (frame just committed) and KeepAlive (no frame).
        core.publication_dirty_drain().await?;
        cmd_handle.record(handle_start.elapsed().as_secs_f64());
        Ok(())
    }

    /// Advance `last_applied_lsn` forward to `lsn`, updating the Prometheus
    /// gauge. No-op if `lsn` does not advance the watermark.
    fn applied_lsn_advance(&mut self, lsn: u64) {
        if lsn > self.last_applied_lsn {
            self.last_applied_lsn = lsn;
            // LSNs past 2^53 lose precision in f64 (~9 PB of WAL — irrelevant).
            #[allow(clippy::cast_precision_loss)]
            crate::metrics::handles().cdc.applied_lsn.set(lsn as f64);
        }
    }

    /// Defensive (PGC-264): an unchanged-toast marker in a tuple that cannot
    /// carry one per the pgoutput protocol (insert images, delete/key tuples).
    /// The event is dropped by the caller; invalidating every query over the
    /// relation keeps that safe.
    fn toast_unexpected_invalidate(core: &mut WriterCore, relation_oid: u32, tuple_kind: &str) {
        error!(
            relation_oid,
            tuple_kind, "unexpected unchanged-toast marker; invalidating relation queries"
        );
        if let Some(update_queries) = core.cache.update_queries.get(&relation_oid) {
            core.frame_invalidations.extend(
                update_queries
                    .iter_complexity_ordered()
                    .map(|q| q.fingerprint),
            );
        }
        crate::metrics::handles().cdc.toast_fallbacks.increment(1);
    }

    /// Record a complete in-batch write of a row into the toast overlay
    /// (PGC-264): later toasted updates of the same PK repair from these
    /// values instead of the (now stale) pre-batch committed image. Gated on
    /// the relation having a toastable column — only those can see a toasted
    /// update, so only they ever consult the overlay.
    fn toast_overlay_record_write(
        core: &mut WriterCore,
        relation_oid: u32,
        row_data: &[Option<ByteString>],
    ) {
        let Some(table_metadata) = core.cache.tables.get1(&relation_oid) else {
            return;
        };
        if !table_metadata.has_toastable_column() {
            return;
        }
        let Some(key) = pk_body_render(table_metadata, row_data) else {
            return;
        };
        // Reuse a pooled Vec (field access keeps the `core.cache` borrow of
        // `table_metadata` disjoint from the pool and overlay borrows).
        let mut values = core.toast_overlay_pool.pop().unwrap_or_default();
        Self::toastable_values_extend(table_metadata, row_data, &mut values);
        let displaced = core
            .batch_toast_overlay
            .insert((relation_oid, key), ToastOverlayEntry::Values(values));
        core.toast_overlay_recycle(displaced);
    }

    /// Collect a row image's toastable-column `(position, value)` pairs into
    /// `values` — the payload of a [`ToastOverlayEntry::Values`].
    fn toastable_values_extend(
        table_metadata: &TableMetadata,
        row_data: &[Option<ByteString>],
        values: &mut Vec<(usize, Option<ByteString>)>,
    ) {
        values.extend(
            table_metadata
                .columns
                .iter()
                .filter(|c| c.is_toastable())
                .map(|c| (c.index(), row_data.get(c.index()).cloned().flatten())),
        );
    }

    /// Tombstone a PK in the toast overlay (PGC-264): the row was deleted (or
    /// its old key vacated) this batch, so its pre-batch image must not be
    /// used as a repair source.
    fn toast_overlay_record_delete(
        core: &mut WriterCore,
        relation_oid: u32,
        row_data: &[Option<ByteString>],
    ) {
        let Some(table_metadata) = core.cache.tables.get1(&relation_oid) else {
            return;
        };
        if !table_metadata.has_toastable_column() {
            return;
        }
        if let Some(key) = pk_body_render(table_metadata, row_data) {
            let displaced = core
                .batch_toast_overlay
                .insert((relation_oid, key), ToastOverlayEntry::Deleted);
            core.toast_overlay_recycle(displaced);
        }
    }

    /// Resolve every `UpdateToasted` in one replay's events (PGC-264), in two
    /// passes over the arrival order:
    ///
    /// 1. Maintain the batch toast overlay: complete writes record their
    ///    toastable values per PK, deletes (and vacated old PKs) tombstone,
    ///    truncates guard the relation and drop its prior entries (writes
    ///    after the truncate re-arm repair). A toasted update whose source PK
    ///    (the old PK when the PK changed) has an overlay value repairs from
    ///    it in memory; a tombstone or guarded relation falls back; anything
    ///    else queues for the lookup pass.
    /// 2. One batched lookup per relation against the pre-batch committed
    ///    image. The only in-batch writes pass 1 couldn't see for a queued
    ///    event's source PK are earlier queued toasted updates themselves
    ///    (a complete write or delete in between would have armed pass-1
    ///    repair or fallback), so repairs chain in arrival order: each
    ///    repaired event's post-image is the repair source for the next
    ///    same-PK event, seeded from the lookup. Absent rows (and lookup
    ///    failures — the slot is already acked at decode time, PGC-147, so
    ///    there is no redelivery to lean on) fall back. The chain's final
    ///    post-images then land in the overlay without displacing pass-1
    ///    entries, which always stem from arrival-later complete writes.
    ///
    /// No `UpdateToasted` remains in `events` afterwards.
    async fn toast_repair_events(core: &mut WriterCore, events: &mut [FrameRowEvent]) {
        let metrics = &crate::metrics::handles().cdc;
        let mut pending: HashMap<u32, Vec<PendingRepairSlot>> = HashMap::new();

        for (idx, event) in events.iter_mut().enumerate() {
            match event {
                FrameRowEvent::Insert {
                    relation_oid,
                    row_data,
                } => {
                    Self::toast_overlay_record_write(core, *relation_oid, row_data);
                }
                FrameRowEvent::Update {
                    relation_oid,
                    key_data,
                    new_row_data,
                } => {
                    Self::toast_overlay_record_write(core, *relation_oid, new_row_data);
                    if !key_data.is_empty() {
                        Self::toast_overlay_record_delete(core, *relation_oid, key_data);
                    }
                }
                FrameRowEvent::Delete {
                    relation_oid,
                    row_data,
                } => {
                    Self::toast_overlay_record_delete(core, *relation_oid, row_data);
                }
                FrameRowEvent::Truncate { relation_oids } => {
                    for &oid in relation_oids.iter() {
                        core.toast_overlay_relation_invalidate(oid);
                    }
                }
                FrameRowEvent::Boundary { .. } | FrameRowEvent::UpdateToastFallback { .. } => {}
                FrameRowEvent::UpdateToasted { .. } => {
                    let FrameRowEvent::UpdateToasted {
                        relation_oid,
                        key_data,
                        mut new_row_data,
                        toasted,
                    } = std::mem::replace(event, FrameRowEvent::Boundary { commit_lsn: 0 })
                    else {
                        continue;
                    };
                    *event = Self::toast_resolve_from_overlay(
                        core,
                        &mut pending,
                        idx,
                        relation_oid,
                        key_data,
                        &mut new_row_data,
                        toasted,
                    );
                    // `new_row_data` was moved back inside the resolved event.
                }
            }
        }

        // Pass 2: batched lookups, one statement per relation. `chain` holds
        // the per-PK toastable state as it advances through this relation's
        // queued events in arrival order — a queued event is an in-batch
        // write the overlay never saw, so the next same-PK event must repair
        // from its post-image, not the pre-batch image.
        for (relation_oid, pendings) in pending {
            let lookup = Self::toast_lookup_batch(core, relation_oid, &pendings).await;
            let mut chain: HashMap<EcoString, ToastOverlayEntry> = HashMap::new();
            for p in pendings {
                let Some(slot) = events.get_mut(p.event_idx) else {
                    continue;
                };
                let FrameRowEvent::UpdateToasted {
                    relation_oid,
                    key_data,
                    mut new_row_data,
                    toasted,
                } = std::mem::replace(slot, FrameRowEvent::Boundary { commit_lsn: 0 })
                else {
                    continue;
                };
                let source_values = match chain.get(&p.overlay_key) {
                    Some(ToastOverlayEntry::Values(values)) => Some(values),
                    Some(ToastOverlayEntry::Deleted) => None,
                    None => lookup.as_ref().and_then(|rows| rows.get(&p.raw_pk)),
                };
                let mut repaired = source_values.is_some();
                if let Some(values) = source_values {
                    for &t in &toasted {
                        match values.iter().find(|(pos, _)| *pos == t) {
                            Some((_, v)) => {
                                if let Some(cell) = new_row_data.get_mut(t) {
                                    *cell = v.clone();
                                }
                            }
                            None => repaired = false,
                        }
                    }
                }

                let pk_changed = !key_data.is_empty();
                *slot = if repaired {
                    metrics.toast_repairs.increment(1);
                    // Advance the chain to this event's post-image under the
                    // row's resulting PK; a vacated old PK is dead as a
                    // repair source.
                    if let Some(table_metadata) = core.cache.tables.get1(&relation_oid) {
                        let mut post = Vec::new();
                        Self::toastable_values_extend(table_metadata, &new_row_data, &mut post);
                        let result_key = if pk_changed {
                            pk_body_render(table_metadata, &new_row_data)
                        } else {
                            Some(p.overlay_key.clone())
                        };
                        if pk_changed {
                            chain.insert(p.overlay_key.clone(), ToastOverlayEntry::Deleted);
                        }
                        if let Some(key) = result_key {
                            chain.insert(key, ToastOverlayEntry::Values(post));
                        }
                    }
                    FrameRowEvent::Update {
                        relation_oid,
                        key_data,
                        new_row_data,
                    }
                } else {
                    // The fallback handler deletes the row: later queued
                    // events of either PK must not repair from the (stale)
                    // pre-batch image.
                    chain.insert(p.overlay_key.clone(), ToastOverlayEntry::Deleted);
                    if pk_changed
                        && let Some(table_metadata) = core.cache.tables.get1(&relation_oid)
                        && let Some(key) = pk_body_render(table_metadata, &new_row_data)
                    {
                        chain.insert(key, ToastOverlayEntry::Deleted);
                    }
                    Self::toast_fallback_build(core, relation_oid, key_data, new_row_data, &toasted)
                };
            }
            // Flush the chain's final post-images. `or_insert`: a pass-1
            // entry always stems from a complete write later in arrival
            // order than every queued event, so it must win; tombstones were
            // already recorded eagerly (pass-1 Queue branch for vacated old
            // PKs, `toast_fallback_build` for fallen-back rows).
            for (key, entry) in chain {
                if matches!(entry, ToastOverlayEntry::Values(_)) {
                    core.batch_toast_overlay
                        .entry((relation_oid, key))
                        .or_insert(entry);
                }
            }
        }
    }

    /// Pass-1 resolution of one `UpdateToasted`: repair from the overlay,
    /// fall back, or queue for the batched lookup (returning the event
    /// unchanged). Also performs the event's own overlay bookkeeping.
    #[allow(clippy::too_many_arguments)]
    fn toast_resolve_from_overlay(
        core: &mut WriterCore,
        pending: &mut HashMap<u32, Vec<PendingRepairSlot>>,
        event_idx: usize,
        relation_oid: u32,
        key_data: Vec<Option<ByteString>>,
        new_row_data: &mut Vec<Option<ByteString>>,
        toasted: Vec<usize>,
    ) -> FrameRowEvent {
        let metrics = &crate::metrics::handles().cdc;
        let Some(table_metadata) = core.cache.tables.get1(&relation_oid) else {
            // Unknown relation: handlers no-op on it either way.
            return FrameRowEvent::Update {
                relation_oid,
                key_data,
                new_row_data: std::mem::take(new_row_data),
            };
        };

        // The cached copy the unchanged-toast marker refers to lives under
        // the row's PRE-image key: when this UPDATE changed the PK
        // (`key_data` non-empty), the source row is the old PK.
        let pk_changed = !key_data.is_empty();
        let source_row: &[Option<ByteString>] = if pk_changed { &key_data } else { new_row_data };
        let source_pk = pk_body_render(table_metadata, source_row);

        let resolution = match &source_pk {
            None => ToastResolution::Fallback,
            Some(key) => match core.batch_toast_overlay.get(&(relation_oid, key.clone())) {
                Some(ToastOverlayEntry::Values(values)) => {
                    let mut complete = true;
                    let mut repaired: Vec<(usize, Option<ByteString>)> =
                        Vec::with_capacity(toasted.len());
                    for &t in &toasted {
                        match values.iter().find(|(pos, _)| *pos == t) {
                            Some((_, v)) => repaired.push((t, v.clone())),
                            None => complete = false,
                        }
                    }
                    if complete {
                        ToastResolution::Repaired(repaired)
                    } else {
                        ToastResolution::Fallback
                    }
                }
                Some(ToastOverlayEntry::Deleted) => ToastResolution::Fallback,
                None if core.batch_toast_guard_oids.contains(&relation_oid) => {
                    ToastResolution::Fallback
                }
                None => {
                    // Raw PK values for matching the lookup result; a NULL PK
                    // value can never match a lookup row, so fall back.
                    let raw: Option<Vec<ByteString>> = table_metadata
                        .primary_key_columns
                        .iter()
                        .map(|pk_column| {
                            table_metadata
                                .columns
                                .get(pk_column.as_str())
                                .and_then(|c| source_row.get(c.index()).cloned().flatten())
                        })
                        .collect();
                    match raw {
                        Some(raw_pk) => ToastResolution::Queue(raw_pk),
                        None => ToastResolution::Fallback,
                    }
                }
            },
        };

        match resolution {
            ToastResolution::Repaired(values) => {
                for (t, v) in values {
                    if let Some(slot) = new_row_data.get_mut(t) {
                        *slot = v;
                    }
                }
                metrics.toast_repairs.increment(1);
                let new_row_data = std::mem::take(new_row_data);
                Self::toast_overlay_record_write(core, relation_oid, &new_row_data);
                if pk_changed {
                    Self::toast_overlay_record_delete(core, relation_oid, &key_data);
                }
                FrameRowEvent::Update {
                    relation_oid,
                    key_data,
                    new_row_data,
                }
            }
            ToastResolution::Queue(raw_pk) => {
                // The vacated old PK is gone whatever pass 2 decides; the new
                // PK's overlay entry is written by pass 2.
                if pk_changed {
                    Self::toast_overlay_record_delete(core, relation_oid, &key_data);
                }
                pending
                    .entry(relation_oid)
                    .or_default()
                    .push(PendingRepairSlot {
                        event_idx,
                        overlay_key: source_pk.expect("queued resolution rendered a source pk"),
                        raw_pk,
                    });
                FrameRowEvent::UpdateToasted {
                    relation_oid,
                    key_data,
                    new_row_data: std::mem::take(new_row_data),
                    toasted,
                }
            }
            ToastResolution::Fallback => {
                if pk_changed {
                    Self::toast_overlay_record_delete(core, relation_oid, &key_data);
                }
                Self::toast_fallback_build(
                    core,
                    relation_oid,
                    key_data,
                    std::mem::take(new_row_data),
                    &toasted,
                )
            }
        }
    }

    /// Build the conservative fallback event for an unrepairable toasted
    /// update, tombstoning its (to-be-deleted) row in the overlay.
    fn toast_fallback_build(
        core: &mut WriterCore,
        relation_oid: u32,
        key_data: Vec<Option<ByteString>>,
        new_row_data: Vec<Option<ByteString>>,
        toasted: &[usize],
    ) -> FrameRowEvent {
        let toasted_columns: Vec<EcoString> = core
            .cache
            .tables
            .get1(&relation_oid)
            .map(|table_metadata| {
                table_metadata
                    .columns
                    .iter()
                    .filter(|c| toasted.contains(&c.index()))
                    .map(|c| c.name.clone())
                    .collect()
            })
            .unwrap_or_default();
        // The fallback handler deletes the row; later in-batch repairs must
        // not trust either image.
        Self::toast_overlay_record_delete(core, relation_oid, &new_row_data);
        crate::metrics::handles().cdc.toast_fallbacks.increment(1);
        debug!(relation_oid, "toast repair fell back");
        FrameRowEvent::UpdateToastFallback {
            relation_oid,
            key_data,
            new_row_data,
            toasted_columns,
        }
    }

    /// One batched pre-batch-image lookup for a relation's queued repairs:
    /// `SELECT <pk cols>, <toastable cols> FROM rel WHERE <pk> IN (…)`,
    /// deduplicated by PK. Returns raw-PK → toastable `(position, value)`
    /// pairs, or `None` if the lookup failed (callers fall back).
    async fn toast_lookup_batch(
        core: &WriterCore,
        relation_oid: u32,
        pendings: &[PendingRepairSlot],
    ) -> Option<HashMap<Vec<ByteString>, Vec<(usize, Option<ByteString>)>>> {
        let table_metadata = core.cache.tables.get1(&relation_oid)?;
        let pk_columns: Vec<&EcoString> = table_metadata
            .primary_key_columns
            .iter()
            .map(|pk_column| {
                table_metadata
                    .columns
                    .get(pk_column.as_str())
                    .map(|c| &c.name)
            })
            .collect::<Option<Vec<_>>>()?;
        let toastable: Vec<(usize, &EcoString)> = table_metadata
            .columns
            .iter()
            .filter(|c| c.is_toastable())
            .map(|c| (c.index(), &c.name))
            .collect();

        let mut sql = String::with_capacity(SQL_BUFFER_CAPACITY);
        sql.push_str("SELECT ");
        for (i, column) in pk_columns
            .iter()
            .copied()
            .chain(toastable.iter().map(|(_, name)| *name))
            .enumerate()
        {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(column);
        }
        let _ = write!(
            sql,
            " FROM {}.{} WHERE ",
            table_metadata.schema, table_metadata.name
        );
        let multi_pk = pk_columns.len() > 1;
        if multi_pk {
            sql.push('(');
            for (i, column) in pk_columns.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                sql.push_str(column);
            }
            sql.push(')');
        } else {
            sql.push_str(pk_columns.first()?);
        }
        sql.push_str(" IN (");
        let mut seen: HashSet<&[ByteString]> = HashSet::new();
        let mut first = true;
        for p in pendings {
            if !seen.insert(p.raw_pk.as_slice()) {
                continue;
            }
            if !first {
                sql.push_str(", ");
            }
            first = false;
            if multi_pk {
                sql.push('(');
            }
            for (i, value) in p.raw_pk.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                sql.push_str(&escape::escape_literal(value));
            }
            if multi_pk {
                sql.push(')');
            }
        }
        sql.push(')');

        match core.db_cache.simple_query(&sql).await {
            Ok(msgs) => {
                let mut rows = HashMap::new();
                for msg in msgs {
                    let SimpleQueryMessage::Row(row) = msg else {
                        continue;
                    };
                    let key: Option<Vec<ByteString>> = (0..pk_columns.len())
                        .map(|i| row.get(i).map(ByteString::from))
                        .collect();
                    let Some(key) = key else { continue };
                    let values: Vec<(usize, Option<ByteString>)> = toastable
                        .iter()
                        .enumerate()
                        .map(|(j, (pos, _))| {
                            (*pos, row.get(pk_columns.len() + j).map(ByteString::from))
                        })
                        .collect();
                    rows.insert(key, values);
                }
                Some(rows)
            }
            Err(e) => {
                error!(
                    relation_oid,
                    "batched toast repair lookup failed, falling back to invalidation: {e}"
                );
                None
            }
        }
    }

    /// Buffer an unconditional upsert of `row_data` into the relation's cache
    /// table in the open frame (PGC-228), opening the frame txn if needed.
    async fn frame_cache_upsert(
        &mut self,
        core: &mut WriterCore,
        relation_oid: u32,
        row_data: &[Option<ByteString>],
    ) -> CacheResult<()> {
        core.frame_begin_ensure([relation_oid]);
        let table_metadata =
            core.cache
                .tables
                .get1(&relation_oid)
                .ok_or(CacheError::UnknownTable {
                    oid: Some(relation_oid),
                    name: None,
                })?;
        // A re-upserted PK is present again for later batched frames' row-
        // change classification (PGC-242). Gated: rendering is free when no
        // batch deletes are outstanding.
        if !core.batch_deleted_pks.is_empty()
            && let Some(key) = pk_body_render(table_metadata, row_data)
        {
            core.batch_deleted_pks.remove(&(relation_oid, key));
        }
        self.cache_upsert_unconditional_into(&mut core.frame_buf, table_metadata, row_data);
        self.frame_write_finish(core).await
    }

    /// Buffer a PK-qualified delete of `row_data` from the relation's cache
    /// table into the open frame (PGC-228), opening the frame txn if needed.
    /// The relation is known-present by the time a handler reaches a delete, so
    /// a missing entry is a hard `UnknownTable` error rather than a skip.
    async fn frame_cache_delete(
        &mut self,
        core: &mut WriterCore,
        relation_oid: u32,
        row_data: &[Option<ByteString>],
    ) -> CacheResult<()> {
        self.frame_cache_delete_inner(core, relation_oid, row_data, true)
            .await
    }

    /// `frame_cache_delete` without the PGC-250 lost-key record: for evicting
    /// a row that is still alive at origin while every query over the relation
    /// is being invalidated (toast fallback under active tracking, PGC-264).
    /// Recording the key would make later populations' merges omit the live
    /// row (PGC-261); the invalidations supersede the in-flight populations
    /// (generation bump), so nothing can resurrect the evicted stale version.
    async fn frame_cache_delete_unrecorded(
        &mut self,
        core: &mut WriterCore,
        relation_oid: u32,
        row_data: &[Option<ByteString>],
    ) -> CacheResult<()> {
        self.frame_cache_delete_inner(core, relation_oid, row_data, false)
            .await
    }

    async fn frame_cache_delete_inner(
        &mut self,
        core: &mut WriterCore,
        relation_oid: u32,
        row_data: &[Option<ByteString>],
        record_lost_key: bool,
    ) -> CacheResult<()> {
        core.frame_begin_ensure([relation_oid]);
        let table_metadata =
            core.cache
                .tables
                .get1(&relation_oid)
                .ok_or(CacheError::UnknownTable {
                    oid: Some(relation_oid),
                    name: None,
                })?;
        // Buffer the removed PK for any in-flight population over this relation
        // so its merge doesn't resurrect the row (PGC-250). Stamped with the
        // frame's commit LSN and recorded at CommitMark (the commit LSN isn't
        // known yet); dropped if the frame rolls back. Skip rendering the key
        // entirely when no population is recording this relation (the steady
        // state) — `record` would discard it anyway.
        let deleted_key =
            if record_lost_key && core.population_deleted_keys.is_recording(relation_oid) {
                pk_body_render(table_metadata, row_data)
            } else {
                None
            };
        // Track batch-deleted PKs so a later batched frame's row-change
        // classification sees the deletion the pre-batch snapshot can't
        // (PGC-242). Re-rendered when not already rendered for PGC-250.
        if let Some(key) = deleted_key
            .clone()
            .or_else(|| pk_body_render(table_metadata, row_data))
        {
            core.batch_deleted_pks.insert((relation_oid, key));
        }
        self.cache_delete_into(&mut core.frame_buf, table_metadata, row_data)?;
        if let Some(key) = deleted_key {
            core.frame_deleted_keys.push((relation_oid, key));
        }
        self.frame_write_finish(core).await
    }

    /// Handle INSERT operation.
    // Trace level: at info/debug the fmt layer allocates per-span extensions,
    // which would put a heap allocation on every CDC event.
    #[instrument(skip_all, level = "trace")]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn handle_insert(
        &mut self,
        core: &mut WriterCore,
        relation_oid: u32,
        row_data: &[Option<ByteString>],
        batch: Option<BatchEvalView<'_>>,
    ) -> CacheResult<()> {
        let start = Instant::now();
        crate::metrics::handles().cdc.handle_inserts.increment(1);

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
                row_data,
                None,
                CdcOperation::Upsert,
            )
            .attach_loc("checking for query invalidations")?;

        // Defer the actual invalidation to just before the frame COMMIT
        // (frame_invalidations_flush) so it is atomic with the maintenance
        // it accompanies rather than visible mid-frame.
        core.frame_invalidations.extend(fp_list);

        let matched = self
            .update_queries_execute_batch(core, relation_oid, row_data, batch)
            .await?;

        // The inserted row is alive at origin: cancel any tracked deletion of
        // its key so population merges don't omit it (PGC-260). When the key
        // was tracked but no query matched (nothing upserted), write the row
        // anyway — its presence in the shared table is what makes the
        // cancellation safe in every merge interleaving (merges never
        // overwrite, so neither an old-snapshot nor a new-snapshot population
        // can regress it).
        let tracked = core.population_deleted_key_cancel(relation_oid, row_data);
        if tracked && !matched {
            self.frame_cache_upsert(core, relation_oid, row_data)
                .await?;
        }

        crate::metrics::handles()
            .cdc
            .handle_insert_seconds
            .record(start.elapsed().as_secs_f64());
        Ok(())
    }

    /// Handle UPDATE operation.
    // Trace level: at info/debug the fmt layer allocates per-span extensions,
    // which would put a heap allocation on every CDC event.
    #[instrument(skip_all, level = "trace")]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn handle_update(
        &mut self,
        core: &mut WriterCore,
        relation_oid: u32,
        key_data: &[Option<ByteString>],
        new_row_data: &[Option<ByteString>],
        batch: Option<BatchEvalView<'_>>,
    ) -> CacheResult<()> {
        let start = Instant::now();
        crate::metrics::handles().cdc.handle_updates.increment(1);

        // See handle_insert: an untracked relation's CDC is a benign skip.
        if !core.cache.tables.contains_key1(&relation_oid) {
            return Ok(());
        }

        // PGC-227: when no cached query over this relation can have its UPDATE
        // invalidation depend on which columns changed or whether the row is
        // cached, both `query_row_changes` (a SELECT round-trip) and the
        // invalidation check are provably no-ops — skip them.
        let needs_change_eval = core
            .cache
            .update_queries
            .get(&relation_oid)
            .is_some_and(|q| q.needs_change_eval());

        if needs_change_eval {
            // The batch deleted this PK after the pre-batch snapshot that the
            // row-change lookups read: classify it UNCACHED so the
            // entering-invalidation the per-frame flow produced still fires
            // (PGC-242; lost otherwise on cross-frame delete/PK-flip + update).
            let batch_deleted = !core.batch_deleted_pks.is_empty()
                && core
                    .cache
                    .tables
                    .get1(&relation_oid)
                    .and_then(|table_metadata| pk_body_render(table_metadata, new_row_data))
                    .is_some_and(|key| core.batch_deleted_pks.contains(&(relation_oid, key)));
            // Batched row-change result if the segment eval covered this event
            // (PGC-241 stage 3), else the per-row SELECT.
            let row_changes_fallback;
            let row_changes: Option<&HashMap<EcoString, bool>> = if batch_deleted {
                None
            } else {
                match batch.as_ref().and_then(|view| view.row_change) {
                    Some(batched) => batched,
                    None => {
                        row_changes_fallback = self
                            .query_row_changes(core, relation_oid, new_row_data)
                            .await?;
                        row_changes_fallback.as_ref()
                    }
                }
            };
            trace!("row_changes {:?}", row_changes);

            let fp_list = self.update_queries_check_invalidate(
                core,
                relation_oid,
                row_changes,
                new_row_data,
                Some(key_data),
                CdcOperation::Upsert,
            )?;
            trace!("invalidation_count {}", fp_list.len());
            // Deferred to frame_invalidations_flush (see handle_insert).
            core.frame_invalidations.extend(fp_list);
        }

        let matched = self
            .update_queries_execute_batch(core, relation_oid, new_row_data, batch)
            .await?;

        if matched {
            // The upserted row supersedes any tracked deletion of its key —
            // including a previously-deleted PK this row's new PK reuses
            // (PGC-260).
            core.population_deleted_key_cancel(relation_oid, new_row_data);
        } else {
            // Update-out: the row left every live predicate, but it is still
            // alive at origin. While populations are in flight, deleting it
            // and recording its key would make a later population's merge omit
            // a live row (PGC-261) — instead upsert the new version: serving
            // re-evaluates predicates so nothing serves it, an old-snapshot
            // merge can't resurrect the old version (merges never overwrite),
            // and a later population finds it present. With no population in
            // flight, keep the delete (shared-table leanness; the key record
            // would be discarded anyway).
            if core.population_deleted_keys.is_recording(relation_oid) {
                core.population_deleted_key_cancel(relation_oid, new_row_data);
                self.frame_cache_upsert(core, relation_oid, new_row_data)
                    .await?;
            } else {
                self.frame_cache_delete(core, relation_oid, new_row_data)
                    .await?;
            }
        }

        // Any update may move the row out of a Fresh MV's predicate, and only
        // membership *hits* dirty-mark — a row leaving query A while still
        // matching query B (`matched` above), or a PK-change with other
        // columns changed, would otherwise leave A's MV serving the departed
        // row forever (PGC-254/PGC-265; the old image isn't available to
        // detect departure precisely — PGC-255 tracks precision). Coarsely
        // dirty the relation's dirtyable MVs; `mv_dirty_mark` self-gates
        // (Fresh and Building only).
        core.mv_dirty_mark_relation(relation_oid);

        // A non-empty `key_data` means the PK changed; delete the old PK too.
        if !key_data.is_empty() {
            self.frame_cache_delete(core, relation_oid, key_data)
                .await?;
        }

        crate::metrics::handles()
            .cdc
            .handle_update_seconds
            .record(start.elapsed().as_secs_f64());
        Ok(())
    }

    /// Whether a query must be invalidated on the toast-fallback path without
    /// (or regardless of) membership evaluation (PGC-264). The row may or may
    /// not be cached and its column changes are unknowable, so this folds both
    /// `row_cached_invalidation_check` (changes assumed) and
    /// `row_uncached_invalidation_check` (Upsert) into their conservative
    /// union. Queries passing this still get membership-evaluated by the
    /// caller — a match invalidates too, since the incomplete image can't be
    /// upserted.
    fn toast_fallback_structural_invalidate(
        update_query: &UpdateQuery,
        table_metadata: &TableMetadata,
        new_row_data: &[Option<ByteString>],
        key_data: &[Option<ByteString>],
        toasted_columns: &[EcoString],
    ) -> bool {
        // Constraint/predicate evaluation reads an elided column → nothing
        // below (nor the caller's membership eval) can be trusted.
        if update_query
            .predicate_columns
            .iter()
            .any(|c| toasted_columns.contains(c))
        {
            return true;
        }
        match update_query.source {
            // Always invalidated on a cached UPDATE (row_cached_invalidation_check).
            UpdateQuerySource::Subquery(_) | UpdateQuerySource::OuterJoinOptional => true,
            UpdateQuerySource::FromClause | UpdateQuerySource::OuterJoinTerminal => {
                // Window-boundary columns may have changed; the replacement
                // row is by definition uncached (PGC-94).
                if update_query.has_limit {
                    return true;
                }
                // Single-table: membership eval alone decides (the eval is
                // trustworthy past the predicate_columns gate above).
                if update_query.resolved.is_single_table() {
                    return false;
                }
                // Multi-table: a join-column change can create join matches
                // the cache tables can't see, so membership eval saying
                // "no match" doesn't rule out growth. Mirror
                // row_uncached_invalidation_check's relevance tests.
                if update_query
                    .constraints
                    .table_constraints
                    .contains_key(table_metadata.name.as_str())
                {
                    row_constraints_match(&update_query.constraints, table_metadata, new_row_data)
                } else {
                    !join_membership_unchanged(update_query, table_metadata, Some(key_data))
                }
            }
        }
    }

    /// Conservative decide-pass path for an UPDATE whose unchanged-toast
    /// columns could not be repaired (PGC-264). The image is incomplete: it
    /// must never reach the shared cache table, and predicates over the elided
    /// columns can't be evaluated.
    ///
    /// With no population recording the relation: invalidate every query that
    /// might be affected (structural sensitivity, or membership match — the
    /// matched row can't be upserted); provably unaffected queries need
    /// nothing beyond the row's eviction.
    ///
    /// With recording active, the PGC-261 hazard applies: the row is alive at
    /// origin, so deleting it with a lost-key record makes later populations'
    /// merges omit it — including merges for queries registered *after* this
    /// event, which no invalidation here can cover. Instead invalidate every
    /// query over the relation (superseding their in-flight populations via
    /// the generation bump) and evict the stale row without a record; later
    /// populations snapshot post-update origin and merge cleanly.
    // Trace level: at info/debug the fmt layer allocates per-span extensions,
    // which would put a heap allocation on every CDC event.
    #[instrument(skip_all, level = "trace")]
    async fn handle_update_toast_fallback(
        &mut self,
        core: &mut WriterCore,
        relation_oid: u32,
        key_data: &[Option<ByteString>],
        new_row_data: &[Option<ByteString>],
        toasted_columns: &[EcoString],
    ) -> CacheResult<()> {
        // See handle_insert: an untracked relation's CDC is a benign skip.
        if !core.cache.tables.contains_key1(&relation_oid) {
            return Ok(());
        }

        let recording = core.population_deleted_keys.is_recording(relation_oid);
        let mut fp_list: Vec<Fingerprint> = Vec::new();
        if let (Some(update_queries), Some(table_metadata)) = (
            core.cache.update_queries.get(&relation_oid),
            core.cache.tables.get1(&relation_oid),
        ) {
            if recording {
                fp_list.extend(
                    update_queries
                        .iter_complexity_ordered()
                        .map(|q| q.fingerprint)
                        .filter(|fp| !core.frame_invalidations.contains(fp)),
                );
            } else {
                let mut pg_eval: Vec<&UpdateQuery> = Vec::new();
                for update_query in update_queries.iter_complexity_ordered() {
                    if core.frame_invalidations.contains(&update_query.fingerprint) {
                        continue;
                    }
                    if Self::toast_fallback_structural_invalidate(
                        update_query,
                        table_metadata,
                        new_row_data,
                        key_data,
                        toasted_columns,
                    ) {
                        fp_list.push(update_query.fingerprint);
                        continue;
                    }
                    match update_query.eval_strategy {
                        UpdateEvalStrategy::LocalEval => {
                            if update_query_matches_locally(
                                update_query,
                                table_metadata,
                                new_row_data,
                            ) {
                                fp_list.push(update_query.fingerprint);
                            }
                        }
                        UpdateEvalStrategy::PgEval => pg_eval.push(update_query),
                    }
                }
                if !pg_eval.is_empty() {
                    let matched = self
                        .pg_eval_matches(&pg_eval, table_metadata, new_row_data)
                        .await
                        .attach_loc("toast fallback membership eval")?;
                    fp_list.extend(matched);
                }
            }
        }
        trace!(
            relation_oid,
            recording,
            invalidations = fp_list.len(),
            "toast fallback handled"
        );
        // Deferred to frame_invalidations_flush (see handle_insert).
        core.frame_invalidations.extend(fp_list);

        // Same coarse Fresh-MV rule as handle_update (PGC-254).
        core.mv_dirty_mark_relation(relation_oid);

        // The new-PK row is alive at origin: under recording its eviction must
        // not be recorded (see doc comment). The old PK after a PK change is
        // genuinely dead at origin, so that delete records normally.
        if recording {
            self.frame_cache_delete_unrecorded(core, relation_oid, new_row_data)
                .await?;
        } else {
            self.frame_cache_delete(core, relation_oid, new_row_data)
                .await?;
        }
        if !key_data.is_empty() {
            self.frame_cache_delete(core, relation_oid, key_data)
                .await?;
        }
        Ok(())
    }

    /// Handle DELETE operation.
    ///
    /// Deletes the row from cache tables and checks for subquery invalidations.
    /// For Exclusion subquery tables (NOT IN, NOT EXISTS), a DELETE shrinks the
    /// exclusion set, which grows the outer result set — requiring invalidation.
    // Trace level: at info/debug the fmt layer allocates per-span extensions,
    // which would put a heap allocation on every CDC event.
    #[instrument(skip_all, level = "trace")]
    pub async fn handle_delete(
        &mut self,
        core: &mut WriterCore,
        relation_oid: u32,
        row_data: &[Option<ByteString>],
    ) -> CacheResult<()> {
        let start = Instant::now();
        crate::metrics::handles().cdc.handle_deletes.increment(1);

        if !core.cache.tables.contains_key1(&relation_oid) {
            error!("No table metadata found for relation_oid: {}", relation_oid);
            crate::metrics::handles()
                .cdc
                .handle_delete_seconds
                .record(start.elapsed().as_secs_f64());
            return Ok(());
        }

        // Buffer the delete for the frame flush (PGC-228).
        self.frame_cache_delete(core, relation_oid, row_data)
            .await?;

        // A deleted row leaves stale rows in any Fresh MV that materialized it;
        // CDC removals never went through the upsert path's dirty-mark, so the
        // MV would serve the deleted row forever. Coarsely dirty the relation's
        // Fresh MVs (PGC-254 rung 1 — the delete tuple lacks the non-PK columns
        // needed to identify which MVs actually contained the row).
        core.mv_dirty_mark_relation(relation_oid);

        // Check for subquery invalidations — removing a row can expand the
        // final result set for Exclusion/Scalar subquery tables
        if core.cache.update_queries.contains_key(&relation_oid) {
            let fp_list = self
                .update_queries_check_invalidate(
                    core,
                    relation_oid,
                    None,
                    row_data,
                    None,
                    CdcOperation::Delete,
                )
                .attach_loc("checking delete invalidations")?;

            // Deferred to frame_invalidations_flush (see handle_insert).
            core.frame_invalidations.extend(fp_list);
        }

        crate::metrics::handles()
            .cdc
            .handle_delete_seconds
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
            core.frame_begin_ensure(relation_oids.iter().copied());
            core.frame_buf.push_str(&sql);
            self.frame_write_finish(core).await?;
        }

        for oid in relation_oids {
            core.cache_table_invalidate(*oid)
                .await
                .attach_loc("invalidating queries on truncate")?;
            // A population reading this relation with a pre-truncate snapshot
            // would resurrect truncated rows on merge. Raise its abort watermark
            // to the truncate's commit LSN at CommitMark (PGC-250).
            core.frame_truncated_relations.push(*oid);
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
        fingerprint: Fingerprint,
    ) -> CacheResult<()> {
        // Pinned queries: defer readmission to the writer event loop. Still
        // drain parked waiters — the readmit's Ready can itself be superseded
        // under churn (waiting on it risks the same hang as the unpinned path).
        if core
            .cache
            .cached_queries
            .get1(&fingerprint)
            .is_some_and(|q| q.pinned)
        {
            debug!("pinned query invalidated, deferring readmit {fingerprint}");
            let _ = core.query_tx.send(QueryCommand::Readmit { fingerprint });
            core.waiters_fail(fingerprint);
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
        // the same get_mut block so dispatches observe both transitions
        // atomically — a reader that sees state=Invalidated never sees the MV
        // in a stale-Fresh state.
        if let Some(mut entry) = core.state_view.cached_queries.get_mut(&fingerprint) {
            entry.state = CachedQueryState::Invalidated;
            entry.referenced = false;
            if let Some(dirtied) = entry.mv.state.dirtied() {
                entry.mv.state = dirtied;
            }
        }

        // Drain coalesced waiters parked on this query's now-dead population.
        core.waiters_fail(fingerprint);

        // Purge stale rows if generation threshold moved
        let new_threshold = core.cache.generation_purge_threshold();
        if new_threshold > prev_generation_threshold {
            core.cache.current_size = core.cache_size_load().await?;
            let disk_limit = core.disk_limit_compute(cfg.cache_size);
            if disk_limit.is_some_and(|s| core.cache.current_size > s) {
                core.generation_purge(new_threshold).await?;
                core.cache.current_size = core.cache_size_load().await?;
            }
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
        row_data: &[Option<ByteString>],
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
        row_data: &[Option<ByteString>],
    ) -> bool {
        row_constraints_match(constraints, table_metadata, row_data)
    }

    /// Determine if a query should be invalidated when the row is not currently cached.
    /// Returns true if the query should be invalidated.
    fn row_uncached_invalidation_check(
        &self,
        update_query: &UpdateQuery,
        table_metadata: &TableMetadata,
        row_data: &[Option<ByteString>],
        key_data: Option<&[Option<ByteString>]>,
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
        row_data: &[Option<ByteString>],
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

        // LIMIT windowing: an UPDATE that changes a column defining this
        // query's window boundary (ORDER BY / WHERE / HAVING) may push the
        // cached row out of the window — and the untracked row that should
        // take its place is, by definition, not in the cache. Invalidate
        // to force repopulation. PGC-94.
        if update_query.has_limit && matches!(update_query.source, UpdateQuerySource::FromClause) {
            for column in &update_query.limit_window_columns {
                if *row_changes.get(column.as_str()).unwrap_or(&false) {
                    return true;
                }
            }
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
        row_data: &[Option<ByteString>],
        key_data: Option<&[Option<ByteString>]>,
        operation: CdcOperation,
    ) -> CacheResult<Vec<Fingerprint>> {
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
            // `Some` → row is cached (UPDATE main path); `None` → row not cached
            // (INSERT, DELETE, or UPDATE of an uncached row).
            let invalidate = match row_changes {
                Some(row_changes) => self.row_cached_invalidation_check(
                    update_query,
                    table_metadata,
                    row_data,
                    row_changes,
                ),
                None => self.row_uncached_invalidation_check(
                    update_query,
                    table_metadata,
                    row_data,
                    key_data,
                    operation,
                ),
            };
            if invalidate {
                // Drift guard (PGC-227): on the UPDATE path (`Upsert`), a query
                // that invalidates here MUST be `change_dependent`, or
                // `handle_update` would have skipped this check and served
                // stale. `update_invalidation_possible` is the single source of
                // truth; this fails the moment a check branch diverges from it.
                // DELETE has its own invalidation branches that fire
                // independently of the flag, so scope the guard to `Upsert`.
                debug_assert!(
                    operation != CdcOperation::Upsert || update_query.change_dependent,
                    "invalidation fired for a non-change_dependent query on the UPDATE \
                     path: update_invalidation_possible is out of sync with \
                     row_*_invalidation_check"
                );
                fp_list.push(update_query.fingerprint);
            }
        }

        Ok(fp_list)
    }

    /// Decide whether a CDC row belongs in cache and, if so, upsert it once.
    ///
    /// Phase A (read-only): determine which cached queries the row matches.
    /// LocalEval queries are evaluated in Rust; PgEval queries are batched into
    /// combined `SELECT EXISTS (p1), …` round-trips on one autocommit pool
    /// connection, so they observe the pre-source-transaction snapshot, never
    /// the in-flight frame's uncommitted writes.
    ///
    /// Phase B (in-frame): if any query matched, a single unconditional upsert
    /// into the source table's cache table on `cdc_write_conn`. The shared
    /// cache table holds the row iff some cached query needs it, so one upsert
    /// suffices regardless of how many matched.
    ///
    /// Returns true if the row matched any cached query.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) async fn update_queries_execute_batch(
        &mut self,
        core: &mut WriterCore,
        relation_oid: u32,
        row_data: &[Option<ByteString>],
        batch: Option<BatchEvalView<'_>>,
    ) -> CacheResult<bool> {
        // Phase A: membership evaluation. The `core` borrow is confined to
        // this block so the in-frame write below doesn't hold it.
        let matched = {
            // No cached query references this relation → nothing to upsert.
            // Not an error (the relation simply isn't maintained in place).
            let Some(update_queries) = core.cache.update_queries.get(&relation_oid) else {
                return Ok(false);
            };
            let Some(table_metadata) = core.cache.tables.get1(&relation_oid) else {
                return Ok(false);
            };

            let total_queries = update_queries.queries.len();
            trace!("update_queries_execute_batch start [{total_queries}]");
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
                crate::metrics::handles().cdc.local_eval_hits.increment(1);
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
                // Batch-covered queries consult the precomputed segment matrix
                // (PGC-241) — no round-trip. `frame_invalidations` was already
                // applied above (the matrix is built unfiltered); the Fresh-MV
                // dirty-mark self-gates, mirroring the per-row fresh/rest split.
                let (batched, fallback): (Vec<&UpdateQuery>, Vec<&UpdateQuery>) = match &batch {
                    Some(view) => pg_eval
                        .into_iter()
                        .partition(|q| view.covers(q.fingerprint)),
                    None => (Vec::new(), pg_eval),
                };
                if let Some(view) = &batch {
                    for update_query in &batched {
                        if view.hit(update_query.fingerprint) {
                            trace!(
                                "update_queries batched pg-eval matched fingerprint {}",
                                update_query.fingerprint
                            );
                            core.mv_dirty_mark(update_query.fingerprint);
                            matched = true;
                        }
                    }
                }

                // Per-row fallback for non-batchable shapes / uncovered rows.
                let (fresh_pg, rest_pg): (Vec<&UpdateQuery>, Vec<&UpdateQuery>) = fallback
                    .into_iter()
                    .partition(|q| core.mv_dirty_eval_required(q.fingerprint));

                let fresh_hits = self
                    .pg_eval_matches(&fresh_pg, table_metadata, row_data)
                    .await?;
                for &fingerprint in &fresh_hits {
                    core.mv_dirty_mark(fingerprint);
                }
                let mut pg_hit = !fresh_hits.is_empty();
                matched |= pg_hit;

                // Non-Fresh queries only decide the upsert: skip them entirely
                // once anything matched, else stop at the first match.
                if !matched && self.pg_eval_any(&rest_pg, table_metadata, row_data).await? {
                    matched = true;
                    pg_hit = true;
                }

                if pg_hit {
                    crate::metrics::handles().cdc.pg_eval_hits.increment(1);
                }
            }

            matched
        };

        // Phase B: single in-frame write, buffered for the frame flush (PGC-228).
        if matched {
            self.frame_cache_upsert(core, relation_oid, row_data)
                .await?;
        }

        Ok(matched)
    }

    /// Evaluate each query's membership predicate against the CDC row and return
    /// the fingerprints that matched. Predicates are combined into a single
    /// `SELECT EXISTS (p1), EXISTS (p2), …` per `PG_EVAL_CHUNK`-sized chunk — one
    /// round-trip and one boolean column per query — instead of a `simple_query`
    /// per query. Every query is evaluated (no short-circuit) so each match is
    /// reported; callers that need per-query identity (Fresh-MV dirty-marking)
    /// use this. Use `pg_eval_any` when only "did anything match" is needed.
    async fn pg_eval_matches(
        &mut self,
        queries: &[&UpdateQuery],
        table_metadata: &TableMetadata,
        row_data: &[Option<ByteString>],
    ) -> CacheResult<Vec<Fingerprint>> {
        if queries.is_empty() {
            return Ok(Vec::new());
        }
        let mut hits = Vec::new();
        for chunk in queries.chunks(PG_EVAL_CHUNK) {
            self.pg_eval_buf.clear();
            self.pg_eval_buf.push_str("SELECT ");
            for (i, uq) in chunk.iter().enumerate() {
                if i > 0 {
                    self.pg_eval_buf.push_str(", ");
                }
                self.pg_eval_buf.push_str("EXISTS (");
                Self::cache_predicate_into(
                    &mut self.pg_eval_buf,
                    &uq.resolved,
                    table_metadata,
                    row_data,
                )?;
                self.pg_eval_buf.push(')');
            }
            let Some(row) =
                Self::pg_eval_chunk_row(&self.cache_eval_conn, &self.pg_eval_buf).await?
            else {
                continue;
            };
            // One boolean column per query; column `i` ↔ `chunk[i]`.
            for (i, uq) in chunk.iter().enumerate() {
                if row.get(i) == Some("t") {
                    trace!(
                        "update_queries pg-eval matched fingerprint {}",
                        uq.fingerprint
                    );
                    hits.push(uq.fingerprint);
                }
            }
        }
        Ok(hits)
    }

    /// Whether the CDC row matches *any* of `queries` — for the membership-only
    /// (non-`Fresh`) set, where one match is enough to trigger the shared-table
    /// upsert and individual fingerprints are never needed. Predicates are
    /// OR-combined per `PG_EVAL_CHUNK`-sized chunk so Postgres short-circuits the
    /// chain server-side, and evaluation stops at the first chunk that hits.
    async fn pg_eval_any(
        &mut self,
        queries: &[&UpdateQuery],
        table_metadata: &TableMetadata,
        row_data: &[Option<ByteString>],
    ) -> CacheResult<bool> {
        if queries.is_empty() {
            return Ok(false);
        }
        for chunk in queries.chunks(PG_EVAL_CHUNK) {
            self.pg_eval_buf.clear();
            self.pg_eval_buf.push_str("SELECT ");
            for (i, uq) in chunk.iter().enumerate() {
                if i > 0 {
                    self.pg_eval_buf.push_str(" OR ");
                }
                self.pg_eval_buf.push_str("EXISTS (");
                Self::cache_predicate_into(
                    &mut self.pg_eval_buf,
                    &uq.resolved,
                    table_metadata,
                    row_data,
                )?;
                self.pg_eval_buf.push(')');
            }
            let Some(row) =
                Self::pg_eval_chunk_row(&self.cache_eval_conn, &self.pg_eval_buf).await?
            else {
                continue;
            };
            if row.get(0) == Some("t") {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Run one combined predicate `SELECT` and return its single result row, or
    /// `None` if the result carried no row (impossible for a well-formed
    /// `SELECT EXISTS (...)`, treated as no-match). Shared by `pg_eval_matches`
    /// and `pg_eval_any`.
    async fn pg_eval_chunk_row(conn: &Client, sql: &str) -> CacheResult<Option<SimpleQueryRow>> {
        let msgs = match conn.simple_query(sql).await {
            Ok(m) => m,
            Err(e) => {
                error!("predicate eval error: {}", error_chain_format(&e));
                return Err(CacheError::PgError(e).into());
            }
        };
        Ok(msgs.into_iter().find_map(|m| {
            if let SimpleQueryMessage::Row(row) = m {
                Some(row)
            } else {
                None
            }
        }))
    }

    /// Append one cached query's membership predicate — the inner SELECT of a
    /// `SELECT EXISTS (...)`, with the CDC row's values substituted for the
    /// changed table — into `buf`. Read-only; evaluated against the
    /// pre-transaction snapshot. Caller wraps it in `EXISTS (...)`.
    pub(super) fn cache_predicate_into(
        buf: &mut String,
        resolved: &ResolvedQueryExpr,
        table_metadata: &TableMetadata,
        row_data: &[Option<ByteString>],
    ) -> CacheResult<()> {
        let resolved_select = resolved.as_select().ok_or(CacheError::InvalidQuery)?;
        let value_select = resolved_select_node_table_replace_with_values(
            resolved_select,
            table_metadata,
            row_data,
        )
        .map_err(|e| e.context_transform(CacheError::from))?;
        Deparse::deparse(&value_select, buf);
        Ok(())
    }

    /// Build an unconditional UPSERT for the row — `INSERT ... ON CONFLICT DO UPDATE`
    /// with no WHERE predicate. Used by the LocalEval fast path once the Rust
    /// evaluator has already decided the row belongs in cache.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    /// Append an unconditional upsert for `row_data` into `buf` (PGC-228:
    /// builders write into the reused frame buffer instead of allocating a
    /// per-statement `String`).
    pub(super) fn cache_upsert_unconditional_into(
        &self,
        buf: &mut String,
        table_metadata: &TableMetadata,
        row_data: &[Option<ByteString>],
    ) {
        // Columns with a value in `row_data` are emitted in three passes
        // (names, values, conflict tail) over the position-sorted column
        // store, writing straight into `buf` — no per-event Vec or String.
        let schema = &table_metadata.schema;
        let table = &table_metadata.name;

        let _ = write!(buf, "INSERT INTO {schema}.{table} (");
        let mut first = true;
        for column_meta in &table_metadata.columns {
            if row_data.get(column_meta.index()).is_none() {
                continue;
            }
            if !first {
                buf.push_str(", ");
            }
            buf.push_str(column_meta.name.as_str());
            first = false;
        }
        buf.push_str(") VALUES (");
        let mut first = true;
        for column_meta in &table_metadata.columns {
            let Some(row_value) = row_data.get(column_meta.index()) else {
                continue;
            };
            if !first {
                buf.push_str(", ");
            }
            match row_value.as_deref() {
                Some(value) => {
                    let _ = escape::escape_literal_into(value, buf);
                }
                None => buf.push_str("NULL"),
            }
            first = false;
        }
        buf.push_str(") ON CONFLICT (");
        for (i, pk) in table_metadata.primary_key_columns.iter().enumerate() {
            if i > 0 {
                buf.push_str(", ");
            }
            buf.push_str(pk);
        }
        buf.push(')');
        cdc_on_conflict_tail_append(buf, table_metadata, row_data);
    }

    // Trace level: at info/debug the fmt layer allocates per-span extensions,
    // which would put a heap allocation on every CDC event.
    #[instrument(skip_all, level = "trace")]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    /// Append a PK-qualified delete for `row_data` into `buf` (PGC-228).
    pub(super) fn cache_delete_into(
        &self,
        buf: &mut String,
        table_metadata: &TableMetadata,
        row_data: &[Option<ByteString>],
    ) -> CacheResult<()> {
        let _ = write!(
            buf,
            "DELETE FROM {}.{} WHERE ",
            table_metadata.schema, table_metadata.name
        );

        let mut has_pk = false;
        for pk_column in &table_metadata.primary_key_columns {
            if let Some(column_meta) = table_metadata.columns.get(pk_column.as_str()) {
                let position = column_meta.index();
                if let Some(row_value) = row_data.get(position) {
                    if has_pk {
                        buf.push_str(" AND ");
                    }
                    let _ = write!(buf, "{pk_column} = ");
                    match row_value.as_deref() {
                        Some(value) => {
                            let _ = escape::escape_literal_into(value, buf);
                        }
                        None => buf.push_str("NULL"),
                    }
                    has_pk = true;
                }
            }
        }

        if !has_pk {
            error!("Cannot build DELETE WHERE clause: no primary key values found");
            return Err(CacheError::NoPrimaryKey.into());
        }

        Ok(())
    }
}

impl WriterCore {
    /// Invalidate all cached queries that reference a table.
    pub(super) async fn cache_table_invalidate(&mut self, relation_oid: u32) -> CacheResult<()> {
        let fingerprints: Vec<Fingerprint> = self
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
    pub(super) async fn cache_query_evict(&mut self, fingerprint: Fingerprint) -> CacheResult<()> {
        let Some(query) = self.cache.cached_queries.remove1(&fingerprint) else {
            trace!(fingerprint = %fingerprint, "cache_query_evict: not found, skipping");
            return Ok(());
        };

        debug!(
            fingerprint = %fingerprint,
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

        // Drain coalesced waiters parked on the now-removed query (eviction can
        // remove a Loading query whose waiters would otherwise never be drained).
        self.waiters_fail(fingerprint);

        self.cache
            .update_queries_remove_fingerprint(fingerprint, &query.relation_oids);

        // Purge generations based on new threshold
        let new_threshold = self.cache.generation_purge_threshold();
        if new_threshold > prev_generation_threshold {
            self.cache.current_size = self.cache_size_load().await?;
            let disk_limit = self.disk_limit_compute(self.cache.dynamic.load().cache_size);
            if disk_limit.is_some_and(|s| self.cache.current_size > s) {
                self.generation_purge(new_threshold).await?;
                self.cache.current_size = self.cache_size_load().await?;
            }
        }

        Ok(())
    }
}

/// Append the tail of an upsert SQL: either ` DO UPDATE SET <non-pk cols>` or
/// ` DO NOTHING` if the table has no non-PK columns. PG rejects `DO UPDATE SET`
/// with an empty SET list, so PK-only tables must use `DO NOTHING`.
///
/// Assumes the caller has already emitted `INSERT INTO ... ON CONFLICT (<pk>)`.
fn cdc_on_conflict_tail_append(
    sql: &mut String,
    table_metadata: &TableMetadata,
    row_data: &[Option<ByteString>],
) {
    let is_pk = |name: &str| {
        table_metadata
            .primary_key_columns
            .iter()
            .any(|pk| pk.as_str() == name)
    };
    let mut first = true;
    for column_meta in &table_metadata.columns {
        if row_data.get(column_meta.index()).is_none() || is_pk(column_meta.name.as_str()) {
            continue;
        }
        if first {
            sql.push_str(" DO UPDATE SET ");
        } else {
            sql.push_str(", ");
        }
        let col = column_meta.name.as_str();
        let _ = write!(sql, "{col} = EXCLUDED.{col}");
        first = false;
    }
    if first {
        sql.push_str(" DO NOTHING");
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
    row_data: &[Option<ByteString>],
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
    key_data: Option<&[Option<ByteString>]>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnMetadata, ColumnStore};
    use crate::query::ast::LiteralValue;
    use crate::query::cast::CastTarget;
    use tokio_postgres::types::Type;

    // Row layout: [id INT4 (PK), name TEXT, created_at TIMESTAMP].
    fn fixture_table() -> TableMetadata {
        let columns = ColumnStore::new([
            ColumnMetadata {
                name: "id".into(),
                position: 1,
                type_oid: 23,
                data_type: Type::INT4,
                type_name: "int4".into(),
                cache_type_name: "int4".into(),
                is_primary_key: true,
            },
            ColumnMetadata {
                name: "name".into(),
                position: 2,
                type_oid: 25,
                data_type: Type::TEXT,
                type_name: "text".into(),
                cache_type_name: "text".into(),
                is_primary_key: false,
            },
            ColumnMetadata {
                name: "created_at".into(),
                position: 3,
                type_oid: 1114,
                data_type: Type::TIMESTAMP,
                type_name: "timestamp".into(),
                cache_type_name: "timestamp".into(),
                is_primary_key: false,
            },
        ]);
        TableMetadata {
            relation_oid: 1001,
            name: "users".into(),
            schema: "public".into(),
            primary_key_columns: vec!["id".into()],
            columns,
            indexes: Vec::new(),
        }
    }

    fn constraints_for(table: &str, tcs: Vec<TableConstraint>) -> QueryConstraints {
        let mut q = QueryConstraints::default();
        q.table_constraints.insert(EcoString::from(table), tcs);
        q
    }

    #[test]
    fn no_constraints_for_table_matches() {
        let table = fixture_table();
        let constraints = QueryConstraints::default();
        let row = vec![Some("1".into()), Some("alice".into()), None];
        assert!(row_constraints_match(&constraints, &table, &row));
    }

    #[test]
    fn bare_comparison_matches_when_value_equal() {
        let table = fixture_table();
        let constraints = constraints_for(
            "users",
            vec![TableConstraint::Comparison(
                "id".into(),
                BinaryOp::Equal,
                LiteralValue::Integer(1),
            )],
        );
        let row = vec![Some("1".into()), Some("alice".into()), None];
        assert!(row_constraints_match(&constraints, &table, &row));
    }

    #[test]
    fn bare_comparison_misses_when_value_differs() {
        let table = fixture_table();
        let constraints = constraints_for(
            "users",
            vec![TableConstraint::Comparison(
                "id".into(),
                BinaryOp::Equal,
                LiteralValue::Integer(1),
            )],
        );
        let row = vec![Some("2".into()), Some("alice".into()), None];
        assert!(!row_constraints_match(&constraints, &table, &row));
    }

    // PGC-182: CastComparison constraints must coerce the row's wire-text via
    // the cast target before comparing. These tests exercise the new arm.

    #[test]
    fn cast_comparison_int4_matches_when_coerced_value_equal() {
        let table = fixture_table();
        let constraints = constraints_for(
            "users",
            vec![TableConstraint::CastComparison(
                "name".into(),
                CastTarget::Int4,
                BinaryOp::Equal,
                LiteralValue::Integer(42),
            )],
        );
        // name="42" coerces to Integer(42) → matches literal Integer(42).
        let row = vec![Some("1".into()), Some("42".into()), None];
        assert!(row_constraints_match(&constraints, &table, &row));
    }

    #[test]
    fn cast_comparison_int4_misses_when_coerced_value_differs() {
        let table = fixture_table();
        let constraints = constraints_for(
            "users",
            vec![TableConstraint::CastComparison(
                "name".into(),
                CastTarget::Int4,
                BinaryOp::Equal,
                LiteralValue::Integer(42),
            )],
        );
        let row = vec![Some("1".into()), Some("99".into()), None];
        assert!(!row_constraints_match(&constraints, &table, &row));
    }

    #[test]
    fn cast_comparison_int4_misses_when_row_unparseable() {
        // `'abc'::int4` raises in postgres; locally we treat it as non-match.
        let table = fixture_table();
        let constraints = constraints_for(
            "users",
            vec![TableConstraint::CastComparison(
                "name".into(),
                CastTarget::Int4,
                BinaryOp::Equal,
                LiteralValue::Integer(42),
            )],
        );
        let row = vec![Some("1".into()), Some("abc".into()), None];
        assert!(!row_constraints_match(&constraints, &table, &row));
    }

    #[test]
    fn cast_comparison_bool_matches_via_pg_bool_spelling() {
        let table = fixture_table();
        let constraints = constraints_for(
            "users",
            vec![TableConstraint::CastComparison(
                "name".into(),
                CastTarget::Bool,
                BinaryOp::Equal,
                LiteralValue::Boolean(true),
            )],
        );
        let row = vec![Some("1".into()), Some("yes".into()), None];
        assert!(row_constraints_match(&constraints, &table, &row));
    }

    #[test]
    fn cast_comparison_date_matches_via_timestamp_prefix() {
        let table = fixture_table();
        let constraints = constraints_for(
            "users",
            vec![TableConstraint::CastComparison(
                "created_at".into(),
                CastTarget::Date,
                BinaryOp::Equal,
                LiteralValue::String("2024-01-15".into()),
            )],
        );
        let row = vec![
            Some("1".into()),
            Some("alice".into()),
            Some("2024-01-15 09:00:00".into()),
        ];
        assert!(row_constraints_match(&constraints, &table, &row));
    }

    #[test]
    fn cast_comparison_null_row_value_misses() {
        let table = fixture_table();
        let constraints = constraints_for(
            "users",
            vec![TableConstraint::CastComparison(
                "name".into(),
                CastTarget::Int4,
                BinaryOp::Equal,
                LiteralValue::Integer(42),
            )],
        );
        let row = vec![Some("1".into()), None, None];
        assert!(!row_constraints_match(&constraints, &table, &row));
    }

    #[test]
    fn cast_comparison_inequality_compares_numerically() {
        // Locks the PGC-186 op-flip fix on the CDC pre-filter path too:
        // `name::int4 > 100` matches when name="500".
        let table = fixture_table();
        let constraints = constraints_for(
            "users",
            vec![TableConstraint::CastComparison(
                "name".into(),
                CastTarget::Int4,
                BinaryOp::GreaterThan,
                LiteralValue::Integer(100),
            )],
        );
        let row = vec![Some("1".into()), Some("500".into()), None];
        assert!(row_constraints_match(&constraints, &table, &row));
    }
}
