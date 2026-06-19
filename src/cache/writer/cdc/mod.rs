use crate::catalog::Oid;
use crate::pg::Lsn;
use std::num::NonZeroUsize;

use lru::LruCache;
use tokio_postgres::{Client, Statement};
use tracing::warn;

use crate::settings::Settings;

use super::super::{CacheError, CacheResult, MapIntoReport};
use super::core::WriterCore;
use crate::pg;

mod dispatch;
mod frame;
mod invalidation;
mod segment_eval;
mod sql;
mod toast_repair;

pub(in crate::cache::writer::cdc) use invalidation::{
    MemoOp, eval_candidates, memo_frame_accumulate, update_query_matches_locally,
};
pub(in crate::cache::writer::cdc) use segment_eval::{
    BatchEvalView, PreparedEvalKey, SegmentMembership,
};

/// Default capacity for dynamically built SQL strings.
pub(super) const SQL_BUFFER_CAPACITY: usize = 1024;

/// Max membership predicates combined into one `pg_eval_matches` query, bounding
/// the combined query's parse/plan cost for relations with many PgEval queries.
pub(super) const PG_EVAL_CHUNK: usize = 32;

/// Max CDC rows per batched membership statement (PGC-241). With inlined
/// VALUES the per-statement SQL is ~`PG_EVAL_CHUNK × PG_EVAL_ROW_CHUNK ×
/// row_width`, so this bounds statement size the way `FRAME_BUF_CAPACITY`
/// bounds the frame buffer; round-trips collapse `K → ⌈K/64⌉` per query chunk.
pub(super) const PG_EVAL_ROW_CHUNK: usize = 64;

/// Prepared-eval statement cache bound (PGC-241 stage 4). Per-literal
/// registration can produce thousands of live fingerprints; the LRU keeps the
/// hot working set prepared and ages the rest out (which also closes them
/// server-side). If `cdc_prepared_misses` tracks executions, the working set
/// exceeds this and prepare-per-use is thrashing — raise it or gate on
/// second use (until shape-parameterized update queries, PGC-257, collapse
/// the cardinality).
// Compile-time evaluated: cannot panic at runtime.
pub(super) const PREPARED_EVAL_CACHE_CAPACITY: NonZeroUsize = match NonZeroUsize::new(512) {
    Some(capacity) => capacity,
    None => unreachable!(),
};

/// Max complete source frames accumulated per batch (PGC-242). The event cap
/// (`FRAME_ROWS_CAPACITY`) usually triggers first for fat frames; this bounds
/// the memo-bracket window and the 40P01 recovery blast radius for streams of
/// tiny frames.
pub(super) const BATCH_FRAMES_MAX: usize = 256;

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
    pub(super) last_applied_lsn: Lsn,
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
    prepared_row_change: LruCache<Oid, Statement>,
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
            last_applied_lsn: Lsn::from_raw(0),
            pg_eval_buf: String::with_capacity(SQL_BUFFER_CAPACITY),
            prepared_membership: LruCache::new(PREPARED_EVAL_CACHE_CAPACITY),
            prepared_row_change: LruCache::new(PREPARED_EVAL_CACHE_CAPACITY),
        })
    }
}
