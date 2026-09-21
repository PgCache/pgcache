//! Per-connection log of forwarded writes for read-after-write consistency
//! (PGC-124 / PGC-366).
//!
//! Strictly per-connection: a connection tracks only the writes *it* forwarded
//! to origin, so a subsequent cacheable read on the same connection can be
//! forwarded (never served stale) while those writes are in the
//! commit→CDC-apply window. No cross-connection guarantees — that is CDC's job.
//!
//! # Shape
//!
//! Writes aggregate per table, not per statement: thousands of writes to one
//! table collapse to O(1) state, so a bulk load never bloats memory or the
//! per-read gate. Each table carries its own **active** tier gathering new
//! (unstamped) writes plus a bounded queue of **stamped** tiers, each draining
//! as the CDC apply watermark passes its own commit-LSN bound. Bounds are per
//! table and per batch, so one table's clearance is never held back by
//! another's later bound, and a fresh write never gates an older,
//! already-applied one; only when the queue saturates (CDC lag spanning more
//! probe windows than [`WAITING_TIERS_MAX`]) does a new batch fold into the
//! newest tier under the later bound — old bounds stay anchored (ADR-051).
//!
//! The gate consulting this log ([`WriteLog::decide`]) forwards a read that
//! could be superseded by a pending write. A non-row-enumerable write makes its
//! table `opaque` (any read of it intersects); a row-enumerable INSERT keeps its
//! rows ([`InsertAggregate`]), a DELETE keeps its WHERE predicate, and an UPDATE
//! keeps its WHERE plus post-update image ([`UpdatePredicate`]) — so a read
//! provably disjoint from every one can still be served (PGC-369/381/382).
//!
//! Submodules: [`aggregate`] holds the per-table pending-write state and its
//! disjointness checks; [`tiers`] the per-table LSN tier queues; [`log`] the
//! [`WriteLog`] itself (record, stamp, purge, decide).

mod aggregate;
mod log;
#[cfg(test)]
mod tests;
mod tiers;

use crate::pg::Lsn;

pub(in crate::proxy::connection) use log::WriteLog;

/// Reason the read-after-write gate forwarded a cacheable read to origin
/// instead of serving it from cache (PGC-124), for metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::proxy::connection) enum RawForwardReason {
    /// A pending write against a table the read references.
    Table,
    /// A connection-scoped pending write (unknown target table): every read on
    /// the connection intersects until it clears.
    Connection,
}

/// The read-after-write gate's verdict for one cacheable read ([`WriteLog::decide`]).
/// Side-effect-free so the caller records metrics exactly once per read; the three
/// variants make the illegal "forwarding yet proven disjoint" state unrepresentable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::proxy::connection) enum RawDecision {
    /// Serve from cache — no pending write on this connection touches the read.
    Serve,
    /// Serve from cache — a pending row-enumerable write on a referenced table
    /// was proven row-level disjoint from the read (PGC-379); carries which write
    /// kinds contributed, for per-kind metrics.
    ServeDisjoint(DisjointKinds),
    /// Forward to origin — a pending write the read can't rule out. Carries the
    /// binding clearance constraint for forward attribution (PGC-440).
    Forward(RawForwardReason, RawBlocker),
}

/// The clearance constraint a forwarded read is waiting on: the slowest-to-clear
/// blocker among the first blocking table's tiers (or the connection scope) —
/// what must resolve before an identical read could serve (PGC-440).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::proxy::connection) enum RawBlocker {
    /// A blocking write has no commit-LSN bound yet (probe outstanding, or the
    /// connection scope is unstampable): no watermark advance can clear it.
    Unstamped,
    /// All blocking writes are stamped; the latest bound among them — the read
    /// serves once `settled_lsn` passes it.
    Stamped(Lsn),
}

impl RawBlocker {
    /// Fold another blocking tier's constraint in: `Unstamped` dominates, else
    /// the later bound wins (every blocking tier must clear before serving).
    fn merge(self, other: RawBlocker) -> RawBlocker {
        match (self, other) {
            (RawBlocker::Stamped(a), RawBlocker::Stamped(b)) => RawBlocker::Stamped(a.max(b)),
            _ => RawBlocker::Unstamped,
        }
    }
}

/// Which pipeline stage a forward's blocker is stuck at, for the
/// `raw.forward_blocked` cause metric (PGC-440).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::proxy::connection) enum RawForwardCause {
    /// No commit-LSN bound yet: waiting on the post-commit probe.
    Unstamped,
    /// Bound past the decode-stage receive cursor: origin hasn't delivered the
    /// WAL — recoverable by soliciting walsender progress, not by faster apply.
    DeliveryLag,
    /// Bound received but not settled: decode/apply/settle is behind.
    ApplyLag,
}

/// Classify a forward's blocker against the decode-stage receive cursor
/// (`None` = cache down/restarting: nothing delivered this generation, so a
/// stamped bound is by definition undelivered). The settled watermark needs no
/// comparison — the caller purges settled writes immediately before deciding.
pub(in crate::proxy::connection) fn forward_cause(
    blocker: RawBlocker,
    received: Option<Lsn>,
) -> RawForwardCause {
    match blocker {
        RawBlocker::Unstamped => RawForwardCause::Unstamped,
        RawBlocker::Stamped(bound) => match received {
            Some(received) if bound <= received => RawForwardCause::ApplyLag,
            _ => RawForwardCause::DeliveryLag,
        },
    }
}

/// Which pending write kinds a served read was proven disjoint from (PGC-384).
/// A read can be disjoint from several kinds at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(in crate::proxy::connection) struct DisjointKinds {
    pub insert: bool,
    pub delete: bool,
    pub update: bool,
}

impl DisjointKinds {
    fn any(self) -> bool {
        self.insert || self.delete || self.update
    }
}
