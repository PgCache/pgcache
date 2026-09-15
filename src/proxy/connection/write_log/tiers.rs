//! LSN tier queues: per-table active/stamped tiers and the connection-scoped
//! entry, each draining as the settled watermark passes its bound.

use smallvec::SmallVec;

use crate::pg::Lsn;

use super::aggregate::TableAggregate;

/// Stamped tiers a table holds before a further stamp merges the two oldest.
/// Two keeps the common CDC-lag regime (lag under two probe windows) merge-free
/// — each batch drains on its own bound — without unbounded tier growth;
/// `tier_merges` counts saturation of both slots.
pub(super) const WAITING_TIERS: usize = 2;

/// One table's pending writes: an active tier gathering unstamped writes, plus
/// a bounded queue of stamped tiers each draining on its own per-table bound.
#[derive(Debug, Default)]
pub(super) struct TableTiers {
    /// Stamped tiers, oldest first (bounds are monotonic); each drains once
    /// the CDC apply watermark passes its commit-LSN bound. Bounded to
    /// [`WAITING_TIERS`]; a stamp past that merges the two oldest under the
    /// later bound.
    pub(super) waiting: SmallVec<[(Lsn, TableAggregate); WAITING_TIERS]>,
    /// Gathering unstamped writes. The sequence is that of the newest write
    /// folded in — the probe stamps a tier only when no write arrived after the
    /// probe sampled its bound.
    pub(super) active: Option<(u64, TableAggregate)>,
}

impl TableTiers {
    pub(super) fn is_empty(&self) -> bool {
        self.waiting.is_empty() && self.active.is_none()
    }

    /// Fold a write into the active tier, marking it with `seq`.
    pub(super) fn active_mut(&mut self, seq: u64) -> &mut TableAggregate {
        let (latest_seq, agg) = self
            .active
            .get_or_insert_with(|| (seq, TableAggregate::default()));
        *latest_seq = seq;
        agg
    }

    /// Promote the active tier to a waiting slot under the probe's bound, if no
    /// write arrived after the probe sampled it. When the queue is full (the
    /// oldest bounds haven't cleared), the two oldest tiers merge under the
    /// later of their bounds — conservative, and scoped to this table only.
    pub(super) fn stamp(&mut self, stamp_seq: u64, lsn: Lsn) {
        if !matches!(&self.active, Some((latest_seq, _)) if *latest_seq <= stamp_seq) {
            return;
        }
        let Some((_, agg)) = self.active.take() else {
            return;
        };
        if self.waiting.len() >= WAITING_TIERS {
            crate::metrics::handles().raw.tier_merges.increment(1);
            let (oldest_lsn, oldest_agg) = self.waiting.remove(0);
            if let Some((next_lsn, next_agg)) = self.waiting.first_mut() {
                next_agg.merge(oldest_agg);
                // Bounds are monotonic in stamp order, so the later of the two
                // is the surviving slot's own (max is belt-and-braces).
                *next_lsn = (*next_lsn).max(oldest_lsn);
            }
        }
        self.waiting.push((lsn, agg));
    }
}

/// Connection-scoped pending writes whose target table is unknown (DDL, CALL,
/// EXECUTE, multi-statement, unparseable forwards). While pending, *every* read
/// on the connection intersects.
#[derive(Debug, Default)]
pub(super) struct ConnectionTiers {
    /// Stamped: clears once the watermark passes the bound.
    pub(super) waiting: Option<Lsn>,
    /// Unstamped: the newest connection-scoped write's sequence.
    pub(super) active: Option<u64>,
    /// `PREPARE TRANSACTION`: the commit happens later, possibly from another
    /// session, so no probe LSN can bound it. Never cleared by the watermark;
    /// dropped only at connection close.
    pub(super) unstampable: bool,
}

impl ConnectionTiers {
    pub(super) fn is_empty(&self) -> bool {
        self.waiting.is_none() && self.active.is_none() && !self.unstampable
    }

    pub(super) fn stamp(&mut self, stamp_seq: u64, lsn: Lsn) {
        // A 2PC prepare must never take an LSN bound: its commit happens
        // later, possibly from another session. `record` refuses writes while
        // unstampable, but this state must hold even if that ever changes.
        if self.unstampable {
            return;
        }
        let stampable = matches!(self.active, Some(latest_seq) if latest_seq <= stamp_seq);
        if !stampable {
            return;
        }
        self.active = None;
        // Later bound wins on collision, as for tables.
        if self.waiting.is_some() {
            crate::metrics::handles().raw.tier_merges.increment(1);
        }
        self.waiting = Some(self.waiting.map_or(lsn, |prior| lsn.max(prior)));
    }
}

impl TableTiers {
    /// Both tiers' aggregates, waiting first.
    pub(super) fn aggregates(&self) -> impl Iterator<Item = &TableAggregate> {
        self.waiting
            .iter()
            .map(|(_, agg)| agg)
            .chain(self.active.iter().map(|(_, agg)| agg))
    }
}
