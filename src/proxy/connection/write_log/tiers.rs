//! LSN tier queues: per-table active/stamped tiers and the connection-scoped
//! entry, each draining as the settled watermark passes its bound.

use std::time::Instant;

use smallvec::SmallVec;

use crate::pg::Lsn;

use super::RawBlocker;
use super::aggregate::TableAggregate;

/// Stamped tiers a table holds before a further stamp coarsens the newest.
/// Merges stay absent while settle lag is under `WAITING_TIERS_MAX` stamp
/// intervals — each batch drains on its own bound; `tier_merges` counts
/// saturation (ADR-051).
pub(super) const WAITING_TIERS_MAX: usize = 8;

/// SmallVec inline slots. Depth beyond this exists only while settle lags, so
/// the common calm-state allocation stays small and deep tiers heap-spill.
const WAITING_TIERS_INLINE: usize = 2;

/// One stamped batch of pending writes awaiting its clearance bound.
#[derive(Debug)]
pub(super) struct WaitingTier {
    pub(super) bound: Lsn,
    /// When the batch was stamped; a fold keeps the earliest, so the
    /// clearance histogram reports worst-case content age (PGC-440).
    pub(super) stamped_at: Instant,
    pub(super) aggregate: TableAggregate,
}

/// One table's pending writes: an active tier gathering unstamped writes, plus
/// a bounded queue of stamped tiers each draining on its own per-table bound.
#[derive(Debug, Default)]
pub(super) struct TableTiers {
    /// Stamped tiers, oldest first (bounds are monotonic); each drains once
    /// the CDC apply watermark passes its commit-LSN bound. Bounded to
    /// [`WAITING_TIERS_MAX`]; a stamp past that folds into the newest tier
    /// under the later bound, so old bounds stay anchored (ADR-051).
    pub(super) waiting: SmallVec<[WaitingTier; WAITING_TIERS_INLINE]>,
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
    /// oldest bounds haven't cleared), the incoming batch folds into the
    /// *newest* waiting tier under the later bound: coarsening lands on the
    /// writes whose clearance is farthest away anyway, while the oldest tiers
    /// keep their anchored bounds and drain on schedule — merging the oldest
    /// instead would re-push their bound on every stamp and starve the
    /// longest-waiting readers under sustained churn (ADR-051).
    pub(super) fn stamp(&mut self, stamp_seq: u64, lsn: Lsn) {
        if !matches!(&self.active, Some((latest_seq, _)) if *latest_seq <= stamp_seq) {
            return;
        }
        let Some((_, agg)) = self.active.take() else {
            return;
        };
        if self.waiting.len() >= WAITING_TIERS_MAX
            && let Some(newest) = self.waiting.last_mut()
        {
            crate::metrics::handles().raw.tier_merges.increment(1);
            newest.aggregate.merge(agg);
            // Bounds are monotonic in stamp order, so the incoming bound is
            // the later one (max is belt-and-braces). `stamped_at` keeps the
            // tier's earlier value: the histogram tracks worst-case age.
            newest.bound = newest.bound.max(lsn);
            return;
        }
        self.waiting.push(WaitingTier {
            bound: lsn,
            stamped_at: Instant::now(),
            aggregate: agg,
        });
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

    /// The clearance constraint of a non-empty connection scope: unstamped
    /// (active or 2PC-unstampable) dominates the stamped bound, since every
    /// pending entry must clear before any read serves (PGC-440).
    pub(super) fn blocker(&self) -> RawBlocker {
        if self.active.is_some() || self.unstampable {
            RawBlocker::Unstamped
        } else {
            self.waiting
                .map_or(RawBlocker::Unstamped, RawBlocker::Stamped)
        }
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
        self.aggregates_bounded().map(|(_, agg)| agg)
    }

    /// All aggregates with their clearance bound: `Some(lsn)` for stamped
    /// waiting tiers, `None` for the active (unstamped) tier. For the gate's
    /// forward attribution (PGC-440).
    pub(super) fn aggregates_bounded(
        &self,
    ) -> impl Iterator<Item = (Option<Lsn>, &TableAggregate)> {
        self.waiting
            .iter()
            .map(|tier| (Some(tier.bound), &tier.aggregate))
            .chain(self.active.iter().map(|(_, agg)| (None, agg)))
    }
}
