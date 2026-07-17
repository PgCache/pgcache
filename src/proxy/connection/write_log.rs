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
//! per-read gate. On top of that sits a bounded **active/waiting** pair of
//! segments giving LSN granularity — `active` gathers new (unstamped) writes;
//! at most one stamped `waiting` segment drains as the CDC apply watermark
//! passes its commit-LSN bound. Separating the two means a fresh write never
//! gates an older, already-applied one: the waiting batch clears on its own
//! LSN even while active holds newer writes.
//!
//! The gate that consults this log lands in PGC-368; row-level INSERT precision
//! (an `InsertAggregate` inside [`TableAggregate`]) lands in PGC-369. For now a
//! table with any pending write is opaque (any read of it intersects).

// The stamp/roll/drain API and the gate accessors are built here but consumed
// by PGC-367 (probe + watermark) and PGC-368 (gate); until those land they are
// exercised only by unit tests. Remove when the consumers are wired.
#![allow(dead_code)]

use std::collections::{HashMap, VecDeque};

use crate::pg::Lsn;
use crate::query::write::{RelationRef, WriteClass};

/// Max segments kept before the two oldest are merged. Two — one `active`
/// gathering writes, one `waiting` to clear — recovers LSN granularity without
/// per-statement state; when CDC keeps up the waiting batch clears before each
/// probe, so no merge happens and precision is per-batch. Raising this to keep
/// more LSN tiers under sustained lag is a one-line change.
const MAX_SEGMENTS: usize = 2;

/// The commit-LSN bound on a segment's writes — the point past which the CDC
/// apply watermark guarantees every write in the segment is applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::proxy::connection) enum PendingLsn {
    /// No bound sampled yet (the probe hasn't stamped this segment). Never
    /// clears until stamped.
    Unstamped,
    /// The apply watermark clears this segment once it reaches `lsn`.
    Stamped(Lsn),
    /// `PREPARE TRANSACTION`: the commit happens later, possibly from another
    /// session, so no probe LSN can bound it. Never cleared by the watermark;
    /// dropped only at connection close.
    Unstampable,
}

impl PendingLsn {
    /// The later-clearing of two bounds, for merging segments. A bound that
    /// never clears via the watermark (`Unstampable`, or an `Unstamped` segment
    /// that — once merged — is no longer the active one a probe could stamp)
    /// dominates a `Stamped` one; two `Stamped` bounds take the max. Keeping the
    /// *more* conservative bound guarantees a merged segment never clears before
    /// either input would have.
    fn later_of(self, other: PendingLsn) -> PendingLsn {
        match (self, other) {
            (PendingLsn::Unstampable, _) | (_, PendingLsn::Unstampable) => PendingLsn::Unstampable,
            (PendingLsn::Unstamped, _) | (_, PendingLsn::Unstamped) => PendingLsn::Unstamped,
            (PendingLsn::Stamped(a), PendingLsn::Stamped(b)) => PendingLsn::Stamped(a.max(b)),
        }
    }
}

/// Pending write state for one table within a segment. Opaque-only for now —
/// PGC-369 adds row-level INSERT precision here.
#[derive(Debug, Default, Clone)]
pub(in crate::proxy::connection) struct TableAggregate {
    /// A non-row-enumerable write (UPDATE/DELETE/MERGE/TRUNCATE, or a degraded
    /// INSERT) is pending against this table → any read of it intersects.
    pub opaque: bool,
}

/// Connection-scoped pending writes whose target table is unknown (DDL, CALL,
/// EXECUTE, multi-statement, unparseable forwards). While set, *every* read
/// intersects until the segment clears.
#[derive(Debug, Default, Clone)]
pub(in crate::proxy::connection) struct ConnAggregate {
    pub opaque: bool,
}

/// One LSN tier of aggregated writes.
#[derive(Debug, Clone)]
pub(in crate::proxy::connection) struct WriteSegment {
    lsn: PendingLsn,
    /// Sequence of the newest write folded in — the probe stamps a segment only
    /// when no write arrived after the probe sampled its bound.
    latest_seq: u64,
    tables: HashMap<RelationRef, TableAggregate>,
    connection: ConnAggregate,
}

impl WriteSegment {
    fn new(seq: u64) -> Self {
        Self {
            lsn: PendingLsn::Unstamped,
            latest_seq: seq,
            tables: HashMap::new(),
            connection: ConnAggregate::default(),
        }
    }

    /// Fold the other segment's aggregates into this one (used when merging the
    /// two oldest segments on overflow). Opaque flags OR together, and the
    /// merged bound is the later-clearing of the two — so a never-clearing
    /// `Unstampable` (2PC) segment is never lost into a clearable one.
    fn absorb(&mut self, other: WriteSegment) {
        for (relation, agg) in other.tables {
            self.tables.entry(relation).or_default().opaque |= agg.opaque;
        }
        self.connection.opaque |= other.connection.opaque;
        self.lsn = self.lsn.later_of(other.lsn);
    }
}

/// Per-connection write log. See the module docs for the model.
pub(in crate::proxy::connection) struct WriteLog {
    /// Newest-last. The back segment is `active` (unstamped, gathering) whenever
    /// one exists; every segment ahead of it is stamped. Bounded to
    /// [`MAX_SEGMENTS`] after each stamp by merging the two oldest.
    segments: VecDeque<WriteSegment>,
    next_seq: u64,
    /// Recording is off entirely when the feature is disabled or the connection
    /// can't serve from cache (`cache_disabled`).
    enabled: bool,
}

impl WriteLog {
    pub(in crate::proxy::connection) fn new(enabled: bool) -> Self {
        Self {
            segments: VecDeque::new(),
            next_seq: 0,
            enabled,
        }
    }

    /// Disable recording (e.g. the connection's database didn't match and the
    /// cache is off for its lifetime).
    pub(in crate::proxy::connection) fn disable(&mut self) {
        self.enabled = false;
        self.segments.clear();
    }

    pub(in crate::proxy::connection) fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub(in crate::proxy::connection) fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    pub(in crate::proxy::connection) fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// The `active` (back, unstamped) segment, creating one if the log is empty
    /// or its back segment has already been stamped.
    fn active(&mut self) -> &mut WriteSegment {
        let need_new = match self.segments.back() {
            None => true,
            Some(back) => !matches!(back.lsn, PendingLsn::Unstamped),
        };
        if need_new {
            let seq = self.next_seq;
            self.segments.push_back(WriteSegment::new(seq));
        }
        self.segments.back_mut().expect("active segment present")
    }

    /// Record a forwarded write into the active segment. No-op when disabled.
    pub(in crate::proxy::connection) fn record(&mut self, class: &WriteClass) {
        if !self.enabled {
            return;
        }
        crate::metrics::handles().raw.writes_recorded.increment(1);
        let seq = self.next_seq;
        self.next_seq += 1;
        let seg = self.active();
        seg.latest_seq = seq;
        match class {
            // Row-level precision arrives in PGC-369; for now an INSERT is an
            // opaque write against its table like any other.
            WriteClass::InsertRows(insert) => {
                seg.tables
                    .entry(insert.relation.clone())
                    .or_default()
                    .opaque = true;
            }
            WriteClass::Table(relation) => {
                seg.tables.entry(relation.clone()).or_default().opaque = true;
            }
            WriteClass::Connection => {
                seg.connection.opaque = true;
            }
            WriteClass::ConnectionUnstampable => {
                seg.connection.opaque = true;
                seg.lsn = PendingLsn::Unstampable;
            }
        }
    }

    /// The sequence to hand the probe at injection: writes recorded up to and
    /// including this may be stamped with the probe's LSN. `None` when there is
    /// nothing to stamp (no active segment awaiting a bound).
    pub(in crate::proxy::connection) fn stamp_seq(&self) -> Option<u64> {
        self.segments
            .back()
            .filter(|s| matches!(s.lsn, PendingLsn::Unstamped))
            .map(|s| s.latest_seq)
    }

    /// Stamp the active segment with the probe's LSN bound, but only if no write
    /// arrived after the probe sampled it (`active.latest_seq <= stamp_seq`).
    /// Otherwise the sample may predate a later write's commit, so skip and let
    /// a subsequent probe retry. Rolls the stamped segment to `waiting` and
    /// enforces the segment bound.
    pub(in crate::proxy::connection) fn stamp(&mut self, stamp_seq: u64, lsn: Lsn) {
        let Some(active) = self.segments.back_mut() else {
            return;
        };
        // An Unstampable (2PC) segment must never take an LSN bound.
        if !matches!(active.lsn, PendingLsn::Unstamped) || active.latest_seq > stamp_seq {
            return;
        }
        active.lsn = PendingLsn::Stamped(lsn);
        self.segments_bound();
    }

    /// Drop every leading segment the watermark has passed. `Unstamped` and
    /// `Unstampable` segments never clear here.
    pub(in crate::proxy::connection) fn purge(&mut self, watermark: Lsn) {
        while let Some(front) = self.segments.front() {
            match front.lsn {
                PendingLsn::Stamped(lsn) if lsn <= watermark => {
                    self.segments.pop_front();
                }
                PendingLsn::Stamped(_) | PendingLsn::Unstamped | PendingLsn::Unstampable => break,
            }
        }
    }

    /// Merge the two oldest segments until at most [`MAX_SEGMENTS`] remain. The
    /// merged segment takes the higher (later) LSN and the union of aggregates —
    /// conservative (it clears no earlier than either input) and it never
    /// touches the active (back) segment.
    fn segments_bound(&mut self) {
        while self.segments.len() > MAX_SEGMENTS {
            crate::metrics::handles().raw.segment_merges.increment(1);
            let older = self.segments.pop_front().expect("two segments to merge");
            let next = self.segments.front_mut().expect("merge target present");
            // `absorb` unions the aggregates and takes the later-clearing bound,
            // so merging never lets an Unstampable (2PC) segment clear early.
            next.absorb(older);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::write::{InsertRow, InsertStatement, RelationRef};
    use std::sync::Arc;

    fn table(name: &str) -> WriteClass {
        WriteClass::Table(RelationRef {
            schema: None,
            name: name.into(),
        })
    }

    fn insert(name: &str) -> WriteClass {
        WriteClass::InsertRows(Arc::new(InsertStatement {
            relation: RelationRef {
                schema: None,
                name: name.into(),
            },
            columns: vec!["id".into()],
            rows: vec![InsertRow::new()],
        }))
    }

    /// Whether `relation` is opaque in any segment (the PGC-368 gate will read
    /// this; here it just inspects state).
    fn table_pending(log: &WriteLog, relation: &str) -> bool {
        let rel = RelationRef {
            schema: None,
            name: relation.into(),
        };
        log.segments
            .iter()
            .any(|s| s.tables.get(&rel).is_some_and(|a| a.opaque))
    }

    fn connection_pending(log: &WriteLog) -> bool {
        log.segments.iter().any(|s| s.connection.opaque)
    }

    #[test]
    fn test_record_aggregates_into_active_segment() {
        let mut log = WriteLog::new(true);
        log.record(&table("orders"));
        log.record(&table("orders"));
        log.record(&insert("users"));
        // Thousands would collapse the same way: one segment, per-table opaque.
        assert_eq!(log.segment_count(), 1);
        assert!(table_pending(&log, "orders"));
        assert!(table_pending(&log, "users"));
        assert!(!table_pending(&log, "items"));
    }

    #[test]
    fn test_disabled_records_nothing() {
        let mut log = WriteLog::new(false);
        log.record(&table("orders"));
        assert!(log.is_empty());
    }

    #[test]
    fn test_connection_scope_write() {
        let mut log = WriteLog::new(true);
        log.record(&WriteClass::Connection);
        assert!(connection_pending(&log));
    }

    #[test]
    fn test_stamp_then_purge_clears_segment() {
        let mut log = WriteLog::new(true);
        log.record(&table("orders"));
        let seq = log.stamp_seq().expect("active awaiting a bound");
        log.stamp(seq, Lsn::from_raw(100));
        // Not yet applied.
        log.purge(Lsn::from_raw(50));
        assert!(table_pending(&log, "orders"));
        // Watermark reaches the bound → clears.
        log.purge(Lsn::from_raw(100));
        assert!(log.is_empty());
    }

    #[test]
    fn test_stamp_skipped_when_write_arrived_after_injection() {
        let mut log = WriteLog::new(true);
        log.record(&table("orders"));
        let seq = log.stamp_seq().expect("bound pending");
        // A write races in after the probe sampled its bound.
        log.record(&table("items"));
        log.stamp(seq, Lsn::from_raw(100));
        // The sample may predate `items`' commit, so nothing is stamped; the
        // whole active segment stays unstamped and never clears on this LSN.
        log.purge(Lsn::from_raw(1_000));
        assert!(table_pending(&log, "orders"));
        assert!(table_pending(&log, "items"));
    }

    #[test]
    fn test_active_waiting_separation() {
        // A fresh write must not gate an older, already-applied batch.
        let mut log = WriteLog::new(true);
        log.record(&table("orders"));
        let seq = log.stamp_seq().expect("bound pending");
        log.stamp(seq, Lsn::from_raw(100)); // waiting: orders @ 100
        log.record(&table("items")); // active: items, unstamped
        assert_eq!(log.segment_count(), 2);
        // Watermark passes the waiting batch but not the active writes.
        log.purge(Lsn::from_raw(100));
        assert!(!table_pending(&log, "orders")); // cleared independently
        assert!(table_pending(&log, "items")); // active still pending
    }

    #[test]
    fn test_overflow_merges_oldest_two() {
        let mut log = WriteLog::new(true);
        // Three stamped batches at rising LSNs, none cleared.
        for (i, tbl) in ["a", "b", "c"].iter().enumerate() {
            log.record(&table(tbl));
            let seq = log.stamp_seq().expect("bound pending");
            log.stamp(seq, Lsn::from_raw((i as u64 + 1) * 100));
        }
        // Bounded to MAX_SEGMENTS; the two oldest merged, all tables retained.
        assert_eq!(log.segment_count(), MAX_SEGMENTS);
        assert!(table_pending(&log, "a"));
        assert!(table_pending(&log, "b"));
        assert!(table_pending(&log, "c"));
        // The merged (oldest) segment took the later bound (200), so `a` and `b`
        // clear together only once the watermark reaches it.
        log.purge(Lsn::from_raw(100));
        assert!(table_pending(&log, "a"));
        log.purge(Lsn::from_raw(200));
        assert!(!table_pending(&log, "a"));
        assert!(!table_pending(&log, "b"));
        assert!(table_pending(&log, "c"));
    }

    #[test]
    fn test_unstampable_never_clears() {
        let mut log = WriteLog::new(true);
        log.record(&WriteClass::ConnectionUnstampable);
        // A probe cannot bound a 2PC prepare.
        assert_eq!(log.stamp_seq(), None);
        log.purge(Lsn::from_raw(u64::MAX));
        assert!(connection_pending(&log));
    }

    #[test]
    fn test_unstampable_survives_merge() {
        // A 2PC prepare's pending state must not clear when overflow merges its
        // (never-clearing) segment into a stamped one.
        let mut log = WriteLog::new(true);
        log.record(&WriteClass::ConnectionUnstampable); // oldest: never clears
        for (i, tbl) in ["x", "y"].iter().enumerate() {
            log.record(&table(tbl));
            let seq = log.stamp_seq().expect("bound pending");
            log.stamp(seq, Lsn::from_raw((i as u64 + 1) * 100));
        }
        // Overflow merged the Unstampable segment into a stamped one; the merged
        // bound must stay never-clearing.
        assert_eq!(log.segment_count(), MAX_SEGMENTS);
        log.purge(Lsn::from_raw(u64::MAX));
        assert!(
            connection_pending(&log),
            "2PC pending state must survive a segment merge"
        );
    }

    #[test]
    fn test_disable_clears_existing() {
        let mut log = WriteLog::new(true);
        log.record(&table("orders"));
        log.disable();
        assert!(log.is_empty());
        assert!(!log.is_enabled());
        log.record(&table("orders"));
        assert!(log.is_empty());
    }
}
