//! Origin MVCC snapshots, for deciding whether a decoded transaction was
//! visible to a read.

use tokio_postgres::Client;

/// An origin snapshot as `pg_current_snapshot()` reports it: full 64-bit xids,
/// `xip` sorted ascending.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OriginSnapshot {
    xmin: u64,
    xmax: u64,
    xip: Vec<u64>,
}

impl OriginSnapshot {
    pub(crate) fn new(xmin: u64, xmax: u64, mut xip: Vec<u64>) -> Self {
        xip.sort_unstable();
        Self { xmin, xmax, xip }
    }

    /// Take a fresh snapshot on `client` (autocommit, so it is the snapshot a
    /// statement issued now would read under).
    pub(crate) async fn current(client: &Client) -> Result<Self, tokio_postgres::Error> {
        let row = client
            .query_one(
                "SELECT pg_snapshot_xmin(s)::text::int8, pg_snapshot_xmax(s)::text::int8, \
                 ARRAY(SELECT pg_snapshot_xip(s)::text::int8) FROM pg_current_snapshot() s",
                &[],
            )
            .await?;
        let xmin: i64 = row.try_get(0)?;
        let xmax: i64 = row.try_get(1)?;
        let xip: Vec<i64> = row.try_get(2)?;
        Ok(Self::new(
            xmin.unsigned_abs(),
            xmax.unsigned_abs(),
            xip.into_iter().map(i64::unsigned_abs).collect(),
        ))
    }

    /// Whether the committed top-level transaction `xid` (32-bit, as pgoutput
    /// sends it) is visible to this snapshot.
    pub(crate) fn sees(&self, xid: u32) -> bool {
        let full = self.xid_widen(xid);
        if full < self.xmin {
            return true;
        }
        if full >= self.xmax {
            return false;
        }
        self.xip.binary_search(&full).is_err()
    }

    /// A snapshot spans fewer than 2^31 xids, so `xid`'s epoch is the one that
    /// puts it within 2^31 of `xmax`.
    fn xid_widen(&self, xid: u32) -> u64 {
        let xmax_low = u32::try_from(self.xmax & u64::from(u32::MAX)).unwrap_or_default();
        let delta = xmax_low.wrapping_sub(xid).cast_signed();
        self.xmax.saturating_add_signed(-i64::from(delta))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sees_follows_xmin_xmax_and_xip() {
        let snapshot = OriginSnapshot::new(100, 110, vec![105, 102]);
        assert!(snapshot.sees(99), "below xmin");
        assert!(snapshot.sees(100), "at xmin, not in progress");
        assert!(!snapshot.sees(102), "in progress");
        assert!(snapshot.sees(103), "committed between xmin and xmax");
        assert!(!snapshot.sees(105), "in progress");
        assert!(!snapshot.sees(110), "at xmax");
        assert!(!snapshot.sees(200), "after xmax");
    }

    #[test]
    fn test_sees_widens_across_an_epoch_boundary() {
        let epoch = 1u64 << 32;
        let snapshot = OriginSnapshot::new(epoch - 5, epoch + 5, vec![epoch - 2, epoch + 1]);
        assert!(snapshot.sees(u32::MAX - 5), "previous epoch, below xmin");
        assert!(snapshot.sees(u32::MAX - 3), "previous epoch, committed");
        assert!(!snapshot.sees(u32::MAX - 1), "previous epoch, in progress");
        assert!(snapshot.sees(0), "new epoch, committed");
        assert!(!snapshot.sees(1), "new epoch, in progress");
        assert!(!snapshot.sees(5), "new epoch, at xmax");
    }
}
