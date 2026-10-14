//! The consistency checks, as pure functions over a served snapshot.
//!
//! A snapshot is the `(group_id, version)` of every row read through the proxy
//! in one `SELECT`. The invariants:
//!
//! 1. **Intra-snapshot atomicity** — each group shows exactly one version.
//! 2. **Per-group monotonicity** — a group's version never decreases across
//!    snapshots (versions only ever increment at origin, and group ids are
//!    never reused at a lower version). One move is exempt: a cache-served read
//!    below an earlier read pgcache forwarded to origin. The cache serves at its
//!    apply watermark, which may trail what origin already showed (ADR-054).
//! 3. **Paired-group equality** — groups bumped together in one transaction
//!    always show equal versions.
//!
//! (Final cache-vs-origin exact equality lives in `snapshot`, not here, since
//! it compares two live reads rather than reducing one snapshot.)

use std::collections::{HashMap, HashSet};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Violation {
    /// A single group exposed two different versions in one snapshot.
    Intra {
        group: i32,
        version_a: i32,
        version_b: i32,
    },
    /// A group's version went backwards between snapshots.
    Monotonic {
        group: i32,
        previous: i32,
        observed: i32,
        /// The backwards read was forwarded to origin, not cache-served.
        forwarded: bool,
    },
    /// A bumped-together pair showed unequal versions.
    Pair {
        group_a: i32,
        version_a: i32,
        group_b: i32,
        version_b: i32,
    },
    /// A read inside a transaction block did not show the block's own
    /// uncommitted bump (PGC-387).
    OwnWrite {
        group: i32,
        expected: i32,
        observed: i32,
    },
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Violation::Intra {
                group,
                version_a,
                version_b,
            } => write!(
                f,
                "intra-snapshot: group {group} exposed versions {version_a} and {version_b} \
                 (a multi-row update applied torn)"
            ),
            Violation::Monotonic {
                group,
                previous,
                observed,
                forwarded,
            } => write!(
                f,
                "monotonicity: group {group} went backwards from version {previous} to {observed} \
                 in a {} read (a stale write overwrote a newer value)",
                if *forwarded {
                    "forwarded"
                } else {
                    "cache-served"
                }
            ),
            Violation::Pair {
                group_a,
                version_a,
                group_b,
                version_b,
            } => write!(
                f,
                "paired-group: group {group_a}=v{version_a} but group {group_b}=v{version_b} \
                 (a cross-group transaction applied torn)"
            ),
            Violation::OwnWrite {
                group,
                expected,
                observed,
            } => write!(
                f,
                "own-write: group {group} bumped to v{expected} inside a transaction but a read \
                 in the same transaction showed v{observed} (served from cache past the block's \
                 own write)"
            ),
        }
    }
}

/// Reduce a snapshot to one version per group, failing on the first group that
/// exposes two versions (invariant 1).
pub fn intra_snapshot_reduce(rows: &[(i32, i32)]) -> Result<HashMap<i32, i32>, Violation> {
    let mut versions: HashMap<i32, i32> = HashMap::new();
    for &(group, version) in rows {
        match versions.get(&group) {
            Some(&seen) if seen != version => {
                return Err(Violation::Intra {
                    group,
                    version_a: seen,
                    version_b: version,
                });
            }
            _ => {
                versions.insert(group, version);
            }
        }
    }
    Ok(versions)
}

/// Tracks the highest version seen per group across snapshots (invariant 2).
///
/// A forwarded read sees origin, which is never behind anything already
/// observed, so it must not fall below any earlier read. A cache-served read
/// must not fall below an earlier cache-served read: the apply watermark only
/// advances. Falling below only a forwarded read is the accepted ADR-054
/// window, counted rather than failed.
#[derive(Debug, Default)]
pub struct MonotonicTracker {
    max: HashMap<i32, i32>,
    cache_max: HashMap<i32, i32>,
    forward_regressions: u64,
}

impl MonotonicTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold one snapshot's group→version map in, returning the first backwards
    /// move if any. `forwarded` holds the groups whose read was forwarded.
    pub fn observe(
        &mut self,
        group_versions: &HashMap<i32, i32>,
        forwarded: &HashSet<i32>,
    ) -> Option<Violation> {
        for (&group, &version) in group_versions {
            let origin_served = forwarded.contains(&group);
            let floor = if origin_served {
                self.max.get(&group)
            } else {
                self.cache_max.get(&group)
            };
            if let Some(&previous) = floor
                && version < previous
            {
                return Some(Violation::Monotonic {
                    group,
                    previous,
                    observed: version,
                    forwarded: origin_served,
                });
            }
            let max = self.max.entry(group).or_insert(version);
            if version < *max {
                self.forward_regressions += 1;
            }
            *max = (*max).max(version);
            if !origin_served {
                let cache_max = self.cache_max.entry(group).or_insert(version);
                *cache_max = (*cache_max).max(version);
            }
        }
        None
    }

    /// Cache-served reads that fell below an earlier forwarded read.
    pub fn forward_regressions(&self) -> u64 {
        self.forward_regressions
    }
}

/// Check that each pair present in the snapshot shows equal versions
/// (invariant 3). A pair with one side absent is skipped — paired groups are
/// never emptied, so this only elides the brief window before first seed
/// replication.
pub fn pair_check(group_versions: &HashMap<i32, i32>, pairs: &[(i32, i32)]) -> Option<Violation> {
    for &(group_a, group_b) in pairs {
        if let (Some(&version_a), Some(&version_b)) =
            (group_versions.get(&group_a), group_versions.get(&group_b))
            && version_a != version_b
        {
            return Some(Violation::Pair {
                group_a,
                version_a,
                group_b,
                version_b,
            });
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intra_reduce_accepts_consistent_groups() {
        let rows = vec![(1, 5), (1, 5), (2, 3), (2, 3)];
        let map = intra_snapshot_reduce(&rows).expect("consistent snapshot reduces");
        assert_eq!(map.get(&1), Some(&5));
        assert_eq!(map.get(&2), Some(&3));
    }

    #[test]
    fn intra_reduce_flags_torn_group() {
        let rows = vec![(1, 5), (1, 6)];
        let v = intra_snapshot_reduce(&rows).expect_err("torn group is a violation");
        assert_eq!(
            v,
            Violation::Intra {
                group: 1,
                version_a: 5,
                version_b: 6
            }
        );
    }

    fn cache_served() -> HashSet<i32> {
        HashSet::new()
    }

    #[test]
    fn monotonic_accepts_nondecreasing() {
        let mut t = MonotonicTracker::new();
        let none = cache_served();
        assert_eq!(t.observe(&HashMap::from([(1, 1), (2, 1)]), &none), None);
        assert_eq!(t.observe(&HashMap::from([(1, 2), (2, 1)]), &none), None);
        assert_eq!(t.observe(&HashMap::from([(1, 2), (2, 5)]), &none), None);
    }

    #[test]
    fn monotonic_flags_backwards_move() {
        let mut t = MonotonicTracker::new();
        assert_eq!(t.observe(&HashMap::from([(1, 7)]), &cache_served()), None);
        assert_eq!(
            t.observe(&HashMap::from([(1, 6)]), &cache_served()),
            Some(Violation::Monotonic {
                group: 1,
                previous: 7,
                observed: 6,
                forwarded: false,
            })
        );
    }

    #[test]
    fn monotonic_counts_cache_read_below_forwarded_read() {
        let mut t = MonotonicTracker::new();
        assert_eq!(t.observe(&HashMap::from([(1, 5)]), &cache_served()), None);
        assert_eq!(
            t.observe(&HashMap::from([(1, 9)]), &HashSet::from([1])),
            None
        );
        assert_eq!(t.observe(&HashMap::from([(1, 8)]), &cache_served()), None);
        assert_eq!(t.forward_regressions(), 1);
    }

    #[test]
    fn monotonic_flags_cache_read_below_earlier_cache_read() {
        let mut t = MonotonicTracker::new();
        assert_eq!(t.observe(&HashMap::from([(1, 6)]), &cache_served()), None);
        assert_eq!(
            t.observe(&HashMap::from([(1, 9)]), &HashSet::from([1])),
            None
        );
        assert_eq!(
            t.observe(&HashMap::from([(1, 5)]), &cache_served()),
            Some(Violation::Monotonic {
                group: 1,
                previous: 6,
                observed: 5,
                forwarded: false,
            })
        );
    }

    #[test]
    fn monotonic_flags_forwarded_read_below_any_earlier_read() {
        let mut t = MonotonicTracker::new();
        assert_eq!(t.observe(&HashMap::from([(1, 7)]), &cache_served()), None);
        assert_eq!(
            t.observe(&HashMap::from([(1, 6)]), &HashSet::from([1])),
            Some(Violation::Monotonic {
                group: 1,
                previous: 7,
                observed: 6,
                forwarded: true,
            })
        );
    }

    #[test]
    fn pair_check_accepts_equal_and_flags_unequal() {
        let pairs = vec![(100, 101)];
        assert_eq!(
            pair_check(&HashMap::from([(100, 3), (101, 3)]), &pairs),
            None
        );
        assert_eq!(
            pair_check(&HashMap::from([(100, 4), (101, 3)]), &pairs),
            Some(Violation::Pair {
                group_a: 100,
                version_a: 4,
                group_b: 101,
                version_b: 3
            })
        );
    }

    #[test]
    fn pair_check_skips_absent_side() {
        let pairs = vec![(100, 101)];
        assert_eq!(pair_check(&HashMap::from([(100, 4)]), &pairs), None);
    }
}
