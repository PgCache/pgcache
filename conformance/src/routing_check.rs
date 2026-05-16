//! Verify a statement's actual pgcache routing against its `# pgcache:`
//! annotation by diffing `/status` counter snapshots taken before and
//! after the statement's two pgcache executions.
//!
//! The harness runs each statement against pgcache twice — once to
//! populate, once to attempt a cache hit — so a correctly cached query
//! records at least one miss and at least one hit for its fingerprint.
//! We cannot recompute pgcache's fingerprint client-side, so the
//! statement is attributed to whichever fingerprints' counters moved
//! (execution is serialized; the harness is the only client).

use anyhow::{Result, bail};

use crate::annotation::Routing;
use crate::status_client::StatusSnapshot;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Delta {
    hit: u64,
    miss: u64,
}

/// Sum the counter movement across every fingerprint that changed
/// between the two snapshots (new fingerprints count as moved-from-zero).
fn delta_total(before: &StatusSnapshot, after: &StatusSnapshot) -> Delta {
    let mut total = Delta::default();
    for (fp, post) in &after.queries {
        let pre = before.queries.get(fp).copied().unwrap_or_default();
        total.hit += post.hit_count.saturating_sub(pre.hit_count);
        total.miss += post.miss_count.saturating_sub(pre.miss_count);
    }
    total
}

/// Assert that the counter deltas between `before` and `after` are
/// consistent with `expected`.
pub fn assert_routing(
    expected: Routing,
    before: &StatusSnapshot,
    after: &StatusSnapshot,
) -> Result<()> {
    let d = delta_total(before, after);
    match expected {
        Routing::Any => Ok(()),
        Routing::Cached => {
            if d.hit == 0 {
                bail!(
                    "expected `cached` but pgcache served no cache hit \
                     (hit delta {}, miss delta {})",
                    d.hit,
                    d.miss
                );
            }
            Ok(())
        }
        Routing::Passthrough => {
            if d.hit > 0 {
                bail!(
                    "expected `passthrough` but pgcache served {} cache hit(s) \
                     (miss delta {})",
                    d.hit,
                    d.miss
                );
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::status_client::QueryCounters;

    fn snap(fp: u64, hit: u64, miss: u64) -> StatusSnapshot {
        let mut s = StatusSnapshot::default();
        s.queries.insert(
            fp,
            QueryCounters {
                hit_count: hit,
                miss_count: miss,
            },
        );
        s
    }

    #[test]
    fn any_always_passes() {
        let b = StatusSnapshot::default();
        let a = snap(7, 0, 1);
        assert!(assert_routing(Routing::Any, &b, &a).is_ok());
    }

    #[test]
    fn cached_requires_a_hit() {
        let before = snap(7, 0, 0);
        let after_hit = snap(7, 1, 1);
        assert!(assert_routing(Routing::Cached, &before, &after_hit).is_ok());

        let after_miss_only = snap(7, 0, 1);
        assert!(assert_routing(Routing::Cached, &before, &after_miss_only).is_err());
    }

    #[test]
    fn cached_counts_a_brand_new_fingerprint() {
        let before = StatusSnapshot::default();
        let after = snap(42, 2, 1);
        assert!(assert_routing(Routing::Cached, &before, &after).is_ok());
    }

    #[test]
    fn passthrough_rejects_any_hit() {
        let before = snap(7, 0, 0);
        let after = snap(7, 1, 0);
        assert!(assert_routing(Routing::Passthrough, &before, &after).is_err());
    }

    #[test]
    fn passthrough_allows_miss_only() {
        let before = snap(7, 0, 0);
        let after = snap(7, 0, 2);
        assert!(assert_routing(Routing::Passthrough, &before, &after).is_ok());
    }
}
