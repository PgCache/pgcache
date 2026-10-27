//! Privilege verdicts for the hit path (PGC-463, ADR-061). A cached result
//! is served to a role only once origin has said the role may read every
//! column, schema and function the query touches. Verdicts are cached per
//! (role, object) with the catalog snapshot they were read under, and each
//! DDL the stream applies drops exactly the verdicts whose read could not
//! have seen it. Role changes are not on the stream: the writer polls a
//! digest of the role catalog, and a change drops every stored verdict.

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use ecow::EcoString;

use crate::oid::Oid;
use crate::query::resolve::PrivilegeNeeds;

/// What origin lets a role read of one relation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RelationVerdict {
    /// A lookup is in flight; reads forward meanwhile.
    Pending,
    /// `USAGE` on the schema and `SELECT` on the whole table.
    Full,
    /// `USAGE` on the schema and `SELECT` on exactly these columns; a read
    /// referencing no column needs at least one.
    Columns(Arc<HashSet<EcoString>>),
    /// No `USAGE` on the schema, no `SELECT` at all, or no such role.
    Denied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FunctionVerdict {
    Pending,
    /// `EXECUTE` on every overload of the name.
    Allowed,
    Denied,
}

/// Origin's `pg_current_snapshot()` at a lookup: which transactions its
/// catalog reads could see. Transaction ids are 64-bit (`xid8`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogSnapshot {
    xmin: u64,
    xmax: u64,
    in_progress: Vec<u64>,
}

impl CatalogSnapshot {
    /// Parse the `pg_snapshot` text form `xmin:xmax:xip1,xip2,…`.
    pub(crate) fn parse(text: &str) -> Option<Self> {
        let mut parts = text.split(':');
        let xmin = parts.next()?.parse().ok()?;
        let xmax = parts.next()?.parse().ok()?;
        let in_progress = parts
            .next()?
            .split(',')
            .filter(|xid| !xid.is_empty())
            .map(str::parse)
            .collect::<Result<Vec<u64>, _>>()
            .ok()?;
        parts.next().is_none().then_some(Self {
            xmin,
            xmax,
            in_progress,
        })
    }

    /// Whether the snapshot's reads saw a transaction committed with the
    /// stream's 32-bit `xid`, taken in the epoch nearest `xmax`.
    fn sees(&self, xid: u32) -> bool {
        let xid = self.xid8(xid);
        xid < self.xmin || (xid < self.xmax && !self.in_progress.contains(&xid))
    }

    /// Whether every transaction `other` saw committed, this snapshot saw
    /// too: nothing `other` could see is still ahead of or in progress at
    /// this one. Compared on `xmax` and the in-progress list, not `xmin`,
    /// which one long-running transaction pins for every later snapshot.
    fn covers(&self, other: &Self) -> bool {
        self.xmax >= other.xmax
            && self
                .in_progress
                .iter()
                .filter(|xid| **xid < other.xmax)
                .all(|xid| other.in_progress.contains(xid))
    }

    /// The 64-bit form of a 32-bit transaction id: the epoch of `xmax`,
    /// adjusted when the id sits on the other side of an epoch boundary.
    fn xid8(&self, xid: u32) -> u64 {
        const EPOCH: u64 = 1 << 32;
        const HALF: u64 = 1 << 31;
        let candidate = (self.xmax & !(EPOCH - 1)) | u64::from(xid);
        if candidate > self.xmax && candidate - self.xmax > HALF {
            candidate - EPOCH
        } else if candidate < self.xmax && self.xmax - candidate > HALF {
            candidate + EPOCH
        } else {
            candidate
        }
    }
}

/// The objects a check found no verdict for, to look up at origin. `seq`
/// counts the DDL applied before the check: a result is validated against
/// every DDL applied since.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrivilegeLookup {
    pub(crate) role: EcoString,
    pub(crate) seq: u64,
    pub(crate) relations: Vec<Oid>,
    pub(crate) functions: Vec<EcoString>,
}

impl PrivilegeLookup {
    pub(crate) fn is_empty(&self) -> bool {
        self.relations.is_empty() && self.functions.is_empty()
    }
}

/// Origin's answers for one lookup, with the snapshot they were read under.
#[derive(Debug)]
pub(crate) struct PrivilegeResults {
    pub(crate) snapshot: CatalogSnapshot,
    pub(crate) relations: Vec<(Oid, RelationVerdict)>,
    pub(crate) functions: Vec<(EcoString, FunctionVerdict)>,
}

/// Whether a role may be served a cached read.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PrivilegeCheck {
    Allowed,
    Denied,
    /// Some verdict is missing or in flight. The lookup lists what this
    /// check marked pending; empty when another check already did.
    Pending(PrivilegeLookup),
}

/// A stored verdict and the snapshot its lookup read under; `None` while
/// pending.
type Stored<V> = (V, Option<Arc<CatalogSnapshot>>);

/// What has pruned the store so far: the DDL transactions applied, by
/// sequence number, as far back as a lookup in flight can need them, and the
/// snapshot the latest role change was observed under.
#[derive(Debug, Default)]
struct PruneLog {
    next_seq: u64,
    applied: VecDeque<(u64, u32)>,
    role_change: Option<CatalogSnapshot>,
}

/// Lookups older than this many DDL are dropped rather than validated.
const DDL_LOG_CAPACITY: usize = 256;

impl PruneLog {
    fn push(&mut self, xid: u32) {
        self.applied.push_back((self.next_seq, xid));
        self.next_seq += 1;
        if self.applied.len() > DDL_LOG_CAPACITY {
            self.applied.pop_front();
        }
    }

    /// The xids applied since `seq`, or `None` when the log no longer
    /// reaches back that far.
    fn since(&self, seq: u64) -> Option<Vec<u32>> {
        let oldest = self.applied.front().map_or(self.next_seq, |(s, _)| *s);
        (seq >= oldest).then(|| {
            self.applied
                .iter()
                .filter(|(s, _)| *s >= seq)
                .map(|(_, xid)| *xid)
                .collect()
        })
    }
}

/// The verdict store: wait-free reads on the hit path, written by the
/// writer's lookup command and pruned by its schema-change handler and its
/// role poll.
#[derive(Debug, Default)]
pub struct PrivilegeVerdicts {
    prune: Mutex<PruneLog>,
    relations: DashMap<(EcoString, Oid), Stored<RelationVerdict>>,
    functions: DashMap<(EcoString, EcoString), Stored<FunctionVerdict>>,
}

impl PrivilegeVerdicts {
    fn prune_log(&self) -> std::sync::MutexGuard<'_, PruneLog> {
        // The log is a bounded deque; a poisoned lock holds nothing worse
        // than a truncated history, which only forwards.
        self.prune
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Check `role` against `needs`, marking every object without a verdict
    /// pending so exactly one lookup runs for it. A recorded denial wins:
    /// nothing is marked once one is found.
    pub(crate) fn check(&self, role: &EcoString, needs: &PrivilegeNeeds) -> PrivilegeCheck {
        if self.denied(role, needs) {
            return PrivilegeCheck::Denied;
        }
        let mut lookup = PrivilegeLookup {
            role: role.clone(),
            seq: self.prune_log().next_seq,
            relations: Vec::new(),
            functions: Vec::new(),
        };
        let mut pending = false;
        for oid in needs.relations.keys() {
            match self.relations.entry((role.clone(), *oid)) {
                Entry::Vacant(slot) => {
                    slot.insert((RelationVerdict::Pending, None));
                    lookup.relations.push(*oid);
                    pending = true;
                }
                Entry::Occupied(slot) => pending |= slot.get().0 == RelationVerdict::Pending,
            }
        }
        for name in &needs.functions {
            match self.functions.entry((role.clone(), name.clone())) {
                Entry::Vacant(slot) => {
                    slot.insert((FunctionVerdict::Pending, None));
                    lookup.functions.push(name.clone());
                    pending = true;
                }
                Entry::Occupied(slot) => pending |= slot.get().0 == FunctionVerdict::Pending,
            }
        }
        if pending {
            PrivilegeCheck::Pending(lookup)
        } else {
            PrivilegeCheck::Allowed
        }
    }

    /// Whether a recorded verdict already denies `needs`: no `SELECT` on a
    /// relation, a referenced column outside the granted set (or none
    /// granted at all when no column is referenced), or no `EXECUTE`.
    fn denied(&self, role: &EcoString, needs: &PrivilegeNeeds) -> bool {
        let relation_denied = needs.relations.iter().any(|(oid, columns)| {
            match self
                .relations
                .get(&(role.clone(), *oid))
                .map(|v| v.0.clone())
            {
                Some(RelationVerdict::Denied) => true,
                Some(RelationVerdict::Columns(granted)) => {
                    if columns.is_empty() {
                        granted.is_empty()
                    } else {
                        !columns.is_subset(&granted)
                    }
                }
                Some(RelationVerdict::Pending | RelationVerdict::Full) | None => false,
            }
        });
        relation_denied
            || needs.functions.iter().any(|name| {
                self.functions
                    .get(&(role.clone(), name.clone()))
                    .is_some_and(|v| v.0 == FunctionVerdict::Denied)
            })
    }

    /// Record origin's answers for `lookup`, unless a DDL applied since the
    /// check, or the latest role change, was invisible to the snapshot they
    /// were read under (or the log no longer says): the pending marks are
    /// then dropped and the next read asks again.
    pub(crate) fn apply(&self, lookup: &PrivilegeLookup, results: PrivilegeResults) {
        let valid = {
            let log = self.prune_log();
            log.since(lookup.seq)
                .is_some_and(|xids| xids.iter().all(|xid| results.snapshot.sees(*xid)))
                && log
                    .role_change
                    .as_ref()
                    .is_none_or(|change| results.snapshot.covers(change))
        };
        if !valid {
            self.pending_clear(lookup);
            return;
        }
        let snapshot = Arc::new(results.snapshot);
        for (oid, verdict) in results.relations {
            self.relations.insert(
                (lookup.role.clone(), oid),
                (verdict, Some(Arc::clone(&snapshot))),
            );
        }
        for (name, verdict) in results.functions {
            self.functions.insert(
                (lookup.role.clone(), name),
                (verdict, Some(Arc::clone(&snapshot))),
            );
        }
    }

    /// The lookup failed: drop its pending marks so the next read asks again.
    pub(crate) fn pending_clear(&self, lookup: &PrivilegeLookup) {
        for oid in &lookup.relations {
            self.relations
                .remove_if(&(lookup.role.clone(), *oid), |_, v| {
                    v.0 == RelationVerdict::Pending
                });
        }
        for name in &lookup.functions {
            self.functions
                .remove_if(&(lookup.role.clone(), name.clone()), |_, v| {
                    v.0 == FunctionVerdict::Pending
                });
        }
    }

    /// The stream applied a DDL committed by `xid`: it may have altered
    /// who reads what, so every verdict whose read could not have seen it
    /// is forgotten. Pending marks stay; their results are validated on
    /// arrival.
    pub(crate) fn ddl_applied(&self, xid: u32) {
        self.prune_log().push(xid);
        let unseen = |snapshot: &Option<Arc<CatalogSnapshot>>| {
            snapshot.as_ref().is_some_and(|s| !s.sees(xid))
        };
        self.relations.retain(|_, v| !unseen(&v.1));
        self.functions.retain(|_, v| !unseen(&v.1));
    }

    /// The role poll saw the role catalog change, under `snapshot` when the
    /// poll could read one: membership, superuser or inheritance may have
    /// changed for any role, so every stored verdict is forgotten. Pending
    /// marks stay; their results are validated against the snapshot on
    /// arrival. Without one, nothing in flight can be validated, and the
    /// writer's sequential execution is what keeps a stale result out.
    pub(crate) fn roles_changed(&self, snapshot: Option<CatalogSnapshot>) {
        if let Some(snapshot) = snapshot {
            self.prune_log().role_change = Some(snapshot);
        }
        self.relations
            .retain(|_, v| v.0 == RelationVerdict::Pending);
        self.functions
            .retain(|_, v| v.0 == FunctionVerdict::Pending);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn needs(relations: &[(u32, &[&str])], functions: &[&str]) -> PrivilegeNeeds {
        PrivilegeNeeds {
            relations: relations
                .iter()
                .map(|(oid, columns)| {
                    (
                        Oid::from_raw(*oid),
                        columns.iter().map(|c| EcoString::from(*c)).collect(),
                    )
                })
                .collect::<HashMap<_, _>>(),
            functions: functions.iter().map(|f| EcoString::from(*f)).collect(),
        }
    }

    fn columns(names: &[&str]) -> RelationVerdict {
        RelationVerdict::Columns(Arc::new(
            names.iter().map(|c| EcoString::from(*c)).collect(),
        ))
    }

    fn snapshot(text: &str) -> CatalogSnapshot {
        CatalogSnapshot::parse(text).expect("snapshot parses")
    }

    fn results(snapshot_text: &str, relations: Vec<(Oid, RelationVerdict)>) -> PrivilegeResults {
        PrivilegeResults {
            snapshot: snapshot(snapshot_text),
            relations,
            functions: vec![],
        }
    }

    fn pending(
        store: &PrivilegeVerdicts,
        role: &EcoString,
        need: &PrivilegeNeeds,
    ) -> PrivilegeLookup {
        let PrivilegeCheck::Pending(lookup) = store.check(role, need) else {
            panic!("the check is pending");
        };
        lookup
    }

    #[test]
    fn test_snapshot_parses_and_sees_committed_transactions_only() {
        let s = snapshot("100:110:103,107");
        assert!(s.sees(99), "below xmin");
        assert!(s.sees(100), "xmin itself is not in progress");
        assert!(s.sees(105), "between, not in progress");
        assert!(!s.sees(103), "in progress");
        assert!(!s.sees(110), "xmax is not yet visible");
        assert!(!s.sees(111), "after xmax");
        assert!(CatalogSnapshot::parse("100:100:").is_some());
        assert!(CatalogSnapshot::parse("100:100").is_none());
        assert!(CatalogSnapshot::parse("a:b:").is_none());
    }

    #[test]
    fn test_snapshot_maps_stream_xids_across_an_epoch_boundary() {
        let epoch = 1u64 << 32;
        let s = CatalogSnapshot {
            xmin: epoch + 5,
            xmax: epoch + 10,
            in_progress: vec![],
        };
        assert!(
            s.sees(u32::MAX - 1),
            "just before the epoch rolled: committed"
        );
        assert!(!s.sees(12), "ahead of xmax in the new epoch");
        let s = CatalogSnapshot {
            xmin: epoch - 10,
            xmax: epoch - 3,
            in_progress: vec![],
        };
        assert!(!s.sees(2), "xid 2 of the next epoch is after xmax");
        assert!(s.sees(u32::MAX - 20), "committed in the current epoch");
    }

    #[test]
    fn test_first_check_marks_pending_and_lists_the_lookup() {
        let store = PrivilegeVerdicts::default();
        let role = EcoString::from("reader");
        let need = needs(&[(10, &["id"])], &["f"]);
        let lookup = pending(&store, &role, &need);
        assert_eq!(lookup.relations, vec![Oid::from_raw(10)]);
        assert_eq!(lookup.functions, vec![EcoString::from("f")]);
        assert!(pending(&store, &role, &need).is_empty(), "already marked");
    }

    #[test]
    fn test_column_verdict_requires_every_referenced_column_or_any_one() {
        let store = PrivilegeVerdicts::default();
        let role = EcoString::from("reader");
        let lookup = pending(&store, &role, &needs(&[(10, &["id", "v"])], &[]));
        store.apply(
            &lookup,
            results("1:1:", vec![(Oid::from_raw(10), columns(&["id"]))]),
        );
        assert_eq!(
            store.check(&role, &needs(&[(10, &["id", "v"])], &[])),
            PrivilegeCheck::Denied
        );
        assert_eq!(
            store.check(&role, &needs(&[(10, &["id"])], &[])),
            PrivilegeCheck::Allowed
        );
        assert_eq!(
            store.check(&role, &needs(&[(10, &[])], &[])),
            PrivilegeCheck::Allowed,
            "a read of no column needs any one granted column"
        );
    }

    #[test]
    fn test_denied_wins_before_anything_is_marked_pending() {
        let store = PrivilegeVerdicts::default();
        let role = EcoString::from("outsider");
        let lookup = pending(&store, &role, &needs(&[(10, &["id"])], &[]));
        store.apply(
            &lookup,
            results("1:1:", vec![(Oid::from_raw(10), RelationVerdict::Denied)]),
        );
        assert_eq!(
            store.check(&role, &needs(&[(10, &["id"]), (11, &["x"])], &["f"])),
            PrivilegeCheck::Denied
        );
        assert!(
            !store
                .relations
                .contains_key(&(role.clone(), Oid::from_raw(11)))
        );
        assert!(!store.functions.contains_key(&(role, EcoString::from("f"))));
    }

    #[test]
    fn test_ddl_drops_only_verdicts_whose_read_could_not_see_it() {
        let store = PrivilegeVerdicts::default();
        let role = EcoString::from("reader");
        let old = pending(&store, &role, &needs(&[(10, &["id"])], &[]));
        store.apply(
            &old,
            results("50:50:", vec![(Oid::from_raw(10), RelationVerdict::Full)]),
        );
        let new = pending(&store, &role, &needs(&[(11, &["id"])], &[]));
        store.apply(
            &new,
            results("80:80:", vec![(Oid::from_raw(11), RelationVerdict::Full)]),
        );

        // DDL 60 committed after the first read and before the second.
        store.ddl_applied(60);
        assert!(matches!(
            store.check(&role, &needs(&[(10, &["id"])], &[])),
            PrivilegeCheck::Pending(_)
        ));
        assert_eq!(
            store.check(&role, &needs(&[(11, &["id"])], &[])),
            PrivilegeCheck::Allowed,
            "the second read already saw the DDL"
        );
    }

    #[test]
    fn test_result_whose_snapshot_missed_a_ddl_applied_since_the_check_is_dropped() {
        let store = PrivilegeVerdicts::default();
        let role = EcoString::from("reader");
        let need = needs(&[(10, &["id"])], &[]);
        let lookup = pending(&store, &role, &need);
        store.ddl_applied(70);
        store.apply(
            &lookup,
            results("50:50:", vec![(Oid::from_raw(10), RelationVerdict::Full)]),
        );
        let again = pending(&store, &role, &need);
        assert_eq!(again.relations, vec![Oid::from_raw(10)], "asks again");
        // A result that saw the DDL is kept.
        store.apply(
            &again,
            results("80:80:", vec![(Oid::from_raw(10), RelationVerdict::Full)]),
        );
        assert_eq!(store.check(&role, &need), PrivilegeCheck::Allowed);
    }

    #[test]
    fn test_lookup_older_than_the_ddl_log_is_dropped() {
        let store = PrivilegeVerdicts::default();
        let role = EcoString::from("reader");
        let need = needs(&[(10, &["id"])], &[]);
        let lookup = pending(&store, &role, &need);
        for xid in (0u32..).take(DDL_LOG_CAPACITY + 2) {
            store.ddl_applied(xid);
        }
        store.apply(
            &lookup,
            results(
                "1000:1000:",
                vec![(Oid::from_raw(10), RelationVerdict::Full)],
            ),
        );
        assert!(matches!(
            store.check(&role, &need),
            PrivilegeCheck::Pending(_)
        ));
    }

    #[test]
    fn test_snapshot_covers_everything_an_earlier_one_saw() {
        let change = snapshot("50:60:55");
        assert!(snapshot("50:60:55").covers(&change), "the same snapshot");
        assert!(
            snapshot("50:70:55").covers(&change),
            "later, with the same long-running transaction still open"
        );
        assert!(
            snapshot("50:70:55,65").covers(&change),
            "a transaction opened after the change is not one it saw"
        );
        assert!(
            !snapshot("50:58:").covers(&change),
            "an earlier snapshot has not reached the change's xmax"
        );
        assert!(
            !snapshot("50:70:58").covers(&change),
            "58 was committed at the change and is in progress here, so this read is older"
        );
    }

    #[test]
    fn test_role_change_forgets_stored_verdicts_and_keeps_pending_marks() {
        let store = PrivilegeVerdicts::default();
        let role = EcoString::from("member");
        let stored = pending(&store, &role, &needs(&[(10, &["id"])], &["f"]));
        store.apply(
            &stored,
            PrivilegeResults {
                snapshot: snapshot("50:50:"),
                relations: vec![(Oid::from_raw(10), RelationVerdict::Full)],
                functions: vec![(EcoString::from("f"), FunctionVerdict::Allowed)],
            },
        );
        let in_flight = pending(&store, &role, &needs(&[(11, &["id"])], &[]));

        store.roles_changed(Some(snapshot("60:60:")));
        assert!(
            matches!(
                store.check(&role, &needs(&[(10, &["id"])], &["f"])),
                PrivilegeCheck::Pending(_)
            ),
            "stored verdicts are forgotten"
        );
        assert!(
            pending(&store, &role, &needs(&[(11, &["id"])], &[])).is_empty(),
            "the in-flight mark stays"
        );
        // Its result predates the change and is dropped; a later one lands.
        store.apply(
            &in_flight,
            results("55:55:", vec![(Oid::from_raw(11), RelationVerdict::Full)]),
        );
        let again = pending(&store, &role, &needs(&[(11, &["id"])], &[]));
        assert_eq!(again.relations, vec![Oid::from_raw(11)], "asks again");
        store.apply(
            &again,
            results("60:61:", vec![(Oid::from_raw(11), RelationVerdict::Full)]),
        );
        assert_eq!(
            store.check(&role, &needs(&[(11, &["id"])], &[])),
            PrivilegeCheck::Allowed
        );
    }

    #[test]
    fn test_role_change_without_a_snapshot_still_forgets_stored_verdicts() {
        let store = PrivilegeVerdicts::default();
        let role = EcoString::from("member");
        let need = needs(&[(10, &["id"])], &[]);
        let lookup = pending(&store, &role, &need);
        store.apply(
            &lookup,
            results("50:50:", vec![(Oid::from_raw(10), RelationVerdict::Full)]),
        );
        store.roles_changed(None);
        let again = pending(&store, &role, &need);
        assert_eq!(again.relations, vec![Oid::from_raw(10)]);
        store.apply(
            &again,
            results("50:50:", vec![(Oid::from_raw(10), RelationVerdict::Full)]),
        );
        assert_eq!(store.check(&role, &need), PrivilegeCheck::Allowed);
    }

    #[test]
    fn test_failed_lookup_clears_only_its_pending_marks() {
        let store = PrivilegeVerdicts::default();
        let role = EcoString::from("reader");
        let lookup = pending(&store, &role, &needs(&[(10, &["id"])], &["f"]));
        store.pending_clear(&lookup);
        let again = pending(&store, &role, &needs(&[(10, &["id"])], &["f"]));
        assert_eq!(again.relations, vec![Oid::from_raw(10)]);
        assert_eq!(again.functions, vec![EcoString::from("f")]);
    }
}
