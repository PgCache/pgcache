//! Sub-linear subsumption candidate lookup.
//!
//! Replaces the linear scan over `UpdateQueries.queries` previously done by
//! `subsumption_check`. See PGC-119 for the V0 design notes.
//!
//! For each table, queries are partitioned by their constraint-column set.
//! Within a class, equality-pure queries are hash-indexed by the joint
//! value tuple; queries with any non-equality constraint (Range/IN/etc.) go
//! to a fallback `complex` bucket that's linearly scanned at lookup.
//!
//! Lookup is **lossy-safe**: missed subsumption opportunities just mean we
//! populate from origin instead of stamping existing rows. Wrong subsumption
//! claims would be a correctness bug; we never make those.

use std::collections::{HashMap, HashSet};

use ecow::EcoString;

use crate::query::ast::{BinaryOp, LiteralValue};
use crate::query::constraints::TableConstraint;

/// Sorted, deduplicated set of column names — canonical key for a
/// subsumption class. Two queries constraining the same columns hash to
/// the same `ColumnSet` regardless of source order.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ColumnSet(Vec<EcoString>);

impl ColumnSet {
    pub fn new(mut cols: Vec<EcoString>) -> Self {
        cols.sort();
        cols.dedup();
        Self(cols)
    }

    pub fn columns(&self) -> &[EcoString] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

/// Sub-linear subsumption candidate lookup.
#[derive(Debug, Default)]
pub struct SubsumptionIndex {
    classes: HashMap<ColumnSet, SubsumptionClass>,
    /// Reverse lookup so `remove(fingerprint)` doesn't need to re-classify
    /// the caller's constraints.
    membership: HashMap<u64, Membership>,
}

#[derive(Debug, Default)]
struct SubsumptionClass {
    /// Fingerprints whose constraints on every class column are pure
    /// `Equal(v)`. Keyed by joint value tuple in class-column order.
    equality: HashMap<Vec<LiteralValue>, Vec<u64>>,
    /// Fingerprints with at least one non-equality constraint on any class
    /// column. Linearly scanned at lookup; kept small per V0's lossy-safe
    /// model.
    complex: Vec<u64>,
}

#[derive(Debug)]
struct Membership {
    columns: ColumnSet,
    payload: MembershipPayload,
}

#[derive(Debug)]
enum MembershipPayload {
    Equality(Vec<LiteralValue>),
    Complex,
}

impl SubsumptionIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Index a query's constraints on a single table.
    ///
    /// Caller is responsible for skipping queries that should never be
    /// considered for subsumption (e.g. `has_limit=true`, multi-table
    /// parents). Those should not reach this method. Idempotent on the same
    /// `fingerprint`: previous membership is removed before re-indexing.
    pub fn insert(&mut self, fingerprint: u64, table_constraints: &[TableConstraint]) {
        if self.membership.contains_key(&fingerprint) {
            self.remove(fingerprint);
        }
        match classify(table_constraints) {
            Classification::EqualityPure { columns, values } => {
                let bucket = self.classes.entry(columns.clone()).or_default();
                bucket
                    .equality
                    .entry(values.clone())
                    .or_default()
                    .push(fingerprint);
                self.membership.insert(
                    fingerprint,
                    Membership {
                        columns,
                        payload: MembershipPayload::Equality(values),
                    },
                );
            }
            Classification::Complex { columns } => {
                let bucket = self.classes.entry(columns.clone()).or_default();
                bucket.complex.push(fingerprint);
                self.membership.insert(
                    fingerprint,
                    Membership {
                        columns,
                        payload: MembershipPayload::Complex,
                    },
                );
            }
        }
    }

    /// Remove a query's entry. O(1) plus the per-bucket vector retain.
    /// Removal is a cold path (failure cleanup, eviction).
    pub fn remove(&mut self, fingerprint: u64) {
        let Some(membership) = self.membership.remove(&fingerprint) else {
            return;
        };
        let Some(bucket) = self.classes.get_mut(&membership.columns) else {
            return;
        };
        match membership.payload {
            MembershipPayload::Equality(values) => {
                if let Some(fps) = bucket.equality.get_mut(&values) {
                    fps.retain(|fp| *fp != fingerprint);
                    if fps.is_empty() {
                        bucket.equality.remove(&values);
                    }
                }
            }
            MembershipPayload::Complex => bucket.complex.retain(|fp| *fp != fingerprint),
        }
        if bucket.equality.is_empty() && bucket.complex.is_empty() {
            self.classes.remove(&membership.columns);
        }
    }

    /// Collect candidate parent fingerprints whose constraints might subsume
    /// the new query's constraints on this table. The caller runs the
    /// existing detailed `table_constraints_subsumed` check on each.
    ///
    /// Returns parents whose constraint-column set is a subset of new's, with
    /// equality-pure parents on a matching value tuple short-circuited via
    /// hash lookup. Complex-bucket entries are always returned for any
    /// subset class — fine-grained range/IN/etc. matching happens in the
    /// caller.
    pub fn candidates(&self, new_constraints: &[TableConstraint]) -> HashSet<u64> {
        let mut candidates = HashSet::new();
        let new_class = classify(new_constraints);
        let (new_columns, new_values_opt) = match &new_class {
            Classification::EqualityPure { columns, values } => (columns, Some(values)),
            Classification::Complex { columns } => (columns, None),
        };

        for subset in column_set_powerset(new_columns) {
            let Some(bucket) = self.classes.get(&subset) else {
                continue;
            };
            // Equality probe: when new is equality-pure on `subset`, parents
            // with exactly-matching values are candidates. Independently, the
            // empty subset always probes the empty-tuple key — that bucket
            // holds truly unconstrained parents, which subsume any new query
            // regardless of new's shape.
            let probe_values = if subset.columns().is_empty() {
                Some(Vec::new())
            } else {
                new_values_opt.and_then(|nv| project_values(new_columns, nv, &subset))
            };
            if let Some(values) = probe_values
                && let Some(fps) = bucket.equality.get(&values)
            {
                candidates.extend(fps);
            }
            candidates.extend(&bucket.complex);
        }
        candidates
    }

    /// Number of column-set classes across all entries. Useful for metrics
    /// and for sanity-checking the partitioning fan-out.
    pub fn classes_len(&self) -> usize {
        self.classes.len()
    }

    /// Total fingerprints across all complex buckets. Should stay low for
    /// equality-heavy workloads; rising values indicate V1 within-class
    /// indexing may be warranted.
    pub fn complex_total(&self) -> usize {
        self.classes.values().map(|c| c.complex.len()).sum()
    }
}

enum Classification {
    EqualityPure {
        columns: ColumnSet,
        values: Vec<LiteralValue>,
    },
    Complex {
        columns: ColumnSet,
    },
}

/// Classify a query's table constraints. Equality-pure iff every constraint
/// is `Comparison(_, Equal, _)` and every constrained column has exactly one
/// such constraint with a consistent value.
fn classify(constraints: &[TableConstraint]) -> Classification {
    let mut equality: HashMap<EcoString, LiteralValue> = HashMap::new();
    let mut all_columns: HashSet<EcoString> = HashSet::new();
    let mut complex = false;

    for tc in constraints {
        match tc {
            TableConstraint::Comparison(col, BinaryOp::Equal, val) => {
                all_columns.insert(col.clone());
                match equality.get(col) {
                    Some(prev) if prev == val => {}
                    Some(_) => complex = true,
                    None => {
                        equality.insert(col.clone(), val.clone());
                    }
                }
            }
            TableConstraint::Comparison(col, _, _) | TableConstraint::AnyOf(col, _) => {
                all_columns.insert(col.clone());
                complex = true;
            }
            // Cast constraints sit in a different value domain from bare
            // comparisons; the equality-pure fast bucket can't index them
            // by `(column, value)`. Mark as Complex so detailed subsumption
            // (`table_constraints_subsumed`) handles the cast logic.
            TableConstraint::CastComparison(col, _, _, _) => {
                all_columns.insert(col.clone());
                complex = true;
            }
        }
    }

    let columns = ColumnSet::new(all_columns.into_iter().collect());

    if !complex && equality.len() == columns.len() {
        let values: Vec<LiteralValue> = columns
            .columns()
            .iter()
            .map(|c| equality.remove(c).expect("equality maps every column"))
            .collect();
        Classification::EqualityPure { columns, values }
    } else {
        Classification::Complex { columns }
    }
}

/// Enumerate all subsets of a column set, each as a sorted `ColumnSet`.
/// Bounded by 2^|set| — typical |constraint_columns| ≤ 4 keeps this small.
fn column_set_powerset(set: &ColumnSet) -> Vec<ColumnSet> {
    let cols = set.columns();
    let n = cols.len();
    let mut subsets = Vec::with_capacity(1usize << n);
    for mask in 0u32..(1u32 << n) {
        let mut subset = Vec::with_capacity(mask.count_ones() as usize);
        for (i, col) in cols.iter().enumerate() {
            if mask & (1 << i) != 0 {
                subset.push(col.clone());
            }
        }
        // `cols` is sorted, so the subset stays sorted by construction.
        subsets.push(ColumnSet(subset));
    }
    subsets
}

/// Project a value tuple onto a subset of the original column set. Both
/// `full_columns` and `subset` are sorted; we walk in lockstep.
fn project_values(
    full_columns: &ColumnSet,
    full_values: &[LiteralValue],
    subset: &ColumnSet,
) -> Option<Vec<LiteralValue>> {
    let mut result = Vec::with_capacity(subset.len());
    let mut full_iter = full_columns.columns().iter().zip(full_values);
    for sub_col in subset.columns() {
        loop {
            let (col, val) = full_iter.next()?;
            if col == sub_col {
                result.push(val.clone());
                break;
            }
        }
    }
    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::ast::{BinaryOp, LiteralValue};
    use crate::query::cast::CastTarget;
    use ecow::EcoString;

    fn col(s: &str) -> EcoString {
        EcoString::from(s)
    }

    fn int(n: i64) -> LiteralValue {
        LiteralValue::Integer(n)
    }

    fn eq(c: &str, v: LiteralValue) -> TableConstraint {
        TableConstraint::Comparison(col(c), BinaryOp::Equal, v)
    }

    fn gt(c: &str, v: LiteralValue) -> TableConstraint {
        TableConstraint::Comparison(col(c), BinaryOp::GreaterThan, v)
    }

    fn any_of(c: &str, vs: Vec<LiteralValue>) -> TableConstraint {
        TableConstraint::AnyOf(col(c), vs)
    }

    fn cast_eq(c: &str, cast: CastTarget, v: LiteralValue) -> TableConstraint {
        TableConstraint::CastComparison(col(c), cast, BinaryOp::Equal, v)
    }

    #[test]
    fn empty_index_has_no_candidates() {
        let idx = SubsumptionIndex::new();
        let candidates = idx.candidates(&[eq("id", int(42))]);
        assert!(candidates.is_empty());
    }

    #[test]
    fn equality_pure_exact_match() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[eq("id", int(42))]);
        idx.insert(2, &[eq("id", int(99))]);

        let candidates = idx.candidates(&[eq("id", int(42))]);
        assert_eq!(candidates, [1].into_iter().collect());
    }

    #[test]
    fn equality_pure_different_value_misses() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[eq("id", int(42))]);

        let candidates = idx.candidates(&[eq("id", int(99))]);
        assert!(candidates.is_empty());
    }

    #[test]
    fn parent_broader_via_subset_filter() {
        let mut idx = SubsumptionIndex::new();
        // Parent constrains only category=5 — class {category}
        idx.insert(1, &[eq("category", int(5))]);

        // New constrains category=5 AND status='active' — class {category, status}
        // Subset enumeration should include {category} and find the parent.
        let new = vec![
            eq("category", int(5)),
            eq("status", LiteralValue::String("active".into())),
        ];
        let candidates = idx.candidates(&new);
        assert!(candidates.contains(&1));
    }

    #[test]
    fn parent_with_unconstrained_column_finds_via_empty_class() {
        let mut idx = SubsumptionIndex::new();
        // Parent: full table scan, no constraints — class {}
        idx.insert(1, &[]);

        // New: WHERE id = 42 — class {id}
        // Empty subset of {id} should hit the empty class and pull the parent.
        let candidates = idx.candidates(&[eq("id", int(42))]);
        assert!(candidates.contains(&1));
    }

    #[test]
    fn complex_constraint_lands_in_complex_bucket() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[gt("id", int(100))]);
        idx.insert(
            2,
            &[any_of("status", vec![LiteralValue::String("a".into())])],
        );

        // New: WHERE id = 200 — pure equality on {id}.
        // Parent 1 is in class {id}.complex (gt is non-equality).
        // Parent 2 is in class {status}.complex.
        // Powerset of new's {id} = {{}, {id}}. Only {id} class will be hit;
        // parent 1 should be a candidate via complex scan.
        let candidates = idx.candidates(&[eq("id", int(200))]);
        assert!(candidates.contains(&1));
        assert!(!candidates.contains(&2)); // {status} ⊄ {id}, never visited
    }

    #[test]
    fn mixed_equality_and_complex_both_returned() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[eq("id", int(42))]); // class {id}.equality[(42,)]
        idx.insert(2, &[gt("id", int(0))]); // class {id}.complex

        let candidates = idx.candidates(&[eq("id", int(42))]);
        assert_eq!(candidates, [1, 2].into_iter().collect());
    }

    #[test]
    fn remove_drops_entry() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[eq("id", int(42))]);
        idx.remove(1);

        assert!(idx.candidates(&[eq("id", int(42))]).is_empty());
        assert_eq!(idx.classes_len(), 0);
    }

    #[test]
    fn remove_keeps_unrelated_entries() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[eq("id", int(42))]);
        idx.insert(2, &[eq("id", int(42))]);
        idx.remove(1);

        assert_eq!(
            idx.candidates(&[eq("id", int(42))]),
            [2].into_iter().collect()
        );
    }

    #[test]
    fn column_order_does_not_affect_class_membership() {
        let mut idx = SubsumptionIndex::new();
        // Insert as [a, b]
        idx.insert(1, &[eq("a", int(1)), eq("b", int(2))]);
        // Lookup with [b, a] — same class.
        let candidates = idx.candidates(&[eq("b", int(2)), eq("a", int(1))]);
        assert_eq!(candidates, [1].into_iter().collect());
    }

    #[test]
    fn contradictory_equality_lands_in_complex() {
        let mut idx = SubsumptionIndex::new();
        // WHERE a=1 AND a=2 — same column, conflicting values. Classifier
        // falls back to complex.
        idx.insert(1, &[eq("a", int(1)), eq("a", int(2))]);
        // Probing equality lookup with a=1 must not return this entry from
        // the equality bucket (it's in complex). But complex scan finds it.
        let candidates = idx.candidates(&[eq("a", int(1))]);
        assert!(candidates.contains(&1));
    }

    #[test]
    fn empty_new_query_finds_only_unconstrained_parents() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[]); // unconstrained — class {}
        idx.insert(2, &[eq("id", int(42))]); // class {id}, not a subset of {}

        let candidates = idx.candidates(&[]);
        assert_eq!(candidates, [1].into_iter().collect());
    }

    #[test]
    fn powerset_bounded_by_column_count() {
        // 4 columns → 16 subsets. Just confirm we don't explode for a
        // realistic max.
        let cols = ColumnSet::new(vec![col("a"), col("b"), col("c"), col("d")]);
        assert_eq!(column_set_powerset(&cols).len(), 16);
    }

    // Regression: unconstrained parents must be findable by *complex* new
    // queries (range, IN, NOT IN). The fix was probing the empty-subset
    // equality bucket regardless of whether new is equality-pure.

    #[test]
    fn unconstrained_parent_subsumes_complex_new_range() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[]); // unconstrained — class {}

        // New has a range constraint, not equality. Classified as Complex.
        let candidates = idx.candidates(&[gt("id", int(10))]);
        assert!(
            candidates.contains(&1),
            "unconstrained parent should subsume any range query"
        );
    }

    #[test]
    fn unconstrained_parent_subsumes_complex_new_inset() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[]);

        let candidates = idx.candidates(&[any_of("id", vec![int(1), int(2), int(3)])]);
        assert!(
            candidates.contains(&1),
            "unconstrained parent should subsume any IN-set query"
        );
    }

    #[test]
    fn unconstrained_parent_subsumes_mixed_new() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[]);

        // Mix of equality and non-equality across columns. Classified Complex
        // (any non-equality constraint demotes the whole query to Complex).
        let candidates = idx.candidates(&[eq("a", int(5)), gt("b", int(10))]);
        assert!(
            candidates.contains(&1),
            "unconstrained parent should subsume mixed new queries"
        );
    }

    #[test]
    fn unconstrained_new_finds_unconstrained_parent() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[]);
        idx.insert(2, &[eq("id", int(42))]);

        let candidates = idx.candidates(&[]);
        assert_eq!(candidates, [1].into_iter().collect());
    }

    // Idempotency: re-inserting the same fingerprint should replace the
    // previous indexing (not double-count).

    #[test]
    fn reinsert_same_fingerprint_replaces() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[eq("id", int(5))]);
        // Same fingerprint, different value — lookup of old value misses.
        idx.insert(1, &[eq("id", int(10))]);

        assert!(idx.candidates(&[eq("id", int(5))]).is_empty());
        assert_eq!(
            idx.candidates(&[eq("id", int(10))]),
            [1].into_iter().collect()
        );
    }

    #[test]
    fn reinsert_changing_shape_replaces() {
        let mut idx = SubsumptionIndex::new();
        // Start as Equality-pure on {id}.
        idx.insert(1, &[eq("id", int(5))]);
        // Re-insert with a range — now Complex on {id}.
        idx.insert(1, &[gt("id", int(0))]);

        // Lookup of id=42: equality bucket has no (42,) entry (we replaced
        // the (5,) entry with a complex one), but the complex bucket is
        // always scanned for visited subsets, so the range parent is found.
        let candidates = idx.candidates(&[eq("id", int(42))]);
        assert_eq!(candidates, [1].into_iter().collect());
        // The (5,) equality bucket should be gone (no entry for fp=1 in it).
        // Confirm by counting classes — there's only the {id} class with one
        // complex entry, no leftover empty equality buckets.
        assert_eq!(idx.classes_len(), 1);
    }

    #[test]
    fn remove_unconstrained_drops_empty_class() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[]);
        idx.remove(1);

        assert_eq!(idx.classes_len(), 0);
        assert!(idx.candidates(&[]).is_empty());
    }

    // Documented limitation: parents in non-empty equality classes are only
    // probed when new is *fully* equality-pure on at least one matching
    // subset. When new is overall Complex (any non-equality constraint), the
    // equality probe is skipped for non-empty subsets — even if new has
    // matching equality on the subset's columns. This is a lossy-safe
    // false negative: we populate from origin rather than stamping, never
    // a wrong subsumption claim.
    //
    // The fingerprint can still be found via the complex bucket of the
    // matching subset class, so the only true miss is when the parent is in
    // an equality bucket of a non-empty class AND new is overall complex.
    #[test]
    fn known_limitation_equality_parent_missed_by_complex_new() {
        let mut idx = SubsumptionIndex::new();
        // Parent: WHERE a = 5 — lives in class {a}.equality[(5,)]
        idx.insert(1, &[eq("a", int(5))]);

        // New: WHERE a = 5 AND b > 10 — Complex overall, columns {a, b}
        let new = vec![eq("a", int(5)), gt("b", int(10))];
        let candidates = idx.candidates(&new);

        // The parent COULD subsume (parent's a=5 ⊇ new's a=5∧b>10 on column
        // a; parent has no constraint on b → covers all b). But the index
        // currently misses this — see comment above. Update this test if the
        // limitation is removed (per-column equality detection in candidates).
        assert!(
            !candidates.contains(&1),
            "limitation: equality parent in non-empty class not probed when new is Complex"
        );
    }

    // PGC-182: CastComparison constraints route through Complex classification
    // so the equality-pure fast-bucket doesn't try to index them by raw value
    // (their values live in the cast-output domain, not the column domain).

    #[test]
    fn cast_comparison_classifies_as_complex() {
        let constraint = cast_eq("name", CastTarget::Int4, int(42));
        let class = classify(&[constraint]);
        assert!(matches!(class, Classification::Complex { .. }));
    }

    #[test]
    fn cast_comparison_alongside_equality_classifies_as_complex() {
        // Mixed: a bare equality + a cast comparison. Cast presence forces Complex.
        let constraints = vec![eq("id", int(1)), cast_eq("name", CastTarget::Int4, int(42))];
        let class = classify(&constraints);
        assert!(matches!(class, Classification::Complex { .. }));
    }
}
