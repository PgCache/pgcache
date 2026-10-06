//! Value-domain range algebra: reduce a column's constraints to a
//! [`ColumnRange`], and decide whether one range subsumes another.
//!
//! Pure value-level reasoning — no AST. The AST-walking extraction that
//! produces the constraints lives in [`extract`](super::extract); this module
//! is also the range vocabulary consumed by `query::constraint_index`.

use std::cmp::Ordering;
use std::collections::HashSet;

use super::TableConstraint;
use crate::query::ast::{BinaryOp, LiteralValue};

/// One end of a column's value range
#[derive(Debug, Clone)]
pub(crate) struct RangeBound {
    pub(crate) value: LiteralValue,
    pub(super) inclusive: bool, // true = >= or <=, false = > or <
}

/// Canonical representation of all constraints on a single column, reduced
/// from a set of (BinaryOp, LiteralValue) pairs. Used by subsumption checking.
#[derive(Debug, Clone)]
pub(crate) enum ColumnRange {
    /// Values are incomparable (Parameter, Null, mixed types) — can't reason
    Unknown,
    /// No constraints — any value matches
    Unconstrained,
    /// Contradictory constraints — no value can satisfy (e.g., = 5 AND > 10)
    Empty,
    /// Exactly one value: column = v
    Equal(LiteralValue),
    /// Finite set of allowed values: column IN (v1, v2, ...)
    InSet(HashSet<LiteralValue>),
    /// Bounded interval with possible exclusions
    Range {
        lower: Option<RangeBound>,
        upper: Option<RangeBound>,
        not_equal: Vec<LiteralValue>,
    },
}

/// Returns true if the value is incomparable for range analysis (Parameter, Null, NullWithCast).
pub(super) fn literal_value_is_incomparable(v: &LiteralValue) -> bool {
    matches!(
        v,
        LiteralValue::Parameter(_) | LiteralValue::Null | LiteralValue::NullWithCast(_)
    )
}

/// Which end of a range a bound sits on. Every bound rule below is written for
/// a lower bound; an upper bound is the same rule with the order reversed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BoundSide {
    Lower,
    Upper,
}

impl BoundSide {
    pub(super) const BOTH: [Self; 2] = [Self::Lower, Self::Upper];

    /// `ord` as seen from this side: for an upper bound, "greater" means
    /// further inside the range.
    fn orient(self, ord: Ordering) -> Ordering {
        match self {
            Self::Lower => ord,
            Self::Upper => ord.reverse(),
        }
    }

    /// Whether a value ordered `value_vs_bound` against a bound on this side
    /// satisfies it.
    pub(super) fn admits(self, value_vs_bound: Ordering, inclusive: bool) -> bool {
        match self.orient(value_vs_bound) {
            Ordering::Greater => true,
            Ordering::Equal => inclusive,
            Ordering::Less => false,
        }
    }
}

/// Tighten a bound: keep the more restrictive of the two (the higher lower
/// bound, the lower upper bound). At equal values, exclusive is tighter than
/// inclusive. Returns None if values are incomparable.
fn bound_tighten(
    side: BoundSide,
    existing: &RangeBound,
    candidate: &RangeBound,
) -> Option<RangeBound> {
    literal_value_order(&existing.value, &candidate.value).map(|ord| match side.orient(ord) {
        // candidate is further inside → tighter
        Ordering::Less => candidate.clone(),
        Ordering::Greater => existing.clone(),
        // same value: exclusive wins
        Ordering::Equal => RangeBound {
            value: existing.value.clone(),
            inclusive: existing.inclusive && candidate.inclusive,
        },
    })
}

/// Whether `value` satisfies `bound`; None if incomparable.
fn value_satisfies(side: BoundSide, value: &LiteralValue, bound: &RangeBound) -> Option<bool> {
    literal_value_order(value, &bound.value).map(|ord| side.admits(ord, bound.inclusive))
}

/// Merge `candidate` into the bound on `side`; false if incomparable.
fn bound_merge(slot: &mut Option<RangeBound>, side: BoundSide, candidate: RangeBound) -> bool {
    let merged = match slot.as_ref() {
        None => Some(candidate),
        Some(existing) => bound_tighten(side, existing, &candidate),
    };
    match merged {
        Some(bound) => {
            *slot = Some(bound);
            true
        }
        None => false,
    }
}

/// Build a ColumnRange from all constraints on a single column.
pub(crate) fn column_range_build(constraints: &[&TableConstraint]) -> ColumnRange {
    if constraints.is_empty() {
        return ColumnRange::Unconstrained;
    }

    // Separate comparisons from in-sets
    let mut comparisons: Vec<(BinaryOp, &LiteralValue)> = Vec::new();
    let mut in_set: Option<&[LiteralValue]> = None;

    for tc in constraints {
        match tc {
            TableConstraint::Comparison(_, op, value)
            | TableConstraint::CastComparison(_, _, op, value) => {
                comparisons.push((*op, value));
            }
            TableConstraint::AnyOf(_, values) => {
                // Multiple AnyOf on same column: intersect sets
                in_set = Some(match in_set {
                    None => values.as_slice(),
                    Some(_existing) => {
                        // Rare case — for now treat as Unknown
                        return ColumnRange::Unknown;
                    }
                });
            }
        }
    }

    // If we have an in-set, integrate with any comparisons
    if let Some(set_values) = in_set {
        return in_set_range_build(set_values, &comparisons);
    }

    // No in-set — pure comparison logic
    comparison_range_build(&comparisons)
}

/// Build a ColumnRange from an IN-set, optionally intersected with comparisons.
fn in_set_range_build(
    set_values: &[LiteralValue],
    comparisons: &[(BinaryOp, &LiteralValue)],
) -> ColumnRange {
    if set_values.is_empty() {
        return ColumnRange::Empty;
    }

    // Any incomparable value in the set makes it unknowable
    if set_values.iter().any(literal_value_is_incomparable) {
        return ColumnRange::Unknown;
    }

    // If no comparisons, return the set directly
    if comparisons.is_empty() {
        return ColumnRange::InSet(set_values.iter().cloned().collect());
    }

    // Build a temporary range from comparisons and filter the set
    let filter_range = comparison_range_build(comparisons);

    match filter_range {
        ColumnRange::Unknown => ColumnRange::Unknown,
        ColumnRange::Empty => ColumnRange::Empty,
        ColumnRange::Unconstrained => ColumnRange::InSet(set_values.iter().cloned().collect()),
        ColumnRange::Equal(v) => {
            if set_values.contains(&v) {
                ColumnRange::Equal(v)
            } else {
                ColumnRange::Empty
            }
        }
        ColumnRange::InSet(_) => {
            // comparison_range_build never produces InSet; Unknown is the safe
            // answer (never claims subsumption) if that ever changes.
            debug_assert!(false, "comparison_range_build never produces InSet");
            ColumnRange::Unknown
        }
        ColumnRange::Range {
            ref lower,
            ref upper,
            ref not_equal,
        } => {
            let mut iter = set_values
                .iter()
                .filter(|v| range_contains_value(RangeView::new(lower, upper, not_equal), v))
                .cloned();
            match iter.next() {
                None => ColumnRange::Empty,
                Some(first) => match iter.next() {
                    None => ColumnRange::Equal(first),
                    Some(second) => {
                        let mut set: HashSet<LiteralValue> = HashSet::from_iter([first, second]);
                        set.extend(iter);
                        ColumnRange::InSet(set)
                    }
                },
            }
        }
    }
}

/// Build a ColumnRange from comparison-only constraints (no in-sets).
fn comparison_range_build(comparisons: &[(BinaryOp, &LiteralValue)]) -> ColumnRange {
    if comparisons.is_empty() {
        return ColumnRange::Unconstrained;
    }

    let mut equal_value: Option<&LiteralValue> = None;
    let mut lower: Option<RangeBound> = None;
    let mut upper: Option<RangeBound> = None;
    let mut not_equal: Vec<LiteralValue> = Vec::new();

    for &(op, value) in comparisons {
        if literal_value_is_incomparable(value) {
            return ColumnRange::Unknown;
        }
        match op {
            BinaryOp::Equal => match equal_value {
                None => equal_value = Some(value),
                Some(existing) if *existing == *value => {} // duplicate
                Some(_) => return ColumnRange::Empty,       // contradictory: = 5 AND = 3
            },
            BinaryOp::NotEqual => {
                not_equal.push(value.clone());
            }
            BinaryOp::GreaterThan | BinaryOp::GreaterThanOrEqual => {
                let candidate = RangeBound {
                    value: value.clone(),
                    inclusive: op == BinaryOp::GreaterThanOrEqual,
                };
                if !bound_merge(&mut lower, BoundSide::Lower, candidate) {
                    return ColumnRange::Unknown;
                }
            }
            BinaryOp::LessThan | BinaryOp::LessThanOrEqual => {
                let candidate = RangeBound {
                    value: value.clone(),
                    inclusive: op == BinaryOp::LessThanOrEqual,
                };
                if !bound_merge(&mut upper, BoundSide::Upper, candidate) {
                    return ColumnRange::Unknown;
                }
            }
            BinaryOp::And
            | BinaryOp::Or
            | BinaryOp::Like
            | BinaryOp::ILike
            | BinaryOp::NotLike
            | BinaryOp::NotILike => return ColumnRange::Unknown,
        }
    }

    // If we have an equality, validate it against bounds and not-equals
    if let Some(eq_val) = equal_value {
        for (side, bound) in [(BoundSide::Lower, &lower), (BoundSide::Upper, &upper)] {
            let Some(bound) = bound else { continue };
            match value_satisfies(side, eq_val, bound) {
                Some(true) => {}
                Some(false) => return ColumnRange::Empty,
                None => return ColumnRange::Unknown,
            }
        }
        if not_equal.contains(eq_val) {
            return ColumnRange::Empty;
        }
        return ColumnRange::Equal(eq_val.clone());
    }

    // Check that bounds aren't contradictory (lower > upper)
    if let (Some(lb), Some(ub)) = (&lower, &upper) {
        match literal_value_order(&lb.value, &ub.value) {
            Some(Ordering::Greater) => return ColumnRange::Empty,
            Some(Ordering::Equal) => {
                if !lb.inclusive || !ub.inclusive {
                    return ColumnRange::Empty;
                }
                // Both inclusive at same value: degenerate range → single point
                if not_equal.contains(&lb.value) {
                    return ColumnRange::Empty;
                }
                return ColumnRange::Equal(lb.value.clone());
            }
            Some(Ordering::Less) => {} // valid range
            None => return ColumnRange::Unknown,
        }
    }

    ColumnRange::Range {
        lower,
        upper,
        not_equal,
    }
}

/// The bounds and exclusions of a [`ColumnRange::Range`], borrowed.
#[derive(Debug, Clone, Copy)]
pub(super) struct RangeView<'a> {
    pub(super) lower: &'a Option<RangeBound>,
    pub(super) upper: &'a Option<RangeBound>,
    pub(super) not_equal: &'a [LiteralValue],
}

impl<'a> RangeView<'a> {
    pub(super) fn new(
        lower: &'a Option<RangeBound>,
        upper: &'a Option<RangeBound>,
        not_equal: &'a [LiteralValue],
    ) -> Self {
        Self {
            lower,
            upper,
            not_equal,
        }
    }

    pub(super) fn bound(&self, side: BoundSide) -> Option<&'a RangeBound> {
        match side {
            BoundSide::Lower => self.lower.as_ref(),
            BoundSide::Upper => self.upper.as_ref(),
        }
    }
}

/// Check if a value falls within a range (satisfies bounds and isn't
/// excluded). An incomparable bound counts as not containing it.
fn range_contains_value(range: RangeView<'_>, value: &LiteralValue) -> bool {
    let within = BoundSide::BOTH.into_iter().all(|side| {
        range
            .bound(side)
            .is_none_or(|bound| value_satisfies(side, value, bound) == Some(true))
    });
    within && !range.not_equal.contains(value)
}

/// Whether `a` is at least as restrictive as `b` on `side`. At the same value,
/// `a` is at least as tight if it is exclusive or both are inclusive.
fn bound_at_least_as_tight(side: BoundSide, a: &RangeBound, b: &RangeBound) -> Option<bool> {
    literal_value_order(&a.value, &b.value).map(|ord| match side.orient(ord) {
        Ordering::Greater => true,
        Ordering::Less => false,
        Ordering::Equal => !a.inclusive || b.inclusive,
    })
}

/// A cached bound is covered when new has a bound on that side at least as
/// tight; new being open-ended there is not covered.
fn bound_covered(side: BoundSide, cached: Option<&RangeBound>, new: Option<&RangeBound>) -> bool {
    match (cached, new) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(cached), Some(new)) => bound_at_least_as_tight(side, new, cached) == Some(true),
    }
}

fn range_subsumes_range(cached: RangeView<'_>, new: RangeView<'_>) -> bool {
    let bounds_covered = BoundSide::BOTH
        .into_iter()
        .all(|side| bound_covered(side, cached.bound(side), new.bound(side)));
    // Each cached exclusion must be excluded by new too: in new's own
    // exclusions, or outside new's range entirely.
    let new_bounds_only = RangeView {
        not_equal: &[],
        ..new
    };
    bounds_covered
        && cached.not_equal.iter().all(|excluded| {
            new.not_equal.contains(excluded) || !range_contains_value(new_bounds_only, excluded)
        })
}

/// Check if cached's ColumnRange subsumes new's ColumnRange.
/// Returns true if every value matching new also matches cached.
pub(super) fn column_range_subsumes(cached: &ColumnRange, new: &ColumnRange) -> bool {
    match (cached, new) {
        // Unknown: can't reason
        (ColumnRange::Unknown, _) | (_, ColumnRange::Unknown) => false,

        // Empty cached: no data to serve from
        (ColumnRange::Empty, _) => false,

        // Empty new: returns nothing, trivially covered
        (_, ColumnRange::Empty) => true,

        // Unconstrained cached: loaded all rows
        (ColumnRange::Unconstrained, _) => true,

        // Unconstrained new: wants everything, cached is restricted
        (_, ColumnRange::Unconstrained) => false,

        // Equal vs Equal
        (ColumnRange::Equal(a), ColumnRange::Equal(b)) => *a == *b,

        // Equal cached can't subsume anything broader
        (ColumnRange::Equal(_), ColumnRange::Range { .. } | ColumnRange::InSet(_)) => false,

        // InSet cached, InSet new: subset check
        (ColumnRange::InSet(cached_set), ColumnRange::InSet(new_set)) => {
            new_set.is_subset(cached_set)
        }

        // InSet cached, Equal new: point in set
        (ColumnRange::InSet(set), ColumnRange::Equal(v)) => set.contains(v),

        // InSet cached, Range new: set is finite, range may be infinite — not subsumed
        (ColumnRange::InSet(_), ColumnRange::Range { .. }) => false,

        // Range cached, InSet new: check all values in the set are within range
        (
            ColumnRange::Range {
                lower,
                upper,
                not_equal,
            },
            ColumnRange::InSet(set),
        ) => set
            .iter()
            .all(|v| range_contains_value(RangeView::new(lower, upper, not_equal), v)),

        // Range cached, Equal new: check point within interval
        (
            ColumnRange::Range {
                lower,
                upper,
                not_equal,
            },
            ColumnRange::Equal(v),
        ) => range_contains_value(RangeView::new(lower, upper, not_equal), v),

        // Range vs Range: full containment check
        (
            ColumnRange::Range {
                lower: cl,
                upper: cu,
                not_equal: cne,
            },
            ColumnRange::Range {
                lower: nl,
                upper: nu,
                not_equal: nne,
            },
        ) => range_subsumes_range(RangeView::new(cl, cu, cne), RangeView::new(nl, nu, nne)),
    }
}

/// Compare two literal values for ordering. Returns None if the values are
/// not comparable (different types, Parameters, Nulls) — and always for
/// strings: byte order does not mirror PostgreSQL collation order (`'a' >
/// 'B'` in bytes but `'a' < 'B'` under en_US), so an ordering claim over text
/// could prove a false exclusion and serve stale data (PGC-446). String
/// *equality* is decided by `range_overlap::literal_value_eq` instead.
pub(super) fn literal_value_order(
    a: &LiteralValue,
    b: &LiteralValue,
) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (LiteralValue::Integer(a), LiteralValue::Integer(b)) => Some(a.cmp(b)),
        (LiteralValue::Float(a), LiteralValue::Float(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

/// Total order for canonicalization only (deterministic `InSet` hashing and
/// dedup) — never a PostgreSQL-semantic claim, so byte order on strings is
/// fine here. Incomparable pairs order as equal, matching the previous
/// `unwrap_or(Equal)` at the sort sites.
pub(super) fn literal_value_canonical_order(a: &LiteralValue, b: &LiteralValue) -> Ordering {
    match (a, b) {
        (LiteralValue::Integer(a), LiteralValue::Integer(b)) => a.cmp(b),
        (LiteralValue::Float(a), LiteralValue::Float(b)) => a.cmp(b),
        (LiteralValue::String(a), LiteralValue::String(b)) => a.cmp(b),
        (LiteralValue::StringWithCast(a, _), LiteralValue::StringWithCast(b, _)) => a.cmp(b),
        _ => Ordering::Equal,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::wildcard_enum_match_arm)]

    use super::*;

    // ========== ColumnRange unit tests ==========

    /// Helper: build a ColumnRange from comparison tuples (convenience for tests)
    fn range_from_comparisons(comparisons: &[(BinaryOp, LiteralValue)]) -> ColumnRange {
        let tcs: Vec<TableConstraint> = comparisons
            .iter()
            .map(|(op, val)| TableConstraint::Comparison("col".into(), *op, val.clone()))
            .collect();
        let refs: Vec<&TableConstraint> = tcs.iter().collect();
        column_range_build(&refs)
    }

    #[test]
    fn test_column_range_build_unconstrained() {
        let range = range_from_comparisons(&[]);
        assert!(matches!(range, ColumnRange::Unconstrained));
    }

    #[test]
    fn test_column_range_build_equal() {
        let range = range_from_comparisons(&[(BinaryOp::Equal, LiteralValue::Integer(5))]);
        assert!(matches!(
            range,
            ColumnRange::Equal(LiteralValue::Integer(5))
        ));
    }

    #[test]
    fn test_column_range_build_contradictory_equals() {
        let range = range_from_comparisons(&[
            (BinaryOp::Equal, LiteralValue::Integer(5)),
            (BinaryOp::Equal, LiteralValue::Integer(3)),
        ]);
        assert!(matches!(range, ColumnRange::Empty));
    }

    #[test]
    fn test_column_range_build_equal_with_not_equal_contradiction() {
        let range = range_from_comparisons(&[
            (BinaryOp::Equal, LiteralValue::Integer(5)),
            (BinaryOp::NotEqual, LiteralValue::Integer(5)),
        ]);
        assert!(matches!(range, ColumnRange::Empty));
    }

    #[test]
    fn test_column_range_build_bounds_contradictory() {
        let range = range_from_comparisons(&[
            (BinaryOp::GreaterThan, LiteralValue::Integer(10)),
            (BinaryOp::LessThan, LiteralValue::Integer(5)),
        ]);
        assert!(matches!(range, ColumnRange::Empty));
    }

    #[test]
    fn test_column_range_build_bounds_equal_exclusive() {
        let range = range_from_comparisons(&[
            (BinaryOp::GreaterThan, LiteralValue::Integer(5)),
            (BinaryOp::LessThan, LiteralValue::Integer(5)),
        ]);
        assert!(matches!(range, ColumnRange::Empty));
    }

    /// A labelled build input and a check on the range it produces.
    type BuildCase = (
        &'static str,
        Vec<(BinaryOp, LiteralValue)>,
        fn(&ColumnRange) -> bool,
    );

    fn is_equal_to(range: &ColumnRange, expected: i64) -> bool {
        matches!(range, ColumnRange::Equal(LiteralValue::Integer(v)) if *v == expected)
    }

    fn bound_is(bound: &Option<RangeBound>, expected: i64, inclusive: bool) -> bool {
        matches!(bound, Some(b) if b.value == LiteralValue::Integer(expected) && b.inclusive == inclusive)
    }

    #[test]
    fn test_column_range_build_bound_interactions() {
        use BinaryOp::{Equal, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual};
        use LiteralValue::Integer;
        let cases: [BuildCase; 5] = [
            (
                "= 5 AND > 10 contradicts",
                vec![(Equal, Integer(5)), (GreaterThan, Integer(10))],
                |r| matches!(r, ColumnRange::Empty),
            ),
            (
                "= 5 AND > 3 keeps the point",
                vec![(Equal, Integer(5)), (GreaterThan, Integer(3))],
                |r| is_equal_to(r, 5),
            ),
            (
                ">= 5 AND <= 5 collapses to a point",
                vec![
                    (GreaterThanOrEqual, Integer(5)),
                    (LessThanOrEqual, Integer(5)),
                ],
                |r| is_equal_to(r, 5),
            ),
            (
                "> 3 AND > 7 keeps the higher lower bound",
                vec![(GreaterThan, Integer(3)), (GreaterThan, Integer(7))],
                |r| {
                    matches!(r, ColumnRange::Range { lower, upper: None, .. }
                        if bound_is(lower, 7, false))
                },
            ),
            (
                "< 10 AND < 5 keeps the lower upper bound",
                vec![(LessThan, Integer(10)), (LessThan, Integer(5))],
                |r| {
                    matches!(r, ColumnRange::Range { lower: None, upper, .. }
                        if bound_is(upper, 5, false))
                },
            ),
        ];
        for (label, comparisons, expected) in cases {
            let range = range_from_comparisons(&comparisons);
            assert!(expected(&range), "{label}: got {range:?}");
        }
    }

    #[test]
    fn test_column_range_build_parameter_unknown() {
        let range =
            range_from_comparisons(&[(BinaryOp::Equal, LiteralValue::Parameter("$1".into()))]);
        assert!(matches!(range, ColumnRange::Unknown));
    }

    #[test]
    fn test_column_range_build_null_unknown() {
        let range = range_from_comparisons(&[(BinaryOp::Equal, LiteralValue::Null)]);
        assert!(matches!(range, ColumnRange::Unknown));
    }

    /// The canonicalization comparator keeps a total byte order for strings —
    /// hash determinism only, never a PG-semantic claim.
    #[test]
    fn test_canonical_order_sorts_strings() {
        let mut values = [
            LiteralValue::String("b".into()),
            LiteralValue::String("a".into()),
        ];
        values.sort_by(literal_value_canonical_order);
        assert_eq!(values[0], LiteralValue::String("a".into()));
    }
}
