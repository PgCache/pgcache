//! Value-domain range algebra: reduce a column's constraints to a
//! [`ColumnRange`], and decide whether one range subsumes another.
//!
//! Pure value-level reasoning — no AST. The AST-walking extraction that
//! produces the constraints lives in [`extract`](super::extract); this module
//! is also the range vocabulary consumed by `query::constraint_index`.

use std::cmp::Ordering;
use std::collections::HashSet;

use crate::query::ast::{BinaryOp, LiteralValue};

use super::TableConstraint;

/// One end of a column's value range
#[derive(Debug, Clone)]
pub(crate) struct RangeBound {
    pub(crate) value: LiteralValue,
    inclusive: bool, // true = >= or <=, false = > or <
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

/// Tighten a lower bound: keep the higher (more restrictive) of the two.
/// At equal values, exclusive (>) is tighter than inclusive (>=).
/// Returns None if values are incomparable.
fn lower_bound_tighten(existing: &RangeBound, candidate: &RangeBound) -> Option<RangeBound> {
    literal_value_order(&existing.value, &candidate.value).map(|ord| {
        match ord {
            // candidate is higher → tighter
            Ordering::Less => candidate.clone(),
            // existing is higher → keep it
            Ordering::Greater => existing.clone(),
            // same value: exclusive wins
            Ordering::Equal => RangeBound {
                value: existing.value.clone(),
                inclusive: existing.inclusive && candidate.inclusive,
            },
        }
    })
}

/// Tighten an upper bound: keep the lower (more restrictive) of the two.
/// At equal values, exclusive (<) is tighter than inclusive (<=).
/// Returns None if values are incomparable.
fn upper_bound_tighten(existing: &RangeBound, candidate: &RangeBound) -> Option<RangeBound> {
    literal_value_order(&existing.value, &candidate.value).map(|ord| {
        match ord {
            // candidate is lower → tighter
            Ordering::Greater => candidate.clone(),
            // existing is lower → keep it
            Ordering::Less => existing.clone(),
            // same value: exclusive wins
            Ordering::Equal => RangeBound {
                value: existing.value.clone(),
                inclusive: existing.inclusive && candidate.inclusive,
            },
        }
    })
}

/// Check if a value satisfies a lower bound (value > bound or value >= bound).
/// Returns None if values are incomparable.
fn value_satisfies_lower(value: &LiteralValue, bound: &RangeBound) -> Option<bool> {
    literal_value_order(value, &bound.value).map(|ord| match ord {
        Ordering::Greater => true,
        Ordering::Equal => bound.inclusive,
        Ordering::Less => false,
    })
}

/// Check if a value satisfies an upper bound (value < bound or value <= bound).
/// Returns None if values are incomparable.
fn value_satisfies_upper(value: &LiteralValue, bound: &RangeBound) -> Option<bool> {
    literal_value_order(value, &bound.value).map(|ord| match ord {
        Ordering::Less => true,
        Ordering::Equal => bound.inclusive,
        Ordering::Greater => false,
    })
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
        ColumnRange::InSet(_) => unreachable!("comparison_range_build never produces InSet"),
        ColumnRange::Range {
            ref lower,
            ref upper,
            ref not_equal,
        } => {
            let mut iter = set_values
                .iter()
                .filter(|v| range_contains_value(lower, upper, not_equal, v))
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
                lower = Some(match lower {
                    None => candidate,
                    Some(existing) => match lower_bound_tighten(&existing, &candidate) {
                        Some(tighter) => tighter,
                        None => return ColumnRange::Unknown,
                    },
                });
            }
            BinaryOp::LessThan | BinaryOp::LessThanOrEqual => {
                let candidate = RangeBound {
                    value: value.clone(),
                    inclusive: op == BinaryOp::LessThanOrEqual,
                };
                upper = Some(match upper {
                    None => candidate,
                    Some(existing) => match upper_bound_tighten(&existing, &candidate) {
                        Some(tighter) => tighter,
                        None => return ColumnRange::Unknown,
                    },
                });
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
        if let Some(ref lb) = lower {
            match value_satisfies_lower(eq_val, lb) {
                Some(true) => {}
                Some(false) => return ColumnRange::Empty,
                None => return ColumnRange::Unknown,
            }
        }
        if let Some(ref ub) = upper {
            match value_satisfies_upper(eq_val, ub) {
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

/// Check if a value falls within a range (satisfies bounds and isn't excluded).
fn range_contains_value(
    lower: &Option<RangeBound>,
    upper: &Option<RangeBound>,
    not_equal: &[LiteralValue],
    value: &LiteralValue,
) -> bool {
    if let Some(lb) = lower {
        match value_satisfies_lower(value, lb) {
            Some(true) => {}
            _ => return false, // fails bound or incomparable
        }
    }
    if let Some(ub) = upper {
        match value_satisfies_upper(value, ub) {
            Some(true) => {}
            _ => return false,
        }
    }
    !not_equal.contains(value)
}

/// Check if a lower bound `a` is at least as tight as lower bound `b`.
/// "At least as tight" means a >= b (a excludes fewer values on the low end).
fn lower_bound_at_least_as_tight(a: &RangeBound, b: &RangeBound) -> Option<bool> {
    literal_value_order(&a.value, &b.value).map(|ord| match ord {
        Ordering::Greater => true,
        Ordering::Less => false,
        // Same value: a is at least as tight if a is exclusive or both are inclusive
        Ordering::Equal => !a.inclusive || b.inclusive,
    })
}

/// Check if an upper bound `a` is at least as tight as upper bound `b`.
/// "At least as tight" means a <= b.
fn upper_bound_at_least_as_tight(a: &RangeBound, b: &RangeBound) -> Option<bool> {
    literal_value_order(&a.value, &b.value).map(|ord| match ord {
        Ordering::Less => true,
        Ordering::Greater => false,
        Ordering::Equal => !a.inclusive || b.inclusive,
    })
}

/// Check if new's range is contained within cached's range, and all cached
/// exclusions are satisfied by new.
fn range_subsumes_range(
    cached_lower: &Option<RangeBound>,
    cached_upper: &Option<RangeBound>,
    cached_not_equal: &[LiteralValue],
    new_lower: &Option<RangeBound>,
    new_upper: &Option<RangeBound>,
    new_not_equal: &[LiteralValue],
) -> bool {
    // Cached has lower bound → new must have one that's at least as tight
    if let Some(cl) = cached_lower {
        match new_lower {
            None => return false, // new is open-ended below
            Some(nl) => match lower_bound_at_least_as_tight(nl, cl) {
                Some(true) => {}
                _ => return false,
            },
        }
    }

    // Cached has upper bound → new must have one that's at least as tight
    if let Some(cu) = cached_upper {
        match new_upper {
            None => return false, // new is open-ended above
            Some(nu) => match upper_bound_at_least_as_tight(nu, cu) {
                Some(true) => {}
                _ => return false,
            },
        }
    }

    // Each cached not_equal must be excluded by new: either in new's not_equal
    // list, or outside new's range entirely
    for excluded in cached_not_equal {
        if new_not_equal.contains(excluded) {
            continue;
        }
        // Check if the excluded value is outside new's range
        if !range_contains_value(new_lower, new_upper, &[], excluded) {
            continue;
        }
        // The value is inside new's range and not in new's exclusion list
        return false;
    }

    true
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
            .all(|v| range_contains_value(lower, upper, not_equal, v)),

        // Range cached, Equal new: check point within interval
        (
            ColumnRange::Range {
                lower,
                upper,
                not_equal,
            },
            ColumnRange::Equal(v),
        ) => range_contains_value(lower, upper, not_equal, v),

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
        ) => range_subsumes_range(cl, cu, cne, nl, nu, nne),
    }
}

/// Compare two literal values for ordering. Returns None if the values
/// are not comparable (different types, Parameters, Nulls).
pub(super) fn literal_value_order(
    a: &LiteralValue,
    b: &LiteralValue,
) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (LiteralValue::Integer(a), LiteralValue::Integer(b)) => Some(a.cmp(b)),
        (LiteralValue::Float(a), LiteralValue::Float(b)) => Some(a.cmp(b)),
        (LiteralValue::String(a), LiteralValue::String(b)) => Some(a.cmp(b)),
        (LiteralValue::StringWithCast(a, _), LiteralValue::StringWithCast(b, _)) => Some(a.cmp(b)),
        _ => None,
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
    fn test_column_range_build_equal_with_contradictory_bound() {
        let range = range_from_comparisons(&[
            (BinaryOp::Equal, LiteralValue::Integer(5)),
            (BinaryOp::GreaterThan, LiteralValue::Integer(10)),
        ]);
        assert!(matches!(range, ColumnRange::Empty));
    }

    #[test]
    fn test_column_range_build_equal_with_consistent_bound() {
        let range = range_from_comparisons(&[
            (BinaryOp::Equal, LiteralValue::Integer(5)),
            (BinaryOp::GreaterThan, LiteralValue::Integer(3)),
        ]);
        assert!(matches!(
            range,
            ColumnRange::Equal(LiteralValue::Integer(5))
        ));
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

    #[test]
    fn test_column_range_build_bounds_equal_inclusive() {
        // >= 5 AND <= 5 → collapses to Equal(5)
        let range = range_from_comparisons(&[
            (BinaryOp::GreaterThanOrEqual, LiteralValue::Integer(5)),
            (BinaryOp::LessThanOrEqual, LiteralValue::Integer(5)),
        ]);
        assert!(matches!(
            range,
            ColumnRange::Equal(LiteralValue::Integer(5))
        ));
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

    #[test]
    fn test_column_range_build_lower_tightening() {
        let range = range_from_comparisons(&[
            (BinaryOp::GreaterThan, LiteralValue::Integer(3)),
            (BinaryOp::GreaterThan, LiteralValue::Integer(7)),
        ]);
        match range {
            ColumnRange::Range {
                lower: Some(lb),
                upper: None,
                ..
            } => {
                assert_eq!(lb.value, LiteralValue::Integer(7));
                assert!(!lb.inclusive);
            }
            _ => panic!("expected Range with lower bound"),
        }
    }

    #[test]
    fn test_column_range_build_upper_tightening() {
        let range = range_from_comparisons(&[
            (BinaryOp::LessThan, LiteralValue::Integer(10)),
            (BinaryOp::LessThan, LiteralValue::Integer(5)),
        ]);
        match range {
            ColumnRange::Range {
                lower: None,
                upper: Some(ub),
                ..
            } => {
                assert_eq!(ub.value, LiteralValue::Integer(5));
                assert!(!ub.inclusive);
            }
            _ => panic!("expected Range with upper bound"),
        }
    }
}
