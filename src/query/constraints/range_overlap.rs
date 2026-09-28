//! Read-after-write overlap algebra (PGC-124/379): can a pending write's rows
//! match a read? Reduces both sides to per-column [`ColumnRange`]s and decides
//! containment/disjointness. Only the proxy's write log consumes it, so it is
//! compiled out of the analysis-only build.

use std::cmp::Ordering;
use std::collections::HashMap;
#[cfg(test)]
use std::collections::HashSet;

use ecow::EcoString;

use super::range::{
    ColumnRange, RangeBound, column_range_build, literal_value_is_incomparable, literal_value_order,
};
use super::subsume::constraints_group_by_column;
use super::{QueryConstraints, TableConstraint};
use crate::query::ast::{BinaryOp, LiteralValue};

/// Reduce a set of bare-column `column op literal` comparisons (from a raw-tree
/// DELETE/UPDATE WHERE, PGC-381) to a per-column [`ColumnRange`] map — the write
/// side of read-after-write disjointness, mirroring [`table_column_ranges`] on
/// the read side but sourced from comparisons the classifier extracted without
/// resolution.
pub(crate) fn column_ranges_from_comparisons(
    comparisons: &[(EcoString, BinaryOp, LiteralValue)],
) -> HashMap<EcoString, ColumnRange> {
    let mut by_column: HashMap<EcoString, Vec<TableConstraint>> = HashMap::new();
    for (column, op, value) in comparisons {
        by_column
            .entry(column.clone())
            .or_default()
            .push(TableConstraint::Comparison(
                column.clone(),
                *op,
                value.clone(),
            ));
    }
    by_column
        .into_iter()
        .map(|(column, cs)| {
            let refs: Vec<&TableConstraint> = cs.iter().collect();
            (column, column_range_build(&refs))
        })
        .collect()
}

/// Reduce a query's constraints on `table` to a per-column [`ColumnRange`], for
/// the read side of read-after-write disjointness (PGC-124). Only bare-column
/// comparisons are kept — a cast comparison (`col::date = …`) constrains a
/// derived value, not the raw column the inserted row supplies, so it can't
/// prove disjointness against a raw inserted value. A column absent from the
/// result is unconstrained by the read (and so can't exclude any insert).
pub(crate) fn table_column_ranges(
    constraints: &QueryConstraints,
    table: &str,
) -> HashMap<EcoString, ColumnRange> {
    let Some(table_cs) = constraints.table_constraints.get(table) else {
        return HashMap::new();
    };
    constraints_group_by_column(table_cs)
        .into_iter()
        .filter(|((_, cast), _)| cast.is_none())
        .map(|((col, _), cs)| (EcoString::from(col), column_range_build(cs.as_slice())))
        .collect()
}

/// Exact three-way comparison between an integer and a float value, without the
/// lossy rounding of `i as f64`. `floor(f)` is recovered exactly as an `i64`
/// whenever `|f| < 2^63`, so the result never claims a false inequality — which
/// for read-after-write exclusion (PGC-124) would drop a matching inserted row
/// and serve stale data.
#[allow(clippy::cast_possible_truncation)] // floor is integral and |f| < 2^63 → the cast is exact
fn int_float_cmp(i: i64, f: f64) -> Ordering {
    // Outside the i64 range `floor(f) as i64` would saturate; resolve by sign.
    // 2^63 is exactly representable in f64.
    let two_pow_63 = 2.0_f64.powi(63);
    if f >= two_pow_63 {
        return Ordering::Less; // i < f
    }
    if f < -two_pow_63 {
        return Ordering::Greater; // i > f
    }
    let floor = f.floor();
    let floor_int = floor as i64; // exact: integral and within i64 range
    match i.cmp(&floor_int) {
        Ordering::Greater => Ordering::Greater,
        Ordering::Less => Ordering::Less,
        // i == floor(f): equal only if f has no fractional part, else floor(f) < f.
        Ordering::Equal if f == floor => Ordering::Equal,
        Ordering::Equal => Ordering::Less,
    }
}

/// Like [`literal_value_order`] but also orders `Integer`/`Float` spellings of
/// the same value against each other (`10` vs `10.0`), exactly. Scoped to the
/// read-after-write exclusion path so subsumption's stricter same-type ordering
/// is unaffected (its precision extension is tracked separately).
fn literal_value_order_numeric(a: &LiteralValue, b: &LiteralValue) -> Option<Ordering> {
    match (a, b) {
        (LiteralValue::Integer(i), LiteralValue::Float(f)) => {
            Some(int_float_cmp(*i, (*f).into_inner()))
        }
        (LiteralValue::Float(f), LiteralValue::Integer(i)) => {
            Some(int_float_cmp(*i, (*f).into_inner()).reverse())
        }
        _ => literal_value_order(a, b),
    }
}

/// Whether a concrete `value` falls within a column's `range`.
///
/// `Some(true)` = in range, `Some(false)` = provably excluded, `None` =
/// undecidable (an incomparable value, an `Unknown` range, or a bound that
/// can't be ordered against the value). Used for read-after-write disjointness
/// (PGC-124): a `Some(false)` on any predicate column proves an inserted row
/// can't match the read, so the read may be served despite the pending insert.
///
/// Comparisons go through [`literal_value_order_numeric`], so an integer read
/// literal and a float inserted value (or vice-versa) are compared by numeric
/// value rather than by `LiteralValue` variant — a `= 10` read is neither
/// wrongly excluded from nor wrongly forwarded past a pending `10.0`.
pub(crate) fn column_range_contains(range: &ColumnRange, value: &LiteralValue) -> Option<bool> {
    if literal_value_is_incomparable(value) {
        return None;
    }
    match range {
        ColumnRange::Unconstrained => Some(true),
        ColumnRange::Empty => Some(false),
        ColumnRange::Unknown => None,
        ColumnRange::Equal(v) => literal_value_eq(v, value),
        ColumnRange::InSet(set) => {
            // Excluded only if provably unequal to *every* element; an element
            // incomparable to `value` leaves the membership undecidable.
            let mut all_unequal = true;
            for elem in set {
                match literal_value_eq(elem, value) {
                    Some(true) => return Some(true),
                    Some(false) => {}
                    None => all_unequal = false,
                }
            }
            all_unequal.then_some(false)
        }
        ColumnRange::Range {
            lower,
            upper,
            not_equal,
        } => {
            if not_equal
                .iter()
                .any(|nv| literal_value_eq(nv, value) == Some(true))
            {
                return Some(false);
            }
            let lower_ok = match lower {
                Some(bound) => match literal_value_order_numeric(value, &bound.value)? {
                    Ordering::Greater => true,
                    Ordering::Equal => bound.inclusive,
                    Ordering::Less => false,
                },
                None => true,
            };
            let upper_ok = match upper {
                Some(bound) => match literal_value_order_numeric(value, &bound.value)? {
                    Ordering::Less => true,
                    Ordering::Equal => bound.inclusive,
                    Ordering::Greater => false,
                },
                None => true,
            };
            Some(lower_ok && upper_ok)
        }
    }
}

/// Whether two ranges over the same column are provably disjoint — no value can
/// satisfy both. Sound in one direction only: returns `true` only when disjoint
/// is certain; every uncertainty (an `Unknown` range, an incomparable bound)
/// returns `false`. The building block for [`column_ranges_disjoint`], the
/// UPDATE/DELETE read-after-write predicate check (PGC-379).
fn column_range_disjoint(a: &ColumnRange, b: &ColumnRange) -> bool {
    use ColumnRange::{Empty, Equal, InSet, Range, Unconstrained, Unknown};
    match (a, b) {
        // An unsatisfiable range shares no value with anything.
        (Empty, _) | (_, Empty) => true,
        // Can't reason about an opaque range.
        (Unknown, _) | (_, Unknown) => false,
        // A single point is disjoint iff the other range provably excludes it.
        (Equal(x), other) | (other, Equal(x)) => column_range_contains(other, x) == Some(false),
        // A set is disjoint iff every member is provably excluded by the other.
        (InSet(set), other) | (other, InSet(set)) => set
            .iter()
            .all(|v| column_range_contains(other, v) == Some(false)),
        // Any value satisfies an unconstrained range, so it overlaps every
        // non-empty range (the `Empty` cases returned above).
        (Unconstrained, _) | (_, Unconstrained) => false,
        (
            Range {
                lower: la,
                upper: ua,
                ..
            },
            Range {
                lower: lb,
                upper: ub,
                ..
            },
        ) => bound_below(ua, lb) || bound_below(ub, la),
    }
}

/// Whether an interval whose upper bound is `upper` lies entirely below one
/// whose lower bound is `lower` (`upper < lower`), so the two can't overlap.
/// `None` bounds are ±infinity and can't separate. `not_equal` holes are
/// ignored: treating a range as its bounds only ever reports *less*
/// disjointness, never a false positive.
fn bound_below(upper: &Option<RangeBound>, lower: &Option<RangeBound>) -> bool {
    let (Some(u), Some(l)) = (upper, lower) else {
        return false;
    };
    match literal_value_order_numeric(&u.value, &l.value) {
        Some(Ordering::Less) => true,
        // Touch at a single value: disjoint unless both sides include it.
        Some(Ordering::Equal) => !(u.inclusive && l.inclusive),
        Some(Ordering::Greater) | None => false,
    }
}

/// Whether two per-column predicate range maps are provably disjoint — no row
/// can satisfy both (PGC-379). `true` if either predicate is itself
/// unsatisfiable (an `Empty` column range), or some column constrained by *both*
/// has disjoint ranges. A column present in only one map is unconstrained in the
/// other, so it can't establish disjointness. Sound in one direction: only
/// returns `true` when disjoint is provable.
pub(crate) fn column_ranges_disjoint(
    a: &HashMap<EcoString, ColumnRange>,
    b: &HashMap<EcoString, ColumnRange>,
) -> bool {
    if a.values().any(|r| matches!(r, ColumnRange::Empty))
        || b.values().any(|r| matches!(r, ColumnRange::Empty))
    {
        return true;
    }
    a.iter()
        .any(|(col, ra)| b.get(col).is_some_and(|rb| column_range_disjoint(ra, rb)))
}

/// Whether two literal values are provably equal (`Some(true)`), provably
/// unequal (`Some(false)`), or undecidable (`None`). Strings compare by
/// bytes: PostgreSQL guarantees byte equality ⇔ collation equality for
/// deterministic collations; explicitly-created nondeterministic collations
/// (which PG itself heavily restricts) are a documented exclusion (PGC-446).
pub(super) fn literal_value_eq(a: &LiteralValue, b: &LiteralValue) -> Option<bool> {
    match (a, b) {
        (LiteralValue::Integer(a), LiteralValue::Integer(b)) => Some(a == b),
        (LiteralValue::Float(a), LiteralValue::Float(b)) => Some(a == b),
        (LiteralValue::Integer(i), LiteralValue::Float(f))
        | (LiteralValue::Float(f), LiteralValue::Integer(i)) => {
            Some(int_float_cmp(*i, (*f).into_inner()) == Ordering::Equal)
        }
        (LiteralValue::String(a), LiteralValue::String(b)) => Some(a == b),
        (LiteralValue::StringWithCast(a, _), LiteralValue::StringWithCast(b, _)) => Some(a == b),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::wildcard_enum_match_arm)]

    use ordered_float::NotNan;

    use super::*;

    fn range_from_comparisons(comparisons: &[(BinaryOp, LiteralValue)]) -> ColumnRange {
        let tcs: Vec<TableConstraint> = comparisons
            .iter()
            .map(|(op, val)| TableConstraint::Comparison("col".into(), *op, val.clone()))
            .collect();
        let refs: Vec<&TableConstraint> = tcs.iter().collect();
        column_range_build(&refs)
    }

    // ========== column_range_contains: numeric-aware exclusion (PGC-124) ==========

    fn int(v: i64) -> LiteralValue {
        LiteralValue::Integer(v)
    }

    fn float(v: f64) -> LiteralValue {
        LiteralValue::Float(NotNan::new(v).expect("finite float"))
    }

    #[test]
    fn test_contains_equal_same_type() {
        let range = ColumnRange::Equal(int(10));
        assert_eq!(column_range_contains(&range, &int(10)), Some(true));
        assert_eq!(column_range_contains(&range, &int(20)), Some(false));
    }

    #[test]
    fn test_contains_equal_int_float_match() {
        // `WHERE id = 10` against a pending `10.0` must read as equal, so the
        // row is NOT excluded (the read intersects and forwards).
        assert_eq!(
            column_range_contains(&ColumnRange::Equal(int(10)), &float(10.0)),
            Some(true)
        );
        assert_eq!(
            column_range_contains(&ColumnRange::Equal(float(10.0)), &int(10)),
            Some(true)
        );
    }

    #[test]
    fn test_contains_equal_int_float_disjoint() {
        // Provably unequal across the type mismatch → excluded (read serves).
        assert_eq!(
            column_range_contains(&ColumnRange::Equal(int(10)), &float(20.0)),
            Some(false)
        );
        assert_eq!(
            column_range_contains(&ColumnRange::Equal(int(10)), &float(10.5)),
            Some(false)
        );
        // A fractional value near the integer is still provably unequal, both
        // directions (`10` vs `10.1` and `10.1` vs `10`).
        assert_eq!(
            column_range_contains(&ColumnRange::Equal(int(10)), &float(10.1)),
            Some(false)
        );
        assert_eq!(
            column_range_contains(&ColumnRange::Equal(float(10.1)), &int(10)),
            Some(false)
        );
        assert_eq!(int_float_cmp(10, 10.1), Ordering::Less);
        assert_eq!(int_float_cmp(10, 9.9), Ordering::Greater);
    }

    #[test]
    fn test_contains_equal_cross_type_undecidable() {
        // A text literal vs an integer value can't be proven equal or unequal.
        assert_eq!(
            column_range_contains(
                &ColumnRange::Equal(LiteralValue::String("2".into())),
                &int(2)
            ),
            None
        );
    }

    #[test]
    fn test_contains_inset_int_float() {
        let set = ColumnRange::InSet(HashSet::from([int(1), int(2), int(3)]));
        assert_eq!(column_range_contains(&set, &float(2.0)), Some(true));
        assert_eq!(column_range_contains(&set, &float(5.0)), Some(false));
        assert_eq!(column_range_contains(&set, &int(3)), Some(true));
    }

    #[test]
    fn test_contains_inset_incomparable_element_undecidable() {
        // A mixed-type set with an element incomparable to the value leaves
        // membership undecidable rather than falsely excluding.
        let set = ColumnRange::InSet(HashSet::from([LiteralValue::String("x".into()), int(1)]));
        assert_eq!(column_range_contains(&set, &float(9.0)), None);
    }

    #[test]
    fn test_contains_range_int_float_bounds() {
        // `WHERE price > 5`, integer bound, float inserted values.
        let range = range_from_comparisons(&[(BinaryOp::GreaterThan, int(5))]);
        assert_eq!(column_range_contains(&range, &float(4.5)), Some(false));
        assert_eq!(column_range_contains(&range, &float(5.5)), Some(true));
        assert_eq!(column_range_contains(&range, &float(5.0)), Some(false)); // exclusive
    }

    #[test]
    fn test_contains_range_not_equal_int_float() {
        let range = range_from_comparisons(&[
            (BinaryOp::GreaterThanOrEqual, int(0)),
            (BinaryOp::NotEqual, int(7)),
        ]);
        assert_eq!(column_range_contains(&range, &float(7.0)), Some(false));
        assert_eq!(column_range_contains(&range, &float(8.0)), Some(true));
    }

    #[test]
    fn test_int_float_cmp_exact_at_large_magnitude() {
        // 2^53 + 1 is not representable as f64 (rounds to 2^53); the comparison
        // must still report inequality rather than a false equal.
        let big = (1i64 << 53) + 1;
        assert_eq!(
            int_float_cmp(big, 9_007_199_254_740_992.0),
            Ordering::Greater
        );
        assert_eq!(int_float_cmp(0, 0.0), Ordering::Equal);
        assert_eq!(int_float_cmp(3, 3.0), Ordering::Equal);
        assert_eq!(int_float_cmp(3, 2.9), Ordering::Greater);
        assert_eq!(int_float_cmp(3, 3.1), Ordering::Less);
    }

    // ========== column_range_disjoint / column_ranges_disjoint (PGC-379) ==========

    fn gt(v: i64) -> ColumnRange {
        range_from_comparisons(&[(BinaryOp::GreaterThan, int(v))])
    }

    fn lt(v: i64) -> ColumnRange {
        range_from_comparisons(&[(BinaryOp::LessThan, int(v))])
    }

    fn inset(vals: &[i64]) -> ColumnRange {
        ColumnRange::InSet(vals.iter().map(|v| int(*v)).collect())
    }

    #[test]
    fn test_range_disjoint_equal() {
        assert!(column_range_disjoint(
            &ColumnRange::Equal(int(5)),
            &ColumnRange::Equal(int(1))
        ));
        assert!(!column_range_disjoint(
            &ColumnRange::Equal(int(5)),
            &ColumnRange::Equal(int(5))
        ));
    }

    #[test]
    fn test_range_disjoint_equal_vs_range() {
        // id = 5 vs id > 10 → disjoint; vs id < 10 → overlaps.
        assert!(column_range_disjoint(&ColumnRange::Equal(int(5)), &gt(10)));
        assert!(!column_range_disjoint(&ColumnRange::Equal(int(5)), &lt(10)));
    }

    #[test]
    fn test_range_disjoint_range_vs_range() {
        assert!(column_range_disjoint(&lt(5), &gt(10))); // (,5) and (10,) separated
        assert!(!column_range_disjoint(&lt(5), &gt(3))); // overlap on (3,5)
    }

    #[test]
    fn test_range_disjoint_touching_bounds() {
        // (,5) exclusive upper vs [5,) inclusive lower → meet at 5 but neither
        // both-inclusive: x < 5 and x >= 5 is unsatisfiable → disjoint.
        let lt5 = lt(5);
        let ge5 = range_from_comparisons(&[(BinaryOp::GreaterThanOrEqual, int(5))]);
        assert!(column_range_disjoint(&lt5, &ge5));
        // [5,) inclusive vs (,5] inclusive → both include 5 → overlap.
        let le5 = range_from_comparisons(&[(BinaryOp::LessThanOrEqual, int(5))]);
        assert!(!column_range_disjoint(&ge5, &le5));
    }

    #[test]
    fn test_range_disjoint_inset() {
        assert!(column_range_disjoint(&inset(&[1, 2, 3]), &inset(&[5, 6])));
        assert!(!column_range_disjoint(&inset(&[1, 2, 3]), &inset(&[3, 4])));
        // set vs point / range
        assert!(column_range_disjoint(
            &inset(&[1, 2, 3]),
            &ColumnRange::Equal(int(9))
        ));
        assert!(column_range_disjoint(&inset(&[1, 2, 3]), &gt(10)));
    }

    #[test]
    fn test_range_disjoint_int_float_numeric() {
        // 5 vs 5.0 are numerically equal → NOT disjoint; 5 vs 6.0 → disjoint.
        assert!(!column_range_disjoint(
            &ColumnRange::Equal(int(5)),
            &ColumnRange::Equal(float(5.0))
        ));
        assert!(column_range_disjoint(
            &ColumnRange::Equal(int(5)),
            &ColumnRange::Equal(float(6.0))
        ));
    }

    #[test]
    fn test_range_disjoint_unknown_empty_unconstrained() {
        // Unknown can't be reasoned about → never disjoint.
        assert!(!column_range_disjoint(
            &ColumnRange::Unknown,
            &ColumnRange::Equal(int(5))
        ));
        // Empty is unsatisfiable → disjoint from anything.
        assert!(column_range_disjoint(
            &ColumnRange::Empty,
            &ColumnRange::Equal(int(5))
        ));
        // Unconstrained overlaps every non-empty range.
        assert!(!column_range_disjoint(
            &ColumnRange::Unconstrained,
            &ColumnRange::Equal(int(5))
        ));
    }

    fn ranges(pairs: &[(&str, ColumnRange)]) -> HashMap<EcoString, ColumnRange> {
        pairs
            .iter()
            .map(|(c, r)| ((*c).into(), r.clone()))
            .collect()
    }

    #[test]
    fn test_ranges_disjoint_map() {
        // A column constrained by both, disjointly → whole predicates disjoint.
        assert!(column_ranges_disjoint(
            &ranges(&[("id", ColumnRange::Equal(int(5)))]),
            &ranges(&[("id", ColumnRange::Equal(int(1)))]),
        ));
        // Same column, overlapping → not disjoint.
        assert!(!column_ranges_disjoint(
            &ranges(&[("id", ColumnRange::Equal(int(5)))]),
            &ranges(&[("id", ColumnRange::Equal(int(5)))]),
        ));
        // No shared column → can't establish disjointness.
        assert!(!column_ranges_disjoint(
            &ranges(&[("id", ColumnRange::Equal(int(5)))]),
            &ranges(&[("other", ColumnRange::Equal(int(1)))]),
        ));
        // One disjoint shared column is enough, even if another overlaps.
        assert!(column_ranges_disjoint(
            &ranges(&[
                ("id", ColumnRange::Equal(int(5))),
                ("x", ColumnRange::Equal(int(9)))
            ]),
            &ranges(&[
                ("id", ColumnRange::Equal(int(1))),
                ("x", ColumnRange::Equal(int(9)))
            ]),
        ));
        // An empty map (no predicate) overlaps everything.
        assert!(!column_ranges_disjoint(
            &HashMap::new(),
            &ranges(&[("id", ColumnRange::Equal(int(5)))]),
        ));
        // An unsatisfiable (Empty) column range → disjoint from anything.
        assert!(column_ranges_disjoint(
            &ranges(&[("id", ColumnRange::Empty)]),
            &ranges(&[("other", ColumnRange::Equal(int(5)))]),
        ));
    }
    /// PGC-446: byte order is not collation order, so ordering claims over
    /// strings must be undecidable — a false exclusion would serve stale data.
    #[test]
    fn test_string_ordering_is_undecidable() {
        let a = LiteralValue::String("a".into());
        let b = LiteralValue::String("B".into());
        assert_eq!(literal_value_order(&a, &b), None);
        // A text range bound cannot exclude a pending value ('a' < 'B' under
        // en_US even though 'a' > 'B' in bytes).
        let range = ColumnRange::Range {
            lower: None,
            upper: Some(RangeBound {
                value: b,
                inclusive: false,
            }),
            not_equal: vec![],
        };
        assert_eq!(column_range_contains(&range, &a), None);
    }

    /// String equality stays decidable by bytes (sound for deterministic
    /// collations), so text-PK point disjointness is preserved.
    #[test]
    fn test_string_equality_still_decides() {
        let a = LiteralValue::String("a".into());
        let a2 = LiteralValue::String("a".into());
        let b = LiteralValue::String("b".into());
        assert_eq!(literal_value_eq(&a, &a2), Some(true));
        assert_eq!(literal_value_eq(&a, &b), Some(false));
        assert_eq!(
            column_range_contains(&ColumnRange::Equal(a.clone()), &a2),
            Some(true)
        );
        assert_eq!(
            column_range_contains(&ColumnRange::Equal(a), &b),
            Some(false)
        );
    }
}
