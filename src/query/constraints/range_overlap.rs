//! Read-after-write overlap algebra (PGC-124/379): can a pending write's rows
//! match a read? Reduces both sides to per-column [`ColumnRange`]s and decides
//! containment/disjointness. Only the proxy's write log consumes it, so it is
//! compiled out of the analysis-only build.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use ecow::EcoString;

#[cfg(test)]
use super::ColumnConstraintKind;
use super::TableConstraint;
#[cfg(test)]
use super::range::column_range_build;
use super::range::{BoundSide, ColumnRange, RangeBound, RangeView};
use super::subsume::TableRanges;
use super::value::{ConstraintValue, ValueRef};

/// Reduce a table's constraints to a per-column [`ColumnRange`]: a
/// reference's filters on the read side of read-after-write disjointness
/// (PGC-124), or a DELETE/UPDATE WHERE's comparisons on the write side
/// (PGC-381). Only bare-column constraints are kept — a cast comparison
/// (`col::date = …`) constrains a derived value, not the raw column the
/// inserted row supplies, so it can't prove disjointness against a raw
/// inserted value. A column absent from the result is unconstrained (and so
/// can't exclude any row).
pub(crate) fn table_column_ranges(filters: &[TableConstraint]) -> HashMap<EcoString, ColumnRange> {
    TableRanges::build(filters)
        .bare_ranges()
        .map(|(column, range)| (column.clone(), range.clone()))
        .collect()
}

/// Whether a concrete `value` falls within a column's `range`.
///
/// `Some(true)` = in range, `Some(false)` = provably excluded, `None` =
/// undecidable (an incomparable value, an `Unknown` range, or a bound that
/// can't be ordered against the value). Used for read-after-write disjointness
/// (PGC-124): a `Some(false)` on any predicate column proves an inserted row
/// can't match the read, so the read may be served despite the pending insert.
///
/// A read's typed value reads a write's literal as a value of its own
/// column ([`ValueRef::value_eq`]), so the two are compared by value however
/// each was spelled: a `= 10` read is neither wrongly excluded from nor
/// wrongly forwarded past a pending `10.0` or `'10'`.
pub(crate) fn column_range_contains(range: &ColumnRange, value: ValueRef<'_>) -> Option<bool> {
    if value.is_incomparable() {
        return None;
    }
    match range {
        ColumnRange::Unconstrained => Some(true),
        ColumnRange::Empty => Some(false),
        ColumnRange::Unknown => None,
        // A non-NULL value never satisfies `IS NULL`; a NULL one returned above.
        ColumnRange::Null => Some(false),
        ColumnRange::Equal(v) => v.as_ref().value_eq(value),
        ColumnRange::InSet(set) => in_set_contains(set, value),
        ColumnRange::Range {
            lower,
            upper,
            not_equal,
        } => range_contains(RangeView::new(lower, upper, not_equal), value),
    }
}

/// Excluded only if provably unequal to *every* element; an element
/// incomparable to `value` leaves the membership undecidable.
fn in_set_contains(set: &HashSet<ConstraintValue>, value: ValueRef<'_>) -> Option<bool> {
    let mut all_unequal = true;
    for elem in set {
        match elem.as_ref().value_eq(value) {
            Some(true) => return Some(true),
            Some(false) => {}
            None => all_unequal = false,
        }
    }
    all_unequal.then_some(false)
}

/// An explicit exclusion rules the value out; otherwise it must satisfy both
/// bounds, and a bound incomparable to it leaves containment undecidable.
fn range_contains(range: RangeView<'_>, value: ValueRef<'_>) -> Option<bool> {
    if range
        .not_equal
        .iter()
        .any(|nv| nv.as_ref().value_eq(value) == Some(true))
    {
        return Some(false);
    }
    let mut admitted = true;
    for side in BoundSide::BOTH {
        if let Some(bound) = range.bound(side) {
            let ord = value.value_order(bound.value.as_ref())?;
            admitted &= side.admits(ord, bound.inclusive);
        }
    }
    Some(admitted)
}

/// Whether two ranges over the same column are provably disjoint — no value can
/// satisfy both. Sound in one direction only: returns `true` only when disjoint
/// is certain; every uncertainty (an `Unknown` range, an incomparable bound)
/// returns `false`. The building block for [`column_ranges_disjoint`], the
/// UPDATE/DELETE read-after-write predicate check (PGC-379).
fn column_range_disjoint(a: &ColumnRange, b: &ColumnRange) -> bool {
    use ColumnRange::{Empty, Equal, InSet, Null, Range, Unconstrained, Unknown};
    match (a, b) {
        // An unsatisfiable range shares no value with anything.
        (Empty, _) | (_, Empty) => true,
        // Can't reason about an opaque range.
        (Unknown, _) | (_, Unknown) => false,
        // A single point is disjoint iff the other range provably excludes it.
        (Equal(x), other) | (other, Equal(x)) => {
            column_range_contains(other, x.as_ref()) == Some(false)
        }
        // A set is disjoint iff every member is provably excluded by the other.
        (InSet(set), other) | (other, InSet(set)) => set
            .iter()
            .all(|v| column_range_contains(other, v.as_ref()) == Some(false)),
        (Null, Null) => false,
        // Any value satisfies an unconstrained range, so it overlaps every
        // non-empty range (the `Empty` cases returned above).
        (Unconstrained, _) | (_, Unconstrained) => false,
        // A range built from comparisons excludes NULL. A point or set is
        // decided above, where a NULL literal stays undecidable.
        (Null, Range { .. }) | (Range { .. }, Null) => true,
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
    match u.value.value_order(&l.value) {
        Some(Ordering::Less) => true,
        // Touch at a single value: disjoint unless both sides include it.
        Some(Ordering::Equal) => !(u.inclusive && l.inclusive),
        Some(Ordering::Greater) | None => false,
    }
}

/// Whether a read reference's ranges and a write's per-column ranges are
/// provably disjoint — no row can satisfy both (PGC-379). `true` if either
/// predicate is itself unsatisfiable (an `Empty` column range), or some column
/// constrained by *both* has disjoint ranges. A column present on only one
/// side is unconstrained on the other, so it can't establish disjointness.
/// Sound in one direction: only returns `true` when disjoint is provable.
pub(crate) fn column_ranges_disjoint(
    read: &TableRanges,
    write: &HashMap<EcoString, ColumnRange>,
) -> bool {
    if read
        .bare_ranges()
        .any(|(_, range)| matches!(range, ColumnRange::Empty))
        || write
            .values()
            .any(|range| matches!(range, ColumnRange::Empty))
    {
        return true;
    }
    read.bare_ranges().any(|(column, read_range)| {
        write
            .get(column)
            .is_some_and(|write_range| column_range_disjoint(read_range, write_range))
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::wildcard_enum_match_arm)]

    use ordered_float::NotNan;

    use super::*;
    use crate::query::ast::BinaryOp;
    use postgres_types::Type;

    use crate::catalog::TextOrder;
    use crate::query::ast::LiteralValue;
    use crate::query::constraints::value::int_float_cmp;
    use crate::query::typed_compare::ColumnCompare;

    fn range_from_comparisons(comparisons: &[(BinaryOp, ConstraintValue)]) -> ColumnRange {
        let tcs: Vec<TableConstraint> = comparisons
            .iter()
            .map(|(op, val)| TableConstraint {
                column: "col".into(),
                kind: ColumnConstraintKind::Comparison {
                    op: *op,
                    value: val.clone(),
                },
            })
            .collect();
        let refs: Vec<&TableConstraint> = tcs.iter().collect();
        column_range_build(&refs)
    }

    // ========== column_range_contains: numeric-aware exclusion (PGC-124) ==========

    /// Whether `range` holds `value`, a literal as a write carries it.
    fn contains(range: &ColumnRange, value: &ConstraintValue) -> Option<bool> {
        column_range_contains(range, value.as_ref())
    }

    /// A literal as a write carries it.
    fn int(v: i64) -> ConstraintValue {
        LiteralValue::Integer(v).into()
    }

    fn float(v: f64) -> ConstraintValue {
        LiteralValue::Float(NotNan::new(v).expect("finite float")).into()
    }

    fn plain(s: &str) -> ConstraintValue {
        LiteralValue::String(s.into()).into()
    }

    /// `s` read as a value of a text column whose collation is `order`.
    fn text_value(s: &str, order: TextOrder) -> ConstraintValue {
        ConstraintValue::of_column(
            ColumnCompare::new(&Type::TEXT, order),
            LiteralValue::String(s.into()),
        )
    }

    #[test]
    fn test_contains_equal_same_type() {
        let range = ColumnRange::Equal(int(10));
        assert_eq!(contains(&range, &int(10)), Some(true));
        assert_eq!(contains(&range, &int(20)), Some(false));
    }

    #[test]
    fn test_contains_equal_int_float_match() {
        // `WHERE id = 10` against a pending `10.0` must read as equal, so the
        // row is NOT excluded (the read intersects and forwards).
        assert_eq!(
            contains(&ColumnRange::Equal(int(10)), &float(10.0)),
            Some(true)
        );
        assert_eq!(
            contains(&ColumnRange::Equal(float(10.0)), &int(10)),
            Some(true)
        );
    }

    #[test]
    fn test_contains_equal_int_float_disjoint() {
        // Provably unequal across the type mismatch → excluded (read serves).
        assert_eq!(
            contains(&ColumnRange::Equal(int(10)), &float(20.0)),
            Some(false)
        );
        assert_eq!(
            contains(&ColumnRange::Equal(int(10)), &float(10.5)),
            Some(false)
        );
        // A fractional value near the integer is still provably unequal, both
        // directions (`10` vs `10.1` and `10.1` vs `10`).
        assert_eq!(
            contains(&ColumnRange::Equal(int(10)), &float(10.1)),
            Some(false)
        );
        assert_eq!(
            contains(&ColumnRange::Equal(float(10.1)), &int(10)),
            Some(false)
        );
        assert_eq!(int_float_cmp(10, 10.1), Ordering::Less);
        assert_eq!(int_float_cmp(10, 9.9), Ordering::Greater);
    }

    #[test]
    fn test_contains_equal_cross_type_undecidable() {
        // A text literal vs an integer value can't be proven equal or unequal.
        assert_eq!(contains(&ColumnRange::Equal(plain("2")), &int(2)), None);
    }

    #[test]
    fn test_contains_inset_int_float() {
        let set = ColumnRange::InSet(HashSet::from([int(1), int(2), int(3)]));
        assert_eq!(contains(&set, &float(2.0)), Some(true));
        assert_eq!(contains(&set, &float(5.0)), Some(false));
        assert_eq!(contains(&set, &int(3)), Some(true));
    }

    #[test]
    fn test_contains_inset_incomparable_element_undecidable() {
        // A mixed-type set with an element incomparable to the value leaves
        // membership undecidable rather than falsely excluding.
        let set = ColumnRange::InSet(HashSet::from([plain("x"), int(1)]));
        assert_eq!(contains(&set, &float(9.0)), None);
    }

    #[test]
    fn test_contains_range_int_float_bounds() {
        // `WHERE price > 5`, integer bound, float inserted values.
        let range = range_from_comparisons(&[(BinaryOp::GreaterThan, int(5))]);
        assert_eq!(contains(&range, &float(4.5)), Some(false));
        assert_eq!(contains(&range, &float(5.5)), Some(true));
        assert_eq!(contains(&range, &float(5.0)), Some(false)); // exclusive
    }

    #[test]
    fn test_contains_range_not_equal_int_float() {
        let range = range_from_comparisons(&[
            (BinaryOp::GreaterThanOrEqual, int(0)),
            (BinaryOp::NotEqual, int(7)),
        ]);
        assert_eq!(contains(&range, &float(7.0)), Some(false));
        assert_eq!(contains(&range, &float(8.0)), Some(true));
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

    /// The read side of a disjointness check: `pairs` as a reference's ranges.
    fn read_ranges(pairs: &[(&str, ColumnRange)]) -> TableRanges {
        TableRanges::from_bare(pairs.iter().map(|(c, r)| ((*c).into(), r.clone())))
    }

    #[test]
    fn test_ranges_disjoint_map() {
        // A column constrained by both, disjointly → whole predicates disjoint.
        assert!(column_ranges_disjoint(
            &read_ranges(&[("id", ColumnRange::Equal(int(5)))]),
            &ranges(&[("id", ColumnRange::Equal(int(1)))]),
        ));
        // Same column, overlapping → not disjoint.
        assert!(!column_ranges_disjoint(
            &read_ranges(&[("id", ColumnRange::Equal(int(5)))]),
            &ranges(&[("id", ColumnRange::Equal(int(5)))]),
        ));
        // No shared column → can't establish disjointness.
        assert!(!column_ranges_disjoint(
            &read_ranges(&[("id", ColumnRange::Equal(int(5)))]),
            &ranges(&[("other", ColumnRange::Equal(int(1)))]),
        ));
        // One disjoint shared column is enough, even if another overlaps.
        assert!(column_ranges_disjoint(
            &read_ranges(&[
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
            &TableRanges::default(),
            &ranges(&[("id", ColumnRange::Equal(int(5)))]),
        ));
        // An unsatisfiable (Empty) column range → disjoint from anything.
        assert!(column_ranges_disjoint(
            &read_ranges(&[("id", ColumnRange::Empty)]),
            &ranges(&[("other", ColumnRange::Equal(int(5)))]),
        ));
    }
    /// A read of only NULL rows provably excludes any value, and is disjoint
    /// from every comparison-derived range; a NULL literal, as `SET x = NULL`
    /// arrives, stays undecidable (PGC-533).
    #[test]
    fn test_null_range_contains_and_disjoint() {
        assert_eq!(contains(&ColumnRange::Null, &int(5)), Some(false));
        assert_eq!(
            contains(&ColumnRange::Null, &LiteralValue::Null.into()),
            None
        );
        assert!(column_range_disjoint(
            &ColumnRange::Null,
            &ColumnRange::Equal(int(5))
        ));
        assert!(column_range_disjoint(&ColumnRange::Null, &gt(1)));
        assert!(column_range_disjoint(&ColumnRange::Null, &inset(&[1, 2])));
        assert!(!column_range_disjoint(
            &ColumnRange::Null,
            &ColumnRange::Null
        ));
        assert!(!column_range_disjoint(
            &ColumnRange::Null,
            &ColumnRange::Unconstrained
        ));
        assert!(!column_range_disjoint(
            &ColumnRange::Null,
            &ColumnRange::Equal(LiteralValue::Null.into())
        ));
        assert!(!column_range_disjoint(
            &ColumnRange::Null,
            &ColumnRange::InSet(HashSet::from([int(1), LiteralValue::Null.into()]))
        ));
        // `IS NOT NULL` is the unbounded range: it overlaps every value.
        let not_null = ColumnRange::Range {
            lower: None,
            upper: None,
            not_equal: vec![],
        };
        assert_eq!(contains(&not_null, &int(5)), Some(true));
        assert!(!column_range_disjoint(
            &not_null,
            &ColumnRange::Equal(int(5))
        ));
        assert!(column_range_disjoint(&not_null, &ColumnRange::Null));
    }

    /// PGC-446: byte order is not collation order, so a string's order is
    /// undecided unless it is a value of a byte-ordered column — a false
    /// exclusion would serve stale data.
    #[test]
    fn test_string_ordering_is_undecidable() {
        for (a, b) in [
            (plain("a"), plain("B")),
            (
                text_value("a", TextOrder::Linguistic),
                text_value("B", TextOrder::Linguistic),
            ),
            (plain("a"), text_value("B", TextOrder::Linguistic)),
        ] {
            assert_eq!(a.value_order(&b), None);
            // A text range bound cannot exclude a pending value ('a' < 'B'
            // under en_US even though 'a' > 'B' in bytes).
            let range = ColumnRange::Range {
                lower: None,
                upper: Some(RangeBound {
                    value: b,
                    inclusive: false,
                }),
                not_equal: vec![],
            };
            assert_eq!(contains(&range, &a), None);
        }
    }

    /// A value of a byte-ordered column orders by its bytes, against another
    /// such value and against the plain string a write to the column carries.
    #[test]
    fn test_byte_ordered_text_orders_by_bytes() {
        let ordered = |s: &str| text_value(s, TextOrder::Bytes);
        assert_eq!(
            ordered("a").value_order(&ordered("B")),
            Some(Ordering::Greater)
        );
        assert_eq!(plain("A").value_order(&ordered("B")), Some(Ordering::Less));
        let range = ColumnRange::Range {
            lower: None,
            upper: Some(RangeBound {
                value: ordered("B"),
                inclusive: false,
            }),
            not_equal: vec![],
        };
        assert_eq!(contains(&range, &plain("a")), Some(false));
        assert_eq!(contains(&range, &plain("A")), Some(true));
        assert!(column_range_disjoint(
            &range,
            &ColumnRange::Equal(plain("a"))
        ));
    }

    /// Text equality stays decidable by bytes (sound for deterministic
    /// collations), so text-PK point disjointness is preserved: between a
    /// read's value and a write's string, and between two raw strings.
    #[test]
    fn test_string_equality_still_decides() {
        for read in [plain("a"), text_value("a", TextOrder::Linguistic)] {
            let range = ColumnRange::Equal(read);
            assert_eq!(contains(&range, &plain("a")), Some(true));
            assert_eq!(contains(&range, &plain("b")), Some(false));
        }
    }

    /// A read's value reads a write's literal as a value of its own column:
    /// spellings of one value are equal, and types that order do so by value.
    #[test]
    fn test_typed_value_reads_write_literal_by_value() {
        let value = |ty: &Type, s: &str| {
            ConstraintValue::of_column(
                ColumnCompare::new(ty, TextOrder::Linguistic),
                LiteralValue::String(s.into()),
            )
        };
        // (column type, read literal, write literal naming the same value, one that differs)
        let cases = [
            (Type::NUMERIC, "10.5", "10.50", "10.51"),
            (Type::INT4, "7", "07", "8"),
            (Type::BPCHAR, "a", "a  ", "b"),
            (
                Type::UUID,
                "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
                "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11",
                "b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
            ),
            (
                Type::TIMESTAMP,
                "2024-03-01 00:00:00",
                "2024-03-01",
                "2024-03-02",
            ),
            (Type::BOOL, "t", "yes", "no"),
        ];
        for (ty, read, same, other) in cases {
            let range = ColumnRange::Equal(value(&ty, read));
            assert_eq!(contains(&range, &plain(same)), Some(true), "{ty} {same}");
            assert_eq!(contains(&range, &plain(other)), Some(false), "{ty} {other}");
        }
        let range = ColumnRange::Equal(value(&Type::INT4, "7"));
        assert_eq!(contains(&range, &int(7)), Some(true));
        assert_eq!(contains(&range, &float(7.5)), Some(false));
        assert_eq!(
            contains(&range, &plain("seven")),
            None,
            "unparseable is undecided"
        );

        let after = ColumnRange::Range {
            lower: Some(RangeBound {
                value: value(&Type::DATE, "2024-01-01"),
                inclusive: false,
            }),
            upper: None,
            not_equal: vec![],
        };
        assert_eq!(contains(&after, &plain("2024-06-01")), Some(true));
        assert_eq!(contains(&after, &plain("2023-06-01")), Some(false));
    }
}
