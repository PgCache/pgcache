//! Value-domain range algebra: reduce a column's constraints to a
//! [`ColumnRange`], and decide whether one range subsumes another.
//!
//! Pure value-level reasoning — no AST. The AST-walking extraction that
//! produces the constraints lives in [`extract`](super::extract); this module
//! is also the range vocabulary consumed by `query::constraint_index`.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use ecow::EcoString;

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

/// Compare two literal values for ordering. Returns None if the values are
/// not comparable (different types, Parameters, Nulls) — and always for
/// strings: byte order does not mirror PostgreSQL collation order (`'a' >
/// 'B'` in bytes but `'a' < 'B'` under en_US), so an ordering claim over text
/// could prove a false exclusion and serve stale data (PGC-446). String
/// *equality* is decided by [`literal_value_eq`] instead.
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

    use ordered_float::NotNan;

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
