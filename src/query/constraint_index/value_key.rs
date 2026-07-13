//! The canonical bucket key, and the classification that routes a
//! `ColumnRange` to its sub-index.

use std::collections::HashSet;

use ecow::EcoString;
use ordered_float::NotNan;

use crate::query::ast::LiteralValue;
use crate::query::constraints::ColumnRange;

/// Which `ColumnIndex` sub-structure a column range belongs in.
pub(super) enum Placement<'a> {
    Eq(ValueKey),
    InSet(&'a HashSet<LiteralValue>),
    RangeLower(ValueKey),
    RangeUpper(ValueKey),
    /// Two-sided range with orderable bounds (PGC-189).
    RangeBoth {
        lower: ValueKey,
        upper: ValueKey,
    },
    Opaque,
}

/// Classify a `ColumnRange` into its sub-index. Single-sided ranges with an
/// orderable bound get a structured bucket; two-sided ranges with orderable
/// bounds land in `range_both`; everything else — `Unknown`/`Empty`/
/// `Unconstrained`, non-orderable bounds — routes to the linear `opaque`
/// fallback.
pub(super) fn placement(range: &ColumnRange) -> Placement<'_> {
    match range {
        ColumnRange::Equal(v) => ValueKey::try_new(v).map_or(Placement::Opaque, Placement::Eq),
        // All members must be keyable for the inverted `inset` index; one
        // unkeyable member routes the whole constraint to `opaque`
        // (deterministic, so insert and remove agree).
        ColumnRange::InSet(set) => {
            if set.iter().all(|v| ValueKey::try_new(v).is_some()) {
                Placement::InSet(set)
            } else {
                Placement::Opaque
            }
        }
        ColumnRange::Range { lower, upper, .. } => match (lower, upper) {
            (Some(lb), None) => {
                ValueKey::try_new(&lb.value).map_or(Placement::Opaque, Placement::RangeLower)
            }
            (None, Some(ub)) => {
                ValueKey::try_new(&ub.value).map_or(Placement::Opaque, Placement::RangeUpper)
            }
            (Some(lb), Some(ub)) => {
                match (ValueKey::try_new(&lb.value), ValueKey::try_new(&ub.value)) {
                    (Some(lower), Some(upper)) => Placement::RangeBoth { lower, upper },
                    _ => Placement::Opaque,
                }
            }
            (None, None) => Placement::Opaque,
        },
        ColumnRange::Unknown | ColumnRange::Unconstrained | ColumnRange::Empty => Placement::Opaque,
    }
}

/// Canonical, totally-ordered bucket key. Collapses `Integer(n)` and
/// `Float(n)` to a single numeric key so a row value coerced to either variant
/// probes the same bucket — load-bearing for the point probe, where a missed
/// entry is a stale read, not just a lost optimization. Integers past 2^53 may
/// share one `f64`, which only ever over-returns (the caller's precise check
/// rejects); it never drops a true match.
///
/// `String` and `StringWithCast` both key by their string content; non-keyable
/// values (`Null`, `Parameter`, `Array`) route to the `opaque` fallback.
/// Derived `Ord` orders by variant first (`Num` < `Str` < `Bool`), then by
/// value — `Bool` never reaches a range `BTreeMap`, and a single column never
/// mixes `Num`/`Str` meaningfully.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(super) enum ValueKey {
    Num(NotNan<f64>),
    Str(EcoString),
    Bool(bool),
}

impl ValueKey {
    pub(super) fn try_new(v: &LiteralValue) -> Option<Self> {
        match v {
            // Integers past 2^53 may share an f64 — see the type doc; only
            // over-returns, never drops a true match.
            #[allow(clippy::cast_precision_loss)]
            LiteralValue::Integer(n) => Some(ValueKey::Num(
                NotNan::new(*n as f64).expect("i64 as f64 is never NaN"),
            )),
            LiteralValue::Float(f) => Some(ValueKey::Num(*f)),
            LiteralValue::String(s) | LiteralValue::StringWithCast(s, _) => {
                Some(ValueKey::Str(s.clone()))
            }
            LiteralValue::Boolean(b) => Some(ValueKey::Bool(*b)),
            LiteralValue::Null
            | LiteralValue::NullWithCast(_)
            | LiteralValue::Parameter(_)
            | LiteralValue::Array(..) => None,
        }
    }
}
