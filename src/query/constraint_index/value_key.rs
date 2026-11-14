//! The canonical bucket key, and the classification that routes a
//! `ColumnRange` to its sub-index.

use ecow::EcoString;
use ordered_float::NotNan;

use crate::query::ast::LiteralValue;
use crate::query::constraints::{ColumnRange, ConstraintValue};

/// Which `ColumnIndex` sub-structure a column range belongs in.
pub(super) enum Placement {
    Eq(ValueKey),
    /// Every member's key, in set iteration order.
    InSet(Vec<ValueKey>),
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
pub(super) fn placement(range: &ColumnRange) -> Placement {
    match range {
        ColumnRange::Equal(v) => ValueKey::try_new(v).map_or(Placement::Opaque, Placement::Eq),
        // All members must be keyable for the inverted `inset` index; one
        // unkeyable member routes the whole constraint to `opaque`
        // (deterministic, so insert and remove agree).
        ColumnRange::InSet(set) => set
            .iter()
            .map(ValueKey::try_new)
            .collect::<Option<Vec<_>>>()
            .map_or(Placement::Opaque, Placement::InSet),
        ColumnRange::Range { lower, upper, .. } => {
            match (lower, upper) {
                (Some(lb), None) => ValueKey::try_new_bound(&lb.value)
                    .map_or(Placement::Opaque, Placement::RangeLower),
                (None, Some(ub)) => ValueKey::try_new_bound(&ub.value)
                    .map_or(Placement::Opaque, Placement::RangeUpper),
                (Some(lb), Some(ub)) => {
                    match (
                        ValueKey::try_new_bound(&lb.value),
                        ValueKey::try_new_bound(&ub.value),
                    ) {
                        (Some(lower), Some(upper)) => Placement::RangeBoth { lower, upper },
                        _ => Placement::Opaque,
                    }
                }
                (None, None) => Placement::Opaque,
            }
        }
        ColumnRange::Unknown
        | ColumnRange::Unconstrained
        | ColumnRange::Empty
        | ColumnRange::Null => Placement::Opaque,
    }
}

/// Canonical, totally-ordered bucket key. Collapses `Integer(n)` and
/// `Float(n)` to a single numeric key so a row value coerced to either variant
/// probes the same bucket — load-bearing for the point probe, where a missed
/// entry is a stale read, not just a lost optimization. Integers past 2^53 may
/// share one `f64`, which only ever over-returns (the caller's precise check
/// rejects); it never drops a true match.
///
/// A typed value keys by its index-key form and a raw string by its
/// content, so a row's wire text meets the entry whatever either was
/// spelled; a range bound also has to order ([`ValueKey::try_new_bound`]).
/// Non-keyable
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
    /// The bucket key of a value: a typed one by its index-key form, a raw
    /// literal by what it is. An opaque value, a parameter and NULL have
    /// none.
    pub(super) fn try_new(v: &ConstraintValue) -> Option<Self> {
        match v {
            ConstraintValue::Typed(value) => Self::of_literal(&value.index_key()?),
            ConstraintValue::Raw(literal) => Self::of_literal(literal),
            ConstraintValue::Opaque(_) => None,
        }
    }

    fn of_literal(v: &LiteralValue) -> Option<Self> {
        match v {
            // Integers past 2^53 may share an f64 — see the type doc; only
            // over-returns, never drops a true match.
            #[allow(clippy::cast_precision_loss)]
            LiteralValue::Integer(n) => NotNan::new(*n as f64).ok().map(ValueKey::Num),
            LiteralValue::Float(f) => Some(ValueKey::Num(*f)),
            LiteralValue::String(s) | LiteralValue::StringWithCast(s, _) => {
                Some(ValueKey::Str(s.clone()))
            }
            LiteralValue::Boolean(b) => Some(ValueKey::Bool(*b)),
            LiteralValue::Null
            | LiteralValue::NullWithCast(_)
            | LiteralValue::Parameter(_)
            | LiteralValue::SessionTerm(_)
            | LiteralValue::Array(..) => None,
        }
    }

    /// The key of a range bound, for the ordered range maps, which walk
    /// `Str` keys in byte order. That is the order of the column's values
    /// only where the column orders locally (PGC-446): a typed bound that
    /// does not, and a raw string whose collation is unknown, have no key,
    /// so the range goes to the opaque fallback and the precise check
    /// decides.
    pub(super) fn try_new_bound(v: &ConstraintValue) -> Option<Self> {
        match v {
            ConstraintValue::Typed(value) => value.orders().then(|| Self::try_new(v))?,
            ConstraintValue::Raw(LiteralValue::String(_) | LiteralValue::StringWithCast(..))
            | ConstraintValue::Opaque(_) => None,
            ConstraintValue::Raw(_) => Self::try_new(v),
        }
    }
}
