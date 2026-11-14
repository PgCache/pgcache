//! The value a constraint compares its column against, and how two of them
//! compare. A read's literal is parsed once as its column's value; a write's
//! is kept as written, since the proxy classifies writes without a catalog,
//! and takes its type from the read value it meets.

use std::cmp::Ordering;

#[cfg(feature = "proxy")]
use crate::query::ast::BinaryOp;
use crate::query::ast::LiteralValue;
#[cfg(feature = "proxy")]
use crate::query::typed_compare::typed_compare;
use crate::query::typed_compare::{ColumnCompare, ColumnValue};

/// What a constraint compares its column against.
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum ConstraintValue {
    /// A literal parsed as its column's value: equal values are equal, and
    /// they order where the column's values do.
    Typed(ColumnValue),
    /// A literal whose value in its column is not known (it does not parse
    /// as the column's type, or the type is one the comparator does not
    /// read). Nothing is decided about it, so no range is built over it.
    Opaque(LiteralValue),
    /// A literal as written, its column's type unknown or beside the point:
    /// a write's, a value in a cast's domain, an index key, a parameter or
    /// NULL. Numbers compare as numbers, and strings equal by bytes.
    Raw(LiteralValue),
}

impl std::fmt::Debug for ConstraintValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Typed(value) => value.fmt(f),
            Self::Opaque(literal) => write!(f, "Opaque({literal:?})"),
            Self::Raw(literal) => literal.fmt(f),
        }
    }
}

impl From<LiteralValue> for ConstraintValue {
    fn from(literal: LiteralValue) -> Self {
        Self::Raw(literal)
    }
}

impl ConstraintValue {
    /// `literal` compared to a column that compares as `column`: its value
    /// there when it parses, a parameter or NULL as written, anything else
    /// opaque.
    pub fn of_column(column: ColumnCompare, literal: LiteralValue) -> Self {
        if literal_is_incomparable(&literal) {
            return Self::Raw(literal);
        }
        match ColumnValue::parse(column, &literal) {
            Some(value) => Self::Typed(value),
            None => Self::Opaque(literal),
        }
    }

    /// The literal of a value kept as written.
    pub fn as_raw(&self) -> Option<&LiteralValue> {
        match self {
            Self::Raw(literal) => Some(literal),
            Self::Typed(_) | Self::Opaque(_) => None,
        }
    }

    /// [`Self::as_raw`], mutably: where a write's bind values are filled in.
    pub fn as_raw_mut(&mut self) -> Option<&mut LiteralValue> {
        match self {
            Self::Raw(literal) => Some(literal),
            Self::Typed(_) | Self::Opaque(_) => None,
        }
    }

    /// `row_text op self` for a present (non-NULL) value of a column that
    /// compares as `column`; `None` when undecided, which every caller
    /// takes on its conservative side.
    #[cfg(feature = "proxy")]
    pub(crate) fn row_compare(
        &self,
        column: ColumnCompare,
        row_text: &str,
        op: BinaryOp,
    ) -> Option<bool> {
        match self {
            Self::Typed(value) => value.row_compare(row_text, op),
            Self::Opaque(_) => None,
            Self::Raw(literal) => typed_compare(column, literal, row_text, op),
        }
    }

    /// Whether no range can be built over the value: a parameter, a session
    /// term, NULL, or a literal of unknown value.
    pub(crate) fn is_incomparable(&self) -> bool {
        self.as_ref().is_incomparable()
    }

    pub(crate) fn as_ref(&self) -> ValueRef<'_> {
        match self {
            Self::Typed(value) => ValueRef::Typed(value),
            Self::Opaque(_) => ValueRef::Opaque,
            Self::Raw(literal) => ValueRef::Raw(literal),
        }
    }

    /// [`ValueRef::value_order`] of the two.
    pub(crate) fn value_order(&self, other: &Self) -> Option<Ordering> {
        self.as_ref().value_order(other.as_ref())
    }

    /// A total order for canonicalization only (deterministic set order and
    /// dedup), never a claim about the values: undecided pairs order as
    /// equal.
    pub(crate) fn canonical_order(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Typed(a), Self::Typed(b)) => a.canonical_order(b),
            (Self::Raw(a), Self::Raw(b)) => literal_canonical_order(a, b),
            _ => Ordering::Equal,
        }
    }
}

/// A [`ConstraintValue`] by reference, so a write's literal is compared
/// where it sits instead of being wrapped.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ValueRef<'a> {
    Typed(&'a ColumnValue),
    Opaque,
    Raw(&'a LiteralValue),
}

impl ValueRef<'_> {
    pub(crate) fn is_incomparable(self) -> bool {
        match self {
            Self::Typed(_) => false,
            Self::Opaque => true,
            Self::Raw(literal) => literal_is_incomparable(literal),
        }
    }

    /// Whether the two are provably equal (`Some(true)`), provably unequal
    /// (`Some(false)`) or undecided. A typed value reads a raw one as a
    /// value of its own column.
    #[cfg(feature = "proxy")]
    pub(crate) fn value_eq(self, other: Self) -> Option<bool> {
        match (self, other) {
            (Self::Typed(a), Self::Typed(b)) => a.value_eq(b),
            (Self::Typed(typed), Self::Raw(literal)) | (Self::Raw(literal), Self::Typed(typed)) => {
                typed.value_eq(&typed.literal_parse(literal)?)
            }
            (Self::Raw(a), Self::Raw(b)) => literal_eq(a, b),
            (Self::Opaque, _) | (_, Self::Opaque) => None,
        }
    }

    /// The order of the two, when their column has one that bytes decide.
    pub(crate) fn value_order(self, other: Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Typed(a), Self::Typed(b)) => a.value_order(b),
            (Self::Typed(typed), Self::Raw(literal)) => {
                typed.value_order(&typed.literal_parse(literal)?)
            }
            (Self::Raw(literal), Self::Typed(typed)) => {
                typed.literal_parse(literal)?.value_order(typed)
            }
            (Self::Raw(a), Self::Raw(b)) => literal_order(a, b),
            (Self::Opaque, _) | (_, Self::Opaque) => None,
        }
    }
}

/// Parameters, session terms and NULL: no value to compare.
pub(crate) fn literal_is_incomparable(literal: &LiteralValue) -> bool {
    matches!(
        literal,
        LiteralValue::Parameter(_)
            | LiteralValue::SessionTerm(_)
            | LiteralValue::Null
            | LiteralValue::NullWithCast(_)
    )
}

/// Exact comparison of an integer against a float. Casting the `i64` to
/// `f64` would lose precision past 2^53 and could fabricate an inequality,
/// so the float's integer part is compared as an integer and the fraction
/// breaks the tie; a float beyond the `i64` range is decided by its sign.
pub(super) fn int_float_cmp(i: i64, f: f64) -> Ordering {
    // 2^63 is exactly representable in f64.
    let two_pow_63 = 2.0_f64.powi(63);
    if f >= two_pow_63 {
        return Ordering::Less;
    }
    if f < -two_pow_63 {
        return Ordering::Greater;
    }
    let floor = f.floor();
    // `floor` is within `i64` after the range guards above.
    #[allow(clippy::cast_possible_truncation)]
    let floor_int = floor as i64;
    match i.cmp(&floor_int) {
        Ordering::Less => Ordering::Less,
        Ordering::Greater => Ordering::Greater,
        Ordering::Equal if f == floor => Ordering::Equal,
        Ordering::Equal => Ordering::Less,
    }
}

/// Raw literals: numbers by value, strings and booleans equal by bytes, any
/// other pairing undecided.
#[cfg(feature = "proxy")]
fn literal_eq(a: &LiteralValue, b: &LiteralValue) -> Option<bool> {
    match (a, b) {
        (LiteralValue::String(a), LiteralValue::String(b)) => Some(a == b),
        (LiteralValue::StringWithCast(a, _), LiteralValue::StringWithCast(b, _)) => Some(a == b),
        (LiteralValue::Boolean(a), LiteralValue::Boolean(b)) => Some(a == b),
        _ => literal_order(a, b).map(|order| order == Ordering::Equal),
    }
}

/// Raw literals order only as numbers: a string's order is its column's
/// collation's, which a raw literal does not know (PGC-446).
fn literal_order(a: &LiteralValue, b: &LiteralValue) -> Option<Ordering> {
    match (a, b) {
        (LiteralValue::Integer(a), LiteralValue::Integer(b)) => Some(a.cmp(b)),
        (LiteralValue::Float(a), LiteralValue::Float(b)) => Some(a.cmp(b)),
        (LiteralValue::Integer(i), LiteralValue::Float(f)) => {
            Some(int_float_cmp(*i, (*f).into_inner()))
        }
        (LiteralValue::Float(f), LiteralValue::Integer(i)) => {
            Some(int_float_cmp(*i, (*f).into_inner()).reverse())
        }
        _ => None,
    }
}

fn literal_canonical_order(a: &LiteralValue, b: &LiteralValue) -> Ordering {
    match (a, b) {
        (LiteralValue::String(a), LiteralValue::String(b)) => a.cmp(b),
        (LiteralValue::StringWithCast(a, _), LiteralValue::StringWithCast(b, _)) => a.cmp(b),
        _ => literal_order(a, b).unwrap_or(Ordering::Equal),
    }
}
