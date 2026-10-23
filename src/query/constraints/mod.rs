//! The column constraint vocabulary and the value-domain range algebra over
//! it.
//!
//! * [`range`] — reduces a column's constraints to a [`ColumnRange`] and
//!   decides containment between ranges, also consumed by
//!   `query::constraint_index`.
//! * [`subsume`] — whether a parent's region holds every row a reference's
//!   filters admit, column by column.
//! * `range_overlap` — read-after-write containment/disjointness over the same
//!   ranges; proxy-only.
//!
//! The constraints themselves come from `query::predicates`, projected to
//! column names.

use ecow::EcoString;

use crate::query::ast::{BinaryOp, LiteralValue};
use crate::query::cast::CastTarget;

mod range;
#[cfg(feature = "proxy")]
mod range_overlap;
mod subsume;

pub use subsume::table_constraints_subsumed;

pub(crate) use range::{
    ColumnRange, column_range_build, literal_value_canonical_order, literal_value_is_incomparable,
    literal_value_order,
};
#[cfg(feature = "proxy")]
pub(crate) use range_overlap::{
    column_range_contains, column_ranges_disjoint, table_column_ranges,
};

/// A constraint on one column, keyed by `C`: a column binding inside the
/// predicate model, a column name once projected for a table's consumers.
/// The constraints in one list are AND-connected.
#[derive(Debug, Clone, PartialEq)]
pub struct Constraint<C> {
    pub column: C,
    pub kind: ColumnConstraintKind,
}

impl<C> Constraint<C> {
    /// The same constraint keyed by `f(column)`.
    pub fn map_column<D>(self, f: impl FnOnce(C) -> D) -> Constraint<D> {
        Constraint {
            column: f(self.column),
            kind: self.kind,
        }
    }
}

/// A constraint by column name, as subsumption, the constraint index,
/// read-after-write and CDC row matching see it.
pub type TableConstraint = Constraint<EcoString>;

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnConstraintKind {
    /// `column op value`.
    Comparison { op: BinaryOp, value: LiteralValue },
    /// Set membership; the values are sorted and distinct.
    AnyOf(Vec<LiteralValue>),
    /// A comparison in the cast's output domain, so subsumption buckets it by
    /// `(column, cast)` apart from bare comparisons on the same column.
    CastComparison {
        cast: CastTarget,
        op: BinaryOp,
        value: LiteralValue,
    },
    /// `column IS [NOT] NULL`.
    NullTest(NullTest),
}

impl ColumnConstraintKind {
    /// The cast whose output domain the constraint compares in.
    pub fn cast_target(&self) -> Option<&CastTarget> {
        match self {
            ColumnConstraintKind::CastComparison { cast, .. } => Some(cast),
            ColumnConstraintKind::Comparison { .. }
            | ColumnConstraintKind::AnyOf(_)
            | ColumnConstraintKind::NullTest(_) => None,
        }
    }

    /// Every literal the constraint compares against.
    pub fn literals_mut(&mut self) -> impl Iterator<Item = &mut LiteralValue> {
        let (single, many): (Option<&mut LiteralValue>, &mut [LiteralValue]) = match self {
            ColumnConstraintKind::Comparison { value, .. }
            | ColumnConstraintKind::CastComparison { value, .. } => (Some(value), &mut []),
            ColumnConstraintKind::AnyOf(values) => (None, values.as_mut_slice()),
            ColumnConstraintKind::NullTest(_) => (None, &mut []),
        };
        single.into_iter().chain(many)
    }
}

/// The polarity of a null test. `IsNull` is the one recognized conjunct that
/// holds on an outer join's NULL padding, so it never reduces a join.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullTest {
    IsNull,
    IsNotNull,
}
