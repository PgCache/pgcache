//! The column constraint vocabulary and the value-domain range algebra over
//! it.
//!
//! * [`range`] — reduces a column's constraints to a [`ColumnRange`] and
//!   decides containment between ranges, also consumed by
//!   `query::constraint_index`.
//! * [`subsume`] — a table's constraints reduced once to per-column
//!   [`TableRanges`], and whether a parent's region holds every row a
//!   reference's ranges admit.
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
mod value;

pub use subsume::{TableRanges, table_ranges_subsumed};

#[cfg(test)]
pub(crate) use subsume::table_constraints_subsumed;

pub(crate) use range::ColumnRange;
#[cfg(test)]
pub(crate) use range::column_range_build;
#[cfg(feature = "proxy")]
pub(crate) use range_overlap::{
    column_range_contains, column_ranges_disjoint, table_column_ranges,
};
pub use value::ConstraintValue;
#[cfg(feature = "proxy")]
pub(crate) use value::ValueRef;
pub(crate) use value::literal_is_incomparable;

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
    Comparison {
        op: BinaryOp,
        value: ConstraintValue,
    },
    /// Set membership; the values are sorted and distinct.
    AnyOf(Vec<ConstraintValue>),
    /// A comparison in the cast's output domain, so subsumption buckets it by
    /// `(column, cast)` apart from bare comparisons on the same column.
    CastComparison {
        cast: CastTarget,
        op: BinaryOp,
        value: ConstraintValue,
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

    /// Every literal the constraint compares against that is kept as
    /// written: all of a write's, whose bind values are filled in here.
    pub fn literals_mut(&mut self) -> impl Iterator<Item = &mut LiteralValue> {
        let (single, many): (Option<&mut ConstraintValue>, &mut [ConstraintValue]) = match self {
            ColumnConstraintKind::Comparison { value, .. }
            | ColumnConstraintKind::CastComparison { value, .. } => (Some(value), &mut []),
            ColumnConstraintKind::AnyOf(values) => (None, values.as_mut_slice()),
            ColumnConstraintKind::NullTest(_) => (None, &mut []),
        };
        single
            .into_iter()
            .chain(many)
            .filter_map(ConstraintValue::as_raw_mut)
    }
}

/// The polarity of a null test. `IsNull` is the one recognized conjunct that
/// holds on an outer join's NULL padding, so it never reduces a join.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullTest {
    IsNull,
    IsNotNull,
}
