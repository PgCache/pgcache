//! Column constraints by name and the value-domain range algebra over them.
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
    column_range_contains, column_ranges_disjoint, column_ranges_from_comparisons,
    table_column_ranges,
};

/// A constraint clause organized for per-table CDC matching and subsumption.
/// All clauses within a table's Vec are AND-connected.
#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraint {
    /// Single comparison: column op value
    Comparison(EcoString, BinaryOp, LiteralValue),
    /// At least one must match (IN semantics): column = v1 OR column = v2 OR ...
    AnyOf(EcoString, Vec<LiteralValue>),
    /// Cast-output comparison: `column::cast op value`. Stored separately so
    /// subsumption buckets it by `(column, cast)` independently of bare
    /// `Comparison` constraints on the same column.
    CastComparison(EcoString, CastTarget, BinaryOp, LiteralValue),
    /// `column IS [NOT] NULL`.
    NullTest(EcoString, NullTest),
}

/// The polarity of a null test. `IsNull` is the one recognized conjunct that
/// holds on an outer join's NULL padding, so it never reduces a join.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullTest {
    IsNull,
    IsNotNull,
}
