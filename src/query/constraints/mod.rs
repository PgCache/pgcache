//! Constraint extraction and subsumption.
//!
//! Two vocabularies, split accordingly:
//!
//! * [`extract`] — walks a resolved query's AST to produce [`ColumnConstraint`]s
//!   and [`ColumnEquivalence`]s, and is the home of `analyze_query_constraints`.
//! * [`range`] — pure value-domain range algebra over the resulting constraints
//!   ([`ColumnRange`] and its subsumption rules), also consumed by
//!   `query::constraint_index`.
//! * [`subsume`] — reduces both sides to per-column ranges and compares them.
//!
//! This module holds only the shared types.

use std::collections::{HashMap, HashSet};

use ecow::EcoString;

use crate::query::ast::{BinaryOp, LiteralValue};
use crate::query::cast::CastTarget;
use crate::query::resolved::ResolvedColumnNode;

mod extract;
mod range;
mod subsume;
#[cfg(test)]
mod tests;

pub use extract::analyze_query_constraints;
pub use subsume::table_constraints_subsumed;

pub(crate) use range::{ColumnRange, column_range_build};

/// A column constraint extracted from WHERE/JOIN conditions
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ColumnConstraint {
    /// Single comparison: column op value
    Comparison {
        column: ResolvedColumnNode,
        op: BinaryOp,
        value: LiteralValue,
    },
    /// Set membership: column IN (v1, v2, ...)
    /// Values are sorted for deterministic Hash/Eq.
    InSet {
        column: ResolvedColumnNode,
        values: Vec<LiteralValue>,
    },
    /// Comparison through a non-identity cast: `column::cast op value`.
    /// The value lives in the cast-output domain, so subsumption math
    /// must bucket these separately from bare `Comparison` constraints
    /// on the same column.
    CastComparison {
        column: ResolvedColumnNode,
        cast: CastTarget,
        op: BinaryOp,
        value: LiteralValue,
    },
}

impl ColumnConstraint {
    pub fn column(&self) -> &ResolvedColumnNode {
        match self {
            ColumnConstraint::Comparison { column, .. }
            | ColumnConstraint::InSet { column, .. }
            | ColumnConstraint::CastComparison { column, .. } => column,
        }
    }
}

/// An equivalence between two columns
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnEquivalence {
    pub left: ResolvedColumnNode,
    pub right: ResolvedColumnNode,
}

impl ColumnEquivalence {
    /// Returns true if this equivalence represents a join condition:
    /// columns from different tables, or same table with different aliases (self-join)
    pub fn is_join(&self) -> bool {
        self.left.table != self.right.table || self.left.table_alias != self.right.table_alias
    }
}

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
}

/// Analysis results for a query showing all constant constraints
#[derive(Debug, Clone)]
pub struct QueryConstraints {
    /// All column constraints (from WHERE + propagated through JOINs)
    pub column_constraints: HashSet<ColumnConstraint>,

    /// Column equivalences from JOIN conditions and WHERE clause
    pub equivalences: HashSet<ColumnEquivalence>,

    /// Constraints organized by table for quick lookup
    pub table_constraints: HashMap<EcoString, Vec<TableConstraint>>,

    /// True when the analyzer recognized every expression in the WHERE
    /// clause; false if it encountered any expression it can't extract
    /// constraints from (e.g. `MultiOp::Any`, `OR`, `Unary`, function
    /// calls, subqueries). PGC-106: subsumption uses this to distinguish
    /// "no WHERE clause / full table scan" (complete=true, empty
    /// `table_constraints`) from "WHERE clause we couldn't analyze"
    /// (complete=false, empty `table_constraints`). Without this, a
    /// cached query with `WHERE id = ANY(...)` was wrongly treated as a
    /// full scan and incorrectly subsumed unrelated queries.
    pub where_analysis_complete: bool,
}

impl Default for QueryConstraints {
    fn default() -> Self {
        Self {
            column_constraints: HashSet::new(),
            equivalences: HashSet::new(),
            table_constraints: HashMap::new(),
            where_analysis_complete: true,
        }
    }
}

impl QueryConstraints {
    /// Returns column names involved in join conditions for the given table
    pub fn table_join_columns<'a>(&'a self, table_name: &'a str) -> impl Iterator<Item = &'a str> {
        self.equivalences
            .iter()
            .filter(|eq| eq.is_join())
            .filter_map(move |eq| {
                if eq.left.table == table_name {
                    Some(eq.left.column.as_str())
                } else if eq.right.table == table_name {
                    Some(eq.right.column.as_str())
                } else {
                    None
                }
            })
    }
}
