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
//! * `range_overlap` — read-after-write containment/disjointness over the same
//!   ranges; proxy-only.
//!
//! This module holds only the shared types.

use std::collections::{HashMap, HashSet};

use ecow::EcoString;

use crate::query::ast::{BinaryOp, LiteralValue, NodeId};
use crate::query::cast::CastTarget;
use crate::query::resolve::{Bindings, ColumnBinding, ResolvedQuery, TableSlot};

mod range;
#[cfg(feature = "proxy")]
mod range_overlap;
mod subsume;
#[cfg(test)]
mod tests;
mod tree;

pub use subsume::table_constraints_subsumed;
pub use tree::query_constraints_analyze_tree;

pub(crate) use range::{ColumnRange, column_range_build};
#[cfg(feature = "proxy")]
pub(crate) use range_overlap::{
    column_range_contains, column_ranges_disjoint, column_ranges_from_comparisons,
    table_column_ranges,
};

/// The column a constraint is on: its table as the query names it. The
/// alias is how the query spells it and no part of its identity, so a
/// constraint propagates across an equivalence whatever the spelling.
#[derive(Debug, Clone)]
pub struct ConstraintColumn {
    pub schema: EcoString,
    pub table: EcoString,
    pub alias: Option<EcoString>,
    pub column: EcoString,
}

impl PartialEq for ConstraintColumn {
    fn eq(&self, other: &Self) -> bool {
        self.schema == other.schema && self.table == other.table && self.column == other.column
    }
}

impl Eq for ConstraintColumn {}

impl std::hash::Hash for ConstraintColumn {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.schema.hash(state);
        self.table.hash(state);
        self.column.hash(state);
    }
}

impl ConstraintColumn {
    /// The column the reference at `id` stands for; `None` for an
    /// outer-join merged column or an output name.
    pub fn at(bound: &ResolvedQuery, id: NodeId) -> Option<Self> {
        Self::of_binding(bound.bindings(), bound.column_binding_at(id)?)
    }

    pub(super) fn of_binding(bindings: &Bindings, binding: ColumnBinding) -> Option<Self> {
        let column = bindings.binding_column(&binding)?.name.clone();
        Some(match bindings.slot(binding.slot)? {
            TableSlot::Base { table, alias } => Self {
                schema: table.schema.clone(),
                table: table.name.clone(),
                alias: alias.clone(),
                column,
            },
            // A derived table is named by its alias and has no schema.
            TableSlot::Derived { table, .. } => Self {
                schema: EcoString::new(),
                table: table.name.clone(),
                alias: Some(table.name.clone()),
                column,
            },
        })
    }
}

/// A column constraint extracted from WHERE/JOIN conditions
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ColumnConstraint {
    /// Single comparison: column op value
    Comparison {
        column: ConstraintColumn,
        op: BinaryOp,
        value: LiteralValue,
    },
    /// Set membership: column IN (v1, v2, ...)
    /// Values are sorted for deterministic Hash/Eq.
    InSet {
        column: ConstraintColumn,
        values: Vec<LiteralValue>,
    },
    /// Comparison through a non-identity cast: `column::cast op value`.
    /// The value lives in the cast-output domain, so subsumption math
    /// must bucket these separately from bare `Comparison` constraints
    /// on the same column.
    CastComparison {
        column: ConstraintColumn,
        cast: CastTarget,
        op: BinaryOp,
        value: LiteralValue,
    },
}

impl ColumnConstraint {
    pub fn column(&self) -> &ConstraintColumn {
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
    pub left: ConstraintColumn,
    pub right: ConstraintColumn,
}

impl ColumnEquivalence {
    /// Returns true if this equivalence represents a join condition:
    /// columns from different tables, or same table with different aliases (self-join)
    pub fn is_join(&self) -> bool {
        self.left.table != self.right.table || self.left.alias != self.right.alias
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

pub(super) fn propagate_constraints(
    mut constraints: HashSet<ColumnConstraint>,
    equivalences: &HashSet<ColumnEquivalence>,
) -> HashSet<ColumnConstraint> {
    // Fixpoint iteration: propagate until no changes
    loop {
        let new_constraints: Vec<ColumnConstraint> = equivalences
            .iter()
            .flat_map(|equiv| {
                constraints
                    .iter()
                    .filter_map(move |constraint| constraint.across(equiv))
            })
            .collect();
        let mut changed = false;
        for constraint in new_constraints {
            changed |= constraints.insert(constraint);
        }
        if !changed {
            return constraints;
        }
    }
}

pub(super) fn query_constraints_build(
    constraints: HashSet<ColumnConstraint>,
    equivalences: HashSet<ColumnEquivalence>,
    where_analysis_complete: bool,
) -> QueryConstraints {
    let column_constraints = propagate_constraints(constraints, &equivalences);
    let mut table_constraints: HashMap<EcoString, Vec<TableConstraint>> = HashMap::new();
    for constraint in &column_constraints {
        let tc = match constraint {
            ColumnConstraint::Comparison {
                column, op, value, ..
            } => TableConstraint::Comparison(column.column.clone(), *op, value.clone()),
            ColumnConstraint::InSet { column, values, .. } => {
                TableConstraint::AnyOf(column.column.clone(), values.clone())
            }
            ColumnConstraint::CastComparison {
                column,
                cast,
                op,
                value,
            } => TableConstraint::CastComparison(
                column.column.clone(),
                cast.clone(),
                *op,
                value.clone(),
            ),
        };
        table_constraints
            .entry(constraint.column().table.clone())
            .or_default()
            .push(tc);
    }

    QueryConstraints {
        column_constraints,
        equivalences,
        table_constraints,
        where_analysis_complete,
    }
}

impl ColumnConstraint {
    /// This constraint copied onto the other column of `equiv`, when it
    /// constrains one of them.
    fn across(&self, equiv: &ColumnEquivalence) -> Option<ColumnConstraint> {
        let other = if *self.column() == equiv.left {
            &equiv.right
        } else if *self.column() == equiv.right {
            &equiv.left
        } else {
            return None;
        };
        match self {
            ColumnConstraint::Comparison { op, value, .. } => Some(ColumnConstraint::Comparison {
                column: other.clone(),
                op: *op,
                value: value.clone(),
            }),
            ColumnConstraint::InSet { values, .. } => Some(ColumnConstraint::InSet {
                column: other.clone(),
                values: values.clone(),
            }),
            // CastComparison doesn't propagate — `val::int = 5 AND val = other_val`
            // does NOT imply `other_val::int = 5` unless we also know
            // `other_val` is cast-compatible. Conservative skip.
            ColumnConstraint::CastComparison { .. } => None,
        }
    }
}
