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

use crate::query::ast::{BinaryOp, LiteralValue};
use crate::query::cast::CastTarget;
use crate::query::resolve::{Bindings, ColumnBinding, TableSlot};

mod range;
#[cfg(feature = "proxy")]
mod range_overlap;
mod subsume;
#[cfg(test)]
mod tests;
mod tree;

pub use subsume::table_constraints_subsumed;
pub use tree::query_constraints_analyze;

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

/// An equality in an outer join's ON clause between a preserved-side and a
/// nullable-side column. Every preserved row survives the join, so a
/// constraint crosses it from the preserved side only.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OuterJoinEquality {
    pub preserved: ConstraintColumn,
    pub nullable: ConstraintColumn,
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

    /// Preserved-to-nullable equalities from outer-join ON clauses.
    pub outer_equalities: HashSet<OuterJoinEquality>,

    /// Preserved-side columns an outer join's ON clause references outside
    /// `outer_equalities`. They filter no preserved row, but a change to one
    /// can attach a nullable-side row, so they count as join columns.
    pub outer_match_columns: HashSet<ConstraintColumn>,

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
    /// full scan and incorrectly subsumed unrelated queries. ON clauses
    /// don't count: subsumption admission rules out any join that could
    /// drop a parent's rows.
    pub where_analysis_complete: bool,
}

impl Default for QueryConstraints {
    fn default() -> Self {
        Self {
            column_constraints: HashSet::new(),
            equivalences: HashSet::new(),
            outer_equalities: HashSet::new(),
            outer_match_columns: HashSet::new(),
            table_constraints: HashMap::new(),
            where_analysis_complete: true,
        }
    }
}

impl QueryConstraints {
    /// Returns column names involved in join conditions for the given table
    pub fn table_join_columns<'a>(&'a self, table_name: &'a str) -> impl Iterator<Item = &'a str> {
        let equivalence_columns = self
            .equivalences
            .iter()
            .filter(|eq| eq.is_join())
            .flat_map(|eq| [&eq.left, &eq.right])
            .filter(move |column| column.table == table_name)
            .map(|column| column.column.as_str());
        let outer_columns = self
            .outer_equalities
            .iter()
            .flat_map(|eq| [&eq.preserved, &eq.nullable])
            .chain(&self.outer_match_columns)
            .filter(move |column| column.table == table_name)
            .map(|column| column.column.as_str());
        equivalence_columns.chain(outer_columns)
    }
}

/// `constraints` closed under the equalities: each equivalence carries a
/// constraint both ways, each outer-join equality from its preserved column
/// to its nullable one only. Nothing propagates from a `repeated_tables`
/// column: its references share one column identity, so a constraint on one
/// would cross an equality the other is in.
pub(super) fn propagate_constraints(
    mut constraints: HashSet<ColumnConstraint>,
    equivalences: &HashSet<ColumnEquivalence>,
    outer_equalities: &HashSet<OuterJoinEquality>,
    repeated_tables: &HashSet<EcoString>,
) -> HashSet<ColumnConstraint> {
    let edges: Vec<(&ConstraintColumn, &ConstraintColumn)> = equivalences
        .iter()
        .flat_map(|equiv| [(&equiv.left, &equiv.right), (&equiv.right, &equiv.left)])
        .chain(
            outer_equalities
                .iter()
                .map(|equality| (&equality.preserved, &equality.nullable)),
        )
        .filter(|(from, _)| !repeated_tables.contains(&from.table))
        .collect();
    // Fixpoint iteration: propagate until no changes
    loop {
        let new_constraints: Vec<ColumnConstraint> = edges
            .iter()
            .flat_map(|&(from, to)| {
                constraints
                    .iter()
                    .filter(move |constraint| constraint.column() == from)
                    .filter_map(move |constraint| constraint.restated_on(to))
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

/// `repeated_tables` are names more than one reference binds: their
/// constraints are keyed by name, so they hold for no single reference and
/// are left out, which marks the analysis incomplete.
pub(super) fn query_constraints_build(
    constraints: HashSet<ColumnConstraint>,
    equivalences: HashSet<ColumnEquivalence>,
    outer_equalities: HashSet<OuterJoinEquality>,
    outer_match_columns: HashSet<ConstraintColumn>,
    repeated_tables: &HashSet<EcoString>,
    mut where_analysis_complete: bool,
) -> QueryConstraints {
    let column_constraints = propagate_constraints(
        constraints,
        &equivalences,
        &outer_equalities,
        repeated_tables,
    );
    let mut table_constraints: HashMap<EcoString, Vec<TableConstraint>> = HashMap::new();
    for constraint in &column_constraints {
        if repeated_tables.contains(&constraint.column().table) {
            where_analysis_complete = false;
            continue;
        }
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
        outer_equalities,
        outer_match_columns,
        table_constraints,
        where_analysis_complete,
    }
}

impl ColumnConstraint {
    /// This constraint restated on `other`, a column equal to its own.
    fn restated_on(&self, other: &ConstraintColumn) -> Option<ColumnConstraint> {
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
