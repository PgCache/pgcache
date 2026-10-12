//! The column constraints and equivalences a SELECT's predicates imply,
//! read from the plain tree and its bindings.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use ecow::EcoString;

use super::range::{
    literal_value_canonical_order, literal_value_is_incomparable, literal_value_order,
};
use super::{
    ColumnConstraint, ColumnEquivalence, ConstraintColumn, QueryConstraints, TableConstraint,
};
use crate::query::ast::{
    BinaryExpr, BinaryOp, JoinQual, LiteralValue, MultiExpr, MultiOp, ScalarExpr, SelectNode,
    TableSource, WhereExpr,
};
use crate::query::cast::{
    cast_target_is_coercion_supported, comparison_canonicalize, where_scalar_leaf,
};
use crate::query::resolve::ResolvedQuery;

/// Accumulates the constraints and equivalences a query's predicates imply.
///
/// `complete` is set to `false` whenever the analyzer drops an expression
/// without extracting a constraint from it (`MultiOp::Any`, `OR`,
/// non-comparison binary shapes, subqueries): a cached query with a WHERE
/// clause that could not be fully analysed must not be taken for a full
/// table scan by subsumption.
struct Collector<'a> {
    bound: &'a ResolvedQuery,
    constraints: HashSet<ColumnConstraint>,
    equivalences: HashSet<ColumnEquivalence>,
    complete: bool,
}

/// The constraints of `select`, a SELECT of `bound`.
pub fn query_constraints_analyze(bound: &ResolvedQuery, select: &SelectNode) -> QueryConstraints {
    let mut collector = Collector {
        bound,
        constraints: HashSet::new(),
        equivalences: HashSet::new(),
        complete: true,
    };
    if let Some(where_clause) = &select.where_clause {
        collector.where_analyze(where_clause);
    }
    for source in &select.from {
        collector.join_source_analyze(source);
    }
    query_constraints_build(
        collector.constraints,
        collector.equivalences,
        collector.complete,
    )
}

impl Collector<'_> {
    fn column(&self, node: &crate::query::ast::ColumnNode) -> Option<ConstraintColumn> {
        ConstraintColumn::of(self.bound, node)
    }

    /// Equality, inequality, BETWEEN, IN and `= ANY` on column-vs-literal
    /// comparisons, and column equalities, through any depth of AND.
    fn where_analyze(&mut self, expr: &WhereExpr) {
        match expr {
            WhereExpr::Binary(binary) if binary.op.is_comparison() => {
                self.comparison_analyze(binary);
            }
            WhereExpr::Binary(binary) if binary.op == BinaryOp::And => {
                self.where_analyze(&binary.lexpr);
                self.where_analyze(&binary.rexpr);
            }
            WhereExpr::Multi(multi) => self.multi_analyze(multi),
            WhereExpr::Scalar(_)
            | WhereExpr::Unary(_)
            | WhereExpr::Binary(_)
            | WhereExpr::Subquery { .. } => self.complete = false,
        }
    }

    fn comparison_analyze(&mut self, binary: &BinaryExpr) {
        if let Some((node, target, op, value)) = comparison_canonicalize(self.bound, binary) {
            let (Some(column), Some(base)) = (self.column(node), self.bound.column_type(node))
            else {
                self.complete = false;
                return;
            };
            match target {
                None => {
                    self.constraints.insert(ColumnConstraint::Comparison {
                        column,
                        op,
                        value: value.clone(),
                    });
                }
                Some(cast) if cast_target_is_coercion_supported(cast, base) => {
                    self.constraints.insert(ColumnConstraint::CastComparison {
                        column,
                        cast: cast.clone(),
                        op,
                        value: value.clone(),
                    });
                }
                Some(_) => self.complete = false,
            }
            return;
        }
        if let (Some(ScalarExpr::Column(left)), Some(ScalarExpr::Column(right))) = (
            where_scalar_leaf(self.bound, &binary.lexpr),
            where_scalar_leaf(self.bound, &binary.rexpr),
        ) && binary.op == BinaryOp::Equal
            && let (Some(left), Some(right)) = (self.column(left), self.column(right))
        {
            self.equivalences.insert(ColumnEquivalence { left, right });
            return;
        }
        self.complete = false;
    }

    fn multi_analyze(&mut self, multi: &MultiExpr) {
        match multi.op {
            MultiOp::Between | MultiOp::BetweenSymmetric => self.between_extract(multi),
            MultiOp::In => self.in_extract(&multi.exprs),
            MultiOp::NotIn => self.not_in_extract(&multi.exprs),
            // `col = ANY(<array literal>)` is `col IN (<elements>)` for set
            // membership (PGC-106); other comparisons under ANY are not.
            MultiOp::Any {
                comparison: BinaryOp::Equal,
            } => self.any_eq_array_extract(&multi.exprs),
            MultiOp::NotBetween
            | MultiOp::NotBetweenSymmetric
            | MultiOp::Any { .. }
            | MultiOp::All { .. } => self.complete = false,
        }
    }

    /// A join's ON predicate, or the column equalities a `USING` /
    /// `NATURAL` join merges on; nested joins recursively.
    fn join_source_analyze(&mut self, source: &TableSource) {
        let TableSource::Join(join) = source else {
            return;
        };
        match &join.qual {
            JoinQual::On(condition) => self.where_analyze(condition),
            JoinQual::Using(_) | JoinQual::Natural => {
                let bindings = self.bound.bindings();
                for merged in bindings
                    .merged_columns()
                    .iter()
                    .filter(|merged| merged.join.index() == join.id.index())
                {
                    if let (Some(left), Some(right)) = (
                        constraint_column_of_binding(self.bound, &merged.left),
                        constraint_column_of_binding(self.bound, &merged.right),
                    ) {
                        self.equivalences.insert(ColumnEquivalence { left, right });
                    }
                }
            }
            JoinQual::Cross => {}
        }
        self.join_source_analyze(&join.left);
        self.join_source_analyze(&join.right);
    }

    /// `[subject, low, high]` as two inequalities; SYMMETRIC bounds are
    /// normalised to `(min, max)` first.
    fn between_extract(&mut self, multi: &MultiExpr) {
        let [
            WhereExpr::Scalar(ScalarExpr::Column(node)),
            WhereExpr::Scalar(ScalarExpr::Literal(low)),
            WhereExpr::Scalar(ScalarExpr::Literal(high)),
        ] = multi.exprs.as_slice()
        else {
            return;
        };
        let Some(column) = self.column(node) else {
            return;
        };
        let (low, high) = if multi.op == MultiOp::BetweenSymmetric {
            match literal_value_order(low, high) {
                Some(Ordering::Greater) => (high, low),
                Some(_) => (low, high),
                None => return,
            }
        } else {
            (low, high)
        };
        self.constraints.insert(ColumnConstraint::Comparison {
            column: column.clone(),
            op: BinaryOp::GreaterThanOrEqual,
            value: low.clone(),
        });
        self.constraints.insert(ColumnConstraint::Comparison {
            column,
            op: BinaryOp::LessThanOrEqual,
            value: high.clone(),
        });
    }

    /// `[subject, v1, …]`: all values must be comparable literals.
    fn in_extract(&mut self, exprs: &[WhereExpr]) {
        let Some(WhereExpr::Scalar(ScalarExpr::Column(node))) = exprs.first() else {
            return;
        };
        let Some(column) = self.column(node) else {
            return;
        };
        let mut values = Vec::new();
        for expr in exprs.get(1..).unwrap_or_default() {
            let WhereExpr::Scalar(ScalarExpr::Literal(value)) = expr else {
                return;
            };
            if literal_value_is_incomparable(value) {
                return;
            }
            values.push(value.clone());
        }
        values.sort_by(literal_value_canonical_order);
        values.dedup();
        self.constraints
            .insert(ColumnConstraint::InSet { column, values });
    }

    /// `[subject, rhs]` where `rhs` is an array literal or `ARRAY[…]` of
    /// literals; anything else leaves the analysis incomplete.
    fn any_eq_array_extract(&mut self, exprs: &[WhereExpr]) {
        let [
            WhereExpr::Scalar(ScalarExpr::Column(node)),
            WhereExpr::Scalar(rhs),
        ] = exprs
        else {
            self.complete = false;
            return;
        };
        let mut values: Vec<LiteralValue> = match rhs {
            ScalarExpr::Literal(LiteralValue::Array(values, _)) => values.clone(),
            ScalarExpr::Array(elements) => {
                let mut out = Vec::with_capacity(elements.len());
                for element in elements {
                    let ScalarExpr::Literal(value) = element else {
                        self.complete = false;
                        return;
                    };
                    out.push(value.clone());
                }
                out
            }
            ScalarExpr::Literal(_)
            | ScalarExpr::Column(_)
            | ScalarExpr::Function(_)
            | ScalarExpr::Case(_)
            | ScalarExpr::Arithmetic(_)
            | ScalarExpr::Subquery(_)
            | ScalarExpr::TypeCast { .. } => {
                self.complete = false;
                return;
            }
        };
        if values.iter().any(literal_value_is_incomparable) {
            self.complete = false;
            return;
        }
        let Some(column) = self.column(node) else {
            self.complete = false;
            return;
        };
        values.sort_by(literal_value_canonical_order);
        values.dedup();
        self.constraints
            .insert(ColumnConstraint::InSet { column, values });
    }

    /// `NOT IN (v1, v2)` as `<> v1 AND <> v2`.
    fn not_in_extract(&mut self, exprs: &[WhereExpr]) {
        let Some(WhereExpr::Scalar(ScalarExpr::Column(node))) = exprs.first() else {
            return;
        };
        let Some(column) = self.column(node) else {
            return;
        };
        for expr in exprs.get(1..).unwrap_or_default() {
            let WhereExpr::Scalar(ScalarExpr::Literal(value)) = expr else {
                return;
            };
            if literal_value_is_incomparable(value) {
                return;
            }
            self.constraints.insert(ColumnConstraint::Comparison {
                column: column.clone(),
                op: BinaryOp::NotEqual,
                value: value.clone(),
            });
        }
    }
}

/// [`ConstraintColumn::of`] for a binding rather than a reference node.
/// The constraint column a binding names; slots are the same in both id
/// spaces, so either bindings table answers.
pub(super) fn constraint_column_of_binding(
    bound: &ResolvedQuery,
    binding: &crate::query::resolve::ColumnBinding,
) -> Option<ConstraintColumn> {
    let bindings = bound.bindings();
    let column = bindings.binding_column(binding)?.name.clone();
    Some(match bindings.slot(binding.slot)? {
        crate::query::resolve::TableSlot::Base { table, alias } => ConstraintColumn {
            schema: table.schema.clone(),
            table: table.name.clone(),
            alias: alias.clone(),
            column,
        },
        crate::query::resolve::TableSlot::Derived { table, .. } => ConstraintColumn {
            schema: ecow::EcoString::new(),
            table: table.name.clone(),
            alias: Some(table.name.clone()),
            column,
        },
    })
}

/// Propagate constraints through column equivalences using fixpoint iteration
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

/// The constraints of a query from what its predicates imply: propagated
/// through the equivalences, then organised by table.
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
