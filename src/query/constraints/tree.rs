//! The column constraints and equivalences a SELECT's predicates imply,
//! read from the rope and its bindings: the walk of `bound.rs` over views.

use std::cmp::Ordering;
use std::collections::HashSet;

use super::bound::{constraint_column_of_binding, query_constraints_build};
use super::range::{
    literal_value_canonical_order, literal_value_is_incomparable, literal_value_order,
};
use super::{ColumnConstraint, ColumnEquivalence, ConstraintColumn, QueryConstraints};
use crate::query::ast::rope::{
    Binary, Column, JoinQual, Literal, Multi, Run, Scalar, ScalarKind, Select, Source, Where,
    WhereKind,
};
use crate::query::ast::{BinaryOp, LiteralValue, MultiOp};
use crate::query::cast::{
    CastTarget, cast_target_is_coercion_supported, comparison_canonicalize_tree, scalar_leaf_tree,
};
use crate::query::resolve::ResolvedQuery;

struct Collector<'a> {
    bound: &'a ResolvedQuery,
    constraints: HashSet<ColumnConstraint>,
    equivalences: HashSet<ColumnEquivalence>,
    complete: bool,
}

/// The constraints of `select`, a SELECT of `bound`'s rope.
pub fn query_constraints_analyze_tree(
    bound: &ResolvedQuery,
    select: Select<'_>,
) -> QueryConstraints {
    let mut collector = Collector {
        bound,
        constraints: HashSet::new(),
        equivalences: HashSet::new(),
        complete: true,
    };
    if let Some(where_clause) = select.where_clause() {
        collector.where_analyze(where_clause);
    }
    for source in select.from() {
        collector.join_source_analyze(source);
    }
    query_constraints_build(
        collector.constraints,
        collector.equivalences,
        collector.complete,
    )
}

/// The column and literal of `exprs[index]` when it is a bare column or
/// literal, as the extractors pattern-match the owned list.
enum Operand<'t> {
    Column(Column<'t>),
    Literal(Literal<'t>),
    Other,
}

fn operand(expr: Where<'_>) -> Operand<'_> {
    match expr.kind() {
        WhereKind::Scalar(scalar) => match scalar.kind() {
            ScalarKind::Column(column) => Operand::Column(column),
            ScalarKind::Literal(literal) => Operand::Literal(literal),
            ScalarKind::Function(_)
            | ScalarKind::Case(_)
            | ScalarKind::Arithmetic(_)
            | ScalarKind::Subquery(_)
            | ScalarKind::Array(_)
            | ScalarKind::TypeCast(_) => Operand::Other,
        },
        WhereKind::Unary(_)
        | WhereKind::Binary(_)
        | WhereKind::Multi(_)
        | WhereKind::Subquery(_) => Operand::Other,
    }
}

impl Collector<'_> {
    fn column(&self, column: Column<'_>) -> Option<ConstraintColumn> {
        ConstraintColumn::at(self.bound, column.id())
    }

    fn where_analyze(&mut self, expr: Where<'_>) {
        match expr.kind() {
            WhereKind::Binary(binary) if binary.op().is_comparison() => {
                self.comparison_analyze(binary);
            }
            WhereKind::Binary(binary) if binary.op() == BinaryOp::And => {
                self.where_analyze(binary.lexpr());
                self.where_analyze(binary.rexpr());
            }
            WhereKind::Multi(multi) => self.multi_analyze(multi),
            WhereKind::Scalar(_)
            | WhereKind::Unary(_)
            | WhereKind::Binary(_)
            | WhereKind::Subquery(_) => self.complete = false,
        }
    }

    fn comparison_analyze(&mut self, binary: Binary<'_>) {
        if let Some((column, target, op, value)) = comparison_canonicalize_tree(self.bound, binary)
        {
            self.canonical_insert(column, target, op, value);
        } else if let Some((left, right)) = self.column_pair(binary) {
            self.equivalences.insert(ColumnEquivalence { left, right });
        } else {
            self.complete = false;
        }
    }

    /// `column op value`, or `column::cast op value` under a supported
    /// coercion; any other cast leaves the analysis incomplete.
    fn canonical_insert(
        &mut self,
        column: Column<'_>,
        target: Option<&CastTarget>,
        op: BinaryOp,
        value: &LiteralValue,
    ) {
        let (Some(constraint_column), Some(base)) =
            (self.column(column), self.bound.column_type_at(column.id()))
        else {
            self.complete = false;
            return;
        };
        match target {
            None => {
                self.constraints.insert(ColumnConstraint::Comparison {
                    column: constraint_column,
                    op,
                    value: value.clone(),
                });
            }
            Some(cast) if cast_target_is_coercion_supported(cast, base) => {
                self.constraints.insert(ColumnConstraint::CastComparison {
                    column: constraint_column,
                    cast: cast.clone(),
                    op,
                    value: value.clone(),
                });
            }
            Some(_) => self.complete = false,
        }
    }

    /// The two columns of a `column = column` equality.
    fn column_pair(&self, binary: Binary<'_>) -> Option<(ConstraintColumn, ConstraintColumn)> {
        if binary.op() != BinaryOp::Equal {
            return None;
        }
        let left = scalar_leaf_tree(self.bound, binary.lexpr())?;
        let right = scalar_leaf_tree(self.bound, binary.rexpr())?;
        let (ScalarKind::Column(left), ScalarKind::Column(right)) = (left.kind(), right.kind())
        else {
            return None;
        };
        Some((self.column(left)?, self.column(right)?))
    }

    fn multi_analyze(&mut self, multi: Multi<'_>) {
        match multi.op() {
            MultiOp::Between | MultiOp::BetweenSymmetric => self.between_extract(multi),
            MultiOp::In => self.in_extract(multi.exprs()),
            MultiOp::NotIn => self.not_in_extract(multi.exprs()),
            // `col = ANY(<array literal>)` is `col IN (<elements>)` for set
            // membership (PGC-106); other comparisons under ANY are not.
            MultiOp::Any {
                comparison: BinaryOp::Equal,
            } => self.any_eq_array_extract(multi.exprs()),
            MultiOp::NotBetween
            | MultiOp::NotBetweenSymmetric
            | MultiOp::Any { .. }
            | MultiOp::All { .. } => self.complete = false,
        }
    }

    fn join_source_analyze(&mut self, source: Source<'_>) {
        let Source::Join(join) = source else {
            return;
        };
        match join.qual() {
            JoinQual::On(condition) => self.where_analyze(condition),
            JoinQual::Using(_) | JoinQual::Natural => {
                let bindings = self.bound.bindings();
                let join_id = join.id();
                for merged in bindings
                    .merged_columns()
                    .iter()
                    .filter(|merged| merged.join.index() == join_id.index())
                {
                    if let (Some(left), Some(right)) = (
                        constraint_column_of_binding(bindings, &merged.left),
                        constraint_column_of_binding(bindings, &merged.right),
                    ) {
                        self.equivalences.insert(ColumnEquivalence { left, right });
                    }
                }
            }
            JoinQual::Cross => {}
        }
        self.join_source_analyze(join.left());
        self.join_source_analyze(join.right());
    }

    fn between_extract(&mut self, multi: Multi<'_>) {
        let mut exprs = multi.exprs();
        let (Some(first), Some(second), Some(third), None) =
            (exprs.next(), exprs.next(), exprs.next(), exprs.next())
        else {
            return;
        };
        let (Operand::Column(column), Operand::Literal(low), Operand::Literal(high)) =
            (operand(first), operand(second), operand(third))
        else {
            return;
        };
        let Some(column) = self.column(column) else {
            return;
        };
        let (low, high) = (low.value(), high.value());
        let (low, high) = if multi.op() == MultiOp::BetweenSymmetric {
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

    /// The subject column and the literal values after it, when the
    /// operands are exactly that; `None` otherwise.
    fn column_and_literals<'t>(
        &self,
        mut exprs: Run<'t, Where<'t>>,
    ) -> Option<(ConstraintColumn, Vec<LiteralValue>)> {
        let Some(Operand::Column(column)) = exprs.next().map(operand) else {
            return None;
        };
        let column = self.column(column)?;
        let mut values = Vec::new();
        for expr in exprs {
            let Operand::Literal(literal) = operand(expr) else {
                return None;
            };
            if literal_value_is_incomparable(literal.value()) {
                return None;
            }
            values.push(literal.value().clone());
        }
        Some((column, values))
    }

    fn in_extract<'t>(&mut self, exprs: Run<'t, Where<'t>>) {
        let Some((column, mut values)) = self.column_and_literals(exprs) else {
            return;
        };
        values.sort_by(literal_value_canonical_order);
        values.dedup();
        self.constraints
            .insert(ColumnConstraint::InSet { column, values });
    }

    fn any_eq_array_extract<'t>(&mut self, mut exprs: Run<'t, Where<'t>>) {
        let (Some(first), Some(second), None) = (exprs.next(), exprs.next(), exprs.next()) else {
            self.complete = false;
            return;
        };
        let (Operand::Column(column), WhereKind::Scalar(rhs)) = (operand(first), second.kind())
        else {
            self.complete = false;
            return;
        };
        let Some(mut values) = array_literal_values(rhs) else {
            self.complete = false;
            return;
        };
        if values.iter().any(literal_value_is_incomparable) {
            self.complete = false;
            return;
        }
        let Some(column) = self.column(column) else {
            self.complete = false;
            return;
        };
        values.sort_by(literal_value_canonical_order);
        values.dedup();
        self.constraints
            .insert(ColumnConstraint::InSet { column, values });
    }

    /// Each `col <> value` up to the first operand that is not a comparable
    /// literal, as the owned walk inserts them.
    fn not_in_extract<'t>(&mut self, mut exprs: Run<'t, Where<'t>>) {
        let Some(Operand::Column(column)) = exprs.next().map(operand) else {
            return;
        };
        let Some(column) = self.column(column) else {
            return;
        };
        for expr in exprs {
            let Operand::Literal(literal) = operand(expr) else {
                return;
            };
            if literal_value_is_incomparable(literal.value()) {
                return;
            }
            self.constraints.insert(ColumnConstraint::Comparison {
                column: column.clone(),
                op: BinaryOp::NotEqual,
                value: literal.value().clone(),
            });
        }
    }
}

/// The elements of an array literal or an `ARRAY[...]` of literals.
fn array_literal_values(rhs: Scalar<'_>) -> Option<Vec<LiteralValue>> {
    match rhs.kind() {
        ScalarKind::Literal(literal) => match literal.value() {
            LiteralValue::Array(values, _) => Some(values.clone()),
            LiteralValue::String(_)
            | LiteralValue::StringWithCast(..)
            | LiteralValue::Integer(_)
            | LiteralValue::Float(_)
            | LiteralValue::Boolean(_)
            | LiteralValue::Null
            | LiteralValue::NullWithCast(_)
            | LiteralValue::Parameter(_) => None,
        },
        ScalarKind::Array(array) => {
            let mut out = Vec::with_capacity(array.elements().len());
            for element in array.elements() {
                let ScalarKind::Literal(literal) = element.kind() else {
                    return None;
                };
                out.push(literal.value().clone());
            }
            Some(out)
        }
        ScalarKind::Column(_)
        | ScalarKind::Function(_)
        | ScalarKind::Case(_)
        | ScalarKind::Arithmetic(_)
        | ScalarKind::Subquery(_)
        | ScalarKind::TypeCast(_) => None,
    }
}
