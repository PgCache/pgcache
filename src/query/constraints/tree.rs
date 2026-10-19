//! The column constraints and equivalences a SELECT's predicates imply,
//! read from the rope and its bindings.

use std::cmp::Ordering;
use std::collections::HashSet;

use super::query_constraints_build;
use super::range::{
    literal_value_canonical_order, literal_value_is_incomparable, literal_value_order,
};
use super::{
    ColumnConstraint, ColumnEquivalence, ConstraintColumn, OuterJoinEquality, QueryConstraints,
};
use crate::query::ast::rope::{
    Binary, Column, Join, JoinQual, Literal, Multi, Run, Scalar, ScalarKind, Select, Source, Where,
    WhereKind,
};
use crate::query::ast::{BinaryOp, JoinType, LiteralValue, MultiOp};
use crate::query::cast::{
    CastTarget, cast_target_is_coercion_supported, comparison_canonicalize, scalar_leaf,
};
use crate::query::resolve::{Bindings, ResolvedQuery, SlotId};

struct Collector<'a> {
    bound: &'a ResolvedQuery,
    constraints: HashSet<ColumnConstraint>,
    equivalences: HashSet<ColumnEquivalence>,
    outer_equalities: HashSet<OuterJoinEquality>,
    outer_match_columns: HashSet<ConstraintColumn>,
    /// Slots a constraint or equality has proven non-NULL. Every extracted
    /// predicate rejects NULL, so an outer join whose nullable side holds one
    /// of these filters out its NULL-padded rows and acts as an inner join.
    matched_slots: HashSet<SlotId>,
    /// The nullable side's slots while an outer join's ON clause is
    /// analyzed; a column outside them is on the preserved side.
    nullable_slots: Option<HashSet<SlotId>>,
    complete: bool,
}

/// The constraints of `select`, a SELECT of `bound`'s rope.
pub fn query_constraints_analyze(bound: &ResolvedQuery, select: Select<'_>) -> QueryConstraints {
    let mut collector = Collector {
        bound,
        constraints: HashSet::new(),
        equivalences: HashSet::new(),
        outer_equalities: HashSet::new(),
        outer_match_columns: HashSet::new(),
        matched_slots: HashSet::new(),
        nullable_slots: None,
        complete: true,
    };
    // WHERE first, then joins outermost first, so `matched_slots` holds every
    // predicate above a join by the time the join is classified.
    if let Some(where_clause) = select.where_clause() {
        collector.where_analyze(where_clause);
    }
    for source in select.from() {
        collector.join_source_analyze(source);
    }
    query_constraints_build(
        collector.constraints,
        collector.equivalences,
        collector.outer_equalities,
        collector.outer_match_columns,
        collector.complete,
    )
}

/// The column and literal of `exprs[index]` when it is a bare column or
/// literal, as the extractors pattern-match the operand list.
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
    /// The column a reference stands for, with the slot it binds to.
    fn column_resolve(&self, column: Column<'_>) -> Option<(ConstraintColumn, SlotId)> {
        let binding = self.bound.column_binding_at(column.id())?;
        let constraint_column = ConstraintColumn::of_binding(self.bound.bindings(), binding)?;
        Some((constraint_column, binding.slot))
    }

    fn slot_preserved(&self, slot: SlotId) -> bool {
        self.nullable_slots
            .as_ref()
            .is_some_and(|nullable| !nullable.contains(&slot))
    }

    /// A constraint on a preserved-side column of an outer join's ON clause
    /// filters no row, so it is kept only as a join column.
    fn constraint_insert(&mut self, slot: SlotId, constraint: ColumnConstraint) {
        if self.slot_preserved(slot) {
            self.outer_match_columns.insert(constraint.column().clone());
            return;
        }
        self.matched_slots.insert(slot);
        self.constraints.insert(constraint);
    }

    fn equality_insert(
        &mut self,
        (left, left_slot): (ConstraintColumn, SlotId),
        (right, right_slot): (ConstraintColumn, SlotId),
    ) {
        match (
            self.slot_preserved(left_slot),
            self.slot_preserved(right_slot),
        ) {
            (false, false) => {
                self.matched_slots.extend([left_slot, right_slot]);
                self.equivalences.insert(ColumnEquivalence { left, right });
            }
            (true, false) => {
                self.matched_slots.insert(right_slot);
                self.outer_equalities.insert(OuterJoinEquality {
                    preserved: left,
                    nullable: right,
                });
            }
            (false, true) => {
                self.matched_slots.insert(left_slot);
                self.outer_equalities.insert(OuterJoinEquality {
                    preserved: right,
                    nullable: left,
                });
            }
            (true, true) => {
                self.outer_match_columns.extend([left, right]);
            }
        }
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
        if let Some((column, target, op, value)) = comparison_canonicalize(self.bound, binary) {
            self.canonical_insert(column, target, op, value);
        } else if let Some((left, right)) = self.column_pair(binary) {
            self.equality_insert(left, right);
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
        let (Some((constraint_column, slot)), Some(base)) = (
            self.column_resolve(column),
            self.bound.column_type_at(column.id()),
        ) else {
            self.complete = false;
            return;
        };
        match target {
            None => {
                self.constraint_insert(
                    slot,
                    ColumnConstraint::Comparison {
                        column: constraint_column,
                        op,
                        value: value.clone(),
                    },
                );
            }
            Some(cast) if cast_target_is_coercion_supported(cast, base) => {
                self.constraint_insert(
                    slot,
                    ColumnConstraint::CastComparison {
                        column: constraint_column,
                        cast: cast.clone(),
                        op,
                        value: value.clone(),
                    },
                );
            }
            Some(_) => self.complete = false,
        }
    }

    /// The two columns of a `column = column` equality.
    fn column_pair(
        &self,
        binary: Binary<'_>,
    ) -> Option<((ConstraintColumn, SlotId), (ConstraintColumn, SlotId))> {
        if binary.op() != BinaryOp::Equal {
            return None;
        }
        let left = scalar_leaf(self.bound, binary.lexpr())?;
        let right = scalar_leaf(self.bound, binary.rexpr())?;
        let (ScalarKind::Column(left), ScalarKind::Column(right)) = (left.kind(), right.kind())
        else {
            return None;
        };
        Some((self.column_resolve(left)?, self.column_resolve(right)?))
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
        self.nullable_slots = self.join_nullable_slots(join);
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
                        ConstraintColumn::of_binding(bindings, merged.left),
                        ConstraintColumn::of_binding(bindings, merged.right),
                    ) {
                        self.equality_insert((left, merged.left.slot), (right, merged.right.slot));
                    }
                }
            }
            JoinQual::Cross => {}
        }
        self.nullable_slots = None;
        self.join_source_analyze(join.left());
        self.join_source_analyze(join.right());
    }

    /// The slots of `join`'s nullable side; `None` when no row of it is
    /// NULL-padded: an inner join, or an outer join reduced to one because a
    /// predicate above rejects NULL on its nullable side. A FULL join
    /// preserves both sides, so none of its slots are nullable.
    fn join_nullable_slots(&self, join: Join<'_>) -> Option<HashSet<SlotId>> {
        let nullable_side = match join.join_type() {
            JoinType::Inner => return None,
            JoinType::Full => return Some(HashSet::new()),
            JoinType::Left => join.right(),
            JoinType::Right => join.left(),
        };
        let mut nullable = HashSet::new();
        source_slots_collect(self.bound.bindings(), nullable_side, &mut nullable);
        if nullable.is_disjoint(&self.matched_slots) {
            Some(nullable)
        } else {
            None
        }
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
        let Some((column, slot)) = self.column_resolve(column) else {
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
        self.constraint_insert(
            slot,
            ColumnConstraint::Comparison {
                column: column.clone(),
                op: BinaryOp::GreaterThanOrEqual,
                value: low.clone(),
            },
        );
        self.constraint_insert(
            slot,
            ColumnConstraint::Comparison {
                column,
                op: BinaryOp::LessThanOrEqual,
                value: high.clone(),
            },
        );
    }

    /// The subject column and the literal values after it, when the
    /// operands are exactly that; `None` otherwise.
    fn column_and_literals<'t>(
        &self,
        mut exprs: Run<'t, Where<'t>>,
    ) -> Option<(ConstraintColumn, SlotId, Vec<LiteralValue>)> {
        let Some(Operand::Column(column)) = exprs.next().map(operand) else {
            return None;
        };
        let (column, slot) = self.column_resolve(column)?;
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
        Some((column, slot, values))
    }

    fn in_extract<'t>(&mut self, exprs: Run<'t, Where<'t>>) {
        let Some((column, slot, mut values)) = self.column_and_literals(exprs) else {
            return;
        };
        values.sort_by(literal_value_canonical_order);
        values.dedup();
        self.constraint_insert(slot, ColumnConstraint::InSet { column, values });
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
        let Some((column, slot)) = self.column_resolve(column) else {
            self.complete = false;
            return;
        };
        values.sort_by(literal_value_canonical_order);
        values.dedup();
        self.constraint_insert(slot, ColumnConstraint::InSet { column, values });
    }

    /// Each `col <> value` up to the first operand that is not a comparable
    /// literal.
    fn not_in_extract<'t>(&mut self, mut exprs: Run<'t, Where<'t>>) {
        let Some(Operand::Column(column)) = exprs.next().map(operand) else {
            return;
        };
        let Some((column, slot)) = self.column_resolve(column) else {
            return;
        };
        for expr in exprs {
            let Operand::Literal(literal) = operand(expr) else {
                return;
            };
            if literal_value_is_incomparable(literal.value()) {
                return;
            }
            self.constraint_insert(
                slot,
                ColumnConstraint::Comparison {
                    column: column.clone(),
                    op: BinaryOp::NotEqual,
                    value: literal.value().clone(),
                },
            );
        }
    }
}

/// The slot of every table, derived table and CTE reference under `source`.
fn source_slots_collect(bindings: &Bindings, source: Source<'_>, slots: &mut HashSet<SlotId>) {
    let id = match source {
        Source::Join(join) => {
            source_slots_collect(bindings, join.left(), slots);
            source_slots_collect(bindings, join.right(), slots);
            return;
        }
        Source::Table(table) => table.id(),
        Source::Subquery(subquery) => subquery.id(),
        Source::CteRef(cte_ref) => cte_ref.id(),
    };
    slots.extend(bindings.source(id));
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
