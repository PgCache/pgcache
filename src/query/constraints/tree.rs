//! The column constraints and equivalences a SELECT's predicates imply,
//! read from the rope and its bindings.

use std::collections::{HashMap, HashSet};

use ecow::EcoString;

use super::query_constraints_build;
use super::{
    ColumnConstraint, ColumnEquivalence, ConstraintColumn, OuterJoinEquality, QueryConstraints,
};
use crate::query::ast::JoinType;
use crate::query::ast::rope::{Join, JoinQual, Select, Source, Where};
use crate::query::predicates::{
    BoundColumnConstraint, ColumnConstraintKind, ConjunctForm, conjunct_parts_collect,
};
use crate::query::resolve::{Bindings, ColumnBinding, ResolvedQuery, SlotId};

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
    /// The slots each table name binds, over the FROM entries and every
    /// column a predicate reads. A name bound twice keys one constraint set
    /// for two references, which holds for neither.
    table_slots: HashMap<EcoString, HashSet<SlotId>>,
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
        table_slots: HashMap::new(),
        complete: true,
    };
    // WHERE first, then joins outermost first, so `matched_slots` holds every
    // predicate above a join by the time the join is classified.
    if let Some(where_clause) = select.where_clause() {
        collector.where_analyze(where_clause);
    }
    let where_analysis_complete = collector.complete;
    for source in select.from() {
        collector.join_source_analyze(source);
    }
    let repeated_tables = collector.repeated_tables(select);
    query_constraints_build(
        collector.constraints,
        collector.equivalences,
        collector.outer_equalities,
        collector.outer_match_columns,
        &repeated_tables,
        where_analysis_complete,
    )
}

impl Collector<'_> {
    /// The column a binding stands for, with its slot.
    fn column_resolve(&self, binding: ColumnBinding) -> Option<(ConstraintColumn, SlotId)> {
        let constraint_column = ConstraintColumn::of_binding(self.bound.bindings(), binding)?;
        Some((constraint_column, binding.slot))
    }

    fn table_slot_record(&mut self, slot: SlotId, column: &ConstraintColumn) {
        self.table_slots
            .entry(column.table.clone())
            .or_default()
            .insert(slot);
    }

    /// Table names bound by more than one slot of `select`'s FROM entries
    /// and predicate columns.
    fn repeated_tables(&mut self, select: Select<'_>) -> HashSet<EcoString> {
        let bindings = self.bound.bindings();
        let mut from_slots = HashSet::new();
        for source in select.from() {
            source_slots_collect(bindings, source, &mut from_slots);
        }
        for slot in from_slots {
            if let Some(table_slot) = bindings.slot(slot) {
                self.table_slots
                    .entry(table_slot.table().name.clone())
                    .or_default()
                    .insert(slot);
            }
        }
        std::mem::take(&mut self.table_slots)
            .into_iter()
            .filter(|(_, slots)| slots.len() > 1)
            .map(|(table, _)| table)
            .collect()
    }

    fn slot_preserved(&self, slot: SlotId) -> bool {
        self.nullable_slots
            .as_ref()
            .is_some_and(|nullable| !nullable.contains(&slot))
    }

    /// A constraint on a preserved-side column of an outer join's ON clause
    /// filters no row, so it is kept only as a join column.
    fn constraint_insert(&mut self, slot: SlotId, constraint: ColumnConstraint) {
        self.table_slot_record(slot, constraint.column());
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
        self.table_slot_record(left_slot, &left);
        self.table_slot_record(right_slot, &right);
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
        let mut parts = Vec::new();
        conjunct_parts_collect(self.bound, expr, &mut parts);
        for part in parts {
            self.form_insert(&part.form);
        }
    }

    fn form_insert(&mut self, form: &ConjunctForm) {
        match form {
            ConjunctForm::Constraint(constraint) => {
                match column_constraint_build(self.bound.bindings(), constraint) {
                    Some(column_constraint) => {
                        self.constraint_insert(constraint.column.slot, column_constraint);
                    }
                    None => self.complete = false,
                }
            }
            ConjunctForm::Equality(left, right) => {
                match (self.column_resolve(*left), self.column_resolve(*right)) {
                    (Some(left), Some(right)) => self.equality_insert(left, right),
                    _ => self.complete = false,
                }
            }
            ConjunctForm::Opaque => self.complete = false,
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

/// `constraint` keyed by the column's table name.
fn column_constraint_build(
    bindings: &Bindings,
    constraint: &BoundColumnConstraint,
) -> Option<ColumnConstraint> {
    let column = ConstraintColumn::of_binding(bindings, constraint.column)?;
    Some(match &constraint.kind {
        ColumnConstraintKind::Comparison { op, value } => ColumnConstraint::Comparison {
            column,
            op: *op,
            value: value.clone(),
        },
        ColumnConstraintKind::AnyOf(values) => ColumnConstraint::InSet {
            column,
            values: values.clone(),
        },
        ColumnConstraintKind::CastComparison { cast, op, value } => {
            ColumnConstraint::CastComparison {
                column,
                cast: cast.clone(),
                op: *op,
                value: value.clone(),
            }
        }
    })
}
