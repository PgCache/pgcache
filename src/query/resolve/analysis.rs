//! Whole-query facts read from a [`ResolvedQuery`]: the tree plus its
//! bindings, with no resolved hierarchy in between.

#![allow(clippy::wildcard_enum_match_arm)]

use std::collections::{HashMap, HashSet};

use ecow::EcoString;
use postgres_types::Type;

use super::bindings::{Bindings, ColumnBinding, ColumnTarget, ResolvedQuery, StarItem, TableSlot};
use crate::catalog::TableMetadata;
use crate::query::ast::NodeId;
use crate::query::ast::rope::{
    Body, NodeKind, Query, Scalar, ScalarKind, Select, SelectItem, View,
};

/// The columns a query reads from each table, by table name.
pub type ColumnsByTable = HashMap<EcoString, HashSet<EcoString>>;

impl ResolvedQuery {
    /// The root SELECT, when the query is one.
    pub fn select(&self) -> Option<Select<'_>> {
        match self.tree().root().body() {
            Body::Select(select) => Some(select),
            Body::SetOp(_) | Body::Values(_) => None,
        }
    }

    /// The SELECT at position `id`, when the node there is one.
    pub fn select_node(&self, id: NodeId) -> Option<Select<'_>> {
        matches!(self.tree().node(id).kind, NodeKind::Select { .. })
            .then(|| Select::from_ref(self.tree().root().node_ref().at_position(id)))
    }

    /// [`Bindings::column_by_id`] for the column at `id`.
    pub fn column_target_at(&self, id: NodeId) -> Option<&ColumnTarget> {
        self.bindings().column_by_id(id)
    }

    /// The catalog column the reference at `id` stands for: its own, or
    /// the left column of an inner-join merged column (which is exactly
    /// that column); nothing for an outer-join merged column or an output
    /// name.
    pub fn column_binding_at(&self, id: NodeId) -> Option<ColumnBinding> {
        binding_of_target(self.bindings(), self.column_target_at(id)?)
    }

    pub fn column_table_at(&self, id: NodeId) -> Option<&TableMetadata> {
        binding_table(self.bindings(), self.column_binding_at(id)?)
    }

    pub fn column_name_at(&self, id: NodeId) -> Option<&EcoString> {
        binding_name(self.bindings(), self.column_binding_at(id)?)
    }

    pub fn column_type_at(&self, id: NodeId) -> Option<&Type> {
        binding_type(self.bindings(), self.column_binding_at(id)?)
    }

    /// Which columns of each table the query reads anywhere: select list,
    /// predicates, join conditions (a `USING`/`NATURAL` join through both
    /// sides of each merged column), grouping, ordering and subqueries, with
    /// stars expanded. Every base table has an entry, possibly empty; two
    /// tables of one name in different schemas share an entry, which only
    /// over-approximates.
    pub fn columns_by_table(&self) -> ColumnsByTable {
        let bindings = self.bindings();
        let mut columns: ColumnsByTable = HashMap::new();
        for slot in bindings.slots() {
            if let TableSlot::Base { table, .. } = slot {
                columns.entry(table.name.clone()).or_default();
            }
        }
        for position in self.tree().root().node_ref().subtree() {
            match position.kind() {
                NodeKind::Column { .. } => {
                    if let Some(ColumnTarget::Column(binding)) =
                        bindings.column_by_id(position.id())
                    {
                        self.column_add(&mut columns, binding);
                    }
                    // A merged-column reference reads the join's side columns, which
                    // the merged-column loop below already adds.
                }
                NodeKind::Star { .. } => {
                    for item in bindings.star_by_id(position.id()).unwrap_or(&[]) {
                        if let StarItem::Column(binding) = item {
                            self.column_add(&mut columns, binding);
                        }
                    }
                }
                _ => {}
            }
        }
        for merged in bindings.merged_columns() {
            self.column_add(&mut columns, &merged.left);
            self.column_add(&mut columns, &merged.right);
        }
        columns
    }

    /// The output names of `query`, one per output position with stars
    /// expanded: an alias, else the column name of a column reference, else
    /// nothing. A set operation's outputs are its leftmost branch's.
    pub fn output_names(&self, query: Query<'_>) -> Vec<Option<EcoString>> {
        let select = match query.body() {
            Body::Select(select) => select,
            Body::SetOp(set_op) => return self.output_names(set_op.left()),
            Body::Values(_) => return Vec::new(),
        };
        let Some(items) = select.items() else {
            return Vec::new();
        };
        let bindings = self.bindings();
        let mut names = Vec::with_capacity(items.len());
        for item in items {
            match item {
                SelectItem::Expr(expr) => {
                    names.push(
                        expr.alias()
                            .cloned()
                            .or_else(|| self.column_output_name(expr.expr())),
                    );
                }
                SelectItem::Star(star) => {
                    names.extend(
                        bindings
                            .star_by_id(star.id())
                            .unwrap_or(&[])
                            .iter()
                            .map(|item| star_item_name(bindings, item)),
                    );
                }
            }
        }
        names
    }

    /// The name PostgreSQL gives an unaliased output: the column name of a
    /// column reference (a merged outer-join column is `coalesce`, which has
    /// none); nothing for any other expression.
    pub(crate) fn column_output_name(&self, expr: Scalar<'_>) -> Option<EcoString> {
        let ScalarKind::Column(column) = expr.kind() else {
            return None;
        };
        match self.column_target_at(column.id())? {
            ColumnTarget::Column(binding) => self
                .bindings()
                .binding_column(binding)
                .map(|c| c.name.clone()),
            ColumnTarget::Merged(id) => self
                .bindings()
                .merged(*id)
                .filter(|merged| !merged.outer)
                .map(|merged| merged.name.clone()),
            ColumnTarget::Output => None,
        }
    }

    fn column_add(&self, columns: &mut ColumnsByTable, binding: &ColumnBinding) {
        let bindings = self.bindings();
        if let (Some(slot), Some(column)) = (
            bindings.slot(binding.slot),
            bindings.binding_column(binding),
        ) {
            columns
                .entry(slot.table().name.clone())
                .or_default()
                .insert(column.name.clone());
        }
    }
}

/// The output name of one column a star expands to.
pub(super) fn star_item_name(bindings: &Bindings, item: &StarItem) -> Option<EcoString> {
    match item {
        StarItem::Column(binding) => bindings.binding_column(binding).map(|c| c.name.clone()),
        StarItem::Merged(id) => bindings.merged(*id).map(|m| m.name.clone()),
    }
}

/// The binding a reference's target names, when it names one column.
pub(super) fn binding_of_target(
    bindings: &Bindings,
    target: &ColumnTarget,
) -> Option<ColumnBinding> {
    match target {
        ColumnTarget::Column(binding) => Some(*binding),
        ColumnTarget::Merged(merged) => bindings
            .merged(*merged)
            .filter(|merged| !merged.outer)
            .map(|merged| merged.left),
        ColumnTarget::Output => None,
    }
}

pub(super) fn binding_table(bindings: &Bindings, binding: ColumnBinding) -> Option<&TableMetadata> {
    bindings.slot(binding.slot).map(TableSlot::table)
}

pub(super) fn binding_name(bindings: &Bindings, binding: ColumnBinding) -> Option<&EcoString> {
    bindings.binding_column(&binding).map(|c| &c.name)
}

pub(super) fn binding_type(bindings: &Bindings, binding: ColumnBinding) -> Option<&Type> {
    bindings
        .binding_column(&binding)
        .map(|column| &column.data_type)
}
