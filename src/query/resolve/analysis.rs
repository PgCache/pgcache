//! Whole-query facts read from a [`ResolvedQuery`]: the tree plus its
//! bindings, with no resolved hierarchy in between.

#![allow(clippy::wildcard_enum_match_arm)]

use std::collections::{HashMap, HashSet};

use ecow::EcoString;
use postgres_types::Type;

use super::bindings::{
    Bindings, BoundView, ColumnBinding, ColumnTarget, ResolvedQuery, StarItem, TableSlot,
};
use crate::catalog::TableMetadata;
use crate::oid::Oid;
use crate::query::ast::NodeId;
use crate::query::ast::rope::{
    Body, NodeKind, Query, Scalar, ScalarKind, Select, SelectItem, View,
};

/// What origin checks before it runs a read (PGC-463): `SELECT` on every
/// column referenced per base relation (or, with none referenced, on the
/// relation or any one column), `USAGE` on each relation's schema, and
/// `EXECUTE` on every function called, by name.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PrivilegeNeeds {
    pub relations: HashMap<Oid, HashSet<EcoString>>,
    /// Lowercased function names, schema-qualified or not: `EXECUTE` is
    /// required on every overload of the name, as the volatility map takes
    /// the worst case over overloads.
    pub functions: Vec<EcoString>,
}

impl<'a> BoundView<'a> {
    /// The root SELECT, when the query is one.
    pub fn select(self) -> Option<Select<'a>> {
        match self.tree().root().body() {
            Body::Select(select) => Some(select),
            Body::SetOp(_) | Body::Values(_) => None,
        }
    }

    /// The SELECT at position `id`, when the node there is one.
    pub fn select_node(self, id: NodeId) -> Option<Select<'a>> {
        matches!(self.tree().node(id).kind, NodeKind::Select { .. })
            .then(|| Select::from_ref(self.tree().root().node_ref().at_position(id)))
    }

    /// [`Bindings::column_by_id`] for the column at `id`.
    pub fn column_target_at(self, id: NodeId) -> Option<&'a ColumnTarget> {
        self.bindings().column_by_id(id)
    }

    /// The catalog column the reference at `id` stands for: its own, or
    /// the left column of an inner-join merged column (which is exactly
    /// that column); nothing for an outer-join merged column or an output
    /// name.
    pub fn column_binding_at(self, id: NodeId) -> Option<ColumnBinding> {
        binding_of_target(self.bindings(), self.column_target_at(id)?)
    }

    pub fn column_table_at(self, id: NodeId) -> Option<&'a TableMetadata> {
        binding_table(self.bindings(), self.column_binding_at(id)?)
    }

    pub fn column_name_at(self, id: NodeId) -> Option<&'a EcoString> {
        binding_name(self.bindings(), self.column_binding_at(id)?)
    }

    pub fn column_type_at(self, id: NodeId) -> Option<&'a Type> {
        binding_type(self.bindings(), self.column_binding_at(id)?)
    }
}

impl ResolvedQuery {
    /// The root SELECT, when the query is one.
    pub fn select(&self) -> Option<Select<'_>> {
        self.view().select()
    }

    /// The SELECT at position `id`, when the node there is one.
    pub fn select_node(&self, id: NodeId) -> Option<Select<'_>> {
        self.view().select_node(id)
    }

    /// [`Bindings::column_by_id`] for the column at `id`.
    pub fn column_target_at(&self, id: NodeId) -> Option<&ColumnTarget> {
        self.view().column_target_at(id)
    }

    /// The catalog column the reference at `id` stands for
    /// ([`BoundView::column_binding_at`]).
    pub fn column_binding_at(&self, id: NodeId) -> Option<ColumnBinding> {
        self.view().column_binding_at(id)
    }

    pub fn column_table_at(&self, id: NodeId) -> Option<&TableMetadata> {
        self.view().column_table_at(id)
    }

    pub fn column_name_at(&self, id: NodeId) -> Option<&EcoString> {
        self.view().column_name_at(id)
    }

    pub fn column_type_at(&self, id: NodeId) -> Option<&Type> {
        self.view().column_type_at(id)
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
}

/// Add the column `binding` names to its base table's entry, keyed by
/// `key`. A derived table's column is not a read of any relation: the
/// base columns its body reads are references of their own.
fn column_add<K: std::hash::Hash + Eq>(
    bindings: &Bindings,
    columns: &mut HashMap<K, HashSet<EcoString>>,
    key: &impl Fn(&TableMetadata) -> K,
    binding: &ColumnBinding,
) {
    if let (Some(TableSlot::Base { table, .. }), Some(column)) = (
        bindings.slot(binding.slot),
        bindings.binding_column(binding),
    ) {
        columns
            .entry(key(table))
            .or_default()
            .insert(column.name.clone());
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

impl BoundView<'_> {
    /// The privileges a read of this query needs at origin: the columns it
    /// references per base relation, keyed by OID so same-named tables in
    /// two schemas stay apart, and the functions it calls. The columns are
    /// read anywhere: select list, predicates, join conditions (a
    /// `USING`/`NATURAL` join through both sides of each merged column),
    /// grouping, ordering and subqueries, with stars expanded; every base
    /// relation has an entry, possibly empty.
    pub fn privilege_needs(self) -> PrivilegeNeeds {
        let relations = self.columns_collect(|table| table.relation_oid);
        let mut functions: Vec<EcoString> = self
            .tree()
            .nodes()
            .filter_map(|node| match &node.kind {
                NodeKind::Function { name, .. } => Some(name.clone()),
                _ => None,
            })
            .collect();
        functions.sort_unstable();
        functions.dedup();
        PrivilegeNeeds {
            relations,
            functions,
        }
    }

    /// The columns read from each base table, keyed by `key` of the table.
    fn columns_collect<K: std::hash::Hash + Eq>(
        self,
        key: impl Fn(&TableMetadata) -> K,
    ) -> HashMap<K, HashSet<EcoString>> {
        let bindings = self.bindings();
        let mut columns: HashMap<K, HashSet<EcoString>> = HashMap::new();
        for slot in bindings.slots() {
            if let TableSlot::Base { table, .. } = slot {
                columns.entry(key(table)).or_default();
            }
        }
        for position in self.tree().root().node_ref().subtree() {
            match position.kind() {
                NodeKind::Column { .. } => {
                    if let Some(ColumnTarget::Column(binding)) =
                        bindings.column_by_id(position.id())
                    {
                        column_add(bindings, &mut columns, &key, binding);
                    }
                    // A merged-column reference reads the join's side columns, which
                    // the merged-column loop below already adds.
                }
                NodeKind::Star { .. } => {
                    for item in bindings.star_by_id(position.id()).unwrap_or(&[]) {
                        if let StarItem::Column(binding) = item {
                            column_add(bindings, &mut columns, &key, binding);
                        }
                    }
                }
                _ => {}
            }
        }
        for merged in bindings.merged_columns() {
            column_add(bindings, &mut columns, &key, &merged.left);
            column_add(bindings, &mut columns, &key, &merged.right);
        }
        columns
    }
}
