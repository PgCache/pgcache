//! Whole-query facts read from a [`ResolvedQuery`]: the tree plus its
//! bindings, with no resolved hierarchy in between.

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;

use ecow::EcoString;
use postgres_types::Type;

use super::bindings::{Bindings, ColumnBinding, ColumnTarget, ResolvedQuery, StarItem, TableSlot};
use crate::catalog::TableMetadata;
use crate::oid::Oid;
use crate::query::ast::rope::{
    Body, NodeKind, Query, Scalar, ScalarKind, Select, SelectItem, View,
};
use crate::query::ast::{
    AstNode, ColumnNode, NodeId, QueryBody, QueryExpr, SelectColumn, SelectColumns, SelectNode,
    StarNode,
};

/// The columns a query reads from each table, by table name.
pub type ColumnsByTable = HashMap<EcoString, HashSet<EcoString>>;

impl ResolvedQuery {
    /// The top-level SELECT, when the body is one.
    pub fn select(&self) -> Option<&SelectNode> {
        match &self.tree().body {
            QueryBody::Select(select) => Some(select),
            QueryBody::SetOp(_) | QueryBody::Values(_) => None,
        }
    }

    /// The SELECT node with `id`, anywhere in the tree.
    pub fn select_node(&self, id: NodeId) -> Option<&SelectNode> {
        let found = self
            .tree()
            .try_for_each_node::<SelectNode, &SelectNode>(&mut |select| {
                if select.id.index() == id.index() {
                    ControlFlow::Break(select)
                } else {
                    ControlFlow::Continue(())
                }
            });
        match found {
            ControlFlow::Break(select) => Some(select),
            ControlFlow::Continue(()) => None,
        }
    }

    /// The catalog column a reference stands for: its own, or the left
    /// column of an inner-join merged column (which is exactly that
    /// column); nothing for an outer-join merged column or an output name.
    /// The root SELECT of the rope, when the query is one.
    pub fn rope_select(&self) -> Option<Select<'_>> {
        match self.rope().root().body() {
            Body::Select(select) => Some(select),
            Body::SetOp(_) | Body::Values(_) => None,
        }
    }

    /// The SELECT at rope position `id`, when the node there is one.
    pub fn rope_select_node(&self, id: NodeId) -> Option<Select<'_>> {
        matches!(self.rope().node(id).kind, NodeKind::Select { .. })
            .then(|| Select::from_ref(self.rope().root().node_ref().at_position(id)))
    }

    /// [`Bindings::column_by_id`] in the rope's id space.
    pub fn column_target_at(&self, id: NodeId) -> Option<&ColumnTarget> {
        self.rope_bindings().column_by_id(id)
    }

    /// The catalog column a reference stands for: its own, or the left
    /// column of an inner-join merged column (which is exactly that
    /// column); nothing for an outer-join merged column or an output name.
    pub fn column_binding(&self, node: &ColumnNode) -> Option<ColumnBinding> {
        binding_of_target(self.bindings(), self.column(node)?)
    }

    /// [`ResolvedQuery::column_binding`] for the column at rope position `id`.
    pub fn column_binding_at(&self, id: NodeId) -> Option<ColumnBinding> {
        binding_of_target(self.rope_bindings(), self.column_target_at(id)?)
    }

    pub fn column_table(&self, node: &ColumnNode) -> Option<&TableMetadata> {
        binding_table(self.bindings(), self.column_binding(node)?)
    }

    pub fn column_table_at(&self, id: NodeId) -> Option<&TableMetadata> {
        binding_table(self.rope_bindings(), self.column_binding_at(id)?)
    }

    pub fn column_name(&self, node: &ColumnNode) -> Option<&EcoString> {
        binding_name(self.bindings(), self.column_binding(node)?)
    }

    pub fn column_name_at(&self, id: NodeId) -> Option<&EcoString> {
        binding_name(self.rope_bindings(), self.column_binding_at(id)?)
    }

    /// How many FROM entries of the query are `relation_oid`, subqueries
    /// included: a self-join references the relation twice.
    pub fn relation_occurrences(&self, relation_oid: Oid) -> usize {
        self.bindings()
            .slots()
            .iter()
            .filter(|slot| matches!(slot, TableSlot::Base { table, .. } if table.relation_oid == relation_oid))
            .count()
    }

    pub fn column_type(&self, node: &ColumnNode) -> Option<&Type> {
        binding_type(self.bindings(), self.column_binding(node)?)
    }

    pub fn column_type_at(&self, id: NodeId) -> Option<&Type> {
        binding_type(self.rope_bindings(), self.column_binding_at(id)?)
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
        for column in self.tree().nodes::<ColumnNode>() {
            if let Some(ColumnTarget::Column(binding)) = bindings.column(column) {
                self.column_add(&mut columns, binding);
            }
            // A merged-column reference reads the join's side columns, which
            // the merged-column loop below already adds.
        }
        for star in self.tree().nodes::<StarNode>() {
            for item in bindings.star(star).unwrap_or(&[]) {
                if let StarItem::Column(binding) = item {
                    self.column_add(&mut columns, binding);
                }
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
    pub fn output_names(&self, query: &QueryExpr) -> Vec<Option<EcoString>> {
        let select = match &query.body {
            QueryBody::Select(select) => select,
            QueryBody::SetOp(set_op) => return self.output_names(&set_op.left),
            QueryBody::Values(_) => return Vec::new(),
        };
        let SelectColumns::Columns(columns) = &select.columns else {
            return Vec::new();
        };
        let bindings = self.bindings();
        let mut names = Vec::with_capacity(columns.len());
        for column in columns {
            match column {
                SelectColumn::Expr { expr, alias } => {
                    names.push(alias.clone().or_else(|| self.column_output_name(expr)));
                }
                SelectColumn::Star(star) => {
                    names.extend(
                        bindings
                            .star(star)
                            .unwrap_or(&[])
                            .iter()
                            .map(|item| star_item_name(bindings, item)),
                    );
                }
            }
        }
        names
    }

    /// [`ResolvedQuery::output_names`] for a rope query.
    pub fn rope_output_names(&self, query: Query<'_>) -> Vec<Option<EcoString>> {
        let select = match query.body() {
            Body::Select(select) => select,
            Body::SetOp(set_op) => return self.rope_output_names(set_op.left()),
            Body::Values(_) => return Vec::new(),
        };
        let Some(items) = select.items() else {
            return Vec::new();
        };
        let bindings = self.rope_bindings();
        let mut names = Vec::with_capacity(items.len());
        for item in items {
            match item {
                SelectItem::Expr(expr) => {
                    names.push(
                        expr.alias()
                            .cloned()
                            .or_else(|| self.rope_column_output_name(expr.expr())),
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

    /// [`ResolvedQuery::column_output_name`] for a rope expression.
    pub(crate) fn rope_column_output_name(&self, expr: Scalar<'_>) -> Option<EcoString> {
        let ScalarKind::Column(column) = expr.kind() else {
            return None;
        };
        match self.column_target_at(column.id())? {
            ColumnTarget::Column(binding) => self
                .rope_bindings()
                .binding_column(binding)
                .map(|c| c.name.clone()),
            ColumnTarget::Merged(id) => self
                .rope_bindings()
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
fn star_item_name(bindings: &Bindings, item: &StarItem) -> Option<EcoString> {
    match item {
        StarItem::Column(binding) => bindings.binding_column(binding).map(|c| c.name.clone()),
        StarItem::Merged(id) => bindings.merged(*id).map(|m| m.name.clone()),
    }
}

/// The binding a reference's target names, when it names one column.
fn binding_of_target(bindings: &Bindings, target: &ColumnTarget) -> Option<ColumnBinding> {
    match target {
        ColumnTarget::Column(binding) => Some(*binding),
        ColumnTarget::Merged(merged) => bindings
            .merged(*merged)
            .filter(|merged| !merged.outer)
            .map(|merged| merged.left),
        ColumnTarget::Output => None,
    }
}

fn binding_table(bindings: &Bindings, binding: ColumnBinding) -> Option<&TableMetadata> {
    bindings.slot(binding.slot).map(TableSlot::table)
}

fn binding_name(bindings: &Bindings, binding: ColumnBinding) -> Option<&EcoString> {
    bindings.binding_column(&binding).map(|c| &c.name)
}

fn binding_type(bindings: &Bindings, binding: ColumnBinding) -> Option<&Type> {
    bindings
        .binding_column(&binding)
        .map(|column| &column.data_type)
}
