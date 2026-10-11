//! Whole-query facts read from a [`ResolvedQuery`]: the tree plus its
//! bindings, with no resolved hierarchy in between.

use std::collections::HashMap;

use ecow::EcoString;
use postgres_types::Type;

use super::bindings::{ColumnBinding, ColumnTarget, ResolvedQuery, StarItem, TableSlot};
use crate::catalog::TableMetadata;
use crate::oid::Oid;
use crate::query::ast::{
    AstNode, ColumnNode, QueryBody, QueryExpr, SelectColumn, SelectColumns, SelectNode, StarNode,
};
use crate::query::resolved::ColumnsByTable;

impl ResolvedQuery {
    /// The top-level SELECT, when the body is one.
    pub fn select(&self) -> Option<&SelectNode> {
        match &self.tree().body {
            QueryBody::Select(select) => Some(select),
            QueryBody::SetOp(_) | QueryBody::Values(_) => None,
        }
    }

    /// The catalog column a reference stands for: its own, or the left
    /// column of an inner-join merged column (which is exactly that
    /// column); nothing for an outer-join merged column or an output name.
    pub fn column_binding(&self, node: &ColumnNode) -> Option<ColumnBinding> {
        match self.column(node)? {
            ColumnTarget::Column(binding) => Some(*binding),
            ColumnTarget::Merged(id) => self
                .bindings()
                .merged(*id)
                .filter(|merged| !merged.outer)
                .map(|merged| merged.left),
            ColumnTarget::Output => None,
        }
    }

    /// The table of the column a reference stands for (a derived table's
    /// synthetic metadata is named by its alias).
    pub fn column_table(&self, node: &ColumnNode) -> Option<&TableMetadata> {
        let binding = self.column_binding(node)?;
        self.bindings().slot(binding.slot).map(TableSlot::table)
    }

    /// The name of the column a reference stands for.
    pub fn column_name(&self, node: &ColumnNode) -> Option<&EcoString> {
        let binding = self.column_binding(node)?;
        self.bindings().binding_column(&binding).map(|c| &c.name)
    }

    /// How many FROM entries name the relation `relation_oid`, at any depth.
    pub fn relation_occurrences(&self, relation_oid: Oid) -> usize {
        self.bindings()
            .slots()
            .iter()
            .filter(|slot| matches!(slot, TableSlot::Base { table, .. } if table.relation_oid == relation_oid))
            .count()
    }

    /// The type of the column a reference stands for.
    pub fn column_type(&self, node: &ColumnNode) -> Option<&Type> {
        let binding = self.column_binding(node)?;
        self.bindings()
            .binding_column(&binding)
            .map(|column| &column.data_type)
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
                    for item in bindings.star(star).unwrap_or(&[]) {
                        names.push(match item {
                            StarItem::Column(binding) => {
                                bindings.binding_column(binding).map(|c| c.name.clone())
                            }
                            StarItem::Merged(id) => bindings.merged(*id).map(|m| m.name.clone()),
                        });
                    }
                }
            }
        }
        names
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
