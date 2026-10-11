//! Whole-query facts read from a [`ResolvedQuery`]: the tree plus its
//! bindings, with no resolved hierarchy in between.

use std::collections::HashMap;

use ecow::EcoString;

use super::bindings::{ColumnBinding, ColumnTarget, ResolvedQuery, StarItem, TableSlot};
use crate::query::ast::{
    AstNode, ColumnNode, QueryBody, QueryExpr, SelectColumn, SelectColumns, StarNode,
};
use crate::query::resolved::ColumnsByTable;

impl ResolvedQuery {
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
