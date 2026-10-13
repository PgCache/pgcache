//! Owned-tree readers of a bindings table: what the bound query answered
//! about the owned tree before it held only the rope, kept for the oracle
//! tests that compare the two until the owned tree goes.

use ecow::EcoString;
use postgres_types::Type;

use super::analysis::{binding_of_target, binding_type, star_item_name};
use super::bindings::{Bindings, ColumnBinding, ColumnTarget};
use super::render::{Bound, Output};
use crate::query::ast::{
    ColumnNode, DeparseWith, OrderByClause, QueryBody, QueryExpr, ScalarExpr, SelectColumn,
    SelectColumns, SelectNode,
};

/// The top-level SELECT, when the body is one.
pub(crate) fn select_owned(query: &QueryExpr) -> Option<&SelectNode> {
    match &query.body {
        QueryBody::Select(select) => Some(select),
        QueryBody::SetOp(_) | QueryBody::Values(_) => None,
    }
}

pub(crate) fn column_binding_owned(
    bindings: &Bindings,
    node: &ColumnNode,
) -> Option<ColumnBinding> {
    binding_of_target(bindings, bindings.column(node)?)
}

pub(crate) fn column_type_owned<'a>(bindings: &'a Bindings, node: &ColumnNode) -> Option<&'a Type> {
    binding_type(bindings, column_binding_owned(bindings, node)?)
}

/// The output names of `query`, one per output position with stars
/// expanded.
pub(crate) fn output_names_owned(bindings: &Bindings, query: &QueryExpr) -> Vec<Option<EcoString>> {
    let select = match &query.body {
        QueryBody::Select(select) => select,
        QueryBody::SetOp(set_op) => return output_names_owned(bindings, &set_op.left),
        QueryBody::Values(_) => return Vec::new(),
    };
    let SelectColumns::Columns(columns) = &select.columns else {
        return Vec::new();
    };
    let mut names = Vec::with_capacity(columns.len());
    for column in columns {
        match column {
            SelectColumn::Expr { expr, alias } => {
                names.push(
                    alias
                        .clone()
                        .or_else(|| column_output_name_owned(bindings, expr)),
                );
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

fn column_output_name_owned(bindings: &Bindings, expr: &ScalarExpr) -> Option<EcoString> {
    let ScalarExpr::Column(column) = expr else {
        return None;
    };
    match bindings.column(column)? {
        ColumnTarget::Column(binding) => bindings.binding_column(binding).map(|c| c.name.clone()),
        ColumnTarget::Merged(id) => bindings
            .merged(*id)
            .filter(|merged| !merged.outer)
            .map(|merged| merged.name.clone()),
        ColumnTarget::Output => None,
    }
}

/// The 1-based select-list position an ORDER BY item of `select` names.
pub(crate) fn order_by_position_owned(
    bindings: &Bindings,
    select: &SelectNode,
    clause: &OrderByClause,
) -> Option<usize> {
    let outputs = select_outputs_owned(bindings, select);
    let found = if let ScalarExpr::Column(column) = &clause.expr
        && matches!(bindings.column(column), Some(ColumnTarget::Output))
    {
        outputs
            .iter()
            .position(|output| output.name.as_ref() == Some(&column.column))
    } else {
        let mut text = String::new();
        clause
            .expr
            .deparse_with(&mut Bound::text(bindings), &mut text);
        outputs.iter().position(|output| output.text == text)
    };
    found.map(|index| index + 1)
}

fn select_outputs_owned(bindings: &Bindings, select: &SelectNode) -> Vec<Output> {
    let cx = Bound::text(bindings);
    let SelectColumns::Columns(columns) = &select.columns else {
        return Vec::new();
    };
    let mut outputs = Vec::with_capacity(columns.len());
    for column in columns {
        match column {
            SelectColumn::Expr { expr, alias } => {
                let mut text = String::new();
                expr.deparse_with(&mut Bound::text(bindings), &mut text);
                let name = alias
                    .clone()
                    .or_else(|| column_output_name_owned(bindings, expr));
                outputs.push(Output { text, name });
            }
            SelectColumn::Star(star) => {
                for item in bindings.star(star).unwrap_or(&[]) {
                    outputs.push(cx.star_item_output(item));
                }
            }
        }
    }
    outputs
}
