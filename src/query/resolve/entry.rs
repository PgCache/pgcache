//! The public entry point: bind a parsed `QueryExpr` against the catalog,
//! plus the scoped variant used for subquery bodies.

use super::bindings::{Bindings, BindingsBuilder};
use super::clauses::{
    group_by_resolve, order_by_as_outputs, order_by_resolve, select_columns_resolve,
};
use super::expr::where_expr_resolve;
use super::scope::{OuterTable, OutputColumn, ResolutionScope};
use super::table::table_source_resolve;
use crate::catalog::Catalog;
use crate::query::ast::{QueryBody, QueryExpr, SelectNode};
use crate::query::resolve::ResolveResult;

/// Bind every table and column reference of a finalized `query` to the
/// catalog (ADR-057).
pub fn query_expr_resolve_bound(
    query: &QueryExpr,
    tables: &Catalog,
    search_path: &[&str],
) -> ResolveResult<Bindings> {
    let mut bindings = BindingsBuilder::default();
    query_expr_resolve_scoped(query, tables, search_path, Vec::new(), &mut bindings)?;
    Ok(bindings.finish())
}

/// Bind a SELECT in a pre-built scope, returning its output columns.
///
/// Called by `query_expr_resolve_scoped` with a scope whose `outer_tables`
/// are set for correlated subquery resolution.
pub(super) fn select_node_resolve_scoped<'a>(
    select: &'a SelectNode,
    tables: &'a Catalog,
    scope: &mut ResolutionScope<'a>,
    search_path: &[&'a str],
) -> ResolveResult<Vec<OutputColumn>> {
    for table_source in &select.from {
        table_source_resolve(table_source, tables, scope, search_path)?;
    }
    let outputs = select_columns_resolve(&select.columns, scope)?;
    if let Some(where_clause) = &select.where_clause {
        where_expr_resolve(where_clause, scope)?;
    }
    group_by_resolve(&select.group_by, scope)?;
    if let Some(having) = &select.having {
        where_expr_resolve(having, scope)?;
    }
    Ok(outputs)
}

/// Bind a QueryExpr in a scope that sees `outer_tables`, returning its
/// output columns. The top level passes no outer tables; subquery bodies
/// pass a snapshot of their ancestors' scopes.
pub(super) fn query_expr_resolve_scoped<'a>(
    query: &'a QueryExpr,
    catalog_tables: &'a Catalog,
    search_path: &[&'a str],
    outer_tables: Vec<OuterTable>,
    bindings: &'a mut BindingsBuilder,
) -> ResolveResult<Vec<OutputColumn>> {
    let mut scope =
        ResolutionScope::new_with_outer(catalog_tables, search_path, outer_tables, bindings);

    let outputs = match &query.body {
        QueryBody::Select(select) => {
            select_node_resolve_scoped(select, catalog_tables, &mut scope, search_path)?
        }
        QueryBody::Values(_) => Vec::new(),
        QueryBody::SetOp(set_op) => {
            // Each branch is independent: the same outer tables, separate
            // scopes so their FROM tables don't bleed across branches. The
            // output columns are the leftmost branch's.
            let outer = scope.outer_tables.clone();
            let left = query_expr_resolve_scoped(
                &set_op.left,
                catalog_tables,
                search_path,
                outer.clone(),
                &mut *scope.bindings,
            )?;
            query_expr_resolve_scoped(
                &set_op.right,
                catalog_tables,
                search_path,
                outer,
                &mut *scope.bindings,
            )?;
            left
        }
    };

    // ORDER BY binds through the body's scope for a SELECT; a set
    // operation's or VALUES' ORDER BY names output columns only.
    match &query.body {
        QueryBody::Select(_) => order_by_resolve(&query.order_by, &mut scope, Some(&outputs))?,
        QueryBody::SetOp(_) | QueryBody::Values(_) => {
            order_by_as_outputs(&query.order_by, &mut *scope.bindings);
        }
    }
    Ok(outputs)
}
