//! The entry point: bind a query against the catalog, and the scoped
//! variant used for subquery bodies.

use super::ResolveResult;
use super::bindings::{Bindings, BindingsBuilder};
use super::clauses::{
    group_by_resolve, order_by_as_outputs, order_by_resolve, select_items_resolve,
};
use super::expr::where_resolve;
use super::scope::{OuterTables, OutputColumn, ResolutionScope};
use super::table::source_resolve;
use crate::catalog::Catalog;
use crate::query::ast::rope::{Body, Query, QueryTree, Select};

/// Bind every table and column reference of `tree` to the catalog
/// (ADR-057).
pub fn query_tree_resolve(
    tree: &QueryTree,
    tables: &Catalog,
    search_path: &[&str],
) -> ResolveResult<Bindings> {
    let mut bindings = BindingsBuilder::sized_for(tree);
    query_resolve_scoped(
        tree.root(),
        tables,
        search_path,
        OuterTables::new(),
        &mut bindings,
    )?;
    Ok(bindings.finish())
}

/// Bind a SELECT in a pre-built scope, returning its output columns.
fn select_resolve_scoped<'a>(
    select: Select<'a>,
    tables: &'a Catalog,
    scope: &mut ResolutionScope<'a>,
    search_path: &[&'a str],
) -> ResolveResult<Vec<OutputColumn>> {
    for source in select.from() {
        source_resolve(source, tables, scope, search_path)?;
    }
    let outputs = select_items_resolve(select, scope)?;
    if let Some(where_clause) = select.where_clause() {
        where_resolve(where_clause, scope)?;
    }
    group_by_resolve(select.group_by(), scope)?;
    if let Some(having) = select.having() {
        where_resolve(having, scope)?;
    }
    Ok(outputs)
}

/// Bind a query in a scope that sees `outer_tables`, returning its output
/// columns.
pub(super) fn query_resolve_scoped<'a>(
    query: Query<'a>,
    catalog_tables: &'a Catalog,
    search_path: &'a [&'a str],
    outer_tables: OuterTables,
    bindings: &'a mut BindingsBuilder,
) -> ResolveResult<Vec<OutputColumn>> {
    let mut scope =
        ResolutionScope::new_with_outer(catalog_tables, search_path, outer_tables, bindings);

    let body = query.body();
    let outputs = match body {
        Body::Select(select) => {
            select_resolve_scoped(select, catalog_tables, &mut scope, search_path)?
        }
        Body::Values(_) => Vec::new(),
        Body::SetOp(set_op) => {
            // Each branch is independent: the same outer tables, separate
            // scopes so their FROM tables don't bleed across branches. The
            // output columns are the leftmost branch's.
            let outer = scope.outer_tables.clone();
            let left = query_resolve_scoped(
                set_op.left(),
                catalog_tables,
                search_path,
                outer.clone(),
                &mut *scope.bindings,
            )?;
            query_resolve_scoped(
                set_op.right(),
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
    match body {
        Body::Select(_) => order_by_resolve(query.order_by(), &mut scope, Some(&outputs))?,
        Body::SetOp(_) | Body::Values(_) => {
            order_by_as_outputs(query.order_by(), &mut *scope.bindings);
        }
    }
    Ok(outputs)
}
