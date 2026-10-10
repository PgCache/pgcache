//! The public entry points: resolve a parsed `QueryExpr` / `SelectNode`
//! against the catalog, plus the scoped variant used for subquery bodies.

use iddqd::BiHashMap;

use super::bindings::{Bindings, BindingsBuilder};
use super::clauses::{
    group_by_resolve, having_resolve, limit_resolve, order_by_as_identifiers, order_by_resolve,
    select_columns_resolve,
};
use super::expr::where_expr_resolve;
use super::scope::{OuterTable, ResolutionScope};
use super::table::table_source_resolve;
use crate::catalog::TableMetadata;
use crate::query::ast::{QueryBody, QueryExpr, SelectNode};
use crate::query::resolved::{
    ResolveResult, ResolvedColumnNode, ResolvedQueryBody, ResolvedQueryExpr, ResolvedSelectNode,
    ResolvedSetOpNode,
};

/// Resolve a QueryExpr to a ResolvedQueryExpr
pub fn query_expr_resolve(
    query: &QueryExpr,
    tables: &BiHashMap<TableMetadata>,
    search_path: &[&str],
) -> ResolveResult<ResolvedQueryExpr> {
    query_expr_resolve_bound(query, tables, search_path).map(|(resolved, _)| resolved)
}

/// Resolve a QueryExpr, returning the resolved tree together with the plain
/// tree's bindings (ADR-057); both describe the same resolution.
pub fn query_expr_resolve_bound(
    query: &QueryExpr,
    tables: &BiHashMap<TableMetadata>,
    search_path: &[&str],
) -> ResolveResult<(ResolvedQueryExpr, Bindings)> {
    let mut bindings = BindingsBuilder::default();
    let (resolved, _outer_refs) =
        query_expr_resolve_scoped(query, tables, search_path, Vec::new(), &mut bindings)?;
    Ok((resolved, bindings.finish()))
}

/// Resolve a SelectNode to a ResolvedSelectNode
pub fn select_node_resolve(
    select: &SelectNode,
    tables: &BiHashMap<TableMetadata>,
    search_path: &[&str],
) -> ResolveResult<ResolvedSelectNode> {
    let mut bindings = BindingsBuilder::default();
    let mut scope = ResolutionScope::new(tables, search_path, &mut bindings);
    select_node_resolve_scoped(select, tables, &mut scope, search_path)
}

/// Resolve a SelectNode using a pre-built scope.
///
/// Called by the public `select_node_resolve` (with a fresh scope) and by
/// `query_expr_resolve_scoped` (with a scope that has `outer_tables` set for
/// correlated subquery resolution).
pub(super) fn select_node_resolve_scoped<'a>(
    select: &'a SelectNode,
    tables: &'a BiHashMap<TableMetadata>,
    scope: &mut ResolutionScope<'a>,
    search_path: &[&'a str],
) -> ResolveResult<ResolvedSelectNode> {
    // First pass: resolve all table references and build scope
    let mut resolved_from = Vec::new();
    for table_source in &select.from {
        let resolved = table_source_resolve(table_source, tables, scope, search_path)?;
        resolved_from.push(resolved);
    }

    // Resolve SELECT columns
    let resolved_columns = select_columns_resolve(&select.columns, scope)?;

    // Resolve WHERE clause
    let resolved_where = match &select.where_clause {
        Some(w) => Some(where_expr_resolve(w, scope)?),
        None => None,
    };

    // Resolve GROUP BY clause
    let resolved_group_by = group_by_resolve(&select.group_by, scope)?;

    // Resolve HAVING clause
    let resolved_having = having_resolve(select.having.as_ref(), scope)?;

    Ok(ResolvedSelectNode {
        distinct: select.distinct,
        columns: resolved_columns,
        from: resolved_from,
        where_clause: resolved_where,
        group_by: resolved_group_by,
        having: resolved_having,
    })
}

/// Resolve a QueryExpr in a scope that sees `outer_tables`, collecting outer
/// refs. The top level passes no outer tables; subquery bodies pass a
/// snapshot of their ancestors' scopes. Returns the resolved query and the
/// outer column references found within it.
pub(super) fn query_expr_resolve_scoped<'a>(
    query: &'a QueryExpr,
    catalog_tables: &'a BiHashMap<TableMetadata>,
    search_path: &[&'a str],
    outer_tables: Vec<OuterTable>,
    bindings: &'a mut BindingsBuilder,
) -> ResolveResult<(ResolvedQueryExpr, Vec<ResolvedColumnNode>)> {
    let mut scope =
        ResolutionScope::new_with_outer(catalog_tables, search_path, outer_tables, bindings);

    let body = match &query.body {
        QueryBody::Select(select) => {
            let resolved =
                select_node_resolve_scoped(select, catalog_tables, &mut scope, search_path)?;
            ResolvedQueryBody::Select(Box::new(resolved))
        }
        QueryBody::Values(values) => ResolvedQueryBody::Values(values.clone()),
        QueryBody::SetOp(set_op) => {
            // Each branch is independent — resolve with the same outer_tables but
            // separate scopes so their FROM tables don't bleed across branches.
            let outer = scope.outer_tables.clone();
            let (left_resolved, left_outer_refs) = query_expr_resolve_scoped(
                &set_op.left,
                catalog_tables,
                search_path,
                outer.clone(),
                &mut *scope.bindings,
            )?;
            let (right_resolved, right_outer_refs) = query_expr_resolve_scoped(
                &set_op.right,
                catalog_tables,
                search_path,
                outer,
                &mut *scope.bindings,
            )?;
            scope.outer_refs.extend(left_outer_refs);
            scope.outer_refs.extend(right_outer_refs);
            ResolvedQueryBody::SetOp(ResolvedSetOpNode {
                op: set_op.op,
                all: set_op.all,
                left: Box::new(left_resolved),
                right: Box::new(right_resolved),
            })
        }
    };

    // ORDER BY resolution depends on query type:
    // - Simple SELECT: the body's scope, with its tables and merged columns
    // - Set operations/VALUES: unqualified identifiers (output column names)
    let order_by = match &query.body {
        QueryBody::Select(_) => {
            order_by_resolve(&query.order_by, &mut scope, body.select_columns())?
        }
        QueryBody::SetOp(_) | QueryBody::Values(_) => {
            order_by_as_identifiers(&query.order_by, &mut *scope.bindings)
        }
    };

    let limit = limit_resolve(query.limit.as_ref());
    let outer_refs = scope.outer_refs;

    Ok((
        ResolvedQueryExpr {
            body,
            order_by,
            limit,
        },
        outer_refs,
    ))
}
