//! The public entry points: resolve a parsed `QueryExpr` / `SelectNode`
//! against the catalog, plus the scoped variants used for subquery bodies.

use iddqd::BiHashMap;

use crate::catalog::TableMetadata;
use crate::query::ast::{QueryBody, QueryExpr, SelectNode};
use crate::query::resolved::{
    ResolveResult, ResolvedColumnNode, ResolvedQueryBody, ResolvedQueryExpr, ResolvedSelectNode,
    ResolvedSetOpNode,
};

use super::clauses::{
    group_by_resolve, having_resolve, limit_resolve, order_by_as_identifiers, order_by_resolve,
    select_columns_resolve,
};
use super::expr::where_expr_resolve;
use super::scope::ResolutionScope;
use super::table::table_source_resolve;

// ============================================================================
// Resolution functions for new QueryExpr type hierarchy
// ============================================================================

/// Resolve a QueryExpr to a ResolvedQueryExpr
pub fn query_expr_resolve(
    query: &QueryExpr,
    tables: &BiHashMap<TableMetadata>,
    search_path: &[&str],
) -> ResolveResult<ResolvedQueryExpr> {
    let body = query_body_resolve(&query.body, tables, search_path)?;

    // ORDER BY resolution depends on query type:
    // - Simple SELECT: resolve against table columns
    // - Set operations/VALUES: use unqualified identifiers (output column names)
    let order_by = match &query.body {
        QueryBody::Select(select) => {
            let mut scope = ResolutionScope::new(tables, search_path);
            for table_source in &select.from {
                let _ = table_source_resolve(table_source, tables, &mut scope, search_path);
            }
            order_by_resolve(&query.order_by, &mut scope, body.select_columns())?
        }
        QueryBody::SetOp(_) | QueryBody::Values(_) => order_by_as_identifiers(&query.order_by),
    };

    let limit = limit_resolve(query.limit.as_ref());

    Ok(ResolvedQueryExpr {
        body,
        order_by,
        limit,
    })
}

/// Resolve a QueryBody to a ResolvedQueryBody
pub(super) fn query_body_resolve(
    body: &QueryBody,
    tables: &BiHashMap<TableMetadata>,
    search_path: &[&str],
) -> ResolveResult<ResolvedQueryBody> {
    match body {
        QueryBody::Select(select) => {
            let resolved = select_node_resolve(select, tables, search_path)?;
            Ok(ResolvedQueryBody::Select(Box::new(resolved)))
        }
        QueryBody::Values(values) => {
            // VALUES clauses contain only literals, no resolution needed
            Ok(ResolvedQueryBody::Values(values.clone()))
        }
        QueryBody::SetOp(set_op) => {
            let left = query_expr_resolve(&set_op.left, tables, search_path)?;
            let right = query_expr_resolve(&set_op.right, tables, search_path)?;
            Ok(ResolvedQueryBody::SetOp(ResolvedSetOpNode {
                op: set_op.op,
                all: set_op.all,
                left: Box::new(left),
                right: Box::new(right),
            }))
        }
    }
}

/// Resolve a SelectNode to a ResolvedSelectNode
pub fn select_node_resolve(
    select: &SelectNode,
    tables: &BiHashMap<TableMetadata>,
    search_path: &[&str],
) -> ResolveResult<ResolvedSelectNode> {
    let mut scope = ResolutionScope::new(tables, search_path);
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

/// Resolve a QueryExpr using a pre-built outer_tables context, collecting outer refs.
///
/// Used by `ResolutionScope::subquery_resolve` to resolve correlated subquery bodies.
/// Returns the resolved query and the outer column references found within it.
pub(super) fn query_expr_resolve_scoped(
    query: &QueryExpr,
    catalog_tables: &BiHashMap<TableMetadata>,
    search_path: &[&str],
    outer_tables: Vec<(TableMetadata, Option<String>)>,
) -> ResolveResult<(ResolvedQueryExpr, Vec<ResolvedColumnNode>)> {
    let mut scope = ResolutionScope::new_with_outer(catalog_tables, search_path, outer_tables);

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
            )?;
            let (right_resolved, right_outer_refs) =
                query_expr_resolve_scoped(&set_op.right, catalog_tables, search_path, outer)?;
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

    // ORDER BY: build a fresh scope with the same outer_tables so correlated refs
    // in ORDER BY (rare but possible) are handled correctly.
    let order_by = match &query.body {
        QueryBody::Select(select) => {
            let mut order_scope = ResolutionScope::new_with_outer(
                catalog_tables,
                search_path,
                scope.outer_tables.clone(),
            );
            for table_source in &select.from {
                let _ = table_source_resolve(
                    table_source,
                    catalog_tables,
                    &mut order_scope,
                    search_path,
                );
            }
            order_by_resolve(&query.order_by, &mut order_scope, body.select_columns())?
        }
        QueryBody::SetOp(_) | QueryBody::Values(_) => order_by_as_identifiers(&query.order_by),
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
