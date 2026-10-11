//! Resolve the clauses hanging off a SELECT: the target list (including `*`
//! expansion), ORDER BY, GROUP BY, HAVING and LIMIT.

use ecow::EcoString;

use super::column::column_resolve;
use super::expr::{scalar_expr_resolve, where_expr_resolve};
use super::scope::{ResolutionScope, ScopeEntry};
use crate::query::ast::{
    ColumnNode, LimitClause, LiteralValue, OrderByClause, ScalarExpr, SelectColumn, SelectColumns,
    StarNode, WhereExpr,
};
use crate::query::resolve::{BindingsBuilder, ColumnBinding, ColumnTarget, StarItem};
use crate::query::resolved::{
    ResolveResult, ResolvedArithmeticExpr, ResolvedColumnNode, ResolvedFunctionCall,
    ResolvedLimitClause, ResolvedOrderByClause, ResolvedScalarExpr, ResolvedSelectColumn,
    ResolvedSelectColumns, ResolvedWhereExpr,
};

/// Resolve SELECT columns
///
/// Star expressions (`*` or `t1.*`) are expanded inline to all columns from
/// matching tables in scope.
pub(super) fn select_columns_resolve(
    columns: &SelectColumns,
    scope: &mut ResolutionScope<'_>,
) -> ResolveResult<ResolvedSelectColumns> {
    let SelectColumns::Columns(cols) = columns else {
        return Ok(ResolvedSelectColumns::None);
    };
    let mut resolved_cols = Vec::new();
    for col in cols {
        match col {
            SelectColumn::Star(star) => {
                star_expand(star, scope, &mut resolved_cols);
            }
            SelectColumn::Expr { expr, alias } => {
                let resolved_expr = scalar_expr_resolve(expr, scope)?;
                resolved_cols.push(ResolvedSelectColumn {
                    expr: resolved_expr,
                    alias: alias.as_deref().map(EcoString::from),
                });
            }
        }
    }
    Ok(ResolvedSelectColumns::Columns(resolved_cols))
}

/// Expand `*` or `qualifier.*` into the matching tables' columns.
///
/// Unqualified `*` over a USING/NATURAL join emits the merged join column(s)
/// once and first (as Postgres does), then each table's remaining (non-join)
/// columns. Qualified `t.*` is that table's columns verbatim (join column
/// included).
fn star_expand(
    star: &StarNode,
    scope: &mut ResolutionScope<'_>,
    out: &mut Vec<ResolvedSelectColumn>,
) {
    let qualifier = star.qualifier.as_deref();
    let mut items = Vec::new();
    if qualifier.is_none() {
        out.extend(scope.merged_columns.iter().map(|m| ResolvedSelectColumn {
            expr: m.expr.clone(),
            alias: Some(m.name.clone()),
        }));
        items.extend(
            scope
                .merged_columns
                .iter()
                .map(|m| StarItem::Merged(m.merged)),
        );
    }
    for entry in &scope.entries {
        if qualifier.is_none_or(|q| entry.qualifier_matches(q)) {
            entry_columns_expand(entry, qualifier.is_none(), scope, out, &mut items);
        }
    }
    scope.bindings.star_bind(star.id, items);
}

/// One table's columns for `*`. Under unqualified `*`, columns merged into a
/// USING/NATURAL join column are skipped (they were emitted once already).
fn entry_columns_expand(
    entry: &ScopeEntry<'_>,
    unqualified: bool,
    scope: &ResolutionScope<'_>,
    out: &mut Vec<ResolvedSelectColumn>,
    items: &mut Vec<StarItem>,
) {
    let table_metadata = entry.metadata();
    let key = entry.qualifier_key();
    for column_metadata in table_metadata.columns.iter() {
        if unqualified && merged_join_column(scope, key, &column_metadata.name) {
            continue;
        }
        out.push(ResolvedSelectColumn {
            expr: ResolvedScalarExpr::Column(ResolvedColumnNode {
                schema: table_metadata.schema.clone(),
                table: table_metadata.name.clone(),
                table_alias: entry.alias().map(EcoString::from),
                column: column_metadata.name.clone(),
                column_metadata: column_metadata.clone(),
            }),
            alias: None,
        });
        items.push(StarItem::Column(ColumnBinding {
            slot: entry.slot(),
            position: column_metadata.position,
        }));
    }
}

/// Whether `column` of the table keyed `key` was merged into a USING/NATURAL
/// join column.
fn merged_join_column(scope: &ResolutionScope<'_>, key: &str, column: &str) -> bool {
    scope
        .merged_consumed
        .iter()
        .any(|(k, c)| k == key && c == column)
}

/// Resolve ORDER BY clauses. When `select_columns` is provided, an unqualified
/// identifier that matches a SELECT-list output name resolves to
/// `ResolvedScalarExpr::Identifier` — matching PostgreSQL's precedence rule
/// that ORDER BY matches output names before falling back to column lookup.
pub(super) fn order_by_resolve(
    order_by: &[OrderByClause],
    scope: &mut ResolutionScope<'_>,
    select_columns: Option<&ResolvedSelectColumns>,
) -> ResolveResult<Vec<ResolvedOrderByClause>> {
    let mut resolved = Vec::with_capacity(order_by.len());
    for clause in order_by {
        let resolved_expr = match order_by_alias_match(&clause.expr, select_columns) {
            Some((col, ident)) => {
                scope
                    .bindings
                    .column_bind(col.id, ColumnTarget::Output, false);
                ident
            }
            None => scalar_expr_resolve(&clause.expr, scope)?,
        };
        resolved.push(ResolvedOrderByClause {
            expr: resolved_expr,
            direction: clause.direction.clone(),
            null_order: clause.null_order,
        });
    }
    Ok(resolved)
}

/// If `clause_expr` is an unqualified column reference whose name matches a
/// SELECT-list output name, return it with its `Identifier` form.
pub(super) fn order_by_alias_match<'e>(
    clause_expr: &'e ScalarExpr,
    select_columns: Option<&ResolvedSelectColumns>,
) -> Option<(&'e ColumnNode, ResolvedScalarExpr)> {
    let ScalarExpr::Column(col) = clause_expr else {
        return None;
    };
    if col.table.is_some() {
        return None;
    }
    select_columns?.position_by_output_name(col.column.as_str())?;
    Some((col, ResolvedScalarExpr::Identifier(col.column.clone())))
}

/// Convert ORDER BY clauses to use unqualified Identifier expressions.
/// Used for set operations where ORDER BY references output columns by name.
pub(super) fn order_by_as_identifiers(
    order_by: &[OrderByClause],
    bindings: &mut BindingsBuilder,
) -> Vec<ResolvedOrderByClause> {
    order_by
        .iter()
        .map(|clause| {
            let expr = scalar_expr_to_identifier(&clause.expr, bindings);
            ResolvedOrderByClause {
                expr,
                direction: clause.direction.clone(),
                null_order: clause.null_order,
            }
        })
        .collect()
}

/// Convert a ScalarExpr to a ResolvedScalarExpr using unqualified Identifier for columns.
/// Used for ORDER BY in set operations where columns reference output names, not table columns.
pub(super) fn scalar_expr_to_identifier(
    expr: &ScalarExpr,
    bindings: &mut BindingsBuilder,
) -> ResolvedScalarExpr {
    match expr {
        ScalarExpr::Column(col) => {
            bindings.column_bind(col.id, ColumnTarget::Output, false);
            ResolvedScalarExpr::Identifier(col.column.as_str().into())
        }
        ScalarExpr::Literal(lit) => ResolvedScalarExpr::Literal(lit.clone()),
        ScalarExpr::Function(func) => ResolvedScalarExpr::Function(ResolvedFunctionCall {
            name: func.name.as_str().into(),
            args: func
                .args
                .iter()
                .map(|arg| scalar_expr_to_identifier(arg, bindings))
                .collect(),
            agg_star: func.agg_star,
            agg_distinct: func.agg_distinct,
            // Set-op ORDER BY references output column names only;
            // intra-function decorations don't apply.
            agg_order: vec![],
            agg_filter: None,
            over: None,
        }),
        ScalarExpr::Case(_) | ScalarExpr::Subquery(_) => {
            // CASE and subquery expressions in ORDER BY are uncommon; use null as fallback
            ResolvedScalarExpr::Literal(LiteralValue::Null)
        }
        ScalarExpr::Arithmetic(arith) => ResolvedScalarExpr::Arithmetic(ResolvedArithmeticExpr {
            left: Box::new(scalar_expr_to_identifier(&arith.left, bindings)),
            op: arith.op,
            right: Box::new(scalar_expr_to_identifier(&arith.right, bindings)),
        }),
        ScalarExpr::Array(elems) => ResolvedScalarExpr::Array(
            elems
                .iter()
                .map(|elem| scalar_expr_to_identifier(elem, bindings))
                .collect(),
        ),
        ScalarExpr::TypeCast { expr, target } => ResolvedScalarExpr::TypeCast {
            expr: Box::new(scalar_expr_to_identifier(expr, bindings)),
            target: target.clone(),
        },
    }
}

/// Resolve GROUP BY clauses
pub(super) fn group_by_resolve(
    group_by: &[ColumnNode],
    scope: &mut ResolutionScope<'_>,
) -> ResolveResult<Vec<ResolvedColumnNode>> {
    let mut resolved = Vec::with_capacity(group_by.len());
    for col in group_by {
        resolved.push(column_resolve(col, scope)?);
    }
    Ok(resolved)
}

/// Resolve HAVING clause
pub(super) fn having_resolve(
    having: Option<&WhereExpr>,
    scope: &mut ResolutionScope<'_>,
) -> ResolveResult<Option<ResolvedWhereExpr>> {
    match having {
        Some(h) => Ok(Some(where_expr_resolve(h, scope)?)),
        None => Ok(None),
    }
}

/// Resolve LIMIT clause
pub(super) fn limit_resolve(limit: Option<&LimitClause>) -> Option<ResolvedLimitClause> {
    let limit = limit?;

    Some(ResolvedLimitClause {
        count: limit.count.clone(),
        offset: limit.offset.clone(),
    })
}
