//! Resolve a column reference against the scope, falling back to the outer
//! scope's tables — which is what marks a reference as correlated.

use ecow::EcoString;
use rootcause::Report;

use crate::query::ast::ColumnNode;
use crate::query::resolved::{ResolveError, ResolveResult, ResolvedColumnNode, ResolvedScalarExpr};

use super::scope::ResolutionScope;

/// Resolve a column reference to a resolved column node.
///
/// When the column cannot be found in the inner scope and the scope has
/// `outer_tables` set (i.e. we are inside a subquery body), the outer tables
/// are tried as a fallback. On a successful outer-table match the resolved node
/// is recorded in `scope.outer_refs` — marking this as a correlated reference —
/// and returned as a normal column node so the inner query remains fully resolved.
pub(super) fn column_resolve(
    column_node: &ColumnNode,
    scope: &mut ResolutionScope<'_>,
) -> ResolveResult<ResolvedColumnNode> {
    let column_name = &column_node.column;

    // Table-qualified reference (e.g. `o.id`)
    if let Some(table_qualifier) = &column_node.table {
        // Try inner scope first
        if let Some((table_metadata, alias)) = scope.table_scope_find(table_qualifier) {
            let column_metadata = table_metadata
                .columns
                .get(column_name.as_str())
                .ok_or_else(|| {
                    Report::from(ResolveError::ColumnNotFound {
                        table: table_metadata.name.to_string(),
                        column: column_name.to_string(),
                    })
                })?;
            return Ok(ResolvedColumnNode {
                schema: table_metadata.schema.clone(),
                table: table_metadata.name.clone(),
                table_alias: alias.map(EcoString::from),
                column: column_metadata.name.clone(),
                column_metadata: column_metadata.clone(),
            });
        }

        // Fall back to outer scope (correlated reference)
        if let Some((outer_meta, outer_alias)) = scope.outer_table_scope_find(table_qualifier) {
            let column_metadata =
                outer_meta
                    .columns
                    .get(column_name.as_str())
                    .ok_or_else(|| {
                        Report::from(ResolveError::ColumnNotFound {
                            table: outer_meta.name.to_string(),
                            column: column_name.to_string(),
                        })
                    })?;
            let resolved = ResolvedColumnNode {
                schema: outer_meta.schema.clone(),
                table: outer_meta.name.clone(),
                table_alias: outer_alias.map(EcoString::from),
                column: column_metadata.name.clone(),
                column_metadata: column_metadata.clone(),
            };
            scope.outer_refs.push(resolved.clone());
            return Ok(resolved);
        }

        return Err(Report::from(ResolveError::TableNotFound {
            name: table_qualifier.to_string(),
        }));
    }

    // Unqualified reference to a USING/NATURAL merged column. The
    // scalar path intercepts this earlier; reaching here means a
    // column-node-only context (GROUP BY): an inner join's merged
    // column is the left column; an outer join's is `COALESCE`, which
    // is not a column node, so forward the query.
    if let Some(merged) = scope.merged_column_find(column_name.as_str()) {
        // Inner invariant: the merged value is exactly the left column
        // node. Outer: it is `COALESCE`, not a column node → forward.
        if !merged.outer
            && let ResolvedScalarExpr::Column(c) = &merged.expr
        {
            return Ok(c.clone());
        }
        return Err(Report::from(ResolveError::UnsupportedJoinQualifier));
    }

    // Unqualified column — search inner scope first
    let matches = scope.column_matches_find(column_name.as_str());
    match matches.as_slice() {
        [] => {
            // Fall back to outer scope (correlated reference)
            let outer_match = scope.outer_tables.iter().find_map(|(meta, alias)| {
                meta.columns
                    .get(column_name.as_str())
                    .map(|col_meta| (meta, alias.as_deref(), col_meta))
            });
            if let Some((outer_meta, outer_alias, col_meta)) = outer_match {
                let resolved = ResolvedColumnNode {
                    schema: outer_meta.schema.clone(),
                    table: outer_meta.name.clone(),
                    table_alias: outer_alias.map(EcoString::from),
                    column: col_meta.name.clone(),
                    column_metadata: col_meta.clone(),
                };
                scope.outer_refs.push(resolved.clone());
                return Ok(resolved);
            }
            Err(Report::from(ResolveError::ColumnNotFound {
                table: "<unknown>".to_owned(),
                column: column_name.to_string(),
            }))
        }
        [(table_metadata, alias, column_metadata)] => Ok(ResolvedColumnNode {
            schema: table_metadata.schema.clone(),
            table: table_metadata.name.clone(),
            table_alias: alias.map(EcoString::from),
            column: column_metadata.name.clone(),
            column_metadata: (*column_metadata).clone(),
        }),
        _ => Err(Report::from(ResolveError::AmbiguousColumn {
            column: column_name.to_string(),
        })),
    }
}
