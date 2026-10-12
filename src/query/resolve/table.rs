//! Bind a FROM entry: catalog lookup for a table reference, and the
//! recursive walk over tables, joins and derived tables.

use std::sync::Arc;

use rootcause::Report;

use super::bindings::TableSlot;
use super::entry::query_expr_resolve_scoped;
use super::expr::where_expr_resolve;
use super::join_using::{JoinScopeRanges, join_natural_common_columns, join_using_or_cross};
use super::scope::ResolutionScope;
use crate::catalog::{Catalog, CatalogTable};
use crate::query::ast::{JoinQual, TableSource};
use crate::query::resolve::{ResolveError, ResolveResult};

/// Find table metadata for a table reference.
///
/// If the table has an explicit schema qualifier, use it directly.
/// Otherwise, search through the search_path schemas in order.
pub(super) fn table_metadata_find<'map>(
    schema: Option<&'map str>,
    table_name: &'map str,
    tables: &'map Catalog,
    search_path: &[&'map str],
) -> Option<&'map CatalogTable> {
    // If table has explicit schema, use it directly
    if let Some(schema) = schema {
        let table_metadata = tables.get2(&(schema, table_name))?;
        return Some(table_metadata);
    }

    // Search through search_path schemas in order
    for schema in search_path {
        if let Some(table_metadata) = tables.get2(&(*schema, table_name)) {
            return Some(table_metadata);
        }
    }

    None
}

/// Bind a table source (table, join, or subquery) into the scope.
pub(super) fn table_source_resolve<'a>(
    source: &'a TableSource,
    tables: &'a Catalog,
    scope: &mut ResolutionScope<'a>,
    search_path: &[&'a str],
) -> ResolveResult<()> {
    match source {
        TableSource::Table(table_node) => {
            let table_metadata = table_metadata_find(
                table_node.schema.as_deref(),
                &table_node.name,
                tables,
                search_path,
            )
            .ok_or_else(|| {
                Report::from(ResolveError::TableNotFound {
                    name: table_node.name.to_string(),
                })
            })?;

            let slot = scope.bindings.slot_push(
                table_node.id,
                TableSlot::Base {
                    table: Arc::clone(table_metadata.shared()),
                    alias: table_node.alias.as_ref().map(|a| a.name.clone()),
                },
            );
            scope.table_scope_add(
                table_metadata,
                table_node.alias.as_ref().map(|a| a.name.as_str()),
                slot,
            );
            Ok(())
        }
        TableSource::Join(join_node) => {
            // Scope index ranges per side, to qualify USING/NATURAL
            // columns to the input that exposes them.
            let left_lo = scope.entries.len();
            table_source_resolve(&join_node.left, tables, scope, search_path)?;
            let mid = scope.entries.len();
            table_source_resolve(&join_node.right, tables, scope, search_path)?;
            let ranges = JoinScopeRanges {
                left_lo,
                mid,
                hi: scope.entries.len(),
            };

            // `USING`/`NATURAL` add merged-column scope entries so `*` and
            // unqualified references see the single merged column. `Cross`
            // has no join predicate — a cartesian product; filtering lives
            // in WHERE.
            let jt = join_node.join_type;
            match &join_node.qual {
                JoinQual::On(cond) => where_expr_resolve(cond, scope),
                JoinQual::Cross => Ok(()),
                JoinQual::Using(cols) => {
                    join_using_or_cross(scope, ranges, cols.clone(), jt, join_node.id)
                }
                JoinQual::Natural => {
                    let cols = join_natural_common_columns(scope, ranges);
                    join_using_or_cross(scope, ranges, cols, jt, join_node.id)
                }
            }
        }
        TableSource::Subquery(subquery) => {
            // Require alias for table subqueries
            let alias = subquery.alias.as_ref().ok_or_else(|| {
                Report::from(ResolveError::SubqueryAliasNotFound {
                    alias: "<missing>".to_owned(),
                })
            })?;

            // FROM-clause subqueries use a fresh scope — LATERAL (which
            // would require access to the outer scope) is not supported and
            // produces TableNotFound if attempted.
            let outputs = query_expr_resolve_scoped(
                &subquery.query,
                tables,
                search_path,
                Vec::new(),
                &mut *scope.bindings,
            )?;

            // Add derived table to outer scope so outer columns can reference it
            scope.derived_table_scope_add(&outputs, &alias.name, subquery.id)
        }
        TableSource::CteRef(cte_ref) => {
            let alias_name = cte_ref
                .alias
                .as_ref()
                .map(|a| a.name.as_str())
                .unwrap_or(&cte_ref.cte_name);

            // CTE bodies use a fresh scope (non-correlated)
            let outputs = query_expr_resolve_scoped(
                &cte_ref.query,
                tables,
                search_path,
                Vec::new(),
                &mut *scope.bindings,
            )?;

            scope.derived_table_scope_add(&outputs, alias_name, cte_ref.id)
        }
    }
}
