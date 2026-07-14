//! Resolve a FROM entry: catalog lookup for a table reference, and the
//! recursive walk over tables, joins and derived tables.

use ecow::EcoString;
use iddqd::BiHashMap;
use rootcause::Report;

use crate::cache::SubqueryKind;
use crate::catalog::TableMetadata;
use crate::query::ast::{JoinQual, TableAlias, TableNode, TableSource};
use crate::query::resolved::{
    ResolveError, ResolveResult, ResolvedJoinNode, ResolvedJoinQual, ResolvedTableNode,
    ResolvedTableSource, ResolvedTableSubqueryNode,
};

use super::entry::query_expr_resolve;
use super::expr::where_expr_resolve;
use super::join_using::{JoinScopeRanges, join_natural_common_columns, join_using_or_cross};
use super::scope::ResolutionScope;

/// Find table metadata for a table reference.
///
/// If the table has an explicit schema qualifier, use it directly.
/// Otherwise, search through the search_path schemas in order.
pub(super) fn table_metadata_find<'map, 'node: 'map>(
    table_node: &'node TableNode,
    tables: &'map BiHashMap<TableMetadata>,
    search_path: &[&'map str],
) -> Option<&'map TableMetadata> {
    let table_name = table_node.name.as_str();

    // If table has explicit schema, use it directly
    if let Some(schema) = &table_node.schema {
        let table_metadata = tables.get2(&(schema.as_str(), table_name))?;
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

/// Resolve a table source (table, join, or subquery)
pub(super) fn table_source_resolve<'a>(
    source: &'a TableSource,
    tables: &'a BiHashMap<TableMetadata>,
    scope: &mut ResolutionScope<'a>,
    search_path: &[&'a str],
) -> ResolveResult<ResolvedTableSource> {
    match source {
        TableSource::Table(table_node) => {
            // First find the table metadata (which gives us the schema)
            let table_metadata =
                table_metadata_find(table_node, tables, search_path).ok_or_else(|| {
                    Report::from(ResolveError::TableNotFound {
                        name: table_node.name.to_string(),
                    })
                })?;

            scope.table_scope_add(
                table_metadata,
                table_node.alias.as_ref().map(|a| a.name.as_str()),
            );

            let resolved = ResolvedTableNode {
                schema: table_metadata.schema.clone(),
                name: table_metadata.name.clone(),
                alias: table_node.alias.as_ref().map(|a| a.name.as_str().into()),
                relation_oid: table_metadata.relation_oid,
            };

            Ok(ResolvedTableSource::Table(resolved))
        }
        TableSource::Join(join_node) => {
            // Scope index ranges per side, to qualify USING/NATURAL
            // columns to the input that exposes them.
            let left_lo = scope.entries.len();
            let resolved_left = table_source_resolve(&join_node.left, tables, scope, search_path)?;
            let mid = scope.entries.len();
            let resolved_right =
                table_source_resolve(&join_node.right, tables, scope, search_path)?;
            let ranges = JoinScopeRanges {
                left_lo,
                mid,
                hi: scope.entries.len(),
            };

            // `USING`/`NATURAL` keep their qualifier (deparsed verbatim
            // so Postgres merges the join column) and carry a synthesized
            // equi-`predicate` for freshness/invalidation analysis, plus
            // merged-column scope entries so `*` / unqualified refs see
            // the single merged column. `Cross` has no join predicate —
            // a cartesian product; filtering lives in WHERE.
            let jt = join_node.join_type;
            let qual = match &join_node.qual {
                JoinQual::On(cond) => ResolvedJoinQual::On(where_expr_resolve(cond, scope)?),
                JoinQual::Cross => ResolvedJoinQual::Cross,
                JoinQual::Using(cols) => join_using_or_cross(scope, ranges, cols.clone(), jt)?,
                JoinQual::Natural => {
                    let cols = join_natural_common_columns(scope, ranges);
                    join_using_or_cross(scope, ranges, cols, jt)?
                }
            };

            Ok(ResolvedTableSource::Join(Box::new(ResolvedJoinNode {
                join_type: join_node.join_type,
                left: resolved_left,
                right: resolved_right,
                qual,
            })))
        }
        TableSource::Subquery(subquery) => {
            // Require alias for table subqueries
            let alias = subquery.alias.as_ref().ok_or_else(|| {
                Report::from(ResolveError::SubqueryAliasNotFound {
                    alias: "<missing>".to_owned(),
                })
            })?;

            // FROM-clause subqueries use a fresh scope — LATERAL (which would require access
            // to the outer scope) is not supported and produces TableNotFound if attempted.
            let resolved_query = query_expr_resolve(&subquery.query, tables, search_path)?;

            // Add derived table to outer scope so outer columns can reference it
            scope.derived_table_scope_add(&resolved_query, &alias.name);

            Ok(ResolvedTableSource::Subquery(ResolvedTableSubqueryNode {
                query: Box::new(resolved_query),
                alias: alias.clone(),
                subquery_kind: SubqueryKind::Inclusion,
            }))
        }
        TableSource::CteRef(cte_ref) => {
            let alias_name = cte_ref
                .alias
                .as_ref()
                .map(|a| a.name.as_str())
                .unwrap_or(&cte_ref.cte_name);

            // CTE bodies use a fresh scope (non-correlated)
            let resolved_query = query_expr_resolve(&cte_ref.query, tables, search_path)?;

            let alias = TableAlias {
                name: EcoString::from(alias_name),
                columns: cte_ref.column_aliases.clone(),
            };

            scope.derived_table_scope_add(&resolved_query, alias_name);

            Ok(ResolvedTableSource::Subquery(ResolvedTableSubqueryNode {
                query: Box::new(resolved_query),
                alias,
                subquery_kind: SubqueryKind::Inclusion,
            }))
        }
    }
}
