//! Resolve a column reference against the scope, falling back to the outer
//! scope's tables — which is what marks a reference as correlated.

use ecow::EcoString;
use rootcause::Report;

use super::bindings::{ColumnBinding, ColumnTarget, MergedId, SlotId};
use super::scope::ResolutionScope;
use crate::catalog::{ColumnMetadata, TableMetadata};
use crate::query::ast::ColumnNode;
use crate::query::resolved::{ResolveError, ResolveResult, ResolvedColumnNode, ResolvedScalarExpr};

/// What a column name denotes in scope: the resolved node the resolved tree
/// carries, and the binding the plain tree records for it.
pub(super) enum ColumnLookup {
    Column {
        resolved: ResolvedColumnNode,
        binding: ColumnBinding,
        correlated: bool,
    },
    /// An inner join's `USING`/`NATURAL` merged column, which is exactly its
    /// left column.
    Merged {
        resolved: ResolvedColumnNode,
        merged: MergedId,
    },
}

/// Resolve a column reference to a resolved column node, recording its
/// binding against the node.
pub(super) fn column_resolve(
    column_node: &ColumnNode,
    scope: &mut ResolutionScope<'_>,
) -> ResolveResult<ResolvedColumnNode> {
    match column_lookup(column_node, scope)? {
        ColumnLookup::Column {
            resolved,
            binding,
            correlated,
        } => {
            scope
                .bindings
                .column_bind(column_node.id, ColumnTarget::Column(binding), correlated);
            Ok(resolved)
        }
        ColumnLookup::Merged { resolved, merged } => {
            scope
                .bindings
                .column_bind(column_node.id, ColumnTarget::Merged(merged), false);
            Ok(resolved)
        }
    }
}

/// Look a column reference up without recording a binding, for callers
/// resolving a synthesized reference rather than a node of the tree.
///
/// When the column cannot be found in the inner scope and the scope has
/// `outer_tables` set (i.e. we are inside a subquery body), the outer tables
/// are tried as a fallback. On a successful outer-table match the resolved node
/// is recorded in `scope.outer_refs` — marking this as a correlated reference —
/// and returned as a normal column node so the inner query remains fully resolved.
pub(super) fn column_lookup(
    column_node: &ColumnNode,
    scope: &mut ResolutionScope<'_>,
) -> ResolveResult<ColumnLookup> {
    let column_name = &column_node.column;

    // Table-qualified reference (e.g. `o.id`)
    if let Some(table_qualifier) = &column_node.table {
        // Try inner scope first
        if let Some(entry) = scope.table_scope_find(table_qualifier) {
            let table_metadata = entry.metadata();
            let column_metadata = table_metadata
                .columns
                .get(column_name.as_str())
                .ok_or_else(|| column_not_found(&table_metadata.name, column_name))?;
            return Ok(ColumnLookup::Column {
                resolved: column_resolved(table_metadata, entry.alias(), column_metadata),
                binding: column_binding(entry.slot(), column_metadata),
                correlated: false,
            });
        }

        // Fall back to outer scope (correlated reference)
        if let Some(outer) = scope.outer_table_scope_find(table_qualifier) {
            let column_metadata = outer
                .table
                .columns
                .get(column_name.as_str())
                .ok_or_else(|| column_not_found(&outer.table.name, column_name))?;
            let resolved = column_resolved(&outer.table, outer.alias.as_deref(), column_metadata);
            let binding = column_binding(outer.slot, column_metadata);
            scope.outer_refs.push(resolved.clone());
            return Ok(ColumnLookup::Column {
                resolved,
                binding,
                correlated: true,
            });
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
            return Ok(ColumnLookup::Merged {
                resolved: c.clone(),
                merged: merged.merged,
            });
        }
        return Err(Report::from(ResolveError::UnsupportedJoinQualifier));
    }

    // Unqualified column — search inner scope first
    let matches = scope.column_matches_find(column_name.as_str());
    match matches.as_slice() {
        [] => {
            // Fall back to outer scope (correlated reference)
            let outer_match = scope.outer_tables.iter().find_map(|outer| {
                outer
                    .table
                    .columns
                    .get(column_name.as_str())
                    .map(|col_meta| (outer, col_meta))
            });
            if let Some((outer, column_metadata)) = outer_match {
                let resolved =
                    column_resolved(&outer.table, outer.alias.as_deref(), column_metadata);
                let binding = column_binding(outer.slot, column_metadata);
                scope.outer_refs.push(resolved.clone());
                return Ok(ColumnLookup::Column {
                    resolved,
                    binding,
                    correlated: true,
                });
            }
            Err(column_not_found("<unknown>", column_name))
        }
        [(entry, column_metadata)] => Ok(ColumnLookup::Column {
            resolved: column_resolved(entry.metadata(), entry.alias(), column_metadata),
            binding: column_binding(entry.slot(), column_metadata),
            correlated: false,
        }),
        _ => Err(Report::from(ResolveError::AmbiguousColumn {
            column: column_name.to_string(),
        })),
    }
}

fn column_resolved(
    table_metadata: &TableMetadata,
    alias: Option<&str>,
    column_metadata: &ColumnMetadata,
) -> ResolvedColumnNode {
    ResolvedColumnNode {
        schema: table_metadata.schema.clone(),
        table: table_metadata.name.clone(),
        table_alias: alias.map(EcoString::from),
        column: column_metadata.name.clone(),
        column_metadata: column_metadata.clone(),
    }
}

fn column_binding(slot: SlotId, column_metadata: &ColumnMetadata) -> ColumnBinding {
    ColumnBinding {
        slot,
        position: column_metadata.position,
    }
}

fn column_not_found(table: &str, column: &str) -> Report<ResolveError> {
    Report::from(ResolveError::ColumnNotFound {
        table: table.to_owned(),
        column: column.to_owned(),
    })
}
