//! Bind a column reference against the scope, falling back to the outer
//! scope's tables — which is what marks a reference as correlated.

use rootcause::Report;

use super::bindings::{ColumnBinding, ColumnTarget, MergedId, SlotId};
use super::scope::ResolutionScope;
use crate::catalog::ColumnMetadata;
use crate::query::ast::NodeId;
use crate::query::resolve::{ResolveError, ResolveResult};

/// What a column name denotes in scope.
pub(super) enum ColumnLookup {
    Column {
        binding: ColumnBinding,
        correlated: bool,
    },
    /// An inner join's `USING`/`NATURAL` merged column, which is exactly its
    /// left column.
    Merged { merged: MergedId },
}

/// Bind the column reference at `node`, recording its target against it.
pub(super) fn column_resolve(
    node: NodeId,
    qualifier: Option<&str>,
    column: &str,
    scope: &mut ResolutionScope<'_>,
) -> ResolveResult<ColumnLookup> {
    let lookup = column_lookup(qualifier, column, scope)?;
    match &lookup {
        ColumnLookup::Column {
            binding,
            correlated,
        } => scope
            .bindings
            .column_bind(node, ColumnTarget::Column(*binding), *correlated),
        ColumnLookup::Merged { merged } => {
            scope
                .bindings
                .column_bind(node, ColumnTarget::Merged(*merged), false);
        }
    }
    Ok(lookup)
}

/// Look a column reference up without recording a binding, for callers
/// resolving a synthesized reference rather than a node of the tree.
///
/// When the column cannot be found in the inner scope and the scope has
/// `outer_tables` set (i.e. we are inside a subquery body), the outer tables
/// are tried as a fallback; a match there is a correlated reference.
pub(super) fn column_lookup(
    qualifier: Option<&str>,
    column_name: &str,
    scope: &mut ResolutionScope<'_>,
) -> ResolveResult<ColumnLookup> {
    // Table-qualified reference (e.g. `o.id`)
    if let Some(table_qualifier) = qualifier {
        // Try inner scope first
        if let Some(entry) = scope.table_scope_find(table_qualifier) {
            let table_metadata = entry.metadata();
            let column_metadata = table_metadata
                .columns
                .get(column_name)
                .ok_or_else(|| column_not_found(&table_metadata.name, column_name))?;
            return Ok(ColumnLookup::Column {
                binding: column_binding(entry.slot(), column_metadata),
                correlated: false,
            });
        }

        // Fall back to outer scope (correlated reference)
        if let Some(outer) = scope.outer_table_scope_find(table_qualifier) {
            let column_metadata = outer
                .table
                .columns
                .get(column_name)
                .ok_or_else(|| column_not_found(&outer.table.name, column_name))?;
            return Ok(ColumnLookup::Column {
                binding: column_binding(outer.slot, column_metadata),
                correlated: true,
            });
        }

        return Err(Report::from(ResolveError::TableNotFound {
            name: table_qualifier.to_owned(),
        }));
    }

    // Unqualified reference to a USING/NATURAL merged column. The scalar
    // path intercepts this earlier; reaching here means a column-node-only
    // context (GROUP BY): an inner join's merged column is the left column;
    // an outer join's is `COALESCE`, which is not a column, so forward the
    // query.
    if let Some(merged) = scope.merged_column_find(column_name) {
        if !merged.outer {
            return Ok(ColumnLookup::Merged {
                merged: merged.merged,
            });
        }
        return Err(Report::from(ResolveError::UnsupportedJoinQualifier));
    }

    // Unqualified column — search inner scope first
    let matches = scope.column_matches_find(column_name);
    match matches.as_slice() {
        [] => {
            // Fall back to outer scope (correlated reference)
            let outer_match = scope.outer_tables.iter().find_map(|outer| {
                outer
                    .table
                    .columns
                    .get(column_name)
                    .map(|col_meta| (outer, col_meta))
            });
            if let Some((outer, column_metadata)) = outer_match {
                return Ok(ColumnLookup::Column {
                    binding: column_binding(outer.slot, column_metadata),
                    correlated: true,
                });
            }
            Err(column_not_found("<unknown>", column_name))
        }
        [(entry, column_metadata)] => Ok(ColumnLookup::Column {
            binding: column_binding(entry.slot(), column_metadata),
            correlated: false,
        }),
        _ => Err(Report::from(ResolveError::AmbiguousColumn {
            column: column_name.to_owned(),
        })),
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
