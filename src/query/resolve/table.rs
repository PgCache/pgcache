//! Bind a FROM entry: catalog lookup for a table reference, and the walk
//! over tables, joins and derived tables.

use std::sync::Arc;

use ecow::EcoString;
use rootcause::Report;

use super::bindings::TableSlot;
use super::entry::query_resolve_scoped;
use super::expr::where_resolve;
use super::join_using::{JoinScopeRanges, join_natural_common_columns, join_using_or_cross};
use super::scope::ResolutionScope;
use super::{ResolveError, ResolveResult};
use crate::catalog::{Catalog, CatalogTable};
use crate::query::ast::rope::{CteRef, Join, JoinQual, Source, Subquery, Table};

/// Bind a table source (table, join, or subquery) into the scope.
pub(super) fn source_resolve<'a>(
    source: Source<'a>,
    tables: &'a Catalog,
    scope: &mut ResolutionScope<'a>,
    search_path: &[&'a str],
) -> ResolveResult<()> {
    match source {
        Source::Table(table) => table_resolve(table, tables, scope, search_path),
        Source::Join(join) => join_resolve(join, tables, scope, search_path),
        Source::Subquery(subquery) => subquery_resolve(subquery, tables, scope, search_path),
        Source::CteRef(cte_ref) => cte_ref_resolve(cte_ref, tables, scope, search_path),
    }
}

fn table_resolve<'a>(
    table: Table<'a>,
    tables: &'a Catalog,
    scope: &mut ResolutionScope<'a>,
    search_path: &[&'a str],
) -> ResolveResult<()> {
    let table_metadata = table_metadata_find(
        table.schema().map(EcoString::as_str),
        table.name(),
        tables,
        search_path,
    )
    .ok_or_else(|| {
        Report::from(ResolveError::TableNotFound {
            name: table.name().to_string(),
        })
    })?;
    let slot = scope.bindings.slot_push(
        table.id(),
        TableSlot::Base {
            table: Arc::clone(table_metadata.shared()),
            alias: table.alias().cloned(),
        },
    );
    scope.table_scope_add(table_metadata, table.alias().map(EcoString::as_str), slot);
    Ok(())
}

/// Bind both sides, then the qualification: `USING`/`NATURAL` add
/// merged-column scope entries so `*` and unqualified references see the
/// single merged column; `CROSS` has no predicate.
fn join_resolve<'a>(
    join: Join<'a>,
    tables: &'a Catalog,
    scope: &mut ResolutionScope<'a>,
    search_path: &[&'a str],
) -> ResolveResult<()> {
    // Scope index ranges per side, to qualify USING/NATURAL columns to the
    // input that exposes them.
    let left_lo = scope.entries.len();
    source_resolve(join.left(), tables, scope, search_path)?;
    let mid = scope.entries.len();
    source_resolve(join.right(), tables, scope, search_path)?;
    let ranges = JoinScopeRanges {
        left_lo,
        mid,
        hi: scope.entries.len(),
    };
    let join_type = join.join_type();
    match join.qual() {
        JoinQual::On(condition) => where_resolve(condition, scope),
        JoinQual::Cross => Ok(()),
        JoinQual::Using(columns) => {
            let columns = columns.map(|name| name.text().clone()).collect();
            join_using_or_cross(scope, ranges, columns, join_type, join.id())
        }
        JoinQual::Natural => {
            let columns = join_natural_common_columns(scope, ranges);
            join_using_or_cross(scope, ranges, columns, join_type, join.id())
        }
    }
}

/// A FROM subquery binds in a fresh scope: LATERAL, which would need the
/// outer scope, is unsupported and reports its outer table as not found.
fn subquery_resolve<'a>(
    subquery: Subquery<'a>,
    tables: &'a Catalog,
    scope: &mut ResolutionScope<'a>,
    search_path: &[&'a str],
) -> ResolveResult<()> {
    let alias = subquery.alias().ok_or_else(|| {
        Report::from(ResolveError::SubqueryAliasNotFound {
            alias: "<missing>".to_owned(),
        })
    })?;
    let outputs = query_resolve_scoped(
        subquery.query(),
        tables,
        search_path,
        Vec::new(),
        &mut *scope.bindings,
    )?;
    scope.derived_table_scope_add(&outputs, alias, subquery.id())
}

/// A CTE body binds in a fresh scope, never correlated.
fn cte_ref_resolve<'a>(
    cte_ref: CteRef<'a>,
    tables: &'a Catalog,
    scope: &mut ResolutionScope<'a>,
    search_path: &[&'a str],
) -> ResolveResult<()> {
    let alias_name = cte_ref.alias().unwrap_or_else(|| cte_ref.cte_name());
    let outputs = query_resolve_scoped(
        cte_ref.query(),
        tables,
        search_path,
        Vec::new(),
        &mut *scope.bindings,
    )?;
    scope.derived_table_scope_add(&outputs, alias_name, cte_ref.id())
}

/// Find table metadata for a table reference.
///
/// If the table has an explicit schema qualifier, use it directly.
/// Otherwise, search through the search_path schemas in order.
fn table_metadata_find<'map>(
    schema: Option<&'map str>,
    table_name: &'map str,
    tables: &'map Catalog,
    search_path: &[&'map str],
) -> Option<&'map CatalogTable> {
    if let Some(schema) = schema {
        return tables.get2(&(schema, table_name));
    }
    search_path
        .iter()
        .find_map(|schema| tables.get2(&(*schema, table_name)))
}
