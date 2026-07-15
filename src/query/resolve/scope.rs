//! The resolution scope: which tables, aliases, derived tables and merged
//! join columns are visible while resolving a query, plus the derived-table
//! column extraction that feeds it.

use ecow::EcoString;
use iddqd::BiHashMap;
use tokio_postgres::types::Type;

use crate::catalog::{ColumnMetadata, ColumnStore, TableMetadata};
use crate::oid::Oid;
use crate::query::ast::QueryExpr;
use crate::query::resolved::{
    ResolveResult, ResolvedColumnNode, ResolvedQueryBody, ResolvedQueryExpr, ResolvedScalarExpr,
    ResolvedSelectColumns,
};

use super::entry::query_expr_resolve_scoped;
use super::join_using::MergedJoinColumn;

/// A FROM-clause entry visible in scope. Entries are kept in FROM order,
/// which `*` expansion must follow (PostgreSQL expands `*` in FROM order).
#[derive(Debug)]
pub(super) enum ScopeEntry<'a> {
    /// Catalog (base) table with optional alias.
    Base {
        metadata: &'a TableMetadata,
        alias: Option<&'a str>,
    },
    /// Derived table (FROM subquery) with owned synthetic metadata whose
    /// columns are determined by the subquery's output.
    Derived {
        metadata: TableMetadata,
        alias: EcoString,
    },
}

impl ScopeEntry<'_> {
    pub(super) fn metadata(&self) -> &TableMetadata {
        match self {
            ScopeEntry::Base { metadata, .. } => metadata,
            ScopeEntry::Derived { metadata, .. } => metadata,
        }
    }

    pub(super) fn alias(&self) -> Option<&str> {
        match self {
            ScopeEntry::Base { alias, .. } => *alias,
            ScopeEntry::Derived { alias, .. } => Some(alias.as_str()),
        }
    }

    /// The scope key a column of this entry is qualified by: the alias,
    /// else the table name. Matches the `merged_consumed` key convention.
    pub(super) fn qualifier_key(&self) -> &str {
        self.alias().unwrap_or(self.metadata().name.as_str())
    }

    /// Whether a query qualifier (e.g. the `t` of `t.*`) refers to this
    /// entry: alias match, else table-name match. A derived table's
    /// synthetic metadata is named after its alias, so both arms agree.
    pub(super) fn qualifier_matches(&self, qualifier: &str) -> bool {
        self.alias().is_some_and(|a| a == qualifier) || self.metadata().name == qualifier
    }
}

/// Resolution scope tracking available tables and their aliases.
#[derive(Debug)]
pub(super) struct ResolutionScope<'a> {
    /// Base and derived tables visible in this scope, in FROM order.
    pub(super) entries: Vec<ScopeEntry<'a>>,
    /// Catalog of all known tables (for subquery resolution)
    pub(super) catalog_tables: &'a BiHashMap<TableMetadata>,
    /// Search path for schema resolution
    pub(super) search_path: Vec<&'a str>,
    /// Owned snapshot of ancestor scope tables for correlated reference fallback.
    /// Populated when this scope was created for a WHERE/SELECT subquery body.
    /// Empty at top level.
    pub(super) outer_tables: Vec<(TableMetadata, Option<String>)>,
    /// Correlated column references found during resolution of this scope's expressions.
    /// Populated by `column_resolve` when it falls back to `outer_tables`.
    pub(super) outer_refs: Vec<ResolvedColumnNode>,
    /// `USING`/`NATURAL` merged join columns, in `*`-expansion order.
    pub(super) merged_columns: Vec<MergedJoinColumn>,
    /// `(scope-key, column)` pairs consumed into a merge — skipped by
    /// unqualified `*` (the single merged column replaces them).
    pub(super) merged_consumed: Vec<(EcoString, EcoString)>,
}

impl<'a> ResolutionScope<'a> {
    pub(super) fn new(
        catalog_tables: &'a BiHashMap<TableMetadata>,
        search_path: &[&'a str],
    ) -> Self {
        Self {
            entries: Vec::new(),
            catalog_tables,
            search_path: search_path.to_vec(),
            outer_tables: Vec::new(),
            outer_refs: Vec::new(),
            merged_columns: Vec::new(),
            merged_consumed: Vec::new(),
        }
    }

    /// Create a scope for resolving an inner subquery body.
    ///
    /// `outer_tables` is an owned snapshot of the ancestor scopes' tables, used as
    /// fallback when column resolution fails in this scope (correlated references).
    pub(super) fn new_with_outer(
        catalog_tables: &'a BiHashMap<TableMetadata>,
        search_path: &[&'a str],
        outer_tables: Vec<(TableMetadata, Option<String>)>,
    ) -> Self {
        Self {
            entries: Vec::new(),
            catalog_tables,
            search_path: search_path.to_vec(),
            outer_tables,
            outer_refs: Vec::new(),
            merged_columns: Vec::new(),
            merged_consumed: Vec::new(),
        }
    }

    /// Snapshot the current scope's tables (including derived and outer) for passing
    /// to a child subquery scope. The child needs access to all ancestor tables.
    pub(super) fn scope_tables_snapshot(&self) -> Vec<(TableMetadata, Option<String>)> {
        let mut snapshot: Vec<(TableMetadata, Option<String>)> = self
            .entries
            .iter()
            .map(|entry| (entry.metadata().clone(), entry.alias().map(str::to_owned)))
            .collect();
        // Include ancestors so nested correlation can reach any level
        snapshot.extend(self.outer_tables.iter().cloned());
        snapshot
    }

    /// Find a table in the outer scope by name or alias.
    pub(super) fn outer_table_scope_find(
        &self,
        name: &str,
    ) -> Option<(&TableMetadata, Option<&str>)> {
        self.outer_tables
            .iter()
            .find(|(meta, alias)| {
                if let Some(alias_name) = alias {
                    alias_name == name
                } else {
                    meta.name == name
                }
            })
            .map(|(meta, alias)| (meta, alias.as_deref()))
    }

    /// Add a table to the scope
    pub(super) fn table_scope_add(&mut self, metadata: &'a TableMetadata, alias: Option<&'a str>) {
        self.entries.push(ScopeEntry::Base { metadata, alias });
    }

    /// A `USING`/`NATURAL` merged join column by (output) name.
    pub(super) fn merged_column_find(&self, name: &str) -> Option<&MergedJoinColumn> {
        self.merged_columns.iter().find(|m| m.name == name)
    }

    /// Find table metadata by name or alias.
    /// Checks both catalog tables and derived tables (FROM subqueries).
    pub(super) fn table_scope_find(&self, name: &str) -> Option<(&TableMetadata, Option<&str>)> {
        // Alias-EXCLUSIVE matching, unlike `qualifier_matches`: an aliased
        // table is not referencable by its underlying name (and the VALUES
        // transform relies on this — it aliases a replacement subquery with
        // the original table name, which must not resolve to the base
        // entry's still-aliased occurrence).
        self.entries
            .iter()
            .find(|entry| match entry {
                ScopeEntry::Base { metadata, alias } => {
                    alias.map_or(metadata.name == name, |a| a == name)
                }
                ScopeEntry::Derived { alias, .. } => alias == name,
            })
            .map(|entry| (entry.metadata(), entry.alias()))
    }

    /// Add a derived table (FROM subquery) to the scope.
    ///
    /// Extracts output columns from the resolved inner query and creates synthetic
    /// `TableMetadata` so the outer query can resolve column references against
    /// the subquery alias.
    pub(super) fn derived_table_scope_add(
        &mut self,
        resolved_query: &ResolvedQueryExpr,
        alias: &str,
    ) {
        let columns = derived_table_columns_extract(resolved_query);

        let synthetic_metadata = TableMetadata {
            replica_identity_full: false,
            relation_oid: Oid::from_raw(0),
            name: alias.into(),
            schema: "".into(),
            primary_key_columns: Vec::new(),
            columns: ColumnStore::new(columns),
            indexes: Vec::new(),
        };

        self.entries.push(ScopeEntry::Derived {
            metadata: synthetic_metadata,
            alias: alias.into(),
        });
    }

    /// Find all tables in scope that contain a given column (for unqualified column resolution).
    pub(super) fn column_matches_find<'b>(
        &'b self,
        column: &str,
    ) -> Vec<(&'b TableMetadata, Option<&'b str>, &'b ColumnMetadata)> {
        let mut matches = Vec::new();

        for entry in &self.entries {
            if let Some(col_meta) = entry.metadata().columns.get(column) {
                matches.push((entry.metadata(), entry.alias(), col_meta));
            }
        }

        matches
    }

    /// Resolve an inner subquery, collecting any outer column references.
    ///
    /// Used for WHERE-clause and SELECT-list subqueries where correlated references
    /// are allowed. Outer column references are resolved against `outer_tables` and
    /// collected; they appear as normal `Column` nodes in the resolved inner query.
    pub(super) fn subquery_resolve(
        &self,
        query: &QueryExpr,
    ) -> ResolveResult<(ResolvedQueryExpr, Vec<ResolvedColumnNode>)> {
        query_expr_resolve_scoped(
            query,
            self.catalog_tables,
            &self.search_path,
            self.scope_tables_snapshot(),
        )
    }
}

/// Extract output column metadata from a resolved query for derived table scope.
///
/// Handles the three cases:
/// - `SELECT *`: returns all columns from all tables in the inner query
/// - `SELECT col1, col2`: returns column metadata for each, using aliases as names
/// - `SELECT <none>`: returns empty (e.g., EXISTS subqueries)
fn derived_table_columns_extract(resolved_query: &ResolvedQueryExpr) -> Vec<ColumnMetadata> {
    let select = match &resolved_query.body {
        ResolvedQueryBody::Select(select) => select,
        // Set operation output columns are defined by the leftmost SELECT
        ResolvedQueryBody::SetOp(set_op) => {
            return derived_table_columns_extract(&set_op.left);
        }
        ResolvedQueryBody::Values(_) => return Vec::new(),
    };

    match &select.columns {
        ResolvedSelectColumns::None => Vec::new(),
        ResolvedSelectColumns::Columns(cols) => cols
            .iter()
            .enumerate()
            .filter_map(|(i, col)| {
                // Functions, literals, etc. without an alias have no stable
                // output name — skip them.
                let name = col.output_name()?.clone();

                // Use column metadata from the source column if available,
                // otherwise create a synthetic entry with TEXT type
                let base_meta = match &col.expr {
                    ResolvedScalarExpr::Column(c) => c.column_metadata.clone(),
                    ResolvedScalarExpr::Identifier(_)
                    | ResolvedScalarExpr::Function(_)
                    | ResolvedScalarExpr::Literal(_)
                    | ResolvedScalarExpr::Case(_)
                    | ResolvedScalarExpr::Arithmetic(_)
                    | ResolvedScalarExpr::Subquery(..)
                    | ResolvedScalarExpr::Array(_)
                    | ResolvedScalarExpr::TypeCast { .. } => ColumnMetadata {
                        name: name.clone(),
                        position: i16::try_from(i + 1).expect("column position fits in i16"),
                        type_oid: 25, // TEXT OID
                        data_type: Type::TEXT,
                        type_name: EcoString::from("text"),
                        cache_type_name: EcoString::from("text"),
                        is_primary_key: false,
                    },
                };

                // Override name with alias if provided (the column metadata
                // from the source has the original name)
                Some(ColumnMetadata {
                    name,
                    position: i16::try_from(i + 1).expect("column position fits in i16"),
                    ..base_meta
                })
            })
            .collect(),
    }
}
