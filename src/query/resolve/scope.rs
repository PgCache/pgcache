//! The resolution scope: which tables, aliases, derived tables and merged
//! join columns are visible while binding a query, plus the derived-table
//! column synthesis that feeds it.

use std::sync::Arc;

use ecow::EcoString;
use postgres_types::Type;
use rootcause::Report;

use super::bindings::{BindingsBuilder, ColumnBinding, SlotId, TableSlot};
use super::join_using::MergedJoinColumn;
use super::rope::query_resolve_scoped;
use crate::catalog::{
    Catalog, CatalogTable, ColumnMetadata, ColumnPosition, ColumnStore, TableMetadata,
};
use crate::oid::{Oid, TypeOid};
use crate::query::ast::NodeId;
use crate::query::ast::rope::Query;
use crate::query::resolve::{ResolveError, ResolveResult};

/// A FROM-clause entry visible in scope. Entries are kept in FROM order,
/// which `*` expansion must follow (PostgreSQL expands `*` in FROM order).
#[derive(Debug)]
pub(super) enum ScopeEntry<'a> {
    /// Catalog (base) table with optional alias.
    Base {
        table: &'a CatalogTable,
        alias: Option<&'a str>,
        slot: SlotId,
    },
    /// Derived table (FROM subquery) with synthetic metadata whose columns
    /// are determined by the subquery's output, shared with its slot.
    Derived {
        table: Arc<TableMetadata>,
        alias: EcoString,
        slot: SlotId,
    },
}

impl ScopeEntry<'_> {
    pub(super) fn metadata(&self) -> &TableMetadata {
        match self {
            ScopeEntry::Base { table, .. } => table,
            ScopeEntry::Derived { table, .. } => table,
        }
    }

    fn shared(&self) -> Arc<TableMetadata> {
        match self {
            ScopeEntry::Base { table, .. } => Arc::clone(table.shared()),
            ScopeEntry::Derived { table, .. } => Arc::clone(table),
        }
    }

    pub(super) fn alias(&self) -> Option<&str> {
        match self {
            ScopeEntry::Base { alias, .. } => *alias,
            ScopeEntry::Derived { alias, .. } => Some(alias.as_str()),
        }
    }

    /// The binding slot this entry was given when it entered scope.
    pub(super) fn slot(&self) -> SlotId {
        match self {
            ScopeEntry::Base { slot, .. } | ScopeEntry::Derived { slot, .. } => *slot,
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

/// A table of an enclosing scope, snapshotted for correlated lookups.
#[derive(Debug, Clone)]
pub(super) struct OuterTable {
    pub(super) table: Arc<TableMetadata>,
    pub(super) alias: Option<EcoString>,
    pub(super) slot: SlotId,
}

/// One output column of a SELECT list: its name when it has one (an alias,
/// or the column it is), and the catalog column it carries through, which
/// a derived table's synthetic column takes its type from.
#[derive(Debug, Clone)]
pub(super) struct OutputColumn {
    pub(super) name: Option<EcoString>,
    pub(super) source: Option<ColumnBinding>,
}

/// Resolution scope tracking available tables and their aliases.
#[derive(Debug)]
pub(super) struct ResolutionScope<'a> {
    /// Base and derived tables visible in this scope, in FROM order.
    pub(super) entries: Vec<ScopeEntry<'a>>,
    /// Catalog of all known tables (for subquery resolution)
    pub(super) catalog_tables: &'a Catalog,
    /// Search path for schema resolution
    pub(super) search_path: Vec<&'a str>,
    /// Owned snapshot of ancestor scope tables for correlated reference
    /// fallback. Populated when this scope was created for a WHERE/SELECT
    /// subquery body. Empty at top level.
    pub(super) outer_tables: Vec<OuterTable>,
    /// `USING`/`NATURAL` merged join columns, in `*`-expansion order.
    pub(super) merged_columns: Vec<MergedJoinColumn>,
    /// `(scope-key, column)` pairs consumed into a merge — skipped by
    /// unqualified `*` (the single merged column replaces them).
    pub(super) merged_consumed: Vec<(EcoString, EcoString)>,
    /// Bindings of the whole tree, shared with every nested scope.
    pub(super) bindings: &'a mut BindingsBuilder,
}

impl<'a> ResolutionScope<'a> {
    /// Create a scope for resolving an inner subquery body.
    ///
    /// `outer_tables` is an owned snapshot of the ancestor scopes' tables,
    /// used as fallback when column resolution fails in this scope
    /// (correlated references).
    pub(super) fn new_with_outer(
        catalog_tables: &'a Catalog,
        search_path: &[&'a str],
        outer_tables: Vec<OuterTable>,
        bindings: &'a mut BindingsBuilder,
    ) -> Self {
        Self {
            entries: Vec::new(),
            catalog_tables,
            search_path: search_path.to_vec(),
            outer_tables,
            merged_columns: Vec::new(),
            merged_consumed: Vec::new(),
            bindings,
        }
    }

    /// Snapshot the current scope's tables (including derived and outer)
    /// for passing to a child subquery scope. The child needs access to all
    /// ancestor tables.
    pub(super) fn scope_tables_snapshot(&self) -> Vec<OuterTable> {
        let mut snapshot: Vec<OuterTable> = self
            .entries
            .iter()
            .map(|entry| OuterTable {
                table: entry.shared(),
                alias: entry.alias().map(EcoString::from),
                slot: entry.slot(),
            })
            .collect();
        // Include ancestors so nested correlation can reach any level
        snapshot.extend(self.outer_tables.iter().cloned());
        snapshot
    }

    /// Find a table in the outer scope by name or alias.
    pub(super) fn outer_table_scope_find(&self, name: &str) -> Option<&OuterTable> {
        self.outer_tables.iter().find(|outer| match &outer.alias {
            Some(alias) => alias == name,
            None => outer.table.name == name,
        })
    }

    /// Add a table to the scope
    pub(super) fn table_scope_add(
        &mut self,
        table: &'a CatalogTable,
        alias: Option<&'a str>,
        slot: SlotId,
    ) {
        self.entries.push(ScopeEntry::Base { table, alias, slot });
    }

    /// A `USING`/`NATURAL` merged join column by (output) name.
    pub(super) fn merged_column_find(&self, name: &str) -> Option<&MergedJoinColumn> {
        self.merged_columns.iter().find(|m| m.name == name)
    }

    /// Find the scope entry a qualifier names.
    /// Checks both catalog tables and derived tables (FROM subqueries).
    pub(super) fn table_scope_find(&self, name: &str) -> Option<&ScopeEntry<'a>> {
        // Alias-EXCLUSIVE matching, unlike `qualifier_matches`: an aliased
        // table is not referencable by its underlying name (and the VALUES
        // transform relies on this — it aliases a replacement subquery with
        // the original table name, which must not resolve to the base
        // entry's still-aliased occurrence).
        self.entries.iter().find(|entry| match entry {
            ScopeEntry::Base { table, alias, .. } => {
                alias.map_or(table.name == name, |a| a == name)
            }
            ScopeEntry::Derived { alias, .. } => alias == name,
        })
    }

    /// Add a derived table (FROM subquery or CTE reference at `node`) to
    /// the scope, with synthetic `TableMetadata` made from the inner
    /// query's output columns so the outer query can bind references
    /// against the alias.
    pub(super) fn derived_table_scope_add(
        &mut self,
        outputs: &[OutputColumn],
        alias: &str,
        node: NodeId,
    ) -> ResolveResult<()> {
        let columns = derived_table_columns(outputs, self.bindings).ok_or_else(|| {
            Report::from(ResolveError::TooManyColumns {
                alias: alias.to_owned(),
            })
        })?;

        let table = Arc::new(TableMetadata {
            replica_identity_full: false,
            relation_oid: Oid::from_raw(0),
            name: alias.into(),
            schema: "".into(),
            primary_key_columns: Vec::new(),
            columns: ColumnStore::new(columns),
            indexes: Vec::new(),
        });
        let slot = self.bindings.slot_push(
            node,
            TableSlot::Derived {
                node,
                table: Arc::clone(&table),
            },
        );

        self.entries.push(ScopeEntry::Derived {
            table,
            alias: alias.into(),
            slot,
        });
        Ok(())
    }

    /// Find all tables in scope that contain a given column (for
    /// unqualified column resolution).
    pub(super) fn column_matches_find<'b>(
        &'b self,
        column: &str,
    ) -> Vec<(&'b ScopeEntry<'a>, &'b ColumnMetadata)> {
        let mut matches = Vec::new();

        for entry in &self.entries {
            if let Some(col_meta) = entry.metadata().columns.get(column) {
                matches.push((entry, col_meta));
            }
        }

        matches
    }

    /// Bind an inner subquery. Used for WHERE-clause and SELECT-list
    /// subqueries, where correlated references are allowed: a column
    /// found only in `outer_tables` binds to the outer slot and is
    /// recorded as correlated.
    /// Resolve a subquery body in a scope that sees this one's tables and
    /// its ancestors' as outer tables.
    pub(super) fn subquery_resolve_tree(&mut self, query: Query<'a>) -> ResolveResult<()> {
        self.subquery_resolve_with(|tables, search_path, outer_tables, bindings| {
            query_resolve_scoped(query, tables, search_path, outer_tables, bindings)
        })
    }

    /// Run `resolve` for a subquery body in a scope that sees this one's
    /// tables and its ancestors' as outer tables.
    fn subquery_resolve_with<F>(&mut self, resolve: F) -> ResolveResult<()>
    where
        F: for<'b> FnOnce(
            &'b Catalog,
            &'b [&'b str],
            Vec<OuterTable>,
            &'b mut BindingsBuilder,
        ) -> ResolveResult<Vec<OutputColumn>>,
    {
        let outer_tables = self.scope_tables_snapshot();
        resolve(
            self.catalog_tables,
            &self.search_path,
            outer_tables,
            &mut *self.bindings,
        )?;
        Ok(())
    }
}

/// The columns of a derived table from its query's output columns: one per
/// named output, at its position in the list, typed as the catalog column
/// it carries through or as TEXT otherwise. `None` when a position is
/// beyond `i16::MAX`.
pub(super) fn derived_table_columns(
    outputs: &[OutputColumn],
    bindings: &BindingsBuilder,
) -> Option<Vec<ColumnMetadata>> {
    let mut columns = Vec::new();
    for (index, output) in outputs.iter().enumerate() {
        // Functions, literals, etc. without an alias have no stable output
        // name — skip them.
        let Some(name) = &output.name else {
            continue;
        };
        let position = ColumnPosition::from_index(index)?;
        let base = output
            .source
            .and_then(|binding| bindings.binding_column(&binding))
            .cloned();
        columns.push(match base {
            Some(base) => ColumnMetadata {
                name: name.clone(),
                position,
                ..base
            },
            None => ColumnMetadata {
                name: name.clone(),
                position,
                type_oid: TypeOid::from_raw(25), // TEXT OID
                data_type: Type::TEXT,
                type_name: EcoString::from("text"),
                cache_type_name: EcoString::from("text"),
                is_primary_key: false,
            },
        });
    }
    Some(columns)
}
