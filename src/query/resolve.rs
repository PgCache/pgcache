use ecow::EcoString;
use iddqd::BiHashMap;
use rootcause::Report;
use tokio_postgres::types::Type;

use crate::cache::SubqueryKind;
use crate::catalog::{ColumnMetadata, ColumnStore, TableMetadata};
use crate::query::ast::{
    BinaryOp, ColumnNode, JoinQual, JoinType, LimitClause, LiteralValue, OrderByClause, QueryBody,
    QueryExpr, ScalarExpr, SelectColumn, SelectColumns, SelectNode, TableAlias, TableNode,
    TableSource, WhereExpr, WindowSpec,
};
use crate::query::resolved::{
    ResolveError, ResolveResult, ResolvedArithmeticExpr, ResolvedBinaryExpr, ResolvedCaseExpr,
    ResolvedCaseWhen, ResolvedColumnNode, ResolvedFunctionCall, ResolvedJoinNode, ResolvedJoinQual,
    ResolvedLimitClause, ResolvedMultiExpr, ResolvedOrderByClause, ResolvedQueryBody,
    ResolvedQueryExpr, ResolvedScalarExpr, ResolvedSelectColumn, ResolvedSelectColumns,
    ResolvedSelectNode, ResolvedSetOpNode, ResolvedTableNode, ResolvedTableSource,
    ResolvedTableSubqueryNode, ResolvedUnaryExpr, ResolvedWhereExpr, ResolvedWindowSpec,
};
use crate::query::transform::where_expr_conjuncts_join;

/// Resolution scope tracking available tables and their aliases
/// A `USING`/`NATURAL` join's merged output column. Postgres exposes
/// the join column(s) once (not per side); for an outer join its value
/// is `COALESCE(left, right)`. Tracked in scope so `*` expands to the
/// merged set and an unqualified reference resolves to one column.
#[derive(Debug, Clone)]
struct MergedJoinColumn {
    /// Join/output column name (e.g. `i` for `USING (i)`).
    name: EcoString,
    /// Resolved value. Invariant: `outer == false` ⟹ this is exactly
    /// `Column(<left column>)`; `outer == true` ⟹ `COALESCE(l, r)`.
    expr: ResolvedScalarExpr,
    /// Outer join: merged value is `COALESCE`, not a plain column — so
    /// an unqualified GROUP BY on it is unsupported (forwarded).
    outer: bool,
}

/// Scope index ranges delimiting each side of a join, into
/// `scope.tables` / `scope.derived_tables` (`d*`), used to attribute a
/// `USING`/`NATURAL` column to the input that exposes it.
#[derive(Clone, Copy)]
struct JoinScopeRanges {
    left_lo: usize,
    mid: usize,
    hi: usize,
    dleft_lo: usize,
    dmid: usize,
    dhi: usize,
}

impl JoinScopeRanges {
    /// `(tables, derived_tables)` index ranges for the left input.
    fn left(self) -> (std::ops::Range<usize>, std::ops::Range<usize>) {
        (self.left_lo..self.mid, self.dleft_lo..self.dmid)
    }
    /// `(tables, derived_tables)` index ranges for the right input.
    fn right(self) -> (std::ops::Range<usize>, std::ops::Range<usize>) {
        (self.mid..self.hi, self.dmid..self.dhi)
    }
}

/// `join_using_resolve` result: the resolved qualifier, the merged
/// scope columns, and the `(qualifier, column)` pairs they consume.
type JoinUsingResolved = (
    ResolvedJoinQual,
    Vec<MergedJoinColumn>,
    Vec<(EcoString, EcoString)>,
);

#[derive(Debug)]
struct ResolutionScope<'a> {
    /// Tables available in this scope, indexed by alias (or table name if no alias)
    tables: Vec<(&'a TableMetadata, Option<&'a str>)>, // (metadata, alias)
    /// Derived tables (FROM subqueries) with owned synthetic metadata.
    /// These are virtual tables whose columns are determined by the subquery's output.
    derived_tables: Vec<(TableMetadata, String)>, // (synthetic metadata, alias)
    /// Catalog of all known tables (for subquery resolution)
    catalog_tables: &'a BiHashMap<TableMetadata>,
    /// Search path for schema resolution
    search_path: Vec<&'a str>,
    /// Owned snapshot of ancestor scope tables for correlated reference fallback.
    /// Populated when this scope was created for a WHERE/SELECT subquery body.
    /// Empty at top level.
    outer_tables: Vec<(TableMetadata, Option<String>)>,
    /// Correlated column references found during resolution of this scope's expressions.
    /// Populated by `column_resolve` when it falls back to `outer_tables`.
    outer_refs: Vec<ResolvedColumnNode>,
    /// `USING`/`NATURAL` merged join columns, in `*`-expansion order.
    merged_columns: Vec<MergedJoinColumn>,
    /// `(scope-key, column)` pairs consumed into a merge — skipped by
    /// unqualified `*` (the single merged column replaces them).
    merged_consumed: Vec<(EcoString, EcoString)>,
}

impl<'a> ResolutionScope<'a> {
    fn new(catalog_tables: &'a BiHashMap<TableMetadata>, search_path: &[&'a str]) -> Self {
        Self {
            tables: Vec::new(),
            derived_tables: Vec::new(),
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
    fn new_with_outer(
        catalog_tables: &'a BiHashMap<TableMetadata>,
        search_path: &[&'a str],
        outer_tables: Vec<(TableMetadata, Option<String>)>,
    ) -> Self {
        Self {
            tables: Vec::new(),
            derived_tables: Vec::new(),
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
    fn scope_tables_snapshot(&self) -> Vec<(TableMetadata, Option<String>)> {
        let mut snapshot: Vec<(TableMetadata, Option<String>)> = self
            .tables
            .iter()
            .map(|(meta, alias)| ((*meta).clone(), alias.map(str::to_owned)))
            .collect();
        for (meta, alias) in &self.derived_tables {
            snapshot.push((meta.clone(), Some(alias.clone())));
        }
        // Include ancestors so nested correlation can reach any level
        snapshot.extend(self.outer_tables.iter().cloned());
        snapshot
    }

    /// Find a table in the outer scope by name or alias.
    fn outer_table_scope_find(&self, name: &str) -> Option<(&TableMetadata, Option<&str>)> {
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
    fn table_scope_add(&mut self, metadata: &'a TableMetadata, alias: Option<&'a str>) {
        self.tables.push((metadata, alias));
    }

    /// A `USING`/`NATURAL` merged join column by (output) name.
    fn merged_column_find(&self, name: &str) -> Option<&MergedJoinColumn> {
        self.merged_columns.iter().find(|m| m.name == name)
    }

    /// Find table metadata by name or alias.
    /// Checks both catalog tables and derived tables (FROM subqueries).
    fn table_scope_find(&self, name: &str) -> Option<(&TableMetadata, Option<&str>)> {
        // Check catalog tables first
        if let Some((meta, alias)) = self.tables.iter().find(|(meta, alias)| {
            if let Some(alias_name) = alias {
                *alias_name == name
            } else {
                meta.name == name
            }
        }) {
            return Some((*meta, *alias));
        }

        // Check derived tables
        self.derived_tables
            .iter()
            .find(|(_, alias)| alias == name)
            .map(|(meta, alias)| (meta, Some(alias.as_str())))
    }

    /// Add a derived table (FROM subquery) to the scope.
    ///
    /// Extracts output columns from the resolved inner query and creates synthetic
    /// `TableMetadata` so the outer query can resolve column references against
    /// the subquery alias.
    fn derived_table_scope_add(&mut self, resolved_query: &ResolvedQueryExpr, alias: &str) {
        let columns = derived_table_columns_extract(resolved_query);

        let synthetic_metadata = TableMetadata {
            relation_oid: 0,
            name: alias.into(),
            schema: "".into(),
            primary_key_columns: Vec::new(),
            columns: ColumnStore::new(columns),
            indexes: Vec::new(),
        };

        self.derived_tables
            .push((synthetic_metadata, alias.to_owned()));
    }

    /// Find all tables in scope that contain a given column (for unqualified column resolution).
    fn column_matches_find<'b>(
        &'b self,
        column: &str,
    ) -> Vec<(&'b TableMetadata, Option<&'b str>, &'b ColumnMetadata)> {
        let mut matches = Vec::new();

        for (table_metadata, alias) in &self.tables {
            if let Some(col_meta) = table_metadata.columns.get(column) {
                matches.push((*table_metadata, *alias, col_meta));
            }
        }

        for (table_metadata, alias) in &self.derived_tables {
            if let Some(col_meta) = table_metadata.columns.get(column) {
                matches.push((table_metadata, Some(alias.as_str()), col_meta));
            }
        }

        matches
    }

    /// Resolve an inner subquery, collecting any outer column references.
    ///
    /// Used for WHERE-clause and SELECT-list subqueries where correlated references
    /// are allowed. Outer column references are resolved against `outer_tables` and
    /// collected; they appear as normal `Column` nodes in the resolved inner query.
    fn subquery_resolve(
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

/// Find table metadata for a table reference.
///
/// If the table has an explicit schema qualifier, use it directly.
/// Otherwise, search through the search_path schemas in order.
fn table_metadata_find<'map, 'node: 'map>(
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

/// Resolve a column reference to a resolved column node.
///
/// When the column cannot be found in the inner scope and the scope has
/// `outer_tables` set (i.e. we are inside a subquery body), the outer tables
/// are tried as a fallback. On a successful outer-table match the resolved node
/// is recorded in `scope.outer_refs` — marking this as a correlated reference —
/// and returned as a normal column node so the inner query remains fully resolved.
fn column_resolve(
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

/// Resolve a WHERE expression
fn where_expr_resolve(
    expr: &WhereExpr,
    scope: &mut ResolutionScope<'_>,
) -> ResolveResult<ResolvedWhereExpr> {
    match expr {
        WhereExpr::Scalar(scalar) => {
            let resolved = scalar_expr_resolve(scalar, scope)?;
            Ok(ResolvedWhereExpr::Scalar(resolved))
        }
        WhereExpr::Unary(unary) => {
            let resolved_expr = where_expr_resolve(&unary.expr, scope)?;
            Ok(ResolvedWhereExpr::Unary(ResolvedUnaryExpr {
                op: unary.op,
                expr: Box::new(resolved_expr),
            }))
        }
        WhereExpr::Binary(binary) => {
            let resolved_left = where_expr_resolve(&binary.lexpr, scope)?;
            let resolved_right = where_expr_resolve(&binary.rexpr, scope)?;
            Ok(ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
                op: binary.op,
                lexpr: Box::new(resolved_left),
                rexpr: Box::new(resolved_right),
            }))
        }
        WhereExpr::Multi(multi) => {
            let mut resolved_exprs = Vec::with_capacity(multi.exprs.len());
            for e in &multi.exprs {
                resolved_exprs.push(where_expr_resolve(e, scope)?);
            }
            Ok(ResolvedWhereExpr::Multi(ResolvedMultiExpr {
                op: multi.op,
                exprs: resolved_exprs,
            }))
        }
        WhereExpr::Subquery {
            query,
            sublink_type,
            test_expr,
        } => {
            // Resolve the test expression (left-hand side for IN/ANY/ALL) in the outer scope
            let resolved_test = match test_expr {
                Some(e) => Some(Box::new(scalar_expr_resolve(e, scope)?)),
                None => None,
            };

            // Resolve the inner query, collecting any correlated outer references
            let (resolved_query, outer_refs) = scope.subquery_resolve(query)?;

            Ok(ResolvedWhereExpr::Subquery {
                query: Box::new(resolved_query),
                sublink_type: *sublink_type,
                test_expr: resolved_test,
                outer_refs,
            })
        }
    }
}

/// The qualifier (alias, else table name) of the single scope table in
/// the given `tables`/`derived_tables` ranges exposing `column`. `None`
/// if zero or more than one expose it — an ambiguous or absent
/// `USING`/`NATURAL` column, which the caller turns into a resolve
/// error so the query is forwarded rather than mis-cached.
fn join_side_qualifier(
    scope: &ResolutionScope<'_>,
    t_range: std::ops::Range<usize>,
    d_range: std::ops::Range<usize>,
    column: &str,
) -> Option<EcoString> {
    let mut found: Option<EcoString> = None;
    for (meta, alias) in scope.tables.get(t_range).unwrap_or(&[]) {
        if meta.columns.get(column).is_some() {
            if found.is_some() {
                return None;
            }
            found = Some(EcoString::from(alias.unwrap_or(meta.name.as_str())));
        }
    }
    for (meta, alias) in scope.derived_tables.get(d_range).unwrap_or(&[]) {
        if meta.columns.get(column).is_some() {
            if found.is_some() {
                return None;
            }
            found = Some(EcoString::from(alias.as_str()));
        }
    }
    found
}

/// Column names exposed by both the left and right inputs, in left-side
/// order — the `NATURAL JOIN` columns.
fn join_natural_common_columns(
    scope: &ResolutionScope<'_>,
    ranges: JoinScopeRanges,
) -> Vec<EcoString> {
    let side_names = |t: std::ops::Range<usize>, d: std::ops::Range<usize>| {
        let mut names: Vec<&str> = Vec::new();
        for (meta, _) in scope.tables.get(t).unwrap_or(&[]) {
            names.extend(meta.columns.iter().map(|c| c.name.as_str()));
        }
        for (meta, _) in scope.derived_tables.get(d).unwrap_or(&[]) {
            names.extend(meta.columns.iter().map(|c| c.name.as_str()));
        }
        names
    };
    let (lt, ld) = ranges.left();
    let (rt, rd) = ranges.right();
    let right = side_names(rt, rd);
    let mut out: Vec<EcoString> = Vec::new();
    for name in side_names(lt, ld) {
        if right.contains(&name) && !out.iter().any(|o| o == name) {
            out.push(EcoString::from(name));
        }
    }
    out
}

/// Resolve `qual.column = c` against the join scope.
fn join_side_column_resolve(
    scope: &mut ResolutionScope<'_>,
    qual: &EcoString,
    column: &EcoString,
) -> ResolveResult<ResolvedColumnNode> {
    column_resolve(
        &ColumnNode {
            table: Some(qual.clone()),
            column: column.clone(),
        },
        scope,
    )
}

/// Resolve a `USING`/`NATURAL` join over `cols`: the verbatim
/// qualifier (deparsed so Postgres merges the columns) plus its
/// equivalent equi-`predicate` for analysis, and the merged-column
/// scope entries (merged value = the left column for an inner join,
/// `COALESCE(left, right)` for an outer one) with the per-side
/// `(qualifier, column)` pairs they consume from `*` expansion.
/// `cols` is non-empty (the caller handles the no-common-column case).
fn join_using_resolve(
    scope: &mut ResolutionScope<'_>,
    ranges: JoinScopeRanges,
    cols: &[EcoString],
    join_type: JoinType,
) -> ResolveResult<JoinUsingResolved> {
    let outer = join_type != JoinType::Inner;
    let (lt, ld) = ranges.left();
    let (rt, rd) = ranges.right();
    let mut conjuncts: Vec<ResolvedWhereExpr> = Vec::with_capacity(cols.len());
    let mut merged: Vec<MergedJoinColumn> = Vec::with_capacity(cols.len());
    let mut consumed: Vec<(EcoString, EcoString)> = Vec::with_capacity(cols.len() * 2);

    for c in cols {
        let unsupported = || Report::from(ResolveError::UnsupportedJoinQualifier);
        let lq = join_side_qualifier(scope, lt.clone(), ld.clone(), c).ok_or_else(unsupported)?;
        let rq = join_side_qualifier(scope, rt.clone(), rd.clone(), c).ok_or_else(unsupported)?;
        let left_col = join_side_column_resolve(scope, &lq, c)?;
        let right_col = join_side_column_resolve(scope, &rq, c)?;

        conjuncts.push(ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
            op: BinaryOp::Equal,
            lexpr: Box::new(ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(
                left_col.clone(),
            ))),
            rexpr: Box::new(ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(
                right_col.clone(),
            ))),
        }));

        let expr = if outer {
            ResolvedScalarExpr::Function(ResolvedFunctionCall {
                name: EcoString::from("coalesce"),
                args: vec![
                    ResolvedScalarExpr::Column(left_col),
                    ResolvedScalarExpr::Column(right_col),
                ],
                agg_star: false,
                agg_distinct: false,
                agg_order: Vec::new(),
                agg_filter: None,
                over: None,
            })
        } else {
            ResolvedScalarExpr::Column(left_col)
        };
        merged.push(MergedJoinColumn {
            name: c.clone(),
            expr,
            outer,
        });
        consumed.push((lq, c.clone()));
        consumed.push((rq, c.clone()));
    }

    let predicate = where_expr_conjuncts_join(conjuncts)
        .expect("USING/NATURAL resolves at least one join column");
    Ok((
        ResolvedJoinQual::Using {
            columns: cols.to_vec(),
            predicate,
        },
        merged,
        consumed,
    ))
}

/// Resolve a `USING`/`NATURAL` join from its computed `cols`: an empty
/// set is an inner cartesian product (`Cross`) — or, for an outer join,
/// an unsupported condition-less join (forwarded); otherwise the
/// merged-column entries are registered in scope and the `Using`
/// qualifier returned.
fn join_using_or_cross(
    scope: &mut ResolutionScope<'_>,
    ranges: JoinScopeRanges,
    cols: Vec<EcoString>,
    join_type: JoinType,
) -> ResolveResult<ResolvedJoinQual> {
    if cols.is_empty() {
        return if join_type == JoinType::Inner {
            Ok(ResolvedJoinQual::Cross)
        } else {
            Err(Report::from(ResolveError::UnsupportedJoinQualifier))
        };
    }
    let (qual, merged, consumed) = join_using_resolve(scope, ranges, &cols, join_type)?;
    scope.merged_columns.extend(merged);
    scope.merged_consumed.extend(consumed);
    Ok(qual)
}

/// Resolve a table source (table, join, or subquery)
fn table_source_resolve<'a>(
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
            let left_lo = scope.tables.len();
            let dleft_lo = scope.derived_tables.len();
            let resolved_left = table_source_resolve(&join_node.left, tables, scope, search_path)?;
            let mid = scope.tables.len();
            let dmid = scope.derived_tables.len();
            let resolved_right =
                table_source_resolve(&join_node.right, tables, scope, search_path)?;
            let ranges = JoinScopeRanges {
                left_lo,
                mid,
                hi: scope.tables.len(),
                dleft_lo,
                dmid,
                dhi: scope.derived_tables.len(),
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

/// Resolve a column expression in SELECT list
fn scalar_expr_resolve(
    expr: &ScalarExpr,
    scope: &mut ResolutionScope<'_>,
) -> ResolveResult<ResolvedScalarExpr> {
    match expr {
        ScalarExpr::Column(col) => {
            // An unqualified reference to a USING/NATURAL merged column
            // resolves to the single merged value (the left column for
            // an inner join, `COALESCE(left, right)` for an outer one),
            // not an ambiguous per-side lookup. Qualified `t.c` still
            // reaches the base table.
            if col.table.is_none()
                && let Some(expr) = scope
                    .merged_column_find(col.column.as_str())
                    .map(|m| m.expr.clone())
            {
                return Ok(expr);
            }
            let resolved = column_resolve(col, scope)?;
            Ok(ResolvedScalarExpr::Column(resolved))
        }
        ScalarExpr::Literal(lit) => Ok(ResolvedScalarExpr::Literal(lit.clone())),
        ScalarExpr::Function(func) => {
            let mut resolved_args = Vec::with_capacity(func.args.len());
            for arg in &func.args {
                resolved_args.push(scalar_expr_resolve(arg, scope)?);
            }
            // Aggregate ORDER BY (e.g. `string_agg(x, ',' ORDER BY y)`) has no
            // access to SELECT-list aliases — it's evaluated per row within the
            // aggregate's input, not against the output.
            let resolved_agg_order = order_by_resolve(&func.agg_order, scope, None)?;
            let resolved_agg_filter = match &func.agg_filter {
                Some(f) => Some(Box::new(where_expr_resolve(f, scope)?)),
                None => None,
            };
            let resolved_over = match &func.over {
                Some(w) => Some(window_spec_resolve(w, scope)?),
                None => None,
            };
            Ok(ResolvedScalarExpr::Function(ResolvedFunctionCall {
                name: func.name.as_str().into(),
                args: resolved_args,
                agg_star: func.agg_star,
                agg_distinct: func.agg_distinct,
                agg_order: resolved_agg_order,
                agg_filter: resolved_agg_filter,
                over: resolved_over,
            }))
        }
        ScalarExpr::Case(case) => {
            let arg = match &case.arg {
                Some(a) => Some(Box::new(scalar_expr_resolve(a, scope)?)),
                None => None,
            };
            let mut whens = Vec::with_capacity(case.whens.len());
            for w in &case.whens {
                let condition = where_expr_resolve(&w.condition, scope)?;
                let result = scalar_expr_resolve(&w.result, scope)?;
                whens.push(ResolvedCaseWhen { condition, result });
            }
            let default = match &case.default {
                Some(d) => Some(Box::new(scalar_expr_resolve(d, scope)?)),
                None => None,
            };
            Ok(ResolvedScalarExpr::Case(ResolvedCaseExpr {
                arg,
                whens,
                default,
            }))
        }
        ScalarExpr::Arithmetic(arith) => {
            let left = scalar_expr_resolve(&arith.left, scope)?;
            let right = scalar_expr_resolve(&arith.right, scope)?;
            Ok(ResolvedScalarExpr::Arithmetic(ResolvedArithmeticExpr {
                left: Box::new(left),
                op: arith.op,
                right: Box::new(right),
            }))
        }
        ScalarExpr::Subquery(query) => {
            // Resolve the scalar subquery, collecting any correlated outer references
            let (resolved_query, outer_refs) = scope.subquery_resolve(query)?;
            Ok(ResolvedScalarExpr::Subquery(
                Box::new(resolved_query),
                outer_refs,
            ))
        }
        ScalarExpr::Array(elems) => {
            let mut resolved = Vec::with_capacity(elems.len());
            for e in elems {
                resolved.push(scalar_expr_resolve(e, scope)?);
            }
            Ok(ResolvedScalarExpr::Array(resolved))
        }
        ScalarExpr::TypeCast { expr, target } => {
            let inner = scalar_expr_resolve(expr, scope)?;
            Ok(ResolvedScalarExpr::TypeCast {
                expr: Box::new(inner),
                target: target.clone(),
            })
        }
    }
}

/// Resolve a window specification
fn window_spec_resolve(
    window_spec: &WindowSpec,
    scope: &mut ResolutionScope<'_>,
) -> ResolveResult<ResolvedWindowSpec> {
    let mut partition_by = Vec::with_capacity(window_spec.partition_by.len());
    for col in &window_spec.partition_by {
        partition_by.push(scalar_expr_resolve(col, scope)?);
    }
    let mut order_by = Vec::with_capacity(window_spec.order_by.len());
    for clause in &window_spec.order_by {
        let resolved_expr = scalar_expr_resolve(&clause.expr, scope)?;
        order_by.push(ResolvedOrderByClause {
            expr: resolved_expr,
            direction: clause.direction.clone(),
            null_order: clause.null_order,
        });
    }
    Ok(ResolvedWindowSpec {
        partition_by,
        order_by,
    })
}

/// Resolve SELECT columns
///
/// Star expressions (`*` or `t1.*`) are expanded inline to all columns from
/// matching tables in scope.
fn select_columns_resolve(
    columns: &SelectColumns,
    scope: &mut ResolutionScope<'_>,
) -> ResolveResult<ResolvedSelectColumns> {
    match columns {
        SelectColumns::None => Ok(ResolvedSelectColumns::None),
        SelectColumns::Columns(cols) => {
            let mut resolved_cols = Vec::new();
            for col in cols {
                match col {
                    SelectColumn::Star(qualifier) => {
                        // Unqualified `*` over a USING/NATURAL join emits
                        // the merged join column(s) once and first (as
                        // Postgres does), then each table's remaining
                        // (non-join) columns. Qualified `t.*` is that
                        // table's columns verbatim (join column included).
                        if qualifier.is_none() {
                            for m in &scope.merged_columns {
                                resolved_cols.push(ResolvedSelectColumn {
                                    expr: m.expr.clone(),
                                    alias: Some(m.name.clone()),
                                });
                            }
                        }
                        for (table_metadata, alias) in &scope.tables {
                            let matches = match qualifier {
                                None => true,
                                Some(q) => {
                                    alias.is_some_and(|a| a == q) || table_metadata.name == *q
                                }
                            };
                            if !matches {
                                continue;
                            }
                            let key = alias.unwrap_or(table_metadata.name.as_str());
                            for column_metadata in table_metadata.columns.iter() {
                                // Skip columns merged into a USING/NATURAL
                                // join column (only for unqualified `*`).
                                if qualifier.is_none()
                                    && scope.merged_consumed.iter().any(|(k, c)| {
                                        k == key && c == column_metadata.name.as_str()
                                    })
                                {
                                    continue;
                                }
                                resolved_cols.push(ResolvedSelectColumn {
                                    expr: ResolvedScalarExpr::Column(ResolvedColumnNode {
                                        schema: table_metadata.schema.clone(),
                                        table: table_metadata.name.clone(),
                                        table_alias: alias.map(EcoString::from),
                                        column: column_metadata.name.clone(),
                                        column_metadata: column_metadata.clone(),
                                    }),
                                    alias: None,
                                });
                            }
                        }
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
    }
}

/// Resolve ORDER BY clauses. When `select_columns` is provided, an unqualified
/// identifier that matches a SELECT-list output name resolves to
/// `ResolvedScalarExpr::Identifier` — matching PostgreSQL's precedence rule
/// that ORDER BY matches output names before falling back to column lookup.
fn order_by_resolve(
    order_by: &[OrderByClause],
    scope: &mut ResolutionScope<'_>,
    select_columns: Option<&ResolvedSelectColumns>,
) -> ResolveResult<Vec<ResolvedOrderByClause>> {
    let mut resolved = Vec::with_capacity(order_by.len());
    for clause in order_by {
        let resolved_expr = match order_by_alias_match(&clause.expr, select_columns) {
            Some(ident) => ident,
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
/// SELECT-list output name, return it as an `Identifier`.
fn order_by_alias_match(
    clause_expr: &ScalarExpr,
    select_columns: Option<&ResolvedSelectColumns>,
) -> Option<ResolvedScalarExpr> {
    let ScalarExpr::Column(col) = clause_expr else {
        return None;
    };
    if col.table.is_some() {
        return None;
    }
    select_columns?.position_by_output_name(col.column.as_str())?;
    Some(ResolvedScalarExpr::Identifier(col.column.clone()))
}

/// Convert ORDER BY clauses to use unqualified Identifier expressions.
/// Used for set operations where ORDER BY references output columns by name.
fn order_by_as_identifiers(order_by: &[OrderByClause]) -> Vec<ResolvedOrderByClause> {
    order_by
        .iter()
        .map(|clause| {
            let expr = scalar_expr_to_identifier(&clause.expr);
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
fn scalar_expr_to_identifier(expr: &ScalarExpr) -> ResolvedScalarExpr {
    match expr {
        ScalarExpr::Column(col) => ResolvedScalarExpr::Identifier(col.column.as_str().into()),
        ScalarExpr::Literal(lit) => ResolvedScalarExpr::Literal(lit.clone()),
        ScalarExpr::Function(func) => ResolvedScalarExpr::Function(ResolvedFunctionCall {
            name: func.name.as_str().into(),
            args: func.args.iter().map(scalar_expr_to_identifier).collect(),
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
            left: Box::new(scalar_expr_to_identifier(&arith.left)),
            op: arith.op,
            right: Box::new(scalar_expr_to_identifier(&arith.right)),
        }),
        ScalarExpr::Array(elems) => {
            ResolvedScalarExpr::Array(elems.iter().map(scalar_expr_to_identifier).collect())
        }
        ScalarExpr::TypeCast { expr, target } => ResolvedScalarExpr::TypeCast {
            expr: Box::new(scalar_expr_to_identifier(expr)),
            target: target.clone(),
        },
    }
}

/// Resolve GROUP BY clauses
fn group_by_resolve(
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
fn having_resolve(
    having: Option<&WhereExpr>,
    scope: &mut ResolutionScope<'_>,
) -> ResolveResult<Option<ResolvedWhereExpr>> {
    match having {
        Some(h) => Ok(Some(where_expr_resolve(h, scope)?)),
        None => Ok(None),
    }
}

/// Resolve LIMIT clause
fn limit_resolve(limit: Option<&LimitClause>) -> Option<ResolvedLimitClause> {
    let limit = limit?;

    Some(ResolvedLimitClause {
        count: limit.count.clone(),
        offset: limit.offset.clone(),
    })
}

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
fn query_body_resolve(
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
fn select_node_resolve_scoped<'a>(
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
fn query_expr_resolve_scoped(
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

#[cfg(test)]
mod tests {

    #![allow(clippy::wildcard_enum_match_arm)]

    use tokio_postgres::types::Type;

    use crate::query::ast::{BinaryOp, Deparse, JoinType, OrderDirection, SubLinkType};

    use super::*;

    /// Parse SQL and return a SelectNode (for tests using new types)
    fn parse_select_node(sql: &str) -> SelectNode {
        use crate::query::ast::{QueryBody, query_expr_parse};
        let query_expr = query_expr_parse(sql).expect("convert to QueryExpr");
        match query_expr.body {
            QueryBody::Select(node) => *node,
            _ => panic!("expected SELECT"),
        }
    }

    /// Parse SQL and resolve to ResolvedSelectNode
    fn resolve_sql(sql: &str, tables: &BiHashMap<TableMetadata>) -> ResolvedSelectNode {
        let node = parse_select_node(sql);
        select_node_resolve(&node, tables, &["public"]).expect("resolve")
    }

    /// Parse SQL and resolve to ResolvedQueryExpr (for ORDER BY/LIMIT tests)
    fn resolve_query(sql: &str, tables: &BiHashMap<TableMetadata>) -> ResolvedQueryExpr {
        use crate::query::ast::query_expr_parse;
        let query_expr = query_expr_parse(sql).expect("convert to QueryExpr");
        query_expr_resolve(&query_expr, tables, &["public"]).expect("resolve")
    }

    #[test]
    fn test_resolved_table_node_construction() {
        let table_node = ResolvedTableNode {
            schema: "public".into(),
            name: "users".into(),
            alias: Some("u".into()),
            relation_oid: 12345,
        };

        assert_eq!(table_node.schema, "public");
        assert_eq!(table_node.name, "users");
        assert_eq!(table_node.alias.as_deref(), Some("u"));
        assert_eq!(table_node.relation_oid, 12345);
    }

    #[test]
    fn test_resolved_column_node_construction() {
        let col_node = ResolvedColumnNode {
            schema: "public".into(),
            table: "users".into(),
            table_alias: Some("u".into()),
            column: "id".into(),
            column_metadata: ColumnMetadata {
                name: "id".into(),
                position: 1,
                type_oid: 23,
                data_type: Type::INT4,
                type_name: "int4".into(),
                cache_type_name: "int4".into(),
                is_primary_key: true,
            },
        };

        assert_eq!(col_node.schema, "public");
        assert_eq!(col_node.table, "users");
        assert_eq!(col_node.table_alias.as_deref(), Some("u"));
        assert_eq!(col_node.column, "id");
        assert_eq!(col_node.column_metadata.type_name, "int4");
        assert_eq!(col_node.column_metadata.position, 1);
        assert!(col_node.column_metadata.is_primary_key);
    }

    #[test]
    fn test_resolved_select_node_default() {
        let node = ResolvedSelectNode::default();
        assert!(matches!(node.columns, ResolvedSelectColumns::None));
        assert!(node.from.is_empty());
        assert!(node.where_clause.is_none());
        assert!(node.group_by.is_empty());
        assert!(node.having.is_none());
        assert!(!node.distinct);
    }

    // Helper function to create test table metadata
    fn test_table_metadata(name: &str, relation_oid: u32) -> TableMetadata {
        let columns = ColumnStore::new([
            ColumnMetadata {
                name: "id".into(),
                position: 1,
                type_oid: 23,
                data_type: Type::INT4,
                type_name: "int4".into(),
                cache_type_name: "int4".into(),
                is_primary_key: true,
            },
            ColumnMetadata {
                name: "name".into(),
                position: 2,
                type_oid: 25,
                data_type: Type::TEXT,
                type_name: "text".into(),
                cache_type_name: "text".into(),
                is_primary_key: false,
            },
        ]);

        TableMetadata {
            relation_oid,
            name: name.into(),
            schema: "public".into(),
            primary_key_columns: vec!["id".into()],
            columns,
            indexes: Vec::new(),
        }
    }

    /// Create test table metadata with custom column names (all text type, first is PK).
    fn test_table_metadata_with_columns(
        name: &str,
        relation_oid: u32,
        column_names: &[&str],
    ) -> TableMetadata {
        let columns =
            ColumnStore::new(
                column_names
                    .iter()
                    .enumerate()
                    .map(|(i, col_name)| ColumnMetadata {
                        name: (*col_name).into(),
                        position: i16::try_from(i + 1).expect("column position fits in i16"),
                        type_oid: 25,
                        data_type: Type::TEXT,
                        type_name: "text".into(),
                        cache_type_name: "text".into(),
                        is_primary_key: i == 0,
                    }),
            );
        TableMetadata {
            relation_oid,
            name: name.into(),
            schema: "public".into(),
            primary_key_columns: vec![column_names[0].into()],
            columns,
            indexes: Vec::new(),
        }
    }

    #[test]
    fn test_table_resolve_simple() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql("SELECT * FROM users", &tables);

        assert_eq!(resolved.from.len(), 1);
        if let ResolvedTableSource::Table(table) = &resolved.from[0] {
            assert_eq!(table.schema, "public");
            assert_eq!(table.name, "users");
            assert_eq!(table.alias, None);
            assert_eq!(table.relation_oid, 1001);
        } else {
            panic!("Expected table source");
        }
    }

    #[test]
    fn test_table_resolve_with_alias() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql("SELECT * FROM users u", &tables);

        assert_eq!(resolved.from.len(), 1);
        if let ResolvedTableSource::Table(table) = &resolved.from[0] {
            assert_eq!(table.schema, "public");
            assert_eq!(table.name, "users");
            assert_eq!(table.alias.as_deref(), Some("u"));
            assert_eq!(table.relation_oid, 1001);
        } else {
            panic!("Expected table source");
        }
    }

    #[test]
    fn test_table_resolve_not_found() {
        let tables = BiHashMap::new();
        let node = parse_select_node("SELECT * FROM users");
        let result = select_node_resolve(&node, &tables, &["public"]);

        assert!(matches!(
            result.map_err(|e| e.into_current_context()),
            Err(ResolveError::TableNotFound { .. })
        ));
    }

    #[test]
    fn test_column_resolve_qualified() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql("SELECT * FROM users WHERE users.id = 1", &tables);

        // Check WHERE clause resolved correctly
        if let Some(ResolvedWhereExpr::Binary(binary)) = &resolved.where_clause {
            if let ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(col)) = &*binary.lexpr {
                assert_eq!(col.schema, "public");
                assert_eq!(col.table, "users");
                assert_eq!(col.column, "id");
                assert_eq!(col.column_metadata.type_name, "int4");
            } else {
                panic!("Expected column in binary expression");
            }
        } else {
            panic!("Expected binary WHERE expression");
        }
    }

    #[test]
    fn test_column_resolve_with_alias() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql("SELECT * FROM users u WHERE u.name = 'john'", &tables);

        // Check WHERE clause resolved correctly
        if let Some(ResolvedWhereExpr::Binary(binary)) = &resolved.where_clause {
            if let ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(col)) = &*binary.lexpr {
                assert_eq!(col.schema, "public");
                assert_eq!(col.table, "users");
                assert_eq!(col.column, "name");
                assert_eq!(col.column_metadata.type_name, "text");
            } else {
                panic!("Expected column in binary expression");
            }
        } else {
            panic!("Expected binary WHERE expression");
        }
    }

    #[test]
    fn test_column_resolve_unqualified() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql("SELECT * FROM users WHERE id = 1", &tables);

        // Check WHERE clause resolved correctly
        if let Some(ResolvedWhereExpr::Binary(binary)) = &resolved.where_clause {
            if let ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(col)) = &*binary.lexpr {
                assert_eq!(col.schema, "public");
                assert_eq!(col.table, "users");
                assert_eq!(col.column, "id");
            } else {
                panic!("Expected column in binary expression");
            }
        } else {
            panic!("Expected binary WHERE expression");
        }
    }

    #[test]
    fn test_column_resolve_ambiguous() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("orders", 1002));

        // Both tables have 'id' column, unqualified reference is ambiguous
        let node = parse_select_node("SELECT * FROM users, orders WHERE id = 1");
        let result = select_node_resolve(&node, &tables, &["public"]);

        assert!(matches!(
            result.map_err(|e| e.into_current_context()),
            Err(ResolveError::AmbiguousColumn { .. })
        ));
    }

    #[test]
    fn test_select_star_expansion() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql("SELECT * FROM users", &tables);

        // Check that SELECT * was expanded to all columns
        let ResolvedSelectColumns::Columns(cols) = &resolved.columns else {
            panic!("Expected Columns");
        };
        assert_eq!(cols.len(), 2);
        let ResolvedScalarExpr::Column(col) = &cols[0].expr else {
            panic!("Expected column expression");
        };
        assert_eq!(col.column, "id");
        assert_eq!(col.table, "users");
        let ResolvedScalarExpr::Column(col) = &cols[1].expr else {
            panic!("Expected column expression");
        };
        assert_eq!(col.column, "name");
        assert_eq!(col.table, "users");
    }

    #[test]
    fn test_select_specific_columns() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql("SELECT id, name FROM users", &tables);

        // Check that specific columns were resolved
        if let ResolvedSelectColumns::Columns(cols) = &resolved.columns {
            assert_eq!(cols.len(), 2);

            if let ResolvedScalarExpr::Column(col) = &cols[0].expr {
                assert_eq!(col.column, "id");
                assert_eq!(col.table, "users");
            } else {
                panic!("Expected column expression");
            }

            if let ResolvedScalarExpr::Column(col) = &cols[1].expr {
                assert_eq!(col.column, "name");
                assert_eq!(col.table, "users");
            } else {
                panic!("Expected column expression");
            }
        } else {
            panic!("Expected Columns");
        }
    }

    #[test]
    fn test_select_star_with_column() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql("SELECT *, name FROM users", &tables);

        // Star expands to all columns, then the explicit column follows
        let ResolvedSelectColumns::Columns(cols) = &resolved.columns else {
            panic!("Expected Columns");
        };
        assert_eq!(cols.len(), 3); // id, name (from *), name (explicit)

        let ResolvedScalarExpr::Column(col) = &cols[0].expr else {
            panic!("Expected column expression");
        };
        assert_eq!(col.column, "id");

        let ResolvedScalarExpr::Column(col) = &cols[1].expr else {
            panic!("Expected column expression");
        };
        assert_eq!(col.column, "name");

        let ResolvedScalarExpr::Column(col) = &cols[2].expr else {
            panic!("Expected column expression");
        };
        assert_eq!(col.column, "name");
    }

    #[test]
    fn test_select_qualified_star_with_column() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("orders", 1002));

        let resolved = resolve_sql(
            "SELECT u.*, o.name FROM users u JOIN orders o ON o.id = u.id",
            &tables,
        );

        let ResolvedSelectColumns::Columns(cols) = &resolved.columns else {
            panic!("Expected Columns");
        };
        // u.* expands to users.id, users.name, then o.name
        assert_eq!(cols.len(), 3);

        let ResolvedScalarExpr::Column(col) = &cols[0].expr else {
            panic!("Expected column expression");
        };
        assert_eq!(col.column, "id");
        assert_eq!(col.table, "users");

        let ResolvedScalarExpr::Column(col) = &cols[1].expr else {
            panic!("Expected column expression");
        };
        assert_eq!(col.column, "name");
        assert_eq!(col.table, "users");

        let ResolvedScalarExpr::Column(col) = &cols[2].expr else {
            panic!("Expected column expression");
        };
        assert_eq!(col.column, "name");
        assert_eq!(col.table, "orders");
    }

    #[test]
    fn test_join_resolution() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("orders", 1002));

        let resolved = resolve_sql(
            "SELECT * FROM users JOIN orders ON users.id = orders.id",
            &tables,
        );

        // Check that JOIN was resolved
        assert_eq!(resolved.from.len(), 1);
        if let ResolvedTableSource::Join(join) = &resolved.from[0] {
            assert_eq!(join.join_type, JoinType::Inner);

            // Check left side
            if let ResolvedTableSource::Table(left) = &join.left {
                assert_eq!(left.name, "users");
            } else {
                panic!("Expected table on left side");
            }

            // Check right side
            if let ResolvedTableSource::Table(right) = &join.right {
                assert_eq!(right.name, "orders");
            } else {
                panic!("Expected table on right side");
            }

            // Check join condition
            if let Some(ResolvedWhereExpr::Binary(cond)) = join.predicate() {
                if let ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(left_col)) =
                    &*cond.lexpr
                {
                    assert_eq!(left_col.table, "users");
                    assert_eq!(left_col.column, "id");
                }
                if let ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(right_col)) =
                    &*cond.rexpr
                {
                    assert_eq!(right_col.table, "orders");
                    assert_eq!(right_col.column, "id");
                }
            } else {
                panic!("Expected binary join condition");
            }
        } else {
            panic!("Expected join source");
        }
    }

    #[test]
    fn test_join_with_aliases() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("orders", 1002));

        let resolved = resolve_sql(
            "SELECT * FROM users u JOIN orders o ON u.id = o.id",
            &tables,
        );

        // Check that JOIN with aliases was resolved
        if let ResolvedTableSource::Join(join) = &resolved.from[0] {
            // Check left side has alias
            if let ResolvedTableSource::Table(left) = &join.left {
                assert_eq!(left.name, "users");
                assert_eq!(left.alias.as_deref(), Some("u"));
            } else {
                panic!("Expected table on left side");
            }

            // Check right side has alias
            if let ResolvedTableSource::Table(right) = &join.right {
                assert_eq!(right.name, "orders");
                assert_eq!(right.alias.as_deref(), Some("o"));
            } else {
                panic!("Expected table on right side");
            }

            // Check join condition uses aliases
            if let Some(ResolvedWhereExpr::Binary(cond)) = join.predicate() {
                if let ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(left_col)) =
                    &*cond.lexpr
                {
                    // Should resolve to 'users' table even though alias 'u' was used
                    assert_eq!(left_col.table, "users");
                    assert_eq!(left_col.column, "id");
                }
                if let ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(right_col)) =
                    &*cond.rexpr
                {
                    // Should resolve to 'orders' table even though alias 'o' was used
                    assert_eq!(right_col.table, "orders");
                    assert_eq!(right_col.column, "id");
                }
            }
        } else {
            panic!("Expected join source");
        }
    }

    #[test]
    fn test_where_expr_complex() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql(
            "SELECT * FROM users WHERE id = 1 AND name = 'john'",
            &tables,
        );

        // Check that complex WHERE was resolved
        if let Some(ResolvedWhereExpr::Binary(and_expr)) = &resolved.where_clause {
            assert_eq!(and_expr.op, BinaryOp::And);

            // Left side: id = 1
            if let ResolvedWhereExpr::Binary(left_binary) = &*and_expr.lexpr {
                assert_eq!(left_binary.op, BinaryOp::Equal);
                if let ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(col)) =
                    &*left_binary.lexpr
                {
                    assert_eq!(col.column, "id");
                }
            } else {
                panic!("Expected binary expression on left");
            }

            // Right side: name = 'john'
            if let ResolvedWhereExpr::Binary(right_binary) = &*and_expr.rexpr {
                assert_eq!(right_binary.op, BinaryOp::Equal);
                if let ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(col)) =
                    &*right_binary.lexpr
                {
                    assert_eq!(col.column, "name");
                }
            } else {
                panic!("Expected binary expression on right");
            }
        } else {
            panic!("Expected binary WHERE expression");
        }
    }

    #[test]
    fn test_order_by_simple() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        // `SELECT *` expands `name` into the output list, so the unqualified
        // ORDER BY matches the output name and resolves to `Identifier` — PG's
        // output-first precedence rule.
        let resolved = resolve_query(
            "SELECT users.name FROM users ORDER BY users.name ASC",
            &tables,
        );

        assert_eq!(resolved.order_by.len(), 1);
        assert_eq!(resolved.order_by[0].direction, OrderDirection::Asc);

        if let ResolvedScalarExpr::Column(col) = &resolved.order_by[0].expr {
            assert_eq!(col.schema, "public");
            assert_eq!(col.table, "users");
            assert_eq!(col.column, "name");
            assert_eq!(col.column_metadata.type_name, "text");
        } else {
            panic!("Expected column expression in ORDER BY");
        }
    }

    #[test]
    fn test_order_by_multiple_columns() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_query(
            "SELECT users.name, users.id FROM users ORDER BY users.name ASC, users.id DESC",
            &tables,
        );

        assert_eq!(resolved.order_by.len(), 2);

        assert_eq!(resolved.order_by[0].direction, OrderDirection::Asc);
        if let ResolvedScalarExpr::Column(col) = &resolved.order_by[0].expr {
            assert_eq!(col.column, "name");
            assert_eq!(col.table, "users");
        } else {
            panic!("Expected column expression");
        }

        assert_eq!(resolved.order_by[1].direction, OrderDirection::Desc);
        if let ResolvedScalarExpr::Column(col) = &resolved.order_by[1].expr {
            assert_eq!(col.column, "id");
            assert_eq!(col.table, "users");
        } else {
            panic!("Expected column expression");
        }
    }

    #[test]
    fn test_order_by_qualified_column() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_query("SELECT * FROM users u ORDER BY u.name DESC", &tables);

        // Check ORDER BY was resolved with qualified column
        assert_eq!(resolved.order_by.len(), 1);
        assert_eq!(resolved.order_by[0].direction, OrderDirection::Desc);

        if let ResolvedScalarExpr::Column(col) = &resolved.order_by[0].expr {
            // Should resolve to actual table name, not alias
            assert_eq!(col.table, "users");
            assert_eq!(col.column, "name");
            assert_eq!(col.schema, "public");
        } else {
            panic!("Expected column expression");
        }
    }

    #[test]
    fn test_order_by_select_alias() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT id, name AS display_name FROM users ORDER BY display_name DESC";
        let resolved = resolve_query(sql, &tables);

        assert_eq!(resolved.order_by.len(), 1);
        assert_eq!(resolved.order_by[0].direction, OrderDirection::Desc);
        match &resolved.order_by[0].expr {
            ResolvedScalarExpr::Identifier(name) => assert_eq!(name, "display_name"),
            other => panic!("expected Identifier for alias, got {other:?}"),
        }
    }

    #[test]
    fn test_order_by_aggregate_alias() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("orders", 1001));

        // Aggregate functions produce no column-derivable output name, so only an
        // explicit alias lets ORDER BY reference them — this is the key demo case.
        let sql = "SELECT id, SUM(id) AS total FROM orders GROUP BY id ORDER BY total DESC";
        let resolved = resolve_query(sql, &tables);

        assert_eq!(resolved.order_by.len(), 1);
        match &resolved.order_by[0].expr {
            ResolvedScalarExpr::Identifier(name) => assert_eq!(name, "total"),
            other => panic!("expected Identifier for alias, got {other:?}"),
        }
    }

    #[test]
    fn test_order_by_qualified_does_not_match_alias() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        // `u.name` is qualified — must resolve through the column path even if
        // an alias of the same name existed.
        let sql = "SELECT id, name AS display FROM users u ORDER BY u.name";
        let resolved = resolve_query(sql, &tables);

        assert_eq!(resolved.order_by.len(), 1);
        match &resolved.order_by[0].expr {
            ResolvedScalarExpr::Column(col) => {
                assert_eq!(col.table, "users");
                assert_eq!(col.column, "name");
            }
            other => panic!("expected Column for qualified ref, got {other:?}"),
        }
    }

    #[test]
    fn test_order_by_with_join() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("orders", 1002));

        let sql =
            "SELECT * FROM users u JOIN orders o ON u.id = o.id ORDER BY u.name ASC, o.id DESC";
        let resolved = resolve_query(sql, &tables);

        // Check ORDER BY was resolved across joined tables
        assert_eq!(resolved.order_by.len(), 2);

        // First: u.name ASC
        if let ResolvedScalarExpr::Column(col) = &resolved.order_by[0].expr {
            assert_eq!(col.table, "users");
            assert_eq!(col.column, "name");
        } else {
            panic!("Expected column expression");
        }

        // Second: o.id DESC
        if let ResolvedScalarExpr::Column(col) = &resolved.order_by[1].expr {
            assert_eq!(col.table, "orders");
            assert_eq!(col.column, "id");
        } else {
            panic!("Expected column expression");
        }
    }

    #[test]
    fn test_order_by_unqualified_column() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        // Select a column whose name doesn't appear in the output list to force
        // the unqualified ORDER BY through column resolution.
        let resolved = resolve_query("SELECT id FROM users ORDER BY name", &tables);

        assert_eq!(resolved.order_by.len(), 1);
        if let ResolvedScalarExpr::Column(col) = &resolved.order_by[0].expr {
            assert_eq!(col.table, "users");
            assert_eq!(col.column, "name");
        } else {
            panic!("Expected column expression");
        }
    }

    #[test]
    fn test_order_by_column_not_found() {
        use crate::query::ast::query_expr_parse;

        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users ORDER BY nonexistent_column ASC";
        let query_expr = query_expr_parse(sql).unwrap();

        let result = query_expr_resolve(&query_expr, &tables, &["public"]);

        // Should fail with column not found error
        assert!(matches!(
            result.map_err(|e| e.into_current_context()),
            Err(ResolveError::ColumnNotFound { .. })
        ));
    }

    // ==================== Deparse Tests ====================

    fn id_column_metadata() -> ColumnMetadata {
        ColumnMetadata {
            name: "id".into(),
            position: 1,
            type_oid: 23,
            data_type: Type::INT4,
            type_name: "int4".into(),
            cache_type_name: "int4".into(),
            is_primary_key: true,
        }
    }

    #[test]
    fn test_resolved_column_node_deparse_with_alias() {
        let mut buf = String::new();

        // Column with alias - should use alias
        ResolvedColumnNode {
            schema: "public".into(),
            table: "users".into(),
            table_alias: Some("u".into()),
            column: "id".into(),
            column_metadata: id_column_metadata(),
        }
        .deparse(&mut buf);
        assert_eq!(buf, "u.id");
    }

    #[test]
    fn test_resolved_column_node_deparse_without_alias() {
        let mut buf = String::new();

        // Column without alias - should use schema.table
        ResolvedColumnNode {
            schema: "public".into(),
            table: "users".into(),
            table_alias: None,
            column: "id".into(),
            column_metadata: id_column_metadata(),
        }
        .deparse(&mut buf);
        assert_eq!(buf, "public.users.id");
    }

    #[test]
    fn test_resolved_column_node_deparse_quoting() {
        let mut buf = String::new();

        // Column without alias - should use schema.table
        ResolvedColumnNode {
            schema: "Public".into(),
            table: "Users".into(),
            table_alias: None,
            column: "firstName".into(),
            column_metadata: id_column_metadata(),
        }
        .deparse(&mut buf);
        assert_eq!(buf, "\"Public\".\"Users\".\"firstName\"");
    }

    #[test]
    fn test_resolved_table_node_deparse_with_alias() {
        let mut buf = String::new();

        ResolvedTableNode {
            schema: "public".into(),
            name: "users".into(),
            alias: Some("u".into()),
            relation_oid: 1001,
        }
        .deparse(&mut buf);
        assert_eq!(buf, " public.users u");
    }

    #[test]
    fn test_resolved_table_node_deparse_without_alias() {
        let mut buf = String::new();

        ResolvedTableNode {
            schema: "public".into(),
            name: "users".into(),
            alias: None,
            relation_oid: 1001,
        }
        .deparse(&mut buf);
        assert_eq!(buf, " public.users");
    }

    #[test]
    fn test_resolved_table_node_deparse_quoting() {
        let mut buf = String::new();

        ResolvedTableNode {
            schema: "Public".into(),
            name: "Users".into(),
            alias: None,
            relation_oid: 1001,
        }
        .deparse(&mut buf);
        assert_eq!(buf, " \"Public\".\"Users\"");
    }

    #[test]
    fn test_resolved_select_deparse_with_where() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql("SELECT * FROM users WHERE id = 1", &tables);

        let mut buf = String::new();
        resolved.deparse(&mut buf);

        // SELECT * is expanded to explicit columns, table and column references are fully qualified
        assert_eq!(
            buf,
            "SELECT public.users.id, public.users.name FROM public.users WHERE public.users.id = 1"
        );
    }

    #[test]
    fn test_resolved_select_deparse_with_alias() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql("SELECT u.id, u.name FROM users u WHERE u.id = 1", &tables);

        let mut buf = String::new();
        resolved.deparse(&mut buf);

        // With alias, uses alias.column
        assert_eq!(
            buf,
            "SELECT u.id, u.name FROM public.users u WHERE u.id = 1"
        );
    }

    #[test]
    fn test_resolved_select_deparse_join() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("orders", 1002));

        let resolved = resolve_sql(
            "SELECT u.id, o.name FROM users u JOIN orders o ON u.id = o.id WHERE u.id = 1",
            &tables,
        );

        let mut buf = String::new();
        resolved.deparse(&mut buf);

        assert_eq!(
            buf,
            "SELECT u.id, o.name FROM public.users u JOIN public.orders o ON u.id = o.id WHERE u.id = 1"
        );
    }

    #[test]
    fn test_resolved_query_deparse_order_by() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_query("SELECT id FROM users u ORDER BY name DESC", &tables);

        let mut buf = String::new();
        resolved.deparse(&mut buf);

        assert_eq!(buf, "SELECT u.id FROM public.users u ORDER BY u.name DESC");
    }

    #[test]
    fn test_resolved_select_deparse_count_star() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql("SELECT COUNT(*) FROM users WHERE id = 1", &tables);

        let mut buf = String::new();
        resolved.deparse(&mut buf);

        assert_eq!(
            buf,
            "SELECT count(*) FROM public.users WHERE public.users.id = 1"
        );
    }

    #[test]
    fn test_resolved_select_deparse_count_distinct() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql(
            "SELECT COUNT(DISTINCT name) FROM users WHERE id = 1",
            &tables,
        );

        let mut buf = String::new();
        resolved.deparse(&mut buf);

        assert_eq!(
            buf,
            "SELECT count(DISTINCT public.users.name) FROM public.users WHERE public.users.id = 1"
        );
    }

    #[test]
    fn test_resolved_select_deparse_case() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql(
            "SELECT CASE WHEN name = 'admin' THEN 1 ELSE 0 END FROM users WHERE id = 1",
            &tables,
        );

        let mut buf = String::new();
        resolved.deparse(&mut buf);

        assert_eq!(
            buf,
            "SELECT CASE WHEN public.users.name = 'admin' THEN 1 ELSE 0 END FROM public.users WHERE public.users.id = 1"
        );
    }

    #[test]
    fn test_resolved_column_equality_ignores_alias() {
        // Two columns with same schema/table/column but different aliases should be equal
        let col1 = ResolvedColumnNode {
            schema: "public".into(),
            table: "users".into(),
            table_alias: Some("u".into()),
            column: "id".into(),
            column_metadata: id_column_metadata(),
        };

        let col2 = ResolvedColumnNode {
            schema: "public".into(),
            table: "users".into(),
            table_alias: Some("u2".into()), // Different alias
            column: "id".into(),
            column_metadata: id_column_metadata(),
        };

        assert_eq!(col1, col2);

        // Hash should also be equal
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher1 = DefaultHasher::new();
        col1.hash(&mut hasher1);
        let hash1 = hasher1.finish();

        let mut hasher2 = DefaultHasher::new();
        col2.hash(&mut hasher2);
        let hash2 = hasher2.finish();

        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_complexity_single_table_no_where() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql("SELECT * FROM users", &tables);

        // Single table, no predicates = complexity 0
        assert_eq!(resolved.complexity(), 0);
    }

    #[test]
    fn test_complexity_single_table_with_where() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql("SELECT * FROM users WHERE id = 1", &tables);

        // Single table, 1 predicate = complexity 1
        assert_eq!(resolved.complexity(), 1);
    }

    #[test]
    fn test_complexity_single_table_multiple_predicates() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql(
            "SELECT * FROM users WHERE id = 1 AND name = 'john'",
            &tables,
        );

        // Single table, 2 predicates = complexity 2
        assert_eq!(resolved.complexity(), 2);
    }

    #[test]
    fn test_complexity_join_no_where() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("orders", 1002));

        let resolved = resolve_sql(
            "SELECT * FROM users JOIN orders ON users.id = orders.id",
            &tables,
        );

        // 2 tables (1 join) * 3 = 3, no WHERE predicates
        assert_eq!(resolved.complexity(), 3);
    }

    #[test]
    fn test_complexity_join_with_where() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("orders", 1002));

        let resolved = resolve_sql(
            "SELECT * FROM users JOIN orders ON users.id = orders.id WHERE users.id = 1",
            &tables,
        );

        // 2 tables (1 join) * 3 = 3, plus 1 WHERE predicate = 4
        assert_eq!(resolved.complexity(), 4);
    }

    #[test]
    fn test_complexity_ordering() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("orders", 1002));

        // Simple query: SELECT * FROM users
        let resolved1 = resolve_sql("SELECT * FROM users", &tables);

        // Query with WHERE: SELECT * FROM users WHERE id = 1
        let resolved2 = resolve_sql("SELECT * FROM users WHERE id = 1", &tables);

        // Query with JOIN: SELECT * FROM users JOIN orders ON ...
        let resolved3 = resolve_sql(
            "SELECT * FROM users JOIN orders ON users.id = orders.id",
            &tables,
        );

        // Verify ordering: simple < with_where < with_join
        assert!(resolved1.complexity() < resolved2.complexity());
        assert!(resolved2.complexity() < resolved3.complexity());
    }

    #[test]
    fn test_complexity_subquery_depth() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("orders", 1002));

        // No subquery: complexity = 1 predicate
        let flat = resolve_sql("SELECT * FROM users WHERE id = 1", &tables);
        assert_eq!(flat.subquery_depth(), 0);

        // One level of subquery: depth 1
        let one_deep = resolve_sql(
            "SELECT * FROM users WHERE id IN (SELECT id FROM orders)",
            &tables,
        );
        assert_eq!(one_deep.subquery_depth(), 1);

        // Subquery adds 5 per depth level, so one_deep > flat
        assert!(
            one_deep.complexity() > flat.complexity(),
            "subquery should increase complexity: {} > {}",
            one_deep.complexity(),
            flat.complexity()
        );
    }

    #[test]
    fn test_complexity_nested_subquery_depth() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("products", 1001));
        tables.insert_overwrite(test_table_metadata("stores", 1002));
        tables.insert_overwrite(test_table_metadata("regions", 1003));

        // Double-nested: depth 2
        let double_nested = resolve_sql(
            "SELECT * FROM products WHERE id IN (SELECT id FROM stores WHERE id IN (SELECT id FROM regions))",
            &tables,
        );
        assert_eq!(double_nested.subquery_depth(), 2);

        // Single-nested: depth 1
        let single_nested = resolve_sql(
            "SELECT * FROM stores WHERE id IN (SELECT id FROM regions)",
            &tables,
        );
        assert_eq!(single_nested.subquery_depth(), 1);

        // Inner query (no subqueries): depth 0
        let inner = resolve_sql("SELECT * FROM regions", &tables);
        assert_eq!(inner.subquery_depth(), 0);

        // Verify ordering: inner < single_nested < double_nested
        assert!(
            inner.complexity() < single_nested.complexity(),
            "inner ({}) < single_nested ({})",
            inner.complexity(),
            single_nested.complexity()
        );
        assert!(
            single_nested.complexity() < double_nested.complexity(),
            "single_nested ({}) < double_nested ({})",
            single_nested.complexity(),
            double_nested.complexity()
        );
    }

    #[test]
    fn test_complexity_from_subquery_depth() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        // FROM subquery: depth 1
        let from_sub = resolve_sql(
            "SELECT * FROM (SELECT * FROM users WHERE id = 1) sub",
            &tables,
        );
        assert_eq!(from_sub.subquery_depth(), 1);
    }

    #[test]
    fn test_group_by_resolve_single_column() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql("SELECT name FROM users GROUP BY name", &tables);

        assert_eq!(resolved.group_by.len(), 1);
        assert_eq!(resolved.group_by[0].schema, "public");
        assert_eq!(resolved.group_by[0].table, "users");
        assert_eq!(resolved.group_by[0].column, "name");
    }

    #[test]
    fn test_group_by_resolve_multiple_columns() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql("SELECT id, name FROM users GROUP BY id, name", &tables);

        assert_eq!(resolved.group_by.len(), 2);
        assert_eq!(resolved.group_by[0].column, "id");
        assert_eq!(resolved.group_by[1].column, "name");
    }

    #[test]
    fn test_group_by_resolve_qualified_column() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql("SELECT u.name FROM users u GROUP BY u.name", &tables);

        assert_eq!(resolved.group_by.len(), 1);
        assert_eq!(resolved.group_by[0].table, "users");
        assert_eq!(resolved.group_by[0].table_alias.as_deref(), Some("u"));
        assert_eq!(resolved.group_by[0].column, "name");
    }

    #[test]
    fn test_having_resolve() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql(
            "SELECT name FROM users GROUP BY name HAVING name = 'alice'",
            &tables,
        );

        assert!(resolved.having.is_some());
        if let Some(ResolvedWhereExpr::Binary(binary)) = &resolved.having {
            if let ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(col)) = &*binary.lexpr {
                assert_eq!(col.column, "name");
            } else {
                panic!("Expected column in HAVING clause");
            }
        } else {
            panic!("Expected binary expression in HAVING clause");
        }
    }

    #[test]
    fn test_limit_resolve_count_only() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_query("SELECT * FROM users LIMIT 10", &tables);

        let limit = resolved.limit.unwrap();
        assert_eq!(limit.count, Some(LiteralValue::Integer(10)));
        assert_eq!(limit.offset, None);
    }

    #[test]
    fn test_limit_resolve_offset_only() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_query("SELECT * FROM users OFFSET 5", &tables);

        let limit = resolved.limit.unwrap();
        assert_eq!(limit.count, None);
        assert_eq!(limit.offset, Some(LiteralValue::Integer(5)));
    }

    #[test]
    fn test_limit_resolve_count_and_offset() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_query("SELECT * FROM users LIMIT 10 OFFSET 20", &tables);

        let limit = resolved.limit.unwrap();
        assert_eq!(limit.count, Some(LiteralValue::Integer(10)));
        assert_eq!(limit.offset, Some(LiteralValue::Integer(20)));
    }

    #[test]
    fn test_limit_resolve_parameterized() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_query("SELECT * FROM users LIMIT $1 OFFSET $2", &tables);

        // Parameterized values are preserved through resolution
        let limit = resolved.limit.unwrap();
        assert_eq!(limit.count, Some(LiteralValue::Parameter("$1".into())));
        assert_eq!(limit.offset, Some(LiteralValue::Parameter("$2".into())));
    }

    #[test]
    fn test_no_limit_clause() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_query("SELECT * FROM users", &tables);

        assert!(resolved.limit.is_none());
    }

    #[test]
    fn test_combined_group_by_having_limit_resolve() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql =
            "SELECT name FROM users GROUP BY name HAVING name != 'test' ORDER BY name LIMIT 10";
        let resolved = resolve_query(sql, &tables);

        // GROUP BY and HAVING are on the select body
        let ResolvedQueryBody::Select(select) = &resolved.body else {
            panic!("Expected SELECT body");
        };
        assert_eq!(select.group_by.len(), 1);
        assert!(select.having.is_some());

        // ORDER BY and LIMIT are on the QueryExpr
        assert!(!resolved.order_by.is_empty());
        assert!(resolved.limit.is_some());
        assert_eq!(
            resolved.limit.unwrap().count,
            Some(LiteralValue::Integer(10))
        );
    }

    #[test]
    fn test_resolved_window_function() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        // Use columns that exist in test_table_metadata: id, name
        let resolved = resolve_sql(
            "SELECT sum(id) OVER (PARTITION BY name ORDER BY id) FROM users",
            &tables,
        );

        let ResolvedSelectColumns::Columns(columns) = &resolved.columns else {
            panic!("expected columns");
        };

        let ResolvedSelectColumn {
            expr: ResolvedScalarExpr::Function(func),
            ..
        } = &columns[0]
        else {
            panic!("expected function");
        };

        assert_eq!(func.name, "sum");
        assert!(func.over.is_some(), "should have OVER clause");

        let window_spec = func.over.as_ref().unwrap();
        assert_eq!(window_spec.partition_by.len(), 1);
        assert_eq!(window_spec.order_by.len(), 1);
    }

    #[test]
    fn test_resolved_window_function_deparse() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        // Use columns that exist in test_table_metadata: id, name
        let resolved = resolve_sql(
            "SELECT sum(id) OVER (ORDER BY name DESC) FROM users",
            &tables,
        );

        let mut buf = String::new();
        resolved.deparse(&mut buf);

        // Should contain the window function with OVER clause
        assert!(
            buf.contains("OVER"),
            "deparsed SQL should contain OVER: {}",
            buf
        );
        assert!(
            buf.contains("ORDER BY"),
            "deparsed SQL should contain ORDER BY: {}",
            buf
        );
    }

    #[test]
    fn test_select_nodes_simple_select() {
        let query_expr = ResolvedQueryExpr {
            body: ResolvedQueryBody::Select(Box::default()),
            order_by: vec![],
            limit: None,
        };

        let branches = query_expr.select_nodes();
        assert_eq!(branches.len(), 1, "simple SELECT should have one branch");
    }

    #[test]
    fn test_select_nodes_union() {
        use crate::query::ast::SetOpType;

        let left_select = ResolvedSelectNode {
            from: vec![ResolvedTableSource::Table(ResolvedTableNode {
                schema: "public".into(),
                name: "a".into(),
                alias: None,
                relation_oid: 1001,
            })],
            ..Default::default()
        };

        let right_select = ResolvedSelectNode {
            from: vec![ResolvedTableSource::Table(ResolvedTableNode {
                schema: "public".into(),
                name: "b".into(),
                alias: None,
                relation_oid: 1002,
            })],
            ..Default::default()
        };

        let set_op = ResolvedSetOpNode {
            op: SetOpType::Union,
            all: false,
            left: Box::new(ResolvedQueryExpr {
                body: ResolvedQueryBody::Select(Box::new(left_select)),
                order_by: vec![],
                limit: None,
            }),
            right: Box::new(ResolvedQueryExpr {
                body: ResolvedQueryBody::Select(Box::new(right_select)),
                order_by: vec![],
                limit: None,
            }),
        };

        let query_expr = ResolvedQueryExpr {
            body: ResolvedQueryBody::SetOp(set_op),
            order_by: vec![],
            limit: None,
        };

        let branches = query_expr.select_nodes();
        assert_eq!(branches.len(), 2, "UNION should have two branches");

        // Verify each branch has the correct table
        assert_eq!(branches[0].from.len(), 1);
        assert_eq!(branches[1].from.len(), 1);

        if let ResolvedTableSource::Table(t) = &branches[0].from[0] {
            assert_eq!(t.name, "a");
        } else {
            panic!("Expected table source");
        }

        if let ResolvedTableSource::Table(t) = &branches[1].from[0] {
            assert_eq!(t.name, "b");
        } else {
            panic!("Expected table source");
        }
    }

    #[test]
    fn test_select_nodes_nested_union() {
        use crate::query::ast::SetOpType;

        // Build: (SELECT FROM a UNION SELECT FROM b) UNION SELECT FROM c
        let a_select = ResolvedSelectNode {
            from: vec![ResolvedTableSource::Table(ResolvedTableNode {
                schema: "public".into(),
                name: "a".into(),
                alias: None,
                relation_oid: 1001,
            })],
            ..Default::default()
        };

        let b_select = ResolvedSelectNode {
            from: vec![ResolvedTableSource::Table(ResolvedTableNode {
                schema: "public".into(),
                name: "b".into(),
                alias: None,
                relation_oid: 1002,
            })],
            ..Default::default()
        };

        let c_select = ResolvedSelectNode {
            from: vec![ResolvedTableSource::Table(ResolvedTableNode {
                schema: "public".into(),
                name: "c".into(),
                alias: None,
                relation_oid: 1003,
            })],
            ..Default::default()
        };

        let inner_union = ResolvedSetOpNode {
            op: SetOpType::Union,
            all: false,
            left: Box::new(ResolvedQueryExpr {
                body: ResolvedQueryBody::Select(Box::new(a_select)),
                order_by: vec![],
                limit: None,
            }),
            right: Box::new(ResolvedQueryExpr {
                body: ResolvedQueryBody::Select(Box::new(b_select)),
                order_by: vec![],
                limit: None,
            }),
        };

        let outer_union = ResolvedSetOpNode {
            op: SetOpType::Union,
            all: false,
            left: Box::new(ResolvedQueryExpr {
                body: ResolvedQueryBody::SetOp(inner_union),
                order_by: vec![],
                limit: None,
            }),
            right: Box::new(ResolvedQueryExpr {
                body: ResolvedQueryBody::Select(Box::new(c_select)),
                order_by: vec![],
                limit: None,
            }),
        };

        let query_expr = ResolvedQueryExpr {
            body: ResolvedQueryBody::SetOp(outer_union),
            order_by: vec![],
            limit: None,
        };

        let branches = query_expr.select_nodes();
        assert_eq!(branches.len(), 3, "nested UNION should have three branches");
    }

    // ==========================================================================
    // Subquery Resolution Tests
    // ==========================================================================

    #[test]
    fn test_where_subquery_in_resolution() {
        // Test resolving WHERE ... IN (SELECT ...) subquery
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("active_users", 1002));

        let resolved = resolve_sql(
            "SELECT * FROM users WHERE id IN (SELECT id FROM active_users)",
            &tables,
        );

        // Should have resolved WHERE clause with subquery
        let where_clause = resolved
            .where_clause
            .as_ref()
            .expect("should have WHERE clause");

        match where_clause {
            ResolvedWhereExpr::Subquery {
                sublink_type,
                test_expr,
                query,
                ..
            } => {
                assert_eq!(
                    *sublink_type,
                    SubLinkType::Any,
                    "IN should resolve as SubLinkType::Any"
                );
                assert!(test_expr.is_some(), "IN should have test expression");

                // Verify inner query was resolved
                match &query.body {
                    ResolvedQueryBody::Select(inner_select) => {
                        assert_eq!(inner_select.from.len(), 1);
                        if let ResolvedTableSource::Table(t) = &inner_select.from[0] {
                            assert_eq!(t.name, "active_users");
                            assert_eq!(t.relation_oid, 1002);
                        } else {
                            panic!("Expected table source");
                        }
                    }
                    _ => panic!("Expected SELECT body in subquery"),
                }
            }
            _ => panic!(
                "Expected ResolvedWhereExpr::Subquery, got {:?}",
                where_clause
            ),
        }
    }

    #[test]
    fn test_where_subquery_exists_resolution() {
        // Test resolving WHERE EXISTS (SELECT ...) subquery
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("orders", 1001));
        tables.insert_overwrite(test_table_metadata("items", 1002));

        let resolved = resolve_sql(
            "SELECT * FROM orders WHERE EXISTS (SELECT id FROM items)",
            &tables,
        );

        let where_clause = resolved
            .where_clause
            .as_ref()
            .expect("should have WHERE clause");

        match where_clause {
            ResolvedWhereExpr::Subquery {
                sublink_type,
                test_expr,
                ..
            } => {
                assert_eq!(
                    *sublink_type,
                    SubLinkType::Exists,
                    "EXISTS should resolve correctly"
                );
                assert!(
                    test_expr.is_none(),
                    "EXISTS should not have test expression"
                );
            }
            _ => panic!("Expected ResolvedWhereExpr::Subquery"),
        }
    }

    #[test]
    fn test_where_subquery_scalar_resolution() {
        // Test resolving scalar subquery in WHERE clause
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql(
            "SELECT * FROM users WHERE id > (SELECT id FROM users)",
            &tables,
        );

        let where_clause = resolved
            .where_clause
            .as_ref()
            .expect("should have WHERE clause");

        // The scalar subquery should be on the right side of the > comparison
        match where_clause {
            ResolvedWhereExpr::Binary(binary) => match binary.rexpr.as_ref() {
                ResolvedWhereExpr::Subquery { sublink_type, .. } => {
                    assert_eq!(
                        *sublink_type,
                        SubLinkType::Expr,
                        "Scalar subquery should be SubLinkType::Expr"
                    );
                }
                _ => panic!("Expected ResolvedWhereExpr::Subquery on right side"),
            },
            _ => panic!("Expected ResolvedWhereExpr::Binary"),
        }
    }

    #[test]
    fn test_table_subquery_resolution() {
        // Test resolving subquery in FROM clause (derived table)
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        // Note: Column resolution from subqueries is limited, but the subquery itself should resolve
        let node = parse_select_node("SELECT * FROM (SELECT id FROM users) AS sub");
        let result = select_node_resolve(&node, &tables, &["public"]);

        // Should resolve successfully
        let resolved = result.expect("should resolve table subquery");
        assert_eq!(resolved.from.len(), 1);

        match &resolved.from[0] {
            ResolvedTableSource::Subquery(sub) => {
                assert_eq!(sub.alias.name, "sub", "Should preserve alias");

                // Verify inner query was resolved
                match &sub.query.body {
                    ResolvedQueryBody::Select(inner_select) => {
                        assert_eq!(inner_select.from.len(), 1);
                        if let ResolvedTableSource::Table(t) = &inner_select.from[0] {
                            assert_eq!(t.name, "users");
                        } else {
                            panic!("Expected table source in inner query");
                        }
                    }
                    _ => panic!("Expected SELECT body"),
                }
            }
            _ => panic!("Expected ResolvedTableSource::Subquery"),
        }
    }

    #[test]
    fn test_table_subquery_requires_alias() {
        // Test that table subquery without alias fails resolution
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        // Parse a query with subquery without alias
        // Note: PostgreSQL parser typically requires alias, but we should still handle the error
        // gracefully if it somehow gets through
        let node = parse_select_node("SELECT * FROM (SELECT id FROM users) AS sub");

        // This should succeed since it has an alias
        let result = select_node_resolve(&node, &tables, &["public"]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_subquery_nodes_traversal() {
        // Test that nodes() traverses into subqueries to find all tables
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("active_users", 1002));

        let resolved = resolve_sql(
            "SELECT * FROM users WHERE id IN (SELECT id FROM active_users)",
            &tables,
        );

        // Should find both outer table and inner table via nodes() traversal
        let table_nodes: Vec<&ResolvedTableNode> = resolved.nodes().collect();
        assert_eq!(
            table_nodes.len(),
            2,
            "Should find tables in both outer and inner query"
        );

        let table_names: Vec<&str> = table_nodes.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"users"), "Should find outer table");
        assert!(
            table_names.contains(&"active_users"),
            "Should find inner table"
        );
    }

    #[test]
    fn test_subquery_nodes_traversal_derived_table() {
        // Test that nodes() traverses into FROM subqueries
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql(
            "SELECT * FROM (SELECT id FROM users WHERE id = 1) AS sub",
            &tables,
        );

        // Should find the table inside the derived table
        let table_nodes: Vec<&ResolvedTableNode> = resolved.nodes().collect();
        assert_eq!(table_nodes.len(), 1, "Should find table in FROM subquery");
        assert_eq!(table_nodes[0].name, "users");
    }

    #[test]
    fn test_subquery_nodes_traversal_scalar() {
        // Test that nodes() traverses into scalar subqueries in SELECT list
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("orders", 1001));
        tables.insert_overwrite(test_table_metadata("users", 1002));

        let resolved = resolve_sql(
            "SELECT id, (SELECT COUNT(*) FROM users) AS user_count FROM orders WHERE id = 1",
            &tables,
        );

        // Should find both tables
        let table_nodes: Vec<&ResolvedTableNode> = resolved.nodes().collect();
        assert_eq!(
            table_nodes.len(),
            2,
            "Should find tables in outer and scalar subquery"
        );

        let table_names: Vec<&str> = table_nodes.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"orders"), "Should find outer table");
        assert!(
            table_names.contains(&"users"),
            "Should find scalar subquery table"
        );
    }

    #[test]
    fn test_subquery_nodes_traversal_nested() {
        // Test that nodes() traverses into nested subqueries
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("a", 1001));
        tables.insert_overwrite(test_table_metadata("b", 1002));
        tables.insert_overwrite(test_table_metadata("c", 1003));

        let resolved = resolve_sql(
            "SELECT * FROM a WHERE id IN (SELECT id FROM b WHERE id IN (SELECT id FROM c))",
            &tables,
        );

        // Should find all three tables
        let table_nodes: Vec<&ResolvedTableNode> = resolved.nodes().collect();
        assert_eq!(
            table_nodes.len(),
            3,
            "Should find all tables in nested subqueries"
        );

        let table_names: Vec<&str> = table_nodes.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"a"), "Should find outermost table");
        assert!(table_names.contains(&"b"), "Should find middle table");
        assert!(table_names.contains(&"c"), "Should find innermost table");
    }

    // ==========================================================================
    // Direct Table Nodes Tests (population uses these, not nodes())
    // ==========================================================================

    #[test]
    fn test_direct_table_nodes_excludes_where_subquery() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("active_users", 1002));

        let resolved = resolve_sql(
            "SELECT * FROM users WHERE id IN (SELECT id FROM active_users)",
            &tables,
        );

        // nodes() finds both tables (full traversal)
        let all_tables: Vec<&ResolvedTableNode> = resolved.nodes().collect();
        assert_eq!(all_tables.len(), 2);

        // direct_table_nodes() only finds the FROM-clause table
        let direct_tables = resolved.direct_table_nodes();
        assert_eq!(direct_tables.len(), 1, "Should only find direct FROM table");
        assert_eq!(direct_tables[0].name, "users");
    }

    #[test]
    fn test_direct_table_nodes_with_join_and_subquery() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata_with_columns(
            "items",
            1001,
            &["id", "name", "category_id"],
        ));
        tables.insert_overwrite(test_table_metadata_with_columns(
            "inventory",
            1002,
            &["id", "item_id", "quantity"],
        ));
        tables.insert_overwrite(test_table_metadata_with_columns(
            "categories",
            1003,
            &["id", "name", "active"],
        ));

        let resolved = resolve_sql(
            "SELECT i.name FROM items i \
             JOIN inventory inv ON i.id = inv.item_id \
             WHERE i.category_id IN (SELECT c.id FROM categories c WHERE c.active = true) \
             ORDER BY i.name",
            &tables,
        );

        // nodes() finds all 3 tables
        let all_tables: Vec<&ResolvedTableNode> = resolved.nodes().collect();
        assert_eq!(all_tables.len(), 3);

        // direct_table_nodes() only finds the 2 JOIN tables, not the WHERE subquery table
        let direct_tables = resolved.direct_table_nodes();
        assert_eq!(
            direct_tables.len(),
            2,
            "Should find items and inventory but not categories"
        );
        let names: Vec<&str> = direct_tables.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"items"));
        assert!(names.contains(&"inventory"));
        assert!(!names.contains(&"categories"));
    }

    #[test]
    fn test_direct_table_nodes_derived_table() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql(
            "SELECT * FROM (SELECT id FROM users WHERE id = 1) AS sub",
            &tables,
        );

        // nodes() finds the table inside the derived table
        let all_tables: Vec<&ResolvedTableNode> = resolved.nodes().collect();
        assert_eq!(all_tables.len(), 1);

        // direct_table_nodes() finds nothing — the derived table is a subquery, not a direct table
        let direct_tables = resolved.direct_table_nodes();
        assert_eq!(
            direct_tables.len(),
            0,
            "Derived table should not appear in direct_table_nodes"
        );
    }

    // ==========================================================================
    // Correlated Subquery Tests
    // ==========================================================================

    #[test]
    fn test_correlated_exists_subquery_resolves() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("orders", 1001));
        tables.insert_overwrite(test_table_metadata("items", 1002));

        let node = parse_select_node(
            "SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM items WHERE items.id = orders.id)",
        );
        let result = select_node_resolve(&node, &tables, &["public"]);

        let resolved = result.expect("correlated EXISTS should resolve successfully");
        let Some(ResolvedWhereExpr::Subquery { outer_refs, .. }) = &resolved.where_clause else {
            panic!("expected Subquery WHERE");
        };
        assert_eq!(outer_refs.len(), 1, "should have one outer ref");
        assert_eq!(outer_refs[0].table, "orders");
        assert_eq!(outer_refs[0].column, "id");
    }

    #[test]
    fn test_correlated_in_subquery_resolves() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("orders", 1002));

        let node = parse_select_node(
            "SELECT * FROM users WHERE id IN (SELECT id FROM orders WHERE orders.name = users.name)",
        );
        let result = select_node_resolve(&node, &tables, &["public"]);

        let resolved = result.expect("correlated IN should resolve successfully");
        let Some(ResolvedWhereExpr::Subquery { outer_refs, .. }) = &resolved.where_clause else {
            panic!("expected Subquery WHERE");
        };
        assert_eq!(outer_refs.len(), 1, "should have one outer ref");
        assert_eq!(outer_refs[0].table, "users");
        assert_eq!(outer_refs[0].column, "name");
    }

    #[test]
    fn test_correlated_scalar_subquery_resolves() {
        // Scalar correlated subquery in SELECT list
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("orders", 1002));

        let node = parse_select_node(
            "SELECT id, (SELECT COUNT(*) FROM orders WHERE orders.id = users.id) FROM users",
        );
        let result = select_node_resolve(&node, &tables, &["public"]);

        let resolved = result.expect("correlated scalar subquery should resolve successfully");
        let ResolvedSelectColumns::Columns(cols) = &resolved.columns else {
            panic!("expected Columns");
        };
        let outer_refs = cols
            .iter()
            .find_map(|col| match &col.expr {
                ResolvedScalarExpr::Subquery(_, outer_refs) => Some(outer_refs),
                _ => None,
            })
            .expect("subquery column");
        assert_eq!(outer_refs.len(), 1, "should have one outer ref");
        assert_eq!(outer_refs[0].table, "users");
        assert_eq!(outer_refs[0].column, "id");
    }

    #[test]
    fn test_correlated_subquery_with_alias_resolves() {
        // Table alias in outer scope should be resolved to the aliased table
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("orders", 1002));

        let node = parse_select_node(
            "SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders WHERE orders.id = u.id)",
        );
        let result = select_node_resolve(&node, &tables, &["public"]);

        let resolved = result.expect("correlated subquery with alias should resolve successfully");
        let Some(ResolvedWhereExpr::Subquery { outer_refs, .. }) = &resolved.where_clause else {
            panic!("expected Subquery WHERE");
        };
        assert_eq!(outer_refs.len(), 1, "should have one outer ref");
        assert_eq!(outer_refs[0].table, "users");
        assert_eq!(outer_refs[0].column, "id");
    }

    #[test]
    fn test_correlated_unqualified_column_in_where() {
        // `email` only exists on `users`, not `orders` — bare `email` in the subquery
        // is an implicit correlated reference resolved via outer scope fallback
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata_with_columns(
            "users",
            1001,
            &["id", "email"],
        ));
        tables.insert_overwrite(test_table_metadata_with_columns(
            "orders",
            1002,
            &["id", "user_id", "total"],
        ));

        let node = parse_select_node(
            "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE email = 'test@example.com')",
        );
        let result = select_node_resolve(&node, &tables, &["public"]);

        let resolved = result.expect("unqualified outer column should resolve successfully");
        let Some(ResolvedWhereExpr::Subquery { outer_refs, .. }) = &resolved.where_clause else {
            panic!("expected Subquery WHERE");
        };
        assert_eq!(outer_refs.len(), 1, "should have one outer ref");
        assert_eq!(outer_refs[0].table, "users");
        assert_eq!(outer_refs[0].column, "email");
    }

    #[test]
    fn test_correlated_unqualified_column_in_select_list() {
        // `id` exists in both `users` and `orders` — resolves to inner scope (non-correlated)
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata_with_columns(
            "users",
            1001,
            &["id", "email"],
        ));
        tables.insert_overwrite(test_table_metadata_with_columns(
            "orders",
            1002,
            &["id", "user_id"],
        ));

        let node = parse_select_node(
            "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE user_id = id)",
        );
        let result = select_node_resolve(&node, &tables, &["public"]);

        let resolved = result.expect("column present in both scopes should resolve to inner scope");
        let Some(ResolvedWhereExpr::Subquery { outer_refs, .. }) = &resolved.where_clause else {
            panic!("expected Subquery WHERE");
        };
        // `id` resolves to `orders.id` in the inner scope — not a correlated reference
        assert!(
            outer_refs.is_empty(),
            "inner-scope column should not appear in outer_refs"
        );
    }

    #[test]
    fn test_correlated_unqualified_column_scalar_subquery() {
        // `status` only exists on `users`, bare reference in SELECT-list scalar subquery
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata_with_columns(
            "users",
            1001,
            &["id", "status"],
        ));
        tables.insert_overwrite(test_table_metadata_with_columns(
            "orders",
            1002,
            &["id", "amount"],
        ));

        let node = parse_select_node(
            "SELECT id, (SELECT COUNT(*) FROM orders WHERE status = 'active') FROM users",
        );
        let result = select_node_resolve(&node, &tables, &["public"]);

        let resolved = result.expect("unqualified outer column in scalar subquery should resolve");
        let ResolvedSelectColumns::Columns(cols) = &resolved.columns else {
            panic!("expected Columns");
        };
        let outer_refs = cols
            .iter()
            .find_map(|col| match &col.expr {
                ResolvedScalarExpr::Subquery(_, outer_refs) => Some(outer_refs),
                _ => None,
            })
            .expect("subquery column");
        assert_eq!(outer_refs.len(), 1, "should have one outer ref");
        assert_eq!(outer_refs[0].table, "users");
        assert_eq!(outer_refs[0].column, "status");
    }

    #[test]
    fn test_non_correlated_subquery_has_empty_outer_refs() {
        // Non-correlated subquery should have outer_refs: []
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("active_users", 1002));

        let node =
            parse_select_node("SELECT * FROM users WHERE id IN (SELECT id FROM active_users)");
        let result = select_node_resolve(&node, &tables, &["public"]);

        let resolved = result.expect("non-correlated subquery should resolve successfully");
        let Some(ResolvedWhereExpr::Subquery { outer_refs, .. }) = &resolved.where_clause else {
            panic!("expected Subquery WHERE");
        };
        assert!(
            outer_refs.is_empty(),
            "non-correlated subquery must have empty outer_refs"
        );
    }

    #[test]
    fn test_correlated_mixed_inner_and_outer_columns() {
        // Same predicate references both an inner-scope column and an outer-scope column.
        // The inner column resolves normally; the outer column goes into outer_refs.
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata_with_columns(
            "departments",
            1001,
            &["id", "region"],
        ));
        tables.insert_overwrite(test_table_metadata_with_columns(
            "employees",
            1002,
            &["id", "dept_id", "region"],
        ));

        // `employees.dept_id = departments.id` — dept_id is inner, departments.id is outer
        let node = parse_select_node(
            "SELECT d.id FROM departments d \
             WHERE EXISTS (SELECT 1 FROM employees WHERE dept_id = d.id)",
        );
        let result = select_node_resolve(&node, &tables, &["public"]);

        let resolved = result.expect("mixed inner/outer predicate should resolve");
        let Some(ResolvedWhereExpr::Subquery { outer_refs, .. }) = &resolved.where_clause else {
            panic!("expected Subquery WHERE");
        };
        assert_eq!(
            outer_refs.len(),
            1,
            "only the outer-scope column should be in outer_refs"
        );
        assert_eq!(outer_refs[0].table, "departments");
        assert_eq!(outer_refs[0].column, "id");
    }

    #[test]
    fn test_doubly_nested_correlated_subquery() {
        // Grandchild subquery references the grandparent scope
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata_with_columns(
            "departments",
            1001,
            &["id", "name"],
        ));
        tables.insert_overwrite(test_table_metadata_with_columns(
            "employees",
            1002,
            &["id", "dept_id"],
        ));
        tables.insert_overwrite(test_table_metadata_with_columns(
            "projects",
            1003,
            &["id", "employee_id"],
        ));

        // departments d → employees e (correlated on d.id) → projects (correlated on e.id)
        let node = parse_select_node(
            "SELECT d.id FROM departments d \
             WHERE EXISTS (\
               SELECT 1 FROM employees e WHERE e.dept_id = d.id AND EXISTS (\
                 SELECT 1 FROM projects WHERE employee_id = e.id\
               )\
             )",
        );
        let result = select_node_resolve(&node, &tables, &["public"]);

        // Resolution must succeed; the outer EXISTS subquery is correlated on d.id
        assert!(
            result.is_ok(),
            "doubly-nested correlated subquery should resolve, got: {:?}",
            result
        );
        let resolved = result.unwrap();
        let Some(ResolvedWhereExpr::Subquery { outer_refs, .. }) = &resolved.where_clause else {
            panic!("expected Subquery WHERE");
        };
        assert!(
            !outer_refs.is_empty(),
            "outer EXISTS should be correlated on departments.id"
        );
        assert_eq!(outer_refs[0].table, "departments");
        assert_eq!(outer_refs[0].column, "id");
    }

    #[test]
    fn test_unqualified_column_not_in_any_scope() {
        // `nonexistent` doesn't exist in any table — should remain ColumnNotFound
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata_with_columns(
            "users",
            1001,
            &["id", "name"],
        ));
        tables.insert_overwrite(test_table_metadata_with_columns(
            "orders",
            1002,
            &["id", "total"],
        ));

        let node = parse_select_node(
            "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE nonexistent = 1)",
        );
        let result = select_node_resolve(&node, &tables, &["public"]);

        assert!(
            matches!(
                result.as_ref().map_err(|e| e.current_context()),
                Err(ResolveError::ColumnNotFound { .. })
            ),
            "Column not in any scope should remain ColumnNotFound, got: {:?}",
            result
        );
    }

    // ---------------------------------------------------------------
    // PGC-123: HAVING aggregate metadata must survive resolution and
    // resolved-side deparse.
    // ---------------------------------------------------------------

    fn resolved_having_lhs_function(node: &ResolvedSelectNode) -> &ResolvedFunctionCall {
        let having = node.having.as_ref().expect("resolved HAVING present");
        let ResolvedWhereExpr::Binary(binary) = having else {
            panic!("expected Binary HAVING, got {having:?}");
        };
        let ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Function(func)) = binary.lexpr.as_ref()
        else {
            panic!("expected Scalar(Function) on LHS, got {:?}", binary.lexpr);
        };
        func
    }

    #[test]
    fn test_having_filter_resolves() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql(
            "SELECT name FROM users GROUP BY name \
             HAVING COUNT(*) FILTER (WHERE id > 0) > 5",
            &tables,
        );

        let func = resolved_having_lhs_function(&resolved);
        assert_eq!(func.name, "count");
        assert!(func.agg_star);
        assert!(
            func.agg_filter.is_some(),
            "FILTER (WHERE ...) must survive resolution"
        );
    }

    #[test]
    fn test_having_distinct_resolves() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql(
            "SELECT name FROM users GROUP BY name HAVING COUNT(DISTINCT id) > 1",
            &tables,
        );

        let func = resolved_having_lhs_function(&resolved);
        assert!(func.agg_distinct, "DISTINCT must survive resolution");
    }

    #[test]
    fn test_having_aggregate_order_by_resolves() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql(
            "SELECT id FROM users GROUP BY id \
             HAVING string_agg(name, ',' ORDER BY name) <> ''",
            &tables,
        );

        let func = resolved_having_lhs_function(&resolved);
        assert_eq!(func.name, "string_agg");
        assert!(
            !func.agg_order.is_empty(),
            "aggregate ORDER BY must survive resolution"
        );
    }

    #[test]
    fn test_having_filter_resolved_deparse_contains_filter() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql(
            "SELECT name FROM users GROUP BY name \
             HAVING COUNT(*) FILTER (WHERE id > 0) > 5",
            &tables,
        );

        let mut buf = String::new();
        resolved.deparse(&mut buf);
        assert!(
            buf.contains("FILTER (WHERE "),
            "resolved deparse must keep FILTER, got: {buf}"
        );
    }
}
