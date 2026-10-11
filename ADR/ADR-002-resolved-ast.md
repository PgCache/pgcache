# ADR-002: Resolved AST for Query Analysis

**Status**: Superseded by [ADR-057](ADR-057-query-tree-with-side-table-bindings.md): the resolved hierarchy is gone; resolution is a side table of bindings on the one plain tree.
**Date**: 2025-10-10

## Context

Query analysis in pgcache requires understanding table and column references to determine cacheability and generate update queries. The original AST from pg_query contains unresolved references (table names, column names) that require metadata lookups at multiple points in the codebase.

## Decision

Implement a **Resolved AST** system that:

1. Takes the parsed AST from pg_query
2. Resolves all table and column references using catalog metadata
3. Produces a fully-qualified AST with complete type information
4. Enables more sophisticated query analysis and optimization

### Architecture

**Core Types** (in `src/query/resolved.rs`):

- `ResolvedTableNode`: Table reference with schema, name, alias, and relation OID
- `ResolvedColumnNode`: Column reference with schema, table, column name, and full `ColumnMetadata`
- `ResolvedWhereExpr`: WHERE expression tree with fully qualified column references
- `ResolvedSelectStatement`: Complete SELECT statement with all references resolved

**Resolution Process**:

1. **Table Resolution**: Looks up tables in `BiHashMap<TableMetadata>`, resolves schemas, validates existence
2. **Scope Building**: Tracks available tables and aliases as query is processed
3. **Column Resolution**: Resolves column references (qualified or unqualified) against scope
4. **JOIN Resolution**: Recursively resolves JOIN trees, adding tables to scope
5. **SELECT Column Resolution**: Resolves column list, expanding `SELECT *` to all columns

**Key Features**:

- Qualified name resolution (`users.id` → `public.users.id` with full metadata)
- Alias handling (`u.id` where `users u` → `public.users.id`)
- Unqualified column resolution with ambiguity detection
- SELECT * expansion to explicit column list
- Type information attached to all column references

### Error Handling

Custom error types in `ResolveError`:
- `TableNotFound`: Table doesn't exist in catalog
- `ColumnNotFound`: Column doesn't exist in specified table
- `AmbiguousColumn`: Unqualified column exists in multiple tables
- `SchemaNotFound`: Schema mismatch
- `InvalidTableRef`: Unsupported table source (e.g., subqueries)

## Consequences

### Positive

1. **Single Resolution Point**: Table/column lookups happen once during resolution instead of scattered throughout codebase
2. **Type Safety**: Column metadata (type, position, primary key status) is attached to references
3. **Better Analysis**: Fully qualified references enable more sophisticated cacheability analysis
4. **Clearer Code**: Query analysis code works with resolved references instead of string lookups
5. **Future Optimization**: Resolved AST enables constant folding, predicate pushdown, and other optimizations

### Negative

1. **Additional Processing**: Resolution step adds overhead (mitigated by only resolving cacheable queries)
2. **Memory Usage**: Resolved AST is larger than original AST (includes full metadata)
3. **Integration Complexity**: Cache system needs refactoring to use resolved AST

## References

- Implementation: `src/query/resolved.rs`
- Original AST: `src/query/ast.rs`
- Catalog types: `src/catalog.rs`
- Planning document: `pgcache/ADR/ResolvedASTPlan.md`
