# ADR-003: Constant Propagation for Query Constraint Analysis

**Status**: Superseded by [ADR-059](ADR-059-per-reference-predicate-model.md)
**Date**: 2025-10-11

## Context

Cache invalidation decisions in pgcache need to understand which rows can possibly match a cached query. Consider this example:

```sql
SELECT * FROM test t
JOIN test_map tm ON tm.test_id = t.id
WHERE t.id = 1;
```

From the WHERE clause, we know `t.id = 1`. From the JOIN condition `tm.test_id = t.id`, we can infer that `tm.test_id` must also equal 1. This propagated constraint enables more efficient cache invalidation:

1. Only check rows where `test_map.test_id = 1` when invalidating
2. Skip entire tables if constraints don't match
3. Generate more precise update queries

Without constraint propagation, the cache system would need to evaluate the full JOIN condition for every row change, which is expensive and may require fetching additional data.

## Decision

Implement a **constant propagation analysis** that:

1. Extracts equality constraints from WHERE clauses (`column = literal`)
2. Extracts column equivalences from JOIN conditions and WHERE clauses (`column = column`)
3. Propagates constant values through equivalences using fixpoint iteration
4. Produces a `QueryConstraints` structure containing all derived constraints

### Architecture

**Core Types**:

```rust
/// A constraint that a column equals a constant value
struct ColumnConstraint {
    column: ResolvedColumnNode,
    value: LiteralValue,
}

/// An equivalence between two columns
struct ColumnEquivalence {
    left: ResolvedColumnNode,
    right: ResolvedColumnNode,
}

/// Analysis results for a query
pub struct QueryConstraints {
    /// All column constraints (from WHERE + propagated through JOINs)
    pub column_constraints: HashMap<ResolvedColumnNode, LiteralValue>,

    /// Column equivalences from JOIN conditions and WHERE clause
    pub equivalences: HashSet<ColumnEquivalence>,

    /// Constraints organized by table for quick lookup
    pub table_constraints: HashMap<String, Vec<(String, LiteralValue)>>,
}
```

**Analysis Pipeline**:

1. **Unified Expression Analysis**: Both WHERE clauses and JOIN conditions are `ResolvedWhereExpr`, so a single `analyze_equality_expr()` function extracts both constraints and equivalences from any expression tree.

2. **Query-Wide Collection**: Traverse the entire resolved query (WHERE clause and all JOIN conditions) to collect all equality information.

3. **Fixpoint Propagation**: Iteratively propagate constraints through equivalences until no new constraints are discovered. For example:
   - Given: `a = 1` and `a ≡ b` and `b ≡ c`
   - Iteration 1: `a = 1` → `b = 1`
   - Iteration 2: `b = 1` → `c = 1`
   - Result: All three columns constrained to 1

4. **Table Organization**: Group constraints by table name for efficient lookup during CDC processing.

### Key Design Decisions

**Why Resolved AST?** Constant propagation requires fully qualified column references. The Resolved AST (ADR-002) provides `ResolvedColumnNode` with schema, table, and column information, enabling precise constraint tracking across JOINs.

**Why Fixpoint Iteration?** Transitive equivalences require multiple passes. A fixpoint algorithm naturally handles arbitrary equivalence chains without requiring explicit transitive closure computation.

**Why HashSet for Equivalences?** JOIN conditions may appear in both the JOIN ON clause and the WHERE clause (e.g., for query optimization). HashSet automatically deduplicates these redundant equivalences.

**Scope Limitations**:
- Only handles equality operators (`=`)
- Does not propagate through OR conditions (multiple possible values)
- Does not handle range constraints (`<`, `>`, `BETWEEN`)
- Does not handle IN clauses or set membership
- Conflict detection (e.g., `a = 1 AND a = 2`) is not implemented initially

These limitations are acceptable because:
1. Equality constraints are the most common pattern in OLTP queries
2. The cache system degrades gracefully - uncaptured constraints result in conservative invalidation
3. Future enhancements can add support for additional patterns incrementally

## Consequences

### Positive

1. **Precise Cache Invalidation**: Skip checking rows that cannot possibly match query constraints
2. **Reduced CDC Processing**: Filter irrelevant change events before expensive query evaluation
3. **Optimized Update Queries**: Generate simpler SQL for cache updates by directly using propagated constraints
4. **Natural Integration**: Works seamlessly with Resolved AST (ADR-002)
5. **Extensible Design**: Fixpoint algorithm generalizes to future constraint types (ranges, IN clauses)

### Negative

1. **Analysis Overhead**: Every cacheable query requires constraint propagation analysis (mitigated by caching analysis results with query)
2. **Memory Usage**: QueryConstraints structure persists for lifetime of cached query entry
3. **Implementation Complexity**: Fixpoint iteration and transitive reasoning adds code complexity
4. **Limited Scope**: Initial implementation only handles equality, leaving optimization opportunities

### Trade-offs

**Completeness vs Performance**: Could implement full transitive closure computation, but fixpoint iteration is simpler and sufficient for typical query patterns.

**Precision vs Coverage**: Focusing on equality constraints captures the common case while keeping implementation manageable. Range constraints and other patterns can be added if profiling shows benefit.

## Future Enhancements

1. **Conflict Detection**: Detect impossible queries like `a = 1 AND a = 2`
2. **Range Constraints**: Propagate inequalities (`a < 5` + `b = a` → `b < 5`)
3. **Set Membership**: Handle IN clauses (`a IN (1, 2)` + `b = a` → `b IN (1, 2)`)
4. **Expression Simplification**: Solve simple equations (`a + 1 = 2` → `a = 1`)
5. **NULL Handling**: Track `IS NULL` / `IS NOT NULL` constraints
