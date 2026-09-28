//! MV shape classification: whether a resolved query's shape can ever benefit
//! from a materialized result. Registration-time, pure analysis — compiled in
//! both builds, since admission analysis uses it; the MV state machine and
//! serve path live in `mv` (proxy-only).

use std::collections::HashSet;

use ecow::EcoString;

use crate::query::ast::SetOpType;
use crate::query::resolved::{
    ResolvedQueryBody, ResolvedQueryExpr, ResolvedScalarExpr, ResolvedSelectColumns,
    ResolvedSelectNode, ResolvedTableSource,
};

/// Shape classification set at registration from the decorrelated, resolved
/// form of the query. Never changes for the life of the cache entry; eviction
/// + re-registration is the only way to re-classify.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShapeGate {
    /// Shape can benefit from an MV — it does non-trivial recompute (join,
    /// window, aggregate, GROUP BY, HAVING, DISTINCT, dedup set-op). Admission is
    /// decided at first build by two independent tests, materialize if *either*
    /// passes (see `mv_build`): row reduction (`result × ratio ≤ source rows`)
    /// OR compute avoidance (`source rows ≥ compute threshold`).
    Gated,
    /// Shape rules out benefit; never materialize. Single-table plain projection
    /// (the source-row cache already holds exactly these rows), `UNION ALL`
    /// (trivial concat of cached branches), and `VALUES`.
    Skip,
}

impl ShapeGate {
    /// True when the shape transforms row cardinality — the query's result-row
    /// count is not the source-row count (aggregate reduces to groups, window
    /// annotates but preserves rows yet depends on the full partition, etc).
    ///
    /// Used by the source-row caching layer to force `max_limit = None`: a
    /// user LIMIT bounds result rows, so applying it to source-row population
    /// would truncate the input and produce wrong results on re-evaluation
    /// (e.g. `SELECT count(*) FROM t LIMIT 3` cached with 3 source rows
    /// returns 3, not the real count). Plain projection (`Skip`) is safe
    /// because result rows = source rows, so LIMIT translates one-to-one.
    pub fn is_reducer(self) -> bool {
        matches!(self, ShapeGate::Gated)
    }
}

/// True when every top-level ORDER BY expression is viable to serve from the
/// MV. For SELECT bodies, the expression must be structurally present in the
/// SELECT list (positional lookup). For SET OP bodies, each ORDER BY must be
/// an `Identifier` whose name matches an output column of the leftmost SELECT
/// (which is what `CREATE TABLE AS` uses for MV column names).
///
/// Matching is structural (or by output name for `Identifier`, which is what
/// alias-referenced ORDER BY resolves to). Future improvement:
///   - Normalized matching (handles small rewrites like constant folding
///     between SELECT and ORDER BY, if those turn out to happen in practice).
fn order_by_serve_viable(resolved: &ResolvedQueryExpr) -> bool {
    if resolved.order_by.is_empty() {
        return true;
    }
    match &resolved.body {
        ResolvedQueryBody::Select(select) => resolved
            .order_by
            .iter()
            .all(|o| select.columns.columns_position_of(&o.expr).is_some()),
        ResolvedQueryBody::SetOp(_) => {
            let Some(leftmost) = resolved_leftmost_select(resolved) else {
                return false;
            };
            resolved.order_by.iter().all(|o| match &o.expr {
                ResolvedScalarExpr::Identifier(name) => leftmost
                    .columns
                    .position_by_output_name(name.as_str())
                    .is_some(),
                ResolvedScalarExpr::Column(_)
                | ResolvedScalarExpr::Function(_)
                | ResolvedScalarExpr::Literal(_)
                | ResolvedScalarExpr::Case(_)
                | ResolvedScalarExpr::Arithmetic(_)
                | ResolvedScalarExpr::Subquery(_, _)
                | ResolvedScalarExpr::Array(_)
                | ResolvedScalarExpr::TypeCast { .. } => false,
            })
        }
        ResolvedQueryBody::Values(_) => false,
    }
}

/// Walk down the left side of a (possibly nested) set op until we hit a
/// SELECT body. `None` when the leftmost body is `Values` (unusual — not a
/// real MV candidate).
fn resolved_leftmost_select(resolved: &ResolvedQueryExpr) -> Option<&ResolvedSelectNode> {
    match &resolved.body {
        ResolvedQueryBody::Select(s) => Some(s),
        ResolvedQueryBody::SetOp(setop) => resolved_leftmost_select(&setop.left),
        ResolvedQueryBody::Values(_) => None,
    }
}

/// Classify a resolved query's shape to decide whether it's a materialization
/// candidate. Runs at registration on the decorrelated form (see caller in
/// writer/query.rs), so the shape we classify is the shape we'll actually
/// populate and serve against.
///
/// Classification is **top-level only** — we do not descend into scalar
/// subqueries in the SELECT list or subqueries in the FROM/WHERE clauses,
/// since those don't change the outer query's output shape.
///
/// `aggregate_functions` is the set of aggregate function names loaded from
/// `pg_proc` at writer startup (`catalog::aggregate_functions_load`).
pub fn shape_classify(
    resolved: &ResolvedQueryExpr,
    aggregate_functions: &HashSet<EcoString>,
) -> ShapeGate {
    let shape = match &resolved.body {
        // VALUES is already literal — nothing to materialize that we don't
        // already emit inline on every serve.
        ResolvedQueryBody::Values(_) => return ShapeGate::Skip,

        ResolvedQueryBody::SetOp(setop) => {
            // UNION ALL is strictly additive — no dedup, no compute. Branches
            // are already cached; concatenating them at serve time is two seq
            // scans, cheap. MV would duplicate storage without saving anything.
            if setop.op == SetOpType::Union && setop.all {
                return ShapeGate::Skip;
            }
            // UNION (dedup), INTERSECT [ALL], EXCEPT [ALL] do real dedup work and
            // can reduce the result — the build-time gates decide.
            ShapeGate::Gated
        }

        ResolvedQueryBody::Select(select) => {
            // Any non-trivial recompute — window, aggregate/GROUP BY/HAVING/
            // DISTINCT, or a join — can earn an MV. The two build-time gates
            // (row reduction OR compute avoidance) decide whether it actually
            // does; classification only fences off shapes that can never benefit.
            let computes = columns_any(&select.columns, &scalar_expr_has_window)
                || select.distinct
                || !select.group_by.is_empty()
                || select.having.is_some()
                || columns_any(&select.columns, &|e| e.has_aggregate(aggregate_functions))
                || select_has_join(select);
            if computes {
                ShapeGate::Gated
            } else {
                // Single-table plain filter/projection — source-row cache
                // already stores exactly these rows; MV would duplicate it.
                return ShapeGate::Skip;
            }
        }
    };

    // MV-specific viability: every top-level ORDER BY expression must be
    // serveable against the MV. If not, downgrade to Skip — serving without
    // the ORDER BY would give arbitrary rows for user LIMIT < max_limit.
    if !order_by_serve_viable(resolved) {
        return ShapeGate::Skip;
    }

    shape
}

/// Top-level FROM joins two or more base tables (explicit JOIN or comma
/// list). Subqueries in FROM are not descended into.
pub(super) fn select_has_join(select: &ResolvedSelectNode) -> bool {
    select.from.len() > 1
        || select
            .from
            .iter()
            .any(|src| matches!(src, ResolvedTableSource::Join(_)))
}

/// Returns true if any top-level column expression in the SELECT list satisfies
/// `pred`. Does not descend into subqueries.
pub(super) fn columns_any<P>(columns: &ResolvedSelectColumns, pred: &P) -> bool
where
    P: Fn(&ResolvedScalarExpr) -> bool,
{
    match columns {
        ResolvedSelectColumns::None => false,
        ResolvedSelectColumns::Columns(cols) => cols.iter().any(|c| pred(&c.expr)),
    }
}

/// True if the expression contains a window function (any `FuncCall` with an
/// `OVER (...)` clause). Descends through Function args, CASE branches, and
/// Arithmetic operands, but not into scalar subqueries.
pub(super) fn scalar_expr_has_window(expr: &ResolvedScalarExpr) -> bool {
    match expr {
        ResolvedScalarExpr::Function(func) => {
            func.over.is_some() || func.args.iter().any(scalar_expr_has_window)
        }
        ResolvedScalarExpr::Case(case) => {
            case.arg.as_ref().is_some_and(|a| scalar_expr_has_window(a))
                || case.whens.iter().any(|w| scalar_expr_has_window(&w.result))
                || case
                    .default
                    .as_ref()
                    .is_some_and(|d| scalar_expr_has_window(d))
        }
        ResolvedScalarExpr::Arithmetic(a) => {
            scalar_expr_has_window(&a.left) || scalar_expr_has_window(&a.right)
        }
        ResolvedScalarExpr::Array(elems) => elems.iter().any(scalar_expr_has_window),
        ResolvedScalarExpr::TypeCast { expr, .. } => scalar_expr_has_window(expr),
        ResolvedScalarExpr::Column(_)
        | ResolvedScalarExpr::Identifier(_)
        | ResolvedScalarExpr::Literal(_)
        | ResolvedScalarExpr::Subquery(_, _) => false,
    }
}

#[cfg(test)]
pub(super) mod tests {
    use iddqd::BiHashMap;
    use postgres_types::Type;

    use super::*;
    use crate::catalog::{ColumnMetadata, ColumnStore, TableMetadata};
    use crate::oid::Oid;
    use crate::query::ast::query_expr_parse;
    use crate::query::resolved::query_expr_resolve;

    #[test]
    fn is_reducer_matches_non_projection_shapes() {
        assert!(ShapeGate::Gated.is_reducer());
        assert!(!ShapeGate::Skip.is_reducer());
    }

    // ==================== Classifier tests ====================

    fn test_table(name: &str, oid: Oid, cols: &[&str]) -> TableMetadata {
        let columns = ColumnStore::new(cols.iter().enumerate().map(|(i, c)| {
            let is_pk = i == 0;
            ColumnMetadata {
                name: (*c).into(),
                position: i16::try_from(i + 1).expect("column position fits in i16"),
                type_oid: if is_pk { 23 } else { 25 },
                data_type: if is_pk { Type::INT4 } else { Type::TEXT },
                type_name: if is_pk { "int4" } else { "text" }.into(),
                cache_type_name: if is_pk { "int4" } else { "text" }.into(),
                is_primary_key: is_pk,
            }
        }));
        TableMetadata {
            replica_identity_full: false,
            relation_oid: oid,
            name: name.into(),
            schema: "public".into(),
            primary_key_columns: vec![cols[0].into()],
            columns,
            indexes: Vec::new(),
        }
    }

    pub(crate) fn test_tables() -> BiHashMap<TableMetadata> {
        let mut t = BiHashMap::new();
        t.insert_overwrite(test_table(
            "orders",
            Oid::from_raw(1),
            &["id", "status", "total"],
        ));
        t.insert_overwrite(test_table(
            "users",
            Oid::from_raw(2),
            &["id", "name", "email"],
        ));
        t
    }

    fn test_aggregate_functions() -> HashSet<EcoString> {
        [
            "count",
            "sum",
            "avg",
            "min",
            "max",
            "array_agg",
            "string_agg",
        ]
        .into_iter()
        .map(EcoString::from)
        .collect()
    }

    fn classify(sql: &str) -> ShapeGate {
        let ast = query_expr_parse(sql).expect("convert to AST");
        let resolved =
            query_expr_resolve(&ast, &test_tables(), &["public"]).expect("resolve query");
        shape_classify(&resolved, &test_aggregate_functions())
    }

    #[test]
    fn classify_plain_filter_is_skip() {
        assert_eq!(
            classify("SELECT * FROM orders WHERE id = 1"),
            ShapeGate::Skip
        );
    }

    #[test]
    fn classify_projection_is_skip() {
        assert_eq!(classify("SELECT id, status FROM orders"), ShapeGate::Skip);
    }

    #[test]
    fn classify_bare_aggregate_is_gated() {
        assert_eq!(classify("SELECT count(*) FROM orders"), ShapeGate::Gated);
        assert_eq!(classify("SELECT sum(total) FROM orders"), ShapeGate::Gated);
    }

    #[test]
    fn classify_group_by_is_gated() {
        assert_eq!(
            classify("SELECT status, count(*) FROM orders GROUP BY status"),
            ShapeGate::Gated
        );
    }

    #[test]
    fn classify_having_is_gated() {
        // HAVING on a GROUP BY query — row reduction via either signal.
        assert_eq!(
            classify("SELECT status, count(*) FROM orders GROUP BY status HAVING count(*) > 5"),
            ShapeGate::Gated
        );
    }

    #[test]
    fn classify_distinct_is_gated() {
        assert_eq!(
            classify("SELECT DISTINCT status FROM orders"),
            ShapeGate::Gated
        );
    }

    #[test]
    fn classify_window_function_is_gated() {
        assert_eq!(
            classify("SELECT id, row_number() OVER (ORDER BY total) FROM orders"),
            ShapeGate::Gated
        );
    }

    #[test]
    fn classify_window_with_group_by_is_gated() {
        // Even with GROUP BY, a window function plus GROUP BY is still Gated (the window
        // compute-expensive signal; row reduction is the size signal).
        assert_eq!(
            classify(
                "SELECT status, count(*), row_number() OVER (ORDER BY status) \
                 FROM orders GROUP BY status"
            ),
            ShapeGate::Gated
        );
    }

    #[test]
    fn classify_union_dedup_is_gated() {
        assert_eq!(
            classify("SELECT id FROM orders UNION SELECT id FROM users"),
            ShapeGate::Gated
        );
    }

    #[test]
    fn classify_union_all_is_skip() {
        // UNION ALL is strictly additive — no dedup, branches already cached.
        assert_eq!(
            classify("SELECT id FROM orders UNION ALL SELECT id FROM users"),
            ShapeGate::Skip
        );
    }

    #[test]
    fn classify_intersect_is_gated() {
        assert_eq!(
            classify("SELECT id FROM orders INTERSECT SELECT id FROM users"),
            ShapeGate::Gated
        );
        assert_eq!(
            classify("SELECT id FROM orders INTERSECT ALL SELECT id FROM users"),
            ShapeGate::Gated
        );
    }

    #[test]
    fn classify_except_is_gated() {
        assert_eq!(
            classify("SELECT id FROM orders EXCEPT SELECT id FROM users"),
            ShapeGate::Gated
        );
        assert_eq!(
            classify("SELECT id FROM orders EXCEPT ALL SELECT id FROM users"),
            ShapeGate::Gated
        );
    }

    #[test]
    fn classify_setop_order_by_identifier_in_select_is_gated() {
        // `id` appears in the left branch's SELECT list; the set-op's output
        // column is named `id`, so `ORDER BY id` is serveable against the MV.
        assert_eq!(
            classify("SELECT id FROM orders UNION SELECT id FROM users ORDER BY id DESC"),
            ShapeGate::Gated
        );
    }

    #[test]
    fn classify_setop_order_by_unknown_identifier_is_skip() {
        // `status` is NOT in the set-op's output (SELECT list is just `id`),
        // so MV can't preserve the sort — downgrade to Skip.
        assert_eq!(
            classify("SELECT id FROM orders UNION SELECT id FROM users ORDER BY status DESC"),
            ShapeGate::Skip
        );
    }

    #[test]
    fn classify_aggregate_inside_case_is_gated() {
        // Aggregate nested inside CASE branch should still be detected.
        assert_eq!(
            classify(
                "SELECT CASE WHEN status = 'open' THEN count(*) ELSE 0 END \
                 FROM orders GROUP BY status"
            ),
            ShapeGate::Gated
        );
    }

    #[test]
    fn classify_aggregate_in_subquery_does_not_reduce_outer() {
        // A scalar subquery with count() in the SELECT list doesn't make the
        // outer query a reduction shape — it.s a plain projection over orders.
        assert_eq!(
            classify("SELECT id, (SELECT count(*) FROM users) AS user_count FROM orders"),
            ShapeGate::Skip
        );
    }

    #[test]
    fn classify_join_without_aggregate_is_gated() {
        // A plain join's result is the same predicate-scoped rows it scans, so
        // the row-reduction gate can't apply; its MV value is avoiding the
        // re-join, gated on input size (PGC-330).
        assert_eq!(
            classify("SELECT o.id, u.name FROM orders o JOIN users u ON o.id = u.id"),
            ShapeGate::Gated
        );
    }

    #[test]
    fn classify_join_with_aggregate_is_gated() {
        // The aggregate signal wins over the join: this reduces rows.
        assert_eq!(
            classify(
                "SELECT u.id, count(o.id) FROM users u \
                 JOIN orders o ON u.id = o.id GROUP BY u.id"
            ),
            ShapeGate::Gated
        );
    }

    // ==================== ORDER BY interaction ====================

    #[test]
    fn classify_gated_with_order_by_selected_aggregate_is_gated() {
        // ORDER BY count(*) — count(*) is in SELECT, position lookup succeeds.
        assert_eq!(
            classify(
                "SELECT status, count(*) FROM orders GROUP BY status \
                 ORDER BY count(*) DESC"
            ),
            ShapeGate::Gated
        );
    }

    #[test]
    fn classify_gated_with_order_by_selected_group_column_is_gated() {
        assert_eq!(
            classify("SELECT status, count(*) FROM orders GROUP BY status ORDER BY status"),
            ShapeGate::Gated
        );
    }

    #[test]
    fn classify_gated_aggregate_order_by_not_in_select_downgrades_to_skip() {
        // ORDER BY sum(total) where sum is NOT in SELECT list — can't preserve
        // the sort into the MV (sum is not a stored column). Downgrade to Skip.
        assert_eq!(
            classify(
                "SELECT status, count(*) FROM orders GROUP BY status \
                 ORDER BY sum(total) DESC"
            ),
            ShapeGate::Skip
        );
    }

    #[test]
    fn classify_gated_window_order_by_not_in_select_downgrades_to_skip() {
        // Window functions are Gated, but ORDER BY must still resolve
        // against the SELECT list.
        assert_eq!(
            classify(
                "SELECT id, row_number() OVER (ORDER BY total) \
                 FROM orders ORDER BY total DESC"
            ),
            ShapeGate::Skip
        );
    }
}
