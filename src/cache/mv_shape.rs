//! MV shape classification: whether a resolved query's shape can ever benefit
//! from a materialized result. Registration-time, pure analysis — compiled in
//! both builds, since admission analysis uses it; the MV state machine and
//! serve path live in `mv` (proxy-only).

use std::collections::HashSet;

use ecow::EcoString;

use crate::query::ast::SetOpType;
use crate::query::ast::rope::{
    Body, OrderBy, Query, Run, Scalar, ScalarKind, Select, SelectItem, Source,
};
use crate::query::resolve::{ColumnTarget, ResolvedQuery};

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

/// Whether every ORDER BY item of the query can be served: an ORDER BY of
/// a SELECT names a select-list position, and one of a set operation
/// names an output of its leftmost branch.
fn order_by_serve_viable(bound: &ResolvedQuery) -> bool {
    let query = bound.tree().root();
    let mut order_by = query.order_by();
    if order_by.len() == 0 {
        return true;
    }
    match query.body() {
        Body::Select(select) => {
            order_by.all(|clause| bound.order_by_position(select, clause).is_some())
        }
        Body::SetOp(_) => leftmost_query(query)
            .is_some_and(|leftmost| set_op_order_by_viable(bound, leftmost, order_by)),
        Body::Values(_) => false,
    }
}

/// Every ORDER BY item of a set operation names an output of its leftmost
/// branch by name.
fn set_op_order_by_viable<'t>(
    bound: &ResolvedQuery,
    leftmost: Query<'t>,
    mut order_by: Run<'t, OrderBy<'t>>,
) -> bool {
    let names = bound.output_names(leftmost);
    order_by.all(|clause| match clause.expr().kind() {
        ScalarKind::Column(column)
            if matches!(
                bound.column_target_at(column.id()),
                Some(ColumnTarget::Output)
            ) =>
        {
            names
                .iter()
                .any(|name| name.as_ref() == Some(column.column()))
        }
        ScalarKind::Column(_)
        | ScalarKind::Function(_)
        | ScalarKind::Literal(_)
        | ScalarKind::Case(_)
        | ScalarKind::Arithmetic(_)
        | ScalarKind::Subquery(_)
        | ScalarKind::Array(_)
        | ScalarKind::TypeCast(_) => false,
    })
}

/// The leftmost SELECT query of a set operation tree.
fn leftmost_query(query: Query<'_>) -> Option<Query<'_>> {
    match query.body() {
        Body::Select(_) => Some(query),
        Body::SetOp(set_op) => leftmost_query(set_op.left()),
        Body::Values(_) => None,
    }
}

/// Classify a query for the MV layer: `Gated` when its result is computed
/// from the source rows (aggregates, GROUP BY, HAVING, DISTINCT, windows,
/// joins, deduplicating set operations) and can be served from an MV;
/// `Skip` when it is a plain projection, a `UNION ALL`, VALUES, or its
/// ORDER BY cannot be served.
pub fn shape_classify(
    bound: &ResolvedQuery,
    aggregate_functions: &HashSet<EcoString>,
) -> ShapeGate {
    let shape = match bound.tree().root().body() {
        Body::Values(_) => return ShapeGate::Skip,
        Body::SetOp(set_op) => {
            if set_op.op() == SetOpType::Union && set_op.all() {
                return ShapeGate::Skip;
            }
            ShapeGate::Gated
        }
        Body::Select(select) => {
            if select_computes(select, aggregate_functions) {
                ShapeGate::Gated
            } else {
                return ShapeGate::Skip;
            }
        }
    };
    if !order_by_serve_viable(bound) {
        return ShapeGate::Skip;
    }
    shape
}

/// Whether a SELECT computes its result from its source rows rather than
/// projecting them one for one: a window, DISTINCT, GROUP BY, HAVING,
/// aggregate or join. Its result rows don't map to a bounded set of source
/// rows, so a LIMIT can't cap its population (ADR-060).
pub(crate) fn select_computes(
    select: Select<'_>,
    aggregate_functions: &HashSet<EcoString>,
) -> bool {
    select_reshapes(select, aggregate_functions) || select_has_join(select)
}

/// Whether a branch reshapes its rows: a window function, DISTINCT, GROUP
/// BY, HAVING or an aggregate. A join alone does not; its result's top N
/// still maps to a bounded set of each table's rows (PGC-570).
pub(crate) fn select_reshapes(
    select: Select<'_>,
    aggregate_functions: &HashSet<EcoString>,
) -> bool {
    items_any(select, &|e| e.has_window())
        || select.distinct()
        || select.group_by().len() > 0
        || select.having().is_some()
        || items_any(select, &|e| e.has_aggregate(aggregate_functions))
}

fn select_has_join(select: Select<'_>) -> bool {
    select.from().len() > 1
        || select
            .from()
            .any(|source| matches!(source, Source::Join(_)))
}

fn items_any<P>(select: Select<'_>, pred: &P) -> bool
where
    P: Fn(Scalar<'_>) -> bool,
{
    match select.items() {
        None => false,
        Some(mut items) => items.any(|item| match item {
            SelectItem::Expr(expr) => pred(expr.expr()),
            SelectItem::Star(_) => false,
        }),
    }
}

#[cfg(test)]
pub(super) mod tests {
    use iddqd::BiHashMap;
    use postgres_types::Type;

    use super::*;
    use crate::catalog::{Catalog, ColumnMetadata, ColumnPosition, TableMetadata};
    use crate::oid::{Oid, TypeOid};

    // ==================== Classifier tests ====================

    fn test_table(name: &str, oid: Oid, cols: &[&str]) -> TableMetadata {
        let columns = cols.iter().enumerate().map(|(i, c)| {
            let is_pk = i == 0;
            ColumnMetadata {
                name: (*c).into(),
                position: ColumnPosition::from_index(i).expect("column position in range"),
                type_oid: TypeOid::from_raw(if is_pk { 23 } else { 25 }),
                data_type: if is_pk { Type::INT4 } else { Type::TEXT },
                type_name: if is_pk { "int4" } else { "text" }.into(),
                cache_type_name: if is_pk { "int4" } else { "text" }.into(),
                is_primary_key: is_pk,
            }
        });
        TableMetadata::for_test(name, oid.get(), columns)
    }

    pub(crate) fn test_tables() -> Catalog {
        let mut t = BiHashMap::new();
        t.insert_overwrite(
            test_table("orders", Oid::from_raw(1), &["id", "status", "total"]).into(),
        );
        t.insert_overwrite(test_table("users", Oid::from_raw(2), &["id", "name", "email"]).into());
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
        let tables = test_tables();
        let bound = ResolvedQuery::parse_resolve(sql, &tables, &["public"]).expect("resolve query");
        shape_classify(&bound, &test_aggregate_functions())
    }

    /// A computed shape computes whether or not its ORDER BY can be served,
    /// which demotes its MV gate to `Skip` (PGC-527).
    #[test]
    fn test_select_computes_independent_of_order_by() {
        let tables = test_tables();
        for (sql, computes) in [
            (
                "SELECT id, status FROM orders ORDER BY total LIMIT 2",
                false,
            ),
            (
                "SELECT count(*) FROM orders GROUP BY status ORDER BY status LIMIT 2",
                true,
            ),
            (
                "SELECT id, row_number() OVER (ORDER BY total) FROM orders ORDER BY total DESC LIMIT 3",
                true,
            ),
            (
                "SELECT u.name FROM users u JOIN orders o ON o.id = u.id ORDER BY o.total DESC LIMIT 5",
                true,
            ),
            (
                "SELECT DISTINCT status FROM orders ORDER BY status LIMIT 2",
                true,
            ),
        ] {
            let bound =
                ResolvedQuery::parse_resolve(sql, &tables, &["public"]).expect("resolve query");
            let select = bound.select().expect("a SELECT");
            assert_eq!(
                select_computes(select, &test_aggregate_functions()),
                computes,
                "{sql}"
            );
        }
    }

    #[test]
    fn test_classify_plain_filter_is_skip() {
        assert_eq!(
            classify("SELECT * FROM orders WHERE id = 1"),
            ShapeGate::Skip
        );
    }

    #[test]
    fn test_classify_projection_is_skip() {
        assert_eq!(classify("SELECT id, status FROM orders"), ShapeGate::Skip);
    }

    #[test]
    fn test_classify_bare_aggregate_is_gated() {
        assert_eq!(classify("SELECT count(*) FROM orders"), ShapeGate::Gated);
        assert_eq!(classify("SELECT sum(total) FROM orders"), ShapeGate::Gated);
    }

    #[test]
    fn test_classify_group_by_is_gated() {
        assert_eq!(
            classify("SELECT status, count(*) FROM orders GROUP BY status"),
            ShapeGate::Gated
        );
    }

    #[test]
    fn test_classify_having_is_gated() {
        // HAVING on a GROUP BY query — row reduction via either signal.
        assert_eq!(
            classify("SELECT status, count(*) FROM orders GROUP BY status HAVING count(*) > 5"),
            ShapeGate::Gated
        );
    }

    #[test]
    fn test_classify_distinct_is_gated() {
        assert_eq!(
            classify("SELECT DISTINCT status FROM orders"),
            ShapeGate::Gated
        );
    }

    #[test]
    fn test_classify_window_function_is_gated() {
        assert_eq!(
            classify("SELECT id, row_number() OVER (ORDER BY total) FROM orders"),
            ShapeGate::Gated
        );
    }

    #[test]
    fn test_classify_window_with_group_by_is_gated() {
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
    fn test_classify_union_dedup_is_gated() {
        assert_eq!(
            classify("SELECT id FROM orders UNION SELECT id FROM users"),
            ShapeGate::Gated
        );
    }

    #[test]
    fn test_classify_union_all_is_skip() {
        // UNION ALL is strictly additive — no dedup, branches already cached.
        assert_eq!(
            classify("SELECT id FROM orders UNION ALL SELECT id FROM users"),
            ShapeGate::Skip
        );
    }

    #[test]
    fn test_classify_intersect_is_gated() {
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
    fn test_classify_except_is_gated() {
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
    fn test_classify_setop_order_by_identifier_in_select_is_gated() {
        // `id` appears in the left branch's SELECT list; the set-op's output
        // column is named `id`, so `ORDER BY id` is serveable against the MV.
        assert_eq!(
            classify("SELECT id FROM orders UNION SELECT id FROM users ORDER BY id DESC"),
            ShapeGate::Gated
        );
    }

    #[test]
    fn test_classify_setop_order_by_unknown_identifier_is_skip() {
        // `status` is NOT in the set-op's output (SELECT list is just `id`),
        // so MV can't preserve the sort — downgrade to Skip.
        assert_eq!(
            classify("SELECT id FROM orders UNION SELECT id FROM users ORDER BY status DESC"),
            ShapeGate::Skip
        );
    }

    #[test]
    fn test_classify_aggregate_inside_case_is_gated() {
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
    fn test_classify_aggregate_in_subquery_does_not_reduce_outer() {
        // A scalar subquery with count() in the SELECT list doesn't make the
        // outer query a reduction shape — it.s a plain projection over orders.
        assert_eq!(
            classify("SELECT id, (SELECT count(*) FROM users) AS user_count FROM orders"),
            ShapeGate::Skip
        );
    }

    #[test]
    fn test_classify_join_without_aggregate_is_gated() {
        // A plain join's result is the same predicate-scoped rows it scans, so
        // the row-reduction gate can't apply; its MV value is avoiding the
        // re-join, gated on input size (PGC-330).
        assert_eq!(
            classify("SELECT o.id, u.name FROM orders o JOIN users u ON o.id = u.id"),
            ShapeGate::Gated
        );
    }

    #[test]
    fn test_classify_join_with_aggregate_is_gated() {
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
    fn test_classify_gated_with_order_by_selected_aggregate_is_gated() {
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
    fn test_classify_gated_with_order_by_selected_group_column_is_gated() {
        assert_eq!(
            classify("SELECT status, count(*) FROM orders GROUP BY status ORDER BY status"),
            ShapeGate::Gated
        );
    }

    #[test]
    fn test_classify_gated_aggregate_order_by_not_in_select_downgrades_to_skip() {
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

    /// A positional ORDER BY is servable: it names its output by position.
    #[test]
    fn test_classify_grouped_positional_order_by_is_gated() {
        assert_eq!(
            classify("SELECT status, count(*) FROM orders GROUP BY status ORDER BY 2 DESC"),
            ShapeGate::Gated
        );
    }

    #[test]
    fn test_classify_gated_window_order_by_not_in_select_downgrades_to_skip() {
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
