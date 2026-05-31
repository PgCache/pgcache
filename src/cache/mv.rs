//! Materialized query results.
//!
//! Types and helpers for the MV cache layer. The MV state machine is driven
//! entirely from the writer task; the coordinator reads `MvState` to pick
//! between the fast path (serve from MV table) and the fallthrough path
//! (deparse resolved query against source-row cache).
//!
//! See `docs/materialized-results.md` for the full design.

use std::collections::HashSet;
use std::fmt::Write;
use std::sync::Arc;

use ecow::EcoString;
use postgres_protocol::escape::escape_identifier;

use crate::query::ast::{Deparse, LimitClause, OrderDirection, SetOpType};
use crate::query::resolved::{
    ResolvedOrderByClause, ResolvedQueryBody, ResolvedQueryExpr, ResolvedScalarExpr,
    ResolvedSelectColumns, ResolvedSelectNode, ResolvedTableSource,
};

/// Shape classification set at registration from the decorrelated, resolved
/// form of the query. Never changes for the life of the cache entry; eviction
/// + re-registration is the only way to re-classify.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShapeGate {
    /// Materialize unconditionally; skip the size gate.
    /// Reserved for window functions — recomputing partition/sort on every
    /// serve is expensive regardless of cardinality.
    Materialize,
    /// Shape suggests reduction is possible; apply the size gate at first
    /// population. Aggregates, GROUP BY, HAVING, DISTINCT.
    Measure,
    /// Shape rules out benefit; never materialize.
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
        matches!(self, ShapeGate::Materialize | ShapeGate::Measure)
    }
}

/// Runtime state of the materialized result for a cached query.
///
/// `Fresh` is the only state that produces a fast-path dispatch; all others
/// fall through to source-row evaluation. `Skipped` and `Ineligible` are
/// terminal for the life of the cache entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MvState {
    /// `ShapeGate::Skip` — never materialize. Terminal.
    Skipped,
    /// `ShapeGate::Measure` size gate evaluated and failed. Terminal — never
    /// materialize for the life of this cache entry.
    Ineligible,
    /// Should have a fresh MV but doesn't. `has_table` distinguishes the two
    /// sub-cases: `false` = never built (first build pending, includes the
    /// Measure gate check); `true` = stale table from a prior Fresh flipped
    /// by CDC invalidation or LimitBump (rebuild pending).
    Pending { has_table: bool },
    /// Build command sent to writer, writer hasn't processed yet. `has_table`
    /// is inherited from the `Pending` that triggered this dispatch and tells
    /// the writer which build path to take (`CREATE TABLE AS` vs
    /// `TRUNCATE + INSERT`).
    Scheduled { has_table: bool },
    /// Table exists and contents are fresh. Serve-path fast path.
    Fresh,
}

impl MvState {
    /// True when an on-disk MV table backs this state. Used by eviction to
    /// decide whether to issue `DROP TABLE` and by the pre-sweep to decide
    /// whether there are stale bytes to reclaim.
    pub fn has_table(self) -> bool {
        match self {
            MvState::Fresh => true,
            MvState::Pending { has_table } | MvState::Scheduled { has_table } => has_table,
            MvState::Skipped | MvState::Ineligible => false,
        }
    }
}

/// Initial `MvState` derived from a `ShapeGate` at registration. No table
/// exists yet in any case.
pub fn mv_state_initial(gate: ShapeGate) -> MvState {
    match gate {
        ShapeGate::Skip => MvState::Skipped,
        ShapeGate::Measure | ShapeGate::Materialize => MvState::Pending { has_table: false },
    }
}

/// All MV state for one cached query. Lives on `CachedQueryView`,
/// written by the writer: registration sets `shape_gate`/`state`; MV
/// build captures `output_columns` and flips `state` to `Fresh`.
#[derive(Debug, Clone)]
pub struct MvMeta {
    pub shape_gate: ShapeGate,
    pub state: MvState,
    /// PostgreSQL's output column names, captured at first build and
    /// reused across rebuilds. `None` until the MV has ever been built.
    pub output_columns: Option<Arc<[EcoString]>>,
    /// LIMIT cap for the MV body — set for join shapes (top-N over the
    /// join), `None` otherwise. Dispatch falls through when an incoming
    /// variant needs more rows than the MV holds.
    pub limit: Option<u64>,
}

impl MvMeta {
    /// Registration-time state for `shape_gate` — no table, no names yet.
    pub fn new(shape_gate: ShapeGate, limit: Option<u64>) -> Self {
        Self {
            shape_gate,
            state: mv_state_initial(shape_gate),
            output_columns: None,
            limit,
        }
    }
}

/// Serve-dispatch outcome. The `Mv` variant carries the column names
/// pulled from the *same* locked view observation that saw `Fresh`, so
/// "serve from MV without names" is unrepresentable past this point.
pub enum MvServe {
    Mv(Arc<[EcoString]>),
    SourceRow,
}

/// Format the cache-DB table name for an MV keyed by fingerprint.
/// Convention: `pgcache_mv.q_<fingerprint>`. The `q_` prefix keeps the
/// identifier unquoted-safe (PostgreSQL requires a letter/underscore first).
pub fn mv_table_name(fingerprint: u64) -> String {
    format!("pgcache_mv.q_{fingerprint}")
}

/// Build the serve-time SQL for reading from an MV table.
///
/// Shape: `SELECT * FROM <mv_table> [ORDER BY ...] [LIMIT ...]`.
///
/// Two ORDER BY strategies depending on body:
///
/// - **SELECT body** — emit **positional** (`ORDER BY 2 DESC`). The MV table's
///   columns come from `CREATE TABLE AS` and don't match the source-qualified
///   refs (`public.orders.status`, `count(orders.id)`) that resolved
///   `order_by` carries — emitting the expression would reference tables not
///   in the serve-time FROM clause.
///
/// - **SET OP body** — emit **direct deparse**. The resolver produces
///   `Identifier(name)` for set-op ORDER BY (see `order_by_as_identifiers`),
///   and those bare names match the MV column names (derived from the left
///   branch's SELECT-list aliases by `CREATE TABLE AS` on a set op). So the
///   naive deparse already works; no positional indirection needed.
///
/// In both cases the classifier (`shape_classify`) has already downgraded
/// queries whose ORDER BY can't be served against the MV to `Skip`, so this
/// function is called only on queries with viable ORDER BY.
///
/// Serve-time ORDER BY is essential even though population already applied it:
/// `SELECT * FROM mv` returns rows in arbitrary physical order, so for user
/// LIMIT < max_limit we need the re-sort to guarantee the correct top-M
/// subset. No generation SET — MV tables are not `pgcache_pgrx`-tracked.
pub fn mv_serve_sql(
    fingerprint: u64,
    resolved: &ResolvedQueryExpr,
    limit: Option<&LimitClause>,
    output_columns: &[EcoString],
) -> String {
    let table = mv_table_name(fingerprint);
    let mut sql = String::with_capacity(16 + table.len() + output_columns.len() * 24);
    // MV physical columns are positional (`c0..`) so duplicate output
    // names (e.g. two `count`) are storable; alias them back here. Empty
    // `output_columns` is a defensive fallback — a `Fresh` MV always has
    // captured names (the worker logs this case).
    sql.push_str("SELECT ");
    if output_columns.is_empty() {
        sql.push('*');
    } else {
        for (i, name) in output_columns.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            let _ = write!(sql, "c{i} AS {}", escape_identifier(name.as_str()));
        }
    }
    sql.push_str(" FROM ");
    sql.push_str(&table);

    if !resolved.order_by.is_empty() {
        match &resolved.body {
            ResolvedQueryBody::Select(select) => {
                mv_order_by_positional(&mut sql, &resolved.order_by, &select.columns);
            }
            ResolvedQueryBody::SetOp(_) => {
                mv_order_by_direct(&mut sql, &resolved.order_by);
            }
            ResolvedQueryBody::Values(_) => {
                unreachable!("MV fast path on Values body — classifier should have Skipped")
            }
        }
    }
    if let Some(l) = limit {
        l.deparse(&mut sql);
    }
    sql
}

/// Emit `ORDER BY N ASC|DESC, ...` by looking each expression's 1-based
/// position up in the SELECT list. Classifier guarantees a position exists.
fn mv_order_by_positional(
    sql: &mut String,
    order_by: &[ResolvedOrderByClause],
    columns: &ResolvedSelectColumns,
) {
    sql.push_str(" ORDER BY");
    let mut sep = "";
    for o in order_by {
        let pos = columns.columns_position_of(&o.expr).unwrap_or_else(|| {
            unreachable!("ORDER BY expression not in SELECT list — classifier invariant");
        });
        let _ = write!(sql, "{sep} {pos}");
        match o.direction {
            OrderDirection::Asc => sql.push_str(" ASC"),
            OrderDirection::Desc => sql.push_str(" DESC"),
        }
        sep = ",";
    }
}

/// Emit `ORDER BY <expr> ASC|DESC, ...` by deparsing each expression directly.
/// Used for set-op MVs where the resolver produces `Identifier` expressions
/// whose bare names match the MV table's column names.
fn mv_order_by_direct(sql: &mut String, order_by: &[ResolvedOrderByClause]) {
    sql.push_str(" ORDER BY");
    let mut sep = "";
    for o in order_by {
        sql.push_str(sep);
        sql.push(' ');
        o.deparse(sql);
        sep = ",";
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
            // UNION ALL is strictly additive — no dedup, no reduction. Branches
            // are already cached; concatenating them at serve time is two seq
            // scans, cheap. MV would duplicate storage without saving compute.
            if setop.op == SetOpType::Union && setop.all {
                return ShapeGate::Skip;
            }
            // UNION (dedup), INTERSECT [ALL], EXCEPT [ALL] all reduce the
            // result to ≤ sum/min/left of branches — candidates for Measure.
            // Size gate at first build decides whether reduction is enough
            // to justify storage.
            ShapeGate::Measure
        }

        ResolvedQueryBody::Select(select) => {
            if columns_any(&select.columns, &scalar_expr_has_window) {
                // Window function → Materialize unconditionally (expensive on
                // pure compute grounds, even when result cardinality matches
                // source cardinality).
                ShapeGate::Materialize
            } else if select.distinct
                || !select.group_by.is_empty()
                || select.having.is_some()
                || columns_any(&select.columns, &|e| e.has_aggregate(aggregate_functions))
            {
                // Reducing / transforming shape → Measure.
                ShapeGate::Measure
            } else if select_has_join(select) {
                // Join result is generally far smaller than its inputs; size
                // gate decides whether the reduction justifies storage.
                ShapeGate::Measure
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
fn select_has_join(select: &ResolvedSelectNode) -> bool {
    select.from.len() > 1
        || select
            .from
            .iter()
            .any(|src| matches!(src, ResolvedTableSource::Join(_)))
}

/// Top-level body is a SELECT with a join. Only joins get an MV LIMIT
/// cap; other reducers collapse the input regardless.
pub fn resolved_has_join(resolved: &ResolvedQueryExpr) -> bool {
    match &resolved.body {
        ResolvedQueryBody::Select(s) => select_has_join(s),
        ResolvedQueryBody::SetOp(_) | ResolvedQueryBody::Values(_) => false,
    }
}

/// Returns true if any top-level column expression in the SELECT list satisfies
/// `pred`. Does not descend into subqueries.
fn columns_any<P>(columns: &ResolvedSelectColumns, pred: &P) -> bool
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
fn scalar_expr_has_window(expr: &ResolvedScalarExpr) -> bool {
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
mod tests {

    use iddqd::BiHashMap;
    use tokio_postgres::types::Type;

    use super::*;
    use crate::catalog::{ColumnMetadata, ColumnStore, TableMetadata};
    use crate::query::ast::query_expr_parse;
    use crate::query::resolved::query_expr_resolve;

    #[test]
    fn mv_table_name_format() {
        assert_eq!(mv_table_name(0), "pgcache_mv.q_0");
        assert_eq!(mv_table_name(42), "pgcache_mv.q_42");
        assert_eq!(mv_table_name(u64::MAX), "pgcache_mv.q_18446744073709551615");
    }

    #[test]
    fn mv_state_initial_maps_from_gate() {
        assert_eq!(mv_state_initial(ShapeGate::Skip), MvState::Skipped);
        assert_eq!(
            mv_state_initial(ShapeGate::Measure),
            MvState::Pending { has_table: false }
        );
        assert_eq!(
            mv_state_initial(ShapeGate::Materialize),
            MvState::Pending { has_table: false }
        );
    }

    #[test]
    fn is_reducer_matches_non_projection_shapes() {
        assert!(ShapeGate::Materialize.is_reducer());
        assert!(ShapeGate::Measure.is_reducer());
        assert!(!ShapeGate::Skip.is_reducer());
    }

    #[test]
    fn has_table_covers_only_on_disk_states() {
        assert!(!MvState::Skipped.has_table());
        assert!(!MvState::Ineligible.has_table());
        assert!(!MvState::Pending { has_table: false }.has_table());
        assert!(!MvState::Scheduled { has_table: false }.has_table());
        assert!(MvState::Pending { has_table: true }.has_table());
        assert!(MvState::Scheduled { has_table: true }.has_table());
        assert!(MvState::Fresh.has_table());
    }

    // ==================== Classifier tests ====================

    fn test_table(name: &str, oid: u32, cols: &[&str]) -> TableMetadata {
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
            relation_oid: oid,
            name: name.into(),
            schema: "public".into(),
            primary_key_columns: vec![cols[0].into()],
            columns,
            indexes: Vec::new(),
        }
    }

    fn test_tables() -> BiHashMap<TableMetadata> {
        let mut t = BiHashMap::new();
        t.insert_overwrite(test_table("orders", 1, &["id", "status", "total"]));
        t.insert_overwrite(test_table("users", 2, &["id", "name", "email"]));
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
    fn classify_bare_aggregate_is_measure() {
        assert_eq!(classify("SELECT count(*) FROM orders"), ShapeGate::Measure);
        assert_eq!(
            classify("SELECT sum(total) FROM orders"),
            ShapeGate::Measure
        );
    }

    #[test]
    fn classify_group_by_is_measure() {
        assert_eq!(
            classify("SELECT status, count(*) FROM orders GROUP BY status"),
            ShapeGate::Measure
        );
    }

    #[test]
    fn classify_having_is_measure() {
        // HAVING on a GROUP BY query — Measure via either signal.
        assert_eq!(
            classify("SELECT status, count(*) FROM orders GROUP BY status HAVING count(*) > 5"),
            ShapeGate::Measure
        );
    }

    #[test]
    fn classify_distinct_is_measure() {
        assert_eq!(
            classify("SELECT DISTINCT status FROM orders"),
            ShapeGate::Measure
        );
    }

    #[test]
    fn classify_window_function_is_materialize() {
        assert_eq!(
            classify("SELECT id, row_number() OVER (ORDER BY total) FROM orders"),
            ShapeGate::Materialize
        );
    }

    #[test]
    fn classify_window_takes_precedence_over_measure() {
        // Even with GROUP BY, a window function forces Materialize (window is the
        // compute-expensive signal; Measure is a size-reduction signal).
        assert_eq!(
            classify(
                "SELECT status, count(*), row_number() OVER (ORDER BY status) \
                 FROM orders GROUP BY status"
            ),
            ShapeGate::Materialize
        );
    }

    #[test]
    fn classify_union_dedup_is_measure() {
        assert_eq!(
            classify("SELECT id FROM orders UNION SELECT id FROM users"),
            ShapeGate::Measure
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
    fn classify_intersect_is_measure() {
        assert_eq!(
            classify("SELECT id FROM orders INTERSECT SELECT id FROM users"),
            ShapeGate::Measure
        );
        assert_eq!(
            classify("SELECT id FROM orders INTERSECT ALL SELECT id FROM users"),
            ShapeGate::Measure
        );
    }

    #[test]
    fn classify_except_is_measure() {
        assert_eq!(
            classify("SELECT id FROM orders EXCEPT SELECT id FROM users"),
            ShapeGate::Measure
        );
        assert_eq!(
            classify("SELECT id FROM orders EXCEPT ALL SELECT id FROM users"),
            ShapeGate::Measure
        );
    }

    #[test]
    fn classify_setop_order_by_identifier_in_select_is_measure() {
        // `id` appears in the left branch's SELECT list; the set-op's output
        // column is named `id`, so `ORDER BY id` is serveable against the MV.
        assert_eq!(
            classify("SELECT id FROM orders UNION SELECT id FROM users ORDER BY id DESC"),
            ShapeGate::Measure
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
    fn classify_aggregate_inside_case_is_measure() {
        // Aggregate nested inside CASE branch should still be detected.
        assert_eq!(
            classify(
                "SELECT CASE WHEN status = 'open' THEN count(*) ELSE 0 END \
                 FROM orders GROUP BY status"
            ),
            ShapeGate::Measure
        );
    }

    #[test]
    fn classify_aggregate_in_subquery_does_not_measure_outer() {
        // A scalar subquery with count() in the SELECT list doesn't make the
        // outer query Measure — the outer query is a plain projection over orders.
        assert_eq!(
            classify("SELECT id, (SELECT count(*) FROM users) AS user_count FROM orders"),
            ShapeGate::Skip
        );
    }

    #[test]
    fn classify_join_without_aggregate_is_measure() {
        // A join's result is generally far smaller than its joined inputs;
        // the size gate (with the user's LIMIT folded in via mv_limit at
        // registration) decides whether it's worth materializing.
        assert_eq!(
            classify("SELECT o.id, u.name FROM orders o JOIN users u ON o.id = u.id"),
            ShapeGate::Measure
        );
    }

    #[test]
    fn classify_join_with_aggregate_is_measure() {
        assert_eq!(
            classify(
                "SELECT u.id, count(o.id) FROM users u \
                 JOIN orders o ON u.id = o.id GROUP BY u.id"
            ),
            ShapeGate::Measure
        );
    }

    // ==================== ORDER BY interaction ====================

    #[test]
    fn classify_measure_with_order_by_selected_aggregate_is_measure() {
        // ORDER BY count(*) — count(*) is in SELECT, position lookup succeeds.
        assert_eq!(
            classify(
                "SELECT status, count(*) FROM orders GROUP BY status \
                 ORDER BY count(*) DESC"
            ),
            ShapeGate::Measure
        );
    }

    #[test]
    fn classify_measure_with_order_by_selected_group_column_is_measure() {
        assert_eq!(
            classify("SELECT status, count(*) FROM orders GROUP BY status ORDER BY status"),
            ShapeGate::Measure
        );
    }

    #[test]
    fn classify_measure_with_order_by_not_in_select_downgrades_to_skip() {
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
    fn classify_materialize_with_order_by_not_in_select_downgrades_to_skip() {
        // Window functions force Materialize, but ORDER BY must still resolve
        // against the SELECT list.
        assert_eq!(
            classify(
                "SELECT id, row_number() OVER (ORDER BY total) \
                 FROM orders ORDER BY total DESC"
            ),
            ShapeGate::Skip
        );
    }

    // ==================== mv_serve_sql positional ORDER BY ====================

    fn resolve_for_serve(sql: &str) -> ResolvedQueryExpr {
        let ast = query_expr_parse(sql).expect("convert to AST");
        query_expr_resolve(&ast, &test_tables(), &["public"]).expect("resolve query")
    }

    /// Empty `output_columns` exercises the positional `SELECT *` fallback,
    /// which keeps the ORDER BY / LIMIT assertions below orthogonal to the
    /// aliased-projection tests.
    fn build_serve_sql(sql: &str) -> String {
        mv_serve_sql(42, &resolve_for_serve(sql), None, &[])
    }

    fn build_serve_sql_named(sql: &str, names: &[&str]) -> String {
        let names: Vec<EcoString> = names.iter().map(|n| EcoString::from(*n)).collect();
        mv_serve_sql(42, &resolve_for_serve(sql), None, &names)
    }

    #[test]
    fn mv_serve_sql_no_order_by() {
        let out = build_serve_sql("SELECT status, count(*) FROM orders GROUP BY status");
        assert_eq!(out, "SELECT * FROM pgcache_mv.q_42");
    }

    #[test]
    fn mv_serve_sql_aliases_positional_columns_back() {
        let out = build_serve_sql_named(
            "SELECT status, count(*) FROM orders GROUP BY status",
            &["status", "count"],
        );
        assert_eq!(
            out,
            r#"SELECT c0 AS "status", c1 AS "count" FROM pgcache_mv.q_42"#
        );
    }

    #[test]
    fn mv_serve_sql_allows_duplicate_output_names() {
        // PGC-136: two unaliased count(*) — illegal as table columns,
        // legal as a result set via positional storage + aliased serve.
        let out =
            build_serve_sql_named("SELECT count(*), count(*) FROM orders", &["count", "count"]);
        assert_eq!(
            out,
            r#"SELECT c0 AS "count", c1 AS "count" FROM pgcache_mv.q_42"#
        );
    }

    #[test]
    fn mv_serve_sql_aliased_with_positional_order_by() {
        let out = build_serve_sql_named(
            "SELECT status, count(*) FROM orders GROUP BY status ORDER BY count(*) DESC",
            &["status", "count"],
        );
        assert_eq!(
            out,
            r#"SELECT c0 AS "status", c1 AS "count" FROM pgcache_mv.q_42 ORDER BY 2 DESC"#
        );
    }

    #[test]
    fn mv_serve_sql_positional_order_by_aggregate_desc() {
        let out = build_serve_sql(
            "SELECT status, count(*) FROM orders GROUP BY status ORDER BY count(*) DESC",
        );
        assert_eq!(out, "SELECT * FROM pgcache_mv.q_42 ORDER BY 2 DESC");
    }

    #[test]
    fn mv_serve_sql_positional_order_by_column_asc() {
        let out = build_serve_sql(
            "SELECT status, count(*) FROM orders GROUP BY status ORDER BY status ASC",
        );
        assert_eq!(out, "SELECT * FROM pgcache_mv.q_42 ORDER BY 1 ASC");
    }

    #[test]
    fn mv_serve_sql_positional_order_by_multiple() {
        let out = build_serve_sql(
            "SELECT status, count(*) FROM orders GROUP BY status \
             ORDER BY count(*) DESC, status ASC",
        );
        assert_eq!(out, "SELECT * FROM pgcache_mv.q_42 ORDER BY 2 DESC, 1 ASC");
    }

    // ==================== SetOp body serve SQL ====================

    #[test]
    fn mv_serve_sql_setop_no_order_by() {
        let out = build_serve_sql("SELECT id FROM orders UNION SELECT id FROM users");
        assert_eq!(out, "SELECT * FROM pgcache_mv.q_42");
    }

    #[test]
    fn mv_serve_sql_setop_order_by_identifier_deparses_directly() {
        // SetOp ORDER BY is Identifier-based; the bare name matches the MV
        // column, so we emit it directly (no positional indirection).
        let out =
            build_serve_sql("SELECT id FROM orders UNION SELECT id FROM users ORDER BY id DESC");
        assert_eq!(out, "SELECT * FROM pgcache_mv.q_42 ORDER BY id DESC");
    }

    #[test]
    fn mv_serve_sql_intersect_no_order_by() {
        let out = build_serve_sql("SELECT id FROM orders INTERSECT SELECT id FROM users");
        assert_eq!(out, "SELECT * FROM pgcache_mv.q_42");
    }
}
