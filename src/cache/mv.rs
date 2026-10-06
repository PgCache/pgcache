//! Materialized query results.
//!
//! Types and helpers for the MV cache layer. The MV state machine is driven
//! entirely from the writer task; the dispatch reads `MvState` to pick
//! between the fast path (serve from MV table) and the fallthrough path
//! (deparse resolved query against source-row cache). Proxy-only; the
//! registration-time shape classifier both builds need is `mv_shape`.
//!
//! See `docs/materialized-results.md` for the full design.

use std::fmt::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ecow::EcoString;
use postgres_protocol::escape::escape_identifier;

use super::mv_shape::{ShapeGate, columns_any, scalar_expr_has_window, select_has_join};
use crate::query::Fingerprint;
use crate::query::ast::{Deparse, LimitClause, OrderDirection};
use crate::query::resolved::{
    ResolvedOrderByClause, ResolvedQueryBody, ResolvedQueryExpr, ResolvedSelectColumns,
};

/// Runtime state of the materialized result for a cached query.
///
/// `Fresh` is the only state that produces a fast-path dispatch; all others
/// fall through to source-row evaluation. `Skipped` and `Ineligible` are
/// terminal for the life of the cache entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MvState {
    /// `ShapeGate::Skip` — never materialize. Terminal.
    Skipped,
    /// The first-build gate (row-reduction OR compute-avoidance test) evaluated
    /// and neither passed. Terminal — never materialize for the life of this
    /// cache entry.
    Ineligible,
    /// Should have a fresh MV but doesn't. `has_table` distinguishes the two
    /// sub-cases: `false` = never built (first build pending, includes the
    /// first-build gate check); `true` = stale table from a prior Fresh flipped
    /// by CDC invalidation or LimitBump (rebuild pending).
    Pending { has_table: bool },
    /// Build command sent to writer, writer hasn't processed yet. `has_table`
    /// is inherited from the `Pending` that triggered this dispatch and tells
    /// the writer which build path to take (`CREATE TABLE AS` vs
    /// `TRUNCATE + INSERT`).
    Scheduled { has_table: bool },
    /// Build task in flight on the shared runtime. `has_table` is the bit the
    /// build started with (picks the reset target if the build fails).
    Building { has_table: bool },
    /// A CDC change dirtied the query while its build was in flight; the
    /// build's result must be discarded at completion. `has_table` is
    /// inherited from `Building`.
    BuildingDirty { has_table: bool },
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
            MvState::Pending { has_table }
            | MvState::Scheduled { has_table }
            | MvState::Building { has_table }
            | MvState::BuildingDirty { has_table } => has_table,
            MvState::Skipped | MvState::Ineligible => false,
        }
    }

    /// State after a relevant CDC change (insert/update/delete that could
    /// affect the query's result), or `None` when the change has no effect.
    /// `Fresh` loses its table-is-current claim; an in-flight build is marked
    /// so its result is discarded at completion.
    pub fn dirtied(self) -> Option<MvState> {
        match self {
            MvState::Fresh => Some(MvState::Pending { has_table: true }),
            MvState::Building { has_table } => Some(MvState::BuildingDirty { has_table }),
            MvState::Skipped
            | MvState::Ineligible
            | MvState::Pending { .. }
            | MvState::Scheduled { .. }
            | MvState::BuildingDirty { .. } => None,
        }
    }
}

/// A `Fresh` MV dirtied before living this long counts as a wasted build:
/// its build cost was never amortized (PGC-364 discard-backoff).
pub(crate) const MV_PAYOFF_WINDOW: Duration = Duration::from_secs(5);
/// First backoff interval, doubling per additional consecutive wasted build.
pub(crate) const MV_BACKOFF_BASE: Duration = Duration::from_secs(1);
/// Backoff ceiling — a permanently thrashing MV still probes this often, so
/// the worst-case aggregate rebuild rate is N-thrashers / cap.
pub(crate) const MV_BACKOFF_CAP: Duration = Duration::from_secs(300);

/// Rebuild cooldown after `wasted` consecutive no-payoff builds: `None` below
/// the engagement threshold (one wasted build is normal — any write to a
/// cached table causes one), then exponential from [`MV_BACKOFF_BASE`] capped
/// at [`MV_BACKOFF_CAP`].
pub(crate) fn backoff_duration(wasted: u32) -> Option<Duration> {
    if wasted < 2 {
        return None;
    }
    // 2^9 s = 512 s already exceeds the cap; clamping the shift avoids overflow.
    let doublings = (wasted - 2).min(9);
    Some((MV_BACKOFF_BASE * (1 << doublings)).min(MV_BACKOFF_CAP))
}

/// Initial `MvState` derived from a `ShapeGate` at registration. No table
/// exists yet in any case.
pub fn mv_state_initial(gate: ShapeGate) -> MvState {
    match gate {
        ShapeGate::Skip => MvState::Skipped,
        ShapeGate::Gated => MvState::Pending { has_table: false },
    }
}

/// All MV state for one cached query. Lives on `CachedQueryView`,
/// written by the writer: registration sets `shape_gate`/`state`; MV
/// build captures `output_columns` and flips `state` to `Fresh`.
#[derive(Debug, Clone)]
pub struct MvMeta {
    pub shape_gate: ShapeGate,
    /// Private so no raw `mv.state = …` write exists outside this module: writer
    /// transitions go through `WriterCore::mv_state_write`, the dispatch side
    /// through `state_set`.
    state: MvState,
    /// PostgreSQL's output column names, captured at first build and
    /// reused across rebuilds. `None` until the MV has ever been built.
    pub output_columns: Option<Arc<[EcoString]>>,
    /// LIMIT cap for the MV body — set for join shapes (top-N over the
    /// join), `None` otherwise. Dispatch falls through when an incoming
    /// variant needs more rows than the MV holds.
    pub limit: Option<u64>,
    /// When the MV last flipped `Fresh` — measures whether a build's cost was
    /// amortized before the next dirty (PGC-364).
    fresh_at: Option<Instant>,
    /// Consecutive builds with no payoff: discarded (`BuildingDirty`), failed,
    /// or `Fresh` dirtied inside [`MV_PAYOFF_WINDOW`]. Reset when a `Fresh`
    /// outlives the window.
    wasted_builds: u32,
    /// While in the future, `Pending` entries are not scheduled for rebuild —
    /// hits serve from source rows instead (discard-backoff, PGC-364).
    retry_after: Option<Instant>,
}

impl MvMeta {
    /// Registration-time state for `shape_gate` — no table, no names yet.
    pub fn new(shape_gate: ShapeGate, limit: Option<u64>) -> Self {
        Self {
            shape_gate,
            state: mv_state_initial(shape_gate),
            output_columns: None,
            limit,
            fresh_at: None,
            wasted_builds: 0,
            retry_after: None,
        }
    }

    /// Current MV state (`MvState` is `Copy`).
    pub fn state(&self) -> MvState {
        self.state
    }

    /// Consecutive no-payoff builds (for status/observability).
    pub fn wasted_builds(&self) -> u32 {
        self.wasted_builds
    }

    /// Remaining rebuild cooldown, `None` when scheduling is permitted.
    pub fn backoff_remaining(&self, now: Instant) -> Option<Duration> {
        let retry_after = self.retry_after?;
        (retry_after > now).then(|| retry_after - now)
    }

    /// Whether a `Pending → Scheduled` build dispatch is currently permitted.
    pub fn build_permitted(&self, now: Instant) -> bool {
        self.retry_after.is_none_or(|t| now >= t)
    }

    /// Stamp the `→ Fresh` flip so the next dirty can judge the build's payoff.
    pub(super) fn fresh_mark(&mut self, now: Instant) {
        self.fresh_at = Some(now);
    }

    /// Record a no-payoff build (discarded, failed, or short-lived `Fresh`) and
    /// arm the rebuild cooldown once past the engagement threshold.
    pub(super) fn waste_record(&mut self, now: Instant) {
        self.wasted_builds = self.wasted_builds.saturating_add(1);
        self.retry_after = backoff_duration(self.wasted_builds).map(|d| now + d);
    }

    /// Apply the dirty transition (`MvState::dirtied`) with its backoff
    /// bookkeeping: a `Fresh` that outlived [`MV_PAYOFF_WINDOW`] proves payoff
    /// and resets the waste counter; a younger one counts as a wasted build
    /// (an unstamped `Fresh` conservatively counts as waste too). The
    /// `Building → BuildingDirty` flip records nothing here — that build's
    /// waste is counted once, at completion discard. No-op for non-dirtiable
    /// states.
    pub(super) fn dirty_apply(&mut self, now: Instant) {
        let Some(next) = self.state.dirtied() else {
            return;
        };
        if self.state == MvState::Fresh {
            match self.fresh_at {
                Some(t) if now.duration_since(t) >= MV_PAYOFF_WINDOW => {
                    self.wasted_builds = 0;
                    self.retry_after = None;
                }
                _ => self.waste_record(now),
            }
            self.fresh_at = None;
        }
        self.state = next;
    }

    /// Raw state write. Prefer `WriterCore::mv_state_write`, which also keeps the
    /// dirtiable-MV index consistent (PGC-338); call this directly only for the
    /// non-dirtiable dispatch-side transition (`mv_schedule`), which the index
    /// never tracks.
    pub(super) fn state_set(&mut self, state: MvState) {
        self.state = state;
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
pub fn mv_table_name(fingerprint: Fingerprint) -> String {
    format!("pgcache_mv.q_{fingerprint}")
}

/// Build the serve-time SQL for reading from an MV table, into a caller-provided
/// buffer (cleared first) so the serve path can reuse the connection's recycled
/// `sql_buf` rather than allocating a fresh `String` per cache hit.
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
pub fn mv_serve_sql_into(
    sql: &mut String,
    fingerprint: Fingerprint,
    resolved: &ResolvedQueryExpr,
    limit: Option<&LimitClause>,
    output_columns: &[EcoString],
) {
    let table = mv_table_name(fingerprint);
    sql.clear();
    sql.reserve(16 + table.len() + output_columns.len() * 24);
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
                mv_order_by_positional(sql, &resolved.order_by, &select.columns);
            }
            ResolvedQueryBody::SetOp(_) => {
                mv_order_by_direct(sql, &resolved.order_by);
            }
            ResolvedQueryBody::Values(_) => {
                unreachable!("MV fast path on Values body — classifier should have Skipped")
            }
        }
    }
    if let Some(l) = limit {
        l.deparse(sql);
    }
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

/// Top-level body is a SELECT with a join. Only joins get an MV LIMIT
/// cap; other reducers collapse the input regardless.
pub(crate) fn resolved_has_join(resolved: &ResolvedQueryExpr) -> bool {
    match &resolved.body {
        ResolvedQueryBody::Select(s) => select_has_join(s),
        ResolvedQueryBody::SetOp(_) | ResolvedQueryBody::Values(_) => false,
    }
}

/// Top-level SELECT projects a window function. A window MV must store the whole
/// result (the window depends on the full partition), so it's excluded from the
/// join top-N `mv_limit` cap even when it also contains a join.
pub(crate) fn resolved_has_window(resolved: &ResolvedQueryExpr) -> bool {
    match &resolved.body {
        ResolvedQueryBody::Select(s) => columns_any(&s.columns, &scalar_expr_has_window),
        ResolvedQueryBody::SetOp(_) | ResolvedQueryBody::Values(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::mv_shape::tests::test_tables;
    use crate::query::ast::query_expr_parse;
    use crate::query::resolved::query_expr_resolve;

    #[test]
    fn test_mv_table_name_format() {
        assert_eq!(mv_table_name(Fingerprint::from_raw(0)), "pgcache_mv.q_0");
        assert_eq!(mv_table_name(Fingerprint::from_raw(42)), "pgcache_mv.q_42");
        assert_eq!(
            mv_table_name(Fingerprint::from_raw(u64::MAX)),
            "pgcache_mv.q_18446744073709551615"
        );
    }

    #[test]
    fn test_mv_state_initial_maps_from_gate() {
        assert_eq!(mv_state_initial(ShapeGate::Skip), MvState::Skipped);
        assert_eq!(
            mv_state_initial(ShapeGate::Gated),
            MvState::Pending { has_table: false }
        );
    }

    #[test]
    fn test_has_table_covers_only_on_disk_states() {
        assert!(!MvState::Skipped.has_table());
        assert!(!MvState::Ineligible.has_table());
        assert!(!MvState::Pending { has_table: false }.has_table());
        assert!(!MvState::Scheduled { has_table: false }.has_table());
        assert!(!MvState::Building { has_table: false }.has_table());
        assert!(!MvState::BuildingDirty { has_table: false }.has_table());
        assert!(MvState::Pending { has_table: true }.has_table());
        assert!(MvState::Scheduled { has_table: true }.has_table());
        assert!(MvState::Building { has_table: true }.has_table());
        assert!(MvState::BuildingDirty { has_table: true }.has_table());
        assert!(MvState::Fresh.has_table());
    }

    #[test]
    fn test_dirtied_invalidates_fresh_and_marks_in_flight_builds() {
        assert_eq!(
            MvState::Fresh.dirtied(),
            Some(MvState::Pending { has_table: true })
        );
        assert_eq!(
            MvState::Building { has_table: false }.dirtied(),
            Some(MvState::BuildingDirty { has_table: false })
        );
        assert_eq!(
            MvState::Building { has_table: true }.dirtied(),
            Some(MvState::BuildingDirty { has_table: true })
        );
    }

    #[test]
    fn test_dirtied_is_noop_for_already_dirty_or_terminal_states() {
        assert_eq!(MvState::Skipped.dirtied(), None);
        assert_eq!(MvState::Ineligible.dirtied(), None);
        assert_eq!(MvState::Pending { has_table: true }.dirtied(), None);
        assert_eq!(MvState::Scheduled { has_table: true }.dirtied(), None);
        assert_eq!(MvState::BuildingDirty { has_table: true }.dirtied(), None);
    }

    // ==================== Discard-backoff tests (PGC-364) ====================

    /// A gated `MvMeta` driven to `Fresh` with `fresh_at` stamped at `now`.
    fn fresh_meta(now: Instant) -> MvMeta {
        let mut meta = MvMeta::new(ShapeGate::Gated, None);
        meta.state_set(MvState::Fresh);
        meta.fresh_mark(now);
        meta
    }

    #[test]
    fn test_backoff_duration_engages_at_second_waste() {
        assert_eq!(backoff_duration(0), None);
        assert_eq!(backoff_duration(1), None);
        assert_eq!(backoff_duration(2), Some(MV_BACKOFF_BASE));
        assert_eq!(backoff_duration(3), Some(MV_BACKOFF_BASE * 2));
        assert_eq!(backoff_duration(4), Some(MV_BACKOFF_BASE * 4));
    }

    #[test]
    fn test_backoff_duration_caps() {
        assert_eq!(backoff_duration(20), Some(MV_BACKOFF_CAP));
        assert_eq!(backoff_duration(u32::MAX), Some(MV_BACKOFF_CAP));
    }

    #[test]
    fn test_dirty_apply_young_fresh_counts_waste_first_free() {
        let now = Instant::now();
        let mut meta = fresh_meta(now);
        meta.dirty_apply(now + MV_PAYOFF_WINDOW / 2);
        assert_eq!(meta.state(), MvState::Pending { has_table: true });
        assert_eq!(meta.wasted_builds(), 1);
        // First waste is free: any write to a cached table causes one.
        assert!(meta.build_permitted(now + MV_PAYOFF_WINDOW / 2));
    }

    #[test]
    fn test_dirty_apply_second_waste_arms_cooldown() {
        let now = Instant::now();
        let mut meta = fresh_meta(now);
        meta.dirty_apply(now); // waste 1
        meta.state_set(MvState::Fresh);
        meta.fresh_mark(now);
        meta.dirty_apply(now); // waste 2 → cooldown armed
        assert_eq!(meta.wasted_builds(), 2);
        assert!(!meta.build_permitted(now));
        assert_eq!(meta.backoff_remaining(now), Some(MV_BACKOFF_BASE));
        // Permitted again once the cooldown elapses.
        assert!(meta.build_permitted(now + MV_BACKOFF_BASE));
        assert_eq!(meta.backoff_remaining(now + MV_BACKOFF_BASE), None);
    }

    #[test]
    fn test_dirty_apply_long_lived_fresh_resets() {
        let now = Instant::now();
        let mut meta = fresh_meta(now);
        meta.waste_record(now);
        meta.waste_record(now);
        assert!(!meta.build_permitted(now));
        meta.dirty_apply(now + MV_PAYOFF_WINDOW);
        assert_eq!(meta.state(), MvState::Pending { has_table: true });
        assert_eq!(meta.wasted_builds(), 0);
        assert!(meta.build_permitted(now + MV_PAYOFF_WINDOW));
    }

    #[test]
    fn test_dirty_apply_unstamped_fresh_is_conservative_waste() {
        let now = Instant::now();
        let mut meta = MvMeta::new(ShapeGate::Gated, None);
        meta.state_set(MvState::Fresh); // no fresh_mark
        meta.dirty_apply(now);
        assert_eq!(meta.wasted_builds(), 1);
    }

    #[test]
    fn test_dirty_apply_building_records_nothing() {
        // The in-flight dirty only flips the state; the build's waste is
        // counted once, at completion discard.
        let now = Instant::now();
        let mut meta = MvMeta::new(ShapeGate::Gated, None);
        meta.state_set(MvState::Building { has_table: false });
        meta.dirty_apply(now);
        assert_eq!(meta.state(), MvState::BuildingDirty { has_table: false });
        assert_eq!(meta.wasted_builds(), 0);
        assert!(meta.build_permitted(now));
    }

    #[test]
    fn test_dirty_apply_noop_states_untouched() {
        let now = Instant::now();
        let mut meta = MvMeta::new(ShapeGate::Gated, None);
        meta.dirty_apply(now);
        assert_eq!(meta.state(), MvState::Pending { has_table: false });
        assert_eq!(meta.wasted_builds(), 0);
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
        let mut out = String::new();
        mv_serve_sql_into(
            &mut out,
            Fingerprint::from_raw(42),
            &resolve_for_serve(sql),
            None,
            &[],
        );
        out
    }

    fn build_serve_sql_named(sql: &str, names: &[&str]) -> String {
        let names: Vec<EcoString> = names.iter().map(|n| EcoString::from(*n)).collect();
        let mut out = String::new();
        mv_serve_sql_into(
            &mut out,
            Fingerprint::from_raw(42),
            &resolve_for_serve(sql),
            None,
            &names,
        );
        out
    }

    #[test]
    fn test_mv_serve_sql_no_order_by() {
        let out = build_serve_sql("SELECT status, count(*) FROM orders GROUP BY status");
        assert_eq!(out, "SELECT * FROM pgcache_mv.q_42");
    }

    #[test]
    fn test_mv_serve_sql_aliases_positional_columns_back() {
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
    fn test_mv_serve_sql_allows_duplicate_output_names() {
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
    fn test_mv_serve_sql_aliased_with_positional_order_by() {
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
    fn test_mv_serve_sql_positional_order_by_aggregate_desc() {
        let out = build_serve_sql(
            "SELECT status, count(*) FROM orders GROUP BY status ORDER BY count(*) DESC",
        );
        assert_eq!(out, "SELECT * FROM pgcache_mv.q_42 ORDER BY 2 DESC");
    }

    #[test]
    fn test_mv_serve_sql_positional_order_by_column_asc() {
        let out = build_serve_sql(
            "SELECT status, count(*) FROM orders GROUP BY status ORDER BY status ASC",
        );
        assert_eq!(out, "SELECT * FROM pgcache_mv.q_42 ORDER BY 1 ASC");
    }

    #[test]
    fn test_mv_serve_sql_positional_order_by_multiple() {
        let out = build_serve_sql(
            "SELECT status, count(*) FROM orders GROUP BY status \
             ORDER BY count(*) DESC, status ASC",
        );
        assert_eq!(out, "SELECT * FROM pgcache_mv.q_42 ORDER BY 2 DESC, 1 ASC");
    }

    // ==================== SetOp body serve SQL ====================

    #[test]
    fn test_mv_serve_sql_setop_no_order_by() {
        let out = build_serve_sql("SELECT id FROM orders UNION SELECT id FROM users");
        assert_eq!(out, "SELECT * FROM pgcache_mv.q_42");
    }

    #[test]
    fn test_mv_serve_sql_setop_order_by_identifier_deparses_directly() {
        // SetOp ORDER BY is Identifier-based; the bare name matches the MV
        // column, so we emit it directly (no positional indirection).
        let out =
            build_serve_sql("SELECT id FROM orders UNION SELECT id FROM users ORDER BY id DESC");
        assert_eq!(out, "SELECT * FROM pgcache_mv.q_42 ORDER BY id DESC");
    }

    #[test]
    fn test_mv_serve_sql_intersect_no_order_by() {
        let out = build_serve_sql("SELECT id FROM orders INTERSECT SELECT id FROM users");
        assert_eq!(out, "SELECT * FROM pgcache_mv.q_42");
    }
}
