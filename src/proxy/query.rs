use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
    num::NonZeroUsize,
    sync::Arc,
};

use lru::LruCache;

use ecow::EcoString;
use tokio_util::bytes::BytesMut;
use tracing::{debug, trace};

use crate::{
    cache::query::{CacheableQuery, query_has_volatile_function},
    catalog::FunctionVolatility,
    id_hash::{BuildIdHasher, impl_id_hashable},
    query::ast::{
        AstError, LiteralValue, QueryBody, QueryExpr, RawStatement, ScalarExpr, SelectColumn,
        SelectColumns, statement_convert_raw,
    },
    query::write::WriteClass,
};

use super::{ParseError, cacheability_store::CacheabilityStore};

/// Name of the pseudo-function the proxy intercepts to explain a cached query
/// against the cache database (PGC-345). It is never executed as a real
/// function — the proxy recognizes the call shape and routes it to the cache.
const EXPLAIN_FUNCTION_NAME: &str = "pgcache_explain";

/// What `pgcache_explain(...)` should explain: either an inline SQL string
/// (re-parsed and fingerprinted in the cache dispatch) or a fingerprint value
/// as printed by `/status`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExplainTarget {
    Sql(String),
    Fingerprint(u64),
}

/// A recognized `pgcache_explain(<target>[, <options>])` call. `options` is the
/// verbatim EXPLAIN option list (e.g. `ANALYZE, FORMAT JSON`), empty when the
/// second argument is absent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainSpec {
    pub target: ExplainTarget,
    pub options: EcoString,
}

/// Recognize a bare `SELECT pgcache_explain(<string>[, <string>])` in an
/// already-converted [`QueryExpr`] and extract its arguments, or `None` for any
/// other statement. Runs on the AST the cacheability path already built (inside
/// [`analyze`]), so detection costs no extra parse/convert on the query hot path
/// (PGC-345).
///
/// Both arguments are string literals. The first is the target: a value that
/// parses as `u64` is a fingerprint (as printed by `/status`), otherwise it is
/// inline SQL to explain. The optional second argument is the verbatim EXPLAIN
/// option list. Fingerprints must be quoted (`pgcache_explain('12345')`): they
/// span the full `u64` range, which PostgreSQL parses as a float literal whose
/// value the shared literal converter would round to `f64`, losing precision.
fn explain_spec_extract(query: &QueryExpr) -> Option<ExplainSpec> {
    // Must be exactly the bare projection `SELECT pgcache_explain(...)`: no CTEs,
    // ORDER BY, LIMIT, FROM, WHERE, GROUP BY, HAVING, or DISTINCT — otherwise a
    // real query that merely mentions the function would be intercepted.
    if !query.ctes.is_empty() || !query.order_by.is_empty() || query.limit.is_some() {
        return None;
    }
    let QueryBody::Select(select) = &query.body else {
        return None;
    };
    if !select.from.is_empty()
        || select.where_clause.is_some()
        || !select.group_by.is_empty()
        || select.having.is_some()
        || select.distinct
    {
        return None;
    }
    let SelectColumns::Columns(columns) = &select.columns else {
        return None;
    };
    let [
        SelectColumn::Expr {
            expr: ScalarExpr::Function(func),
            ..
        },
    ] = columns.as_slice()
    else {
        return None;
    };
    // `func.name` is the last name component, so a schema-qualified
    // `x.pgcache_explain(...)` also matches. Acceptable: the name is
    // pgcache-reserved, so shadowing a real user function of that name is not a
    // concern worth carrying the raw funcname list to detect.
    if func.name != EXPLAIN_FUNCTION_NAME
        || func.agg_star
        || func.agg_distinct
        || func.over.is_some()
        || func.agg_filter.is_some()
        || !func.agg_order.is_empty()
    {
        return None;
    }

    let (first, options) = match func.args.as_slice() {
        [first] => (first, EcoString::new()),
        [first, second] => (first, EcoString::from(arg_string_extract(second)?)),
        _ => return None,
    };
    let first = arg_string_extract(first)?;
    let target = match first.parse::<u64>() {
        Ok(fingerprint) => ExplainTarget::Fingerprint(fingerprint),
        Err(_) => ExplainTarget::Sql(first.to_owned()),
    };
    Some(ExplainSpec { target, options })
}

/// Test-only: parse `sql` then run [`explain_spec_extract`]. Production
/// detection runs inside [`analyze`] on the AST it already converted, so this
/// wrapper (which parses) is never on a hot path.
#[cfg(test)]
fn explain_intercept_parse(sql: &str) -> Option<ExplainSpec> {
    let query = pg_query::parse_raw_scoped(sql, |tree| unsafe {
        crate::query::ast::query_expr_convert_raw(tree)
    })
    .ok()?
    .ok()?;
    explain_spec_extract(&query)
}

/// A plain string-literal argument's value, or `None` for any other expression
/// (a cast, number, column reference, ...). Both `pgcache_explain` arguments are
/// string literals.
fn arg_string_extract(arg: &ScalarExpr) -> Option<&str> {
    if let ScalarExpr::Literal(LiteralValue::String(value)) = arg {
        Some(value.as_str())
    } else {
        None
    }
}

/// A hash of a SQL query's *text* (not its AST). Keys the proxy's cacheability
/// memo — identical query text yields the same cacheability verdict, so the
/// parse/convert/classify work is done once. Deliberately distinct from
/// [`Fingerprint`](crate::query::Fingerprint), an AST content hash: different
/// input, different domain, not interchangeable. Already a uniformly-distributed
/// hash, so the memo uses the passthrough [`BuildIdHasher`].
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct SqlTextHash(u64);

impl_id_hashable!(SqlTextHash);

impl SqlTextHash {
    /// Hash a SQL string's text.
    pub(super) fn of(sql: &str) -> Self {
        let mut hasher = DefaultHasher::new();
        sql.hash(&mut hasher);
        Self(hasher.finish())
    }
}

/// Bounded per connection so a dynamic-SQL workload can't grow it unbounded,
/// matching the describe cache. A miss here is not a re-analysis — it falls
/// back to the shared store, which is a sharded-map lookup and an `Arc` clone
/// — so erring small costs little.
const CACHEABILITY_MEMO_CAPACITY: NonZeroUsize = NonZeroUsize::new(256).unwrap();

/// The proxy's per-connection cacheability memo: SQL text hash → the analyzed
/// verdict (cacheable AST, forward reason, or an explain interception), so the
/// parse/convert/classify work is done once per distinct query text.
///
/// Entries are cheap handles into [`CacheabilityStore`]; the payload itself is
/// interned there once for the whole process. Dropping a handle — by LRU
/// eviction, epoch drain, or the connection closing — releases the key back to
/// the store, which reclaims it if nothing else holds it.
pub(super) struct CacheabilityCache {
    entries: LruCache<SqlTextHash, Arc<Action>, BuildIdHasher<SqlTextHash>>,
    store: Arc<CacheabilityStore>,
    /// Drain epoch this memo was last reconciled against.
    epoch: u64,
}

impl CacheabilityCache {
    pub(super) fn new(store: Arc<CacheabilityStore>) -> Self {
        Self {
            entries: LruCache::with_hasher(CACHEABILITY_MEMO_CAPACITY, BuildIdHasher::default()),
            epoch: store.epoch(),
            store,
        }
    }

    /// Feed a memory-pressure sample through to the shared store. Called where
    /// the connection already holds a `CacheDispatch`, which carries the flag
    /// for the current cache generation.
    pub(in crate::proxy) fn pressure_observe(&self, pressured: bool) {
        self.store.pressure_observed(pressured);
    }

    /// Drop every handle and release the keys, so the store can reclaim any
    /// entry this memo was the last to hold.
    fn drain(&mut self) {
        let keys: Vec<SqlTextHash> = self.entries.iter().map(|(key, _)| *key).collect();
        self.entries.clear();
        for key in keys {
            self.store.release(key);
        }
    }

    /// Reconcile against a wholesale drop: clearing the store alone frees
    /// nothing while connections still hold `Arc`s, so each connection drains
    /// when it notices the epoch move.
    pub(in crate::proxy) fn epoch_reconcile(&mut self) {
        let current = self.store.epoch();
        if current != self.epoch {
            self.drain();
            self.epoch = current;
        }
    }

    /// Record a handle, releasing whatever the LRU evicted to make room.
    fn remember(&mut self, key: SqlTextHash, action: Arc<Action>) {
        if let Some((evicted_key, evicted)) = self.entries.push(key, action) {
            // Drop before releasing: the scrutinee temporary outlives the body,
            // so a handle still held here keeps the store's count above 1 and
            // the release silently does nothing.
            drop(evicted);
            self.store.release(evicted_key);
        }
    }
}

impl Drop for CacheabilityCache {
    fn drop(&mut self) {
        self.drain();
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) enum ForwardReason {
    UnsupportedStatement,
    UncacheableSelect,
    Invalid,
}

/// Map an AST conversion failure to its forward reason (metric bucket).
fn ast_error_forward_reason(ast_error: &AstError) -> ForwardReason {
    match ast_error {
        AstError::UnsupportedStatement { .. } => {
            // Not a SELECT statement (INSERT, UPDATE, DELETE, DDL, etc.)
            ForwardReason::UnsupportedStatement
        }
        AstError::UnsupportedSelectFeature { .. }
        | AstError::UnsupportedFeature { .. }
        | AstError::UnsupportedJoinType
        | AstError::UnsupportedSubLinkType { .. }
        | AstError::WhereParseError(_) => {
            debug!(%ast_error, "forwarding query: AST conversion failed");
            ForwardReason::UncacheableSelect
        }
        AstError::MultipleStatements | AstError::MissingStatement | AstError::InvalidTableRef => {
            debug!(%ast_error, "forwarding query: invalid");
            ForwardReason::Invalid
        }
    }
}

/// Verdict from [`analyze`]. Cloned cheaply from the memo on a hit (`CacheCheck`
/// and `Explain` are `Arc`s; `Forward` is `Copy`).
#[derive(Clone)]
pub(super) enum Action {
    Forward(ForwardReason),
    /// A forwarded statement that may modify table data; carries the write
    /// classification for the connection's read-after-write log (PGC-124).
    ForwardWrite(ForwardReason, WriteClass),
    CacheCheck(Arc<CacheableQuery>),
    /// `SELECT pgcache_explain(...)` — route to the cache to explain a cached
    /// query's cache-side plan rather than forward to origin (PGC-345).
    Explain(Arc<ExplainSpec>),
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(super) async fn handle_query(
    data: &BytesMut,
    cacheability_cache: &mut CacheabilityCache,
    func_volatility: &HashMap<EcoString, FunctionVolatility>,
) -> Result<Action, ParseError> {
    let query = query_sql_extract(data).ok_or(ParseError::InvalidUtf8)?;
    analyze(query, cacheability_cache, func_volatility)
}

/// Extract the SQL text from a simple-query (`Q`) frame: tag (1) + length (4) +
/// the null-terminated query string. Returns `None` on a malformed frame or
/// non-UTF-8 body.
pub(super) fn query_sql_extract(data: &BytesMut) -> Option<&str> {
    let len_bytes: [u8; 4] = data.get(1..5).and_then(|s| s.try_into().ok())?;
    let msg_len = u32::from_be_bytes(len_bytes) as usize;
    data.get(5..msg_len).and_then(|b| str::from_utf8(b).ok())
}

/// Cacheability analysis for a SQL string, memoized on a hash of the text. On a
/// hit the parse/convert/classify work is skipped entirely. Shared by the
/// simple-query and extended (Parse) paths.
///
/// Two tiers. The per-connection LRU is the hot path and needs no
/// synchronization. On a miss the shared [`CacheabilityStore`] is consulted,
/// which costs a sharded-map lookup and an `Arc` clone rather than a
/// re-analysis; only a miss in both tiers parses. The verdict is interned there
/// so every connection shares one payload.
pub(super) fn analyze(
    sql: &str,
    cacheability_cache: &mut CacheabilityCache,
    func_volatility: &HashMap<EcoString, FunctionVolatility>,
) -> Result<Action, ParseError> {
    cacheability_cache.epoch_reconcile();

    let key = SqlTextHash::of(sql);

    if let Some(action) = cacheability_cache.entries.get(&key) {
        trace!("cacheability memo hit");
        return Ok((**action).clone());
    }

    if let Some(action) = cacheability_cache.store.get(key) {
        trace!("cacheability store hit");
        let verdict = (*action).clone();
        cacheability_cache.remember(key, action);
        return Ok(verdict);
    }

    // Build the QueryExpr straight off the raw parse tree, skipping the protobuf
    // serialize/decode round-trip (PGC-192). This is the only parse/convert; the
    // explain interception, cacheability classification, and write
    // classification (PGC-124) all read this one pass.
    let convert_result =
        pg_query::parse_raw_scoped(sql, |tree| unsafe { statement_convert_raw(tree) })?;

    let action = match convert_result {
        Ok(RawStatement::Select {
            converted: Ok(query),
            ..
        }) => {
            // `SELECT pgcache_explain(...)` is intercepted before cacheability
            // classification (it would otherwise be an uncacheable unknown
            // function) and routed to the cache to explain a cached plan.
            if let Some(spec) = explain_spec_extract(&query) {
                Action::Explain(Arc::new(spec))
            } else {
                match CacheableQuery::cacheable(&query, func_volatility) {
                    // `cacheable` just passed, so `try_new` (same validation)
                    // cannot fail; forward conservatively if it ever disagrees.
                    Ok(()) => match CacheableQuery::try_new(*query, func_volatility) {
                        Ok(cacheable_query) => Action::CacheCheck(Arc::new(cacheable_query)),
                        Err(_) => Action::Forward(ForwardReason::UncacheableSelect),
                    },
                    Err(cacheability_error) => {
                        debug!(%cacheability_error, "uncacheable SELECT");
                        // A volatile (or unknown) function anywhere in the query
                        // can modify the database, so an uncacheable SELECT
                        // carrying one is a potential write (PGC-124). Scanned
                        // only now that the query is known uncacheable — the
                        // cacheable path never pays for it.
                        if query_has_volatile_function(&query, func_volatility) {
                            Action::ForwardWrite(
                                ForwardReason::UncacheableSelect,
                                WriteClass::Connection,
                            )
                        } else {
                            Action::Forward(ForwardReason::UncacheableSelect)
                        }
                    }
                }
            }
        }
        Ok(RawStatement::Select {
            converted: Err(ast_error),
            cte_write,
        }) => {
            let reason = ast_error_forward_reason(&ast_error);
            match cte_write {
                // A data-modifying CTE: the "select" writes.
                Some(class) => Action::ForwardWrite(reason, class),
                None => Action::Forward(reason),
            }
        }
        Ok(RawStatement::Write(class)) => {
            Action::ForwardWrite(ForwardReason::UnsupportedStatement, class)
        }
        Ok(RawStatement::ReadOnlyUtility { .. }) => {
            Action::Forward(ForwardReason::UnsupportedStatement)
        }
        Err(ast_error) => {
            let reason = ast_error_forward_reason(&ast_error);
            // Any statement of a multi-statement batch can write.
            if matches!(ast_error, AstError::MultipleStatements) {
                Action::ForwardWrite(reason, WriteClass::Connection)
            } else {
                Action::Forward(reason)
            }
        }
    };

    // Don't memoize writes: DML text is almost always unique (fresh literals
    // per statement), so a write entry would rarely be re-hit, and its class
    // can carry a per-statement payload (INSERT rows). Interning them would
    // grow the per-connection memo and the shared store with data they never
    // serve. Reads intern as before.
    if matches!(action, Action::ForwardWrite(..)) {
        return Ok(action);
    }
    let interned = cacheability_cache.store.intern(key, action);
    let verdict = (*interned).clone();
    cacheability_cache.remember(key, interned);
    Ok(verdict)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::wildcard_enum_match_arm)]

    use super::*;

    use std::sync::Arc;

    use crate::proxy::cacheability_store::CacheabilityStore;

    fn empty_volatility() -> HashMap<EcoString, FunctionVolatility> {
        HashMap::new()
    }

    /// Two connections analyzing the same text must share one interned payload.
    /// This is the defect behind GitHub issue #4: previously each connection
    /// called `Arc::new` on its own analysis, so N connections retained N ASTs.
    #[test]
    fn test_repeat_text_across_connections_interns_once() {
        let store = Arc::new(CacheabilityStore::new());
        let fv = empty_volatility();
        let sql = "SELECT a FROM t WHERE id = 1";

        let mut first = CacheabilityCache::new(Arc::clone(&store));
        let mut second = CacheabilityCache::new(Arc::clone(&store));

        let a = analyze(sql, &mut first, &fv).expect("analyze on the first connection");
        let b = analyze(sql, &mut second, &fv).expect("analyze on the second connection");

        let (Action::CacheCheck(a), Action::CacheCheck(b)) = (a, b) else {
            panic!("expected a cacheable verdict for a plain SELECT");
        };
        assert!(
            Arc::ptr_eq(&a, &b),
            "both connections must share one interned payload"
        );
    }

    /// Distinct texts must not accumulate past the per-connection bound, and
    /// what falls out must be released so the store can reclaim it. Without a
    /// bound a pooled connection accumulates every text it ever sees.
    #[test]
    fn test_memo_is_bounded_and_releases_on_eviction() {
        let store = Arc::new(CacheabilityStore::new());
        let fv = empty_volatility();
        let mut memo = CacheabilityCache::new(Arc::clone(&store));

        let overshoot = CACHEABILITY_MEMO_CAPACITY.get() * 2;
        for i in 0..overshoot {
            let sql = format!("SELECT a FROM t WHERE id = {i}");
            analyze(&sql, &mut memo, &fv).expect("analyze a distinct literal");
        }

        assert_eq!(
            memo.entries.len(),
            CACHEABILITY_MEMO_CAPACITY.get(),
            "per-connection memo must stay at its cap"
        );
        assert_eq!(
            store.len(),
            CACHEABILITY_MEMO_CAPACITY.get(),
            "evicted texts must be released, not retained by the store"
        );
    }

    /// Closing a connection releases everything it held, so nothing survives a
    /// connection's lifetime unless another connection still wants it.
    #[test]
    fn test_connection_close_reclaims_its_entries() {
        let store = Arc::new(CacheabilityStore::new());
        let fv = empty_volatility();

        let mut memo = CacheabilityCache::new(Arc::clone(&store));
        analyze("SELECT a FROM t WHERE id = 1", &mut memo, &fv).expect("analyze");
        analyze("SELECT a FROM t WHERE id = 2", &mut memo, &fv).expect("analyze");
        assert_eq!(store.len(), 2);

        drop(memo);
        assert_eq!(
            store.len(),
            0,
            "closing the connection reclaims its entries"
        );
    }

    /// A text two connections share survives one of them closing.
    #[test]
    fn test_shared_entry_survives_one_connection_closing() {
        let store = Arc::new(CacheabilityStore::new());
        let fv = empty_volatility();
        let sql = "SELECT a FROM t WHERE id = 1";

        let mut first = CacheabilityCache::new(Arc::clone(&store));
        let mut second = CacheabilityCache::new(Arc::clone(&store));
        analyze(sql, &mut first, &fv).expect("analyze");
        analyze(sql, &mut second, &fv).expect("analyze");

        drop(first);
        assert_eq!(store.len(), 1, "the other connection still references it");
        drop(second);
        assert_eq!(store.len(), 0);
    }

    /// A pressure drop must actually free: clearing the store alone frees
    /// nothing while connections still hold handles, so connections drain on
    /// the epoch move.
    #[test]
    fn test_pressure_drop_drains_connection_memos() {
        let store = Arc::new(CacheabilityStore::new());
        let fv = empty_volatility();
        let mut memo = CacheabilityCache::new(Arc::clone(&store));
        analyze("SELECT a FROM t WHERE id = 1", &mut memo, &fv).expect("analyze");

        store.pressure_observed(true);
        assert_eq!(store.len(), 0, "store cleared");
        assert_eq!(memo.entries.len(), 1, "connection has not noticed yet");

        // Next query on that connection reconciles the epoch and drains.
        analyze("SELECT a FROM t WHERE id = 2", &mut memo, &fv).expect("analyze");
        assert_eq!(memo.entries.len(), 1, "drained, then re-populated with one");
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_explain_intercept_parse_sql_argument() {
        let spec = explain_intercept_parse("SELECT pgcache_explain('SELECT id FROM orders')")
            .expect("detect SQL-mode explain");
        assert_eq!(
            spec,
            ExplainSpec {
                target: ExplainTarget::Sql("SELECT id FROM orders".to_owned()),
                options: EcoString::new(),
            }
        );
    }

    #[test]
    fn test_explain_intercept_parse_fingerprint_argument() {
        let spec = explain_intercept_parse("SELECT pgcache_explain('12345')")
            .expect("detect fingerprint-mode explain");
        assert_eq!(spec.target, ExplainTarget::Fingerprint(12345));
    }

    #[test]
    fn test_explain_intercept_parse_fingerprint_above_i64_max() {
        // Fingerprints span the full u64 range. Quoted, the digits survive as a
        // string literal and parse losslessly — unlike an unquoted literal this
        // large, which PostgreSQL parses as a float the AST rounds to f64.
        let big = u64::MAX;
        let spec = explain_intercept_parse(&format!("SELECT pgcache_explain('{big}')"))
            .expect("detect large fingerprint");
        assert_eq!(spec.target, ExplainTarget::Fingerprint(big));
    }

    #[test]
    fn test_explain_intercept_parse_unquoted_number_is_not_fingerprint() {
        // A fingerprint must be quoted; an unquoted numeric argument is not a
        // string literal, so it is not intercepted (falls through to normal
        // handling rather than being explained against a rounded value).
        assert!(explain_intercept_parse("SELECT pgcache_explain(12345)").is_none());
    }

    #[test]
    fn test_explain_intercept_parse_with_options() {
        let spec =
            explain_intercept_parse("SELECT pgcache_explain('SELECT 1', 'ANALYZE, FORMAT JSON')")
                .expect("detect explain with options");
        assert_eq!(spec.target, ExplainTarget::Sql("SELECT 1".to_owned()));
        assert_eq!(spec.options, "ANALYZE, FORMAT JSON");
    }

    #[test]
    fn test_explain_intercept_parse_rejects_non_explain() {
        assert!(explain_intercept_parse("SELECT 1").is_none());
        assert!(explain_intercept_parse("SELECT id FROM orders").is_none());
        // A different function call must not be intercepted.
        assert!(explain_intercept_parse("SELECT now()").is_none());
        // The pseudo-function projected over a table is not the bare call form.
        assert!(explain_intercept_parse("SELECT pgcache_explain('x') FROM orders").is_none());
        // Too many arguments.
        assert!(explain_intercept_parse("SELECT pgcache_explain('x', 'y', 'z')").is_none());
        assert!(explain_intercept_parse("INSERT INTO t VALUES (1)").is_none());
        assert!(explain_intercept_parse("not even sql").is_none());
    }

    fn analyze_fresh(sql: &str, fv: &HashMap<EcoString, FunctionVolatility>) -> Action {
        let store = Arc::new(CacheabilityStore::new());
        analyze(sql, &mut CacheabilityCache::new(store), fv).expect("analyze sql")
    }

    #[test]
    fn test_analyze_dml_classifies_as_write() {
        let fv = HashMap::new();
        match analyze_fresh("INSERT INTO t (a) VALUES (1)", &fv) {
            Action::ForwardWrite(
                ForwardReason::UnsupportedStatement,
                WriteClass::InsertRows(_),
            ) => {}
            other => panic!(
                "expected InsertRows write, got {:?}",
                discriminant_name(&other)
            ),
        }
        match analyze_fresh("UPDATE t SET a = 1", &fv) {
            Action::ForwardWrite(ForwardReason::UnsupportedStatement, WriteClass::Table(_)) => {}
            other => panic!("expected Table write, got {:?}", discriminant_name(&other)),
        }
        match analyze_fresh("DELETE FROM t WHERE id = 1", &fv) {
            Action::ForwardWrite(
                ForwardReason::UnsupportedStatement,
                WriteClass::DeleteRows(_),
            ) => {}
            other => panic!(
                "expected DeleteRows write, got {:?}",
                discriminant_name(&other)
            ),
        }
    }

    #[test]
    fn test_analyze_txn_control_is_plain_forward() {
        let fv = HashMap::new();
        for sql in ["BEGIN", "COMMIT", "SET search_path TO public"] {
            assert!(
                matches!(
                    analyze_fresh(sql, &fv),
                    Action::Forward(ForwardReason::UnsupportedStatement)
                ),
                "for {sql:?}"
            );
        }
    }

    #[test]
    fn test_analyze_volatile_where_function_is_a_write() {
        // Unknown and volatile functions in WHERE can modify the database;
        // stable ones cannot (PGC-124).
        let mut fv = HashMap::new();
        fv.insert(EcoString::from("f_volatile"), FunctionVolatility::Volatile);
        fv.insert(EcoString::from("f_stable"), FunctionVolatility::Stable);

        for sql in [
            "SELECT * FROM t WHERE f_volatile(a) = 1",
            "SELECT * FROM t WHERE f_unknown(a) = 1",
        ] {
            assert!(
                matches!(
                    analyze_fresh(sql, &fv),
                    Action::ForwardWrite(ForwardReason::UncacheableSelect, WriteClass::Connection)
                ),
                "for {sql:?}"
            );
        }
        assert!(matches!(
            analyze_fresh("SELECT * FROM t WHERE f_stable(a) = 1", &fv),
            Action::Forward(ForwardReason::UncacheableSelect)
        ));
    }

    #[test]
    fn test_analyze_volatile_nested_in_stable_is_a_write() {
        // A volatile nested inside a stable function still executes and can
        // modify the DB. The cacheability check short-circuits on the outer
        // stable, so write detection must walk the whole tree independently.
        let mut fv = HashMap::new();
        fv.insert(EcoString::from("f_volatile"), FunctionVolatility::Volatile);
        fv.insert(EcoString::from("f_stable"), FunctionVolatility::Stable);

        assert!(
            matches!(
                analyze_fresh("SELECT * FROM t WHERE f_stable(f_volatile(a)) = 1", &fv),
                Action::ForwardWrite(ForwardReason::UncacheableSelect, WriteClass::Connection)
            ),
            "stable wrapping volatile must be a write"
        );
        // Stable wrapping stable is still just a read.
        assert!(matches!(
            analyze_fresh("SELECT * FROM t WHERE f_stable(f_stable(a)) = 1", &fv),
            Action::Forward(ForwardReason::UncacheableSelect)
        ));
    }

    #[test]
    fn test_analyze_multi_statement_is_a_write() {
        assert!(matches!(
            analyze_fresh("SELECT 1; SELECT 2", &HashMap::new()),
            Action::ForwardWrite(ForwardReason::Invalid, WriteClass::Connection)
        ));
    }

    #[test]
    fn test_writes_are_not_memoized() {
        let fv = HashMap::new();
        let mut cache = CacheabilityCache::new(Arc::new(CacheabilityStore::new()));
        // A write is classified but left out of the memo (DML text is ~unique
        // and its class can carry a row payload).
        assert!(matches!(
            analyze("INSERT INTO t (a) VALUES (1)", &mut cache, &fv),
            Ok(Action::ForwardWrite(..))
        ));
        assert!(cache.entries.is_empty(), "writes must not be memoized");

        // A read verdict is memoized as before.
        assert!(matches!(
            analyze("SELECT * FROM t WHERE a = 1", &mut cache, &fv),
            Ok(Action::CacheCheck(_))
        ));
        assert_eq!(cache.entries.len(), 1);
    }

    fn discriminant_name(action: &Action) -> &'static str {
        match action {
            Action::Forward(_) => "Forward",
            Action::ForwardWrite(..) => "ForwardWrite",
            Action::CacheCheck(_) => "CacheCheck",
            Action::Explain(_) => "Explain",
        }
    }
}
