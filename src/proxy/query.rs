use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
    sync::Arc,
};

use ecow::EcoString;
use tokio_util::bytes::BytesMut;
use tracing::{debug, trace};

use crate::{
    cache::query::CacheableQuery,
    catalog::FunctionVolatility,
    id_hash::{BuildIdHasher, impl_id_hashable},
    query::ast::{
        AstError, LiteralValue, QueryBody, QueryExpr, ScalarExpr, SelectColumn, SelectColumns,
        query_expr_convert_raw,
    },
};

use super::ParseError;

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
    let query = pg_query::parse_raw_scoped(sql, |tree| unsafe { query_expr_convert_raw(tree) })
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

/// `HashMap` keyed by `SqlTextHash` with the passthrough hasher (key is already
/// a hash) — parallels the `FingerprintMap` aliases so the identity hasher and
/// its key type travel together.
pub(super) type SqlTextHashMap<V> = HashMap<SqlTextHash, V, BuildIdHasher<SqlTextHash>>;

/// The proxy's per-connection cacheability memo: SQL text hash → the analyzed
/// verdict (cacheable AST, forward reason, or an explain interception), so the
/// parse/convert/classify work is done once per distinct query text.
pub(super) type CacheabilityCache = SqlTextHashMap<Action>;

#[derive(Debug, Clone, Copy)]
pub(super) enum ForwardReason {
    UnsupportedStatement,
    UncacheableSelect,
    Invalid,
}

/// Verdict from [`analyze`]. Cloned cheaply from the memo on a hit (`CacheCheck`
/// and `Explain` are `Arc`s; `Forward` is `Copy`).
#[derive(Clone)]
pub(super) enum Action {
    Forward(ForwardReason),
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

/// Cacheability analysis for a SQL string, memoized in `cacheability_cache` keyed on a
/// hash of the text. On a hit the parse/convert/classify work is skipped
/// entirely; on a miss the result (cacheable AST or forward reason) is cached.
/// Shared by the simple-query and extended (Parse) paths.
pub(super) fn analyze(
    sql: &str,
    cacheability_cache: &mut CacheabilityCache,
    func_volatility: &HashMap<EcoString, FunctionVolatility>,
) -> Result<Action, ParseError> {
    let key = SqlTextHash::of(sql);

    if let Some(action) = cacheability_cache.get(&key) {
        trace!("cacheability memo hit");
        return Ok(action.clone());
    }

    // Build the QueryExpr straight off the raw parse tree, skipping the protobuf
    // serialize/decode round-trip (PGC-192). This is the only parse/convert; the
    // explain interception and cacheability classification both read this AST.
    let convert_result =
        pg_query::parse_raw_scoped(sql, |tree| unsafe { query_expr_convert_raw(tree) })?;

    let action = match convert_result {
        Ok(query) => {
            // `SELECT pgcache_explain(...)` is intercepted before cacheability
            // classification (it would otherwise be an uncacheable unknown
            // function) and routed to the cache to explain a cached plan.
            if let Some(spec) = explain_spec_extract(&query) {
                Action::Explain(Arc::new(spec))
            } else {
                match CacheableQuery::try_new(query, func_volatility) {
                    Ok(cacheable_query) => Action::CacheCheck(Arc::new(cacheable_query)),
                    Err(cacheability_error) => {
                        debug!(%cacheability_error, "uncacheable SELECT");
                        Action::Forward(ForwardReason::UncacheableSelect)
                    }
                }
            }
        }
        Err(ast_error) => {
            let reason = match &ast_error {
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
                AstError::MultipleStatements
                | AstError::MissingStatement
                | AstError::InvalidTableRef => {
                    debug!(%ast_error, "forwarding query: invalid");
                    ForwardReason::Invalid
                }
            };
            Action::Forward(reason)
        }
    };

    cacheability_cache.insert(key, action.clone());
    Ok(action)
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
