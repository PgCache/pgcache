//! The `SELECT pgcache_explain(...)` interception (PGC-345): recognising the
//! pseudo-function's bare call shape in an already-converted query.

use ecow::EcoString;

use crate::query::ast::LiteralValue;
use crate::query::ast::rope::{Body, Function, QueryTree, Scalar, ScalarKind, Select, SelectItem};

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
pub(super) fn explain_spec_extract(tree: &QueryTree) -> Option<ExplainSpec> {
    let select = bare_select(tree)?;
    let function = single_function_item(select)?;
    // `name()` is the last name component, so a schema-qualified
    // `x.pgcache_explain(...)` also matches.
    if function.name() != EXPLAIN_FUNCTION_NAME || !plain_call(function) {
        return None;
    }
    explain_spec_build(function)
}

/// The SELECT of a bare projection: no CTEs, ORDER BY, LIMIT, FROM, WHERE,
/// GROUP BY, HAVING, or DISTINCT, otherwise a real query that merely mentions
/// the function would be intercepted.
fn bare_select(tree: &QueryTree) -> Option<Select<'_>> {
    let root = tree.root();
    let unordered = root.order_by().len() == 0 && !root.has_limit();
    if root.ctes().len() > 0 || !unordered {
        return None;
    }
    let Body::Select(select) = root.body() else {
        return None;
    };
    select_is_bare(select).then_some(select)
}

fn select_is_bare(select: Select<'_>) -> bool {
    let unfiltered = select.from().len() == 0 && select.where_clause().is_none();
    let ungrouped = select.group_by().len() == 0 && select.having().is_none();
    unfiltered && ungrouped && !select.distinct()
}

/// The function call a SELECT projects as its only item.
fn single_function_item(select: Select<'_>) -> Option<Function<'_>> {
    let mut items = select.items()?;
    let (Some(SelectItem::Expr(item)), None) = (items.next(), items.next()) else {
        return None;
    };
    let ScalarKind::Function(function) = item.expr().kind() else {
        return None;
    };
    Some(function)
}

/// The spec from `pgcache_explain`'s one or two string arguments: a target
/// that parses as `u64` is a fingerprint, otherwise inline SQL; the second is
/// the option list.
fn explain_spec_build(function: Function<'_>) -> Option<ExplainSpec> {
    let mut args = function.args();
    let (first, options) = match (args.next(), args.next(), args.next()) {
        (Some(first), None, _) => (first, EcoString::new()),
        (Some(first), Some(second), None) => (first, EcoString::from(arg_string(second)?)),
        _ => return None,
    };
    let first = arg_string(first)?;
    let target = match first.parse::<u64>() {
        Ok(fingerprint) => ExplainTarget::Fingerprint(fingerprint),
        Err(_) => ExplainTarget::Sql(first.to_owned()),
    };
    Some(ExplainSpec { target, options })
}

/// A plain call: not an aggregate (`*`, DISTINCT, FILTER, ORDER BY) or window
/// (OVER) form.
fn plain_call(function: Function<'_>) -> bool {
    let no_aggregate_form = !function.agg_star() && !function.agg_distinct();
    let unfiltered = function.over().is_none() && function.filter().is_none();
    let unordered = unfiltered && function.agg_order().len() == 0;
    no_aggregate_form && unordered
}

/// A plain string-literal argument's value, or `None` for any other expression
/// (a cast, number, column reference, ...). Both `pgcache_explain` arguments are
/// string literals.
fn arg_string(arg: Scalar<'_>) -> Option<&str> {
    if let Some(LiteralValue::String(value)) = arg.literal() {
        Some(value.as_str())
    } else {
        None
    }
}

/// Test-only: parse `sql` then run [`explain_spec_extract`]. Production
/// detection runs inside [`analyze`] on the AST it already converted, so this
/// wrapper (which parses) is never on a hot path.
#[cfg(test)]
fn explain_intercept_parse(sql: &str) -> Option<ExplainSpec> {
    let query = pg_query::parse_raw_scoped(sql, |tree| unsafe {
        crate::query::ast::query_tree_convert_raw(tree, sql.len())
    })
    .ok()?
    .ok()?;
    explain_spec_extract(&query)
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
        let not_intercepted = [
            "SELECT 1",
            "SELECT id FROM orders",
            // A different function call must not be intercepted.
            "SELECT now()",
            // The pseudo-function projected over a table is not the bare call form.
            "SELECT pgcache_explain('x') FROM orders",
            // Too many arguments.
            "SELECT pgcache_explain('x', 'y', 'z')",
            "INSERT INTO t VALUES (1)",
            "not even sql",
        ];
        for sql in not_intercepted {
            assert!(explain_intercept_parse(sql).is_none(), "{sql}");
        }
    }
}
