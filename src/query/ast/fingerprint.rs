use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use ecow::EcoString;

use super::{AstNode, LiteralValue, QueryExpr, SelectNode};
use crate::query::literal_settings::LiteralSettings;
use crate::query::{Fingerprint, SessionKey};

/// Domain tag mixed into a session key after the AST so it never shares a
/// value with a resolved fingerprint built from the same query.
const DOMAIN_SESSION: u8 = 2;

/// Generate a fingerprint hash for a SelectNode.
/// This is used for cache key generation.
pub fn select_node_fingerprint(node: &SelectNode) -> u64 {
    let mut hasher = DefaultHasher::new();
    node.hash(&mut hasher);
    hasher.finish()
}

/// Hash the parts of a `QueryExpr` that identify it: `ctes`, `body`,
/// `order_by`. LIMIT/OFFSET are intentionally excluded so that queries
/// differing only in LIMIT/OFFSET share a cache entry; the cache dispatch
/// tracks `max_limit` separately to decide when cached rows are sufficient.
fn query_expr_hasher(query: &QueryExpr) -> DefaultHasher {
    let mut hasher = DefaultHasher::new();
    query.ctes.hash(&mut hasher);
    query.body.hash(&mut hasher);
    query.order_by.hash(&mut hasher);
    hasher
}

/// The content fingerprint of a `QueryExpr` as written. Not a cache identity
/// (that is the resolved query's, see
/// [`resolved_query_fingerprint`](crate::query::resolved::resolved_query_fingerprint));
/// used where only the text's own identity matters, such as tests.
pub fn query_expr_fingerprint(query: &QueryExpr) -> Fingerprint {
    Fingerprint::from_raw(query_expr_hasher(query).finish())
}

/// The proxy's key for a query under the session's resolved search_path and,
/// when the query has a string literal whose meaning they could change, its
/// literal settings. The alias map maps it to the fingerprint the writer
/// resolved the triple to.
pub fn query_expr_session_key(
    query: &QueryExpr,
    search_path: &[EcoString],
    literals: &LiteralSettings,
) -> SessionKey {
    let mut hasher = query_expr_hasher(query);
    hasher.write_u8(DOMAIN_SESSION);
    search_path.hash(&mut hasher);
    if query_expr_has_string_literal(query) {
        literals.hash(&mut hasher);
    }
    SessionKey::from_raw(hasher.finish())
}

fn query_expr_has_string_literal(query: &QueryExpr) -> bool {
    query.nodes::<LiteralValue>().any(|literal| {
        matches!(
            literal,
            LiteralValue::String(_) | LiteralValue::StringWithCast(..)
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::ast::query_expr_parse;

    fn parse(sql: &str) -> QueryExpr {
        query_expr_parse(sql).expect("parse query")
    }

    fn path(schemas: &[&str]) -> Vec<EcoString> {
        schemas.iter().map(|s| EcoString::from(*s)).collect()
    }

    fn session_key(sql: &str, search_path: &[&str]) -> SessionKey {
        query_expr_session_key(&parse(sql), &path(search_path), &LiteralSettings::default())
    }

    #[test]
    fn test_session_key_folds_literal_settings_only_with_a_string_literal() {
        let tokyo = LiteralSettings::from_texts("Asia/Tokyo", "ISO, MDY", "postgres", "Default");
        let utc = LiteralSettings::default();
        let numeric = parse("SELECT id FROM users WHERE id = 1");
        assert_eq!(
            query_expr_session_key(&numeric, &path(&["public"]), &tokyo),
            query_expr_session_key(&numeric, &path(&["public"]), &utc)
        );
        let text = parse("SELECT id FROM users WHERE created_at > '2024-01-01'");
        assert_ne!(
            query_expr_session_key(&text, &path(&["public"]), &tokyo),
            query_expr_session_key(&text, &path(&["public"]), &utc)
        );
    }

    #[test]
    fn test_session_key_varies_with_path_and_text() {
        let sql = "SELECT id FROM users WHERE id = 1";
        assert_eq!(session_key(sql, &["alpha"]), session_key(sql, &["alpha"]));
        assert_ne!(session_key(sql, &["alpha"]), session_key(sql, &["beta"]));
        assert_ne!(
            session_key(sql, &["alpha"]),
            session_key(sql, &["alpha", "public"])
        );
        assert_ne!(
            session_key(sql, &["alpha"]),
            session_key("SELECT id FROM users WHERE id = 2", &["alpha"])
        );
    }

    #[test]
    fn test_session_key_excludes_limit_and_offset() {
        let base = session_key("SELECT id FROM users", &["alpha"]);
        assert_eq!(
            base,
            session_key("SELECT id FROM users LIMIT 5", &["alpha"])
        );
        assert_eq!(
            base,
            session_key("SELECT id FROM users LIMIT 5 OFFSET 2", &["alpha"])
        );
    }

    #[test]
    fn test_session_key_never_equals_the_plain_fingerprint() {
        let query = parse("SELECT id FROM users WHERE id = 1");
        assert_ne!(
            query_expr_session_key(&query, &path(&[]), &LiteralSettings::default()).get(),
            query_expr_fingerprint(&query).get()
        );
    }
}
