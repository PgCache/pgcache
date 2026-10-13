use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use ecow::EcoString;

use super::rope::{NodeKind, QueryTree};
use super::{LiteralValue, QueryExpr, SelectNode};
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
/// (that is the bound query's, `ResolvedQuery::fingerprint`); used where
/// only the text's own identity matters, such as tests.
pub fn query_expr_fingerprint(query: &QueryExpr) -> Fingerprint {
    Fingerprint::from_raw(query_expr_hasher(query).finish())
}

/// The proxy's key for a query under the session's resolved search_path and,
/// when the query has a string literal whose meaning they could change, its
/// literal settings. The alias map maps it to the fingerprint the writer
/// resolved the triple to. The root LIMIT/OFFSET are excluded, so queries
/// differing only there share an entry; dispatch tracks `max_limit` apart.
pub fn query_tree_session_key(
    tree: &QueryTree,
    search_path: &[EcoString],
    literals: &LiteralSettings,
) -> SessionKey {
    let mut hasher = DefaultHasher::new();
    tree.session_hash(&mut hasher);
    hasher.write_u8(DOMAIN_SESSION);
    search_path.hash(&mut hasher);
    if query_tree_has_string_literal(tree) {
        literals.hash(&mut hasher);
    }
    SessionKey::from_raw(hasher.finish())
}

/// The key of a parameterized read before substitution: the tree with its
/// placeholders, then the bind values as sent. The literal settings always
/// fold in, since a bound value may be a string whose meaning they decide.
pub fn query_tree_session_key_with_binds(
    tree: &QueryTree,
    binds: &impl Hash,
    search_path: &[EcoString],
    literals: &LiteralSettings,
) -> SessionKey {
    let mut hasher = DefaultHasher::new();
    tree.session_hash(&mut hasher);
    hasher.write_u8(DOMAIN_SESSION);
    search_path.hash(&mut hasher);
    binds.hash(&mut hasher);
    literals.hash(&mut hasher);
    SessionKey::from_raw(hasher.finish())
}

fn query_tree_has_string_literal(tree: &QueryTree) -> bool {
    tree.nodes().any(|node| {
        matches!(
            node.kind,
            NodeKind::Literal(LiteralValue::String(_) | LiteralValue::StringWithCast(..))
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::ast::{query_expr_parse, query_tree_parse};

    fn parse(sql: &str) -> QueryTree {
        query_tree_parse(sql).expect("parse query")
    }

    fn path(schemas: &[&str]) -> Vec<EcoString> {
        schemas.iter().map(|s| EcoString::from(*s)).collect()
    }

    fn session_key(sql: &str, search_path: &[&str]) -> SessionKey {
        query_tree_session_key(&parse(sql), &path(search_path), &LiteralSettings::default())
    }

    #[test]
    fn test_session_key_folds_literal_settings_only_with_a_string_literal() {
        let tokyo = LiteralSettings::from_texts("Asia/Tokyo", "ISO, MDY", "postgres", "Default");
        let utc = LiteralSettings::default();
        let numeric = parse("SELECT id FROM users WHERE id = 1");
        assert_eq!(
            query_tree_session_key(&numeric, &path(&["public"]), &tokyo),
            query_tree_session_key(&numeric, &path(&["public"]), &utc)
        );
        let text = parse("SELECT id FROM users WHERE created_at > '2024-01-01'");
        assert_ne!(
            query_tree_session_key(&text, &path(&["public"]), &tokyo),
            query_tree_session_key(&text, &path(&["public"]), &utc)
        );
    }

    #[test]
    fn test_session_key_with_binds_keys_on_values_and_settings() {
        let query = parse("SELECT id FROM users WHERE id = $1");
        let utc = LiteralSettings::default();
        let tokyo = LiteralSettings::from_texts("Asia/Tokyo", "ISO, MDY", "postgres", "Default");
        let key = |binds: &[u8], literals: &LiteralSettings| {
            query_tree_session_key_with_binds(&query, &binds, &path(&["public"]), literals)
        };
        assert_eq!(key(b"1", &utc), key(b"1", &utc));
        assert_ne!(key(b"1", &utc), key(b"2", &utc));
        assert_ne!(key(b"1", &utc), key(b"1", &tokyo));
        assert_ne!(
            key(b"1", &utc),
            query_tree_session_key(&query, &path(&["public"]), &utc)
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
        let owned = query_expr_parse("SELECT id FROM users WHERE id = 1").expect("parse query");
        let tree = QueryTree::from_query_expr(&owned);
        assert_ne!(
            query_tree_session_key(&tree, &path(&[]), &LiteralSettings::default()).get(),
            query_expr_fingerprint(&owned).get()
        );
    }
}
