use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::ops::ControlFlow;

use ecow::EcoString;

use super::{AstNode, QueryExpr, SelectNode, TableNode};
use crate::query::{Fingerprint, QueryKey, SessionKey};

/// Domain tags mixed into the hash after the AST so a session key and a
/// canonical fingerprint built from the same query and the same schema list
/// never share a value.
const DOMAIN_CANONICAL: u8 = 1;
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

/// The content fingerprint of a `QueryExpr` as written. This is the cache
/// identity only for a query whose table names are all schema-qualified; an
/// unqualified name makes the identity depend on resolution, see
/// [`query_expr_fingerprint_canonical`] and [`query_expr_session_key`].
pub fn query_expr_fingerprint(query: &QueryExpr) -> Fingerprint {
    Fingerprint::from_raw(query_expr_hasher(query).finish())
}

/// Whether any table reference in the query resolves through search_path.
/// CTE references are their own node type, so a bare name here is always a
/// relation.
pub fn query_expr_has_unqualified_table(query: &QueryExpr) -> bool {
    query
        .try_for_each_node::<TableNode, ()>(&mut |table| {
            if table.schema.is_none() {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })
        .is_break()
}

/// The cache identity of a query once each unqualified table name has been
/// resolved to a schema. `schema_of` is consulted for every unqualified
/// [`TableNode`] in traversal order; qualified names are hashed as written.
/// With no unqualified names this equals [`query_expr_fingerprint`], so a
/// fully qualified query gets the same fingerprint from either path.
pub fn query_expr_fingerprint_canonical(
    query: &QueryExpr,
    mut schema_of: impl FnMut(&TableNode) -> EcoString,
) -> Fingerprint {
    let mut hasher = query_expr_hasher(query);
    let mut tagged = false;
    let _ = query.try_for_each_node::<TableNode, ()>(&mut |table| {
        if table.schema.is_none() {
            if !tagged {
                hasher.write_u8(DOMAIN_CANONICAL);
                tagged = true;
            }
            schema_of(table).hash(&mut hasher);
        }
        ControlFlow::Continue(())
    });
    Fingerprint::from_raw(hasher.finish())
}

/// The proxy's key for a query under the session's resolved search_path. A
/// fully qualified query is already canonical; otherwise the path is folded
/// in and the alias map decides which cached query the pair resolved to.
pub fn query_expr_session_key(query: &QueryExpr, search_path: &[EcoString]) -> QueryKey {
    let mut hasher = query_expr_hasher(query);
    if !query_expr_has_unqualified_table(query) {
        return QueryKey::Canonical(Fingerprint::from_raw(hasher.finish()));
    }
    hasher.write_u8(DOMAIN_SESSION);
    search_path.hash(&mut hasher);
    QueryKey::Session(SessionKey::from_raw(hasher.finish()))
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
        match query_expr_session_key(&parse(sql), &path(search_path)) {
            QueryKey::Session(key) => key,
            QueryKey::Canonical(fp) => panic!("expected a session key, got canonical {fp}"),
        }
    }

    fn canonical_key(sql: &str, search_path: &[&str]) -> Fingerprint {
        match query_expr_session_key(&parse(sql), &path(search_path)) {
            QueryKey::Canonical(fp) => fp,
            QueryKey::Session(key) => panic!("expected a canonical key, got session {key}"),
        }
    }

    #[test]
    fn test_has_unqualified_table_detects_bare_relation() {
        assert!(query_expr_has_unqualified_table(&parse(
            "SELECT id FROM users WHERE id = 1"
        )));
        assert!(!query_expr_has_unqualified_table(&parse(
            "SELECT id FROM public.users WHERE id = 1"
        )));
    }

    #[test]
    fn test_has_unqualified_table_sees_through_joins_and_subqueries() {
        assert!(query_expr_has_unqualified_table(&parse(
            "SELECT u.id FROM public.users u JOIN orders o ON o.user_id = u.id"
        )));
        assert!(query_expr_has_unqualified_table(&parse(
            "SELECT id FROM public.users WHERE id IN (SELECT user_id FROM orders)"
        )));
    }

    #[test]
    fn test_has_unqualified_table_ignores_cte_references() {
        assert!(!query_expr_has_unqualified_table(&parse(
            "WITH c AS (SELECT id FROM public.users) SELECT id FROM c"
        )));
        // The CTE body itself can still carry a bare relation.
        assert!(query_expr_has_unqualified_table(&parse(
            "WITH c AS (SELECT id FROM users) SELECT id FROM c"
        )));
    }

    #[test]
    fn test_session_key_qualified_query_is_canonical_and_path_independent() {
        let sql = "SELECT id FROM public.users WHERE id = 1";
        let a = canonical_key(sql, &["alpha"]);
        let b = canonical_key(sql, &["beta", "public"]);
        assert_eq!(a, b);
        assert_eq!(a, query_expr_fingerprint(&parse(sql)));
    }

    #[test]
    fn test_session_key_unqualified_query_varies_with_path() {
        let sql = "SELECT id FROM users WHERE id = 1";
        assert_ne!(session_key(sql, &["alpha"]), session_key(sql, &["beta"]));
        assert_ne!(
            session_key(sql, &["alpha"]),
            session_key(sql, &["alpha", "public"])
        );
        assert_eq!(session_key(sql, &["alpha"]), session_key(sql, &["alpha"]));
    }

    #[test]
    fn test_session_key_cte_only_query_is_canonical() {
        let sql = "WITH c AS (SELECT id FROM public.users) SELECT id FROM c";
        assert_eq!(
            canonical_key(sql, &["alpha"]),
            canonical_key(sql, &["beta"])
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
    fn test_canonical_fingerprint_qualified_query_equals_plain_fingerprint() {
        let query = parse("SELECT id FROM public.users WHERE id = 1");
        let canonical =
            query_expr_fingerprint_canonical(&query, |_| panic!("no unqualified tables"));
        assert_eq!(canonical, query_expr_fingerprint(&query));
    }

    #[test]
    fn test_canonical_fingerprint_varies_with_resolved_schema() {
        let query = parse("SELECT id FROM users WHERE id = 1");
        let alpha = query_expr_fingerprint_canonical(&query, |_| "alpha".into());
        let beta = query_expr_fingerprint_canonical(&query, |_| "beta".into());
        assert_ne!(alpha, beta);
        assert_ne!(alpha, query_expr_fingerprint(&query));
        assert_eq!(
            alpha,
            query_expr_fingerprint_canonical(&query, |_| "alpha".into())
        );
    }

    #[test]
    fn test_canonical_fingerprint_consults_each_unqualified_table_in_order() {
        let query = parse("SELECT u.id FROM users u JOIN orders o ON o.user_id = u.id");
        let mut seen = Vec::new();
        let _ = query_expr_fingerprint_canonical(&query, |table| {
            seen.push(table.name.clone());
            "alpha".into()
        });
        assert_eq!(
            seen,
            vec![EcoString::from("users"), EcoString::from("orders")]
        );

        // Swapping which table lands in which schema changes the identity.
        let users_alpha = query_expr_fingerprint_canonical(&query, |t| {
            if t.name == "users" { "alpha" } else { "beta" }.into()
        });
        let users_beta = query_expr_fingerprint_canonical(&query, |t| {
            if t.name == "users" { "beta" } else { "alpha" }.into()
        });
        assert_ne!(users_alpha, users_beta);
    }

    #[test]
    fn test_canonical_fingerprint_ignores_qualified_tables_in_mixed_query() {
        let query = parse("SELECT u.id FROM public.users u JOIN orders o ON o.user_id = u.id");
        let mut seen = Vec::new();
        let _ = query_expr_fingerprint_canonical(&query, |table| {
            seen.push(table.name.clone());
            "alpha".into()
        });
        assert_eq!(seen, vec![EcoString::from("orders")]);
    }

    #[test]
    fn test_session_key_and_canonical_fingerprint_never_coincide() {
        let query = parse("SELECT id FROM users WHERE id = 1");
        let canonical = query_expr_fingerprint_canonical(&query, |_| "public".into());
        let session = session_key("SELECT id FROM users WHERE id = 1", &["public"]);
        assert_ne!(canonical.get(), session.get());
    }
}
