//! Query shape: a resolved query's *parameterized* form, used to share one
//! prepared statement per shape on the cache serve path instead of one per
//! literal (PGC-294).
//!
//! Two queries that differ only in their literal values have the same shape, so
//! the serve-side prepared-statement cache can key on [`ShapeKey`] rather than
//! the per-literal `Fingerprint`. The shape is derived from the resolved,
//! schema-qualified AST — the same form `deparsed_sql` is produced from — by
//! replacing literals with `$N` placeholders. It is **additive**: the per-literal
//! fingerprint that keys the cache model is unchanged.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use ecow::EcoString;

use crate::query::ast::{Deparse, LiteralValue};
use crate::query::resolved::ResolvedQueryExpr;
use crate::query::transform::resolved_query_expr_parameterize;

/// Hash of a query shape — the serve prepared-statement cache key. Distinct from
/// `Fingerprint` (which keys the per-literal cache model) so the two can't be
/// confused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ShapeKey(u64);

impl ShapeKey {
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    pub const fn raw(self) -> u64 {
        self.0
    }
}

/// A resolved query's parameterized shape: the prepared-statement text, its key,
/// and the literals to bind per serve.
#[derive(Debug, Clone)]
pub struct QueryShape {
    /// Parameterized SQL body, with `$1..$k` placeholders — the prepared-
    /// statement text. Queries differing only in literals produce identical SQL.
    pub sql: EcoString,
    /// Hash of `sql`. Shared across all queries with this shape.
    pub key: ShapeKey,
    /// The literals replaced by `$1..$k`, in placeholder order — the per-serve
    /// bind values (`literals[N-1]` binds to `$N`).
    pub literals: Vec<LiteralValue>,
}

/// Derive a resolved query's shape: parameterize its literals, deparse the
/// parameterized form into the shape SQL, and hash that SQL into a [`ShapeKey`].
/// The top-level LIMIT is excluded (the serve path appends it as separate
/// `$`-params, after the shape body).
pub fn query_shape_derive(resolved: &ResolvedQueryExpr) -> QueryShape {
    let (shaped, literals) = resolved_query_expr_parameterize(resolved);
    let mut sql = String::new();
    shaped.deparse(&mut sql);

    let mut hasher = DefaultHasher::new();
    sql.hash(&mut hasher);
    let key = ShapeKey::from_raw(hasher.finish());

    QueryShape {
        sql: sql.into(),
        key,
        literals,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnMetadata, ColumnStore, Oid, TableMetadata};
    use crate::query::ast::query_expr_parse;
    use crate::query::resolve::query_expr_resolve;
    use iddqd::BiHashMap;
    use tokio_postgres::types::Type;

    fn users_table() -> TableMetadata {
        let columns = ColumnStore::new([
            ColumnMetadata {
                name: "id".into(),
                position: 1,
                type_oid: 23,
                data_type: Type::INT4,
                type_name: "int4".into(),
                cache_type_name: "int4".into(),
                is_primary_key: true,
            },
            ColumnMetadata {
                name: "name".into(),
                position: 2,
                type_oid: 25,
                data_type: Type::TEXT,
                type_name: "text".into(),
                cache_type_name: "text".into(),
                is_primary_key: false,
            },
        ]);
        TableMetadata {
            replica_identity_full: false,
            relation_oid: Oid::from_raw(1001),
            name: "users".into(),
            schema: "public".into(),
            primary_key_columns: vec!["id".into()],
            columns,
            indexes: Vec::new(),
        }
    }

    fn resolve(sql: &str) -> ResolvedQueryExpr {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(users_table());
        let query_expr = query_expr_parse(sql).expect("parse");
        query_expr_resolve(&query_expr, &tables, &["public"]).expect("resolve")
    }

    #[test]
    fn test_shape_carries_placeholders_and_binds() {
        let shape = query_shape_derive(&resolve("SELECT id, name FROM users WHERE id = 42"));
        assert!(shape.sql.contains("$1"), "shape sql: {}", shape.sql);
        assert!(!shape.sql.contains("42"));
        assert_eq!(shape.literals, vec![LiteralValue::Integer(42)]);
    }

    /// Queries differing only in their literal share one shape SQL and key.
    #[test]
    fn test_shape_shared_across_literals() {
        let a = query_shape_derive(&resolve("SELECT id FROM users WHERE name = 'user1'"));
        let b = query_shape_derive(&resolve("SELECT id FROM users WHERE name = 'user2'"));
        assert_eq!(a.sql, b.sql);
        assert_eq!(a.key, b.key);
        // ...but the per-serve binds differ.
        assert_eq!(a.literals, vec![LiteralValue::String("user1".into())]);
        assert_eq!(b.literals, vec![LiteralValue::String("user2".into())]);
    }

    /// Different shapes (different predicate columns) get different keys.
    #[test]
    fn test_shape_distinguishes_different_shapes() {
        let by_id = query_shape_derive(&resolve("SELECT id FROM users WHERE id = 1"));
        let by_name = query_shape_derive(&resolve("SELECT id FROM users WHERE name = 'x'"));
        assert_ne!(by_id.key, by_name.key);
        assert_ne!(by_id.sql, by_name.sql);
    }
}
