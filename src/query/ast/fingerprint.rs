use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use super::{QueryExpr, SelectNode};
use crate::query::Fingerprint;

/// Generate a fingerprint hash for a SelectNode.
/// This is used for cache key generation.
pub fn select_node_fingerprint(node: &SelectNode) -> u64 {
    let mut hasher = DefaultHasher::new();
    node.hash(&mut hasher);
    hasher.finish()
}

/// Generate a fingerprint hash for a QueryExpr.
/// This is used for cache key generation.
///
/// Intentionally excludes LIMIT/OFFSET so that queries differing only
/// in LIMIT/OFFSET share a cache entry. The cache dispatch tracks
/// `max_limit` separately to decide when cached rows are sufficient.
pub fn query_expr_fingerprint(query: &QueryExpr) -> Fingerprint {
    let mut hasher = DefaultHasher::new();
    query.ctes.hash(&mut hasher);
    query.body.hash(&mut hasher);
    query.order_by.hash(&mut hasher);
    Fingerprint::from_raw(hasher.finish())
}
