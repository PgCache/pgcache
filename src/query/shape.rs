//! Query shape: a bound query's *parameterized* form, used to share one
//! prepared statement per shape on the cache serve path instead of one per
//! literal (PGC-294).
//!
//! Two queries that differ only in their literal values have the same shape, so
//! the serve-side prepared-statement cache can key on [`ShapeKey`] rather than
//! the per-literal `Fingerprint`. The shape is rendered from the bound query —
//! the same form its serve SQL is — with its predicate literals replaced by
//! `$N` placeholders (`ResolvedQuery::shape`). It is **additive**: the
//! per-literal fingerprint that keys the cache model is unchanged.

use ecow::EcoString;

use crate::query::ast::LiteralValue;

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

/// A bound query's parameterized shape: the prepared-statement text, its key,
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
