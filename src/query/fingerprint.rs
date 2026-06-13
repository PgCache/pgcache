use std::fmt;

use serde::{Deserialize, Serialize};

/// A query fingerprint: a content hash of a `QueryExpr` (excluding LIMIT/OFFSET)
/// produced by [`query_expr_fingerprint`](super::ast::query_expr_fingerprint).
/// It is the cache's identity for a query shape and the key of every
/// per-query map.
///
/// A newtype over `u64` for type safety — fingerprints share `u64`'s layout
/// with generations, LSNs, and relation oids, and the compiler otherwise can't
/// stop them being mixed. Construction is the explicit, greppable
/// [`Fingerprint::from_raw`]; there is deliberately no `From<u64>` or `Deref`,
/// so every crossing between a raw `u64` and a `Fingerprint` is intentional.
///
/// `#[repr(transparent)]` with a single-field `Hash`, so a passthrough hasher
/// over a `Fingerprint`-keyed map sees exactly the underlying `u64`.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Fingerprint(u64);

impl Fingerprint {
    /// Wrap a raw `u64` as a `Fingerprint`. Intentional and greppable — the
    /// only entry from an untyped `u64` (the fingerprint function, parsing a
    /// fingerprint back out of a cache-table name, and tests).
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// The underlying `u64` (for formatting into table names, metrics, logs,
    /// and wire fields that are still untyped).
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Fingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl fmt::Debug for Fingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Fingerprint({})", self.0)
    }
}
