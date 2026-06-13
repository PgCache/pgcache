use std::collections::{HashMap, HashSet};
use std::fmt;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use crate::id_hash::{BuildIdHasher, impl_id_hashable};

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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Fingerprint(u64);

impl_id_hashable!(Fingerprint);

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

/// `HashMap` keyed by `Fingerprint` with the passthrough [`IdHasher`](crate::id_hash::IdHasher):
/// the key is already a hash, so a lookup doesn't recompute one.
pub type FingerprintMap<V> = HashMap<Fingerprint, V, BuildIdHasher<Fingerprint>>;
/// `HashSet` of `Fingerprint` with the passthrough hasher.
pub type FingerprintSet = HashSet<Fingerprint, BuildIdHasher<Fingerprint>>;
/// `DashMap` keyed by `Fingerprint` with the passthrough hasher.
pub type FingerprintDashMap<V> = DashMap<Fingerprint, V, BuildIdHasher<Fingerprint>>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::hash::BuildHasher;

    /// The load-bearing invariant for the identity-hashed maps: a `Fingerprint`
    /// hashes (under the passthrough hasher) to exactly its own `u64`. If the
    /// derive ever stopped routing through `write_u64`, identity hashing would
    /// silently collide every key.
    #[test]
    fn test_fingerprint_identity_hashes_to_its_u64() {
        let build = BuildIdHasher::<Fingerprint>::default();
        for n in [0u64, 1, 0xdead_beef, u64::MAX] {
            assert_eq!(build.hash_one(Fingerprint::from_raw(n)), n);
        }
    }
}
