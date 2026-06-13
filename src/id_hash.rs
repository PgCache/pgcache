use std::hash::{BuildHasherDefault, Hasher};

/// Passthrough hasher for keys that are *already* uniformly-distributed hashes
/// — query fingerprints today, and other hash-derived newtypes (e.g. a future
/// `SqlTextHash`) as they adopt it. Re-running a general-purpose hash over a
/// value that is itself a hash is pure waste; this returns the key's own bits.
///
/// Only single-`u64` / single-`u32` keys are valid. Anything else trips the
/// debug assertion (loud in tests) and falls back to FNV-1a, so a misuse
/// degrades to slow-but-correct rather than silently colliding everything into
/// one bucket.
///
/// Safe distribution-wise *only* because the keys are hashes: applied to a
/// structured key (a sequential oid, say) it would cluster. The type system is
/// the guard — the [`crate::query::FingerprintMap`] aliases pin it to
/// `Fingerprint` keys, and a non-hash key type can't reach this hasher.
#[derive(Default)]
pub struct IdHasher(u64);

impl Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write_u64(&mut self, n: u64) {
        self.0 = n;
    }

    fn write_u32(&mut self, n: u32) {
        self.0 = u64::from(n);
    }

    fn write(&mut self, bytes: &[u8]) {
        debug_assert!(false, "IdHasher used on a non-u64/u32 key");
        for &b in bytes {
            self.0 = (self.0 ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3);
        }
    }
}

/// `BuildHasher` for [`IdHasher`], for use as the `S` parameter of
/// `HashMap`/`HashSet`/`DashMap`.
pub type BuildIdHasher = BuildHasherDefault<IdHasher>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::hash::BuildHasher;

    #[test]
    fn test_id_hasher_passes_u64_through() {
        let b = BuildIdHasher::default();
        for n in [0u64, 1, 0xdead_beef, u64::MAX] {
            assert_eq!(b.hash_one(n), n, "u64 key should hash to itself");
        }
    }

    #[test]
    fn test_id_hasher_passes_u32_through() {
        let b = BuildIdHasher::default();
        assert_eq!(b.hash_one(42u32), 42);
    }
}
