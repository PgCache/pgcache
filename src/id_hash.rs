use std::hash::{BuildHasher, Hash, Hasher};
use std::marker::PhantomData;

/// Passthrough hasher for keys that are *already* uniformly-distributed hashes
/// — `Fingerprint` and `SqlTextHash` today. Re-running a general-purpose hash
/// over a value that is itself a hash is pure waste; this returns the key's own
/// bits. Distribution is safe *only* because the keys are hashes: applied to a
/// structured key (a sequential oid, say) it would cluster into adjacent
/// buckets and degrade the map to near-O(n).
///
/// That restriction is **compiler-enforced**, not by convention:
/// [`BuildIdHasher<K>`] only implements [`Default`] for `K: IdHashable`, and
/// `IdHashable` is a sealed marker a type opts into by an explicit impl in this
/// crate. So an identity-hashed map can only be *constructed* with a key that
/// has been deliberately vetted as a hash — `HashMap<Oid, V, BuildIdHasher<Oid>>`
/// fails to build, because `Oid` is not `IdHashable`.
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
        // An `IdHashable` key writes exactly one `u64`/`u32` (it's a transparent
        // newtype over a hash), so this path is unreachable for vetted keys.
        // Kept correct (FNV-1a) rather than panicking in release so a future
        // mistake degrades instead of corrupting.
        debug_assert!(false, "IdHasher used on a non-u64/u32 key");
        for &b in bytes {
            self.0 = (self.0 ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3);
        }
    }
}

pub(crate) mod sealed {
    pub trait Sealed {}
}

/// Marker for key types whose value is already a uniformly-distributed hash, so
/// they may use the passthrough [`IdHasher`]. Sealed: a type opts in only via an
/// explicit impl in this crate (it must also impl the private `Sealed`
/// supertrait), so a sequential id like `Oid` or `Lsn` cannot accidentally be
/// identity-hashed.
pub trait IdHashable: sealed::Sealed + Hash + Eq {}

/// Implement [`IdHashable`] for a hash-derived newtype. Use only for types whose
/// value is a full-width hash (a `DefaultHasher`/SipHash output), never a
/// sequential id.
macro_rules! impl_id_hashable {
    ($ty:ty) => {
        impl $crate::id_hash::sealed::Sealed for $ty {}
        impl $crate::id_hash::IdHashable for $ty {}
    };
}
pub(crate) use impl_id_hashable;

/// `BuildHasher` for [`IdHasher`], usable as the `S` parameter of
/// `HashMap`/`HashSet`/`DashMap`. The phantom key makes [`Default`] (and hence
/// every map constructor) available only for `K: IdHashable`.
///
/// A hash-derived key works:
/// ```
/// use pgcache_lib::id_hash::BuildIdHasher;
/// use pgcache_lib::query::Fingerprint;
/// use std::collections::HashMap;
/// let _: HashMap<Fingerprint, u8, BuildIdHasher<Fingerprint>> = HashMap::default();
/// ```
///
/// A sequential id does not — it is not `IdHashable`, so the map can't be built:
/// ```compile_fail
/// use pgcache_lib::id_hash::BuildIdHasher;
/// use pgcache_lib::catalog::Oid;
/// use std::collections::HashMap;
/// let _: HashMap<Oid, u8, BuildIdHasher<Oid>> = HashMap::default();
/// ```
pub struct BuildIdHasher<K>(PhantomData<fn() -> K>);

impl<K: IdHashable> Default for BuildIdHasher<K> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<K> Clone for BuildIdHasher<K> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<K> BuildHasher for BuildIdHasher<K> {
    type Hasher = IdHasher;

    fn build_hasher(&self) -> IdHasher {
        IdHasher::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn passthrough(write: impl FnOnce(&mut IdHasher)) -> u64 {
        let mut h = IdHasher::default();
        write(&mut h);
        h.finish()
    }

    #[test]
    fn test_id_hasher_passes_u64_through() {
        for n in [0u64, 1, 0xdead_beef, u64::MAX] {
            assert_eq!(passthrough(|h| h.write_u64(n)), n);
        }
    }

    #[test]
    fn test_id_hasher_passes_u32_through() {
        assert_eq!(passthrough(|h| h.write_u32(42)), 42);
    }
}
