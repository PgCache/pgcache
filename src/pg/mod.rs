use std::fmt;

use postgres_types::PgLsn;
use serde::{Deserialize, Serialize};

pub(crate) mod cache_connection;
pub(crate) mod cdc;
pub(crate) mod connect;
pub(crate) mod protocol;

pub use connect::{config_build, config_connect, connect};

/// A PostgreSQL WAL log sequence number — a monotonic byte position in the WAL
/// stream, used as the CDC apply/flush/snapshot watermark. A newtype over `u64`
/// for type safety: LSNs share `u64`'s layout with generations and other
/// counters, and the compiler otherwise can't stop them being mixed.
///
/// Construction is the explicit, greppable [`Lsn::from_raw`]; there is no
/// `From<u64>` or `Deref`. The wire boundary (`PgLsn`, `wal_end`) and the
/// byte-distance arithmetic for replication lag are the intentional crossings.
/// Not a hash — never key an identity-hashed map with it.
#[repr(transparent)]
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Default, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Lsn(u64);

impl Lsn {
    /// Wrap a raw `u64` WAL position as an `Lsn`. The only entry from an
    /// untyped `u64` — the wire boundary, SQL queries, and tests.
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// The underlying `u64` position, for the wire, SQL, and metrics.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Bytes of WAL between `earlier` and `self` — the difference of two
    /// positions is a byte distance, not another `Lsn` (cf.
    /// `Instant::saturating_duration_since`). Saturating because positions can
    /// transiently arrive out of order; an earlier-than-`earlier` `self`
    /// reports zero lag rather than underflowing.
    pub const fn saturating_bytes_since(self, earlier: Lsn) -> u64 {
        self.0.saturating_sub(earlier.0)
    }
}

impl From<PgLsn> for Lsn {
    fn from(lsn: PgLsn) -> Self {
        Self(u64::from(lsn))
    }
}

impl From<Lsn> for PgLsn {
    fn from(lsn: Lsn) -> Self {
        PgLsn::from(lsn.0)
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

pub fn identifier_needs_quotes(id: &str) -> bool {
    match id.as_bytes() {
        [] => true,
        [first, rest @ ..] => {
            (!first.is_ascii_lowercase() && *first != b'_')
                || !rest
                    .iter()
                    .all(|&b| b == b'_' || b.is_ascii_lowercase() || b.is_ascii_digit())
        }
    }
}
