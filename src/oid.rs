//! The PostgreSQL relation OID newtype.

use std::fmt;

use serde::{Deserialize, Serialize};
use tokio_postgres::types::{FromSql, IsNull, ToSql, Type, to_sql_checked};
use tokio_util::bytes::BytesMut;

/// A PostgreSQL relation OID (`pg_class.oid`). A newtype over `u32` for type
/// safety — oids share `u32`'s layout with column positions, counts, and other
/// bare integers, and the compiler otherwise can't stop them being mixed.
/// Construction is the explicit, greppable [`Oid::from_raw`]; there is
/// deliberately no `From<u32>` or `Deref`, so every crossing from a raw `u32`
/// (the CDC `rel_id`, the pgrx extension) is intentional. The SQL boundary is
/// handled by the `ToSql`/`FromSql` impls below, not `from_raw`.
///
/// Unlike a [`Fingerprint`](crate::query::Fingerprint), an oid is *sequential*,
/// not a hash — Oid-keyed maps must use the default hasher, never the
/// passthrough [`IdHasher`](crate::id_hash::IdHasher), or they would cluster.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Oid(u32);

impl Oid {
    /// Wrap a raw `u32` as an `Oid`. Intentional and greppable — the entry from
    /// an untyped `u32` (CDC `rel_id`, the pgrx extension, and tests; SQL goes
    /// through `FromSql`).
    pub const fn from_raw(value: u32) -> Self {
        Self(value)
    }

    /// The underlying `u32`, for SQL parameters, the pgrx extension, and logs.
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl fmt::Display for Oid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

// SQL boundary: an `Oid` serializes as its underlying `u32` (PostgreSQL `oid`),
// so catalog queries can bind `&oid` and read `row.get::<_, Oid>` directly
// instead of `.get()`/`from_raw` at each call site.
impl ToSql for Oid {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        self.0.to_sql(ty, out)
    }

    fn accepts(ty: &Type) -> bool {
        <u32 as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

impl<'a> FromSql<'a> for Oid {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        <u32 as FromSql>::from_sql(ty, raw).map(Oid)
    }

    fn accepts(ty: &Type) -> bool {
        <u32 as FromSql>::accepts(ty)
    }
}
