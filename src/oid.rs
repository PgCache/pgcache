//! PostgreSQL OID newtypes: relation OIDs (`Oid`), type OIDs (`TypeOid`) and
//! function OIDs (`FunctionOid`).

use std::fmt;

use bytes::BytesMut;
use postgres_types::{FromSql, IsNull, ToSql, Type, to_sql_checked};
use serde::{Deserialize, Serialize};

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

/// A PostgreSQL type OID (`pg_type.oid`): a Bind parameter's declared type, a
/// column's type. Kept distinct from [`Oid`] (relations) for the same reason
/// `Oid` exists — the two share `u32`'s layout and nothing else. Construction
/// is [`TypeOid::from_raw`] at the wire / catalog boundary, or
/// [`TypeOid::from_type`] for a known type.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TypeOid(u32);

impl TypeOid {
    /// "Unspecified" on the wire: the server infers the type.
    pub const UNSPECIFIED: Self = Self(0);

    /// Wrap a raw `u32` read off the wire or from a catalog row.
    pub const fn from_raw(value: u32) -> Self {
        Self(value)
    }

    /// The OID of a type the driver knows.
    pub fn from_type(ty: &Type) -> Self {
        Self(ty.oid())
    }

    /// The underlying `u32`, for the wire and logs.
    pub const fn get(self) -> u32 {
        self.0
    }

    /// The driver's view of this type, when it is a builtin.
    pub fn pg_type(self) -> Option<Type> {
        Type::from_oid(self.0)
    }
}

impl fmt::Display for TypeOid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl ToSql for TypeOid {
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

impl<'a> FromSql<'a> for TypeOid {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        <u32 as FromSql>::from_sql(ty, raw).map(TypeOid)
    }

    fn accepts(ty: &Type) -> bool {
        <u32 as FromSql>::accepts(ty)
    }
}

/// A PostgreSQL function OID (`pg_proc.oid`): what a call in a query resolves
/// to, and the dependency a cached entry records on it (PGC-578). Distinct
/// from [`Oid`] and [`TypeOid`] for the same reason those two are: all three
/// are `u32`s, and only the type stops a relation's oid naming a function.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FunctionOid(u32);

impl FunctionOid {
    /// Below this, an oid is a builtin's: `FirstNormalObjectId` in PostgreSQL.
    const FIRST_NORMAL_OBJECT_ID: u32 = 16384;

    /// Wrap a raw `u32` from a catalog row.
    pub const fn from_raw(value: u32) -> Self {
        Self(value)
    }

    /// The underlying `u32`, for SQL parameters and logs.
    pub const fn get(self) -> u32 {
        self.0
    }

    /// Whether the function ships with PostgreSQL rather than being created
    /// in the database (by an extension or a user).
    pub const fn is_builtin(self) -> bool {
        self.0 < Self::FIRST_NORMAL_OBJECT_ID
    }
}

impl fmt::Display for FunctionOid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl ToSql for FunctionOid {
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

impl<'a> FromSql<'a> for FunctionOid {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        <u32 as FromSql>::from_sql(ty, raw).map(FunctionOid)
    }

    fn accepts(ty: &Type) -> bool {
        <u32 as FromSql>::accepts(ty)
    }
}
