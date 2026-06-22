//! Database catalog metadata structures.
//!
//! This module contains metadata about database schemas, tables, and columns.
//! These structures are used by both the cache subsystem (for tracking tables)
//! and the query resolution subsystem (for name resolution and type information).

use std::collections::{HashMap, HashSet};
use std::fmt;

use ecow::EcoString;
use iddqd::{BiHashItem, bi_upcast};
use serde::{Deserialize, Serialize};
use tokio_postgres::types::{FromSql, IsNull, Kind, ToSql, Type, to_sql_checked};
use tokio_postgres::{Client, Error};
use tokio_util::bytes::BytesMut;

use crate::cache::CacheError;

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

use crate::query::ast::{ColumnNode, ScalarExpr, SelectColumn, SelectColumns, TableAlias};
use crate::query::resolved::{
    ResolvedColumnNode, ResolvedScalarExpr, ResolvedSelectColumn, ResolvedSelectColumns,
};

/// Column storage: sorted by position with O(1) name lookups.
///
/// Columns are stored once in position order (table definition order).
/// A name→index map provides fast lookups by column name.
#[derive(Debug, Clone)]
pub struct ColumnStore {
    /// Columns sorted by position (table definition order)
    sorted: Vec<ColumnMetadata>,
    /// Column name → index into `sorted`
    by_name: HashMap<EcoString, usize>,
}

impl ColumnStore {
    /// Build a `ColumnStore` from an unsorted iterator of columns.
    pub fn new(columns: impl IntoIterator<Item = ColumnMetadata>) -> Self {
        let mut sorted: Vec<ColumnMetadata> = columns.into_iter().collect();
        sorted.sort_by_key(|c| c.position);
        let by_name = sorted
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.clone(), i))
            .collect();
        Self { sorted, by_name }
    }

    /// Look up a column by name.
    pub fn get(&self, name: &str) -> Option<&ColumnMetadata> {
        let &idx = self.by_name.get(name)?;
        self.sorted.get(idx)
    }

    /// Iterate columns in position order.
    pub fn iter(&self) -> std::slice::Iter<'_, ColumnMetadata> {
        self.sorted.iter()
    }

    /// Number of columns.
    pub fn len(&self) -> usize {
        self.sorted.len()
    }

    /// Whether the store is empty.
    pub fn is_empty(&self) -> bool {
        self.sorted.is_empty()
    }
}

impl<'a> IntoIterator for &'a ColumnStore {
    type Item = &'a ColumnMetadata;
    type IntoIter = std::slice::Iter<'a, ColumnMetadata>;

    fn into_iter(self) -> Self::IntoIter {
        self.sorted.iter()
    }
}

impl PartialEq for ColumnStore {
    fn eq(&self, other: &Self) -> bool {
        self.sorted == other.sorted
    }
}

impl Eq for ColumnStore {}

/// Metadata about a database table.
///
/// Contains schema information, column definitions, and primary key metadata
/// for a PostgreSQL table. This information is fetched from the database
/// information_schema and pg_catalog.
#[derive(Debug, Clone)]
pub struct TableMetadata {
    /// PostgreSQL relation OID
    pub relation_oid: Oid,
    /// Table name (unqualified)
    pub name: EcoString,
    /// Schema name (e.g., "public")
    pub schema: EcoString,
    /// Names of columns that form the primary key
    pub primary_key_columns: Vec<EcoString>,
    /// Column metadata sorted by position with name lookups
    pub columns: ColumnStore,
    /// Index metadata for non-primary-key indexes
    pub indexes: Vec<IndexMetadata>,
}

impl TableMetadata {
    /// Compare table schema (columns, primary key) without comparing indexes.
    ///
    /// Used to determine if a table needs recreation. Index changes don't
    /// require table recreation.
    pub fn schema_eq(&self, other: &TableMetadata) -> bool {
        self.relation_oid == other.relation_oid
            && self.name == other.name
            && self.schema == other.schema
            && self.primary_key_columns == other.primary_key_columns
            && self.columns == other.columns
    }

    /// Whether any column can carry a TOASTed value (PGC-264); see
    /// [`ColumnMetadata::is_toastable`].
    pub fn has_toastable_column(&self) -> bool {
        self.columns.iter().any(ColumnMetadata::is_toastable)
    }

    /// Generate SELECT columns for all columns in this table.
    ///
    /// If an alias is provided, column references will use the alias name
    /// and respect any column aliases defined in the TableAlias.
    ///
    pub fn select_columns(&self, alias: Option<&TableAlias>) -> SelectColumns {
        let columns = self
            .columns
            .iter()
            .map(|c| SelectColumn::Expr {
                expr: ScalarExpr::Column(ColumnNode {
                    table: if let Some(alias) = alias {
                        Some(alias.name.clone())
                    } else {
                        Some(EcoString::from(self.name.as_str()))
                    },
                    column: if let Some(alias) = alias {
                        alias
                            .columns
                            .get(c.index())
                            .map(EcoString::as_str)
                            .unwrap_or(c.name.as_str())
                            .into()
                    } else {
                        EcoString::from(c.name.as_str())
                    },
                }),
                alias: None,
            })
            .collect();

        SelectColumns::Columns(columns)
    }

    /// Generate resolved SELECT columns for all columns in this table.
    ///
    /// Creates `ResolvedSelectColumns::Columns` with fully qualified column references.
    /// If a table_alias is provided, columns will use that alias for deparsing.
    pub fn resolved_select_columns(&self, table_alias: Option<&str>) -> ResolvedSelectColumns {
        let columns = self
            .columns
            .iter()
            .map(|c| ResolvedSelectColumn {
                expr: ResolvedScalarExpr::Column(ResolvedColumnNode {
                    schema: self.schema.clone(),
                    table: self.name.clone(),
                    table_alias: table_alias.map(EcoString::from),
                    column: c.name.clone(),
                    column_metadata: c.clone(),
                }),
                alias: None,
            })
            .collect();

        ResolvedSelectColumns::Columns(columns)
    }
}

impl BiHashItem for TableMetadata {
    type K1<'a> = Oid;
    type K2<'a> = (&'a str, &'a str);

    fn key1(&self) -> Self::K1<'_> {
        self.relation_oid
    }

    fn key2(&self) -> Self::K2<'_> {
        (self.schema.as_str(), self.name.as_str())
    }

    bi_upcast!();
}

/// Metadata about a table column.
///
/// Contains type information and position data for a single column
/// within a table.
///
/// String fields use `EcoString` for reduced clone cost: short names (≤15 bytes)
/// are stored inline (stack-only memcpy), longer names like
/// `"timestamp with time zone"` get a single heap allocation shared via
/// refcount across all clones.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnMetadata {
    /// Column name
    pub name: EcoString,
    /// 1-based position in table (matches PostgreSQL attnum)
    pub position: i16,
    /// PostgreSQL type OID (original from origin database, used in RowDescription)
    pub type_oid: u32,
    /// Parsed PostgreSQL type (may be Domain, Enum, etc.)
    pub data_type: Type,
    /// Human-readable type name from origin (e.g., "year", "mood")
    pub type_name: EcoString,
    /// Type name for cache table creation (e.g., "integer" for year domain, "text" for enums)
    pub cache_type_name: EcoString,
    /// Whether this column is part of the primary key
    pub is_primary_key: bool,
}

impl ColumnMetadata {
    /// 0-based index for a column's position in a user column array.
    ///
    /// Converts the PostgreSQL 1-based `attnum` to a usable Rust slice index.
    /// User columns always have `position >= 1`; system columns (negative attnum)
    /// are not represented here.
    pub fn index(&self) -> usize {
        usize::try_from(self.position - 1).expect("user column position is >= 1")
    }

    /// Whether this column's type can carry a TOASTed value (PGC-264).
    /// Conservative by construction: only known fixed-width types are
    /// excluded, so an unrecognized type counts as toastable — a whitelist
    /// miss must cost a little bookkeeping, never correctness.
    pub fn is_toastable(&self) -> bool {
        !matches!(
            self.data_type,
            Type::BOOL
                | Type::CHAR
                | Type::INT2
                | Type::INT4
                | Type::INT8
                | Type::OID
                | Type::FLOAT4
                | Type::FLOAT8
                | Type::MONEY
                | Type::DATE
                | Type::TIME
                | Type::TIMETZ
                | Type::TIMESTAMP
                | Type::TIMESTAMPTZ
                | Type::INTERVAL
                | Type::UUID
                | Type::MACADDR
                | Type::MACADDR8
        )
    }
}

impl std::hash::Hash for ColumnMetadata {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Hash all fields except data_type (which doesn't implement Hash)
        self.name.hash(state);
        self.position.hash(state);
        self.type_oid.hash(state);
        self.type_name.hash(state);
        self.cache_type_name.hash(state);
        self.is_primary_key.hash(state);
    }
}

/// Metadata about a table index.
///
/// Holds the origin's canonical index definition (`pg_get_indexdef`) so the
/// cache table can reproduce partial indexes, expression indexes, `DESC`,
/// opclasses, and collations faithfully. Primary key indexes are excluded
/// since they are created by the PRIMARY KEY constraint.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexMetadata {
    /// Index name (for reference/logging, not used in CREATE INDEX)
    pub name: EcoString,
    /// Whether this is a unique index
    pub is_unique: bool,
    /// Canonical `CREATE INDEX` statement from `pg_get_indexdef`, retargeted to
    /// the cache table at creation time.
    pub definition: EcoString,
}

/// PostgreSQL function volatility classification.
///
/// Determines whether a function's result depends only on its inputs (immutable),
/// on inputs plus database state (stable), or can vary between calls (volatile).
/// Used to decide whether functions in WHERE/FROM clauses are safe for caching.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionVolatility {
    Immutable,
    Stable,
    Volatile,
}

/// Load function volatilities from pg_proc.
///
/// Queries the origin database for all scalar functions and builds a map
/// from unqualified function name to worst-case volatility across all
/// overloads and schemas. Aggregate and window functions are excluded.
///
/// The map is keyed by lowercase function name to match PostgreSQL's
/// case-insensitive identifier handling.
pub async fn function_volatility_map_load(
    client: &Client,
) -> Result<HashMap<EcoString, FunctionVolatility>, Error> {
    let rows = client
        .query(
            "SELECT p.proname,
                    MAX(CASE p.provolatile
                        WHEN 'v' THEN 2
                        WHEN 's' THEN 1
                        ELSE 0
                    END) AS worst_volatility
             FROM pg_proc p
             JOIN pg_namespace n ON p.pronamespace = n.oid
             WHERE p.prokind NOT IN ('a', 'w')
             GROUP BY p.proname",
            &[],
        )
        .await?;

    let mut map = HashMap::with_capacity(rows.len());
    for row in &rows {
        let name: &str = row.get(0);
        let worst: i32 = row.get(1);
        let volatility = match worst {
            0 => FunctionVolatility::Immutable,
            1 => FunctionVolatility::Stable,
            _ => FunctionVolatility::Volatile,
        };
        map.insert(name.into(), volatility);
    }

    Ok(map)
}

/// Load aggregate function names from pg_proc.
///
/// Queries the origin database for all aggregate functions and builds a set
/// of lowercase function names. Used during decorrelation to determine whether
/// a scalar subquery's output expression contains an aggregate (which controls
/// whether a GROUP BY is needed in the derived table).
pub async fn aggregate_functions_load(client: &Client) -> Result<HashSet<EcoString>, Error> {
    let rows = client
        .query(
            "SELECT DISTINCT lower(p.proname) FROM pg_proc p WHERE p.prokind = 'a'",
            &[],
        )
        .await?;

    let mut set = HashSet::with_capacity(rows.len());
    for row in &rows {
        let name: &str = row.get(0);
        set.insert(name.into());
    }

    Ok(set)
}

/// Resolves a PostgreSQL Type to its cache-compatible storage type name.
///
/// - **Domains** are resolved to their base type recursively (e.g., `year` → `integer`)
/// - **Enums** are stored as `text` (PostgreSQL transmits enum labels as text)
/// - **Composites** return an error (not yet supported)
/// - **Arrays** resolve the element type and append `[]`
/// - **Built-in types** use their PostgreSQL name directly
///
/// # Errors
///
/// Returns `CacheError::UnsupportedType` for composite types.
pub fn cache_type_name_resolve(data_type: &Type) -> Result<String, CacheError> {
    match data_type.kind() {
        Kind::Domain(base) => cache_type_name_resolve(base),
        Kind::Enum(_) => Ok("text".to_owned()),
        Kind::Composite(_) => Err(CacheError::UnsupportedType {
            type_name: data_type.name().to_owned(),
            reason: "composite types not yet supported".to_owned(),
        }),
        Kind::Array(elem) => {
            let elem_name = cache_type_name_resolve(elem)?;
            Ok(format!("{}[]", elem_name))
        }
        // Built-in types use their PostgreSQL name directly
        Kind::Simple | Kind::Pseudo | Kind::Range(_) | Kind::Multirange(_) => {
            Ok(data_type.name().to_owned())
        }
        // Kind is non-exhaustive; treat unknown kinds as their type name
        _ => Ok(data_type.name().to_owned()),
    }
}
