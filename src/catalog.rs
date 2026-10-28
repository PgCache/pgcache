//! Database catalog metadata structures.
//!
//! This module contains metadata about database schemas, tables, and columns.
//! These structures are used by both the cache subsystem (for tracking tables)
//! and the query resolution subsystem (for name resolution and type information).

use std::collections::HashMap;
#[cfg(feature = "proxy")]
use std::collections::HashSet;
use std::ops::Deref;
use std::sync::Arc;

use ecow::EcoString;
use iddqd::{BiHashItem, BiHashMap, bi_upcast};
use postgres_types::{Kind, Type};
use smallvec::{SmallVec, smallvec};
#[cfg(feature = "proxy")]
use tokio_postgres::{Client, Error};

use crate::cache::CacheError;
use crate::oid::{Oid, TypeOid};

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

    /// Look up a column by its position.
    pub fn by_position(&self, position: ColumnPosition) -> Option<&ColumnMetadata> {
        self.sorted
            .get(position.index())
            .filter(|column| column.position == position)
            .or_else(|| {
                self.sorted
                    .iter()
                    .find(|column| column.position == position)
            })
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

/// A set of a table's columns, by the 0-based index of each column's
/// position (`ColumnMetadata::index`), which is how CDC row images are
/// indexed. Word-wise, so a subset test over a wide table is a few ANDs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnMask {
    words: SmallVec<[u64; 2]>,
}

impl ColumnMask {
    const WORD_BITS: usize = u64::BITS as usize;

    /// The empty set, sized for `columns` columns.
    pub fn with_len(columns: usize) -> Self {
        Self {
            words: smallvec![0; columns.div_ceil(Self::WORD_BITS)],
        }
    }

    /// Every column of `table`.
    pub fn all(table: &TableMetadata) -> Self {
        let mut mask = Self::with_len(table.columns.len());
        for column in &table.columns {
            mask.set(column.index());
        }
        mask
    }

    /// The columns of `table` that `names` name; `None` when a name is not
    /// a column of the table, so the caller falls back to reading every
    /// column.
    pub fn columns_named<'a>(
        table: &TableMetadata,
        names: impl IntoIterator<Item = &'a str>,
    ) -> Option<Self> {
        let mut mask = Self::with_len(table.columns.len());
        for name in names {
            mask.set(table.columns.get(name)?.index());
        }
        Some(mask)
    }

    pub fn set(&mut self, index: usize) {
        let word = index / Self::WORD_BITS;
        if word >= self.words.len() {
            self.words.resize(word + 1, 0);
        }
        if let Some(bits) = self.words.get_mut(word) {
            *bits |= 1 << (index % Self::WORD_BITS);
        }
    }

    pub fn contains(&self, index: usize) -> bool {
        self.words
            .get(index / Self::WORD_BITS)
            .is_some_and(|word| word & (1 << (index % Self::WORD_BITS)) != 0)
    }

    /// Whether every column in `self` is also in `other`.
    pub fn is_subset_of(&self, other: &Self) -> bool {
        self.words.iter().enumerate().all(|(i, word)| {
            let in_other = other.words.get(i).copied().unwrap_or(0);
            word & !in_other == 0
        })
    }
}

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
    /// Whether the relation's REPLICA IDENTITY is FULL: pgoutput then carries
    /// the complete old row on UPDATE/DELETE, so the CDC old-image ladder is
    /// bypassed (the tuple itself is authoritative) and PK changes are
    /// detected by comparing PK columns rather than key-tuple presence.
    pub replica_identity_full: bool,
    /// Whether origin applies row-level security to the relation
    /// (`pg_class.relrowsecurity`): a query over it is registered only with
    /// the applicable policies inlined (PGC-113).
    pub row_security: bool,
    /// Every partition under this table, leaves and intermediate levels
    /// alike (`pg_inherits` descendants). Under `publish_via_partition_root`
    /// a partition's rows stream as this table's, so a schema change signalled
    /// on a partition (truncate, drop, a sub-partition attached) is resolved
    /// to this table through the list. Empty for a plain table.
    pub partitions: Vec<Oid>,
    /// Every table above this one (`pg_inherits` ancestors). An `ALTER
    /// TABLE` on a partitioned root reaches its partitions, so a change
    /// signalled on an ancestor is resolved to this table through the list.
    /// Empty for a plain table or a root.
    pub ancestors: Vec<Oid>,
    /// Every relation a forwarded write can name whose rows are this table's:
    /// the partitions below and above it, and the views over any of those
    /// (views over views included). The read-after-write gate expands a write
    /// to such a name into this table (ADR-048). Empty for a plain table with
    /// no views.
    pub write_aliases: Vec<WriteAlias>,
}

/// A schema-qualified relation name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RelationName {
    pub schema: EcoString,
    pub name: EcoString,
}

/// One entry of [`TableMetadata::write_aliases`]. A write through a view
/// cannot keep its rows (view columns may be renamed or computed), so the gate
/// records it as an opaque write on the table; a partition shares the table's
/// column names and keeps row-level precision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteAlias {
    pub relation: RelationName,
    pub is_view: bool,
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

    /// A `public.<name>` fixture for tests and benches: the key is the
    /// columns flagged `is_primary_key`, no indexes, no partition tree,
    /// default replica identity. The one place a new catalog-only field gets its
    /// fixture default.
    pub fn for_test(
        name: &str,
        oid: u32,
        columns: impl IntoIterator<Item = ColumnMetadata>,
    ) -> Self {
        let columns = ColumnStore::new(columns);
        let primary_key_columns = columns
            .iter()
            .filter(|column| column.is_primary_key)
            .map(|column| column.name.clone())
            .collect();
        Self {
            relation_oid: Oid::from_raw(oid),
            name: name.into(),
            schema: "public".into(),
            primary_key_columns,
            columns,
            row_security: false,
            indexes: Vec::new(),
            replica_identity_full: false,
            partitions: Vec::new(),
            ancestors: Vec::new(),
            write_aliases: Vec::new(),
        }
    }
}

/// A catalog entry shared by every resolution that references it (ADR-057):
/// a cached query keeps the metadata it was resolved against after a refresh
/// replaces the entry, as it kept copies before.
#[derive(Debug, Clone)]
pub struct CatalogTable(Arc<TableMetadata>);

impl CatalogTable {
    pub fn shared(&self) -> &Arc<TableMetadata> {
        &self.0
    }
}

impl Deref for CatalogTable {
    type Target = TableMetadata;

    fn deref(&self) -> &TableMetadata {
        &self.0
    }
}

impl From<TableMetadata> for CatalogTable {
    fn from(table: TableMetadata) -> Self {
        Self(Arc::new(table))
    }
}

impl BiHashItem for CatalogTable {
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

/// The tables the cache tracks, by relation oid and by `(schema, name)`.
pub type Catalog = BiHashMap<CatalogTable>;

/// A user column's 1-based position (PostgreSQL `attnum`), always in
/// `1..=i16::MAX`, so its 0-based slice index cannot underflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ColumnPosition(i16);

impl ColumnPosition {
    pub const FIRST: Self = Self(1);
    pub const MAX: Self = Self(i16::MAX);

    /// The position of the column at 0-based `index`; `None` past `i16::MAX`.
    pub fn from_index(index: usize) -> Option<Self> {
        Self::from_attnum(i64::try_from(index).ok()?.checked_add(1)?)
    }

    /// The position for a 1-based `attnum`; `None` outside `1..=i16::MAX`.
    pub fn from_attnum(attnum: i64) -> Option<Self> {
        i16::try_from(attnum).ok().filter(|n| *n >= 1).map(Self)
    }

    /// 0-based index into a user column array.
    pub fn index(self) -> usize {
        usize::from(self.0.unsigned_abs().saturating_sub(1))
    }

    #[cfg(test)]
    pub(crate) const fn from_raw(attnum: i16) -> Self {
        Self(attnum)
    }
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
    pub position: ColumnPosition,
    /// PostgreSQL type OID (original from origin database, used in RowDescription)
    pub type_oid: TypeOid,
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
    pub fn index(&self) -> usize {
        self.position.index()
    }

    /// Enum, domain over enum (recursive), or array of such — types whose
    /// cache storage (text) does not preserve origin ordering, so
    /// order-dependent uses must not be admitted to the cache.
    pub fn is_enum_ordered(&self) -> bool {
        fn kind_is_enum(t: &Type) -> bool {
            match t.kind() {
                Kind::Enum(_) => true,
                Kind::Domain(base) => kind_is_enum(base),
                Kind::Array(elem) => kind_is_enum(elem),
                Kind::Range(sub) | Kind::Multirange(sub) => kind_is_enum(sub),
                Kind::Simple | Kind::Pseudo | Kind::Composite(_) => false,
                _ => false,
            }
        }
        kind_is_enum(&self.data_type)
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

/// SQL behind [`function_volatility_map_load`]. Public so pgcache-fit's
/// embedded snapshot regenerates from the identical query and cannot drift
/// from the loader.
pub const FUNCTION_VOLATILITY_SQL: &str = "SELECT p.proname,
        MAX(CASE p.provolatile
            WHEN 'v' THEN 2
            WHEN 's' THEN 1
            ELSE 0
        END) AS worst_volatility
 FROM pg_proc p
 JOIN pg_namespace n ON p.pronamespace = n.oid
 WHERE p.prokind NOT IN ('a', 'w')
 GROUP BY p.proname";

/// SQL behind [`aggregate_functions_load`]; public for the same
/// anti-drift reason as [`FUNCTION_VOLATILITY_SQL`].
pub const AGGREGATE_FUNCTIONS_SQL: &str =
    "SELECT DISTINCT lower(p.proname) FROM pg_proc p WHERE p.prokind = 'a'";

/// Load function volatilities from pg_proc.
///
/// Queries the origin database for all scalar functions and builds a map
/// from unqualified function name to worst-case volatility across all
/// overloads and schemas. Aggregate and window functions are excluded.
///
/// The map is keyed by lowercase function name to match PostgreSQL's
/// case-insensitive identifier handling.
#[cfg(feature = "proxy")]
pub async fn function_volatility_map_load(
    client: &Client,
) -> Result<HashMap<EcoString, FunctionVolatility>, Error> {
    let rows = client.query(FUNCTION_VOLATILITY_SQL, &[]).await?;

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
#[cfg(feature = "proxy")]
pub async fn aggregate_functions_load(client: &Client) -> Result<HashSet<EcoString>, Error> {
    let rows = client.query(AGGREGATE_FUNCTIONS_SQL, &[]).await?;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_position_bounds() {
        assert_eq!(ColumnPosition::from_index(0), Some(ColumnPosition::FIRST));
        assert_eq!(ColumnPosition::FIRST.index(), 0);
        assert_eq!(
            ColumnPosition::from_attnum(5).map(ColumnPosition::index),
            Some(4)
        );
        assert_eq!(
            ColumnPosition::from_attnum(i64::from(i16::MAX)),
            Some(ColumnPosition::MAX)
        );
        for out_of_range in [0, -1, i64::from(i16::MAX) + 1] {
            assert_eq!(
                ColumnPosition::from_attnum(out_of_range),
                None,
                "attnum {out_of_range}"
            );
        }
        let past_max = usize::try_from(i16::MAX).expect("i16::MAX as usize");
        assert_eq!(ColumnPosition::from_index(past_max), None);
    }

    /// A mask names columns by index; a name the table lacks yields no
    /// mask; a set is a subset of another when it adds no column, and a
    /// column past the inline words still fits.
    #[test]
    fn test_column_mask_subset_and_naming() {
        let table = TableMetadata::for_test(
            "t",
            7,
            [
                column_fixture("id", 1, true),
                column_fixture("a", 2, false),
                column_fixture("b", 3, false),
            ],
        );
        let all = ColumnMask::all(&table);
        let ab = ColumnMask::columns_named(&table, ["a", "b"]).expect("named");
        let a = ColumnMask::columns_named(&table, ["a"]).expect("named");
        assert!(ColumnMask::columns_named(&table, ["a", "zz"]).is_none());
        assert!(a.is_subset_of(&ab));
        assert!(!ab.is_subset_of(&a));
        assert!(ab.is_subset_of(&all));
        assert!(ColumnMask::with_len(3).is_subset_of(&a), "empty set");
        assert!(a.contains(1) && !a.contains(0) && !a.contains(2));

        let mut wide = ColumnMask::with_len(3);
        wide.set(130);
        assert!(wide.contains(130));
        assert!(!wide.is_subset_of(&all));
        assert!(all.is_subset_of(&ColumnMask::all(&table)));
    }

    fn column_fixture(name: &str, position: i16, is_primary_key: bool) -> ColumnMetadata {
        ColumnMetadata {
            name: name.into(),
            position: ColumnPosition::from_raw(position),
            type_oid: TypeOid::from_raw(Type::TEXT.oid()),
            data_type: Type::TEXT,
            type_name: "text".into(),
            cache_type_name: "text".into(),
            is_primary_key,
        }
    }
}
