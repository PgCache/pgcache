use std::collections::HashMap;
use std::sync::Arc;

use ecow::EcoString;
use postgres_protocol::escape;
use rootcause::Report;
use tokio_postgres::Row;
use tokio_postgres::types::{ToSql, Type};
use tracing::{debug, error, info, instrument, warn};

use super::cache_index::index_sql_retarget;
use super::core::WriterCore;
use crate::cache::messages::PartitionCreated;
use crate::cache::runtime::collation_probe::sample_sorted_under;
use crate::cache::types::WriteTargetMap;
use crate::cache::{CacheError, CacheResult, MapIntoReport, ReportExt};
use crate::catalog::{
    CatalogTable, CollationName, ColumnCollation, ColumnMetadata, ColumnPosition, ColumnStore,
    IndexMetadata, RelationName, TableMetadata, TextOrder, WriteAlias, cache_type_name_resolve,
};
use crate::oid::{Oid, TypeOid};
use crate::result::error_chain_format;

/// A column's resolved type triple — one resolution shared by both
/// metadata-derivation paths (registration and CDC Relation re-resolve),
/// so `schema_eq` compares like against like (PGC-266).
pub(super) struct ColumnTypeInfo {
    pub(super) data_type: Type,
    pub(super) type_name: EcoString,
    pub(super) cache_type_name: EcoString,
}

impl WriterCore {
    /// Resolve a column type oid: builtin fast path via `Type::from_oid`,
    /// else origin catalog lookup (memoized per connection), then the
    /// cache-side type mapping.
    pub(super) async fn column_type_resolve(
        &self,
        type_oid: TypeOid,
    ) -> CacheResult<ColumnTypeInfo> {
        let data_type = match type_oid.pg_type() {
            Some(t) => t,
            None => self
                .db_origin
                .get_type(type_oid.get())
                .await
                .map_into_report::<CacheError>()?,
        };
        let type_name: EcoString = data_type.name().into();
        let cache_type_name: EcoString = cache_type_name_resolve(&data_type)?.into();
        Ok(ColumnTypeInfo {
            data_type,
            type_name,
            cache_type_name,
        })
    }

    /// Re-resolve origin-only column types (enums, domains, arrays of
    /// such) in CDC-decoded Relation metadata. The decoder is sync with no
    /// origin access and falls back to TEXT for non-builtin oids; without
    /// this pass `schema_eq` sees a phantom schema change on every
    /// Relation message and recreates the cache table (PGC-266).
    ///
    /// Never errors the CDC path: a failed lookup (type dropped
    /// concurrently) or unsupported type keeps the decode-time TEXT
    /// fallback, degrading to the recreate behavior for that table only.
    pub(super) async fn table_metadata_types_resolve(&self, metadata: &mut TableMetadata) {
        if metadata
            .columns
            .iter()
            .all(|c| c.type_oid.pg_type().is_some())
        {
            return;
        }

        let mut columns: Vec<ColumnMetadata> = metadata.columns.iter().cloned().collect();
        for column in &mut columns {
            if column.type_oid.pg_type().is_some() {
                continue;
            }
            match self.column_type_resolve(column.type_oid).await {
                Ok(info) => {
                    column.data_type = info.data_type;
                    column.type_name = info.type_name;
                    column.cache_type_name = info.cache_type_name;
                }
                Err(e) => {
                    warn!(
                        "type resolve failed for oid {} of {}.{}, keeping text fallback: {}",
                        column.type_oid,
                        metadata.schema,
                        metadata.name,
                        error_chain_format(e.current_context())
                    );
                }
            }
        }
        metadata.columns = ColumnStore::new(columns);
    }

    #[instrument(skip_all)]
    pub(super) async fn cache_table_create(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> CacheResult<TableMetadata> {
        let table = self.query_table_metadata(schema, table).await?;
        self.cache_table_create_from_metadata(&table).await?;
        Ok(table)
    }

    #[instrument(skip_all)]
    pub(super) async fn query_table_metadata(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> CacheResult<TableMetadata> {
        let rows = self.query_table_columns_get(schema, table).await?;
        let Some(first) = rows.first() else {
            return Err(CacheError::UnknownTable {
                oid: None,
                name: None,
            }
            .into());
        };
        let relation_oid: Oid = first.get("relation_oid");
        let schema: &str = match schema {
            Some(schema) => schema,
            None => first.get("table_schema"),
        };
        let row_security: bool = first.get("row_security");
        if self.inheritance_children_exist(relation_oid).await? {
            return Err(CacheError::InheritanceParent {
                schema: schema.into(),
                name: table.into(),
            }
            .into());
        }

        let mut columns: Vec<ColumnMetadata> = Vec::with_capacity(rows.len());
        for row in &rows {
            columns.push(self.column_metadata_build(row, table).await?);
        }
        let mut primary_key_columns = primary_key_columns_of(&columns);

        let indexes = self.query_table_indexes_get(relation_oid).await?;
        let identity = self.query_replica_identity_get(relation_oid).await?;
        if let Some(key) = self
            .replica_identity_key_get(identity, relation_oid)
            .await?
        {
            key_columns_mark(&mut columns, &key);
            primary_key_columns = key;
        }
        let partitions = self.query_table_partitions_get(relation_oid).await?;
        let ancestors = self.query_table_ancestors_get(relation_oid).await?;
        let write_aliases = self
            .query_write_aliases_get(&[relation_oid])
            .await?
            .remove(&relation_oid)
            .unwrap_or_default();
        // Old tuples are written under the identity of the partition that
        // holds the row, which a partitioned table's own does not set.
        if !self
            .partitions_carry_key(&partitions, &primary_key_columns)
            .await?
        {
            return Err(CacheError::PartitionIdentity {
                schema: schema.into(),
                name: table.into(),
            }
            .into());
        }
        let replica_identity_full =
            identity == ReplicaIdentity::Full && self.partitions_identity_full(&partitions).await?;

        let table_metadata = TableMetadata {
            replica_identity_full,
            row_security,
            name: table.into(),
            schema: schema.into(),
            relation_oid,
            primary_key_columns,
            columns: ColumnStore::new(columns),
            indexes,
            partitions,
            ancestors,
            write_aliases,
        };
        self.table_collations_check(&table_metadata).await?;
        Ok(table_metadata)
    }

    /// One column's metadata from a [`Self::query_table_columns_get`] row.
    async fn column_metadata_build(&self, row: &Row, table: &str) -> CacheResult<ColumnMetadata> {
        let type_oid: TypeOid = row.get("type_oid");
        let type_info = self.column_type_resolve(type_oid).await?;
        let pg_position: i64 = row.get("position");
        let column_name: String = row.get("column_name");
        let position = ColumnPosition::from_attnum(pg_position).ok_or_else(|| {
            Report::from(CacheError::ColumnPositionOutOfRange {
                position: pg_position,
                column_name: column_name.clone(),
                table_name: table.to_owned(),
            })
        })?;
        Ok(ColumnMetadata {
            name: column_name.into(),
            position,
            type_oid,
            data_type: type_info.data_type,
            type_name: type_info.type_name,
            cache_type_name: type_info.cache_type_name,
            is_primary_key: row.get("is_primary_key"),
            collation: self.column_collation_read(row),
        })
    }

    /// A column's collation from the `collation_*` columns of a catalog row:
    /// the one it declares, else the database default.
    fn column_collation_read(&self, row: &Row) -> ColumnCollation {
        let (Some(provider), Some(schema), Some(name)) = (
            row.get::<_, Option<&str>>("collation_provider"),
            row.get::<_, Option<&str>>("collation_schema"),
            row.get::<_, Option<&str>>("collation_name"),
        ) else {
            return ColumnCollation::database_default(self.database_text_order);
        };
        let collate: Option<&str> = row.get("collation_collate");
        let deterministic: bool = row.get("collation_deterministic");
        ColumnCollation {
            name: Some(CollationName {
                schema: schema.into(),
                name: name.into(),
            }),
            text_order: TextOrder::of_collation(provider, collate, deterministic),
        }
    }

    /// Refuse a table with a column whose declared collation the cache
    /// database lacks or orders differently from origin: the cache table is
    /// created under it, and a served `ORDER BY` or range reads its order.
    /// The database default was checked at startup.
    pub(super) async fn table_collations_check(&self, metadata: &TableMetadata) -> CacheResult<()> {
        for column in &metadata.columns {
            let Some(collation) = &column.collation.name else {
                continue;
            };
            let known = self.collation_verdicts.borrow().get(collation).copied();
            let verdict = match known {
                Some(verdict) => verdict,
                None => {
                    let verdict = self
                        .collation_verdict(collation, column.collation.text_order)
                        .await?;
                    self.collation_verdicts
                        .borrow_mut()
                        .insert(collation.clone(), verdict);
                    verdict
                }
            };
            if let Some(reason) = verdict {
                return Err(CacheError::CollationUnavailable {
                    schema: metadata.schema.clone(),
                    name: metadata.name.clone(),
                    column: column.name.clone(),
                    collation: format!("{}.{}", collation.schema, collation.name).into(),
                    reason,
                }
                .into());
            }
        }
        Ok(())
    }

    /// Why the cache database cannot stand in for origin under `collation`,
    /// or `None` when it can. A byte-ordered collation only has to exist;
    /// any other has to sort the probe sample as origin does.
    async fn collation_verdict(
        &self,
        collation: &CollationName,
        text_order: TextOrder,
    ) -> CacheResult<Option<&'static str>> {
        // Asked of the catalog first: a failed statement would abort a CDC
        // transaction open on the cache session.
        let present = self
            .db_cache
            .query_opt(
                COLLATION_PRESENT_SQL,
                &[&collation.schema.as_str(), &collation.name.as_str()],
            )
            .await
            .map_into_report::<CacheError>()
            .attach_loc("looking a collation up on the cache database")?
            .is_some();
        if !present {
            return Ok(Some("does not have"));
        }
        if text_order == TextOrder::Bytes {
            return Ok(None);
        }
        let cache_order = sample_sorted_under(&self.db_cache, collation)
            .await
            .attach_loc("sorting the collation sample on the cache database")?;
        let origin_order = sample_sorted_under(&self.db_origin, collation)
            .await
            .attach_loc("sorting the collation sample on origin")?;
        Ok((origin_order != cache_order).then_some("orders differently from origin"))
    }

    /// Fill the collations a Relation message does not carry from the
    /// catalog, so `schema_eq` compares like against like and a collation
    /// changed without the schema-change signal is seen. If the catalog can't
    /// be read, the registered table's collations stand in rather than
    /// failing the stream.
    pub(super) async fn table_metadata_collations_resolve(
        &self,
        metadata: &mut TableMetadata,
    ) -> CacheResult<()> {
        let read = self
            .db_origin
            .query(RELATION_COLLATIONS_SQL, &[&metadata.relation_oid])
            .await
            .map_into_report::<CacheError>();
        let collations: HashMap<EcoString, ColumnCollation> = match read {
            Ok(rows) => rows
                .iter()
                .map(|row| {
                    let name: &str = row.get("column_name");
                    (name.into(), self.column_collation_read(row))
                })
                .collect(),
            Err(e) => {
                let Some(table) = self.cache.tables.get1(&metadata.relation_oid) else {
                    return Err(e);
                };
                warn!(
                    "reading the collations of {}.{} failed, keeping the registered ones: {}",
                    metadata.schema,
                    metadata.name,
                    error_chain_format(e.current_context())
                );
                table
                    .columns
                    .iter()
                    .map(|column| (column.name.clone(), column.collation.clone()))
                    .collect()
            }
        };
        let columns: Vec<ColumnMetadata> = metadata
            .columns
            .iter()
            .map(|column| ColumnMetadata {
                collation: collations
                    .get(&column.name)
                    .cloned()
                    .unwrap_or(ColumnCollation::UNRESOLVED),
                ..column.clone()
            })
            .collect();
        metadata.columns = ColumnStore::new(columns);
        Ok(())
    }

    /// The key the cache keys on under REPLICA IDENTITY USING INDEX: a CDC
    /// key tuple carries only the identity's columns, unique and NOT NULL by
    /// rule (PGC-531). `None` under any other identity, or an index the
    /// catalog no longer has.
    async fn replica_identity_key_get(
        &self,
        identity: ReplicaIdentity,
        relation_oid: Oid,
    ) -> CacheResult<Option<Vec<EcoString>>> {
        if identity != ReplicaIdentity::Index {
            return Ok(None);
        }
        let key = self
            .query_key_columns_get(KeyColumns::ReplicaIdentityIndex, relation_oid)
            .await?;
        Ok((!key.is_empty()).then_some(key))
    }

    #[instrument(skip_all)]
    pub(super) async fn query_table_columns_get(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> CacheResult<Vec<Row>> {
        let scope = match schema {
            Some(_) => TABLE_COLUMNS_NAMED_SCHEMA,
            None => TABLE_COLUMNS_SEARCH_PATH,
        };
        let sql = format!("{TABLE_COLUMNS_SELECT}{scope}{TABLE_COLUMNS_TAIL}");
        let params: Vec<&(dyn ToSql + Sync)> = match &schema {
            Some(schema) => vec![&table, schema],
            None => vec![&table],
        };
        self.db_origin
            .query(&sql, &params)
            .await
            .map_into_report::<CacheError>()
    }

    /// Find one child partition of a partitioned table, if any.
    /// Returns None for regular tables or partitioned tables with no children.
    #[instrument(skip_all)]
    pub(super) async fn partition_child_find(&self, relation_oid: Oid) -> CacheResult<Option<Oid>> {
        let sql = r"
            SELECT inhrelid::oid
            FROM pg_inherits
            WHERE inhparent = $1
            LIMIT 1
        ";

        let row = self
            .db_origin
            .query_opt(sql, &[&relation_oid])
            .await
            .map_into_report::<CacheError>()?;

        Ok(row.map(|r| r.get::<_, Oid>(0)))
    }

    /// Every descendant partition of the relation, in no particular order.
    pub(super) async fn query_table_partitions_get(
        &self,
        relation_oid: Oid,
    ) -> CacheResult<Vec<Oid>> {
        self.query_inherits_closure_get(InheritsDirection::Descendants, relation_oid)
            .await
    }

    /// Every ancestor of the relation, in no particular order.
    pub(super) async fn query_table_ancestors_get(
        &self,
        relation_oid: Oid,
    ) -> CacheResult<Vec<Oid>> {
        self.query_inherits_closure_get(InheritsDirection::Ancestors, relation_oid)
            .await
    }

    #[instrument(skip_all)]
    async fn query_inherits_closure_get(
        &self,
        direction: InheritsDirection,
        relation_oid: Oid,
    ) -> CacheResult<Vec<Oid>> {
        let rows = self
            .db_origin
            .query(direction.closure_sql(), &[&relation_oid])
            .await
            .map_into_report::<CacheError>()?;
        Ok(rows.iter().map(|row| row.get::<_, Oid>(0)).collect())
    }

    /// Whether the relation has children that are not partitions: plain
    /// table inheritance.
    #[instrument(skip_all)]
    async fn inheritance_children_exist(&self, relation_oid: Oid) -> CacheResult<bool> {
        let row = self
            .db_origin
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_inherits i \
                 JOIN pg_class c ON c.oid = i.inhrelid \
                 WHERE i.inhparent = $1 AND NOT c.relispartition)",
                &[&relation_oid],
            )
            .await
            .map_into_report::<CacheError>()?;
        Ok(row.get(0))
    }

    /// Whether every partition that holds rows writes old tuples carrying
    /// `key`: REPLICA IDENTITY FULL, or an identity (the primary key by
    /// default, or the index named) whose columns include it.
    #[instrument(skip_all)]
    async fn partitions_carry_key(
        &self,
        partitions: &[Oid],
        key: &[EcoString],
    ) -> CacheResult<bool> {
        if partitions.is_empty() {
            return Ok(true);
        }
        let key: Vec<&str> = key.iter().map(EcoString::as_str).collect();
        let row = self
            .db_origin
            .query_one(
                "SELECT NOT EXISTS ( \
                     SELECT 1 FROM pg_class c \
                     WHERE c.oid = ANY($1) AND c.relkind <> 'p' AND c.relreplident <> 'f' \
                       AND NOT $2::text[] <@ coalesce(( \
                           SELECT array_agg(a.attname::text) \
                           FROM pg_index i \
                           JOIN pg_attribute a \
                             ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
                           WHERE i.indrelid = c.oid \
                             AND ((c.relreplident = 'd' AND i.indisprimary) \
                               OR (c.relreplident = 'i' AND i.indisreplident))), '{}'))",
                &[&partitions, &key],
            )
            .await
            .map_into_report::<CacheError>()?;
        Ok(row.get(0))
    }

    /// Whether every partition that holds rows is REPLICA IDENTITY FULL.
    #[instrument(skip_all)]
    async fn partitions_identity_full(&self, partitions: &[Oid]) -> CacheResult<bool> {
        if partitions.is_empty() {
            return Ok(true);
        }
        let row = self
            .db_origin
            .query_one(
                "SELECT coalesce(bool_and(relreplident = 'f'), true) FROM pg_class \
                 WHERE oid = ANY($1) AND relkind <> 'p'",
                &[&partitions],
            )
            .await
            .map_into_report::<CacheError>()?;
        Ok(row.get(0))
    }

    /// The parents of an inheritance child, as origin holds them now. Empty
    /// for a partition, a table with no parent, and a relation dropped since.
    #[instrument(skip_all)]
    pub(super) async fn inheritance_parents_get(&self, relation_oid: Oid) -> CacheResult<Vec<Oid>> {
        let rows = self
            .db_origin
            .query(
                "SELECT i.inhparent FROM pg_inherits i \
                 JOIN pg_class c ON c.oid = i.inhrelid \
                 WHERE i.inhrelid = $1 AND NOT c.relispartition",
                &[&relation_oid],
            )
            .await
            .map_into_report::<CacheError>()?;
        Ok(rows.iter().map(|row| row.get::<_, Oid>(0)).collect())
    }

    /// The relations a write can name whose rows are a root's: every
    /// partition below and above it, and the views over any of those, views
    /// over views included (`pg_depend` from a view's rewrite rule to the
    /// relations it reads). Keyed by root; a root with no aliases has no entry.
    #[instrument(skip_all)]
    pub(super) async fn query_write_aliases_get(
        &self,
        roots: &[Oid],
    ) -> CacheResult<HashMap<Oid, Vec<WriteAlias>>> {
        if roots.is_empty() {
            return Ok(HashMap::new());
        }
        let sql = r"
            WITH RECURSIVE down(root, oid) AS (
                SELECT oid, oid FROM unnest($1::oid[]) AS r(oid)
                UNION
                SELECT d.root, i.inhrelid FROM pg_inherits i JOIN down d ON i.inhparent = d.oid
            ), up(root, oid) AS (
                SELECT oid, oid FROM unnest($1::oid[]) AS r(oid)
                UNION
                SELECT u.root, i.inhparent FROM pg_inherits i JOIN up u ON i.inhrelid = u.oid
            ), related(root, oid) AS (
                (SELECT root, oid FROM down UNION SELECT root, oid FROM up)
                UNION
                SELECT v.root, r.ev_class
                  FROM related v
                  JOIN pg_depend d ON d.refclassid = 'pg_class'::regclass AND d.refobjid = v.oid
                  JOIN pg_rewrite r ON d.classid = 'pg_rewrite'::regclass AND r.oid = d.objid
                  JOIN pg_class c ON c.oid = r.ev_class AND c.relkind = 'v'
                 WHERE r.ev_class <> v.oid
            )
            SELECT rel.root::oid, n.nspname, c.relname, c.relkind = 'v' AS is_view
              FROM related rel
              JOIN pg_class c ON c.oid = rel.oid
              JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE rel.oid <> rel.root
        ";
        let rows = self
            .db_origin
            .query(sql, &[&roots])
            .await
            .map_into_report::<CacheError>()?;
        let mut aliases: HashMap<Oid, Vec<WriteAlias>> = HashMap::new();
        for row in rows {
            let schema: &str = row.get(1);
            let name: &str = row.get(2);
            aliases
                .entry(row.get::<_, Oid>(0))
                .or_default()
                .push(WriteAlias {
                    relation: RelationName {
                        schema: schema.into(),
                        name: name.into(),
                    },
                    is_view: row.get(3),
                });
        }
        Ok(aliases)
    }

    /// Republish the write-target map the read-after-write gate resolves a
    /// forwarded write with (ADR-048, PGC-542). Called after every change to
    /// the cached tables or their alias lists.
    pub(super) fn write_targets_publish(&self) {
        let map = WriteTargetMap::from_tables(self.cache.tables.iter().map(|table| &**table));
        self.state_view.write_targets.store(Arc::new(map));
    }

    /// Reload the alias lists of the cached tables in `roots` from the origin
    /// catalog and republish the map.
    pub(super) async fn write_aliases_reload(&mut self, roots: &[Oid]) -> CacheResult<()> {
        let mut aliases = self.query_write_aliases_get(roots).await?;
        for root in roots {
            let Some(table) = self.cache.tables.get1(root) else {
                continue;
            };
            let write_aliases = aliases.remove(root).unwrap_or_default();
            if table.write_aliases == write_aliases {
                continue;
            }
            let mut table = (**table).clone();
            table.write_aliases = write_aliases;
            self.cache.tables.insert_overwrite(table.into());
        }
        self.write_targets_publish();
        Ok(())
    }

    /// Fill the primary key a Relation message left empty under FULL or
    /// NOTHING (PGC-530) from the catalog, which also catches a key redefined
    /// without the schema-change signal. If the catalog can't be read, the
    /// registered table's key stands in rather than failing the stream.
    pub(super) async fn table_metadata_primary_key_resolve(
        &self,
        metadata: &mut TableMetadata,
    ) -> CacheResult<()> {
        if !metadata.primary_key_columns.is_empty() {
            return Ok(());
        }
        let key = match self
            .query_key_columns_get(KeyColumns::PrimaryKey, metadata.relation_oid)
            .await
        {
            Ok(key) => key,
            Err(e) => {
                let Some(table) = self.cache.tables.get1(&metadata.relation_oid) else {
                    return Err(e);
                };
                warn!(
                    "reading the primary key of {}.{} failed, keeping the registered key: {}",
                    metadata.schema,
                    metadata.name,
                    error_chain_format(e.current_context())
                );
                table.primary_key_columns.clone()
            }
        };
        if key.is_empty() {
            return Ok(());
        }
        let columns: Vec<ColumnMetadata> = metadata
            .columns
            .iter()
            .map(|column| ColumnMetadata {
                is_primary_key: key.contains(&column.name),
                ..column.clone()
            })
            .collect();
        metadata.columns = ColumnStore::new(columns);
        metadata.primary_key_columns = key;
        Ok(())
    }

    /// A key's columns in column order; empty when the relation has none.
    async fn query_key_columns_get(
        &self,
        key: KeyColumns,
        relation_oid: Oid,
    ) -> CacheResult<Vec<EcoString>> {
        let rows = self
            .db_origin
            .query(key.columns_sql(), &[&relation_oid])
            .await
            .map_into_report::<CacheError>()?;
        rows.iter()
            .map(|row| {
                row.try_get::<_, &str>(0)
                    .map(EcoString::from)
                    .map_into_report::<CacheError>()
            })
            .collect()
    }

    #[instrument(skip_all)]
    /// The origin relation's REPLICA IDENTITY (`pg_class.relreplident`).
    /// Absent rows (dropped mid-registration) read as DEFAULT — the
    /// conservative old-image ladder then applies.
    async fn query_replica_identity_get(&self, relation_oid: Oid) -> CacheResult<ReplicaIdentity> {
        let rows = self
            .db_origin
            .query(
                "SELECT relreplident::text AS identity FROM pg_class WHERE oid = $1",
                &[&relation_oid],
            )
            .await
            .map_into_report::<CacheError>()?;
        let identity = match rows.first() {
            Some(row) => row
                .try_get::<_, &str>("identity")
                .map_into_report::<CacheError>()?,
            None => "d",
        };
        Ok(match identity {
            "f" => ReplicaIdentity::Full,
            "i" => ReplicaIdentity::Index,
            "n" => ReplicaIdentity::Nothing,
            _ => ReplicaIdentity::Default,
        })
    }

    /// Whether `pg_class.relrowsecurity` is set on the origin relation. An
    /// absent row (dropped mid-stream) reads as not secured.
    pub(super) async fn query_row_security_get(&self, relation_oid: Oid) -> CacheResult<bool> {
        let rows = self
            .db_origin
            .query(
                "SELECT relrowsecurity FROM pg_class WHERE oid = $1",
                &[&relation_oid],
            )
            .await
            .map_into_report::<CacheError>()?;
        rows.first()
            .map(|row| row.try_get(0).map_into_report::<CacheError>())
            .unwrap_or(Ok(false))
    }

    pub(super) async fn query_table_indexes_get(
        &self,
        relation_oid: Oid,
    ) -> CacheResult<Vec<IndexMetadata>> {
        // For partitioned tables, query indexes from a child partition.
        // Falls back to parent (works for regular tables and partitioned tables with no children).
        let child_oid = self.partition_child_find(relation_oid).await?;
        let target_oid = child_oid.unwrap_or(relation_oid);

        let sql = r"
            SELECT
                i.relname AS index_name,
                ix.indisunique AS is_unique,
                pg_get_indexdef(ix.indexrelid) AS definition
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_class t ON t.oid = ix.indrelid
            WHERE t.oid = $1
              AND NOT ix.indisprimary
            ORDER BY i.relname;
        ";

        let rows = self
            .db_origin
            .query(sql, &[&target_oid])
            .await
            .map_into_report::<CacheError>()?;

        let indexes = rows
            .iter()
            .map(|row| IndexMetadata {
                name: row.get::<_, String>("index_name").into(),
                is_unique: row.get("is_unique"),
                definition: row.get::<_, String>("definition").into(),
            })
            .collect();

        Ok(indexes)
    }

    /// The schema an unqualified table name resolves to under `search_path`:
    /// the first schema on the path that holds a table of that name on origin,
    /// memoized per (path, name) in `schema_resolutions`.
    #[instrument(skip_all)]
    pub(super) async fn schema_for_table_find(
        &mut self,
        table_name: &str,
        search_path: &Arc<[EcoString]>,
    ) -> CacheResult<EcoString> {
        if let Some(schema) = self
            .schema_resolutions
            .get(search_path.as_ref())
            .and_then(|by_name| by_name.get(table_name))
        {
            return Ok(schema.clone());
        }
        let schema = self.schema_for_table_query(table_name, search_path).await?;
        self.schema_resolutions
            .entry(Arc::clone(search_path))
            .or_default()
            .insert(EcoString::from(table_name), schema.clone());
        Ok(schema)
    }

    async fn schema_for_table_query(
        &self,
        table_name: &str,
        search_path: &[EcoString],
    ) -> CacheResult<EcoString> {
        let search_path: Vec<&str> = search_path.iter().map(EcoString::as_str).collect();
        let sql = r"
            SELECT n.nspname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = $1
            AND c.relkind IN ('r', 'p')
            AND n.nspname = any($2)
            ORDER BY array_position($2::text[], n.nspname::text)
            LIMIT 1;
        ";

        let rows = self
            .db_origin
            .query(sql, &[&table_name, &search_path])
            .await
            .map_into_report::<CacheError>()?;

        rows.first()
            .map(|row| EcoString::from(row.get::<_, &str>(0)))
            .ok_or_else(|| {
                CacheError::UnknownTable {
                    oid: None,
                    name: Some(table_name.to_owned()),
                }
                .into()
            })
    }

    #[instrument(skip_all)]
    pub(super) async fn cache_table_create_from_metadata(
        &self,
        table_metadata: &TableMetadata,
    ) -> CacheResult<()> {
        let ddl = cache_table_ddl_build(table_metadata);
        for sql in [&ddl.create_schema, &ddl.drop_table, &ddl.create_table] {
            self.db_cache
                .execute(sql, &[])
                .await
                .map_into_report::<CacheError>()?;
        }
        self.cache_indexes_create(table_metadata).await;
        self.db_cache
            .execute(&ddl.enable_tracking, &[])
            .await
            .map_into_report::<CacheError>()?;
        Ok(())
    }

    /// Best-effort: an index is a performance optimization, not a
    /// correctness requirement. An expression index may reference a function
    /// absent from the cache DB; let it fail without aborting table caching.
    async fn cache_indexes_create(&self, table_metadata: &TableMetadata) {
        let schema = escape::escape_identifier(&table_metadata.schema);
        let table = escape::escape_identifier(&table_metadata.name);
        for index in &table_metadata.indexes {
            let Some(index_sql) = index_sql_retarget(
                &index.definition,
                index.is_unique,
                &table_metadata.schema,
                &table_metadata.name,
                &table_metadata.primary_key_columns,
            ) else {
                warn!(
                    "skipping index {} on {schema}.{table}: no USING clause in definition",
                    index.name
                );
                continue;
            };
            if let Err(e) = self
                .db_cache
                .execute(&index_sql, &[])
                .await
                .map_into_report::<CacheError>()
            {
                warn!(
                    "creating index {} on {schema}.{table}: {}",
                    index.name,
                    error_chain_format(e.current_context()),
                );
            }
        }
    }

    /// Drop the source-row cache tables for `relation_oids` and remove their
    /// metadata. Dropping the table unlinks its files, returning the disk space
    /// to the cache volume (observed by a later tick's statvfs read, PGC-276).
    ///
    /// Best-effort: logs and continues on DB failures rather than aborting
    /// the eviction path. Oids without metadata are silently skipped.
    #[instrument(skip(self, relation_oids))]
    pub(super) async fn cache_tables_drop(&mut self, relation_oids: &[Oid]) {
        let tables: Vec<CatalogTable> = relation_oids
            .iter()
            .filter_map(|oid| self.cache.tables.remove1(*oid))
            .collect();
        if !tables.is_empty() {
            self.write_targets_publish();
        }
        self.cache_table_objects_drop(&tables).await;
    }

    /// Drop the cache tables of metadata a schema-change signal already
    /// removed from `cache.tables`. One recreated since under the same name
    /// (a registration, or a Relation message) replaced the object on the way
    /// in, so dropping by name now would take the new table with it.
    pub(super) async fn forgotten_tables_drop(&mut self) {
        if self.forgotten_tables.is_empty() {
            return;
        }
        let forgotten = std::mem::take(&mut self.forgotten_tables);
        let orphaned: Vec<CatalogTable> = forgotten
            .into_iter()
            .filter(|table| {
                !self
                    .cache
                    .tables
                    .contains_key2(&(table.schema.as_str(), table.name.as_str()))
            })
            .collect();
        self.cache_table_objects_drop(&orphaned).await;
    }

    async fn cache_table_objects_drop(&mut self, tables: &[CatalogTable]) {
        let quoted: Vec<String> = tables
            .iter()
            .map(|table| {
                format!(
                    "{}.{}",
                    escape::escape_identifier(&table.schema),
                    escape::escape_identifier(&table.name)
                )
            })
            .collect();
        if quoted.is_empty() {
            return;
        }
        let sql = format!("DROP TABLE IF EXISTS {}", quoted.join(", "));
        debug!("dropping {} orphaned cache table(s)", quoted.len());
        if let Err(e) = self
            .db_cache
            .execute(&sql, &[])
            .await
            .map_into_report::<CacheError>()
        {
            error!(
                "dropping orphaned cache tables: {}",
                error_chain_format(e.current_context()),
            );
        }
    }

    /// The cached tables a signal on `relation_oid` lands on: the tables
    /// whose partition tree holds it, and every cached partition under it
    /// (an `ALTER TABLE` on a partitioned root reaches its partitions).
    /// The cached tables origin holds in `namespace_oid`: the ones an `ALTER
    /// SCHEMA` renamed or re-owned, which the signal does not list.
    pub(super) async fn tables_in_namespace(&self, namespace_oid: Oid) -> CacheResult<Vec<Oid>> {
        let cached: Vec<Oid> = self
            .cache
            .tables
            .iter()
            .map(|table| table.relation_oid)
            .collect();
        if cached.is_empty() {
            return Ok(Vec::new());
        }
        let rows = self
            .db_origin
            .query(
                "SELECT oid FROM pg_class WHERE relnamespace = $1 AND oid = ANY($2)",
                &[&namespace_oid, &cached],
            )
            .await
            .map_into_report::<CacheError>()?;
        rows.iter()
            .map(|row| row.try_get(0).map_into_report::<CacheError>())
            .collect()
    }

    pub(super) fn tables_over_relation(&self, relation_oid: Oid) -> Vec<Oid> {
        self.cache
            .tables
            .iter()
            .filter(|table| {
                table_tree_holds(table, relation_oid) || table.ancestors.contains(&relation_oid)
            })
            .map(|table| table.relation_oid)
            .collect()
    }

    /// The cached tables whose own partition tree holds `relation_oid`: the
    /// relation itself when cached, and every cached table above it (a
    /// partition's rows are its root's; a cached mid-level partition counts
    /// too).
    fn tables_holding_relation(&self, relation_oid: Oid) -> Vec<Oid> {
        self.cache
            .tables
            .iter()
            .filter(|table| table_tree_holds(table, relation_oid))
            .map(|table| table.relation_oid)
            .collect()
    }

    /// A partition created under a cached table (directly or under one of its
    /// partitions): extend the partition list so a later truncate or drop of
    /// it resolves to the table, and reload the write aliases so a write to it
    /// does. A registration that loaded the catalog after the create already
    /// lists it.
    pub(super) async fn table_partition_add(
        &mut self,
        created: PartitionCreated,
    ) -> CacheResult<()> {
        let owners = self.tables_holding_relation(created.parent);
        for owner in &owners {
            let Some(table) = self.cache.tables.get1(owner) else {
                continue;
            };
            if table.partitions.contains(&created.partition) {
                continue;
            }
            let mut table = (**table).clone();
            table.partitions.push(created.partition);
            self.cache.tables.insert_overwrite(table.into());
        }
        self.stream_relations_publish();
        self.write_aliases_reload(&owners).await
    }
}

fn table_tree_holds(table: &TableMetadata, relation_oid: Oid) -> bool {
    table.relation_oid == relation_oid || table.partitions.contains(&relation_oid)
}

impl WriterCore {
    /// Register table metadata from CDC processing.
    #[instrument(skip_all)]
    pub(super) async fn cache_table_register(
        &mut self,
        mut table_metadata: TableMetadata,
    ) -> CacheResult<()> {
        let relation_oid = table_metadata.relation_oid;

        let table_exists = self.cache.tables.contains_key1(&relation_oid);
        if table_exists {
            if let Some(current_table) = self.cache.tables.get1(&relation_oid)
                && current_table.schema_eq(&table_metadata)
            {
                // Without the schema-change signal a replica identity change
                // arrives only here; the shape and its queries stand.
                if current_table.replica_identity_full != table_metadata.replica_identity_full {
                    let updated = TableMetadata {
                        replica_identity_full: table_metadata.replica_identity_full,
                        ..TableMetadata::clone(current_table)
                    };
                    self.cache.tables.insert_overwrite(updated.into());
                }
                return Ok(());
            }

            info!(
                "Table {} (OID: {}) recreating table, invalidating queries",
                table_metadata.name, relation_oid
            );

            self.cache_table_invalidate(relation_oid).await?;
            // The cache table is about to be recreated with a new shape; discard
            // the relation's pooled staging tables so no population reuses an
            // old-shape one (PGC-293 review).
            self.staging_pool_relation_purge(relation_oid).await;
        }

        // A Relation message carries columns only; the catalog-only facts
        // are read fresh (a stale partition tree would lose a drop signal).
        if table_metadata.indexes.is_empty() {
            table_metadata.indexes = self.query_table_indexes_get(relation_oid).await?;
        }
        table_metadata.partitions = self.query_table_partitions_get(relation_oid).await?;
        table_metadata.ancestors = self.query_table_ancestors_get(relation_oid).await?;
        table_metadata.write_aliases = self
            .query_write_aliases_get(&[relation_oid])
            .await?
            .remove(&relation_oid)
            .unwrap_or_default();

        self.cache_table_create_from_metadata(&table_metadata)
            .await?;

        self.cache.tables.insert_overwrite(table_metadata.into());
        self.write_targets_publish();
        self.stream_relations_publish();

        Ok(())
    }
}

/// A relation's REPLICA IDENTITY setting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplicaIdentity {
    Default,
    Full,
    Index,
    Nothing,
}

/// The column projection every table-metadata read starts from; a scope
/// clause and [`TABLE_COLUMNS_TAIL`] complete it.
const TABLE_COLUMNS_SELECT: &str = r"
    SELECT
        c.oid AS relation_oid,
        n.nspname AS table_schema,
        c.relname AS table_name,
        a.attname AS column_name,
        RANK() OVER (order by a.attnum) AS position,
        a.atttypid AS type_oid,
        pg_catalog.format_type(a.atttypid, a.atttypmod) as type_name,
        a.attnum = any(pgc.conkey) as is_primary_key,
        c.relrowsecurity AS row_security,
        co.collprovider::text AS collation_provider,
        co.collcollate AS collation_collate,
        co.collisdeterministic AS collation_deterministic,
        con.nspname AS collation_schema,
        co.collname AS collation_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_attribute a ON a.attrelid = c.oid
    JOIN pg_type t ON t.oid = a.atttypid
    JOIN pg_constraint pgc ON pgc.conrelid = c.oid
    LEFT JOIN pg_collation co ON co.oid = a.attcollation AND a.attcollation <> 100
    LEFT JOIN pg_namespace con ON con.oid = co.collnamespace
    WHERE c.relname = $1
    AND ";

/// Each live column's declared collation, for a relation named by oid. The
/// join leaves the `pg_collation` columns NULL for the database default
/// (oid 100) and for a type without a collation, as [`TABLE_COLUMNS_SELECT`]
/// does.
const RELATION_COLLATIONS_SQL: &str = r"
    SELECT
        a.attname AS column_name,
        co.collprovider::text AS collation_provider,
        co.collcollate AS collation_collate,
        co.collisdeterministic AS collation_deterministic,
        con.nspname AS collation_schema,
        co.collname AS collation_name
    FROM pg_attribute a
    LEFT JOIN pg_collation co ON co.oid = a.attcollation AND a.attcollation <> 100
    LEFT JOIN pg_namespace con ON con.oid = co.collnamespace
    WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped";

/// A row when the connected database has collation `$1.$2` for its encoding.
const COLLATION_PRESENT_SQL: &str = r"
    SELECT 1
    FROM pg_collation co
    JOIN pg_namespace n ON n.oid = co.collnamespace
    WHERE n.nspname = $1 AND co.collname = $2
    AND co.collencoding IN (
        -1, (SELECT encoding FROM pg_database WHERE datname = current_database())
    )";

/// The table in the schema given as `$2`.
const TABLE_COLUMNS_NAMED_SCHEMA: &str = "n.nspname = $2";

/// The first table of that name along the session's search path.
const TABLE_COLUMNS_SEARCH_PATH: &str = r"c.oid = (
        SELECT c2.oid
        FROM pg_class c2
        JOIN pg_namespace n2 ON n2.oid = c2.relnamespace
        WHERE c2.relname = $1
        AND c2.relkind IN ('r', 'p')
        AND n2.nspname = any(current_schemas(false))
        ORDER BY array_position(current_schemas(false), n2.nspname)
        LIMIT 1
    )";

const TABLE_COLUMNS_TAIL: &str = r"
    AND a.attnum > 0
    AND pgc.contype = 'p'
    AND NOT a.attisdropped
    ORDER BY a.attnum";

/// A direction through `pg_inherits`.
#[derive(Clone, Copy)]
enum InheritsDirection {
    Descendants,
    Ancestors,
}

impl InheritsDirection {
    /// The transitive closure of `$1` in this direction, one oid per row.
    fn closure_sql(self) -> &'static str {
        match self {
            Self::Descendants => {
                r"
                WITH RECURSIVE descendants(oid) AS (
                    SELECT inhrelid FROM pg_inherits WHERE inhparent = $1
                    UNION
                    SELECT i.inhrelid FROM pg_inherits i JOIN descendants d ON i.inhparent = d.oid
                )
                SELECT oid::oid FROM descendants
                "
            }
            Self::Ancestors => {
                r"
                WITH RECURSIVE ancestors(oid) AS (
                    SELECT inhparent FROM pg_inherits WHERE inhrelid = $1
                    UNION
                    SELECT i.inhparent FROM pg_inherits i JOIN ancestors a ON i.inhrelid = a.oid
                )
                SELECT oid::oid FROM ancestors
                "
            }
        }
    }
}

/// A key of relation `$1` whose column names the cache keys on.
#[derive(Clone, Copy)]
enum KeyColumns {
    /// The primary key's columns, as registration reads them.
    PrimaryKey,
    /// The replica identity index's columns.
    ReplicaIdentityIndex,
}

impl KeyColumns {
    /// The key's column names in column order, one per row.
    fn columns_sql(self) -> &'static str {
        match self {
            Self::PrimaryKey => {
                "SELECT a.attname FROM pg_constraint pgc \
                 JOIN pg_attribute a ON a.attrelid = pgc.conrelid AND a.attnum = any(pgc.conkey) \
                 WHERE pgc.conrelid = $1 AND pgc.contype = 'p' AND NOT a.attisdropped \
                 ORDER BY a.attnum"
            }
            Self::ReplicaIdentityIndex => {
                "SELECT a.attname FROM pg_index i \
                 JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = any(i.indkey) \
                 WHERE i.indrelid = $1 AND i.indisreplident AND NOT a.attisdropped \
                 ORDER BY a.attnum"
            }
        }
    }
}

/// The names of the columns marked primary key, in column order.
fn primary_key_columns_of(columns: &[ColumnMetadata]) -> Vec<EcoString> {
    columns
        .iter()
        .filter(|column| column.is_primary_key)
        .map(|column| column.name.clone())
        .collect()
}

/// Marks exactly the columns in `key` as the primary key.
fn key_columns_mark(columns: &mut [ColumnMetadata], key: &[EcoString]) {
    for column in columns {
        column.is_primary_key = key.contains(&column.name);
    }
}

/// The statements that create a source-row cache table: schema, drop of a
/// previous copy, the unlogged table itself, and the generation-tracking
/// trigger enable that runs once its indexes are in place.
struct CacheTableDdl {
    create_schema: String,
    drop_table: String,
    create_table: String,
    enable_tracking: String,
}

fn cache_table_ddl_build(table_metadata: &TableMetadata) -> CacheTableDdl {
    let schema = escape::escape_identifier(&table_metadata.schema);
    let table = escape::escape_identifier(&table_metadata.name);
    let column_defs: Vec<String> = table_metadata
        .columns
        .iter()
        .map(|c| {
            let collate = c.collation.name.as_ref().map_or_else(String::new, |n| {
                format!(
                    " COLLATE {}.{}",
                    escape::escape_identifier(&n.schema),
                    escape::escape_identifier(&n.name)
                )
            });
            format!(
                "    {} {}{collate}",
                escape::escape_identifier(&c.name),
                c.cache_type_name
            )
        })
        .collect();
    let column_defs = column_defs.join(",\n");
    let primary_key = table_metadata
        .primary_key_columns
        .iter()
        .map(|c| escape::escape_identifier(c))
        .collect::<Vec<_>>()
        .join(", ");
    CacheTableDdl {
        create_schema: format!("CREATE SCHEMA IF NOT EXISTS {schema}"),
        drop_table: format!("DROP TABLE IF EXISTS {schema}.{table}"),
        create_table: format!(
            "CREATE UNLOGGED TABLE {schema}.{table} (\n{column_defs},\n\tPRIMARY KEY({primary_key})\n)"
        ),
        enable_tracking: format!(
            "SELECT pgcache_enable_tracking({}::regclass::oid)",
            escape::escape_literal(&format!("{schema}.{table}"))
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_table_ddl_build_quotes_and_keys() {
        let column = |name: &str, is_primary_key: bool| ColumnMetadata {
            name: name.into(),
            position: ColumnPosition::from_raw(1),
            type_oid: TypeOid::from_raw(23),
            data_type: Type::INT4,
            type_name: "int4".into(),
            cache_type_name: "int4".into(),
            is_primary_key,
            collation: ColumnCollation::BYTES,
        };
        let metadata =
            TableMetadata::for_test("Posts", 1, [column("id", true), column("order", false)]);
        let ddl = cache_table_ddl_build(&metadata);
        assert_eq!(ddl.create_schema, "CREATE SCHEMA IF NOT EXISTS \"public\"");
        assert_eq!(ddl.drop_table, "DROP TABLE IF EXISTS \"public\".\"Posts\"");
        assert_eq!(
            ddl.create_table,
            "CREATE UNLOGGED TABLE \"public\".\"Posts\" (\n    \"id\" int4,\n    \"order\" int4,\n\tPRIMARY KEY(\"id\")\n)"
        );
        assert_eq!(
            ddl.enable_tracking,
            "SELECT pgcache_enable_tracking('\"public\".\"Posts\"'::regclass::oid)"
        );
    }

    /// A column that declares a collation is created under it; one on the
    /// database default names none.
    #[test]
    fn test_cache_table_ddl_build_names_declared_collations() {
        let column = |name: &str, collation: ColumnCollation| ColumnMetadata {
            name: name.into(),
            position: ColumnPosition::from_raw(1),
            type_oid: TypeOid::from_raw(25),
            data_type: Type::TEXT,
            type_name: "text".into(),
            cache_type_name: "text".into(),
            is_primary_key: name == "id",
            collation,
        };
        let declared = ColumnCollation {
            name: Some(CollationName {
                schema: "pg_catalog".into(),
                name: "C".into(),
            }),
            text_order: TextOrder::Bytes,
        };
        let metadata = TableMetadata::for_test(
            "t",
            1,
            [
                column("id", ColumnCollation::LINGUISTIC),
                column("code", declared),
            ],
        );
        assert_eq!(
            cache_table_ddl_build(&metadata).create_table,
            "CREATE UNLOGGED TABLE \"public\".\"t\" (\n    \"id\" text,\n    \"code\" text COLLATE \"pg_catalog\".\"C\",\n\tPRIMARY KEY(\"id\")\n)"
        );
    }
}
