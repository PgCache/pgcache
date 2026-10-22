use std::collections::HashMap;
use std::sync::Arc;

use ecow::EcoString;
use postgres_protocol::escape;
use rootcause::Report;
use tokio_postgres::Row;
use tokio_postgres::types::{ToSql, Type};
use tracing::{debug, error, info, instrument, warn};

use super::core::WriterCore;
use crate::cache::messages::PartitionCreated;
use crate::cache::types::WriteAliasMap;
use crate::cache::{CacheError, CacheResult, MapIntoReport};
use crate::catalog::{
    CatalogTable, ColumnMetadata, ColumnPosition, ColumnStore, IndexMetadata, RelationName,
    TableMetadata, WriteAlias, cache_type_name_resolve,
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

        Ok(TableMetadata {
            replica_identity_full: identity == ReplicaIdentity::Full,
            name: table.into(),
            schema: schema.into(),
            relation_oid,
            primary_key_columns,
            columns: ColumnStore::new(columns),
            indexes,
            partitions,
            ancestors,
            write_aliases,
        })
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
        })
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

    /// Republish the write-alias map the read-after-write gate expands a
    /// forwarded write with (ADR-048). Called after every change to the
    /// cached tables or their alias lists.
    pub(super) fn write_aliases_publish(&self) {
        let map = WriteAliasMap::from_tables(self.cache.tables.iter().map(|table| &**table));
        self.state_view.write_aliases.store(Arc::new(map));
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
        self.write_aliases_publish();
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
            self.write_aliases_publish();
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
    /// relation itself when cached, and every cached table above it (under
    /// `publish_via_partition_root` a partition's rows are its root's; a
    /// cached mid-level partition counts too).
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
        self.write_aliases_reload(&owners).await
    }
}

fn table_tree_holds(table: &TableMetadata, relation_oid: Oid) -> bool {
    table.relation_oid == relation_oid || table.partitions.contains(&relation_oid)
}

/// Retarget a canonical `pg_get_indexdef` statement onto the cache table.
///
/// `pg_get_indexdef` emits `CREATE [UNIQUE] INDEX name ON schema.tbl USING ...`.
/// Everything from `USING` onward references only column names — predicates and
/// expressions never qualify the table — so we rebuild an anonymous index
/// against the cache's own schema/table. This also retargets indexes read from
/// a child partition, whose definition names the child rather than the parent
/// cache table. Returns `None` if the definition has no `USING` clause.
fn index_sql_retarget(
    definition: &str,
    is_unique: bool,
    schema: &str,
    table: &str,
) -> Option<String> {
    let (_, using) = definition.split_once(" USING ")?;
    let unique = if is_unique { "UNIQUE " } else { "" };
    Some(format!(
        "CREATE {unique}INDEX ON {}.{} USING {using}",
        escape::escape_identifier(schema),
        escape::escape_identifier(table)
    ))
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
        self.write_aliases_publish();

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
        a.attnum = any(pgc.conkey) as is_primary_key
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_attribute a ON a.attrelid = c.oid
    JOIN pg_type t ON t.oid = a.atttypid
    JOIN pg_constraint pgc ON pgc.conrelid = c.oid
    WHERE c.relname = $1
    AND ";

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
            format!(
                "    {} {}",
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
    fn test_index_sql_retarget_partial() {
        let def = "CREATE INDEX posts_owneruserid_idx ON public.posts USING btree (owneruserid) WHERE (owneruserid IS NOT NULL)";
        let sql =
            index_sql_retarget(def, false, "public", "posts").expect("retarget partial index");
        assert_eq!(
            sql,
            "CREATE INDEX ON \"public\".\"posts\" USING btree (owneruserid) WHERE (owneruserid IS NOT NULL)"
        );
    }

    #[test]
    fn test_index_sql_retarget_unique_desc() {
        let def = "CREATE UNIQUE INDEX foo_idx ON public.foo USING btree (created_at DESC, name)";
        let sql = index_sql_retarget(def, true, "public", "foo").expect("retarget unique index");
        assert_eq!(
            sql,
            "CREATE UNIQUE INDEX ON \"public\".\"foo\" USING btree (created_at DESC, name)"
        );
    }

    #[test]
    fn test_index_sql_retarget_partition_child_retargeted() {
        // Definition read from a child partition names the child; output must
        // target the parent cache table.
        let def = "CREATE INDEX posts_2023_parentid_idx ON public.posts_2023 USING btree (parentid) WHERE (parentid IS NOT NULL)";
        let sql = index_sql_retarget(def, false, "public", "posts").expect("retarget child index");
        assert_eq!(
            sql,
            "CREATE INDEX ON \"public\".\"posts\" USING btree (parentid) WHERE (parentid IS NOT NULL)"
        );
    }

    #[test]
    fn test_index_sql_retarget_no_using_returns_none() {
        assert!(index_sql_retarget("not an index def", false, "public", "posts").is_none());
    }

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
}
