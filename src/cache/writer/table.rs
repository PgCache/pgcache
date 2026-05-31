use ecow::EcoString;
use tokio_postgres::Row;
use tokio_postgres::types::Type;
use tracing::{debug, error, instrument};

use crate::catalog::{
    ColumnMetadata, ColumnStore, IndexMetadata, TableMetadata, cache_type_name_resolve,
};
use crate::result::error_chain_format;

use super::super::{CacheError, CacheResult, MapIntoReport};
use super::core::WriterCore;

impl WriterCore {
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

        let mut primary_key_columns: Vec<EcoString> = Vec::new();
        let mut columns: Vec<ColumnMetadata> = Vec::with_capacity(rows.len());
        let mut relation_oid: Option<u32> = None;
        let mut schema: Option<&str> = schema;

        for row in &rows {
            if relation_oid.is_none() {
                relation_oid = Some(row.get("relation_oid"));
            }

            if schema.is_none() {
                schema = Some(row.get("table_schema"));
            }

            let type_oid: u32 = row.get("type_oid");

            // Try built-in types first (fast path), then discover custom types from origin
            let data_type = match Type::from_oid(type_oid) {
                Some(t) => t,
                None => self
                    .db_origin
                    .get_type(type_oid)
                    .await
                    .map_into_report::<CacheError>()?,
            };

            let type_name = data_type.name().to_owned();
            let cache_type_name = cache_type_name_resolve(&data_type)?;
            let pg_position: i64 = row.get("position");

            let column = ColumnMetadata {
                name: row.get::<_, String>("column_name").into(),
                position: i16::try_from(pg_position).expect("column position fits in i16"),
                type_oid,
                data_type,
                type_name: type_name.into(),
                cache_type_name: cache_type_name.into(),
                is_primary_key: row.get("is_primary_key"),
            };

            if column.is_primary_key {
                primary_key_columns.push(column.name.clone());
            }

            columns.push(column);
        }

        let Some(relation_oid) = relation_oid else {
            return Err(CacheError::UnknownTable {
                oid: relation_oid,
                name: None,
            }
            .into());
        };

        let Some(schema) = schema else {
            return Err(CacheError::UnknownSchema.into());
        };

        let indexes = self.query_table_indexes_get(relation_oid).await?;

        let table = TableMetadata {
            name: table.into(),
            schema: schema.into(),
            relation_oid,
            primary_key_columns,
            columns: ColumnStore::new(columns),
            indexes,
        };

        Ok(table)
    }

    #[instrument(skip_all)]
    pub(super) async fn query_table_columns_get(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> CacheResult<Vec<Row>> {
        let rows = if let Some(schema) = schema {
            let sql = r"
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
                AND n.nspname = $2
                AND a.attnum > 0
                AND pgc.contype = 'p'
                AND NOT a.attisdropped
                ORDER BY a.attnum;
            ";

            self.db_origin
                .query(sql, &[&table, &schema])
                .await
                .map_into_report::<CacheError>()?
        } else {
            let sql = r"
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
                AND c.oid = (
                    SELECT c2.oid
                    FROM pg_class c2
                    JOIN pg_namespace n2 ON n2.oid = c2.relnamespace
                    WHERE c2.relname = $1
                    AND c2.relkind IN ('r', 'p')
                    AND n2.nspname = any(current_schemas(false))
                    ORDER BY array_position(current_schemas(false), n2.nspname)
                    LIMIT 1
                )
                AND a.attnum > 0
                AND pgc.contype = 'p'
                AND NOT a.attisdropped
                ORDER BY a.attnum;
            ";

            self.db_origin
                .query(sql, &[&table])
                .await
                .map_into_report::<CacheError>()?
        };

        Ok(rows)
    }

    /// Find one child partition of a partitioned table, if any.
    /// Returns None for regular tables or partitioned tables with no children.
    #[instrument(skip_all)]
    pub(super) async fn partition_child_find(&self, relation_oid: u32) -> CacheResult<Option<u32>> {
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

        Ok(row.map(|r| r.get::<_, u32>(0)))
    }

    #[instrument(skip_all)]
    pub(super) async fn query_table_indexes_get(
        &self,
        relation_oid: u32,
    ) -> CacheResult<Vec<IndexMetadata>> {
        // For partitioned tables, query indexes from a child partition.
        // Falls back to parent (works for regular tables and partitioned tables with no children).
        let child_oid = self.partition_child_find(relation_oid).await?;
        let target_oid = child_oid.unwrap_or(relation_oid);

        let sql = r"
            SELECT
                i.relname AS index_name,
                ix.indisunique AS is_unique,
                am.amname AS method,
                array_agg(a.attname ORDER BY array_position(ix.indkey::int[], a.attnum::int)) AS columns
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_am am ON am.oid = i.relam
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
            WHERE t.oid = $1
              AND NOT ix.indisprimary
              AND ix.indexprs IS NULL
              AND ix.indpred IS NULL
            GROUP BY i.relname, ix.indisunique, am.amname, ix.indkey
            ORDER BY i.relname;
        ";

        let rows = self
            .db_origin
            .query(sql, &[&target_oid])
            .await
            .map_into_report::<CacheError>()?;

        let indexes = rows
            .iter()
            .map(|row| {
                let columns: Vec<EcoString> = row
                    .get::<_, Vec<String>>("columns")
                    .into_iter()
                    .map(Into::into)
                    .collect();
                IndexMetadata {
                    name: row.get::<_, String>("index_name").into(),
                    is_unique: row.get("is_unique"),
                    method: row.get::<_, String>("method").into(),
                    columns,
                }
            })
            .collect();

        Ok(indexes)
    }

    #[instrument(skip_all)]
    pub(super) async fn schema_for_table_find(
        &self,
        table_name: &str,
        search_path: &[&str],
    ) -> CacheResult<String> {
        for schema in search_path {
            if self.cache.tables.get2(&(*schema, table_name)).is_some() {
                return Ok((*schema).to_owned());
            }
        }

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
            .map(|row| row.get::<_, String>(0))
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
        let schema = &table_metadata.schema;
        let table = &table_metadata.name;

        let column_defs: Vec<String> = table_metadata
            .columns
            .iter()
            .map(|c| format!("    \"{}\" {}", c.name, c.cache_type_name))
            .collect();
        let column_defs = column_defs.join(",\n");

        let primary_key = table_metadata
            .primary_key_columns
            .iter()
            .map(|c| format!("\"{c}\""))
            .collect::<Vec<_>>()
            .join(", ");

        let create_schema_sql = format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\"");
        let drop_sql = format!("DROP TABLE IF EXISTS \"{schema}\".\"{table}\"");
        let create_sql = format!(
            "CREATE UNLOGGED TABLE \"{schema}\".\"{table}\" (\n{column_defs},\n\tPRIMARY KEY({primary_key})\n)"
        );

        self.db_cache
            .execute(&create_schema_sql, &[])
            .await
            .map_into_report::<CacheError>()?;
        self.db_cache
            .execute(&drop_sql, &[])
            .await
            .map_into_report::<CacheError>()?;
        self.db_cache
            .execute(&create_sql, &[])
            .await
            .map_into_report::<CacheError>()?;

        for index in &table_metadata.indexes {
            let unique = if index.is_unique { "UNIQUE " } else { "" };
            let method = &index.method;
            let columns = index.columns.join(", ");
            let index_sql = format!(
                "CREATE {unique}INDEX ON \"{schema}\".\"{table}\" USING {method} ({columns})"
            );
            self.db_cache
                .execute(&index_sql, &[])
                .await
                .map_into_report::<CacheError>()?;
        }

        // Enable generation tracking triggers on the table
        let enable_tracking_sql =
            format!("SELECT pgcache_enable_tracking('\"{schema}\".\"{table}\"'::regclass::oid)");
        self.db_cache
            .execute(&enable_tracking_sql, &[])
            .await
            .map_into_report::<CacheError>()?;

        Ok(())
    }

    /// Drop the source-row cache tables for `relation_oids` and remove their
    /// metadata. Dropping the table also drops its
    /// `pgcache_track_modification` trigger, which is what
    /// `pgcache_total_size` sums — without this, evicted tables keep
    /// contributing to the cache-size accounting.
    ///
    /// Best-effort: logs and continues on DB failures rather than aborting
    /// the eviction path. Oids without metadata are silently skipped.
    #[instrument(skip(self, relation_oids))]
    pub(super) async fn cache_tables_drop(&mut self, relation_oids: &[u32]) {
        let mut quoted = Vec::with_capacity(relation_oids.len());
        for oid in relation_oids {
            if let Some(table) = self.cache.tables.remove1(oid) {
                quoted.push(format!("\"{}\".\"{}\"", table.schema, table.name));
            }
        }
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
}
