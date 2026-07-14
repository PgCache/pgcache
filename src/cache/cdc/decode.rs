//! pgoutput decoding: turn a replication message body into the cache's own
//! `TableMetadata` / `CdcValue` shapes.
//!
//! Pure decode — no connection, no LSN state. The replication stream loop that
//! feeds these lives in [`cdc`](super).

use ecow::EcoString;
use postgres_replication::protocol::{
    DeleteBody, InsertBody, RelationBody, ReplicaIdentity, TupleData, UpdateBody,
};
use tokio_postgres::Error;
use tracing::error;

use crate::catalog::{ColumnMetadata, ColumnStore, TableMetadata, cache_type_name_resolve};
use crate::oid::Oid;
use crate::pg::protocol::ByteString;

use super::super::messages::CdcValue;

/// Parse RelationBody into TableMetadata for cache registration.
pub(super) fn parse_relation_to_table_metadata(relation_body: &RelationBody) -> TableMetadata {
    let relation_oid = Oid::from_raw(relation_body.rel_id());
    let table_name: EcoString = relation_body.name().unwrap_or("unknown_table").into();
    let schema_name: EcoString = relation_body.namespace().unwrap_or("unknown_schema").into();

    // Build column metadata from relation body
    let mut columns = Vec::new();
    let mut primary_key_columns = Vec::new();

    for (idx, column) in relation_body.columns().iter().enumerate() {
        let is_primary_key = column.flags() == 1; // flags field is 1 when column is part of primary key

        let type_oid = column.type_id().cast_unsigned();
        let data_type = tokio_postgres::types::Type::from_oid(type_oid)
            .unwrap_or(tokio_postgres::types::Type::TEXT); // Fallback for unknown types

        let type_name = data_type.name().to_owned();
        // For CDC-discovered types, use the resolved cache type name.
        // Unknown types fall back to TEXT, which resolves to "text" for cache.
        let cache_type_name =
            cache_type_name_resolve(&data_type).unwrap_or_else(|_| "text".to_owned());

        let column_metadata = ColumnMetadata {
            name: column.name().unwrap_or("unknown_column").into(),
            position: i16::try_from(idx + 1).expect("column position fits in i16"),
            type_oid,
            data_type,
            type_name: type_name.into(),
            cache_type_name: cache_type_name.into(),
            is_primary_key,
        };

        if is_primary_key {
            primary_key_columns.push(column.name().unwrap_or("unknown_column").into());
        }

        columns.push(column_metadata);
    }

    TableMetadata {
        replica_identity_full: matches!(relation_body.replica_identity(), ReplicaIdentity::Full),
        name: table_name,
        schema: schema_name,
        relation_oid,
        primary_key_columns,
        columns: ColumnStore::new(columns),
        indexes: Vec::new(), // Indexes are queried separately in cache_table_register
    }
}

/// Parse row data from InsertBody into a Vec of column values indexed by position.
pub(super) fn parse_insert_row_data(body: InsertBody) -> Result<Vec<CdcValue>, Error> {
    Ok(tuple_data_parse(body.into_tuple().into_data()))
}

/// Parse old and new row data from UpdateBody into Vecs of column values indexed by position.
#[allow(clippy::type_complexity)]
pub(super) fn parse_update_row_data(
    body: UpdateBody,
) -> Result<(Vec<CdcValue>, Vec<CdcValue>), Error> {
    let (key_tuple, old_tuple, new_tuple) = body.into_tuples();
    let new_row_data = tuple_data_parse(new_tuple.into_data());

    // 'K' (REPLICA IDENTITY DEFAULT, sent only on PK change) or 'O'
    // (REPLICA IDENTITY FULL, the complete old row on every update) —
    // mutually exclusive per pgoutput. Downstream distinguishes them via
    // `TableMetadata::replica_identity_full` / `update_pk_changed`.
    let key_data = key_tuple
        .or(old_tuple)
        .map(|kt| tuple_data_parse(kt.into_data()))
        .unwrap_or_default();

    Ok((key_data, new_row_data))
}

/// Parse row data from DeleteBody into a Vec of column values indexed by position.
pub(super) fn parse_delete_row_data(body: DeleteBody) -> Result<Vec<CdcValue>, Error> {
    // DeleteBody contains either key_tuple (for tables with REPLICA IDENTITY USING INDEX)
    // or old_tuple (for tables with REPLICA IDENTITY FULL)
    let (key_tuple, old_tuple) = body.into_tuples();
    let Some(tuple) = key_tuple.or(old_tuple) else {
        // No tuple data available (REPLICA IDENTITY NOTHING)
        error!("DELETE operation requires REPLICA IDENTITY FULL or USING INDEX, found NOTHING");
        return Ok(Vec::new());
    };

    Ok(tuple_data_parse(tuple.into_data()))
}

/// `tuple_data_parse` reuses the source Vec's allocation via the in-place
/// collect specialization, which holds only while the element layouts match.
/// A size change downgrades it to a silent per-event allocation — fail the
/// build instead so the regression is visible.
const _: () = assert!(
    std::mem::size_of::<TupleData>() == std::mem::size_of::<CdcValue>()
        && std::mem::align_of::<TupleData>() == std::mem::align_of::<CdcValue>(),
);

/// Convert replication TupleData into column values, preserving the
/// unchanged-toast marker distinctly from NULL (PGC-264). Consumes the Vec so
/// the conversion is in place — no per-event allocation (see the layout
/// assertion above).
fn tuple_data_parse(columns: Vec<TupleData>) -> Vec<CdcValue> {
    columns
        .into_iter()
        .map(|col| match col {
            TupleData::Null => CdcValue::Null,
            TupleData::UnchangedToast => CdcValue::Toasted,
            // Zero-copy: the value is a refcounted view of the replication
            // frame. PG sends valid UTF-8 in text format; an invalid sequence
            // falls back to a lossy copy rather than dropping the event.
            TupleData::Text(data) => CdcValue::Text(
                ByteString::from_utf8(data.clone())
                    .unwrap_or_else(|_| ByteString::from(String::from_utf8_lossy(&data).as_ref())),
            ),
            TupleData::Binary(_) => unreachable!("pgcache uses text-format replication"),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tuple_data_parse_preserves_unchanged_toast() {
        let columns = vec![
            TupleData::Null,
            TupleData::UnchangedToast,
            TupleData::Text("abc".as_bytes().into()),
        ];
        assert_eq!(
            tuple_data_parse(columns),
            vec![
                CdcValue::Null,
                CdcValue::Toasted,
                CdcValue::Text("abc".into()),
            ]
        );
    }
}
