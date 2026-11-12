//! Routing of stream relations to cache tables.
//!
//! A row change streams under the leaf partition that holds the row, in the
//! leaf's own column layout. The cached tables it belongs to are the leaf
//! itself and every cached table above it; a table with no partitions is its
//! own only target and takes no route.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::core::WriterCore;
use crate::cache::messages::CdcValue;
use crate::catalog::TableMetadata;
use crate::oid::Oid;

/// Where each of a target table's columns sits in a leaf's tuple, by target
/// column index. `None` when the two layouts agree, the usual case.
pub(super) type ColumnMap = Option<Arc<[usize]>>;

/// Why a leaf's rows cannot be applied to a target table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LayoutMismatch {
    /// No `Relation` message has described the leaf.
    LeafUnknown,
    /// The target is no longer a cached table.
    TargetUnknown,
    /// The column sets differ: one side changed shape and the other's
    /// metadata predates it.
    Columns,
    /// The leaf's replica identity does not carry the target's key.
    Key,
}

impl LayoutMismatch {
    pub(super) fn reason(self) -> &'static str {
        match self {
            Self::LeafUnknown => "the partition's layout is unknown",
            Self::TargetUnknown => "the table is no longer cached",
            Self::Columns => "the partition's columns differ from the table's",
            Self::Key => "the partition's replica identity lacks the table's key",
        }
    }
}

#[derive(Default)]
pub(super) struct StreamRoutes {
    /// The cached tables each partition's rows land in. Only partitions
    /// under an active table have an entry.
    targets: HashMap<Oid, Vec<Oid>>,
    /// Every relation the stream has described, as its latest `Relation`
    /// message gave it. Kept past eviction: the walsender describes a
    /// relation once per session, not once per registration.
    layouts: HashMap<Oid, Arc<TableMetadata>>,
    column_maps: HashMap<(Oid, Oid), ColumnMap>,
}

impl StreamRoutes {
    /// The cached tables a row change on `stream_oid` lands in; `None` for a
    /// relation that is not a partition of an active table.
    pub(super) fn targets(&self, stream_oid: Oid) -> Option<&[Oid]> {
        self.targets.get(&stream_oid).map(Vec::as_slice)
    }

    /// Record a relation's layout from its `Relation` message.
    pub(super) fn layout_record(&mut self, layout: Arc<TableMetadata>) {
        let stream_oid = layout.relation_oid;
        self.column_maps.retain(|(leaf, _), _| *leaf != stream_oid);
        self.layouts.insert(stream_oid, layout);
    }
}

/// The map from `target`'s column order to `leaf`'s.
fn column_map_build(
    target: &TableMetadata,
    leaf: &TableMetadata,
) -> Result<ColumnMap, LayoutMismatch> {
    if target.columns.len() != leaf.columns.len() {
        return Err(LayoutMismatch::Columns);
    }
    let key_carried = leaf.replica_identity_full
        || target
            .primary_key_columns
            .iter()
            .all(|key| leaf.primary_key_columns.contains(key));
    if !key_carried {
        return Err(LayoutMismatch::Key);
    }
    let mut map: Vec<usize> = Vec::with_capacity(target.columns.len());
    for column in &target.columns {
        let source = leaf
            .columns
            .get(column.name.as_str())
            .filter(|source| source.type_oid == column.type_oid)
            .ok_or(LayoutMismatch::Columns)?;
        map.push(source.index());
    }
    let aligned = map
        .iter()
        .enumerate()
        .all(|(index, source)| index == *source);
    Ok((!aligned).then(|| Arc::from(map)))
}

/// A leaf tuple in the target's column order. An empty tuple (no key image)
/// stays empty.
pub(super) fn row_remap(map: &ColumnMap, row: &[CdcValue]) -> Vec<CdcValue> {
    match map {
        None => row.to_vec(),
        Some(_) if row.is_empty() => Vec::new(),
        Some(map) => map
            .iter()
            .map(|source| row.get(*source).cloned().unwrap_or(CdcValue::Null))
            .collect(),
    }
}

impl WriterCore {
    /// Rebuild the routes and the shared stream-relation set from the active
    /// tables and their partition trees. Called after every change to either.
    pub(super) fn stream_relations_publish(&mut self) {
        let mut relations: HashSet<Oid> = HashSet::with_capacity(self.relation_refcounts.len());
        let mut targets: HashMap<Oid, Vec<Oid>> = HashMap::new();
        for &table_oid in self.relation_refcounts.keys() {
            relations.insert(table_oid);
            let Some(table) = self.cache.tables.get1(&table_oid) else {
                continue;
            };
            for &partition in &table.partitions {
                relations.insert(partition);
                targets.entry(partition).or_default().push(table_oid);
            }
        }
        for (partition, tables) in &mut targets {
            if self.relation_refcounts.contains_key(partition) {
                tables.push(*partition);
            }
        }
        self.stream_routes.targets = targets;
        self.stream_routes.column_maps.clear();
        self.stream_relations.store(Arc::new(relations));
    }

    /// A partition under `target` reported its replica identity. The table
    /// counts as REPLICA IDENTITY FULL only while every partition seen is:
    /// the old tuples are the partition's, and without FULL they hold the
    /// key alone.
    pub(super) fn replica_identity_narrow(&mut self, target: Oid, partition_full: bool) {
        if partition_full {
            return;
        }
        let Some(table) = self.cache.tables.get1(&target) else {
            return;
        };
        if !table.replica_identity_full {
            return;
        }
        let narrowed = TableMetadata {
            replica_identity_full: false,
            ..TableMetadata::clone(table)
        };
        self.cache.tables.insert_overwrite(narrowed.into());
    }

    /// The column map that carries `leaf`'s tuples into `target`.
    pub(super) fn stream_column_map(
        &mut self,
        leaf: Oid,
        target: Oid,
    ) -> Result<ColumnMap, LayoutMismatch> {
        if let Some(map) = self.stream_routes.column_maps.get(&(leaf, target)) {
            return Ok(map.clone());
        }
        let layout = self
            .stream_routes
            .layouts
            .get(&leaf)
            .ok_or(LayoutMismatch::LeafUnknown)?;
        let table = self
            .cache
            .tables
            .get1(&target)
            .ok_or(LayoutMismatch::TargetUnknown)?;
        let map = column_map_build(table, layout)?;
        self.stream_routes
            .column_maps
            .insert((leaf, target), map.clone());
        Ok(map)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use tokio_postgres::types::Type;

    use super::*;
    use crate::catalog::{ColumnMetadata, ColumnPosition};
    use crate::oid::TypeOid;
    use crate::pg::protocol::ByteString;

    fn table(oid: u32, columns: &[(&str, Type, bool)]) -> TableMetadata {
        let columns = columns
            .iter()
            .enumerate()
            .map(|(index, (name, data_type, key))| ColumnMetadata {
                name: (*name).into(),
                position: ColumnPosition::from_raw(i16::try_from(index).expect("few columns") + 1),
                type_oid: TypeOid::from_raw(data_type.oid()),
                data_type: data_type.clone(),
                type_name: data_type.name().into(),
                cache_type_name: data_type.name().into(),
                is_primary_key: *key,
            });
        TableMetadata::for_test("t", oid, columns)
    }

    fn text(value: &'static str) -> CdcValue {
        CdcValue::Text(ByteString::from_utf8(Bytes::from_static(value.as_bytes())).expect("utf8"))
    }

    #[test]
    fn test_column_map_build_aligned_layout_needs_no_map() {
        let root = table(1, &[("id", Type::INT4, true), ("v", Type::TEXT, false)]);
        let leaf = table(2, &[("id", Type::INT4, true), ("v", Type::TEXT, false)]);
        assert_eq!(column_map_build(&root, &leaf), Ok(None));
    }

    #[test]
    fn test_column_map_build_reordered_layout_maps_by_name() {
        let root = table(
            1,
            &[
                ("id", Type::INT4, true),
                ("k", Type::INT4, true),
                ("v", Type::TEXT, false),
            ],
        );
        let leaf = table(
            2,
            &[
                ("v", Type::TEXT, false),
                ("k", Type::INT4, true),
                ("id", Type::INT4, true),
            ],
        );
        let map = column_map_build(&root, &leaf).expect("layouts are compatible");
        assert_eq!(map.as_deref(), Some([2, 1, 0].as_slice()));
        let row = [text("x"), text("15"), text("1")];
        assert_eq!(row_remap(&map, &row), [text("1"), text("15"), text("x")]);
        assert_eq!(row_remap(&map, &[]), []);
    }

    #[test]
    fn test_column_map_build_refuses_differing_columns() {
        let root = table(1, &[("id", Type::INT4, true), ("v", Type::TEXT, false)]);
        let wider = table(
            2,
            &[
                ("id", Type::INT4, true),
                ("v", Type::TEXT, false),
                ("extra", Type::INT4, false),
            ],
        );
        let renamed = table(3, &[("id", Type::INT4, true), ("w", Type::TEXT, false)]);
        let retyped = table(4, &[("id", Type::INT4, true), ("v", Type::INT4, false)]);
        for leaf in [&wider, &renamed, &retyped] {
            assert_eq!(column_map_build(&root, leaf), Err(LayoutMismatch::Columns));
        }
    }

    #[test]
    fn test_column_map_build_refuses_identity_without_the_key() {
        let root = table(1, &[("id", Type::INT4, true), ("v", Type::TEXT, false)]);
        let mut leaf = table(2, &[("id", Type::INT4, false), ("v", Type::TEXT, true)]);
        assert_eq!(column_map_build(&root, &leaf), Err(LayoutMismatch::Key));
        leaf.replica_identity_full = true;
        assert_eq!(column_map_build(&root, &leaf), Ok(None));
    }
}
