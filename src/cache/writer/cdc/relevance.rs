//! What an UPDATE provably left as it was, for the memo and MV relevance
//! test (PGC-247/404): a candidate reading none of the changed columns
//! keeps its memo entry and its MV table.

use super::RelationUpdate;
use crate::cache::update_query::RowChanges;
use crate::catalog::{ColumnMask, TableMetadata};

/// The columns an UPDATE provably kept, or `None` when nothing is provable
/// and every candidate must count as touched: a PK change (membership), or
/// a row the cache could not compare (absent from the cache table, a
/// batch-deleted PK, a failed lookup).
///
/// Under `REPLICA IDENTITY FULL` the old tuple is in the message, so the
/// comparison is text equality in Rust. Otherwise a column is unchanged only
/// when the row-change booleans carry it as unchanged: a projection the
/// fold did not deliver counts as changed.
pub(super) fn update_unchanged_columns(
    table: &TableMetadata,
    update: RelationUpdate<'_>,
    pk_changed: bool,
    row_changes: Option<&RowChanges>,
) -> Option<ColumnMask> {
    if pk_changed {
        return None;
    }
    let mut unchanged = ColumnMask::with_len(table.columns.len());
    if table.replica_identity_full && !update.key_data.is_empty() {
        for column in &table.columns {
            let index = column.index();
            if update.key_data.get(index) == update.new_row_data.get(index) {
                unchanged.set(index);
            }
        }
        return Some(unchanged);
    }
    let changes = row_changes?;
    for (name, change) in changes {
        if !change.changed
            && let Some(column) = table.columns.get(name)
        {
            unchanged.set(column.index());
        }
    }
    Some(unchanged)
}

#[cfg(test)]
mod tests {
    use postgres_types::Type;

    use super::*;
    use crate::cache::update_query::ColumnChange;
    use crate::catalog::{ColumnMetadata, ColumnPosition};
    use crate::oid::TypeOid;
    use crate::pg::protocol::ByteString;

    /// `t(<names>)`, the first column its key.
    fn table_with_columns(names: &[&str]) -> TableMetadata {
        let columns = names.iter().enumerate().map(|(i, name)| ColumnMetadata {
            name: (*name).into(),
            position: ColumnPosition::from_raw(i16::try_from(i).expect("few columns") + 1),
            type_oid: TypeOid::from_raw(Type::TEXT.oid()),
            data_type: Type::TEXT,
            type_name: "text".into(),
            cache_type_name: "text".into(),
            is_primary_key: i == 0,
        });
        TableMetadata::for_test("t", 1001, columns)
    }

    fn row(values: &[&str]) -> Vec<Option<ByteString>> {
        values.iter().map(|v| Some(ByteString::from(*v))).collect()
    }

    fn changes(entries: &[(&str, bool)]) -> RowChanges {
        entries
            .iter()
            .map(|(name, changed)| {
                (
                    (*name).into(),
                    ColumnChange {
                        changed: *changed,
                        ..ColumnChange::default()
                    },
                )
            })
            .collect()
    }

    /// A PK change proves nothing, whatever the booleans say.
    #[test]
    fn test_pk_change_is_unprovable() {
        let table = table_with_columns(&["id", "a", "b"]);
        let new = row(&["2", "x", "y"]);
        let update = RelationUpdate::new(table.relation_oid, &[], &new);
        let booleans = changes(&[("id", false), ("a", false), ("b", false)]);
        assert!(update_unchanged_columns(&table, update, true, Some(&booleans)).is_none());
    }

    /// Only a column the booleans carry as unchanged is unchanged; a missing
    /// projection counts as changed, and no booleans prove nothing.
    #[test]
    fn test_booleans_mark_only_carried_unchanged_columns() {
        let table = table_with_columns(&["id", "a", "b"]);
        let new = row(&["1", "x", "y"]);
        let update = RelationUpdate::new(table.relation_oid, &[], &new);
        let booleans = changes(&[("id", false), ("a", true)]);
        let unchanged = update_unchanged_columns(&table, update, false, Some(&booleans))
            .expect("booleans present");
        assert!(unchanged.contains(0));
        assert!(!unchanged.contains(1), "changed");
        assert!(!unchanged.contains(2), "not projected");
        assert!(update_unchanged_columns(&table, update, false, None).is_none());
    }

    /// Under REPLICA IDENTITY FULL the old tuple decides: text-equal columns
    /// are unchanged, the rest changed.
    #[test]
    fn test_full_identity_compares_old_tuple() {
        let mut table = table_with_columns(&["id", "a", "b"]);
        table.replica_identity_full = true;
        let old = row(&["1", "x", "y"]);
        let new = row(&["1", "x", "z"]);
        let update = RelationUpdate::new(table.relation_oid, &old, &new);
        let unchanged =
            update_unchanged_columns(&table, update, false, None).expect("old tuple present");
        assert!(unchanged.contains(0));
        assert!(unchanged.contains(1));
        assert!(!unchanged.contains(2));
    }
}
