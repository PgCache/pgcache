//! The event-trigger payload (`pg::event_trigger`): which origin relations a
//! DDL command altered, dropped, truncated or created as partitions. Every
//! object the trigger saw is listed; only tables matter to the cache today,
//! the rest is reported for the policy, grant and view consumers to come.

use serde::Deserialize;
use tracing::trace;

use crate::cache::messages::{PartitionCreated, SchemaChange};
use crate::oid::Oid;

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
enum Payload {
    Ddl { objects: Vec<DdlObject> },
    Drop { objects: Vec<DroppedObject> },
    Truncate { objects: Vec<TruncatedObject> },
}

#[derive(Deserialize)]
struct DdlObject {
    tag: String,
    #[serde(rename = "type")]
    object_type: String,
    oid: Option<u32>,
    /// The direct parent when the object is a partition (`pg_inherits`, still
    /// visible at `ddl_command_end`).
    #[serde(default)]
    parent: Option<u32>,
}

#[derive(Deserialize)]
struct DroppedObject {
    #[serde(rename = "type")]
    object_type: String,
    oid: Option<u32>,
}

#[derive(Deserialize)]
struct TruncatedObject {
    oid: Option<u32>,
}

/// `pg_event_trigger_ddl_commands().object_type` for plain and partitioned
/// tables alike; under `publish_via_partition_root` an `ALTER TABLE` on a
/// partitioned parent reports the parent, the OID pgcache caches under.
const TABLE: &str = "table";
/// A foreign table can be a partition too.
const FOREIGN_TABLE: &str = "foreign table";

/// The one command tag that changes an existing table: every subcommand
/// (columns, ATTACH/DETACH PARTITION, RLS flags, OWNER, RENAME) reports it.
/// `CREATE TABLE` also lists a `table` object, but a table with no pre-state
/// has nothing cached to evict, and its message routinely arrives after a
/// query has already registered on the new table.
const ALTER_TABLE: &str = "ALTER TABLE";
const CREATE_TABLE: &str = "CREATE TABLE";
const CREATE_FOREIGN_TABLE: &str = "CREATE FOREIGN TABLE";

fn is_table(object_type: &str) -> bool {
    object_type == TABLE || object_type == FOREIGN_TABLE
}

/// Decode a `pgcache` logical message into the relation OIDs it affects.
pub(super) fn schema_change_parse(content: &[u8]) -> Result<SchemaChange, serde_json::Error> {
    let mut change = SchemaChange::default();
    match serde_json::from_slice::<Payload>(content)? {
        Payload::Ddl { objects } => {
            for object in objects {
                let tag = object.tag.as_str();
                let object_type = object.object_type.as_str();
                match (tag, object.oid, object.parent) {
                    (ALTER_TABLE, Some(oid), _) if is_table(object_type) => {
                        change.altered_tables.push(Oid::from_raw(oid));
                    }
                    (CREATE_TABLE | CREATE_FOREIGN_TABLE, Some(oid), Some(parent))
                        if is_table(object_type) =>
                    {
                        change.partitions_created.push(PartitionCreated {
                            partition: Oid::from_raw(oid),
                            parent: Oid::from_raw(parent),
                        });
                    }
                    _ => trace!(tag, object_type, "schema change on an untracked object"),
                }
            }
        }
        Payload::Drop { objects } => {
            for object in objects {
                if let Some(oid) = object.oid
                    && is_table(&object.object_type)
                {
                    change.dropped_tables.push(Oid::from_raw(oid));
                }
            }
        }
        Payload::Truncate { objects } => {
            change
                .truncated_tables
                .extend(objects.into_iter().filter_map(|o| o.oid).map(Oid::from_raw));
        }
    }
    Ok(change)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_change_parse_collects_altered_tables() {
        let payload = br#"{"kind":"ddl","objects":[
            {"tag":"ALTER TABLE","type":"table","oid":16384,"identity":"public.plain","parent":null},
            {"tag":"CREATE TABLE","type":"table","oid":16500,"identity":"public.fresh","parent":null},
            {"tag":"CREATE POLICY","type":"policy","oid":16400,"identity":"pol on public.plain","parent":null},
            {"tag":"GRANT","type":"TABLE","oid":null,"identity":null,"parent":null}]}"#;
        let change = schema_change_parse(payload).expect("parse ddl payload");
        assert_eq!(change.altered_tables, vec![Oid::from_raw(16384)]);
        assert!(change.dropped_tables.is_empty());
        assert!(change.partitions_created.is_empty());
    }

    #[test]
    fn test_schema_change_parse_collects_created_partitions_with_parent() {
        let payload = br#"{"kind":"ddl","objects":[
            {"tag":"CREATE TABLE","type":"table","oid":16500,"identity":"public.events_p2","parent":16384},
            {"tag":"CREATE FOREIGN TABLE","type":"foreign table","oid":16510,"identity":"public.events_p3","parent":16384}]}"#;
        let change = schema_change_parse(payload).expect("parse ddl payload");
        assert!(change.altered_tables.is_empty());
        assert_eq!(
            change.partitions_created,
            vec![
                PartitionCreated {
                    partition: Oid::from_raw(16500),
                    parent: Oid::from_raw(16384)
                },
                PartitionCreated {
                    partition: Oid::from_raw(16510),
                    parent: Oid::from_raw(16384)
                }
            ]
        );
    }

    #[test]
    fn test_schema_change_parse_collects_dropped_tables_only() {
        let payload = br#"{"kind":"drop","objects":[
            {"type":"table","oid":16384,"identity":"public.plain","original":true},
            {"type":"type","oid":16386,"identity":"public.plain","original":false},
            {"type":"index","oid":16390,"identity":"public.plain_pkey","original":false},
            {"type":"trigger","oid":16391,"identity":"pgcache_truncate on public.child","original":false},
            {"type":"table","oid":16500,"identity":"public.child","original":false}]}"#;
        let change = schema_change_parse(payload).expect("parse drop payload");
        assert!(change.altered_tables.is_empty());
        assert_eq!(
            change.dropped_tables,
            vec![Oid::from_raw(16384), Oid::from_raw(16500)]
        );
    }

    #[test]
    fn test_schema_change_parse_collects_truncated_partitions() {
        let payload = br#"{"kind":"truncate","objects":[{"oid":16500,"identity":"events_p2"}]}"#;
        let change = schema_change_parse(payload).expect("parse truncate payload");
        assert_eq!(change.truncated_tables, vec![Oid::from_raw(16500)]);
        assert!(!change.is_empty());
    }

    #[test]
    fn test_schema_change_parse_rejects_unknown_kind() {
        assert!(schema_change_parse(br#"{"kind":"rename","objects":[]}"#).is_err());
        assert!(schema_change_parse(b"not json").is_err());
    }
}
