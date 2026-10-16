//! The event-trigger payload (`pg::event_trigger`): which origin relations a
//! DDL command altered or dropped. Every object the trigger saw is listed;
//! only tables matter to the cache today, the rest is reported for the
//! policy, grant and view consumers to come.

use serde::Deserialize;
use tracing::trace;

use crate::cache::messages::SchemaChange;
use crate::oid::Oid;

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
enum Payload {
    Ddl { objects: Vec<DdlObject> },
    Drop { objects: Vec<DroppedObject> },
}

#[derive(Deserialize)]
struct DdlObject {
    tag: String,
    #[serde(rename = "type")]
    object_type: String,
    oid: Option<u32>,
}

#[derive(Deserialize)]
struct DroppedObject {
    #[serde(rename = "type")]
    object_type: String,
    oid: Option<u32>,
}

/// `pg_event_trigger_ddl_commands().object_type` for plain and partitioned
/// tables alike; under `publish_via_partition_root` an `ALTER TABLE` on a
/// partitioned parent reports the parent, the OID pgcache caches under.
const TABLE: &str = "table";

/// The one command tag that changes an existing table: every subcommand
/// (columns, ATTACH/DETACH PARTITION, RLS flags, OWNER, RENAME) reports it.
/// `CREATE TABLE` also lists a `table` object, but a table with no pre-state
/// has nothing cached to evict, and its message routinely arrives after a
/// query has already registered on the new table.
const ALTER_TABLE: &str = "ALTER TABLE";

/// Decode a `pgcache` logical message into the relation OIDs it affects.
pub(super) fn schema_change_parse(content: &[u8]) -> Result<SchemaChange, serde_json::Error> {
    let mut change = SchemaChange::default();
    match serde_json::from_slice::<Payload>(content)? {
        Payload::Ddl { objects } => {
            for object in objects {
                match (object.tag.as_str(), object.object_type.as_str(), object.oid) {
                    (ALTER_TABLE, TABLE, Some(oid)) => {
                        change.altered_tables.push(Oid::from_raw(oid));
                    }
                    (tag, object_type, _) => {
                        trace!(tag, object_type, "schema change on an untracked object");
                    }
                }
            }
        }
        Payload::Drop { objects } => {
            for object in objects {
                if let (TABLE, Some(oid)) = (object.object_type.as_str(), object.oid) {
                    change.dropped_tables.push(Oid::from_raw(oid));
                }
            }
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
            {"tag":"ALTER TABLE","type":"table","oid":16384,"identity":"public.plain"},
            {"tag":"CREATE TABLE","type":"table","oid":16500,"identity":"public.fresh"},
            {"tag":"CREATE POLICY","type":"policy","oid":16400,"identity":"pol on public.plain"},
            {"tag":"GRANT","type":"TABLE","oid":null,"identity":null}]}"#;
        let change = schema_change_parse(payload).expect("parse ddl payload");
        assert_eq!(change.altered_tables, vec![Oid::from_raw(16384)]);
        assert!(change.dropped_tables.is_empty());
    }

    #[test]
    fn test_schema_change_parse_collects_dropped_tables_only() {
        let payload = br#"{"kind":"drop","objects":[
            {"type":"table","oid":16384,"identity":"public.plain","original":true},
            {"type":"type","oid":16386,"identity":"public.plain","original":false},
            {"type":"index","oid":16390,"identity":"public.plain_pkey","original":false},
            {"type":"table","oid":16500,"identity":"public.child","original":false}]}"#;
        let change = schema_change_parse(payload).expect("parse drop payload");
        assert!(change.altered_tables.is_empty());
        assert_eq!(
            change.dropped_tables,
            vec![Oid::from_raw(16384), Oid::from_raw(16500)]
        );
    }

    #[test]
    fn test_schema_change_parse_rejects_unknown_kind() {
        assert!(schema_change_parse(br#"{"kind":"rename","objects":[]}"#).is_err());
        assert!(schema_change_parse(b"not json").is_err());
    }
}
