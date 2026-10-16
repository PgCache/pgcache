//! The origin schema-change signal.
//!
//! Two event triggers on origin emit every DDL command and every dropped
//! object as a transactional logical message with the `pgcache` prefix. The
//! message is decoded at the command's position inside its transaction, so
//! the CDC stream carries schema changes with the same ordering as the row
//! changes around them. Nothing else on the stream reports `DROP TABLE`, a
//! partition attach/detach, or an `ALTER TABLE` not followed by a write.
//!
//! Creating event triggers needs a superuser (or the managed-PG master role),
//! which pgcache's own role usually is not: installation is attempted at
//! provisioning and otherwise left to the admin, with the SQL printed by
//! `pgcache --check`. Without the triggers pgcache keeps its older behaviour,
//! learning of a schema change only from the next write to the table.

use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, Error};
use tracing::{info, warn};

/// Names of the two triggers, as `pg_event_trigger.evtname`.
pub(crate) const TRIGGER_DDL_COMMAND_END: &str = "pgcache_ddl_command_end";
pub(crate) const TRIGGER_SQL_DROP: &str = "pgcache_sql_drop";

/// Idempotent: re-runnable on an origin that already has the triggers, and
/// safe when two pgcache instances provision the same database at once (no
/// drop-and-recreate window). `ENABLE ALWAYS` keeps the triggers firing under
/// `session_replication_role = replica`, which some migration tools set.
///
/// Self-noise is filtered at the source: pgcache's own `ALTER PUBLICATION`
/// per registration and eviction reports `publication relation` objects, and
/// a message there would put a row-less frame on the stream at every
/// registration. Temp tables are the other high-frequency DDL with nothing
/// cacheable behind it.
pub(crate) const INSTALL_SQL: &str = r"
CREATE SCHEMA IF NOT EXISTS pgcache;

CREATE OR REPLACE FUNCTION pgcache.ddl_command_end() RETURNS event_trigger
LANGUAGE plpgsql AS $$
DECLARE
    objects json;
BEGIN
    SELECT json_agg(json_build_object(
               'tag', command_tag, 'type', object_type,
               'oid', objid::bigint, 'identity', object_identity))
      INTO objects
      FROM pg_event_trigger_ddl_commands()
     WHERE object_type NOT LIKE 'publication%'
       AND coalesce(schema_name, '') NOT LIKE 'pg\_temp%';
    IF objects IS NOT NULL THEN
        PERFORM pg_logical_emit_message(true, 'pgcache',
            json_build_object('kind', 'ddl', 'objects', objects)::text);
    END IF;
END
$$;

CREATE OR REPLACE FUNCTION pgcache.sql_drop() RETURNS event_trigger
LANGUAGE plpgsql AS $$
DECLARE
    objects json;
BEGIN
    SELECT json_agg(json_build_object(
               'type', object_type, 'oid', objid::bigint,
               'identity', object_identity, 'original', original))
      INTO objects
      FROM pg_event_trigger_dropped_objects()
     WHERE object_type NOT LIKE 'publication%'
       AND coalesce(schema_name, '') NOT LIKE 'pg\_temp%';
    IF objects IS NOT NULL THEN
        PERFORM pg_logical_emit_message(true, 'pgcache',
            json_build_object('kind', 'drop', 'objects', objects)::text);
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = 'pgcache_ddl_command_end') THEN
        CREATE EVENT TRIGGER pgcache_ddl_command_end ON ddl_command_end
            EXECUTE FUNCTION pgcache.ddl_command_end();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = 'pgcache_sql_drop') THEN
        CREATE EVENT TRIGGER pgcache_sql_drop ON sql_drop
            EXECUTE FUNCTION pgcache.sql_drop();
    END IF;
END
$$;

ALTER EVENT TRIGGER pgcache_ddl_command_end ENABLE ALWAYS;
ALTER EVENT TRIGGER pgcache_sql_drop ENABLE ALWAYS;
";

/// The triggers are shared by every pgcache instance on the database, so no
/// instance drops them on its own; this is the admin's uninstall.
pub(crate) const UNINSTALL_SQL: &str = "DROP EVENT TRIGGER IF EXISTS pgcache_ddl_command_end; \
     DROP EVENT TRIGGER IF EXISTS pgcache_sql_drop; \
     DROP SCHEMA IF EXISTS pgcache CASCADE;";

/// Whether each trigger exists and fires. Both must, for the signal to be
/// complete: `ddl_command_end` alone never reports a drop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct EventTriggerState {
    pub(crate) ddl_command_end: TriggerPresence,
    pub(crate) sql_drop: TriggerPresence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum TriggerPresence {
    #[default]
    Missing,
    Disabled,
    Enabled,
}

impl EventTriggerState {
    pub(crate) fn installed(self) -> bool {
        self.ddl_command_end == TriggerPresence::Enabled
            && self.sql_drop == TriggerPresence::Enabled
    }

    pub(crate) fn any_present(self) -> bool {
        self.ddl_command_end != TriggerPresence::Missing
            || self.sql_drop != TriggerPresence::Missing
    }
}

/// Read the triggers' presence from `pg_event_trigger`.
pub(crate) async fn event_triggers_state_load(client: &Client) -> Result<EventTriggerState, Error> {
    let rows = client
        .query(
            "SELECT evtname::text, evtenabled::text FROM pg_event_trigger WHERE evtname IN ($1, $2)",
            &[&TRIGGER_DDL_COMMAND_END, &TRIGGER_SQL_DROP],
        )
        .await?;
    let mut state = EventTriggerState::default();
    for row in rows {
        let name: String = row.try_get(0)?;
        let enabled: String = row.try_get(1)?;
        // `evtenabled` is the fires-when char: O(rigin), A(lways), R(eplica), D(isabled).
        let presence = match enabled.as_str() {
            "O" | "A" => TriggerPresence::Enabled,
            _ => TriggerPresence::Disabled,
        };
        if name == TRIGGER_DDL_COMMAND_END {
            state.ddl_command_end = presence;
        } else if name == TRIGGER_SQL_DROP {
            state.sql_drop = presence;
        }
    }
    Ok(state)
}

/// Install the triggers. `Ok(false)` when the role may not create event
/// triggers (SQLSTATE 42501): pgcache then runs without the signal.
pub(crate) async fn event_triggers_install(client: &Client, role: &str) -> Result<bool, Error> {
    match client.batch_execute(INSTALL_SQL).await {
        Ok(()) => {
            info!("schema-change event triggers installed on origin");
            Ok(true)
        }
        Err(e) if e.code() == Some(&SqlState::INSUFFICIENT_PRIVILEGE) => {
            warn!(
                "schema-change event triggers not installed: {role} may not create event \
                 triggers, so a schema change on a cached table is observed only at the next \
                 write to it; `pgcache --check` prints the SQL for an administrator"
            );
            Ok(false)
        }
        Err(e) => Err(e),
    }
}

/// Shutdown reminder: the triggers stay installed on purpose (see
/// [`UNINSTALL_SQL`]), and a sibling instance may still depend on them.
pub(crate) async fn event_triggers_shutdown_note(client: &Client) {
    match event_triggers_state_load(client).await {
        Ok(state) if state.any_present() => info!(
            "leaving the schema-change event triggers installed on origin (shared by every \
             pgcache on this database); to remove them run: {UNINSTALL_SQL}"
        ),
        Ok(_) => {}
        Err(e) => warn!("could not read origin event triggers at shutdown: {e}"),
    }
}
