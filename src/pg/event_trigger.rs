//! The origin schema-change signal.
//!
//! Two event triggers on origin emit every DDL command and every dropped
//! object as a transactional logical message with the `pgcache` prefix. The
//! message is decoded at the command's position inside its transaction, so
//! the CDC stream carries schema changes with the same ordering as the row
//! changes around them. Nothing else on the stream reports `DROP TABLE`, a
//! partition attach/detach, or an `ALTER TABLE` not followed by a write.
//!
//! `TRUNCATE` is not DDL and fires no event trigger; the stream reports it,
//! for a partition as for any table.
//!
//! Creating event triggers needs a superuser (or the managed-PG master role),
//! which pgcache's own role usually is not: installation is attempted at
//! provisioning and otherwise left to the admin, with the SQL printed by
//! `pgcache --check`.
//!
//! The signal is a requirement. pgcache refuses to start unless both
//! triggers fire and were installed by the script this binary carries
//! ([`SIGNAL_VERSION`]), and the writer polls for the same while running:
//! dropping or disabling an event trigger fires none, so nothing on the
//! stream reports it.

use std::fmt;

use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, Error};
use tracing::{info, warn};

use crate::pg::cdc::message_prefix;

/// Names of the two triggers, as `pg_event_trigger.evtname`; macros as well
/// as consts so the SQL below can `concat!` the literals in.
macro_rules! trigger_ddl_command_end {
    () => {
        "pgcache_ddl_command_end"
    };
}
macro_rules! trigger_sql_drop {
    () => {
        "pgcache_sql_drop"
    };
}
pub(crate) const TRIGGER_DDL_COMMAND_END: &str = trigger_ddl_command_end!();
pub(crate) const TRIGGER_SQL_DROP: &str = trigger_sql_drop!();

/// The version of [`INSTALL_SQL`], bumped with every change to it. The
/// script records it on origin, so an install this binary could not replace
/// (an administrator ran an older script) is told apart from a current one.
macro_rules! signal_version {
    () => {
        1
    };
}
pub(crate) const SIGNAL_VERSION: i32 = signal_version!();

/// Idempotent: re-runnable on an origin that already has the triggers, and
/// serialised by an advisory lock when two pgcache instances provision the
/// same database at once (`CREATE SCHEMA IF NOT EXISTS` and the trigger
/// creates would otherwise race to a duplicate-key error). The script runs
/// as one implicit transaction. `ENABLE ALWAYS` keeps the triggers firing
/// under `session_replication_role = replica`, which some migration tools
/// set.
///
/// Self-noise is filtered at the source. pgcache's own `ALTER PUBLICATION`
/// per registration and eviction reports `publication relation` objects, and
/// a message there would put a row-less frame on the stream at every
/// registration; temp tables are the other high-frequency DDL with nothing
/// cacheable behind it. The `pgcache_signal.installing` setting mutes the
/// DDL trigger while this script runs its own DDL.
pub(crate) const INSTALL_SQL: &str = concat!(
    r"
SELECT pg_advisory_xact_lock(hashtext('pgcache_event_triggers'));
SELECT set_config('pgcache_signal.installing', '1', false);

CREATE SCHEMA IF NOT EXISTS pgcache;

CREATE OR REPLACE FUNCTION pgcache.signal_version() RETURNS integer
LANGUAGE sql IMMUTABLE AS 'SELECT ",
    signal_version!(),
    r"';

CREATE OR REPLACE FUNCTION pgcache.ddl_command_end() RETURNS event_trigger
LANGUAGE plpgsql AS $$
DECLARE
    objects json;
BEGIN
    IF current_setting('pgcache_signal.installing', true) = '1' THEN
        RETURN;
    END IF;
    SELECT json_agg(json_build_object(
               'tag', command_tag, 'type', object_type,
               'oid', objid::bigint, 'identity', object_identity,
               'parent', (SELECT min(inhparent)::bigint FROM pg_inherits WHERE inhrelid = objid)))
      INTO objects
      FROM pg_event_trigger_ddl_commands()
     WHERE object_type NOT LIKE 'publication%'
       AND coalesce(schema_name, '') NOT LIKE 'pg\_temp%'
       AND coalesce(schema_name, '') <> 'pgcache';
    IF objects IS NOT NULL THEN
        PERFORM pg_logical_emit_message(true, '",
    message_prefix!(),
    r"',
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
       AND coalesce(schema_name, '') NOT LIKE 'pg\_temp%'
       AND coalesce(schema_name, '') <> 'pgcache';
    IF objects IS NOT NULL THEN
        PERFORM pg_logical_emit_message(true, '",
    message_prefix!(),
    r"',
            json_build_object('kind', 'drop', 'objects', objects)::text);
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = '",
    trigger_ddl_command_end!(),
    r"') THEN
        CREATE EVENT TRIGGER ",
    trigger_ddl_command_end!(),
    r" ON ddl_command_end
            EXECUTE FUNCTION pgcache.ddl_command_end();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = '",
    trigger_sql_drop!(),
    r"') THEN
        CREATE EVENT TRIGGER ",
    trigger_sql_drop!(),
    r" ON sql_drop
            EXECUTE FUNCTION pgcache.sql_drop();
    END IF;
END
$$;

ALTER EVENT TRIGGER ",
    trigger_ddl_command_end!(),
    r" ENABLE ALWAYS;
ALTER EVENT TRIGGER ",
    trigger_sql_drop!(),
    r" ENABLE ALWAYS;

SELECT set_config('pgcache_signal.installing', '', false);
"
);

/// The triggers are shared by every pgcache instance on the database, so no
/// instance drops them on its own; this is the admin's uninstall.
pub(crate) const UNINSTALL_SQL: &str = concat!(
    "DROP EVENT TRIGGER IF EXISTS ",
    trigger_ddl_command_end!(),
    "; DROP EVENT TRIGGER IF EXISTS ",
    trigger_sql_drop!(),
    "; DROP SCHEMA IF EXISTS pgcache CASCADE;"
);

/// The signal as origin holds it: whether each trigger exists and fires,
/// and the version of the script that installed them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct EventTriggerState {
    pub(crate) ddl_command_end: TriggerPresence,
    pub(crate) sql_drop: TriggerPresence,
    /// `None` when origin has no version marker, or one that does not read
    /// as a version.
    pub(crate) version: Option<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum TriggerPresence {
    #[default]
    Missing,
    Disabled,
    Enabled,
}

/// Why the signal cannot be relied on. pgcache learns of a schema change on
/// a cached table from the signal alone, so any of these means cached
/// results may outlive the shape they were read under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalDefect {
    Missing,
    Disabled,
    Stale,
}

impl fmt::Display for SignalDefect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Missing => "not installed",
            Self::Disabled => "incomplete or disabled",
            Self::Stale => "installed by another pgcache version",
        })
    }
}

impl EventTriggerState {
    /// What is wrong with the signal, if anything. Both triggers must fire:
    /// `ddl_command_end` alone never reports a drop.
    pub(crate) fn defect(self) -> Option<SignalDefect> {
        match (self.ddl_command_end, self.sql_drop) {
            (TriggerPresence::Missing, TriggerPresence::Missing) => Some(SignalDefect::Missing),
            (TriggerPresence::Enabled, TriggerPresence::Enabled) => {
                (self.version != Some(SIGNAL_VERSION)).then_some(SignalDefect::Stale)
            }
            _ => Some(SignalDefect::Disabled),
        }
    }

    pub(crate) fn any_present(self) -> bool {
        self.ddl_command_end != TriggerPresence::Missing
            || self.sql_drop != TriggerPresence::Missing
    }
}

/// The version a `pgcache.signal_version()` body records: `SELECT <n>`.
fn signal_version_parse(body: &str) -> Option<i32> {
    body.trim().strip_prefix("SELECT ")?.trim().parse().ok()
}

/// Read the signal's state from the catalog. The version comes from the
/// marker function's body, which any role can read: calling it would need
/// `USAGE` on a schema an administrator may have created.
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
    let marker = client
        .query_opt(
            "SELECT p.prosrc FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace \
             WHERE n.nspname = 'pgcache' AND p.proname = 'signal_version' AND p.pronargs = 0",
            &[],
        )
        .await?;
    if let Some(row) = marker {
        let body: String = row.try_get(0)?;
        state.version = signal_version_parse(&body);
    }
    Ok(state)
}

/// Install the triggers, replacing an earlier version's. `Ok(false)` when
/// the role may not create event triggers (SQLSTATE 42501): an administrator
/// has to have installed them, which the caller verifies.
pub(crate) async fn event_triggers_install(client: &Client, role: &str) -> Result<bool, Error> {
    match client.batch_execute(INSTALL_SQL).await {
        Ok(()) => {
            info!("schema-change event triggers installed on origin");
            Ok(true)
        }
        Err(e) if e.code() == Some(&SqlState::INSUFFICIENT_PRIVILEGE) => {
            info!("{role} may not create event triggers: relying on an administrator's install");
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

#[cfg(test)]
mod tests {
    use super::*;

    fn state(
        ddl_command_end: TriggerPresence,
        sql_drop: TriggerPresence,
        version: Option<i32>,
    ) -> EventTriggerState {
        EventTriggerState {
            ddl_command_end,
            sql_drop,
            version,
        }
    }

    #[test]
    fn test_defect_names_what_is_wrong_with_the_signal() {
        use TriggerPresence::{Disabled, Enabled, Missing};

        let current = Some(SIGNAL_VERSION);
        assert_eq!(state(Enabled, Enabled, current).defect(), None);
        assert_eq!(
            state(Missing, Missing, None).defect(),
            Some(SignalDefect::Missing)
        );
        // The marker alone is not the signal.
        assert_eq!(
            state(Missing, Missing, current).defect(),
            Some(SignalDefect::Missing)
        );
        for half in [
            state(Enabled, Missing, current),
            state(Missing, Enabled, current),
            state(Enabled, Disabled, current),
            state(Disabled, Disabled, current),
        ] {
            assert_eq!(half.defect(), Some(SignalDefect::Disabled));
        }
        for version in [None, Some(SIGNAL_VERSION - 1), Some(SIGNAL_VERSION + 1)] {
            assert_eq!(
                state(Enabled, Enabled, version).defect(),
                Some(SignalDefect::Stale)
            );
        }
    }

    #[test]
    fn test_signal_version_parse_reads_the_marker_body() {
        assert_eq!(signal_version_parse("SELECT 1"), Some(1));
        assert_eq!(signal_version_parse(" SELECT 12 "), Some(12));
        assert_eq!(signal_version_parse("SELECT one"), None);
        assert_eq!(signal_version_parse(""), None);
    }

    #[test]
    fn test_install_sql_records_the_version() {
        let marker = format!("AS 'SELECT {SIGNAL_VERSION}';");
        assert!(INSTALL_SQL.contains(&marker));
    }

    /// FNV-1a over the script. A change to [`INSTALL_SQL`] must come with a
    /// bump of `signal_version!` and the digest recorded here, or an origin
    /// holding the old script would pass for current.
    #[test]
    fn test_install_sql_change_requires_a_version_bump() {
        let digest = INSTALL_SQL
            .bytes()
            .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
                (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
            });
        assert_eq!(
            (SIGNAL_VERSION, digest),
            (1, 17_406_344_873_354_807_640),
            "INSTALL_SQL changed: bump signal_version! and record the new digest"
        );
    }
}
