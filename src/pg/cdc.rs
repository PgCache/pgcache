use std::io;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

use error_set::error_set;
use postgres_types::PgLsn;
use rootcause::Report;
use tokio_postgres::config::ReplicationMode;
use tokio_postgres::{Client, Error, SimpleQueryMessage};
use tracing::{debug, error, info, warn};

use super::connect::{config_build, config_connect, connect};
use super::event_trigger::{
    SignalDefect, event_triggers_install, event_triggers_shutdown_note, event_triggers_state_load,
    legacy_truncate_triggers_warn,
};
use crate::pg::{Lsn, identifier_quote_into};
use crate::result::{MapIntoReport, ReportExt, error_chain_format};
use crate::settings::{PgSettings, Settings};

error_set! {
    PgCdcError := {
        IoError(io::Error),
        PgError(Error),
        /// pgcache does not run without the schema-change signal.
        #[display("the schema-change event triggers on origin are {defect}")]
        SchemaSignal {
            defect: SignalDefect,
        },
    }
}

/// Result type with location-tracking error reports for CDC operations.
pub(crate) type PgCdcResult<T> = Result<T, Report<PgCdcError>>;

/// Prefix of the logical messages pgcache emits on origin (the event-trigger
/// schema-change signal and the writer's WAL-flush marker). A macro as well
/// as a const, so the trigger SQL can `concat!` the literal in.
macro_rules! message_prefix {
    () => {
        "pgcache"
    };
}
pub(crate) use message_prefix;
pub(crate) const MESSAGE_PREFIX: &str = message_prefix!();

/// What this process has put on origin, so that leaving removes exactly that.
/// A publication and slot it never provisioned may be another instance's, and
/// so may a slot held by a backend other than its own stream's.
struct OriginFootprint {
    provisioned: AtomicBool,
    /// Backend pid of the latest CDC stream; 0 before the first attaches.
    stream_pid: AtomicI32,
}

static ORIGIN_FOOTPRINT: OriginFootprint = OriginFootprint {
    provisioned: AtomicBool::new(false),
    stream_pid: AtomicI32::new(0),
};

/// Who holds the slot when pgcache leaves.
#[derive(Debug, PartialEq, Eq)]
enum SlotHolder {
    /// No backend, or the closing stream's own: ours to drop.
    Own,
    /// Another connection is streaming from it.
    Foreign(i32),
}

fn slot_holder_classify(active_pid: Option<i32>, stream_pid: i32) -> SlotHolder {
    match active_pid {
        Some(pid) if pid != stream_pid => SlotHolder::Foreign(pid),
        Some(_) | None => SlotHolder::Own,
    }
}

/// Record the CDC stream's backend, so cleanup can tell its slot's holder
/// from another instance's.
pub(crate) async fn stream_backend_record(client: &Client) -> Result<(), Error> {
    let messages = client.simple_query("SELECT pg_backend_pid()").await?;
    let pid = messages.iter().find_map(|message| {
        let SimpleQueryMessage::Row(row) = message else {
            return None;
        };
        row.get(0).and_then(|pid| pid.parse::<i32>().ok())
    });
    match pid {
        Some(pid) => ORIGIN_FOOTPRINT.stream_pid.store(pid, Ordering::Relaxed),
        None => warn!("could not read the CDC stream's backend pid"),
    }
    Ok(())
}

/// Ensures publication and replication slot exist on origin.
/// Uses a regular (non-replication) connection since replication connections can't run SQL.
pub(crate) async fn replication_provision(settings: &Settings) -> PgCdcResult<()> {
    let publication_name = &settings.cdc.publication_name;
    let slot_name = &settings.cdc.slot_name;

    debug!(
        "Provisioning replication: publication={}, slot={}",
        publication_name, slot_name
    );

    let client = connect(&settings.origin, "replication provisioning")
        .await
        .map_into_report::<PgCdcError>()
        .attach_loc("connecting for replication provision")?;

    debug!("cdc provisioning connected");
    ORIGIN_FOOTPRINT.provisioned.store(true, Ordering::Relaxed);

    // Drop and recreate publication to ensure clean state.
    // Created empty — tables are added dynamically as queries register.
    let drop_pub = format!("DROP PUBLICATION IF EXISTS {}", publication_name);
    client
        .execute(&drop_pub, &[])
        .await
        .map_into_report::<PgCdcError>()
        .attach_loc("dropping existing publication")?;

    // Without publish_via_partition_root a partition's rows stream under the
    // partition itself, whichever of its ancestors are published.
    let create_pub = format!("CREATE PUBLICATION {publication_name}");
    client
        .execute(&create_pub, &[])
        .await
        .map_into_report::<PgCdcError>()
        .attach_loc("creating publication")?;
    debug!("Publication created (empty)");

    event_triggers_install(&client, &settings.origin.user)
        .await
        .map_into_report::<PgCdcError>()
        .attach_loc("installing schema-change event triggers")?;
    let signal = event_triggers_state_load(&client)
        .await
        .map_into_report::<PgCdcError>()
        .attach_loc("reading the schema-change event triggers")?;
    if let Some(defect) = signal.defect() {
        return Err(PgCdcError::SchemaSignal { defect }.into());
    }
    legacy_truncate_triggers_warn(&client).await;

    // Check if replication slot exists
    let slot_exists = client
        .query_opt(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await
        .map_into_report::<PgCdcError>()?
        .is_some();

    if !slot_exists {
        debug!("Creating replication slot: {}", slot_name);
        client
            .execute(
                "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
                &[&slot_name],
            )
            .await
            .map_into_report::<PgCdcError>()
            .attach_loc("creating replication slot")?;
        debug!("Replication slot created successfully");
    } else {
        debug!("Replication slot already exists: {}", slot_name);
    }

    Ok(())
}

/// Remove what this process provisioned on origin: its publication, and its
/// replication slot unless another connection is streaming from it. Runs
/// when pgcache leaves, after the CDC stream's connection is closed; does
/// nothing when provisioning never ran.
pub(crate) async fn replication_cleanup(settings: &Settings) -> PgCdcResult<()> {
    if !ORIGIN_FOOTPRINT.provisioned.load(Ordering::Relaxed) {
        return Ok(());
    }
    let publication_name = &settings.cdc.publication_name;
    let slot_name = &settings.cdc.slot_name;

    debug!(
        "Cleaning up replication: publication={}, slot={}",
        publication_name, slot_name
    );

    let client = connect(&settings.origin, "replication cleanup")
        .await
        .map_into_report::<PgCdcError>()
        .attach_loc("connecting for replication cleanup")?;

    let slot = client
        .query_opt(
            "SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await
        .map_into_report::<PgCdcError>()
        .attach_loc("looking up the replication slot")?;
    if let Some(row) = slot {
        let stream_pid = ORIGIN_FOOTPRINT.stream_pid.load(Ordering::Relaxed);
        match slot_holder_classify(row.get(0), stream_pid) {
            SlotHolder::Own => {
                replication_slot_drop(settings)
                    .await
                    .attach_loc("dropping replication slot")?;
                info!("dropped replication slot {slot_name} on origin");
            }
            SlotHolder::Foreign(pid) => warn!(
                "replication slot {slot_name} is held by another connection (backend {pid}); \
                 leaving it on origin"
            ),
        }
    }

    debug!("Dropping publication: {}", publication_name);
    let drop_pub = format!("DROP PUBLICATION IF EXISTS {}", publication_name);
    client
        .execute(&drop_pub, &[])
        .await
        .map_into_report::<PgCdcError>()
        .attach_loc("dropping publication")?;

    event_triggers_shutdown_note(&client).await;

    debug!("Replication cleanup complete");
    Ok(())
}

/// Drop the slot over a replication connection, whose `WAIT` holds the
/// command until the slot is released. `pg_drop_replication_slot` raises on
/// an active slot instead, and the stream's backend can outlive the
/// connection pgcache closed.
async fn replication_slot_drop(settings: &Settings) -> PgCdcResult<()> {
    let client = connect_replication(&settings.replication, "replication slot drop")
        .await
        .map_into_report::<PgCdcError>()
        .attach_loc("connecting to drop the replication slot")?;

    let mut command = String::from("DROP_REPLICATION_SLOT ");
    identifier_quote_into(&settings.cdc.slot_name, &mut command);
    command.push_str(" WAIT");
    debug!("Dropping replication slot: {}", settings.cdc.slot_name);
    client
        .simple_query(&command)
        .await
        .map_into_report::<PgCdcError>()?;
    Ok(())
}

/// Query the replication slot's confirmed flush LSN.
///
/// Returns `Some(lsn)` if the slot exists, `None` if the slot has been dropped.
/// A never-consumed slot may have a NULL confirmed_flush_lsn, which is returned as 0.
pub(crate) async fn slot_confirmed_lsn(settings: &Settings) -> PgCdcResult<Option<Lsn>> {
    let slot_name = &settings.cdc.slot_name;

    let client = connect(&settings.origin, "slot LSN check")
        .await
        .map_into_report::<PgCdcError>()
        .attach_loc("connecting to check slot LSN")?;

    let row = client
        .query_opt(
            "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await
        .map_into_report::<PgCdcError>()
        .attach_loc("querying confirmed_flush_lsn")?;

    match row {
        Some(row) => {
            let lsn: Option<PgLsn> = row.get(0);
            Ok(Some(lsn.map(Lsn::from).unwrap_or_default()))
        }
        None => Ok(None),
    }
}

/// Connect to a PostgreSQL database in logical replication mode.
///
/// This is used for CDC streaming connections that receive logical replication events.
pub(crate) async fn connect_replication(
    settings: &PgSettings,
    context: &str,
) -> Result<Client, Error> {
    let mut config = config_build(settings);
    config.replication_mode(ReplicationMode::Logical);
    debug!(
        "[{context}] connecting to postgres (replication=logical) {}:{} db={} user={} ssl={:?}",
        settings.host, settings.port, settings.database, settings.user, settings.ssl_mode
    );
    let result = config_connect(config, settings.ssl_mode, context).await;
    if let Err(ref e) = result {
        error!(
            "[{context}] replication connection failed to {}:{} db={} user={} ssl={:?}: {}",
            settings.host,
            settings.port,
            settings.database,
            settings.user,
            settings.ssl_mode,
            error_chain_format(e),
        );
        // Logical replication cannot run over a connection pooler (PgBouncer,
        // Neon's `-pooler` endpoint, etc.). This is the most common CDC
        // misconfiguration, so call it out explicitly rather than leaving the
        // operator to decode a generic connection error.
        if settings.host.contains("pooler") || settings.host.contains("pgbouncer") {
            error!(
                "[{context}] replication host {:?} looks like a connection pooler; logical \
                 replication requires a direct connection — point replication at the origin's \
                 direct (non-pooled) endpoint via REPLICATION_URL or REPLICATION_HOST",
                settings.host,
            );
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slot_holder_classify() {
        assert_eq!(slot_holder_classify(None, 0), SlotHolder::Own);
        assert_eq!(slot_holder_classify(None, 41), SlotHolder::Own);
        assert_eq!(slot_holder_classify(Some(41), 41), SlotHolder::Own);
        assert_eq!(slot_holder_classify(Some(41), 42), SlotHolder::Foreign(41));
        // A startup that failed before any stream attached holds nothing.
        assert_eq!(slot_holder_classify(Some(41), 0), SlotHolder::Foreign(41));
    }
}
