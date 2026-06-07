use error_set::error_set;
use postgres_types::PgLsn;
use rootcause::Report;
use std::io;
use tokio_postgres::config::ReplicationMode;
use tokio_postgres::{Client, Error};
use tracing::{debug, error};

use crate::result::{MapIntoReport, ReportExt, error_chain_format};
use crate::settings::{PgSettings, Settings};

use super::connect::{config_build, config_connect, connect};

error_set! {
    PgCdcError := {
        IoError(io::Error),
        PgError(Error),
    }
}

/// Result type with location-tracking error reports for CDC operations.
pub type PgCdcResult<T> = Result<T, Report<PgCdcError>>;

/// Ensures publication and replication slot exist on origin.
/// Uses a regular (non-replication) connection since replication connections can't run SQL.
pub async fn replication_provision(settings: &Settings) -> PgCdcResult<()> {
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

    // Drop and recreate publication to ensure clean state.
    // Created empty — tables are added dynamically as queries register.
    let drop_pub = format!("DROP PUBLICATION IF EXISTS {}", publication_name);
    client
        .execute(&drop_pub, &[])
        .await
        .map_into_report::<PgCdcError>()
        .attach_loc("dropping existing publication")?;

    // publish_via_partition_root ensures CDC reports parent table OID for partition changes
    let create_pub = format!(
        "CREATE PUBLICATION {} WITH (publish_via_partition_root = true)",
        publication_name
    );
    client
        .execute(&create_pub, &[])
        .await
        .map_into_report::<PgCdcError>()
        .attach_loc("creating publication")?;
    debug!("Publication created (empty)");

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

/// Cleanup publication and replication slot from origin.
/// Uses a regular (non-replication) connection since replication connections can't run SQL.
pub async fn replication_cleanup(settings: &Settings) -> PgCdcResult<()> {
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

    // Drop replication slot if it exists
    debug!("Dropping replication slot: {}", slot_name);
    client
        .execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await
        .map_into_report::<PgCdcError>()
        .attach_loc("dropping replication slot")?;

    // Drop publication if it exists
    debug!("Dropping publication: {}", publication_name);
    let drop_pub = format!("DROP PUBLICATION IF EXISTS {}", publication_name);
    client
        .execute(&drop_pub, &[])
        .await
        .map_into_report::<PgCdcError>()
        .attach_loc("dropping publication")?;

    debug!("Replication cleanup complete");
    Ok(())
}

/// Query the replication slot's confirmed flush LSN.
///
/// Returns `Some(lsn)` if the slot exists, `None` if the slot has been dropped.
/// A never-consumed slot may have a NULL confirmed_flush_lsn, which is returned as 0.
pub async fn slot_confirmed_lsn(settings: &Settings) -> PgCdcResult<Option<u64>> {
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
            Ok(Some(lsn.map(u64::from).unwrap_or(0)))
        }
        None => Ok(None),
    }
}

/// Connect to a PostgreSQL database in logical replication mode.
///
/// This is used for CDC streaming connections that receive logical replication events.
pub async fn connect_replication(settings: &PgSettings, context: &str) -> Result<Client, Error> {
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
