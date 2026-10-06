//! Connecting to the origin, normally and for replication, with remediation
//! worded for the failure and the host.

use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, Error};

use super::{CheckResult, Environment};
use crate::pg::cdc::connect_replication;
use crate::pg::connect;
use crate::result::error_chain_format;
use crate::settings::PgSettings;

fn in_docker() -> bool {
    std::env::var_os("PGCACHE_DOCKER").is_some()
}

fn host_is_loopback(host: &str) -> bool {
    matches!(host, "localhost" | "127.0.0.1" | "::1")
}

fn host_looks_pooled(host: &str) -> bool {
    host.contains("pooler") || host.contains("pgbouncer")
}

pub(super) async fn origin_connect_check(
    settings: &PgSettings,
) -> Result<(CheckResult, Client), CheckResult> {
    match connect(settings, "preflight").await {
        Ok(client) => {
            let version = client
                .query_one("SELECT current_setting('server_version')", &[])
                .await
                .and_then(|row| row.try_get::<_, String>(0))
                .unwrap_or_else(|_| "unknown version".to_owned());
            Ok((
                CheckResult::pass("connection", format!("PostgreSQL {version}")),
                client,
            ))
        }
        Err(e) => Err(connect_failure(settings, &e)),
    }
}

fn connect_failure(settings: &PgSettings, error: &Error) -> CheckResult {
    let detail = error_chain_format(error);
    let remediation = match error.as_db_error() {
        Some(db) => db_connect_remediation(settings, db.code(), db.message()),
        None => transport_remediation(settings, &detail),
    };
    CheckResult::fail(
        "connection",
        format!(
            "could not connect to {}:{}: {detail}",
            settings.host, settings.port
        ),
        remediation,
    )
}

fn db_connect_remediation(settings: &PgSettings, code: &SqlState, message: &str) -> String {
    if *code == SqlState::INVALID_PASSWORD {
        return format!(
            "The password for role {} was rejected. Check it, or pass it with ORIGIN_PASSWORD.",
            settings.user
        );
    }
    if *code == SqlState::INVALID_CATALOG_NAME {
        return format!(
            "Database {} does not exist on this server. Check ORIGIN_DATABASE.",
            settings.database
        );
    }
    if *code == SqlState::INVALID_AUTHORIZATION_SPECIFICATION {
        if message.contains("SSL") || message.contains("encryption") {
            return "The server requires TLS for this client. Set ORIGIN_SSL_MODE=require \
                    (or add sslmode=require to the URL)."
                .to_owned();
        }
        return format!(
            "Add a pg_hba.conf entry that lets this client in, for example:\n  \
             host  {}  {}  <pgcache address>/32  scram-sha-256\n\
             then reload: SELECT pg_reload_conf();",
            settings.database, settings.user
        );
    }
    "Check the credentials and the server log for the rejection reason.".to_owned()
}

/// What a non-database connection failure was, read from its error text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransportFailure {
    PasswordMissing,
    Refused,
    TimedOut,
    Unresolved,
    Tls,
    Other,
}

impl TransportFailure {
    /// Error-text markers per failure, tested in order; the first match wins.
    const MARKERS: &[(&[&str], Self)] = &[
        (&["password missing"], Self::PasswordMissing),
        (&["refused"], Self::Refused),
        (&["timed out"], Self::TimedOut),
        (&["lookup", "resolve"], Self::Unresolved),
        (&["tls", "TLS", "certificate"], Self::Tls),
    ];

    fn classify(detail: &str) -> Self {
        Self::MARKERS
            .iter()
            .find(|(markers, _)| markers.iter().any(|marker| detail.contains(marker)))
            .map_or(Self::Other, |(_, failure)| *failure)
    }
}

fn transport_remediation(settings: &PgSettings, detail: &str) -> String {
    let failure = TransportFailure::classify(detail);
    let unreachable = matches!(
        failure,
        TransportFailure::Refused | TransportFailure::TimedOut
    );
    let container_loopback = host_is_loopback(&settings.host) && in_docker();
    if unreachable && container_loopback {
        return "Inside a container, localhost is the container itself. Use host.docker.internal \
                as the origin host (on Linux add --add-host=host.docker.internal:host-gateway \
                to docker run)."
            .to_owned();
    }
    let remediation = match failure {
        TransportFailure::PasswordMissing => {
            "The server asked for a password and none was given. Pass it with \
             ORIGIN_PASSWORD (or in the URL)."
        }
        TransportFailure::Refused => {
            "Nothing is listening at that host and port. Check that PostgreSQL is running, that \
             listen_addresses covers this interface, and that the port is right."
        }
        TransportFailure::TimedOut => {
            "The host did not answer. Check the hostname, firewall or security-group rules, and \
             that the port is open."
        }
        TransportFailure::Unresolved => "The hostname did not resolve. Check it for a typo.",
        TransportFailure::Tls => {
            "TLS negotiation failed. Try ORIGIN_SSL_MODE=require, or verify-full only when the \
             server certificate is signed by a trusted CA."
        }
        TransportFailure::Other => "Check the host, port and network path to the origin.",
    };
    remediation.to_owned()
}

pub(super) async fn replication_connect_check(
    settings: &PgSettings,
    environment: Environment,
) -> CheckResult {
    match connect_replication(settings, "preflight").await {
        Ok(_client) => CheckResult::pass(
            "replication connection",
            format!(
                "logical replication connection to {}:{} accepted",
                settings.host, settings.port
            ),
        ),
        Err(e) => CheckResult::fail(
            "replication connection",
            format!(
                "could not open a logical replication connection to {}:{}: {}",
                settings.host,
                settings.port,
                error_chain_format(&e)
            ),
            replication_remediation(settings, &e, environment),
        ),
    }
}

fn replication_remediation(
    settings: &PgSettings,
    error: &Error,
    environment: Environment,
) -> String {
    if host_looks_pooled(&settings.host) {
        return "This host looks like a connection pooler, and logical replication needs a direct \
                connection. Point REPLICATION_URL (or REPLICATION_HOST) at the origin's direct \
                endpoint."
            .to_owned();
    }
    let Some(db) = error.as_db_error() else {
        return "If a pooler sits between pgcache and the origin, point REPLICATION_URL at the \
                direct endpoint."
            .to_owned();
    };
    let code = db.code();
    if *code == SqlState::INVALID_AUTHORIZATION_SPECIFICATION {
        return format!(
            "Add a pg_hba.conf entry that lets role {} reach database {} (logical replication \
             connections match the database name, not the 'replication' keyword), then \
             SELECT pg_reload_conf();",
            settings.user, settings.database
        );
    }
    if *code == SqlState::INSUFFICIENT_PRIVILEGE {
        return match environment {
            Environment::Rds => format!("GRANT rds_replication TO \"{}\";", settings.user),
            Environment::SelfHosted | Environment::CloudSql | Environment::Neon => {
                format!("ALTER ROLE \"{}\" REPLICATION;", settings.user)
            }
        };
    }
    if *code == SqlState::TOO_MANY_CONNECTIONS {
        return "Every wal sender is in use. Raise max_wal_senders (restart required) or stop an \
                unused replication client."
            .to_owned();
    }
    "Check the origin server log for the rejection reason.".to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transport_failure_classify_checks_markers_in_order() {
        let cases = [
            ("password missing", TransportFailure::PasswordMissing),
            ("connection refused", TransportFailure::Refused),
            ("connect timed out", TransportFailure::TimedOut),
            ("failed to lookup address", TransportFailure::Unresolved),
            ("could not resolve host", TransportFailure::Unresolved),
            ("tls handshake", TransportFailure::Tls),
            ("invalid certificate", TransportFailure::Tls),
            ("connection reset", TransportFailure::Other),
            // Earlier markers win when several appear.
            (
                "password missing; refused",
                TransportFailure::PasswordMissing,
            ),
            ("refused after timed out", TransportFailure::Refused),
        ];
        for (detail, expected) in cases {
            assert_eq!(TransportFailure::classify(detail), expected, "{detail}");
        }
    }
}
