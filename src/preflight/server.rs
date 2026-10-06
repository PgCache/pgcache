//! Server-level settings: hosting environment, version, wal_level,
//! replication slot capacity and pg_stat_statements.

use tokio_postgres::{Client, Error};

use super::{CheckResult, Environment};

const MIN_SERVER_VERSION_NUM: i32 = 160_000;

// --- environment --------------------------------------------------------

pub(super) async fn environment_detect(client: &Client) -> Environment {
    let Ok(rows) = client
        .query(
            "SELECT rolname::text FROM pg_roles \
             WHERE rolname IN ('rds_superuser', 'cloudsqlsuperuser', 'neon_superuser')",
            &[],
        )
        .await
    else {
        return Environment::SelfHosted;
    };
    for row in rows {
        match row.try_get::<_, String>(0).as_deref() {
            Ok("rds_superuser") => return Environment::Rds,
            Ok("cloudsqlsuperuser") => return Environment::CloudSql,
            Ok("neon_superuser") => return Environment::Neon,
            _ => {}
        }
    }
    Environment::SelfHosted
}

// --- server version -----------------------------------------------------

pub(super) async fn server_version_check(client: &Client) -> CheckResult {
    match server_version_load(client).await {
        Ok((num, text)) => server_version_evaluate(num, &text),
        Err(e) => CheckResult::skipped("server version", e),
    }
}

async fn server_version_load(client: &Client) -> Result<(i32, String), Error> {
    let row = client
        .query_one(
            "SELECT current_setting('server_version_num')::int, current_setting('server_version')",
            &[],
        )
        .await?;
    Ok((row.try_get(0)?, row.try_get(1)?))
}

fn server_version_evaluate(num: i32, text: &str) -> CheckResult {
    if num >= MIN_SERVER_VERSION_NUM {
        CheckResult::pass("server version", format!("PostgreSQL {text}"))
    } else {
        CheckResult::fail(
            "server version",
            format!("PostgreSQL {text} is too old"),
            "pgcache needs PostgreSQL 16 or newer on the origin.",
        )
    }
}

// --- wal_level ----------------------------------------------------------

pub(super) async fn wal_level_check(client: &Client, environment: Environment) -> CheckResult {
    let level = client
        .query_one("SELECT current_setting('wal_level')", &[])
        .await
        .and_then(|row| row.try_get::<_, String>(0));
    match level {
        Ok(level) => wal_level_evaluate(&level, environment),
        Err(e) => CheckResult::skipped("wal_level", e),
    }
}

fn wal_level_evaluate(level: &str, environment: Environment) -> CheckResult {
    if level == "logical" {
        return CheckResult::pass("wal_level", "logical");
    }
    let remediation = match environment {
        Environment::SelfHosted => {
            "ALTER SYSTEM SET wal_level = logical;\n\
             then restart PostgreSQL (a reload is not enough)."
        }
        Environment::Rds => {
            "Set rds.logical_replication = 1 in the instance's DB parameter group, then reboot \
             the instance."
        }
        Environment::CloudSql => {
            "Set the cloudsql.logical_decoding flag to on; Cloud SQL restarts the instance to \
             apply it."
        }
        Environment::Neon => {
            "Enable logical replication for the project in the Neon console (Project settings, \
             Logical replication)."
        }
    };
    CheckResult::fail(
        "wal_level",
        format!("wal_level is '{level}'; logical replication needs 'logical'"),
        remediation,
    )
}

// --- replication slot capacity -----------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SlotCapacity {
    pub max_slots: i32,
    pub used_slots: i32,
    pub max_senders: i32,
    pub active_senders: i32,
    /// Our own slot already exists, so it needs no free capacity.
    pub own_slot_exists: bool,
    pub inactive_slots: Vec<String>,
}

pub(super) async fn replication_slots_check(
    client: &Client,
    slot_name: &str,
    environment: Environment,
) -> CheckResult {
    match slot_capacity_load(client, slot_name).await {
        Ok(capacity) => replication_slots_evaluate(&capacity, environment),
        Err(e) => CheckResult::skipped("replication slots", e),
    }
}

async fn slot_capacity_load(client: &Client, slot_name: &str) -> Result<SlotCapacity, Error> {
    let row = client
        .query_one(
            "SELECT current_setting('max_replication_slots')::int, \
                    (SELECT count(*)::int FROM pg_replication_slots), \
                    current_setting('max_wal_senders')::int, \
                    (SELECT count(*)::int FROM pg_stat_replication), \
                    EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1), \
                    (SELECT coalesce(array_agg(slot_name::text ORDER BY slot_name), '{}') \
                       FROM pg_replication_slots WHERE NOT active AND slot_name <> $1)",
            &[&slot_name],
        )
        .await?;
    Ok(SlotCapacity {
        max_slots: row.try_get(0)?,
        used_slots: row.try_get(1)?,
        max_senders: row.try_get(2)?,
        active_senders: row.try_get(3)?,
        own_slot_exists: row.try_get(4)?,
        inactive_slots: row.try_get(5)?,
    })
}

fn replication_slots_evaluate(capacity: &SlotCapacity, environment: Environment) -> CheckResult {
    let slot_free = capacity.own_slot_exists || capacity.used_slots < capacity.max_slots;
    let sender_free = capacity.active_senders < capacity.max_senders;
    let usage = format!(
        "{} of {} replication slots and {} of {} wal senders in use",
        capacity.used_slots, capacity.max_slots, capacity.active_senders, capacity.max_senders
    );
    if slot_free && sender_free {
        return CheckResult::pass("replication slots", usage);
    }

    let mut lines = Vec::new();
    let raise_hint = match environment {
        Environment::SelfHosted => {
            if !slot_free {
                lines.push(format!(
                    "ALTER SYSTEM SET max_replication_slots = {};",
                    capacity.max_slots + 1
                ));
            }
            if !sender_free {
                lines.push(format!(
                    "ALTER SYSTEM SET max_wal_senders = {};",
                    capacity.max_senders + 1
                ));
            }
            "then restart PostgreSQL."
        }
        Environment::Rds => {
            "Raise max_replication_slots and max_wal_senders in the DB parameter group, then \
             reboot the instance."
        }
        Environment::CloudSql | Environment::Neon => {
            "Raise max_replication_slots and max_wal_senders through the provider's settings \
             (a restart is required)."
        }
    };
    lines.push(raise_hint.to_owned());
    if let Some(first_inactive) = capacity.inactive_slots.first().filter(|_| !slot_free) {
        lines.push(format!(
            "Or drop an inactive slot you no longer need: SELECT pg_drop_replication_slot('{first_inactive}');"
        ));
    }
    CheckResult::fail(
        "replication slots",
        format!("{usage}; pgcache needs one of each"),
        lines.join("\n"),
    )
}

// --- pg_stat_statements -------------------------------------------------

pub(super) async fn pg_stat_statements_check(client: &Client) -> CheckResult {
    let name = "pg_stat_statements";
    let installed = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')",
            &[],
        )
        .await
        .and_then(|row| row.try_get::<_, bool>(0));
    match installed {
        Ok(true) => CheckResult::pass(
            name,
            "installed; export it to the Fit Analyzer (pgcache.com/fit) to score this workload",
        ),
        Ok(false) => CheckResult::pass(
            name,
            "not installed (optional; only the Fit Analyzer uses it)",
        ),
        Err(e) => CheckResult::skipped(name, e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::preflight::Verdict;

    #[test]
    fn test_server_version_evaluate_rejects_pre_16() {
        assert_eq!(
            server_version_evaluate(150_004, "15.4").verdict,
            Verdict::Fail
        );
        assert_eq!(
            server_version_evaluate(160_000, "16.0").verdict,
            Verdict::Pass
        );
    }

    #[test]
    fn test_wal_level_evaluate_words_remediation_by_environment() {
        assert_eq!(
            wal_level_evaluate("logical", Environment::Rds).verdict,
            Verdict::Pass
        );
        let self_hosted = wal_level_evaluate("replica", Environment::SelfHosted);
        assert_eq!(self_hosted.verdict, Verdict::Fail);
        assert!(
            self_hosted
                .remediation
                .as_deref()
                .is_some_and(|r| r.contains("ALTER SYSTEM SET wal_level = logical"))
        );
        let rds = wal_level_evaluate("replica", Environment::Rds);
        assert!(
            rds.remediation
                .as_deref()
                .is_some_and(|r| r.contains("rds.logical_replication"))
        );
    }

    #[test]
    fn test_replication_slots_evaluate_counts_own_slot_as_free() {
        let full = SlotCapacity {
            max_slots: 2,
            used_slots: 2,
            max_senders: 10,
            active_senders: 0,
            own_slot_exists: false,
            inactive_slots: vec!["old_slot".to_owned()],
        };
        let result = replication_slots_evaluate(&full, Environment::SelfHosted);
        assert_eq!(result.verdict, Verdict::Fail);
        let remediation = result.remediation.unwrap_or_default();
        assert!(remediation.contains("max_replication_slots = 3"));
        assert!(remediation.contains("pg_drop_replication_slot('old_slot')"));

        let own = SlotCapacity {
            own_slot_exists: true,
            ..full
        };
        assert_eq!(
            replication_slots_evaluate(&own, Environment::SelfHosted).verdict,
            Verdict::Pass
        );
    }
}
