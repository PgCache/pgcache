//! The connecting role's privileges and any existing publication or slot.

use tokio_postgres::{Client, Error};

use super::{CheckResult, Environment};
use crate::settings::{CdcSettings, PgSettings};

// --- role privileges ----------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RoleInfo {
    pub name: String,
    pub superuser: bool,
    pub replication: bool,
    pub create_on_database: bool,
    pub rds_replication: bool,
}

pub(super) async fn role_info_load(client: &Client) -> Result<RoleInfo, Error> {
    let row = client
        .query_one(
            "SELECT current_user::text, r.rolsuper, r.rolreplication, \
                    has_database_privilege(current_database(), 'CREATE'), \
                    CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rds_replication') \
                         THEN pg_has_role(current_user, 'rds_replication', 'MEMBER') \
                         ELSE false END \
             FROM pg_roles r WHERE r.rolname = current_user",
            &[],
        )
        .await?;
    Ok(RoleInfo {
        name: row.try_get(0)?,
        superuser: row.try_get(1)?,
        replication: row.try_get(2)?,
        create_on_database: row.try_get(3)?,
        rds_replication: row.try_get(4)?,
    })
}

pub(super) fn role_privileges_evaluate(
    role: &RoleInfo,
    origin: &PgSettings,
    environment: Environment,
) -> CheckResult {
    if role.superuser {
        return CheckResult::pass("role privileges", format!("{} is a superuser", role.name));
    }
    let can_replicate = role.replication || role.rds_replication;
    if can_replicate && role.create_on_database {
        return CheckResult::pass(
            "role privileges",
            format!("{} has REPLICATION and CREATE on the database", role.name),
        );
    }

    let (missing, statements) = privilege_grants_missing(role, origin, environment);
    CheckResult::fail(
        "role privileges",
        format!(
            "{} lacks {} (needed to create the publication and slot)",
            role.name,
            missing.join(" and ")
        ),
        format!(
            "{}\n{}",
            admin_role_hint(environment),
            statements.join("\n")
        ),
    )
}

/// The privileges the role lacks and the GRANT statements that add them.
fn privilege_grants_missing(
    role: &RoleInfo,
    origin: &PgSettings,
    environment: Environment,
) -> (Vec<&'static str>, Vec<String>) {
    let mut missing = Vec::new();
    let mut statements = Vec::new();
    if !(role.replication || role.rds_replication) {
        missing.push("REPLICATION");
        statements.push(match environment {
            Environment::Rds => format!("GRANT rds_replication TO \"{}\";", role.name),
            Environment::SelfHosted | Environment::CloudSql | Environment::Neon => {
                format!("ALTER ROLE \"{}\" REPLICATION;", role.name)
            }
        });
    }
    if !role.create_on_database {
        missing.push("CREATE on the database");
        statements.push(format!(
            "GRANT CREATE ON DATABASE \"{}\" TO \"{}\";",
            origin.database, role.name
        ));
    }
    (missing, statements)
}

/// Who should run the grants: RDS has no superuser, only its master user.
fn admin_role_hint(environment: Environment) -> &'static str {
    match environment {
        Environment::Rds => "Run as the master user:",
        Environment::SelfHosted | Environment::CloudSql | Environment::Neon => {
            "Run as a superuser:"
        }
    }
}

// --- existing CDC objects -----------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CdcObjects {
    pub publication_exists: bool,
    /// `Some(active)` when a slot with our name already exists.
    pub slot_active: Option<bool>,
    pub slot_retained_wal: Option<String>,
}

pub(super) async fn cdc_objects_check(client: &Client, cdc: &CdcSettings) -> CheckResult {
    match cdc_objects_load(client, cdc).await {
        Ok(objects) => cdc_objects_evaluate(&objects, cdc),
        Err(e) => CheckResult::skipped("existing cdc objects", e),
    }
}

async fn cdc_objects_load(client: &Client, cdc: &CdcSettings) -> Result<CdcObjects, Error> {
    let row = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1), \
                    s.active, \
                    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), s.restart_lsn)) \
             FROM (SELECT 1) AS one \
             LEFT JOIN pg_replication_slots s ON s.slot_name = $2",
            &[&cdc.publication_name, &cdc.slot_name],
        )
        .await?;
    Ok(CdcObjects {
        publication_exists: row.try_get(0)?,
        slot_active: row.try_get(1)?,
        slot_retained_wal: row.try_get(2)?,
    })
}

fn cdc_objects_evaluate(objects: &CdcObjects, cdc: &CdcSettings) -> CheckResult {
    let name = "existing cdc objects";
    match objects.slot_active {
        Some(true) => CheckResult::fail(
            name,
            format!(
                "replication slot '{}' is already active: another pgcache or consumer is \
                 attached to it",
                cdc.slot_name
            ),
            "Stop the other instance, or give this one distinct names with CDC_SUFFIX \
             (or --cdc_slot_name and --cdc_publication_name).",
        ),
        Some(false) => CheckResult::warn(
            name,
            format!(
                "slot '{}' exists from an earlier run and holds {} of WAL",
                cdc.slot_name,
                objects
                    .slot_retained_wal
                    .as_deref()
                    .unwrap_or("an unknown amount")
            ),
            format!(
                "pgcache will reuse it. If it is stale, drop it so the origin can reclaim WAL:\n\
                 SELECT pg_drop_replication_slot('{}');",
                cdc.slot_name
            ),
        ),
        None if objects.publication_exists => CheckResult::pass(
            name,
            format!(
                "publication '{}' exists (pgcache recreates it at startup); slot '{}' will be \
                 created",
                cdc.publication_name, cdc.slot_name
            ),
        ),
        None => CheckResult::pass(
            name,
            format!(
                "none yet; pgcache creates publication '{}' and slot '{}' at startup",
                cdc.publication_name, cdc.slot_name
            ),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::preflight::Verdict;
    use crate::settings::SslMode;

    fn origin() -> PgSettings {
        PgSettings {
            host: "db".to_owned(),
            port: 5432,
            user: "app".to_owned(),
            password: None,
            database: "shop".to_owned(),
            ssl_mode: SslMode::Disable,
        }
    }

    #[test]
    fn test_role_privileges_evaluate_lists_missing_grants() {
        let role = RoleInfo {
            name: "app".to_owned(),
            superuser: false,
            replication: false,
            create_on_database: false,
            rds_replication: false,
        };
        let result = role_privileges_evaluate(&role, &origin(), Environment::SelfHosted);
        assert_eq!(result.verdict, Verdict::Fail);
        assert!(
            result
                .finding
                .contains("REPLICATION and CREATE on the database")
        );
        let remediation = result.remediation.unwrap_or_default();
        assert!(remediation.contains("ALTER ROLE \"app\" REPLICATION;"));
        assert!(remediation.contains("GRANT CREATE ON DATABASE \"shop\" TO \"app\";"));

        let rds = role_privileges_evaluate(&role, &origin(), Environment::Rds);
        assert!(
            rds.remediation
                .as_deref()
                .is_some_and(|r| r.contains("GRANT rds_replication TO \"app\";"))
        );

        let granted = RoleInfo {
            replication: true,
            create_on_database: true,
            ..role
        };
        assert_eq!(
            role_privileges_evaluate(&granted, &origin(), Environment::SelfHosted).verdict,
            Verdict::Pass
        );
    }

    #[test]
    fn test_cdc_objects_evaluate_active_slot_fails_inactive_warns() {
        let cdc = CdcSettings {
            publication_name: "pgcache_pub".to_owned(),
            slot_name: "pgcache_slot".to_owned(),
        };
        let active = CdcObjects {
            publication_exists: true,
            slot_active: Some(true),
            slot_retained_wal: Some("16 MB".to_owned()),
        };
        assert_eq!(cdc_objects_evaluate(&active, &cdc).verdict, Verdict::Fail);
        let inactive = CdcObjects {
            slot_active: Some(false),
            ..active
        };
        let result = cdc_objects_evaluate(&inactive, &cdc);
        assert_eq!(result.verdict, Verdict::Warn);
        assert!(result.finding.contains("16 MB"));
        let none = CdcObjects {
            publication_exists: false,
            slot_active: None,
            slot_retained_wal: None,
        };
        assert_eq!(cdc_objects_evaluate(&none, &cdc).verdict, Verdict::Pass);
    }
}
