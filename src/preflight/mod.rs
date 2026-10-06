//! Origin readiness checks behind `pgcache --check`.
//!
//! A first run usually fails on the origin side (wal_level, privileges, a
//! stale slot) or silently never caches (no primary keys, views, RLS). Each
//! check names one such cause and carries its own remediation, worded for
//! the detected hosting environment. Each check has a `*_load` that queries
//! the origin and a pure `*_evaluate` that turns the rows into a verdict, so
//! the verdict logic is unit-testable without a database.

mod connection;
mod privileges;
mod relations;
mod report;
mod server;

use std::fmt::Display;

use crate::settings::PreflightSettings;

pub use report::report_render;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Pass,
    Warn,
    Fail,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckResult {
    pub name: &'static str,
    pub verdict: Verdict,
    pub finding: String,
    pub remediation: Option<String>,
}

impl CheckResult {
    pub fn pass(name: &'static str, finding: impl Into<String>) -> Self {
        Self {
            name,
            verdict: Verdict::Pass,
            finding: finding.into(),
            remediation: None,
        }
    }

    pub fn warn(
        name: &'static str,
        finding: impl Into<String>,
        remediation: impl Into<String>,
    ) -> Self {
        Self {
            name,
            verdict: Verdict::Warn,
            finding: finding.into(),
            remediation: Some(remediation.into()),
        }
    }

    pub fn fail(
        name: &'static str,
        finding: impl Into<String>,
        remediation: impl Into<String>,
    ) -> Self {
        Self {
            name,
            verdict: Verdict::Fail,
            finding: finding.into(),
            remediation: Some(remediation.into()),
        }
    }

    /// A check whose query failed. The origin may still be fine, so this is
    /// a warning rather than a failure.
    pub fn skipped(name: &'static str, error: impl Display) -> Self {
        Self {
            name,
            verdict: Verdict::Warn,
            finding: format!("could not check: {error}"),
            remediation: None,
        }
    }
}

/// Hosting environment, which decides how a remediation is worded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Environment {
    #[default]
    SelfHosted,
    Rds,
    CloudSql,
    Neon,
}

#[derive(Debug, Default)]
pub struct PreflightReport {
    /// `user@host:port/database` of the origin that was checked.
    pub target: String,
    pub results: Vec<CheckResult>,
}

impl PreflightReport {
    pub fn failed(&self) -> bool {
        self.count(Verdict::Fail) > 0
    }

    pub fn count(&self, verdict: Verdict) -> usize {
        self.results.iter().filter(|r| r.verdict == verdict).count()
    }
}

/// Run every check against the origin. A failed connection ends the run,
/// since nothing else can be observed without one.
pub async fn preflight_run(settings: &PreflightSettings) -> PreflightReport {
    let origin = &settings.origin;
    let mut report = PreflightReport {
        target: format!(
            "{}@{}:{}/{}",
            origin.user, origin.host, origin.port, origin.database
        ),
        results: Vec::new(),
    };

    let client = match connection::origin_connect_check(origin).await {
        Ok((result, client)) => {
            report.results.push(result);
            client
        }
        Err(result) => {
            report.results.push(result);
            return report;
        }
    };

    let environment = server::environment_detect(&client).await;
    report
        .results
        .push(server::server_version_check(&client).await);
    report
        .results
        .push(server::wal_level_check(&client, environment).await);
    report
        .results
        .push(server::replication_slots_check(&client, &settings.cdc.slot_name, environment).await);

    let role = privileges::role_info_load(&client).await;
    let (superuser, role_name) = match &role {
        Ok(role) => (role.superuser, role.name.clone()),
        Err(_) => (false, origin.user.clone()),
    };
    report.results.push(match &role {
        Ok(role) => privileges::role_privileges_evaluate(role, origin, environment),
        Err(e) => CheckResult::skipped("role privileges", e),
    });

    report
        .results
        .push(privileges::cdc_objects_check(&client, &settings.cdc).await);
    report
        .results
        .push(connection::replication_connect_check(&settings.replication, environment).await);

    match relations::relations_load(&client, &settings.allowed_tables).await {
        Ok(relations) => {
            report.results.push(relations::table_ownership_evaluate(
                &relations, superuser, &role_name,
            ));
            report
                .results
                .push(relations::primary_keys_evaluate(&relations));
            report.results.push(relations::views_evaluate(&relations));
            report
                .results
                .push(relations::row_level_security_evaluate(&relations));
        }
        Err(e) => report.results.push(CheckResult::skipped("tables", e)),
    }

    report
        .results
        .push(server::pg_stat_statements_check(&client).await);
    report
}
