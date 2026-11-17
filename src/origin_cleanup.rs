//! `--cleanup`: remove from origin what a pgcache instance left there. For an
//! instance that was killed before it could clean up after itself, or one
//! being decommissioned; a pgcache that shuts down on a signal does this for
//! its slot and publication on its own.

use tokio_postgres::{Client, Error};

use crate::pg::event_trigger::{UNINSTALL_SQL, event_triggers_state_load};
use crate::pg::{connect, identifier_quote_into};
use crate::result::error_chain_format;
use crate::settings::PreflightSettings;

/// What cleanup did about one object on origin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupOutcome {
    Dropped,
    Absent,
    /// A backend is streaming from the slot: a pgcache is still running.
    InUse {
        backend: i32,
    },
    /// Present and left in place, as the shared objects are without `--purge`.
    Kept,
}

#[derive(Debug)]
pub struct CleanupStep {
    pub object: String,
    pub outcome: CleanupOutcome,
}

#[derive(Debug)]
pub struct CleanupReport {
    /// `user@host:port/database` of the origin that was cleaned.
    pub target: String,
    pub steps: Vec<CleanupStep>,
    /// The failure that ended the run early, if one did.
    pub error: Option<String>,
}

impl CleanupReport {
    pub fn failed(&self) -> bool {
        self.error.is_some()
            || self
                .steps
                .iter()
                .any(|step| matches!(step.outcome, CleanupOutcome::InUse { .. }))
    }
}

/// Drop the instance's replication slot and publication, and with `purge`
/// the schema-change event triggers and `pgcache` schema every instance on
/// the database shares. A slot in use ends the run with nothing removed.
pub async fn origin_cleanup_run(settings: &PreflightSettings, purge: bool) -> CleanupReport {
    let origin = &settings.origin;
    let mut report = CleanupReport {
        target: format!(
            "{}@{}:{}/{}",
            origin.user, origin.host, origin.port, origin.database
        ),
        steps: Vec::new(),
        error: None,
    };
    let outcome = async {
        let client = connect(origin, "origin cleanup").await?;
        steps_run(&client, settings, purge, &mut report.steps).await
    }
    .await;
    if let Err(e) = outcome {
        report.error = Some(error_chain_format(&e));
    }
    report
}

async fn steps_run(
    client: &Client,
    settings: &PreflightSettings,
    purge: bool,
    steps: &mut Vec<CleanupStep>,
) -> Result<(), Error> {
    let slot_name = &settings.cdc.slot_name;
    let slot = slot_drop(client, slot_name).await?;
    steps.push(CleanupStep {
        object: format!("replication slot {slot_name}"),
        outcome: slot,
    });
    if matches!(slot, CleanupOutcome::InUse { .. }) {
        return Ok(());
    }

    let publication_name = &settings.cdc.publication_name;
    steps.push(CleanupStep {
        object: format!("publication {publication_name}"),
        outcome: publication_drop(client, publication_name).await?,
    });

    steps.push(CleanupStep {
        object: "schema-change event triggers and schema pgcache".to_owned(),
        outcome: signal_drop(client, purge).await?,
    });
    Ok(())
}

async fn slot_drop(client: &Client, slot_name: &str) -> Result<CleanupOutcome, Error> {
    let slot = client
        .query_opt(
            "SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await?;
    let Some(row) = slot else {
        return Ok(CleanupOutcome::Absent);
    };
    if let Some(backend) = row.try_get::<_, Option<i32>>(0)? {
        return Ok(CleanupOutcome::InUse { backend });
    }
    client
        .execute("SELECT pg_drop_replication_slot($1)", &[&slot_name])
        .await?;
    Ok(CleanupOutcome::Dropped)
}

async fn publication_drop(
    client: &Client,
    publication_name: &str,
) -> Result<CleanupOutcome, Error> {
    let exists = client
        .query_opt(
            "SELECT 1 FROM pg_publication WHERE pubname = $1",
            &[&publication_name],
        )
        .await?
        .is_some();
    if !exists {
        return Ok(CleanupOutcome::Absent);
    }
    let mut statement = String::from("DROP PUBLICATION ");
    identifier_quote_into(publication_name, &mut statement);
    client.execute(&statement, &[]).await?;
    Ok(CleanupOutcome::Dropped)
}

async fn signal_drop(client: &Client, purge: bool) -> Result<CleanupOutcome, Error> {
    let triggers = event_triggers_state_load(client).await?.any_present();
    let schema = client
        .query_opt("SELECT 1 FROM pg_namespace WHERE nspname = 'pgcache'", &[])
        .await?
        .is_some();
    if !triggers && !schema {
        return Ok(CleanupOutcome::Absent);
    }
    if !purge {
        return Ok(CleanupOutcome::Kept);
    }
    client.batch_execute(UNINSTALL_SQL).await?;
    Ok(CleanupOutcome::Dropped)
}

/// Plain-text rendering of a [`CleanupReport`]: the format the Docker
/// entrypoint and the try script print unchanged.
pub fn cleanup_report_render(report: &CleanupReport) -> String {
    let mut lines = vec![
        format!("pgcache cleanup: origin {}", report.target),
        String::new(),
    ];
    for step in &report.steps {
        lines.push(match step.outcome {
            CleanupOutcome::Dropped => format!("  dropped  {}", step.object),
            CleanupOutcome::Absent => format!("  absent   {}", step.object),
            CleanupOutcome::InUse { backend } => format!(
                "  in use   {} (backend {backend}): a pgcache is streaming from it; stop it first",
                step.object
            ),
            CleanupOutcome::Kept => format!(
                "  kept     {} (shared by every pgcache on this database; --purge removes them)",
                step.object
            ),
        });
    }
    if let Some(error) = &report.error {
        lines.push(format!("  error    {error}"));
    }
    lines.push(String::new());
    lines.push(if report.failed() {
        "Cleanup did not finish; see above.".to_owned()
    } else {
        "Cleanup finished.".to_owned()
    });
    lines.push(String::new());
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(steps: Vec<CleanupStep>, error: Option<&str>) -> CleanupReport {
        CleanupReport {
            target: "u@h:5432/d".to_owned(),
            steps,
            error: error.map(str::to_owned),
        }
    }

    fn step(object: &str, outcome: CleanupOutcome) -> CleanupStep {
        CleanupStep {
            object: object.to_owned(),
            outcome,
        }
    }

    #[test]
    fn test_cleanup_report_render_lists_each_outcome() {
        let report = report(
            vec![
                step("replication slot s", CleanupOutcome::Dropped),
                step("publication p", CleanupOutcome::Absent),
                step("triggers", CleanupOutcome::Kept),
            ],
            None,
        );
        let text = cleanup_report_render(&report);
        assert!(text.starts_with("pgcache cleanup: origin u@h:5432/d\n"));
        assert!(text.contains("  dropped  replication slot s\n"));
        assert!(text.contains("  absent   publication p\n"));
        assert!(text.contains("  kept     triggers (shared"));
        assert!(text.contains("Cleanup finished."));
        assert!(!report.failed());
    }

    #[test]
    fn test_cleanup_report_fails_on_a_slot_in_use_or_an_error() {
        let in_use = report(
            vec![step(
                "replication slot s",
                CleanupOutcome::InUse { backend: 7 },
            )],
            None,
        );
        assert!(in_use.failed());
        assert!(cleanup_report_render(&in_use).contains("in use   replication slot s (backend 7)"));

        let errored = report(Vec::new(), Some("connection refused"));
        assert!(errored.failed());
        assert!(cleanup_report_render(&errored).contains("  error    connection refused"));
    }
}
