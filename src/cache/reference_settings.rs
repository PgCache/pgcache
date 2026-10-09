//! The reference session settings pgcache's own sessions run under (PGC-287
//! class 2): origin's defaults for the settings that fix what a literal
//! means, read once per cache generation from an origin connection. Every
//! session pgcache opens — the writer's, the population workers', the MV
//! builders', the CDC writer's and the replication session that renders
//! pgoutput text — is pinned to them, so population, PgEval and CDC text all
//! agree with each other. `DateStyle` output is forced to ISO so the typed
//! comparator's wire-text parsers hold; its input order is origin's.

use std::sync::{Arc, LazyLock};

use arc_swap::ArcSwap;
use ecow::EcoString;
use tokio_postgres::Client;

use super::{CacheError, CacheResult, MapIntoReport, ReportExt};

/// Origin's defaults for the literal-semantics settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReferenceSettings {
    pub(crate) timezone: EcoString,
    /// The input order half of `DateStyle` (`MDY`, `DMY`, `YMD`).
    pub(crate) datestyle_order: EcoString,
    pub(crate) intervalstyle: EcoString,
}

impl Default for ReferenceSettings {
    fn default() -> Self {
        Self {
            timezone: "UTC".into(),
            datestyle_order: "MDY".into(),
            intervalstyle: "postgres".into(),
        }
    }
}

impl ReferenceSettings {
    /// Read the reference from an origin session's current values.
    pub(crate) async fn probe(client: &Client) -> CacheResult<Self> {
        let show = async |name: &str| -> CacheResult<String> {
            let row = client
                .query_one(&format!("SHOW {name}"), &[])
                .await
                .map_into_report::<CacheError>()
                .attach_loc("reading a reference session setting")?;
            Ok(row.get::<_, String>(0))
        };
        let datestyle = show("DateStyle").await?;
        Ok(Self {
            timezone: show("TimeZone").await?.into(),
            datestyle_order: datestyle_order(&datestyle).into(),
            intervalstyle: show("IntervalStyle").await?.into(),
        })
    }

    /// The statements that pin a session to the reference.
    pub(crate) fn pin_sql(&self) -> String {
        format!(
            "SET TimeZone = '{}'; SET DateStyle = 'ISO, {}'; SET IntervalStyle = '{}'",
            quote(&self.timezone),
            quote(&self.datestyle_order),
            quote(&self.intervalstyle),
        )
    }

    /// Pin `client`'s session to the reference.
    pub(crate) async fn pin(&self, client: &Client, context: &str) -> CacheResult<()> {
        client
            .batch_execute(&self.pin_sql())
            .await
            .map_into_report::<CacheError>()
            .attach_loc(format!(
                "pinning {context} session to the reference settings"
            ))
    }
}

/// The order half of a `DateStyle` value (`ISO, MDY` → `MDY`); `MDY` when
/// the value names none, as PostgreSQL defaults it.
fn datestyle_order(value: &str) -> &str {
    value
        .split(',')
        .map(str::trim)
        .find(|part| matches!(part.to_ascii_uppercase().as_str(), "MDY" | "DMY" | "YMD"))
        .unwrap_or("MDY")
}

fn quote(value: &str) -> String {
    value.replace('\'', "''")
}

static CURRENT: LazyLock<ArcSwap<ReferenceSettings>> =
    LazyLock::new(|| ArcSwap::from_pointee(ReferenceSettings::default()));

/// Install the generation's reference, read at cache setup before any
/// pgcache-owned session opens.
pub(crate) fn install(reference: ReferenceSettings) {
    CURRENT.store(Arc::new(reference));
}

/// The current reference.
pub(crate) fn current() -> Arc<ReferenceSettings> {
    CURRENT.load_full()
}

/// Pin `client`'s session to the current reference.
pub(crate) async fn pin(client: &Client, context: &str) -> CacheResult<()> {
    current().pin(client, context).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datestyle_order_extracts_the_order_half() {
        assert_eq!(datestyle_order("ISO, MDY"), "MDY");
        assert_eq!(datestyle_order("Postgres, DMY"), "DMY");
        assert_eq!(datestyle_order("ISO"), "MDY");
        assert_eq!(datestyle_order("German, ymd"), "ymd");
    }

    #[test]
    fn test_pin_sql_forces_iso_output_and_quotes_values() {
        let reference = ReferenceSettings {
            timezone: "America/New_York".into(),
            datestyle_order: "DMY".into(),
            intervalstyle: "sql_standard".into(),
        };
        assert_eq!(
            reference.pin_sql(),
            "SET TimeZone = 'America/New_York'; SET DateStyle = 'ISO, DMY'; \
             SET IntervalStyle = 'sql_standard'"
        );
        let odd = ReferenceSettings {
            timezone: "it's".into(),
            ..ReferenceSettings::default()
        };
        assert!(odd.pin_sql().starts_with("SET TimeZone = 'it''s'"));
    }
}
