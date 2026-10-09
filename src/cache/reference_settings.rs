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
use crate::query::literal_settings::LiteralSettings;

/// Origin's defaults for the literal-semantics settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReferenceSettings {
    pub(crate) timezone: EcoString,
    pub(crate) intervalstyle: EcoString,
    /// The same values as a literal-settings profile (with `DateStyle`'s
    /// order half), for comparison with a session's when an entry can only
    /// be served under the reference.
    pub(crate) literals: LiteralSettings,
    /// Origin's default `lc_monetary`, `lc_numeric` and `lc_time`: every
    /// client inherits them, so every serve applies them on the cache DB.
    pub(crate) locales: [EcoString; LOCALE_GUCS.len()],
}

/// The locale settings a serve applies, in the order `locales` holds them.
const LOCALE_GUCS: [&str; 3] = ["lc_monetary", "lc_numeric", "lc_time"];

impl Default for ReferenceSettings {
    fn default() -> Self {
        Self::new("UTC", "ISO, MDY", "postgres", "Default", ["C", "C", "C"])
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
        let mut locales = [String::new(), String::new(), String::new()];
        for (locale, name) in locales.iter_mut().zip(LOCALE_GUCS) {
            *locale = show(name).await?;
        }
        Ok(Self::new(
            &show("TimeZone").await?,
            &show("DateStyle").await?,
            &show("IntervalStyle").await?,
            &show("timezone_abbreviations").await?,
            locales.each_ref().map(String::as_str),
        ))
    }

    fn new(
        timezone: &str,
        datestyle: &str,
        intervalstyle: &str,
        abbreviations: &str,
        locales: [&str; LOCALE_GUCS.len()],
    ) -> Self {
        Self {
            timezone: timezone.into(),
            intervalstyle: intervalstyle.into(),
            literals: LiteralSettings::from_texts(
                timezone,
                datestyle,
                intervalstyle,
                abbreviations,
            ),
            locales: locales.map(EcoString::from),
        }
    }

    /// The statement that applies the reference locales, as a serve does.
    pub(crate) fn locales_check_sql(&self) -> String {
        let calls: Vec<String> = LOCALE_GUCS
            .iter()
            .zip(&self.locales)
            .map(|(name, value)| format!("set_config('{name}','{}',false)", quote(value)))
            .collect();
        format!("SELECT {}", calls.join(","))
    }

    /// Every serve applies the reference locales on the cache DB; a cache
    /// host without them could never serve a hit, so refuse to start instead.
    pub(crate) async fn locales_check(&self, cache: &Client) -> CacheResult<()> {
        cache
            .batch_execute(&self.locales_check_sql())
            .await
            .map_into_report::<CacheError>()
            .attach_loc(format!(
                "applying origin's default locales on the cache database \
                 (lc_monetary={}, lc_numeric={}, lc_time={}): install them on the cache host",
                self.locales[0], self.locales[1], self.locales[2]
            ))
    }

    /// The statements that pin a session to the reference: the date-time
    /// settings, and the locales a `money` or `to_char` text is rendered and
    /// parsed under on either side of population and pgoutput.
    pub(crate) fn pin_sql(&self) -> String {
        let mut sql = format!(
            "SET TimeZone = '{}'; SET DateStyle = 'ISO, {}'; SET IntervalStyle = '{}'",
            quote(&self.timezone),
            self.literals.date_order.as_str(),
            quote(&self.intervalstyle),
        );
        for (name, value) in LOCALE_GUCS.iter().zip(&self.locales) {
            sql.push_str(&format!("; SET {name} = '{}'", quote(value)));
        }
        sql
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
    use crate::query::literal_settings::DateOrder;

    /// The pin and the literal profile read the order half of `DateStyle`
    /// the same way, every spelling included.
    #[test]
    fn test_pin_order_matches_the_literal_profile() {
        let reference = ReferenceSettings::new(
            "UTC",
            "ISO, European",
            "postgres",
            "Default",
            ["C", "C", "C"],
        );
        assert_eq!(reference.literals.date_order, DateOrder::DayMonthYear);
        assert!(
            reference.pin_sql().contains("SET DateStyle = 'ISO, DMY'"),
            "{}",
            reference.pin_sql()
        );
    }

    #[test]
    fn test_pin_sql_forces_iso_output_and_quotes_values() {
        let reference = ReferenceSettings::new(
            "America/New_York",
            "ISO, DMY",
            "sql_standard",
            "Default",
            ["C", "C", "C"],
        );
        assert_eq!(
            reference.pin_sql(),
            "SET TimeZone = 'America/New_York'; SET DateStyle = 'ISO, DMY'; \
             SET IntervalStyle = 'sql_standard'; SET lc_monetary = 'C'; \
             SET lc_numeric = 'C'; SET lc_time = 'C'"
        );
        let odd = ReferenceSettings {
            timezone: "it's".into(),
            ..ReferenceSettings::default()
        };
        assert!(odd.pin_sql().starts_with("SET TimeZone = 'it''s'"));
    }

    #[test]
    fn test_locales_check_sql_applies_each_locale_quoted() {
        let reference = ReferenceSettings::new(
            "UTC",
            "ISO, MDY",
            "postgres",
            "Default",
            ["en_US.UTF-8", "C", "it's"],
        );
        assert_eq!(
            reference.locales_check_sql(),
            "SELECT set_config('lc_monetary','en_US.UTF-8',false),\
             set_config('lc_numeric','C',false),set_config('lc_time','it''s',false)"
        );
    }
}
