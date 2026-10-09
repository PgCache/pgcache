//! The session settings that fix what a date-time literal means (PGC-287
//! class 2): the zone a zone-less `timestamptz` literal is in, the field
//! order of an ambiguous date, the interval input style and the abbreviation
//! set. Carried with a query from the proxy to the writer, where resolution
//! canonicalises literals under them; hashed into the session key of a query
//! with a string literal so two sessions whose literals could mean different
//! instants never share an alias.

use ecow::EcoString;
use jiff::tz::TimeZone;

/// The field order `DateStyle` gives an ambiguous numeric date.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum DateOrder {
    #[default]
    MonthDayYear,
    DayMonthYear,
    YearMonthDay,
}

impl DateOrder {
    /// The spelling `SET DateStyle` takes.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MonthDayYear => "MDY",
            Self::DayMonthYear => "DMY",
            Self::YearMonthDay => "YMD",
        }
    }

    /// The order half of a `DateStyle` value (`ISO, DMY` → day first); MDY
    /// when the value names none, as PostgreSQL defaults it.
    pub fn from_datestyle(value: &str) -> Self {
        value
            .split(',')
            .map(str::trim)
            .find_map(|part| match part.to_ascii_uppercase().as_str() {
                "MDY" | "US" | "NONEURO" | "NONEUROPEAN" => Some(Self::MonthDayYear),
                "DMY" | "EURO" | "EUROPEAN" => Some(Self::DayMonthYear),
                "YMD" => Some(Self::YearMonthDay),
                _ => None,
            })
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LiteralSettings {
    /// `TimeZone` as the session reports it (an IANA name, a POSIX string or
    /// an abbreviation).
    pub timezone: EcoString,
    pub date_order: DateOrder,
    pub intervalstyle: EcoString,
    pub timezone_abbreviations: EcoString,
}

impl Default for LiteralSettings {
    fn default() -> Self {
        Self {
            timezone: "UTC".into(),
            date_order: DateOrder::MonthDayYear,
            intervalstyle: "postgres".into(),
            timezone_abbreviations: "Default".into(),
        }
    }
}

impl LiteralSettings {
    /// From the settings' text values as a session reports them.
    pub fn from_texts(
        timezone: &str,
        datestyle: &str,
        intervalstyle: &str,
        timezone_abbreviations: &str,
    ) -> Self {
        Self {
            timezone: timezone.into(),
            date_order: DateOrder::from_datestyle(datestyle),
            intervalstyle: intervalstyle.into(),
            timezone_abbreviations: timezone_abbreviations.into(),
        }
    }

    /// The session zone, when the zone database knows it. `None` leaves a
    /// zone-less `timestamptz` literal uncanonicalised.
    pub fn zone(&self) -> Option<TimeZone> {
        let name = self.timezone.as_str();
        if name.eq_ignore_ascii_case("UTC") || name.eq_ignore_ascii_case("GMT") {
            return Some(TimeZone::UTC);
        }
        TimeZone::get(name)
            .ok()
            .or_else(|| TimeZone::posix(name).ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_date_order_from_datestyle() {
        assert_eq!(
            DateOrder::from_datestyle("ISO, MDY"),
            DateOrder::MonthDayYear
        );
        assert_eq!(
            DateOrder::from_datestyle("ISO, DMY"),
            DateOrder::DayMonthYear
        );
        assert_eq!(
            DateOrder::from_datestyle("Postgres, ymd"),
            DateOrder::YearMonthDay
        );
        assert_eq!(DateOrder::from_datestyle("ISO"), DateOrder::MonthDayYear);
        assert_eq!(DateOrder::from_datestyle("German"), DateOrder::MonthDayYear);
    }

    #[test]
    fn test_zone_lookup() {
        let tokyo = LiteralSettings::from_texts("Asia/Tokyo", "ISO, MDY", "postgres", "Default");
        let utc = LiteralSettings::from_texts("UTC", "ISO, MDY", "postgres", "Default");
        assert_ne!(tokyo, utc);
        assert!(tokyo.zone().is_some());
        assert_eq!(utc.zone(), Some(TimeZone::UTC));
        assert!(
            LiteralSettings::from_texts(
                "EST5EDT,M3.2.0,M11.1.0",
                "ISO, MDY",
                "postgres",
                "Default"
            )
            .zone()
            .is_some()
        );
        assert!(
            LiteralSettings::from_texts("Nowhere/Land", "ISO, MDY", "postgres", "Default")
                .zone()
                .is_none()
        );
    }
}
