//! The session settings a text-format result is rendered under (PGC-287,
//! class 3): the values a serve applies on its cache-DB connection before the
//! SELECT, and the profile hash that keys memo entries and coalesce groups so
//! one client's rendering is never handed to another.

use std::hash::{Hash, Hasher};

use ecow::EcoString;

use crate::query::guc::Guc;

/// The rendering settings, in the order the serve statement binds them.
pub(crate) const OUTPUT_GUCS: [Guc; 11] = [
    Guc::TimeZone,
    Guc::DateStyle,
    Guc::IntervalStyle,
    Guc::ExtraFloatDigits,
    Guc::ByteaOutput,
    Guc::ClientEncoding,
    Guc::LcMonetary,
    Guc::LcNumeric,
    Guc::LcTime,
    Guc::XmlBinary,
    Guc::XmlOption,
];

/// The prepared statement that applies the profile on a serve connection:
/// one `set_config` per [`OUTPUT_GUCS`] entry, `$1..$11` in that order. A
/// literal so it is `&'static str`; the unit test pins it to the list.
pub(crate) const SETOUT_SQL: &str = "SELECT set_config('timezone',$1,false),set_config('datestyle',$2,false),set_config('intervalstyle',$3,false),set_config('extra_float_digits',$4,false),set_config('bytea_output',$5,false),set_config('client_encoding',$6,false),set_config('lc_monetary',$7,false),set_config('lc_numeric',$8,false),set_config('lc_time',$9,false),set_config('xmlbinary',$10,false),set_config('xmloption',$11,false)";

/// A client's rendering settings as the proxy knows them at dispatch.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OutputSettings {
    values: [EcoString; OUTPUT_GUCS.len()],
}

/// The hash of an [`OutputSettings`]. Every value counts whatever the result
/// format: binary results carry no protocol-level rendering, but a query can
/// still compute zone- or locale-dependent text (`at::text`, `to_char(…)`),
/// so sharing binary results across settings waits for the class 2 slice,
/// which classifies such expressions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct OutputProfile(u64);

impl OutputSettings {
    /// Build from a lookup of each rendering setting's value.
    pub fn from_values(mut value_of: impl FnMut(Guc) -> EcoString) -> Self {
        Self {
            values: OUTPUT_GUCS.map(&mut value_of),
        }
    }

    /// The values in [`OUTPUT_GUCS`] order, for binding `$1..$11`.
    pub fn values(&self) -> &[EcoString] {
        &self.values
    }

    /// The profile a result rendered under these settings carries, and what
    /// a serve connection remembers having applied.
    pub fn profile(&self) -> OutputProfile {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.values.hash(&mut hasher);
        OutputProfile(hasher.finish())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::wildcard_enum_match_arm)]

    use super::*;

    fn settings(timezone: &str, encoding: &str) -> OutputSettings {
        OutputSettings::from_values(|guc| match guc {
            Guc::TimeZone => timezone.into(),
            Guc::ClientEncoding => encoding.into(),
            _ => "x".into(),
        })
    }

    #[test]
    fn test_setout_sql_matches_guc_order() {
        let columns: Vec<String> = OUTPUT_GUCS
            .iter()
            .enumerate()
            .map(|(i, guc)| format!("set_config('{}',${},false)", guc.name(), i + 1))
            .collect();
        assert_eq!(SETOUT_SQL, format!("SELECT {}", columns.join(",")));
    }

    #[test]
    fn test_profile_covers_every_value() {
        let utc = settings("UTC", "UTF8");
        assert_eq!(utc.profile(), settings("UTC", "UTF8").profile());
        assert_ne!(utc.profile(), settings("Asia/Tokyo", "UTF8").profile());
        assert_ne!(utc.profile(), settings("UTC", "LATIN1").profile());
    }
}
