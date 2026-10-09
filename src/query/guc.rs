//! The session settings (GUCs) pgcache tracks per client connection (PGC-287).
//!
//! A tracked setting is one whose value can change which rows a query selects
//! (name resolution, literal parsing, expression semantics), how a result is
//! rendered as text, or which rows a role may see. Planner, timeout and
//! operational settings are deliberately absent.

use std::fmt;

/// A tracked session setting. The discriminant indexes [`GucSet`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Guc {
    // Class 1: name resolution.
    SearchPath,
    // Class 2 (some also class 3): literal parsing and expression semantics.
    TimeZone,
    DateStyle,
    IntervalStyle,
    StandardConformingStrings,
    BackslashQuote,
    TransformNullEquals,
    ArrayNulls,
    DefaultTextSearchConfig,
    GinFuzzySearchLimit,
    TimezoneAbbreviations,
    // Class 3: text rendering of results.
    ExtraFloatDigits,
    ByteaOutput,
    ClientEncoding,
    LcMonetary,
    LcNumeric,
    LcTime,
    XmlBinary,
    XmlOption,
    // Class 4: identity and row visibility.
    Role,
    SessionAuthorization,
    RowSecurity,
    // Transaction default read at BEGIN (ADR-054).
    DefaultTransactionIsolation,
}

impl Guc {
    /// Every tracked setting, in discriminant order.
    pub const ALL: [Guc; 23] = [
        Guc::SearchPath,
        Guc::TimeZone,
        Guc::DateStyle,
        Guc::IntervalStyle,
        Guc::StandardConformingStrings,
        Guc::BackslashQuote,
        Guc::TransformNullEquals,
        Guc::ArrayNulls,
        Guc::DefaultTextSearchConfig,
        Guc::GinFuzzySearchLimit,
        Guc::TimezoneAbbreviations,
        Guc::ExtraFloatDigits,
        Guc::ByteaOutput,
        Guc::ClientEncoding,
        Guc::LcMonetary,
        Guc::LcNumeric,
        Guc::LcTime,
        Guc::XmlBinary,
        Guc::XmlOption,
        Guc::Role,
        Guc::SessionAuthorization,
        Guc::RowSecurity,
        Guc::DefaultTransactionIsolation,
    ];

    /// The setting's name as `pg_settings` spells it (lowercase).
    pub const fn name(self) -> &'static str {
        match self {
            Guc::SearchPath => "search_path",
            Guc::TimeZone => "timezone",
            Guc::DateStyle => "datestyle",
            Guc::IntervalStyle => "intervalstyle",
            Guc::StandardConformingStrings => "standard_conforming_strings",
            Guc::BackslashQuote => "backslash_quote",
            Guc::TransformNullEquals => "transform_null_equals",
            Guc::ArrayNulls => "array_nulls",
            Guc::DefaultTextSearchConfig => "default_text_search_config",
            Guc::GinFuzzySearchLimit => "gin_fuzzy_search_limit",
            Guc::TimezoneAbbreviations => "timezone_abbreviations",
            Guc::ExtraFloatDigits => "extra_float_digits",
            Guc::ByteaOutput => "bytea_output",
            Guc::ClientEncoding => "client_encoding",
            Guc::LcMonetary => "lc_monetary",
            Guc::LcNumeric => "lc_numeric",
            Guc::LcTime => "lc_time",
            Guc::XmlBinary => "xmlbinary",
            Guc::XmlOption => "xmloption",
            Guc::Role => "role",
            Guc::SessionAuthorization => "session_authorization",
            Guc::RowSecurity => "row_security",
            Guc::DefaultTransactionIsolation => "default_transaction_isolation",
        }
    }

    /// The tracked setting a name refers to, if any. Case-insensitive, as
    /// PostgreSQL treats setting names (`ParameterStatus` spells some in
    /// mixed case: `DateStyle`, `TimeZone`).
    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|guc| guc.name().eq_ignore_ascii_case(name))
    }

    /// Whether a value sampled inside a transaction block is the value in
    /// force for reads in that block. False only for the isolation default,
    /// which PostgreSQL reads at `BEGIN`.
    pub const fn block_sample_valid(self) -> bool {
        !matches!(self, Guc::DefaultTransactionIsolation)
    }

    /// Whether a cacheable read depends on the setting's value. The isolation
    /// default does not: a block's level is fixed when it starts (ADR-054)
    /// and gated separately, and outside a block the default is irrelevant.
    pub const fn gates_reads(self) -> bool {
        !matches!(self, Guc::DefaultTransactionIsolation)
    }
}

impl fmt::Display for Guc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A set of tracked settings, one bit per [`Guc`].
#[derive(Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct GucSet(u32);

impl GucSet {
    pub const EMPTY: GucSet = GucSet(0);

    pub const fn single(guc: Guc) -> Self {
        GucSet(1 << guc as u8)
    }

    pub const fn all() -> Self {
        GucSet((1u32 << Guc::ALL.len()) - 1)
    }

    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub const fn contains(self, guc: Guc) -> bool {
        self.0 & (1 << guc as u8) != 0
    }

    pub const fn insert(&mut self, guc: Guc) {
        self.0 |= 1 << guc as u8;
    }

    pub const fn remove(&mut self, guc: Guc) {
        self.0 &= !(1 << guc as u8);
    }

    pub const fn union(self, other: Self) -> Self {
        GucSet(self.0 | other.0)
    }

    pub fn iter(self) -> impl Iterator<Item = Guc> {
        Guc::ALL.into_iter().filter(move |guc| self.contains(*guc))
    }
}

impl fmt::Debug for GucSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(self.iter()).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_guc_names_round_trip_case_insensitively() {
        for guc in Guc::ALL {
            assert_eq!(Guc::parse(guc.name()), Some(guc));
            assert_eq!(Guc::parse(&guc.name().to_uppercase()), Some(guc));
        }
        assert_eq!(Guc::parse("TimeZone"), Some(Guc::TimeZone));
        assert_eq!(Guc::parse("DateStyle"), Some(Guc::DateStyle));
        assert_eq!(Guc::parse("work_mem"), None);
        assert_eq!(Guc::parse("app.tenant"), None);
    }

    #[test]
    fn test_guc_discriminants_index_the_set() {
        for (index, guc) in Guc::ALL.into_iter().enumerate() {
            assert_eq!(guc as usize, index);
        }
        let all = GucSet::all();
        for guc in Guc::ALL {
            assert!(all.contains(guc));
        }
        assert_eq!(all.iter().count(), Guc::ALL.len());
    }

    #[test]
    fn test_guc_set_operations() {
        let mut set = GucSet::EMPTY;
        assert!(set.is_empty());
        set.insert(Guc::TimeZone);
        assert!(set.contains(Guc::TimeZone));
        assert!(!set.contains(Guc::Role));
        let joined = set.union(GucSet::single(Guc::Role));
        assert_eq!(
            joined.iter().collect::<Vec<_>>(),
            vec![Guc::TimeZone, Guc::Role]
        );
        set.remove(Guc::TimeZone);
        assert!(set.is_empty());
        assert_eq!(
            format!("{:?}", GucSet::single(Guc::SearchPath)),
            "{SearchPath}"
        );
    }
}
