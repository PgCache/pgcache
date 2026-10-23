//! Per-connection tracking of the session settings that can change what a
//! cached query means (PGC-287): their values as the proxy knows them, in
//! session and transaction-block scope, and the views dispatch reads from
//! them (the resolved search_path, the isolation default).
//!
//! Values arrive three ways. Origin reports some settings by `ParameterStatus`
//! on every change; once a setting has been reported this way, statements that
//! touch it are not tracked (origin will say). The rest are sampled by one
//! probe (`SELECT current_setting(…)` over every tracked setting, one column
//! each in [`Guc::ALL`] order) whenever something is unknown; the probe's own
//! `ReadyForQuery` status says whether the sample is the session's value or
//! the open block's. In between, the analyzer's [`SettingsEffect`] of each
//! forwarded statement marks what it may have changed, in which scope.

use std::sync::Arc;

use ecow::EcoString;
use tracing::debug;

use crate::cache::output_settings::OutputSettings;
use crate::pg::protocol::backend::TransactionStatus;
use crate::proxy::search_path::SearchPath;
use crate::query::guc::Guc;
use crate::query::keying_settings::KeyingSettings;
use crate::query::literal_settings::LiteralSettings;
use crate::query::write::{IsolationLevel, SettingsEffect};

/// A setting's value as the proxy knows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SettingValue {
    Unknown,
    Known(EcoString),
}

impl SettingValue {
    fn known(&self) -> Option<&str> {
        match self {
            Self::Known(value) => Some(value),
            Self::Unknown => None,
        }
    }
}

#[derive(Debug, Clone)]
struct SettingState {
    /// The session value: what reads see outside a block, and inside one
    /// unless `block` overrides it.
    session: SettingValue,
    /// The mutation sequence at which `session` last became unknown. A probe
    /// injected before that point sampled origin before the mutation ran, so
    /// its value must not fill this.
    session_since: u64,
    /// `Some` while the open block may hold a different value (`SET LOCAL`,
    /// any `SET` inside the block). Dropped when the block ends.
    block: Option<SettingValue>,
    /// As `session_since`, for `block`.
    block_since: u64,
    /// A statement touched this setting inside the open block, so a
    /// `ROLLBACK TO SAVEPOINT` may have reverted the block value.
    touched_in_block: bool,
    /// Origin reports this setting by `ParameterStatus` on every change.
    reported: bool,
    /// Whether the setting has a block value to track: an in-block sample is
    /// meaningful and a block-scoped change is visible to reads
    /// ([`Guc::block_sample_valid`]).
    block_sample_valid: bool,
}

impl SettingState {
    fn for_guc(guc: Guc) -> Self {
        Self {
            session: SettingValue::Unknown,
            session_since: 0,
            block: None,
            block_since: 0,
            touched_in_block: false,
            reported: false,
            block_sample_valid: guc.block_sample_valid(),
        }
    }

    fn session_unknown_mark(&mut self, seq: u64) {
        self.session = SettingValue::Unknown;
        self.session_since = seq;
    }

    /// No block value to track means nothing to mark: a block-scoped change
    /// is invisible to reads, a session-scoped one is confirmed after the
    /// block.
    fn block_unknown_mark(&mut self, seq: u64) {
        if !self.block_sample_valid {
            return;
        }
        self.block = Some(SettingValue::Unknown);
        self.block_since = seq;
        self.touched_in_block = true;
    }

    /// How a forwarded statement's change to this setting lands: a reported
    /// setting is left to origin's `ParameterStatus`; otherwise the block
    /// mark (a block-scoped change, or any change inside a block), the
    /// session mark (a session-scoped change), or both.
    fn effect_mark(&mut self, local: bool, in_block: bool, seq: u64) {
        if self.reported {
            return;
        }
        if local || in_block {
            self.block_unknown_mark(seq);
        }
        if !local {
            self.session_unknown_mark(seq);
        }
    }

    /// Fill the session value from a sample taken at `seq`, if it was
    /// unknown since before then.
    fn session_fill(&mut self, value: &EcoString, seq: u64) {
        if self.session == SettingValue::Unknown && self.session_since <= seq {
            self.session = SettingValue::Known(value.clone());
        }
    }

    /// A sample taken with no block open. A block mark made after the probe
    /// was injected belongs to a block the sample predates, so it stays.
    fn idle_sample_apply(&mut self, value: &EcoString, seq: u64) {
        if self.block_since <= seq {
            self.block = None;
        }
        self.session_fill(value, seq);
    }

    /// A sample taken inside a block: it fills the block override when one
    /// is marked unknown, else the session value the block inherits. Nothing
    /// for a setting whose in-block sample is meaningless.
    fn block_sample_apply(&mut self, value: &EcoString, seq: u64) {
        if !self.block_sample_valid {
            return;
        }
        match &self.block {
            Some(SettingValue::Unknown) if self.block_since <= seq => {
                self.block = Some(SettingValue::Known(value.clone()));
            }
            Some(_) => {}
            None => self.session_fill(value, seq),
        }
    }

    /// The value in force for a read in the given scope.
    fn effective(&self, in_block: bool) -> &SettingValue {
        match (&self.block, in_block) {
            (Some(block), true) => block,
            _ => &self.session,
        }
    }

    /// Whether a probe would learn something at this scope: an unknown block
    /// override, or an unknown session value a block does not hide.
    fn unknown_at(&self, in_block: bool) -> bool {
        match (&self.block, in_block) {
            (Some(block), true) => *block == SettingValue::Unknown,
            (Some(_), false) | (None, _) => self.session == SettingValue::Unknown,
        }
    }
}

/// The resolved search_path dispatch keys on (ADR-006).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SearchPathState {
    /// No authoritative value yet.
    Unknown,
    /// Resolved, `$user` expanded, shared: dispatching a cache query clones
    /// the `Arc` instead of collecting a fresh `Vec` per query.
    Resolved(Arc<[EcoString]>),
    /// The path lists `pg_catalog` after another schema. Only then can a user
    /// function, type or operator with a builtin's exact signature shadow the
    /// builtin on origin, while the cache DB (which has no user objects) would
    /// run the builtin. Cacheable queries forward until the path changes.
    BuiltinsShadowed,
}

/// `pg_catalog` is searched first unless listed explicitly, in which case it
/// is searched at its listed position.
fn builtins_shadowed(resolved: &[EcoString]) -> bool {
    resolved
        .iter()
        .position(|schema| schema == "pg_catalog")
        .is_some_and(|index| index > 0)
}

impl SearchPathState {
    /// Build the resolved state from a raw search_path value, expanding
    /// `$user` to the effective user name.
    pub(crate) fn resolved(value: &str, user: Option<&str>) -> Self {
        let search_path = SearchPath::parse(value);
        let resolved: Arc<[EcoString]> = search_path.resolve(user).map(EcoString::from).collect();
        if builtins_shadowed(&resolved) {
            debug!("search_path lists pg_catalog after another schema; cacheable queries forward");
            return Self::BuiltinsShadowed;
        }
        Self::Resolved(resolved)
    }

    /// The resolved search_path, if available.
    pub(crate) fn resolve(&self) -> Option<Arc<[EcoString]>> {
        match self {
            Self::Unknown | Self::BuiltinsShadowed => None,
            Self::Resolved(search_path) => Some(Arc::clone(search_path)),
        }
    }
}

/// The session's `default_transaction_isolation` as the proxy knows it
/// (ADR-054).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IsolationState {
    Unknown,
    Known(IsolationLevel),
}

/// Why a cacheable read cannot be keyed right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SettingsForwardReason {
    /// Some tracked setting has no known value at the read's scope.
    SettingUnknown(Guc),
    /// The effective search_path lists `pg_catalog` after another schema.
    BuiltinsShadowed,
    /// A setting changes how origin parses the statement itself, so the
    /// proxy's parse (and the cache's) would not match origin's.
    ParsingIncompatible(Guc),
}

impl SettingsForwardReason {
    pub(crate) fn describe(self) -> &'static str {
        match self {
            Self::SettingUnknown(Guc::SearchPath) => "search_path unknown",
            Self::SettingUnknown(_) => "session setting unknown",
            Self::BuiltinsShadowed => "search_path lists pg_catalog after another schema",
            Self::ParsingIncompatible(_) => "session setting changes statement parsing",
        }
    }
}

/// Settings whose non-default value changes what a statement means in a way
/// the proxy's parser does not model: `standard_conforming_strings` off
/// (pg_query parses as if on), `transform_null_equals` on (`= NULL` becomes
/// `IS NULL`), `array_nulls` off (`'NULL'` array elements are strings), a
/// non-zero `gin_fuzzy_search_limit` (GIN results become a random sample) and
/// a `client_encoding` other than UTF-8 (the proxy decodes statements and
/// parameters only as UTF-8).
fn parsing_incompatible(guc: Guc, value: &str) -> bool {
    let is = |expected: &str| value.trim().eq_ignore_ascii_case(expected);
    match guc {
        Guc::StandardConformingStrings | Guc::ArrayNulls => !is("on"),
        Guc::TransformNullEquals => is("on"),
        Guc::GinFuzzySearchLimit => !is("0"),
        Guc::ClientEncoding => !is("UTF8"),
        Guc::SearchPath
        | Guc::TimeZone
        | Guc::DateStyle
        | Guc::IntervalStyle
        | Guc::BackslashQuote
        | Guc::DefaultTextSearchConfig
        | Guc::TimezoneAbbreviations
        | Guc::ExtraFloatDigits
        | Guc::ByteaOutput
        | Guc::LcMonetary
        | Guc::LcNumeric
        | Guc::LcTime
        | Guc::XmlBinary
        | Guc::XmlOption
        | Guc::Role
        | Guc::SessionAuthorization
        | Guc::RowSecurity
        | Guc::DefaultTransactionIsolation => false,
    }
}

/// Every tracked setting of one client session, plus the views derived from
/// them. Pure: the connection applies protocol events to it and reads back.
#[derive(Debug, Clone)]
pub(crate) struct SessionSettings {
    states: [SettingState; Guc::ALL.len()],
    /// The user from the startup message, the fallback for `$user` until
    /// `session_authorization` and `role` are known.
    startup_user: Option<EcoString>,
    /// Counts mutations that mark values unknown. A probe carries the value at
    /// its injection so its sample fills only values unknown since before it.
    mutation_seq: u64,
    search_path: SearchPathState,
    isolation: IsolationState,
    /// The read-scope views for a read outside a block and for one inside,
    /// rebuilt only when a value changes ([`Self::views_rederive`]) so a read
    /// costs two `Arc` clones, not a settings parse and eleven string clones.
    idle: ScopeViews,
    block: ScopeViews,
}

/// What a read at one scope dispatches with, derived from the values in
/// force there.
#[derive(Debug, Clone)]
struct ScopeViews {
    keying: Result<Arc<KeyingSettings>, SettingsForwardReason>,
    label: Arc<KeyingSettings>,
    output: Arc<OutputSettings>,
    /// The role in force: `role` unless `none`, else `session_authorization`,
    /// else the startup user; empty while unknown (`keying` then forwards).
    role: EcoString,
}

impl ScopeViews {
    fn unknown() -> Self {
        let label = Arc::new(KeyingSettings {
            search_path: Arc::from([]),
            literals: LiteralSettings::default(),
        });
        Self {
            keying: Err(SettingsForwardReason::SettingUnknown(Guc::SearchPath)),
            label,
            output: Arc::new(OutputSettings::default()),
            role: EcoString::new(),
        }
    }
}

impl SessionSettings {
    pub(crate) fn new() -> Self {
        let mut settings = Self {
            states: Guc::ALL.map(SettingState::for_guc),
            startup_user: None,
            mutation_seq: 0,
            search_path: SearchPathState::Unknown,
            isolation: IsolationState::Unknown,
            idle: ScopeViews::unknown(),
            block: ScopeViews::unknown(),
        };
        settings.views_rederive();
        settings
    }

    fn scope(&self, in_block: bool) -> &ScopeViews {
        if in_block { &self.block } else { &self.idle }
    }

    /// The sequence a probe injected now should carry.
    pub(crate) fn probe_seq(&self) -> u64 {
        self.mutation_seq
    }

    // A discriminant is always below `Guc::ALL.len()` (pinned by a test in
    // `query::guc`), so these cannot go out of bounds.
    #[expect(clippy::indexing_slicing)]
    fn state(&self, guc: Guc) -> &SettingState {
        &self.states[guc as usize]
    }

    #[expect(clippy::indexing_slicing)]
    fn state_mut(&mut self, guc: Guc) -> &mut SettingState {
        &mut self.states[guc as usize]
    }

    /// The effective search_path view (the open block's if it has one).
    pub(crate) fn search_path(&self) -> &SearchPathState {
        &self.search_path
    }

    /// The session's isolation default.
    pub(crate) fn isolation(&self) -> IsolationState {
        self.isolation
    }

    /// The value in force for a read at this scope.
    pub(crate) fn effective(&self, guc: Guc, in_block: bool) -> &SettingValue {
        self.state(guc).effective(in_block)
    }

    /// The settings a cacheable read is keyed under, or why it forwards:
    /// every tracked setting must be known at the read's scope, and the path
    /// must not shadow builtins.
    pub(crate) fn serve_keying(
        &self,
        in_block: bool,
    ) -> Result<Arc<KeyingSettings>, SettingsForwardReason> {
        self.scope(in_block).keying.clone()
    }

    /// Keying settings for a key that is only a label (telemetry, a batch
    /// context): an unknown search_path reads as empty rather than forwarding.
    pub(crate) fn keying_label(&self, in_block: bool) -> Arc<KeyingSettings> {
        Arc::clone(&self.scope(in_block).label)
    }

    /// The rendering settings in force for a read at this scope. The caller
    /// has checked [`Self::serve_keying`], so every value is known; an
    /// unknown one (explain's path, which does not check) renders as empty.
    pub(crate) fn output_settings(&self, in_block: bool) -> Arc<OutputSettings> {
        Arc::clone(&self.scope(in_block).output)
    }

    /// The role whose privileges a read at this scope runs under at origin
    /// (PGC-463). The caller has checked [`Self::serve_keying`], so it is
    /// known; empty otherwise.
    pub(crate) fn effective_role(&self, in_block: bool) -> EcoString {
        self.scope(in_block).role.clone()
    }

    fn scope_views_build(&self, in_block: bool) -> ScopeViews {
        let literals = self.literal_settings(in_block);
        ScopeViews {
            keying: self.serve_search_path(in_block).map(|search_path| {
                Arc::new(KeyingSettings {
                    search_path,
                    literals: literals.clone(),
                })
            }),
            label: Arc::new(KeyingSettings {
                search_path: self.search_path.resolve().unwrap_or_else(|| Arc::from([])),
                literals,
            }),
            output: Arc::new(self.output_settings_build(in_block)),
            role: self
                .role_in_force(in_block)
                .map(EcoString::from)
                .unwrap_or_default(),
        }
    }

    fn serve_search_path(&self, in_block: bool) -> Result<Arc<[EcoString]>, SettingsForwardReason> {
        if let Some(guc) = Guc::ALL
            .into_iter()
            .filter(|guc| guc.gates_reads())
            .find(|guc| self.effective(*guc, in_block) == &SettingValue::Unknown)
        {
            return Err(SettingsForwardReason::SettingUnknown(guc));
        }
        if let Some(guc) = [
            Guc::StandardConformingStrings,
            Guc::TransformNullEquals,
            Guc::ArrayNulls,
            Guc::GinFuzzySearchLimit,
            Guc::ClientEncoding,
        ]
        .into_iter()
        .find(|guc| {
            self.effective(*guc, in_block)
                .known()
                .is_some_and(|value| parsing_incompatible(*guc, value))
        }) {
            return Err(SettingsForwardReason::ParsingIncompatible(guc));
        }
        match &self.search_path {
            SearchPathState::Resolved(path) => Ok(Arc::clone(path)),
            SearchPathState::BuiltinsShadowed => Err(SettingsForwardReason::BuiltinsShadowed),
            SearchPathState::Unknown => Err(SettingsForwardReason::SettingUnknown(Guc::SearchPath)),
        }
    }

    fn output_settings_build(&self, in_block: bool) -> OutputSettings {
        OutputSettings::from_values(|guc| {
            self.effective(guc, in_block)
                .known()
                .map(EcoString::from)
                .unwrap_or_default()
        })
    }

    /// The literal-semantics settings in force for a read at this scope; the
    /// caller has checked [`Self::serve_keying`], so every value is known.
    fn literal_settings(&self, in_block: bool) -> LiteralSettings {
        let value = |guc: Guc| self.effective(guc, in_block).known().unwrap_or_default();
        LiteralSettings::from_texts(
            value(Guc::TimeZone),
            value(Guc::DateStyle),
            value(Guc::IntervalStyle),
            value(Guc::TimezoneAbbreviations),
        )
    }

    /// Whether a probe at this scope would learn a value. Inside a block,
    /// settings whose in-block sample is meaningless are not waited on.
    pub(crate) fn probe_needed(&self, in_block: bool) -> bool {
        Guc::ALL.into_iter().any(|guc| {
            (!in_block || guc.block_sample_valid()) && self.state(guc).unknown_at(in_block)
        })
    }

    /// The startup message's user: the `$user` fallback.
    pub(crate) fn startup_user_set(&mut self, user: Option<&str>) {
        self.startup_user = user.map(EcoString::from);
        self.views_rederive();
    }

    /// Origin reported a setting's current value. The session value is final
    /// (origin re-reports a reverting `SET LOCAL` at block end), and from now
    /// on statements touching it are left to origin.
    pub(crate) fn parameter_status(&mut self, guc: Guc, value: &str) {
        let state = self.state_mut(guc);
        state.session = SettingValue::Known(value.into());
        state.block = None;
        state.reported = true;
        self.views_rederive();
    }

    /// A statement that may change settings was forwarded. `in_block` is
    /// whether it lands inside a block (open, or a forwarded `BEGIN` not yet
    /// acknowledged).
    pub(crate) fn effect_apply(&mut self, effect: SettingsEffect, in_block: bool) {
        let inert = effect.inert_at(in_block);
        if inert && !effect.block_revert {
            return;
        }
        self.mutation_seq += 1;
        let seq = self.mutation_seq;
        if effect.block_revert {
            self.block_touched_revert(seq);
        }
        if !inert {
            for guc in Guc::ALL {
                if effect.touches(guc) {
                    self.state_mut(guc).effect_mark(effect.local, in_block, seq);
                }
            }
        }
        self.views_rederive();
    }

    /// A `ROLLBACK TO SAVEPOINT` may have reverted every block value a
    /// statement touched inside the block.
    fn block_touched_revert(&mut self, seq: u64) {
        for state in &mut self.states {
            if state.touched_in_block {
                state.block_unknown_mark(seq);
            }
        }
    }

    /// Mark the session value of one setting unknown (an isolation effect
    /// the analyzer could not read).
    pub(crate) fn session_unknown_mark(&mut self, guc: Guc) {
        self.mutation_seq += 1;
        let seq = self.mutation_seq;
        self.state_mut(guc).session_unknown_mark(seq);
        self.views_rederive();
    }

    /// The session isolation default as a parsed `SET` established it.
    pub(crate) fn isolation_known_set(&mut self, level: IsolationLevel) {
        self.state_mut(Guc::DefaultTransactionIsolation).session =
            SettingValue::Known(isolation_setting_text(level).into());
        self.views_rederive();
    }

    /// The open block ended: block values are gone, and session values were
    /// already marked unknown by any session-scoped change made inside it.
    pub(crate) fn block_exit(&mut self) {
        for state in &mut self.states {
            state.block = None;
            state.touched_in_block = false;
        }
        self.views_rederive();
    }

    /// Apply a settings probe's row (one column per [`Guc::ALL`] entry) under
    /// the status its `ReadyForQuery` carried. `seq` is the sequence the probe
    /// was injected at: it fills only values that were unknown since before
    /// then, never a known value and never one a later statement marked
    /// (the sample predates that statement on origin).
    pub(crate) fn probe_apply(
        &mut self,
        columns: &[Option<EcoString>],
        status: TransactionStatus,
        seq: u64,
    ) {
        if status == TransactionStatus::Failed {
            debug!("settings probe answered in a failed block, discarding");
            return;
        }
        let in_block = status == TransactionStatus::InTransaction;
        for (guc, column) in Guc::ALL.into_iter().zip(columns) {
            let Some(value) = column else {
                continue;
            };
            let state = self.state_mut(guc);
            if in_block {
                state.block_sample_apply(value, seq);
            } else {
                state.idle_sample_apply(value, seq);
            }
        }
        self.views_rederive();
    }

    /// The user `$user` expands to: the current role when one is set, else the
    /// session authorization, else the startup user. Each at the value in
    /// force: the open block's override when it has one.
    fn user_for_search_path(&self) -> Option<&str> {
        self.role_in_force(true)
    }

    /// The role origin runs a statement as at this scope: the current role
    /// when one is set, else the session authorization, else the startup user.
    fn role_in_force(&self, in_block: bool) -> Option<&str> {
        let known = |guc: Guc| self.effective(guc, in_block).known();
        if let Some(role) = known(Guc::Role)
            && !role.eq_ignore_ascii_case("none")
        {
            return Some(role);
        }
        known(Guc::SessionAuthorization).or(self.startup_user.as_deref())
    }

    fn views_rederive(&mut self) {
        let in_block_value = self.state(Guc::SearchPath).block.as_ref();
        let effective = in_block_value.unwrap_or(&self.state(Guc::SearchPath).session);
        let search_path = match effective {
            SettingValue::Known(value) => {
                SearchPathState::resolved(value, self.user_for_search_path())
            }
            SettingValue::Unknown => SearchPathState::Unknown,
        };
        if search_path != self.search_path {
            self.search_path = search_path;
        }
        self.isolation = match self.state(Guc::DefaultTransactionIsolation).session.known() {
            Some(value) => match IsolationLevel::parse(value) {
                Some(level) => IsolationState::Known(level),
                None => IsolationState::Unknown,
            },
            None => IsolationState::Unknown,
        };
        self.idle = self.scope_views_build(false);
        self.block = self.scope_views_build(true);
    }
}

/// The level as `SHOW default_transaction_isolation` prints it.
fn isolation_setting_text(level: IsolationLevel) -> &'static str {
    match level {
        IsolationLevel::ReadCommitted => "read committed",
        IsolationLevel::RepeatableRead => "repeatable read",
        IsolationLevel::Serializable => "serializable",
    }
}

/// The settings probe: one `current_setting` column per [`Guc::ALL`] entry,
/// in order. `missing_ok` so an unset custom setting yields NULL rather than
/// an error. A literal so it is a `&'static str` for [`ProbeQuery`]; the unit
/// test pins it to the enum.
pub(crate) const SETTINGS_PROBE_SQL: &str = "SELECT current_setting('search_path',true),current_setting('timezone',true),current_setting('datestyle',true),current_setting('intervalstyle',true),current_setting('standard_conforming_strings',true),current_setting('backslash_quote',true),current_setting('transform_null_equals',true),current_setting('array_nulls',true),current_setting('default_text_search_config',true),current_setting('gin_fuzzy_search_limit',true),current_setting('timezone_abbreviations',true),current_setting('extra_float_digits',true),current_setting('bytea_output',true),current_setting('client_encoding',true),current_setting('lc_monetary',true),current_setting('lc_numeric',true),current_setting('lc_time',true),current_setting('xmlbinary',true),current_setting('xmloption',true),current_setting('role',true),current_setting('session_authorization',true),current_setting('row_security',true),current_setting('default_transaction_isolation',true)";

#[cfg(test)]
mod tests {
    #![allow(clippy::wildcard_enum_match_arm)]

    use super::*;
    use crate::query::guc::GucSet;

    fn all_known(status: TransactionStatus) -> SessionSettings {
        let mut settings = SessionSettings::new();
        settings.startup_user_set(Some("alice"));
        settings.probe_apply(&sample("public"), status, settings.probe_seq());
        settings
    }

    /// A forwarded statement that touches no setting (every plain write)
    /// leaves the views and the probe sequence alone.
    #[test]
    fn test_inert_effect_leaves_views_and_sequence_untouched() {
        let mut settings = all_known(TransactionStatus::Idle);
        let keying = settings.serve_keying(false).expect("every setting known");
        let seq = settings.probe_seq();
        settings.effect_apply(SettingsEffect::default(), false);
        assert!(Arc::ptr_eq(
            &keying,
            &settings.serve_keying(false).expect("every setting known")
        ));
        assert_eq!(settings.probe_seq(), seq);
    }

    /// A read dispatches with the views built at the last change: the same
    /// `Arc`s until a value changes, fresh ones reflecting the change after.
    #[test]
    fn test_scope_views_are_shared_until_a_value_changes() {
        let mut settings = all_known(TransactionStatus::Idle);
        let keying = settings.serve_keying(false).expect("every setting known");
        let output = settings.output_settings(false);
        assert!(Arc::ptr_eq(
            &keying,
            &settings.serve_keying(false).expect("every setting known")
        ));
        assert!(Arc::ptr_eq(&output, &settings.output_settings(false)));

        settings.parameter_status(Guc::TimeZone, "Asia/Tokyo");
        let changed = settings.serve_keying(false).expect("every setting known");
        assert!(!Arc::ptr_eq(&keying, &changed));
        assert_eq!(changed.literals.timezone, "Asia/Tokyo");
        assert_ne!(settings.output_settings(false).profile(), output.profile());
    }

    /// A probe row with `search_path` set and every other setting a plain value.
    fn sample(search_path: &str) -> Vec<Option<EcoString>> {
        Guc::ALL
            .into_iter()
            .map(|guc| match guc {
                Guc::SearchPath => Some(search_path.into()),
                Guc::Role => Some("none".into()),
                Guc::SessionAuthorization => Some("alice".into()),
                Guc::DefaultTransactionIsolation => Some("read committed".into()),
                Guc::StandardConformingStrings | Guc::ArrayNulls => Some("on".into()),
                Guc::TransformNullEquals => Some("off".into()),
                Guc::GinFuzzySearchLimit => Some("0".into()),
                Guc::ClientEncoding => Some("UTF8".into()),
                _ => Some("x".into()),
            })
            .collect()
    }

    fn resolved(settings: &SessionSettings) -> Vec<String> {
        settings
            .search_path()
            .resolve()
            .expect("resolved search_path")
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    #[test]
    fn test_probe_sql_matches_guc_order() {
        let columns: Vec<String> = Guc::ALL
            .into_iter()
            .map(|guc| format!("current_setting('{}',true)", guc.name()))
            .collect();
        assert_eq!(SETTINGS_PROBE_SQL, format!("SELECT {}", columns.join(",")));
    }

    #[test]
    fn test_new_connection_forwards_until_probed() {
        let mut settings = SessionSettings::new();
        assert!(settings.probe_needed(false));
        assert_eq!(
            settings.serve_search_path(false),
            Err(SettingsForwardReason::SettingUnknown(Guc::SearchPath))
        );
        settings.probe_apply(
            &sample("public"),
            TransactionStatus::Idle,
            settings.probe_seq(),
        );
        assert!(!settings.probe_needed(false));
        assert_eq!(resolved(&settings), vec!["public"]);
        assert_eq!(
            settings.isolation(),
            IsolationState::Known(IsolationLevel::ReadCommitted)
        );
    }

    #[test]
    fn test_parameter_status_sets_and_marks_reported() {
        let mut settings = all_known(TransactionStatus::Idle);
        settings.parameter_status(Guc::SearchPath, r#""$user", public"#);
        assert_eq!(resolved(&settings), vec!["alice", "public"]);
        // Reported: a later SET is left to origin, nothing becomes unknown.
        settings.effect_apply(SettingsEffect::guc(Guc::SearchPath, false), false);
        assert!(!settings.probe_needed(false));
        assert_eq!(resolved(&settings), vec!["alice", "public"]);
    }

    #[test]
    fn test_set_marks_unknown_and_probe_restores() {
        let mut settings = all_known(TransactionStatus::Idle);
        settings.effect_apply(SettingsEffect::guc(Guc::SearchPath, false), false);
        assert!(settings.probe_needed(false));
        assert_eq!(settings.search_path(), &SearchPathState::Unknown);
        assert_eq!(
            settings.serve_search_path(false),
            Err(SettingsForwardReason::SettingUnknown(Guc::SearchPath))
        );
        settings.probe_apply(
            &sample("myapp, public"),
            TransactionStatus::Idle,
            settings.probe_seq(),
        );
        assert_eq!(resolved(&settings), vec!["myapp", "public"]);
    }

    #[test]
    fn test_set_local_scopes_to_block_and_reverts_at_exit() {
        let mut settings = all_known(TransactionStatus::Idle);
        settings.effect_apply(SettingsEffect::guc(Guc::SearchPath, true), true);
        // Session value untouched; the block needs a probe.
        assert!(settings.probe_needed(true));
        assert!(!settings.probe_needed(false));
        settings.probe_apply(
            &sample("blockschema"),
            TransactionStatus::InTransaction,
            settings.probe_seq(),
        );
        assert_eq!(resolved(&settings), vec!["blockschema"]);
        assert!(settings.serve_search_path(true).is_ok());
        settings.block_exit();
        assert_eq!(resolved(&settings), vec!["public"]);
        assert!(!settings.probe_needed(false));
    }

    #[test]
    fn test_set_inside_block_marks_both_scopes() {
        let mut settings = all_known(TransactionStatus::Idle);
        settings.effect_apply(SettingsEffect::guc(Guc::TimeZone, false), true);
        assert!(settings.probe_needed(true));
        settings.probe_apply(
            &sample("public"),
            TransactionStatus::InTransaction,
            settings.probe_seq(),
        );
        assert!(!settings.probe_needed(true));
        // Commit or rollback: the session value is still unknown until a
        // probe outside the block says.
        settings.block_exit();
        assert!(settings.probe_needed(false));
        assert_eq!(
            settings.serve_search_path(false),
            Err(SettingsForwardReason::SettingUnknown(Guc::TimeZone))
        );
        settings.probe_apply(
            &sample("public"),
            TransactionStatus::Idle,
            settings.probe_seq(),
        );
        assert!(!settings.probe_needed(false));
    }

    #[test]
    fn test_untouched_block_needs_no_probe_at_exit() {
        let mut settings = all_known(TransactionStatus::Idle);
        settings.block_exit();
        assert!(!settings.probe_needed(false));
        assert!(settings.serve_search_path(false).is_ok());
    }

    #[test]
    fn test_rollback_to_savepoint_reverts_touched_block_values() {
        let mut settings = all_known(TransactionStatus::Idle);
        settings.effect_apply(SettingsEffect::guc(Guc::TimeZone, true), true);
        settings.probe_apply(
            &sample("public"),
            TransactionStatus::InTransaction,
            settings.probe_seq(),
        );
        assert!(!settings.probe_needed(true));
        settings.effect_apply(SettingsEffect::BLOCK_REVERT, true);
        assert!(settings.probe_needed(true));
        assert_eq!(
            settings.effective(Guc::TimeZone, true),
            &SettingValue::Unknown
        );
        assert_eq!(
            settings.effective(Guc::SearchPath, true),
            &SettingValue::Known("public".into())
        );
    }

    #[test]
    fn test_isolation_default_not_sampled_inside_a_block() {
        let mut settings = all_known(TransactionStatus::Idle);
        settings.session_unknown_mark(Guc::DefaultTransactionIsolation);
        assert!(!settings.probe_needed(true), "cannot be learned in a block");
        assert!(settings.probe_needed(false));
        // It never gates a read: the block's level is fixed at BEGIN.
        assert!(settings.serve_search_path(true).is_ok());
        assert!(settings.serve_search_path(false).is_ok());
        // A block-scoped change to it is invisible to reads.
        settings.effect_apply(
            SettingsEffect::guc(Guc::DefaultTransactionIsolation, true),
            true,
        );
        assert!(!settings.probe_needed(true));
        settings.probe_apply(
            &sample("public"),
            TransactionStatus::InTransaction,
            settings.probe_seq(),
        );
        assert_eq!(settings.isolation(), IsolationState::Unknown);
        settings.probe_apply(
            &sample("public"),
            TransactionStatus::Idle,
            settings.probe_seq(),
        );
        assert_eq!(
            settings.isolation(),
            IsolationState::Known(IsolationLevel::ReadCommitted)
        );
        settings.isolation_known_set(IsolationLevel::Serializable);
        assert_eq!(
            settings.isolation(),
            IsolationState::Known(IsolationLevel::Serializable)
        );
    }

    /// A probe injected before a later SET sampled origin before that SET ran:
    /// its value must not fill what the SET marked unknown.
    #[test]
    fn test_probe_fills_only_values_unknown_since_before_it() {
        let mut settings = all_known(TransactionStatus::Idle);
        settings.effect_apply(SettingsEffect::guc(Guc::ExtraFloatDigits, false), false);
        let probe_seq = settings.probe_seq();
        settings.effect_apply(SettingsEffect::guc(Guc::ByteaOutput, false), false);
        let mut row = sample("public");
        row[Guc::ByteaOutput as usize] = Some("hex".into());
        settings.probe_apply(&row, TransactionStatus::Idle, probe_seq);
        assert_eq!(
            settings.effective(Guc::ExtraFloatDigits, false),
            &SettingValue::Known("x".into())
        );
        assert_eq!(
            settings.effective(Guc::ByteaOutput, false),
            &SettingValue::Unknown
        );
        assert!(settings.probe_needed(false));
        let mut row = sample("public");
        row[Guc::ByteaOutput as usize] = Some("escape".into());
        settings.probe_apply(&row, TransactionStatus::Idle, settings.probe_seq());
        assert_eq!(
            settings.effective(Guc::ByteaOutput, false),
            &SettingValue::Known("escape".into())
        );
    }

    /// Settings that change parsing forward the session while set.
    #[test]
    fn test_parsing_incompatible_settings_forward() {
        let mut settings = all_known(TransactionStatus::Idle);
        assert!(settings.serve_search_path(false).is_ok());
        for (guc, bad, good) in [
            (Guc::StandardConformingStrings, "off", "on"),
            (Guc::TransformNullEquals, "on", "off"),
            (Guc::ArrayNulls, "off", "on"),
            (Guc::GinFuzzySearchLimit, "100", "0"),
            (Guc::ClientEncoding, "LATIN1", "UTF8"),
        ] {
            settings.effect_apply(SettingsEffect::guc(guc, false), false);
            let mut row = sample("public");
            row[guc as usize] = Some(bad.into());
            settings.probe_apply(&row, TransactionStatus::Idle, settings.probe_seq());
            assert_eq!(
                settings.serve_search_path(false),
                Err(SettingsForwardReason::ParsingIncompatible(guc)),
                "{guc}={bad}"
            );
            settings.effect_apply(SettingsEffect::guc(guc, false), false);
            let mut row = sample("public");
            row[guc as usize] = Some(good.into());
            settings.probe_apply(&row, TransactionStatus::Idle, settings.probe_seq());
            assert!(settings.serve_search_path(false).is_ok(), "{guc}={good}");
        }
    }

    #[test]
    fn test_failed_block_sample_is_discarded() {
        let mut settings = SessionSettings::new();
        settings.probe_apply(
            &sample("public"),
            TransactionStatus::Failed,
            settings.probe_seq(),
        );
        assert!(settings.probe_needed(false));
    }

    #[test]
    fn test_role_drives_user_expansion() {
        let mut settings = all_known(TransactionStatus::Idle);
        settings.parameter_status(Guc::SearchPath, "$user, public");
        assert_eq!(resolved(&settings), vec!["alice", "public"]);
        // `SET ROLE bob`: the role is unknown until the probe says.
        settings.effect_apply(SettingsEffect::guc(Guc::Role, false), false);
        let mut row = sample("$user, public");
        row[Guc::Role as usize] = Some("bob".into());
        settings.probe_apply(&row, TransactionStatus::Idle, settings.probe_seq());
        assert_eq!(resolved(&settings), vec!["bob", "public"]);
    }

    #[test]
    fn test_pg_catalog_after_schema_forwards() {
        let mut settings = all_known(TransactionStatus::Idle);
        settings.effect_apply(SettingsEffect::guc(Guc::SearchPath, false), false);
        settings.probe_apply(
            &sample("myschema, pg_catalog"),
            TransactionStatus::Idle,
            settings.probe_seq(),
        );
        assert_eq!(settings.search_path(), &SearchPathState::BuiltinsShadowed);
        assert_eq!(
            settings.serve_search_path(false),
            Err(SettingsForwardReason::BuiltinsShadowed)
        );
        assert!(!settings.probe_needed(false), "the value is known");
        settings.effect_apply(SettingsEffect::guc(Guc::SearchPath, false), false);
        settings.probe_apply(
            &sample("pg_catalog, myschema"),
            TransactionStatus::Idle,
            settings.probe_seq(),
        );
        assert_eq!(resolved(&settings), vec!["pg_catalog", "myschema"]);
    }

    #[test]
    fn test_reset_all_marks_every_unreported_setting() {
        let mut settings = all_known(TransactionStatus::Idle);
        settings.parameter_status(Guc::ClientEncoding, "UTF8");
        settings.effect_apply(SettingsEffect::all(), false);
        assert_eq!(
            settings.effective(Guc::ClientEncoding, false),
            &SettingValue::Known("UTF8".into())
        );
        assert_eq!(
            settings.effective(Guc::TimeZone, false),
            &SettingValue::Unknown
        );
        let _ = GucSet::EMPTY;
    }

    /// An idle sample answers a probe injected before a `SET LOCAL` was
    /// forwarded: it says nothing about the block the `SET LOCAL` opened, so
    /// the block mark stays.
    #[test]
    fn test_idle_sample_keeps_a_block_mark_made_after_injection() {
        let mut settings = all_known(TransactionStatus::Idle);
        let seq = settings.probe_seq();
        settings.effect_apply(SettingsEffect::guc(Guc::ByteaOutput, true), true);
        settings.probe_apply(&sample("public"), TransactionStatus::Idle, seq);
        assert!(
            settings.probe_needed(true),
            "the block value is still unknown"
        );
        assert_eq!(
            settings.effective(Guc::ByteaOutput, true),
            &SettingValue::Unknown
        );
    }

    /// `SET LOCAL` outside a block changes nothing at origin (a warning), so
    /// it must not leave a block mark that no probe would ever clear.
    #[test]
    fn test_local_set_outside_a_block_changes_nothing() {
        let mut settings = all_known(TransactionStatus::Idle);
        settings.effect_apply(SettingsEffect::guc(Guc::SearchPath, true), false);
        assert_eq!(resolved(&settings), vec!["public"]);
        assert!(!settings.probe_needed(false));
        assert!(settings.serve_search_path(false).is_ok());
    }

    /// `$user` follows the role in force for the read: a `SET LOCAL ROLE`
    /// inside the block, once the in-block probe has read it.
    #[test]
    fn test_user_expands_from_the_block_role() {
        let mut settings = SessionSettings::new();
        settings.startup_user_set(Some("alice"));
        let path = r#""$user", public"#;
        settings.probe_apply(&sample(path), TransactionStatus::Idle, settings.probe_seq());
        assert_eq!(resolved(&settings), vec!["alice", "public"]);
        settings.effect_apply(SettingsEffect::guc(Guc::Role, true), true);
        assert!(settings.probe_needed(true));
        let mut row = sample(path);
        row[Guc::Role as usize] = Some("bob".into());
        settings.probe_apply(&row, TransactionStatus::InTransaction, settings.probe_seq());
        assert_eq!(resolved(&settings), vec!["bob", "public"]);
        settings.block_exit();
        assert_eq!(resolved(&settings), vec!["alice", "public"]);
    }
}
