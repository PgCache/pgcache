use std::sync::Arc;

use ecow::EcoString;
use tokio_util::bytes::BytesMut;
use tracing::{debug, trace};

use super::ConnectionState;
use crate::pg::protocol::{
    backend::{PgBackendMessage, PgBackendMessageType, data_row_first_column},
    frontend::{PgFrontendMessage, simple_query_message_build},
};
use crate::proxy::search_path::{SearchPath, search_path_mutations_raw};

/// Extract the SQL text (without the trailing null) from a simple-query
/// `'Q'` message body. Returns `None` if the frame is malformed or the text
/// is not valid UTF-8.
fn query_message_sql(data: &BytesMut) -> Option<&str> {
    // Frame layout: tag(1) | len(4) | sql(N) | nul(1); len counts itself+body,
    // so the SQL text (excluding the nul) lies at bytes 5..len_field.
    let len_bytes: [u8; 4] = data.get(1..5)?.try_into().ok()?;
    let msg_len = u32::from_be_bytes(len_bytes) as usize;
    str::from_utf8(data.get(5..msg_len)?).ok()
}

/// State machine for stripping proxy-added parts out of a client request's
/// own origin response. Only one intercept can be active at a time. Whole
/// injected queries (probes) are not intercepts: their replies are ordered and
/// swallowed by the egress queue's `Probe` slots (see `probe.rs`).
pub(super) enum OriginIntercept {
    /// No intercept active — origin messages forwarded normally.
    None,
    /// pgcache prepended a `Parse` ahead of the client's `Bind+Execute+Sync`
    /// because origin didn't know the statement name. Swallow the resulting
    /// `ParseComplete` (the client didn't ask for it) and let `BindComplete`
    /// onward flow through unchanged.
    LazyParseInline { statement_name: EcoString },
    /// Piggyback: the client's Query was rewritten to append `; SHOW search_path`.
    /// Responses for the original statement are forwarded; the SHOW's response
    /// is stripped and parsed into `search_path_state`.
    TrailingShowSearchPath(TrailingShowState),
}

/// Sub-state for `OriginIntercept::TrailingShowSearchPath`.
#[derive(Debug, Clone, Copy)]
pub(super) enum TrailingShowState {
    /// Before the first `CommandComplete` or `ErrorResponse` — forwarding
    /// responses for the original (client-written) statement.
    PreShow,
    /// After the original statement's `CommandComplete` — intercepting the
    /// injected SHOW's `RowDescription`, `DataRow`, `CommandComplete`.
    InShow,
    /// Original statement errored; PostgreSQL skips subsequent statements in
    /// the simple-query batch, so no SHOW response will arrive. Forward
    /// everything through to the final `ReadyForQuery`.
    Error,
}

/// Append `; SHOW search_path` to a simple-query message, returning a new
/// frame. Caller must have verified the original message parses as exactly
/// one statement with a detected mutation.
fn query_message_append_show_search_path(data: &BytesMut) -> Option<BytesMut> {
    let sql = query_message_sql(data)?;
    simple_query_message_build(&format!("{sql}; SHOW search_path")).ok()
}

/// Search path discovery state machine.
///
/// pgcache needs the session's search_path for table resolution. PG 18+ sends
/// it via ParameterStatus at startup and again on every change. Older versions
/// send it only via an explicit `SHOW search_path` query, so we discover and
/// re-discover it around detected mutations (see `search_path_mutates_*`) and
/// transaction boundaries.
pub(super) enum SearchPathState {
    /// No authoritative value: either before the first ReadyForQuery, or after
    /// a detected mutation (SET/RESET search_path, DISCARD ALL, COMMIT/ROLLBACK).
    /// Cacheable queries are forwarded until the next SHOW response (or
    /// ParameterStatus on PG18+) resolves the value.
    Unknown,

    /// search_path has been resolved (either from ParameterStatus or SHOW
    /// query), pre-expanded ($user → session_user) and shared: dispatching a
    /// cache query clones the Arc instead of collecting a fresh Vec per query.
    Resolved(Arc<[EcoString]>),

    /// The resolved path lists `pg_catalog` after another schema. Only then
    /// can a user function, type or operator with a builtin's exact signature
    /// shadow the builtin on origin, while the cache DB (which has no user
    /// objects) would run the builtin. Cacheable queries forward until the
    /// path changes; the value is known, so nothing is re-probed.
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
    /// Build the resolved state from a raw search_path value, expanding $user
    /// to session_user. session_user comes from the startup message, so it is
    /// final before any of the transitions into this state.
    pub(super) fn resolved(value: &str, session_user: Option<&str>) -> Self {
        let search_path = SearchPath::parse(value);
        let resolved: Arc<[EcoString]> = search_path
            .resolve(session_user)
            .map(EcoString::from)
            .collect();
        if builtins_shadowed(&resolved) {
            debug!("search_path lists pg_catalog after another schema; cacheable queries forward");
            return Self::BuiltinsShadowed;
        }
        Self::Resolved(resolved)
    }

    /// Why a cacheable query is forwarded while this state has no path.
    pub(super) fn forward_reason(&self) -> &'static str {
        match self {
            Self::Unknown => "search_path unknown",
            Self::BuiltinsShadowed => "search_path lists pg_catalog after another schema",
            Self::Resolved(_) => "search_path resolved",
        }
    }

    /// The resolved search_path, if available.
    pub(super) fn resolve(&self) -> Option<Arc<[EcoString]>> {
        match self {
            Self::Unknown | Self::BuiltinsShadowed => None,
            Self::Resolved(search_path) => Some(Arc::clone(search_path)),
        }
    }
}

impl ConnectionState {
    /// Mark search_path as needing rediscovery and clear the describe-cache:
    /// RowDescription column type_oids depend on the resolved search_path,
    /// so cached entries could carry stale column metadata.
    pub(super) fn search_path_mark_unknown(&mut self) {
        self.search_path_state = SearchPathState::Unknown;
        if !self.describe_cache.is_empty() {
            let n = self.describe_cache.len();
            self.describe_cache.clear();
            crate::metrics::handles()
                .conn
                .describe_invalidations
                .increment(n as u64);
        }
    }

    /// Handle an origin message during an active intercept.
    /// Returns true if the message was consumed (caller should not forward).
    #[expect(clippy::wildcard_enum_match_arm)]
    pub(super) fn origin_intercept_handle(&mut self, msg: &PgBackendMessage) -> bool {
        match &self.origin_intercept {
            OriginIntercept::None => false,

            OriginIntercept::LazyParseInline { statement_name } => {
                let stmt_name = statement_name.clone();
                match msg.message_type {
                    PgBackendMessageType::ParseComplete => {
                        // Swallow ParseComplete (client didn't ask for it),
                        // mark origin-prepared, let the rest flow through.
                        if let Some(stmt) = self.prepared_statements.get_mut(stmt_name.as_str()) {
                            stmt.origin_prepared = true;
                            trace!("origin_prepared set for '{}' (lazy parse)", stmt_name);
                        }
                        self.origin_intercept = OriginIntercept::None;
                        true
                    }
                    _ => {
                        // Parse failed (or unexpected response): drop the
                        // intercept and let ErrorResponse + RFQ reach the client.
                        self.origin_intercept = OriginIntercept::None;
                        false
                    }
                }
            }

            &OriginIntercept::TrailingShowSearchPath(state) => {
                self.trailing_show_search_path_handle(state, msg)
            }
        }
    }

    /// Process one origin message under the piggyback intercept.
    ///
    /// Response layout for the rewritten `<stmt>; SHOW search_path`:
    /// - `<stmt>` responses up to its `CommandComplete` or `ErrorResponse`
    /// - on success: SHOW's `RowDescription`, one `DataRow`, `CommandComplete`
    /// - final `ReadyForQuery`
    ///
    /// The original-statement responses are forwarded to the client; the
    /// SHOW portion is consumed and its DataRow is parsed into
    /// `search_path_state`. If the original statement errored, the SHOW is
    /// skipped by PostgreSQL and everything forwards through to the RFQ.
    #[expect(clippy::wildcard_enum_match_arm)]
    pub(super) fn trailing_show_search_path_handle(
        &mut self,
        state: TrailingShowState,
        msg: &PgBackendMessage,
    ) -> bool {
        match state {
            TrailingShowState::PreShow => {
                match msg.message_type {
                    PgBackendMessageType::CommandComplete => {
                        self.origin_intercept =
                            OriginIntercept::TrailingShowSearchPath(TrailingShowState::InShow);
                    }
                    PgBackendMessageType::ErrorResponse => {
                        debug!("piggyback: original statement errored, SHOW skipped");
                        self.origin_intercept =
                            OriginIntercept::TrailingShowSearchPath(TrailingShowState::Error);
                    }
                    _ => {}
                }
                false
            }
            TrailingShowState::InShow => match msg.message_type {
                PgBackendMessageType::DataRows => {
                    if let Some(value) = data_row_first_column(&msg.data) {
                        debug!("piggyback: received search_path from SHOW: {}", value);
                        self.search_path_state =
                            SearchPathState::resolved(value, self.session_user.as_deref());
                        self.search_path_just_piggyback_resolved = true;
                    }
                    true
                }
                PgBackendMessageType::ReadyForQuery => {
                    debug!("piggyback: complete");
                    self.origin_intercept = OriginIntercept::None;
                    false
                }
                // RowDescription and SHOW's CommandComplete are for the client's
                // eyes: strip them.
                PgBackendMessageType::RowDescription | PgBackendMessageType::CommandComplete => {
                    true
                }
                // Anything else at this phase is unexpected; pass through to
                // avoid stalling the protocol.
                _ => false,
            },
            TrailingShowState::Error => {
                if matches!(msg.message_type, PgBackendMessageType::ReadyForQuery) {
                    self.origin_intercept = OriginIntercept::None;
                }
                false
            }
        }
    }

    /// Inspect an outgoing simple-query `Query` message for search_path
    /// mutations. Marks the cached search_path stale on any detected mutation
    /// (SET/RESET search_path, DISCARD ALL, COMMIT/ROLLBACK). When the message
    /// is a single such statement and no other intercept is active, rewrites
    /// the message to append `; SHOW search_path` and installs the
    /// `TrailingShowSearchPath` intercept so the SHOW's response is captured
    /// and stripped before reaching the client — avoiding the extra round
    /// trip a lazy SHOW would cost.
    pub(super) fn search_path_inspect_query(&mut self, msg: &mut PgFrontendMessage) {
        // Fast path: PG18+ auto-reports search_path via ParameterStatus, so
        // the origin will push every change before the next RFQ. Defensive
        // marking and the piggyback SHOW just waste cycles.
        if self.search_path_auto_reported {
            return;
        }

        let Some(sql) = query_message_sql(&msg.data) else {
            return;
        };
        let Ok(mutations) =
            pg_query::parse_raw_scoped(sql, |tree| unsafe { search_path_mutations_raw(tree) })
        else {
            return;
        };

        if mutations.any {
            debug!("search_path mutation detected in Query");
            self.search_path_mark_unknown();
        }

        // Piggyback only on single-statement, piggyback-safe mutations, and
        // only when no other intercept is active (otherwise the inline SHOW
        // response would collide with the existing intercept's state machine).
        if mutations.single_piggybackable.is_some()
            && matches!(self.origin_intercept, OriginIntercept::None)
            && let Some(rewritten) = query_message_append_show_search_path(&msg.data)
        {
            debug!("piggybacking SHOW search_path onto mutation query");
            msg.data = rewritten;
            self.origin_intercept =
                OriginIntercept::TrailingShowSearchPath(TrailingShowState::PreShow);
        }
    }

    /// Detect a search_path mutation in an extended-protocol Parse and mark the
    /// cached search_path stale. Pre-PG18 only (PG18+ auto-reports search_path);
    /// no piggyback for extended — the lazy SHOW-on-RFQ path handles rediscovery.
    pub(super) fn search_path_parse_inspect(&mut self, sql: &str) {
        if self.search_path_auto_reported {
            return;
        }
        if let Ok(mutations) =
            pg_query::parse_raw_scoped(sql, |tree| unsafe { search_path_mutations_raw(tree) })
            && mutations.any
        {
            debug!("search_path mutation detected in Parse");
            self.search_path_mark_unknown();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolved_names(value: &str, session_user: Option<&str>) -> Option<Vec<String>> {
        SearchPathState::resolved(value, session_user)
            .resolve()
            .map(|path| path.iter().map(ToString::to_string).collect())
    }

    #[test]
    fn test_resolved_default_path_expands_user() {
        assert_eq!(
            resolved_names(r#""$user", public"#, Some("alice")),
            Some(vec!["alice".to_owned(), "public".to_owned()])
        );
    }

    #[test]
    fn test_resolved_pg_catalog_first_is_not_shadowed() {
        assert_eq!(
            resolved_names("pg_catalog, public", None),
            Some(vec!["pg_catalog".to_owned(), "public".to_owned()])
        );
    }

    #[test]
    fn test_resolved_pg_catalog_after_another_schema_forwards() {
        assert!(matches!(
            SearchPathState::resolved("myschema, pg_catalog", None),
            SearchPathState::BuiltinsShadowed
        ));
        assert_eq!(resolved_names("myschema, pg_catalog, public", None), None);
        // `$user` expanding ahead of pg_catalog counts as another schema.
        assert_eq!(resolved_names("$user, pg_catalog", Some("alice")), None);
    }

    #[test]
    fn test_resolved_quoted_pg_catalog_is_the_same_schema() {
        assert_eq!(resolved_names(r#"myschema, "pg_catalog""#, None), None);
    }
}
