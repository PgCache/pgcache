use ecow::EcoString;
use tracing::trace;

use super::ConnectionState;
use crate::pg::protocol::backend::{PgBackendMessage, PgBackendMessageType};

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
}

impl ConnectionState {
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
        }
    }
}
