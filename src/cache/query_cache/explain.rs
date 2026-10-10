//! Routing a `pgcache_explain(...)` request (PGC-345): resolve its target to a
//! cached query and hand an [`ExplainJob`] to the serve pool, which borrows a
//! connection and runs the actual EXPLAIN off the connection's thread.

use ecow::EcoString;
use tracing::debug;

use super::{CacheDispatch, ServeJob};
use crate::cache::explain::{ExplainJob, ExplainKind};
use crate::cache::messages::CacheReply;
use crate::cache::mv::{MvServe, MvState};
use crate::cache::reply::ReplySender;
use crate::cache::types::{CachedQueryState, CachedQueryView};
use crate::pg::protocol::backend::TransactionStatus;
use crate::proxy::{ClientSocket, ExplainSpec, ExplainTarget};
use crate::query::ast::query_expr_convert_raw;
use crate::query::keying_settings::KeyingSettings;
use crate::query::{Fingerprint, SessionKey};
use crate::timing::QueryTiming;

/// The client an explain response goes back to.
pub(super) struct ExplainClient {
    pub(super) client_socket: ClientSocket,
    pub(super) reply_tx: ReplySender<CacheReply>,
    pub(super) timing: QueryTiming,
    pub(super) transaction_status: TransactionStatus,
}

impl CacheDispatch {
    /// Route a `pgcache_explain(...)` request to the serve pool.
    pub(super) fn explain_dispatch(
        &self,
        spec: ExplainSpec,
        client: ExplainClient,
        keying: &KeyingSettings,
    ) {
        let job = ExplainJob {
            client_socket: client.client_socket,
            reply_tx: client.reply_tx,
            timing: client.timing,
            transaction_status: client.transaction_status,
            kind: self.explain_kind_build(spec, keying),
        };
        if self.serve_tx.send(ServeJob::Explain(job)).is_err() {
            // Serve channel closed (subsystem teardown): the leased socket drops
            // with the job and the connection tears down.
            debug!("serve channel closed; dropping explain request");
        }
    }

    /// Resolve an [`ExplainSpec`] to the concrete work the serve pool should do:
    /// a [`ExplainKind::Run`] for a Ready cached query, or
    /// [`ExplainKind::Unavailable`] with a reason otherwise.
    fn explain_kind_build(&self, spec: ExplainSpec, keying: &KeyingSettings) -> ExplainKind {
        let fingerprint = match self.explain_fingerprint(&spec.target, keying) {
            Ok(fingerprint) => fingerprint,
            Err(unavailable) => return unavailable,
        };
        let Some(view) = self
            .state_view
            .cached_queries
            .get(&fingerprint)
            .map(|view| view.clone())
        else {
            return ExplainKind::Unavailable {
                message: format!("query not cached (fingerprint {fingerprint})").into(),
            };
        };
        explain_kind_for_view(fingerprint, view, spec.options)
    }
}

impl CacheDispatch {
    /// The fingerprint an explain target names, or why it can't be resolved.
    /// Inline SQL is keyed the way dispatch keys it: under the session's
    /// search_path, through the alias map when a table name is unqualified.
    fn explain_fingerprint(
        &self,
        target: &ExplainTarget,
        keying: &KeyingSettings,
    ) -> Result<Fingerprint, ExplainKind> {
        match target {
            ExplainTarget::Fingerprint(value) => Ok(Fingerprint::from_raw(*value)),
            ExplainTarget::Sql(sql) => {
                let key = explain_sql_key(sql, keying).ok_or(ExplainKind::Unavailable {
                    message: "could not parse query for explain".into(),
                })?;
                self.fingerprint_for_key(key)
                    .ok_or(ExplainKind::Unavailable {
                        message:
                            "query not cached (no session has resolved it under this search_path)"
                                .into(),
                    })
            }
        }
    }
}

/// The explain work for a cached query in `view`'s state: only a Ready query
/// with a serve shape can run.
fn explain_kind_for_view(
    fingerprint: Fingerprint,
    view: CachedQueryView,
    options: EcoString,
) -> ExplainKind {
    let CachedQueryView {
        state,
        serve_shape,
        mv,
        ..
    } = view;
    match (state, serve_shape) {
        (CachedQueryState::Ready, Some(serve_shape)) => {
            // Read-only backend decision: reflect what would serve now without
            // the serve-path `mv_dispatch_decide` side effects (a diagnostic
            // must not schedule an MV build or move serve metrics). A Fresh MV
            // with captured columns serves from the MV; everything else serves
            // from source rows.
            let mv = match (mv.state(), mv.serve_plan) {
                (MvState::Fresh, Some(plan)) => MvServe::Mv(plan),
                _ => MvServe::SourceRow,
            };
            ExplainKind::Run {
                fingerprint,
                mv,
                serve_shape,
                options,
            }
        }
        (CachedQueryState::Ready, None) => ExplainKind::Unavailable {
            message: "query cannot be served from cache (no resolved form)".into(),
        },
        (
            state @ (CachedQueryState::Pending { .. }
            | CachedQueryState::Loading
            | CachedQueryState::Invalidated),
            _,
        ) => ExplainKind::Unavailable {
            message: format!("query cannot be served from cache (state {state:?})").into(),
        },
    }
}

/// Key the inline SQL of a `pgcache_explain('<sql>')` request the same way
/// dispatch keys it (raw-tree convert → `query_expr_session_key`), so the
/// lookup hits the cached entry. `None` if the argument doesn't parse as a SELECT.
fn explain_sql_key(sql: &str, keying: &KeyingSettings) -> Option<SessionKey> {
    pg_query::parse_raw_scoped(sql, |tree| unsafe { query_expr_convert_raw(tree) })
        .ok()
        .and_then(Result::ok)
        .map(|query| keying.session_key(&query))
}
