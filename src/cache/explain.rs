//! Serve `pgcache_explain(...)` (PGC-345): run an `EXPLAIN` of a cached query's
//! cache-side SQL against the cache database and synthesize a `QUERY PLAN`
//! result for the client. A deliberately simple path that borrows a pooled
//! connection and reads the whole response — it does not share the hot serve
//! state machine (`handle_cached_query`), whose invariants (PGC-291/278, memo,
//! coalescing) do not apply to a one-shot diagnostic.

use std::time::Instant;

use ecow::EcoString;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::{Sender, UnboundedSender};
use tokio_util::bytes::BytesMut;
use tracing::debug;

use crate::cache::messages::{CacheOutcome, CacheReply};
use crate::cache::mv::{MvServe, mv_table_name};
use crate::cache::reply::ReplySender;
use crate::cache::serve::ConnectionGuard;
use crate::cache::types::{CacheStateView, SharedResolved};
use crate::pg::cache_connection::{CacheConnection, ExplainOutcome};
use crate::pg::protocol::encode::{
    READY_FOR_QUERY_IDLE_MSG, command_complete_tag_encode, data_row_text_encode,
    notice_response_encode, row_description_text_encode,
};
use crate::proxy::ClientSocket;
use crate::query::resolved::ResolvedQueryExpr;
use crate::query::{Fingerprint, QueryShape, query_shape_derive};
use crate::timing::QueryTiming;

/// A unit of explain work routed through the serve pool, carrying the leased
/// client socket and reply channel so the same return path as a normal serve
/// applies.
pub struct ExplainJob {
    pub client_socket: ClientSocket,
    pub reply_tx: ReplySender<CacheReply>,
    pub timing: QueryTiming,
    pub kind: ExplainKind,
}

pub enum ExplainKind {
    /// Run `EXPLAIN` of the cached query's cache-side SQL.
    Run {
        fingerprint: Fingerprint,
        mv: MvServe,
        /// Source-row shape (`None` falls back to deriving one from `resolved`).
        serve_shape: Option<QueryShape>,
        resolved: SharedResolved,
        /// Verbatim EXPLAIN option list (may be empty).
        options: EcoString,
    },
    /// The query is not cached / cannot be served from cache; report `message`
    /// to the client.
    Unavailable { message: EcoString },
}

/// Handle one [`ExplainJob`]: run the EXPLAIN (when applicable), synthesize the
/// `QUERY PLAN` response, write it to the client, and return the connection.
pub async fn handle_explain_request(
    conn: CacheConnection,
    return_tx: Sender<CacheConnection>,
    replenish_tx: UnboundedSender<()>,
    job: ExplainJob,
    state_view: &CacheStateView,
) {
    let ExplainJob {
        mut client_socket,
        reply_tx,
        mut timing,
        kind,
    } = job;
    timing.worker_start_at = Some(Instant::now());
    let mut guard = ConnectionGuard::new(conn, return_tx, replenish_tx);

    let response = match kind {
        ExplainKind::Unavailable { message } => {
            explain_response_encode(&[], &[format!("pgcache: {message}")])
        }
        ExplainKind::Run {
            fingerprint,
            mv,
            serve_shape,
            resolved,
            options,
        } => {
            // Source-row queries can also serve from the in-process response
            // memo (ADR-036); report whether a live entry currently exists.
            let memoized = state_view.memo.fingerprint_memoized(fingerprint);
            let (explain_sql, literals, notices) = explain_sql_build(
                fingerprint,
                &mv,
                serve_shape.as_ref(),
                &resolved,
                &options,
                memoized,
            );
            match guard.conn.take() {
                None => explain_response_encode(&[], &["pgcache: no cache connection".to_owned()]),
                Some(mut conn) => match conn.explain_collect(&explain_sql, &literals).await {
                    Ok(ExplainOutcome::Plan(lines)) => {
                        guard.conn = Some(conn);
                        explain_response_encode(&notices, &lines)
                    }
                    Ok(ExplainOutcome::CacheError(message)) => {
                        guard.conn = Some(conn);
                        explain_response_encode(
                            &[],
                            &[format!("pgcache: cache DB error: {message}")],
                        )
                    }
                    Err(_) => {
                        // Connection-fatal (I/O / desync): discard via the poisoned guard.
                        guard.poisoned = true;
                        explain_response_encode(
                            &[],
                            &["pgcache: explain failed (cache connection error)".to_owned()],
                        )
                    }
                },
            }
        }
    };

    timing.response_written_at = Some(Instant::now());
    if let Err(e) = client_socket.write_all(&response).await {
        debug!("explain client write failed: {e}");
    }
    if let Err(e) = guard.release().await {
        debug!("explain connection release failed: {e}");
    }
    let _ = reply_tx.send(CacheReply {
        socket: client_socket,
        outcome: CacheOutcome::Complete(Some(timing)),
    });
}

/// Build the `EXPLAIN`-wrapped cache-side SQL, its bind literals, and the
/// diagnostics lines (each emitted as its own NOTICE): fingerprint, chosen
/// backend, and the rewritten cache-side SQL.
///
/// Both backends explain the parameterized **source-row** query against the
/// cached base tables. For a source-row query that is the serve itself. For an
/// MV-backed query the steady-state serve is a trivial scan of the
/// `pgcache_mv.q_<fp>` table (already stated by the backend NOTICE), so the plan
/// slot instead shows the source-table query the MV materializes — the plan that
/// runs when the MV is stale / during build, and the informative one. It is a
/// valid execution because a Ready (non-invalidated) query's base tables are
/// complete in the cache. The per-request `LIMIT`/`OFFSET` is intentionally
/// omitted — the plan reflects the cached relation scan, not a particular limit.
fn explain_sql_build(
    fingerprint: Fingerprint,
    mv: &MvServe,
    serve_shape: Option<&QueryShape>,
    resolved: &ResolvedQueryExpr,
    options: &str,
    memoized: bool,
) -> (String, Vec<crate::query::ast::LiteralValue>, Vec<String>) {
    let prefix = explain_prefix_build(options);

    // Matching is by fingerprint, which hashes the query's literals, so the
    // bound constants are exactly the caller's. The only fidelity gap is plan
    // caching: this is a fresh custom plan for those constants, whereas a
    // long-lived worker statement may have switched to a generic plan.
    let (shape_sql, literals) = match serve_shape {
        Some(shape) => (shape.sql.to_string(), shape.literals.clone()),
        None => {
            let derived = query_shape_derive(resolved);
            (derived.sql.to_string(), derived.literals)
        }
    };

    let mut notices = vec![format!("pgcache_explain: fingerprint={fingerprint}")];
    match mv {
        MvServe::Mv(_) => {
            notices.push(format!(
                "pgcache_explain: backend=MV {}",
                mv_table_name(fingerprint)
            ));
            notices.push(
                "pgcache_explain: steady-state serve scans the MV table; the plan below is the \
                 source-table fallback (runs while the MV is stale / during build)"
                    .to_owned(),
            );
        }
        MvServe::SourceRow => {
            notices.push(
                "pgcache_explain: backend=source rows (custom plan for this query's constants; \
                 a long-lived worker statement may use a generic plan)"
                    .to_owned(),
            );
            notices.push(
                if memoized {
                    "pgcache_explain: memo=hit (served from the in-process response memo; the \
                     plan below is the cache-DB fallback)"
                } else {
                    "pgcache_explain: memo=miss (executes the plan below against the cache DB)"
                }
                .to_owned(),
            );
        }
    }
    notices.push(format!("pgcache_explain: rewritten SQL: {shape_sql}"));

    (format!("{prefix}{shape_sql}"), literals, notices)
}

/// Build the `EXPLAIN`/`EXPLAIN (<options>)` prefix from a verbatim option list.
fn explain_prefix_build(options: &str) -> String {
    let options = options.trim();
    if options.is_empty() {
        "EXPLAIN ".to_owned()
    } else {
        format!("EXPLAIN ({options}) ")
    }
}

/// Encode the synthesized client response: one NOTICE per diagnostics line, then
/// a one-column `QUERY PLAN` result with one row per plan line,
/// `CommandComplete EXPLAIN`, and `ReadyForQuery`.
fn explain_response_encode(notices: &[String], plan_lines: &[String]) -> BytesMut {
    let mut buf = BytesMut::new();
    for notice in notices {
        notice_response_encode(notice, &mut buf);
    }
    row_description_text_encode("QUERY PLAN", &mut buf);
    for line in plan_lines {
        data_row_text_encode(Some(line), &mut buf);
    }
    command_complete_tag_encode("EXPLAIN", &mut buf);
    buf.extend_from_slice(READY_FOR_QUERY_IDLE_MSG);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg::protocol::backend::{
        COMMAND_COMPLETE_TAG, DATA_ROW_TAG, READY_FOR_QUERY_TAG, ROW_DESCRIPTION_TAG,
    };

    #[test]
    fn test_explain_prefix_build() {
        assert_eq!(explain_prefix_build(""), "EXPLAIN ");
        assert_eq!(explain_prefix_build("  "), "EXPLAIN ");
        assert_eq!(
            explain_prefix_build("ANALYZE, FORMAT JSON"),
            "EXPLAIN (ANALYZE, FORMAT JSON) "
        );
    }

    #[test]
    fn test_explain_response_encode_frame_sequence() {
        let notices = vec![
            "pgcache_explain: backend=source rows".to_owned(),
            "pgcache_explain: rewritten SQL: SELECT id FROM orders".to_owned(),
        ];
        let lines = vec![
            "Seq Scan on orders".to_owned(),
            "  Filter: (id = 1)".to_owned(),
        ];
        let buf = explain_response_encode(&notices, &lines);

        // Collect frame tags in order by walking the length-prefixed frames.
        let mut tags = Vec::new();
        let mut i = 0;
        while i < buf.len() {
            tags.push(buf[i]);
            let len = usize::try_from(i32::from_be_bytes(
                buf[i + 1..i + 5].try_into().expect("length"),
            ))
            .expect("frame length non-negative");
            i += 1 + len;
        }
        assert_eq!(
            tags,
            vec![
                b'N',                 // NoticeResponse: backend
                b'N',                 // NoticeResponse: rewritten SQL
                ROW_DESCRIPTION_TAG,  // RowDescription
                DATA_ROW_TAG,         // plan line 1
                DATA_ROW_TAG,         // plan line 2
                COMMAND_COMPLETE_TAG, // CommandComplete
                READY_FOR_QUERY_TAG,  // ReadyForQuery
            ]
        );
    }

    #[test]
    fn test_explain_response_encode_without_notice_starts_with_row_description() {
        let buf = explain_response_encode(&[], &["pgcache: query not cached".to_owned()]);
        assert_eq!(buf[0], ROW_DESCRIPTION_TAG);
    }
}
