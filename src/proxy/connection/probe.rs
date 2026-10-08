//! Origin probes: queries the proxy injects on a connection's origin socket
//! for its own bookkeeping, whose responses are swallowed rather than relayed.
//!
//! Three probes exist — `SHOW search_path` (pre-PG18, ADR-006), `SHOW
//! default_transaction_isolation` (ADR-054) and `SELECT
//! pg_current_wal_insert_lsn()` (ADR-048). Each takes a `Probe` slot in the
//! egress queue at its position in origin's reply order, so its reply is
//! recognised by position however many client requests are in flight around
//! it (see `proxy::egress`). No probe needs a quiescent socket: it is injected
//! at any origin `ReadyForQuery`, client-visible or a probe's own, and the
//! conditions are purely state-based. A probe that must not run inside a
//! transaction block is skipped while one is open or a forwarded `BEGIN` is
//! unacknowledged, and its own `ReadyForQuery` status is the final check:
//! anything but idle discards the sample.

use postgres_types::PgLsn;
use tracing::debug;

use super::{ConnectionState, IsolationState, SearchPathState};
use crate::pg::Lsn;
use crate::pg::protocol::{
    backend::{PgBackendMessage, PgBackendMessageType, TransactionStatus, data_row_first_column},
    frontend::ProbeQuery,
};
use crate::proxy::egress::{ProbeKind, ProbeResult};
use crate::query::write::IsolationLevel;

const SEARCH_PATH_PROBE: ProbeQuery = ProbeQuery::new("SHOW search_path;");
const DEFAULT_TRANSACTION_ISOLATION_PROBE: ProbeQuery =
    ProbeQuery::new("SHOW default_transaction_isolation;");
const WAL_INSERT_LSN_PROBE: ProbeQuery = ProbeQuery::new("SELECT pg_current_wal_insert_lsn()");

impl ConnectionState {
    /// Inject every probe the connection's state calls for. Run after each
    /// origin `ReadyForQuery`. Skipped while an extended-protocol group has
    /// bytes on the socket outside any egress slot (a Flush-terminated
    /// Describe, or a Close/Flush ahead of its Sync): their replies carry no
    /// `ReadyForQuery` to order a probe against.
    pub(super) fn probes_inject(&mut self) {
        if self.flush_describe_pending || self.extended.group_origin_forwarded {
            return;
        }
        if matches!(self.search_path_state, SearchPathState::Unknown)
            && !self.egress.probe_pending(ProbeKind::SearchPath)
        {
            debug!("search_path unknown, probing");
            self.probe_send(ProbeKind::SearchPath, &SEARCH_PATH_PROBE);
        }
        if self.isolation_probe_needed() && !self.egress.probe_pending(ProbeKind::IsolationDefault)
        {
            debug!("default_transaction_isolation unknown, probing");
            self.probe_send(
                ProbeKind::IsolationDefault,
                &DEFAULT_TRANSACTION_ISOLATION_PROBE,
            );
            crate::metrics::handles().txn.isolation_probes.increment(1);
        }
        self.commit_lsn_probe_inject();
    }

    /// Maintain the read-after-write log (ADR-048): drop writes the CDC
    /// watermark has passed, then bound the still-unstamped writes with a
    /// commit-LSN probe. The probe runs after every write forwarded before it,
    /// so its LSN bounds exactly the writes recorded through `stamp_seq`; a
    /// write forwarded later has a higher sequence and is left for the next
    /// probe. Never injected into a block, where those writes are uncommitted.
    fn commit_lsn_probe_inject(&mut self) {
        if self.write_log.is_empty() {
            return;
        }
        // Read the watermark through the current dispatch so a cache restart
        // never surfaces a stale-high value.
        if let Some(watermark) = self.dispatch_handle.settled_lsn() {
            self.write_log.purge(watermark);
        }
        if self.forwarding_into_block()
            || self
                .egress
                .probe_pending(ProbeKind::CommitLsn { stamp_seq: 0 })
        {
            return;
        }
        if let Some(stamp_seq) = self.write_log.stamp_seq() {
            self.probe_send(ProbeKind::CommitLsn { stamp_seq }, &WAL_INSERT_LSN_PROBE);
            crate::metrics::handles().raw.probes.increment(1);
        }
    }

    fn probe_send(&mut self, kind: ProbeKind, query: &ProbeQuery) {
        self.egress.probe_push(kind);
        self.origin_write_buf.push_back(query.message());
    }

    /// Consume one origin message that belongs to the probe at the head of
    /// origin's reply order. Its `ReadyForQuery` completes the probe and runs
    /// another injection pass.
    #[expect(clippy::wildcard_enum_match_arm)]
    pub(super) fn probe_message_handle(&mut self, msg: &PgBackendMessage) {
        match msg.message_type {
            PgBackendMessageType::DataRows => {
                if let Some(value) = data_row_first_column(&msg.data) {
                    self.egress.probe_row(value.into());
                }
            }
            PgBackendMessageType::ReadyForQuery => {
                let status = TransactionStatus::from_ready_for_query(&msg.data);
                if let Some(result) = self.egress.probe_complete() {
                    self.probe_apply(result, status);
                }
                self.probes_inject();
            }
            _ => {}
        }
    }

    /// Apply a completed probe. An unreadable value leaves the state as it
    /// was (unknown means forward; the next `ReadyForQuery` re-probes) rather
    /// than looping.
    fn probe_apply(&mut self, result: ProbeResult, status: TransactionStatus) {
        match (result.kind, result.row) {
            (ProbeKind::SearchPath, Some(value)) => {
                debug!("received search_path from SHOW query: {}", value);
                self.search_path_state =
                    SearchPathState::resolved(&value, self.session_user.as_deref());
            }
            (ProbeKind::IsolationDefault, Some(value)) => {
                if status != TransactionStatus::Idle {
                    debug!("isolation probe answered inside a block, discarding");
                    return;
                }
                if let Some(level) = IsolationLevel::parse(&value) {
                    debug!(?level, "received default_transaction_isolation");
                    self.session_isolation = IsolationState::Known(level);
                }
            }
            (ProbeKind::CommitLsn { stamp_seq }, Some(value)) => {
                if status != TransactionStatus::Idle {
                    debug!("commit-LSN probe answered inside a block, discarding");
                    return;
                }
                if let Ok(lsn) = value.parse::<PgLsn>() {
                    self.write_log.stamp(stamp_seq, Lsn::from(lsn));
                }
            }
            (kind, None) => debug!(?kind, "probe returned no row"),
        }
    }
}
