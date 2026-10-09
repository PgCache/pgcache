//! Origin probes: queries the proxy injects on a connection's origin socket
//! for its own bookkeeping, whose responses are swallowed rather than relayed.
//!
//! Two probes exist — the session-settings sample (PGC-287: one
//! `current_setting` column per tracked setting, covering the pre-PG18
//! search_path discovery of ADR-006 and the isolation default of ADR-054) and
//! `SELECT pg_current_wal_insert_lsn()` (ADR-048). Each takes a `Probe` slot
//! in the egress queue at its position in origin's reply order, so its reply
//! is recognised by position however many client requests are in flight
//! around it (see `proxy::egress`). No probe needs a quiescent socket: it is
//! injected at any origin `ReadyForQuery`, client-visible or a probe's own,
//! and the conditions are purely state-based. The settings probe runs inside
//! blocks too — a `SET LOCAL` value is what an in-block read must honour —
//! and its own `ReadyForQuery` status scopes the sample. The commit-LSN probe
//! must not run inside a block and is skipped while one is open or a
//! forwarded `BEGIN` is unacknowledged; its status is the final check.

use postgres_types::PgLsn;
use tracing::debug;

use super::ConnectionState;
use crate::pg::Lsn;
use crate::pg::protocol::{
    backend::{PgBackendMessage, PgBackendMessageType, TransactionStatus, data_row_columns},
    frontend::ProbeQuery,
};
use crate::proxy::egress::{ProbeKind, ProbeResult};
use crate::proxy::session_settings::SETTINGS_PROBE_SQL;

const SETTINGS_PROBE: ProbeQuery = ProbeQuery::new(SETTINGS_PROBE_SQL);
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
        if self
            .session_settings
            .probe_needed(self.forwarding_into_block())
            && !self.egress.probe_pending(ProbeKind::SessionSettings)
        {
            debug!("session settings unknown, probing");
            self.probe_send(ProbeKind::SessionSettings, &SETTINGS_PROBE);
            crate::metrics::handles().conn.settings_probes.increment(1);
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
                if let Some(columns) = data_row_columns(&msg.data) {
                    self.egress
                        .probe_row(columns.into_iter().map(|c| c.map(Into::into)).collect());
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
            (ProbeKind::SessionSettings, Some(columns)) => {
                debug!(?status, "received session settings sample");
                self.session_settings_update(|settings| settings.probe_apply(&columns, status));
            }
            (ProbeKind::CommitLsn { stamp_seq }, Some(columns)) => {
                if status != TransactionStatus::Idle {
                    debug!("commit-LSN probe answered inside a block, discarding");
                    return;
                }
                if let Some(Some(value)) = columns.first()
                    && let Ok(lsn) = value.parse::<PgLsn>()
                {
                    self.write_log.stamp(stamp_seq, Lsn::from(lsn));
                }
            }
            (kind, None) => debug!(?kind, "probe returned no row"),
        }
    }
}
