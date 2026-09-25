//! Isolation-level tracking for in-transaction cache serving (PGC-387).
//!
//! A read inside a transaction block may be served from cache only at READ
//! COMMITTED, where each statement sees the latest committed state plus the
//! transaction's own writes — exactly the cache plus the per-connection write
//! log. Stricter levels pin a snapshot the cache cannot reproduce, so they
//! forward. The effective level is invisible on the wire (neither
//! `transaction_isolation` nor `default_transaction_isolation` is reported),
//! so the proxy discovers the session default once with an injected `SHOW`
//! (an origin intercept, like the pre-PG18 search_path probe) and tracks every
//! statement that can change it from then on.
//!
//! A block's level is fixed when the block starts: PostgreSQL reads
//! `default_transaction_isolation` at `BEGIN`, and a later change to the
//! default applies to the *next* block. The proxy mirrors that by snapshotting
//! its session state into a block-scoped state on the idle→in-block
//! ReadyForQuery edge (a `BEGIN ... ISOLATION LEVEL` clause, forwarded
//! earlier, takes precedence), and never probes while a block is open — a
//! probe there would read the default, not the block. Explicit statements only
//! ever tighten the tracked state; only the probe establishes READ COMMITTED,
//! so a statement that failed at origin can never make the proxy serve
//! wrongly. Unknown means forward, and unknown is transient: one round trip.
//!
//! Effects are applied when a statement is forwarded, which can run ahead of
//! the ReadyForQuery that reports the block it lands in (pipelining). Block
//! membership is therefore tracked by counting forwarded `BEGIN`s not yet
//! acknowledged by an idle→in-block edge, not by the lagging status alone.
//!
//! Not observable, and documented as limitations: a `postgresql.conf` reload
//! that changes the default under a live session, and a `SET` executed from
//! inside a function body.

use tracing::debug;

use crate::pg::protocol::{
    backend::{PgBackendMessage, PgBackendMessageType, TransactionStatus, data_row_first_column},
    frontend::simple_query_message_build,
};
use crate::query::write::{IsolationEffect, IsolationLevel, StatementEffects, TransactionBoundary};

use super::*;

/// Injected probe for the session default; its response is swallowed by
/// [`OriginIntercept::DefaultTransactionIsolation`].
const DEFAULT_TRANSACTION_ISOLATION_PROBE: &str = "SHOW default_transaction_isolation;";

/// An isolation level as the proxy knows it — for the session default, and
/// for the block currently open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::proxy::connection) enum IsolationState {
    /// Not yet probed, or invalidated by a mutation. In-transaction reads
    /// forward while a block is in this state.
    Unknown,
    Known(IsolationLevel),
}

/// Why an in-transaction cacheable read was forwarded instead of served.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::proxy::connection) enum TransactionForwardReason {
    /// The block is in the failed state: origin rejects every statement until
    /// it ends, and the client must see that error.
    Failed,
    /// The block's isolation level was not established when it started.
    IsolationUnknown,
    /// REPEATABLE READ or SERIALIZABLE: the read must see the block's snapshot.
    IsolationStrict,
}

impl ConnectionState {
    /// Whether the statement being forwarded lands inside a block: the status
    /// says so, or a forwarded `BEGIN` has not been acknowledged yet.
    fn forwarding_into_block(&self) -> bool {
        self.in_transaction() || self.begins_forwarded > 0
    }

    /// Fold a forwarded statement's isolation effect into the tracked state.
    /// A statement can only tighten what is known: an explicit READ COMMITTED
    /// is confirmed by re-probing rather than trusted (it may have failed at
    /// origin, e.g. `SET TRANSACTION` after the first query).
    pub(in crate::proxy::connection) fn isolation_effects_apply(
        &mut self,
        effects: &StatementEffects,
    ) {
        let begins = effects.transaction == Some(TransactionBoundary::Begin);
        match effects.isolation {
            IsolationEffect::None => {}
            IsolationEffect::Transaction(level) => {
                if level != IsolationLevel::ReadCommitted {
                    if begins {
                        // `BEGIN ... ISOLATION LEVEL`: takes effect on the
                        // idle→in-block edge this statement produces.
                        self.pending_block_isolation = Some(level);
                    } else if self.forwarding_into_block() {
                        // `SET TRANSACTION` inside a block tightens it; outside
                        // one PostgreSQL ignores it with a warning.
                        self.block_isolation = IsolationState::Known(level);
                    }
                }
            }
            IsolationEffect::SessionDefault(level) => {
                self.session_isolation = if level == IsolationLevel::ReadCommitted {
                    IsolationState::Unknown
                } else {
                    IsolationState::Known(level)
                };
                self.isolation_mutated_in_block |= self.forwarding_into_block();
            }
            IsolationEffect::SessionUnknown => {
                self.session_isolation = IsolationState::Unknown;
                self.isolation_mutated_in_block |= self.forwarding_into_block();
            }
        }
        if begins {
            self.begins_forwarded = self.begins_forwarded.saturating_add(1);
        }
    }

    /// Whether a cacheable read may be served from cache given the current
    /// transaction state: outside a block always; inside one only when the
    /// block is healthy and started at READ COMMITTED.
    pub(in crate::proxy::connection) fn transaction_serve_check(
        &self,
    ) -> Result<(), TransactionForwardReason> {
        match self.transaction_status {
            TransactionStatus::Idle => Ok(()),
            TransactionStatus::Failed => Err(TransactionForwardReason::Failed),
            TransactionStatus::InTransaction => match self.block_isolation {
                IsolationState::Known(IsolationLevel::ReadCommitted) => Ok(()),
                IsolationState::Known(_) => Err(TransactionForwardReason::IsolationStrict),
                IsolationState::Unknown => Err(TransactionForwardReason::IsolationUnknown),
            },
        }
    }

    /// Track block boundaries from a ReadyForQuery's status. Idle→in-block
    /// fixes the new block's level (a forwarded `BEGIN` clause, else the
    /// session default as known now); in-block→idle drops it and, if the
    /// session default was touched inside the block, re-probes it — a `SET`
    /// inside a rolled-back block reverts, and `SET LOCAL` reverts regardless.
    pub(in crate::proxy::connection) fn isolation_block_transition(
        &mut self,
        previous: TransactionStatus,
    ) {
        let was_in_block = previous != TransactionStatus::Idle;
        if !was_in_block && self.in_transaction() {
            self.block_isolation = match self.pending_block_isolation.take() {
                Some(level) => IsolationState::Known(level),
                None => self.session_isolation,
            };
            self.begins_forwarded = self.begins_forwarded.saturating_sub(1);
        } else if was_in_block && !self.in_transaction() {
            self.block_isolation = IsolationState::Unknown;
            if self.isolation_mutated_in_block {
                self.isolation_mutated_in_block = false;
                debug!("txn ended after an isolation mutation, marking default unknown");
                self.session_isolation = IsolationState::Unknown;
            }
        }
    }

    /// Inject the next pending session-discovery probe, if any, at a
    /// client-visible ReadyForQuery: search_path first (pre-PG18 only), then
    /// the isolation default. One intercept at a time.
    pub(in crate::proxy::connection) fn session_discovery_inject(&mut self) {
        if !matches!(self.origin_intercept, OriginIntercept::None) {
            return;
        }
        if let SearchPathState::Unknown = self.search_path_state {
            debug!("search_path unknown, sending SHOW search_path query");
            self.origin_intercept = OriginIntercept::SearchPath;
            // Injected query: its response is fully swallowed by the
            // SearchPath intercept, so it gets no client egress slot.
            self.origin_write_buf
                .push_back(simple_query_message_build("SHOW search_path;"));
            return;
        }
        self.isolation_probe_inject();
    }

    /// Inject the isolation-default probe if it is unknown, no intercept is
    /// active, and no block is open (inside one the probe would report the
    /// default, not the block's level — and in a failed block it would just
    /// error). Also the only probe chained onto another intercept's
    /// completion: a probe that leaves the state unknown is not retried until
    /// the next client-visible ReadyForQuery, so nothing can loop.
    pub(in crate::proxy::connection) fn isolation_probe_inject(&mut self) {
        if self.session_isolation != IsolationState::Unknown
            || self.in_transaction()
            || !matches!(self.origin_intercept, OriginIntercept::None)
        {
            return;
        }
        debug!("default_transaction_isolation unknown, probing");
        self.origin_intercept = OriginIntercept::DefaultTransactionIsolation;
        self.origin_write_buf.push_back(simple_query_message_build(
            DEFAULT_TRANSACTION_ISOLATION_PROBE,
        ));
        crate::metrics::handles().txn.isolation_probes.increment(1);
    }

    /// Consume one origin message under the isolation probe intercept. Returns
    /// whether the intercept is complete (its ReadyForQuery arrived).
    #[expect(clippy::wildcard_enum_match_arm)]
    pub(in crate::proxy::connection) fn default_transaction_isolation_handle(
        &mut self,
        msg: &PgBackendMessage,
    ) -> bool {
        match msg.message_type {
            PgBackendMessageType::DataRows => {
                // Unreadable output stays Unknown (forward) rather than re-probe
                // forever: the next client ReadyForQuery triggers a fresh probe.
                if let Some(level) =
                    data_row_first_column(&msg.data).and_then(IsolationLevel::parse)
                {
                    debug!(?level, "received default_transaction_isolation");
                    self.session_isolation = IsolationState::Known(level);
                }
                false
            }
            PgBackendMessageType::ReadyForQuery => true,
            _ => false,
        }
    }
}
