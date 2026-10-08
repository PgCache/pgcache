//! Isolation-level tracking for in-transaction cache serving (PGC-387).
//!
//! A read inside a transaction block may be served from cache only at READ
//! COMMITTED, where each statement sees the latest committed state plus the
//! transaction's own writes — exactly the cache plus the per-connection write
//! log. Stricter levels pin a snapshot the cache cannot reproduce, so they
//! forward. The effective level is invisible on the wire (neither
//! `transaction_isolation` nor `default_transaction_isolation` is reported),
//! so the proxy discovers the session default once with an injected `SHOW`
//! (an ordered probe, see `probe.rs`) and tracks every statement that can
//! change it from then on.
//!
//! A block's level is fixed when the block starts: PostgreSQL reads
//! `default_transaction_isolation` at `BEGIN`, and a later change to the
//! default applies to the *next* block. The proxy mirrors that by snapshotting
//! its session state into a block-scoped state on the idle→in-block
//! ReadyForQuery edge (a `BEGIN ... ISOLATION LEVEL` clause, forwarded
//! earlier, takes precedence), and never probes while a block is open or a
//! forwarded `BEGIN` is unacknowledged — a probe there would read the default,
//! not the block (its own ReadyForQuery status is the final check). Explicit
//! statements only ever tighten the tracked state; only the probe establishes
//! READ COMMITTED, so a statement that failed at origin can never make the
//! proxy serve wrongly. Unknown means forward, and unknown is transient: one
//! round trip.
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

use super::ConnectionState;
use crate::pg::protocol::backend::TransactionStatus;
use crate::query::write::{IsolationEffect, IsolationLevel, StatementEffects, TransactionBoundary};

/// An isolation level as the proxy knows it — for the session default, and
/// for the block currently open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum IsolationState {
    /// Not yet probed, or invalidated by a mutation. In-transaction reads
    /// forward while a block is in this state.
    Unknown,
    Known(IsolationLevel),
}

/// Why an in-transaction cacheable read was forwarded instead of served.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TransactionForwardReason {
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
    pub(super) fn forwarding_into_block(&self) -> bool {
        self.in_transaction() || self.begins_forwarded > 0
    }

    /// Fold a forwarded statement's isolation effect into the tracked state.
    /// A statement can only tighten what is known: an explicit READ COMMITTED
    /// is confirmed by re-probing rather than trusted (it may have failed at
    /// origin, e.g. `SET TRANSACTION` after the first query).
    pub(super) fn isolation_effects_apply(&mut self, effects: &StatementEffects) {
        let begins = effects.transaction == Some(TransactionBoundary::Begin);
        match effects.isolation {
            IsolationEffect::None => {}
            IsolationEffect::Transaction(level) => self.transaction_isolation_apply(level, begins),
            IsolationEffect::SessionDefault(IsolationLevel::ReadCommitted)
            | IsolationEffect::SessionUnknown => {
                self.session_isolation_set(IsolationState::Unknown)
            }
            IsolationEffect::SessionDefault(level) => {
                self.session_isolation_set(IsolationState::Known(level));
            }
        }
        if begins {
            self.begins_forwarded = self.begins_forwarded.saturating_add(1);
        }
    }

    /// A transaction-scoped level (`BEGIN ... ISOLATION LEVEL`, `SET
    /// TRANSACTION`). READ COMMITTED never tightens anything, so it is ignored.
    fn transaction_isolation_apply(&mut self, level: IsolationLevel, begins: bool) {
        if level == IsolationLevel::ReadCommitted {
            return;
        }
        if begins {
            // `BEGIN ... ISOLATION LEVEL`: takes effect on the idle→in-block
            // edge this statement produces.
            self.pending_block_isolation = Some(level);
        } else if self.forwarding_into_block() {
            // `SET TRANSACTION` inside a block tightens it; outside one
            // PostgreSQL ignores it with a warning.
            self.block_isolation = IsolationState::Known(level);
        }
    }

    /// A session-default change. Inside a block it may revert (rollback, `SET
    /// LOCAL`), so the block's end re-probes.
    fn session_isolation_set(&mut self, state: IsolationState) {
        self.session_isolation = state;
        self.isolation_mutated_in_block |= self.forwarding_into_block();
    }

    /// Whether a cacheable read may be served from cache given the current
    /// transaction state: outside a block always; inside one only when the
    /// block is healthy and started at READ COMMITTED.
    pub(super) fn transaction_serve_check(&self) -> Result<(), TransactionForwardReason> {
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
    pub(super) fn isolation_block_transition(&mut self, previous: TransactionStatus) {
        let was_in_block = previous != TransactionStatus::Idle;
        match (was_in_block, self.in_transaction()) {
            (false, true) => self.block_enter(),
            (true, false) => self.block_exit(),
            (false, false) | (true, true) => {}
        }
    }

    fn block_enter(&mut self) {
        self.block_isolation = match self.pending_block_isolation.take() {
            Some(level) => IsolationState::Known(level),
            None => self.session_isolation,
        };
        self.begins_forwarded = self.begins_forwarded.saturating_sub(1);
    }

    fn block_exit(&mut self) {
        self.block_isolation = IsolationState::Unknown;
        if self.isolation_mutated_in_block {
            self.isolation_mutated_in_block = false;
            debug!("txn ended after an isolation mutation, marking default unknown");
            self.session_isolation = IsolationState::Unknown;
        }
    }

    /// The session default is unknown and no block is open (or about to be)
    /// that would hide it.
    pub(super) fn isolation_probe_needed(&self) -> bool {
        self.session_isolation == IsolationState::Unknown && !self.forwarding_into_block()
    }
}
