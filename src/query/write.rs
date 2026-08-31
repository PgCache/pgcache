//! Write-statement classification for per-connection read-after-write
//! tracking (PGC-124). Produced at cacheability-analysis time by
//! [`ast::statement_convert_raw`](crate::query::ast::statement_convert_raw);
//! consumed by the proxy's per-connection write log.

use std::sync::Arc;

use ecow::EcoString;
use smallvec::SmallVec;

use crate::query::ast::LiteralValue;

/// Row cap for [`WriteClass::InsertRows`]; larger INSERTs degrade to
/// [`WriteClass::Table`].
pub const INSERT_MAX_ROWS: usize = 64;

/// Effect of a transaction-control statement on the session's
/// explicit-transaction state. `Begin` also covers `COMMIT AND CHAIN` /
/// `ROLLBACK AND CHAIN` (the session re-enters a transaction); savepoint
/// operations (SAVEPOINT, RELEASE, ROLLBACK TO) leave the state unchanged
/// and carry no boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionBoundary {
    Begin,
    End,
}

/// A table reference as written in a DML statement. Never schema-resolved —
/// the proxy has no catalog knowledge, so consumers must treat an unqualified
/// name conservatively (same-name ⇒ possibly the same table).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationRef {
    pub schema: Option<EcoString>,
    pub name: EcoString,
}

/// How much the classifier could prove about a statement that may modify
/// table data. Every fallback degrades toward a *wider* scope (more
/// conservative reads later), never toward missing a write.
#[derive(Debug, Clone)]
pub enum WriteClass {
    /// `INSERT ... VALUES` with an explicit column list and extractable rows.
    InsertRows(Arc<InsertStatement>),
    /// Target relation known, effect not row-enumerable (UPDATE, DELETE,
    /// MERGE, degraded INSERT forms, COPY FROM, TRUNCATE).
    Table(RelationRef),
    /// Scope unknown: DDL, CALL, DO, EXECUTE, EXPLAIN, multi-statement, or
    /// anything unrecognized.
    Connection,
    /// `PREPARE TRANSACTION`: the commit happens later, possibly from another
    /// session, so a commit-LSN bound sampled at this statement's
    /// ReadyForQuery would under-bound it. The write-log entry must never be
    /// LSN-stamped; it clears only at connection close.
    ConnectionUnstampable,
}

/// One VALUES row, positionally aligned with [`InsertStatement::columns`].
/// `None` = cell value unknown (DEFAULT, cast, expression, ...) — a cell
/// that can never prove disjointness.
pub type InsertRow = SmallVec<[Option<LiteralValue>; 4]>;

/// An extracted `INSERT ... VALUES` statement.
#[derive(Debug)]
pub struct InsertStatement {
    pub relation: RelationRef,
    /// Explicit target column names, in statement order.
    pub columns: Vec<EcoString>,
    pub rows: Vec<InsertRow>,
}
