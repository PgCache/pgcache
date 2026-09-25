//! Write-statement classification for per-connection read-after-write
//! tracking (PGC-124). Produced at cacheability-analysis time by
//! [`ast::statement_convert_raw`](crate::query::ast::statement_convert_raw);
//! consumed by the proxy's per-connection write log.

use std::sync::Arc;

use ecow::EcoString;
use smallvec::SmallVec;

use crate::query::ast::{BinaryOp, LiteralValue};

/// Classification-time cap on extracted `INSERT ... VALUES` rows: a larger
/// statement degrades to [`WriteClass::Table`] rather than pay per-cell
/// extraction — which runs during query analysis for every deployment, read-
/// after-write enabled or not, and again per Bind on the extended path.
/// Accumulation across statements is capped separately (and much higher) in
/// the write log's column-major aggregate — see `INSERT_MERGED_ROWS_CAP`.
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

/// Transaction isolation levels as they matter to in-transaction cache
/// serving (PGC-387). `READ UNCOMMITTED` folds into `ReadCommitted`, which is
/// how PostgreSQL treats it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl IsolationLevel {
    /// Parse a level as PostgreSQL spells it in `SHOW` output, `SET` values
    /// and `ISOLATION LEVEL` clauses (case-insensitive, whitespace-normalized).
    pub fn parse(value: &str) -> Option<Self> {
        let mut words = value.split_whitespace().map(str::to_ascii_lowercase);
        let first = words.next()?;
        let second = words.next();
        if words.next().is_some() {
            return None;
        }
        match (first.as_str(), second.as_deref()) {
            ("read", Some("committed" | "uncommitted")) => Some(Self::ReadCommitted),
            ("repeatable", Some("read")) => Some(Self::RepeatableRead),
            ("serializable", None) => Some(Self::Serializable),
            _ => None,
        }
    }
}

/// How a forwarded statement changes the session's isolation-level state, as
/// tracked by the proxy for in-transaction cache serving (PGC-387). Produced at
/// cacheability-analysis time (memoized per statement text).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IsolationEffect {
    /// No effect on isolation state.
    #[default]
    None,
    /// Sets the level of the current (or, for `BEGIN`, the starting)
    /// transaction: `BEGIN/START TRANSACTION ... ISOLATION LEVEL`,
    /// `SET TRANSACTION ISOLATION LEVEL`, `SET transaction_isolation`.
    Transaction(IsolationLevel),
    /// Sets the session default: `SET [SESSION] default_transaction_isolation`,
    /// `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL`.
    SessionDefault(IsolationLevel),
    /// The session default is no longer known: `RESET`, `RESET ALL`,
    /// `DISCARD ALL`, `SET ... TO DEFAULT`, `SET LOCAL`, a `set_config()` call,
    /// or a level the parser couldn't read.
    SessionUnknown,
}

/// Everything a forwarded statement does to the proxy's per-connection state,
/// bundled so every forward path (simple query, extended Sync, extended Flush,
/// forward-without-cache-decision) applies it the same way.
#[derive(Debug, Clone, Default)]
pub struct StatementEffects {
    /// Write classification for the read-after-write log (PGC-124); `None` =
    /// provably read-only.
    pub write: Option<WriteClass>,
    /// Effect on the session's isolation-level state (PGC-387).
    pub isolation: IsolationEffect,
    /// Transaction-control boundary, for block-scoped isolation bookkeeping.
    pub transaction: Option<TransactionBoundary>,
}

impl StatementEffects {
    pub fn read_only(isolation: IsolationEffect, transaction: Option<TransactionBoundary>) -> Self {
        Self {
            write: None,
            isolation,
            transaction,
        }
    }

    pub fn write(class: WriteClass, isolation: IsolationEffect) -> Self {
        Self {
            write: Some(class),
            isolation,
            transaction: None,
        }
    }

    /// A statement the proxy could not classify (parse failure, unresolvable
    /// extended-protocol portal): assume a connection-scoped write and a
    /// changed isolation default. Every fallback widens, never misses.
    pub fn unknown() -> Self {
        Self {
            write: Some(WriteClass::Connection),
            isolation: IsolationEffect::SessionUnknown,
            transaction: None,
        }
    }
}

/// A table reference as written in a DML statement. Never schema-resolved —
/// the proxy has no catalog knowledge, so consumers must treat an unqualified
/// name conservatively (same-name ⇒ possibly the same table).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    /// `DELETE` from a single table with an extractable bare-column WHERE
    /// predicate (PGC-381). A read whose predicate is provably disjoint from the
    /// delete's can still be served — a delete only shrinks the result set.
    DeleteRows(Arc<DeleteStatement>),
    /// `UPDATE` of a single table with an extractable bare-column WHERE and SET
    /// list (PGC-382). Served only if the read is disjoint from both the WHERE
    /// (no matched row) and the post-update image (no row grows into the read).
    UpdateRows(Arc<UpdateStatement>),
    /// Target relation known, effect not row-enumerable (MERGE, degraded
    /// INSERT/DELETE/UPDATE forms, COPY FROM, TRUNCATE).
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

/// One bare-column comparison from a DELETE/UPDATE WHERE, normalized to
/// `column op literal` (PGC-381).
pub type WriteComparison = (EcoString, BinaryOp, LiteralValue);

/// An extracted single-table `DELETE` with a bare-column WHERE predicate.
/// `comparisons` are AND-ed conjuncts (single-column equality/range only);
/// a predicate-less DELETE never produces this (it classifies as `Table`).
#[derive(Debug)]
pub struct DeleteStatement {
    pub relation: RelationRef,
    pub comparisons: Vec<WriteComparison>,
}

/// One `SET column = value` assignment. `None` = the new value isn't a literal
/// (subquery, expression, column reference, DEFAULT, or parameter), so the
/// post-update value of the column is unknown (PGC-382).
pub type SetAssignment = (EcoString, Option<LiteralValue>);

/// An extracted single-table `UPDATE` with a bare-column WHERE predicate and a
/// SET list. Both are needed to bound the affected rows and their post-update
/// image; a predicate-less UPDATE never produces this (it classifies as `Table`).
#[derive(Debug)]
pub struct UpdateStatement {
    pub relation: RelationRef,
    pub where_comparisons: Vec<WriteComparison>,
    pub set: Vec<SetAssignment>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_isolation_level_parse() {
        assert_eq!(
            IsolationLevel::parse("read committed"),
            Some(IsolationLevel::ReadCommitted)
        );
        assert_eq!(
            IsolationLevel::parse("READ  Uncommitted"),
            Some(IsolationLevel::ReadCommitted)
        );
        assert_eq!(
            IsolationLevel::parse(" repeatable read "),
            Some(IsolationLevel::RepeatableRead)
        );
        assert_eq!(
            IsolationLevel::parse("Serializable"),
            Some(IsolationLevel::Serializable)
        );
        assert_eq!(IsolationLevel::parse(""), None);
        assert_eq!(IsolationLevel::parse("read"), None);
        assert_eq!(IsolationLevel::parse("serializable read"), None);
        assert_eq!(IsolationLevel::parse("snapshot"), None);
    }
}
