//! Database drivers for the two endpoints under test.
//!
//! Origin is the oracle: every statement runs there first and its
//! result is the expected value. pgcache runs the same statement twice
//! (populate, then a cache-hit attempt) and its result is compared
//! against origin using standard sqllogictest sort/hash semantics.

use anyhow::{Context, Result, anyhow};
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

use crate::cdc_settling::lsn_parse;

/// A Postgres connection to one endpoint under test. `label` ("origin"
/// or "pgcache") only flavors diagnostics — origin and pgcache differ
/// solely in role, not in protocol handling.
pub struct SqlDriver {
    client: Client,
    label: &'static str,
}

impl SqlDriver {
    pub async fn connect(conn_str: &str, label: &'static str) -> Result<Self> {
        let (client, connection) = tokio_postgres::connect(conn_str, NoTls)
            .await
            .with_context(|| format!("connecting to {label}"))?;
        tokio::spawn(async move {
            // Ends with an error at teardown (pgcache killed / temp DBs
            // dropped) — expected. A real mid-run failure surfaces via the
            // failing query/ping, so this is debug-only noise otherwise.
            if let Err(e) = connection.await {
                tracing::debug!(label, error = %e, "connection task ended");
            }
        });
        Ok(Self { client, label })
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Verify the connection is live.
    pub async fn ping(&self) -> Result<()> {
        self.client
            .simple_query("SELECT 1")
            .await
            .with_context(|| format!("{} preflight `SELECT 1`", self.label))?;
        Ok(())
    }

    pub async fn run(&self, sql: &str) -> RunOutcome {
        run_simple(&self.client, sql).await
    }

    /// Current WAL write position, used as the CDC settling watermark
    /// (origin only — pgcache has no independent WAL).
    pub async fn current_wal_lsn(&self) -> Result<u64> {
        let msgs = self
            .client
            .simple_query("SELECT pg_current_wal_lsn()")
            .await
            .context("querying pg_current_wal_lsn()")?;
        for msg in msgs {
            if let SimpleQueryMessage::Row(r) = msg {
                let raw = r
                    .try_get(0)
                    .ok()
                    .flatten()
                    .ok_or_else(|| anyhow!("pg_current_wal_lsn() returned NULL"))?;
                return lsn_parse(raw);
            }
        }
        Err(anyhow!("pg_current_wal_lsn() returned no rows"))
    }
}

/// NULL renders as the literal `NULL`, matching sqllogictest convention
/// and keeping origin/pgcache text representations comparable.
const NULL: &str = "NULL";

/// Execute one SQL string via the simple query protocol and normalize
/// the outcome.
///
/// The simple protocol returns every column as text, so origin and
/// pgcache results are directly comparable without per-type decoding.
/// Errors are reduced to their SQLSTATE when available — comparing full
/// message text across the two engines is too brittle, and SQLSTATE is
/// the stable contract.
pub async fn run_simple(client: &Client, sql: &str) -> RunOutcome {
    let messages = match client.simple_query(sql).await {
        Ok(m) => m,
        Err(e) => {
            let tag = e
                .code()
                .map(|c| c.code().to_string())
                .unwrap_or_else(|| e.to_string());
            return RunOutcome::Error(tag);
        }
    };

    let mut rows: Vec<Vec<String>> = Vec::new();
    let mut column_count = 0usize;
    let mut is_query = false;
    let mut rows_affected = 0u64;

    for msg in messages {
        match msg {
            SimpleQueryMessage::RowDescription(cols) => {
                is_query = true;
                column_count = cols.len();
            }
            SimpleQueryMessage::Row(r) => {
                is_query = true;
                column_count = r.len();
                let mut row = Vec::with_capacity(r.len());
                for i in 0..r.len() {
                    let v = r.try_get(i).ok().flatten().unwrap_or(NULL);
                    row.push(v.to_string());
                }
                rows.push(row);
            }
            SimpleQueryMessage::CommandComplete(n) => rows_affected = n,
            _ => {}
        }
    }

    if is_query {
        RunOutcome::Query(QueryResult { column_count, rows })
    } else {
        RunOutcome::Statement { rows_affected }
    }
}

/// A normalized result set for cross-engine comparison.
///
/// Rows are stringified per sqllogictest conventions so origin and
/// pgcache outputs can be diffed regardless of wire representation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryResult {
    pub column_count: usize,
    pub rows: Vec<Vec<String>>,
}

/// Outcome of running a single SQL string against an endpoint.
#[derive(Debug, Clone)]
pub enum RunOutcome {
    /// A statement (no result set) completed; carries rows affected.
    Statement { rows_affected: u64 },
    /// A query returned a result set.
    Query(QueryResult),
    /// The engine returned an error (message captured for comparison).
    Error(String),
}
