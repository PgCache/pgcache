//! One population task: stream each branch's rows from origin into the
//! relation's staging table in PK-sorted batches (PGC-250, PGC-133).

use std::collections::HashSet;
use std::fmt::Write;
use std::pin::Pin;
use std::sync::Arc;
#[cfg(feature = "fault-injection")]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(feature = "fault-injection")]
use std::time::Duration;
use std::time::Instant;

use ecow::EcoString;
use postgres_protocol::escape;
use postgres_types::PgLsn;
#[cfg(feature = "fault-injection")]
use tokio::time::sleep;
use tokio_postgres::{Client, SimpleColumn, SimpleQueryMessage, SimpleQueryRow, SimpleQueryStream};
use tokio_stream::StreamExt;
use tracing::trace;

use super::{PopulationConnections, PopulationOutcome, PopulationWork};
use crate::cache::{CacheError, CacheResult, MapIntoReport, ReportExt};
use crate::catalog::TableMetadata;
use crate::oid::Oid;
use crate::pg::Lsn;
use crate::query::ast::Deparse;
use crate::query::resolved::{ResolvedSelectNode, ResolvedTableNode};
use crate::query::transform::resolved_select_node_replace;

/// Number of rows to batch per INSERT statement sent to the cache database.
const POPULATION_INSERT_BATCH_SIZE: usize = 200;

/// Test-only population delay (fault-injection feature, PGC-250). Sleeps after
/// the origin snapshot has been read but before the rows are inserted into the
/// cache, so a test can apply a CDC delete / update-out-of-predicate to the
/// just-read rows during the gap and deterministically provoke the
/// population-vs-CDC ordering hazard (ghost rows). Compiled out without the
/// feature.
#[cfg(feature = "fault-injection")]
async fn fault_population_delay() {
    if let Some(ms) = std::env::var("PGCACHE_FAULT_POPULATION_DELAY_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|ms| *ms > 0)
    {
        sleep(Duration::from_millis(ms)).await;
    }

    // One-shot variant: delays only the FIRST population. PGC-260 tests need a
    // long-running guard population (keeps deleted-key tracking alive) while a
    // later population runs undelayed inside the guard's window.
    static DELAY_ONCE: AtomicBool = AtomicBool::new(true);
    if let Some(ms) = std::env::var("PGCACHE_FAULT_POPULATION_DELAY_ONCE_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|ms| *ms > 0)
        && DELAY_ONCE.swap(false, Ordering::Relaxed)
    {
        sleep(Duration::from_millis(ms)).await;
    }
}
#[cfg(not(feature = "fault-injection"))]
async fn fault_population_delay() {}

/// Background task for populating cache with query results.
/// Runs on a dedicated pool connection to avoid session variable conflicts.
///
/// For queries with multiple SELECT branches (set operations), each branch is
/// processed independently. This correctly handles UNION/INTERSECT/EXCEPT where
/// different branches may reference different tables with different columns.
/// Subquery tables are handled as separate branches, so each branch only
/// loads the tables directly in its FROM clause.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(super) async fn population_task(
    work: &PopulationWork,
    connections: &PopulationConnections,
) -> CacheResult<PopulationOutcome> {
    // Generation stamping no longer happens here — it moves to the writer's
    // merge, which inserts the staged rows into the tracked shared table
    // (PGC-250).
    let mut cached_bytes: usize = 0;
    let mut row_count: u64 = 0;
    let task_start = Instant::now();

    // Relations whose staging table has been (re)created this attempt: the first
    // branch touching a relation starts it fresh; later branches append.
    // Reset each attempt so a deadlock retry starts clean.
    let mut reset: HashSet<Oid> = HashSet::new();
    let mut staged: Vec<(Oid, EcoString)> = Vec::new();

    for branch in &work.branches {
        for table_node in branch.direct_table_nodes() {
            let table = table_metadata_find(&work.table_metadata, table_node)?;
            let (staging, needs_create) = staging_table_find(&work.staging, table)?;
            let fresh = reset.insert(table.relation_oid);
            if fresh && needs_create {
                staging_table_create(&connections.cache, table, &staging).await?;
            }
            let sql = population_select_build(table, table_node, branch, work.max_limit);
            let (bytes, rows) = population_stream(connections, &sql, table, &staging).await?;
            cached_bytes += bytes;
            row_count += rows;
            if fresh {
                staged.push((table.relation_oid, staging));
            }
        }
    }

    // Capture the snapshot upper-bound LSN after all reads, for the
    // deferred-Ready gate (PGC-250 Slice B).
    let snapshot_lsn = origin_snapshot_lsn(&connections.origin).await?;

    trace!(
        "population complete for query {}, total_time={:?} bytes={cached_bytes} rows={row_count} snapshot_lsn={snapshot_lsn}",
        work.fingerprint,
        task_start.elapsed()
    );
    Ok(PopulationOutcome {
        cached_bytes,
        row_count,
        staged,
        snapshot_lsn,
    })
}

fn table_metadata_find<'a>(
    table_metadata: &'a [TableMetadata],
    table_node: &ResolvedTableNode,
) -> CacheResult<&'a TableMetadata> {
    table_metadata
        .iter()
        .find(|t| t.relation_oid == table_node.relation_oid)
        .ok_or_else(|| {
            CacheError::UnknownTable {
                oid: Some(table_node.relation_oid),
                name: Some(table_node.name.to_string()),
            }
            .into()
        })
}

/// The staging table checked out for `table` at dispatch (PGC-293), and
/// whether it is a freshly minted slot that must be created.
fn staging_table_find(
    staging_tables: &[(Oid, EcoString, bool)],
    table: &TableMetadata,
) -> CacheResult<(EcoString, bool)> {
    staging_tables
        .iter()
        .find(|(oid, _, _)| *oid == table.relation_oid)
        .map(|(_, name, needs_create)| (name.clone(), *needs_create))
        .ok_or_else(|| {
            CacheError::UnknownTable {
                oid: Some(table.relation_oid),
                name: Some(table.name.to_string()),
            }
            .into()
        })
}

/// Create a freshly minted pool slot on first touch. A reused slot is already
/// an empty table (the writer empties it on check-in — PGC-293; bloat is left
/// to autovacuum), so no DDL runs per population. `IF NOT EXISTS` keeps a
/// deadlock retry from re-creating the slot it made on the first attempt.
async fn staging_table_create(
    db_cache: &Client,
    table: &TableMetadata,
    staging: &str,
) -> CacheResult<()> {
    let create = format!(
        "CREATE UNLOGGED TABLE IF NOT EXISTS pgcache_stage.{staging} (LIKE {}.{})",
        escape::escape_identifier(&table.schema),
        escape::escape_identifier(&table.name)
    );
    db_cache
        .batch_execute(&create)
        .await
        .map_into_report::<CacheError>()
}

/// The origin SELECT for one table of a branch: the branch with its select
/// list replaced by the table's columns, capped at the query's max limit.
fn population_select_build(
    table: &TableMetadata,
    table_node: &ResolvedTableNode,
    branch: &ResolvedSelectNode,
    max_limit: Option<u64>,
) -> String {
    let select_columns = table.resolved_select_columns(table_node.alias.as_deref());
    let new_ast = resolved_select_node_replace(branch, select_columns);
    let mut buf = String::with_capacity(1024);
    new_ast.deparse(&mut buf);
    if let Some(limit) = max_limit {
        write!(buf, " LIMIT {limit}").ok();
    }
    buf
}

/// Origin WAL position as a `u64` byte offset (the same encoding as the
/// replication stream's LSNs), captured after the population reads. Used as the
/// merge-gate snapshot LSN (PGC-272).
///
/// Uses the insert LSN (`pg_current_wal_insert_lsn`): it is always at or past
/// the commit LSN of every committed, read-visible row, so the gate holds the
/// merge until CDC has applied past everything the reads could have seen —
/// correct even under `synchronous_commit=off`, where a read-visible row's WAL
/// may not yet be flushed (a flush-LSN snapshot can fall *below* such a row's
/// commit and release the merge too early — PGC-290).
///
/// The insert pointer can sit ahead of the flushed/streamed position the apply
/// watermark reaches, so on an idle origin this LSN may not be reached by normal
/// stream progress (PGC-289). Reachability is restored on demand by the writer's
/// `origin_flush_force` (`core.rs`), which advances the origin flush pointer past
/// a stuck snapshot only when a merge has been gated past the grace window.
async fn origin_snapshot_lsn(db_origin: &Client) -> CacheResult<Lsn> {
    let row = db_origin
        .query_one("SELECT pg_current_wal_insert_lsn()", &[])
        .await
        .map_into_report::<CacheError>()?;
    Ok(Lsn::from(row.get::<_, PgLsn>(0)))
}

/// Stream one table's rows into its staging table, recording the stream time.
async fn population_stream(
    connections: &PopulationConnections,
    sql: &str,
    table: &TableMetadata,
    staging: &str,
) -> CacheResult<(usize, u64)> {
    let stream_start = Instant::now();
    let (bytes, rows) = staging_rows_load(connections, sql, table, staging).await?;
    let stream_elapsed = stream_start.elapsed();
    crate::metrics::handles()
        .reg
        .population_stream
        .record(stream_elapsed.as_secs_f64());
    trace!(
        "population table {}.{} elapsed={stream_elapsed:?} bytes={bytes} rows={rows}",
        table.schema, table.name
    );
    Ok((bytes, rows))
}

/// Stream one table's rows from origin into its staging table in batches
/// (PGC-250), without materializing the result set in memory. Returns
/// `(cached_bytes, row_count)`.
async fn staging_rows_load(
    connections: &PopulationConnections,
    sql: &str,
    table: &TableMetadata,
    staging: &str,
) -> CacheResult<(usize, u64)> {
    let stream = connections
        .origin
        .simple_query_raw(sql)
        .await
        .map_into_report::<CacheError>()?;
    tokio::pin!(stream);

    let Some(row_description) = row_description_read(stream.as_mut()).await? else {
        return Ok((0, 0));
    };
    let mut batch = StagingBatch::new(
        &connections.cache,
        insert_statement_build(table, &row_description, staging),
    );

    // Snapshot is fixed at query execution (RowDescription received above);
    // rows are not yet in the cache. See PGC-250.
    fault_population_delay().await;

    loop {
        match stream.next().await {
            Some(Ok(SimpleQueryMessage::Row(row))) => batch.row_push(&row).await?,
            Some(Ok(SimpleQueryMessage::CommandComplete(_))) | None => break,
            Some(Ok(_)) => {}
            Some(Err(e)) => return Err(CacheError::from(e).into()),
        }
    }
    batch.finish().await
}

/// Read the stream's leading RowDescription; `None` for an empty stream.
async fn row_description_read(
    mut stream: Pin<&mut SimpleQueryStream>,
) -> CacheResult<Option<Arc<[SimpleColumn]>>> {
    match stream.next().await {
        Some(Ok(SimpleQueryMessage::RowDescription(cols))) => Ok(Some(cols)),
        Some(Ok(_)) => Err(CacheError::InvalidMessage.into()),
        Some(Err(e)) => {
            Err(CacheError::from(e).into()).attach_loc("reading population row description")
        }
        None => Ok(None),
    }
}

/// Pre-computed parts of the batched INSERT...ON CONFLICT statement.
struct InsertStatement {
    prefix: String,
    suffix: String,
    /// Column positions of primary key fields, for detecting NULL-padded phantom rows.
    pkey_positions: Vec<usize>,
    num_columns: usize,
}

/// Build the staging INSERT template from the row description and table metadata.
///
/// Targets the population's per-relation staging table in `pgcache_stage`. No
/// `ON CONFLICT`: staging is a fresh per-population table, and re-stamping
/// pre-existing rows with the query generation happens in the writer's merge,
/// not here (PGC-250). `pkey_positions` is still computed to drop NULL-padded
/// phantom rows from outer joins.
fn insert_statement_build(
    table: &TableMetadata,
    row_description: &Arc<[SimpleColumn]>,
    staging: &str,
) -> InsertStatement {
    let columns: Vec<String> = row_description
        .iter()
        .map(|c| escape::escape_identifier(c.name()))
        .collect();

    let pkey_positions: Vec<usize> = table
        .primary_key_columns
        .iter()
        .filter_map(|pk| row_description.iter().position(|c| c.name() == pk.as_str()))
        .collect();

    let columns_joined = columns.join(",");

    InsertStatement {
        prefix: format!("INSERT INTO pgcache_stage.{staging}({columns_joined}) VALUES "),
        suffix: String::new(),
        pkey_positions,
        num_columns: row_description.len(),
    }
}

/// Rows accumulated for the next multi-row INSERT into a staging table, plus
/// the running totals for the table.
struct StagingBatch<'a> {
    db_cache: &'a Client,
    insert: InsertStatement,
    value_tuples: Vec<(Vec<String>, String)>,
    cached_bytes: usize,
    row_count: u64,
}

impl<'a> StagingBatch<'a> {
    fn new(db_cache: &'a Client, insert: InsertStatement) -> Self {
        Self {
            db_cache,
            insert,
            value_tuples: Vec::with_capacity(POPULATION_INSERT_BATCH_SIZE),
            cached_bytes: 0,
            row_count: 0,
        }
    }

    /// Add a row, flushing once the batch is full. Phantom rows are skipped.
    async fn row_push(&mut self, row: &SimpleQueryRow) -> CacheResult<()> {
        let Some((pk_key, tuple, bytes)) = row_to_tuple(row, &self.insert) else {
            return Ok(());
        };
        self.cached_bytes += bytes;
        self.row_count += 1;
        self.value_tuples.push((pk_key, tuple));
        if self.value_tuples.len() >= POPULATION_INSERT_BATCH_SIZE {
            self.flush().await?;
        }
        Ok(())
    }

    /// Flush the remainder and return `(cached_bytes, row_count)`.
    async fn finish(mut self) -> CacheResult<(usize, u64)> {
        if !self.value_tuples.is_empty() {
            self.flush().await?;
        }
        Ok((self.cached_bytes, self.row_count))
    }

    /// Send the batch as a single multi-row INSERT, then clear it.
    async fn flush(&mut self) -> CacheResult<()> {
        let sql = batch_sql_build(
            &self.insert.prefix,
            &self.insert.suffix,
            &mut self.value_tuples,
        );
        self.db_cache
            .batch_execute(&sql)
            .await
            .map_into_report::<CacheError>()?;
        self.value_tuples.clear();
        Ok(())
    }
}

/// Convert a streamed row into `(pk_key, tuple_string, row_byte_count)`.
///
/// Returns `None` for phantom rows (NULL primary keys from outer joins).
/// `pk_key` is the escaped primary-key values, used to order rows within a batch.
fn row_to_tuple(
    row: &SimpleQueryRow,
    insert: &InsertStatement,
) -> Option<(Vec<String>, String, usize)> {
    // Skip NULL-padded phantom rows from outer joins
    if insert
        .pkey_positions
        .iter()
        .any(|&pos| row.get(pos).is_none())
    {
        return None;
    }
    Some(escaped_tuple_build(
        insert.num_columns,
        &insert.pkey_positions,
        |idx| row.get(idx),
    ))
}

/// Build the `(pk_key, tuple_string, row_byte_count)` for one row, escaping each
/// value directly into the tuple buffer — no per-column `Vec<String>` and no
/// `join` temporary (PGC-347). `get(idx)` returns the column value or `None` for
/// SQL NULL. Split from [`row_to_tuple`] so it can be unit-tested without a live
/// `SimpleQueryRow`.
fn escaped_tuple_build<'a>(
    num_columns: usize,
    pkey_positions: &[usize],
    get: impl Fn(usize) -> Option<&'a str>,
) -> (Vec<String>, String, usize) {
    // Escaped PK values in conflict-column order. PK identity is snapshot-stable,
    // so this key is identical across workers even when non-PK columns drift
    // between MVCC snapshots; sorting on it (PGC-133) avoids the PK-index deadlock.
    // PKs are non-NULL here (phantom rows already skipped by the caller).
    let pk_key: Vec<String> = pkey_positions
        .iter()
        .map(|&pos| get(pos).map(literal_escape).unwrap_or_default())
        .collect();

    // Pre-pass to size the tuple buffer exactly once (no growth reallocs).
    let row_bytes: usize = (0..num_columns)
        .map(|idx| get(idx).map_or(0, str::len))
        .sum();
    // Slack for escaping (doubled quotes/backslashes), separators, and parens.
    let mut tuple = String::with_capacity(row_bytes + row_bytes / 4 + num_columns + 2);
    tuple.push('(');
    for idx in 0..num_columns {
        if idx > 0 {
            tuple.push(',');
        }
        // Reuse the already-escaped PK value instead of escaping it a second time.
        match pk_escaped_get(&pk_key, pkey_positions, idx) {
            Some(escaped) => tuple.push_str(escaped),
            None => value_escape_into(get(idx), &mut tuple),
        }
    }
    tuple.push(')');

    (pk_key, tuple, row_bytes)
}

fn literal_escape(value: &str) -> String {
    let mut escaped = String::new();
    let _ = escape::escape_literal_into(value, &mut escaped);
    escaped
}

/// The escaped PK value for column `idx`, if it is a PK column.
fn pk_escaped_get<'k>(
    pk_key: &'k [String],
    pkey_positions: &[usize],
    idx: usize,
) -> Option<&'k str> {
    pkey_positions
        .iter()
        .position(|&pos| pos == idx)
        .and_then(|i| pk_key.get(i))
        .map(String::as_str)
}

/// Append a column value as a SQL literal, or `NULL`.
fn value_escape_into(value: Option<&str>, out: &mut String) {
    match value {
        Some(value) => {
            let _ = escape::escape_literal_into(value, out);
        }
        None => out.push_str("NULL"),
    }
}

/// Sort rows by primary key, then assemble the multi-row INSERT statement.
///
/// Sorting every batch (including the final partial one) by PK keeps the
/// PK-index lock acquisition order consistent across concurrent populations,
/// which is what prevents the deadlock in PGC-133. Each flush autocommits, so
/// a consistent intra-batch order is sufficient.
fn batch_sql_build(
    insert_prefix: &str,
    insert_suffix: &str,
    value_tuples: &mut [(Vec<String>, String)],
) -> String {
    value_tuples.sort_unstable_by(|a, b| a.0.cmp(&b.0));

    let mut sql = String::with_capacity(
        insert_prefix.len()
            + insert_suffix.len()
            + value_tuples.iter().map(|(_, t)| t.len() + 1).sum::<usize>(),
    );
    sql.push_str(insert_prefix);
    for (i, (_, tuple)) in value_tuples.iter().enumerate() {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(tuple);
    }
    sql.push_str(insert_suffix);
    sql
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two populations that stream the same rows in different orders must emit
    /// byte-identical INSERT bodies so PG locks the PK index in the same order.
    #[test]
    fn test_batch_sql_build_orders_by_pk() {
        let prefix = "INSERT INTO t(a,b) VALUES ";
        let suffix = " ON CONFLICT (a) DO NOTHING";

        let mut worker_a = vec![
            (vec!["1".to_owned()], "(1,'x')".to_owned()),
            (vec!["3".to_owned()], "(3,'y')".to_owned()),
            (vec!["2".to_owned()], "(2,'z')".to_owned()),
        ];
        // Same rows, different stream order, and a non-PK column that drifted
        // between MVCC snapshots — the PK key must still drive the ordering.
        let mut worker_b = vec![
            (vec!["2".to_owned()], "(2,'DRIFTED')".to_owned()),
            (vec!["1".to_owned()], "(1,'x')".to_owned()),
            (vec!["3".to_owned()], "(3,'y')".to_owned()),
        ];

        let sql_a = batch_sql_build(prefix, suffix, &mut worker_a);
        let sql_b = batch_sql_build(prefix, suffix, &mut worker_b);

        assert_eq!(
            sql_a,
            "INSERT INTO t(a,b) VALUES (1,'x'),(2,'z'),(3,'y') ON CONFLICT (a) DO NOTHING"
        );
        // PK order is identical across workers; only the drifted non-PK value
        // differs, never the row sequence.
        assert_eq!(
            sql_b,
            "INSERT INTO t(a,b) VALUES (1,'x'),(2,'DRIFTED'),(3,'y') ON CONFLICT (a) DO NOTHING"
        );
    }

    #[test]
    fn test_batch_sql_build_composite_pk_no_separator_ambiguity() {
        // ("a","b") vs ("ab", "") must not collide into the same sort key.
        let mut rows = vec![
            (
                vec!["'ab'".to_owned(), "''".to_owned()],
                "('ab','')".to_owned(),
            ),
            (
                vec!["'a'".to_owned(), "'b'".to_owned()],
                "('a','b')".to_owned(),
            ),
        ];
        let sql = batch_sql_build("", "", &mut rows);
        assert_eq!(sql, "('a','b'),('ab','')");
    }

    /// The prior implementation, reproduced as an oracle: escape every column
    /// into a `Vec<String>`, then `(` + join(",") + `)`. PGC-347's zero-alloc
    /// `escaped_tuple_build` must match it byte-for-byte.
    fn oracle(values: &[Option<&str>], pkey_positions: &[usize]) -> (Vec<String>, String) {
        let escaped: Vec<String> = values
            .iter()
            .map(|v| {
                v.map(escape::escape_literal)
                    .unwrap_or_else(|| "NULL".to_owned())
            })
            .collect();
        let pk: Vec<String> = pkey_positions
            .iter()
            .filter_map(|&pos| escaped.get(pos).cloned())
            .collect();
        (pk, format!("({})", escaped.join(",")))
    }

    #[test]
    fn test_escaped_tuple_build_matches_prior_output() {
        let cases: &[&[Option<&str>]] = &[
            &[Some("1"), Some("x")],
            &[Some("42"), None, Some("o'brien")], // NULL + quote-escaping
            &[Some("a\\b"), Some("both ' and \\")], // backslash → E'…'
            &[Some(""), Some("''''")],            // empty + only quotes
            &[Some("café ☕"), None],             // multibyte + NULL
        ];
        for &values in cases {
            let pkey_positions = [0usize];
            let (pk, tuple, row_bytes) =
                escaped_tuple_build(values.len(), &pkey_positions, |idx| values[idx]);
            let (want_pk, want_tuple) = oracle(values, &pkey_positions);
            assert_eq!(tuple, want_tuple, "tuple mismatch for {values:?}");
            assert_eq!(pk, want_pk, "pk_key mismatch for {values:?}");
            let want_bytes: usize = values.iter().map(|v| v.map_or(0, str::len)).sum();
            assert_eq!(row_bytes, want_bytes, "row_bytes mismatch for {values:?}");
        }
    }

    #[test]
    fn test_escaped_tuple_build_composite_pk() {
        let values = [Some("ab"), Some("cd"), Some("ef")];
        let (pk, tuple, _) = escaped_tuple_build(3, &[0, 2], |idx| values[idx]);
        assert_eq!(tuple, "('ab','cd','ef')");
        assert_eq!(pk, vec!["'ab'".to_owned(), "'ef'".to_owned()]);
    }
}
