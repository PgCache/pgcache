use std::fmt::Write;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use postgres_protocol::escape;
use rootcause::prelude::ResultExt;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio_postgres::{Client, SimpleColumn, SimpleQueryMessage, SimpleQueryRow};
use tokio_stream::StreamExt;
use tracing::{debug, error, trace};

use crate::catalog::TableMetadata;
use crate::metrics::names;
use crate::query::ast::Deparse;
use crate::query::resolved::{ResolvedSelectNode, ResolvedTableNode};
use crate::query::transform::resolved_select_node_replace;

use super::super::{CacheError, CacheResult, MapIntoReport, messages::QueryCommand};
use super::PopulationWork;

/// Number of rows to batch per INSERT statement sent to the cache database.
const POPULATION_INSERT_BATCH_SIZE: usize = 200;

/// Persistent population worker that processes work items from a channel.
/// Each worker owns its own cache database connection.
pub async fn population_worker(
    id: usize,
    mut rx: UnboundedReceiver<PopulationWork>,
    db_origin: Rc<Client>,
    db_cache: Client,
    query_tx: UnboundedSender<QueryCommand>,
) {
    debug!("population worker {id} started");

    let mut idle_start = Instant::now();
    while let Some(work) = rx.recv().await {
        // Time spent waiting on rx — recorded as a histogram so the `_sum`
        // gives cumulative idle time per worker (utilization signal) and the
        // quantiles surface variance. Pairs with task_seconds and wall clock
        // to compute per-worker utilization.
        metrics::histogram!(names::CACHE_POPULATION_WORKER_IDLE_SECONDS, "worker" => id.to_string())
            .record(idle_start.elapsed().as_secs_f64());

        // Channel depth gauge; queue length never approaches 2^53.
        #[allow(clippy::cast_precision_loss)]
        metrics::gauge!(names::CACHE_POPULATION_WORKER_QUEUE, "worker" => id.to_string())
            .set(rx.len() as f64);

        metrics::histogram!(names::CACHE_POPULATION_WAIT_SECONDS)
            .record(work.enqueued_at.elapsed().as_secs_f64());

        let task_start = Instant::now();
        let result = population_task(
            work.fingerprint,
            work.generation,
            &work.branches,
            &work.table_metadata,
            work.max_limit,
            Rc::clone(&db_origin),
            &db_cache,
        )
        .await;
        metrics::histogram!(names::CACHE_POPULATION_TASK_SECONDS)
            .record(task_start.elapsed().as_secs_f64());

        idle_start = Instant::now();

        match result {
            Ok((cached_bytes, row_count)) => {
                if query_tx
                    .send(QueryCommand::Ready {
                        fingerprint: work.fingerprint,
                        cached_bytes,
                        row_count,
                    })
                    .is_err()
                {
                    error!("population worker {id}: failed to send QueryReady");
                }
            }
            Err(e) => {
                // Log the bare SQLSTATE, not the error chain: the chain walker
                // leaks the offending SQL via the PG DETAIL field (PGC-133).
                let sqlstate = if let CacheError::PgError(pg) = e.current_context() {
                    pg.code().map(|c| c.code())
                } else {
                    None
                };
                error!(
                    "population worker {id}: population failed for query {} sqlstate={}: {e}",
                    work.fingerprint,
                    sqlstate.unwrap_or("-"),
                );
                if query_tx
                    .send(QueryCommand::Failed {
                        fingerprint: work.fingerprint,
                    })
                    .is_err()
                {
                    error!("population worker {id}: failed to send QueryFailed");
                }
            }
        }
    }

    debug!("population worker {id} shutting down");
}

/// Background task for populating cache with query results.
/// Runs on a dedicated pool connection to avoid session variable conflicts.
///
/// For queries with multiple SELECT branches (set operations), each branch is
/// processed independently. This correctly handles UNION/INTERSECT/EXCEPT where
/// different branches may reference different tables with different columns.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn population_task(
    fingerprint: u64,
    generation: u64,
    branches: &[ResolvedSelectNode],
    table_metadata: &[TableMetadata],
    max_limit: Option<u64>,
    db_origin: Rc<Client>,
    db_cache: &Client,
) -> CacheResult<(usize, u64)> {
    // Set generation for tracking triggers. Simple Query (one round-trip)
    // rather than `execute` (Extended Query, Parse+Bind+Execute+Sync) — SET
    // doesn't benefit from prepare, and the single-round-trip path more
    // honestly reflects the trivial work being done.
    let set_generation_sql = format!("SET mem.query_generation = {generation}");
    db_cache
        .simple_query(&set_generation_sql)
        .await
        .map_into_report::<CacheError>()?;

    let mut total_bytes: usize = 0;
    let mut total_rows: u64 = 0;
    let task_start = Instant::now();

    // Process each SELECT branch independently
    // For simple SELECT queries, there's just one branch
    // For set operations, each branch fetches its own tables
    for branch in branches {
        // Find tables directly in this branch's FROM clause (not in subqueries).
        // Subquery tables are handled as separate branches.
        for table_node in branch.direct_table_nodes() {
            let table = table_metadata
                .iter()
                .find(|t| t.relation_oid == table_node.relation_oid)
                .ok_or(CacheError::UnknownTable {
                    oid: Some(table_node.relation_oid),
                    name: Some(table_node.name.to_string()),
                })?;

            let stream_start = Instant::now();
            let (bytes, rows) =
                population_stream(&db_origin, db_cache, table, table_node, branch, max_limit)
                    .await?;
            let stream_elapsed = stream_start.elapsed();
            metrics::histogram!(names::CACHE_POPULATION_STREAM_SECONDS)
                .record(stream_elapsed.as_secs_f64());

            total_bytes += bytes;
            total_rows += rows;

            trace!(
                "population table {}.{} elapsed={:?} bytes={bytes} rows={rows}",
                table.schema, table.name, stream_elapsed
            );
        }
    }

    let task_elapsed = task_start.elapsed();

    // Reset generation — same Simple Query rationale as the forward SET.
    db_cache
        .simple_query("SET mem.query_generation = 0")
        .await
        .map_into_report::<CacheError>()?;

    trace!(
        "population complete for query {fingerprint}, total_time={:?} bytes={total_bytes} rows={total_rows}",
        task_elapsed
    );
    Ok((total_bytes, total_rows))
}

/// Pre-computed parts of the batched INSERT...ON CONFLICT statement.
struct InsertStatement {
    prefix: String,
    suffix: String,
    /// Column positions of primary key fields, for detecting NULL-padded phantom rows.
    pkey_positions: Vec<usize>,
    num_columns: usize,
}

/// Build the INSERT statement template from the row description and table metadata.
///
/// Pre-computes column lists, conflict clause, and primary key positions so that
/// the streaming loop only needs to format value tuples.
fn insert_statement_build(
    table: &TableMetadata,
    row_description: &Arc<[SimpleColumn]>,
) -> InsertStatement {
    let columns: Vec<String> = row_description
        .iter()
        .map(|c| format!("\"{}\"", c.name()))
        .collect();

    let pkey_columns: Vec<String> = table
        .primary_key_columns
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect();

    let pkey_positions: Vec<usize> = table
        .primary_key_columns
        .iter()
        .filter_map(|pk| row_description.iter().position(|c| c.name() == pk.as_str()))
        .collect();

    let columns_joined = columns.join(",");
    let pkey_joined = pkey_columns.join(",");

    // On conflict we don't overwrite existing rows: CDC keeps their values
    // consistent with origin, so re-writing non-PK columns from EXCLUDED would
    // be wasted work. The UPSERT's only job here is to fire
    // `pgcache_track_modification` (an AFTER UPDATE trigger) so pre-existing
    // rows get linked to this population's query generation. Writing each PK
    // column to its own EXCLUDED value is a data no-op (the arbiter guarantees
    // they're equal) that still fires the trigger; `DO NOTHING` would skip it
    // and leave the row unlinked from this generation. Unqualified `<pk> =
    // <pk>` on the RHS is ambiguous per PG 42702 — EXCLUDED qualifies it.
    let self_assign = pkey_columns
        .iter()
        .map(|c| format!("{c} = EXCLUDED.{c}"))
        .collect::<Vec<_>>()
        .join(", ");
    let suffix = format!(" ON CONFLICT ({pkey_joined}) DO UPDATE SET {self_assign}");

    InsertStatement {
        prefix: format!(
            "INSERT INTO \"{}\".\"{}\"({columns_joined}) VALUES ",
            table.schema, table.name
        ),
        suffix,
        pkey_positions,
        num_columns: row_description.len(),
    }
}

/// Convert a streamed row into `(pk_key, tuple_string, row_byte_count)`.
///
/// Returns `None` for phantom rows (NULL primary keys from outer joins).
/// `pk_key` is the escaped primary-key values, used to order rows within a batch.
fn row_to_tuple(
    row: &SimpleQueryRow,
    insert: &InsertStatement,
    values_buf: &mut Vec<String>,
    tuple_buf: &mut String,
) -> Option<(Vec<String>, String, usize)> {
    // Skip NULL-padded phantom rows from outer joins
    if insert
        .pkey_positions
        .iter()
        .any(|&pos| row.get(pos).is_none())
    {
        return None;
    }

    let mut row_bytes = 0;
    values_buf.clear();
    for idx in 0..insert.num_columns {
        let value = row.get(idx);
        row_bytes += value.map_or(0, |v| v.len());
        values_buf.push(
            value
                .map(escape::escape_literal)
                .unwrap_or_else(|| "NULL".to_owned()),
        );
    }

    // Escaped PK values in conflict-column order. PK identity is snapshot-stable,
    // so this key is identical across workers even when non-PK columns drift
    // between MVCC snapshots; sorting on it (PGC-133) avoids the PK-index deadlock.
    let pk_key: Vec<String> = insert
        .pkey_positions
        .iter()
        .filter_map(|&pos| values_buf.get(pos).cloned())
        .collect();

    tuple_buf.clear();
    tuple_buf.push('(');
    tuple_buf.push_str(&values_buf.join(","));
    tuple_buf.push(')');

    Some((pk_key, tuple_buf.clone(), row_bytes))
}

/// Fetch data from origin and stream it into the cache database in batches.
///
/// Streams rows from origin via SimpleQueryStream, batching INSERT...ON CONFLICT
/// statements in groups of POPULATION_INSERT_BATCH_SIZE rows. This avoids materializing
/// the entire result set in memory.
/// Returns `(cached_bytes, row_count)`.
async fn population_stream(
    db_origin: &Client,
    db_cache: &Client,
    table: &TableMetadata,
    table_node: &ResolvedTableNode,
    branch: &ResolvedSelectNode,
    max_limit: Option<u64>,
) -> CacheResult<(usize, u64)> {
    // Build the SELECT query
    let select_columns = table.resolved_select_columns(table_node.alias.as_deref());
    let new_ast = resolved_select_node_replace(branch, select_columns);
    let mut buf = String::with_capacity(1024);
    new_ast.deparse(&mut buf);

    if let Some(limit) = max_limit {
        write!(buf, " LIMIT {limit}").ok();
    }

    // Start streaming from origin
    let stream = db_origin
        .simple_query_raw(&buf)
        .await
        .map_into_report::<CacheError>()?;
    tokio::pin!(stream);

    // Extract RowDescription (first item from stream)
    let row_description = match stream.next().await {
        Some(Ok(SimpleQueryMessage::RowDescription(cols))) => cols,
        Some(Ok(_)) => return Err(CacheError::InvalidMessage.into()),
        Some(Err(e)) => {
            let report: CacheResult<(usize, u64)> = Err(CacheError::from(e).into());
            return report.attach(buf);
        }
        None => return Ok((0, 0)),
    };

    let insert = insert_statement_build(table, &row_description);

    let mut cached_bytes: usize = 0;
    let mut row_count: u64 = 0;
    let mut value_tuples: Vec<(Vec<String>, String)> =
        Vec::with_capacity(POPULATION_INSERT_BATCH_SIZE);
    let mut values_buf: Vec<String> = Vec::with_capacity(insert.num_columns);
    let mut tuple_buf = String::new();

    loop {
        match stream.next().await {
            Some(Ok(SimpleQueryMessage::Row(row))) => {
                if let Some((pk_key, tuple, bytes)) =
                    row_to_tuple(&row, &insert, &mut values_buf, &mut tuple_buf)
                {
                    cached_bytes += bytes;
                    row_count += 1;
                    value_tuples.push((pk_key, tuple));

                    if value_tuples.len() >= POPULATION_INSERT_BATCH_SIZE {
                        population_batch_flush(
                            db_cache,
                            &insert.prefix,
                            &insert.suffix,
                            &mut value_tuples,
                        )
                        .await?;
                    }
                }
            }
            Some(Ok(SimpleQueryMessage::CommandComplete(_))) => break,
            Some(Ok(_)) => continue,
            Some(Err(e)) => return Err(CacheError::from(e).into()),
            None => break,
        }
    }

    if !value_tuples.is_empty() {
        population_batch_flush(db_cache, &insert.prefix, &insert.suffix, &mut value_tuples).await?;
    }

    Ok((cached_bytes, row_count))
}

/// Flush a batch of value tuples as a single multi-row INSERT, then clear it.
async fn population_batch_flush(
    db_cache: &Client,
    insert_prefix: &str,
    insert_suffix: &str,
    value_tuples: &mut Vec<(Vec<String>, String)>,
) -> CacheResult<()> {
    let sql = batch_sql_build(insert_prefix, insert_suffix, value_tuples);

    db_cache
        .batch_execute(&sql)
        .await
        .map_into_report::<CacheError>()?;

    value_tuples.clear();
    Ok(())
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
    fn batch_sql_build_orders_by_pk() {
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
    fn batch_sql_build_composite_pk_no_separator_ambiguity() {
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
}
