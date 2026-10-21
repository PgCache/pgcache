//! One population task: stream each branch's rows from origin into the
//! relation's staging table with COPY (PGC-250).

use std::collections::HashSet;
use std::fmt::Write;
use std::sync::Arc;
#[cfg(feature = "fault-injection")]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(feature = "fault-injection")]
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use ecow::EcoString;
use futures_util::SinkExt;
use postgres_protocol::escape;
use postgres_types::PgLsn;
#[cfg(feature = "fault-injection")]
use tokio::time::sleep;
use tokio_postgres::Client;
use tokio_stream::StreamExt;
use tracing::trace;

use super::{PopulationConnections, PopulationOutcome, PopulationWork};
use crate::cache::{CacheError, CacheResult, MapIntoReport, ReportExt};
use crate::catalog::TableMetadata;
use crate::oid::Oid;
use crate::pg::{Lsn, OriginSnapshot};
use crate::query::ast::Clause;
use crate::query::ast::rope::deparse::{from_deparse_with, where_clause_deparse_with};
use crate::query::ast::rope::{Select, Table};
use crate::query::resolve::ResolvedQuery;
use crate::query::update::select_branches_with_source;

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

    // Taken before the first read, so every transaction it sees is visible to
    // each per-table read that follows.
    let snapshot = OriginSnapshot::current(&connections.origin)
        .await
        .map(Arc::new)
        .map_into_report::<CacheError>()
        .attach_loc("reading the population's origin snapshot")?;

    for PopulationRead { table_node, select } in population_reads(&work.population, work.max_limit)
    {
        let table = table_metadata_find(&work.table_metadata, &work.population, table_node)?;
        let (staging, needs_create) = staging_table_find(&work.staging, table)?;
        let fresh = reset.insert(table.relation_oid);
        if fresh && needs_create {
            staging_table_create(&connections.cache, table, &staging).await?;
        }
        let copy_out = population_copy_out_sql(&select, table);
        let copy_in = staging_copy_in_sql(&work.population, table_node, &staging);
        let (bytes, rows) = population_stream(connections, &copy_out, &copy_in, table).await?;
        cached_bytes += bytes;
        row_count += rows;
        if fresh {
            staged.push((table.relation_oid, staging));
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
        snapshot,
        snapshot_lsn,
    })
}

/// One origin read of a population: a table of a branch and its select.
struct PopulationRead<'a> {
    table_node: Table<'a>,
    select: String,
}

/// Every (branch, table) read of the population in order, skipping a read
/// another already issued. Each reference to a CTE carries its own copy of
/// the body, and all reads share one origin snapshot.
fn population_reads(population: &ResolvedQuery, max_limit: Option<u64>) -> Vec<PopulationRead<'_>> {
    let root = population.select();
    let mut issued = HashSet::new();
    let mut reads = Vec::new();
    for (branch, _) in select_branches_with_source(population.tree().root()) {
        // A LIMIT bounds the result's rows, never a subquery's.
        let limit = max_limit.filter(|_| root.is_some_and(|root| root.id() == branch.id()));
        for table_node in branch.direct_tables() {
            let select = population_select_build(population, table_node, branch, limit);
            if issued.insert((table_relation_oid(population, table_node), select.clone())) {
                reads.push(PopulationRead { table_node, select });
            }
        }
    }
    reads
}

fn table_relation_oid(population: &ResolvedQuery, table_node: Table<'_>) -> Option<Oid> {
    let bindings = population.bindings();
    bindings
        .source(table_node.id())
        .and_then(|slot| bindings.slot(slot))
        .map(|slot| slot.table().relation_oid)
}

fn table_metadata_find<'a>(
    table_metadata: &'a [Arc<TableMetadata>],
    population: &ResolvedQuery,
    table_node: Table<'_>,
) -> CacheResult<&'a TableMetadata> {
    let relation_oid = table_relation_oid(population, table_node);
    relation_oid
        .and_then(|oid| {
            table_metadata
                .iter()
                .map(|t| &**t)
                .find(|t| t.relation_oid == oid)
        })
        .ok_or_else(|| {
            CacheError::UnknownTable {
                oid: relation_oid,
                name: Some(table_node.name().to_string()),
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

/// The origin SELECT for one table of a branch: the branch's FROM and WHERE
/// with the table's columns as the select list. DISTINCT folds join fan-out;
/// GROUP BY and HAVING are dropped because the cache holds raw rows and
/// aggregates at retrieval.
///
/// `limit` is the root's cap, which only a query reading one table with an
/// ORDER BY it can render has (`population_max_limit`): the read is sorted as
/// the query is, so it loads the query's top rows, and one table has no
/// fan-out to fold.
fn population_select_build(
    population: &ResolvedQuery,
    table_node: Table<'_>,
    branch: Select<'_>,
    limit: Option<u64>,
) -> String {
    let single_table = branch.from().len() == 1 && branch.direct_tables().len() == 1;
    debug_assert!(
        limit.is_none() || single_table,
        "a capped population reads one table"
    );
    let window = limit.filter(|_| single_table).and_then(|limit| {
        let order_by = population_order_by(population, branch);
        debug_assert!(order_by.is_some(), "a capped population renders its sort");
        Some((order_by?, limit))
    });
    let mut buf = String::with_capacity(1024);
    let mut cx = population.deparse_context();
    buf.push_str(if window.is_some() {
        "SELECT"
    } else {
        "SELECT DISTINCT"
    });
    population.source_columns_into(table_node.id(), &mut buf);
    from_deparse_with(branch.from(), &mut cx, &mut buf);
    where_clause_deparse_with(
        branch.where_clause(),
        Clause::Where,
        " WHERE ",
        &mut cx,
        &mut buf,
    );
    if let Some((order_by, limit)) = window {
        buf.push_str(&order_by);
        write!(buf, " LIMIT {limit}").ok();
    }
    buf
}

/// The query's ORDER BY over `select`'s FROM, empty when it has none; `None`
/// when an item names no output.
fn population_order_by(population: &ResolvedQuery, select: Select<'_>) -> Option<String> {
    let mut buf = String::new();
    let mut separator = " ORDER BY ";
    for clause in population.tree().root().order_by() {
        buf.push_str(separator);
        if !population.order_by_source_into(select, clause, &mut buf) {
            return None;
        }
        separator = ", ";
    }
    Some(buf)
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

/// Wrap a population SELECT in the origin COPY that streams it, dropping
/// NULL-padded phantom rows from outer joins: a cached table's PK is never NULL,
/// and staging inherits its NOT NULL constraints. The filter sits outside the
/// SELECT so it applies after the LIMIT, as row skipping did.
fn population_copy_out_sql(select: &str, table: &TableMetadata) -> String {
    debug_assert!(
        !table.primary_key_columns.is_empty(),
        "populated table without a primary key"
    );
    let mut sql = format!("COPY (SELECT * FROM ({select}) population");
    let mut keyword = " WHERE ";
    for pk in &table.primary_key_columns {
        write!(
            sql,
            "{keyword}population.{} IS NOT NULL",
            escape::escape_identifier(pk)
        )
        .ok();
        keyword = " AND ";
    }
    sql.push_str(") TO STDOUT");
    sql
}

/// The cache COPY that loads a table's population rows into its staging table,
/// naming the columns in the population SELECT's order.
fn staging_copy_in_sql(population: &ResolvedQuery, table_node: Table<'_>, staging: &str) -> String {
    let mut sql = format!("COPY pgcache_stage.{staging} (");
    population.source_column_names_into(table_node.id(), &mut sql);
    sql.push_str(") FROM STDIN");
    sql
}

/// Stream one table's rows into its staging table, recording the stream time.
async fn population_stream(
    connections: &PopulationConnections,
    copy_out: &str,
    copy_in: &str,
    table: &TableMetadata,
) -> CacheResult<(usize, u64)> {
    let stream_start = Instant::now();
    let (bytes, rows) = staging_rows_load(connections, copy_out, copy_in).await?;
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

/// Pipe one table's rows from origin into its staging table: origin's COPY text
/// is forwarded to the cache's COPY unread, so no row is parsed, escaped or
/// rendered as SQL here. Both sessions are pinned to the reference settings,
/// so the text one renders is the text the other parses. Returns
/// `(copied_bytes, row_count)`.
async fn staging_rows_load(
    connections: &PopulationConnections,
    copy_out: &str,
    copy_in: &str,
) -> CacheResult<(usize, u64)> {
    let source = connections
        .origin
        .copy_out_simple(copy_out)
        .await
        .map_into_report::<CacheError>()
        .attach_loc("starting the population COPY from origin")?;
    tokio::pin!(source);

    // The origin statement has started, so its snapshot is fixed; rows are not
    // yet in staging. See PGC-250.
    fault_population_delay().await;

    // Extended-protocol COPY: `copy_in_simple` ends with the Sync the extended
    // protocol needs, and the stray ReadyForQuery it draws desyncs the session.
    let sink = connections
        .cache
        .copy_in::<_, Bytes>(copy_in)
        .await
        .map_into_report::<CacheError>()
        .attach_loc("starting the staging COPY on the cache")?;
    tokio::pin!(sink);

    // An error return drops the sink unfinished, which aborts the staging COPY.
    let mut copied_bytes = 0;
    while let Some(chunk) = source.next().await {
        let chunk = chunk.map_into_report::<CacheError>()?;
        copied_bytes += chunk.len();
        sink.feed(chunk).await.map_into_report::<CacheError>()?;
    }
    let rows = sink
        .as_mut()
        .finish()
        .await
        .map_into_report::<CacheError>()?;
    Ok((copied_bytes, rows))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== population select ====================

    /// An int4 table whose leading `pk_count` columns form the primary key.
    fn table_metadata(name: &str, oid: u32, columns: &[&str], pk_count: usize) -> TableMetadata {
        use crate::catalog::{ColumnMetadata, ColumnPosition};
        use crate::oid::TypeOid;
        use postgres_types::Type;
        let store = columns
            .iter()
            .enumerate()
            .map(|(i, column)| ColumnMetadata {
                name: (*column).into(),
                position: ColumnPosition::from_index(i).expect("column position in range"),
                type_oid: TypeOid::from_raw(23),
                data_type: Type::INT4,
                type_name: "int4".into(),
                cache_type_name: "int4".into(),
                is_primary_key: i < pk_count,
            });
        TableMetadata::for_test(name, oid, store)
    }

    fn population_catalog() -> crate::catalog::Catalog {
        let mut tables = iddqd::BiHashMap::new();
        for table in [
            table_metadata("users", 1, &["id", "name"], 1),
            table_metadata("orders", 2, &["id", "user_id", "total"], 1),
            table_metadata("Odd Table", 3, &["Region", "id", "it's"], 2),
        ] {
            tables.insert_overwrite(table.into());
        }
        tables
    }

    /// Every (branch, table) population select of `sql`, in population order.
    fn population_selects(sql: &str, max_limit: Option<u64>) -> Vec<String> {
        let catalog = population_catalog();
        let population = ResolvedQuery::parse_resolve(sql, &catalog, &["public"]).expect("resolve");
        population_reads(&population, max_limit)
            .into_iter()
            .map(|read| read.select)
            .collect()
    }

    /// Each table of a branch is fetched with the branch's FROM and WHERE,
    /// DISTINCT, without its grouping, ordering or projection. A read another
    /// already issued, as from a CTE's copy per reference, is skipped.
    #[test]
    fn test_population_select_cases() {
        #[rustfmt::skip]
        let cases: [(&str, &str, Option<u64>, &[&str]); 15] = [
            ("single table", "SELECT name FROM users WHERE id = 1 ORDER BY name", Some(5), &[
                "SELECT public.users.id, public.users.name FROM public.users WHERE public.users.id = 1 ORDER BY public.users.name ASC LIMIT 5",
            ]),
            ("aliased join", "SELECT u.name, o.total FROM users u JOIN orders o ON o.user_id = u.id WHERE o.total > 2", None, &[
                "SELECT DISTINCT u.id, u.name FROM public.users u JOIN public.orders o ON o.user_id = u.id WHERE o.total > 2",
                "SELECT DISTINCT o.id, o.user_id, o.total FROM public.users u JOIN public.orders o ON o.user_id = u.id WHERE o.total > 2",
            ]),
            ("grouping dropped", "SELECT user_id, count(*) FROM orders GROUP BY user_id HAVING count(*) > 1", None, &[
                "SELECT DISTINCT public.orders.id, public.orders.user_id, public.orders.total FROM public.orders",
            ]),
            ("set operation", "SELECT id FROM users WHERE id = 1 UNION SELECT user_id FROM orders WHERE total = 2", None, &[
                "SELECT DISTINCT public.users.id, public.users.name FROM public.users WHERE public.users.id = 1",
                "SELECT DISTINCT public.orders.id, public.orders.user_id, public.orders.total FROM public.orders WHERE public.orders.total = 2",
            ]),
            ("subquery branch", "SELECT id FROM users WHERE id IN (SELECT user_id FROM orders WHERE total = 3)", None, &[
                "SELECT DISTINCT public.users.id, public.users.name FROM public.users WHERE public.users.id IN (SELECT public.orders.user_id FROM public.orders WHERE public.orders.total = 3)",
                "SELECT DISTINCT public.orders.id, public.orders.user_id, public.orders.total FROM public.orders WHERE public.orders.total = 3",
            ]),
            ("CTE referenced twice", "WITH c AS (SELECT id, name FROM users WHERE id < 10) SELECT a.name FROM c a JOIN c b ON a.id = b.id", None, &[
                "SELECT DISTINCT public.users.id, public.users.name FROM public.users WHERE public.users.id < 10",
            ]),
            ("CTE in a subquery", "WITH c AS (SELECT id, name FROM users WHERE id < 10) SELECT name FROM c WHERE id IN (SELECT id FROM c WHERE name = 'x')", None, &[
                "SELECT DISTINCT public.users.id, public.users.name FROM public.users WHERE public.users.id < 10",
            ]),
            ("self-join reads both references", "SELECT u.name FROM users u JOIN users v ON v.id = u.id", None, &[
                "SELECT DISTINCT u.id, u.name FROM public.users u JOIN public.users v ON v.id = u.id",
                "SELECT DISTINCT v.id, v.name FROM public.users u JOIN public.users v ON v.id = u.id",
            ]),
            ("capped by alias", "SELECT name AS n FROM users ORDER BY n DESC", Some(3), &[
                "SELECT public.users.id, public.users.name FROM public.users ORDER BY public.users.name DESC LIMIT 3",
            ]),
            ("capped by position", "SELECT total, id FROM orders ORDER BY 1 DESC NULLS LAST, 2", Some(4), &[
                "SELECT public.orders.id, public.orders.user_id, public.orders.total FROM public.orders ORDER BY public.orders.total DESC NULLS LAST, public.orders.id ASC LIMIT 4",
            ]),
            ("capped by star position", "SELECT * FROM orders ORDER BY 3", Some(4), &[
                "SELECT public.orders.id, public.orders.user_id, public.orders.total FROM public.orders ORDER BY public.orders.total ASC LIMIT 4",
            ]),
            ("capped by expression", "SELECT name FROM users ORDER BY id + 0 DESC", Some(2), &[
                "SELECT public.users.id, public.users.name FROM public.users ORDER BY (public.users.id + 0) DESC LIMIT 2",
            ]),
            ("capped without order", "SELECT name FROM users", Some(2), &[
                "SELECT public.users.id, public.users.name FROM public.users LIMIT 2",
            ]),
            ("subquery branch uncapped", "SELECT id FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 1) ORDER BY id", Some(2), &[
                "SELECT public.users.id, public.users.name FROM public.users WHERE public.users.id IN (SELECT public.orders.user_id FROM public.orders WHERE public.orders.total > 1) ORDER BY public.users.id ASC LIMIT 2",
                "SELECT DISTINCT public.orders.id, public.orders.user_id, public.orders.total FROM public.orders WHERE public.orders.total > 1",
            ]),
            ("derived-table root uncapped", "SELECT id FROM (SELECT id, name FROM users WHERE id > 1) d ORDER BY id", Some(2), &[
                "SELECT DISTINCT public.users.id, public.users.name FROM public.users WHERE public.users.id > 1",
            ]),
        ];
        for (label, sql, max_limit, expected) in cases {
            assert_eq!(
                population_selects(sql, max_limit),
                expected,
                "{label}: {sql}"
            );
        }
    }

    // ==================== COPY statements ====================

    /// The origin COPY wraps the select and drops phantom rows on every PK
    /// column, quoted, after the select's own LIMIT.
    #[test]
    fn test_population_copy_out_sql() {
        let odd = table_metadata("Odd Table", 3, &["Region", "id", "it's"], 2);
        assert_eq!(
            population_copy_out_sql("SELECT DISTINCT o.\"Region\", o.id FROM t o LIMIT 5", &odd),
            "COPY (SELECT * FROM (SELECT DISTINCT o.\"Region\", o.id FROM t o LIMIT 5) population \
             WHERE population.\"Region\" IS NOT NULL AND population.\"id\" IS NOT NULL) TO STDOUT"
        );
    }

    /// The staging COPY names each table's columns in its population select's
    /// order, quoted as the select quotes them.
    #[test]
    fn test_staging_copy_in_sql() {
        let catalog = population_catalog();
        let population = ResolvedQuery::parse_resolve(
            "SELECT u.name FROM users u JOIN \"Odd Table\" o ON o.id = u.id",
            &catalog,
            &["public"],
        )
        .expect("resolve");
        let mut statements = Vec::new();
        for (branch, _) in select_branches_with_source(population.tree().root()) {
            for table_node in branch.direct_tables() {
                statements.push(staging_copy_in_sql(&population, table_node, "stage_1"));
            }
        }
        assert_eq!(
            statements,
            [
                "COPY pgcache_stage.stage_1 (id,name) FROM STDIN",
                "COPY pgcache_stage.stage_1 (\"Region\",id,\"it's\") FROM STDIN",
            ]
        );
    }
}
