//! Reads of the served view used by the checks. The SQL is supplied by the
//! active [`Scenario`](crate::scenario::Scenario), so the same readers serve
//! both the single-table and the two-table (join) variants.
//!
//! All reads here go through the proxy — that is the consistency contract.
//!
//! The oracle reads the **per-group query shape** (`WHERE group_id = $1`), the
//! same queries the readers drive. This matters: population-race bugs (ghost
//! rows) live in the result of the query that populated while a row it read was
//! concurrently removed. A query warmed clean before load and maintained
//! in-place by CDC (e.g. the full-table scan) never exposes them — so the
//! oracle must inspect the queries that actually populate under load.

use anyhow::Result;
use tokio_postgres::{Client, Statement};

use crate::db;
use crate::schema::DATA_MAX;

/// Upper bound of the cross-group read probe (lower half of the data range).
pub const PROBE_DATA_HI: i32 = DATA_MAX / 2;

/// `(id, group_id, version, data)` for every row across `groups`, read one
/// group at a time through the per-group query shape.
pub async fn per_group_rows(
    client: &Client,
    sql: &str,
    groups: &[i32],
) -> Result<Vec<(i32, i32, i32, i32)>> {
    let stmt = client.prepare(sql).await?;
    let mut out = Vec::new();
    for &g in groups {
        out.extend(group_query(client, &stmt, g).await?);
    }
    Ok(out)
}

async fn group_query(
    client: &Client,
    stmt: &Statement,
    group: i32,
) -> Result<Vec<(i32, i32, i32, i32)>> {
    let rows = db::query_timed(client, stmt, &[&group], "per-group read").await?;
    Ok(rows
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
        .collect())
}

/// A pool of proxy connections for fanning per-group reads out concurrently.
///
/// The served per-group read can block on the deferred-Ready gate while a query
/// (re)populates under load; reading every group serially on one connection
/// would let one slow group throttle the whole snapshot rate. Round-robining
/// the groups across N connections runs those blocking reads concurrently, so a
/// snapshot costs `ceil(groups / N)` blocking reads instead of `groups`.
pub struct GroupReader {
    conns: Vec<(Client, Statement)>,
}

impl GroupReader {
    pub async fn connect(url: &str, conns: usize, sql: &str) -> Result<Self> {
        let mut pool = Vec::with_capacity(conns);
        for _ in 0..conns.max(1) {
            let client = db::connect(url).await?;
            let stmt = client.prepare(sql).await?;
            pool.push((client, stmt));
        }
        Ok(Self { conns: pool })
    }

    /// Read every row across `groups`, distributing the groups round-robin over
    /// the pooled connections and running each connection's slice concurrently.
    pub async fn read(&self, groups: &[i32]) -> Result<Vec<(i32, i32, i32, i32)>> {
        let n = self.conns.len();
        let slices = self.conns.iter().enumerate().map(|(i, (client, stmt))| {
            let mine: Vec<i32> = groups.iter().copied().skip(i).step_by(n).collect();
            async move {
                let mut out = Vec::new();
                for g in mine {
                    out.extend(group_query(client, stmt, g).await?);
                }
                Ok::<_, anyhow::Error>(out)
            }
        });
        let results = futures_util::future::try_join_all(slices).await?;
        Ok(results.into_iter().flatten().collect())
    }
}

/// `(group_id, version)` for all paired groups, read in **one** query so the
/// result is a single atomic snapshot. Reading the two sides of a pair with
/// separate queries would straddle a concurrent cross-group commit and look
/// torn even when the cache is consistent — the pair check needs one snapshot.
pub async fn pair_versions(
    client: &Client,
    sql: &str,
    pair_ids: &[i32],
) -> Result<Vec<(i32, i32)>> {
    let rows = db::query_timed(client, sql, &[&pair_ids], "paired-group read").await?;
    Ok(rows.iter().map(|r| (r.get(0), r.get(1))).collect())
}

/// `(group_id, version)` for the lower-half `data` range — a predicated cached
/// query, checked for intra-group atomicity within its result.
pub async fn cross_group_probe(client: &Client, sql: &str) -> Result<Vec<(i32, i32)>> {
    let rows = db::query_timed(client, sql, &[], "cross-group probe").await?;
    Ok(rows.iter().map(|r| (r.get(0), r.get(1))).collect())
}

/// `(id, group_id, version, data)` for every row, ordered by id — the whole
/// table via a single scan, for the global cache-vs-origin equality check.
pub async fn full_table(client: &Client, sql: &str) -> Result<Vec<(i32, i32, i32, i32)>> {
    let rows = db::query_timed(client, sql, &[], "full-table read").await?;
    Ok(rows
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
        .collect())
}

/// Project per-group rows to the `(group_id, version)` pairs the invariants
/// operate on.
pub fn group_versions(rows: &[(i32, i32, i32, i32)]) -> Vec<(i32, i32)> {
    rows.iter().map(|&(_, g, v, _)| (g, v)).collect()
}

/// Compare two row sets (sorting both first), returning a description of the
/// first difference (or a count mismatch), or `None` if identical.
pub fn equality_diff(
    cache: &[(i32, i32, i32, i32)],
    origin: &[(i32, i32, i32, i32)],
) -> Option<String> {
    let mut cache = cache.to_vec();
    let mut origin = origin.to_vec();
    cache.sort_unstable();
    origin.sort_unstable();
    if cache.len() != origin.len() {
        return Some(format!(
            "row count differs: cache has {}, origin has {}",
            cache.len(),
            origin.len()
        ));
    }
    cache
        .iter()
        .zip(&origin)
        .find(|(c, o)| c != o)
        .map(|(c, o)| format!("row differs: cache {c:?} vs origin {o:?}"))
}
