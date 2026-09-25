//! The load generator: concurrent writer and reader tasks driven through the
//! proxy. The SQL for each op comes from the active [`Scenario`], so the same
//! tasks drive both variants. Every op preserves the group-version invariant at
//! origin by construction (see `scenario` / `schema` for the rationale).

use anyhow::{Result, bail};
use clap::ValueEnum;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use tokio::time::{Duration, Instant};
use tokio_postgres::error::SqlState;

use crate::db;
use crate::invariants::{Violation, intra_snapshot_reduce};
use crate::scenario::{Scenario, Variant};
use crate::schema::DATA_MAX;
use crate::snapshot::PROBE_DATA_HI;

/// Isolation level of the explicit transaction blocks `--txn-reads` opens.
/// REPEATABLE READ stands in for every strict level: pgcache forwards all
/// in-block reads at any level other than READ COMMITTED through one path, and
/// SERIALIZABLE would only add PostgreSQL's SSI aborts to the contended mix.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum TxnIsolation {
    ReadCommitted,
    RepeatableRead,
}

impl TxnIsolation {
    pub fn begin_sql(self) -> &'static str {
        match self {
            Self::ReadCommitted => "BEGIN",
            Self::RepeatableRead => "BEGIN ISOLATION LEVEL REPEATABLE READ",
        }
    }
}

/// The in-transaction mix (PGC-387): when enabled, half of the reader's reads
/// and half of the writer's bumps run inside an explicit block.
#[derive(Clone, Copy, Debug)]
pub struct TxnMix {
    pub enabled: bool,
    pub isolation: TxnIsolation,
}

impl TxnMix {
    fn roll(self, rng: &mut StdRng) -> bool {
        self.enabled && rng.random_bool(0.5)
    }
}

/// Whether an error is a serialization failure (`40001`): expected for a
/// REPEATABLE READ bump racing another writer on the same rows, so the block
/// is rolled back and the op skipped rather than failed.
fn serialization_failure(err: &anyhow::Error) -> bool {
    err.chain()
        .filter_map(|cause| cause.downcast_ref::<tokio_postgres::Error>())
        .any(|pg| pg.code() == Some(&SqlState::T_R_SERIALIZATION_FAILURE))
}

/// Every group in a served per-group result must show one version (the same
/// intra-snapshot atomicity the oracle checks, applied to an in-block read).
fn rows_uniform_check(rows: &[tokio_postgres::Row], what: &str) -> Result<()> {
    let versions: Vec<(i32, i32)> = rows.iter().map(|r| (r.get(1), r.get(2))).collect();
    if let Err(v) = intra_snapshot_reduce(&versions) {
        bail!("consistency violation ({what}): {v}");
    }
    Ok(())
}

/// Per-task tally of write ops executed.
#[derive(Debug, Default, Clone, Copy)]
pub struct OpCounts {
    pub version_bump: u64,
    pub cross_group_txn: u64,
    pub delete: u64,
    pub insert: u64,
    pub pk_update: u64,
    /// In-block bumps rolled back on a serialization failure (strict levels).
    pub serialization_failure: u64,
}

impl OpCounts {
    pub fn merge(&mut self, other: OpCounts) {
        self.version_bump += other.version_bump;
        self.cross_group_txn += other.cross_group_txn;
        self.delete += other.delete;
        self.insert += other.insert;
        self.pk_update += other.pk_update;
        self.serialization_failure += other.serialization_failure;
    }

    pub fn total(&self) -> u64 {
        self.version_bump + self.cross_group_txn + self.delete + self.insert + self.pk_update
    }
}

enum WriteOp {
    VersionBump,
    CrossGroupTxn,
    Delete,
    Insert,
    PkUpdate,
}

/// `(op, weight)` mix. Bumps dominate (they're the core invariant driver);
/// deletes are kept low so normal groups don't deplete over a long run.
const WRITE_MIX: &[(WriteOp, u32)] = &[
    (WriteOp::VersionBump, 50),
    (WriteOp::CrossGroupTxn, 15),
    (WriteOp::PkUpdate, 15),
    (WriteOp::Insert, 10),
    (WriteOp::Delete, 10),
];

fn write_op_pick(rng: &mut StdRng) -> &'static WriteOp {
    let total: u32 = WRITE_MIX.iter().map(|(_, w)| w).sum();
    let mut roll = rng.random_range(0..total);
    for (op, weight) in WRITE_MIX {
        if roll < *weight {
            return op;
        }
        roll -= *weight;
    }
    &WriteOp::VersionBump
}

/// One in-block bump: `BEGIN; bump g RETURNING; read g (own-write check);
/// read other (uniformity); COMMIT`. Errors propagate with the block still
/// open; the caller rolls back.
async fn txn_bump_block(
    client: &tokio_postgres::Client,
    txn: TxnMix,
    bump_returning: &tokio_postgres::Statement,
    per_group: &tokio_postgres::Statement,
    g: i32,
    other: i32,
) -> Result<()> {
    db::batch_timed(client, txn.isolation.begin_sql(), "txn begin").await?;
    let bumped = db::query_timed(client, bump_returning, &[&vec![g]], "txn version bump").await?;
    let expected: i32 = bumped.first().map(|r| r.get(1)).unwrap_or(0);
    let rows = db::query_timed(client, per_group, &[&g], "txn own-write read").await?;
    if let Some(observed) = rows
        .iter()
        .map(|r| r.get::<_, i32>(2))
        .find(|v| *v != expected)
    {
        bail!(
            "consistency violation: {}",
            Violation::OwnWrite {
                group: g,
                expected,
                observed,
            }
        );
    }
    let rows = db::query_timed(client, per_group, &[&other], "txn other-group read").await?;
    rows_uniform_check(&rows, "txn other-group read")?;
    db::batch_timed(client, "COMMIT", "txn commit").await
}

/// Drive writes through the proxy until `deadline`, pausing `think_ms` between
/// ops to cap the aggregate write rate.
pub async fn writer_task(
    proxy_url: String,
    scenario: Scenario,
    seed: u64,
    think_ms: u64,
    bump_groups: usize,
    txn: TxnMix,
    deadline: Instant,
) -> Result<OpCounts> {
    let client = db::connect(&proxy_url).await?;
    let mut rng = StdRng::seed_from_u64(seed);
    let mut counts = OpCounts::default();
    let groups = scenario.model.groups;
    let think = (think_ms > 0).then(|| Duration::from_millis(think_ms));
    // A version bump touches this many groups in one statement/txn (>1 = fat
    // CDC frame). Clamped to the normal-group count; paired groups are excluded.
    let bump_span = bump_groups.clamp(1, groups as usize) as i32;

    let bump = client.prepare(&scenario.version_bump()).await?;
    let bump_returning = client.prepare(&scenario.version_bump_returning()).await?;
    let per_group = client.prepare(&scenario.per_group_select()).await?;
    let delete = client.prepare(&scenario.item_delete()).await?;
    let pk_update = client.prepare(&scenario.pk_update()).await?;
    let insert = client.prepare(&scenario.item_insert()).await?;

    while Instant::now() < deadline {
        match write_op_pick(&mut rng) {
            WriteOp::VersionBump if txn.roll(&mut rng) => {
                // Bump one group inside a block, then read it back inside the
                // same block: every row must show the bumped version — the
                // block's own uncommitted write can never be hidden by a cache
                // serve (PGC-387). A second, untouched group is read too (it
                // may serve from cache at READ COMMITTED) and must be uniform.
                let g = rng.random_range(0..groups);
                let other = rng.random_range(0..groups);
                match txn_bump_block(&client, txn, &bump_returning, &per_group, g, other).await {
                    Ok(()) => counts.version_bump += 1,
                    // A strict-level block can fail at any step up to and
                    // including COMMIT when it races another writer.
                    Err(e) if serialization_failure(&e) => {
                        db::batch_timed(&client, "ROLLBACK", "txn rollback").await?;
                        counts.serialization_failure += 1;
                    }
                    Err(e) => {
                        let _ = db::batch_timed(&client, "ROLLBACK", "txn rollback").await;
                        return Err(e);
                    }
                }
            }
            WriteOp::VersionBump => {
                // Contiguous window of `bump_span` normal groups → a frame with
                // that many row changes on the version table.
                let start = rng.random_range(0..=(groups - bump_span));
                let group_ids: Vec<i32> = (start..start + bump_span).collect();
                db::execute_timed(&client, &bump, &[&group_ids], "version bump").await?;
                counts.version_bump += 1;
            }
            WriteOp::CrossGroupTxn => {
                let (a, b) = scenario.model.pairs[rng.random_range(0..scenario.model.pairs.len())];
                db::batch_timed(&client, &scenario.cross_group_txn(a, b), "cross-group txn")
                    .await?;
                counts.cross_group_txn += 1;
            }
            WriteOp::Delete => {
                let g = rng.random_range(0..groups);
                db::execute_timed(&client, &delete, &[&g], "delete").await?;
                counts.delete += 1;
            }
            WriteOp::Insert => {
                let data = rng.random_range(0..DATA_MAX);
                match scenario.variant {
                    // Single-table: a brand-new group at version 0.
                    Variant::SingleTable => {
                        db::execute_timed(&client, &insert, &[&data], "insert").await?;
                    }
                    // Two-table: a new item in an existing normal group, which
                    // grows and so invalidates that group's join query.
                    Variant::TwoTable => {
                        let g = rng.random_range(0..groups);
                        db::execute_timed(&client, &insert, &[&g, &data], "insert").await?;
                    }
                }
                counts.insert += 1;
            }
            WriteOp::PkUpdate => {
                let g = rng.random_range(0..groups);
                db::execute_timed(&client, &pk_update, &[&g], "pk update").await?;
                counts.pk_update += 1;
            }
        }

        if let Some(d) = think {
            tokio::time::sleep(d).await;
        }
    }

    Ok(counts)
}

/// Drive cacheable reads through the proxy until `deadline`, keeping queries
/// populated and served. Returns the number of reads issued.
pub async fn reader_task(
    proxy_url: String,
    scenario: Scenario,
    seed: u64,
    txn: TxnMix,
    deadline: Instant,
) -> Result<u64> {
    let client = db::connect(&proxy_url).await?;
    let mut rng = StdRng::seed_from_u64(seed);

    // Normal and paired groups both get single-group read traffic.
    let readable = scenario.model.all_groups();

    let single = client.prepare(&scenario.per_group_select()).await?;
    let cross = scenario.cross_group_select(PROBE_DATA_HI);

    let mut reads = 0u64;
    while Instant::now() < deadline {
        if txn.roll(&mut rng) {
            // Two reads inside one block: each must be internally atomic
            // (PGC-387 in-transaction serving).
            let g = readable[rng.random_range(0..readable.len())];
            db::batch_timed(&client, txn.isolation.begin_sql(), "txn begin").await?;
            let rows = db::query_timed(&client, &single, &[&g], "txn single-group read").await?;
            rows_uniform_check(&rows, "txn single-group read")?;
            let probe = db::query_timed(&client, &cross, &[], "txn cross-group read").await?;
            let pairs: Vec<(i32, i32)> = probe.iter().map(|r| (r.get(0), r.get(1))).collect();
            if let Err(v) = intra_snapshot_reduce(&pairs) {
                bail!("consistency violation (txn cross-group read): {v}");
            }
            db::batch_timed(&client, "COMMIT", "txn commit").await?;
            reads += 2;
            continue;
        }
        if rng.random_range(0..100) < 70 {
            let g = readable[rng.random_range(0..readable.len())];
            db::query_timed(&client, &single, &[&g], "single-group read").await?;
        } else {
            db::query_timed(&client, &cross, &[], "cross-group read").await?;
        }
        reads += 1;
    }

    Ok(reads)
}
