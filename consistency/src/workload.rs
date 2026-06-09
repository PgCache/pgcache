//! The load generator: concurrent writer and reader tasks driven through the
//! proxy. The SQL for each op comes from the active [`Scenario`], so the same
//! tasks drive both variants. Every op preserves the group-version invariant at
//! origin by construction (see `scenario` / `schema` for the rationale).

use anyhow::Result;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use tokio::time::{Duration, Instant};

use crate::db;
use crate::scenario::{Scenario, Variant};
use crate::schema::DATA_MAX;
use crate::snapshot::PROBE_DATA_HI;

/// Per-task tally of write ops executed.
#[derive(Debug, Default, Clone, Copy)]
pub struct OpCounts {
    pub version_bump: u64,
    pub cross_group_txn: u64,
    pub delete: u64,
    pub insert: u64,
    pub pk_update: u64,
}

impl OpCounts {
    pub fn merge(&mut self, other: OpCounts) {
        self.version_bump += other.version_bump;
        self.cross_group_txn += other.cross_group_txn;
        self.delete += other.delete;
        self.insert += other.insert;
        self.pk_update += other.pk_update;
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

/// Drive writes through the proxy until `deadline`, pausing `think_ms` between
/// ops to cap the aggregate write rate.
pub async fn writer_task(
    proxy_url: String,
    scenario: Scenario,
    seed: u64,
    think_ms: u64,
    deadline: Instant,
) -> Result<OpCounts> {
    let client = db::connect(&proxy_url).await?;
    let mut rng = StdRng::seed_from_u64(seed);
    let mut counts = OpCounts::default();
    let groups = scenario.model.groups;
    let think = (think_ms > 0).then(|| Duration::from_millis(think_ms));

    let bump = client.prepare(&scenario.version_bump()).await?;
    let delete = client.prepare(&scenario.item_delete()).await?;
    let pk_update = client.prepare(&scenario.pk_update()).await?;
    let insert = client.prepare(&scenario.item_insert()).await?;

    while Instant::now() < deadline {
        match write_op_pick(&mut rng) {
            WriteOp::VersionBump => {
                let g = rng.random_range(0..groups);
                db::execute_timed(&client, &bump, &[&g], "version bump").await?;
                counts.version_bump += 1;
            }
            WriteOp::CrossGroupTxn => {
                let (a, b) = scenario.model.pairs[rng.random_range(0..scenario.model.pairs.len())];
                db::batch_timed(&client, &scenario.cross_group_txn(a, b), "cross-group txn").await?;
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
