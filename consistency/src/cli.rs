use std::path::PathBuf;

use clap::Parser;

use crate::scenario::Variant;

/// Consistency stress harness for pgcache: drives concurrent reads + writes
/// against the group-version schema and checks the served view stays
/// consistent.
#[derive(Debug, Parser)]
#[command(name = "pgcache-consistency")]
pub struct Cli {
    /// Schema scenario: `single-table` (version on the row) or `two-table`
    /// (version reached through a join; invalidated and repopulated under load).
    #[arg(long, value_enum, default_value = "single-table")]
    pub scenario: Variant,

    /// How long to drive load, in seconds.
    #[arg(long, default_value_t = 30)]
    pub duration_secs: u64,

    /// Interval between served-view snapshots, in milliseconds.
    #[arg(long, default_value_t = 250)]
    pub snapshot_interval_ms: u64,

    /// Concurrent write tasks.
    #[arg(long, default_value_t = 4)]
    pub writers: usize,

    /// Concurrent read tasks (drive cache population/serving traffic).
    #[arg(long, default_value_t = 4)]
    pub readers: usize,

    /// Think time between writes per writer task, in milliseconds (0 = none).
    /// Caps the aggregate write rate so the single-threaded cache writer can
    /// keep up — needed for the two-table scenario, where each write also pays
    /// join-invalidation cost and unpaced writers outrun CDC apply.
    #[arg(long, default_value_t = 0)]
    pub write_think_ms: u64,

    /// Connections the snapshot reader fans per-group reads across. Higher =
    /// more snapshots/sec when cold reads block on the deferred-Ready gate.
    #[arg(long, default_value_t = 8)]
    pub snapshot_conns: usize,

    /// Number of normal (single-bump) groups, seeded at version 0.
    #[arg(long, default_value_t = 50)]
    pub groups: i32,

    /// Number of paired groups (each pair bumped together in one txn), seeded
    /// at version 0.
    #[arg(long, default_value_t = 8)]
    pub pairs: i32,

    /// Rows seeded per group.
    #[arg(long, default_value_t = 20)]
    pub rows_per_group: i32,

    /// RNG seed (defaults to entropy; logged so a failure can be reproduced).
    #[arg(long)]
    pub seed: Option<u64>,

    /// pgcache worker count.
    #[arg(long, default_value_t = 4)]
    pub workers: usize,

    /// pgcache log level.
    #[arg(long, default_value = "info")]
    pub log_level: String,

    /// Spawn this prebuilt pgcache binary instead of building from the
    /// workspace. Useful for running the harness against an older build.
    #[arg(long)]
    pub pgcache_bin: Option<PathBuf>,
}
