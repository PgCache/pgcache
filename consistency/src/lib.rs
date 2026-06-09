//! Consistency stress harness (PGC-252).
//!
//! Drives sustained concurrent reads + writes through pgcache against a
//! schema whose correctness is checkable by a simple invariant — every row
//! in a group shares one `version` — and continuously snapshots the served
//! view to catch consistency violations the deterministic fault-injection
//! tests (`tests/population_cdc_consistency_test.rs`) don't provoke.
//!
//! The pass/fail oracle reads *through the proxy*: the physical cache tables
//! legitimately hold rows that are never served (orphans, generation-0,
//! in-flight population), so a raw cache dump would report false positives by
//! design.
//!
//! Run explicitly (not part of `cargo test`):
//! ```text
//! cargo run -p pgcache-consistency -- --duration-secs 60
//! cargo run -p pgcache-consistency -- --scenario two-table --write-think-ms 3
//! ```
//! Two scenarios: `single-table` (version on the row; queries maintained
//! in-place, never invalidated) and `two-table` (version reached through a
//! join; item inserts invalidate and repopulate the join continuously). The
//! join scenario is writer-bound — the single-threaded cache writer pays
//! join-invalidation cost per write — so pace writers with `--write-think-ms`
//! (or use fewer `--writers`), else CDC apply falls behind and the final settle
//! times out.
//!
//! A failure prints the seed; re-run with `--seed <n>` to reproduce.

pub mod cli;
pub mod db;
pub mod invariants;
pub mod runner;
pub mod scenario;
pub mod schema;
pub mod snapshot;
pub mod workload;
