use std::thread;

use tokio_util::sync::CancellationToken;

use crate::cache::CacheResult;

mod cdc_driver;
mod memory_monitor;
mod reg_gate;
mod reset;
mod serve_pool;
mod setup;
mod supervise;

pub use supervise::{cache_generation_start, cache_supervise};

/// One generation of the cache subsystem: the cancel token that fires on its
/// death and the scoped thread handles to reap once it does.
pub struct CacheGeneration<'scope> {
    cancel: CancellationToken,
    handles: Vec<thread::ScopedJoinHandle<'scope, CacheResult<()>>>,
}
