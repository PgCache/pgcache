//! Population workers: the dispatcher pairing queued work with idle workers,
//! the worker loop (`worker`), and the origin-to-staging row stream
//! (`stream`).

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use ecow::EcoString;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tokio_postgres::Client;

use super::PopulationWork;
use crate::cache::messages::QueryCommand;
use crate::cache::population_pool::PopulationPool;
use crate::oid::Oid;
use crate::pg::Lsn;
use crate::settings::PgSettings;

mod stream;
mod worker;

pub(super) use worker::{population_dispatcher, population_worker_connect, population_worker_run};

/// How long to hold off spawn attempts after a population worker's
/// connections failed to open, so a down origin isn't hammered with connect
/// attempts every reconcile tick.
pub(super) const POPULATION_SPAWN_COOLDOWN: Duration = Duration::from_secs(5);

/// Everything needed to spawn one population worker outside `new()` (elastic
/// scale-up, PGC-437).
#[derive(Clone)]
pub(super) struct PopulationSpawnContext {
    pub(super) idle_tx: UnboundedSender<oneshot::Sender<PopulationWork>>,
    pub(super) cache_settings: PgSettings,
    pub(super) origin_settings: PgSettings,
    pub(super) query_tx: UnboundedSender<QueryCommand>,
    pub(super) throttled: Arc<AtomicBool>,
    pub(super) pool: Arc<PopulationPool>,
}

/// One population worker's connection pair: it reads from origin and writes
/// staging tables on the cache.
pub(super) struct PopulationConnections {
    origin: Client,
    cache: Client,
}

/// A completed population task, handed to the writer as a `PopulationMerge`.
struct PopulationOutcome {
    cached_bytes: usize,
    row_count: u64,
    /// Staging tables loaded, one per relation.
    staged: Vec<(Oid, EcoString)>,
    /// Origin WAL position after the reads; the merge gate's deadline.
    snapshot_lsn: Lsn,
}
