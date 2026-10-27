use std::sync::Arc;

use tokio::sync::watch;

use super::CacheDispatch;
use crate::cache::types::{ReadFacts, RlsRelations, WriteTargetMap};
use crate::pg::Lsn;
use crate::query::SessionKey;

/// Connection-side handle to the current [`CacheDispatch`]. Hot-swaps across cache
/// restarts via a `watch` channel; `None` before the cache is ready or while it
/// is restarting (connections then forward to origin).
#[derive(Clone)]
pub struct CacheDispatchHandle {
    rx: watch::Receiver<Option<CacheDispatch>>,
}

impl CacheDispatchHandle {
    /// Snapshot the current cache, if it is up. The clone is cheap (channels +
    /// `Arc`s) and gives the caller an owned `CacheDispatch` to dispatch against.
    pub fn current(&self) -> Option<CacheDispatch> {
        self.rx.borrow().clone()
    }

    /// The current generation's settled watermark, or `None` if the
    /// cache is down/restarting. Reads through the watch without cloning the
    /// dispatch, so a connection never caches an `Arc` to a dead generation's
    /// (stale-high) watermark — the read always reflects the live generation
    /// (PGC-124).
    pub fn settled_lsn(&self) -> Option<Lsn> {
        self.rx.borrow().as_ref().map(CacheDispatch::settled_lsn)
    }

    /// The current generation's decode-stage receive cursor, or `None` if the
    /// cache is down/restarting. For the read-after-write gate's forward
    /// attribution (PGC-440).
    pub fn received_lsn(&self) -> Option<Lsn> {
        self.rx.borrow().as_ref().map(CacheDispatch::received_lsn)
    }

    /// The current generation's write-target map, or `None` if the cache is
    /// down/restarting (nothing is served from cache then, so a write recorded
    /// under its own name alone cannot be served stale against).
    pub fn write_targets(&self) -> Option<Arc<WriteTargetMap>> {
        self.rx.borrow().as_ref().map(CacheDispatch::write_targets)
    }

    /// The current generation's row-level-secured relations (PGC-113), or
    /// `None` if the cache is down/restarting.
    pub fn rls_relations(&self) -> Option<Arc<RlsRelations>> {
        self.rx.borrow().as_ref().map(CacheDispatch::rls_relations)
    }

    /// The read facts of a Ready registered query in the current generation,
    /// for the read-after-write gate (PGC-124). Reads through the watch
    /// without cloning the whole dispatch.
    pub(crate) fn cached_query_read_facts(&self, key: SessionKey) -> Option<Arc<ReadFacts>> {
        self.rx
            .borrow()
            .as_ref()
            .and_then(|d| d.cached_query_read_facts(key))
    }
}

/// Publish handle held by `cache_setup` to advertise its built `CacheDispatch`
/// (and retract it on exit).
pub struct CacheDispatchPublisher {
    tx: watch::Sender<Option<CacheDispatch>>,
}

impl CacheDispatchPublisher {
    pub fn publish(&self, dispatch: CacheDispatch) {
        let _ = self.tx.send(Some(dispatch));
    }

    pub fn clear(&self) {
        let _ = self.tx.send(None);
    }
}

/// Supervisor-side owner of the `CacheDispatch` watch. Hands out subscriber handles
/// for connection tasks and a publisher for `cache_setup`; clears on cache exit.
pub struct CacheDispatchUpdater {
    tx: watch::Sender<Option<CacheDispatch>>,
}

impl CacheDispatchUpdater {
    pub fn new() -> (Self, CacheDispatchHandle) {
        let (tx, rx) = watch::channel(None);
        (Self { tx }, CacheDispatchHandle { rx })
    }

    pub fn publisher(&self) -> CacheDispatchPublisher {
        CacheDispatchPublisher {
            tx: self.tx.clone(),
        }
    }

    pub fn subscribe(&self) -> CacheDispatchHandle {
        CacheDispatchHandle {
            rx: self.tx.subscribe(),
        }
    }

    pub fn clear(&self) {
        let _ = self.tx.send(None);
    }
}
