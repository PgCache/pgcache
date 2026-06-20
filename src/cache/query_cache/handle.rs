use tokio::sync::watch;

use super::CacheDispatch;

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
