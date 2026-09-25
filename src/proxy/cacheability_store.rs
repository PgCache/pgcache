//! Process-wide interning store for cacheability verdicts.
//!
//! Analysis is a pure function of the SQL text and the process-global function
//! volatility map — [`CacheableQuery::try_new`](crate::cache::query::CacheableQuery::try_new)
//! takes no connection-local state, and `search_path` travels with the request
//! rather than the verdict. So one parsed verdict serves every connection.
//!
//! Before this store each connection called `Arc::new` on its own analysis, so
//! N connections retained N separate ASTs for the same text — ~2.3 KB each,
//! times connections times distinct texts. Here the payload is interned: the
//! store holds one `Arc<Action>` and connections keep cheap clones in a bounded
//! LRU.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use dashmap::DashMap;
use tokio::sync::watch;

use crate::id_hash::BuildIdHasher;

use super::query::{Action, SqlTextHash};

/// Interned cacheability verdicts shared by every connection.
///
/// Reclamation is by reference count: an entry whose `Arc<Action>` strong count
/// has fallen to 1 is held by this map alone — no connection's LRU still
/// references it — so it can be dropped. In-flight requests are unaffected
/// either way: they carry their own clone of the inner `Arc<CacheableQuery>`,
/// so removing the store entry never pulls a payload out from under one.
pub struct CacheabilityStore {
    entries: DashMap<SqlTextHash, Arc<Action>, BuildIdHasher<SqlTextHash>>,
    /// Bumped by [`pressure_observed`](Self::pressure_observed) when the whole
    /// store is dropped. Connections compare it against their own copy and
    /// drain their LRU when it moves — clearing this map alone would free
    /// nothing while connections still hold `Arc`s.
    epoch: AtomicU64,
    /// Last observed memory-pressure sample, so a drop fires once on the rising
    /// edge rather than once per query for as long as pressure lasts.
    pressured: AtomicBool,
    /// Wakes connections on an epoch move. A connection drains lazily on its
    /// next query, so an idle one would otherwise pin its handles indefinitely
    /// — exactly the pooled connections a pressure drop needs to reclaim.
    epoch_tx: watch::Sender<u64>,
}

impl CacheabilityStore {
    pub fn new() -> Self {
        Self {
            entries: DashMap::with_hasher(BuildIdHasher::default()),
            epoch: AtomicU64::new(0),
            pressured: AtomicBool::new(false),
            epoch_tx: watch::Sender::new(0),
        }
    }

    /// Subscribe to epoch moves. Connections select on this so a drop reaches
    /// idle connections immediately, rather than waiting for traffic that may
    /// never come.
    pub(super) fn epoch_subscribe(&self) -> watch::Receiver<u64> {
        self.epoch_tx.subscribe()
    }

    /// The interned verdict for `key`, if one is present.
    pub(super) fn get(&self, key: SqlTextHash) -> Option<Arc<Action>> {
        self.entries.get(&key).map(|e| Arc::clone(e.value()))
    }

    /// Intern `action` under `key`, returning the shared handle. If another
    /// connection interned the same text first its entry wins and `action` is
    /// discarded — analysis is pure, so the two are equivalent.
    pub(super) fn intern(&self, key: SqlTextHash, action: Action) -> Arc<Action> {
        Arc::clone(
            self.entries
                .entry(key)
                .or_insert_with(|| Arc::new(action))
                .value(),
        )
    }

    /// Drop `key` if no connection LRU still references it.
    ///
    /// Called when an entry leaves a connection's LRU — by eviction, by the
    /// connection closing, or by an epoch drain — which is exactly when its
    /// count can have reached 1. The predicate runs under the shard lock, so a
    /// concurrent [`get`](Self::get) either observes the entry before removal
    /// (count > 1, no removal) or misses it and re-analyzes.
    pub(super) fn release(&self, key: SqlTextHash) {
        self.entries
            .remove_if(&key, |_, entry| Arc::strong_count(entry) == 1);
    }

    /// Current drain epoch. A connection whose copy differs must clear its LRU.
    pub(super) fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Relaxed)
    }

    /// Feed a memory-pressure sample. On the rising edge, drop every interned
    /// verdict and bump the epoch so connections drain their own LRUs.
    ///
    /// This is a last resort, not the steady-state path: it is safe because
    /// these are pure caches — correctness is untouched and the cost of
    /// rebuilding is bounded re-analysis, which is the right trade under
    /// pressure.
    pub(super) fn pressure_observed(&self, pressured: bool) {
        if self
            .pressured
            .compare_exchange(!pressured, pressured, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
            || !pressured
        {
            return;
        }
        self.entries.clear();
        let epoch = self.epoch.fetch_add(1, Ordering::Relaxed) + 1;
        // Wake every connection, including idle ones. Clearing the map above
        // frees nothing on its own while connections still hold handles.
        self.epoch_tx.send_replace(epoch);
        tracing::info!("memory pressure: dropped interned cacheability verdicts");
    }

    /// Number of interned verdicts.
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    /// Publish the entry count, on a timer rather than on membership change.
    /// `DashMap::len` read-locks every shard, so calling it per intern/release
    /// would put an all-shards operation on the churn path and scale badly with
    /// core count — the same trap `state_gauges_update` hit on the writer.
    #[allow(clippy::cast_precision_loss)] // entry counts never approach 2^52
    pub(super) fn gauge_publish(&self) {
        crate::metrics::handles()
            .conn
            .cacheability_entries
            .set(self.len() as f64);
    }
}

impl Default for CacheabilityStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::proxy::query::ForwardReason;
    use crate::query::write::StatementEffects;

    fn forward() -> Action {
        Action::Forward(ForwardReason::Invalid, StatementEffects::default())
    }

    #[test]
    fn test_intern_returns_one_payload_for_repeat_texts() {
        let store = CacheabilityStore::new();
        let key = SqlTextHash::of("SELECT 1");

        let first = store.intern(key, forward());
        let second = store.intern(key, forward());

        assert!(
            Arc::ptr_eq(&first, &second),
            "second intern of the same text must share the first payload"
        );
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_release_keeps_entry_while_a_holder_remains() {
        let store = CacheabilityStore::new();
        let key = SqlTextHash::of("SELECT 1");
        let held = store.intern(key, forward());

        store.release(key);

        assert_eq!(store.len(), 1, "entry is still referenced");
        drop(held);
        store.release(key);
        assert_eq!(store.len(), 0, "last holder gone, entry reclaimed");
    }

    /// The drop must reach idle connections. Draining is lazy — a connection
    /// reconciles on its next query — so without a wakeup an open-but-idle
    /// connection pins its handles indefinitely and the drop reclaims far less
    /// than the cleared map suggests.
    #[tokio::test]
    async fn test_pressure_wakes_idle_subscribers() {
        let store = CacheabilityStore::new();
        let mut idle = store.epoch_subscribe();

        assert!(
            !idle.has_changed().expect("subscription is live"),
            "no epoch move yet"
        );

        store.pressure_observed(true);

        // An idle connection is parked on `changed()`; it must complete rather
        // than wait for traffic that may never arrive.
        tokio::time::timeout(std::time::Duration::from_secs(1), idle.changed())
            .await
            .expect("idle subscriber must be woken by the pressure drop")
            .expect("sender outlives the subscriber");
        assert_eq!(*idle.borrow(), store.epoch());
    }

    #[test]
    fn test_pressure_drops_entries_once_per_episode() {
        let store = CacheabilityStore::new();
        store.intern(SqlTextHash::of("SELECT 1"), forward());
        let epoch_before = store.epoch();

        store.pressure_observed(true);
        assert_eq!(store.len(), 0, "pressure drops interned verdicts");
        let epoch_after = store.epoch();
        assert_ne!(
            epoch_before, epoch_after,
            "epoch moves so connections drain"
        );

        // Still pressured: no further drop, epoch stable.
        store.intern(SqlTextHash::of("SELECT 2"), forward());
        store.pressure_observed(true);
        assert_eq!(store.len(), 1, "no re-drop while pressure persists");
        assert_eq!(store.epoch(), epoch_after);

        // Falling then rising edge drops again.
        store.pressure_observed(false);
        store.pressure_observed(true);
        assert_eq!(store.len(), 0);
        assert_ne!(store.epoch(), epoch_after);
    }
}
