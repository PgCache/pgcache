//! The per-connection cacheability memo: SQL text hash → analyzed verdict,
//! a bounded LRU of handles into the shared [`CacheabilityStore`].

use std::hash::{DefaultHasher, Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::Arc;

use lru::LruCache;
use tracing::trace;

use super::cacheability_store::CacheabilityStore;
use super::query::Action;
use crate::id_hash::{BuildIdHasher, impl_id_hashable};

/// A hash of a SQL query's *text* (not its AST). Keys the proxy's cacheability
/// memo — identical query text yields the same cacheability verdict, so the
/// parse/convert/classify work is done once. Deliberately distinct from
/// [`Fingerprint`](crate::query::Fingerprint), an AST content hash: different
/// input, different domain, not interchangeable. Already a uniformly-distributed
/// hash, so the memo uses the passthrough [`BuildIdHasher`].
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct SqlTextHash(u64);

impl_id_hashable!(SqlTextHash);

impl SqlTextHash {
    /// Hash a SQL string's text.
    pub(super) fn of(sql: &str) -> Self {
        let mut hasher = DefaultHasher::new();
        sql.hash(&mut hasher);
        Self(hasher.finish())
    }
}

/// Bounded per connection so a dynamic-SQL workload can't grow it unbounded,
/// matching the describe cache. A miss here is not a re-analysis — it falls
/// back to the shared store, which is a sharded-map lookup and an `Arc` clone
/// — so erring small costs little.
pub(super) const CACHEABILITY_MEMO_CAPACITY: NonZeroUsize = NonZeroUsize::new(256).unwrap();

/// The proxy's per-connection cacheability memo: SQL text hash → the analyzed
/// verdict (cacheable AST, forward reason, or an explain interception), so the
/// parse/convert/classify work is done once per distinct query text.
///
/// Entries are cheap handles into [`CacheabilityStore`]; the payload itself is
/// interned there once for the whole process. Dropping a handle — by LRU
/// eviction, epoch drain, or the connection closing — releases the key back to
/// the store, which reclaims it if nothing else holds it.
pub(super) struct CacheabilityCache {
    entries: LruCache<SqlTextHash, Arc<Action>, BuildIdHasher<SqlTextHash>>,
    store: Arc<CacheabilityStore>,
    /// Drain epoch this memo was last reconciled against.
    epoch: u64,
}

impl CacheabilityCache {
    pub(super) fn new(store: Arc<CacheabilityStore>) -> Self {
        Self {
            entries: LruCache::with_hasher(CACHEABILITY_MEMO_CAPACITY, BuildIdHasher::default()),
            epoch: store.epoch(),
            store,
        }
    }

    /// Feed a memory-pressure sample through to the shared store. Called where
    /// the connection already holds a `CacheDispatch`, which carries the flag
    /// for the current cache generation.
    pub(super) fn pressure_observe(&self, pressured: bool) {
        self.store.pressure_observed(pressured);
    }

    /// Drop every handle and release the keys, so the store can reclaim any
    /// entry this memo was the last to hold.
    fn drain(&mut self) {
        let keys: Vec<SqlTextHash> = self.entries.iter().map(|(key, _)| *key).collect();
        self.entries.clear();
        for key in keys {
            self.store.release(key);
        }
    }

    /// Reconcile against a wholesale drop: clearing the store alone frees
    /// nothing while connections still hold `Arc`s, so each connection drains
    /// when it notices the epoch move.
    pub(super) fn epoch_reconcile(&mut self) {
        let current = self.store.epoch();
        if current != self.epoch {
            self.drain();
            self.epoch = current;
        }
    }

    /// The memoized verdict for `key`: this connection's tier first, then the
    /// shared store, whose handle is remembered here on the way back.
    pub(super) fn lookup(&mut self, key: SqlTextHash) -> Option<Action> {
        if let Some(action) = self.entries.get(&key) {
            trace!("cacheability memo hit");
            return Some((**action).clone());
        }
        let action = self.store.get(key)?;
        trace!("cacheability store hit");
        let verdict = (*action).clone();
        self.remember(key, action);
        Some(verdict)
    }

    /// Intern a fresh verdict in the store and remember its handle.
    pub(super) fn intern(&mut self, key: SqlTextHash, action: Action) -> Action {
        let interned = self.store.intern(key, action);
        let verdict = (*interned).clone();
        self.remember(key, interned);
        verdict
    }

    #[cfg(test)]
    pub(super) fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// Record a handle, releasing whatever the LRU evicted to make room.
    fn remember(&mut self, key: SqlTextHash, action: Arc<Action>) {
        if let Some((evicted_key, evicted)) = self.entries.push(key, action) {
            // Drop before releasing: the scrutinee temporary outlives the body,
            // so a handle still held here keeps the store's count above 1 and
            // the release silently does nothing.
            drop(evicted);
            self.store.release(evicted_key);
        }
    }
}

impl Drop for CacheabilityCache {
    fn drop(&mut self) {
        self.drain();
    }
}
