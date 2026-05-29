//! Ordered egress queue — the single source of truth for the order in which
//! client-bound responses leave a connection.
//!
//! A connection's client responses are produced by three sources that can
//! complete out of order:
//! * **origin relay** — bytes streamed back from the origin database,
//! * **synth** — locally synthesized responses (e.g. cached ParseComplete +
//!   Describe), and
//! * **cache** — responses produced by the cache worker.
//!
//! Each accepted client request reserves a slot here in receive order. Slots
//! flush to the client strictly in order, and a slot may flush only once it is
//! complete *and* every earlier slot has flushed. This replaces the former ad
//! hoc coordination (`client_write_buf` + `pending_synth` + the
//! `origin_inflight_syncs` gate) with one structure whose invariant is easy to
//! state and check.
//!
//! ## Flush model
//!
//! Only the **head** slot is ever written, because origin answers requests in
//! FIFO order and locally-produced slots are complete the moment they are
//! enqueued:
//! * `Origin` — its already-arrived bytes may stream out as they arrive; the
//!   slot is popped once it is sealed (its `ReadyForQuery` relayed) and drained.
//! * `Synth` — complete on enqueue; popped once drained.
//! * `Cache` — carries no bytes here. When it reaches the head it signals the
//!   connection to dispatch the query to the worker (which writes the response
//!   directly to the client socket); the slot is popped on `CacheReply`.
//!
//! Because the `Cache` slot only dispatches once it is at the head, the worker
//! never writes while earlier responses are still pending — closing the
//! interleaving/reordering hazard (PGC-213) by construction.

use std::collections::VecDeque;

use tokio_util::bytes::Bytes;

/// One pending client-bound response, in receive order. Generic over the cache
/// payload `C` (the proxy uses `CacheMessage`); the queue treats it opaquely.
enum EgressSlot<C> {
    /// Response relayed from origin. `buf` accumulates relayed bytes; `sealed`
    /// is set when the request's `ReadyForQuery` has been relayed, meaning no
    /// further bytes will be appended.
    Origin { buf: VecDeque<Bytes>, sealed: bool },
    /// Locally synthesized response — complete the moment it is enqueued.
    Synth { buf: Bytes },
    /// Cacheable query served by the worker. The message is taken when the slot
    /// reaches the head and is dispatched; `serving` then marks the in-flight
    /// window until `CacheReply`.
    Cache { msg: Option<C>, serving: bool },
}

/// Ordered queue of pending client responses for one connection. Generic over
/// the cache payload `C` so the abstraction is decoupled from `CacheMessage`.
pub(super) struct EgressQueue<C> {
    slots: VecDeque<EgressSlot<C>>,
}

impl<C> EgressQueue<C> {
    pub(super) fn new() -> Self {
        Self {
            slots: VecDeque::new(),
        }
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    // --- Producers ---------------------------------------------------------

    /// Reserve a slot for a client-facing forward to origin. Call this when a
    /// query (or the startup/auth handshake) is sent to origin and its response
    /// will be relayed to the client. Injected, fully-swallowed origin queries
    /// (e.g. a proactive `SHOW search_path`) must NOT open a slot — their bytes
    /// never reach the client.
    pub(super) fn origin_open(&mut self) {
        self.slots.push_back(EgressSlot::Origin {
            buf: VecDeque::new(),
            sealed: false,
        });
    }

    /// Append relayed origin bytes to the oldest unsealed `Origin` slot — the
    /// request origin is currently answering. If no slot is open, one is opened
    /// lazily: this covers phases with no explicit `origin_open` (the
    /// startup/auth handshake, COPY/FunctionCall control flows) where no synth
    /// or cache slot can interleave, so lazy ordering is correct. Query forwards
    /// still call `origin_open` explicitly so a later synth/cache slot can't
    /// jump ahead of an origin response whose bytes haven't arrived yet.
    pub(super) fn origin_append(&mut self, bytes: Bytes) {
        if bytes.is_empty() {
            return;
        }
        if self.oldest_unsealed_origin().is_none() {
            self.origin_open();
        }
        // Always matches: `oldest_unsealed_origin` only yields `Origin` slots and
        // one was just ensured to exist.
        if let Some(EgressSlot::Origin { buf, .. }) = self.oldest_unsealed_origin() {
            buf.push_back(bytes);
        }
    }

    /// Seal the oldest unsealed `Origin` slot (its `ReadyForQuery` was relayed).
    /// A no-op if none is open (the RFQ's bytes were already appended, opening
    /// and then this seals; a stray seal with nothing open is harmless).
    pub(super) fn origin_seal(&mut self) {
        if let Some(EgressSlot::Origin { sealed, .. }) = self.oldest_unsealed_origin() {
            *sealed = true;
        }
        // A slot sealed while already drained (e.g. a JDBC Describe/Flush
        // response, which carries no ReadyForQuery and was flushed before the
        // seal) is complete and must be removed so it can't block later slots.
        self.prune_done();
    }

    /// Remove leading slots that are fully delivered: a sealed, drained
    /// `Origin` slot. (`Synth` slots are dropped by `advance` when drained;
    /// `Cache` slots only by `cache_done`.)
    fn prune_done(&mut self) {
        while matches!(
            self.slots.front(),
            Some(EgressSlot::Origin { buf, sealed: true }) if buf.is_empty()
        ) {
            self.slots.pop_front();
        }
    }

    /// Enqueue a fully-formed synthesized response.
    pub(super) fn synth_push(&mut self, buf: Bytes) {
        if !buf.is_empty() {
            self.slots.push_back(EgressSlot::Synth { buf });
        }
    }

    /// Enqueue a cacheable query to be served by the worker when it reaches the
    /// head of the queue.
    pub(super) fn cache_push(&mut self, msg: C) {
        self.slots.push_back(EgressSlot::Cache {
            msg: Some(msg),
            serving: false,
        });
    }

    // --- Driver ------------------------------------------------------------

    /// Whether the head slot has bytes ready to write to the client right now.
    /// A `Cache` slot is never writable here — it is dispatched to the worker
    /// (via [`Self::cache_dispatch`]) when it reaches the head, and the worker
    /// writes its response directly.
    pub(super) fn has_writable(&self) -> bool {
        match self.slots.front() {
            Some(EgressSlot::Synth { .. }) => true,
            Some(EgressSlot::Origin { buf, .. }) => !buf.is_empty(),
            Some(EgressSlot::Cache { .. }) | None => false,
        }
    }

    /// Next contiguous bytes to write from the head slot, or empty if the head
    /// is not writable.
    pub(super) fn write_chunk(&self) -> &[u8] {
        match self.slots.front() {
            Some(EgressSlot::Synth { buf }) => buf,
            Some(EgressSlot::Origin { buf, .. }) => buf.front().map_or(&[], |b| b),
            _ => &[],
        }
    }

    /// Record that `n` bytes from the head slot were written, popping the slot
    /// when it is fully delivered (a `Synth` slot once drained; an `Origin`
    /// slot once sealed and drained).
    pub(super) fn advance(&mut self, mut n: usize) {
        let Some(front) = self.slots.front_mut() else {
            debug_assert!(n == 0, "advance on empty queue");
            return;
        };
        match front {
            EgressSlot::Synth { buf } => {
                let take = n.min(buf.len());
                let _ = buf.split_to(take);
                if buf.is_empty() {
                    self.slots.pop_front();
                }
            }
            EgressSlot::Origin { buf, sealed } => {
                while n > 0 {
                    let Some(chunk) = buf.front_mut() else { break };
                    let take = n.min(chunk.len());
                    let _ = chunk.split_to(take);
                    n -= take;
                    if chunk.is_empty() {
                        buf.pop_front();
                    }
                }
                if *sealed && buf.is_empty() {
                    self.slots.pop_front();
                }
            }
            EgressSlot::Cache { .. } => debug_assert!(n == 0, "advance on cache head"),
        }
    }

    /// Transition the head `Cache` slot into the serving state and take its
    /// message for dispatch to the worker. Returns `None` if the head is not a
    /// pending cache slot.
    pub(super) fn cache_dispatch(&mut self) -> Option<C> {
        match self.slots.front_mut() {
            Some(EgressSlot::Cache { msg, serving }) if !*serving => {
                *serving = true;
                msg.take()
            }
            _ => None,
        }
    }

    /// Replace the head `Cache` slot with a fresh `Origin` slot in place — used
    /// when a dispatched cacheable query falls back to origin (cache miss/error,
    /// search_path unknown, socket creation failure, or the worker being
    /// unavailable). The slot keeps its position so the forwarded response stays
    /// correctly ordered ahead of any later pipelined slots. The caller already
    /// holds the query message (taken by `cache_dispatch`), so it is not
    /// returned. No-op if the head is not a cache slot.
    pub(super) fn cache_to_origin(&mut self) {
        if let Some(slot @ EgressSlot::Cache { .. }) = self.slots.front_mut() {
            *slot = EgressSlot::Origin {
                buf: VecDeque::new(),
                sealed: false,
            };
        }
    }

    /// Pop the head `Cache` slot after its `CacheReply` was received.
    pub(super) fn cache_done(&mut self) {
        debug_assert!(
            matches!(self.slots.front(), Some(EgressSlot::Cache { serving, .. }) if *serving),
            "cache_done without a serving cache head",
        );
        self.slots.pop_front();
    }

    fn oldest_unsealed_origin(&mut self) -> Option<&mut EgressSlot<C>> {
        self.slots
            .iter_mut()
            .find(|s| matches!(s, EgressSlot::Origin { sealed, .. } if !*sealed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The Cache slot payload is opaque to the queue, so tests use a trivial
    // stand-in instead of constructing a real `CacheMessage`.
    type TestQueue = EgressQueue<&'static str>;

    #[test]
    fn empty_queue_is_idle() {
        let q = TestQueue::new();
        assert!(q.is_empty());
        assert!(!q.has_writable());
        assert!(q.write_chunk().is_empty());
    }

    #[test]
    fn origin_streams_then_pops_on_seal() {
        let mut q = TestQueue::new();
        q.origin_open();
        q.origin_append(Bytes::from_static(b"row1"));
        assert!(q.has_writable());
        assert_eq!(q.write_chunk(), b"row1");

        // Partial write within the chunk.
        q.advance(2);
        assert_eq!(q.write_chunk(), b"w1");
        q.advance(2);
        // Unsealed and drained → still present (more may arrive), but idle.
        assert!(!q.is_empty());
        assert!(!q.has_writable());

        q.origin_append(Bytes::from_static(b"Z"));
        q.origin_seal();
        assert!(q.has_writable());
        q.advance(1);
        // Sealed and drained → popped.
        assert!(q.is_empty());
    }

    #[test]
    fn synth_flushes_and_pops() {
        let mut q = TestQueue::new();
        q.synth_push(Bytes::from_static(b"12345"));
        assert!(q.has_writable());
        q.advance(5);
        assert!(q.is_empty());
    }

    #[test]
    fn empty_synth_ignored() {
        let mut q = TestQueue::new();
        q.synth_push(Bytes::new());
        assert!(q.is_empty());
    }

    #[test]
    fn synth_does_not_jump_ahead_of_earlier_unsealed_origin() {
        // R1 forwarded (origin), R2 synth: the synth must wait behind R1.
        let mut q = TestQueue::new();
        q.origin_open(); // R1
        q.synth_push(Bytes::from_static(b"R2")); // R2

        // R1 has no bytes yet → head idle even though R2 is ready.
        assert!(!q.has_writable());

        // R1's bytes arrive and seal.
        q.origin_append(Bytes::from_static(b"R1"));
        q.origin_seal();
        assert_eq!(q.write_chunk(), b"R1");
        q.advance(2); // R1 done → popped
        // Now R2 (synth) is at the head.
        assert!(q.has_writable());
        assert_eq!(q.write_chunk(), b"R2");
        q.advance(2);
        assert!(q.is_empty());
    }

    #[test]
    fn origin_append_targets_oldest_unsealed_slot() {
        // R1 and R3 both forwarded; origin answers R1 first.
        let mut q = TestQueue::new();
        q.origin_open(); // R1
        q.origin_open(); // R3
        q.origin_append(Bytes::from_static(b"r1"));
        q.origin_seal(); // seals R1
        q.origin_append(Bytes::from_static(b"r3")); // now targets R3

        assert_eq!(q.write_chunk(), b"r1");
        q.advance(2); // R1 popped
        assert_eq!(q.write_chunk(), b"r3");
        q.origin_seal();
        q.advance(2);
        assert!(q.is_empty());
    }

    #[test]
    fn cache_dispatches_only_at_head() {
        let mut q = TestQueue::new();
        q.origin_open(); // R1 ahead of the cache query
        q.cache_push("q2"); // R2 cacheable

        // R1 not yet flushed → cache must not dispatch.
        q.origin_append(Bytes::from_static(b"r1"));
        q.origin_seal();
        assert!(q.has_writable());
        assert!(q.cache_dispatch().is_none());
        q.advance(2); // R1 popped

        // Now the cache slot is at the head.
        assert!(!q.has_writable()); // not writable, but dispatchable
        assert!(q.cache_dispatch().is_some());
        // Dispatched → serving → idle (worker writes directly).
        assert!(!q.has_writable());
        assert!(q.cache_dispatch().is_none());

        q.cache_done();
        assert!(q.is_empty());
    }
}
