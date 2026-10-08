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
//! The queue also orders the proxy's own **probes** (`SHOW search_path`, `SHOW
//! default_transaction_isolation`, `pg_current_wal_insert_lsn()`): queries the
//! proxy injects on the origin socket whose responses must never reach the
//! client. Origin answers everything on its socket in order, so a probe holds
//! a `Probe` slot at its position in that order and its reply is routed there
//! and swallowed — however many client requests are in flight around it. A
//! probe slot carries nothing for the client and is invisible to the flush
//! driver.
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
//!
//! ## Origin reply order vs client order
//!
//! Origin replies route to the oldest unsealed `Origin` or `Probe` slot. The
//! two orders agree because a `Cache` slot is never on the origin socket and,
//! as the connection reads no further client request while one is queued,
//! nothing behind it is either. A new probe is therefore placed after the last
//! on-wire slot — before the first `Cache` slot, else at the back — so a cache
//! fallback that forwards to origin lands behind it in both orders.

use std::collections::VecDeque;
use std::mem::discriminant;

use ecow::EcoString;
use tokio_util::bytes::Bytes;

/// A query the proxy injects on the origin socket for its own bookkeeping. Its
/// response is swallowed at its `Probe` slot and applied by the connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ProbeKind {
    /// `SHOW search_path` — pre-PG18 discovery (ADR-006).
    SearchPath,
    /// `SHOW default_transaction_isolation` (ADR-054).
    IsolationDefault,
    /// `SELECT pg_current_wal_insert_lsn()`: its LSN bounds the writes recorded
    /// through `stamp_seq` (ADR-048).
    CommitLsn { stamp_seq: u64 },
}

impl ProbeKind {
    fn same_kind(self, other: Self) -> bool {
        discriminant(&self) == discriminant(&other)
    }
}

/// A probe whose `ReadyForQuery` arrived: what it was, and the first column of
/// its one `DataRow` (`None` if the probe errored or returned nothing).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ProbeResult {
    pub(super) kind: ProbeKind,
    pub(super) row: Option<EcoString>,
}

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
    /// An injected probe awaiting origin's reply, which is swallowed here. The
    /// slot exists for its position in origin's reply order; it carries nothing
    /// for the client.
    Probe {
        kind: ProbeKind,
        row: Option<EcoString>,
    },
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

    /// Index of the slot the flush driver acts on: the first non-`Probe` slot.
    fn client_head(&self) -> Option<usize> {
        self.slots
            .iter()
            .position(|s| !matches!(s, EgressSlot::Probe { .. }))
    }

    // --- Probes ------------------------------------------------------------

    /// Reserve the probe's place in origin's reply order: after every slot
    /// already on the origin socket, i.e. before the first `Cache` slot (never
    /// on the socket, and nothing behind it is), else at the back. Call this as
    /// the probe query is queued to origin.
    pub(super) fn probe_push(&mut self, kind: ProbeKind) {
        let slot = EgressSlot::Probe { kind, row: None };
        match self
            .slots
            .iter()
            .position(|s| matches!(s, EgressSlot::Cache { .. }))
        {
            Some(idx) => self.slots.insert(idx, slot),
            None => self.slots.push_back(slot),
        }
    }

    /// Whether a probe of this kind (ignoring its payload) is awaiting a reply.
    pub(super) fn probe_pending(&self, kind: ProbeKind) -> bool {
        self.slots
            .iter()
            .any(|s| matches!(s, EgressSlot::Probe { kind: k, .. } if k.same_kind(kind)))
    }

    /// Whether the next origin reply belongs to a probe (and must be swallowed)
    /// rather than to a client request.
    pub(super) fn origin_reply_is_probe(&mut self) -> bool {
        matches!(self.origin_reply_slot(), Some(EgressSlot::Probe { .. }))
    }

    /// Record the probe reply's first column. No-op if the next reply is not a
    /// probe's.
    pub(super) fn probe_row(&mut self, value: EcoString) {
        if let Some(EgressSlot::Probe { row, .. }) = self.origin_reply_slot() {
            *row = Some(value);
        }
    }

    /// The probe's `ReadyForQuery` arrived: remove its slot and hand back what
    /// it collected. `None` if the next reply is not a probe's.
    pub(super) fn probe_complete(&mut self) -> Option<ProbeResult> {
        let idx = self.origin_reply_index()?;
        match self.slots.get(idx) {
            Some(EgressSlot::Probe { .. }) => {}
            _ => return None,
        }
        match self.slots.remove(idx) {
            Some(EgressSlot::Probe { kind, row }) => Some(ProbeResult { kind, row }),
            _ => None,
        }
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
    /// lazily: this covers phases with no explicit `origin_open` (COPY and
    /// FunctionCall control flows) where no synth or cache slot can
    /// interleave, so lazy ordering is correct. Query forwards and the startup
    /// handshake call `origin_open` explicitly so a later synth/cache/probe
    /// slot can't jump ahead of an origin response whose bytes haven't arrived
    /// yet. Bytes due to a probe are not relayed; route those through
    /// [`Self::probe_row`] instead (guarded by [`Self::origin_reply_is_probe`]).
    pub(super) fn origin_append(&mut self, bytes: Bytes) {
        if bytes.is_empty() {
            return;
        }
        match self.origin_reply_slot() {
            Some(EgressSlot::Origin { buf, .. }) => buf.push_back(bytes),
            Some(_) => debug_assert!(false, "origin bytes appended to a probe slot"),
            None => {
                self.origin_open();
                if let Some(EgressSlot::Origin { buf, .. }) = self.slots.back_mut() {
                    buf.push_back(bytes);
                }
            }
        }
    }

    /// Seal the oldest unsealed `Origin` slot (its `ReadyForQuery` was relayed).
    /// A no-op if none is open (the RFQ's bytes were already appended, opening
    /// and then this seals; a stray seal with nothing open is harmless).
    pub(super) fn origin_seal(&mut self) {
        if let Some(EgressSlot::Origin { sealed, .. }) = self.origin_reply_slot() {
            *sealed = true;
        }
        // A slot sealed while already drained (e.g. a JDBC Describe/Flush
        // response, which carries no ReadyForQuery and was flushed before the
        // seal) is complete and must be removed so it can't block later slots.
        self.prune_done();
    }

    /// Remove fully delivered slots at the client head: a sealed, drained
    /// `Origin` slot. (`Synth` slots are dropped by `advance` when drained;
    /// `Cache` slots only by `cache_done`; `Probe` slots by `probe_complete`.)
    fn prune_done(&mut self) {
        while let Some(idx) = self.client_head()
            && matches!(
                self.slots.get(idx),
                Some(EgressSlot::Origin { buf, sealed: true }) if buf.is_empty()
            )
        {
            self.slots.remove(idx);
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
    /// writes its response directly. The head is the first non-probe slot.
    pub(super) fn has_writable(&self) -> bool {
        match self.client_head().and_then(|idx| self.slots.get(idx)) {
            Some(EgressSlot::Synth { .. }) => true,
            Some(EgressSlot::Origin { buf, .. }) => !buf.is_empty(),
            Some(EgressSlot::Cache { .. } | EgressSlot::Probe { .. }) | None => false,
        }
    }

    /// Next contiguous bytes to write from the head slot, or empty if the head
    /// is not writable.
    pub(super) fn write_chunk(&self) -> &[u8] {
        match self.client_head().and_then(|idx| self.slots.get(idx)) {
            Some(EgressSlot::Synth { buf }) => buf,
            Some(EgressSlot::Origin { buf, .. }) => buf.front().map_or(&[], |b| b),
            _ => &[],
        }
    }

    /// Record that `n` bytes from the head slot were written, popping the slot
    /// when it is fully delivered (a `Synth` slot once drained; an `Origin`
    /// slot once sealed and drained).
    pub(super) fn advance(&mut self, mut n: usize) {
        let Some(idx) = self.client_head() else {
            debug_assert!(n == 0, "advance on empty queue");
            return;
        };
        let done = match self.slots.get_mut(idx) {
            Some(EgressSlot::Synth { buf }) => {
                let take = n.min(buf.len());
                let _ = buf.split_to(take);
                buf.is_empty()
            }
            Some(EgressSlot::Origin { buf, sealed }) => {
                while n > 0 {
                    let Some(chunk) = buf.front_mut() else { break };
                    let take = n.min(chunk.len());
                    let _ = chunk.split_to(take);
                    n -= take;
                    if chunk.is_empty() {
                        buf.pop_front();
                    }
                }
                *sealed && buf.is_empty()
            }
            Some(EgressSlot::Cache { .. } | EgressSlot::Probe { .. }) | None => {
                debug_assert!(n == 0, "advance on a non-writable head");
                false
            }
        };
        if done {
            self.slots.remove(idx);
        }
    }

    /// Transition the head `Cache` slot into the serving state and take its
    /// message for dispatch to the worker. Returns `None` if the head is not a
    /// pending cache slot.
    pub(super) fn cache_dispatch(&mut self) -> Option<C> {
        match self.client_head_mut() {
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
        if let Some(slot @ EgressSlot::Cache { .. }) = self.client_head_mut() {
            *slot = EgressSlot::Origin {
                buf: VecDeque::new(),
                sealed: false,
            };
        }
    }

    /// Pop the head `Cache` slot after its `CacheReply` was received.
    pub(super) fn cache_done(&mut self) {
        let Some(idx) = self.client_head() else {
            debug_assert!(false, "cache_done on an empty queue");
            return;
        };
        debug_assert!(
            matches!(self.slots.get(idx), Some(EgressSlot::Cache { serving, .. }) if *serving),
            "cache_done without a serving cache head",
        );
        self.slots.remove(idx);
    }

    fn client_head_mut(&mut self) -> Option<&mut EgressSlot<C>> {
        let idx = self.client_head()?;
        self.slots.get_mut(idx)
    }

    /// Index of the slot origin's next reply belongs to: the oldest unsealed
    /// `Origin` or `Probe` slot.
    fn origin_reply_index(&self) -> Option<usize> {
        self.slots.iter().position(|s| {
            matches!(
                s,
                EgressSlot::Origin { sealed: false, .. } | EgressSlot::Probe { .. }
            )
        })
    }

    fn origin_reply_slot(&mut self) -> Option<&mut EgressSlot<C>> {
        let idx = self.origin_reply_index()?;
        self.slots.get_mut(idx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The Cache slot payload is opaque to the queue, so tests use a trivial
    // stand-in instead of constructing a real `CacheMessage`.
    type TestQueue = EgressQueue<&'static str>;

    #[test]
    fn test_empty_queue_is_idle() {
        let q = TestQueue::new();
        assert!(q.is_empty());
        assert!(!q.has_writable());
        assert!(q.write_chunk().is_empty());
    }

    #[test]
    fn test_origin_streams_then_pops_on_seal() {
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
    fn test_synth_flushes_and_pops() {
        let mut q = TestQueue::new();
        q.synth_push(Bytes::from_static(b"12345"));
        assert!(q.has_writable());
        q.advance(5);
        assert!(q.is_empty());
    }

    #[test]
    fn test_empty_synth_ignored() {
        let mut q = TestQueue::new();
        q.synth_push(Bytes::new());
        assert!(q.is_empty());
    }

    #[test]
    fn test_synth_does_not_jump_ahead_of_earlier_unsealed_origin() {
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
    fn test_origin_append_targets_oldest_unsealed_slot() {
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
    fn test_probe_is_invisible_to_the_client_driver() {
        // A probe behind R1 never blocks or feeds the client; a synth queued
        // behind the probe flushes while the probe is still outstanding.
        let mut q = TestQueue::new();
        q.origin_open(); // R1
        q.probe_push(ProbeKind::SearchPath);
        q.synth_push(Bytes::from_static(b"S"));

        q.origin_append(Bytes::from_static(b"r1"));
        q.origin_seal();
        assert_eq!(q.write_chunk(), b"r1");
        q.advance(2); // R1 popped; probe now oldest
        assert!(!q.origin_reply_is_probe() || q.has_writable());
        assert_eq!(q.write_chunk(), b"S");
        q.advance(1);
        assert!(!q.is_empty(), "probe still outstanding");
        assert!(!q.has_writable());
        assert!(q.write_chunk().is_empty());
    }

    #[test]
    fn test_probe_reply_routed_by_origin_order() {
        // R1 was on the wire before the probe: its reply comes first and is
        // relayed; only then does the probe's reply get swallowed.
        let mut q = TestQueue::new();
        q.origin_open(); // R1
        q.probe_push(ProbeKind::IsolationDefault);
        assert!(!q.origin_reply_is_probe());
        assert!(q.probe_pending(ProbeKind::IsolationDefault));

        q.origin_append(Bytes::from_static(b"r1"));
        q.origin_seal();
        assert!(q.origin_reply_is_probe());
        q.probe_row("read committed".into());
        let done = q.probe_complete().expect("probe at origin head");
        assert_eq!(done.kind, ProbeKind::IsolationDefault);
        assert_eq!(done.row.as_deref(), Some("read committed"));
        assert!(!q.probe_pending(ProbeKind::IsolationDefault));
        // R1's bytes are still owed to the client.
        assert_eq!(q.write_chunk(), b"r1");
        q.advance(2);
        assert!(q.is_empty());
    }

    #[test]
    fn test_probe_lands_before_a_queued_cache_slot() {
        // A cache slot is never on the wire, so a probe injected while one is
        // queued goes ahead of it in origin order; a fallback forward lands
        // behind the probe in both orders.
        let mut q = TestQueue::new();
        q.cache_push("q1");
        q.probe_push(ProbeKind::CommitLsn { stamp_seq: 7 });
        assert!(q.probe_pending(ProbeKind::CommitLsn { stamp_seq: 0 }));
        assert!(q.origin_reply_is_probe());

        // The cache slot still dispatches past the probe, then falls back.
        assert_eq!(q.cache_dispatch(), Some("q1"));
        q.cache_to_origin();
        // Probe reply first (it was written to origin first).
        assert!(q.origin_reply_is_probe());
        q.probe_row("0/1".into());
        let done = q.probe_complete().expect("probe at origin head");
        assert_eq!(done.kind, ProbeKind::CommitLsn { stamp_seq: 7 });
        // Then the forwarded query's reply.
        assert!(!q.origin_reply_is_probe());
        q.origin_append(Bytes::from_static(b"q1"));
        q.origin_seal();
        assert_eq!(q.write_chunk(), b"q1");
        q.advance(2);
        assert!(q.is_empty());
    }

    #[test]
    fn test_probe_complete_only_at_origin_head() {
        let mut q = TestQueue::new();
        q.origin_open(); // R1 unsealed, ahead of the probe
        q.probe_push(ProbeKind::SearchPath);
        assert!(q.probe_complete().is_none());
        q.origin_seal();
        assert!(q.probe_complete().is_some());
    }

    #[test]
    fn test_cache_dispatches_only_at_head() {
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
