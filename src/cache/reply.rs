//! Reusable single-shot reply channel for the serve hot path.
//!
//! Replaces a per-query `tokio::sync::oneshot` (one heap allocation per cache
//! hit) with a slot allocated once per connection and reused for every query.
//! Per-query plumbing is then an `Arc` refcount bump plus a stack-resident
//! `Notified` future — no heap allocation on the hot path.
//!
//! The connection owns the [`ReplySlot`] and is the sole receiver; it mints one
//! [`ReplySender`] per query via [`ReplySlot::sender`]. The strict
//! one-outstanding-query-per-connection invariant means the slot never holds
//! more than one reply at a time, so reuse is sound.
//!
//! Because the `Notify` is reused, a permit can outlive its query: a sender
//! dropped armed while the connection is NOT waiting (e.g. the
//! dispatch-unavailable fallback forwards to origin and discards the
//! `ProxyMessage`) stores a permit that the next query's wait consumes
//! immediately. A bare wakeup therefore proves nothing — the receiver must
//! consult [`ReplyState`]: minting resets it to `Empty`, so a wakeup that
//! finds `Empty` is a stale permit to wait through, while `Dropped` is the
//! genuine sender-died-in-flight signal. (`Arc::strong_count` cannot stand in
//! for this: `Drop` notifies before the sender's `Arc` decrements, so a
//! genuine drop can still show the sender alive at wake time.)

use std::sync::{Arc, Mutex};

use tokio::sync::Notify;
use tokio::sync::futures::Notified;

/// What the receiver finds in the slot on wakeup.
#[derive(Debug, PartialEq, Eq)]
pub enum ReplyState<T> {
    /// No delivery since the current query's sender was minted: the wakeup
    /// was a stale permit from an earlier never-awaited sender. Re-arm and
    /// keep waiting.
    Empty,
    /// The reply.
    Sent(T),
    /// The current query's sender was dropped without sending — the worker
    /// died with the query in flight.
    Dropped,
}

/// Per-connection reusable reply channel. Allocated once; the state cell and
/// `Notify` are reused across every query on the connection.
pub struct ReplySlot<T> {
    state: Mutex<ReplyState<T>>,
    notify: Notify,
}

// Manual impl: `#[derive(Default)]` would demand `T: Default`, but the slot
// starts `Empty` for any `T`.
impl<T> Default for ReplySlot<T> {
    fn default() -> Self {
        Self {
            state: Mutex::new(ReplyState::Empty),
            notify: Notify::new(),
        }
    }
}

impl<T> ReplySlot<T> {
    /// Mint a sender for one query, resetting the slot to `Empty` so leftover
    /// state from a never-awaited predecessor can't masquerade as this
    /// query's outcome. Refcount bump only — no allocation.
    pub fn sender(self: &Arc<Self>) -> ReplySender<T> {
        *self.state.lock().expect("reply slot not poisoned") = ReplyState::Empty;
        ReplySender {
            slot: Arc::clone(self),
            armed: true,
        }
    }

    /// Receiver-side future, polled in the connection's serve `select!`. Pin a
    /// single instance across the whole wait and poll `&mut` it — recreating
    /// it per iteration races with `notify_one` and loses wakeups. (After a
    /// completed poll it must be re-created; an `Empty` take is the one case
    /// where the wait continues past a completion.)
    pub fn notified(&self) -> Notified<'_> {
        self.notify.notified()
    }

    /// Take the slot state, leaving `Empty`.
    pub fn take(&self) -> ReplyState<T> {
        std::mem::replace(
            &mut *self.state.lock().expect("reply slot not poisoned"),
            ReplyState::Empty,
        )
    }
}

/// Owned, `Send`, one-shot send capability for a single query. Moved through
/// [`ProxyMessage`](super::messages::ProxyMessage) to the worker and may sit in
/// the coalescing wait queue before being fulfilled.
pub struct ReplySender<T> {
    slot: Arc<ReplySlot<T>>,
    armed: bool,
}

impl<T> ReplySender<T> {
    /// Deliver the reply, waking the connection. Mirrors
    /// `oneshot::Sender::send`: returns `Err(reply)` if the receiver (the
    /// connection) has already gone away.
    pub fn send(mut self, reply: T) -> Result<(), T> {
        self.armed = false;
        if Arc::strong_count(&self.slot) == 1 {
            return Err(reply);
        }
        *self.slot.state.lock().expect("reply slot not poisoned") = ReplyState::Sent(reply);
        self.slot.notify.notify_one();
        Ok(())
    }

    /// Discard the sender without sending or signalling. The query never
    /// reached a worker (e.g. the dispatch-unavailable fallback forwards it to
    /// origin), so the connection is not waiting; this leaves the slot
    /// untouched — no `Dropped` state, no `Notify` permit — so a later query's
    /// wait has nothing to trip over. (The slot's mint-reset + `Empty` re-arm
    /// still tolerate a *missed* disarm; this just keeps the common degraded
    /// path from manufacturing a stale permit at all.)
    pub fn disarm(mut self) {
        self.armed = false;
    }
}

impl<T> Drop for ReplySender<T> {
    fn drop(&mut self) {
        if self.armed {
            // Dropped without send: record it and wake the receiver so the
            // connection treats this as cache-died-in-flight rather than
            // hanging. If no wait ever consumes this (the sender was
            // discarded before dispatch), the next mint resets the state and
            // the leftover permit surfaces as a harmless `Empty` wakeup.
            *self.slot.state.lock().expect("reply slot not poisoned") = ReplyState::Dropped;
            self.slot.notify.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_send_then_receive() {
        let slot = Arc::new(ReplySlot::default());
        slot.sender().send(7).expect("receiver present");

        slot.notified().await;
        assert_eq!(slot.take(), ReplyState::Sent(7));
    }

    #[tokio::test]
    async fn test_receive_then_send_eager_permit() {
        let slot = Arc::new(ReplySlot::default());
        let notified = slot.notified();
        tokio::pin!(notified);

        // Send arrives before the receiver awaits; the stored permit must make
        // the next poll complete.
        slot.sender().send(7).expect("receiver present");

        notified.await;
        assert_eq!(slot.take(), ReplyState::Sent(7));
    }

    #[tokio::test]
    async fn test_drop_without_send_wakes_receiver_with_dropped() {
        let slot: Arc<ReplySlot<i32>> = Arc::new(ReplySlot::default());
        let sender = slot.sender();
        drop(sender);

        slot.notified().await;
        assert_eq!(slot.take(), ReplyState::Dropped);
    }

    /// `disarm` (the discard path) leaves no permit and no state change, so a
    /// later query's wait pends rather than waking on a stale permit.
    #[tokio::test]
    async fn test_disarm_leaves_no_permit() {
        let slot: Arc<ReplySlot<i32>> = Arc::new(ReplySlot::default());
        slot.sender().disarm();
        assert_eq!(slot.take(), ReplyState::Empty);

        // A fresh wait does not complete off a leftover permit; only the real
        // send wakes it. `biased` polls the wait first, so a spurious permit
        // would resolve it before the yield.
        let sender = slot.sender();
        let notified = slot.notified();
        tokio::pin!(notified);
        tokio::select! {
            biased;
            _ = &mut notified => panic!("disarm left a spurious permit"),
            _ = tokio::task::yield_now() => {}
        }
        sender.send(7).expect("receiver present");
        notified.await;
        assert_eq!(slot.take(), ReplyState::Sent(7));
    }

    /// The stale-permit hazard: a sender dropped armed while nothing waits
    /// (dispatch-unavailable fallback) leaves a stored permit. The next
    /// query's wait wakes on it, must read `Empty` (not `Dropped` — minting
    /// reset the state), re-arm, and still receive the real reply.
    #[tokio::test]
    async fn test_stale_permit_from_unwaited_drop_reads_empty_then_real_reply() {
        let slot = Arc::new(ReplySlot::default());
        drop(slot.sender());

        let sender = slot.sender();
        let notified = slot.notified();
        tokio::pin!(notified);
        notified.as_mut().await;
        assert_eq!(slot.take(), ReplyState::Empty, "stale permit, not Dropped");

        // Re-arm exactly as the connection's wait loop does.
        notified.set(slot.notified());
        sender.send(7).expect("receiver present");
        notified.await;
        assert_eq!(slot.take(), ReplyState::Sent(7));
    }

    #[tokio::test]
    async fn test_reuse_across_cycles() {
        let slot = Arc::new(ReplySlot::default());
        for i in 0..100 {
            let notified = slot.notified();
            tokio::pin!(notified);
            slot.sender().send(i).expect("receiver present");
            notified.await;
            assert_eq!(slot.take(), ReplyState::Sent(i));
            assert_eq!(slot.take(), ReplyState::Empty, "slot cleared after take");
        }
    }

    #[test]
    fn test_send_after_receiver_dropped_is_err() {
        let slot = Arc::new(ReplySlot::default());
        let sender = slot.sender();
        drop(slot);
        assert_eq!(sender.send(7), Err(7));
    }
}
