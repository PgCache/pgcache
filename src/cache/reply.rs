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

use std::sync::{Arc, Mutex};

use tokio::sync::Notify;
use tokio::sync::futures::Notified;

/// Per-connection reusable reply channel. Allocated once; the `value` cell and
/// `Notify` are reused across every query on the connection.
pub struct ReplySlot<T> {
    value: Mutex<Option<T>>,
    notify: Notify,
}

// Manual impl: `#[derive(Default)]` would demand `T: Default`, but the slot
// starts empty (`None`) for any `T`.
impl<T> Default for ReplySlot<T> {
    fn default() -> Self {
        Self {
            value: Mutex::new(None),
            notify: Notify::new(),
        }
    }
}

impl<T> ReplySlot<T> {
    /// Mint a sender for one query. Refcount bump only — no allocation.
    pub fn sender(self: &Arc<Self>) -> ReplySender<T> {
        ReplySender {
            slot: Arc::clone(self),
            armed: true,
        }
    }

    /// Receiver-side future, polled in the connection's serve `select!`. Pin a
    /// single instance across the whole loop and poll `&mut` it — recreating it
    /// per iteration races with `notify_one` and loses wakeups.
    pub fn notified(&self) -> Notified<'_> {
        self.notify.notified()
    }

    /// Take the delivered reply. `None` means the sender was dropped without
    /// sending — the worker died with the query in flight.
    pub fn take(&self) -> Option<T> {
        self.value.lock().expect("reply slot not poisoned").take()
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
        if Arc::strong_count(&self.slot) == 1 {
            self.armed = false;
            return Err(reply);
        }
        self.armed = false;
        *self.slot.value.lock().expect("reply slot not poisoned") = Some(reply);
        self.slot.notify.notify_one();
        Ok(())
    }
}

impl<T> Drop for ReplySender<T> {
    fn drop(&mut self) {
        if self.armed {
            // Dropped without send: wake the receiver to a `None` take so the
            // connection treats this as cache-died-in-flight rather than hanging.
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
        assert_eq!(slot.take(), Some(7));
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
        assert_eq!(slot.take(), Some(7));
    }

    #[tokio::test]
    async fn test_drop_without_send_wakes_receiver_with_none() {
        let slot: Arc<ReplySlot<i32>> = Arc::new(ReplySlot::default());
        let sender = slot.sender();
        drop(sender);

        slot.notified().await;
        assert_eq!(slot.take(), None);
    }

    #[tokio::test]
    async fn test_reuse_across_cycles() {
        let slot = Arc::new(ReplySlot::default());
        for i in 0..100 {
            let notified = slot.notified();
            tokio::pin!(notified);
            slot.sender().send(i).expect("receiver present");
            notified.await;
            assert_eq!(slot.take(), Some(i));
            assert_eq!(slot.take(), None, "slot cleared after take");
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
