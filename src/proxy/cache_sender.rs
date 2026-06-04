use tokio::sync::{mpsc::Sender, watch};

use crate::cache::StatusRequest;

// ---------------------------------------------------------------------------
// StatusSender / StatusSenderUpdater — watch hot-swap for the admin /status
// channel (the only remaining proxy→cache channel; dispatch is now inline).
// ---------------------------------------------------------------------------

pub type StatusSenderInner = Sender<StatusRequest>;

/// Cloneable wrapper for the admin HTTP server to send status requests.
///
/// Automatically picks up the new writer's channel after a cache restart.
#[derive(Clone)]
pub struct StatusSender {
    rx: watch::Receiver<Option<StatusSenderInner>>,
}

impl StatusSender {
    /// Sends a status request, returning `Err` if the cache is unavailable.
    pub async fn send(&self, req: StatusRequest) -> Result<(), StatusRequest> {
        let maybe_sender = self.rx.borrow().clone();
        match maybe_sender {
            Some(sender) => sender.send(req).await.map_err(|e| e.0),
            None => Err(req),
        }
    }
}

/// Server-side updater for the status watch channel.
///
/// Held by `ProxyCacheState`; calls `sender_update` on restart, `sender_clear`
/// when the cache exits.
pub struct StatusSenderUpdater {
    tx: watch::Sender<Option<StatusSenderInner>>,
}

impl StatusSenderUpdater {
    /// Creates a new updater and initial subscriber.
    pub fn new(initial: StatusSenderInner) -> (Self, StatusSender) {
        let (tx, rx) = watch::channel(Some(initial));
        (Self { tx }, StatusSender { rx })
    }

    /// Updates all subscribers with a new status sender (called on successful restart).
    pub fn sender_update(&self, new: StatusSenderInner) {
        let _ = self.tx.send(Some(new));
    }

    /// Clears the status sender, marking cache as unavailable (called on cache exit).
    pub fn sender_clear(&self) {
        let _ = self.tx.send(None);
    }
}
