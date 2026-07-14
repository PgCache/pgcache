//! Request coalescing: fan one cache response out to every client that was
//! waiting on the same fingerprint.

use tokio::io::AsyncWriteExt;
use tokio::sync::broadcast::{self, error::RecvError};
use tokio::task::JoinHandle;
use tokio_util::bytes::Bytes;

use super::super::messages::{CacheOutcome, CacheReply};
use super::super::query_cache::{CoalescedClient, ServeRequest};
use super::super::write_queue::WriteQueue;

/// Outcome of a coalesced client's write task.
pub enum CoalescedOutcome {
    /// All bytes were delivered successfully.
    Complete(CoalescedClient),
    /// Write failed or broadcast lagged — byte stream is corrupted.
    Failed(CoalescedClient),
}

/// Broadcast state for coalesced request handling.
pub(super) struct BroadcastState {
    pub(super) tx: broadcast::Sender<Bytes>,
    pub(super) tasks: Vec<JoinHandle<Result<CoalescedClient, CoalescedClient>>>,
}

/// Push bytes to the primary WriteQueue and broadcast to coalesced clients.
pub(super) fn push_and_broadcast(
    write_queue: &mut WriteQueue,
    broadcast: &Option<BroadcastState>,
    data: impl Into<Bytes>,
) {
    if let Some(bc) = broadcast {
        let bytes: Bytes = data.into();
        let _ = bc.tx.send(bytes.clone());
        write_queue.push(bytes);
    } else {
        write_queue.push(data);
    }
}

/// Create broadcast channel and spawn per-client write tasks.
/// Returns None if there are no coalesced clients.
pub(super) fn broadcast_setup(msg: &mut ServeRequest) -> Option<BroadcastState> {
    if msg.coalesced.is_empty() {
        return None;
    }

    let (tx, _) = broadcast::channel::<Bytes>(64);

    let tasks = msg
        .coalesced
        .drain(..)
        .map(|mut client| {
            let mut rx = tx.subscribe();
            tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(chunk) => {
                            if client.client_socket.write_all(&chunk).await.is_err() {
                                return Err(client);
                            }
                        }
                        Err(RecvError::Closed) => return Ok(client),
                        Err(RecvError::Lagged(_)) => return Err(client),
                    }
                }
            })
        })
        .collect();

    Some(BroadcastState { tx, tasks })
}

/// Drop the broadcast sender, join all tasks, and collect outcomes.
pub(super) async fn broadcast_join(bc: BroadcastState) -> Vec<CoalescedOutcome> {
    drop(bc.tx);

    let mut outcomes = Vec::with_capacity(bc.tasks.len());
    for task in bc.tasks {
        match task.await {
            Ok(Ok(client)) => outcomes.push(CoalescedOutcome::Complete(client)),
            Ok(Err(client)) => outcomes.push(CoalescedOutcome::Failed(client)),
            Err(_) => {} // JoinError — task panicked
        }
    }
    outcomes
}

/// Drop the broadcast sender, join all tasks, and send Error replies.
/// Used when the primary path fails after broadcast was created.
pub(super) async fn broadcast_error_reply(bc: BroadcastState) {
    drop(bc.tx);

    for task in bc.tasks {
        let client = match task.await {
            Ok(Ok(c)) | Ok(Err(c)) => c,
            Err(_) => continue,
        };
        let _ = client.reply_tx.send(CacheReply {
            socket: client.client_socket,
            outcome: CacheOutcome::Error(client.data),
        });
    }
}
