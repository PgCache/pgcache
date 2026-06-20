mod cache_sender;
mod client_stream;
mod connection;
mod egress;
mod origin_stream;
mod query;
pub mod search_path;
mod server;
mod tls_stream;

pub use cache_sender::{StatusSender, StatusSenderUpdater};

pub use client_stream::{ClientSocket, OwnedClientReadHalf};

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

use error_set::error_set;
use nix::errno::Errno;
use rootcause::Report;

use crate::pg::cdc::PgCdcError;
use crate::pg::protocol::ProtocolError;

pub use connection::connection_task;
pub use server::proxy_run;

error_set! {
    ConnectionError := FdError || ConnectError || ReadError || WriteError || DegradedModeExit

    FdError := {
        NixError(Errno),
        FdIoError(io::Error),
    }

    ReadError := {
        ProtocolError(ProtocolError),
        IoError(io::Error),
    }

    WriteError := {
        MpscError,
    }

    ConnectError := {
        NoConnection,
        CdcError(PgCdcError),
        TlsError(io::Error),
    }

    DegradedModeExit := {
        CacheDead,
    }

    ParseError := {
        InvalidUtf8,
        Parse(pg_query::Error)
    }
}

/// Result type with location-tracking error reports for connection operations.
pub type ConnectionResult<T> = Result<T, Report<ConnectionError>>;

// Manual From<io::Error> impl for ConnectionError since error_set doesn't do transitive conversions
impl From<io::Error> for ConnectionError {
    fn from(e: io::Error) -> Self {
        ConnectionError::FdIoError(e)
    }
}

/// Current proxy operating mode for a connection.
///
/// These are the two serving-phase sub-states (the connection owns the client
/// write half in both). The "worker is serving" phase is encoded as control
/// flow in `handle_connection` (the write half is lent to the worker and absent),
/// not as a variant — so "write to the client while it's lent" is
/// unrepresentable.
#[derive(Debug)]
pub(crate) enum ProxyMode {
    /// Normal: read client + origin, flush the egress queue.
    Read,
    /// A cacheable query is queued in the egress queue but not yet at the head.
    /// Drain origin and flush egress without reading the client (no read-ahead),
    /// until the cache slot reaches the head and can be dispatched.
    OriginDrain,
}

/// Proxy health status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ProxyStatus {
    Normal = 0,
    Degraded = 1,
}

struct ProxyStatusInner {
    status: AtomicU8,
    /// Set once the proxy's TCP listener is bound. `/readyz` stays not-ready
    /// until then, so clients don't race the bind (the listener binds only
    /// after the ~hundreds-of-ms cache setup completes).
    listening: AtomicBool,
}

/// Shared health/readiness signal for the proxy, read by the HTTP server for
/// `/readyz`. `listening` is flipped by the accept loop after `bind`.
#[derive(Clone)]
pub struct SharedProxyStatus(Arc<ProxyStatusInner>);

impl Default for SharedProxyStatus {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedProxyStatus {
    pub fn new() -> Self {
        Self(Arc::new(ProxyStatusInner {
            status: AtomicU8::new(ProxyStatus::Normal as u8),
            listening: AtomicBool::new(false),
        }))
    }

    pub fn status_set(&self, status: ProxyStatus) {
        self.0.status.store(status as u8, Ordering::Relaxed);
    }

    pub fn status_get(&self) -> ProxyStatus {
        match self.0.status.load(Ordering::Relaxed) {
            0 => ProxyStatus::Normal,
            _ => ProxyStatus::Degraded,
        }
    }

    /// Mark the proxy listener as bound and accepting connections.
    pub fn listening_set(&self) {
        self.0.listening.store(true, Ordering::Release);
    }

    /// Ready once the listener is bound and the proxy is not degraded.
    pub fn is_ready(&self) -> bool {
        self.0.listening.load(Ordering::Acquire) && self.status_get() == ProxyStatus::Normal
    }
}
