mod cache_sender;
mod client_stream;
mod connection;
mod egress;
mod query;
pub mod search_path;
mod server;
mod tls_stream;

pub use cache_sender::{StatusSender, StatusSenderUpdater};

pub use client_stream::{ClientSocket, OwnedClientReadHalf};

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

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

/// Shared atomic wrapper for `ProxyStatus`.
/// Written by the proxy accept loop, read by the HTTP server for `/readyz`.
#[derive(Clone)]
pub struct SharedProxyStatus(Arc<AtomicU8>);

impl Default for SharedProxyStatus {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedProxyStatus {
    pub fn new() -> Self {
        Self(Arc::new(AtomicU8::new(ProxyStatus::Normal as u8)))
    }

    pub fn status_set(&self, status: ProxyStatus) {
        self.0.store(status as u8, Ordering::Relaxed);
    }

    pub fn status_get(&self) -> ProxyStatus {
        match self.0.load(Ordering::Relaxed) {
            0 => ProxyStatus::Normal,
            _ => ProxyStatus::Degraded,
        }
    }

    pub fn is_ready(&self) -> bool {
        self.status_get() == ProxyStatus::Normal
    }
}
