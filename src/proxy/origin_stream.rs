use std::{io, net::SocketAddr, sync::Arc};

use rootcause::Report;
use tokio::net::TcpStream;

use crate::{
    settings::SslMode,
    tls::{self},
};

use super::tls_stream::{TlsReadHalf, TlsStream, TlsWriteHalf};
use super::{ConnectionError, ConnectionResult};
use crate::result::ReportExt;

// ============================================================================
// OriginStream - type aliases using generic TLS stream types
// ============================================================================

/// Origin database connection stream, either plain TCP or TLS-encrypted.
pub type OriginStream = TlsStream<rustls::ClientConnection>;

/// Borrowed read half of an OriginStream.
pub type OriginReadHalf<'a> = TlsReadHalf<'a, rustls::ClientConnection>;

/// Borrowed write half of an OriginStream.
pub type OriginWriteHalf<'a> = TlsWriteHalf<'a, rustls::ClientConnection>;

/// Create an OriginStream from a tokio-rustls TlsStream.
///
/// Decomposes the TlsStream to allow borrowed splits with `.writable()`.
fn origin_stream_from_tls(tls_stream: tokio_rustls::client::TlsStream<TcpStream>) -> OriginStream {
    let (tcp, client_connection) = tls_stream.into_inner();
    TlsStream::Tls {
        tcp,
        tls_state: Arc::new(std::sync::Mutex::new(client_connection)),
    }
}

/// Connect to the origin database server.
/// Tries each address in sequence until one succeeds.
/// If ssl_mode is Require, performs PostgreSQL SSL negotiation and TLS handshake.
pub(super) async fn origin_connect(
    addrs: &[SocketAddr],
    ssl_mode: SslMode,
    server_name: &str,
) -> ConnectionResult<OriginStream> {
    for addr in addrs {
        if let Ok(stream) = TcpStream::connect(addr).await {
            let _ = stream.set_nodelay(true);
            return match ssl_mode {
                SslMode::Disable => Ok(TlsStream::plain(stream)),
                SslMode::Require | SslMode::VerifyFull => {
                    let tls_stream = tls::pg_tls_connect(stream, ssl_mode, server_name)
                        .await
                        .map_err(|e| {
                            Report::from(ConnectionError::TlsError(io::Error::other(
                                e.into_current_context(),
                            )))
                        })
                        .attach_loc("establishing TLS connection")?;
                    Ok(origin_stream_from_tls(tls_stream))
                }
            };
        }
    }
    Err(ConnectionError::NoConnection.into())
}
