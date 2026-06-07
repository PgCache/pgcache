//! TLS support for PostgreSQL connections
//!
//! This module provides TLS configuration and PostgreSQL SSL negotiation
//! for connecting to origin databases that require encryption.
//!
//! Implements the `MakeTlsConnect` trait for tokio-postgres integration.

use std::convert::TryFrom;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use error_set::error_set;
use rootcause::Report;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};

use crate::result::MapIntoReport;
use crate::settings::SslMode;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, ServerConfig, SignatureScheme};
use rustls_pemfile::{certs, private_key};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio_postgres::tls::{ChannelBinding, TlsConnect, TlsStream};
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream as RustlsTlsStream;

error_set! {
    /// Errors that can occur during TLS connection setup
    TlsError := {
        /// I/O error during connection
        IoError(io::Error),
        /// Rustls error during TLS handshake
        RustlsError(rustls::Error),
        /// Invalid DNS name for server
        InvalidDnsName(rustls::pki_types::InvalidDnsNameError),

        /// Server does not support SSL
        #[display("Server does not support SSL/TLS")]
        SslNotSupported,

        /// Unexpected response from server during SSL negotiation
        #[display("Unexpected response during SSL negotiation: {byte}")]
        UnexpectedResponse { byte: u8 },
    }
}

/// Result type with location-tracking error reports for TLS operations.
pub type TlsResult<T> = Result<T, Report<TlsError>>;

/// A certificate verifier that accepts any server certificate.
///
/// Used for `sslmode=require` where we want encryption without verification.
#[derive(Debug)]
struct NoVerifier;

impl ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ED25519,
            SignatureScheme::ED448,
        ]
    }
}

/// Build a rustls ClientConfig that skips server certificate verification.
///
/// This matches PostgreSQL's `sslmode=require` behavior: the connection is
/// encrypted, but the server's certificate is not validated. Suitable for
/// self-signed certificates or private CAs.
pub fn tls_config_no_verify_build() -> Arc<ClientConfig> {
    let config = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerifier))
        .with_no_client_auth();
    Arc::new(config)
}

/// Build a rustls ClientConfig using webpki root certificates.
///
/// This configuration trusts the standard set of root CA certificates
/// and does not use client authentication. Matches PostgreSQL's
/// `sslmode=verify-full` behavior.
pub fn tls_config_verify_build() -> Arc<ClientConfig> {
    let root_store = RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    Arc::new(config)
}

/// Build a rustls ClientConfig that trusts a specific certificate file.
///
/// This is useful for testing with self-signed certificates.
/// The certificate at the given path is added to the trust store.
pub fn tls_config_with_cert(cert_path: &Path) -> io::Result<Arc<ClientConfig>> {
    let certs = certs_load(cert_path)?;

    let mut root_store = RootCertStore::empty();
    for cert in certs {
        root_store
            .add(cert)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    }

    let config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    Ok(Arc::new(config))
}

/// PostgreSQL SSLRequest message (8 bytes)
///
/// Format: 4-byte length (8) + 4-byte code (80877103)
/// This is sent to negotiate SSL/TLS before the normal startup message.
const SSL_REQUEST: &[u8] = &[0x00, 0x00, 0x00, 0x08, 0x04, 0xd2, 0x16, 0x2f];

/// Perform PostgreSQL SSL negotiation and upgrade connection to TLS
///
/// This implements the PostgreSQL SSL negotiation protocol:
/// 1. Send SSLRequest message to the server
/// 2. Wait for response: 'S' (SSL supported) or 'N' (not supported)
/// 3. If 'S', perform TLS handshake
///
/// # Arguments
/// * `stream` - Plain TCP connection to the PostgreSQL server
/// * `ssl_mode` - TLS verification mode (Require = no verify, VerifyFull = verify)
/// * `server_name` - Hostname for TLS certificate verification
///
/// # Returns
/// * `Ok(TlsStream)` - TLS-wrapped connection ready for PostgreSQL protocol
/// * `Err(TlsError)` - If SSL not supported or TLS handshake fails
pub async fn pg_tls_connect(
    mut stream: TcpStream,
    ssl_mode: SslMode,
    server_name: &str,
) -> TlsResult<RustlsTlsStream<TcpStream>> {
    tracing::debug!("pg_tls_connect: sending SSLRequest to {}", server_name);

    // Send SSLRequest
    stream
        .write_all(SSL_REQUEST)
        .await
        .map_into_report::<TlsError>()?;

    // Read response (single byte: 'S' or 'N')
    let mut response = [0u8; 1];
    stream
        .read_exact(&mut response)
        .await
        .map_into_report::<TlsError>()?;

    tracing::debug!("pg_tls_connect: received response byte: {}", response[0]);

    match response[0] {
        b'S' => {
            // Server accepts SSL - proceed with TLS handshake
            tracing::debug!("pg_tls_connect: server accepts SSL, starting TLS handshake");
            let config = match ssl_mode {
                SslMode::Require => tls_config_no_verify_build(),
                SslMode::VerifyFull => tls_config_verify_build(),
                SslMode::Disable => unreachable!("pg_tls_connect called with SslMode::Disable"),
            };
            let connector = TlsConnector::from(config);
            let server_name: ServerName<'_> = server_name
                .to_owned()
                .try_into()
                .map_into_report::<TlsError>()?;
            let tls_stream = connector
                .connect(server_name, stream)
                .await
                .map_into_report::<TlsError>()?;
            tracing::debug!("pg_tls_connect: TLS handshake complete");
            Ok(tls_stream)
        }
        b'N' => Err(TlsError::SslNotSupported.into()),
        byte => Err(TlsError::UnexpectedResponse { byte }.into()),
    }
}

// ============================================================================
// Server-side TLS (accepting client connections)
// ============================================================================

/// Load certificate chain from PEM file
pub fn certs_load(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    certs(&mut reader).collect()
}

/// Load private key from PEM file
pub fn private_key_load(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    private_key(&mut reader)?
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no private key found"))
}

/// Build rustls ServerConfig for accepting TLS connections from clients
pub fn server_tls_config_build(cert_path: &Path, key_path: &Path) -> io::Result<Arc<ServerConfig>> {
    let certs = certs_load(cert_path)?;
    let key = private_key_load(key_path)?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    Ok(Arc::new(config))
}

/// Shared TLS state that can be cloned across read/write halves.
///
/// This allows multiple components (e.g., connection handler and cache worker)
/// to share the same TLS encryption state. The actual I/O is done on duplicated
/// file descriptors, so only the encryption state needs to be shared.
pub type SharedTlsState<T> = Arc<std::sync::Mutex<T>>;

/// Shared TLS state for client-to-proxy connections (proxy acts as server).
pub type ClientTlsState = SharedTlsState<rustls::ServerConnection>;

/// Result of client TLS negotiation
pub enum ClientTlsResult {
    /// Client wants TLS - returns TCP stream and shared TLS state
    Tls {
        tcp_stream: TcpStream,
        tls_state: ClientTlsState,
    },
    /// Client wants plaintext (no SSLRequest sent or TLS not configured)
    Plain(TcpStream),
    /// Peer closed the connection before sending any bytes. This is the common
    /// shape of an L4/TCP health check (connect, then close). The caller should
    /// drop the connection without dialing origin — otherwise every health probe
    /// triggers and discards a full origin TLS handshake.
    Closed,
}

/// Negotiate TLS with a PostgreSQL client
///
/// This implements the server side of PostgreSQL SSL negotiation:
/// 1. Peek at incoming bytes to check for SSLRequest
/// 2. If SSLRequest and TLS is configured: respond 'S' and perform TLS handshake
/// 3. If SSLRequest but no TLS: respond 'N' and continue with plaintext
/// 4. If not SSLRequest: continue with plaintext (client doesn't want TLS)
///
/// # Arguments
/// * `stream` - TCP connection from client
/// * `acceptor` - Optional TLS acceptor (None if TLS not configured)
///
/// # Returns
/// * `ClientTlsResult::Tls` - TLS-wrapped connection
/// * `ClientTlsResult::Plain` - Plaintext connection
pub async fn client_tls_negotiate(
    mut stream: TcpStream,
    acceptor: Option<&TlsAcceptor>,
) -> TlsResult<ClientTlsResult> {
    tracing::debug!(
        "client_tls_negotiate: starting, acceptor={}",
        acceptor.is_some()
    );
    // Peek at first 8 bytes to check for SSLRequest
    let mut peek_buf = [0u8; 8];
    tracing::debug!("client_tls_negotiate: about to peek");
    let n = stream
        .peek(&mut peek_buf)
        .await
        .map_into_report::<TlsError>()?;
    tracing::debug!("client_tls_negotiate: peek returned {} bytes", n);

    if n == 0 {
        // Peer hung up before sending anything — typically an L4 health check.
        tracing::debug!("client_tls_negotiate: peer closed before startup");
        return Ok(ClientTlsResult::Closed);
    }

    if n >= 8 && peek_buf == SSL_REQUEST {
        // Client sent SSLRequest - consume it
        let mut discard = [0u8; 8];
        stream
            .read_exact(&mut discard)
            .await
            .map_into_report::<TlsError>()?;

        if let Some(acceptor) = acceptor {
            // TLS configured - accept SSL
            stream.write_all(b"S").await.map_into_report::<TlsError>()?;
            stream.flush().await.map_into_report::<TlsError>()?;

            tracing::debug!("client_tls_negotiate: accepted SSL, starting TLS handshake");

            // Perform TLS handshake
            let tls_stream = acceptor
                .accept(stream)
                .await
                .map_into_report::<TlsError>()?;

            tracing::debug!("client_tls_negotiate: TLS handshake complete");

            // Decompose TLS stream into TCP stream + shared TLS state
            // This allows the cache worker to encrypt data using the same session
            let (tcp_stream, server_connection) = tls_stream.into_inner();
            let tls_state = Arc::new(std::sync::Mutex::new(server_connection));

            Ok(ClientTlsResult::Tls {
                tcp_stream,
                tls_state,
            })
        } else {
            // No TLS configured - decline SSL
            tracing::debug!("client_tls_negotiate: TLS not configured, responding 'N'");
            stream.write_all(b"N").await.map_into_report::<TlsError>()?;
            stream.flush().await.map_into_report::<TlsError>()?;
            Ok(ClientTlsResult::Plain(stream))
        }
    } else {
        // No SSLRequest - client wants plaintext
        tracing::debug!("client_tls_negotiate: no SSLRequest, continuing with plaintext");
        Ok(ClientTlsResult::Plain(stream))
    }
}

// Re-export TlsAcceptor for convenience
pub use tokio_rustls::TlsAcceptor;

// ============================================================================
// tokio-postgres TLS integration
// ============================================================================

/// A `MakeTlsConnect` implementation using rustls.
///
/// This creates TLS connectors for tokio-postgres connections.
#[derive(Clone)]
pub struct MakeRustlsConnect {
    config: Arc<ClientConfig>,
}

impl MakeRustlsConnect {
    /// Create a new `MakeRustlsConnect` with the given rustls config.
    pub fn new(config: Arc<ClientConfig>) -> Self {
        Self { config }
    }
}

impl<S> tokio_postgres::tls::MakeTlsConnect<S> for MakeRustlsConnect
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = RustlsStream<S>;
    type TlsConnect = RustlsConnect;
    type Error = rustls::pki_types::InvalidDnsNameError;

    fn make_tls_connect(&mut self, hostname: &str) -> Result<Self::TlsConnect, Self::Error> {
        let server_name = ServerName::try_from(hostname.to_owned())?;
        Ok(RustlsConnect {
            config: Arc::clone(&self.config),
            server_name,
        })
    }
}

/// A `TlsConnect` implementation using rustls.
pub struct RustlsConnect {
    config: Arc<ClientConfig>,
    server_name: ServerName<'static>,
}

impl<S> TlsConnect<S> for RustlsConnect
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = RustlsStream<S>;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send>>;

    fn connect(self, stream: S) -> Self::Future {
        Box::pin(async move {
            let connector = TlsConnector::from(self.config);
            let tls_stream = connector
                .connect(self.server_name, stream)
                .await
                .map_err(io::Error::other)?;
            Ok(RustlsStream(tls_stream))
        })
    }
}

/// A TLS stream wrapper that implements `TlsStream` for tokio-postgres.
pub struct RustlsStream<S>(RustlsTlsStream<S>);

impl<S> AsyncRead for RustlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl<S> AsyncWrite for RustlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

impl<S> TlsStream for RustlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn channel_binding(&self) -> ChannelBinding {
        // For simplicity, return no channel binding
        // Full implementation would require computing SHA-256 of server certificate
        // which is used for SCRAM-SHA-256-PLUS authentication
        // Most PostgreSQL deployments work fine without channel binding
        ChannelBinding::none()
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::sync::Once;
    use tokio_postgres::tls::MakeTlsConnect;

    static INIT: Once = Once::new();

    fn init_crypto_provider() {
        INIT.call_once(|| {
            rustls::crypto::aws_lc_rs::default_provider()
                .install_default()
                .expect("install crypto provider");
        });
    }

    #[test]
    fn test_tls_config_verify_build() {
        init_crypto_provider();
        let config = tls_config_verify_build();
        // Just verify it doesn't panic and returns a valid config
        assert!(Arc::strong_count(&config) == 1);
    }

    #[test]
    fn test_tls_config_no_verify_build() {
        init_crypto_provider();
        let config = tls_config_no_verify_build();
        assert!(Arc::strong_count(&config) == 1);
    }

    #[test]
    fn test_ssl_request_format() {
        // Verify the SSLRequest message format
        assert_eq!(SSL_REQUEST.len(), 8);
        // Length field (big-endian u32 = 8)
        assert_eq!(&SSL_REQUEST[0..4], &[0x00, 0x00, 0x00, 0x08]);
        // SSL request code (80877103 in big-endian = 0x04d2162f)
        assert_eq!(&SSL_REQUEST[4..8], &[0x04, 0xd2, 0x16, 0x2f]);
    }

    #[test]
    fn test_make_rustls_connect() {
        init_crypto_provider();
        let config = tls_config_verify_build();
        let mut make_tls: MakeRustlsConnect = MakeRustlsConnect::new(config);
        // Should succeed with valid hostname
        // Need to specify type parameter for MakeTlsConnect trait method
        let result: Result<RustlsConnect, _> = <MakeRustlsConnect as MakeTlsConnect<
            tokio::net::TcpStream,
        >>::make_tls_connect(
            &mut make_tls, "example.com"
        );
        assert!(result.is_ok());
    }
}
