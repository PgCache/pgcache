//! Generic TLS stream types for both client and origin connections.
//!
//! This module provides stream abstractions that support:
//! - Borrowed splits with `.writable()` for select loops
//! - TLS encryption/decryption via rustls
//!
//! The key insight is that `.writable()` checks TCP-level writability, which
//! works the same for both plain and TLS connections - it's about the underlying
//! socket being ready to accept data.

use std::io;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf, ReadHalf, WriteHalf};

use tracing::{debug, trace};

use crate::tls::SharedTlsState;

// ============================================================================
// TlsConnectionOps - trait for rustls connection types
// ============================================================================

/// Trait abstracting over rustls ServerConnection and ClientConnection.
///
/// Both connection types provide the same operations for TLS I/O, but don't
/// share a common trait in rustls. This trait allows generic code to work
/// with either connection type.
pub trait TlsConnectionOps: Send + 'static {
    /// Read decrypted plaintext from the TLS connection.
    fn reader_read(&mut self, buf: &mut [u8]) -> io::Result<usize>;

    /// Write plaintext to be encrypted by the TLS connection.
    fn writer_write_all(&mut self, buf: &[u8]) -> io::Result<()>;

    /// Read ciphertext from an external source into the TLS engine.
    fn read_tls<R: std::io::Read>(&mut self, rd: &mut R) -> io::Result<usize>;

    /// Write ciphertext from the TLS engine to an external sink.
    fn write_tls<W: std::io::Write>(&mut self, wr: &mut W) -> io::Result<usize>;

    /// Process received ciphertext into plaintext.
    /// Returns IoState indicating how many bytes are available.
    fn process_new_packets(&mut self) -> Result<rustls::IoState, rustls::Error>;

    /// Queue a TLS close_notify alert.
    fn send_close_notify(&mut self);
}

impl TlsConnectionOps for rustls::ServerConnection {
    fn reader_read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        std::io::Read::read(&mut self.reader(), buf)
    }

    fn writer_write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        std::io::Write::write_all(&mut self.writer(), buf)
    }

    fn read_tls<R: std::io::Read>(&mut self, rd: &mut R) -> io::Result<usize> {
        self.deref_mut().read_tls(rd)
    }

    fn write_tls<W: std::io::Write>(&mut self, wr: &mut W) -> io::Result<usize> {
        self.deref_mut().write_tls(wr)
    }

    fn process_new_packets(&mut self) -> Result<rustls::IoState, rustls::Error> {
        self.deref_mut().process_new_packets()
    }

    fn send_close_notify(&mut self) {
        self.deref_mut().send_close_notify();
    }
}

impl TlsConnectionOps for rustls::ClientConnection {
    fn reader_read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        std::io::Read::read(&mut self.reader(), buf)
    }

    fn writer_write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        std::io::Write::write_all(&mut self.writer(), buf)
    }

    fn read_tls<R: std::io::Read>(&mut self, rd: &mut R) -> io::Result<usize> {
        self.deref_mut().read_tls(rd)
    }

    fn write_tls<W: std::io::Write>(&mut self, wr: &mut W) -> io::Result<usize> {
        self.deref_mut().write_tls(wr)
    }

    fn process_new_packets(&mut self) -> Result<rustls::IoState, rustls::Error> {
        self.deref_mut().process_new_packets()
    }

    fn send_close_notify(&mut self) {
        self.deref_mut().send_close_notify();
    }
}

// ============================================================================
// TlsStream - generic stream supporting plain TCP or TLS
// ============================================================================

/// Generic stream supporting both plain TCP and TLS-encrypted connections.
///
/// For TLS connections, the TCP stream and TLS state are stored separately
/// to allow borrowed splits where both halves can access the TLS state.
pub enum TlsStream<T: TlsConnectionOps> {
    /// Plain TCP connection (no encryption)
    Plain(TcpStream),
    /// TLS-encrypted connection
    Tls {
        tcp: TcpStream,
        tls_state: SharedTlsState<T>,
    },
}

impl<T: TlsConnectionOps> TlsStream<T> {
    /// Create a plain TCP stream.
    pub fn plain(tcp: TcpStream) -> Self {
        TlsStream::Plain(tcp)
    }

    /// Create a TLS stream with existing TLS state.
    pub fn tls(tcp: TcpStream, tls_state: SharedTlsState<T>) -> Self {
        TlsStream::Tls { tcp, tls_state }
    }

    /// Split into borrowed read and write halves.
    ///
    /// The write half has `.writable()` which delegates to the underlying TCP stream.
    /// Both halves share the TLS state for encrypted connections.
    pub fn split(&mut self) -> (TlsReadHalf<'_, T>, TlsWriteHalf<'_, T>) {
        match self {
            TlsStream::Plain(tcp) => {
                let (read, write) = tcp.split();
                (TlsReadHalf::Plain(read), TlsWriteHalf::Plain(write))
            }
            TlsStream::Tls { tcp, tls_state } => {
                let (read, write) = tcp.split();
                (
                    TlsReadHalf::Tls {
                        tcp: read,
                        tls_state: Arc::clone(tls_state),
                    },
                    TlsWriteHalf::Tls {
                        tcp: write,
                        tls_state: Arc::clone(tls_state),
                        pending: None,
                    },
                )
            }
        }
    }

    /// Consume the stream into **owned** read and write halves that share one
    /// reactor registration. Unlike [`Self::split`], the halves are `'static`
    /// and `Send`, so the write half can be moved to another task (e.g. leased
    /// to the cache worker). Both halves share the TLS state.
    pub fn into_split(self) -> (OwnedTlsReadHalf<T>, OwnedTlsWriteHalf<T>) {
        match self {
            TlsStream::Plain(tcp) => {
                let (read, write) = tcp.into_split();
                (
                    OwnedTlsReadHalf::Plain(read),
                    OwnedTlsWriteHalf::Plain(write),
                )
            }
            TlsStream::Tls { tcp, tls_state } => {
                let (read, write) = tcp.into_split();
                (
                    OwnedTlsReadHalf::Tls {
                        tcp: read,
                        tls_state: Arc::clone(&tls_state),
                    },
                    OwnedTlsWriteHalf::Tls {
                        tcp: write,
                        tls_state,
                        pending: None,
                    },
                )
            }
        }
    }
}

// ============================================================================
// TlsReadHalf - borrowed read half with TLS support
// ============================================================================

/// Borrowed read half of a TlsStream.
pub enum TlsReadHalf<'a, T: TlsConnectionOps> {
    Plain(ReadHalf<'a>),
    Tls {
        tcp: ReadHalf<'a>,
        tls_state: SharedTlsState<T>,
    },
}

impl<T: TlsConnectionOps> AsyncRead for TlsReadHalf<'_, T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut *self {
            TlsReadHalf::Plain(tcp) => Pin::new(tcp).poll_read(cx, buf),
            TlsReadHalf::Tls { tcp, tls_state } => poll_tls_read(tcp, tls_state, cx, buf),
        }
    }
}

/// Poll TLS read: read ciphertext from TCP, decrypt, return plaintext.
///
/// Single loop with three phases — ordering is critical:
/// 1. Drain all unconsumed ciphertext into rustls first, since `tcp_buf` is
///    stack-local and would be lost if we returned early
/// 2. Only after all ciphertext is consumed, check for buffered plaintext
/// 3. Read more ciphertext from TCP
fn poll_tls_read<T, R>(
    tcp: &mut R,
    tls_state: &SharedTlsState<T>,
    cx: &mut Context<'_>,
    buf: &mut ReadBuf<'_>,
) -> Poll<io::Result<()>>
where
    T: TlsConnectionOps,
    R: AsyncRead + Unpin,
{
    let mut tcp_buf = [0u8; 16 * 1024];
    let mut consumed = 0usize;
    let mut filled = 0usize;

    loop {
        // Phase 1: drain unconsumed ciphertext into rustls. Must complete
        // before returning plaintext — tcp_buf is stack-local and would be
        // lost across calls.
        if let Some(remaining @ &[_, ..]) = tcp_buf.get(consumed..filled) {
            let mut tls = tls_state
                .lock()
                .map_err(|_| io::Error::other("TLS state lock poisoned"))?;

            let mut cursor = std::io::Cursor::new(remaining);
            match tls.read_tls(&mut cursor) {
                Ok(n) => {
                    trace!("tls: read_tls consumed {n}/{} bytes", remaining.len());
                    consumed += n;
                }
                Err(e) => return Poll::Ready(Err(e)),
            }

            if let Err(e) = tls.process_new_packets() {
                debug!("tls: process_new_packets failed, consumed={consumed} filled={filled}: {e}");
                return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidData, e)));
            }

            continue;
        }

        // Phase 2: all ciphertext consumed, check for buffered plaintext
        {
            let mut tls = tls_state
                .lock()
                .map_err(|_| io::Error::other("TLS state lock poisoned"))?;

            let unfilled = buf.initialize_unfilled();
            match tls.reader_read(unfilled) {
                Ok(0) => {}
                Ok(n) => {
                    buf.advance(n);
                    return Poll::Ready(Ok(()));
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => return Poll::Ready(Err(e)),
            }
        }

        // Phase 3: read ciphertext from TCP
        let mut read_buf = ReadBuf::new(&mut tcp_buf);
        match Pin::new(&mut *tcp).poll_read(cx, &mut read_buf) {
            Poll::Ready(Ok(())) => {
                filled = read_buf.filled().len();
                consumed = 0;
                if filled == 0 {
                    return Poll::Ready(Ok(()));
                }
                trace!("tls: tcp read {filled} bytes");
            }
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Pending => return Poll::Pending,
        }
    }
}

// ============================================================================
// PendingWrite - buffered ciphertext awaiting TCP write
// ============================================================================

/// Ciphertext that was encrypted but not yet fully written to TCP.
///
/// When `poll_write` returns `Pending` or achieves only a partial write,
/// the already-encrypted ciphertext must be retained. Re-encrypting the same
/// plaintext would advance the TLS sequence number, causing the peer to fail
/// decryption due to an AEAD nonce mismatch.
pub struct PendingWrite {
    ciphertext: Vec<u8>,
    /// Number of ciphertext bytes already written to TCP.
    offset: usize,
    /// Original plaintext length, returned to the caller on completion.
    plaintext_len: usize,
}

// ============================================================================
// TlsWriteHalf - borrowed write half with .writable() and TLS support
// ============================================================================

/// Borrowed write half of a TlsStream.
///
/// Has `.writable()` method that delegates to the underlying TCP stream,
/// providing proper backpressure handling in select loops.
pub enum TlsWriteHalf<'a, T: TlsConnectionOps> {
    Plain(WriteHalf<'a>),
    Tls {
        tcp: WriteHalf<'a>,
        tls_state: SharedTlsState<T>,
        pending: Option<PendingWrite>,
    },
}

impl<T: TlsConnectionOps> TlsWriteHalf<'_, T> {
    /// Wait for the underlying TCP socket to be writable.
    ///
    /// This delegates to the TCP stream's `.writable()` method, which properly
    /// integrates with tokio's reactor for efficient backpressure handling.
    pub async fn writable(&self) -> io::Result<()> {
        match self {
            TlsWriteHalf::Plain(tcp) => tcp.writable().await,
            TlsWriteHalf::Tls { tcp, .. } => tcp.writable().await,
        }
    }
}

impl<T: TlsConnectionOps> AsyncWrite for TlsWriteHalf<'_, T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            TlsWriteHalf::Plain(tcp) => Pin::new(tcp).poll_write(cx, buf),
            TlsWriteHalf::Tls {
                tcp,
                tls_state,
                pending,
            } => tls_poll_write(Pin::new(tcp), tls_state, cx, buf, pending),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            TlsWriteHalf::Plain(tcp) => Pin::new(tcp).poll_flush(cx),
            TlsWriteHalf::Tls { tcp, .. } => Pin::new(tcp).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            TlsWriteHalf::Plain(tcp) => Pin::new(tcp).poll_shutdown(cx),
            TlsWriteHalf::Tls { tcp, tls_state, .. } => {
                tls_poll_shutdown(Pin::new(tcp), tls_state, cx)
            }
        }
    }
}

// ============================================================================
// Shared TLS I/O helpers
// ============================================================================

/// Encrypt plaintext and write ciphertext to a TCP stream.
///
/// This is the core TLS write implementation shared by TlsWriteHalf and ClientSocket.
///
/// The `pending` buffer ensures that ciphertext is never lost. Encrypting
/// plaintext advances the TLS write sequence number irreversibly — if the
/// TCP write returns `Pending` or is partial, re-encrypting the same plaintext
/// on retry would produce a record with the wrong sequence number, causing the
/// peer to fail AEAD decryption.
pub fn tls_poll_write<T, W>(
    tcp: Pin<&mut W>,
    tls_state: &SharedTlsState<T>,
    cx: &mut Context<'_>,
    buf: &[u8],
    pending: &mut Option<PendingWrite>,
) -> Poll<io::Result<usize>>
where
    T: TlsConnectionOps,
    W: AsyncWrite,
{
    // If we have buffered ciphertext from a previous attempt, retry that
    // without re-encrypting.
    let mut pw = match pending.take() {
        Some(pw) => pw,
        None => {
            let mut tls = tls_state
                .lock()
                .map_err(|_| io::Error::other("TLS state lock poisoned"))?;

            tls.writer_write_all(buf)?;

            let mut cipher_buf = Vec::with_capacity(buf.len() + 64);
            tls.write_tls(&mut cipher_buf)?;

            PendingWrite {
                ciphertext: cipher_buf,
                offset: 0,
                plaintext_len: buf.len(),
            }
        }
    };

    match pw.ciphertext.get(pw.offset..) {
        None | Some(&[]) => Poll::Ready(Ok(pw.plaintext_len)),
        Some(remaining) => match tcp.poll_write(cx, remaining) {
            Poll::Ready(Ok(n)) => {
                pw.offset += n;
                if pw.offset >= pw.ciphertext.len() {
                    Poll::Ready(Ok(pw.plaintext_len))
                } else {
                    // Partial write — save progress and register for wakeup.
                    cx.waker().wake_by_ref();
                    *pending = Some(pw);
                    Poll::Pending
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => {
                *pending = Some(pw);
                Poll::Pending
            }
        },
    }
}

/// Send TLS close_notify and shutdown the TCP stream.
///
/// This is the core TLS shutdown implementation shared by TlsWriteHalf and ClientSocket.
pub fn tls_poll_shutdown<T, W>(
    mut tcp: Pin<&mut W>,
    tls_state: &SharedTlsState<T>,
    cx: &mut Context<'_>,
) -> Poll<io::Result<()>>
where
    T: TlsConnectionOps,
    W: AsyncWrite,
{
    // Send TLS close_notify
    let ciphertext = {
        let mut tls = tls_state
            .lock()
            .map_err(|_| io::Error::other("TLS state lock poisoned"))?;
        tls.send_close_notify();
        let mut buf = Vec::with_capacity(64);
        let _ = tls.write_tls(&mut buf);
        buf
    };

    if !ciphertext.is_empty() {
        let _ = tcp.as_mut().poll_write(cx, &ciphertext);
    }

    tcp.poll_shutdown(cx)
}

// ============================================================================
// Owned halves (from `into_split`) — `'static` + `Send`, for leasing
// ============================================================================

/// Owned read half of a `TlsStream`. Unlike [`TlsReadHalf`] it borrows nothing,
/// so it can outlive the original stream value and be stored in a `FramedRead`.
pub enum OwnedTlsReadHalf<T: TlsConnectionOps> {
    Plain(OwnedReadHalf),
    Tls {
        tcp: OwnedReadHalf,
        tls_state: SharedTlsState<T>,
    },
}

impl<T: TlsConnectionOps> AsyncRead for OwnedTlsReadHalf<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut *self {
            OwnedTlsReadHalf::Plain(tcp) => Pin::new(tcp).poll_read(cx, buf),
            OwnedTlsReadHalf::Tls { tcp, tls_state } => poll_tls_read(tcp, tls_state, cx, buf),
        }
    }
}

/// Owned write half of a `TlsStream`. Unlike [`TlsWriteHalf`] it borrows
/// nothing, so it can be moved to another task (leased to the cache worker).
/// This is the concrete type behind `ClientSocket`.
pub enum OwnedTlsWriteHalf<T: TlsConnectionOps> {
    Plain(OwnedWriteHalf),
    Tls {
        tcp: OwnedWriteHalf,
        tls_state: SharedTlsState<T>,
        pending: Option<PendingWrite>,
    },
}

impl<T: TlsConnectionOps> OwnedTlsWriteHalf<T> {
    /// Wait for the underlying TCP socket to be writable (backpressure gate for
    /// select loops).
    pub async fn writable(&self) -> io::Result<()> {
        match self {
            OwnedTlsWriteHalf::Plain(tcp) => tcp.writable().await,
            OwnedTlsWriteHalf::Tls { tcp, .. } => tcp.writable().await,
        }
    }
}

impl<T: TlsConnectionOps> AsyncWrite for OwnedTlsWriteHalf<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            OwnedTlsWriteHalf::Plain(tcp) => Pin::new(tcp).poll_write(cx, buf),
            OwnedTlsWriteHalf::Tls {
                tcp,
                tls_state,
                pending,
            } => tls_poll_write(Pin::new(tcp), tls_state, cx, buf, pending),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            OwnedTlsWriteHalf::Plain(tcp) => Pin::new(tcp).poll_flush(cx),
            OwnedTlsWriteHalf::Tls { tcp, .. } => Pin::new(tcp).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            OwnedTlsWriteHalf::Plain(tcp) => Pin::new(tcp).poll_shutdown(cx),
            OwnedTlsWriteHalf::Tls { tcp, tls_state, .. } => {
                tls_poll_shutdown(Pin::new(tcp), tls_state, cx)
            }
        }
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            OwnedTlsWriteHalf::Plain(tcp) => Pin::new(tcp).poll_write_vectored(cx, bufs),
            OwnedTlsWriteHalf::Tls {
                tcp,
                tls_state,
                pending,
            } => {
                // TLS cannot vectored-write; write the first non-empty slice.
                let buf = bufs.iter().find(|b| !b.is_empty()).map_or(&[][..], |b| b);
                tls_poll_write(Pin::new(tcp), tls_state, cx, buf, pending)
            }
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            OwnedTlsWriteHalf::Plain(tcp) => tcp.is_write_vectored(),
            OwnedTlsWriteHalf::Tls { .. } => false,
        }
    }
}

impl<T: TlsConnectionOps> std::fmt::Debug for OwnedTlsWriteHalf<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OwnedTlsWriteHalf::Plain(_) => f.write_str("OwnedTlsWriteHalf::Plain"),
            OwnedTlsWriteHalf::Tls { .. } => f.write_str("OwnedTlsWriteHalf::Tls"),
        }
    }
}
