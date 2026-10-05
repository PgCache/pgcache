//! Generic TLS stream types for both client and origin connections.
//!
//! This module provides stream abstractions that support:
//! - Borrowed splits with `.writable()` for select loops
//! - TLS encryption/decryption via rustls
//!
//! The key insight is that `.writable()` checks TCP-level writability, which
//! works the same for both plain and TLS connections - it's about the underlying
//! socket being ready to accept data.

use std::future::Future;
use std::io;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

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

/// Both rustls connection types deref to the same `ConnectionCommon`, so every
/// method body is identical; only the impl header differs.
macro_rules! tls_connection_ops_impl {
    ($conn:ty) => {
        impl TlsConnectionOps for $conn {
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
    };
}

tls_connection_ops_impl!(rustls::ServerConnection);
tls_connection_ops_impl!(rustls::ClientConnection);

// ============================================================================
// TlsStream - generic stream supporting plain TCP or TLS
// ============================================================================

/// Generic stream supporting both plain TCP and TLS-encrypted connections.
///
/// For TLS connections, the TCP stream and TLS state are stored separately
/// to allow borrowed splits where both halves can access the TLS state.
pub(super) enum TlsStream<T: TlsConnectionOps> {
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
    pub(super) fn plain(tcp: TcpStream) -> Self {
        TlsStream::Plain(tcp)
    }

    /// Create a TLS stream with existing TLS state.
    pub(super) fn tls(tcp: TcpStream, tls_state: SharedTlsState<T>) -> Self {
        TlsStream::Tls { tcp, tls_state }
    }

    /// Split into borrowed read and write halves.
    ///
    /// The write half has `.writable()` which delegates to the underlying TCP stream.
    /// Both halves share the TLS state for encrypted connections.
    pub(super) fn split(&mut self) -> (TlsReadHalf<'_, T>, TlsWriteHalf<'_, T>) {
        let (tcp, tls_state) = match self {
            TlsStream::Plain(tcp) => (tcp, None),
            TlsStream::Tls { tcp, tls_state } => (tcp, Some(Arc::clone(tls_state))),
        };
        let (read, write) = tcp.split();
        halves(read, write, tls_state)
    }

    /// Consume the stream into **owned** read and write halves that share one
    /// reactor registration. Unlike [`Self::split`], the halves are `'static`
    /// and `Send`, so the write half can be moved to another task (e.g. leased
    /// to the cache worker). Both halves share the TLS state.
    pub(super) fn into_split(self) -> (OwnedTlsReadHalf<T>, OwnedTlsWriteHalf<T>) {
        let (tcp, tls_state) = match self {
            TlsStream::Plain(tcp) => (tcp, None),
            TlsStream::Tls { tcp, tls_state } => (tcp, Some(tls_state)),
        };
        let (read, write) = tcp.into_split();
        halves(read, write, tls_state)
    }
}

/// Wrap a split TCP stream's halves, sharing the TLS state between them when
/// the connection is encrypted.
fn halves<R, W, T: TlsConnectionOps>(
    read: R,
    write: W,
    tls_state: Option<SharedTlsState<T>>,
) -> (TlsRead<R, T>, TlsWrite<W, T>) {
    match tls_state {
        None => (TlsRead::Plain(read), TlsWrite::Plain(write)),
        Some(tls_state) => (
            TlsRead::Tls {
                tcp: read,
                tls_state: Arc::clone(&tls_state),
            },
            TlsWrite::Tls {
                tcp: write,
                writer: TlsWriter {
                    tls_state,
                    pending: None,
                },
            },
        ),
    }
}

// ============================================================================
// TlsRead - read half with TLS support
// ============================================================================

/// Read half of a [`TlsStream`], over a borrowed or owned TCP read half.
pub enum TlsRead<R, T: TlsConnectionOps> {
    Plain(R),
    Tls {
        tcp: R,
        tls_state: SharedTlsState<T>,
    },
}

/// Borrowed read half (from [`TlsStream::split`]).
pub(super) type TlsReadHalf<'a, T> = TlsRead<ReadHalf<'a>, T>;

/// Owned read half (from [`TlsStream::into_split`]). It borrows nothing, so it
/// can outlive the original stream value and be stored in a `FramedRead`.
pub(super) type OwnedTlsReadHalf<T> = TlsRead<OwnedReadHalf, T>;

impl<R: AsyncRead + Unpin, T: TlsConnectionOps> AsyncRead for TlsRead<R, T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut *self {
            TlsRead::Plain(tcp) => Pin::new(tcp).poll_read(cx, buf),
            TlsRead::Tls { tcp, tls_state } => poll_tls_read(tcp, tls_state, cx, buf),
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
            consumed += tls_ciphertext_feed(tls_state, remaining)?;
            continue;
        }

        // Phase 2: all ciphertext consumed, check for buffered plaintext
        if tls_plaintext_take(tls_state, buf)? {
            return Poll::Ready(Ok(()));
        }

        // Phase 3: read ciphertext from TCP; zero bytes is EOF.
        filled = ready!(tcp_ciphertext_read(tcp, cx, &mut tcp_buf))?;
        consumed = 0;
        if filled == 0 {
            return Poll::Ready(Ok(()));
        }
    }
}

/// Read the next ciphertext from TCP into `tcp_buf`; returns the bytes read.
fn tcp_ciphertext_read<R: AsyncRead + Unpin>(
    tcp: &mut R,
    cx: &mut Context<'_>,
    tcp_buf: &mut [u8],
) -> Poll<io::Result<usize>> {
    let mut read_buf = ReadBuf::new(tcp_buf);
    ready!(Pin::new(tcp).poll_read(cx, &mut read_buf))?;
    let filled = read_buf.filled().len();
    trace!("tls: tcp read {filled} bytes");
    Poll::Ready(Ok(filled))
}

/// Hand received ciphertext to rustls and decrypt it; returns the bytes
/// consumed.
fn tls_ciphertext_feed<T: TlsConnectionOps>(
    tls_state: &SharedTlsState<T>,
    ciphertext: &[u8],
) -> io::Result<usize> {
    let mut tls = tls_state
        .lock()
        .map_err(|_| io::Error::other("TLS state lock poisoned"))?;
    let mut cursor = std::io::Cursor::new(ciphertext);
    let n = tls.read_tls(&mut cursor)?;
    trace!("tls: read_tls consumed {n}/{} bytes", ciphertext.len());
    if let Err(e) = tls.process_new_packets() {
        debug!("tls: process_new_packets failed after consuming {n} bytes: {e}");
        return Err(io::Error::new(io::ErrorKind::InvalidData, e));
    }
    Ok(n)
}

/// Move any decrypted plaintext into `buf`; true when some was produced.
fn tls_plaintext_take<T: TlsConnectionOps>(
    tls_state: &SharedTlsState<T>,
    buf: &mut ReadBuf<'_>,
) -> io::Result<bool> {
    let mut tls = tls_state
        .lock()
        .map_err(|_| io::Error::other("TLS state lock poisoned"))?;
    match tls.reader_read(buf.initialize_unfilled()) {
        Ok(0) => Ok(false),
        Ok(n) => {
            buf.advance(n);
            Ok(true)
        }
        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => Ok(false),
        Err(e) => Err(e),
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
struct PendingWrite {
    ciphertext: Vec<u8>,
    /// Number of ciphertext bytes already written to TCP.
    offset: usize,
    /// Original plaintext length, returned to the caller on completion.
    plaintext_len: usize,
}

// ============================================================================
// TlsWrite - write half with .writable() and TLS support
// ============================================================================

/// A TCP write half whose writability can be awaited — the backpressure gate
/// for select loops. Both tokio write halves have it; this names it for the
/// generic [`TlsWrite`].
pub trait TcpWritable {
    fn writable(&self) -> impl Future<Output = io::Result<()>> + Send + '_;
}

impl TcpWritable for WriteHalf<'_> {
    fn writable(&self) -> impl Future<Output = io::Result<()>> + Send + '_ {
        WriteHalf::writable(self)
    }
}

impl TcpWritable for OwnedWriteHalf {
    fn writable(&self) -> impl Future<Output = io::Result<()>> + Send + '_ {
        OwnedWriteHalf::writable(self)
    }
}

/// The TLS side of a write half: the shared TLS state, and ciphertext from a
/// write the TCP socket hasn't fully taken yet.
pub struct TlsWriter<T: TlsConnectionOps> {
    tls_state: SharedTlsState<T>,
    pending: Option<PendingWrite>,
}

/// Write half of a [`TlsStream`], over a borrowed or owned TCP write half.
/// Its `.writable()` delegates to the underlying TCP stream, providing proper
/// backpressure handling in select loops.
pub enum TlsWrite<W, T: TlsConnectionOps> {
    Plain(W),
    Tls { tcp: W, writer: TlsWriter<T> },
}

/// Borrowed write half (from [`TlsStream::split`]).
pub(super) type TlsWriteHalf<'a, T> = TlsWrite<WriteHalf<'a>, T>;

/// Owned write half (from [`TlsStream::into_split`]). It borrows nothing, so it
/// can be moved to another task (leased to the cache worker). This is the
/// concrete type behind `ClientSocket`.
pub(super) type OwnedTlsWriteHalf<T> = TlsWrite<OwnedWriteHalf, T>;

impl<W: TcpWritable, T: TlsConnectionOps> TlsWrite<W, T> {
    /// Wait for the underlying TCP socket to be writable.
    ///
    /// This delegates to the TCP stream's `.writable()` method, which properly
    /// integrates with tokio's reactor for efficient backpressure handling.
    pub async fn writable(&self) -> io::Result<()> {
        match self {
            TlsWrite::Plain(tcp) | TlsWrite::Tls { tcp, .. } => tcp.writable().await,
        }
    }
}

impl<W: AsyncWrite + Unpin, T: TlsConnectionOps> AsyncWrite for TlsWrite<W, T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            TlsWrite::Plain(tcp) => Pin::new(tcp).poll_write(cx, buf),
            TlsWrite::Tls { tcp, writer } => writer.poll_write(Pin::new(tcp), cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            TlsWrite::Plain(tcp) | TlsWrite::Tls { tcp, .. } => Pin::new(tcp).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            TlsWrite::Plain(tcp) => Pin::new(tcp).poll_shutdown(cx),
            TlsWrite::Tls { tcp, writer } => writer.poll_shutdown(Pin::new(tcp), cx),
        }
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            TlsWrite::Plain(tcp) => Pin::new(tcp).poll_write_vectored(cx, bufs),
            TlsWrite::Tls { tcp, writer } => {
                // TLS cannot vectored-write; write the first non-empty slice.
                let buf = bufs.iter().find(|b| !b.is_empty()).map_or(&[][..], |b| b);
                writer.poll_write(Pin::new(tcp), cx, buf)
            }
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            TlsWrite::Plain(tcp) => tcp.is_write_vectored(),
            TlsWrite::Tls { .. } => false,
        }
    }
}

impl<W, T: TlsConnectionOps> std::fmt::Debug for TlsWrite<W, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TlsWrite::Plain(_) => f.write_str("TlsWrite::Plain"),
            TlsWrite::Tls { .. } => f.write_str("TlsWrite::Tls"),
        }
    }
}

// ============================================================================
// Shared TLS I/O helpers
// ============================================================================

impl<T: TlsConnectionOps> TlsWriter<T> {
    /// Encrypt plaintext and write ciphertext to a TCP stream.
    ///
    /// The `pending` buffer ensures that ciphertext is never lost. Encrypting
    /// plaintext advances the TLS write sequence number irreversibly — if the
    /// TCP write returns `Pending` or is partial, re-encrypting the same
    /// plaintext on retry would produce a record with the wrong sequence
    /// number, causing the peer to fail AEAD decryption.
    fn poll_write<W: AsyncWrite>(
        &mut self,
        tcp: Pin<&mut W>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        // If we have buffered ciphertext from a previous attempt, retry that
        // without re-encrypting.
        let mut pw = match self.pending.take() {
            Some(pw) => pw,
            None => self.encrypt(buf)?,
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
                        self.pending = Some(pw);
                        Poll::Pending
                    }
                }
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                Poll::Pending => {
                    self.pending = Some(pw);
                    Poll::Pending
                }
            },
        }
    }

    /// Encrypt `buf` into a fresh ciphertext write.
    fn encrypt(&self, buf: &[u8]) -> io::Result<PendingWrite> {
        let mut tls = self
            .tls_state
            .lock()
            .map_err(|_| io::Error::other("TLS state lock poisoned"))?;
        tls.writer_write_all(buf)?;
        let mut cipher_buf = Vec::with_capacity(buf.len() + 64);
        tls.write_tls(&mut cipher_buf)?;
        Ok(PendingWrite {
            ciphertext: cipher_buf,
            offset: 0,
            plaintext_len: buf.len(),
        })
    }

    /// Send TLS close_notify and shutdown the TCP stream.
    fn poll_shutdown<W: AsyncWrite>(
        &self,
        mut tcp: Pin<&mut W>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        let ciphertext = {
            let mut tls = self
                .tls_state
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
}
