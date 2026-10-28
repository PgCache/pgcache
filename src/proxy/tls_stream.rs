//! Generic TLS stream types for both client and origin connections.
//!
//! This module provides stream abstractions that support:
//! - Borrowed splits with `.writable()` for select loops
//! - TLS encryption/decryption via rustls
//!
//! The key insight is that `.writable()` checks TCP-level writability, which
//! works the same for both plain and TLS connections - it's about the underlying
//! socket being ready to accept data.

use std::borrow::BorrowMut;
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
    fn process_new_packets(&mut self) -> Result<(), rustls::Error>;

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

            fn process_new_packets(&mut self) -> Result<(), rustls::Error> {
                self.deref_mut().process_new_packets().map(drop)
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
        staged: StagedCiphertext,
    },
}

impl<T: TlsConnectionOps> TlsStream<T> {
    /// Create a plain TCP stream.
    pub(super) fn plain(tcp: TcpStream) -> Self {
        TlsStream::Plain(tcp)
    }

    /// Create a TLS stream with existing TLS state.
    pub(super) fn tls(tcp: TcpStream, tls_state: SharedTlsState<T>) -> Self {
        TlsStream::Tls {
            tcp,
            tls_state,
            staged: StagedCiphertext::default(),
        }
    }

    /// Split into borrowed read and write halves.
    ///
    /// The write half has `.writable()` which delegates to the underlying TCP stream.
    /// Both halves share the TLS state for encrypted connections.
    pub(super) fn split(&mut self) -> (TlsReadHalf<'_, T>, TlsWriteHalf<'_, T>) {
        let (tcp, tls_state) = match self {
            TlsStream::Plain(tcp) => (tcp, None),
            TlsStream::Tls {
                tcp,
                tls_state,
                staged,
            } => (tcp, Some((Arc::clone(tls_state), staged))),
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
            TlsStream::Tls {
                tcp,
                tls_state,
                staged,
            } => (tcp, Some((tls_state, staged))),
        };
        let (read, write) = tcp.into_split();
        halves(read, write, tls_state)
    }
}

/// Wrap a split TCP stream's halves, sharing the TLS state between them when
/// the connection is encrypted. The stream's staged ciphertext goes with the
/// read half, lent to a borrowed one and moved into an owned one, so a split
/// never starts with an empty stage while bytes are still staged.
fn halves<R, W, T: TlsConnectionOps, S: BorrowMut<StagedCiphertext>>(
    read: R,
    write: W,
    tls: Option<(SharedTlsState<T>, S)>,
) -> (TlsRead<R, T, S>, TlsWrite<W, T>) {
    match tls {
        None => (TlsRead::Plain(read), TlsWrite::Plain(write)),
        Some((tls_state, staged)) => (
            TlsRead::Tls {
                tcp: read,
                tls_state: Arc::clone(&tls_state),
                staged,
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

/// Read half of a [`TlsStream`], over a borrowed or owned TCP read half; `S`
/// is the stream's [`StagedCiphertext`], borrowed or owned alike.
pub enum TlsRead<R, T: TlsConnectionOps, S> {
    Plain(R),
    Tls {
        tcp: R,
        tls_state: SharedTlsState<T>,
        staged: S,
    },
}

/// Bytes of one TCP read.
const STAGE_LEN: usize = 16 * 1024;

/// Ciphertext read from TCP and not yet handed to the TLS engine. It outlives
/// one `poll_read`: the engine refuses ciphertext while more than 16 KiB of
/// decrypted plaintext is unread (rustls's received-plaintext limit), so a
/// segment is fed only as fast as the caller drains plaintext, and the rest
/// waits here for the next poll. The buffer is allocated on the first read.
#[derive(Default)]
pub struct StagedCiphertext {
    buf: Option<Box<[u8; STAGE_LEN]>>,
    consumed: usize,
    filled: usize,
}

impl StagedCiphertext {
    fn unconsumed(&self) -> &[u8] {
        self.buf
            .as_deref()
            .and_then(|buf| buf.get(self.consumed..self.filled))
            .unwrap_or_default()
    }

    fn clear(&mut self) {
        self.consumed = 0;
        self.filled = 0;
    }

    fn buffer(&mut self) -> &mut [u8] {
        &mut self.buf.get_or_insert_with(|| Box::new([0; STAGE_LEN]))[..]
    }
}

/// Borrowed read half (from [`TlsStream::split`]).
pub(super) type TlsReadHalf<'a, T> = TlsRead<ReadHalf<'a>, T, &'a mut StagedCiphertext>;

/// Owned read half (from [`TlsStream::into_split`]). It borrows nothing, so it
/// can outlive the original stream value and be stored in a `FramedRead`.
pub(super) type OwnedTlsReadHalf<T> = TlsRead<OwnedReadHalf, T, StagedCiphertext>;

impl<R, T, S> AsyncRead for TlsRead<R, T, S>
where
    R: AsyncRead + Unpin,
    T: TlsConnectionOps,
    S: BorrowMut<StagedCiphertext> + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut *self {
            TlsRead::Plain(tcp) => Pin::new(tcp).poll_read(cx, buf),
            TlsRead::Tls {
                tcp,
                tls_state,
                staged,
            } => poll_tls_read(tcp, tls_state, staged.borrow_mut(), cx, buf),
        }
    }
}

/// Poll TLS read: take decrypted plaintext; failing that, feed staged
/// ciphertext to the engine; failing that, read ciphertext from TCP.
///
/// Plaintext is drained before any ciphertext is fed, so the engine's
/// unread-plaintext limit is never reached; ciphertext the caller's buffer
/// could not absorb stays staged for the next poll instead of being lost.
/// A close_notify from the peer is EOF, whatever follows it on the wire.
fn poll_tls_read<T, R>(
    tcp: &mut R,
    tls_state: &SharedTlsState<T>,
    staged: &mut StagedCiphertext,
    cx: &mut Context<'_>,
    buf: &mut ReadBuf<'_>,
) -> Poll<io::Result<()>>
where
    T: TlsConnectionOps,
    R: AsyncRead + Unpin,
{
    if buf.remaining() == 0 {
        return Poll::Ready(Ok(()));
    }
    loop {
        match tls_plaintext_take(tls_state, buf)? {
            Plaintext::Taken => return Poll::Ready(Ok(())),
            Plaintext::Closed => {
                staged.clear();
                return Poll::Ready(Ok(()));
            }
            Plaintext::None => {}
        }

        let remaining = staged.unconsumed();
        if !remaining.is_empty() {
            let n = tls_ciphertext_feed(tls_state, remaining)?;
            if n == 0 {
                // The engine takes nothing after close_notify.
                staged.clear();
                return Poll::Ready(Ok(()));
            }
            staged.consumed += n;
            continue;
        }

        // Zero bytes is EOF.
        let filled = ready!(tcp_ciphertext_read(tcp, cx, staged.buffer()))?;
        staged.consumed = 0;
        staged.filled = filled;
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

/// What a plaintext take found.
enum Plaintext {
    Taken,
    /// Nothing pending yet.
    None,
    /// The peer's close_notify has been processed and nothing is left.
    Closed,
}

/// Move any decrypted plaintext into `buf`, which must have room.
fn tls_plaintext_take<T: TlsConnectionOps>(
    tls_state: &SharedTlsState<T>,
    buf: &mut ReadBuf<'_>,
) -> io::Result<Plaintext> {
    let mut tls = tls_state
        .lock()
        .map_err(|_| io::Error::other("TLS state lock poisoned"))?;
    match tls.reader_read(buf.initialize_unfilled()) {
        Ok(0) => Ok(Plaintext::Closed),
        Ok(n) => {
            buf.advance(n);
            Ok(Plaintext::Taken)
        }
        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => Ok(Plaintext::None),
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

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::io::Write;
    use std::sync::Mutex;
    use std::time::Duration;

    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
    use rustls::{ClientConnection, DigitallySignedStruct, ServerConnection, SignatureScheme};
    use tokio::io::AsyncReadExt;

    use super::*;

    /// rustls's `DEFAULT_RECEIVED_PLAINTEXT_LIMIT`: `read_tls` refuses more
    /// ciphertext while more than this much decrypted plaintext is unread.
    const PLAINTEXT_LIMIT: usize = 16 * 1024;
    /// How much ciphertext rustls pulls from the reader per `read_tls` call.
    const READ_SIZE: usize = 4096;
    /// The largest TLS record payload.
    const MAX_RECORD: usize = 16 * 1024;
    /// Where the wire is cut into two TCP reads: the first record's tail
    /// lands in the second segment together with every small record.
    const SEGMENT: usize = 16 * 1024;

    /// A TLS engine with rustls's buffering rules and a trivial record format:
    /// a two-byte big-endian length followed by the plaintext payload.
    #[derive(Default)]
    struct FakeTls {
        deframer: Vec<u8>,
        plaintext: VecDeque<u8>,
    }

    impl TlsConnectionOps for FakeTls {
        fn reader_read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if self.plaintext.is_empty() {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            let n = buf.len().min(self.plaintext.len());
            for (dst, src) in buf.iter_mut().zip(self.plaintext.drain(..n)) {
                *dst = src;
            }
            Ok(n)
        }

        fn writer_write_all(&mut self, _buf: &[u8]) -> io::Result<()> {
            unreachable!("read-only fake")
        }

        fn read_tls<R: std::io::Read>(&mut self, rd: &mut R) -> io::Result<usize> {
            if self.plaintext.len() > PLAINTEXT_LIMIT {
                return Err(io::Error::other("received plaintext buffer full"));
            }
            let mut chunk = [0u8; READ_SIZE];
            let n = rd.read(&mut chunk)?;
            self.deframer.extend_from_slice(&chunk[..n]);
            Ok(n)
        }

        fn write_tls<W: std::io::Write>(&mut self, _wr: &mut W) -> io::Result<usize> {
            Ok(0)
        }

        fn process_new_packets(&mut self) -> Result<(), rustls::Error> {
            while let Some(header) = self.deframer.get(..2) {
                let len = usize::from(u16::from_be_bytes([header[0], header[1]]));
                if self.deframer.len() < 2 + len {
                    break;
                }
                let record: Vec<u8> = self.deframer.drain(..2 + len).collect();
                self.plaintext.extend(&record[2..]);
            }
            Ok(())
        }

        fn send_close_notify(&mut self) {}
    }

    /// One maximum-size payload followed by a hundred small ones: decrypted
    /// together they exceed the unread-plaintext limit.
    fn payloads() -> Vec<Vec<u8>> {
        let mut payloads: Vec<Vec<u8>> = vec![(0u8..251).cycle().take(MAX_RECORD).collect()];
        payloads.extend((0..100u8).map(|i| vec![i; 100]));
        payloads
    }

    fn fake_record(payload: &[u8]) -> Vec<u8> {
        let len = u16::try_from(payload.len()).expect("record fits u16");
        let mut out = len.to_be_bytes().to_vec();
        out.extend_from_slice(payload);
        out
    }

    /// Cut the wire into the two TCP reads the scenario needs.
    fn segments(mut wire: Vec<u8>) -> VecDeque<Vec<u8>> {
        let tail = wire.split_off(SEGMENT.min(wire.len()));
        VecDeque::from([wire, tail])
    }

    /// A socket that yields each scripted chunk on one read, then EOF, or
    /// stays open (pending forever) when `hold_open` is set.
    struct ScriptedTcp {
        chunks: VecDeque<Vec<u8>>,
        hold_open: bool,
    }

    impl AsyncRead for ScriptedTcp {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let Some(mut chunk) = self.chunks.pop_front() else {
                return if self.hold_open {
                    Poll::Pending
                } else {
                    Poll::Ready(Ok(()))
                };
            };
            let n = chunk.len().min(buf.remaining());
            buf.put_slice(&chunk[..n]);
            if n < chunk.len() {
                chunk.drain(..n);
                self.chunks.push_front(chunk);
            }
            Poll::Ready(Ok(()))
        }
    }

    /// Everything the read half yields before EOF, read `read_buf_len` bytes
    /// at a time.
    async fn plaintext_through<T: TlsConnectionOps>(
        engine: T,
        tcp: ScriptedTcp,
        read_buf_len: usize,
    ) -> Vec<u8> {
        let tls_state: SharedTlsState<T> = Arc::new(Mutex::new(engine));
        let (mut read, _write) = halves(
            tcp,
            tokio::io::empty(),
            Some((tls_state, StagedCiphertext::default())),
        );
        let mut out = Vec::new();
        let mut buf = vec![0u8; read_buf_len];
        let all = async {
            loop {
                let n = read.read(&mut buf).await.expect("plaintext read");
                if n == 0 {
                    break;
                }
                out.extend_from_slice(&buf[..n]);
            }
        };
        tokio::time::timeout(Duration::from_secs(5), all)
            .await
            .expect("EOF within the timeout");
        out
    }

    fn fake_wire(payloads: &[Vec<u8>]) -> Vec<u8> {
        payloads.iter().flat_map(|p| fake_record(p)).collect()
    }

    #[tokio::test]
    async fn test_tls_read_segment_exceeding_plaintext_limit_framed_buffer() {
        let payloads = payloads();
        let tcp = ScriptedTcp {
            chunks: segments(fake_wire(&payloads)),
            hold_open: false,
        };
        assert_eq!(
            plaintext_through(FakeTls::default(), tcp, 8192).await,
            payloads.concat()
        );
    }

    #[tokio::test]
    async fn test_tls_read_segment_exceeding_plaintext_limit_small_buffer() {
        let payloads = payloads();
        let tcp = ScriptedTcp {
            chunks: segments(fake_wire(&payloads)),
            hold_open: false,
        };
        assert_eq!(
            plaintext_through(FakeTls::default(), tcp, 100).await,
            payloads.concat()
        );
    }

    // ---- the real engine -------------------------------------------------

    /// Accepts any server certificate: the test owns both ends.
    #[derive(Debug)]
    struct AnyServer;

    impl ServerCertVerifier for AnyServer {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp_response: &[u8],
            _now: UnixTime,
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
            rustls::crypto::aws_lc_rs::default_provider()
                .signature_verification_algorithms
                .supported_schemes()
        }
    }

    /// A handshaken client/server pair over the repository's test certificate.
    fn connected_pair() -> (ClientConnection, ServerConnection) {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/data/certs");
        let certs: Vec<CertificateDer<'static>> =
            rustls_pemfile::certs(&mut std::io::BufReader::new(
                std::fs::File::open(format!("{dir}/server.crt")).expect("open server.crt"),
            ))
            .collect::<Result<_, _>>()
            .expect("parse server.crt");
        let key: PrivateKeyDer<'static> =
            rustls_pemfile::private_key(&mut std::io::BufReader::new(
                std::fs::File::open(format!("{dir}/server.key")).expect("open server.key"),
            ))
            .expect("parse server.key")
            .expect("a private key in server.key");
        let server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .expect("server config");
        let client_config = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AnyServer))
            .with_no_client_auth();
        let mut client = ClientConnection::new(
            Arc::new(client_config),
            ServerName::try_from("localhost").expect("server name"),
        )
        .expect("client connection");
        let mut server = ServerConnection::new(Arc::new(server_config)).expect("server connection");
        while client.is_handshaking() || server.is_handshaking() {
            shuttle(&mut client, &mut server);
            shuttle(&mut server, &mut client);
        }
        (client, server)
    }

    /// Move whatever `from` wants to send into `to` and process it.
    fn shuttle<A, B>(from: &mut A, to: &mut B)
    where
        A: TlsConnectionOps,
        B: TlsConnectionOps,
    {
        let mut wire = Vec::new();
        while from.write_tls(&mut wire).expect("write_tls") > 0 {}
        // Feed exactly the wire: a zero-byte read would mark EOF on `to`.
        let mut cursor = std::io::Cursor::new(&wire[..]);
        let wire_len = u64::try_from(wire.len()).expect("wire length fits u64");
        while cursor.position() < wire_len {
            to.read_tls(&mut cursor).expect("read_tls");
        }
        to.process_new_packets().expect("process_new_packets");
    }

    /// Ciphertext for `payloads` as the client sends them, one record each.
    fn client_wire(client: &mut ClientConnection, payloads: &[Vec<u8>]) -> Vec<u8> {
        let mut wire = Vec::new();
        for payload in payloads {
            client.writer().write_all(payload).expect("write payload");
            while client.write_tls(&mut wire).expect("write_tls") > 0 {}
        }
        wire
    }

    #[tokio::test]
    async fn test_tls_read_real_engine_segment_exceeding_plaintext_limit() {
        let (mut client, server) = connected_pair();
        let payloads = payloads();
        let tcp = ScriptedTcp {
            chunks: segments(client_wire(&mut client, &payloads)),
            hold_open: false,
        };
        assert_eq!(plaintext_through(server, tcp, 100).await, payloads.concat());
    }

    /// close_notify ends the stream even when bytes trail it on the wire and
    /// the socket stays open.
    #[tokio::test]
    async fn test_tls_read_real_engine_close_notify_is_eof() {
        let (mut client, server) = connected_pair();
        let payloads = payloads();
        let mut wire = client_wire(&mut client, &payloads);
        client.send_close_notify();
        while client.write_tls(&mut wire).expect("write_tls") > 0 {}
        wire.extend_from_slice(&[0xAA; 300]);
        let tcp = ScriptedTcp {
            chunks: segments(wire),
            hold_open: true,
        };
        assert_eq!(plaintext_through(server, tcp, 100).await, payloads.concat());
    }
}
