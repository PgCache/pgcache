//! Client stream types for handling both plain TCP and TLS connections.
//!
//! These are the client-specific instantiations of the generic stream types in
//! [`super::tls_stream`]. The proxy terminates TLS as a server, so it uses
//! `rustls::ServerConnection`.
//!
//! The connection [`into_split`](TlsStream::into_split)s the client stream once
//! into an owned read half ([`OwnedClientReadHalf`]) and an owned write half
//! ([`ClientSocket`]). The write half is leased to the cache worker per query —
//! the worker writes the response directly and returns it via the reply — so no
//! per-query `dup` is needed.

use super::tls_stream::{OwnedTlsReadHalf, OwnedTlsWriteHalf, TlsStream};

/// Client connection stream, either plain TCP or TLS-encrypted.
///
/// Uses `rustls::ServerConnection` because the proxy acts as a TLS server for
/// incoming client connections.
pub type ClientStream = TlsStream<rustls::ServerConnection>;

/// Owned read half of a `ClientStream` (from `into_split`), held by the
/// connection's `FramedRead`.
pub type OwnedClientReadHalf = OwnedTlsReadHalf<rustls::ServerConnection>;

/// Owned, leasable client write half (from `into_split`). The connection owns
/// it for its own egress writes and lends it to the cache worker per query.
pub type ClientSocket = OwnedTlsWriteHalf<rustls::ServerConnection>;
