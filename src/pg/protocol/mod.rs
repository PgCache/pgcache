#![allow(dead_code)]
// adapted from https://github.com/sunng87/pgwire
use std::borrow::Borrow;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::io;
use std::ops::Deref;
use std::str::Utf8Error;

use error_set::error_set;
use rootcause::Report;
use tokio_util::bytes::{Bytes, BytesMut};

pub(crate) mod backend;
pub(crate) mod encode;
pub(crate) mod extended;
pub(crate) mod frontend;

error_set! {
    ProtocolError := {
        #[display("Invalid protocal version: {major}.{minor}")]
        InvalidProtocolVersion {
            major: i16,
            minor: i16,
        },
        InvalidStartupFrame,
        #[display("Unrecognized message type: {tag}")]
        UnrecognizedMessageType {
            tag: String,
        },
        IoError(io::Error),
    }
}

/// Result type with location-tracking error reports for protocol operations.
pub type ProtocolResult<T> = Result<T, Report<ProtocolError>>;

/// Immutable UTF-8 string backed by a refcounted `Bytes` slice, typically a
/// view into a wire frame. Cloning is a refcount bump, not a deep copy.
#[derive(Clone, Default)]
pub struct ByteString(Bytes);

impl ByteString {
    /// Wrap `bytes` as a string, validating UTF-8 once up front.
    pub fn from_utf8(bytes: Bytes) -> Result<Self, Utf8Error> {
        std::str::from_utf8(&bytes)?;
        Ok(Self(bytes))
    }

    pub fn as_str(&self) -> &str {
        // SAFETY: UTF-8 validated in `from_utf8`; `Bytes` is immutable.
        unsafe { std::str::from_utf8_unchecked(&self.0) }
    }
}

impl Deref for ByteString {
    type Target = str;

    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<str> for ByteString {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for ByteString {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl PartialEq for ByteString {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Eq for ByteString {}

impl PartialEq<str> for ByteString {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for ByteString {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl Hash for ByteString {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl fmt::Debug for ByteString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_str(), f)
    }
}

impl fmt::Display for ByteString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.as_str(), f)
    }
}

impl From<&str> for ByteString {
    fn from(s: &str) -> Self {
        Self(Bytes::copy_from_slice(s.as_bytes()))
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) enum PgConnectionState {
    #[default]
    Startup,
    Authentication,
    Query,
    // FunctionCall,
    // Copy,
    // Termination,
    // ReadyForQuery,
    // QueryInProgress,
    // CopyInProgress(bool),
    // AwaitingSync,
}

pub trait PgMessageType {}

#[derive(Debug)]
pub struct PgMessage<T: PgMessageType> {
    pub message_type: T,
    pub data: BytesMut,
}
