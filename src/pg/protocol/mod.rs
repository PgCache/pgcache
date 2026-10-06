use std::borrow::Borrow;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::str::Utf8Error;

use bytes::Bytes;

#[cfg(feature = "proxy")]
pub(crate) mod backend;
#[cfg(feature = "proxy")]
pub(crate) mod encode;
#[cfg(feature = "proxy")]
pub(crate) mod extended;
#[cfg(feature = "proxy")]
pub(crate) mod frontend;
#[cfg(feature = "proxy")]
pub(crate) mod frontend_encode;
#[cfg(feature = "proxy")]
mod message;
#[cfg(feature = "proxy")]
pub(crate) mod session;

#[cfg(feature = "proxy")]
pub(crate) use message::{
    PgConnectionState, PgMessage, PgMessageType, ProtocolError, ProtocolResult, message_length,
};

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
