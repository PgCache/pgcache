//! Wire-message framing types and the protocol error shared by the proxy's
//! codec modules.

use std::io;

use bytes::BytesMut;
use error_set::error_set;
use rootcause::Report;

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
        #[display("Message of {len} bytes exceeds the protocol's i32 length field")]
        MessageTooLarge {
            len: usize,
        },
    }
}

/// Result type with location-tracking error reports for protocol operations.
pub(crate) type ProtocolResult<T> = Result<T, Report<ProtocolError>>;

/// A message's length as the protocol's `int32` length field.
pub(crate) fn message_length(len: usize) -> ProtocolResult<i32> {
    i32::try_from(len).map_err(|_| Report::from(ProtocolError::MessageTooLarge { len }))
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

pub(crate) trait PgMessageType {}

#[derive(Debug)]
pub(crate) struct PgMessage<T: PgMessageType> {
    pub message_type: T,
    pub data: BytesMut,
}
