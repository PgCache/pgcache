//! Per-connection extended-query session state.
//!
//! The mutable state a proxy connection carries between messages — the prepared
//! statements it has parsed and the portals bound from them — as opposed to the
//! stateless wire parsers in [`extended`](super::extended), which only decode a
//! frame into a DTO.

use std::sync::Arc;

use ecow::EcoString;
use tokio_util::bytes::Bytes;

use super::ByteString;
use crate::cache::query::CacheableQuery;

/// Classification of a prepared statement based on SQL analysis
#[derive(Debug, Clone)]
pub enum StatementType {
    /// SELECT statement that can be cached
    Cacheable(Arc<CacheableQuery>),
    /// Non-SELECT statement (INSERT, UPDATE, DELETE, DDL, etc.)
    NonSelect,
    /// SELECT statement that cannot be cached (complex features)
    UncacheableSelect,
    /// Failed to parse the SQL
    ParseError,
}

/// Prepared statement stored in connection state
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    pub name: EcoString,
    /// `ByteString`: a refcounted view into the Parse frame, so storing and
    /// cloning the SQL (e.g. into the describe-cache key) never copies it.
    pub sql: ByteString,
    /// Parameter type OIDs as resolved by origin's `ParameterDescription`,
    /// falling back to the client-supplied OIDs until origin replies. Used
    /// for query fingerprinting under cacheable execution.
    pub parameter_oids: Vec<u32>,
    /// Immutable snapshot of the client-supplied OIDs from `Parse`. Used in
    /// the describe-cache key so populate and lookup hash identically.
    pub client_parameter_oids: Vec<u32>,
    pub sql_type: StatementType,
    /// Raw ParameterDescription bytes from origin, used for Describe('S') in
    /// pipeline. `Bytes` (not `BytesMut`): written once from origin, then only
    /// read and cheaply (refcount) cloned into the describe cache and synth path.
    pub parameter_description: Option<Bytes>,
    /// Raw RowDescription bytes from origin's Describe('S') response. `None`
    /// when origin returned `NoData`; pair with `describe_no_data` to
    /// distinguish "not yet captured" from "captured, no result columns".
    pub row_description: Option<Bytes>,
    /// Origin's Describe('S') response was `NoData`. See `row_description`.
    pub describe_no_data: bool,
    /// True when origin has acknowledged this statement (ParseComplete received).
    /// Gates pipeline activation — the proxy only buffers Parse/Bind/Execute
    /// when origin_prepared is true.
    pub origin_prepared: bool,
    /// Raw Parse message bytes, stored for proactive origin forwarding.
    /// On cache hit for a named statement, origin never sees Parse — these
    /// bytes let the proxy send Parse+Sync to origin so subsequent Bind-only
    /// or transaction paths work correctly. `Bytes` (refcounted, frozen from the
    /// codec split) so storing it doesn't deep-copy the Parse message.
    pub parse_bytes: Option<Bytes>,
}

/// Result-column format codes from a Bind message, collapsed to intent.
///
/// Per the protocol, zero codes means all-text, one code applies to every
/// column, and N codes are per-column. N identical codes are collapsed to
/// `Uniform` at parse time — they are wire-equivalent to a single code, and
/// the collapse makes "is this uniform?" a variant check while keeping the
/// common cases allocation-free.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub enum ResultFormats {
    /// No format codes: all columns default to text.
    #[default]
    Implicit,
    /// One format code for all columns (0 = text, 1 = binary).
    Uniform(i16),
    /// Genuinely mixed per-column format codes.
    PerColumn(Vec<i16>),
}

impl ResultFormats {
    /// Whether results were requested in binary format. `PerColumn` (mixed)
    /// formats never reach the serve path — they disqualify the cache
    /// candidate — so only `Uniform` can be binary.
    pub fn is_binary(&self) -> bool {
        matches!(self, Self::Uniform(code) if *code != 0)
    }
}

/// Portal (bound prepared statement) stored in connection state
#[derive(Debug, Clone)]
pub struct Portal {
    pub name: EcoString,
    pub statement_name: EcoString,
    pub parameter_values: Vec<Option<Bytes>>,
    pub parameter_formats: Vec<i16>, // 0=text, 1=binary
    pub result_formats: ResultFormats,
}

impl Portal {
    /// Check if any parameter uses binary format (format code 1).
    /// Returns true if binary format is detected, false otherwise.
    pub fn has_binary_parameters(&self) -> bool {
        self.parameter_formats.contains(&1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_portal_has_binary_parameters_all_text() {
        let portal = Portal {
            name: "p1".into(),
            statement_name: "s1".into(),
            parameter_values: vec![Some(Bytes::from_static(b"42"))],
            parameter_formats: vec![0], // text format
            result_formats: ResultFormats::Uniform(0),
        };

        assert!(
            !portal.has_binary_parameters(),
            "All text parameters should return false"
        );
    }

    #[test]
    fn test_portal_has_binary_parameters_with_binary() {
        let portal = Portal {
            name: "p1".into(),
            statement_name: "s1".into(),
            parameter_values: vec![Some(Bytes::from_static(&[0, 0, 0, 42]))],
            parameter_formats: vec![1], // binary format
            result_formats: ResultFormats::Uniform(0),
        };

        assert!(
            portal.has_binary_parameters(),
            "Binary parameter should return true"
        );
    }

    #[test]
    fn test_portal_has_binary_parameters_mixed() {
        let portal = Portal {
            name: "p1".into(),
            statement_name: "s1".into(),
            parameter_values: vec![
                Some(Bytes::from_static(b"text")),
                Some(Bytes::from_static(&[0, 0, 0, 42])),
            ],
            parameter_formats: vec![0, 1], // text, then binary
            result_formats: ResultFormats::Uniform(0),
        };

        assert!(
            portal.has_binary_parameters(),
            "Mixed formats with any binary should return true"
        );
    }

    #[test]
    fn test_portal_has_binary_parameters_empty() {
        let portal = Portal {
            name: "p1".into(),
            statement_name: "s1".into(),
            parameter_values: vec![],
            parameter_formats: vec![],
            result_formats: ResultFormats::Implicit,
        };

        assert!(
            !portal.has_binary_parameters(),
            "No parameters should return false"
        );
    }
}
