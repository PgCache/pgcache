//! Commands for CDC mutations and relation tracking, sent to the writer thread.

use crate::catalog::TableMetadata;
use crate::oid::Oid;
use crate::pg::Lsn;
use crate::pg::protocol::ByteString;

/// A single column value decoded from a pgoutput tuple (PGC-264).
///
/// `Toasted` is pgoutput's "unchanged TOASTed value" marker: the origin elides
/// the value from UPDATE new-row images when the column didn't change, on the
/// contract that the consumer already holds it. It must never be conflated
/// with `Null` — doing so overwrites cached TOAST values with NULL.
// ByteString (ADR-032 boundary exception): each value is a zero-copy
// refcounted view into its replication frame, so decoding and cloning never
// copy the text. The view pins its frame, which is bounded by the row size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CdcValue {
    Null,
    Text(ByteString),
    Toasted,
}

/// Convert decoded tuple values to the downstream row representation,
/// appending to `row_data` (callers pass a recycled Vec so steady-state
/// conversion allocates nothing) and reporting the column indexes that
/// carried the unchanged-toast marker (`Toasted` maps to `None` in the
/// output). Past the writer's repair step the indexes must be empty or
/// handled — this is the only path from `CdcValue` rows to
/// `Option<ByteString>` rows.
pub fn cdc_values_convert(
    values: Vec<CdcValue>,
    row_data: &mut Vec<Option<ByteString>>,
) -> Vec<usize> {
    let mut toasted = Vec::new();
    row_data.reserve(values.len());
    for (idx, value) in values.into_iter().enumerate() {
        row_data.push(match value {
            CdcValue::Null => None,
            CdcValue::Text(text) => Some(text),
            CdcValue::Toasted => {
                toasted.push(idx);
                None
            }
        });
    }
    toasted
}

/// Commands for CDC mutations and relation tracking, sent to the writer thread
#[derive(Debug)]
pub enum CdcCommand {
    /// Source-transaction begin marker. Emitted by the CDC processor for each
    /// pgoutput BEGIN, carrying the source transaction's `xid`. The explicit
    /// delimiter lets the writer enter a frame deterministically (rather than
    /// inferring it from the first mutation), so `FrameState::Idle` genuinely
    /// means "between source transactions".
    Begin { xid: u32 },

    /// Register table metadata from CDC
    TableRegister(TableMetadata),

    /// CDC Insert operation
    Insert {
        relation_oid: Oid,
        row_data: Vec<CdcValue>,
    },

    /// CDC Update operation
    Update {
        relation_oid: Oid,
        key_data: Vec<CdcValue>,
        row_data: Vec<CdcValue>,
    },

    /// CDC Delete operation
    Delete {
        relation_oid: Oid,
        row_data: Vec<CdcValue>,
    },

    /// CDC Truncate operation
    Truncate { relation_oids: Vec<Oid> },

    /// Transaction commit marker. Emitted by the CDC processor after all
    /// mutations from a single transaction have been sent. Carries the
    /// `end_lsn` of the commit record. The writer advances its
    /// `last_applied_lsn` watermark when it processes this command —
    /// guaranteeing the watermark is transaction-aligned.
    CommitMark { lsn: Lsn },

    /// Keep-alive marker. Emitted when the CDC processor receives a
    /// PrimaryKeepAlive whose `wal_end` advances past the previously
    /// observed position. Carries `wal_end`. Allows the writer's
    /// `last_received_lsn` watermark to advance during idle periods
    /// (no published-table transactions) so the gauge remains current.
    KeepAliveMark { lsn: Lsn },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cdc_values_convert_reports_toasted_indexes() {
        let mut row_data = Vec::new();
        let toasted = cdc_values_convert(
            vec![
                CdcValue::Text("a".into()),
                CdcValue::Toasted,
                CdcValue::Null,
                CdcValue::Toasted,
            ],
            &mut row_data,
        );
        assert_eq!(
            row_data,
            vec![Some("a".into()), None, None, None],
            "Toasted and Null both map to None in the row representation"
        );
        assert_eq!(toasted, vec![1, 3]);
    }

    #[test]
    fn test_cdc_values_convert_no_toast() {
        let mut row_data = Vec::new();
        let toasted = cdc_values_convert(
            vec![CdcValue::Null, CdcValue::Text("b".into())],
            &mut row_data,
        );
        assert_eq!(row_data, vec![None, Some("b".into())]);
        assert!(toasted.is_empty());
    }
}
