//! The serve response state machine: which cache-DB completion each step of a
//! serve's pipeline expects, for the frames consumed rather than relayed.

use crate::pg::cache_connection::PrepareOutcome;
use crate::pg::protocol::backend::PgBackendMessageType;

/// Response state machine for the unified serve path (text and binary clients;
/// source-row uses a named prepared statement, MV an unnamed one). Result
/// format (text/binary) is chosen per client; the message *sequence* is the
/// same.
///
/// Source-row pipeline (PGC-235): set_config(generation) + [Close] +
/// Parse/Bind/[Describe('P')]/Execute under one Sync, producing
/// [SetGen ParseComplete →] SetGen BindComplete → SetGen DataRow → SetGen
/// CommandComplete → [CloseComplete →] [SELECT ParseComplete →] BindComplete →
/// [RowDescription →] DataRow* → CommandComplete (SELECT) → ReadyForQuery. The
/// set_config response (a one-row SELECT) is consumed, not relayed; only the
/// SELECT's BindComplete-onward reaches the client. MV path has no set_config
/// prefix and starts at `ParseComplete`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ServeResponseState {
    /// Waiting for the rendering-settings statement's ParseComplete (first
    /// application on this connection only)
    SetOutParse,
    /// Waiting for the rendering-settings statement's BindComplete
    SetOutBind,
    /// Consuming the rendering-settings one-row result
    SetOutData,
    /// Waiting for the set_config statement's ParseComplete (first serve only)
    SetGenParse,
    /// Waiting for the set_config statement's BindComplete
    SetGenBind,
    /// Consuming the set_config one-row result (DataRow then CommandComplete)
    SetGenData,
    /// Waiting for CloseComplete (only when a statement was evicted)
    CloseComplete,
    /// Waiting for ParseComplete
    ParseComplete,
    /// Waiting for BindComplete
    BindComplete,
    /// Waiting for RowDescription (only when include_describe is true)
    DescribeRow,
    /// Streaming DataRow messages
    DataRows,
    /// Done — final ReadyForQuery received
    Done,
}

impl ServeResponseState {
    /// The first completion to expect: the rendering settings' (Parse on their
    /// first application on this connection, else Bind) when they were sent,
    /// else whatever follows them — the generation stamp on the source-row
    /// path, the SELECT on the MV path.
    pub(super) fn initial(prepare: &PrepareOutcome) -> Self {
        if prepare.sent_setout_parse {
            Self::SetOutParse
        } else if prepare.sent_setout {
            Self::SetOutBind
        } else {
            Self::after_setout(prepare)
        }
    }

    /// The first completion after the rendering settings (or in their
    /// absence): the generation stamp on the source-row path, else the SELECT.
    fn after_setout(prepare: &PrepareOutcome) -> Self {
        if prepare.sent_setgen_parse {
            Self::SetGenParse
        } else if prepare.sent_setgen {
            Self::SetGenBind
        } else if prepare.sent_close {
            Self::CloseComplete
        } else {
            Self::select_start(prepare)
        }
    }

    /// The next state for a frame that is consumed rather than relayed, or
    /// `None` when this frame is not such a transition from this state.
    pub(super) fn advance(
        self,
        message_type: PgBackendMessageType,
        prepare: &PrepareOutcome,
        include_describe: bool,
    ) -> Option<Self> {
        let next = match (self, message_type) {
            (Self::SetOutParse, PgBackendMessageType::ParseComplete) => Self::SetOutBind,
            (Self::SetOutBind, PgBackendMessageType::BindComplete) => Self::SetOutData,
            (Self::SetOutData, PgBackendMessageType::DataRows) => Self::SetOutData,
            (Self::SetOutData, PgBackendMessageType::CommandComplete) => {
                Self::after_setout(prepare)
            }
            (Self::SetGenParse, PgBackendMessageType::ParseComplete) => Self::SetGenBind,
            (Self::SetGenBind, PgBackendMessageType::BindComplete) => Self::SetGenData,
            // set_config returns one row; consume it without relaying.
            (Self::SetGenData, PgBackendMessageType::DataRows) => Self::SetGenData,
            // set_config done. A Close (if a statement was evicted) precedes the
            // SELECT; on statement reuse neither Close nor Parse is sent, so skip
            // straight to Bind.
            (Self::SetGenData, PgBackendMessageType::CommandComplete) if prepare.sent_close => {
                Self::CloseComplete
            }
            // A reconciliation Close can ride a reuse serve (no Parse), so the
            // Parse only follows when one was actually sent.
            (Self::SetGenData, PgBackendMessageType::CommandComplete)
            | (Self::CloseComplete, PgBackendMessageType::CloseComplete) => {
                Self::select_start(prepare)
            }
            (Self::ParseComplete, PgBackendMessageType::ParseComplete) => Self::BindComplete,
            (Self::BindComplete, PgBackendMessageType::BindComplete) if include_describe => {
                Self::DescribeRow
            }
            (Self::BindComplete, PgBackendMessageType::BindComplete) => Self::DataRows,
            // Single trailing Sync → one terminal ReadyForQuery. It can't arrive
            // mid-set_config (those advance on Parse/Bind/Data/CC).
            (state, PgBackendMessageType::ReadyForQuery) if !state.in_set_config() => Self::Done,
            (_, _) => return None,
        };
        Some(next)
    }

    /// The SELECT's first completion: its ParseComplete when a Parse was sent,
    /// else its BindComplete.
    fn select_start(prepare: &PrepareOutcome) -> Self {
        if prepare.sent_parse {
            Self::ParseComplete
        } else {
            Self::BindComplete
        }
    }

    fn in_set_config(self) -> bool {
        matches!(
            self,
            Self::SetOutParse
                | Self::SetOutBind
                | Self::SetOutData
                | Self::SetGenParse
                | Self::SetGenBind
                | Self::SetGenData
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use PgBackendMessageType as M;
    use ServeResponseState as S;

    fn prepare(sent_setgen_parse: bool, sent_parse: bool, sent_close: bool) -> PrepareOutcome {
        PrepareOutcome {
            sent_setout: false,
            sent_setout_parse: false,
            sent_setgen: true,
            sent_setgen_parse,
            sent_parse,
            sent_close,
        }
    }

    /// PGC-287: the rendering-settings statement precedes everything; its
    /// result is consumed on both the source-row and MV paths.
    #[test]
    fn test_advance_output_settings_precede_generation_and_mv_select() {
        let source = PrepareOutcome {
            sent_setout: true,
            sent_setout_parse: true,
            ..prepare(false, false, false)
        };
        let initial = ServeResponseState::initial(&source);
        assert_eq!(initial, S::SetOutParse);
        assert_eq!(
            advance_through(
                initial,
                &[
                    M::ParseComplete,
                    M::BindComplete,
                    M::DataRows,
                    M::CommandComplete,
                    M::BindComplete,
                    M::DataRows,
                    M::CommandComplete,
                    M::BindComplete,
                ],
                &source,
                false
            ),
            Some(S::DataRows)
        );

        let mv = PrepareOutcome {
            sent_setout: true,
            sent_setout_parse: false,
            sent_setgen: false,
            sent_setgen_parse: false,
            sent_parse: true,
            sent_close: false,
        };
        let initial = ServeResponseState::initial(&mv);
        assert_eq!(initial, S::SetOutBind);
        assert_eq!(
            advance_through(
                initial,
                &[
                    M::BindComplete,
                    M::DataRows,
                    M::CommandComplete,
                    M::ParseComplete,
                    M::BindComplete,
                ],
                &mv,
                false
            ),
            Some(S::DataRows)
        );
        // No ReadyForQuery can end the serve while the settings result is
        // still being consumed.
        assert_eq!(S::SetOutData.advance(M::ReadyForQuery, &mv, false), None);
    }

    /// Feed `frames` through `advance` from `start`, returning the final state.
    fn advance_through(
        start: ServeResponseState,
        frames: &[PgBackendMessageType],
        prepare: &PrepareOutcome,
        include_describe: bool,
    ) -> Option<ServeResponseState> {
        frames.iter().try_fold(start, |state, &frame| {
            state.advance(frame, prepare, include_describe)
        })
    }

    #[test]
    fn test_advance_walks_the_source_row_prefix() {
        // (sent_setgen_parse, sent_parse, sent_close, include_describe), frames,
        // expected start and end state.
        let cases = [
            (
                prepare(true, true, true),
                true,
                &[
                    M::ParseComplete,
                    M::BindComplete,
                    M::DataRows,
                    M::CommandComplete,
                    M::CloseComplete,
                    M::ParseComplete,
                    M::BindComplete,
                ][..],
                S::SetGenParse,
                S::DescribeRow,
            ),
            (
                prepare(false, false, false),
                false,
                &[
                    M::BindComplete,
                    M::DataRows,
                    M::CommandComplete,
                    M::BindComplete,
                ][..],
                S::SetGenBind,
                S::DataRows,
            ),
        ];
        for (prepare, include_describe, frames, start, end) in cases {
            let initial = ServeResponseState::initial(&prepare);
            assert_eq!(initial, start);
            assert_eq!(
                advance_through(initial, frames, &prepare, include_describe),
                Some(end)
            );
        }
    }

    #[test]
    fn test_advance_reconciliation_close_without_parse_goes_to_bind() {
        let prepare = prepare(false, false, true);
        assert_eq!(
            S::SetGenData.advance(M::CommandComplete, &prepare, false),
            Some(S::CloseComplete)
        );
        assert_eq!(
            S::CloseComplete.advance(M::CloseComplete, &prepare, false),
            Some(S::BindComplete)
        );
    }

    #[test]
    fn test_advance_ready_for_query_ends_only_outside_set_config() {
        let prepare = prepare(true, true, false);
        assert_eq!(
            S::DataRows.advance(M::ReadyForQuery, &prepare, false),
            Some(S::Done)
        );
        for state in [S::SetGenParse, S::SetGenBind, S::SetGenData] {
            assert_eq!(state.advance(M::ReadyForQuery, &prepare, false), None);
        }
    }

    #[test]
    fn test_advance_leaves_relayed_and_unexpected_frames_alone() {
        let prepare = prepare(false, true, false);
        assert_eq!(S::DataRows.advance(M::DataRows, &prepare, false), None);
        assert_eq!(
            S::DataRows.advance(M::CommandComplete, &prepare, false),
            None
        );
        assert_eq!(
            S::DescribeRow.advance(M::RowDescription, &prepare, true),
            None
        );
        assert_eq!(
            S::ParseComplete.advance(M::BindComplete, &prepare, false),
            None
        );
    }
}
