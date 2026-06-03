use std::io;

use error_set::error_set;
use rootcause::Report;
use tokio_postgres::Error;

use crate::query::{
    decorrelate::DecorrelateError, resolved::ResolveError, transform::AstTransformError,
};

/// Result type with location-tracking error reports.
/// Use this for functions where you want automatic file:line capture on errors.
pub type CacheResult<T> = Result<T, Report<CacheError>>;

// Re-export result extensions for convenience when using CacheResult
pub use crate::result::{MapIntoReport, ReportExt};

// Module declarations
mod cdc;
mod fast_path;
pub(crate) mod messages;
pub(crate) mod mv;
pub(crate) mod query;
mod query_cache;
mod runtime;
pub(crate) mod status;
pub mod subsumption_index;
mod types;
mod worker;
mod write_queue;
mod writer;

// Re-export public types
pub use fast_path::{
    CacheFastPath, CacheFastPathHandle, CacheFastPathPublisher, CacheFastPathUpdater,
};
pub use messages::{
    CacheMessage, CacheOutcome, CacheReply, DataStreamState, ProxyMessage, QueryParameter,
    QueryParameters,
};
pub use mv::{
    MvMeta, MvServe, MvState, ShapeGate, mv_serve_sql_into, mv_state_initial, mv_table_name,
    shape_classify,
};
pub use runtime::cache_setup;
pub use status::{
    CacheStatusData, CdcStatusData, LatencyStats, QueryStatusData, StatusRequest, StatusResponse,
};
pub use types::{
    Cache, CachedQuery, CachedQueryState, CachedQueryView, PinnedQuery, SubqueryKind,
    UpdateEvalStrategy, UpdateQueries, UpdateQuery, UpdateQuerySource,
};

error_set! {
    CacheError := WriteError || ReadError || DbError || ParseError || TableError || SendError || QueryResolutionError

    ReadError := {
        IoError(io::Error),
        InvalidMessage,
        /// Cache DB returned an ErrorResponse on the hit path. Triggers a
        /// forward-to-origin retry via CacheReply::Error. `sqlstate` is
        /// `None` only when the ErrorResponse frame was malformed.
        #[display("Cache server error (SQLSTATE: {sqlstate:?})")]
        CacheServerError {
            sqlstate: Option<[u8; 5]>,
        },
    }

    DbError := {
        NoConnection,
        PgError(Error),
        CdcFailure,
        WriterFailure,
        TooManyModifiedRows,
    }

    ParseError := {
        InvalidUtf8,
        Parse(pg_query::Error),
        AstTransformError(AstTransformError),
        /// Query structure is invalid (e.g., expected SELECT but got something else)
        InvalidQuery,
        Other,
    }

    SendError := {
        WorkerSend,
        Reply,
    }

    WriteError := {
        Write,
    }


    TableError := {
        #[display("Unknown Table: oid: {oid:?} name {name:?}")]
        UnknownTable {
            oid: Option<u32>,
            name: Option<String>,
        },
        #[display("Unknown type OID {type_oid} ('{type_name}') for column '{column_name}' in table '{table_name}'")]
        UnknownType {
            type_oid: u32,
            type_name: String,
            column_name: String,
            table_name: String,
        },
        #[display("Unsupported type '{type_name}': {reason}")]
        UnsupportedType {
            type_name: String,
            reason: String,
        },
        UnknownColumn,
        UnknownSchema,
        NoPrimaryKey,
    }

    QueryResolutionError := {
        ResolveError(ResolveError),
        DecorrelateError(DecorrelateError),
    }
}
