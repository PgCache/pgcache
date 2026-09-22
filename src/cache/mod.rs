use std::io;

use crate::oid::Oid;
use error_set::error_set;
use rootcause::Report;
#[cfg(feature = "proxy")]
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
pub mod admission;
#[cfg(feature = "proxy")]
mod cdc;
#[cfg(feature = "proxy")]
mod coalesce_queue;
#[cfg(feature = "proxy")]
mod explain;
#[cfg(feature = "proxy")]
mod fast_path;
#[cfg(feature = "proxy")]
pub(crate) mod memo;
#[cfg(feature = "proxy")]
pub(crate) mod messages;
// MV build/backoff paths are writer-only; the analysis-only build sees them as dead.
#[cfg_attr(not(feature = "proxy"), allow(dead_code))]
pub(crate) mod mv;
#[cfg(feature = "proxy")]
mod population_pool;
pub mod query;
#[cfg(feature = "proxy")]
mod query_cache;
#[cfg(feature = "proxy")]
mod reg_bucket;
mod reg_gate;
#[cfg(feature = "proxy")]
mod reply;
#[cfg(feature = "proxy")]
mod runtime;
#[cfg(feature = "proxy")]
mod serve;
pub mod serve_decision;
mod serve_pool_state;
#[cfg(feature = "proxy")]
pub(crate) mod status;
#[cfg(feature = "proxy")]
mod types;
mod update_query;
#[cfg(feature = "proxy")]
mod write_queue;
#[cfg(feature = "proxy")]
mod writer;

// Re-export public types
pub use query::{CacheabilityError, CacheableQuery, QueryParameter, QueryParameters};

#[cfg(feature = "proxy")]
pub use messages::{CacheMessage, CacheOutcome, CacheReply, DataStreamState, ProxyMessage};
pub use mv::{
    MvMeta, MvServe, MvState, ShapeGate, mv_serve_sql_into, mv_state_initial, mv_table_name,
    shape_classify,
};
#[cfg(feature = "proxy")]
pub use query_cache::{CacheDispatchHandle, CacheDispatchPublisher, CacheDispatchUpdater};
#[cfg(feature = "proxy")]
pub use reply::{ReplySender, ReplySlot, ReplyState};
#[cfg(feature = "proxy")]
pub use runtime::{CacheGeneration, cache_generation_start, cache_supervise};
pub use serve_decision::CachedQueryState;
#[cfg(feature = "proxy")]
pub use status::{
    CacheStatusData, CdcStatusData, LatencyStats, QueryStatusData, StatusRequest, StatusResponse,
};
#[cfg(feature = "proxy")]
pub use types::{Cache, CachedQuery, CachedQueryView, PinnedQuery};
pub use update_query::{
    SubqueryKind, UpdateEvalStrategy, UpdateQueries, UpdateQuery, UpdateQuerySource,
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
        #[cfg(feature = "proxy")]
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
        /// The writer command channel (`query_tx`) is closed — the cache
        /// subsystem is tearing down or restarting. Unrelated to the serve pool,
        /// which degrades to origin-forward on a closed channel rather than
        /// erroring.
        WriterSend,
        Reply,
    }

    WriteError := {
        Write,
    }


    TableError := {
        #[display("Unknown Table: oid: {oid:?} name {name:?}")]
        UnknownTable {
            oid: Option<Oid>,
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
