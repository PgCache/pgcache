use std::fmt;
use std::io;

use ecow::EcoString;
use error_set::error_set;
use rootcause::Report;
#[cfg(feature = "proxy")]
use tokio_postgres::Error;

use crate::oid::{Oid, TypeOid};
#[cfg(feature = "proxy")]
use crate::pg::event_trigger::SignalDefect;
use crate::query::{
    decorrelate::DecorrelateError, resolve::ResolveError, transform::AstTransformError,
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
mod generation;
#[cfg(feature = "proxy")]
mod join_subsumers;
#[cfg(feature = "proxy")]
pub(crate) mod memo;
#[cfg(feature = "proxy")]
pub(crate) mod messages;
#[cfg(feature = "proxy")]
pub(crate) mod mv;
pub(crate) mod mv_shape;
#[cfg(feature = "proxy")]
pub(crate) mod output_settings;
#[cfg(feature = "proxy")]
mod population_pool;
#[cfg(feature = "proxy")]
pub(crate) mod pressure;
#[cfg(feature = "proxy")]
pub(crate) mod privileges;
pub mod query;
#[cfg(feature = "proxy")]
mod query_cache;
#[cfg(feature = "proxy")]
pub(crate) mod reference_settings;
#[cfg(feature = "proxy")]
mod reg_bucket;
#[cfg(feature = "proxy")]
mod reg_gate;
#[cfg(feature = "proxy")]
mod reply;
#[cfg(feature = "proxy")]
mod runtime;
#[cfg(feature = "proxy")]
pub use runtime::database_locale_describe;
#[cfg(feature = "proxy")]
mod serve;
pub mod serve_decision;
#[cfg(feature = "proxy")]
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
pub use generation::Generation;
#[cfg(feature = "proxy")]
pub use messages::{CacheMessage, CacheOutcome, CacheReply, DataStreamState, ProxyMessage};
#[cfg(feature = "proxy")]
pub use mv::{
    MvMeta, MvServe, MvServePlan, MvState, mv_serve_sql_into, mv_state_initial, mv_table_name,
};
pub use mv_shape::{ShapeGate, shape_classify};
#[cfg(feature = "proxy")]
pub use query_cache::{CacheDispatchHandle, CacheDispatchPublisher, CacheDispatchUpdater};
#[cfg(feature = "proxy")]
pub use reply::{Delivery, ReplySender, ReplySlot};
#[cfg(feature = "proxy")]
pub use runtime::{CacheGeneration, cache_generation_start, cache_supervise};
pub use serve_decision::CachedQueryState;
#[cfg(feature = "proxy")]
pub use status::{
    CacheStatusData, CdcStatusData, LatencyStats, QueryStatusData, StatusRequest, StatusResponse,
};
#[cfg(feature = "proxy")]
pub use types::{
    Cache, CachedQuery, CachedQueryView, PinnedQuery, RlsPolicyTerms, RlsRelationTerms,
    RlsRelations, WriteTargetMap,
};
pub use update_query::{
    SubqueryKind, UpdateEvalStrategy, UpdateQueries, UpdateQuery, UpdateQueryKey, UpdateQuerySource,
};

/// The five-character SQLSTATE of a backend `ErrorResponse`.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct SqlState([u8; 5]);

impl SqlState {
    /// `undefined_table`: what a hit gets when its cache table was dropped
    /// between dispatch and SELECT (the eviction-window race).
    pub const UNDEFINED_TABLE: Self = Self(*b"42P01");

    pub const fn new(code: [u8; 5]) -> Self {
        Self(code)
    }
}

impl fmt::Display for SqlState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.escape_ascii())
    }
}

impl fmt::Debug for SqlState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

error_set! {
    CacheError := WriteError || ReadError || DbError || ParseError || TableError || SendError || QueryResolutionError || SetupError || PrivilegeError || RlsError || FunctionError

    ReadError := {
        IoError(io::Error),
        InvalidMessage,
        /// Cache DB returned an ErrorResponse on the hit path. Triggers a
        /// forward-to-origin retry via CacheReply::Error.
        #[display("Cache server error (SQLSTATE {sqlstate})")]
        CacheServerError {
            sqlstate: SqlState,
        },
        /// As `CacheServerError`, from an ErrorResponse frame with no
        /// SQLSTATE field to read.
        #[display("Cache server error (no SQLSTATE in the ErrorResponse)")]
        CacheServerErrorMalformed,
        /// A serve failed after bytes reached the client inside a transaction
        /// block (PGC-387): the connection must close rather than forward.
        ServeAbandonedInBlock,
    }

    DbError := {
        NoConnection,
        #[cfg(feature = "proxy")]
        PgError(Error),
        CdcFailure,
        /// Origin lacks the schema-change signal. At startup pgcache
        /// refuses to run; found by the writer's poll, schema changes may
        /// have gone unreported, so the cache is torn down and rebuilt.
        #[cfg(feature = "proxy")]
        #[display("the schema-change event triggers on origin are {defect}; `pgcache --check` prints the SQL for an administrator to run")]
        SchemaSignal {
            defect: SignalDefect,
        },
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

    SetupError := {
        #[display("Cache-hit latency histogram could not be created")]
        LatencyHistogram,
        /// The installed `pgcache_pgrx` predates a function the writer calls
        /// on every merge chunk; refusing at startup beats failing every merge.
        #[display("installed pgcache_pgrx extension lacks {function}; rebuild and install the extension")]
        ExtensionOutdated {
            function: &'static str,
        },
        /// The cache host lacks a locale every serve applies (origin's default
        /// `lc_monetary`, `lc_numeric` or `lc_time`), so no hit could ever be
        /// served; refuse at startup (PGC-477).
        #[display("cache host cannot apply origin's default locales (lc_monetary={lc_monetary}, lc_numeric={lc_numeric}, lc_time={lc_time}): {detail}; install them on the cache host")]
        ReferenceLocales {
            lc_monetary: String,
            lc_numeric: String,
            lc_time: String,
            detail: String,
        },
        /// The cache cluster cannot create a database under origin's locale
        /// (PGC-587); serving under another would reorder text.
        #[display("cache database cannot be created with origin's locale ({locale}): install the locale on the cache host, or run the cache on a PostgreSQL whose locale provider matches origin's")]
        DatabaseLocale {
            locale: String,
        },
        /// The cache database came up under another locale provider than
        /// origin's, so the same collation name means different rules.
        #[display("cache database uses the {cache} locale provider where origin uses {origin}; the cache cluster's template default took over, so recreate the cache cluster with a matching --locale-provider")]
        LocaleProviderMismatch {
            origin: &'static str,
            cache: &'static str,
        },
        /// The cache host's libc or ICU orders text differently from origin's
        /// under the same collation name (PGC-587).
        #[display("cache host orders text differently from origin: at sample position {position} origin has {origin_string:?} where the cache has {cache_string:?} (origin {origin_locale}; cache {cache_locale}); run the cache on a host whose libc or ICU build matches origin's")]
        CollationMismatch {
            position: usize,
            origin_string: String,
            cache_string: String,
            origin_locale: String,
            cache_locale: String,
        },
    }


    TableError := {
        #[display("Unknown Table: oid: {oid:?} name {name:?}")]
        UnknownTable {
            oid: Option<Oid>,
            name: Option<String>,
        },
        #[display("Unknown type OID {type_oid} ('{type_name}') for column '{column_name}' in table '{table_name}'")]
        UnknownType {
            type_oid: TypeOid,
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
        #[display("Column '{column_name}' in table '{table_name}' has out-of-range position {position}")]
        ColumnPositionOutOfRange {
            position: i64,
            column_name: String,
            table_name: String,
        },
        UnknownSchema,
        NoPrimaryKey,
        /// The table has inheritance children: their rows are part of a
        /// read of it but stream under their own identity, and may repeat
        /// its primary key.
        #[display("table {schema}.{name} has inheritance children")]
        InheritanceParent {
            schema: EcoString,
            name: EcoString,
        },
        /// A partition's replica identity does not carry the table's key:
        /// its updates and deletes stream without the columns that find the
        /// cached row.
        #[display("table {schema}.{name} has a partition whose replica identity lacks the table's key")]
        PartitionIdentity {
            schema: EcoString,
            name: EcoString,
        },
        /// A column's collation is one the cache database lacks or orders
        /// differently from origin, so cached text of it would misorder.
        #[display("table {schema}.{name} has column {column} under collation {collation}, which the cache database {reason}")]
        CollationUnavailable {
            schema: EcoString,
            name: EcoString,
            column: EcoString,
            collation: EcoString,
            reason: &'static str,
        },
    }

    /// Why a read over a row-level-secured relation cannot be registered
    /// with its policies inlined (PGC-113); each forwards the read.
    RlsError := {
        #[display("row-level security: the read carries no session context")]
        RlsNoContext,
        #[display("row-level security: the role in force is unknown")]
        RlsRoleUnknown,
        #[display("row-level security: setting {name} is not known or not set")]
        RlsSettingUnknown {
            name: EcoString,
        },
        #[display("row-level security: no permissive policy applies to the role")]
        RlsDefaultDeny,
        #[display("row-level security: row_security is off and a policy applies")]
        RlsRowSecurityOff,
        #[display("row-level security: policy {policy} cannot be inlined: {reason}")]
        RlsUninlinable {
            policy: EcoString,
            reason: EcoString,
        },
        #[display("row-level security: {reason}")]
        RlsUnsupported {
            reason: &'static str,
        },
        /// pgcache's own role is subject to the policies, so a population
        /// would be filtered by them on top of the inlined ones.
        #[display("row-level security: pgcache's role cannot bypass it, so secured relations are not cached")]
        RlsRoleCannotBypass,
    }

    PrivilegeError := {
        #[display("Unparseable pg_current_snapshot() text '{text}'")]
        PrivilegeSnapshot {
            text: String,
        },
    }

    /// Why a function call keeps a query from registering (PGC-578); each
    /// forwards the read.
    FunctionError := {
        #[display("function {function} with {arg_count} arguments is not on the search_path")]
        FunctionUnknown {
            function: EcoString,
            arg_count: u32,
        },
        /// Neither PostgreSQL's own nor installed on the cache database with
        /// the same body, so it cannot be served or evaluated there (PGC-474).
        #[display("function {function} is not available on the cache database")]
        FunctionNotOnCache {
            function: EcoString,
        },
        /// Resolved on the search_path to an overload that is not immutable,
        /// in a position only an immutable function may hold.
        #[display("function {function} is not immutable on the search_path")]
        FunctionNotImmutable {
            function: EcoString,
        },
        /// The proxy inlined the one function of the name the catalog holds,
        /// but the session's search_path resolves the call to another
        /// function, or to none, or the function was redefined since the
        /// body was inlined (PGC-579).
        #[display("function {function} resolves on the search_path to other than the inlined body")]
        FunctionInlinedElsewhere {
            function: EcoString,
        },
    }

    QueryResolutionError := {
        ResolveError(ResolveError),
        DecorrelateError(DecorrelateError),
        /// The canonical form, its policies inlined, fails a cacheability
        /// rule the query as written passed at the proxy.
        #[display("canonical form is not cacheable: {reason}")]
        CanonicalUncacheable {
            reason: EcoString,
        },
        /// The folded session-term values the proxy keyed the read on do not
        /// pair with the query's fragments (PGC-580); the read forwards.
        #[display("session terms: {reason}")]
        SessionTermValues {
            reason: EcoString,
        },
    }
}
