//! Shared Postgres deadlock detection for the writer subsystem. Both the
//! population worker pool and the CDC apply path treat `40P01` specially
//! (population retries with backoff; CDC recovers by invalidating the
//! affected relations) — the detection lives here so both agree on it.

use super::super::CacheError;

/// Postgres `deadlock_detected`.
pub(super) const SQLSTATE_DEADLOCK: &str = "40P01";

/// SQLSTATE of a `CacheError`, if it wraps a Postgres error.
pub(super) fn cache_error_sqlstate(e: &CacheError) -> Option<&str> {
    if let CacheError::PgError(pg) = e {
        pg.code().map(|c| c.code())
    } else {
        None
    }
}
