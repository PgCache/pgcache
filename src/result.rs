//! Result type extensions for location-tracking error reports.
//!
//! This module provides ergonomic error handling with automatic file:line capture
//! using the rootcause crate.
//!
//! # Usage
//!
//! ```ignore
//! use crate::result::ReportExt;
//!
//! fn level1() -> Result<(), Report<MyError>> {
//!     inner_function()?;  // Location captured here
//!     Ok(())
//! }
//!
//! fn level2() -> Result<(), Report<MyError>> {
//!     level1().attach_loc("in level2")?;  // Adds breadcrumb with location
//!     Ok(())
//! }
//! ```
//!
//! # Output
//!
//! ```text
//!  ● MyError
//!  ├ src/foo.rs:42
//!  ╰ in level2 at src/foo.rs:47
//! ```

use rootcause::Report;
use rootcause::hooks::builtin_hooks::location::Location;

/// A located attachment - combines a message with its source location.
/// Displays as "message at file:line"
#[derive(Debug, Clone)]
pub struct LocatedAttachment {
    pub message: String,
    pub location: Location,
}

impl core::fmt::Display for LocatedAttachment {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{} at {}", self.message, self.location)
    }
}

/// Extension trait to add location + attachment to an existing Report.
/// Use this when propagating errors to add breadcrumbs showing the call path.
pub trait ReportExt<C> {
    type Output;

    /// Attach a message along with the caller's file:line location.
    /// Displays as "message at file:line" to show the propagation path.
    fn attach_loc(self, message: impl Into<String>) -> Self::Output;
}

impl<C> ReportExt<C> for Report<C> {
    type Output = Report<C>;

    #[track_caller]
    fn attach_loc(self, message: impl Into<String>) -> Report<C> {
        self.attach(LocatedAttachment {
            message: message.into(),
            location: Location::caller(),
        })
    }
}

impl<T, C> ReportExt<C> for Result<T, Report<C>> {
    type Output = Result<T, Report<C>>;

    #[track_caller]
    fn attach_loc(self, message: impl Into<String>) -> Result<T, Report<C>> {
        match self {
            Ok(v) => Ok(v),
            Err(e) => Err(e.attach(LocatedAttachment {
                message: message.into(),
                location: Location::caller(),
            })),
        }
    }
}

/// Extension trait to convert `Result<T, E>` to `Result<T, Report<C>>` where `E: Into<C>`.
/// Use this when an error type needs two-step conversion: E → C → Report<C>.
pub trait MapIntoReport<T, E> {
    /// Convert the error through an intermediate type into a Report.
    /// Captures the caller's file:line location.
    fn map_into_report<C>(self) -> Result<T, Report<C>>
    where
        E: Into<C>,
        C: std::error::Error + Send + Sync + 'static;
}

impl<T, E> MapIntoReport<T, E> for Result<T, E> {
    #[track_caller]
    fn map_into_report<C>(self) -> Result<T, Report<C>>
    where
        E: Into<C>,
        C: std::error::Error + Send + Sync + 'static,
    {
        match self {
            Err(e) => Err(Report::new(e.into())),
            Ok(v) => Ok(v),
        }
    }
}

/// Format an error along with its full `source()` chain as `top: cause: cause...`.
///
/// `tokio_postgres::Error` and similar wrapper errors hide the useful detail
/// (SQLSTATE, message, IO cause) behind `source()`. Plain `Display` produces
/// uninformative strings like "db error". This walker surfaces the full chain.
pub fn error_chain_format(e: &dyn std::error::Error) -> String {
    let mut out = e.to_string();
    let mut src = e.source();
    while let Some(cause) = src {
        out.push_str(": ");
        out.push_str(&cause.to_string());
        src = cause.source();
    }
    out
}

#[cfg(test)]
mod tests {

    use std::fmt;

    use rootcause::prelude::ResultExt;

    use super::*;

    #[derive(Debug)]
    struct TestError(&'static str);

    impl fmt::Display for TestError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl std::error::Error for TestError {}

    type TestResult<T> = Result<T, Report<TestError>>;

    fn inner() -> Result<(), TestError> {
        Err(TestError("something failed"))
    }

    fn level1() -> TestResult<()> {
        inner()?;
        Ok(())
    }

    fn level2() -> TestResult<()> {
        level1().attach_loc("in level2")?;
        Ok(())
    }

    fn level3() -> TestResult<()> {
        level2().attach_loc("in level3")?;
        Ok(())
    }

    #[test]
    fn test_basic_conversion() {
        let err = level1().unwrap_err();
        let output = err.to_string();
        assert!(output.contains("something failed"));
        assert!(output.contains("result.rs"));
    }

    #[test]
    fn test_attach_loc_chain() {
        let err = level3().unwrap_err();
        let output = err.to_string();
        assert!(output.contains("something failed"));
        assert!(output.contains("in level2"));
        assert!(output.contains("in level3"));
    }

    #[test]
    fn test_attach_with_attach_loc() {
        let err: TestResult<()> = inner()
            .attach("key: value")
            .map_err(|e| e.attach_loc("with context"));
        let err = err.unwrap_err();
        let output = err.to_string();
        assert!(output.contains("something failed"));
        assert!(output.contains("key: value"));
        assert!(output.contains("with context"));
    }

    // For map_into_report test: an "external" error type
    #[derive(Debug)]
    struct ExternalError(&'static str);

    impl fmt::Display for ExternalError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "external: {}", self.0)
        }
    }

    impl std::error::Error for ExternalError {}

    // Conversion from ExternalError to TestError
    impl From<ExternalError> for TestError {
        fn from(e: ExternalError) -> Self {
            TestError(e.0)
        }
    }

    fn external_call() -> Result<(), ExternalError> {
        Err(ExternalError("connection failed"))
    }

    #[test]
    fn test_map_into_report() {
        // Simulates: tokio_postgres::Error -> CacheError -> Report<CacheError>
        let result: TestResult<()> = external_call().map_into_report();
        let err = result.unwrap_err();
        let output = err.to_string();
        assert!(output.contains("connection failed"));
        assert!(output.contains("result.rs"));
    }
}
