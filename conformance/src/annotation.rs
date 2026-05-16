//! Parser for `# pgcache:` routing directives in `.slt` files.
//!
//! A statement or query may be preceded by a comment line of the form
//! `# pgcache: cached`, asserting how pgcache must route it. Unmarked
//! statements default to [`Routing::Any`] — start lenient, tighten by
//! suite as we learn what should be cacheable.

use std::fmt;

/// Expected pgcache routing for a statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Routing {
    /// Must be served from cache on the second execution.
    Cached,
    /// Must be forwarded to origin; no MV build.
    Passthrough,
    /// Either is acceptable; only correctness is checked.
    #[default]
    Any,
}

impl fmt::Display for Routing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Routing::Cached => "cached",
            Routing::Passthrough => "passthrough",
            Routing::Any => "any",
        };
        f.write_str(s)
    }
}

/// A `# pgcache:` directive was present but its value was not recognized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownRouting {
    pub value: String,
}

impl fmt::Display for UnknownRouting {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "unknown pgcache routing `{}` (expected cached|passthrough|any)",
            self.value
        )
    }
}

impl std::error::Error for UnknownRouting {}

const PREFIX: &str = "pgcache:";

/// Parse a single comment line as a routing directive.
///
/// Returns `None` if the line is not a `# pgcache:` directive at all
/// (an ordinary comment), `Some(Ok(_))` for a recognized directive, and
/// `Some(Err(_))` when the directive is present but its value is junk.
pub fn line_parse(line: &str) -> Option<Result<Routing, UnknownRouting>> {
    let body = line.trim().trim_start_matches('#').trim();
    let rest = body.strip_prefix(PREFIX)?;
    let token = rest.trim().to_ascii_lowercase();
    Some(match token.as_str() {
        "cached" => Ok(Routing::Cached),
        "passthrough" => Ok(Routing::Passthrough),
        "any" => Ok(Routing::Any),
        _ => Err(UnknownRouting { value: token }),
    })
}

/// Scan a statement's leading comment lines for a routing directive.
///
/// The last directive wins, so a block-level default can be overridden
/// by a per-statement line. Absent any directive, defaults to
/// [`Routing::Any`]. A malformed directive is a hard error — a typo'd
/// annotation must not silently degrade to `Any`.
pub fn scan<'a, I>(comments: I) -> Result<Routing, UnknownRouting>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut routing = Routing::Any;
    for line in comments {
        if let Some(parsed) = line_parse(line) {
            routing = parsed?;
        }
    }
    Ok(routing)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unmarked_defaults_to_any() {
        assert_eq!(scan(std::iter::empty()).unwrap(), Routing::Any);
        assert_eq!(
            scan(["-- a comment", "# unrelated note"]).unwrap(),
            Routing::Any
        );
    }

    #[test]
    fn parses_each_variant() {
        assert_eq!(
            line_parse("# pgcache: cached").unwrap().unwrap(),
            Routing::Cached
        );
        assert_eq!(
            line_parse("# pgcache: passthrough").unwrap().unwrap(),
            Routing::Passthrough
        );
        assert_eq!(line_parse("# pgcache: any").unwrap().unwrap(), Routing::Any);
    }

    #[test]
    fn tolerates_whitespace_and_case() {
        assert_eq!(
            line_parse("   #   pgcache:   CACHED  ").unwrap().unwrap(),
            Routing::Cached
        );
    }

    #[test]
    fn non_directive_comment_is_none() {
        assert!(line_parse("# just a comment").is_none());
        assert!(line_parse("not a comment at all").is_none());
    }

    #[test]
    fn malformed_directive_is_error() {
        let err = line_parse("# pgcache: cahced").unwrap().unwrap_err();
        assert_eq!(err.value, "cahced");
        assert!(scan(["# pgcache: bogus"]).is_err());
    }

    #[test]
    fn last_directive_wins() {
        let routing = scan([
            "# pgcache: passthrough",
            "# a note in between",
            "# pgcache: cached",
        ])
        .unwrap();
        assert_eq!(routing, Routing::Cached);
    }
}
