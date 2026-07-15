use std::fmt;

use postgres_types::PgLsn;
use serde::{Deserialize, Serialize};

pub(crate) mod cache_connection;
pub(crate) mod cdc;
pub(crate) mod connect;
pub(crate) mod protocol;

pub use connect::{config_build, config_connect, connect};

/// A PostgreSQL WAL log sequence number — a monotonic byte position in the WAL
/// stream, used as the CDC apply/flush/snapshot watermark. A newtype over `u64`
/// for type safety: LSNs share `u64`'s layout with generations and other
/// counters, and the compiler otherwise can't stop them being mixed.
///
/// Construction is the explicit, greppable [`Lsn::from_raw`]; there is no
/// `From<u64>` or `Deref`. The wire boundary (`PgLsn`, `wal_end`) and the
/// byte-distance arithmetic for replication lag are the intentional crossings.
/// Not a hash — never key an identity-hashed map with it.
#[repr(transparent)]
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Default, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Lsn(u64);

impl Lsn {
    /// Wrap a raw `u64` WAL position as an `Lsn`. The only entry from an
    /// untyped `u64` — the wire boundary, SQL queries, and tests.
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// The underlying `u64` position, for the wire, SQL, and metrics.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Bytes of WAL between `earlier` and `self` — the difference of two
    /// positions is a byte distance, not another `Lsn` (cf.
    /// `Instant::saturating_duration_since`). Saturating because positions can
    /// transiently arrive out of order; an earlier-than-`earlier` `self`
    /// reports zero lag rather than underflowing.
    pub const fn saturating_bytes_since(self, earlier: Lsn) -> u64 {
        self.0.saturating_sub(earlier.0)
    }
}

impl From<PgLsn> for Lsn {
    fn from(lsn: PgLsn) -> Self {
        Self(u64::from(lsn))
    }
}

impl From<Lsn> for PgLsn {
    fn from(lsn: Lsn) -> Self {
        PgLsn::from(lsn.0)
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// PostgreSQL keywords that cannot appear as bare identifiers: the
/// reserved, type_func_name, and col_name categories — everything except
/// unreserved — mirroring PostgreSQL's own `quote_identifier()`. Only
/// all-lowercase words can reach this check (anything else already fails
/// the character-class test), so entries are lowercase. Sorted for
/// binary search; sortedness and category membership are test-enforced
/// against `pg_query::scan`.
static KEYWORDS_QUOTED: &[&str] = &[
    "all",
    "analyse",
    "analyze",
    "and",
    "any",
    "array",
    "as",
    "asc",
    "asymmetric",
    "authorization",
    "between",
    "bigint",
    "binary",
    "bit",
    "boolean",
    "both",
    "case",
    "cast",
    "char",
    "character",
    "check",
    "coalesce",
    "collate",
    "collation",
    "column",
    "concurrently",
    "constraint",
    "create",
    "cross",
    "current_catalog",
    "current_date",
    "current_role",
    "current_schema",
    "current_time",
    "current_timestamp",
    "current_user",
    "dec",
    "decimal",
    "default",
    "deferrable",
    "desc",
    "distinct",
    "do",
    "else",
    "end",
    "except",
    "exists",
    "extract",
    "false",
    "fetch",
    "float",
    "for",
    "foreign",
    "freeze",
    "from",
    "full",
    "grant",
    "greatest",
    "group",
    "grouping",
    "having",
    "ilike",
    "in",
    "initially",
    "inner",
    "inout",
    "int",
    "integer",
    "intersect",
    "interval",
    "into",
    "is",
    "isnull",
    "join",
    "json",
    "json_array",
    "json_arrayagg",
    "json_exists",
    "json_object",
    "json_objectagg",
    "json_query",
    "json_scalar",
    "json_serialize",
    "json_table",
    "json_value",
    "lateral",
    "leading",
    "least",
    "left",
    "like",
    "limit",
    "localtime",
    "localtimestamp",
    "merge_action",
    "national",
    "natural",
    "nchar",
    "none",
    "normalize",
    "not",
    "notnull",
    "null",
    "nullif",
    "numeric",
    "offset",
    "on",
    "only",
    "or",
    "order",
    "out",
    "outer",
    "overlaps",
    "overlay",
    "placing",
    "position",
    "precision",
    "primary",
    "real",
    "references",
    "returning",
    "right",
    "row",
    "select",
    "session_user",
    "setof",
    "similar",
    "smallint",
    "some",
    "substring",
    "symmetric",
    "system_user",
    "table",
    "tablesample",
    "then",
    "time",
    "timestamp",
    "to",
    "trailing",
    "treat",
    "trim",
    "true",
    "union",
    "unique",
    "user",
    "using",
    "values",
    "varchar",
    "variadic",
    "verbose",
    "when",
    "where",
    "window",
    "with",
    "xmlattributes",
    "xmlconcat",
    "xmlelement",
    "xmlexists",
    "xmlforest",
    "xmlnamespaces",
    "xmlparse",
    "xmlpi",
    "xmlroot",
    "xmlserialize",
    "xmltable",
];

/// Whether an identifier must be double-quoted to deparse safely:
/// unsafe characters (not `[a-z_][a-z0-9_]*`), or a PostgreSQL keyword
/// outside the unreserved category (`order`, `user`, ...).
pub fn identifier_needs_quotes(id: &str) -> bool {
    let chars_unsafe = match id.as_bytes() {
        [] => true,
        [first, rest @ ..] => {
            (!first.is_ascii_lowercase() && *first != b'_')
                || !rest
                    .iter()
                    .all(|&b| b == b'_' || b.is_ascii_lowercase() || b.is_ascii_digit())
        }
    };
    chars_unsafe || KEYWORDS_QUOTED.binary_search(&id).is_ok()
}

/// Quote an identifier into `buf`: always wraps in `"` and doubles any
/// embedded `"`. Buffer-writing counterpart of
/// `postgres_protocol::escape::escape_identifier` (which only allocates).
/// Always-quoting needs no keyword knowledge and is a no-op semantically
/// for ordinary lowercase names.
pub fn identifier_quote_into(id: &str, buf: &mut String) {
    buf.push('"');
    if id.contains('"') {
        for ch in id.chars() {
            if ch == '"' {
                buf.push('"');
            }
            buf.push(ch);
        }
    } else {
        buf.push_str(id);
    }
    buf.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keywords_quoted_sorted_and_deduped() {
        assert!(KEYWORDS_QUOTED.windows(2).all(|w| w[0] < w[1]));
    }

    /// Every entry must be a PostgreSQL keyword outside the unreserved
    /// category, per the parser's own scanner.
    #[test]
    fn test_keywords_quoted_match_pg_query_scan() {
        use pg_query::protobuf::KeywordKind;
        for word in KEYWORDS_QUOTED {
            let scanned = pg_query::scan(word).expect("scan keyword");
            let token = scanned.tokens.first().expect("one token");
            let kind = token.keyword_kind();
            assert!(
                matches!(
                    kind,
                    KeywordKind::ReservedKeyword
                        | KeywordKind::TypeFuncNameKeyword
                        | KeywordKind::ColNameKeyword
                ),
                "{word}: expected non-unreserved keyword, scanner says {kind:?}"
            );
        }
    }

    #[test]
    fn test_identifier_needs_quotes_keywords() {
        for word in [
            "order", "user", "select", "table", "where", "between", "left",
        ] {
            assert!(identifier_needs_quotes(word), "{word} must be quoted");
        }
        for word in ["id", "name", "data", "status", "order_id", "users"] {
            assert!(!identifier_needs_quotes(word), "{word} must not be quoted");
        }
    }

    #[test]
    fn test_identifier_needs_quotes_chars() {
        assert!(identifier_needs_quotes(""));
        assert!(identifier_needs_quotes("Order"));
        assert!(identifier_needs_quotes("camelCase"));
        assert!(identifier_needs_quotes("1st"));
        assert!(identifier_needs_quotes("has space"));
        assert!(identifier_needs_quotes("has\"quote"));
        assert!(!identifier_needs_quotes("_private"));
        assert!(!identifier_needs_quotes("a1_b2"));
    }

    #[test]
    fn test_identifier_quote_into() {
        let mut buf = String::new();
        identifier_quote_into("plain", &mut buf);
        assert_eq!(buf, "\"plain\"");

        buf.clear();
        identifier_quote_into("camelCase", &mut buf);
        assert_eq!(buf, "\"camelCase\"");

        buf.clear();
        identifier_quote_into("we\"ird", &mut buf);
        assert_eq!(buf, "\"we\"\"ird\"");
    }
}
