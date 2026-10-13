//! The value types a query tree holds: literals and the operator, join,
//! ordering and frame enums, with how each renders.

use ecow::EcoString;
use ordered_float::NotNan;
use postgres_protocol::escape;
use strum_macros::AsRefStr;

use super::Deparse;

// Core literal value types that can appear in SQL expressions.
//
// Cast fields use `EcoString` because they're nearly always short PG type
// names (`"bytea"`, `"int4[]"`, …) that fit `EcoString`'s inline-storage
// threshold — zero-alloc to construct from a `&'static str` and cheap to
// clone. See PGC-109.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LiteralValue {
    String(EcoString),
    StringWithCast(EcoString, EcoString),
    Integer(i64),
    Float(NotNan<f64>),
    Boolean(bool),
    Null,
    NullWithCast(EcoString),
    Parameter(EcoString), // For $1, $2, etc.
    /// A 1-D array literal: `(elements, cast)`. Produced by binary array
    /// parameter substitution (PGC-103) so the constraint analyzer can
    /// extract `WHERE col = ANY($1)` as an `InSet` constraint and let
    /// ANY-clause queries subsume each other (PGC-106). The cast string
    /// is the array type name, e.g. `"int4[]"`.
    Array(Vec<LiteralValue>, EcoString),
}

impl LiteralValue {
    /// Check if an Option<String> (from CDC row data) matches this LiteralValue.
    /// Parses the string value according to the type of this LiteralValue.
    pub fn matches(&self, row_value: &Option<String>) -> bool {
        match (row_value, self) {
            (None, LiteralValue::Null) => true,
            (None, LiteralValue::NullWithCast(_)) => true,
            (None, _) => false,
            (Some(row_str), LiteralValue::String(constraint_str)) => {
                row_str.as_str() == constraint_str.as_str()
            }
            (Some(row_str), LiteralValue::StringWithCast(constraint_str, _)) => {
                row_str.as_str() == constraint_str.as_str()
            }
            (Some(row_str), LiteralValue::Integer(constraint_int)) => {
                row_str.parse::<i64>().ok() == Some(*constraint_int)
            }
            (Some(row_str), LiteralValue::Float(constraint_float)) => {
                row_str
                    .parse::<f64>()
                    .ok()
                    .and_then(|f| NotNan::new(f).ok())
                    == Some(*constraint_float)
            }
            (Some(row_str), LiteralValue::Boolean(constraint_bool)) => {
                // PostgreSQL sends booleans as "t"/"f" in text protocol (used by CDC)
                match row_str.as_str() {
                    "t" | "true" => *constraint_bool,
                    "f" | "false" => !*constraint_bool,
                    _ => false,
                }
            }
            (Some(_), LiteralValue::Null) => false,
            (Some(_), LiteralValue::NullWithCast(_)) => false,
            (Some(_), LiteralValue::Parameter(_)) => false, // Parameters shouldn't appear in constraints
            // Array constraints flow through `ColumnConstraint::InSet` not
            // `Comparison`, so a row-vs-array equality test is never the
            // right question; reject defensively.
            (Some(_), LiteralValue::Array(_, _)) => false,
        }
    }
}

impl Deparse for LiteralValue {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            LiteralValue::String(s) => emit_escaped_string_literal(s, buf),
            LiteralValue::StringWithCast(s, cast) => {
                emit_escaped_string_literal(s, buf);
                buf.push_str("::");
                buf.push_str(cast);
            }
            LiteralValue::Integer(i) => {
                use std::fmt::Write;
                let _ = write!(buf, "{i}");
            }
            LiteralValue::Float(f) => {
                use std::fmt::Write;
                let _ = write!(buf, "{}", f.into_inner());
            }
            LiteralValue::Boolean(b) => {
                buf.push_str(if *b { "true" } else { "false" });
            }
            LiteralValue::Null => {
                buf.push_str("NULL");
            }
            LiteralValue::NullWithCast(cast) => {
                buf.push_str("NULL::");
                buf.push_str(cast);
            }
            LiteralValue::Parameter(p) => {
                buf.push_str(p);
            }
            LiteralValue::Array(elements, cast) => {
                // Bytes must match the previous `StringWithCast(text, cast)`
                // representation produced for binary array params (PGC-103),
                // so cache fingerprints don't shift between options B and C.
                let mut text = String::with_capacity(2 + elements.len() * 4);
                text.push('{');
                let mut first = true;
                for elem in elements {
                    if !first {
                        text.push(',');
                    }
                    first = false;
                    array_element_text_render(elem, &mut text);
                }
                text.push('}');

                emit_escaped_string_literal(&text, buf);
                buf.push_str("::");
                buf.push_str(cast);
            }
        };

        buf
    }
}

/// Forwards escaped output into a `String`, dropping the single leading space
/// that `escape_literal_into` emits before `E'…'` (libpq's guard against
/// interpolation right after an identifier). Our literals are always in value
/// position, so the space is unwanted; stripping it in the sink keeps the output
/// byte-identical to the old `escape_literal` + strip while avoiding the temp
/// `String` (PGC-346).
struct EscapedLiteralSink<'a> {
    buf: &'a mut String,
    at_start: bool,
}

impl std::fmt::Write for EscapedLiteralSink<'_> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        let s = if self.at_start {
            self.at_start = false;
            s.strip_prefix(' ').unwrap_or(s)
        } else {
            s
        };
        self.buf.push_str(s);
        Ok(())
    }
}

/// Emit a SQL string literal — `'...'` or `E'...'` if any byte needs escaping —
/// directly into `buf` with no intermediate allocation. Reuses the audited
/// `escape_literal_into` for the escaping itself; only the leading-space guard
/// is stripped (see [`EscapedLiteralSink`]).
pub(crate) fn emit_escaped_string_literal(s: &str, buf: &mut String) {
    let mut sink = EscapedLiteralSink {
        buf,
        at_start: true,
    };
    // Writing to a `String` sink is infallible.
    let _ = escape::escape_literal_into(s, &mut sink);
}

fn array_element_text_render(elem: &LiteralValue, out: &mut String) {
    use std::fmt::Write;
    match elem {
        LiteralValue::Null | LiteralValue::NullWithCast(_) => out.push_str("NULL"),
        LiteralValue::Integer(n) => {
            let _ = write!(out, "{n}");
        }
        LiteralValue::Float(f) => {
            let _ = write!(out, "{}", f.into_inner());
        }
        // PG array text format uses single-character `t`/`f`, not `true`/`false`.
        LiteralValue::Boolean(b) => out.push(if *b { 't' } else { 'f' }),
        LiteralValue::String(s) | LiteralValue::StringWithCast(s, _) => {
            array_element_text_push_quoted(s, out);
        }
        // `binary_array_to_literal` rejects multi-dim and only produces
        // scalar elements, so nested Array / unsubstituted Parameter
        // shouldn't reach this branch. `?` marks them visibly without
        // panicking (the resulting SQL would be invalid, fail loudly at
        // origin) — easier to debug than a silent wrong answer.
        LiteralValue::Array(_, _) | LiteralValue::Parameter(_) => out.push('?'),
    }
}

fn array_element_text_push_quoted(s: &str, out: &mut String) {
    // PG array text format requires quoting around elements containing
    // `{`, `}`, `,`, `"`, `\`, whitespace, the empty string, or the
    // literal `NULL` (case-insensitive — otherwise PG reads it as the
    // array null marker). Inside quotes, `"` and `\` are backslash-escaped.
    let needs_quote = s.is_empty()
        || s.eq_ignore_ascii_case("null")
        || s.chars()
            .any(|c| matches!(c, '{' | '}' | ',' | '"' | '\\') || c.is_whitespace());
    if !needs_quote {
        out.push_str(s);
        return;
    }
    out.push('"');
    for c in s.chars() {
        if c == '"' || c == '\\' {
            out.push('\\');
        }
        out.push(c);
    }
    out.push('"');
}

// Unary operators (prefix operators on single expression)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AsRefStr)]
#[strum(serialize_all = "UPPERCASE")]
pub enum UnaryOp {
    Not,
    #[strum(to_string = "IS NULL")]
    IsNull,
    #[strum(to_string = "IS NOT NULL")]
    IsNotNull,
    #[strum(to_string = "IS TRUE")]
    IsTrue,
    #[strum(to_string = "IS NOT TRUE")]
    IsNotTrue,
    #[strum(to_string = "IS FALSE")]
    IsFalse,
    #[strum(to_string = "IS NOT FALSE")]
    IsNotFalse,
}

impl Deparse for UnaryOp {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push_str(self.as_ref());
        buf
    }
}

// Binary operators (infix operators between two expressions)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AsRefStr)]
#[strum(serialize_all = "UPPERCASE")]
pub enum BinaryOp {
    // Logical
    And,
    Or,
    // Comparison
    #[strum(to_string = "=")]
    Equal,
    #[strum(to_string = "!=")]
    NotEqual,
    #[strum(to_string = "<")]
    LessThan,
    #[strum(to_string = "<=")]
    LessThanOrEqual,
    #[strum(to_string = ">")]
    GreaterThan,
    #[strum(to_string = ">=")]
    GreaterThanOrEqual,
    // Pattern matching
    Like,
    ILike,
    #[strum(to_string = "NOT LIKE")]
    NotLike,
    #[strum(to_string = "NOT ILIKE")]
    NotILike,
}

impl Deparse for BinaryOp {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push_str(self.as_ref());
        buf
    }
}

impl BinaryOp {
    /// Returns true if this is a logical operator (AND/OR).
    pub fn is_logical(&self) -> bool {
        matches!(self, BinaryOp::And | BinaryOp::Or)
    }

    /// Returns true if this is a comparison operator (=, !=, <, <=, >, >=).
    pub fn is_comparison(&self) -> bool {
        matches!(
            self,
            BinaryOp::Equal
                | BinaryOp::NotEqual
                | BinaryOp::LessThan
                | BinaryOp::LessThanOrEqual
                | BinaryOp::GreaterThan
                | BinaryOp::GreaterThanOrEqual
        )
    }

    /// Flip a comparison operator for `value op column` -> `column op' value` normalization.
    /// Returns `None` for non-comparison ops.
    pub fn op_flip(&self) -> Option<BinaryOp> {
        match self {
            BinaryOp::Equal => Some(BinaryOp::Equal),
            BinaryOp::NotEqual => Some(BinaryOp::NotEqual),
            BinaryOp::LessThan => Some(BinaryOp::GreaterThan),
            BinaryOp::LessThanOrEqual => Some(BinaryOp::GreaterThanOrEqual),
            BinaryOp::GreaterThan => Some(BinaryOp::LessThan),
            BinaryOp::GreaterThanOrEqual => Some(BinaryOp::LessThanOrEqual),
            BinaryOp::And
            | BinaryOp::Or
            | BinaryOp::Like
            | BinaryOp::ILike
            | BinaryOp::NotLike
            | BinaryOp::NotILike => None,
        }
    }
}

// Multi-operand operators (one subject with multiple values)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MultiOp {
    In,
    NotIn,
    Between,
    NotBetween,
    BetweenSymmetric,
    NotBetweenSymmetric,
    /// `expr op ANY (values)` -- comparison operator determines the test
    Any {
        comparison: BinaryOp,
    },
    /// `expr op ALL (values)` -- comparison operator determines the test
    All {
        comparison: BinaryOp,
    },
}

impl Deparse for MultiOp {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            MultiOp::In => buf.push_str("IN"),
            MultiOp::NotIn => buf.push_str("NOT IN"),
            MultiOp::Between => buf.push_str("BETWEEN"),
            MultiOp::NotBetween => buf.push_str("NOT BETWEEN"),
            MultiOp::BetweenSymmetric => buf.push_str("BETWEEN SYMMETRIC"),
            MultiOp::NotBetweenSymmetric => buf.push_str("NOT BETWEEN SYMMETRIC"),
            MultiOp::Any { comparison } => {
                comparison.deparse(buf);
                buf.push_str(" ANY");
            }
            MultiOp::All { comparison } => {
                comparison.deparse(buf);
                buf.push_str(" ALL");
            }
        }
        buf
    }
}

/// Arithmetic operators for expressions like `amount * 2`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AsRefStr)]
pub enum ArithmeticOp {
    #[strum(to_string = "+")]
    Add,
    #[strum(to_string = "-")]
    Subtract,
    #[strum(to_string = "*")]
    Multiply,
    #[strum(to_string = "/")]
    Divide,
    #[strum(to_string = "%")]
    Modulo,
}

/// SubLink type for subqueries in WHERE clauses
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubLinkType {
    /// EXISTS (SELECT ...)
    Exists,
    /// expr IN (SELECT ...) / expr op ANY (SELECT ...)
    Any,
    /// expr op ALL (SELECT ...)
    All,
    /// Scalar subquery returning single value
    Expr,
}

/// Set operation type for UNION/INTERSECT/EXCEPT
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SetOpType {
    Union,
    Intersect,
    Except,
}

impl Deparse for SetOpType {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            SetOpType::Union => buf.push_str("UNION"),
            SetOpType::Intersect => buf.push_str("INTERSECT"),
            SetOpType::Except => buf.push_str("EXCEPT"),
        }
        buf
    }
}

/// Materialization hint for a CTE definition.
#[derive(Debug, Clone, Copy, PartialEq, Hash)]
pub enum CteMaterialization {
    /// No keyword -- optimizer decides
    Default,
    /// AS MATERIALIZED -- evaluate once, results reused
    Materialized,
    /// AS NOT MATERIALIZED -- optimizer may inline
    NotMaterialized,
}

/// Explicit NULLS ordering on an `ORDER BY` item. `Default` emits nothing so
/// PostgreSQL applies its built-in ASC=last / DESC=first defaults (PGC-144).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NullOrder {
    Default,
    NullsFirst,
    NullsLast,
}

impl Deparse for NullOrder {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            NullOrder::Default => {}
            NullOrder::NullsFirst => buf.push_str(" NULLS FIRST"),
            NullOrder::NullsLast => buf.push_str(" NULLS LAST"),
        }
        buf
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum OrderDirection {
    Asc,
    Desc,
}

impl Deparse for OrderDirection {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            OrderDirection::Asc => buf.push_str("ASC"),
            OrderDirection::Desc => buf.push_str("DESC"),
        }
        buf
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LimitClause {
    pub count: Option<LiteralValue>,
    pub offset: Option<LiteralValue>,
}

impl Deparse for LimitClause {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        if let Some(count) = &self.count {
            buf.push_str(" LIMIT ");
            count.deparse(buf);
        }
        if let Some(offset) = &self.offset {
            buf.push_str(" OFFSET ");
            offset.deparse(buf);
        }
        buf
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Hash)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

impl JoinType {
    /// The SQL keyword for this join type, with a leading space, for
    /// deparse (e.g. `" LEFT JOIN"`). A `NATURAL`/`CROSS` qualifier is
    /// rendered by the caller around this.
    pub(crate) fn join_keyword(self) -> &'static str {
        match self {
            JoinType::Inner => " JOIN",
            JoinType::Left => " LEFT JOIN",
            JoinType::Right => " RIGHT JOIN",
            JoinType::Full => " FULL JOIN",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrameMode {
    Rows,
    Range,
    Groups,
}

impl Deparse for FrameMode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push_str(match self {
            FrameMode::Rows => "ROWS",
            FrameMode::Range => "RANGE",
            FrameMode::Groups => "GROUPS",
        });
        buf
    }
}

/// Frame `EXCLUDE` option. `NoOthers` is the default and emits nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrameExclusion {
    NoOthers,
    CurrentRow,
    Group,
    Ties,
}

impl Deparse for FrameExclusion {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            FrameExclusion::NoOthers => {}
            FrameExclusion::CurrentRow => buf.push_str(" EXCLUDE CURRENT ROW"),
            FrameExclusion::Group => buf.push_str(" EXCLUDE GROUP"),
            FrameExclusion::Ties => buf.push_str(" EXCLUDE TIES"),
        }
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// PGC-346: the zero-alloc `emit_escaped_string_literal` must be
    /// byte-identical to the previous `escape_literal` + leading-space strip.
    fn old_emit(s: &str) -> String {
        let escaped = escape::escape_literal(s);
        if let Some(stripped) = escaped.strip_prefix(" E'") {
            format!("E'{stripped}")
        } else {
            escaped
        }
    }

    #[test]
    fn test_emit_escaped_string_literal_matches_prior_output() {
        let cases = [
            "",
            "plain",
            "o'brien",            // single quote → doubled
            "a\\b",               // backslash → E'…' with doubled backslash
            "both ' and \\ here", // quote + backslash
            "''''",               // only quotes
            "\\\\\\",             // only backslashes
            "café ☕ unicode",    // multibyte, no escaping
            "tab\tnewline\n",     // control chars, no special escaping
        ];
        for case in cases {
            let mut got = String::new();
            emit_escaped_string_literal(case, &mut got);
            assert_eq!(got, old_emit(case), "mismatch for input {case:?}");
        }
    }

    #[test]
    fn test_emit_escaped_string_literal_appends_without_clobbering() {
        let mut buf = String::from("prefix ");
        emit_escaped_string_literal("a\\b", &mut buf);
        assert_eq!(buf, format!("prefix {}", old_emit("a\\b")));
    }
}
