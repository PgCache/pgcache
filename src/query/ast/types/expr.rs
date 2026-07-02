use std::any::Any;
use std::ops::ControlFlow;

use ecow::EcoString;
use ordered_float::NotNan;
use postgres_protocol::escape;
use strum_macros::AsRefStr;

use crate::cache::{SubqueryKind, UpdateQuerySource};
use crate::query::ast::Deparse;
use crate::query::cast::{CastTarget, cast_target_deparse};

use super::*;

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

impl AstNode for LiteralValue {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        ControlFlow::Continue(())
    }
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

// Column reference (potentially qualified: table.column)
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct ColumnNode {
    pub table: Option<EcoString>,
    pub column: EcoString,
}

impl AstNode for ColumnNode {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        ControlFlow::Continue(())
    }
}

impl Deparse for ColumnNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        if let Some(table) = &self.table {
            table.deparse(buf);
            buf.push('.');
        }
        self.column.deparse(buf);

        buf
    }
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

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct UnaryExpr {
    pub op: UnaryOp,
    pub expr: Box<WhereExpr>,
}

impl AstNode for UnaryExpr {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        self.expr.try_for_each_node(f)?;
        ControlFlow::Continue(())
    }
}

impl Deparse for UnaryExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self.op {
            UnaryOp::IsNull
            | UnaryOp::IsNotNull
            | UnaryOp::IsTrue
            | UnaryOp::IsNotTrue
            | UnaryOp::IsFalse
            | UnaryOp::IsNotFalse => {
                // Postfix operators: expr IS NULL, expr IS TRUE, etc.
                self.expr.deparse(buf);
                buf.push(' ');
                self.op.deparse(buf);
            }
            UnaryOp::Not => {
                // Prefix operator: NOT expr
                // NOT has higher precedence than AND/OR, so NOT applied to a
                // logical binary expression needs parentheses to preserve
                // semantics: NOT (a AND b) != NOT a AND b
                let needs_parens = matches!(
                    self.expr.as_ref(),
                    WhereExpr::Binary(child) if child.op.is_logical()
                );
                self.op.deparse(buf);
                buf.push(' ');
                if needs_parens {
                    buf.push('(');
                }
                self.expr.deparse(buf);
                if needs_parens {
                    buf.push(')');
                }
            }
        }
        buf
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct BinaryExpr {
    pub op: BinaryOp,
    pub lexpr: Box<WhereExpr>, // left expression
    pub rexpr: Box<WhereExpr>, // right expression
}

impl AstNode for BinaryExpr {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        self.lexpr.try_for_each_node(f)?;
        self.rexpr.try_for_each_node(f)?;
        ControlFlow::Continue(())
    }
}

impl BinaryExpr {
    /// Whether a child expression needs parentheses to preserve semantics.
    /// This occurs when the child is a logical op with lower precedence than
    /// the parent (i.e., OR nested inside AND).
    fn child_needs_parens(&self, child: &WhereExpr) -> bool {
        if let WhereExpr::Binary(child_expr) = child {
            matches!((&self.op, &child_expr.op), (BinaryOp::And, BinaryOp::Or))
        } else {
            false
        }
    }
}

impl Deparse for BinaryExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        if self.child_needs_parens(&self.lexpr) {
            buf.push('(');
            self.lexpr.deparse(buf);
            buf.push(')');
        } else {
            self.lexpr.deparse(buf);
        }

        buf.push(' ');
        self.op.deparse(buf);
        buf.push(' ');

        if self.child_needs_parens(&self.rexpr) {
            buf.push('(');
            self.rexpr.deparse(buf);
            buf.push(')');
        } else {
            self.rexpr.deparse(buf);
        }

        buf
    }
}

// Multi-operand expressions (for IN, BETWEEN, etc.)
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct MultiExpr {
    pub op: MultiOp,
    pub exprs: Vec<WhereExpr>,
}

impl AstNode for MultiExpr {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        for expr in &self.exprs {
            expr.try_for_each_node(f)?;
        }
        ControlFlow::Continue(())
    }
}

impl Deparse for MultiExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        // MultiExpr format: [column, value1, value2, ...]
        // Output: column IN (value1, value2, ...) or column NOT IN (...)
        let [first, rest @ ..] = self.exprs.as_slice() else {
            return buf;
        };

        // First expression is the column/left side
        first.deparse(buf);

        match self.op {
            MultiOp::In => buf.push_str(" IN ("),
            MultiOp::NotIn => buf.push_str(" NOT IN ("),
            MultiOp::Between
            | MultiOp::NotBetween
            | MultiOp::BetweenSymmetric
            | MultiOp::NotBetweenSymmetric => {
                buf.push(' ');
                self.op.deparse(buf);
                buf.push(' ');
                // BETWEEN low AND high -- exactly 2 bounds
                let mut sep = "";
                for expr in rest {
                    buf.push_str(sep);
                    expr.deparse(buf);
                    sep = " AND ";
                }
                return buf;
            }
            MultiOp::Any { .. } | MultiOp::All { .. } => {
                buf.push(' ');
                self.op.deparse(buf);
                buf.push_str(" (");
            }
        }

        // Remaining expressions are the values
        let mut sep = "";
        for expr in rest {
            buf.push_str(sep);
            expr.deparse(buf);
            sep = ", ";
        }
        buf.push(')');
        buf
    }
}

/// Predicate-bearing expression. Scalar leaves are wrapped in `Scalar(ScalarExpr)`.
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum WhereExpr {
    /// Scalar leaf — value, column, function call, arithmetic, array literal,
    /// scalar subquery, etc. The predicate context interprets the resulting
    /// value (typically boolean).
    Scalar(ScalarExpr),

    Unary(UnaryExpr),
    Binary(BinaryExpr),
    Multi(MultiExpr),

    /// Predicate sublink: EXISTS, IN, ANY, ALL.
    /// Scalar subqueries appear via `Scalar(ScalarExpr::Subquery(...))`.
    Subquery {
        query: Box<QueryExpr>,
        sublink_type: SubLinkType,
        /// Left-hand expression for IN/ANY/ALL (e.g., `id` in `id IN (SELECT ...)`)
        test_expr: Option<Box<ScalarExpr>>,
    },
}

impl AstNode for WhereExpr {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        match self {
            WhereExpr::Scalar(scalar) => scalar.try_for_each_node(f)?,
            WhereExpr::Unary(unary) => unary.try_for_each_node(f)?,
            WhereExpr::Binary(binary) => binary.try_for_each_node(f)?,
            WhereExpr::Multi(multi) => multi.try_for_each_node(f)?,
            WhereExpr::Subquery {
                query, test_expr, ..
            } => {
                query.try_for_each_node(f)?;
                if let Some(e) = test_expr {
                    e.try_for_each_node(f)?;
                }
            }
        }
        ControlFlow::Continue(())
    }
}

impl WhereExpr {
    pub fn has_subqueries(&self) -> bool {
        match self {
            WhereExpr::Scalar(scalar) => scalar.has_subqueries(),
            WhereExpr::Binary(binary) => {
                binary.lexpr.has_subqueries() || binary.rexpr.has_subqueries()
            }
            WhereExpr::Unary(unary) => unary.expr.has_subqueries(),
            WhereExpr::Multi(multi) => multi.exprs.iter().any(|e| e.has_subqueries()),
            WhereExpr::Subquery { .. } => true,
        }
    }

    /// Recursively collect subquery branches with source tracking.
    /// `negated` tracks NOT-wrapping to flip Inclusion/Exclusion for
    /// EXISTS/ANY subqueries. ALL is already Exclusion (NOT IN).
    pub(crate) fn subquery_nodes_collect<'a>(
        &'a self,
        branches: &mut Vec<(&'a SelectNode, UpdateQuerySource)>,
        negated: bool,
    ) {
        match self {
            WhereExpr::Scalar(scalar) => scalar.subquery_nodes_collect(branches),
            WhereExpr::Binary(binary) => {
                binary.lexpr.subquery_nodes_collect(branches, negated);
                binary.rexpr.subquery_nodes_collect(branches, negated);
            }
            WhereExpr::Unary(unary) => {
                let child_negated = if unary.op == UnaryOp::Not {
                    !negated
                } else {
                    negated
                };
                unary.expr.subquery_nodes_collect(branches, child_negated);
            }
            WhereExpr::Multi(multi) => {
                for expr in &multi.exprs {
                    expr.subquery_nodes_collect(branches, negated);
                }
            }
            WhereExpr::Subquery {
                query,
                sublink_type,
                test_expr,
            } => {
                let kind = match sublink_type {
                    SubLinkType::Expr => SubqueryKind::Scalar,
                    SubLinkType::Any | SubLinkType::Exists => {
                        if negated {
                            SubqueryKind::Exclusion
                        } else {
                            SubqueryKind::Inclusion
                        }
                    }
                    SubLinkType::All => {
                        if negated {
                            SubqueryKind::Inclusion
                        } else {
                            SubqueryKind::Exclusion
                        }
                    }
                };
                let source = UpdateQuerySource::Subquery(kind);
                query.select_nodes_collect(branches, source, negated);
                if let Some(test) = test_expr {
                    test.subquery_nodes_collect(branches);
                }
            }
        }
    }
}

impl Deparse for WhereExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            WhereExpr::Scalar(scalar) => {
                scalar.deparse(buf);
            }
            WhereExpr::Unary(expr) => {
                expr.deparse(buf);
            }
            WhereExpr::Binary(expr) => {
                expr.deparse(buf);
            }
            WhereExpr::Multi(expr) => {
                expr.deparse(buf);
            }
            WhereExpr::Subquery {
                query,
                sublink_type,
                test_expr,
            } => match sublink_type {
                SubLinkType::Exists => {
                    buf.push_str("EXISTS (");
                    query.deparse(buf);
                    buf.push(')');
                }
                SubLinkType::Any => {
                    // IN is a special case of ANY
                    if let Some(test) = test_expr {
                        test.deparse(buf);
                        buf.push_str(" IN (");
                        query.deparse(buf);
                        buf.push(')');
                    } else {
                        buf.push('(');
                        query.deparse(buf);
                        buf.push(')');
                    }
                }
                SubLinkType::All => {
                    if let Some(test) = test_expr {
                        test.deparse(buf);
                        buf.push_str(" <> ALL (");
                        query.deparse(buf);
                        buf.push(')');
                    } else {
                        buf.push_str("ALL (");
                        query.deparse(buf);
                        buf.push(')');
                    }
                }
                SubLinkType::Expr => {
                    // Scalar subquery in a predicate position (rare; usually
                    // routed through Scalar). Bare parenthesized query.
                    buf.push('(');
                    query.deparse(buf);
                    buf.push(')');
                }
            },
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

/// Arithmetic expression: `left op right` (e.g., `amount * -1`)
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct ArithmeticExpr {
    pub left: Box<ScalarExpr>,
    pub op: ArithmeticOp,
    pub right: Box<ScalarExpr>,
}

impl AstNode for ArithmeticExpr {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        self.left.try_for_each_node(f)?;
        self.right.try_for_each_node(f)?;
        ControlFlow::Continue(())
    }
}

impl ArithmeticExpr {
    pub fn has_subqueries(&self) -> bool {
        self.left.has_subqueries() || self.right.has_subqueries()
    }
}

impl Deparse for ArithmeticExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push('(');
        self.left.deparse(buf);
        buf.push(' ');
        buf.push_str(self.op.as_ref());
        buf.push(' ');
        self.right.deparse(buf);
        buf.push(')');
        buf
    }
}

/// A scalar-valued expression. Appears in SELECT columns, function args,
/// arithmetic operands, ARRAY elements, CASE arms, TypeCast inner, scalar
/// subqueries, window PARTITION BY / ORDER BY, and predicate leaves.
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum ScalarExpr {
    Column(ColumnNode),
    Function(FunctionCall),
    Literal(LiteralValue),
    Case(CaseExpr),
    Arithmetic(ArithmeticExpr),
    Subquery(Box<QueryExpr>),
    Array(Vec<ScalarExpr>),
    // target classified at AST-conversion time so deparse stays a pure tree
    // walk (no TypeName re-traversal per cache hit) and evaluator/classifier
    // can match on the enum rather than re-parsing strings.
    TypeCast {
        expr: Box<ScalarExpr>,
        target: CastTarget,
    },
}

impl AstNode for ScalarExpr {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        match self {
            ScalarExpr::Column(col) => col.try_for_each_node(f)?,
            ScalarExpr::Function(func) => func.try_for_each_node(f)?,
            ScalarExpr::Literal(lit) => lit.try_for_each_node(f)?,
            ScalarExpr::Case(case) => case.try_for_each_node(f)?,
            ScalarExpr::Arithmetic(arith) => arith.try_for_each_node(f)?,
            ScalarExpr::Subquery(query) => query.try_for_each_node(f)?,
            ScalarExpr::Array(elems) => {
                for e in elems {
                    e.try_for_each_node(f)?;
                }
            }
            ScalarExpr::TypeCast { expr, .. } => expr.try_for_each_node(f)?,
        }
        ControlFlow::Continue(())
    }
}

impl ScalarExpr {
    pub fn has_subqueries(&self) -> bool {
        match self {
            ScalarExpr::Function(func) => func.has_subqueries(),
            ScalarExpr::Case(case) => case.has_subqueries(),
            ScalarExpr::Arithmetic(arith) => arith.has_subqueries(),
            ScalarExpr::Subquery(_) => true,
            ScalarExpr::Array(elems) => elems.iter().any(|e| e.has_subqueries()),
            ScalarExpr::TypeCast { expr, .. } => expr.has_subqueries(),
            ScalarExpr::Column(_) | ScalarExpr::Literal(_) => false,
        }
    }

    /// All subqueries within a scalar expression are Scalar in nature.
    pub(crate) fn subquery_nodes_collect<'a>(
        &'a self,
        branches: &mut Vec<(&'a SelectNode, UpdateQuerySource)>,
    ) {
        match self {
            ScalarExpr::Column(_) | ScalarExpr::Literal(_) => {}
            ScalarExpr::Function(func) => {
                for arg in &func.args {
                    arg.subquery_nodes_collect(branches);
                }
                for clause in &func.agg_order {
                    clause.expr.subquery_nodes_collect(branches);
                }
                if let Some(filter) = &func.agg_filter {
                    filter.subquery_nodes_collect(branches, false);
                }
                if let Some(over) = &func.over {
                    for col in &over.partition_by {
                        col.subquery_nodes_collect(branches);
                    }
                    for clause in &over.order_by {
                        clause.expr.subquery_nodes_collect(branches);
                    }
                }
            }
            ScalarExpr::Case(case) => {
                if let Some(arg) = &case.arg {
                    arg.subquery_nodes_collect(branches);
                }
                for when in &case.whens {
                    when.condition.subquery_nodes_collect(branches, false);
                    when.result.subquery_nodes_collect(branches);
                }
                if let Some(default) = &case.default {
                    default.subquery_nodes_collect(branches);
                }
            }
            ScalarExpr::Arithmetic(arith) => {
                arith.left.subquery_nodes_collect(branches);
                arith.right.subquery_nodes_collect(branches);
            }
            ScalarExpr::Subquery(query) => {
                let source = UpdateQuerySource::Subquery(SubqueryKind::Scalar);
                query.select_nodes_collect(branches, source, false);
            }
            ScalarExpr::Array(elems) => {
                for elem in elems {
                    elem.subquery_nodes_collect(branches);
                }
            }
            ScalarExpr::TypeCast { expr, .. } => {
                expr.subquery_nodes_collect(branches);
            }
        }
    }
}

impl Deparse for ScalarExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            ScalarExpr::Column(col) => col.deparse(buf),
            ScalarExpr::Function(func) => func.deparse(buf),
            ScalarExpr::Literal(lit) => lit.deparse(buf),
            ScalarExpr::Case(case) => case.deparse(buf),
            ScalarExpr::Arithmetic(arith) => arith.deparse(buf),
            ScalarExpr::Subquery(select) => {
                buf.push('(');
                select.deparse(buf);
                buf.push(')');
                buf
            }
            ScalarExpr::Array(elems) => {
                buf.push_str("ARRAY[");
                let mut sep = "";
                for elem in elems {
                    buf.push_str(sep);
                    elem.deparse(buf);
                    sep = ", ";
                }
                buf.push(']');
                buf
            }
            ScalarExpr::TypeCast { expr, target } => {
                buf.push('(');
                expr.deparse(buf);
                buf.push_str(")::");
                buf.push_str(cast_target_deparse(target));
                buf
            }
        }
    }
}

/// CASE expression: CASE [arg] WHEN condition THEN result [...] [ELSE default] END
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct CaseExpr {
    /// For simple CASE (CASE expr WHEN val...), holds the expression being tested.
    /// None for searched CASE (CASE WHEN condition...).
    pub arg: Option<Box<ScalarExpr>>,
    /// List of WHEN clauses
    pub whens: Vec<CaseWhen>,
    /// ELSE result (None means NULL if no WHEN matches)
    pub default: Option<Box<ScalarExpr>>,
}

impl AstNode for CaseExpr {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        if let Some(a) = &self.arg {
            a.try_for_each_node(f)?;
        }
        for w in &self.whens {
            w.try_for_each_node(f)?;
        }
        if let Some(d) = &self.default {
            d.try_for_each_node(f)?;
        }
        ControlFlow::Continue(())
    }
}

impl CaseExpr {
    /// Check if this CASE expression contains sublinks/subqueries
    pub fn has_subqueries(&self) -> bool {
        self.arg.as_ref().is_some_and(|a| a.has_subqueries())
            || self.whens.iter().any(|w| w.has_subqueries())
            || self.default.as_ref().is_some_and(|d| d.has_subqueries())
    }
}

impl Deparse for CaseExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push_str("CASE");
        if let Some(arg) = &self.arg {
            buf.push(' ');
            arg.deparse(buf);
        }
        for when in &self.whens {
            buf.push_str(" WHEN ");
            when.condition.deparse(buf);
            buf.push_str(" THEN ");
            when.result.deparse(buf);
        }
        if let Some(default) = &self.default {
            buf.push_str(" ELSE ");
            default.deparse(buf);
        }
        buf.push_str(" END");
        buf
    }
}

/// A single WHEN clause in a CASE expression
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct CaseWhen {
    /// The condition (for searched CASE) or value (for simple CASE)
    pub condition: WhereExpr,
    /// The result if condition is true/matches
    pub result: ScalarExpr,
}

impl AstNode for CaseWhen {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        self.condition.try_for_each_node(f)?;
        self.result.try_for_each_node(f)?;
        ControlFlow::Continue(())
    }
}

impl CaseWhen {
    pub fn has_subqueries(&self) -> bool {
        self.condition.has_subqueries() || self.result.has_subqueries()
    }
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
    fn emit_escaped_string_literal_matches_prior_output() {
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
    fn emit_escaped_string_literal_appends_without_clobbering() {
        let mut buf = String::from("prefix ");
        emit_escaped_string_literal("a\\b", &mut buf);
        assert_eq!(buf, format!("prefix {}", old_emit("a\\b")));
    }
}
