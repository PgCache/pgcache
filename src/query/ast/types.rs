use std::any::Any;
use std::ops::ControlFlow;

use ecow::EcoString;
use ordered_float::NotNan;
use postgres_protocol::escape;
use smallvec::SmallVec;
use strum_macros::AsRefStr;

use crate::cache::{SubqueryKind, UpdateQuerySource};
use crate::query::cast::{CastTarget, cast_target_deparse};

use super::Deparse;

/// Traversable AST node. The zero-allocation `try_for_each_node` visitor is
/// hand-written per type; `nodes()` is a provided collecting wrapper over it.
pub trait AstNode {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B>;

    /// Collect all descendant nodes of type `N` (provided).
    fn nodes<N: Any>(&self) -> impl Iterator<Item = &N> {
        let mut out = Vec::new();
        let _ = self.try_for_each_node::<N, ()>(&mut |n| {
            out.push(n);
            ControlFlow::Continue(())
        });
        out.into_iter()
    }
}

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

/// Emit a SQL string literal — `'...'` or `E'...'` if any byte needs
/// escaping. `escape_literal` returns the leading-space `" E'..."` form
/// for the latter case; we strip the space so the output stays
/// concatenation-friendly.
fn emit_escaped_string_literal(s: &str, buf: &mut String) {
    let escaped = escape::escape_literal(s);
    if let Some(stripped) = escaped.strip_prefix(" E'") {
        buf.push_str("E'");
        buf.push_str(stripped);
    } else {
        buf.push_str(&escaped);
    }
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

// ============================================================================
// New Query Type Hierarchy (for UNION/INTERSECT/EXCEPT support)
// ============================================================================

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

/// VALUES clause with typed rows - represents `VALUES (1, 'a'), (2, 'b')`
#[derive(Debug, Clone, PartialEq, Hash, Default)]
pub struct ValuesClause {
    pub rows: Vec<Vec<LiteralValue>>,
}

impl AstNode for ValuesClause {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        for row in &self.rows {
            for v in row {
                v.try_for_each_node(f)?;
            }
        }
        ControlFlow::Continue(())
    }
}

impl ValuesClause {
    pub fn has_subqueries(&self) -> bool {
        false // VALUES clauses contain only literals
    }
}

impl Deparse for ValuesClause {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push_str("VALUES ");
        let mut row_sep = "";
        buf.push('(');
        for row in &self.rows {
            buf.push_str(row_sep);
            let mut sep = "";
            for value in row {
                buf.push_str(sep);
                value.deparse(buf);
                sep = ", ";
            }
            row_sep = "), (";
        }
        buf.push(')');
        buf
    }
}

/// Core SELECT without ORDER BY/LIMIT (those go on the parent query)
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct SelectNode {
    pub distinct: bool,
    pub columns: SelectColumns,
    pub from: SmallVec<[TableSource; 1]>,
    pub where_clause: Option<WhereExpr>,
    pub group_by: Vec<ColumnNode>,
    pub having: Option<WhereExpr>,
}

impl Default for SelectNode {
    fn default() -> Self {
        Self {
            distinct: false,
            columns: SelectColumns::None,
            from: SmallVec::new(),
            where_clause: None,
            group_by: Vec::new(),
            having: None,
        }
    }
}

impl AstNode for SelectNode {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        self.columns.try_for_each_node(f)?;
        for t in &self.from {
            t.try_for_each_node(f)?;
        }
        if let Some(w) = &self.where_clause {
            w.try_for_each_node(f)?;
        }
        for c in &self.group_by {
            c.try_for_each_node(f)?;
        }
        if let Some(h) = &self.having {
            h.try_for_each_node(f)?;
        }
        ControlFlow::Continue(())
    }
}

impl SelectNode {
    /// Return table nodes directly in this SELECT's FROM clause.
    /// Traverses JOINs but does NOT descend into subqueries.
    pub fn direct_table_nodes(&self) -> Vec<&TableNode> {
        let mut tables = Vec::with_capacity(self.from.len());
        for source in &self.from {
            source.direct_table_nodes_collect(&mut tables);
        }
        tables
    }

    /// Check if this SELECT references only a single table
    pub fn is_single_table(&self) -> bool {
        matches!(self.from.as_slice(), [TableSource::Table(_)])
    }

    /// Check if this SELECT contains sublinks/subqueries
    pub fn has_subqueries(&self) -> bool {
        // Check columns for subqueries
        if let SelectColumns::Columns(columns) = &self.columns
            && columns.iter().any(SelectColumn::has_subqueries)
        {
            return true;
        }

        // Check FROM clause for subqueries
        if self.from.iter().any(TableSource::has_subqueries) {
            return true;
        }

        // Check WHERE clause for subqueries
        if let Some(where_clause) = &self.where_clause
            && where_clause.has_subqueries()
        {
            return true;
        }

        // Check HAVING clause for subqueries
        if let Some(having) = &self.having
            && having.has_subqueries()
        {
            return true;
        }

        false
    }
}

impl Deparse for SelectNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push_str("SELECT");
        if self.distinct {
            buf.push_str(" DISTINCT");
        }
        self.columns.deparse(buf);

        if !self.from.is_empty() {
            buf.push_str(" FROM");
            let mut sep = "";
            for table in &self.from {
                buf.push_str(sep);
                table.deparse(buf);
                sep = ",";
            }
        }

        if let Some(expr) = &self.where_clause {
            buf.push_str(" WHERE ");
            expr.deparse(buf);
        }

        if !self.group_by.is_empty() {
            buf.push_str(" GROUP BY ");
            let mut sep = "";
            for col in &self.group_by {
                buf.push_str(sep);
                col.deparse(buf);
                sep = ", ";
            }
        }

        if let Some(expr) = &self.having {
            buf.push_str(" HAVING ");
            expr.deparse(buf);
        }

        buf
    }
}

/// Set operation node: `left UNION/INTERSECT/EXCEPT [ALL] right`
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct SetOpNode {
    pub op: SetOpType,
    pub all: bool,
    pub left: Box<QueryExpr>,
    pub right: Box<QueryExpr>,
}

impl AstNode for SetOpNode {
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

impl SetOpNode {
    pub fn has_subqueries(&self) -> bool {
        self.left.has_subqueries() || self.right.has_subqueries()
    }
}

impl Deparse for SetOpNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        self.left.deparse(buf);
        buf.push(' ');
        self.op.deparse(buf);
        if self.all {
            buf.push_str(" ALL");
        }
        buf.push(' ');
        self.right.deparse(buf);
        buf
    }
}

/// The body of a query - SELECT, VALUES, or set operation
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum QueryBody {
    Select(Box<SelectNode>),
    Values(ValuesClause),
    SetOp(SetOpNode),
}

impl AstNode for QueryBody {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        match self {
            QueryBody::Select(select) => select.try_for_each_node(f)?,
            QueryBody::Values(values) => values.try_for_each_node(f)?,
            QueryBody::SetOp(set_op) => set_op.try_for_each_node(f)?,
        }
        ControlFlow::Continue(())
    }
}

impl QueryBody {
    pub fn has_subqueries(&self) -> bool {
        match self {
            QueryBody::Select(select) => select.has_subqueries(),
            QueryBody::Values(values) => values.has_subqueries(),
            QueryBody::SetOp(set_op) => set_op.has_subqueries(),
        }
    }
}

impl Deparse for QueryBody {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            QueryBody::Select(select) => select.deparse(buf),
            QueryBody::Values(values) => values.deparse(buf),
            QueryBody::SetOp(set_op) => set_op.deparse(buf),
        }
    }
}

/// A complete query expression with optional ordering/limiting
///
/// This represents a complete query that can be:
/// - A simple SELECT
/// - A VALUES clause
/// - A set operation (UNION/INTERSECT/EXCEPT)
///
/// ORDER BY and LIMIT apply to the entire query expression.
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct QueryExpr {
    pub ctes: Vec<CteDefinition>,
    pub body: QueryBody,
    pub order_by: SmallVec<[OrderByClause; 1]>,
    pub limit: Option<LimitClause>,
}

impl Default for QueryExpr {
    fn default() -> Self {
        Self {
            ctes: Vec::new(),
            body: QueryBody::Values(ValuesClause::default()),
            order_by: SmallVec::new(),
            limit: None,
        }
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

/// A CTE definition from a WITH clause.
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct CteDefinition {
    pub name: EcoString,
    pub query: QueryExpr,
    pub column_aliases: Vec<EcoString>,
    pub materialization: CteMaterialization,
}

impl AstNode for QueryExpr {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        for c in &self.ctes {
            c.query.try_for_each_node(f)?;
        }
        self.body.try_for_each_node(f)?;
        for o in &self.order_by {
            o.try_for_each_node(f)?;
        }
        ControlFlow::Continue(())
    }
}

impl QueryExpr {
    /// Check if query only references a single table.
    ///
    /// Short-circuits on the second table via the visitor's early-exit rather
    /// than collecting every `TableNode` into a `Vec` (which `nodes()` would).
    pub fn is_single_table(&self) -> bool {
        let mut count = 0u8;
        self.try_for_each_node::<TableNode, ()>(&mut |_| {
            count += 1;
            if count >= 2 {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })
        .is_continue()
    }

    /// Check if query has a WHERE clause (only applies to SELECT bodies)
    pub fn has_where_clause(&self) -> bool {
        match &self.body {
            QueryBody::Select(select) => select.where_clause.is_some(),
            QueryBody::Values(_) | QueryBody::SetOp(_) => false,
        }
    }

    /// Get the WHERE clause if it exists (only for SELECT bodies)
    pub fn where_clause(&self) -> Option<&WhereExpr> {
        match &self.body {
            QueryBody::Select(select) => select.where_clause.as_ref(),
            QueryBody::Values(_) | QueryBody::SetOp(_) => None,
        }
    }

    /// Check if this query contains subqueries
    pub fn has_subqueries(&self) -> bool {
        !self.ctes.is_empty()
            || self.body.has_subqueries()
            || self.order_by.iter().any(|o| o.expr.has_subqueries())
    }

    /// Get the SELECT body if this is a simple SELECT query
    pub fn as_select(&self) -> Option<&SelectNode> {
        match &self.body {
            QueryBody::Select(select) => Some(select),
            QueryBody::Values(_) | QueryBody::SetOp(_) => None,
        }
    }

    /// Extract all SELECT branches from this query expression.
    ///
    /// For a simple SELECT query, returns a single-element vector.
    /// For set operations (UNION/INTERSECT/EXCEPT), recursively extracts
    /// all SELECT branches from both sides.
    /// VALUES clauses are skipped (they don't reference tables).
    /// CTE definitions are collected via CteRef references, not eagerly.
    pub fn select_nodes(&self) -> Vec<&SelectNode> {
        self.select_nodes_with_source()
            .into_iter()
            .map(|(node, _)| node)
            .collect()
    }

    /// Extract all SELECT branches with their source context (FromClause vs Subquery).
    ///
    /// Tracks whether each branch came from the top-level body (FromClause) or
    /// from a subquery context (Subquery with kind). CTE definitions are not
    /// collected directly -- their branches are collected when referenced via
    /// CteRef, inheriting the reference site's source context.
    pub fn select_nodes_with_source(&self) -> Vec<(&SelectNode, UpdateQuerySource)> {
        let mut branches = Vec::new();
        self.select_nodes_collect(&mut branches, UpdateQuerySource::FromClause, false);
        branches
    }

    /// Collects branches with source tracking.
    /// `outer_source` is the source assigned to this query's body branches.
    /// `negated` tracks NOT-wrapping to flip Inclusion/Exclusion.
    pub(crate) fn select_nodes_collect<'a>(
        &'a self,
        branches: &mut Vec<(&'a SelectNode, UpdateQuerySource)>,
        outer_source: UpdateQuerySource,
        negated: bool,
    ) {
        match &self.body {
            QueryBody::Select(select) => {
                branches.push((select, outer_source));
                for source in &select.from {
                    source.subquery_nodes_collect(branches, negated);
                }
                if let Some(where_clause) = &select.where_clause {
                    where_clause.subquery_nodes_collect(branches, negated);
                }
                if let Some(having) = &select.having {
                    having.subquery_nodes_collect(branches, negated);
                }
                select.columns.subquery_nodes_collect(branches);
            }
            QueryBody::SetOp(set_op) => {
                set_op
                    .left
                    .select_nodes_collect(branches, outer_source, negated);
                set_op
                    .right
                    .select_nodes_collect(branches, outer_source, negated);
            }
            QueryBody::Values(_) => {}
        }
    }
}

impl Deparse for QueryExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        if !self.ctes.is_empty() {
            buf.push_str("WITH ");
            for (i, cte) in self.ctes.iter().enumerate() {
                if i > 0 {
                    buf.push_str(", ");
                }
                cte.name.deparse(buf);
                if !cte.column_aliases.is_empty() {
                    buf.push('(');
                    for (j, col) in cte.column_aliases.iter().enumerate() {
                        if j > 0 {
                            buf.push_str(", ");
                        }
                        col.deparse(buf);
                    }
                    buf.push(')');
                }
                buf.push_str(" AS ");
                match cte.materialization {
                    CteMaterialization::Default => {}
                    CteMaterialization::Materialized => buf.push_str("MATERIALIZED "),
                    CteMaterialization::NotMaterialized => buf.push_str("NOT MATERIALIZED "),
                }
                buf.push('(');
                cte.query.deparse(buf);
                buf.push(')');
            }
            buf.push(' ');
        }

        self.body.deparse(buf);

        if !self.order_by.is_empty() {
            buf.push_str(" ORDER BY");
            let mut sep = "";
            for order in &self.order_by {
                buf.push_str(sep);
                buf.push(' ');
                order.deparse(buf);
                sep = ",";
            }
        }

        if let Some(limit) = &self.limit {
            if let Some(count) = &limit.count {
                buf.push_str(" LIMIT ");
                count.deparse(buf);
            }
            if let Some(offset) = &limit.offset {
                buf.push_str(" OFFSET ");
                offset.deparse(buf);
            }
        }

        buf
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum SelectColumns {
    None,
    Columns(Vec<SelectColumn>), // SELECT col1, col2, ... (includes Star entries)
}

impl AstNode for SelectColumns {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        match self {
            SelectColumns::None => {}
            SelectColumns::Columns(columns) => {
                for col in columns {
                    col.try_for_each_node(f)?;
                }
            }
        }
        ControlFlow::Continue(())
    }
}

impl SelectColumns {
    /// Collect subquery branches from SELECT list with source tracking.
    /// All subqueries in a SELECT list are Scalar (must return single value).
    pub(crate) fn subquery_nodes_collect<'a>(
        &'a self,
        branches: &mut Vec<(&'a SelectNode, UpdateQuerySource)>,
    ) {
        if let SelectColumns::Columns(columns) = self {
            for col in columns {
                if let SelectColumn::Expr { expr, .. } = col {
                    expr.subquery_nodes_collect(branches);
                }
            }
        }
    }
}

impl Deparse for SelectColumns {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            SelectColumns::None => buf.push(' '),
            SelectColumns::Columns(cols) => {
                let mut sep = "";
                for col in cols {
                    buf.push_str(sep);
                    col.deparse(buf);
                    sep = ",";
                }
            }
        };

        buf
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum SelectColumn {
    /// A scalar-valued column, optionally aliased.
    Expr {
        expr: ScalarExpr,
        alias: Option<EcoString>,
    },
    /// `*` or `<qualifier>.*`. Cannot be aliased.
    Star(Option<EcoString>),
}

impl SelectColumn {
    /// Inner scalar expression, or `None` for a `Star` column.
    pub fn expr(&self) -> Option<&ScalarExpr> {
        match self {
            SelectColumn::Expr { expr, .. } => Some(expr),
            SelectColumn::Star(_) => None,
        }
    }

    /// Alias of an `Expr` column. `Star` columns cannot be aliased.
    pub fn alias(&self) -> Option<&EcoString> {
        match self {
            SelectColumn::Expr { alias, .. } => alias.as_ref(),
            SelectColumn::Star(_) => None,
        }
    }

    pub fn has_subqueries(&self) -> bool {
        match self {
            SelectColumn::Expr { expr, .. } => expr.has_subqueries(),
            SelectColumn::Star(_) => false,
        }
    }
}

impl AstNode for SelectColumn {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        match self {
            SelectColumn::Expr { expr, .. } => expr.try_for_each_node(f)?,
            SelectColumn::Star(_) => {}
        }
        ControlFlow::Continue(())
    }
}

impl Deparse for SelectColumn {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push(' ');
        match self {
            SelectColumn::Expr { expr, alias } => {
                expr.deparse(buf);
                if let Some(alias) = alias {
                    buf.push_str(" AS ");
                    alias.deparse(buf);
                }
            }
            SelectColumn::Star(qualifier) => {
                if let Some(table) = qualifier {
                    buf.push_str(table);
                    buf.push('.');
                }
                buf.push('*');
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

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct FunctionCall {
    pub name: EcoString,
    pub args: Vec<ScalarExpr>,
    pub agg_star: bool,                     // COUNT(*)
    pub agg_distinct: bool,                 // COUNT(DISTINCT col)
    pub agg_order: Vec<OrderByClause>, // ORDER BY inside aggregate: string_agg(x, ',' ORDER BY x). Vec (not SmallVec): OrderByClause is recursively reachable here, so inline storage would be infinitely sized.
    pub agg_filter: Option<Box<WhereExpr>>, // FILTER (WHERE ...) per-aggregate predicate
    pub over: Option<WindowSpec>,      // Window function OVER clause
}

impl AstNode for FunctionCall {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        for arg in &self.args {
            arg.try_for_each_node(f)?;
        }
        for o in &self.agg_order {
            o.try_for_each_node(f)?;
        }
        if let Some(filter) = &self.agg_filter {
            filter.try_for_each_node(f)?;
        }
        if let Some(over) = &self.over {
            over.try_for_each_node(f)?;
        }
        ControlFlow::Continue(())
    }
}

impl FunctionCall {
    /// Check if this function call contains sublinks/subqueries
    pub fn has_subqueries(&self) -> bool {
        self.args.iter().any(|arg| arg.has_subqueries())
            || self.agg_order.iter().any(|o| o.expr.has_subqueries())
            || self.agg_filter.as_ref().is_some_and(|f| f.has_subqueries())
            || self.over.as_ref().is_some_and(|w| w.has_subqueries())
    }
}

impl Deparse for FunctionCall {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push_str(&self.name.to_uppercase());
        buf.push('(');
        if self.agg_distinct {
            buf.push_str("DISTINCT ");
        }
        if self.agg_star {
            buf.push('*');
        } else {
            let mut sep = "";
            for arg in &self.args {
                buf.push_str(sep);
                arg.deparse(buf);
                sep = ", ";
            }
        }
        if !self.agg_order.is_empty() {
            buf.push_str(" ORDER BY ");
            let mut sep = "";
            for clause in &self.agg_order {
                buf.push_str(sep);
                clause.deparse(buf);
                sep = ", ";
            }
        }
        buf.push(')');
        if let Some(filter) = &self.agg_filter {
            buf.push_str(" FILTER (WHERE ");
            filter.deparse(buf);
            buf.push(')');
        }
        if let Some(over) = &self.over {
            buf.push_str(" OVER ");
            over.deparse(buf);
        }
        buf
    }
}

/// Window specification for OVER clause: OVER (PARTITION BY ... ORDER BY ...)
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct WindowSpec {
    /// PARTITION BY columns
    pub partition_by: Vec<ScalarExpr>,
    /// ORDER BY clauses. Vec (not SmallVec): recursively reachable, see `agg_order`.
    pub order_by: Vec<OrderByClause>,
}

impl AstNode for WindowSpec {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        for p in &self.partition_by {
            p.try_for_each_node(f)?;
        }
        for o in &self.order_by {
            o.try_for_each_node(f)?;
        }
        ControlFlow::Continue(())
    }
}

impl WindowSpec {
    /// Check if this window spec contains sublinks/subqueries
    pub fn has_subqueries(&self) -> bool {
        self.partition_by.iter().any(|p| p.has_subqueries())
            || self.order_by.iter().any(|o| o.expr.has_subqueries())
    }
}

impl Deparse for WindowSpec {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push('(');
        if !self.partition_by.is_empty() {
            buf.push_str("PARTITION BY ");
            let mut sep = "";
            for col in &self.partition_by {
                buf.push_str(sep);
                col.deparse(buf);
                sep = ", ";
            }
        }
        if !self.order_by.is_empty() {
            if !self.partition_by.is_empty() {
                buf.push(' ');
            }
            buf.push_str("ORDER BY ");
            let mut sep = "";
            for clause in &self.order_by {
                buf.push_str(sep);
                clause.deparse(buf);
                sep = ", ";
            }
        }
        buf.push(')');
        buf
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableAlias {
    pub name: EcoString,
    pub columns: Vec<EcoString>,
}

impl Deparse for TableAlias {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        self.name.deparse(buf);
        if !self.columns.is_empty() {
            buf.push('(');
            let mut sep = "";
            for column in self.columns.iter().map(|c| c.as_str()) {
                buf.push_str(sep);
                column.deparse(buf);
                sep = ", ";
            }
            buf.push(')');
        }

        buf
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum TableSource {
    Table(TableNode),
    Subquery(TableSubqueryNode),
    Join(JoinNode),
    CteRef(CteRefNode),
}

/// A reference to a CTE in a FROM clause or subquery position.
///
/// Contains a clone of the CTE's query body so that traversal methods
/// (nodes, select_nodes, etc.) are self-contained without needing
/// external context.
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct CteRefNode {
    pub cte_name: EcoString,
    pub query: Box<QueryExpr>,
    pub column_aliases: Vec<EcoString>,
    pub materialization: CteMaterialization,
    pub alias: Option<TableAlias>,
}

impl AstNode for TableSource {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        match self {
            TableSource::Table(table) => table.try_for_each_node(f)?,
            TableSource::Subquery(sub) => sub.try_for_each_node(f)?,
            TableSource::Join(join) => join.try_for_each_node(f)?,
            TableSource::CteRef(cte_ref) => cte_ref.try_for_each_node(f)?,
        }
        ControlFlow::Continue(())
    }
}

impl TableSource {
    /// Collect direct table nodes, traversing JOINs but not subqueries/CTEs.
    fn direct_table_nodes_collect<'a>(&'a self, tables: &mut Vec<&'a TableNode>) {
        match self {
            TableSource::Table(table) => tables.push(table),
            TableSource::Subquery(_) | TableSource::CteRef(_) => {} // handled as separate branch
            TableSource::Join(join) => {
                join.left.direct_table_nodes_collect(tables);
                join.right.direct_table_nodes_collect(tables);
            }
        }
    }

    /// Check if this table source contains sublinks/subqueries
    pub fn has_subqueries(&self) -> bool {
        match self {
            TableSource::Subquery(_) | TableSource::CteRef(_) => true,
            TableSource::Table(_) => false,
            TableSource::Join(join) => {
                join.left.has_subqueries()
                    || join.right.has_subqueries()
                    || matches!(&join.qual, JoinQual::On(c) if c.has_subqueries())
            }
        }
    }

    /// Collect subquery branches from table sources with source tracking.
    /// FROM subqueries and CteRef inherit the negation context as Inclusion/Exclusion.
    pub(crate) fn subquery_nodes_collect<'a>(
        &'a self,
        branches: &mut Vec<(&'a SelectNode, UpdateQuerySource)>,
        negated: bool,
    ) {
        match self {
            TableSource::Table(_) => {}
            TableSource::Subquery(sub) => {
                let kind = if negated {
                    SubqueryKind::Exclusion
                } else {
                    SubqueryKind::Inclusion
                };
                sub.query.select_nodes_collect(
                    branches,
                    UpdateQuerySource::Subquery(kind),
                    negated,
                );
            }
            TableSource::CteRef(cte_ref) => {
                let kind = if negated {
                    SubqueryKind::Exclusion
                } else {
                    SubqueryKind::Inclusion
                };
                cte_ref.query.select_nodes_collect(
                    branches,
                    UpdateQuerySource::Subquery(kind),
                    negated,
                );
            }
            TableSource::Join(join) => {
                join.left.subquery_nodes_collect(branches, negated);
                join.right.subquery_nodes_collect(branches, negated);
                if let JoinQual::On(condition) = &join.qual {
                    condition.subquery_nodes_collect(branches, negated);
                }
            }
        }
    }
}

impl Deparse for TableSource {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            TableSource::Table(table) => table.deparse(buf),
            TableSource::Subquery(subquery) => subquery.deparse(buf),
            TableSource::Join(join) => join.deparse(buf),
            TableSource::CteRef(cte_ref) => cte_ref.deparse(buf),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableNode {
    pub schema: Option<EcoString>,
    pub name: EcoString,
    pub alias: Option<TableAlias>,
}

impl AstNode for TableNode {
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

impl Deparse for TableNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push(' ');
        if let Some(schema) = &self.schema {
            schema.deparse(buf);
            buf.push('.');
        }

        self.name.deparse(buf);

        if let Some(alias) = &self.alias {
            buf.push(' ');
            alias.deparse(buf);
        }

        buf
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct TableSubqueryNode {
    pub lateral: bool,
    pub query: Box<QueryExpr>,
    pub alias: Option<TableAlias>,
}

impl AstNode for TableSubqueryNode {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        self.query.try_for_each_node(f)?;
        ControlFlow::Continue(())
    }
}

impl Deparse for TableSubqueryNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push(' ');
        if self.lateral {
            buf.push_str("LATERAL ");
        }

        buf.push('(');
        self.query.deparse(buf);
        buf.push(')');

        if let Some(alias) = &self.alias {
            buf.push(' ');
            alias.deparse(buf);
        }

        buf
    }
}

impl AstNode for CteRefNode {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        self.query.try_for_each_node(f)?;
        ControlFlow::Continue(())
    }
}

impl Deparse for CteRefNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push(' ');
        self.cte_name.deparse(buf);

        if let Some(alias) = &self.alias {
            buf.push(' ');
            alias.deparse(buf);
        }

        buf
    }
}

/// A join's qualifier. `ON` / `USING` / `NATURAL` / cross are mutually
/// exclusive in SQL, so they are one enum rather than independent
/// fields that could encode impossible states. The resolver expands
/// `Using`/`Natural` into an equivalent equi-join condition; `Cross`
/// (and a no-common-column `Natural`) is a cartesian product.
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum JoinQual {
    /// `JOIN … ON <expr>`
    On(WhereExpr),
    /// `JOIN … USING (c, …)`
    Using(Vec<EcoString>),
    /// `NATURAL JOIN …`
    Natural,
    /// `CROSS JOIN …` / unqualified inner join — cartesian product.
    Cross,
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct JoinNode {
    pub join_type: JoinType,
    pub left: Box<TableSource>,
    pub right: Box<TableSource>,
    pub qual: JoinQual,
}

impl AstNode for JoinNode {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        // Self-visit like every other node type, so `nodes::<JoinNode>()` finds
        // a join called on directly (not only joins nested under a TableSource).
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        self.left.try_for_each_node(f)?;
        self.right.try_for_each_node(f)?;
        if let JoinQual::On(c) = &self.qual {
            c.try_for_each_node(f)?;
        }
        ControlFlow::Continue(())
    }
}

impl Deparse for JoinNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        self.left.deparse(buf);
        match &self.qual {
            // CROSS JOIN is always inner; the keyword carries the semantics.
            JoinQual::Cross => buf.push_str(" CROSS JOIN"),
            JoinQual::Natural => {
                buf.push_str(" NATURAL");
                buf.push_str(self.join_type.join_keyword());
            }
            JoinQual::On(_) | JoinQual::Using(_) => {
                buf.push_str(self.join_type.join_keyword());
            }
        }
        self.right.deparse(buf);
        match &self.qual {
            JoinQual::On(condition) => {
                buf.push_str(" ON ");
                condition.deparse(buf);
            }
            JoinQual::Using(cols) => {
                buf.push_str(" USING (");
                for (i, c) in cols.iter().enumerate() {
                    if i > 0 {
                        buf.push_str(", ");
                    }
                    buf.push_str(c);
                }
                buf.push(')');
            }
            JoinQual::Natural | JoinQual::Cross => {}
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

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct OrderByClause {
    pub expr: ScalarExpr,
    pub direction: OrderDirection,
    pub null_order: NullOrder,
}

impl AstNode for OrderByClause {
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

impl Deparse for OrderByClause {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        self.expr.deparse(buf);
        buf.push(' ');
        self.direction.deparse(buf);
        self.null_order.deparse(buf);
        buf
    }
}

/// Explicit NULLS ordering on an `ORDER BY` item. `Default` emits nothing so
/// PostgreSQL applies its built-in ASC=last / DESC=first defaults (PGC-144).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NullOrder {
    Default,
    NullsFirst,
    NullsLast,
}

impl AstNode for NullOrder {
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

impl AstNode for OrderDirection {
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
