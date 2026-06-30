//! Compiled WHERE predicate for the LocalEval CDC membership hot path (PGC-339).
//!
//! `where_expr_evaluate` walks the resolved WHERE AST per CDC row per candidate
//! query, re-destructuring each fixed comparison via `canonicalize_comparison`
//! and dereferencing the boxed AST + column metadata every time. For a registered
//! query that work is constant — only the row value changes. `CompiledPredicate`
//! hoists it to registration: the column position, in-table flag, cast target,
//! operator, and literal are resolved once into a flat node, leaving only the
//! row-value lookup + compare per row. The comparison semantics are unchanged —
//! leaves reuse `where_value_compare_string` / `cast_target_coerce_text` /
//! `literal_compare`, so a compiled predicate evaluates identically to
//! `where_expr_evaluate` for the expression it was compiled from.

use crate::pg::protocol::ByteString;
use crate::query::ast::{BinaryOp, LiteralValue, UnaryOp};
use crate::query::cast::{CastTarget, canonicalize_comparison, cast_target_coerce_text};
use crate::query::evaluate::{literal_compare, where_value_compare_string};
use crate::query::resolved::{ResolvedScalarExpr, ResolvedWhereExpr};

/// Pre-canonicalized form of a LocalEval WHERE clause. Built once at registration
/// from a `ResolvedWhereExpr`; evaluated per CDC row. A total mapping of every
/// shape `where_expr_evaluate` accepts — unsupported shapes compile to
/// `ConstFalse`, mirroring its `_ => false` / Like arms.
#[derive(Debug, Clone)]
pub enum CompiledPredicate {
    And(Box<CompiledPredicate>, Box<CompiledPredicate>),
    Or(Box<CompiledPredicate>, Box<CompiledPredicate>),
    Not(Box<CompiledPredicate>),
    /// IS [NOT] NULL / IS [NOT] TRUE / IS [NOT] FALSE. `col` is the row position
    /// of the inner column when it is an in-table `Scalar(Column)`; `None`
    /// otherwise — the inner value is then unconditionally absent, matching
    /// `unary_expr_evaluate`'s `Some(Scalar(Column))`-only value lookup.
    NullCheck {
        op: UnaryOp,
        col: Option<usize>,
    },
    /// `column op literal`, canonical (column-on-LHS) form. `in_table` is the
    /// `col.table == table_name` test resolved at build time.
    Compare {
        col: usize,
        in_table: bool,
        target: Option<CastTarget>,
        op: BinaryOp,
        value: LiteralValue,
    },
    /// Any shape `where_expr_evaluate` decides `false` for without inspecting the
    /// row: Like-family, Multi, Subquery, a bare Scalar leaf, or a comparison
    /// that does not canonicalize to `column op literal`.
    ConstFalse,
}

impl CompiledPredicate {
    /// Compile a resolved WHERE expression for rows of `table_name`. Total —
    /// every input maps to a node.
    pub fn compile(expr: &ResolvedWhereExpr, table_name: &str) -> Self {
        match expr {
            ResolvedWhereExpr::Binary(b) => match b.op {
                BinaryOp::And => Self::And(
                    Box::new(Self::compile(&b.lexpr, table_name)),
                    Box::new(Self::compile(&b.rexpr, table_name)),
                ),
                BinaryOp::Or => Self::Or(
                    Box::new(Self::compile(&b.lexpr, table_name)),
                    Box::new(Self::compile(&b.rexpr, table_name)),
                ),
                BinaryOp::Equal
                | BinaryOp::NotEqual
                | BinaryOp::LessThan
                | BinaryOp::LessThanOrEqual
                | BinaryOp::GreaterThan
                | BinaryOp::GreaterThanOrEqual => match canonicalize_comparison(b) {
                    Some((col, target, op, value)) => Self::Compare {
                        col: col.column_metadata.index(),
                        in_table: col.table.as_str() == table_name,
                        target: target.cloned(),
                        op,
                        value: value.clone(),
                    },
                    None => Self::ConstFalse,
                },
                BinaryOp::Like | BinaryOp::ILike | BinaryOp::NotLike | BinaryOp::NotILike => {
                    Self::ConstFalse
                }
            },
            ResolvedWhereExpr::Unary(u) => match u.op {
                UnaryOp::Not => Self::Not(Box::new(Self::compile(&u.expr, table_name))),
                op @ (UnaryOp::IsNull
                | UnaryOp::IsNotNull
                | UnaryOp::IsTrue
                | UnaryOp::IsNotTrue
                | UnaryOp::IsFalse
                | UnaryOp::IsNotFalse) => Self::NullCheck {
                    op,
                    col: column_position_in_table(&u.expr, table_name),
                },
            },
            ResolvedWhereExpr::Scalar(_)
            | ResolvedWhereExpr::Multi(_)
            | ResolvedWhereExpr::Subquery { .. } => Self::ConstFalse,
        }
    }

    /// Evaluate against a single row. Identical result to
    /// `where_expr_evaluate(expr, row_data, table_name)` for the `expr` this was
    /// compiled from.
    pub fn eval(&self, row_data: &[Option<ByteString>]) -> bool {
        match self {
            Self::And(l, r) => l.eval(row_data) && r.eval(row_data),
            Self::Or(l, r) => l.eval(row_data) || r.eval(row_data),
            Self::Not(inner) => !inner.eval(row_data),
            Self::NullCheck { op, col } => {
                let value = col.and_then(|pos| row_data.get(pos).and_then(|v| v.as_deref()));
                match op {
                    UnaryOp::IsNull => value.is_none(),
                    UnaryOp::IsNotNull => value.is_some(),
                    UnaryOp::IsTrue => matches!(value, Some("t" | "true")),
                    UnaryOp::IsNotTrue => !matches!(value, Some("t" | "true")),
                    UnaryOp::IsFalse => matches!(value, Some("f" | "false")),
                    UnaryOp::IsNotFalse => !matches!(value, Some("f" | "false")),
                    // `Not` compiles to `CompiledPredicate::Not`, never NullCheck.
                    UnaryOp::Not => false,
                }
            }
            Self::Compare {
                col,
                in_table,
                target,
                op,
                value,
            } => {
                if !in_table {
                    return false; // column belongs to another table (NotInTable)
                }
                match row_data.get(*col) {
                    None => false, // position past row end (NotInTable)
                    Some(None) => {
                        // row value is NULL: only `col = NULL` with no cast matches,
                        // mirroring `expr_comparison_evaluate`'s Null arm.
                        target.is_none()
                            && matches!(op, BinaryOp::Equal)
                            && matches!(value, LiteralValue::Null)
                    }
                    Some(Some(bytes)) => {
                        let row_str = bytes.as_str();
                        match target {
                            None => where_value_compare_string(value, row_str, *op),
                            Some(target) => match cast_target_coerce_text(target, row_str) {
                                Some(coerced) => literal_compare(&coerced, *op, value),
                                None => false,
                            },
                        }
                    }
                }
            }
            Self::ConstFalse => false,
        }
    }
}

/// Row position of `expr` when it is an in-table bare column reference, matching
/// the value `unary_expr_evaluate` reads for IS NULL/TRUE/FALSE checks: `None`
/// for any non-column inner, or a column belonging to another table.
fn column_position_in_table(expr: &ResolvedWhereExpr, table_name: &str) -> Option<usize> {
    if let ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(col)) = expr
        && col.table.as_str() == table_name
    {
        Some(col.column_metadata.index())
    } else {
        None
    }
}
