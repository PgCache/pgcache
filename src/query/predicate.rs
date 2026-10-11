//! Compiled WHERE predicate for the LocalEval CDC membership hot path (PGC-339).
//!
//! `where_expr_evaluate` walks the resolved WHERE AST per CDC row per candidate
//! query, re-destructuring each fixed comparison via `canonicalize_comparison`
//! and dereferencing the boxed AST + column metadata every time. For a registered
//! query that work is constant — only the row value changes. `CompiledPredicate`
//! hoists it to registration: the column position, in-table flag, cast target,
//! operator, and literal are resolved once into a flat node, leaving only the
//! row-value lookup + compare per row. The comparison semantics are unchanged —
//! leaves reuse `typed_compare` / `cast_target_coerce_text` /
//! `literal_compare`, so a compiled predicate evaluates identically to
//! `where_expr_evaluate` for the expression it was compiled from.

use crate::pg::protocol::ByteString;
use crate::query::ast::{BinaryOp, LiteralValue, ScalarExpr, UnaryOp, WhereExpr};
#[cfg(test)]
use crate::query::cast::canonicalize_comparison;
use crate::query::cast::{CastTarget, cast_target_coerce_text, comparison_canonicalize};
use crate::query::evaluate::literal_compare;
use crate::query::resolve::ResolvedQuery;
#[cfg(test)]
use crate::query::resolved::ResolvedScalarExpr;
#[cfg(test)]
use crate::query::resolved::ResolvedWhereExpr;
use crate::query::typed_compare::TypedLiteral;

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
        /// `value` parsed once for the column's type, for the uncast
        /// comparison against each row's text.
        literal: TypedLiteral,
    },
    /// Any shape `where_expr_evaluate` decides `false` for without inspecting the
    /// row: Like-family, Multi, Subquery, a bare Scalar leaf, or a comparison
    /// that does not canonicalize to `column op literal`.
    ConstFalse,
}

impl CompiledPredicate {
    /// Compile a WHERE expression of `bound` for rows of `table_name`. Total —
    /// every input maps to a node.
    pub fn compile(bound: &ResolvedQuery, expr: &WhereExpr, table_name: &str) -> Self {
        match expr {
            WhereExpr::Binary(b) => match b.op {
                BinaryOp::And => Self::And(
                    Box::new(Self::compile(bound, &b.lexpr, table_name)),
                    Box::new(Self::compile(bound, &b.rexpr, table_name)),
                ),
                BinaryOp::Or => Self::Or(
                    Box::new(Self::compile(bound, &b.lexpr, table_name)),
                    Box::new(Self::compile(bound, &b.rexpr, table_name)),
                ),
                BinaryOp::Equal
                | BinaryOp::NotEqual
                | BinaryOp::LessThan
                | BinaryOp::LessThanOrEqual
                | BinaryOp::GreaterThan
                | BinaryOp::GreaterThanOrEqual => {
                    let compare = comparison_canonicalize(bound, b).and_then(
                        |(column, target, op, value)| {
                            let binding = bound.column_binding(column)?;
                            let table = bound.column_table(column)?;
                            let data_type = bound.column_type(column)?;
                            Some(Self::Compare {
                                col: binding.position.index(),
                                in_table: table.name.as_str() == table_name,
                                target: target.cloned(),
                                op,
                                value: value.clone(),
                                literal: TypedLiteral::new(data_type, value),
                            })
                        },
                    );
                    compare.unwrap_or(Self::ConstFalse)
                }
                BinaryOp::Like | BinaryOp::ILike | BinaryOp::NotLike | BinaryOp::NotILike => {
                    Self::ConstFalse
                }
            },
            WhereExpr::Unary(u) => match u.op {
                UnaryOp::Not => Self::Not(Box::new(Self::compile(bound, &u.expr, table_name))),
                op @ (UnaryOp::IsNull
                | UnaryOp::IsNotNull
                | UnaryOp::IsTrue
                | UnaryOp::IsNotTrue
                | UnaryOp::IsFalse
                | UnaryOp::IsNotFalse) => Self::NullCheck {
                    op,
                    col: column_position(bound, &u.expr, table_name),
                },
            },
            WhereExpr::Scalar(_) | WhereExpr::Multi(_) | WhereExpr::Subquery { .. } => {
                Self::ConstFalse
            }
        }
    }

    /// [`CompiledPredicate::compile`] from the resolved tree; the oracle its
    /// tests compare against until the resolved tree goes.
    #[cfg(test)]
    pub fn compile_resolved(expr: &ResolvedWhereExpr, table_name: &str) -> Self {
        match expr {
            ResolvedWhereExpr::Binary(b) => match b.op {
                BinaryOp::And => Self::And(
                    Box::new(Self::compile_resolved(&b.lexpr, table_name)),
                    Box::new(Self::compile_resolved(&b.rexpr, table_name)),
                ),
                BinaryOp::Or => Self::Or(
                    Box::new(Self::compile_resolved(&b.lexpr, table_name)),
                    Box::new(Self::compile_resolved(&b.rexpr, table_name)),
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
                        literal: TypedLiteral::new(&col.column_metadata.data_type, value),
                    },
                    None => Self::ConstFalse,
                },
                BinaryOp::Like | BinaryOp::ILike | BinaryOp::NotLike | BinaryOp::NotILike => {
                    Self::ConstFalse
                }
            },
            ResolvedWhereExpr::Unary(u) => match u.op {
                UnaryOp::Not => Self::Not(Box::new(Self::compile_resolved(&u.expr, table_name))),
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
                literal,
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
                            None => literal.row_compare(row_str, *op).unwrap_or(false),
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
fn column_position(bound: &ResolvedQuery, expr: &WhereExpr, table_name: &str) -> Option<usize> {
    let WhereExpr::Scalar(ScalarExpr::Column(column)) = expr else {
        return None;
    };
    let table = bound.column_table(column)?;
    if table.name.as_str() != table_name {
        return None;
    }
    Some(bound.column_binding(column)?.position.index())
}

/// [`column_position`] on the resolved tree.
#[cfg(test)]
fn column_position_in_table(expr: &ResolvedWhereExpr, table_name: &str) -> Option<usize> {
    if let ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(col)) = expr
        && col.table.as_str() == table_name
    {
        Some(col.column_metadata.index())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use iddqd::BiHashMap;
    use postgres_types::Type;

    use super::*;
    use crate::catalog::{Catalog, ColumnMetadata, ColumnPosition, ColumnStore, TableMetadata};
    use crate::oid::{Oid, TypeOid};
    use crate::query::ast::query_expr_parse;
    use crate::query::resolve::query_expr_resolve_bound;

    fn catalog() -> Catalog {
        let column = |name: &str, index: usize, ty: Type| ColumnMetadata {
            name: name.into(),
            position: ColumnPosition::from_index(index).expect("position"),
            type_oid: TypeOid::from_type(&ty),
            type_name: ty.name().into(),
            cache_type_name: ty.name().into(),
            data_type: ty,
            is_primary_key: index == 0,
        };
        let mut tables = BiHashMap::new();
        for (name, oid) in [("t", 1u32), ("b", 2)] {
            tables.insert_overwrite(
                TableMetadata {
                    replica_identity_full: false,
                    relation_oid: Oid::from_raw(oid),
                    name: name.into(),
                    schema: "public".into(),
                    primary_key_columns: vec!["id".into()],
                    columns: ColumnStore::new([
                        column("id", 0, Type::INT4),
                        column("name", 1, Type::TEXT),
                        column("flag", 2, Type::BOOL),
                        column("day", 3, Type::DATE),
                    ]),
                    indexes: Vec::new(),
                }
                .into(),
            );
        }
        tables
    }

    /// The bound compilation is the resolved one, node for node, over every
    /// shape the evaluator accepts or rejects.
    #[test]
    fn test_bound_compile_matches_resolved() {
        let catalog = catalog();
        #[rustfmt::skip]
        let corpus = [
            "SELECT id FROM t WHERE id = 1",
            "SELECT id FROM t WHERE 5 < id AND name <> 'x' OR flag IS TRUE",
            "SELECT id FROM t WHERE NOT (id = 1 AND name IS NOT NULL)",
            "SELECT id FROM t WHERE name::text = 'a' AND id::int8 = 3 AND day::date = '2024-01-01'",
            "SELECT id FROM t WHERE id IN (1, 2) OR name LIKE 'a%'",
            "SELECT id FROM t WHERE id = (SELECT max(id) FROM b)",
            "SELECT t.id FROM t JOIN b ON t.id = b.id WHERE b.name = 'z' AND t.flag IS FALSE",
            "SELECT id FROM t JOIN b USING (id) WHERE id = 7 AND t.name = 'q'",
            "SELECT id FROM t WHERE id = NULL AND flag = true AND name = 1",
        ];
        for sql in corpus {
            let query = Arc::new(query_expr_parse(sql).expect("parse"));
            let (resolved, bindings) =
                query_expr_resolve_bound(&query, &catalog, &["public"]).expect("resolve");
            let bound = crate::query::resolve::ResolvedQuery::new(query, bindings);
            let (Some(select), Some(resolved_select)) = (bound.select(), resolved.as_select())
            else {
                panic!("{sql}: a SELECT");
            };
            let (Some(where_clause), Some(resolved_where)) =
                (&select.where_clause, &resolved_select.where_clause)
            else {
                panic!("{sql}: a WHERE");
            };
            for table in ["t", "b"] {
                assert_eq!(
                    format!(
                        "{:?}",
                        CompiledPredicate::compile(&bound, where_clause, table)
                    ),
                    format!(
                        "{:?}",
                        CompiledPredicate::compile_resolved(resolved_where, table)
                    ),
                    "{sql} for {table}"
                );
            }
        }
    }
}
