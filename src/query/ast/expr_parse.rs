use ecow::EcoString;
use error_set::error_set;
use ordered_float::NotNan;
use tracing::trace;

use pg_query::protobuf::a_const::Val;
use pg_query::protobuf::node::Node as NodeEnum;
use pg_query::protobuf::{
    AConst, AExpr, AExprKind, BoolExpr, BoolExprType, BoolTestType, BooleanTest, ColumnRef,
    NullTest, NullTestType, ParamRef, SelectStmt, SubLink,
};

use super::convert::{
    aexpr_arithmetic_convert, arithmetic_op_extract, node_convert_to_scalar_expr,
};
use super::{
    AstError, BinaryExpr, BinaryOp, ColumnNode, LiteralValue, MultiExpr, MultiOp, ScalarExpr,
    SubLinkType, UnaryExpr, UnaryOp, WhereExpr, select_stmt_to_query_expr,
};

error_set! {
    WhereParseError := {
        #[display("Unsupported WHERE clause pattern")]
        UnsupportedPattern,
        #[display("Unsupported A expression: {expr}")]
        UnsupportedAExpr { expr: String },
        #[display("Unsupported operator: {operator}")]
        UnsupportedOperator { operator: String },
        #[display("Invalid column reference")]
        InvalidColumnRef,
        #[display("Invalid constant value: {value}")]
        InvalidConstValue { value: String },
        #[display("Complex expression not supported: {expr}")]
        ComplexExpression { expr: String },
        #[display("Missing expression")]
        MissingExpression,
        #[display("{error}")]
        Other { error: String },
        /// Wraps a structural AST conversion failure surfaced while parsing
        /// a WHERE-side scalar (function call, scalar subquery, arithmetic
        /// operand, etc.). Boxed to keep the enum size bounded — `AstError`
        /// already contains `WhereParseError(WhereParseError)`.
        #[display("{0}")]
        Conversion(Box<AstError>),
    }
}

/// Parse a WHERE clause from a pg_query AST
/// Currently supports: equality comparisons (=) and boolean operations (AND, OR)
pub fn select_stmt_parse_where(
    select_stmt: &SelectStmt,
) -> Result<Option<WhereExpr>, WhereParseError> {
    let expr = if let Some(where_node) = &select_stmt.where_clause {
        Some(node_convert_to_expr(where_node)?)
    } else {
        None
    };

    Ok(expr)
}

/// Convert a pg_query Node to our WhereExpr - main entry point for recursion
pub fn node_convert_to_expr(node: &pg_query::Node) -> Result<WhereExpr, WhereParseError> {
    match node.node.as_ref() {
        // Predicate-shaped nodes
        Some(NodeEnum::AExpr(expr)) => a_expr_convert(expr),
        Some(NodeEnum::BoolExpr(expr)) => bool_expr_convert(expr),
        Some(NodeEnum::SubLink(sub_link)) => sublink_convert(sub_link),
        Some(NodeEnum::NullTest(null_test)) => null_test_convert(null_test),
        Some(NodeEnum::BooleanTest(bool_test)) => boolean_test_convert(bool_test),

        // Scalar-leaf nodes — wrap into WhereExpr::Scalar
        Some(NodeEnum::ColumnRef(col_ref)) => {
            let column = column_ref_extract(col_ref)?;
            Ok(WhereExpr::Scalar(ScalarExpr::Column(column)))
        }
        Some(NodeEnum::AConst(const_val)) => {
            let value = const_value_extract(const_val)?;
            Ok(WhereExpr::Scalar(ScalarExpr::Literal(value)))
        }
        Some(NodeEnum::ParamRef(param_ref)) => {
            let value = param_ref_extract(param_ref);
            Ok(WhereExpr::Scalar(ScalarExpr::Literal(value)))
        }
        Some(NodeEnum::FuncCall(_)) => {
            // Delegate to convert.rs's scalar converter so aggregate metadata
            // (DISTINCT, ORDER BY, FILTER, OVER) is preserved.
            let scalar = node_convert_to_scalar_expr(node)?;
            Ok(WhereExpr::Scalar(scalar))
        }
        Some(NodeEnum::TypeCast(_)) => {
            // Route through the scalar converter to reuse the existing
            // ScalarExpr::TypeCast resolve/Deparse/constraints path.
            let scalar = node_convert_to_scalar_expr(node)?;
            Ok(WhereExpr::Scalar(scalar))
        }
        unsupported => {
            trace!(?unsupported, "unsupported WHERE clause node type");
            Err(WhereParseError::UnsupportedPattern)
        }
    }
}

/// Convert pg_query SubLink to WhereExpr::Subquery with properly parsed inner query
fn sublink_convert(sub_link: &SubLink) -> Result<WhereExpr, WhereParseError> {
    // Parse the subquery SELECT statement
    let query = match sub_link.subselect.as_ref().and_then(|n| n.node.as_ref()) {
        Some(NodeEnum::SelectStmt(select_stmt)) => select_stmt_to_query_expr(select_stmt)?,
        _ => {
            return Err(WhereParseError::Other {
                error: "SubLink missing or invalid subselect".to_owned(),
            });
        }
    };

    // Parse the test expression (left-hand side for IN/ANY/ALL).
    // Always a scalar (the value being tested against the subquery output).
    let test_expr = sub_link
        .testexpr
        .as_deref()
        .map(node_convert_to_scalar_expr)
        .transpose()?
        .map(Box::new);

    // Convert the SubLink type
    let sublink_type = SubLinkType::try_from(sub_link.sub_link_type())?;

    // Only `<> ALL` (= NOT IN) is anti-join-equivalent; `< ALL`/`> ALL`/etc.
    // have min/max semantics and were silently mis-decorrelated as anti-join
    // (PGC-146). Reject so the query is forwarded.
    if sublink_type == SubLinkType::All {
        sublink_all_operator_check(&sub_link.oper_name)?;
    }

    Ok(WhereExpr::Subquery {
        query: Box::new(query),
        sublink_type,
        test_expr,
    })
}

/// Extract the single-string operator name from an `oper_name` list (SubLink
/// / A_Expr). Any other shape is a structural parse error.
fn operator_name_string_extract<'a>(
    oper_name: &'a [pg_query::Node],
    context: &str,
) -> Result<&'a str, WhereParseError> {
    let [name_node] = oper_name else {
        return Err(WhereParseError::Other {
            error: format!("{context}: expected single name node"),
        });
    };
    let Some(NodeEnum::String(s)) = &name_node.node else {
        return Err(WhereParseError::Other {
            error: format!("{context}: expected string node"),
        });
    };
    Ok(s.sval.as_str())
}

/// Ensure a SubLink of type `ALL` uses the only operator that is
/// anti-join-equivalent: `<>` (semantically `NOT IN`). Other operators
/// produce a parse error and forward the query to origin.
fn sublink_all_operator_check(oper_name: &[pg_query::Node]) -> Result<(), WhereParseError> {
    let op = operator_name_string_extract(oper_name, "ALL operator")?;
    if op == "<>" {
        Ok(())
    } else {
        Err(WhereParseError::UnsupportedOperator {
            operator: format!("ALL with operator '{op}'"),
        })
    }
}

/// Convert pg_query NullTest to WhereExpr (IS NULL / IS NOT NULL)
fn null_test_convert(null_test: &NullTest) -> Result<WhereExpr, WhereParseError> {
    let arg = null_test
        .arg
        .as_ref()
        .ok_or(WhereParseError::MissingExpression)?;

    let op = match null_test.nulltesttype() {
        NullTestType::IsNull => UnaryOp::IsNull,
        NullTestType::IsNotNull => UnaryOp::IsNotNull,
        NullTestType::Undefined => {
            return Err(WhereParseError::UnsupportedAExpr {
                expr: "Undefined NullTest type".to_owned(),
            });
        }
    };

    Ok(WhereExpr::Unary(UnaryExpr {
        op,
        expr: Box::new(node_convert_to_expr(arg)?),
    }))
}

/// Convert pg_query BooleanTest to WhereExpr (IS TRUE / IS FALSE / IS NOT TRUE / IS NOT FALSE)
fn boolean_test_convert(bool_test: &BooleanTest) -> Result<WhereExpr, WhereParseError> {
    let arg = bool_test
        .arg
        .as_ref()
        .ok_or(WhereParseError::MissingExpression)?;

    let op = match bool_test.booltesttype() {
        BoolTestType::IsTrue => UnaryOp::IsTrue,
        BoolTestType::IsNotTrue => UnaryOp::IsNotTrue,
        BoolTestType::IsFalse => UnaryOp::IsFalse,
        BoolTestType::IsNotFalse => UnaryOp::IsNotFalse,
        // IS UNKNOWN / IS NOT UNKNOWN are semantically identical to IS NULL / IS NOT NULL
        BoolTestType::IsUnknown => UnaryOp::IsNull,
        BoolTestType::IsNotUnknown => UnaryOp::IsNotNull,
        BoolTestType::Undefined => {
            return Err(WhereParseError::UnsupportedAExpr {
                expr: "Undefined BooleanTest type".to_owned(),
            });
        }
    };

    Ok(WhereExpr::Unary(UnaryExpr {
        op,
        expr: Box::new(node_convert_to_expr(arg)?),
    }))
}

/// Extract column reference from pg_query ColumnRef
pub(super) fn column_ref_extract(col_ref: &ColumnRef) -> Result<ColumnNode, WhereParseError> {
    if col_ref.fields.is_empty() {
        return Err(WhereParseError::InvalidColumnRef);
    }

    let mut table: Option<EcoString> = None;
    let mut column: Option<EcoString> = None;

    for field in &col_ref.fields {
        match field.node.as_ref() {
            Some(NodeEnum::String(s)) => {
                if column.is_none() {
                    column = Some(EcoString::from(s.sval.as_str()));
                } else {
                    // If we already have a column, previous value becomes table
                    table = column.clone();
                    column = Some(EcoString::from(s.sval.as_str()));
                }
            }
            _ => return Err(WhereParseError::InvalidColumnRef),
        }
    }

    let column = column.ok_or(WhereParseError::InvalidColumnRef)?;
    Ok(ColumnNode { table, column })
}

/// Extract constant value from pg_query A_Const
pub fn const_value_extract(const_val: &AConst) -> Result<LiteralValue, WhereParseError> {
    // Check for NULL values first
    if const_val.isnull {
        return Ok(LiteralValue::Null);
    }

    match const_val.val.as_ref() {
        Some(Val::Sval(s)) => Ok(LiteralValue::String(s.sval.clone())),
        Some(Val::Ival(i)) => Ok(LiteralValue::Integer(i.ival as i64)),
        Some(Val::Fval(f)) => f
            .fval
            .parse::<f64>()
            .ok()
            .and_then(|v| NotNan::new(v).ok())
            .map(LiteralValue::Float)
            .ok_or_else(|| WhereParseError::InvalidConstValue {
                value: f.fval.clone(),
            }),
        Some(Val::Boolval(b)) => Ok(LiteralValue::Boolean(b.boolval)),
        Some(Val::Bsval(bs)) => Ok(LiteralValue::String(bs.bsval.clone())), // Bit strings as strings for now
        None => Ok(LiteralValue::Null),                                     // Fallback for NULL
    }
}

/// Extract parameter reference from pg_query ParamRef
pub(super) fn param_ref_extract(param_ref: &ParamRef) -> LiteralValue {
    LiteralValue::Parameter(format!("${}", param_ref.number))
}

/// Convert PostgreSQL A_Expr (expressions like col = value)
#[expect(clippy::wildcard_enum_match_arm)]
fn a_expr_convert(expr: &AExpr) -> Result<WhereExpr, WhereParseError> {
    match expr.kind() {
        AExprKind::AexprOp => {
            // pg_query uses AExprOp for both arithmetic (+, -, *, /, %) and
            // comparison (=, <, >=, !=). Arithmetic yields a scalar; comparison
            // yields a predicate. Dispatch on the operator name.
            if arithmetic_op_extract(&expr.name).is_ok() {
                let arith = aexpr_arithmetic_convert(expr)?;
                return Ok(WhereExpr::Scalar(ScalarExpr::Arithmetic(arith)));
            }

            let op = operator_extract(&expr.name)?;

            let lexpr = expr
                .lexpr
                .as_ref()
                .ok_or(WhereParseError::MissingExpression)?;
            let rexpr = expr
                .rexpr
                .as_ref()
                .ok_or(WhereParseError::MissingExpression)?;

            Ok(WhereExpr::Binary(BinaryExpr {
                op,
                lexpr: Box::new(node_convert_to_expr(lexpr)?),
                rexpr: Box::new(node_convert_to_expr(rexpr)?),
            }))
        }
        AExprKind::AexprIn => {
            // Handle IN / NOT IN expressions
            // name: ["="] for IN, ["<>"] for NOT IN
            let op = in_operator_extract(&expr.name)?;

            let lexpr = expr
                .lexpr
                .as_ref()
                .ok_or(WhereParseError::MissingExpression)?;
            let rexpr = expr
                .rexpr
                .as_ref()
                .ok_or(WhereParseError::MissingExpression)?;

            // Left side is the column/expression being tested
            let left_expr = node_convert_to_expr(lexpr)?;

            // Right side is a List of values
            let values = in_list_extract(rexpr)?;

            // Build MultiExpr: [column, value1, value2, ...]
            let mut exprs = vec![left_expr];
            exprs.extend(values);

            Ok(WhereExpr::Multi(MultiExpr { op, exprs }))
        }
        AExprKind::AexprBetween
        | AExprKind::AexprNotBetween
        | AExprKind::AexprBetweenSym
        | AExprKind::AexprNotBetweenSym => {
            // Handle BETWEEN / NOT BETWEEN / BETWEEN SYMMETRIC / NOT BETWEEN SYMMETRIC
            // pg_query: lexpr = tested expression, rexpr = List([low, high])
            let op = match expr.kind() {
                AExprKind::AexprBetween => MultiOp::Between,
                AExprKind::AexprNotBetween => MultiOp::NotBetween,
                AExprKind::AexprBetweenSym => MultiOp::BetweenSymmetric,
                _ => MultiOp::NotBetweenSymmetric,
            };

            let lexpr = expr
                .lexpr
                .as_ref()
                .ok_or(WhereParseError::MissingExpression)?;
            let rexpr = expr
                .rexpr
                .as_ref()
                .ok_or(WhereParseError::MissingExpression)?;

            let left_expr = node_convert_to_expr(lexpr)?;
            let bounds = between_bounds_extract(rexpr)?;

            Ok(WhereExpr::Multi(MultiExpr {
                op,
                exprs: vec![left_expr, bounds.0, bounds.1],
            }))
        }
        AExprKind::AexprLike | AExprKind::AexprIlike => {
            // Handle LIKE / ILIKE / NOT LIKE / NOT ILIKE
            // pg_query name: "~~" LIKE, "!~~" NOT LIKE, "~~*" ILIKE, "!~~*" NOT ILIKE
            let op = like_operator_extract(&expr.name)?;

            let lexpr = expr
                .lexpr
                .as_ref()
                .ok_or(WhereParseError::MissingExpression)?;
            let rexpr = expr
                .rexpr
                .as_ref()
                .ok_or(WhereParseError::MissingExpression)?;

            Ok(WhereExpr::Binary(BinaryExpr {
                op,
                lexpr: Box::new(node_convert_to_expr(lexpr)?),
                rexpr: Box::new(node_convert_to_expr(rexpr)?),
            }))
        }
        AExprKind::AexprOpAny | AExprKind::AexprOpAll => {
            // Handle col = ANY(...) / col = ALL(...)
            // pg_query: name = operator (e.g. "="), lexpr = tested expression,
            // rexpr = array expression (AArrayExpr, ParamRef, or ColumnRef)
            let comparison = operator_extract(&expr.name)?;
            let op = match expr.kind() {
                AExprKind::AexprOpAny => MultiOp::Any { comparison },
                _ => MultiOp::All { comparison },
            };

            let lexpr = expr
                .lexpr
                .as_ref()
                .ok_or(WhereParseError::MissingExpression)?;
            let rexpr = expr
                .rexpr
                .as_ref()
                .ok_or(WhereParseError::MissingExpression)?;

            let left_expr = node_convert_to_expr(lexpr)?;
            let right_expr = any_all_rexpr_convert(rexpr)?;

            Ok(WhereExpr::Multi(MultiExpr {
                op,
                exprs: vec![left_expr, right_expr],
            }))
        }
        unsupported_kind => {
            dbg!(unsupported_kind);
            Err(WhereParseError::UnsupportedAExpr {
                expr: format!("{unsupported_kind:?}"),
            })
        }
    }
}

/// Extract IN/NOT IN operator from name nodes
fn in_operator_extract(name_nodes: &[pg_query::Node]) -> Result<MultiOp, WhereParseError> {
    match operator_name_string_extract(name_nodes, "IN operator")? {
        "=" => Ok(MultiOp::In),
        "<>" => Ok(MultiOp::NotIn),
        other => Err(WhereParseError::UnsupportedOperator {
            operator: format!("IN with operator '{other}'"),
        }),
    }
}

/// Extract values from IN list (pg_query List node)
fn in_list_extract(node: &pg_query::Node) -> Result<Vec<WhereExpr>, WhereParseError> {
    let Some(NodeEnum::List(list)) = &node.node else {
        return Err(WhereParseError::Other {
            error: "IN clause: expected List on right side".to_owned(),
        });
    };

    list.items.iter().map(node_convert_to_expr).collect()
}

/// Extract lower and upper bounds from BETWEEN list (pg_query List with exactly 2 items)
fn between_bounds_extract(
    node: &pg_query::Node,
) -> Result<(WhereExpr, WhereExpr), WhereParseError> {
    let Some(NodeEnum::List(list)) = &node.node else {
        return Err(WhereParseError::Other {
            error: "BETWEEN clause: expected List on right side".to_owned(),
        });
    };

    let [low, high] = list.items.as_slice() else {
        return Err(WhereParseError::Other {
            error: format!(
                "BETWEEN clause: expected exactly 2 bounds, got {}",
                list.items.len()
            ),
        });
    };

    Ok((node_convert_to_expr(low)?, node_convert_to_expr(high)?))
}

/// Convert the right-hand side of ANY/ALL.
///
/// The rexpr can be an array constructor (ARRAY[1,2,3]) wrapped as a
/// `ScalarExpr::Array`, or a single expression (parameter $1, column ref)
/// passed through as-is.
fn any_all_rexpr_convert(node: &pg_query::Node) -> Result<WhereExpr, WhereParseError> {
    match &node.node {
        Some(NodeEnum::AArrayExpr(array_expr)) => {
            let elems = array_expr
                .elements
                .iter()
                .map(node_convert_to_scalar_expr)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(WhereExpr::Scalar(ScalarExpr::Array(elems)))
        }
        _ => node_convert_to_expr(node),
    }
}

/// Extract LIKE/ILIKE operator from pg_query name nodes.
///
/// pg_query uses internal operator names: `~~` (LIKE), `!~~` (NOT LIKE),
/// `~~*` (ILIKE), `!~~*` (NOT ILIKE).
fn like_operator_extract(name_nodes: &[pg_query::Node]) -> Result<BinaryOp, WhereParseError> {
    let [name_node] = name_nodes else {
        return Err(WhereParseError::Other {
            error: "LIKE operator: expected single name node".to_owned(),
        });
    };

    let Some(NodeEnum::String(name_str)) = &name_node.node else {
        return Err(WhereParseError::Other {
            error: "LIKE operator: expected string node".to_owned(),
        });
    };

    match name_str.sval.as_str() {
        "~~" => Ok(BinaryOp::Like),
        "!~~" => Ok(BinaryOp::NotLike),
        "~~*" => Ok(BinaryOp::ILike),
        "!~~*" => Ok(BinaryOp::NotILike),
        other => Err(WhereParseError::UnsupportedOperator {
            operator: format!("LIKE with operator '{other}'"),
        }),
    }
}

/// Extract operator from pg_query operator name nodes
fn operator_extract(name_nodes: &[pg_query::Node]) -> Result<BinaryOp, WhereParseError> {
    let [name_node] = name_nodes else {
        return Err(WhereParseError::Other {
            error: "Multi-part operator names not supported".to_owned(),
        });
    };

    match name_node.node.as_ref() {
        Some(NodeEnum::String(s)) => match s.sval.as_str() {
            "=" => Ok(BinaryOp::Equal),
            "!=" | "<>" => Ok(BinaryOp::NotEqual),
            "<" => Ok(BinaryOp::LessThan),
            "<=" => Ok(BinaryOp::LessThanOrEqual),
            ">" => Ok(BinaryOp::GreaterThan),
            ">=" => Ok(BinaryOp::GreaterThanOrEqual),

            op => {
                trace!(operator = %op, "unsupported comparison operator");
                Err(WhereParseError::UnsupportedOperator {
                    operator: op.to_owned(),
                })
            }
        },
        unsupported => {
            trace!(?unsupported, "invalid operator name format");
            Err(WhereParseError::Other {
                error: "Invalid operator name format".to_owned(),
            })
        }
    }
}

/// Convert PostgreSQL BoolExpr (AND, OR, NOT)
fn bool_expr_convert(expr: &BoolExpr) -> Result<WhereExpr, WhereParseError> {
    match expr.boolop() {
        BoolExprType::AndExpr => {
            let [first, second, rest @ ..] = expr.args.as_slice() else {
                return Err(WhereParseError::Other {
                    error: "AND with < 2 arguments not supported".to_owned(),
                });
            };

            // For chained AND expressions (a AND b AND c), build a left-associative tree:
            // ((a AND b) AND c)
            let mut result = WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::And,
                lexpr: Box::new(node_convert_to_expr(first)?),
                rexpr: Box::new(node_convert_to_expr(second)?),
            });

            // Chain additional arguments
            for arg in rest {
                result = WhereExpr::Binary(BinaryExpr {
                    op: BinaryOp::And,
                    lexpr: Box::new(result),
                    rexpr: Box::new(node_convert_to_expr(arg)?),
                });
            }

            Ok(result)
        }
        BoolExprType::OrExpr => {
            let [first, second, rest @ ..] = expr.args.as_slice() else {
                return Err(WhereParseError::Other {
                    error: "OR with < 2 arguments not supported".to_owned(),
                });
            };

            // For chained OR expressions (a OR b OR c), build a left-associative tree:
            // ((a OR b) OR c)
            let mut result = WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Or,
                lexpr: Box::new(node_convert_to_expr(first)?),
                rexpr: Box::new(node_convert_to_expr(second)?),
            });

            // Chain additional arguments
            for arg in rest {
                result = WhereExpr::Binary(BinaryExpr {
                    op: BinaryOp::Or,
                    lexpr: Box::new(result),
                    rexpr: Box::new(node_convert_to_expr(arg)?),
                });
            }

            Ok(result)
        }
        BoolExprType::NotExpr => {
            let [arg] = expr.args.as_slice() else {
                return Err(WhereParseError::Other {
                    error: "NOT with != 1 argument not supported".to_owned(),
                });
            };

            Ok(WhereExpr::Unary(UnaryExpr {
                op: UnaryOp::Not,
                expr: Box::new(node_convert_to_expr(arg)?),
            }))
        }
        BoolExprType::Undefined => Err(WhereParseError::Other {
            error: "Undefined boolean expression type".to_owned(),
        }),
    }
}

#[cfg(test)]
mod tests {

    #![allow(clippy::wildcard_enum_match_arm)]

    use super::*;
    use crate::query::ast::{
        ArithmeticOp, AstError, Deparse, ScalarExpr, SelectColumns, query_expr_convert,
        query_expr_fingerprint,
    };

    /// Parse SQL and extract the WHERE clause via the AST layer.
    fn where_clause_parse(sql: &str) -> Result<Option<WhereExpr>, AstError> {
        let ast = pg_query::parse(sql).unwrap();
        let query_expr = query_expr_convert(&ast)?;
        Ok(query_expr.where_clause().cloned())
    }

    #[test]
    fn fingerprint_literals_differ() {
        let q1 = query_expr_convert(
            &pg_query::parse("select id, str from test where str = 'hello'").unwrap(),
        )
        .unwrap();
        let q2 = query_expr_convert(
            &pg_query::parse("select id, str from test where str = 'bye'").unwrap(),
        )
        .unwrap();

        assert_ne!(query_expr_fingerprint(&q1), query_expr_fingerprint(&q2));
    }

    #[test]
    fn select_columns() {
        let q = query_expr_convert(
            &pg_query::parse("select id, str from test where str = 'hello'").unwrap(),
        )
        .unwrap();
        let select = q.as_select().unwrap();
        let SelectColumns::Columns(cols) = &select.columns else {
            panic!("expected explicit columns");
        };
        assert_eq!(cols.len(), 2);
        assert!(matches!(&cols[0].expr().unwrap(), ScalarExpr::Column(c) if c.column == "id"));
        assert!(matches!(&cols[1].expr().unwrap(), ScalarExpr::Column(c) if c.column == "str"));

        let q = query_expr_convert(
            &pg_query::parse("select count(id), str from test where str = 'hihi'").unwrap(),
        )
        .unwrap();
        let select = q.as_select().unwrap();
        let SelectColumns::Columns(cols) = &select.columns else {
            panic!("expected explicit columns");
        };
        assert_eq!(cols.len(), 2);
        assert!(matches!(&cols[0].expr().unwrap(), ScalarExpr::Function(f) if f.name == "count"));
        assert!(matches!(&cols[1].expr().unwrap(), ScalarExpr::Column(c) if c.column == "str"));
    }

    #[test]
    fn where_clause_simple_equality() {
        let result = where_clause_parse("SELECT id, str FROM test WHERE str = 'hello'");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::Equal,
            lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("str"),
            }))),
            rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                LiteralValue::String("hello".to_owned()),
            ))),
        }));

        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_integer_equality() {
        let result = where_clause_parse("SELECT id FROM test WHERE id = 123");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::Equal,
            lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("id"),
            }))),
            rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                LiteralValue::Integer(123),
            ))),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_boolean_equality() {
        let result = where_clause_parse("SELECT id FROM test WHERE active = true");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::Equal,
            lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("active"),
            }))),
            rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                LiteralValue::Boolean(true),
            ))),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_greater_than() {
        let result = where_clause_parse("SELECT id FROM test WHERE cnt > 0");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::GreaterThan,
            lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("cnt"),
            }))),
            rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                LiteralValue::Integer(0),
            ))),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_and_operation() {
        let result = where_clause_parse("SELECT id FROM test WHERE str = 'hello' AND id = 123");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::And,
            lexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Equal,
                lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                    table: None,
                    column: EcoString::from("str"),
                }))),
                rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                    LiteralValue::String("hello".to_owned()),
                ))),
            })),
            rexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Equal,
                lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                    table: None,
                    column: EcoString::from("id"),
                }))),
                rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                    LiteralValue::Integer(123),
                ))),
            })),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_or_operation() {
        let result = where_clause_parse("SELECT id FROM test WHERE str = 'hello' OR str = 'world'");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::Or,
            lexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Equal,
                lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                    table: None,
                    column: EcoString::from("str"),
                }))),
                rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                    LiteralValue::String("hello".to_owned()),
                ))),
            })),
            rexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Equal,
                lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                    table: None,
                    column: EcoString::from("str"),
                }))),
                rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                    LiteralValue::String("world".to_owned()),
                ))),
            })),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_not_operation() {
        let result = where_clause_parse("SELECT id FROM test WHERE NOT str = 'hello'");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Unary(UnaryExpr {
            op: UnaryOp::Not,
            expr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Equal,
                lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                    table: None,
                    column: EcoString::from("str"),
                }))),
                rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                    LiteralValue::String("hello".to_owned()),
                ))),
            })),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_qualified_column() {
        let result = where_clause_parse("SELECT id FROM test WHERE test.str = 'hello'");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::Equal,
            lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                table: Some(EcoString::from("test")),
                column: EcoString::from("str"),
            }))),
            rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                LiteralValue::String("hello".to_owned()),
            ))),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_null_value() {
        let result = where_clause_parse("SELECT id FROM test WHERE data = NULL");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::Equal,
            lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("data"),
            }))),
            rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::Null))),
        }));

        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_no_where() {
        let result = where_clause_parse("SELECT id FROM test");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        assert_eq!(where_clause, None);
    }

    #[test]
    fn where_clause_not_equal_with_exclamation() {
        let result = where_clause_parse("SELECT id FROM test WHERE id != 123");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::NotEqual,
            lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("id"),
            }))),
            rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                LiteralValue::Integer(123),
            ))),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_not_equal_with_angle_brackets() {
        let result = where_clause_parse("SELECT id FROM test WHERE id <> 123");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::NotEqual,
            lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("id"),
            }))),
            rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                LiteralValue::Integer(123),
            ))),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_less_than() {
        let result = where_clause_parse("SELECT id FROM test WHERE id < 123");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::LessThan,
            lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("id"),
            }))),
            rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                LiteralValue::Integer(123),
            ))),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_less_than_or_equal() {
        let result = where_clause_parse("SELECT id FROM test WHERE id <= 123");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::LessThanOrEqual,
            lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("id"),
            }))),
            rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                LiteralValue::Integer(123),
            ))),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_greater_than_or_equal() {
        let result = where_clause_parse("SELECT id FROM test WHERE id >= 123");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::GreaterThanOrEqual,
            lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("id"),
            }))),
            rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                LiteralValue::Integer(123),
            ))),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_like() {
        let result = where_clause_parse("SELECT id FROM test WHERE name LIKE 'test%'");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Binary(binary) = where_clause else {
            panic!("expected BinaryExpr");
        };

        assert_eq!(binary.op, BinaryOp::Like);

        let WhereExpr::Scalar(ScalarExpr::Column(col)) = binary.lexpr.as_ref() else {
            panic!("expected Column on left");
        };
        assert_eq!(col.column, "name");

        let WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::String(pattern))) =
            binary.rexpr.as_ref()
        else {
            panic!("expected string pattern on right");
        };
        assert_eq!(pattern, "test%");
    }

    #[test]
    fn where_clause_chained_and_operation() {
        let result = where_clause_parse(
            "SELECT id FROM test WHERE name = 'john' AND age > 25 AND active = true",
        );

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        // Should build a left-associative tree: ((name = 'john' AND age > 25) AND active = true)
        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::And,
            lexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::And,
                lexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                    op: BinaryOp::Equal,
                    lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                        table: None,
                        column: EcoString::from("name"),
                    }))),
                    rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                        LiteralValue::String("john".to_owned()),
                    ))),
                })),
                rexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                    op: BinaryOp::GreaterThan,
                    lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                        table: None,
                        column: EcoString::from("age"),
                    }))),
                    rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                        LiteralValue::Integer(25),
                    ))),
                })),
            })),
            rexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Equal,
                lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                    table: None,
                    column: EcoString::from("active"),
                }))),
                rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                    LiteralValue::Boolean(true),
                ))),
            })),
        }));

        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_chained_or_operation() {
        let result = where_clause_parse(
            "SELECT id FROM test WHERE name = 'john' OR name = 'jane' OR name = 'bob'",
        );

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        // Should build a left-associative tree: ((name = 'john' OR name = 'jane') OR name = 'bob')
        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::Or,
            lexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Or,
                lexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                    op: BinaryOp::Equal,
                    lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                        table: None,
                        column: EcoString::from("name"),
                    }))),
                    rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                        LiteralValue::String("john".to_owned()),
                    ))),
                })),
                rexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                    op: BinaryOp::Equal,
                    lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                        table: None,
                        column: EcoString::from("name"),
                    }))),
                    rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                        LiteralValue::String("jane".to_owned()),
                    ))),
                })),
            })),
            rexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Equal,
                lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                    table: None,
                    column: EcoString::from("name"),
                }))),
                rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                    LiteralValue::String("bob".to_owned()),
                ))),
            })),
        }));

        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_parameterized_query_single() {
        let result = where_clause_parse("SELECT id FROM test WHERE id = $1");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::Equal,
            lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("id"),
            }))),
            rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                LiteralValue::Parameter("$1".to_owned()),
            ))),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_parameterized_query_multiple() {
        let result = where_clause_parse("SELECT id FROM test WHERE name = $1 AND age > $2");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::And,
            lexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Equal,
                lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                    table: None,
                    column: EcoString::from("name"),
                }))),
                rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                    LiteralValue::Parameter("$1".to_owned()),
                ))),
            })),
            rexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::GreaterThan,
                lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                    table: None,
                    column: EcoString::from("age"),
                }))),
                rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                    LiteralValue::Parameter("$2".to_owned()),
                ))),
            })),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_parameterized_query_mixed_with_literals() {
        let result = where_clause_parse("SELECT id FROM test WHERE name = $1 AND active = true");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::And,
            lexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Equal,
                lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                    table: None,
                    column: EcoString::from("name"),
                }))),
                rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                    LiteralValue::Parameter("$1".to_owned()),
                ))),
            })),
            rexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Equal,
                lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                    table: None,
                    column: EcoString::from("active"),
                }))),
                rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                    LiteralValue::Boolean(true),
                ))),
            })),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_in_with_strings() {
        let result =
            where_clause_parse("SELECT * FROM t WHERE status IN ('active', 'pending', 'complete')");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Multi(multi) = where_clause else {
            panic!("expected MultiExpr, got {:?}", where_clause);
        };

        assert_eq!(multi.op, MultiOp::In);
        assert_eq!(multi.exprs.len(), 4); // column + 3 values

        // First element should be the column
        let WhereExpr::Scalar(ScalarExpr::Column(col)) = &multi.exprs[0] else {
            panic!("expected Column");
        };
        assert_eq!(col.column, "status");

        // Remaining elements should be string values
        let WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::String(v1))) = &multi.exprs[1]
        else {
            panic!("expected string value");
        };
        assert_eq!(v1, "active");
    }

    #[test]
    fn where_clause_not_in() {
        let result = where_clause_parse("SELECT * FROM t WHERE id NOT IN (1, 2, 3)");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Multi(multi) = where_clause else {
            panic!("expected MultiExpr");
        };

        assert_eq!(multi.op, MultiOp::NotIn);
        assert_eq!(multi.exprs.len(), 4); // column + 3 values
    }

    #[test]
    fn where_clause_in_with_integers() {
        let result = where_clause_parse("SELECT * FROM t WHERE id IN (1, 2, 3)");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Multi(multi) = where_clause else {
            panic!("expected MultiExpr");
        };

        assert_eq!(multi.op, MultiOp::In);

        // Check that values are integers
        let WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::Integer(v1))) = &multi.exprs[1]
        else {
            panic!("expected integer value");
        };
        assert_eq!(*v1, 1);
    }

    #[test]
    fn where_clause_in_combined_with_and() {
        let result = where_clause_parse(
            "SELECT * FROM t WHERE tenant_id = 1 AND status IN ('active', 'pending')",
        );

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        // Should be AND(tenant_id = 1, status IN (...))
        let WhereExpr::Binary(binary) = where_clause else {
            panic!("expected BinaryExpr");
        };

        assert_eq!(binary.op, BinaryOp::And);

        // Right side should be the IN clause
        let WhereExpr::Multi(multi) = binary.rexpr.as_ref() else {
            panic!("expected MultiExpr on right side");
        };
        assert_eq!(multi.op, MultiOp::In);
    }

    #[test]
    fn where_clause_is_null() {
        let result = where_clause_parse("SELECT id FROM test WHERE deleted_at IS NULL");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Unary(unary) = where_clause else {
            panic!("expected UnaryExpr");
        };

        assert_eq!(unary.op, UnaryOp::IsNull);

        let WhereExpr::Scalar(ScalarExpr::Column(col)) = unary.expr.as_ref() else {
            panic!("expected Column");
        };
        assert_eq!(col.column, "deleted_at");
    }

    #[test]
    fn where_clause_is_not_null() {
        let result = where_clause_parse("SELECT id FROM test WHERE name IS NOT NULL");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Unary(unary) = where_clause else {
            panic!("expected UnaryExpr");
        };

        assert_eq!(unary.op, UnaryOp::IsNotNull);

        let WhereExpr::Scalar(ScalarExpr::Column(col)) = unary.expr.as_ref() else {
            panic!("expected Column");
        };
        assert_eq!(col.column, "name");
    }

    #[test]
    fn where_clause_is_true() {
        let result = where_clause_parse("SELECT id FROM test WHERE active IS TRUE");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Unary(unary) = where_clause else {
            panic!("expected UnaryExpr");
        };

        assert_eq!(unary.op, UnaryOp::IsTrue);

        let WhereExpr::Scalar(ScalarExpr::Column(col)) = unary.expr.as_ref() else {
            panic!("expected Column");
        };
        assert_eq!(col.column, "active");
    }

    #[test]
    fn where_clause_is_false() {
        let result = where_clause_parse("SELECT id FROM test WHERE active IS FALSE");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Unary(unary) = where_clause else {
            panic!("expected UnaryExpr");
        };

        assert_eq!(unary.op, UnaryOp::IsFalse);

        let WhereExpr::Scalar(ScalarExpr::Column(col)) = unary.expr.as_ref() else {
            panic!("expected Column");
        };
        assert_eq!(col.column, "active");
    }

    #[test]
    fn where_clause_is_not_true() {
        let result = where_clause_parse("SELECT id FROM test WHERE active IS NOT TRUE");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Unary(unary) = where_clause else {
            panic!("expected UnaryExpr");
        };

        assert_eq!(unary.op, UnaryOp::IsNotTrue);

        let WhereExpr::Scalar(ScalarExpr::Column(col)) = unary.expr.as_ref() else {
            panic!("expected Column");
        };
        assert_eq!(col.column, "active");
    }

    #[test]
    fn where_clause_is_not_false() {
        let result = where_clause_parse("SELECT id FROM test WHERE active IS NOT FALSE");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Unary(unary) = where_clause else {
            panic!("expected UnaryExpr");
        };

        assert_eq!(unary.op, UnaryOp::IsNotFalse);

        let WhereExpr::Scalar(ScalarExpr::Column(col)) = unary.expr.as_ref() else {
            panic!("expected Column");
        };
        assert_eq!(col.column, "active");
    }

    #[test]
    fn where_clause_is_true_combined_with_and() {
        let result = where_clause_parse("SELECT * FROM t WHERE id = 1 AND active IS TRUE");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Binary(binary) = where_clause else {
            panic!("expected BinaryExpr");
        };

        assert_eq!(binary.op, BinaryOp::And);

        let WhereExpr::Unary(unary) = binary.rexpr.as_ref() else {
            panic!("expected UnaryExpr on right side");
        };
        assert_eq!(unary.op, UnaryOp::IsTrue);
    }

    #[test]
    fn where_clause_is_unknown_maps_to_is_null() {
        let result = where_clause_parse("SELECT id FROM test WHERE active IS UNKNOWN");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Unary(unary) = where_clause else {
            panic!("expected UnaryExpr");
        };

        // IS UNKNOWN is semantically identical to IS NULL
        assert_eq!(unary.op, UnaryOp::IsNull);
    }

    #[test]
    fn where_clause_is_not_unknown_maps_to_is_not_null() {
        let result = where_clause_parse("SELECT id FROM test WHERE active IS NOT UNKNOWN");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Unary(unary) = where_clause else {
            panic!("expected UnaryExpr");
        };

        // IS NOT UNKNOWN is semantically identical to IS NOT NULL
        assert_eq!(unary.op, UnaryOp::IsNotNull);
    }

    #[test]
    fn where_clause_is_null_combined_with_and() {
        let result = where_clause_parse("SELECT * FROM t WHERE id = 1 AND deleted_at IS NULL");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        // Should be AND(id = 1, deleted_at IS NULL)
        let WhereExpr::Binary(binary) = where_clause else {
            panic!("expected BinaryExpr");
        };

        assert_eq!(binary.op, BinaryOp::And);

        // Right side should be IS NULL
        let WhereExpr::Unary(unary) = binary.rexpr.as_ref() else {
            panic!("expected UnaryExpr on right side");
        };
        assert_eq!(unary.op, UnaryOp::IsNull);
    }

    #[test]
    fn where_clause_between_integers() {
        let result = where_clause_parse("SELECT * FROM t WHERE id BETWEEN 1 AND 10");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Multi(multi) = where_clause else {
            panic!("expected MultiExpr");
        };

        assert_eq!(multi.op, MultiOp::Between);
        assert_eq!(multi.exprs.len(), 3);

        let WhereExpr::Scalar(ScalarExpr::Column(col)) = &multi.exprs[0] else {
            panic!("expected Column");
        };
        assert_eq!(col.column, "id");

        let WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::Integer(low))) = &multi.exprs[1]
        else {
            panic!("expected integer low bound");
        };
        assert_eq!(*low, 1);

        let WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::Integer(high))) = &multi.exprs[2]
        else {
            panic!("expected integer high bound");
        };
        assert_eq!(*high, 10);
    }

    #[test]
    fn where_clause_not_between() {
        let result = where_clause_parse("SELECT * FROM t WHERE price NOT BETWEEN 100 AND 200");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Multi(multi) = where_clause else {
            panic!("expected MultiExpr");
        };

        assert_eq!(multi.op, MultiOp::NotBetween);
        assert_eq!(multi.exprs.len(), 3);
    }

    #[test]
    fn where_clause_between_with_parameters() {
        let result = where_clause_parse("SELECT * FROM t WHERE created_at BETWEEN $1 AND $2");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Multi(multi) = where_clause else {
            panic!("expected MultiExpr");
        };

        assert_eq!(multi.op, MultiOp::Between);
        assert_eq!(multi.exprs.len(), 3);

        let WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::Parameter(p1))) = &multi.exprs[1]
        else {
            panic!("expected parameter low bound");
        };
        assert_eq!(p1, "$1");

        let WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::Parameter(p2))) = &multi.exprs[2]
        else {
            panic!("expected parameter high bound");
        };
        assert_eq!(p2, "$2");
    }

    #[test]
    fn where_clause_between_combined_with_and() {
        let result =
            where_clause_parse("SELECT * FROM t WHERE tenant_id = 1 AND price BETWEEN 10 AND 50");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Binary(binary) = where_clause else {
            panic!("expected BinaryExpr");
        };

        assert_eq!(binary.op, BinaryOp::And);

        let WhereExpr::Multi(multi) = binary.rexpr.as_ref() else {
            panic!("expected MultiExpr on right side");
        };
        assert_eq!(multi.op, MultiOp::Between);
    }

    #[test]
    fn where_clause_between_strings() {
        let result = where_clause_parse("SELECT * FROM t WHERE name BETWEEN 'alice' AND 'charlie'");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Multi(multi) = where_clause else {
            panic!("expected MultiExpr");
        };

        assert_eq!(multi.op, MultiOp::Between);

        let WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::String(low))) = &multi.exprs[1]
        else {
            panic!("expected string low bound");
        };
        assert_eq!(low, "alice");

        let WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::String(high))) = &multi.exprs[2]
        else {
            panic!("expected string high bound");
        };
        assert_eq!(high, "charlie");
    }

    #[test]
    fn where_clause_between_symmetric() {
        let result = where_clause_parse("SELECT * FROM t WHERE id BETWEEN SYMMETRIC 10 AND 1");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Multi(multi) = where_clause else {
            panic!("expected MultiExpr");
        };

        assert_eq!(multi.op, MultiOp::BetweenSymmetric);
        assert_eq!(multi.exprs.len(), 3);

        let WhereExpr::Scalar(ScalarExpr::Column(col)) = &multi.exprs[0] else {
            panic!("expected Column");
        };
        assert_eq!(col.column, "id");

        let WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::Integer(low))) = &multi.exprs[1]
        else {
            panic!("expected integer low bound");
        };
        assert_eq!(*low, 10);

        let WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::Integer(high))) = &multi.exprs[2]
        else {
            panic!("expected integer high bound");
        };
        assert_eq!(*high, 1);
    }

    #[test]
    fn where_clause_not_between_symmetric() {
        let result = where_clause_parse("SELECT * FROM t WHERE id NOT BETWEEN SYMMETRIC 10 AND 1");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Multi(multi) = where_clause else {
            panic!("expected MultiExpr");
        };

        assert_eq!(multi.op, MultiOp::NotBetweenSymmetric);
        assert_eq!(multi.exprs.len(), 3);
    }

    #[test]
    fn where_clause_any_with_array() {
        let result = where_clause_parse("SELECT * FROM t WHERE id = ANY(ARRAY[1, 2, 3])");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Multi(multi) = where_clause else {
            panic!("expected MultiExpr");
        };

        assert_eq!(
            multi.op,
            MultiOp::Any {
                comparison: BinaryOp::Equal
            }
        );
        // [col, ARRAY[1, 2, 3]]
        assert_eq!(multi.exprs.len(), 2);

        let WhereExpr::Scalar(ScalarExpr::Column(col)) = &multi.exprs[0] else {
            panic!("expected Column");
        };
        assert_eq!(col.column, "id");

        let WhereExpr::Scalar(ScalarExpr::Array(elems)) = &multi.exprs[1] else {
            panic!("expected Array");
        };
        assert_eq!(elems.len(), 3);

        let ScalarExpr::Literal(LiteralValue::Integer(v1)) = &elems[0] else {
            panic!("expected integer");
        };
        assert_eq!(*v1, 1);
    }

    #[test]
    fn where_clause_any_with_parameter() {
        let result = where_clause_parse("SELECT * FROM t WHERE id = ANY($1)");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Multi(multi) = where_clause else {
            panic!("expected MultiExpr");
        };

        assert_eq!(
            multi.op,
            MultiOp::Any {
                comparison: BinaryOp::Equal
            }
        );
        // [col, $1] — parameter passed through as single value
        assert_eq!(multi.exprs.len(), 2);

        let WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::Parameter(p))) = &multi.exprs[1]
        else {
            panic!("expected parameter");
        };
        assert_eq!(p, "$1");
    }

    #[test]
    fn where_clause_all_with_array() {
        let result = where_clause_parse("SELECT * FROM t WHERE score > ALL(ARRAY[80, 90])");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Multi(multi) = where_clause else {
            panic!("expected MultiExpr");
        };

        assert_eq!(
            multi.op,
            MultiOp::All {
                comparison: BinaryOp::GreaterThan
            }
        );
        // [col, ARRAY[80, 90]]
        assert_eq!(multi.exprs.len(), 2);
    }

    #[test]
    fn where_clause_any_not_equal() {
        let result = where_clause_parse("SELECT * FROM t WHERE status <> ANY(ARRAY['a', 'b'])");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Multi(multi) = where_clause else {
            panic!("expected MultiExpr");
        };

        assert_eq!(
            multi.op,
            MultiOp::Any {
                comparison: BinaryOp::NotEqual
            }
        );
        // [col, ARRAY['a', 'b']]
        assert_eq!(multi.exprs.len(), 2);
    }

    #[test]
    fn where_clause_any_combined_with_and() {
        let result =
            where_clause_parse("SELECT * FROM t WHERE tenant_id = 1 AND id = ANY(ARRAY[10, 20])");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Binary(binary) = where_clause else {
            panic!("expected BinaryExpr");
        };

        assert_eq!(binary.op, BinaryOp::And);

        let WhereExpr::Multi(multi) = binary.rexpr.as_ref() else {
            panic!("expected MultiExpr on right side");
        };
        assert_eq!(
            multi.op,
            MultiOp::Any {
                comparison: BinaryOp::Equal
            }
        );
    }

    #[test]
    fn where_clause_not_like() {
        let result = where_clause_parse("SELECT * FROM t WHERE name NOT LIKE '%test%'");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Binary(binary) = where_clause else {
            panic!("expected BinaryExpr");
        };

        assert_eq!(binary.op, BinaryOp::NotLike);
    }

    #[test]
    fn where_clause_ilike() {
        let result = where_clause_parse("SELECT * FROM t WHERE name ILIKE '%test%'");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Binary(binary) = where_clause else {
            panic!("expected BinaryExpr");
        };

        assert_eq!(binary.op, BinaryOp::ILike);
    }

    #[test]
    fn where_clause_not_ilike() {
        let result = where_clause_parse("SELECT * FROM t WHERE name NOT ILIKE '%test%'");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Binary(binary) = where_clause else {
            panic!("expected BinaryExpr");
        };

        assert_eq!(binary.op, BinaryOp::NotILike);
    }

    #[test]
    fn where_clause_like_with_parameter() {
        let result = where_clause_parse("SELECT * FROM t WHERE name LIKE $1");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Binary(binary) = where_clause else {
            panic!("expected BinaryExpr");
        };

        assert_eq!(binary.op, BinaryOp::Like);

        let WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::Parameter(p))) =
            binary.rexpr.as_ref()
        else {
            panic!("expected parameter on right");
        };
        assert_eq!(p, "$1");
    }

    #[test]
    fn where_clause_like_combined_with_and() {
        let result =
            where_clause_parse("SELECT * FROM t WHERE tenant_id = 1 AND name LIKE 'test%'");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Binary(binary) = where_clause else {
            panic!("expected BinaryExpr");
        };

        assert_eq!(binary.op, BinaryOp::And);

        let WhereExpr::Binary(right) = binary.rexpr.as_ref() else {
            panic!("expected BinaryExpr on right side");
        };
        assert_eq!(right.op, BinaryOp::Like);
    }

    // ------------------------------------------------------------------
    // Arithmetic in WHERE (PGC-118 layer 1)
    //
    // Layer 2 (constant_fold) runs at the end of `query_expr_convert`, so
    // pure-literal arithmetic is folded away before these assertions see
    // the tree. Each test uses at least one non-literal operand (column or
    // parameter) to keep the arithmetic node observable.
    // ------------------------------------------------------------------

    /// Pull the RHS of `WHERE col = <rhs>` so each arithmetic test can
    /// assert on the scalar shape directly.
    fn where_clause_rhs(sql: &str) -> ScalarExpr {
        let WhereExpr::Binary(binary) = where_clause_parse(sql).unwrap().unwrap() else {
            panic!("expected binary WHERE");
        };
        let WhereExpr::Scalar(scalar) = *binary.rexpr else {
            panic!("expected scalar RHS");
        };
        scalar
    }

    #[test]
    fn where_clause_arithmetic_with_column_left() {
        let ScalarExpr::Arithmetic(arith) = where_clause_rhs("SELECT * FROM t WHERE x = a + 1")
        else {
            panic!("expected arithmetic RHS");
        };
        assert_eq!(arith.op, ArithmeticOp::Add);
        assert!(matches!(*arith.left, ScalarExpr::Column(ref c) if c.column == "a"));
        assert!(matches!(
            *arith.right,
            ScalarExpr::Literal(LiteralValue::Integer(1))
        ));
    }

    #[test]
    fn where_clause_arithmetic_modulo_with_parameter() {
        let ScalarExpr::Arithmetic(arith) = where_clause_rhs("SELECT * FROM t WHERE x = $1 % 10")
        else {
            panic!("expected arithmetic RHS");
        };
        assert_eq!(arith.op, ArithmeticOp::Modulo);
        assert!(matches!(
            *arith.left,
            ScalarExpr::Literal(LiteralValue::Parameter(_))
        ));
        assert!(matches!(
            *arith.right,
            ScalarExpr::Literal(LiteralValue::Integer(10))
        ));
    }

    #[test]
    fn where_clause_arithmetic_all_ops() {
        for (sql, expected_op) in [
            ("SELECT * FROM t WHERE x = a + 2", ArithmeticOp::Add),
            ("SELECT * FROM t WHERE x = a - 2", ArithmeticOp::Subtract),
            ("SELECT * FROM t WHERE x = a * 2", ArithmeticOp::Multiply),
            ("SELECT * FROM t WHERE x = a / 2", ArithmeticOp::Divide),
            ("SELECT * FROM t WHERE x = a % 3", ArithmeticOp::Modulo),
        ] {
            let ScalarExpr::Arithmetic(arith) = where_clause_rhs(sql) else {
                panic!("expected arithmetic RHS for {sql}");
            };
            assert_eq!(arith.op, expected_op, "{sql}");
        }
    }

    #[test]
    fn where_clause_arithmetic_nested_with_parameter() {
        // PGC-118 bench query shape, with a parameter to keep arithmetic observable.
        let ScalarExpr::Arithmetic(outer) =
            where_clause_rhs("SELECT * FROM t WHERE user_id = $1 % 10000 + 1")
        else {
            panic!("expected outer arithmetic");
        };
        assert_eq!(outer.op, ArithmeticOp::Add);
        let ScalarExpr::Arithmetic(inner) = &*outer.left else {
            panic!("expected inner arithmetic on left");
        };
        assert_eq!(inner.op, ArithmeticOp::Modulo);
        assert!(matches!(
            *outer.right,
            ScalarExpr::Literal(LiteralValue::Integer(1))
        ));
    }

    #[test]
    fn where_clause_arithmetic_two_columns() {
        let ScalarExpr::Arithmetic(arith) = where_clause_rhs("SELECT * FROM t WHERE x = a + b")
        else {
            panic!("expected arithmetic RHS");
        };
        assert_eq!(arith.op, ArithmeticOp::Add);
        assert!(matches!(*arith.left, ScalarExpr::Column(ref c) if c.column == "a"));
        assert!(matches!(*arith.right, ScalarExpr::Column(ref c) if c.column == "b"));
    }

    #[test]
    fn where_clause_arithmetic_deparse() {
        // Round-trip a non-foldable arithmetic query to confirm Scalar wrapping deparses cleanly.
        let q = query_expr_convert(
            &pg_query::parse("SELECT * FROM t WHERE x = a % 10000 + 1").unwrap(),
        )
        .unwrap();
        let mut buf = String::new();
        q.deparse(&mut buf);
        // ArithmeticExpr::deparse parenthesizes each level, so nested
        // `a % 10000 + 1` round-trips as `((a % 10000) + 1)`.
        assert_eq!(buf, "SELECT * FROM t WHERE x = ((a % 10000) + 1)");
    }

    #[test]
    fn where_clause_typecast_column() {
        // PGC-120: column cast on the left of a comparison. Must parse as
        // WhereExpr::Scalar(ScalarExpr::TypeCast{...}), not UnsupportedPattern.
        let where_clause = where_clause_parse("SELECT * FROM t WHERE col::text = 'foo'")
            .unwrap()
            .unwrap();
        let WhereExpr::Binary(binary) = &where_clause else {
            panic!("expected Binary, got {where_clause:?}");
        };
        let WhereExpr::Scalar(ScalarExpr::TypeCast { expr, target }) = &*binary.lexpr else {
            panic!("expected Scalar(TypeCast), got {:?}", binary.lexpr);
        };
        assert_eq!(*target, crate::query::cast::CastTarget::Text);
        assert!(matches!(&**expr, ScalarExpr::Column(c) if c.column == "col"));
    }

    #[test]
    fn where_clause_typecast_deparse() {
        // PGC-120: round-trip a few common cast shapes through Deparse.
        for sql in [
            "SELECT * FROM t WHERE col::text = 'foo'",
            "SELECT * FROM t WHERE created_at::date = '2024-01-01'",
            "SELECT * FROM t WHERE (a + b)::int > 10",
        ] {
            let q = query_expr_convert(&pg_query::parse(sql).unwrap())
                .unwrap_or_else(|e| panic!("convert failed for {sql}: {e}"));
            let mut buf = String::new();
            q.deparse(&mut buf);
            // Re-parse the deparsed SQL to confirm semantic round-trip.
            let q2 = query_expr_convert(&pg_query::parse(&buf).unwrap())
                .unwrap_or_else(|e| panic!("re-convert failed for {buf:?}: {e}"));
            assert_eq!(
                query_expr_fingerprint(&q),
                query_expr_fingerprint(&q2),
                "fingerprint mismatch after deparse round-trip\n  in:  {sql}\n  out: {buf}",
            );
        }
    }
}
