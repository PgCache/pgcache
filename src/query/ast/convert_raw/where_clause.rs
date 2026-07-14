//! The WHERE-clause converter.
//!
//! Its own error domain: everything here fails with [`WhereParseError`], which
//! the caller folds into [`AstError`]. Keeping the boundary in one file makes
//! that split explicit — a WHERE that cannot be represented is a routing
//! decision (forward to origin), not a parse failure.

use ecow::EcoString;
use ordered_float::NotNan;

use pg_query::pg_nodes as pg;

use super::super::raw::{
    NodePtr, cast, cstr, list_is_empty, list_nodes, node_tag, string_node_value,
};
use super::super::*;
use super::{
    aexpr_arithmetic_convert, arithmetic_op_from_str, operator_name_single, scalar_expr_convert,
    select_stmt_to_query_expr, sublink_type_map,
};

// ---------- Literal / column / param extraction ----------

pub(super) unsafe fn const_value_extract(
    c: *const pg::A_Const,
) -> Result<LiteralValue, WhereParseError> {
    unsafe {
        if (*c).isnull {
            return Ok(LiteralValue::Null);
        }
        let val = &(*c).val;
        match val.node.type_ {
            pg::NodeTag_T_Integer => Ok(LiteralValue::Integer(val.ival.ival as i64)),
            pg::NodeTag_T_Float => {
                let s = cstr(val.fval.fval);
                s.parse::<f64>()
                    .ok()
                    .and_then(|v| NotNan::new(v).ok())
                    .map(LiteralValue::Float)
                    .ok_or_else(|| WhereParseError::InvalidConstValue {
                        value: s.to_owned(),
                    })
            }
            pg::NodeTag_T_Boolean => Ok(LiteralValue::Boolean(val.boolval.boolval)),
            pg::NodeTag_T_String => Ok(LiteralValue::String(cstr(val.sval.sval).into())),
            pg::NodeTag_T_BitString => Ok(LiteralValue::String(cstr(val.bsval.bsval).into())),
            _ => Ok(LiteralValue::Null),
        }
    }
}

pub(super) unsafe fn column_ref_extract(
    col_ref: *const pg::ColumnRef,
) -> Result<ColumnNode, WhereParseError> {
    unsafe {
        if list_is_empty((*col_ref).fields) {
            return Err(WhereParseError::InvalidColumnRef);
        }

        let mut table: Option<EcoString> = None;
        let mut column: Option<EcoString> = None;

        for field in list_nodes((*col_ref).fields) {
            match string_node_value(field) {
                Some(s) => {
                    if column.is_none() {
                        column = Some(EcoString::from(s));
                    } else {
                        table = column.clone();
                        column = Some(EcoString::from(s));
                    }
                }
                None => return Err(WhereParseError::InvalidColumnRef),
            }
        }

        let column = column.ok_or(WhereParseError::InvalidColumnRef)?;
        Ok(ColumnNode { table, column })
    }
}

pub(super) unsafe fn param_ref_extract(param_ref: *const pg::ParamRef) -> LiteralValue {
    unsafe { LiteralValue::Parameter(format!("${}", (*param_ref).number).into()) }
}

// ---------- WHERE clause ----------

pub(super) unsafe fn where_expr_convert(node: NodePtr) -> Result<WhereExpr, WhereParseError> {
    unsafe {
        match node_tag(node) {
            pg::NodeTag_T_A_Expr => a_expr_convert(cast::<pg::A_Expr>(node)),
            pg::NodeTag_T_BoolExpr => bool_expr_convert(cast::<pg::BoolExpr>(node)),
            pg::NodeTag_T_SubLink => sublink_convert(cast::<pg::SubLink>(node)),
            pg::NodeTag_T_NullTest => null_test_convert(cast::<pg::NullTest>(node)),
            pg::NodeTag_T_BooleanTest => boolean_test_convert(cast::<pg::BooleanTest>(node)),
            pg::NodeTag_T_ColumnRef => Ok(WhereExpr::Scalar(ScalarExpr::Column(
                column_ref_extract(cast::<pg::ColumnRef>(node))?,
            ))),
            pg::NodeTag_T_A_Const => Ok(WhereExpr::Scalar(ScalarExpr::Literal(
                const_value_extract(cast::<pg::A_Const>(node))?,
            ))),
            pg::NodeTag_T_ParamRef => Ok(WhereExpr::Scalar(ScalarExpr::Literal(
                param_ref_extract(cast::<pg::ParamRef>(node)),
            ))),
            pg::NodeTag_T_FuncCall | pg::NodeTag_T_TypeCast => {
                Ok(WhereExpr::Scalar(scalar_expr_convert(node)?))
            }
            _ => Err(WhereParseError::UnsupportedPattern),
        }
    }
}

pub(super) unsafe fn sublink_convert(
    sub_link: *const pg::SubLink,
) -> Result<WhereExpr, WhereParseError> {
    unsafe {
        let subselect = (*sub_link).subselect as NodePtr;
        let query = if !subselect.is_null() && node_tag(subselect) == pg::NodeTag_T_SelectStmt {
            select_stmt_to_query_expr(cast::<pg::SelectStmt>(subselect))?
        } else {
            return Err(WhereParseError::Other {
                error: "SubLink missing or invalid subselect".to_owned(),
            });
        };

        let test_expr = match ((*sub_link).testexpr as NodePtr).is_null() {
            true => None,
            false => Some(Box::new(scalar_expr_convert((*sub_link).testexpr)?)),
        };

        let sublink_type = sublink_type_map((*sub_link).subLinkType)?;

        if sublink_type == SubLinkType::All {
            sublink_all_operator_check((*sub_link).operName)?;
        }

        Ok(WhereExpr::Subquery {
            query: Box::new(query),
            sublink_type,
            test_expr,
        })
    }
}

pub(super) unsafe fn operator_name_string_extract<'a>(
    oper_name: *const pg::List,
    context: &str,
) -> Result<&'a str, WhereParseError> {
    unsafe {
        let names: Vec<_> = list_nodes(oper_name).collect();
        let [name_node] = names.as_slice() else {
            return Err(WhereParseError::Other {
                error: format!("{context}: expected single name node"),
            });
        };
        string_node_value(*name_node).ok_or_else(|| WhereParseError::Other {
            error: format!("{context}: expected string node"),
        })
    }
}

pub(super) unsafe fn sublink_all_operator_check(
    oper_name: *const pg::List,
) -> Result<(), WhereParseError> {
    unsafe {
        let op = operator_name_string_extract(oper_name, "ALL operator")?;
        if op == "<>" {
            Ok(())
        } else {
            Err(WhereParseError::UnsupportedOperator {
                operator: format!("ALL with operator '{op}'"),
            })
        }
    }
}

pub(super) unsafe fn null_test_convert(
    null_test: *const pg::NullTest,
) -> Result<WhereExpr, WhereParseError> {
    unsafe {
        let arg = (*null_test).arg as NodePtr;
        if arg.is_null() {
            return Err(WhereParseError::MissingExpression);
        }
        let op = match (*null_test).nulltesttype {
            pg::NullTestType_IS_NULL => UnaryOp::IsNull,
            pg::NullTestType_IS_NOT_NULL => UnaryOp::IsNotNull,
            other => {
                return Err(WhereParseError::UnsupportedAExpr {
                    expr: format!("NullTest type {other}"),
                });
            }
        };
        Ok(WhereExpr::Unary(UnaryExpr {
            op,
            expr: Box::new(where_expr_convert(arg)?),
        }))
    }
}

pub(super) unsafe fn boolean_test_convert(
    bool_test: *const pg::BooleanTest,
) -> Result<WhereExpr, WhereParseError> {
    unsafe {
        let arg = (*bool_test).arg as NodePtr;
        if arg.is_null() {
            return Err(WhereParseError::MissingExpression);
        }
        let op = match (*bool_test).booltesttype {
            pg::BoolTestType_IS_TRUE => UnaryOp::IsTrue,
            pg::BoolTestType_IS_NOT_TRUE => UnaryOp::IsNotTrue,
            pg::BoolTestType_IS_FALSE => UnaryOp::IsFalse,
            pg::BoolTestType_IS_NOT_FALSE => UnaryOp::IsNotFalse,
            pg::BoolTestType_IS_UNKNOWN => UnaryOp::IsNull,
            pg::BoolTestType_IS_NOT_UNKNOWN => UnaryOp::IsNotNull,
            other => {
                return Err(WhereParseError::UnsupportedAExpr {
                    expr: format!("BooleanTest type {other}"),
                });
            }
        };
        Ok(WhereExpr::Unary(UnaryExpr {
            op,
            expr: Box::new(where_expr_convert(arg)?),
        }))
    }
}

pub(super) unsafe fn a_expr_convert(expr: *const pg::A_Expr) -> Result<WhereExpr, WhereParseError> {
    unsafe {
        let kind = (*expr).kind;
        let name = (*expr).name;
        let lexpr = (*expr).lexpr as NodePtr;
        let rexpr = (*expr).rexpr as NodePtr;

        match kind {
            pg::A_Expr_Kind_AEXPR_OP => {
                // Extract the operator name once and classify it, rather than
                // speculatively running (and discarding) the arithmetic path.
                let op_name = operator_name_single(name).ok_or_else(|| WhereParseError::Other {
                    error: "Multi-part operator names not supported".to_owned(),
                })?;
                if arithmetic_op_from_str(op_name).is_some() {
                    let arith = aexpr_arithmetic_convert(expr)?;
                    return Ok(WhereExpr::Scalar(ScalarExpr::Arithmetic(arith)));
                }
                let op = binary_op_from_str(op_name).ok_or_else(|| {
                    WhereParseError::UnsupportedOperator {
                        operator: op_name.to_owned(),
                    }
                })?;
                if lexpr.is_null() || rexpr.is_null() {
                    return Err(WhereParseError::MissingExpression);
                }
                Ok(WhereExpr::Binary(BinaryExpr {
                    op,
                    lexpr: Box::new(where_expr_convert(lexpr)?),
                    rexpr: Box::new(where_expr_convert(rexpr)?),
                }))
            }
            pg::A_Expr_Kind_AEXPR_IN => {
                let op = in_operator_extract(name)?;
                if lexpr.is_null() || rexpr.is_null() {
                    return Err(WhereParseError::MissingExpression);
                }
                let left_expr = where_expr_convert(lexpr)?;
                let values = in_list_extract(rexpr)?;
                let mut exprs = vec![left_expr];
                exprs.extend(values);
                Ok(WhereExpr::Multi(MultiExpr { op, exprs }))
            }
            pg::A_Expr_Kind_AEXPR_BETWEEN
            | pg::A_Expr_Kind_AEXPR_NOT_BETWEEN
            | pg::A_Expr_Kind_AEXPR_BETWEEN_SYM
            | pg::A_Expr_Kind_AEXPR_NOT_BETWEEN_SYM => {
                let op = match kind {
                    pg::A_Expr_Kind_AEXPR_BETWEEN => MultiOp::Between,
                    pg::A_Expr_Kind_AEXPR_NOT_BETWEEN => MultiOp::NotBetween,
                    pg::A_Expr_Kind_AEXPR_BETWEEN_SYM => MultiOp::BetweenSymmetric,
                    _ => MultiOp::NotBetweenSymmetric,
                };
                if lexpr.is_null() || rexpr.is_null() {
                    return Err(WhereParseError::MissingExpression);
                }
                let left_expr = where_expr_convert(lexpr)?;
                let bounds = between_bounds_extract(rexpr)?;
                Ok(WhereExpr::Multi(MultiExpr {
                    op,
                    exprs: vec![left_expr, bounds.0, bounds.1],
                }))
            }
            pg::A_Expr_Kind_AEXPR_LIKE | pg::A_Expr_Kind_AEXPR_ILIKE => {
                let op = like_operator_extract(name)?;
                if lexpr.is_null() || rexpr.is_null() {
                    return Err(WhereParseError::MissingExpression);
                }
                Ok(WhereExpr::Binary(BinaryExpr {
                    op,
                    lexpr: Box::new(where_expr_convert(lexpr)?),
                    rexpr: Box::new(where_expr_convert(rexpr)?),
                }))
            }
            pg::A_Expr_Kind_AEXPR_OP_ANY | pg::A_Expr_Kind_AEXPR_OP_ALL => {
                let comparison = operator_extract(name)?;
                let op = match kind {
                    pg::A_Expr_Kind_AEXPR_OP_ANY => MultiOp::Any { comparison },
                    _ => MultiOp::All { comparison },
                };
                if lexpr.is_null() || rexpr.is_null() {
                    return Err(WhereParseError::MissingExpression);
                }
                let left_expr = where_expr_convert(lexpr)?;
                let right_expr = any_all_rexpr_convert(rexpr)?;
                Ok(WhereExpr::Multi(MultiExpr {
                    op,
                    exprs: vec![left_expr, right_expr],
                }))
            }
            other => Err(WhereParseError::UnsupportedAExpr {
                expr: format!("A_Expr_Kind {other}"),
            }),
        }
    }
}

pub(super) unsafe fn in_operator_extract(
    name: *const pg::List,
) -> Result<MultiOp, WhereParseError> {
    unsafe {
        match operator_name_string_extract(name, "IN operator")? {
            "=" => Ok(MultiOp::In),
            "<>" => Ok(MultiOp::NotIn),
            other => Err(WhereParseError::UnsupportedOperator {
                operator: format!("IN with operator '{other}'"),
            }),
        }
    }
}

pub(super) unsafe fn in_list_extract(node: NodePtr) -> Result<Vec<WhereExpr>, WhereParseError> {
    unsafe {
        if node_tag(node) != pg::NodeTag_T_List {
            return Err(WhereParseError::Other {
                error: "IN clause: expected List on right side".to_owned(),
            });
        }
        list_nodes(node as *const pg::List)
            .map(|n| where_expr_convert(n))
            .collect()
    }
}

pub(super) unsafe fn between_bounds_extract(
    node: NodePtr,
) -> Result<(WhereExpr, WhereExpr), WhereParseError> {
    unsafe {
        if node_tag(node) != pg::NodeTag_T_List {
            return Err(WhereParseError::Other {
                error: "BETWEEN clause: expected List on right side".to_owned(),
            });
        }
        let items: Vec<_> = list_nodes(node as *const pg::List).collect();
        let [low, high] = items.as_slice() else {
            return Err(WhereParseError::Other {
                error: format!(
                    "BETWEEN clause: expected exactly 2 bounds, got {}",
                    items.len()
                ),
            });
        };
        Ok((where_expr_convert(*low)?, where_expr_convert(*high)?))
    }
}

pub(super) unsafe fn any_all_rexpr_convert(node: NodePtr) -> Result<WhereExpr, WhereParseError> {
    unsafe {
        if node_tag(node) == pg::NodeTag_T_A_ArrayExpr {
            let elems = list_nodes((*cast::<pg::A_ArrayExpr>(node)).elements)
                .map(|n| scalar_expr_convert(n))
                .collect::<Result<Vec<_>, AstError>>()?;
            Ok(WhereExpr::Scalar(ScalarExpr::Array(elems)))
        } else {
            where_expr_convert(node)
        }
    }
}

pub(super) unsafe fn like_operator_extract(
    name: *const pg::List,
) -> Result<BinaryOp, WhereParseError> {
    unsafe {
        match operator_name_string_extract(name, "LIKE operator")? {
            "~~" => Ok(BinaryOp::Like),
            "!~~" => Ok(BinaryOp::NotLike),
            "~~*" => Ok(BinaryOp::ILike),
            "!~~*" => Ok(BinaryOp::NotILike),
            other => Err(WhereParseError::UnsupportedOperator {
                operator: format!("LIKE with operator '{other}'"),
            }),
        }
    }
}

pub(super) fn binary_op_from_str(op: &str) -> Option<BinaryOp> {
    match op {
        "=" => Some(BinaryOp::Equal),
        "!=" | "<>" => Some(BinaryOp::NotEqual),
        "<" => Some(BinaryOp::LessThan),
        "<=" => Some(BinaryOp::LessThanOrEqual),
        ">" => Some(BinaryOp::GreaterThan),
        ">=" => Some(BinaryOp::GreaterThanOrEqual),
        _ => None,
    }
}

pub(super) unsafe fn operator_extract(name: *const pg::List) -> Result<BinaryOp, WhereParseError> {
    unsafe {
        let op = operator_name_single(name).ok_or_else(|| WhereParseError::Other {
            error: "Multi-part operator names not supported".to_owned(),
        })?;
        binary_op_from_str(op).ok_or_else(|| WhereParseError::UnsupportedOperator {
            operator: op.to_owned(),
        })
    }
}

pub(super) unsafe fn bool_expr_convert(
    expr: *const pg::BoolExpr,
) -> Result<WhereExpr, WhereParseError> {
    unsafe {
        let args: Vec<_> = list_nodes((*expr).args).collect();
        match (*expr).boolop {
            pg::BoolExprType_AND_EXPR | pg::BoolExprType_OR_EXPR => {
                let op = if (*expr).boolop == pg::BoolExprType_AND_EXPR {
                    BinaryOp::And
                } else {
                    BinaryOp::Or
                };
                let [first, second, rest @ ..] = args.as_slice() else {
                    return Err(WhereParseError::Other {
                        error: "boolean expression with < 2 arguments not supported".to_owned(),
                    });
                };
                let mut result = WhereExpr::Binary(BinaryExpr {
                    op,
                    lexpr: Box::new(where_expr_convert(*first)?),
                    rexpr: Box::new(where_expr_convert(*second)?),
                });
                for arg in rest {
                    result = WhereExpr::Binary(BinaryExpr {
                        op,
                        lexpr: Box::new(result),
                        rexpr: Box::new(where_expr_convert(*arg)?),
                    });
                }
                Ok(result)
            }
            pg::BoolExprType_NOT_EXPR => {
                let [arg] = args.as_slice() else {
                    return Err(WhereParseError::Other {
                        error: "NOT with != 1 argument not supported".to_owned(),
                    });
                };
                Ok(WhereExpr::Unary(UnaryExpr {
                    op: UnaryOp::Not,
                    expr: Box::new(where_expr_convert(*arg)?),
                }))
            }
            other => Err(WhereParseError::Other {
                error: format!("boolean expression type {other}"),
            }),
        }
    }
}
