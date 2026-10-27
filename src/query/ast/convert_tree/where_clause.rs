//! The WHERE-clause converter.
//!
//! Its own error domain: everything here fails with [`WhereParseError`], which
//! the caller folds into [`AstError`]. A WHERE that cannot be represented is
//! a routing decision (forward to origin), not a parse failure.

use ecow::EcoString;
use ordered_float::NotNan;
use pg_query::pg_nodes as pg;

use super::scalar::{arithmetic_emit, arithmetic_op_from_str, operator_name_single, scalar_emit};
use super::select::query_emit;
use super::sublink_type_map;
use super::{ExpressionScope, ParseContext};
use crate::query::ast::raw::{
    NodePtr, aexpr_kind_name, cast, cstr, list_is_empty, list_len, list_nodes, node_tag,
    string_node_value,
};
use crate::query::ast::rope::{NodeKind, TreeBuilder};
use crate::query::ast::{
    AstError, BinaryOp, LiteralValue, MultiOp, SubLinkType, UnaryOp, WhereParseError,
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

/// The `(table, column)` a column reference names.
pub(super) unsafe fn column_ref_extract(
    col_ref: *const pg::ColumnRef,
) -> Result<(Option<EcoString>, EcoString), WhereParseError> {
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
        Ok((table, column))
    }
}

pub(super) unsafe fn column_ref_emit(
    builder: &mut TreeBuilder<'_>,
    col_ref: *const pg::ColumnRef,
) -> Result<(), WhereParseError> {
    unsafe {
        let (table, column) = column_ref_extract(col_ref)?;
        builder.leaf(NodeKind::Column { table, column });
        Ok(())
    }
}

pub(super) unsafe fn param_ref_extract(param_ref: *const pg::ParamRef) -> LiteralValue {
    unsafe { LiteralValue::Parameter(format!("${}", (*param_ref).number).into()) }
}

// ---------- WHERE clause ----------

/// A predicate, in the WHERE error domain folded into `AstError`.
/// A predicate over the CTEs of `context`; no window reference resolves
/// inside one.
pub(super) unsafe fn where_emit(
    builder: &mut TreeBuilder<'_>,
    node: NodePtr,
    context: &ParseContext<'_>,
) -> Result<(), AstError> {
    unsafe { where_expr_emit(builder, node, ExpressionScope::of(context)).map_err(AstError::from) }
}

pub(super) unsafe fn where_expr_emit(
    builder: &mut TreeBuilder<'_>,
    node: NodePtr,
    scope: ExpressionScope<'_>,
) -> Result<(), WhereParseError> {
    unsafe {
        match node_tag(node) {
            pg::NodeTag_T_A_Expr => a_expr_emit(builder, cast::<pg::A_Expr>(node), scope),
            pg::NodeTag_T_BoolExpr => bool_expr_emit(builder, cast::<pg::BoolExpr>(node), scope),
            pg::NodeTag_T_SubLink => sublink_emit(builder, cast::<pg::SubLink>(node), scope),
            pg::NodeTag_T_NullTest => null_test_emit(builder, cast::<pg::NullTest>(node), scope),
            pg::NodeTag_T_BooleanTest => {
                boolean_test_emit(builder, cast::<pg::BooleanTest>(node), scope)
            }
            pg::NodeTag_T_ColumnRef => column_ref_emit(builder, cast::<pg::ColumnRef>(node)),
            pg::NodeTag_T_A_Const => {
                let value = const_value_extract(cast::<pg::A_Const>(node))?;
                builder.leaf(NodeKind::Literal(value));
                Ok(())
            }
            pg::NodeTag_T_ParamRef => {
                builder.leaf(NodeKind::Literal(param_ref_extract(cast::<pg::ParamRef>(
                    node,
                ))));
                Ok(())
            }
            pg::NodeTag_T_FuncCall | pg::NodeTag_T_TypeCast | pg::NodeTag_T_SQLValueFunction => {
                scalar_emit(builder, node, scope).map_err(WhereParseError::from)
            }
            _ => Err(WhereParseError::UnsupportedPattern),
        }
    }
}

unsafe fn sublink_emit(
    builder: &mut TreeBuilder<'_>,
    sub_link: *const pg::SubLink,
    scope: ExpressionScope<'_>,
) -> Result<(), WhereParseError> {
    unsafe {
        let subselect = (*sub_link).subselect as NodePtr;
        if subselect.is_null() || node_tag(subselect) != pg::NodeTag_T_SelectStmt {
            return Err(WhereParseError::Other {
                error: "SubLink missing or invalid subselect".to_owned(),
            });
        }
        let test_expr = (*sub_link).testexpr as NodePtr;
        let sublink_type = sublink_type_map((*sub_link).subLinkType)?;
        match sublink_type {
            SubLinkType::All => sublink_all_operator_check((*sub_link).operName)?,
            SubLinkType::Any => sublink_any_operator_check((*sub_link).operName)?,
            SubLinkType::Exists | SubLinkType::Expr => {}
        }
        let open = builder.open(NodeKind::WhereSubquery {
            sublink_type,
            has_test_expr: !test_expr.is_null(),
        });
        query_emit(builder, cast::<pg::SelectStmt>(subselect), scope.context)?;
        if !test_expr.is_null() {
            scalar_emit(builder, test_expr, scope)?;
        }
        builder.close(open);
        Ok(())
    }
}

unsafe fn operator_name_string_extract<'a>(
    oper_name: *const pg::List,
    construct: &str,
) -> Result<&'a str, WhereParseError> {
    unsafe {
        let mut names = list_nodes(oper_name);
        let (Some(name_node), None) = (names.next(), names.next()) else {
            return Err(WhereParseError::Other {
                error: format!("{construct} operator: expected single name node"),
            });
        };
        string_node_value(name_node).ok_or_else(|| WhereParseError::Other {
            error: format!("{construct} operator: expected string node"),
        })
    }
}

/// Map the single operator name of a `construct` (IN, LIKE, ANY, ALL) with
/// `map`, failing with the unsupported operator.
unsafe fn operator_map<T>(
    oper_name: *const pg::List,
    construct: &str,
    map: impl FnOnce(&str) -> Option<T>,
) -> Result<T, WhereParseError> {
    unsafe {
        let op = operator_name_string_extract(oper_name, construct)?;
        map(op).ok_or_else(|| WhereParseError::UnsupportedOperator {
            operator: format!("{construct} with operator '{op}'"),
        })
    }
}

unsafe fn sublink_all_operator_check(oper_name: *const pg::List) -> Result<(), WhereParseError> {
    unsafe { operator_map(oper_name, "ALL", |op| (op == "<>").then_some(())) }
}

/// An ANY sublink is only IN-equivalent for `=`: plain `IN`/`NOT IN`
/// parse with a NIL operName, explicit `= ANY (SELECT ...)` with `=`.
/// Any other operator (`> ANY`, ...) is a quantified range comparison
/// the downstream equality semi-join would silently mis-evaluate, so
/// reject it — the query forwards to origin (PGC-361).
unsafe fn sublink_any_operator_check(oper_name: *const pg::List) -> Result<(), WhereParseError> {
    unsafe {
        if list_is_empty(oper_name) {
            return Ok(());
        }
        operator_map(oper_name, "ANY", |op| (op == "=").then_some(()))
    }
}

unsafe fn unary_emit(
    builder: &mut TreeBuilder<'_>,
    op: UnaryOp,
    arg: NodePtr,
    scope: ExpressionScope<'_>,
) -> Result<(), WhereParseError> {
    unsafe {
        let open = builder.open(NodeKind::Unary { op });
        where_expr_emit(builder, arg, scope)?;
        builder.close(open);
        Ok(())
    }
}

unsafe fn null_test_emit(
    builder: &mut TreeBuilder<'_>,
    null_test: *const pg::NullTest,
    scope: ExpressionScope<'_>,
) -> Result<(), WhereParseError> {
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
        unary_emit(builder, op, arg, scope)
    }
}

unsafe fn boolean_test_emit(
    builder: &mut TreeBuilder<'_>,
    bool_test: *const pg::BooleanTest,
    scope: ExpressionScope<'_>,
) -> Result<(), WhereParseError> {
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
        unary_emit(builder, op, arg, scope)
    }
}

unsafe fn a_expr_emit(
    builder: &mut TreeBuilder<'_>,
    expr: *const pg::A_Expr,
    scope: ExpressionScope<'_>,
) -> Result<(), WhereParseError> {
    unsafe {
        match (*expr).kind {
            pg::A_Expr_Kind_AEXPR_OP => a_expr_operator_emit(builder, expr, scope),
            pg::A_Expr_Kind_AEXPR_IN => a_expr_in_emit(builder, expr, scope),
            pg::A_Expr_Kind_AEXPR_BETWEEN => {
                a_expr_between_emit(builder, expr, MultiOp::Between, scope)
            }
            pg::A_Expr_Kind_AEXPR_NOT_BETWEEN => {
                a_expr_between_emit(builder, expr, MultiOp::NotBetween, scope)
            }
            pg::A_Expr_Kind_AEXPR_BETWEEN_SYM => {
                a_expr_between_emit(builder, expr, MultiOp::BetweenSymmetric, scope)
            }
            pg::A_Expr_Kind_AEXPR_NOT_BETWEEN_SYM => {
                a_expr_between_emit(builder, expr, MultiOp::NotBetweenSymmetric, scope)
            }
            pg::A_Expr_Kind_AEXPR_LIKE | pg::A_Expr_Kind_AEXPR_ILIKE => {
                let op = like_operator_extract((*expr).name)?;
                binary_emit(builder, expr, op, scope)
            }
            pg::A_Expr_Kind_AEXPR_OP_ANY => {
                let comparison = operator_extract((*expr).name)?;
                a_expr_any_all_emit(builder, expr, MultiOp::Any { comparison }, scope)
            }
            pg::A_Expr_Kind_AEXPR_OP_ALL => {
                let comparison = operator_extract((*expr).name)?;
                a_expr_any_all_emit(builder, expr, MultiOp::All { comparison }, scope)
            }
            other => Err(WhereParseError::UnsupportedAExpr {
                expr: format!("{} in WHERE", aexpr_kind_name(other)),
            }),
        }
    }
}

/// Both operands of an operator expression, or `MissingExpression`.
unsafe fn a_expr_operands(expr: *const pg::A_Expr) -> Result<(NodePtr, NodePtr), WhereParseError> {
    unsafe {
        let lexpr = (*expr).lexpr as NodePtr;
        let rexpr = (*expr).rexpr as NodePtr;
        if lexpr.is_null() || rexpr.is_null() {
            return Err(WhereParseError::MissingExpression);
        }
        Ok((lexpr, rexpr))
    }
}

/// `lexpr op rexpr`, both sides as WHERE expressions.
unsafe fn binary_emit(
    builder: &mut TreeBuilder<'_>,
    expr: *const pg::A_Expr,
    op: BinaryOp,
    scope: ExpressionScope<'_>,
) -> Result<(), WhereParseError> {
    unsafe {
        let (lexpr, rexpr) = a_expr_operands(expr)?;
        let open = builder.open(NodeKind::Binary { op });
        where_expr_emit(builder, lexpr, scope)?;
        where_expr_emit(builder, rexpr, scope)?;
        builder.close(open);
        Ok(())
    }
}

/// A plain operator: arithmetic yields a scalar, anything else a comparison.
unsafe fn a_expr_operator_emit(
    builder: &mut TreeBuilder<'_>,
    expr: *const pg::A_Expr,
    scope: ExpressionScope<'_>,
) -> Result<(), WhereParseError> {
    unsafe {
        let op_name = operator_name_single((*expr).name).ok_or_else(|| WhereParseError::Other {
            error: "Multi-part operator names not supported".to_owned(),
        })?;
        if arithmetic_op_from_str(op_name).is_some() {
            return arithmetic_emit(builder, expr, scope).map_err(WhereParseError::from);
        }
        let op =
            binary_op_from_str(op_name).ok_or_else(|| WhereParseError::UnsupportedOperator {
                operator: op_name.to_owned(),
            })?;
        binary_emit(builder, expr, op, scope)
    }
}

unsafe fn a_expr_in_emit(
    builder: &mut TreeBuilder<'_>,
    expr: *const pg::A_Expr,
    scope: ExpressionScope<'_>,
) -> Result<(), WhereParseError> {
    unsafe {
        let op = in_operator_extract((*expr).name)?;
        let (lexpr, rexpr) = a_expr_operands(expr)?;
        // The subject converts before the list is checked, so its error wins.
        let list_ok = node_tag(rexpr) == pg::NodeTag_T_List;
        let open = builder.open(NodeKind::Multi {
            op,
            count: 1 + if list_ok {
                list_len(rexpr as *const pg::List)
            } else {
                0
            },
        });
        where_expr_emit(builder, lexpr, scope)?;
        if !list_ok {
            return Err(WhereParseError::Other {
                error: "IN clause: expected List on right side".to_owned(),
            });
        }
        for value in list_nodes(rexpr as *const pg::List) {
            where_expr_emit(builder, value, scope)?;
        }
        builder.close(open);
        Ok(())
    }
}

unsafe fn a_expr_between_emit(
    builder: &mut TreeBuilder<'_>,
    expr: *const pg::A_Expr,
    op: MultiOp,
    scope: ExpressionScope<'_>,
) -> Result<(), WhereParseError> {
    unsafe {
        let (lexpr, rexpr) = a_expr_operands(expr)?;
        let open = builder.open(NodeKind::Multi { op, count: 3 });
        where_expr_emit(builder, lexpr, scope)?;
        if node_tag(rexpr) != pg::NodeTag_T_List {
            return Err(WhereParseError::Other {
                error: "BETWEEN clause: expected List on right side".to_owned(),
            });
        }
        let bounds = rexpr as *const pg::List;
        let mut items = list_nodes(bounds);
        let (Some(low), Some(high), None) = (items.next(), items.next(), items.next()) else {
            return Err(WhereParseError::Other {
                error: format!(
                    "BETWEEN clause: expected exactly 2 bounds, got {}",
                    list_len(bounds)
                ),
            });
        };
        where_expr_emit(builder, low, scope)?;
        where_expr_emit(builder, high, scope)?;
        builder.close(open);
        Ok(())
    }
}

unsafe fn a_expr_any_all_emit(
    builder: &mut TreeBuilder<'_>,
    expr: *const pg::A_Expr,
    op: MultiOp,
    scope: ExpressionScope<'_>,
) -> Result<(), WhereParseError> {
    unsafe {
        let (lexpr, rexpr) = a_expr_operands(expr)?;
        let open = builder.open(NodeKind::Multi { op, count: 2 });
        where_expr_emit(builder, lexpr, scope)?;
        if node_tag(rexpr) == pg::NodeTag_T_A_ArrayExpr {
            let elements = (*cast::<pg::A_ArrayExpr>(rexpr)).elements;
            let array = builder.open(NodeKind::Array {
                count: list_len(elements),
            });
            for element in list_nodes(elements) {
                scalar_emit(builder, element, scope)?;
            }
            builder.close(array);
        } else {
            where_expr_emit(builder, rexpr, scope)?;
        }
        builder.close(open);
        Ok(())
    }
}

unsafe fn in_operator_extract(name: *const pg::List) -> Result<MultiOp, WhereParseError> {
    unsafe {
        operator_map(name, "IN", |op| match op {
            "=" => Some(MultiOp::In),
            "<>" => Some(MultiOp::NotIn),
            _ => None,
        })
    }
}

unsafe fn like_operator_extract(name: *const pg::List) -> Result<BinaryOp, WhereParseError> {
    unsafe {
        operator_map(name, "LIKE", |op| match op {
            "~~" => Some(BinaryOp::Like),
            "!~~" => Some(BinaryOp::NotLike),
            "~~*" => Some(BinaryOp::ILike),
            "!~~*" => Some(BinaryOp::NotILike),
            _ => None,
        })
    }
}

fn binary_op_from_str(op: &str) -> Option<BinaryOp> {
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

unsafe fn operator_extract(name: *const pg::List) -> Result<BinaryOp, WhereParseError> {
    unsafe {
        let op = operator_name_single(name).ok_or_else(|| WhereParseError::Other {
            error: "Multi-part operator names not supported".to_owned(),
        })?;
        binary_op_from_str(op).ok_or_else(|| WhereParseError::UnsupportedOperator {
            operator: op.to_owned(),
        })
    }
}

unsafe fn bool_expr_emit(
    builder: &mut TreeBuilder<'_>,
    expr: *const pg::BoolExpr,
    scope: ExpressionScope<'_>,
) -> Result<(), WhereParseError> {
    unsafe {
        let args: Vec<_> = list_nodes((*expr).args).collect();
        match (*expr).boolop {
            pg::BoolExprType_AND_EXPR | pg::BoolExprType_OR_EXPR => {
                let op = if (*expr).boolop == pg::BoolExprType_AND_EXPR {
                    BinaryOp::And
                } else {
                    BinaryOp::Or
                };
                if args.len() < 2 {
                    return Err(WhereParseError::Other {
                        error: "boolean expression with < 2 arguments not supported".to_owned(),
                    });
                }
                bool_chain_emit(builder, op, &args, scope)
            }
            pg::BoolExprType_NOT_EXPR => {
                let [arg] = args.as_slice() else {
                    return Err(WhereParseError::Other {
                        error: "NOT with != 1 argument not supported".to_owned(),
                    });
                };
                unary_emit(builder, UnaryOp::Not, *arg, scope)
            }
            other => Err(WhereParseError::Other {
                error: format!("boolean expression type {other}"),
            }),
        }
    }
}

/// `args` as a left-associated chain of `op`: how pg_query's n-ary boolean
/// expression nests.
unsafe fn bool_chain_emit(
    builder: &mut TreeBuilder<'_>,
    op: BinaryOp,
    args: &[NodePtr],
    scope: ExpressionScope<'_>,
) -> Result<(), WhereParseError> {
    unsafe {
        let Some((last, init)) = args.split_last() else {
            return Ok(());
        };
        let open = builder.open(NodeKind::Binary { op });
        if let [first] = init {
            where_expr_emit(builder, *first, scope)?;
        } else {
            bool_chain_emit(builder, op, init, scope)?;
        }
        where_expr_emit(builder, *last, scope)?;
        builder.close(open);
        Ok(())
    }
}
