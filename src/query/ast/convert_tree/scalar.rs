//! Scalar expressions: columns, literals, subqueries, function calls, casts,
//! CASE and arithmetic.

use ecow::EcoString;
use pg_query::pg_nodes as pg;

use super::ExpressionScope;
use super::select::query_emit;
use super::where_clause::{column_ref_emit, const_value_extract, param_ref_extract, where_emit};
use super::window::{order_by_emit, window_emit};
use crate::query::ast::raw::{
    NodePtr, aexpr_kind_name, cast, list_is_empty, list_len, list_nodes, node_tag, node_tag_name,
    string_node_value,
};
use crate::query::ast::rope::{NodeKind, TreeBuilder};
use crate::query::ast::{ArithmeticOp, AstError, Deparse, LiteralValue, SessionTerm};
use crate::query::cast::cast_target_from_canonical;
use crate::query::transform::literal_arithmetic_fold;

/// A scalar expression, with the names it resolves against in `scope`.
pub(super) unsafe fn scalar_emit(
    builder: &mut TreeBuilder<'_>,
    node: NodePtr,
    scope: ExpressionScope<'_>,
) -> Result<(), AstError> {
    unsafe {
        match node_tag(node) {
            pg::NodeTag_T_ColumnRef => {
                column_ref_emit(builder, cast::<pg::ColumnRef>(node)).map_err(AstError::from)
            }
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
            pg::NodeTag_T_SubLink => {
                scalar_subquery_emit(builder, cast::<pg::SubLink>(node), scope)
            }
            pg::NodeTag_T_FuncCall => func_call_emit(builder, cast::<pg::FuncCall>(node), scope),
            pg::NodeTag_T_SQLValueFunction => {
                let term = sql_value_function_term(cast::<pg::SQLValueFunction>(node))?;
                builder.leaf(NodeKind::Literal(LiteralValue::SessionTerm(term)));
                Ok(())
            }
            pg::NodeTag_T_CoalesceExpr => {
                let args = (*cast::<pg::CoalesceExpr>(node)).args;
                function_bare_emit(builder, "coalesce", args, scope)
            }
            pg::NodeTag_T_MinMaxExpr => minmax_emit(builder, cast::<pg::MinMaxExpr>(node), scope),
            pg::NodeTag_T_A_Expr => {
                let aexpr = cast::<pg::A_Expr>(node);
                match (*aexpr).kind {
                    pg::A_Expr_Kind_AEXPR_NULLIF => nullif_emit(builder, aexpr, scope),
                    pg::A_Expr_Kind_AEXPR_OP => arithmetic_emit(builder, aexpr, scope),
                    other => Err(AstError::UnsupportedFeature {
                        feature: format!("{} in a column expression", aexpr_kind_name(other)),
                    }),
                }
            }
            pg::NodeTag_T_CaseExpr => case_emit(builder, cast::<pg::CaseExpr>(node), scope),
            pg::NodeTag_T_TypeCast => type_cast_emit(builder, cast::<pg::TypeCast>(node), scope),
            other => Err(AstError::UnsupportedFeature {
                feature: format!("{} in a column expression", node_tag_name(other)),
            }),
        }
    }
}

unsafe fn scalar_subquery_emit(
    builder: &mut TreeBuilder<'_>,
    sub_link: *const pg::SubLink,
    scope: ExpressionScope<'_>,
) -> Result<(), AstError> {
    unsafe {
        let subselect = (*sub_link).subselect as NodePtr;
        if subselect.is_null() || node_tag(subselect) != pg::NodeTag_T_SelectStmt {
            return Err(AstError::UnsupportedFeature {
                feature: "Sublink subselect".to_owned(),
            });
        }
        let open = builder.open(NodeKind::ScalarSubquery);
        query_emit(builder, cast::<pg::SelectStmt>(subselect), scope.context)?;
        builder.close(open);
        Ok(())
    }
}

/// `GREATEST(...)` / `LEAST(...)` as the function calls they are.
unsafe fn minmax_emit(
    builder: &mut TreeBuilder<'_>,
    minmax: *const pg::MinMaxExpr,
    scope: ExpressionScope<'_>,
) -> Result<(), AstError> {
    unsafe {
        let name = match (*minmax).op {
            pg::MinMaxOp_IS_GREATEST => "greatest",
            pg::MinMaxOp_IS_LEAST => "least",
            other => {
                return Err(AstError::UnsupportedFeature {
                    feature: format!("Unknown MinMaxOp: {other}"),
                });
            }
        };
        function_bare_emit(builder, name, (*minmax).args, scope)
    }
}

unsafe fn type_cast_emit(
    builder: &mut TreeBuilder<'_>,
    tc: *const pg::TypeCast,
    scope: ExpressionScope<'_>,
) -> Result<(), AstError> {
    unsafe {
        let arg = (*tc).arg as NodePtr;
        if arg.is_null() {
            return Err(AstError::UnsupportedFeature {
                feature: "TypeCast missing argument".to_owned(),
            });
        }
        if (*tc).typeName.is_null() {
            return Err(AstError::UnsupportedFeature {
                feature: "TypeCast missing type name".to_owned(),
            });
        }
        let target_type = type_name_render((*tc).typeName)?;
        let open = builder.open(NodeKind::TypeCast {
            target: cast_target_from_canonical(&target_type),
        });
        scalar_emit(builder, arg, scope)?;
        builder.close(open);
        Ok(())
    }
}

unsafe fn type_name_render(tn: *const pg::TypeName) -> Result<EcoString, AstError> {
    unsafe {
        let mut out = type_name_parts(tn)?.join(".");
        if !list_is_empty((*tn).typmods) {
            typmods_render((*tn).typmods, &mut out)?;
        }
        for _ in list_nodes((*tn).arrayBounds) {
            out.push_str("[]");
        }
        Ok(EcoString::from(out))
    }
}

/// The type name's components, without a leading `pg_catalog` qualifier.
unsafe fn type_name_parts<'a>(tn: *const pg::TypeName) -> Result<Vec<&'a str>, AstError> {
    unsafe {
        let parts = list_nodes((*tn).names)
            .map(|n| {
                string_node_value(n).ok_or_else(|| AstError::UnsupportedFeature {
                    feature: format!("{} in a type name", node_tag_name(node_tag(n))),
                })
            })
            .collect::<Result<Vec<&str>, _>>()?;
        match parts.as_slice() {
            [] => Err(AstError::UnsupportedFeature {
                feature: "TypeName with no components".to_owned(),
            }),
            ["pg_catalog", rest @ ..] if !rest.is_empty() => Ok(rest.to_vec()),
            _ => Ok(parts),
        }
    }
}

/// Append `(m1,m2,...)` for the type modifiers.
unsafe fn typmods_render(typmods: *const pg::List, out: &mut String) -> Result<(), AstError> {
    unsafe {
        out.push('(');
        for (i, tm) in list_nodes(typmods).enumerate() {
            if node_tag(tm) != pg::NodeTag_T_A_Const {
                return Err(AstError::UnsupportedFeature {
                    feature: format!("{} as a type modifier", node_tag_name(node_tag(tm))),
                });
            }
            let lit = const_value_extract(cast::<pg::A_Const>(tm)).map_err(|_| {
                AstError::UnsupportedFeature {
                    feature: "TypeName typmod literal".to_owned(),
                }
            })?;
            if i > 0 {
                out.push(',');
            }
            lit.deparse(out);
        }
        out.push(')');
        Ok(())
    }
}

unsafe fn func_call_emit(
    builder: &mut TreeBuilder<'_>,
    func_call: *const pg::FuncCall,
    scope: ExpressionScope<'_>,
) -> Result<(), AstError> {
    unsafe {
        let mut parts = list_nodes((*func_call).funcname).filter_map(|n| string_node_value(n));
        let (schema, name) = match (parts.next(), parts.next(), parts.next()) {
            (Some(name), None, None) => (None, EcoString::from(name)),
            (Some(schema), Some(name), None) => {
                (Some(EcoString::from(schema)), EcoString::from(name))
            }
            _ => {
                return Err(AstError::UnsupportedSelectFeature {
                    feature: "function name with zero or more than two parts".to_owned(),
                });
            }
        };
        if let Some(term) = current_setting_term(func_call, schema.as_deref(), &name)? {
            builder.leaf(NodeKind::Literal(LiteralValue::SessionTerm(term)));
            return Ok(());
        }
        let agg_star = (*func_call).agg_star;
        let agg_filter = (*func_call).agg_filter as NodePtr;
        let over = (*func_call).over;
        let open = builder.open(NodeKind::Function {
            schema,
            name,
            arg_count: if agg_star {
                0
            } else {
                list_len((*func_call).args)
            },
            agg_star,
            agg_distinct: (*func_call).agg_distinct,
            order_count: list_len((*func_call).agg_order),
            has_filter: !agg_filter.is_null(),
            has_over: !over.is_null(),
        });
        if !agg_star {
            for arg in list_nodes((*func_call).args) {
                scalar_emit(builder, arg, scope)?;
            }
        }
        order_by_emit(builder, (*func_call).agg_order, scope)?;
        if !agg_filter.is_null() {
            where_emit(builder, agg_filter, scope.context)?;
        }
        if !over.is_null() {
            window_emit(builder, over, scope)?;
        }
        builder.close(open);
        Ok(())
    }
}

/// `current_user`, `current_role`, `user` and `session_user` are session
/// terms; the date-time value functions are not supported.
unsafe fn sql_value_function_term(
    svf: *const pg::SQLValueFunction,
) -> Result<SessionTerm, AstError> {
    unsafe {
        match (*svf).op {
            pg::SQLValueFunctionOp_SVFOP_CURRENT_USER
            | pg::SQLValueFunctionOp_SVFOP_CURRENT_ROLE
            | pg::SQLValueFunctionOp_SVFOP_USER => Ok(SessionTerm::CurrentUser),
            pg::SQLValueFunctionOp_SVFOP_SESSION_USER => Ok(SessionTerm::SessionUser),
            _ => Err(AstError::UnsupportedFeature {
                feature: "CURRENT_DATE-style value function".to_owned(),
            }),
        }
    }
}

/// `current_setting(<literal name>[, <literal missing_ok>])` as a session
/// term. Any other call of the name stays a function, which the
/// cacheability check rejects as non-immutable.
unsafe fn current_setting_term(
    func_call: *const pg::FuncCall,
    schema: Option<&str>,
    name: &str,
) -> Result<Option<SessionTerm>, AstError> {
    unsafe {
        if !name.eq_ignore_ascii_case("current_setting")
            || schema.is_some_and(|schema| schema != "pg_catalog")
            || (*func_call).agg_star
            || !(*func_call).over.is_null()
            || !(*func_call).agg_filter.is_null()
            || !list_is_empty((*func_call).agg_order)
        {
            return Ok(None);
        }
        let mut args = list_nodes((*func_call).args);
        let (Some(first), second, None) = (args.next(), args.next(), args.next()) else {
            return Ok(None);
        };
        let Some(LiteralValue::String(setting)) = const_literal_peeled(first)? else {
            return Ok(None);
        };
        let missing_ok = match second {
            None => false,
            Some(arg) => match const_literal_peeled(arg)? {
                Some(LiteralValue::Boolean(missing_ok)) => missing_ok,
                _ => return Ok(None),
            },
        };
        Ok(Some(SessionTerm::CurrentSetting {
            name: setting,
            missing_ok,
        }))
    }
}

/// A constant argument, through the cast `pg_get_expr` wraps it in
/// (`'app.tenant'::text`); `None` for anything else.
unsafe fn const_literal_peeled(node: NodePtr) -> Result<Option<LiteralValue>, AstError> {
    unsafe {
        let node = if node_tag(node) == pg::NodeTag_T_TypeCast {
            (*cast::<pg::TypeCast>(node)).arg as NodePtr
        } else {
            node
        };
        if node.is_null() || node_tag(node) != pg::NodeTag_T_A_Const {
            return Ok(None);
        }
        Ok(Some(const_value_extract(cast::<pg::A_Const>(node))?))
    }
}

/// A function the parser spells as its own node (`coalesce`, `greatest`,
/// `least`): a plain call of `name` over `args`.
unsafe fn function_bare_emit(
    builder: &mut TreeBuilder<'_>,
    name: &str,
    args: *const pg::List,
    scope: ExpressionScope<'_>,
) -> Result<(), AstError> {
    unsafe {
        let open = builder.open(function_bare_kind(name, list_len(args)));
        for arg in list_nodes(args) {
            scalar_emit(builder, arg, scope)?;
        }
        builder.close(open);
        Ok(())
    }
}

fn function_bare_kind(name: &str, arg_count: u32) -> NodeKind {
    NodeKind::Function {
        schema: None,
        name: EcoString::from(name),
        arg_count,
        agg_star: false,
        agg_distinct: false,
        order_count: 0,
        has_filter: false,
        has_over: false,
    }
}

unsafe fn nullif_emit(
    builder: &mut TreeBuilder<'_>,
    aexpr: *const pg::A_Expr,
    scope: ExpressionScope<'_>,
) -> Result<(), AstError> {
    unsafe {
        let operands: Vec<NodePtr> = [(*aexpr).lexpr as NodePtr, (*aexpr).rexpr as NodePtr]
            .into_iter()
            .filter(|operand| !operand.is_null())
            .collect();
        let open = builder.open(function_bare_kind(
            "nullif",
            u32::try_from(operands.len()).unwrap_or(u32::MAX),
        ));
        for operand in operands {
            scalar_emit(builder, operand, scope)?;
        }
        builder.close(open);
        Ok(())
    }
}

pub(super) unsafe fn arithmetic_emit(
    builder: &mut TreeBuilder<'_>,
    aexpr: *const pg::A_Expr,
    scope: ExpressionScope<'_>,
) -> Result<(), AstError> {
    unsafe {
        let op = arithmetic_op_extract((*aexpr).name)?;
        if (*aexpr).lexpr.is_null() {
            return Err(AstError::UnsupportedFeature {
                feature: "arithmetic expression without left operand".to_owned(),
            });
        }
        if (*aexpr).rexpr.is_null() {
            return Err(AstError::UnsupportedFeature {
                feature: "arithmetic expression without right operand".to_owned(),
            });
        }
        // Arithmetic over literals alone is one literal in the tree, so two
        // spellings of a value are one query; what does not fold (a
        // placeholder, a column, division by zero) is emitted as written.
        if let Some(folded) = literal_fold(aexpr as NodePtr)? {
            builder.leaf(NodeKind::Literal(folded));
            return Ok(());
        }
        let open = builder.open(NodeKind::Arithmetic { op });
        scalar_emit(builder, (*aexpr).lexpr, scope)?;
        scalar_emit(builder, (*aexpr).rexpr, scope)?;
        builder.close(open);
        Ok(())
    }
}

/// The literal a raw expression folds to: a constant as it is, or
/// arithmetic whose operands both fold and whose result is defined.
unsafe fn literal_fold(node: NodePtr) -> Result<Option<LiteralValue>, AstError> {
    unsafe {
        match node_tag(node) {
            pg::NodeTag_T_A_Const => Ok(Some(const_value_extract(cast::<pg::A_Const>(node))?)),
            pg::NodeTag_T_A_Expr => {
                let aexpr = cast::<pg::A_Expr>(node);
                if (*aexpr).kind != pg::A_Expr_Kind_AEXPR_OP
                    || (*aexpr).lexpr.is_null()
                    || (*aexpr).rexpr.is_null()
                {
                    return Ok(None);
                }
                let Some(op) = operator_name_single((*aexpr).name).and_then(arithmetic_op_from_str)
                else {
                    return Ok(None);
                };
                let (Some(left), Some(right)) = (
                    literal_fold((*aexpr).lexpr as NodePtr)?,
                    literal_fold((*aexpr).rexpr as NodePtr)?,
                ) else {
                    return Ok(None);
                };
                Ok(literal_arithmetic_fold(&left, op, &right))
            }
            _ => Ok(None),
        }
    }
}

pub(super) fn arithmetic_op_from_str(op: &str) -> Option<ArithmeticOp> {
    match op {
        "+" => Some(ArithmeticOp::Add),
        "-" => Some(ArithmeticOp::Subtract),
        "*" => Some(ArithmeticOp::Multiply),
        "/" => Some(ArithmeticOp::Divide),
        "%" => Some(ArithmeticOp::Modulo),
        _ => None,
    }
}

/// The single operator name from a (possibly multi-part) operator `List`, with
/// no intermediate allocation. `None` for multi-part or unparseable names.
pub(super) unsafe fn operator_name_single<'a>(name: *const pg::List) -> Option<&'a str> {
    unsafe {
        let mut it = list_nodes(name);
        match (it.next(), it.next()) {
            (Some(node), None) => string_node_value(node),
            _ => None,
        }
    }
}

unsafe fn arithmetic_op_extract(name: *const pg::List) -> Result<ArithmeticOp, AstError> {
    unsafe {
        let op = operator_name_single(name).ok_or_else(|| AstError::UnsupportedFeature {
            feature: "multi-part operator names in arithmetic".to_owned(),
        })?;
        arithmetic_op_from_str(op).ok_or_else(|| AstError::UnsupportedFeature {
            feature: format!("arithmetic operator: {op}"),
        })
    }
}

unsafe fn case_emit(
    builder: &mut TreeBuilder<'_>,
    case_expr: *const pg::CaseExpr,
    scope: ExpressionScope<'_>,
) -> Result<(), AstError> {
    unsafe {
        let arg = (*case_expr).arg as NodePtr;
        let default = (*case_expr).defresult as NodePtr;
        let open = builder.open(NodeKind::Case {
            has_arg: !arg.is_null(),
            when_count: list_len((*case_expr).args),
            has_default: !default.is_null(),
        });
        if !arg.is_null() {
            scalar_emit(builder, arg, scope)?;
        }
        for when in list_nodes((*case_expr).args) {
            case_when_emit(builder, when, scope)?;
        }
        if !default.is_null() {
            scalar_emit(builder, default, scope)?;
        }
        builder.close(open);
        Ok(())
    }
}

unsafe fn case_when_emit(
    builder: &mut TreeBuilder<'_>,
    node: NodePtr,
    scope: ExpressionScope<'_>,
) -> Result<(), AstError> {
    unsafe {
        if node_tag(node) != pg::NodeTag_T_CaseWhen {
            return Err(AstError::UnsupportedFeature {
                feature: format!("{} as a CASE arm", node_tag_name(node_tag(node))),
            });
        }
        let case_when = cast::<pg::CaseWhen>(node);
        let cond = (*case_when).expr as NodePtr;
        if cond.is_null() {
            return Err(AstError::UnsupportedFeature {
                feature: "CASE WHEN without condition".to_owned(),
            });
        }
        let open = builder.open(NodeKind::When);
        where_emit(builder, cond, scope.context)?;
        let res = (*case_when).result as NodePtr;
        if res.is_null() {
            return Err(AstError::UnsupportedFeature {
                feature: "CASE WHEN without result".to_owned(),
            });
        }
        scalar_emit(builder, res, scope)?;
        builder.close(open);
        Ok(())
    }
}
