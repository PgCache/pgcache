//! Scalar expressions: columns, literals, subqueries, function calls, casts,
//! CASE and arithmetic.

use ecow::EcoString;
use pg_query::pg_nodes as pg;

use super::select::select_stmt_to_query_expr;
use super::where_clause::{
    column_ref_extract, const_value_extract, param_ref_extract, where_expr_convert,
};
use super::window::{window_def_convert, window_order_by_convert};
use crate::query::ast::raw::{
    NodePtr, aexpr_kind_name, cast, list_is_empty, list_nodes, node_tag, node_tag_name,
    string_node_value,
};
use crate::query::ast::{
    ArithmeticExpr, ArithmeticOp, AstError, CaseExpr, CaseWhen, Deparse, FunctionCall, ScalarExpr,
};
use crate::query::cast::cast_target_from_canonical;

pub(super) unsafe fn scalar_expr_convert(node: NodePtr) -> Result<ScalarExpr, AstError> {
    unsafe {
        match node_tag(node) {
            pg::NodeTag_T_ColumnRef => {
                Ok(ScalarExpr::Column(column_ref_extract(
                    cast::<pg::ColumnRef>(node),
                )?))
            }
            pg::NodeTag_T_A_Const => {
                Ok(ScalarExpr::Literal(const_value_extract(
                    cast::<pg::A_Const>(node),
                )?))
            }
            pg::NodeTag_T_ParamRef => {
                Ok(ScalarExpr::Literal(param_ref_extract(
                    cast::<pg::ParamRef>(node),
                )))
            }
            pg::NodeTag_T_SubLink => {
                let sub_link = cast::<pg::SubLink>(node);
                let subselect = (*sub_link).subselect as NodePtr;
                if subselect.is_null() || node_tag(subselect) != pg::NodeTag_T_SelectStmt {
                    return Err(AstError::UnsupportedFeature {
                        feature: "Sublink subselect".to_owned(),
                    });
                }
                let query = select_stmt_to_query_expr(cast::<pg::SelectStmt>(subselect))?;
                Ok(ScalarExpr::Subquery(Box::new(query)))
            }
            pg::NodeTag_T_FuncCall => {
                Ok(ScalarExpr::Function(func_call_convert(
                    cast::<pg::FuncCall>(node),
                )?))
            }
            pg::NodeTag_T_CoalesceExpr => Ok(ScalarExpr::Function(coalesce_expr_convert(cast::<
                pg::CoalesceExpr,
            >(
                node
            ))?)),
            pg::NodeTag_T_MinMaxExpr => Ok(ScalarExpr::Function(minmax_expr_convert(cast::<
                pg::MinMaxExpr,
            >(
                node
            ))?)),
            pg::NodeTag_T_A_Expr => {
                let aexpr = cast::<pg::A_Expr>(node);
                match (*aexpr).kind {
                    pg::A_Expr_Kind_AEXPR_NULLIF => {
                        Ok(ScalarExpr::Function(aexpr_nullif_convert(aexpr)?))
                    }
                    pg::A_Expr_Kind_AEXPR_OP => {
                        Ok(ScalarExpr::Arithmetic(aexpr_arithmetic_convert(aexpr)?))
                    }
                    other => Err(AstError::UnsupportedFeature {
                        feature: format!("{} in a column expression", aexpr_kind_name(other)),
                    }),
                }
            }
            pg::NodeTag_T_CaseExpr => Ok(ScalarExpr::Case(case_expr_convert(
                cast::<pg::CaseExpr>(node),
            )?)),
            pg::NodeTag_T_TypeCast => type_cast_convert(cast::<pg::TypeCast>(node)),
            other => Err(AstError::UnsupportedFeature {
                feature: format!("{} in a column expression", node_tag_name(other)),
            }),
        }
    }
}

unsafe fn type_cast_convert(tc: *const pg::TypeCast) -> Result<ScalarExpr, AstError> {
    unsafe {
        let arg = (*tc).arg as NodePtr;
        if arg.is_null() {
            return Err(AstError::UnsupportedFeature {
                feature: "TypeCast missing argument".to_owned(),
            });
        }
        let inner = scalar_expr_convert(arg)?;
        if (*tc).typeName.is_null() {
            return Err(AstError::UnsupportedFeature {
                feature: "TypeCast missing type name".to_owned(),
            });
        }
        let target_type = type_name_render((*tc).typeName)?;
        let target = cast_target_from_canonical(&target_type);
        Ok(ScalarExpr::TypeCast {
            expr: Box::new(inner),
            target,
        })
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

unsafe fn func_call_convert(func_call: *const pg::FuncCall) -> Result<FunctionCall, AstError> {
    unsafe {
        let name = list_nodes((*func_call).funcname)
            .filter_map(|n| string_node_value(n))
            .next_back()
            .map(EcoString::from)
            .ok_or_else(|| AstError::UnsupportedSelectFeature {
                feature: "function with no name".to_owned(),
            })?;

        let agg_star = (*func_call).agg_star;
        let args = if agg_star {
            vec![]
        } else {
            list_nodes((*func_call).args)
                .map(|n| scalar_expr_convert(n))
                .collect::<Result<Vec<_>, _>>()?
        };

        let agg_order = window_order_by_convert((*func_call).agg_order)?.into_vec();

        let agg_filter = match ((*func_call).agg_filter as NodePtr).is_null() {
            true => None,
            false => Some(Box::new(
                where_expr_convert((*func_call).agg_filter).map_err(AstError::from)?,
            )),
        };

        let over = match ((*func_call).over).is_null() {
            true => None,
            false => Some(window_def_convert((*func_call).over)?),
        };

        Ok(FunctionCall {
            name,
            args,
            agg_star,
            agg_distinct: (*func_call).agg_distinct,
            agg_order,
            agg_filter,
            over,
        })
    }
}

unsafe fn coalesce_expr_convert(
    coalesce: *const pg::CoalesceExpr,
) -> Result<FunctionCall, AstError> {
    unsafe {
        let args = list_nodes((*coalesce).args)
            .map(|n| scalar_expr_convert(n))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(function_call_bare(EcoString::from("coalesce"), args))
    }
}

unsafe fn minmax_expr_convert(minmax: *const pg::MinMaxExpr) -> Result<FunctionCall, AstError> {
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
        let args = list_nodes((*minmax).args)
            .map(|n| scalar_expr_convert(n))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(function_call_bare(EcoString::from(name), args))
    }
}

unsafe fn aexpr_nullif_convert(aexpr: *const pg::A_Expr) -> Result<FunctionCall, AstError> {
    unsafe {
        let mut args = Vec::with_capacity(2);
        if !(*aexpr).lexpr.is_null() {
            args.push(scalar_expr_convert((*aexpr).lexpr)?);
        }
        if !(*aexpr).rexpr.is_null() {
            args.push(scalar_expr_convert((*aexpr).rexpr)?);
        }
        Ok(function_call_bare(EcoString::from("nullif"), args))
    }
}

fn function_call_bare(name: EcoString, args: Vec<ScalarExpr>) -> FunctionCall {
    FunctionCall {
        name,
        args,
        agg_star: false,
        agg_distinct: false,
        agg_order: vec![],
        agg_filter: None,
        over: None,
    }
}

pub(super) unsafe fn aexpr_arithmetic_convert(
    aexpr: *const pg::A_Expr,
) -> Result<ArithmeticExpr, AstError> {
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
        let left = scalar_expr_convert((*aexpr).lexpr)?;
        let right = scalar_expr_convert((*aexpr).rexpr)?;
        Ok(ArithmeticExpr {
            left: Box::new(left),
            op,
            right: Box::new(right),
        })
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

unsafe fn case_expr_convert(case_expr: *const pg::CaseExpr) -> Result<CaseExpr, AstError> {
    unsafe {
        let arg = match ((*case_expr).arg as NodePtr).is_null() {
            true => None,
            false => Some(Box::new(scalar_expr_convert((*case_expr).arg as NodePtr)?)),
        };

        let whens = list_nodes((*case_expr).args)
            .map(|n| case_when_convert(n))
            .collect::<Result<Vec<_>, _>>()?;

        let default = match ((*case_expr).defresult as NodePtr).is_null() {
            true => None,
            false => Some(Box::new(scalar_expr_convert(
                (*case_expr).defresult as NodePtr,
            )?)),
        };

        Ok(CaseExpr {
            arg,
            whens,
            default,
        })
    }
}

unsafe fn case_when_convert(node: NodePtr) -> Result<CaseWhen, AstError> {
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
        let condition = where_expr_convert(cond).map_err(AstError::from)?;

        let res = (*case_when).result as NodePtr;
        if res.is_null() {
            return Err(AstError::UnsupportedFeature {
                feature: "CASE WHEN without result".to_owned(),
            });
        }
        let result = scalar_expr_convert(res)?;

        Ok(CaseWhen { condition, result })
    }
}
