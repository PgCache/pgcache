//! SELECT and query structure: CTEs, set operations, VALUES, the select list,
//! ORDER BY, GROUP BY and LIMIT.

use ecow::EcoString;
use pg_query::pg_nodes as pg;
use smallvec::SmallVec;

use super::ParseContext;
use super::from::from_clause_convert;
use super::scalar::scalar_expr_convert;
use super::where_clause::{column_ref_extract, const_value_extract, where_expr_convert};
use super::window::{
    select_columns_window_refs_resolve, window_clause_extract, window_order_by_convert,
};
use crate::query::ast::raw::{
    NodePtr, cast, cstr, list_is_empty, list_nodes, node_tag, node_tag_name, string_node_value,
};
use crate::query::ast::{
    AstError, ColumnNode, CteDefinition, CteMaterialization, LimitClause, LiteralValue,
    OrderByClause, QueryBody, QueryExpr, SelectColumn, SelectColumns, SelectNode, SetOpNode,
    SetOpType, ValuesClause,
};

unsafe fn with_clause_extract(
    with_clause: *const pg::WithClause,
) -> Result<Vec<CteDefinition>, AstError> {
    unsafe {
        if (*with_clause).recursive {
            return Err(AstError::UnsupportedFeature {
                feature: "WITH RECURSIVE".to_owned(),
            });
        }

        let mut ctes = Vec::new();

        for cte_node in list_nodes((*with_clause).ctes) {
            if node_tag(cte_node) != pg::NodeTag_T_CommonTableExpr {
                return Err(AstError::UnsupportedFeature {
                    feature: format!("{} in WITH clause", node_tag_name(node_tag(cte_node))),
                });
            }
            let cte = cast::<pg::CommonTableExpr>(cte_node);
            let ctename = cstr((*cte).ctename);

            if (*cte).cterecursive {
                return Err(AstError::UnsupportedFeature {
                    feature: format!("recursive CTE: {ctename}"),
                });
            }

            let materialization = match (*cte).ctematerialized {
                pg::CTEMaterialize_CTEMaterializeAlways => CteMaterialization::Materialized,
                pg::CTEMaterialize_CTEMaterializeNever => CteMaterialization::NotMaterialized,
                _ => CteMaterialization::Default,
            };

            let column_aliases = list_nodes((*cte).aliascolnames)
                .filter_map(|n| string_node_value(n).map(EcoString::from))
                .collect();

            let inner = (*cte).ctequery as NodePtr;
            if inner.is_null() || node_tag(inner) != pg::NodeTag_T_SelectStmt {
                return Err(AstError::UnsupportedFeature {
                    feature: format!("CTE query is not SELECT: {ctename}"),
                });
            }

            let ctx = ParseContext { ctes: ctes.clone() };
            let query = select_stmt_to_query_expr_with_ctx(cast::<pg::SelectStmt>(inner), &ctx)?;

            ctes.push(CteDefinition {
                name: EcoString::from(ctename),
                query,
                column_aliases,
                materialization,
            });
        }

        Ok(ctes)
    }
}

pub(super) unsafe fn select_stmt_to_query_expr(
    select_stmt: *const pg::SelectStmt,
) -> Result<QueryExpr, AstError> {
    let ctx = ParseContext::empty();
    unsafe { select_stmt_to_query_expr_with_ctx(select_stmt, &ctx) }
}

pub(super) unsafe fn select_stmt_to_query_expr_with_ctx(
    select_stmt: *const pg::SelectStmt,
    outer_ctx: &ParseContext,
) -> Result<QueryExpr, AstError> {
    unsafe {
        if !list_is_empty((*select_stmt).lockingClause) {
            return Err(AstError::UnsupportedSelectFeature {
                feature: "locking clause (FOR UPDATE/FOR SHARE)".to_owned(),
            });
        }

        let ctes = if !(*select_stmt).withClause.is_null() {
            with_clause_extract((*select_stmt).withClause)?
        } else {
            Vec::new()
        };

        let ctx = ParseContext::with_ctes(outer_ctx, &ctes);

        let order_by = order_by_clause_convert((*select_stmt).sortClause)?;
        let limit = limit_clause_convert(
            (*select_stmt).limitCount as NodePtr,
            (*select_stmt).limitOffset as NodePtr,
        )?;
        let body = query_body_convert(select_stmt, &ctx)?;

        Ok(QueryExpr {
            ctes,
            body,
            order_by,
            limit,
        })
    }
}

/// The query body: VALUES, a plain SELECT, or a set operation.
unsafe fn query_body_convert(
    select_stmt: *const pg::SelectStmt,
    ctx: &ParseContext,
) -> Result<QueryBody, AstError> {
    unsafe {
        match (*select_stmt).op {
            pg::SetOperation_SETOP_NONE if !list_is_empty((*select_stmt).valuesLists) => {
                let rows = value_list_convert((*select_stmt).valuesLists)?;
                Ok(QueryBody::Values(ValuesClause { rows }))
            }
            pg::SetOperation_SETOP_NONE => {
                let select_node = select_stmt_to_select_node(select_stmt, ctx)?;
                Ok(QueryBody::Select(Box::new(select_node)))
            }
            pg::SetOperation_SETOP_UNION => set_op_convert(select_stmt, SetOpType::Union, ctx),
            pg::SetOperation_SETOP_INTERSECT => {
                set_op_convert(select_stmt, SetOpType::Intersect, ctx)
            }
            pg::SetOperation_SETOP_EXCEPT => set_op_convert(select_stmt, SetOpType::Except, ctx),
            other => Err(AstError::UnsupportedFeature {
                feature: format!("set operation: {other}"),
            }),
        }
    }
}

unsafe fn set_op_convert(
    select_stmt: *const pg::SelectStmt,
    op: SetOpType,
    ctx: &ParseContext,
) -> Result<QueryBody, AstError> {
    unsafe {
        let larg = (*select_stmt).larg;
        let rarg = (*select_stmt).rarg;
        if larg.is_null() || rarg.is_null() {
            return Err(AstError::UnsupportedFeature {
                feature: "SET operation without argument".to_owned(),
            });
        }

        let left = select_stmt_to_query_expr_with_ctx(larg, ctx)?;
        let right = select_stmt_to_query_expr_with_ctx(rarg, ctx)?;

        Ok(QueryBody::SetOp(SetOpNode {
            op,
            all: (*select_stmt).all,
            left: Box::new(left),
            right: Box::new(right),
        }))
    }
}

unsafe fn select_stmt_to_select_node(
    select_stmt: *const pg::SelectStmt,
    ctx: &ParseContext,
) -> Result<SelectNode, AstError> {
    unsafe {
        let mut columns = select_columns_convert((*select_stmt).targetList)?;
        let window_defs = window_clause_extract((*select_stmt).windowClause)?;
        select_columns_window_refs_resolve(&mut columns, &window_defs)?;
        let from = from_clause_convert((*select_stmt).fromClause, ctx)?;
        let where_clause = match ((*select_stmt).whereClause as NodePtr).is_null() {
            true => None,
            false => Some(where_expr_convert((*select_stmt).whereClause)?),
        };
        let group_by = group_by_clause_convert((*select_stmt).groupClause)?;
        let having = match ((*select_stmt).havingClause as NodePtr).is_null() {
            true => None,
            false => Some(where_expr_convert((*select_stmt).havingClause)?),
        };

        Ok(SelectNode {
            distinct: !list_is_empty((*select_stmt).distinctClause),
            columns,
            from,
            where_clause,
            group_by,
            having,
        })
    }
}

unsafe fn value_list_convert(
    value_lists: *const pg::List,
) -> Result<Vec<Vec<LiteralValue>>, AstError> {
    unsafe {
        list_nodes(value_lists)
            .map(|row_node| value_row_convert(row_node))
            .collect()
    }
}

unsafe fn value_row_convert(row_node: NodePtr) -> Result<Vec<LiteralValue>, AstError> {
    unsafe {
        if node_tag(row_node) != pg::NodeTag_T_List {
            return Err(AstError::UnsupportedFeature {
                feature: format!("{} as VALUES row", node_tag_name(node_tag(row_node))),
            });
        }
        let mut row = Vec::new();
        for item in list_nodes(row_node as *const pg::List) {
            if node_tag(item) != pg::NodeTag_T_A_Const {
                return Err(AstError::UnsupportedFeature {
                    feature: format!("{} in VALUES", node_tag_name(node_tag(item))),
                });
            }
            row.push(const_value_extract(cast::<pg::A_Const>(item))?);
        }
        Ok(row)
    }
}

unsafe fn select_columns_convert(target_list: *const pg::List) -> Result<SelectColumns, AstError> {
    unsafe {
        if list_is_empty(target_list) {
            return Ok(SelectColumns::None);
        }
        let columns = list_nodes(target_list)
            .map(|target| select_column_convert(target))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(SelectColumns::Columns(columns))
    }
}

unsafe fn select_column_convert(target: NodePtr) -> Result<SelectColumn, AstError> {
    unsafe {
        if node_tag(target) != pg::NodeTag_T_ResTarget {
            return Err(AstError::UnsupportedSelectFeature {
                feature: format!("{} in select list", node_tag_name(node_tag(target))),
            });
        }
        let res_target = cast::<pg::ResTarget>(target);
        let val_node = (*res_target).val as NodePtr;
        if val_node.is_null() {
            return Err(AstError::UnsupportedSelectFeature {
                feature: "ResTarget without value".to_owned(),
            });
        }

        if node_tag(val_node) == pg::NodeTag_T_ColumnRef
            && let Some(star) = star_column(cast::<pg::ColumnRef>(val_node))?
        {
            return Ok(star);
        }

        let name = cstr((*res_target).name);
        let alias = (!name.is_empty()).then(|| EcoString::from(name));
        let expr = scalar_expr_convert(val_node)?;
        Ok(SelectColumn::Expr { expr, alias })
    }
}

/// `*` or `qualifier.*`; `None` for any other column reference.
unsafe fn star_column(column_ref: *const pg::ColumnRef) -> Result<Option<SelectColumn>, AstError> {
    unsafe {
        let fields: SmallVec<[_; 4]> = list_nodes((*column_ref).fields).collect();
        match fields.as_slice() {
            [star] if node_tag(*star) == pg::NodeTag_T_A_Star => Ok(Some(SelectColumn::Star(None))),
            [.., qualifier, star] if node_tag(*star) == pg::NodeTag_T_A_Star => {
                let table = string_node_value(*qualifier).ok_or(AstError::InvalidTableRef)?;
                Ok(Some(SelectColumn::Star(Some(EcoString::from(table)))))
            }
            _ => Ok(None),
        }
    }
}

unsafe fn order_by_clause_convert(
    sort_clause: *const pg::List,
) -> Result<SmallVec<[OrderByClause; 1]>, AstError> {
    unsafe { window_order_by_convert(sort_clause) }
}

unsafe fn group_by_clause_convert(
    group_clause: *const pg::List,
) -> Result<Vec<ColumnNode>, AstError> {
    unsafe {
        let mut group_by = Vec::new();
        for node in list_nodes(group_clause) {
            if node_tag(node) != pg::NodeTag_T_ColumnRef {
                return Err(AstError::UnsupportedFeature {
                    feature: format!("{} in GROUP BY", node_tag_name(node_tag(node))),
                });
            }
            group_by.push(column_ref_extract(cast::<pg::ColumnRef>(node)).map_err(AstError::from)?);
        }
        Ok(group_by)
    }
}

unsafe fn limit_clause_convert(
    limit_count: NodePtr,
    limit_offset: NodePtr,
) -> Result<Option<LimitClause>, AstError> {
    unsafe {
        let count = limit_node_extract(limit_count)?;
        let offset = limit_node_extract(limit_offset)?;
        if count.is_none() && offset.is_none() {
            return Ok(None);
        }
        Ok(Some(LimitClause { count, offset }))
    }
}

unsafe fn limit_node_extract(node: NodePtr) -> Result<Option<LiteralValue>, AstError> {
    unsafe {
        if node.is_null() {
            return Ok(None);
        }
        match node_tag(node) {
            pg::NodeTag_T_A_Const => {
                let value = const_value_extract(cast::<pg::A_Const>(node))?;
                match value {
                    LiteralValue::Integer(_) => Ok(Some(value)),
                    _ => Err(AstError::UnsupportedFeature {
                        feature: format!("LIMIT/OFFSET value: {value:?}"),
                    }),
                }
            }
            pg::NodeTag_T_ParamRef => Ok(Some(LiteralValue::Parameter(
                format!("${}", (*cast::<pg::ParamRef>(node)).number).into(),
            ))),
            other => Err(AstError::UnsupportedFeature {
                feature: format!("{} in LIMIT/OFFSET", node_tag_name(other)),
            }),
        }
    }
}
