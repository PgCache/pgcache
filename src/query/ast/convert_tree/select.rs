//! SELECT and query structure: CTEs, set operations, VALUES, the select list,
//! ORDER BY, GROUP BY and LIMIT.

use ecow::EcoString;
use pg_query::pg_nodes as pg;
use smallvec::SmallVec;

use super::from::source_emit;
use super::scalar::scalar_emit;
use super::where_clause::{column_ref_emit, const_value_extract, where_emit};
use super::window::{WindowDefs, order_by_emit, window_defs_extract};
use super::{ParseContext, RawCte, cte_raw};
use crate::query::ast::raw::{
    NodePtr, cast, cstr, list_is_empty, list_len, list_nodes, node_tag, node_tag_name,
    string_node_value,
};
use crate::query::ast::rope::{NodeKind, TreeBuilder};
use crate::query::ast::{AstError, LiteralValue, SetOpType};

/// Emit a query: its CTEs, body, ORDER BY and LIMIT literals.
pub(super) unsafe fn query_emit(
    builder: &mut TreeBuilder<'_>,
    select_stmt: *const pg::SelectStmt,
    outer_ctx: &ParseContext<'_>,
) -> Result<(), AstError> {
    unsafe {
        if !list_is_empty((*select_stmt).lockingClause) {
            return Err(AstError::UnsupportedSelectFeature {
                feature: "locking clause (FOR UPDATE/FOR SHARE)".to_owned(),
            });
        }
        let ctes = with_clause_extract((*select_stmt).withClause)?;
        let ctx = ParseContext::with_ctes(outer_ctx, &ctes);
        let limit_count = limit_node_extract((*select_stmt).limitCount as NodePtr)?;
        let limit_offset = limit_node_extract((*select_stmt).limitOffset as NodePtr)?;

        let open = builder.open(NodeKind::Query {
            cte_count: list_len_u32(ctes.len()),
            order_by_count: list_len((*select_stmt).sortClause),
            has_limit_count: limit_count.is_some(),
            has_limit_offset: limit_offset.is_some(),
        });
        for (index, cte) in ctes.iter().enumerate() {
            let cte_ctx = ParseContext::with_ctes(outer_ctx, ctes.get(..index).unwrap_or(&[]));
            cte_emit(builder, cte, &cte_ctx)?;
        }
        body_emit(builder, select_stmt, &ctx)?;
        order_by_emit(builder, (*select_stmt).sortClause, None)?;
        for literal in [limit_count, limit_offset].into_iter().flatten() {
            builder.leaf(NodeKind::Literal(literal));
        }
        builder.close(open);
        Ok(())
    }
}

fn list_len_u32(len: usize) -> u32 {
    u32::try_from(len).unwrap_or(u32::MAX)
}

/// The CTEs of a WITH clause, validated in order.
unsafe fn with_clause_extract(with_clause: *const pg::WithClause) -> Result<Vec<RawCte>, AstError> {
    unsafe {
        if with_clause.is_null() {
            return Ok(Vec::new());
        }
        if (*with_clause).recursive {
            return Err(AstError::UnsupportedFeature {
                feature: "WITH RECURSIVE".to_owned(),
            });
        }
        list_nodes((*with_clause).ctes)
            .map(|node| cte_raw(node))
            .collect()
    }
}

/// A CTE definition: its alias names, then its query.
unsafe fn cte_emit(
    builder: &mut TreeBuilder<'_>,
    cte: &RawCte,
    ctx: &ParseContext<'_>,
) -> Result<(), AstError> {
    unsafe {
        let open = builder.open(NodeKind::Cte {
            name: cte.name.clone(),
            alias_count: list_len_u32(cte.column_aliases.len()),
            materialization: cte.materialization,
        });
        for alias in &cte.column_aliases {
            builder.leaf(NodeKind::Name(alias.clone()));
        }
        cte_query_emit(builder, cte, ctx)?;
        builder.close(open);
        Ok(())
    }
}

/// The query of a CTE, converted in the context the CTE was defined in.
pub(super) unsafe fn cte_query_emit(
    builder: &mut TreeBuilder<'_>,
    cte: &RawCte,
    ctx: &ParseContext<'_>,
) -> Result<(), AstError> {
    unsafe {
        let inner = cast::<pg::SelectStmt>((*cte.node).ctequery as NodePtr);
        query_emit(builder, inner, ctx)
    }
}

/// The query body: VALUES, a plain SELECT, or a set operation.
unsafe fn body_emit(
    builder: &mut TreeBuilder<'_>,
    select_stmt: *const pg::SelectStmt,
    ctx: &ParseContext<'_>,
) -> Result<(), AstError> {
    unsafe {
        match (*select_stmt).op {
            pg::SetOperation_SETOP_NONE if !list_is_empty((*select_stmt).valuesLists) => {
                values_emit(builder, (*select_stmt).valuesLists)
            }
            pg::SetOperation_SETOP_NONE => select_emit(builder, select_stmt, ctx),
            pg::SetOperation_SETOP_UNION => {
                set_op_emit(builder, select_stmt, SetOpType::Union, ctx)
            }
            pg::SetOperation_SETOP_INTERSECT => {
                set_op_emit(builder, select_stmt, SetOpType::Intersect, ctx)
            }
            pg::SetOperation_SETOP_EXCEPT => {
                set_op_emit(builder, select_stmt, SetOpType::Except, ctx)
            }
            other => Err(AstError::UnsupportedFeature {
                feature: format!("set operation: {other}"),
            }),
        }
    }
}

unsafe fn set_op_emit(
    builder: &mut TreeBuilder<'_>,
    select_stmt: *const pg::SelectStmt,
    op: SetOpType,
    ctx: &ParseContext<'_>,
) -> Result<(), AstError> {
    unsafe {
        let larg = (*select_stmt).larg;
        let rarg = (*select_stmt).rarg;
        if larg.is_null() || rarg.is_null() {
            return Err(AstError::UnsupportedFeature {
                feature: "SET operation without argument".to_owned(),
            });
        }
        let open = builder.open(NodeKind::SetOp {
            op,
            all: (*select_stmt).all,
        });
        query_emit(builder, larg, ctx)?;
        query_emit(builder, rarg, ctx)?;
        builder.close(open);
        Ok(())
    }
}

/// A SELECT: its items (named windows resolved as they are met), FROM,
/// WHERE, GROUP BY and HAVING.
unsafe fn select_emit(
    builder: &mut TreeBuilder<'_>,
    select_stmt: *const pg::SelectStmt,
    ctx: &ParseContext<'_>,
) -> Result<(), AstError> {
    unsafe {
        let target_list = (*select_stmt).targetList;
        let where_clause = (*select_stmt).whereClause as NodePtr;
        let having = (*select_stmt).havingClause as NodePtr;
        let open = builder.open(NodeKind::Select {
            distinct: !list_is_empty((*select_stmt).distinctClause),
            item_count: (!list_is_empty(target_list)).then(|| list_len(target_list)),
            from_count: list_len((*select_stmt).fromClause),
            has_where: !where_clause.is_null(),
            group_by_count: list_len((*select_stmt).groupClause),
            has_having: !having.is_null(),
        });
        let windows = window_defs_extract((*select_stmt).windowClause)?;
        for target in list_nodes(target_list) {
            select_item_emit(builder, target, &windows)?;
        }
        for from_node in list_nodes((*select_stmt).fromClause) {
            source_emit(builder, from_node, "FROM clause", ctx)?;
        }
        if !where_clause.is_null() {
            where_emit(builder, where_clause)?;
        }
        for node in list_nodes((*select_stmt).groupClause) {
            if node_tag(node) != pg::NodeTag_T_ColumnRef {
                return Err(AstError::UnsupportedFeature {
                    feature: format!("{} in GROUP BY", node_tag_name(node_tag(node))),
                });
            }
            column_ref_emit(builder, cast::<pg::ColumnRef>(node)).map_err(AstError::from)?;
        }
        if !having.is_null() {
            where_emit(builder, having)?;
        }
        builder.close(open);
        Ok(())
    }
}

unsafe fn values_emit(
    builder: &mut TreeBuilder<'_>,
    value_lists: *const pg::List,
) -> Result<(), AstError> {
    unsafe {
        let open = builder.open(NodeKind::Values {
            row_count: list_len(value_lists),
        });
        for row_node in list_nodes(value_lists) {
            values_row_emit(builder, row_node)?;
        }
        builder.close(open);
        Ok(())
    }
}

/// One VALUES row: a list of literals.
unsafe fn values_row_emit(
    builder: &mut TreeBuilder<'_>,
    row_node: NodePtr,
) -> Result<(), AstError> {
    unsafe {
        if node_tag(row_node) != pg::NodeTag_T_List {
            return Err(AstError::UnsupportedFeature {
                feature: format!("{} as VALUES row", node_tag_name(node_tag(row_node))),
            });
        }
        let row = row_node as *const pg::List;
        let open = builder.open(NodeKind::ValuesRow {
            width: list_len(row),
        });
        for item in list_nodes(row) {
            if node_tag(item) != pg::NodeTag_T_A_Const {
                return Err(AstError::UnsupportedFeature {
                    feature: format!("{} in VALUES", node_tag_name(node_tag(item))),
                });
            }
            let value = const_value_extract(cast::<pg::A_Const>(item))?;
            builder.leaf(NodeKind::Literal(value));
        }
        builder.close(open);
        Ok(())
    }
}

/// One select-list entry: a star, or an expression with its alias.
unsafe fn select_item_emit(
    builder: &mut TreeBuilder<'_>,
    target: NodePtr,
    windows: &WindowDefs,
) -> Result<(), AstError> {
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
            && let Some(qualifier) = star_qualifier(cast::<pg::ColumnRef>(val_node))?
        {
            builder.leaf(NodeKind::Star { qualifier });
            return Ok(());
        }
        let name = cstr((*res_target).name);
        let alias = (!name.is_empty()).then(|| EcoString::from(name));
        let open = builder.open(NodeKind::SelectExpr { alias });
        scalar_emit(builder, val_node, Some(windows))?;
        builder.close(open);
        Ok(())
    }
}

/// `Some(qualifier)` for `*` or `qualifier.*`; `None` for any other
/// column reference. The outer option says whether it is a star at all.
unsafe fn star_qualifier(
    column_ref: *const pg::ColumnRef,
) -> Result<Option<Option<EcoString>>, AstError> {
    unsafe {
        let fields: SmallVec<[_; 4]> = list_nodes((*column_ref).fields).collect();
        match fields.as_slice() {
            [star] if node_tag(*star) == pg::NodeTag_T_A_Star => Ok(Some(None)),
            [.., qualifier, star] if node_tag(*star) == pg::NodeTag_T_A_Star => {
                let table = string_node_value(*qualifier).ok_or(AstError::InvalidTableRef)?;
                Ok(Some(Some(EcoString::from(table))))
            }
            _ => Ok(None),
        }
    }
}

/// A LIMIT or OFFSET value: an integer literal or a placeholder.
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
