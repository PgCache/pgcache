//! FROM-clause sources: tables, CTE references, joins and subqueries.

use ecow::EcoString;
use pg_query::pg_nodes as pg;
use smallvec::SmallVec;

use super::ParseContext;
use super::select::select_stmt_to_query_expr_with_ctx;
use super::where_clause::where_expr_convert;
use crate::query::ast::raw::{
    NodePtr, cast, cstr, list_is_empty, list_nodes, node_tag, node_tag_name, string_node_value,
};
use crate::query::ast::{
    AstError, CteRefNode, JoinNode, JoinQual, JoinType, TableAlias, TableNode, TableSource,
    TableSubqueryNode,
};

pub(super) unsafe fn from_clause_convert(
    from_clause: *const pg::List,
    ctx: &ParseContext,
) -> Result<SmallVec<[TableSource; 1]>, AstError> {
    unsafe {
        let mut tables = SmallVec::new();
        for from_node in list_nodes(from_clause) {
            tables.push(table_source_convert(from_node, "FROM clause", ctx)?);
        }
        Ok(tables)
    }
}

unsafe fn table_source_convert(
    node: NodePtr,
    context: &str,
    ctx: &ParseContext,
) -> Result<TableSource, AstError> {
    unsafe {
        match node_tag(node) {
            pg::NodeTag_T_RangeVar => table_node_convert(cast::<pg::RangeVar>(node), ctx),
            pg::NodeTag_T_RangeSubselect => {
                table_subquery_node_convert(cast::<pg::RangeSubselect>(node), ctx)
            }
            pg::NodeTag_T_JoinExpr => join_expr_convert(cast::<pg::JoinExpr>(node), ctx),
            other => Err(AstError::UnsupportedSelectFeature {
                feature: format!("{} in {context}", node_tag_name(other)),
            }),
        }
    }
}

unsafe fn join_expr_convert(
    join_expr: *const pg::JoinExpr,
    ctx: &ParseContext,
) -> Result<TableSource, AstError> {
    unsafe {
        let larg = (*join_expr).larg as NodePtr;
        let rarg = (*join_expr).rarg as NodePtr;
        if larg.is_null() {
            return Err(AstError::UnsupportedSelectFeature {
                feature: "join missing left argument".to_owned(),
            });
        }
        if rarg.is_null() {
            return Err(AstError::UnsupportedSelectFeature {
                feature: "join missing right argument".to_owned(),
            });
        }

        let left_table = table_source_convert(larg, "join left argument", ctx)?;
        let right_table = table_source_convert(rarg, "join right argument", ctx)?;

        let quals = (*join_expr).quals as NodePtr;
        let qual = if !quals.is_null() {
            JoinQual::On(where_expr_convert(quals)?)
        } else if !list_is_empty((*join_expr).usingClause) {
            let cols = list_nodes((*join_expr).usingClause)
                .filter_map(|n| string_node_value(n).map(EcoString::from))
                .collect();
            JoinQual::Using(cols)
        } else if (*join_expr).isNatural {
            JoinQual::Natural
        } else {
            JoinQual::Cross
        };

        Ok(TableSource::Join(JoinNode {
            join_type: join_type_map((*join_expr).jointype)?,
            left: Box::new(left_table),
            right: Box::new(right_table),
            qual,
        }))
    }
}

unsafe fn alias_convert(alias: *const pg::Alias) -> TableAlias {
    unsafe {
        TableAlias {
            name: EcoString::from(cstr((*alias).aliasname)),
            columns: list_nodes((*alias).colnames)
                .filter_map(|n| string_node_value(n).map(EcoString::from))
                .collect(),
        }
    }
}

unsafe fn table_node_convert(
    range_var: *const pg::RangeVar,
    ctx: &ParseContext,
) -> Result<TableSource, AstError> {
    unsafe {
        let schema_str = cstr((*range_var).schemaname);
        let schema = if schema_str.is_empty() {
            None
        } else {
            Some(EcoString::from(schema_str))
        };
        let name = EcoString::from(cstr((*range_var).relname));

        let alias = match ((*range_var).alias).is_null() {
            true => None,
            false => Some(alias_convert((*range_var).alias)),
        };

        if schema.is_none()
            && let Some(cte_def) = ctx.cte_find(&name)
        {
            return Ok(TableSource::CteRef(CteRefNode {
                cte_name: name,
                query: Box::new(cte_def.query.clone()),
                column_aliases: cte_def.column_aliases.clone(),
                materialization: cte_def.materialization,
                alias,
            }));
        }

        Ok(TableSource::Table(TableNode {
            schema,
            name,
            alias,
        }))
    }
}

unsafe fn table_subquery_node_convert(
    range_subselect: *const pg::RangeSubselect,
    ctx: &ParseContext,
) -> Result<TableSource, AstError> {
    unsafe {
        let subquery = (*range_subselect).subquery as NodePtr;
        if subquery.is_null() || node_tag(subquery) != pg::NodeTag_T_SelectStmt {
            return Err(AstError::UnsupportedSelectFeature {
                feature: if subquery.is_null() {
                    "empty subquery in FROM".to_owned()
                } else {
                    format!("{} as subquery in FROM", node_tag_name(node_tag(subquery)))
                },
            });
        }

        let query = select_stmt_to_query_expr_with_ctx(cast::<pg::SelectStmt>(subquery), ctx)?;

        let alias = match ((*range_subselect).alias).is_null() {
            true => None,
            false => Some(alias_convert((*range_subselect).alias)),
        };

        Ok(TableSource::Subquery(TableSubqueryNode {
            lateral: (*range_subselect).lateral,
            query: Box::new(query),
            alias,
        }))
    }
}

fn join_type_map(jt: pg::JoinType) -> Result<JoinType, AstError> {
    match jt {
        pg::JoinType_JOIN_INNER => Ok(JoinType::Inner),
        pg::JoinType_JOIN_LEFT => Ok(JoinType::Left),
        pg::JoinType_JOIN_FULL => Ok(JoinType::Full),
        pg::JoinType_JOIN_RIGHT => Ok(JoinType::Right),
        _ => Err(AstError::UnsupportedJoinType),
    }
}
