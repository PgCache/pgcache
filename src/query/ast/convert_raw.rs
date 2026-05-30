//! Build a [`QueryExpr`] directly from PostgreSQL's raw parse tree (the C node
//! structs from `pg_query::pg_nodes`), via the `pg_query::parse_raw_scoped`
//! callback — the proxy's single SQL-parsing path (PGC-192). It reads tagged C
//! node pointers through [`super::raw`] with no protobuf serialize/decode
//! round-trip. Must run inside the callback (the tree is freed when it returns).

#![allow(clippy::wildcard_enum_match_arm)]

use std::os::raw::c_void;

use ecow::EcoString;
use ordered_float::NotNan;

use pg_query::pg_nodes as pg;

use crate::query::cast::cast_target_from_canonical;
use crate::query::transform::query_expr_constant_fold;

use super::raw::{NodePtr, cast, cstr, list_is_empty, list_nodes, node_tag, string_node_value};
use super::*;

/// Convert the root of a raw parse tree (`List *` of `RawStmt`, as an opaque
/// pointer from `parse_raw_scoped`) into a [`QueryExpr`].
///
/// # Safety
/// `tree_root` must be the live `List *` handed to the `parse_raw_scoped`
/// callback, valid for the duration of this call.
pub unsafe fn query_expr_convert_raw(tree_root: *const c_void) -> Result<QueryExpr, AstError> {
    unsafe {
        let stmts: Vec<_> = list_nodes(tree_root as *const pg::List).collect();
        let [raw_stmt] = stmts.as_slice() else {
            return Err(AstError::MultipleStatements);
        };

        let stmt = (*cast::<pg::RawStmt>(*raw_stmt)).stmt as NodePtr;
        if stmt.is_null() {
            return Err(AstError::MissingStatement);
        }

        let mut query = match node_tag(stmt) {
            pg::NodeTag_T_SelectStmt => select_stmt_to_query_expr(cast::<pg::SelectStmt>(stmt))?,
            other => {
                return Err(AstError::UnsupportedStatement {
                    statement_type: format!("{other:?}"),
                });
            }
        };

        query_expr_constant_fold(&mut query);
        Ok(query)
    }
}

struct ParseContext {
    ctes: Vec<CteDefinition>,
}

impl ParseContext {
    fn empty() -> Self {
        Self { ctes: Vec::new() }
    }

    fn cte_find(&self, name: &str) -> Option<&CteDefinition> {
        self.ctes.iter().find(|c| c.name == name)
    }
}

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
                    feature: format!("WITH clause entry: {:?}", node_tag(cte_node)),
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

unsafe fn select_stmt_to_query_expr(
    select_stmt: *const pg::SelectStmt,
) -> Result<QueryExpr, AstError> {
    let ctx = ParseContext::empty();
    unsafe { select_stmt_to_query_expr_with_ctx(select_stmt, &ctx) }
}

unsafe fn select_stmt_to_query_expr_with_ctx(
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

        let mut all_ctes = outer_ctx.ctes.clone();
        all_ctes.extend(ctes.clone());
        let ctx = ParseContext { ctes: all_ctes };

        let order_by = order_by_clause_convert((*select_stmt).sortClause)?;
        let limit = limit_clause_convert(
            (*select_stmt).limitCount as NodePtr,
            (*select_stmt).limitOffset as NodePtr,
        )?;

        let body = match (*select_stmt).op {
            pg::SetOperation_SETOP_NONE => {
                if !list_is_empty((*select_stmt).valuesLists) {
                    let rows = value_list_convert((*select_stmt).valuesLists)?;
                    QueryBody::Values(ValuesClause { rows })
                } else {
                    let select_node = select_stmt_to_select_node(select_stmt, &ctx)?;
                    QueryBody::Select(Box::new(select_node))
                }
            }
            op @ (pg::SetOperation_SETOP_UNION
            | pg::SetOperation_SETOP_INTERSECT
            | pg::SetOperation_SETOP_EXCEPT) => {
                let larg = (*select_stmt).larg;
                let rarg = (*select_stmt).rarg;
                if larg.is_null() || rarg.is_null() {
                    return Err(AstError::UnsupportedFeature {
                        feature: "SET operation without argument".to_owned(),
                    });
                }

                let left = select_stmt_to_query_expr_with_ctx(larg, &ctx)?;
                let right = select_stmt_to_query_expr_with_ctx(rarg, &ctx)?;

                let op_type = match op {
                    pg::SetOperation_SETOP_UNION => SetOpType::Union,
                    pg::SetOperation_SETOP_INTERSECT => SetOpType::Intersect,
                    _ => SetOpType::Except,
                };

                QueryBody::SetOp(SetOpNode {
                    op: op_type,
                    all: (*select_stmt).all,
                    left: Box::new(left),
                    right: Box::new(right),
                })
            }
            other => {
                return Err(AstError::UnsupportedFeature {
                    feature: format!("set operation: {other}"),
                });
            }
        };

        Ok(QueryExpr {
            ctes,
            body,
            order_by,
            limit,
        })
    }
}

unsafe fn select_stmt_to_select_node(
    select_stmt: *const pg::SelectStmt,
    ctx: &ParseContext,
) -> Result<SelectNode, AstError> {
    unsafe {
        let columns = select_columns_convert((*select_stmt).targetList)?;
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
        let mut rv = Vec::new();
        for row_node in list_nodes(value_lists) {
            if node_tag(row_node) != pg::NodeTag_T_List {
                return Err(AstError::UnsupportedFeature {
                    feature: format!("Values row: {:?}", node_tag(row_node)),
                });
            }
            let mut row = Vec::new();
            for item in list_nodes(row_node as *const pg::List) {
                if node_tag(item) != pg::NodeTag_T_A_Const {
                    return Err(AstError::UnsupportedFeature {
                        feature: format!("Value expression: {:?}", node_tag(item)),
                    });
                }
                row.push(const_value_extract(cast::<pg::A_Const>(item))?);
            }
            rv.push(row);
        }
        Ok(rv)
    }
}

unsafe fn select_columns_convert(target_list: *const pg::List) -> Result<SelectColumns, AstError> {
    unsafe {
        if list_is_empty(target_list) {
            return Ok(SelectColumns::None);
        }

        let mut columns = Vec::new();

        for target in list_nodes(target_list) {
            if node_tag(target) != pg::NodeTag_T_ResTarget {
                return Err(AstError::UnsupportedSelectFeature {
                    feature: format!("Target: {:?}", node_tag(target)),
                });
            }
            let res_target = cast::<pg::ResTarget>(target);
            let val_node = (*res_target).val as NodePtr;
            if val_node.is_null() {
                return Err(AstError::UnsupportedSelectFeature {
                    feature: "ResTarget without value".to_owned(),
                });
            }

            let name = cstr((*res_target).name);
            let alias = if name.is_empty() {
                None
            } else {
                Some(EcoString::from(name))
            };

            if node_tag(val_node) == pg::NodeTag_T_ColumnRef {
                let fields: Vec<_> =
                    list_nodes((*cast::<pg::ColumnRef>(val_node)).fields).collect();

                if let [field] = fields.as_slice()
                    && node_tag(*field) == pg::NodeTag_T_A_Star
                {
                    columns.push(SelectColumn::Star(None));
                    continue;
                }

                if fields.len() >= 2
                    && node_tag(*fields.last().expect("non-empty fields")) == pg::NodeTag_T_A_Star
                {
                    let qualifier = *fields
                        .get(fields.len() - 2)
                        .ok_or(AstError::InvalidTableRef)?;
                    let table = string_node_value(qualifier).ok_or(AstError::InvalidTableRef)?;
                    columns.push(SelectColumn::Star(Some(EcoString::from(table))));
                    continue;
                }
            }

            let expr = scalar_expr_convert(val_node)?;
            columns.push(SelectColumn::Expr { expr, alias });
        }

        Ok(SelectColumns::Columns(columns))
    }
}

unsafe fn from_clause_convert(
    from_clause: *const pg::List,
    ctx: &ParseContext,
) -> Result<Vec<TableSource>, AstError> {
    unsafe {
        let mut tables = Vec::new();
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
                feature: format!("{context} type: {other:?}"),
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
                feature: format!(
                    "subquery: {:?}",
                    (!subquery.is_null()).then(|| node_tag(subquery))
                ),
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

unsafe fn scalar_expr_convert(node: NodePtr) -> Result<ScalarExpr, AstError> {
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
                        feature: format!("Column expression A_Expr kind: {other}"),
                    }),
                }
            }
            pg::NodeTag_T_CaseExpr => Ok(ScalarExpr::Case(case_expr_convert(
                cast::<pg::CaseExpr>(node),
            )?)),
            pg::NodeTag_T_TypeCast => type_cast_convert(cast::<pg::TypeCast>(node)),
            other => Err(AstError::UnsupportedFeature {
                feature: format!("Column expression node: {other:?}"),
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
        let name_nodes: Vec<_> = list_nodes((*tn).names).collect();
        let mut parts: Vec<&str> = Vec::with_capacity(name_nodes.len());
        for n in name_nodes {
            match string_node_value(n) {
                Some(s) => parts.push(s),
                None => {
                    return Err(AstError::UnsupportedFeature {
                        feature: format!("TypeName component: {:?}", node_tag(n)),
                    });
                }
            }
        }
        if parts.is_empty() {
            return Err(AstError::UnsupportedFeature {
                feature: "TypeName with no components".to_owned(),
            });
        }
        let name_start = if parts.len() > 1 && parts.first() == Some(&"pg_catalog") {
            1
        } else {
            0
        };

        let mut out = parts.get(name_start..).unwrap_or(&[]).join(".");

        if !list_is_empty((*tn).typmods) {
            let mut typmod_strs: Vec<String> = Vec::new();
            for tm in list_nodes((*tn).typmods) {
                if node_tag(tm) != pg::NodeTag_T_A_Const {
                    return Err(AstError::UnsupportedFeature {
                        feature: format!("TypeName typmod: {:?}", node_tag(tm)),
                    });
                }
                let lit = const_value_extract(cast::<pg::A_Const>(tm)).map_err(|_| {
                    AstError::UnsupportedFeature {
                        feature: "TypeName typmod literal".to_owned(),
                    }
                })?;
                let mut buf = String::new();
                lit.deparse(&mut buf);
                typmod_strs.push(buf);
            }
            out.push('(');
            out.push_str(&typmod_strs.join(","));
            out.push(')');
        }

        for _ in list_nodes((*tn).arrayBounds) {
            out.push_str("[]");
        }

        Ok(EcoString::from(out))
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

        let agg_order = window_order_by_convert((*func_call).agg_order)?;

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

unsafe fn window_def_convert(win_def: *const pg::WindowDef) -> Result<WindowSpec, AstError> {
    unsafe {
        let partition_by = list_nodes((*win_def).partitionClause)
            .map(|n| scalar_expr_convert(n))
            .collect::<Result<Vec<_>, _>>()?;
        let order_by = window_order_by_convert((*win_def).orderClause)?;
        Ok(WindowSpec {
            partition_by,
            order_by,
        })
    }
}

unsafe fn window_order_by_convert(
    order_clause: *const pg::List,
) -> Result<Vec<OrderByClause>, AstError> {
    unsafe {
        let mut order_by = Vec::new();
        for sort_node in list_nodes(order_clause) {
            if node_tag(sort_node) != pg::NodeTag_T_SortBy {
                return Err(AstError::UnsupportedFeature {
                    feature: format!("ORDER BY node type: {:?}", node_tag(sort_node)),
                });
            }
            order_by.push(sort_by_to_order_clause(cast::<pg::SortBy>(sort_node))?);
        }
        Ok(order_by)
    }
}

unsafe fn sort_by_to_order_clause(sort_by: *const pg::SortBy) -> Result<OrderByClause, AstError> {
    unsafe {
        let expr_node = (*sort_by).node as NodePtr;
        if expr_node.is_null() {
            return Err(AstError::UnsupportedFeature {
                feature: "ORDER BY without expression".to_owned(),
            });
        }
        let expr = scalar_expr_convert(expr_node)?;
        let direction = order_dir_map((*sort_by).sortby_dir)?;
        let null_order = null_order_map((*sort_by).sortby_nulls)?;
        Ok(OrderByClause {
            expr,
            direction,
            null_order,
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

unsafe fn aexpr_arithmetic_convert(aexpr: *const pg::A_Expr) -> Result<ArithmeticExpr, AstError> {
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

unsafe fn arithmetic_op_extract(name: *const pg::List) -> Result<ArithmeticOp, AstError> {
    unsafe {
        let names: Vec<_> = list_nodes(name).collect();
        let [name_node] = names.as_slice() else {
            return Err(AstError::UnsupportedFeature {
                feature: "multi-part operator names in arithmetic".to_owned(),
            });
        };
        match string_node_value(*name_node) {
            Some("+") => Ok(ArithmeticOp::Add),
            Some("-") => Ok(ArithmeticOp::Subtract),
            Some("*") => Ok(ArithmeticOp::Multiply),
            Some("/") => Ok(ArithmeticOp::Divide),
            Some("%") => Ok(ArithmeticOp::Modulo),
            Some(op) => Err(AstError::UnsupportedFeature {
                feature: format!("arithmetic operator: {op}"),
            }),
            None => Err(AstError::UnsupportedFeature {
                feature: "invalid operator name format".to_owned(),
            }),
        }
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
                feature: format!("Expected CaseWhen, got: {:?}", node_tag(node)),
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

unsafe fn order_by_clause_convert(
    sort_clause: *const pg::List,
) -> Result<Vec<OrderByClause>, AstError> {
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
                    feature: format!("GROUP BY expression: {:?}", node_tag(node)),
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
            pg::NodeTag_T_ParamRef => Ok(Some(LiteralValue::Parameter(format!(
                "${}",
                (*cast::<pg::ParamRef>(node)).number
            )))),
            other => Err(AstError::UnsupportedFeature {
                feature: format!("LIMIT/OFFSET expression: {other:?}"),
            }),
        }
    }
}

// ---------- Enum mapping (C int → pgcache enum) ----------

fn join_type_map(jt: pg::JoinType) -> Result<JoinType, AstError> {
    match jt {
        pg::JoinType_JOIN_INNER => Ok(JoinType::Inner),
        pg::JoinType_JOIN_LEFT => Ok(JoinType::Left),
        pg::JoinType_JOIN_FULL => Ok(JoinType::Full),
        pg::JoinType_JOIN_RIGHT => Ok(JoinType::Right),
        _ => Err(AstError::UnsupportedJoinType),
    }
}

fn order_dir_map(dir: pg::SortByDir) -> Result<OrderDirection, AstError> {
    match dir {
        pg::SortByDir_SORTBY_ASC | pg::SortByDir_SORTBY_DEFAULT => Ok(OrderDirection::Asc),
        pg::SortByDir_SORTBY_DESC => Ok(OrderDirection::Desc),
        other => Err(AstError::UnsupportedFeature {
            feature: format!("ORDER BY direction: {other}"),
        }),
    }
}

fn null_order_map(n: pg::SortByNulls) -> Result<NullOrder, AstError> {
    match n {
        pg::SortByNulls_SORTBY_NULLS_DEFAULT => Ok(NullOrder::Default),
        pg::SortByNulls_SORTBY_NULLS_FIRST => Ok(NullOrder::NullsFirst),
        pg::SortByNulls_SORTBY_NULLS_LAST => Ok(NullOrder::NullsLast),
        other => Err(AstError::UnsupportedFeature {
            feature: format!("ORDER BY NULLS ordering: {other}"),
        }),
    }
}

fn sublink_type_map(t: pg::SubLinkType) -> Result<SubLinkType, AstError> {
    match t {
        pg::SubLinkType_EXISTS_SUBLINK => Ok(SubLinkType::Exists),
        pg::SubLinkType_ANY_SUBLINK => Ok(SubLinkType::Any),
        pg::SubLinkType_ALL_SUBLINK => Ok(SubLinkType::All),
        pg::SubLinkType_EXPR_SUBLINK => Ok(SubLinkType::Expr),
        other => Err(AstError::UnsupportedSubLinkType {
            sublink_type: format!("{other}"),
        }),
    }
}

// ---------- Literal / column / param extraction ----------

unsafe fn const_value_extract(c: *const pg::A_Const) -> Result<LiteralValue, WhereParseError> {
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
            pg::NodeTag_T_String => Ok(LiteralValue::String(cstr(val.sval.sval).to_owned())),
            pg::NodeTag_T_BitString => Ok(LiteralValue::String(cstr(val.bsval.bsval).to_owned())),
            _ => Ok(LiteralValue::Null),
        }
    }
}

unsafe fn column_ref_extract(col_ref: *const pg::ColumnRef) -> Result<ColumnNode, WhereParseError> {
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

unsafe fn param_ref_extract(param_ref: *const pg::ParamRef) -> LiteralValue {
    unsafe { LiteralValue::Parameter(format!("${}", (*param_ref).number)) }
}

// ---------- WHERE clause ----------

unsafe fn where_expr_convert(node: NodePtr) -> Result<WhereExpr, WhereParseError> {
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

unsafe fn sublink_convert(sub_link: *const pg::SubLink) -> Result<WhereExpr, WhereParseError> {
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

unsafe fn operator_name_string_extract<'a>(
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

unsafe fn sublink_all_operator_check(oper_name: *const pg::List) -> Result<(), WhereParseError> {
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

unsafe fn null_test_convert(null_test: *const pg::NullTest) -> Result<WhereExpr, WhereParseError> {
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

unsafe fn boolean_test_convert(
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

unsafe fn a_expr_convert(expr: *const pg::A_Expr) -> Result<WhereExpr, WhereParseError> {
    unsafe {
        let kind = (*expr).kind;
        let name = (*expr).name;
        let lexpr = (*expr).lexpr as NodePtr;
        let rexpr = (*expr).rexpr as NodePtr;

        match kind {
            pg::A_Expr_Kind_AEXPR_OP => {
                if arithmetic_op_extract(name).is_ok() {
                    let arith = aexpr_arithmetic_convert(expr)?;
                    return Ok(WhereExpr::Scalar(ScalarExpr::Arithmetic(arith)));
                }
                let op = operator_extract(name)?;
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

unsafe fn in_operator_extract(name: *const pg::List) -> Result<MultiOp, WhereParseError> {
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

unsafe fn in_list_extract(node: NodePtr) -> Result<Vec<WhereExpr>, WhereParseError> {
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

unsafe fn between_bounds_extract(node: NodePtr) -> Result<(WhereExpr, WhereExpr), WhereParseError> {
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

unsafe fn any_all_rexpr_convert(node: NodePtr) -> Result<WhereExpr, WhereParseError> {
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

unsafe fn like_operator_extract(name: *const pg::List) -> Result<BinaryOp, WhereParseError> {
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

unsafe fn operator_extract(name: *const pg::List) -> Result<BinaryOp, WhereParseError> {
    unsafe {
        let names: Vec<_> = list_nodes(name).collect();
        let [name_node] = names.as_slice() else {
            return Err(WhereParseError::Other {
                error: "Multi-part operator names not supported".to_owned(),
            });
        };
        match string_node_value(*name_node) {
            Some("=") => Ok(BinaryOp::Equal),
            Some("!=") | Some("<>") => Ok(BinaryOp::NotEqual),
            Some("<") => Ok(BinaryOp::LessThan),
            Some("<=") => Ok(BinaryOp::LessThanOrEqual),
            Some(">") => Ok(BinaryOp::GreaterThan),
            Some(">=") => Ok(BinaryOp::GreaterThanOrEqual),
            Some(op) => Err(WhereParseError::UnsupportedOperator {
                operator: op.to_owned(),
            }),
            None => Err(WhereParseError::Other {
                error: "Invalid operator name format".to_owned(),
            }),
        }
    }
}

unsafe fn bool_expr_convert(expr: *const pg::BoolExpr) -> Result<WhereExpr, WhereParseError> {
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

/// Parse SQL straight to a `QueryExpr` via the raw path. Test-only convenience
/// shared by unit tests across the crate that previously routed through the
/// (now-removed) protobuf converter.
#[cfg(test)]
pub(crate) fn query_expr_parse(sql: &str) -> Result<QueryExpr, AstError> {
    pg_query::parse_raw_scoped(sql, |tree| unsafe { query_expr_convert_raw(tree) })
        .expect("parse SQL")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Queries exercising every node kind the converter handles. Each must
    /// convert successfully and survive a deparse→reparse roundtrip.
    const CORPUS: &[&str] = &[
        // basic select / projection
        "SELECT id, name FROM users WHERE id = 1",
        "SELECT * FROM products",
        "SELECT t.* FROM users t",
        "SELECT $1 FROM users",
        "SELECT id AS user_id, name AS full_name FROM users",
        "SELECT id, name FROM test.users WHERE active = true",
        "SELECT u.id, u.name FROM users u WHERE u.active = true",
        "SELECT DISTINCT category FROM products",
        // where: comparisons, boolean, params, null
        "SELECT * FROM users WHERE name = 'john' AND active = true",
        "SELECT id FROM test WHERE str = 'hello' OR str = 'world'",
        "SELECT id FROM test WHERE NOT str = 'hello'",
        "SELECT id FROM test WHERE name = 'john' AND age > 25 AND active = true",
        "SELECT id FROM test WHERE id != 123 AND id <> 99 AND id < 5 AND id <= 5 AND id > 1 AND id >= 1",
        "SELECT id FROM test WHERE data = NULL",
        "SELECT id FROM test WHERE name = $1 AND age > $2",
        "SELECT id FROM test WHERE deleted_at IS NULL AND name IS NOT NULL",
        "SELECT id FROM test WHERE active IS TRUE AND a IS NOT TRUE AND b IS FALSE AND c IS NOT FALSE",
        "SELECT id FROM test WHERE active IS UNKNOWN OR active IS NOT UNKNOWN",
        // in / between / like / any / all
        "SELECT * FROM t WHERE status IN ('active', 'pending', 'complete')",
        "SELECT * FROM t WHERE id NOT IN (1, 2, 3)",
        "SELECT * FROM t WHERE n BETWEEN 1 AND 10",
        "SELECT * FROM t WHERE n NOT BETWEEN 1 AND 10",
        "SELECT id FROM test WHERE name LIKE 'test%' AND name NOT LIKE 'x%' AND name ILIKE 'A%'",
        "SELECT * FROM t WHERE id = ANY(ARRAY[1,2,3])",
        "SELECT * FROM t WHERE id = ANY($1)",
        "SELECT * FROM t WHERE id <> ALL (SELECT x FROM y)",
        // arithmetic
        "SELECT a + b, c - d, e * f, g / h, i % j FROM t",
        "SELECT id FROM t WHERE a + b = 10",
        // joins
        "SELECT * FROM invoice JOIN product p ON p.id = invoice.product_id",
        "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id WHERE a.id = 1",
        "SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id LEFT JOIN payments p ON o.id = p.order_id",
        "SELECT * FROM a CROSS JOIN b",
        "SELECT * FROM a NATURAL JOIN b",
        "SELECT * FROM a JOIN b USING (id)",
        "SELECT * FROM a RIGHT JOIN b ON a.id = b.id",
        "SELECT * FROM a FULL JOIN b ON a.id = b.id",
        // subqueries
        "SELECT invoice.id, (SELECT x.data FROM x WHERE 1 = 1) AS one FROM invoice",
        "SELECT * FROM (SELECT * FROM invoice WHERE id = 2) inv",
        "SELECT * FROM (VALUES(1, 2, 'test'), (3, 4, 'a')) v",
        "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.id = t.id)",
        "SELECT * FROM t WHERE id IN (SELECT id FROM u)",
        "SELECT * FROM t WHERE col = (SELECT max(x) FROM u)",
        // aggregates / functions / window
        "SELECT count(*), str FROM test GROUP BY str",
        "SELECT count(DISTINCT id) FROM t",
        "SELECT count(*) FILTER (WHERE active) FROM t",
        "SELECT array_agg(id ORDER BY id DESC) FROM t",
        "SELECT row_number() OVER (PARTITION BY dept ORDER BY salary DESC) FROM emp",
        "SELECT coalesce(a, b, 0), greatest(a, b), least(a, b), nullif(a, b) FROM t",
        // case / cast
        "SELECT CASE WHEN a = 1 THEN 'one' WHEN a = 2 THEN 'two' ELSE 'other' END FROM t",
        "SELECT CASE x WHEN 1 THEN 'a' ELSE 'b' END FROM t",
        "SELECT id::text, n::numeric(10,2), tags::int[] FROM t",
        "SELECT * FROM t WHERE created::date = '2020-01-01'",
        // order by / limit / having
        "SELECT id FROM t ORDER BY name ASC, created DESC NULLS LAST LIMIT 10 OFFSET 5",
        "SELECT id FROM t ORDER BY 1 LIMIT $1",
        "SELECT dept, count(*) FROM emp GROUP BY dept HAVING count(*) > 5",
        // set ops
        "SELECT a FROM t1 UNION SELECT a FROM t2",
        "SELECT a FROM t1 UNION ALL SELECT a FROM t2",
        "SELECT a FROM t1 INTERSECT SELECT a FROM t2",
        "SELECT a FROM t1 EXCEPT SELECT a FROM t2",
        // CTEs
        "WITH c AS (SELECT id FROM t WHERE x = 1) SELECT * FROM c",
        "WITH a AS (SELECT 1 AS x), b AS (SELECT x FROM a) SELECT * FROM b",
        "WITH c AS MATERIALIZED (SELECT id FROM t) SELECT * FROM c",
    ];

    #[test]
    fn corpus_converts_and_roundtrips() {
        let mut failures = Vec::new();
        for sql in CORPUS {
            let Ok(query) = query_expr_parse(sql) else {
                failures.push(format!("\nSQL: {sql}\n  did not convert"));
                continue;
            };
            // Deparse → reparse must yield the same QueryExpr (deparse fidelity).
            let mut buf = String::with_capacity(256);
            query.deparse(&mut buf);
            match query_expr_parse(&buf) {
                Ok(reparsed) if reparsed == query => {}
                other => failures.push(format!(
                    "\nSQL: {sql}\n  deparsed: {buf}\n  roundtrip: {other:?}"
                )),
            }
        }
        assert!(
            failures.is_empty(),
            "raw converter corpus failures ({}):{}",
            failures.len(),
            failures.join("")
        );
    }
}
