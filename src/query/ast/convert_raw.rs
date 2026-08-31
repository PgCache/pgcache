//! Build a [`QueryExpr`] directly from PostgreSQL's raw parse tree (the C node
//! structs from `pg_query::pg_nodes`), via the `pg_query::parse_raw_scoped`
//! callback — the proxy's single SQL-parsing path (PGC-192). It reads tagged C
//! node pointers through [`super::raw`] with no protobuf serialize/decode
//! round-trip. Must run inside the callback (the tree is freed when it returns).

#![allow(clippy::wildcard_enum_match_arm)]

use std::os::raw::c_void;

use ecow::EcoString;
use smallvec::SmallVec;

use pg_query::pg_nodes as pg;

use crate::query::cast::cast_target_from_canonical;
use crate::query::transform::query_expr_constant_fold;
use crate::query::write::{TransactionBoundary, WriteClass};

use super::raw::{NodePtr, cast, cstr, list_is_empty, list_nodes, node_tag, string_node_value};
use super::*;

mod where_clause;
mod window;
mod write;

use where_clause::{
    column_ref_extract, const_value_extract, param_ref_extract, where_expr_convert,
};
use window::{
    select_columns_window_refs_resolve, window_clause_extract, window_def_convert,
    window_order_by_convert, window_refs_assert_resolved,
};

/// Convert the root of a raw parse tree (`List *` of `RawStmt`, as an opaque
/// pointer from `parse_raw_scoped`) into a [`QueryExpr`].
///
/// # Safety
/// `tree_root` must be the live `List *` handed to the `parse_raw_scoped`
/// callback, valid for the duration of this call.
pub unsafe fn query_expr_convert_raw(tree_root: *const c_void) -> Result<QueryExpr, AstError> {
    unsafe {
        let stmt = root_statement(tree_root)?;
        match node_tag(stmt) {
            pg::NodeTag_T_SelectStmt => select_root_convert(cast::<pg::SelectStmt>(stmt)),
            other => Err(AstError::UnsupportedStatement {
                statement_type: format!("{other:?}"),
            }),
        }
    }
}

/// Root-statement classification for the proxy's analyze path: the SELECT
/// conversion result plus enough write classification to feed the
/// per-connection read-after-write log (PGC-124).
#[derive(Debug)]
pub enum RawStatement {
    /// Root was a `SelectStmt`. The converted expression is boxed to keep
    /// the enum near the size of its unit variants.
    Select {
        converted: Result<Box<QueryExpr>, AstError>,
        /// `Some` when conversion failed and the WITH clause contains a
        /// data-modifying CTE — the "select" writes.
        cte_write: Option<WriteClass>,
    },
    /// Root can modify table data (DML, DDL, EXECUTE, unknown, ...).
    Write(WriteClass),
    /// Root provably cannot modify table data (txn control, SET, SHOW, ...).
    ReadOnlyUtility {
        /// Set for transaction-control statements.
        transaction: Option<TransactionBoundary>,
    },
}

/// Classify the root of a raw parse tree, converting a `SelectStmt` root
/// exactly as [`query_expr_convert_raw`] and classifying everything else for
/// write tracking. Errs only on structural failures (multiple statements,
/// empty statement).
///
/// # Safety
/// `tree_root` must be the live `List *` handed to the `parse_raw_scoped`
/// callback, valid for the duration of this call.
pub unsafe fn statement_convert_raw(tree_root: *const c_void) -> Result<RawStatement, AstError> {
    unsafe {
        let stmt = root_statement(tree_root)?;
        Ok(match node_tag(stmt) {
            pg::NodeTag_T_SelectStmt => {
                let select = cast::<pg::SelectStmt>(stmt);
                let converted = select_root_convert(select).map(Box::new);
                let cte_write = if converted.is_err() {
                    write::select_cte_write_class(select)
                } else {
                    None
                };
                RawStatement::Select {
                    converted,
                    cte_write,
                }
            }
            _ => match write::non_select_classify(stmt) {
                write::NonSelectClass::Write(class) => RawStatement::Write(class),
                write::NonSelectClass::ReadOnly(transaction) => {
                    RawStatement::ReadOnlyUtility { transaction }
                }
            },
        })
    }
}

/// Unwrap the single root statement of a parse tree.
unsafe fn root_statement(tree_root: *const c_void) -> Result<NodePtr, AstError> {
    unsafe {
        let mut stmts = list_nodes(tree_root as *const pg::List);
        let (Some(raw_stmt), None) = (stmts.next(), stmts.next()) else {
            return Err(AstError::MultipleStatements);
        };

        let stmt = (*cast::<pg::RawStmt>(raw_stmt)).stmt as NodePtr;
        if stmt.is_null() {
            return Err(AstError::MissingStatement);
        }
        Ok(stmt)
    }
}

/// Convert a root `SelectStmt` including the post-conversion passes shared by
/// both entry points.
unsafe fn select_root_convert(select: *const pg::SelectStmt) -> Result<QueryExpr, AstError> {
    unsafe {
        let mut query = select_stmt_to_query_expr(select)?;
        window_refs_assert_resolved(&query)?;
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

pub(super) unsafe fn select_stmt_to_query_expr(
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
                let fields: SmallVec<[_; 4]> =
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
            pg::NodeTag_T_ParamRef => Ok(Some(LiteralValue::Parameter(
                format!("${}", (*cast::<pg::ParamRef>(node)).number).into(),
            ))),
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

pub(super) fn order_dir_map(dir: pg::SortByDir) -> Result<OrderDirection, AstError> {
    match dir {
        pg::SortByDir_SORTBY_ASC | pg::SortByDir_SORTBY_DEFAULT => Ok(OrderDirection::Asc),
        pg::SortByDir_SORTBY_DESC => Ok(OrderDirection::Desc),
        other => Err(AstError::UnsupportedFeature {
            feature: format!("ORDER BY direction: {other}"),
        }),
    }
}

pub(super) fn null_order_map(n: pg::SortByNulls) -> Result<NullOrder, AstError> {
    match n {
        pg::SortByNulls_SORTBY_NULLS_DEFAULT => Ok(NullOrder::Default),
        pg::SortByNulls_SORTBY_NULLS_FIRST => Ok(NullOrder::NullsFirst),
        pg::SortByNulls_SORTBY_NULLS_LAST => Ok(NullOrder::NullsLast),
        other => Err(AstError::UnsupportedFeature {
            feature: format!("ORDER BY NULLS ordering: {other}"),
        }),
    }
}

pub(super) fn sublink_type_map(t: pg::SubLinkType) -> Result<SubLinkType, AstError> {
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
