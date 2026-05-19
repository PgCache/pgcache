use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use pg_query::ParseResult;
use pg_query::protobuf::{
    AExpr, AExprKind, CaseExpr as PgCaseExpr, CoalesceExpr, CteMaterialize, FuncCall, JoinExpr,
    MinMaxExpr, MinMaxOp, RangeSubselect, SortByDir, SortByNulls, TypeCast, TypeName,
};
use pg_query::protobuf::{Node, RangeVar, SelectStmt, SetOperation, node::Node as NodeEnum};

use crate::query::transform::query_expr_constant_fold;

use super::expr_parse::{
    column_ref_extract, const_value_extract, node_convert_to_expr, param_ref_extract,
    select_stmt_parse_where,
};
use super::*;

/// Convert a pg_query ParseResult into a QueryExpr
pub fn query_expr_convert(ast: &ParseResult) -> Result<QueryExpr, AstError> {
    let [raw_stmt] = ast.protobuf.stmts.as_slice() else {
        return Err(AstError::MultipleStatements);
    };

    let stmt_node = raw_stmt.stmt.as_ref().ok_or(AstError::MissingStatement)?;

    let mut query = match stmt_node.node.as_ref() {
        Some(NodeEnum::SelectStmt(select_stmt)) => select_stmt_to_query_expr(select_stmt),
        Some(other) => Err(AstError::UnsupportedStatement {
            statement_type: format!("{other:?}"),
        }),
        None => Err(AstError::MissingStatement),
    }?;

    // Fold pure-literal arithmetic so different inline-literal queries that
    // reduce to the same value share a fingerprint (PGC-118).
    query_expr_constant_fold(&mut query);

    Ok(query)
}

/// Context for CTE-aware parsing. Tracks CTE definitions from an outer
/// WITH clause so that table references in the body can be recognized as
/// CTE references instead of catalog tables.
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

/// Extract CTE definitions from a WITH clause protobuf.
fn with_clause_extract(
    with_clause: &pg_query::protobuf::WithClause,
) -> Result<Vec<CteDefinition>, AstError> {
    if with_clause.recursive {
        return Err(AstError::UnsupportedFeature {
            feature: "WITH RECURSIVE".to_owned(),
        });
    }

    let mut ctes = Vec::new();

    for cte_node in &with_clause.ctes {
        let Some(NodeEnum::CommonTableExpr(cte)) = &cte_node.node else {
            return Err(AstError::UnsupportedFeature {
                feature: format!("WITH clause entry: {cte_node:?}"),
            });
        };

        if cte.cterecursive {
            return Err(AstError::UnsupportedFeature {
                feature: format!("recursive CTE: {}", cte.ctename),
            });
        }

        let materialization = match CteMaterialize::try_from(cte.ctematerialized) {
            Ok(CteMaterialize::Always) => CteMaterialization::Materialized,
            Ok(CteMaterialize::Never) => CteMaterialization::NotMaterialized,
            _ => CteMaterialization::Default,
        };

        let column_aliases = cte
            .aliascolnames
            .iter()
            .filter_map(|n| {
                if let Some(NodeEnum::String(s)) = &n.node {
                    Some(EcoString::from(s.sval.as_str()))
                } else {
                    None
                }
            })
            .collect();

        // Parse inner query with context of previously parsed CTEs
        // (supports CTE-referencing-CTE)
        let inner_select = cte
            .ctequery
            .as_ref()
            .and_then(|n| n.node.as_ref())
            .ok_or_else(|| AstError::UnsupportedFeature {
                feature: format!("CTE missing query: {}", cte.ctename),
            })?;

        let NodeEnum::SelectStmt(inner_stmt) = inner_select else {
            return Err(AstError::UnsupportedFeature {
                feature: format!("CTE query is not SELECT: {}", cte.ctename),
            });
        };

        let ctx = ParseContext { ctes: ctes.clone() };
        let query = select_stmt_to_query_expr_with_ctx(inner_stmt, &ctx)?;

        ctes.push(CteDefinition {
            name: EcoString::from(cte.ctename.as_str()),
            query,
            column_aliases,
            materialization,
        });
    }

    Ok(ctes)
}

/// Convert a SelectStmt to a QueryExpr, handling set operations.
/// Public entry point — creates an empty ParseContext.
pub fn select_stmt_to_query_expr(select_stmt: &SelectStmt) -> Result<QueryExpr, AstError> {
    let ctx = ParseContext::empty();
    select_stmt_to_query_expr_with_ctx(select_stmt, &ctx)
}

/// Convert a SelectStmt to a QueryExpr with CTE context.
fn select_stmt_to_query_expr_with_ctx(
    select_stmt: &SelectStmt,
    outer_ctx: &ParseContext,
) -> Result<QueryExpr, AstError> {
    // Locking clauses (FOR UPDATE, FOR SHARE, etc.) acquire row locks and must
    // not be cached — reject early before doing any further parsing work.
    if !select_stmt.locking_clause.is_empty() {
        return Err(AstError::UnsupportedSelectFeature {
            feature: "locking clause (FOR UPDATE/FOR SHARE)".to_owned(),
        });
    }

    // Parse WITH clause if present, merging with any outer CTE context
    let ctes = if let Some(with_clause) = &select_stmt.with_clause {
        with_clause_extract(with_clause)?
    } else {
        Vec::new()
    };

    // Build context: outer CTEs + this level's CTEs
    let mut all_ctes = outer_ctx.ctes.clone();
    all_ctes.extend(ctes.clone());
    let ctx = ParseContext { ctes: all_ctes };
    let set_op = SetOperation::try_from(select_stmt.op).unwrap_or(SetOperation::Undefined);

    // ORDER BY and LIMIT apply to the whole query expression
    let order_by = order_by_clause_convert(&select_stmt.sort_clause)?;
    let limit = limit_clause_convert(
        select_stmt.limit_count.as_deref(),
        select_stmt.limit_offset.as_deref(),
    )?;

    let body = match set_op {
        SetOperation::SetopNone | SetOperation::Undefined => {
            // Check if this is a VALUES clause or a SELECT
            if !select_stmt.values_lists.is_empty() {
                let rows = value_list_convert(&select_stmt.values_lists)?;
                QueryBody::Values(ValuesClause { rows })
            } else {
                let select_node = select_stmt_to_select_node(select_stmt, &ctx)?;
                QueryBody::Select(Box::new(select_node))
            }
        }
        SetOperation::SetopUnion | SetOperation::SetopIntersect | SetOperation::SetopExcept => {
            // Parse left and right arguments
            let larg = select_stmt
                .larg
                .as_ref()
                .ok_or_else(|| AstError::UnsupportedFeature {
                    feature: "SET operation without left argument".to_owned(),
                })?;
            let rarg = select_stmt
                .rarg
                .as_ref()
                .ok_or_else(|| AstError::UnsupportedFeature {
                    feature: "SET operation without right argument".to_owned(),
                })?;

            // Recursively convert left and right with CTE context
            let left = select_stmt_to_query_expr_with_ctx(larg, &ctx)?;
            let right = select_stmt_to_query_expr_with_ctx(rarg, &ctx)?;

            let op_type = match set_op {
                SetOperation::SetopUnion => SetOpType::Union,
                SetOperation::SetopIntersect => SetOpType::Intersect,
                SetOperation::SetopExcept => SetOpType::Except,
                // SetopNone and Undefined are handled in the outer match
                SetOperation::SetopNone | SetOperation::Undefined => unreachable!(),
            };

            QueryBody::SetOp(SetOpNode {
                op: op_type,
                all: select_stmt.all,
                left: Box::new(left),
                right: Box::new(right),
            })
        }
    };

    Ok(QueryExpr {
        ctes,
        body,
        order_by,
        limit,
    })
}

/// Convert a SelectStmt to a SelectNode (without ORDER BY/LIMIT)
fn select_stmt_to_select_node(
    select_stmt: &SelectStmt,
    ctx: &ParseContext,
) -> Result<SelectNode, AstError> {
    let columns = select_columns_convert(&select_stmt.target_list)?;
    let from = from_clause_convert(&select_stmt.from_clause, ctx)?;
    let where_clause = select_stmt_parse_where(select_stmt)?;
    let group_by = group_by_clause_convert(&select_stmt.group_clause)?;
    let having = having_clause_convert(select_stmt.having_clause.as_deref())?;

    Ok(SelectNode {
        distinct: !select_stmt.distinct_clause.is_empty(),
        columns,
        from,
        where_clause,
        group_by,
        having,
    })
}

fn value_list_convert(value_list: &[Node]) -> Result<Vec<Vec<LiteralValue>>, AstError> {
    let mut rv = Vec::with_capacity(value_list.len());
    for value in value_list {
        let mut row = Vec::new();
        if let Some(NodeEnum::List(node_list)) = &value.node {
            for item in &node_list.items {
                match &item.node {
                    Some(NodeEnum::AConst(const_val)) => {
                        row.push(const_value_extract(const_val)?);
                    }
                    other => {
                        return Err(AstError::UnsupportedFeature {
                            feature: format!("Value expression: {other:?}"),
                        });
                    }
                }
            }
        } else {
            return Err(AstError::UnsupportedFeature {
                feature: format!("Values: {value:?}"),
            });
        }
        rv.push(row);
    }

    Ok(rv)
}

fn select_columns_convert(target_list: &[Node]) -> Result<SelectColumns, AstError> {
    if target_list.is_empty() {
        return Ok(SelectColumns::None);
    }

    let mut columns = Vec::with_capacity(target_list.len());

    for target in target_list {
        if let Some(NodeEnum::ResTarget(res_target)) = &target.node
            && let Some(val_node) = &res_target.val
        {
            let alias = if res_target.name.is_empty() {
                None
            } else {
                Some(EcoString::from(res_target.name.as_str()))
            };

            match val_node.node.as_ref() {
                Some(NodeEnum::ColumnRef(col_ref)) => {
                    let fields = &col_ref.fields;

                    // Bare star: SELECT *
                    if let [field] = fields.as_slice()
                        && let Some(NodeEnum::AStar(_)) = &field.node
                    {
                        columns.push(SelectColumn::Star(None));
                        continue;
                    }

                    // Qualified star: SELECT t1.*
                    if fields.len() >= 2
                        && matches!(
                            fields.last().and_then(|f| f.node.as_ref()),
                            Some(NodeEnum::AStar(_))
                        )
                    {
                        let qualifier = fields
                            .get(fields.len() - 2)
                            .ok_or(AstError::InvalidTableRef)?;
                        let table = match qualifier.node.as_ref() {
                            Some(NodeEnum::String(s)) => EcoString::from(s.sval.as_str()),
                            _ => return Err(AstError::InvalidTableRef),
                        };
                        columns.push(SelectColumn::Star(Some(table)));
                        continue;
                    }

                    // Regular column reference
                    let column_ref = column_ref_extract(col_ref)?;

                    columns.push(SelectColumn::Expr {
                        expr: ScalarExpr::Column(column_ref),
                        alias,
                    });
                }
                Some(NodeEnum::SubLink(sub_link)) => {
                    match sub_link.subselect.as_ref().and_then(|n| n.node.as_ref()) {
                        Some(NodeEnum::SelectStmt(select_stmt)) => {
                            let query = select_stmt_to_query_expr(select_stmt)?;
                            columns.push(SelectColumn::Expr {
                                expr: ScalarExpr::Subquery(Box::new(query)),
                                alias,
                            });
                        }
                        other => {
                            return Err(AstError::UnsupportedSelectFeature {
                                feature: format!("Column expression: {other:?}"),
                            });
                        }
                    }
                }
                Some(NodeEnum::FuncCall(func_call)) => {
                    let function = func_call_convert(func_call)?;
                    columns.push(SelectColumn::Expr {
                        expr: ScalarExpr::Function(function),
                        alias,
                    });
                }
                Some(NodeEnum::AConst(const_val)) => {
                    let value = const_value_extract(const_val)?;
                    columns.push(SelectColumn::Expr {
                        expr: ScalarExpr::Literal(value),
                        alias,
                    });
                }
                Some(NodeEnum::CoalesceExpr(coalesce)) => {
                    let function = coalesce_expr_convert(coalesce)?;
                    columns.push(SelectColumn::Expr {
                        expr: ScalarExpr::Function(function),
                        alias,
                    });
                }
                Some(NodeEnum::MinMaxExpr(minmax)) => {
                    let function = minmax_expr_convert(minmax)?;
                    columns.push(SelectColumn::Expr {
                        expr: ScalarExpr::Function(function),
                        alias,
                    });
                }
                Some(NodeEnum::AExpr(aexpr))
                    if AExprKind::try_from(aexpr.kind) == Ok(AExprKind::AexprNullif) =>
                {
                    let function = aexpr_nullif_convert(aexpr)?;
                    columns.push(SelectColumn::Expr {
                        expr: ScalarExpr::Function(function),
                        alias,
                    });
                }
                Some(NodeEnum::AExpr(aexpr))
                    if AExprKind::try_from(aexpr.kind) == Ok(AExprKind::AexprOp) =>
                {
                    let arith = aexpr_arithmetic_convert(aexpr)?;
                    columns.push(SelectColumn::Expr {
                        expr: ScalarExpr::Arithmetic(arith),
                        alias,
                    });
                }
                Some(NodeEnum::CaseExpr(case_expr)) => {
                    let case = case_expr_convert(case_expr)?;
                    columns.push(SelectColumn::Expr {
                        expr: ScalarExpr::Case(case),
                        alias,
                    });
                }
                Some(NodeEnum::TypeCast(tc)) => {
                    let expr = type_cast_convert(tc)?;
                    columns.push(SelectColumn::Expr { expr, alias });
                }
                other => {
                    return Err(AstError::UnsupportedSelectFeature {
                        feature: format!("Column expression: {other:?}"),
                    });
                }
            }
        } else {
            return Err(AstError::UnsupportedSelectFeature {
                feature: format!("Target: {target:?}"),
            });
        }
    }

    Ok(SelectColumns::Columns(columns))
}

fn from_clause_convert(
    from_clause: &[Node],
    ctx: &ParseContext,
) -> Result<Vec<TableSource>, AstError> {
    let mut tables = Vec::with_capacity(from_clause.len());

    for from_node in from_clause {
        match &from_node.node {
            Some(NodeEnum::RangeVar(range_var)) => {
                let table_node = table_node_convert(range_var, ctx)?;
                tables.push(table_node);
            }
            Some(NodeEnum::RangeSubselect(range_subselect)) => {
                let table_subquery_node = table_subquery_node_convert(range_subselect, ctx)?;
                tables.push(table_subquery_node);
            }
            Some(NodeEnum::JoinExpr(join_expr)) => {
                let join_node = join_expr_convert(join_expr, ctx)?;
                tables.push(join_node);
            }
            _ => {
                return Err(AstError::UnsupportedSelectFeature {
                    feature: format!("FROM clause type: {from_node:?}"),
                });
            }
        }
    }

    Ok(tables)
}

fn join_arg_convert(node: &Node, side: &str, ctx: &ParseContext) -> Result<TableSource, AstError> {
    match &node.node {
        Some(NodeEnum::RangeVar(range_var)) => table_node_convert(range_var, ctx),
        Some(NodeEnum::RangeSubselect(range_subselect)) => {
            table_subquery_node_convert(range_subselect, ctx)
        }
        Some(NodeEnum::JoinExpr(nested_join)) => {
            // Recursively handle nested joins
            join_expr_convert(nested_join, ctx)
        }
        _ => Err(AstError::UnsupportedSelectFeature {
            feature: format!("join {side} argument: {node:?}"),
        }),
    }
}

fn join_expr_convert(join_expr: &JoinExpr, ctx: &ParseContext) -> Result<TableSource, AstError> {
    // Convert left argument - can be a table, subquery, or another join
    let left_table = if let Some(larg_node) = &join_expr.larg {
        join_arg_convert(larg_node, "left", ctx)?
    } else {
        return Err(AstError::UnsupportedSelectFeature {
            feature: "join missing left argument".to_owned(),
        });
    };

    // Convert right argument - can be a table, subquery, or another join
    let right_table = if let Some(rarg_node) = &join_expr.rarg {
        join_arg_convert(rarg_node, "right", ctx)?
    } else {
        return Err(AstError::UnsupportedSelectFeature {
            feature: "join missing right argument".to_owned(),
        });
    };

    // `ON` / `USING` / `NATURAL` / cross are mutually exclusive in the
    // grammar, so pg_query sets at most one of quals / using_clause /
    // is_natural; everything else is a (cross) cartesian join.
    let qual = if let Some(clause) = &join_expr.quals {
        JoinQual::On(node_convert_to_expr(clause)?)
    } else if !join_expr.using_clause.is_empty() {
        let cols = join_expr
            .using_clause
            .iter()
            .filter_map(|n| match &n.node {
                Some(NodeEnum::String(s)) => Some(EcoString::from(s.sval.as_str())),
                _ => None,
            })
            .collect();
        JoinQual::Using(cols)
    } else if join_expr.is_natural {
        JoinQual::Natural
    } else {
        JoinQual::Cross
    };

    Ok(TableSource::Join(JoinNode {
        join_type: JoinType::try_from(join_expr.jointype())?,
        left: Box::new(left_table),
        right: Box::new(right_table),
        qual,
    }))
}

fn table_node_convert(range_var: &RangeVar, ctx: &ParseContext) -> Result<TableSource, AstError> {
    let schema = if range_var.schemaname.is_empty() {
        None
    } else {
        Some(EcoString::from(range_var.schemaname.as_str()))
    };

    let name = EcoString::from(range_var.relname.as_str());

    let alias = range_var.alias.as_ref().map(|alias_node| TableAlias {
        name: EcoString::from(alias_node.aliasname.as_str()),
        columns: alias_node
            .colnames
            .iter()
            .flat_map(|n| {
                if let Some(NodeEnum::String(string_node)) = &n.node {
                    Some(EcoString::from(string_node.sval.as_str()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>(),
    });

    // Check if this is a CTE reference (unqualified name matching a CTE)
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

fn table_subquery_node_convert(
    range_subselect: &RangeSubselect,
    ctx: &ParseContext,
) -> Result<TableSource, AstError> {
    let Some(NodeEnum::SelectStmt(select_stmt)) = range_subselect
        .subquery
        .as_ref()
        .and_then(|n| n.node.as_ref())
    else {
        return Err(AstError::UnsupportedSelectFeature {
            feature: format!("{:?}", range_subselect.subquery),
        });
    };

    let query = select_stmt_to_query_expr_with_ctx(select_stmt, ctx)?;

    let alias = range_subselect.alias.as_ref().map(|alias_node| TableAlias {
        name: EcoString::from(alias_node.aliasname.as_str()),
        columns: alias_node
            .colnames
            .iter()
            .flat_map(|n| {
                if let Some(NodeEnum::String(string_node)) = &n.node {
                    Some(EcoString::from(string_node.sval.as_str()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>(),
    });

    Ok(TableSource::Subquery(TableSubqueryNode {
        lateral: range_subselect.lateral,
        query: Box::new(query),
        alias,
    }))
}

pub(super) fn node_convert_to_scalar_expr(node: &Node) -> Result<ScalarExpr, AstError> {
    match node.node.as_ref() {
        Some(NodeEnum::ColumnRef(col_ref)) => {
            let column = column_ref_extract(col_ref)?;
            Ok(ScalarExpr::Column(column))
        }
        Some(NodeEnum::AConst(const_val)) => {
            let value = const_value_extract(const_val)?;
            Ok(ScalarExpr::Literal(value))
        }
        Some(NodeEnum::ParamRef(param_ref)) => {
            let value = param_ref_extract(param_ref);
            Ok(ScalarExpr::Literal(value))
        }
        Some(NodeEnum::SubLink(sub_link)) => {
            match sub_link.subselect.as_ref().and_then(|n| n.node.as_ref()) {
                Some(NodeEnum::SelectStmt(select_stmt)) => {
                    let query = select_stmt_to_query_expr(select_stmt)?;
                    Ok(ScalarExpr::Subquery(Box::new(query)))
                }
                other => Err(AstError::UnsupportedFeature {
                    feature: format!("Sublink type: {other:?}"),
                }),
            }
        }
        Some(NodeEnum::FuncCall(func_call)) => {
            let function = func_call_convert(func_call)?;
            Ok(ScalarExpr::Function(function))
        }
        Some(NodeEnum::CoalesceExpr(coalesce)) => {
            let function = coalesce_expr_convert(coalesce)?;
            Ok(ScalarExpr::Function(function))
        }
        Some(NodeEnum::MinMaxExpr(minmax)) => {
            let function = minmax_expr_convert(minmax)?;
            Ok(ScalarExpr::Function(function))
        }
        Some(NodeEnum::AExpr(aexpr))
            if AExprKind::try_from(aexpr.kind) == Ok(AExprKind::AexprNullif) =>
        {
            let function = aexpr_nullif_convert(aexpr)?;
            Ok(ScalarExpr::Function(function))
        }
        Some(NodeEnum::AExpr(aexpr))
            if AExprKind::try_from(aexpr.kind) == Ok(AExprKind::AexprOp) =>
        {
            let arith = aexpr_arithmetic_convert(aexpr)?;
            Ok(ScalarExpr::Arithmetic(arith))
        }
        Some(NodeEnum::CaseExpr(case_expr)) => {
            let case = case_expr_convert(case_expr)?;
            Ok(ScalarExpr::Case(case))
        }
        Some(NodeEnum::TypeCast(tc)) => type_cast_convert(tc),
        other => Err(AstError::UnsupportedFeature {
            feature: format!("Column expression node: {other:?}"),
        }),
    }
}

fn type_cast_convert(tc: &TypeCast) -> Result<ScalarExpr, AstError> {
    let arg = tc
        .arg
        .as_ref()
        .ok_or_else(|| AstError::UnsupportedFeature {
            feature: "TypeCast missing argument".to_owned(),
        })?;
    let inner = node_convert_to_scalar_expr(arg)?;
    let type_name = tc
        .type_name
        .as_ref()
        .ok_or_else(|| AstError::UnsupportedFeature {
            feature: "TypeCast missing type name".to_owned(),
        })?;
    let target_type = type_name_render(type_name)?;
    Ok(ScalarExpr::TypeCast {
        expr: Box::new(inner),
        target_type,
    })
}

/// Render a pg_query `TypeName` to a canonical SQL string (e.g. `int4`,
/// `numeric(10,2)`, `text`). Strips the implicit `pg_catalog.` qualifier
/// PostgreSQL adds for built-in types.
fn type_name_render(tn: &TypeName) -> Result<EcoString, AstError> {
    let mut parts: Vec<&str> = Vec::with_capacity(tn.names.len());
    for n in &tn.names {
        match n.node.as_ref() {
            Some(NodeEnum::String(s)) => parts.push(s.sval.as_str()),
            other => {
                return Err(AstError::UnsupportedFeature {
                    feature: format!("TypeName component: {other:?}"),
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

    if !tn.typmods.is_empty() {
        let mut typmod_strs: Vec<String> = Vec::with_capacity(tn.typmods.len());
        for tm in &tn.typmods {
            match tm.node.as_ref() {
                Some(NodeEnum::AConst(c)) => {
                    let lit = const_value_extract(c).map_err(|_| AstError::UnsupportedFeature {
                        feature: "TypeName typmod literal".to_owned(),
                    })?;
                    let mut buf = String::new();
                    lit.deparse(&mut buf);
                    typmod_strs.push(buf);
                }
                other => {
                    return Err(AstError::UnsupportedFeature {
                        feature: format!("TypeName typmod: {other:?}"),
                    });
                }
            }
        }
        out.push('(');
        out.push_str(&typmod_strs.join(","));
        out.push(')');
    }

    for _ in &tn.array_bounds {
        out.push_str("[]");
    }

    Ok(EcoString::from(out))
}

fn func_call_convert(func_call: &FuncCall) -> Result<FunctionCall, AstError> {
    // Extract function name - last component of qualified name (e.g., "pg_catalog.count" -> "count")
    let name = func_call
        .funcname
        .iter()
        .filter_map(|n| match &n.node {
            Some(NodeEnum::String(s)) => Some(EcoString::from(s.sval.as_str())),
            _ => None,
        })
        .next_back()
        .ok_or_else(|| AstError::UnsupportedSelectFeature {
            feature: "function with no name".to_owned(),
        })?;

    // Handle COUNT(*) - agg_star means no explicit args
    let args = if func_call.agg_star {
        vec![]
    } else {
        func_call
            .args
            .iter()
            .map(node_convert_to_scalar_expr)
            .collect::<Result<Vec<_>, _>>()?
    };

    // Parse aggregate ORDER BY (same structure as window ORDER BY)
    let agg_order = window_order_by_convert(&func_call.agg_order)?;

    // Parse FILTER (WHERE ...) — per-aggregate predicate. Reuses WHERE-clause
    // parsing since the grammar is identical (a boolean expression).
    let agg_filter = func_call
        .agg_filter
        .as_deref()
        .map(node_convert_to_expr)
        .transpose()?
        .map(Box::new);

    // Parse OVER clause for window functions
    let over = func_call
        .over
        .as_ref()
        .map(|win_def| window_def_convert(win_def))
        .transpose()?;

    Ok(FunctionCall {
        name,
        args,
        agg_star: func_call.agg_star,
        agg_distinct: func_call.agg_distinct,
        agg_order,
        agg_filter,
        over,
    })
}

/// Convert a pg_query WindowDef to our WindowSpec
fn window_def_convert(win_def: &pg_query::protobuf::WindowDef) -> Result<WindowSpec, AstError> {
    // Convert PARTITION BY columns
    let partition_by = win_def
        .partition_clause
        .iter()
        .map(node_convert_to_scalar_expr)
        .collect::<Result<Vec<_>, _>>()?;

    // Convert ORDER BY clauses
    let order_by = window_order_by_convert(&win_def.order_clause)?;

    Ok(WindowSpec {
        partition_by,
        order_by,
    })
}

/// Convert window ORDER BY clause (similar to regular ORDER BY but from window context)
fn window_order_by_convert(
    order_clause: &[pg_query::protobuf::Node],
) -> Result<Vec<OrderByClause>, AstError> {
    let mut order_by = Vec::with_capacity(order_clause.len());
    for sort_node in order_clause {
        if let Some(NodeEnum::SortBy(sort_by)) = &sort_node.node {
            order_by.push(sort_by_to_order_clause(sort_by)?);
        } else {
            return Err(AstError::UnsupportedFeature {
                feature: format!("Window ORDER BY node type: {sort_node:?}"),
            });
        }
    }
    Ok(order_by)
}

/// Convert one pg_query `SortBy` to an `OrderByClause` (expr + direction
/// + NULLS ordering). Shared between regular and window ORDER BY paths.
fn sort_by_to_order_clause(
    sort_by: &pg_query::protobuf::SortBy,
) -> Result<OrderByClause, AstError> {
    let expr_node = sort_by.node.as_ref().ok_or(AstError::UnsupportedFeature {
        feature: "ORDER BY without expression".to_owned(),
    })?;
    let expr = node_convert_to_scalar_expr(expr_node)?;
    let direction =
        OrderDirection::try_from(SortByDir::try_from(sort_by.sortby_dir).map_err(|_| {
            AstError::UnsupportedFeature {
                feature: format!("Invalid SortByDir value: {}", sort_by.sortby_dir),
            }
        })?)?;
    let null_order =
        NullOrder::try_from(SortByNulls::try_from(sort_by.sortby_nulls).map_err(|_| {
            AstError::UnsupportedFeature {
                feature: format!("Invalid SortByNulls value: {}", sort_by.sortby_nulls),
            }
        })?)?;
    Ok(OrderByClause {
        expr,
        direction,
        null_order,
    })
}

fn coalesce_expr_convert(coalesce: &CoalesceExpr) -> Result<FunctionCall, AstError> {
    let args = coalesce
        .args
        .iter()
        .map(node_convert_to_scalar_expr)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(FunctionCall {
        name: EcoString::from("coalesce"),
        args,
        agg_star: false,
        agg_distinct: false,
        agg_order: vec![],
        agg_filter: None,
        over: None,
    })
}

fn minmax_expr_convert(minmax: &MinMaxExpr) -> Result<FunctionCall, AstError> {
    let name = match MinMaxOp::try_from(minmax.op) {
        Ok(MinMaxOp::IsGreatest) => "greatest",
        Ok(MinMaxOp::IsLeast) => "least",
        _ => {
            return Err(AstError::UnsupportedFeature {
                feature: format!("Unknown MinMaxOp: {}", minmax.op),
            });
        }
    };

    let args = minmax
        .args
        .iter()
        .map(node_convert_to_scalar_expr)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(FunctionCall {
        name: EcoString::from(name),
        args,
        agg_star: false,
        agg_distinct: false,
        agg_order: vec![],
        agg_filter: None,
        over: None,
    })
}

fn aexpr_nullif_convert(aexpr: &AExpr) -> Result<FunctionCall, AstError> {
    let mut args = Vec::with_capacity(2);

    if let Some(lexpr) = &aexpr.lexpr {
        args.push(node_convert_to_scalar_expr(lexpr)?);
    }
    if let Some(rexpr) = &aexpr.rexpr {
        args.push(node_convert_to_scalar_expr(rexpr)?);
    }

    Ok(FunctionCall {
        name: EcoString::from("nullif"),
        args,
        agg_star: false,
        agg_distinct: false,
        agg_order: vec![],
        agg_filter: None,
        over: None,
    })
}

pub(super) fn aexpr_arithmetic_convert(aexpr: &AExpr) -> Result<ArithmeticExpr, AstError> {
    // Extract operator from name field
    let op = arithmetic_op_extract(&aexpr.name)?;

    let left = aexpr
        .lexpr
        .as_ref()
        .ok_or_else(|| AstError::UnsupportedFeature {
            feature: "arithmetic expression without left operand".to_owned(),
        })
        .and_then(|n| node_convert_to_scalar_expr(n))?;

    let right = aexpr
        .rexpr
        .as_ref()
        .ok_or_else(|| AstError::UnsupportedFeature {
            feature: "arithmetic expression without right operand".to_owned(),
        })
        .and_then(|n| node_convert_to_scalar_expr(n))?;

    Ok(ArithmeticExpr {
        left: Box::new(left),
        op,
        right: Box::new(right),
    })
}

pub(super) fn arithmetic_op_extract(
    name_nodes: &[pg_query::Node],
) -> Result<ArithmeticOp, AstError> {
    let [name_node] = name_nodes else {
        return Err(AstError::UnsupportedFeature {
            feature: "multi-part operator names in arithmetic".to_owned(),
        });
    };

    match name_node.node.as_ref() {
        Some(NodeEnum::String(s)) => match s.sval.as_str() {
            "+" => Ok(ArithmeticOp::Add),
            "-" => Ok(ArithmeticOp::Subtract),
            "*" => Ok(ArithmeticOp::Multiply),
            "/" => Ok(ArithmeticOp::Divide),
            "%" => Ok(ArithmeticOp::Modulo),
            op => Err(AstError::UnsupportedFeature {
                feature: format!("arithmetic operator: {op}"),
            }),
        },
        _ => Err(AstError::UnsupportedFeature {
            feature: "invalid operator name format".to_owned(),
        }),
    }
}

fn case_expr_convert(case_expr: &PgCaseExpr) -> Result<CaseExpr, AstError> {
    // Convert optional arg (for simple CASE: CASE expr WHEN val...)
    let arg = case_expr
        .arg
        .as_ref()
        .map(|n| node_convert_to_scalar_expr(n))
        .transpose()?
        .map(Box::new);

    // Convert WHEN clauses
    let whens = case_expr
        .args
        .iter()
        .map(case_when_convert)
        .collect::<Result<Vec<_>, _>>()?;

    // Convert ELSE clause
    let default = case_expr
        .defresult
        .as_ref()
        .map(|n| node_convert_to_scalar_expr(n))
        .transpose()?
        .map(Box::new);

    Ok(CaseExpr {
        arg,
        whens,
        default,
    })
}

fn case_when_convert(node: &Node) -> Result<CaseWhen, AstError> {
    let Some(NodeEnum::CaseWhen(case_when)) = &node.node else {
        return Err(AstError::UnsupportedFeature {
            feature: format!("Expected CaseWhen, got: {:?}", node.node),
        });
    };

    let condition = case_when
        .expr
        .as_ref()
        .ok_or_else(|| AstError::UnsupportedFeature {
            feature: "CASE WHEN without condition".to_owned(),
        })
        .and_then(|n| node_convert_to_expr(n).map_err(AstError::from))?;

    let result = case_when
        .result
        .as_ref()
        .ok_or_else(|| AstError::UnsupportedFeature {
            feature: "CASE WHEN without result".to_owned(),
        })
        .and_then(|n| node_convert_to_scalar_expr(n))?;

    Ok(CaseWhen { condition, result })
}

fn order_by_clause_convert(sort_clause: &[Node]) -> Result<Vec<OrderByClause>, AstError> {
    let mut order_by = Vec::with_capacity(sort_clause.len());
    for sort_node in sort_clause {
        if let Some(NodeEnum::SortBy(sort_by)) = &sort_node.node {
            order_by.push(sort_by_to_order_clause(sort_by)?);
        } else {
            return Err(AstError::UnsupportedFeature {
                feature: format!("ORDER BY node type: {sort_node:?}"),
            });
        }
    }
    Ok(order_by)
}

fn group_by_clause_convert(group_clause: &[Node]) -> Result<Vec<ColumnNode>, AstError> {
    let mut group_by = Vec::with_capacity(group_clause.len());
    for node in group_clause {
        if let Some(NodeEnum::ColumnRef(col_ref)) = &node.node {
            group_by.push(column_ref_extract(col_ref)?);
        } else {
            return Err(AstError::UnsupportedFeature {
                feature: format!("GROUP BY expression: {node:?}"),
            });
        }
    }
    Ok(group_by)
}

fn having_clause_convert(having_clause: Option<&Node>) -> Result<Option<WhereExpr>, AstError> {
    match having_clause {
        Some(node) => Ok(Some(node_convert_to_expr(node)?)),
        None => Ok(None),
    }
}

fn limit_clause_convert(
    limit_count: Option<&Node>,
    limit_offset: Option<&Node>,
) -> Result<Option<LimitClause>, AstError> {
    let count = limit_node_extract(limit_count)?;
    let offset = limit_node_extract(limit_offset)?;

    if count.is_none() && offset.is_none() {
        return Ok(None);
    }

    Ok(Some(LimitClause { count, offset }))
}

fn limit_node_extract(node: Option<&Node>) -> Result<Option<LiteralValue>, AstError> {
    let Some(node) = node else { return Ok(None) };

    match &node.node {
        Some(NodeEnum::AConst(const_val)) => {
            let value = const_value_extract(const_val)?;
            match value {
                LiteralValue::Integer(_) => Ok(Some(value)),
                LiteralValue::String(_)
                | LiteralValue::StringWithCast(..)
                | LiteralValue::Float(_)
                | LiteralValue::Boolean(_)
                | LiteralValue::Null
                | LiteralValue::NullWithCast(_)
                | LiteralValue::Parameter(_)
                | LiteralValue::Array(_, _) => Err(AstError::UnsupportedFeature {
                    feature: format!("LIMIT/OFFSET value: {value:?}"),
                }),
            }
        }
        Some(NodeEnum::ParamRef(param_ref)) => Ok(Some(LiteralValue::Parameter(format!(
            "${}",
            param_ref.number
        )))),
        other => Err(AstError::UnsupportedFeature {
            feature: format!("LIMIT/OFFSET expression: {other:?}"),
        }),
    }
}

/// Generate a fingerprint hash for a SelectNode.
/// This is used for cache key generation.
pub fn select_node_fingerprint(node: &SelectNode) -> u64 {
    let mut hasher = DefaultHasher::new();
    node.hash(&mut hasher);
    hasher.finish()
}

/// Generate a fingerprint hash for a QueryExpr.
/// This is used for cache key generation.
///
/// Intentionally excludes LIMIT/OFFSET so that queries differing only
/// in LIMIT/OFFSET share a cache entry. The cache coordinator tracks
/// `max_limit` separately to decide when cached rows are sufficient.
pub fn query_expr_fingerprint(query: &QueryExpr) -> u64 {
    let mut hasher = DefaultHasher::new();
    query.ctes.hash(&mut hasher);
    query.body.hash(&mut hasher);
    query.order_by.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {

    #![allow(clippy::wildcard_enum_match_arm)]

    use std::{collections::HashSet, panic};

    use ecow::EcoString;
    use ordered_float::NotNan;

    use crate::cache::{SubqueryKind, UpdateQuerySource};
    use crate::query::ast::*;

    use super::*;

    /// Parse SQL and return a QueryExpr (new type)
    fn parse_query(sql: &str) -> QueryExpr {
        let pg_ast = pg_query::parse(sql).expect("parse SQL");
        query_expr_convert(&pg_ast).expect("convert to QueryExpr")
    }

    /// Parse SQL and return a SelectNode (for tests that need direct access)
    fn parse_select(sql: &str) -> SelectNode {
        let query = parse_query(sql);
        match query.body {
            QueryBody::Select(node) => *node,
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn test_query_expr_convert_simple_select() {
        let ast = parse_query("SELECT id, name FROM users WHERE id = 1");

        assert!(ast.is_single_table());
        assert!(ast.has_where_clause());
        assert_eq!(
            ast.nodes::<TableNode>()
                .map(|t| (t.schema.as_deref(), t.name.as_str()))
                .collect::<HashSet<_>>(),
            HashSet::<(Option<&str>, _)>::from([(None, "users")])
        );
    }

    #[test]
    fn test_query_expr_convert_select_star() {
        let ast = parse_query("SELECT * FROM products");

        assert!(ast.is_single_table());
        assert!(!ast.has_where_clause());
        assert_eq!(
            ast.nodes::<TableNode>()
                .map(|t| (t.schema.as_deref(), t.name.as_str()))
                .collect::<HashSet<_>>(),
            HashSet::<(Option<&str>, _)>::from([(None, "products")])
        );
    }

    #[test]
    fn test_query_expr_convert_where_clause() {
        let ast = parse_query("SELECT * FROM users WHERE name = 'john' AND active = true");

        assert!(ast.has_where_clause());
        let where_clause = ast.where_clause().unwrap();

        // Should convert the same WHERE clause as before
        // (reusing existing WhereExpr conversion)
        assert!(matches!(where_clause, WhereExpr::Binary(_)));
    }

    #[test]
    fn test_query_expr_convert_table_schema() {
        let select = parse_select("SELECT id, name FROM test.users WHERE active = true");

        assert_eq!(select.from.len(), 1);

        let TableSource::Table(table) = &select.from[0] else {
            panic!("expected table");
        };

        assert_eq!(table.schema, Some(EcoString::from("test")));
        assert_eq!(table.name, "users");
        assert_eq!(table.alias, None);

        // Check column references
        if let SelectColumns::Columns(columns) = &select.columns {
            assert_eq!(columns.len(), 2);

            // First column: id
            if let ScalarExpr::Column(col_ref) = &columns[0].expr().expect("non-star SELECT column")
            {
                assert_eq!(col_ref.table, None);
                assert_eq!(col_ref.column, "id");
            }

            // Second column: name
            if let ScalarExpr::Column(col_ref) = &columns[1].expr().expect("non-star SELECT column")
            {
                assert_eq!(col_ref.table, None);
                assert_eq!(col_ref.column, "name");
            }
        }
    }

    #[test]
    fn test_query_expr_convert_table_alias() {
        let select = parse_select("SELECT u.id, u.name FROM users u WHERE u.active = true");

        assert_eq!(select.from.len(), 1);

        let TableSource::Table(table) = &select.from[0] else {
            panic!("expected table");
        };

        assert_eq!(table.name, "users");
        assert_eq!(table.alias.as_ref().unwrap().name, "u");
        assert!(table.alias.as_ref().unwrap().columns.is_empty());

        // Check column references
        if let SelectColumns::Columns(columns) = &select.columns {
            assert_eq!(columns.len(), 2);

            // First column: u.id
            if let ScalarExpr::Column(col_ref) = &columns[0].expr().expect("non-star SELECT column")
            {
                assert_eq!(col_ref.table, Some(EcoString::from("u")));
                assert_eq!(col_ref.column, "id");
            }

            // Second column: u.name
            if let ScalarExpr::Column(col_ref) = &columns[1].expr().expect("non-star SELECT column")
            {
                assert_eq!(col_ref.table, Some(EcoString::from("u")));
                assert_eq!(col_ref.column, "name");
            }
        }
    }

    #[test]
    fn test_query_expr_convert_column_alias() {
        let select = parse_select("SELECT id as user_id, name as full_name FROM users");

        if let SelectColumns::Columns(columns) = &select.columns {
            assert_eq!(columns.len(), 2);

            // First column: id as user_id
            assert_eq!(
                columns[0].alias().cloned(),
                Some(EcoString::from("user_id"))
            );
            if let ScalarExpr::Column(col_ref) = &columns[0].expr().expect("non-star SELECT column")
            {
                assert_eq!(col_ref.column, "id");
            }

            // Second column: name as full_name
            assert_eq!(
                columns[1].alias().cloned(),
                Some(EcoString::from("full_name"))
            );
            if let ScalarExpr::Column(col_ref) = &columns[1].expr().expect("non-star SELECT column")
            {
                assert_eq!(col_ref.column, "name");
            }
        }
    }

    #[test]
    fn test_query_expr_convert_no_alias() {
        let select = parse_select("SELECT id, name FROM users");

        assert_eq!(select.from.len(), 1);

        let TableSource::Table(table) = &select.from[0] else {
            panic!("expected table");
        };

        // Table should have no alias
        assert_eq!(table.alias, None);

        // Columns should have no aliases
        if let SelectColumns::Columns(columns) = &select.columns {
            assert_eq!(columns[0].alias().cloned(), None);
            assert_eq!(columns[1].alias().cloned(), None);
        }
    }

    #[test]
    fn test_query_expr_join() {
        let ast = parse_query("SELECT * FROM invoice JOIN product p ON p.id = invoice.product_id");

        assert_eq!(
            ast.nodes::<TableNode>()
                .map(|t| (t.schema.as_deref(), t.name.as_str(), t.alias.as_ref()))
                .collect::<HashSet<_>>(),
            HashSet::<(Option<&str>, _, _)>::from([
                (None, "invoice", None),
                (
                    None,
                    "product",
                    Some(&TableAlias {
                        name: EcoString::from("p"),
                        columns: Vec::new()
                    })
                )
            ])
        );
    }

    #[test]
    fn test_query_expr_multiple_joins_two_tables() {
        let ast = parse_query(
            "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id WHERE a.id = 1",
        );

        // Should parse successfully with three tables
        let tables: Vec<&TableNode> = ast.nodes().collect();
        assert_eq!(tables.len(), 3);
        assert_eq!(tables[0].name, "a");
        assert_eq!(tables[1].name, "b");
        assert_eq!(tables[2].name, "c");

        // Should have 2 join nodes (nested structure)
        let joins: Vec<&JoinNode> = ast.nodes().collect();
        assert_eq!(joins.len(), 2);
        assert_eq!(joins[0].join_type, JoinType::Inner);
        assert_eq!(joins[1].join_type, JoinType::Inner);

        // Verify each join's condition contains the expected columns
        // Note: We count each join's condition separately to avoid double-counting
        // columns from nested joins (since joins can be nested in the AST)
        let mut col_count = 0;
        for join in &joins {
            if let JoinQual::On(condition) = &join.qual {
                col_count += condition.nodes::<ColumnNode>().count();
            }
        }
        assert_eq!(col_count, 4); // a.id, b.id from first join + b.id, c.id from second join
    }

    #[test]
    fn test_query_expr_multiple_joins_three_tables() {
        let ast = parse_query(
            "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id JOIN d ON c.id = d.id",
        );

        // Should parse successfully with four tables
        let tables: Vec<&TableNode> = ast.nodes().collect();
        assert_eq!(tables.len(), 4);
        assert_eq!(tables[0].name, "a");
        assert_eq!(tables[1].name, "b");
        assert_eq!(tables[2].name, "c");
        assert_eq!(tables[3].name, "d");

        // Should have 3 join nodes (deeply nested structure)
        let joins: Vec<&JoinNode> = ast.nodes().collect();
        assert_eq!(joins.len(), 3);
        assert!(joins.iter().all(|j| j.join_type == JoinType::Inner));
    }

    #[test]
    fn test_query_expr_mixed_join_types() {
        let ast = parse_query(
            "SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id LEFT JOIN payments p ON o.id = p.order_id",
        );

        // Should parse successfully with three tables
        let tables: Vec<&TableNode> = ast.nodes().collect();
        assert_eq!(tables.len(), 3);
        assert_eq!(tables[0].name, "users");
        assert_eq!(tables[1].name, "orders");
        assert_eq!(tables[2].name, "payments");

        // Should have 2 join nodes with different types
        // Note: Joins are returned in traversal order (outer join first, then nested joins)
        // For this query, PostgreSQL creates: (users INNER JOIN orders) LEFT JOIN payments
        // So the outer LEFT join is returned first, then the inner INNER join
        let joins: Vec<&JoinNode> = ast.nodes().collect();
        assert_eq!(joins.len(), 2);
        assert_eq!(joins[0].join_type, JoinType::Left); // Outer join
        assert_eq!(joins[1].join_type, JoinType::Inner); // Nested join
    }

    #[test]
    fn test_query_expr_multiple_joins_deparse() {
        let ast = parse_query("SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id");

        let mut buf = String::with_capacity(1024);
        ast.deparse(&mut buf);

        // Parse the deparsed SQL to verify it's valid
        let ast2 = parse_query(&buf);

        // Should produce identical AST
        assert_eq!(ast, ast2);
    }

    #[test]
    fn test_query_expr_select_subquery() {
        let select = parse_select(
            "SELECT invoice.id, (SELECT x.data FROM x WHERE 1 = 1) as one FROM invoice",
        );

        let SelectColumns::Columns(columns) = &select.columns else {
            panic!();
        };

        assert_eq!(
            columns[0],
            SelectColumn::Expr {
                expr: ScalarExpr::Column(ColumnNode {
                    table: Some(EcoString::from("invoice")),
                    column: EcoString::from("id")
                }),
                alias: None,
            }
        );

        assert!(matches!(
            columns[1].expr().expect("non-star SELECT column"),
            ScalarExpr::Subquery(_)
        ));
        assert_eq!(columns[1].alias().cloned(), Some(EcoString::from("one")));
    }

    #[test]
    fn test_query_expr_table_subquery() {
        let select = parse_select("SELECT * FROM (SELECT * FROM invoice WHERE id = 2) inv");

        assert_eq!(select.from.len(), 1);

        let TableSource::Subquery(subquery) = &select.from[0] else {
            panic!("expected subquery");
        };

        assert!(!subquery.lateral);
        assert_eq!(subquery.alias.as_ref().unwrap().name, "inv");
        assert!(subquery.alias.as_ref().unwrap().columns.is_empty());
    }

    #[test]
    fn test_query_expr_values() {
        let select = parse_select("SELECT * FROM (VALUES(1, 2, 'test'), (3, 4, 'a')) v");

        assert_eq!(select.from.len(), 1);

        let TableSource::Subquery(subquery) = &select.from[0] else {
            panic!("expected subquery");
        };

        assert!(!subquery.lateral);
        assert_eq!(subquery.alias.as_ref().unwrap().name, "v");

        let QueryBody::Values(values_clause) = &subquery.query.body else {
            panic!("expected VALUES clause in subquery");
        };
        assert_eq!(values_clause.rows.len(), 2);
        assert_eq!(values_clause.rows[0].len(), 3);
        assert_eq!(values_clause.rows[1].len(), 3);
    }

    #[test]
    fn test_query_expr_deparse_simple() {
        let sql = "SELECT id, name FROM users";
        let ast = parse_query(sql);

        let mut buf = String::with_capacity(1024);
        ast.deparse(&mut buf);
        assert_eq!(buf, sql);
    }

    #[test]
    fn test_literal_value_deparse() {
        let mut buf = String::new();

        // String literal
        LiteralValue::String("hello".to_owned()).deparse(&mut buf);
        assert_eq!(buf, "'hello'");
        buf.clear();

        // Integer literal
        LiteralValue::Integer(42).deparse(&mut buf);
        assert_eq!(buf, "42");
        buf.clear();

        // Float literal
        LiteralValue::Float(NotNan::new(3.25).unwrap()).deparse(&mut buf);
        assert_eq!(buf, "3.25");
        buf.clear();

        // Boolean literals
        LiteralValue::Boolean(true).deparse(&mut buf);
        assert_eq!(buf, "true");
        buf.clear();

        LiteralValue::Boolean(false).deparse(&mut buf);
        assert_eq!(buf, "false");
        buf.clear();

        // NULL literal
        LiteralValue::Null.deparse(&mut buf);
        assert_eq!(buf, "NULL");
        buf.clear();

        // Parameter
        LiteralValue::Parameter("$1".to_owned()).deparse(&mut buf);
        assert_eq!(buf, "$1");
    }

    #[test]
    fn test_table_node_deparse() {
        let mut buf = String::new();

        // Simple table
        TableNode {
            schema: None,
            name: EcoString::from("users"),
            alias: None,
        }
        .deparse(&mut buf);
        assert_eq!(buf, " users");
        buf.clear();

        // Qualified table with alias
        TableNode {
            schema: Some(EcoString::from("public")),
            name: EcoString::from("users"),
            alias: Some(TableAlias {
                name: EcoString::from("alias"),
                columns: vec![],
            }),
        }
        .deparse(&mut buf);
        assert_eq!(buf, " public.users alias");
        buf.clear();

        // table requires quoting
        TableNode {
            schema: Some(EcoString::from("public")),
            name: EcoString::from("userAccount"),
            alias: Some(TableAlias {
                name: EcoString::from("usrAcc"),
                columns: vec![],
            }),
        }
        .deparse(&mut buf);
        assert_eq!(buf, " public.\"userAccount\" \"usrAcc\"");
    }

    #[test]
    fn test_column_ref_deparse() {
        let mut buf = String::new();

        // Simple column
        ColumnNode {
            table: None,
            column: EcoString::from("id"),
        }
        .deparse(&mut buf);
        assert_eq!(buf, "id");
        buf.clear();

        // Qualified column
        ColumnNode {
            table: Some(EcoString::from("users")),
            column: EcoString::from("name"),
        }
        .deparse(&mut buf);
        assert_eq!(buf, "users.name");
        buf.clear();

        // table and column require quoting
        ColumnNode {
            table: Some(EcoString::from("Users")),
            column: EcoString::from("firstName"),
        }
        .deparse(&mut buf);
        assert_eq!(buf, "\"Users\".\"firstName\"");
    }

    #[test]
    fn test_select_column_alias_deparse() {
        let mut buf = String::new();

        // Simple column
        SelectColumn::Expr {
            expr: ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("id"),
            }),
            alias: Some(EcoString::from("alias")),
        }
        .deparse(&mut buf);
        assert_eq!(buf, " id AS alias");
        buf.clear();

        // Qualified column
        SelectColumn::Expr {
            expr: ScalarExpr::Column(ColumnNode {
                table: Some(EcoString::from("users")),
                column: EcoString::from("name"),
            }),
            alias: Some(EcoString::from("alias")),
        }
        .deparse(&mut buf);
        assert_eq!(buf, " users.name AS alias");
        buf.clear();

        // alias requires quoting
        SelectColumn::Expr {
            expr: ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("id"),
            }),
            alias: Some(EcoString::from("Alias")),
        }
        .deparse(&mut buf);
        assert_eq!(buf, " id AS \"Alias\"");
    }

    #[test]
    fn test_unary_op_deparse() {
        let mut buf = String::new();

        UnaryOp::Not.deparse(&mut buf);
        assert_eq!(buf, "NOT");
        buf.clear();

        UnaryOp::IsNull.deparse(&mut buf);
        assert_eq!(buf, "IS NULL");
        buf.clear();

        UnaryOp::IsNotNull.deparse(&mut buf);
        assert_eq!(buf, "IS NOT NULL");
        buf.clear();

        UnaryOp::IsTrue.deparse(&mut buf);
        assert_eq!(buf, "IS TRUE");
        buf.clear();

        UnaryOp::IsNotTrue.deparse(&mut buf);
        assert_eq!(buf, "IS NOT TRUE");
        buf.clear();

        UnaryOp::IsFalse.deparse(&mut buf);
        assert_eq!(buf, "IS FALSE");
        buf.clear();

        UnaryOp::IsNotFalse.deparse(&mut buf);
        assert_eq!(buf, "IS NOT FALSE");
    }

    #[test]
    fn test_binary_op_deparse() {
        let mut buf = String::new();

        // Comparison operators
        BinaryOp::Equal.deparse(&mut buf);
        assert_eq!(buf, "=");
        buf.clear();

        BinaryOp::NotEqual.deparse(&mut buf);
        assert_eq!(buf, "!=");
        buf.clear();

        BinaryOp::LessThan.deparse(&mut buf);
        assert_eq!(buf, "<");
        buf.clear();

        BinaryOp::GreaterThanOrEqual.deparse(&mut buf);
        assert_eq!(buf, ">=");
        buf.clear();

        // Pattern matching
        BinaryOp::Like.deparse(&mut buf);
        assert_eq!(buf, "LIKE");
        buf.clear();

        BinaryOp::NotLike.deparse(&mut buf);
        assert_eq!(buf, "NOT LIKE");
        buf.clear();

        // Logical operators
        BinaryOp::And.deparse(&mut buf);
        assert_eq!(buf, "AND");
        buf.clear();

        BinaryOp::Or.deparse(&mut buf);
        assert_eq!(buf, "OR");
    }

    #[test]
    fn test_multi_op_deparse() {
        let mut buf = String::new();

        MultiOp::In.deparse(&mut buf);
        assert_eq!(buf, "IN");
        buf.clear();

        MultiOp::NotIn.deparse(&mut buf);
        assert_eq!(buf, "NOT IN");
    }

    #[test]
    fn test_binary_expr_deparse() {
        let mut buf = String::new();

        // Simple equality: id = 1
        let expr = BinaryExpr {
            op: BinaryOp::Equal,
            lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("id"),
            }))),
            rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                LiteralValue::Integer(1),
            ))),
        };

        expr.deparse(&mut buf);
        assert_eq!(buf, "id = 1");
        buf.clear();

        // Complex expression: users.name = 'john'
        let expr = BinaryExpr {
            op: BinaryOp::Equal,
            lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                table: Some(EcoString::from("users")),
                column: EcoString::from("name"),
            }))),
            rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                LiteralValue::String("john".to_owned()),
            ))),
        };

        expr.deparse(&mut buf);
        assert_eq!(buf, "users.name = 'john'");
    }

    #[test]
    fn test_unary_expr_deparse() {
        let mut buf = String::new();

        // NOT active (prefix operator)
        let expr = UnaryExpr {
            op: UnaryOp::Not,
            expr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("active"),
            }))),
        };

        expr.deparse(&mut buf);
        assert_eq!(buf, "NOT active");
        buf.clear();

        // deleted_at IS NULL (postfix operator)
        let expr = UnaryExpr {
            op: UnaryOp::IsNull,
            expr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("deleted_at"),
            }))),
        };

        expr.deparse(&mut buf);
        assert_eq!(buf, "deleted_at IS NULL");
        buf.clear();

        // name IS NOT NULL (postfix operator)
        let expr = UnaryExpr {
            op: UnaryOp::IsNotNull,
            expr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("name"),
            }))),
        };

        expr.deparse(&mut buf);
        assert_eq!(buf, "name IS NOT NULL");
        buf.clear();

        // active IS TRUE (postfix operator)
        let expr = UnaryExpr {
            op: UnaryOp::IsTrue,
            expr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("active"),
            }))),
        };

        expr.deparse(&mut buf);
        assert_eq!(buf, "active IS TRUE");
        buf.clear();

        // active IS NOT FALSE (postfix operator)
        let expr = UnaryExpr {
            op: UnaryOp::IsNotFalse,
            expr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("active"),
            }))),
        };

        expr.deparse(&mut buf);
        assert_eq!(buf, "active IS NOT FALSE");
    }

    #[test]
    fn test_deparse_not_with_and() {
        // NOT has higher precedence than AND, so NOT (a AND b) must keep parens.
        // Without them: NOT a AND b → (NOT a) AND b — different semantics.
        let sql = "SELECT * FROM t WHERE NOT (x = 1 AND y = 2)";
        let ast = parse_query(sql);

        let mut buf = String::with_capacity(1024);
        ast.deparse(&mut buf);

        assert!(
            buf.contains("NOT (x = 1 AND y = 2)"),
            "expected parentheses after NOT around AND, got: {buf}"
        );
    }

    #[test]
    fn test_deparse_not_with_or() {
        // Same issue: NOT (a OR b) must keep parens.
        let sql = "SELECT * FROM t WHERE NOT (x = 1 OR y = 2)";
        let ast = parse_query(sql);

        let mut buf = String::with_capacity(1024);
        ast.deparse(&mut buf);

        assert!(
            buf.contains("NOT (x = 1 OR y = 2)"),
            "expected parentheses after NOT around OR, got: {buf}"
        );
    }

    #[test]
    fn test_select_deparse_with_where() {
        let sql = "SELECT * FROM users WHERE id = 1";
        let ast = parse_query(sql);

        let mut buf = String::with_capacity(1024);
        ast.deparse(&mut buf);
        assert_eq!(buf, sql);
    }

    #[test]
    fn test_select_deparse_distinct() {
        let sql = "SELECT DISTINCT name FROM users";
        let ast = parse_query(sql);

        let mut buf = String::with_capacity(1024);
        ast.deparse(&mut buf);
        assert_eq!(buf, sql);
    }

    #[test]
    fn test_select_deparse_multiple_tables() {
        let sql = "SELECT * FROM users, orders";
        let ast = parse_query(sql);

        let mut buf = String::with_capacity(1024);
        ast.deparse(&mut buf);
        assert_eq!(buf, sql);
    }

    #[test]
    fn test_select_deparse_schema_qualified() {
        let sql = "SELECT * FROM public.users";
        let ast = parse_query(sql);

        let mut buf = String::with_capacity(1024);
        ast.deparse(&mut buf);
        assert_eq!(buf, sql);
    }

    #[test]
    fn test_select_deparse_join() {
        let sql = "SELECT first_name, last_name, film_id FROM actor a \
                JOIN film_actor fa ON a.actor_id = fa.actor_id \
                WHERE a.actor_id = 1";
        let ast = parse_query(sql);

        let mut buf = String::with_capacity(1024);
        ast.deparse(&mut buf);
        assert_eq!(buf, sql);
    }

    #[test]
    fn test_select_deparse_left_join() {
        let sql = "SELECT a.id, b.name FROM a LEFT JOIN b ON a.id = b.a_id WHERE a.id = 1";
        let ast = parse_query(sql);

        let mut buf = String::with_capacity(1024);
        ast.deparse(&mut buf);
        assert_eq!(buf, sql);
    }

    #[test]
    fn test_select_deparse_right_join() {
        let sql = "SELECT a.id, b.name FROM a RIGHT JOIN b ON a.id = b.a_id WHERE b.id = 1";
        let ast = parse_query(sql);

        let mut buf = String::with_capacity(1024);
        ast.deparse(&mut buf);
        assert_eq!(buf, sql);
    }

    #[test]
    fn test_select_deparse_mixed_joins() {
        let sql =
            "SELECT * FROM a JOIN b ON a.id = b.a_id LEFT JOIN c ON b.id = c.b_id WHERE a.id = 1";
        let ast = parse_query(sql);

        let mut buf = String::with_capacity(1024);
        ast.deparse(&mut buf);
        assert_eq!(buf, sql);
    }

    #[test]
    fn test_select_deparse_table_values() {
        let sql = "SELECT fa.actor_id \
            FROM (VALUES ('1', '2'), ('3', '4')) fa(actor_id, film_id) \
            WHERE a.actor_id = 1";
        let ast = parse_query(sql);

        let mut buf = String::with_capacity(1024);
        ast.deparse(&mut buf);
        assert_eq!(buf, sql);
    }

    #[test]
    fn test_round_trip() {
        fn round_trip(sql: &str) {
            // Parse original
            let ast1 = {
                let pg_ast1 = pg_query::parse(sql).unwrap();
                query_expr_convert(&pg_ast1).unwrap()
            };

            // Deparse to string
            let mut deparsed = String::with_capacity(1024);
            ast1.deparse(&mut deparsed);

            // Parse deparsed version
            let ast2 = {
                let pg_ast2 = pg_query::parse(&deparsed).unwrap();
                query_expr_convert(&pg_ast2).unwrap()
            };

            // Should be equivalent
            assert_eq!(ast1, ast2);
        }

        round_trip("SELECT id, name FROM users WHERE active = true");
        round_trip(
            "SELECT id, name \
            FROM users JOIN address a on a.is = users.address_id \
            WHERE active = true",
        );
        round_trip(
            "SELECT id, name \
            FROM (SELECT * FROM users) u \
            WHERE active = true",
        );
    }

    #[test]
    fn test_parameterized_query_single_param() {
        let sql = "SELECT * FROM users WHERE id = $1";
        let ast = parse_query(sql);

        // Verify the WHERE clause contains a parameter
        let where_clause = ast.where_clause().unwrap();
        let literals: Vec<&LiteralValue> = where_clause.nodes().collect();
        assert_eq!(literals.len(), 1);
        assert_eq!(literals[0], &LiteralValue::Parameter("$1".to_owned()));

        // Test deparsing
        let mut deparsed = String::with_capacity(1024);
        ast.deparse(&mut deparsed);
        assert_eq!(deparsed, sql);
    }

    #[test]
    fn test_parameterized_query_multiple_params() {
        let sql = "SELECT * FROM users WHERE name = $1 AND age > $2";
        let ast = parse_query(sql);

        // Verify the WHERE clause contains both parameters
        let where_clause = ast.where_clause().unwrap();
        let literals: Vec<&LiteralValue> = where_clause.nodes().collect();
        assert_eq!(literals.len(), 2);
        assert_eq!(literals[0], &LiteralValue::Parameter("$1".to_owned()));
        assert_eq!(literals[1], &LiteralValue::Parameter("$2".to_owned()));

        // Test deparsing
        let mut deparsed = String::with_capacity(1024);
        ast.deparse(&mut deparsed);
        assert_eq!(deparsed, sql);
    }

    #[test]
    fn test_parameterized_query_mixed_params_and_literals() {
        let sql = "SELECT * FROM users WHERE name = $1 AND active = true";
        let ast = parse_query(sql);

        // Verify the WHERE clause contains parameter and boolean literal
        let where_clause = ast.where_clause().unwrap();
        let literals: Vec<&LiteralValue> = where_clause.nodes().collect();
        assert_eq!(literals.len(), 2);
        assert_eq!(literals[0], &LiteralValue::Parameter("$1".to_owned()));
        assert_eq!(literals[1], &LiteralValue::Boolean(true));

        // Test deparsing
        let mut deparsed = String::with_capacity(1024);
        ast.deparse(&mut deparsed);
        assert_eq!(deparsed, sql);
    }

    #[test]
    fn test_literal_empty_string() {
        let mut buf = String::new();
        LiteralValue::String("".to_owned()).deparse(&mut buf);
        assert_eq!(buf, "''");
    }

    #[test]
    fn test_literal_string_with_quotes() {
        let mut buf = String::new();
        LiteralValue::String("test'quote".to_owned()).deparse(&mut buf);
        // postgres-protocol should properly escape the quote
        assert_eq!(buf, "'test''quote'");
    }

    #[test]
    fn test_literal_string_with_backslashes() {
        let mut buf = String::new();
        LiteralValue::String("test\\path".to_owned()).deparse(&mut buf);
        // postgres-protocol should use E'' syntax for backslashes
        assert_eq!(buf, "E'test\\\\path'");
    }

    #[test]
    fn test_nodes_table_extraction() {
        let ast = parse_query(
            "SELECT first_name, last_name, film_id \
                    FROM actor a \
                    JOIN public.film_actor fa ON a.actor_id = fa.actor_id \
                    WHERE a.actor_id = 1",
        );

        // Test extracting TableNode instances using the generic nodes function
        let tables = ast.nodes::<TableNode>().collect::<Vec<_>>();

        assert_eq!(tables.len(), 2);

        assert_eq!(tables[0].schema, None);
        assert_eq!(tables[0].name, "actor");
        assert_eq!(
            tables[0].alias,
            Some(TableAlias {
                name: EcoString::from("a"),
                columns: vec![]
            })
        );

        assert_eq!(tables[1].schema, Some(EcoString::from("public")));
        assert_eq!(tables[1].name, "film_actor");
        assert_eq!(
            tables[1].alias,
            Some(TableAlias {
                name: EcoString::from("fa"),
                columns: vec![]
            })
        );
    }

    #[test]
    fn test_nodes_table_nodes() {
        let ast = parse_query("SELECT * FROM users u JOIN orders o ON u.id = o.user_id");

        // Test getting TableNode instances using the nodes function
        let table_nodes: Vec<&TableNode> = ast.nodes().collect();
        assert_eq!(table_nodes.len(), 2);

        assert_eq!(table_nodes[0].name, "users");
        assert_eq!(table_nodes[0].alias.as_ref().unwrap().name, "u");

        assert_eq!(table_nodes[1].name, "orders");
        assert_eq!(table_nodes[1].alias.as_ref().unwrap().name, "o");
    }

    #[test]
    fn test_nodes_join_nodes() {
        let ast = parse_query("SELECT * FROM users u JOIN orders o ON u.id = o.user_id");

        // Test getting JoinNode instances using the nodes function
        let join_nodes: Vec<&JoinNode> = ast.nodes().collect();
        assert_eq!(join_nodes.len(), 1);

        assert_eq!(join_nodes[0].join_type, JoinType::Inner);
        assert!(matches!(join_nodes[0].qual, JoinQual::On(_)));
    }

    #[test]
    fn test_nodes_mixed_types() {
        let ast = parse_query("SELECT * FROM users u JOIN orders o ON u.id = o.user_id");

        // Should find both table nodes and join nodes independently
        let table_nodes: Vec<&TableNode> = ast.nodes().collect();
        let join_nodes: Vec<&JoinNode> = ast.nodes().collect();

        assert_eq!(table_nodes.len(), 2);
        assert_eq!(join_nodes.len(), 1);

        // Verify the types are different queries but same AST
        assert_eq!(table_nodes[0].name, "users");
        assert_eq!(table_nodes[1].name, "orders");
        assert_eq!(join_nodes[0].join_type, JoinType::Inner);
    }

    #[test]
    fn test_where_expr_nodes_column() {
        let ast = parse_query("SELECT * FROM users WHERE name = 'john' AND age > 25");

        let where_clause = ast.where_clause().unwrap();
        let columns: Vec<&ColumnNode> = where_clause.nodes().collect();

        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].column, "name");
        assert_eq!(columns[1].column, "age");
    }

    #[test]
    fn test_where_expr_nodes_literal() {
        let ast =
            parse_query("SELECT * FROM users WHERE name = 'john' AND age > 25 AND active = true");

        let where_clause = ast.where_clause().unwrap();
        let literals: Vec<&LiteralValue> = where_clause.nodes().collect();

        assert_eq!(literals.len(), 3);
        assert_eq!(literals[0], &LiteralValue::String("john".to_owned()));
        assert_eq!(literals[1], &LiteralValue::Integer(25));
        assert_eq!(literals[2], &LiteralValue::Boolean(true));
    }

    #[test]
    fn test_where_expr_nodes_binary() {
        let ast = parse_query("SELECT * FROM users WHERE name = 'john' AND age > 25");

        let where_clause = ast.where_clause().unwrap();
        let binary_exprs: Vec<&BinaryExpr> = where_clause.nodes().collect();

        assert_eq!(binary_exprs.len(), 3); // AND, =, >
        assert_eq!(binary_exprs[0].op, BinaryOp::And);
        assert_eq!(binary_exprs[1].op, BinaryOp::Equal);
        assert_eq!(binary_exprs[2].op, BinaryOp::GreaterThan);
    }

    #[test]
    fn test_where_expr_nodes_whole_expr() {
        let ast = parse_query("SELECT * FROM users WHERE name = 'john'");

        let where_clause = ast.where_clause().unwrap();
        let where_exprs: Vec<&WhereExpr> = where_clause.nodes().collect();

        // Should find the root expression plus all child expressions
        assert_eq!(where_exprs.len(), 3); // Binary(name = 'john'), Column(name), Value('john')
    }

    #[test]
    fn test_where_expr_nodes_nested() {
        let ast =
            parse_query("SELECT * FROM users WHERE (name = 'john' OR name = 'jane') AND age > 18");

        let where_clause = ast.where_clause().unwrap();
        let columns: Vec<&ColumnNode> = where_clause.nodes().collect();

        // Should find all column references in nested structure
        assert_eq!(columns.len(), 3); // name, name, age
        assert_eq!(columns[0].column, "name");
        assert_eq!(columns[1].column, "name");
        assert_eq!(columns[2].column, "age");
    }

    #[test]
    fn test_join_condition_nodes() {
        let ast = parse_query("SELECT * FROM users u JOIN orders o ON u.id = o.user_id");

        // Get the join node from the query
        let join_nodes: Vec<&JoinNode> = ast.nodes().collect();
        assert_eq!(join_nodes.len(), 1);

        let join_node = join_nodes[0];

        // Test that we can extract column nodes from the join condition
        let columns: Vec<&ColumnNode> = join_node.nodes().collect();

        // Should find only the condition columns (u.id, o.user_id)
        // since we're specifically collecting ColumnNode instances
        assert_eq!(columns.len(), 2);

        // Verify the condition columns
        assert_eq!(columns[0].table, Some(EcoString::from("u")));
        assert_eq!(columns[0].column, "id");
        assert_eq!(columns[1].table, Some(EcoString::from("o")));
        assert_eq!(columns[1].column, "user_id");
    }

    #[test]
    fn test_query_body_nodes() {
        let ast = parse_query("SELECT * FROM users WHERE id = 1");

        // Test that QueryBody::nodes() delegates to SelectNode
        let tables: Vec<&TableNode> = ast.body.nodes().collect();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, "users");
    }

    #[test]
    fn test_select_columns_nodes() {
        let ast = parse_query("SELECT id, name FROM users");

        // Test that we can extract ColumnNode through SelectColumns
        let columns: Vec<&ColumnNode> = ast.nodes().collect();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].column, "id");
        assert_eq!(columns[1].column, "name");
    }

    #[test]
    fn test_scalar_expr_nodes() {
        let ast = parse_query("SELECT id, name FROM users WHERE active = true");

        // Test that ScalarExpr::nodes() can traverse through to ColumnNode
        let columns: Vec<&ColumnNode> = ast.nodes().collect();
        assert_eq!(columns.len(), 3); // id, name, active
        assert_eq!(columns[0].column, "id");
        assert_eq!(columns[1].column, "name");
        assert_eq!(columns[2].column, "active");
    }

    #[test]
    fn test_function_call_nodes() {
        let func = FunctionCall {
            name: EcoString::from("COUNT"),
            args: vec![ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("id"),
            })],
            agg_star: false,
            agg_distinct: false,
            agg_order: vec![],
            agg_filter: None,
            over: None,
        };

        // Test that FunctionCall::nodes() can extract ColumnNode from args
        let columns: Vec<&ColumnNode> = func.nodes().collect();
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].column, "id");
    }

    #[test]
    fn test_table_node_nodes() {
        let ast = parse_query("SELECT * FROM public.users");

        // Test that TableNode::nodes() returns itself as a leaf node
        let tables: Vec<&TableNode> = ast.nodes().collect();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].schema, Some(EcoString::from("public")));
        assert_eq!(tables[0].name, "users");
    }

    #[test]
    fn test_table_subquery_node_nodes() {
        let ast = parse_query("SELECT * FROM (SELECT id FROM users) sub");

        // TableSourceNodeIter traverses into subqueries
        let subqueries: Vec<&TableSubqueryNode> = ast.nodes().collect();
        assert_eq!(subqueries.len(), 1);

        // Inner table is also reachable
        let tables: Vec<&TableNode> = ast.nodes().collect();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, "users");
    }

    #[test]
    fn test_scalar_expr_subquery_nodes() {
        // Scalar subquery in SELECT list: ScalarExpr::Subquery should traverse into inner query
        let ast = parse_query("SELECT id, (SELECT COUNT(*) FROM orders) FROM users");

        // Should find both tables: users (FROM) and orders (inside scalar subquery)
        let tables: Vec<&TableNode> = ast.nodes().collect();
        let table_names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        assert!(
            table_names.contains(&"users"),
            "should find outer table 'users'"
        );
        assert!(
            table_names.contains(&"orders"),
            "should find inner table 'orders' via ScalarExpr::Subquery traversal"
        );
        assert_eq!(tables.len(), 2);
    }

    #[test]
    fn test_unary_expr_nodes() {
        let ast = parse_query("SELECT * FROM users WHERE NOT active");

        // Test that UnaryExpr::nodes() returns itself
        let unary_exprs: Vec<&UnaryExpr> = ast.nodes().collect();
        assert_eq!(unary_exprs.len(), 1);
        assert_eq!(unary_exprs[0].op, UnaryOp::Not);
    }

    #[test]
    fn test_binary_expr_nodes() {
        let ast = parse_query("SELECT * FROM users WHERE name = 'john'");

        // Test that BinaryExpr::nodes() returns itself
        let binary_exprs: Vec<&BinaryExpr> = ast.nodes().collect();
        assert_eq!(binary_exprs.len(), 1);
        assert_eq!(binary_exprs[0].op, BinaryOp::Equal);
    }

    #[test]
    fn test_multi_expr_nodes() {
        // Test MultiExpr::nodes() with a manually constructed MultiExpr
        let multi = MultiExpr {
            op: MultiOp::In,
            exprs: vec![
                WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                    table: None,
                    column: EcoString::from("id"),
                })),
                WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::Integer(1))),
                WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::Integer(2))),
            ],
        };

        // Test that MultiExpr::nodes() returns itself
        let multi_exprs: Vec<&MultiExpr> = multi.nodes().collect();
        assert_eq!(multi_exprs.len(), 1);
        assert_eq!(multi_exprs[0].op, MultiOp::In);

        // Test that it can extract child nodes
        let columns: Vec<&ColumnNode> = multi.nodes().collect();
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].column, "id");
    }

    #[test]
    fn test_multi_expr_in_deparse() {
        let multi = MultiExpr {
            op: MultiOp::In,
            exprs: vec![
                WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                    table: None,
                    column: EcoString::from("status"),
                })),
                WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::String(
                    "active".to_owned(),
                ))),
                WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::String(
                    "pending".to_owned(),
                ))),
            ],
        };

        let mut buf = String::new();
        multi.deparse(&mut buf);
        assert_eq!(buf, "status IN ('active', 'pending')");
    }

    #[test]
    fn test_multi_expr_not_in_deparse() {
        let multi = MultiExpr {
            op: MultiOp::NotIn,
            exprs: vec![
                WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                    table: None,
                    column: EcoString::from("id"),
                })),
                WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::Integer(1))),
                WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::Integer(2))),
                WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::Integer(3))),
            ],
        };

        let mut buf = String::new();
        multi.deparse(&mut buf);
        assert_eq!(buf, "id NOT IN (1, 2, 3)");
    }

    #[test]
    fn test_in_clause_parse_and_deparse() {
        // Test that IN clause round-trips through parse and deparse
        let select = parse_select("SELECT * FROM t WHERE status IN ('active', 'pending')");

        let where_clause = select.where_clause.as_ref().unwrap();

        let mut buf = String::new();
        where_clause.deparse(&mut buf);
        assert_eq!(buf, "status IN ('active', 'pending')");
    }

    #[test]
    fn test_order_by_simple_asc() {
        let ast = parse_query("SELECT * FROM users ORDER BY name ASC");

        assert_eq!(ast.order_by.len(), 1);
        assert_eq!(ast.order_by[0].direction, OrderDirection::Asc);

        if let ScalarExpr::Column(col) = &ast.order_by[0].expr {
            assert_eq!(col.column, "name");
            assert_eq!(col.table, None);
        } else {
            panic!("Expected column expression");
        }
    }

    #[test]
    fn test_order_by_simple_desc() {
        let ast = parse_query("SELECT * FROM users ORDER BY age DESC");

        assert_eq!(ast.order_by.len(), 1);
        assert_eq!(ast.order_by[0].direction, OrderDirection::Desc);

        if let ScalarExpr::Column(col) = &ast.order_by[0].expr {
            assert_eq!(col.column, "age");
        } else {
            panic!("Expected column expression");
        }
    }

    #[test]
    fn test_order_by_default_direction() {
        let ast = parse_query("SELECT * FROM users ORDER BY name");

        assert_eq!(ast.order_by.len(), 1);
        // Default direction should be ASC
        assert_eq!(ast.order_by[0].direction, OrderDirection::Asc);
    }

    #[test]
    fn test_order_by_multiple_columns() {
        let ast = parse_query("SELECT * FROM users ORDER BY last_name ASC, first_name DESC");

        assert_eq!(ast.order_by.len(), 2);

        // First ORDER BY clause
        assert_eq!(ast.order_by[0].direction, OrderDirection::Asc);
        if let ScalarExpr::Column(col) = &ast.order_by[0].expr {
            assert_eq!(col.column, "last_name");
        } else {
            panic!("Expected column expression");
        }

        // Second ORDER BY clause
        assert_eq!(ast.order_by[1].direction, OrderDirection::Desc);
        if let ScalarExpr::Column(col) = &ast.order_by[1].expr {
            assert_eq!(col.column, "first_name");
        } else {
            panic!("Expected column expression");
        }
    }

    #[test]
    fn test_order_by_qualified_column() {
        let ast = parse_query("SELECT * FROM users u ORDER BY u.name ASC");

        assert_eq!(ast.order_by.len(), 1);

        if let ScalarExpr::Column(col) = &ast.order_by[0].expr {
            assert_eq!(col.table, Some(EcoString::from("u")));
            assert_eq!(col.column, "name");
        } else {
            panic!("Expected column expression");
        }
    }

    #[test]
    fn test_order_by_with_where() {
        let ast = parse_query("SELECT * FROM users WHERE active = true ORDER BY name ASC");

        assert!(ast.has_where_clause());
        assert_eq!(ast.order_by.len(), 1);
        assert_eq!(ast.order_by[0].direction, OrderDirection::Asc);
    }

    #[test]
    fn test_order_by_deparse_asc() {
        let sql = "SELECT * FROM users ORDER BY name ASC";
        let ast = parse_query(sql);

        let mut buf = String::with_capacity(1024);
        ast.deparse(&mut buf);
        assert_eq!(buf, sql);
    }

    #[test]
    fn test_order_by_deparse_desc() {
        let sql = "SELECT * FROM users ORDER BY age DESC";
        let ast = parse_query(sql);

        let mut buf = String::with_capacity(1024);
        ast.deparse(&mut buf);
        assert_eq!(buf, sql);
    }

    #[test]
    fn test_order_by_deparse_multiple() {
        let sql = "SELECT * FROM users ORDER BY last_name ASC, first_name DESC";
        let ast = parse_query(sql);

        let mut buf = String::with_capacity(1024);
        ast.deparse(&mut buf);
        assert_eq!(buf, sql);
    }

    #[test]
    fn test_order_by_round_trip() {
        let queries = vec![
            "SELECT * FROM users ORDER BY name ASC",
            "SELECT * FROM users ORDER BY age DESC",
            "SELECT id, name FROM users WHERE active = true ORDER BY created_at DESC",
            "SELECT * FROM users ORDER BY last_name ASC, first_name ASC, id DESC",
        ];

        for sql in queries {
            // Parse original
            let ast1 = parse_query(sql);

            // Deparse to string
            let mut deparsed = String::with_capacity(1024);
            ast1.deparse(&mut deparsed);

            // Parse deparsed version
            let ast2 = parse_query(&deparsed);

            // Should be equivalent
            assert_eq!(ast1, ast2, "Round trip failed for: {sql}");
        }
    }

    #[test]
    fn test_order_by_nodes_extraction() {
        let ast = parse_query("SELECT * FROM users ORDER BY u.name ASC, age DESC");

        // Extract OrderByClause nodes
        let order_clauses: Vec<&OrderByClause> = ast.nodes().collect();
        assert_eq!(order_clauses.len(), 2);

        // Extract ColumnNode from ORDER BY
        let columns: Vec<&ColumnNode> = ast.nodes().collect();
        // Should find columns in ORDER BY clause
        assert!(columns.iter().any(|c| c.column == "name"));
        assert!(columns.iter().any(|c| c.column == "age"));
    }

    #[test]
    fn test_group_by_single_column() {
        let select = parse_select("SELECT status FROM orders GROUP BY status");

        assert_eq!(select.group_by.len(), 1);
        assert_eq!(select.group_by[0].column, "status");
        assert_eq!(select.group_by[0].table, None);
    }

    #[test]
    fn test_group_by_multiple_columns() {
        let select = parse_select("SELECT status, category FROM orders GROUP BY status, category");

        assert_eq!(select.group_by.len(), 2);
        assert_eq!(select.group_by[0].column, "status");
        assert_eq!(select.group_by[1].column, "category");
    }

    #[test]
    fn test_group_by_qualified_column() {
        let select = parse_select("SELECT o.status FROM orders o GROUP BY o.status");

        assert_eq!(select.group_by.len(), 1);
        assert_eq!(select.group_by[0].column, "status");
        assert_eq!(select.group_by[0].table, Some(EcoString::from("o")));
    }

    #[test]
    fn test_having_simple() {
        let select =
            parse_select("SELECT status FROM orders GROUP BY status HAVING status = 'active'");

        assert!(select.having.is_some());
    }

    #[test]
    fn test_having_with_and() {
        let select = parse_select(
            "SELECT category FROM sales GROUP BY category HAVING category = 'electronics' AND category != 'toys'",
        );

        assert!(!select.group_by.is_empty());
        assert!(select.having.is_some());
    }

    #[test]
    fn test_limit_only() {
        let ast = parse_query("SELECT * FROM users LIMIT 10");

        let limit = ast.limit.as_ref().unwrap();
        assert_eq!(limit.count, Some(LiteralValue::Integer(10)));
        assert_eq!(limit.offset, None);
    }

    #[test]
    fn test_offset_only() {
        let ast = parse_query("SELECT * FROM users OFFSET 20");

        let limit = ast.limit.as_ref().unwrap();
        assert_eq!(limit.count, None);
        assert_eq!(limit.offset, Some(LiteralValue::Integer(20)));
    }

    #[test]
    fn test_limit_and_offset() {
        let ast = parse_query("SELECT * FROM users LIMIT 10 OFFSET 20");

        let limit = ast.limit.as_ref().unwrap();
        assert_eq!(limit.count, Some(LiteralValue::Integer(10)));
        assert_eq!(limit.offset, Some(LiteralValue::Integer(20)));
    }

    #[test]
    fn test_no_limit() {
        let ast = parse_query("SELECT * FROM users");

        assert!(ast.limit.is_none());
    }

    #[test]
    fn test_no_group_by() {
        let select = parse_select("SELECT * FROM users WHERE id = 1");

        assert!(select.group_by.is_empty());
        assert!(select.having.is_none());
    }

    #[test]
    fn test_combined_group_by_having_limit() {
        let ast = parse_query(
            "SELECT status FROM orders GROUP BY status HAVING status != 'cancelled' ORDER BY status DESC LIMIT 10",
        );

        let select = match &ast.body {
            QueryBody::Select(s) => s,
            _ => panic!("expected SELECT"),
        };
        assert_eq!(select.group_by.len(), 1);
        assert!(select.having.is_some());
        assert!(ast.limit.is_some());
        assert!(!ast.order_by.is_empty());
    }

    #[test]
    fn test_limit_parameterized() {
        let ast = parse_query("SELECT * FROM users LIMIT $1");

        let limit = ast.limit.as_ref().unwrap();
        assert_eq!(limit.count, Some(LiteralValue::Parameter("$1".to_owned())));
        assert_eq!(limit.offset, None);
    }

    #[test]
    fn test_limit_and_offset_parameterized() {
        let ast = parse_query("SELECT * FROM users LIMIT $1 OFFSET $2");

        let limit = ast.limit.as_ref().unwrap();
        assert_eq!(limit.count, Some(LiteralValue::Parameter("$1".to_owned())));
        assert_eq!(limit.offset, Some(LiteralValue::Parameter("$2".to_owned())));
    }

    #[test]
    fn test_has_subqueries_in_select_list() {
        let select = parse_select("SELECT id, (SELECT x FROM other WHERE id = 1) as val FROM t");

        assert!(
            select.has_subqueries(),
            "has_subqueries() should detect subquery in SELECT list"
        );
    }

    #[test]
    fn test_has_subqueries_in_from_clause() {
        let select = parse_select("SELECT * FROM (SELECT id FROM users) sub");

        assert!(
            select.has_subqueries(),
            "has_subqueries() should detect subquery in FROM clause"
        );
    }

    #[test]
    fn test_has_subqueries_in_join() {
        let select = parse_select("SELECT * FROM a JOIN (SELECT id FROM b) sub ON a.id = sub.id");

        assert!(
            select.has_subqueries(),
            "has_subqueries() should detect subquery in JOIN"
        );
    }

    #[test]
    fn test_has_subqueries_in_where_clause() {
        let select = parse_select("SELECT * FROM t WHERE id IN (SELECT id FROM other)");

        assert!(
            select.has_subqueries(),
            "has_subqueries() should detect subquery in WHERE clause"
        );
    }

    #[test]
    fn test_has_subqueries_no_subquery() {
        let select = parse_select("SELECT id, name FROM users WHERE active = true");

        assert!(
            !select.has_subqueries(),
            "has_subqueries() should return false when no subquery exists"
        );
    }

    #[test]
    fn test_function_count_star() {
        let select = parse_select("SELECT COUNT(*) FROM users");

        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        assert_eq!(columns.len(), 1);
        let ScalarExpr::Function(func) = &columns[0].expr().expect("non-star SELECT column") else {
            panic!("expected function");
        };
        assert_eq!(func.name, "count");
        assert!(func.agg_star);
        assert!(func.args.is_empty());
    }

    #[test]
    fn test_function_count_column() {
        let select = parse_select("SELECT COUNT(id) FROM users");

        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ScalarExpr::Function(func) = &columns[0].expr().expect("non-star SELECT column") else {
            panic!("expected function");
        };
        assert_eq!(func.name, "count");
        assert!(!func.agg_star);
        assert_eq!(func.args.len(), 1);
    }

    #[test]
    fn test_function_count_distinct() {
        let select = parse_select("SELECT COUNT(DISTINCT status) FROM orders");

        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ScalarExpr::Function(func) = &columns[0].expr().expect("non-star SELECT column") else {
            panic!("expected function");
        };
        assert_eq!(func.name, "count");
        assert!(func.agg_distinct);
    }

    #[test]
    fn test_function_sum() {
        let select = parse_select("SELECT SUM(amount) FROM orders WHERE tenant_id = 1");

        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ScalarExpr::Function(func) = &columns[0].expr().expect("non-star SELECT column") else {
            panic!("expected function");
        };
        assert_eq!(func.name, "sum");
    }

    #[test]
    fn test_function_nested() {
        // Use ROUND(AVG(...)) since COALESCE is parsed as a special CoalesceExpr
        let select = parse_select("SELECT ROUND(AVG(value)) FROM data");

        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ScalarExpr::Function(func) = &columns[0].expr().expect("non-star SELECT column") else {
            panic!("expected function");
        };
        assert_eq!(func.name, "round");
        assert_eq!(func.args.len(), 1);

        // First arg should be AVG(value)
        let ScalarExpr::Function(inner) = &func.args[0] else {
            panic!("expected nested function");
        };
        assert_eq!(inner.name, "avg");
    }

    #[test]
    fn test_function_with_alias() {
        let select = parse_select("SELECT COUNT(*) as total FROM users");

        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        assert_eq!(columns[0].alias().cloned(), Some(EcoString::from("total")));
    }

    #[test]
    fn test_function_mixed_with_columns() {
        let select = parse_select("SELECT id, name, COUNT(*) as cnt FROM users GROUP BY id, name");

        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        assert_eq!(columns.len(), 3);
        assert!(matches!(
            columns[0].expr().expect("non-star SELECT column"),
            ScalarExpr::Column(_)
        ));
        assert!(matches!(
            columns[1].expr().expect("non-star SELECT column"),
            ScalarExpr::Column(_)
        ));
        assert!(matches!(
            columns[2].expr().expect("non-star SELECT column"),
            ScalarExpr::Function(_)
        ));
    }

    #[test]
    fn test_literal_in_select() {
        let select = parse_select("SELECT 42 as answer, 'hello' as greeting FROM t");

        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        assert_eq!(columns.len(), 2);
        assert!(matches!(
            columns[0].expr().expect("non-star SELECT column"),
            ScalarExpr::Literal(LiteralValue::Integer(42))
        ));
        assert!(matches!(
            &columns[1].expr().expect("non-star SELECT column"),
            ScalarExpr::Literal(LiteralValue::String(s)) if s == "hello"
        ));
    }

    #[test]
    fn test_function_deparse_count_star() {
        let func = FunctionCall {
            name: EcoString::from("count"),
            args: vec![],
            agg_star: true,
            agg_distinct: false,
            agg_order: vec![],
            agg_filter: None,
            over: None,
        };
        let mut buf = String::new();
        func.deparse(&mut buf);
        assert_eq!(buf, "COUNT(*)");
    }

    #[test]
    fn test_function_deparse_count_distinct() {
        let func = FunctionCall {
            name: EcoString::from("count"),
            args: vec![ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("status"),
            })],
            agg_star: false,
            agg_distinct: true,
            agg_order: vec![],
            agg_filter: None,
            over: None,
        };
        let mut buf = String::new();
        func.deparse(&mut buf);
        assert_eq!(buf, "COUNT(DISTINCT status)");
    }

    #[test]
    fn test_coalesce() {
        let select = parse_select("SELECT COALESCE(name, 'unknown') FROM users");

        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ScalarExpr::Function(func) = &columns[0].expr().expect("non-star SELECT column") else {
            panic!("expected function");
        };
        assert_eq!(func.name, "coalesce");
        assert_eq!(func.args.len(), 2);
    }

    #[test]
    fn test_coalesce_nested_with_function() {
        let select = parse_select("SELECT COALESCE(MAX(value), 0) FROM data");

        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ScalarExpr::Function(func) = &columns[0].expr().expect("non-star SELECT column") else {
            panic!("expected function");
        };
        assert_eq!(func.name, "coalesce");
        assert_eq!(func.args.len(), 2);

        // First arg should be MAX(value)
        let ScalarExpr::Function(inner) = &func.args[0] else {
            panic!("expected nested function");
        };
        assert_eq!(inner.name, "max");
    }

    #[test]
    fn test_greatest() {
        let select = parse_select("SELECT GREATEST(a, b, c) FROM t");

        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ScalarExpr::Function(func) = &columns[0].expr().expect("non-star SELECT column") else {
            panic!("expected function");
        };
        assert_eq!(func.name, "greatest");
        assert_eq!(func.args.len(), 3);
    }

    #[test]
    fn test_least() {
        let select = parse_select("SELECT LEAST(a, b) FROM t");

        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ScalarExpr::Function(func) = &columns[0].expr().expect("non-star SELECT column") else {
            panic!("expected function");
        };
        assert_eq!(func.name, "least");
        assert_eq!(func.args.len(), 2);
    }

    #[test]
    fn test_nullif() {
        let select = parse_select("SELECT NULLIF(status, 'deleted') FROM items");

        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ScalarExpr::Function(func) = &columns[0].expr().expect("non-star SELECT column") else {
            panic!("expected function");
        };
        assert_eq!(func.name, "nullif");
        assert_eq!(func.args.len(), 2);
    }

    #[test]
    fn test_case_searched() {
        let select =
            parse_select("SELECT CASE WHEN status = 'active' THEN 1 ELSE 0 END FROM items");

        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ScalarExpr::Case(case) = &columns[0].expr().expect("non-star SELECT column") else {
            panic!("expected case expression");
        };
        assert!(case.arg.is_none(), "searched CASE should have no arg");
        assert_eq!(case.whens.len(), 1);
        assert!(case.default.is_some(), "should have ELSE clause");
    }

    #[test]
    fn test_case_simple() {
        let select = parse_select(
            "SELECT CASE status WHEN 'active' THEN 1 WHEN 'pending' THEN 2 ELSE 0 END FROM items",
        );

        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ScalarExpr::Case(case) = &columns[0].expr().expect("non-star SELECT column") else {
            panic!("expected case expression");
        };
        assert!(case.arg.is_some(), "simple CASE should have arg");
        assert_eq!(case.whens.len(), 2);
        assert!(case.default.is_some(), "should have ELSE clause");
    }

    #[test]
    fn test_case_no_else() {
        let select = parse_select("SELECT CASE WHEN x > 0 THEN 'positive' END FROM items");

        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ScalarExpr::Case(case) = &columns[0].expr().expect("non-star SELECT column") else {
            panic!("expected case expression");
        };
        assert!(case.arg.is_none());
        assert_eq!(case.whens.len(), 1);
        assert!(case.default.is_none(), "should have no ELSE clause");
    }

    #[test]
    fn test_case_deparse() {
        let sql = "SELECT CASE WHEN status = 'active' THEN 1 ELSE 0 END FROM items WHERE id = 1";
        let ast = parse_query(sql);

        let mut buf = String::new();
        ast.deparse(&mut buf);
        assert_eq!(
            buf,
            "SELECT CASE WHEN status = 'active' THEN 1 ELSE 0 END FROM items WHERE id = 1"
        );
    }

    #[test]
    fn test_case_has_subqueries() {
        // CASE with subquery in WHEN condition
        let select = parse_select(
            "SELECT CASE WHEN id IN (SELECT id FROM other) THEN 1 ELSE 0 END FROM items",
        );

        assert!(
            select.has_subqueries(),
            "CASE with subquery should have sublink"
        );
    }

    #[test]
    fn test_window_function_simple() {
        let select = parse_select("SELECT sum(amount) OVER (ORDER BY date) FROM orders");

        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ScalarExpr::Function(func) = &columns[0].expr().expect("non-star SELECT column") else {
            panic!("expected function");
        };
        assert_eq!(func.name, "sum");
        assert!(func.over.is_some(), "should have OVER clause");

        let over = func.over.as_ref().unwrap();
        assert!(over.partition_by.is_empty(), "no PARTITION BY");
        assert_eq!(over.order_by.len(), 1, "one ORDER BY clause");
    }

    #[test]
    fn test_window_function_with_partition() {
        let select = parse_select(
            "SELECT sum(amount) OVER (PARTITION BY category ORDER BY date) FROM orders",
        );

        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ScalarExpr::Function(func) = &columns[0].expr().expect("non-star SELECT column") else {
            panic!("expected function");
        };
        assert!(func.over.is_some(), "should have OVER clause");

        let over = func.over.as_ref().unwrap();
        assert_eq!(over.partition_by.len(), 1, "one PARTITION BY column");
        assert_eq!(over.order_by.len(), 1, "one ORDER BY clause");
    }

    #[test]
    fn test_window_function_row_number() {
        let select = parse_select("SELECT row_number() OVER (ORDER BY id) FROM users");

        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ScalarExpr::Function(func) = &columns[0].expr().expect("non-star SELECT column") else {
            panic!("expected function");
        };
        assert_eq!(func.name, "row_number");
        assert!(func.args.is_empty(), "row_number has no args");
        assert!(func.over.is_some(), "should have OVER clause");
    }

    #[test]
    fn test_window_function_deparse() {
        let func = FunctionCall {
            name: EcoString::from("sum"),
            args: vec![ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("amount"),
            })],
            agg_star: false,
            agg_distinct: false,
            agg_order: vec![],
            agg_filter: None,
            over: Some(WindowSpec {
                partition_by: vec![ScalarExpr::Column(ColumnNode {
                    table: None,
                    column: EcoString::from("category"),
                })],
                order_by: vec![OrderByClause {
                    expr: ScalarExpr::Column(ColumnNode {
                        table: None,
                        column: EcoString::from("date"),
                    }),
                    direction: OrderDirection::Asc,
                    null_order: NullOrder::Default,
                }],
            }),
        };
        let mut buf = String::new();
        func.deparse(&mut buf);
        assert_eq!(
            buf,
            "SUM(amount) OVER (PARTITION BY category ORDER BY date ASC)"
        );
    }

    #[test]
    fn test_window_function_multiple_order_by() {
        let sql = "SELECT sum(x) OVER (ORDER BY a ASC, b DESC) FROM t";
        let select = parse_select(sql);

        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ScalarExpr::Function(func) = &columns[0].expr().expect("non-star SELECT column") else {
            panic!("expected function");
        };

        let over = func.over.as_ref().unwrap();
        assert_eq!(over.order_by.len(), 2, "two ORDER BY clauses");
        assert_eq!(over.order_by[0].direction, OrderDirection::Asc);
        assert_eq!(over.order_by[1].direction, OrderDirection::Desc);
    }

    #[test]
    fn test_aggregate_order_by_parse() {
        let sql = "SELECT string_agg(name, ', ' ORDER BY name) FROM t";
        let select = parse_select(sql);

        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ScalarExpr::Function(func) = &columns[0].expr().expect("non-star SELECT column") else {
            panic!("expected function");
        };

        assert_eq!(func.name, "string_agg");
        assert_eq!(func.agg_order.len(), 1, "should have one ORDER BY clause");
        assert_eq!(func.agg_order[0].direction, OrderDirection::Asc);
    }

    #[test]
    fn test_aggregate_order_by_deparse() {
        let sql = "SELECT string_agg(name, ', ' ORDER BY name ASC) FROM t";
        let ast = parse_query(sql);

        let mut buf = String::new();
        ast.deparse(&mut buf);
        assert!(buf.contains("ORDER BY"), "deparsed should contain ORDER BY");
        assert!(
            buf.contains("STRING_AGG"),
            "deparsed should contain STRING_AGG"
        );
    }

    #[test]
    fn test_aggregate_distinct_and_order_by() {
        let sql = "SELECT string_agg(DISTINCT name, ', ' ORDER BY name) FROM t";
        let select = parse_select(sql);

        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ScalarExpr::Function(func) = &columns[0].expr().expect("non-star SELECT column") else {
            panic!("expected function");
        };

        assert!(func.agg_distinct, "should have DISTINCT");
        assert_eq!(func.agg_order.len(), 1, "should have ORDER BY");
    }

    #[test]
    fn test_aggregate_multiple_order_by() {
        let sql = "SELECT array_agg(x ORDER BY y ASC, z DESC) FROM t";
        let select = parse_select(sql);

        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ScalarExpr::Function(func) = &columns[0].expr().expect("non-star SELECT column") else {
            panic!("expected function");
        };

        assert_eq!(func.name, "array_agg");
        assert_eq!(func.agg_order.len(), 2, "should have two ORDER BY clauses");
        assert_eq!(func.agg_order[0].direction, OrderDirection::Asc);
        assert_eq!(func.agg_order[1].direction, OrderDirection::Desc);
    }

    #[test]
    fn test_filter_aggregate_parsed() {
        let sql = "SELECT count(*) FILTER (WHERE x = 1) FROM t";
        let select = parse_select(sql);

        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ScalarExpr::Function(func) = &columns[0].expr().expect("non-star SELECT column") else {
            panic!("expected function");
        };

        assert!(func.agg_star);
        assert!(
            func.agg_filter.is_some(),
            "FILTER predicate should be captured on the FunctionCall"
        );
    }

    /// Two FILTER aggregates with different predicates must round-trip to
    /// distinct function calls — they must not collapse to plain count(*).
    #[test]
    fn test_filter_aggregate_distinct_predicates_roundtrip() {
        let sql = "SELECT count(*) FILTER (WHERE posttypeid = 1) AS questions, \
                   count(*) FILTER (WHERE posttypeid = 2) AS answers FROM posts";
        let select = parse_select(sql);

        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };
        assert_eq!(columns.len(), 2);

        let mut buf = String::new();
        for (i, col) in columns.iter().enumerate() {
            let ScalarExpr::Function(func) = &col.expr().expect("non-star SELECT column") else {
                panic!("expected function in column {i}");
            };
            assert!(
                func.agg_filter.is_some(),
                "column {i} should retain its FILTER predicate"
            );
            buf.clear();
            func.deparse(&mut buf);
            assert!(
                buf.contains("FILTER (WHERE"),
                "column {i} should deparse FILTER clause; got `{buf}`"
            );
        }

        // The two columns must deparse differently — collapsing them was the bug.
        let mut a = String::new();
        let ScalarExpr::Function(f0) = &columns[0].expr().expect("non-star SELECT column") else {
            unreachable!()
        };
        f0.deparse(&mut a);
        let mut b = String::new();
        let ScalarExpr::Function(f1) = &columns[1].expr().expect("non-star SELECT column") else {
            unreachable!()
        };
        f1.deparse(&mut b);
        assert_ne!(a, b, "FILTER predicates 1 and 2 must deparse differently");
    }

    #[test]
    fn test_aggregate_order_by_deparse_roundtrip() {
        let func = FunctionCall {
            name: EcoString::from("string_agg"),
            args: vec![
                ScalarExpr::Column(ColumnNode {
                    table: None,
                    column: EcoString::from("name"),
                }),
                ScalarExpr::Literal(LiteralValue::String(", ".to_owned())),
            ],
            agg_star: false,
            agg_distinct: false,
            agg_order: vec![OrderByClause {
                expr: ScalarExpr::Column(ColumnNode {
                    table: None,
                    column: EcoString::from("name"),
                }),
                direction: OrderDirection::Asc,
                null_order: NullOrder::Default,
            }],
            agg_filter: None,
            over: None,
        };
        let mut buf = String::new();
        func.deparse(&mut buf);
        assert_eq!(buf, "STRING_AGG(name, ', ' ORDER BY name ASC)");
    }

    #[test]
    fn test_arithmetic_multiply_parse() {
        let sql = "SELECT amount * 2 FROM t";
        let select = parse_select(sql);

        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ScalarExpr::Arithmetic(arith) = &columns[0].expr().expect("non-star SELECT column")
        else {
            panic!("expected arithmetic expression");
        };

        assert_eq!(arith.op, ArithmeticOp::Multiply);
    }

    #[test]
    fn test_arithmetic_multiply_negative() {
        let sql = "SELECT amount * -1 FROM t";
        let select = parse_select(sql);

        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ScalarExpr::Arithmetic(arith) = &columns[0].expr().expect("non-star SELECT column")
        else {
            panic!("expected arithmetic expression");
        };

        assert_eq!(arith.op, ArithmeticOp::Multiply);
        // Right side should be -1 (negative literal)
        assert!(matches!(
            arith.right.as_ref(),
            ScalarExpr::Literal(LiteralValue::Integer(-1))
        ));
    }

    #[test]
    fn test_arithmetic_add() {
        let sql = "SELECT price + tax FROM t";
        let select = parse_select(sql);

        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ScalarExpr::Arithmetic(arith) = &columns[0].expr().expect("non-star SELECT column")
        else {
            panic!("expected arithmetic expression");
        };

        assert_eq!(arith.op, ArithmeticOp::Add);
    }

    #[test]
    fn test_arithmetic_subtract() {
        let sql = "SELECT total - discount FROM t";
        let select = parse_select(sql);

        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ScalarExpr::Arithmetic(arith) = &columns[0].expr().expect("non-star SELECT column")
        else {
            panic!("expected arithmetic expression");
        };

        assert_eq!(arith.op, ArithmeticOp::Subtract);
    }

    #[test]
    fn test_arithmetic_divide() {
        let sql = "SELECT total / count FROM t";
        let select = parse_select(sql);

        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ScalarExpr::Arithmetic(arith) = &columns[0].expr().expect("non-star SELECT column")
        else {
            panic!("expected arithmetic expression");
        };

        assert_eq!(arith.op, ArithmeticOp::Divide);
    }

    #[test]
    fn test_arithmetic_deparse() {
        let arith = ArithmeticExpr {
            left: Box::new(ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("amount"),
            })),
            op: ArithmeticOp::Multiply,
            right: Box::new(ScalarExpr::Literal(LiteralValue::Integer(-1))),
        };
        let mut buf = String::new();
        arith.deparse(&mut buf);
        assert_eq!(buf, "(amount * -1)");
    }

    #[test]
    fn test_arithmetic_nested() {
        // (a + b) * c
        let sql = "SELECT (a + b) * c FROM t";
        let select = parse_select(sql);

        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ScalarExpr::Arithmetic(outer) = &columns[0].expr().expect("non-star SELECT column")
        else {
            panic!("expected arithmetic expression");
        };

        assert_eq!(outer.op, ArithmeticOp::Multiply);

        // Left side should be another arithmetic expression (a + b)
        let ScalarExpr::Arithmetic(inner) = outer.left.as_ref() else {
            panic!("expected nested arithmetic expression");
        };
        assert_eq!(inner.op, ArithmeticOp::Add);
    }

    // ========================================================================
    // TypeCast tests
    // ========================================================================

    /// Parse `sql` and return the first SELECT column's expr.
    fn parse_first_column(sql: &str) -> ScalarExpr {
        let select = parse_select(sql);
        let SelectColumns::Columns(columns) = select.columns else {
            panic!("expected columns");
        };
        let SelectColumn::Expr { expr, .. } =
            columns.into_iter().next().expect("at least one column")
        else {
            panic!("expected Expr column");
        };
        expr
    }

    /// Parse `sql` and unwrap the first column as a `(inner, target_type)` cast.
    fn parse_first_typecast(sql: &str) -> (ScalarExpr, EcoString) {
        match parse_first_column(sql) {
            ScalarExpr::TypeCast { expr, target_type } => (*expr, target_type),
            other => panic!("expected type cast, got {other:?}"),
        }
    }

    #[test]
    fn test_type_cast_count_star_int() {
        let (expr, target_type) = parse_first_typecast("SELECT COUNT(*)::INT FROM t");
        assert_eq!(target_type.as_str(), "int4");
        assert!(matches!(expr, ScalarExpr::Function(f) if f.name == "count"));
    }

    #[test]
    fn test_type_cast_column_text() {
        let (expr, target_type) = parse_first_typecast("SELECT col::text FROM t");
        assert_eq!(target_type.as_str(), "text");
        assert!(matches!(expr, ScalarExpr::Column(c) if c.column == "col"));
    }

    #[test]
    fn test_type_cast_arithmetic_numeric_typmods() {
        let (expr, target_type) = parse_first_typecast("SELECT (a + b)::numeric(10,2) FROM t");
        assert_eq!(target_type.as_str(), "numeric(10,2)");
        assert!(matches!(expr, ScalarExpr::Arithmetic(_)));
    }

    #[test]
    fn test_type_cast_qualified_column() {
        let (expr, target_type) = parse_first_typecast("SELECT t.col::int FROM t");
        assert_eq!(target_type.as_str(), "int4");
        let ScalarExpr::Column(col) = expr else {
            panic!("expected column");
        };
        assert_eq!(col.table.as_deref(), Some("t"));
        assert_eq!(col.column.as_str(), "col");
    }

    #[test]
    fn test_type_cast_literal_date_still_parses() {
        let (expr, target_type) = parse_first_typecast("SELECT '2024-01-01'::DATE FROM t");
        assert_eq!(target_type.as_str(), "date");
        assert!(matches!(expr, ScalarExpr::Literal(LiteralValue::String(_))));
    }

    #[test]
    fn test_type_cast_deparse_round_trip() {
        let sql = "SELECT COUNT(*)::INT, SUM(amount)::NUMERIC(18,2) FROM t";
        let select = parse_select(sql);

        let mut buf = String::new();
        select.deparse(&mut buf);
        assert!(
            buf.contains("(COUNT(*))::int4"),
            "expected count cast in: {buf}"
        );
        assert!(
            buf.contains("(SUM(amount))::numeric(18,2)"),
            "expected sum cast in: {buf}"
        );
    }

    #[test]
    fn test_type_cast_aliased() {
        let select = parse_select("SELECT COUNT(*)::INT AS n FROM t");
        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };
        assert_eq!(columns[0].alias().cloned().as_deref(), Some("n"));
        assert!(matches!(
            &columns[0].expr().expect("non-star SELECT column"),
            ScalarExpr::TypeCast { .. }
        ));
    }

    // ========================================================================
    // QueryExpr (new type hierarchy) tests
    // ========================================================================

    #[test]
    fn test_query_expr_simple_select() {
        let sql = "SELECT id, name FROM users WHERE id = 1";
        let pg_ast = pg_query::parse(sql).unwrap();
        let query_expr = query_expr_convert(&pg_ast).unwrap();

        assert!(query_expr.is_single_table());
        assert!(query_expr.has_where_clause());

        let select = query_expr.as_select().unwrap();
        assert_eq!(select.from.len(), 1);
        assert!(matches!(select.columns, SelectColumns::Columns(_)));
    }

    #[test]
    fn test_query_expr_select_star() {
        let sql = "SELECT * FROM products";
        let pg_ast = pg_query::parse(sql).unwrap();
        let query_expr = query_expr_convert(&pg_ast).unwrap();

        assert!(query_expr.is_single_table());
        assert!(!query_expr.has_where_clause());

        let SelectColumns::Columns(cols) = &query_expr.as_select().unwrap().columns else {
            panic!("Expected Columns");
        };
        assert_eq!(cols.len(), 1);
        assert!(matches!(&cols[0], SelectColumn::Star(None)));
    }

    #[test]
    fn test_query_expr_select_star_with_column() {
        let sql = "SELECT *, col FROM test";
        let pg_ast = pg_query::parse(sql).unwrap();
        let query_expr = query_expr_convert(&pg_ast).unwrap();

        let SelectColumns::Columns(cols) = &query_expr.as_select().unwrap().columns else {
            panic!("Expected Columns");
        };
        assert_eq!(cols.len(), 2);
        assert!(matches!(&cols[0], SelectColumn::Star(None)));
        assert!(
            matches!(&cols[1].expr().expect("non-star SELECT column"), ScalarExpr::Column(c) if c.column == "col")
        );
    }

    #[test]
    fn test_query_expr_select_qualified_star() {
        let sql = "SELECT t1.*, t2.col FROM test t1 JOIN test2 t2 ON t2.id = t1.id";
        let pg_ast = pg_query::parse(sql).unwrap();
        let query_expr = query_expr_convert(&pg_ast).unwrap();

        let SelectColumns::Columns(cols) = &query_expr.as_select().unwrap().columns else {
            panic!("Expected Columns");
        };
        assert_eq!(cols.len(), 2);
        assert!(matches!(&cols[0], SelectColumn::Star(Some(t)) if t == "t1"));
        assert!(
            matches!(&cols[1].expr().expect("non-star SELECT column"), ScalarExpr::Column(c) if c.column == "col" && c.table.as_deref() == Some("t2"))
        );
    }

    #[test]
    fn test_query_expr_select_star_deparse() {
        // Bare star
        let sql = "SELECT * FROM products";
        let pg_ast = pg_query::parse(sql).unwrap();
        let query_expr = query_expr_convert(&pg_ast).unwrap();
        let mut buf = String::new();
        query_expr.deparse(&mut buf);
        assert!(
            buf.contains("SELECT *"),
            "should deparse as SELECT *: {buf}"
        );

        // Qualified star with column
        let sql = "SELECT t1.*, t2.name FROM a t1 JOIN b t2 ON t2.id = t1.id";
        let pg_ast = pg_query::parse(sql).unwrap();
        let query_expr = query_expr_convert(&pg_ast).unwrap();
        let mut buf = String::new();
        query_expr.deparse(&mut buf);
        assert!(buf.contains("t1.*"), "should deparse qualified star: {buf}");
        assert!(buf.contains("t2.name"), "should deparse column: {buf}");
    }

    #[test]
    fn test_query_expr_values_clause() {
        let sql = "VALUES (1, 'a'), (2, 'b')";
        let pg_ast = pg_query::parse(sql).unwrap();
        let query_expr = query_expr_convert(&pg_ast).unwrap();

        let QueryBody::Values(values) = &query_expr.body else {
            panic!("expected VALUES clause");
        };

        assert_eq!(values.rows.len(), 2);
        assert_eq!(values.rows[0].len(), 2);
        assert_eq!(values.rows[1].len(), 2);

        // Check first row values
        assert!(matches!(values.rows[0][0], LiteralValue::Integer(1)));
        assert!(matches!(&values.rows[0][1], LiteralValue::String(s) if s == "a"));
    }

    #[test]
    fn test_query_expr_union() {
        let sql = "SELECT a FROM t1 UNION SELECT b FROM t2";
        let pg_ast = pg_query::parse(sql).unwrap();
        let query_expr = query_expr_convert(&pg_ast).unwrap();

        let QueryBody::SetOp(set_op) = &query_expr.body else {
            panic!("expected set operation");
        };

        assert_eq!(set_op.op, SetOpType::Union);
        assert!(!set_op.all);

        // Check left side is a SELECT
        assert!(set_op.left.as_select().is_some());
        // Check right side is a SELECT
        assert!(set_op.right.as_select().is_some());
    }

    #[test]
    fn test_query_expr_union_all() {
        let sql = "SELECT a FROM t1 UNION ALL SELECT b FROM t2";
        let pg_ast = pg_query::parse(sql).unwrap();
        let query_expr = query_expr_convert(&pg_ast).unwrap();

        let QueryBody::SetOp(set_op) = &query_expr.body else {
            panic!("expected set operation");
        };

        assert_eq!(set_op.op, SetOpType::Union);
        assert!(set_op.all);
    }

    #[test]
    fn test_query_expr_union_with_order_by() {
        let sql = "SELECT a FROM t1 UNION SELECT b FROM t2 ORDER BY 1";
        let pg_ast = pg_query::parse(sql).unwrap();
        let query_expr = query_expr_convert(&pg_ast).unwrap();

        // ORDER BY should be at the top level
        assert_eq!(query_expr.order_by.len(), 1);

        let QueryBody::SetOp(set_op) = &query_expr.body else {
            panic!("expected set operation");
        };
        assert_eq!(set_op.op, SetOpType::Union);

        // Sub-queries should not have ORDER BY
        assert!(set_op.left.order_by.is_empty());
        assert!(set_op.right.order_by.is_empty());
    }

    #[test]
    fn test_query_expr_intersect() {
        let sql = "SELECT a FROM t1 INTERSECT SELECT b FROM t2";
        let pg_ast = pg_query::parse(sql).unwrap();
        let query_expr = query_expr_convert(&pg_ast).unwrap();

        let QueryBody::SetOp(set_op) = &query_expr.body else {
            panic!("expected set operation");
        };

        assert_eq!(set_op.op, SetOpType::Intersect);
    }

    #[test]
    fn test_query_expr_except() {
        let sql = "SELECT a FROM t1 EXCEPT SELECT b FROM t2";
        let pg_ast = pg_query::parse(sql).unwrap();
        let query_expr = query_expr_convert(&pg_ast).unwrap();

        let QueryBody::SetOp(set_op) = &query_expr.body else {
            panic!("expected set operation");
        };

        assert_eq!(set_op.op, SetOpType::Except);
    }

    #[test]
    fn test_query_expr_chained_union() {
        let sql = "SELECT a FROM t1 UNION SELECT b FROM t2 UNION SELECT c FROM t3";
        let pg_ast = pg_query::parse(sql).unwrap();
        let query_expr = query_expr_convert(&pg_ast).unwrap();

        let QueryBody::SetOp(outer_set_op) = &query_expr.body else {
            panic!("expected set operation");
        };
        assert_eq!(outer_set_op.op, SetOpType::Union);

        // The nested structure: (t1 UNION t2) UNION t3
        let QueryBody::SetOp(inner_set_op) = &outer_set_op.left.body else {
            panic!("expected nested set operation");
        };
        assert_eq!(inner_set_op.op, SetOpType::Union);

        // Right side of outer should be simple SELECT
        assert!(outer_set_op.right.as_select().is_some());
    }

    #[test]
    fn test_query_expr_deparse_simple_select() {
        let sql = "SELECT id FROM users WHERE id = 1";
        let pg_ast = pg_query::parse(sql).unwrap();
        let query_expr = query_expr_convert(&pg_ast).unwrap();

        let mut buf = String::new();
        query_expr.deparse(&mut buf);
        assert_eq!(buf, "SELECT id FROM users WHERE id = 1");
    }

    #[test]
    fn test_query_expr_deparse_values() {
        let sql = "VALUES (1, 'a')";
        let pg_ast = pg_query::parse(sql).unwrap();
        let query_expr = query_expr_convert(&pg_ast).unwrap();

        let mut buf = String::new();
        query_expr.deparse(&mut buf);
        assert_eq!(buf, "VALUES (1, 'a')");
    }

    #[test]
    fn test_query_expr_deparse_union() {
        let sql = "SELECT a FROM t1 UNION SELECT b FROM t2";
        let pg_ast = pg_query::parse(sql).unwrap();
        let query_expr = query_expr_convert(&pg_ast).unwrap();

        let mut buf = String::new();
        query_expr.deparse(&mut buf);
        assert_eq!(buf, "SELECT a FROM t1 UNION SELECT b FROM t2");
    }

    #[test]
    fn test_query_expr_deparse_union_all() {
        let sql = "SELECT a FROM t1 UNION ALL SELECT b FROM t2";
        let pg_ast = pg_query::parse(sql).unwrap();
        let query_expr = query_expr_convert(&pg_ast).unwrap();

        let mut buf = String::new();
        query_expr.deparse(&mut buf);
        assert_eq!(buf, "SELECT a FROM t1 UNION ALL SELECT b FROM t2");
    }

    #[test]
    fn test_query_expr_order_by_limit() {
        let sql = "SELECT a FROM t ORDER BY a LIMIT 10";
        let pg_ast = pg_query::parse(sql).unwrap();
        let query_expr = query_expr_convert(&pg_ast).unwrap();

        assert_eq!(query_expr.order_by.len(), 1);
        assert!(query_expr.limit.is_some());

        let mut buf = String::new();
        query_expr.deparse(&mut buf);
        assert_eq!(buf, "SELECT a FROM t ORDER BY a ASC LIMIT 10");
    }

    #[test]
    fn test_select_nodes_simple_select() {
        let sql = "SELECT id FROM users WHERE id = 1";
        let pg_ast = pg_query::parse(sql).unwrap();
        let query_expr = query_expr_convert(&pg_ast).unwrap();

        let branches = query_expr.select_nodes();
        assert_eq!(branches.len(), 1, "simple SELECT should have one branch");
    }

    #[test]
    fn test_select_nodes_union() {
        let sql = "SELECT id FROM users UNION SELECT id FROM admins";
        let pg_ast = pg_query::parse(sql).unwrap();
        let query_expr = query_expr_convert(&pg_ast).unwrap();

        let branches = query_expr.select_nodes();
        assert_eq!(branches.len(), 2, "UNION should have two branches");

        // Verify each branch has a FROM clause with different tables
        let tables: Vec<_> = branches
            .iter()
            .filter_map(|b| b.from.first())
            .filter_map(|ts| {
                if let TableSource::Table(t) = ts {
                    Some(t.name.as_str())
                } else {
                    None
                }
            })
            .collect();

        assert!(tables.contains(&"users"));
        assert!(tables.contains(&"admins"));
    }

    #[test]
    fn test_select_nodes_nested_union() {
        let sql = "SELECT id FROM a UNION SELECT id FROM b UNION SELECT id FROM c";
        let pg_ast = pg_query::parse(sql).unwrap();
        let query_expr = query_expr_convert(&pg_ast).unwrap();

        let branches = query_expr.select_nodes();
        assert_eq!(branches.len(), 3, "nested UNION should have three branches");
    }

    #[test]
    fn test_select_nodes_from_subquery() {
        // Derived table in FROM clause
        let sql = "SELECT * FROM (SELECT id FROM users) sub";
        let pg_ast = pg_query::parse(sql).unwrap();
        let query_expr = query_expr_convert(&pg_ast).unwrap();

        let branches = query_expr.select_nodes();
        // Outer SELECT + inner SELECT in FROM subquery
        assert_eq!(branches.len(), 2, "FROM subquery should add one branch");
    }

    #[test]
    fn test_select_nodes_where_subquery() {
        // IN subquery in WHERE clause
        let sql = "SELECT * FROM users WHERE id IN (SELECT user_id FROM active)";
        let pg_ast = pg_query::parse(sql).unwrap();
        let query_expr = query_expr_convert(&pg_ast).unwrap();

        let branches = query_expr.select_nodes();
        // Outer SELECT + inner SELECT in WHERE subquery
        assert_eq!(branches.len(), 2, "WHERE IN subquery should add one branch");
    }

    #[test]
    fn test_select_nodes_exists_subquery() {
        // EXISTS subquery in WHERE clause
        let sql = "SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM items WHERE items.order_id = orders.id)";
        let pg_ast = pg_query::parse(sql).unwrap();
        let query_expr = query_expr_convert(&pg_ast).unwrap();

        let branches = query_expr.select_nodes();
        // Outer SELECT + inner SELECT in EXISTS subquery
        assert_eq!(
            branches.len(),
            2,
            "WHERE EXISTS subquery should add one branch"
        );
    }

    #[test]
    fn test_select_nodes_scalar_subquery() {
        // Scalar subquery in SELECT list
        let sql = "SELECT id, (SELECT name FROM users WHERE users.id = orders.user_id) FROM orders";
        let pg_ast = pg_query::parse(sql).unwrap();
        let query_expr = query_expr_convert(&pg_ast).unwrap();

        let branches = query_expr.select_nodes();
        // Outer SELECT + inner SELECT in SELECT list subquery
        assert_eq!(
            branches.len(),
            2,
            "SELECT list subquery should add one branch"
        );
    }

    #[test]
    fn test_select_nodes_nested_subqueries() {
        // Nested subqueries
        let sql =
            "SELECT * FROM (SELECT id FROM users WHERE id IN (SELECT user_id FROM active)) sub";
        let pg_ast = pg_query::parse(sql).unwrap();
        let query_expr = query_expr_convert(&pg_ast).unwrap();

        let branches = query_expr.select_nodes();
        // Outer SELECT + FROM subquery + nested IN subquery
        assert_eq!(
            branches.len(),
            3,
            "nested subqueries should add all branches"
        );
    }

    // ==========================================================================
    // WHERE Subquery Parsing Tests
    // ==========================================================================

    #[test]
    fn test_where_subquery_in_parsed() {
        // Test that IN subquery is properly parsed
        let select =
            parse_select("SELECT * FROM users WHERE id IN (SELECT user_id FROM active_users)");

        let where_clause = select
            .where_clause
            .as_ref()
            .expect("should have WHERE clause");

        match where_clause {
            WhereExpr::Subquery {
                query,
                sublink_type,
                test_expr,
            } => {
                assert_eq!(
                    *sublink_type,
                    SubLinkType::Any,
                    "IN should parse as SubLinkType::Any"
                );
                assert!(test_expr.is_some(), "IN should have test expression (id)");

                // Verify the inner query structure
                match &query.body {
                    QueryBody::Select(inner_select) => {
                        let tables: Vec<&TableNode> = inner_select.nodes().collect();
                        assert_eq!(tables.len(), 1);
                        assert_eq!(tables[0].name, "active_users");
                    }
                    _ => panic!("Expected inner SELECT"),
                }
            }
            _ => panic!("Expected WhereExpr::Subquery, got {:?}", where_clause),
        }
    }

    #[test]
    fn test_where_subquery_exists_parsed() {
        // Test that EXISTS subquery is properly parsed
        let select = parse_select(
            "SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM items WHERE items.order_id = orders.id)",
        );

        let where_clause = select
            .where_clause
            .as_ref()
            .expect("should have WHERE clause");

        match where_clause {
            WhereExpr::Subquery {
                sublink_type,
                test_expr,
                ..
            } => {
                assert_eq!(
                    *sublink_type,
                    SubLinkType::Exists,
                    "EXISTS should parse as SubLinkType::Exists"
                );
                assert!(
                    test_expr.is_none(),
                    "EXISTS should not have test expression"
                );
            }
            _ => panic!("Expected WhereExpr::Subquery, got {:?}", where_clause),
        }
    }

    #[test]
    fn test_where_subquery_scalar_parsed() {
        // Test that scalar subquery in WHERE is properly parsed
        let select = parse_select("SELECT * FROM users WHERE age > (SELECT AVG(age) FROM users)");

        let where_clause = select
            .where_clause
            .as_ref()
            .expect("should have WHERE clause");

        // The scalar subquery should be on the right side of the > comparison
        match where_clause {
            WhereExpr::Binary(binary) => match binary.rexpr.as_ref() {
                WhereExpr::Subquery {
                    sublink_type,
                    test_expr,
                    ..
                } => {
                    assert_eq!(
                        *sublink_type,
                        SubLinkType::Expr,
                        "Scalar subquery should parse as SubLinkType::Expr"
                    );
                    assert!(
                        test_expr.is_none(),
                        "Scalar subquery should not have test expression"
                    );
                }
                _ => panic!("Expected WhereExpr::Subquery on right side"),
            },
            _ => panic!("Expected WhereExpr::Binary, got {:?}", where_clause),
        }
    }

    #[test]
    fn test_where_subquery_not_in_parsed() {
        // NOT IN should also parse as SubLinkType::Any with proper negation
        let select =
            parse_select("SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM banned_users)");

        // PostgreSQL parses NOT IN as a negated ANY sublink
        // The structure may vary, but we should be able to find the subquery
        assert!(select.where_clause.is_some(), "Should have WHERE clause");
        assert!(select.has_subqueries(), "Should detect sublink in NOT IN");
    }

    #[test]
    fn test_where_subquery_has_subqueries_traversal() {
        // Verify nodes() traverses into subqueries to find TableNode
        let select =
            parse_select("SELECT * FROM users WHERE id IN (SELECT user_id FROM active_users)");

        let tables: Vec<&TableNode> = select.nodes().collect();

        // Should find both outer table (users) and inner table (active_users)
        assert_eq!(
            tables.len(),
            2,
            "Should find tables in both outer query and subquery"
        );

        let table_names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"users"), "Should find outer table");
        assert!(
            table_names.contains(&"active_users"),
            "Should find subquery table"
        );
    }

    #[test]
    fn test_where_subquery_deparse_in() {
        let select =
            parse_select("SELECT * FROM users WHERE id IN (SELECT user_id FROM active_users)");
        let mut buf = String::new();
        select.deparse(&mut buf);

        // The deparsed output should contain IN and the subquery
        assert!(buf.contains("IN"), "Deparsed should contain IN: {}", buf);
        assert!(
            buf.contains("active_users"),
            "Deparsed should contain subquery table: {}",
            buf
        );
    }

    #[test]
    fn test_where_subquery_deparse_exists() {
        let select = parse_select("SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM items)");
        let mut buf = String::new();
        select.deparse(&mut buf);

        assert!(
            buf.contains("EXISTS"),
            "Deparsed should contain EXISTS: {}",
            buf
        );
        assert!(
            buf.contains("items"),
            "Deparsed should contain subquery table: {}",
            buf
        );
    }

    // ====================================================================
    // CTE tests
    // ====================================================================

    #[test]
    fn test_cte_simple_parsed() {
        let query = parse_query("WITH x AS (SELECT id FROM users) SELECT * FROM x");

        // QueryExpr should have one CTE definition
        assert_eq!(query.ctes.len(), 1);
        assert_eq!(query.ctes[0].name, "x");
        assert_eq!(query.ctes[0].materialization, CteMaterialization::Default);

        // Body should reference CTE via CteRef, not Table
        let select = query.as_select().unwrap();
        assert_eq!(select.from.len(), 1);
        match &select.from[0] {
            TableSource::CteRef(cte_ref) => {
                assert_eq!(cte_ref.cte_name, "x");
                assert!(cte_ref.alias.is_none());
                // Inner query should reference "users"
                let inner_tables: Vec<_> = cte_ref
                    .query
                    .nodes::<TableNode>()
                    .map(|t| t.name.as_str())
                    .collect();
                assert_eq!(inner_tables, vec!["users"]);
            }
            other => panic!("expected CteRef, got {other:?}"),
        }
    }

    #[test]
    fn test_cte_deparse() {
        let query = parse_query("WITH x AS (SELECT id FROM users) SELECT * FROM x");
        let mut buf = String::new();
        query.deparse(&mut buf);

        assert!(buf.contains("WITH"), "should contain WITH: {buf}");
        assert!(buf.contains("x AS"), "should contain CTE name: {buf}");
        // The FROM clause should reference x by name, not inline the body
        // The output should have the CTE body once (in WITH) and the name once (in FROM)
        let x_count = buf.matches(" x").count();
        assert!(
            x_count >= 2,
            "should reference x at least twice (definition + FROM): {buf}"
        );
    }

    #[test]
    fn test_cte_column_aliases() {
        let query = parse_query("WITH x(a, b) AS (SELECT id, name FROM users) SELECT * FROM x");

        assert_eq!(query.ctes[0].column_aliases, vec!["a", "b"]);

        // CteRefNode should also have column aliases
        let select = query.as_select().unwrap();
        match &select.from[0] {
            TableSource::CteRef(cte_ref) => {
                assert_eq!(cte_ref.column_aliases, vec!["a", "b"]);
            }
            other => panic!("expected CteRef, got {other:?}"),
        }
    }

    #[test]
    fn test_cte_reference_alias() {
        let query = parse_query("WITH x AS (SELECT id FROM users) SELECT * FROM x AS y");

        let select = query.as_select().unwrap();
        match &select.from[0] {
            TableSource::CteRef(cte_ref) => {
                assert_eq!(cte_ref.cte_name, "x");
                assert_eq!(cte_ref.alias.as_ref().unwrap().name, "y");
            }
            other => panic!("expected CteRef, got {other:?}"),
        }
    }

    #[test]
    fn test_cte_with_join() {
        let query = parse_query(
            "WITH active AS (SELECT id, name FROM users WHERE active = true) \
             SELECT u.id, a.name FROM users u JOIN active a ON u.id = a.id",
        );

        assert_eq!(query.ctes.len(), 1);
        let select = query.as_select().unwrap();
        assert_eq!(select.from.len(), 1);

        // Should be a Join with Table on left and CteRef on right
        match &select.from[0] {
            TableSource::Join(join) => {
                assert!(matches!(*join.left, TableSource::Table(_)));
                assert!(matches!(*join.right, TableSource::CteRef(_)));
            }
            other => panic!("expected Join, got {other:?}"),
        }
    }

    #[test]
    fn test_cte_referencing_cte() {
        let query = parse_query(
            "WITH a AS (SELECT id FROM users), \
             b AS (SELECT * FROM a) \
             SELECT * FROM b",
        );

        assert_eq!(query.ctes.len(), 2);
        assert_eq!(query.ctes[0].name, "a");
        assert_eq!(query.ctes[1].name, "b");

        // CTE b's body should contain a CteRef to a
        let b_select = query.ctes[1].query.as_select().unwrap();
        match &b_select.from[0] {
            TableSource::CteRef(cte_ref) => {
                assert_eq!(cte_ref.cte_name, "a");
            }
            other => panic!("expected CteRef in b's body, got {other:?}"),
        }
    }

    #[test]
    fn test_cte_multiple_references() {
        let query = parse_query(
            "WITH x AS (SELECT id FROM users) \
             SELECT * FROM x a JOIN x b ON a.id = b.id",
        );

        assert_eq!(query.ctes.len(), 1);
        let select = query.as_select().unwrap();

        // Should be a Join with two CteRef nodes
        match &select.from[0] {
            TableSource::Join(join) => {
                assert!(matches!(*join.left, TableSource::CteRef(_)));
                assert!(matches!(*join.right, TableSource::CteRef(_)));
            }
            other => panic!("expected Join, got {other:?}"),
        }
    }

    #[test]
    fn test_cte_in_union() {
        let query = parse_query(
            "WITH x AS (SELECT id FROM users) \
             SELECT * FROM x UNION ALL SELECT * FROM x",
        );

        assert_eq!(query.ctes.len(), 1);

        match &query.body {
            QueryBody::SetOp(set_op) => {
                // Both sides should have CteRef
                let left_select = set_op.left.as_select().unwrap();
                assert!(matches!(&left_select.from[0], TableSource::CteRef(_)));

                let right_select = set_op.right.as_select().unwrap();
                assert!(matches!(&right_select.from[0], TableSource::CteRef(_)));
            }
            _ => panic!("expected SetOp"),
        }
    }

    #[test]
    fn test_cte_unreferenced() {
        let query = parse_query("WITH x AS (SELECT 1) SELECT * FROM users");

        // CTE definition should still be stored
        assert_eq!(query.ctes.len(), 1);
        assert_eq!(query.ctes[0].name, "x");

        // Body should reference users as a Table, not CteRef
        let select = query.as_select().unwrap();
        match &select.from[0] {
            TableSource::Table(table) => {
                assert_eq!(table.name, "users");
            }
            other => panic!("expected Table, got {other:?}"),
        }
    }

    #[test]
    fn test_cte_schema_qualified_not_replaced() {
        let query = parse_query("WITH users AS (SELECT 1 AS id) SELECT * FROM public.users");

        assert_eq!(query.ctes.len(), 1);

        // public.users should remain as Table (schema-qualified)
        let select = query.as_select().unwrap();
        match &select.from[0] {
            TableSource::Table(table) => {
                assert_eq!(table.schema.as_deref(), Some("public"));
                assert_eq!(table.name, "users");
            }
            other => panic!("expected Table, got {other:?}"),
        }
    }

    #[test]
    fn test_cte_recursive_rejected() {
        let result =
            pg_query::parse("WITH RECURSIVE x AS (SELECT 1 UNION ALL SELECT 1) SELECT * FROM x");
        if let Ok(ast) = result {
            let result = query_expr_convert(&ast);
            assert!(result.is_err(), "recursive CTE should be rejected");
            let err = result.unwrap_err();
            assert!(
                err.to_string().contains("RECURSIVE"),
                "error should mention RECURSIVE: {err}"
            );
        }
    }

    #[test]
    fn test_cte_materialized() {
        let query = parse_query("WITH x AS MATERIALIZED (SELECT id FROM users) SELECT * FROM x");

        assert_eq!(
            query.ctes[0].materialization,
            CteMaterialization::Materialized
        );

        let select = query.as_select().unwrap();
        match &select.from[0] {
            TableSource::CteRef(cte_ref) => {
                assert_eq!(cte_ref.materialization, CteMaterialization::Materialized);
            }
            other => panic!("expected CteRef, got {other:?}"),
        }
    }

    #[test]
    fn test_cte_not_materialized() {
        let query =
            parse_query("WITH x AS NOT MATERIALIZED (SELECT id FROM users) SELECT * FROM x");

        assert_eq!(
            query.ctes[0].materialization,
            CteMaterialization::NotMaterialized
        );
    }

    #[test]
    fn test_cte_select_nodes() {
        let query =
            parse_query("WITH x AS (SELECT id FROM users WHERE active = true) SELECT * FROM x");

        let branches = query.select_nodes();
        // Outer SELECT + CteRef body (CTE collected via reference, not eagerly from definition)
        assert_eq!(branches.len(), 2);

        // All branches combined should reference "users"
        let table_names: Vec<_> = branches
            .iter()
            .flat_map(|b| b.direct_table_nodes())
            .map(|t| t.name.as_str())
            .collect();
        assert!(
            table_names.contains(&"users"),
            "branches should reference users: {table_names:?}"
        );
    }

    #[test]
    fn test_cte_nodes_traversal() {
        let query = parse_query("WITH x AS (SELECT id FROM users) SELECT * FROM x");

        let table_names: Vec<_> = query
            .nodes::<TableNode>()
            .map(|t| t.name.as_str())
            .collect();
        assert!(
            table_names.contains(&"users"),
            "nodes traversal should find users: {table_names:?}"
        );
    }

    #[test]
    fn test_cte_deparse_materialized() {
        let query = parse_query("WITH x AS MATERIALIZED (SELECT id FROM users) SELECT * FROM x");
        let mut buf = String::new();
        query.deparse(&mut buf);

        assert!(
            buf.contains("MATERIALIZED"),
            "deparse should contain MATERIALIZED: {buf}"
        );
    }

    #[test]
    fn test_cte_deparse_not_materialized() {
        let query =
            parse_query("WITH x AS NOT MATERIALIZED (SELECT id FROM users) SELECT * FROM x");
        let mut buf = String::new();
        query.deparse(&mut buf);

        assert!(
            buf.contains("NOT MATERIALIZED"),
            "deparse should contain NOT MATERIALIZED: {buf}"
        );
    }

    #[test]
    fn test_cte_multiple_definitions() {
        let query = parse_query(
            "WITH a AS (SELECT id FROM users), \
             b AS (SELECT id FROM products) \
             SELECT * FROM a JOIN b ON a.id = b.id",
        );

        assert_eq!(query.ctes.len(), 2);
        assert_eq!(query.ctes[0].name, "a");
        assert_eq!(query.ctes[1].name, "b");

        let select = query.as_select().unwrap();
        match &select.from[0] {
            TableSource::Join(join) => match (&*join.left, &*join.right) {
                (TableSource::CteRef(left), TableSource::CteRef(right)) => {
                    assert_eq!(left.cte_name, "a");
                    assert_eq!(right.cte_name, "b");
                }
                other => panic!("expected two CteRefs, got {other:?}"),
            },
            other => panic!("expected Join, got {other:?}"),
        }
    }

    // ==========================================================================
    // select_nodes_with_source tests
    // ==========================================================================

    #[test]
    fn test_select_nodes_with_source_simple() {
        let query = parse_query("SELECT id FROM users WHERE id = 1");
        let branches = query.select_nodes_with_source();

        assert_eq!(branches.len(), 1);
        assert_eq!(branches[0].1, UpdateQuerySource::FromClause);
    }

    #[test]
    fn test_select_nodes_with_source_union() {
        let query = parse_query("SELECT id FROM users UNION SELECT id FROM admins");
        let branches = query.select_nodes_with_source();

        assert_eq!(branches.len(), 2);
        // Both branches of a UNION are FromClause
        assert!(
            branches
                .iter()
                .all(|(_, src)| *src == UpdateQuerySource::FromClause)
        );
    }

    #[test]
    fn test_select_nodes_with_source_where_in() {
        let query =
            parse_query("SELECT * FROM users WHERE id IN (SELECT user_id FROM active_users)");
        let branches = query.select_nodes_with_source();

        // Outer SELECT (FromClause) + IN subquery (Inclusion)
        assert_eq!(branches.len(), 2);
        assert_eq!(branches[0].1, UpdateQuerySource::FromClause);
        assert_eq!(
            branches[1].1,
            UpdateQuerySource::Subquery(SubqueryKind::Inclusion)
        );
    }

    #[test]
    fn test_select_nodes_with_source_not_in() {
        let query =
            parse_query("SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM banned_users)");
        let branches = query.select_nodes_with_source();

        assert_eq!(branches.len(), 2);
        assert_eq!(branches[0].1, UpdateQuerySource::FromClause);
        // NOT IN is parsed as SubLinkType::All (already Exclusion)
        // or as NOT wrapping Any (negated → Exclusion)
        assert_eq!(
            branches[1].1,
            UpdateQuerySource::Subquery(SubqueryKind::Exclusion),
            "NOT IN subquery should be Exclusion"
        );
    }

    #[test]
    fn test_select_nodes_with_source_exists() {
        let query = parse_query(
            "SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM items WHERE items.order_id = orders.id)",
        );
        let branches = query.select_nodes_with_source();

        assert_eq!(branches.len(), 2);
        assert_eq!(branches[0].1, UpdateQuerySource::FromClause);
        assert_eq!(
            branches[1].1,
            UpdateQuerySource::Subquery(SubqueryKind::Inclusion)
        );
    }

    #[test]
    fn test_select_nodes_with_source_not_exists() {
        let query = parse_query(
            "SELECT * FROM orders WHERE NOT EXISTS (SELECT 1 FROM items WHERE items.order_id = orders.id)",
        );
        let branches = query.select_nodes_with_source();

        assert_eq!(branches.len(), 2);
        assert_eq!(branches[0].1, UpdateQuerySource::FromClause);
        assert_eq!(
            branches[1].1,
            UpdateQuerySource::Subquery(SubqueryKind::Exclusion),
            "NOT EXISTS subquery should be Exclusion"
        );
    }

    #[test]
    fn test_select_nodes_with_source_scalar_in_where() {
        let query = parse_query("SELECT * FROM users WHERE age > (SELECT AVG(age) FROM users)");
        let branches = query.select_nodes_with_source();

        assert_eq!(branches.len(), 2);
        assert_eq!(branches[0].1, UpdateQuerySource::FromClause);
        assert_eq!(
            branches[1].1,
            UpdateQuerySource::Subquery(SubqueryKind::Scalar),
            "Scalar subquery in WHERE should be Scalar"
        );
    }

    #[test]
    fn test_select_nodes_with_source_scalar_in_select() {
        let query = parse_query(
            "SELECT id, (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) FROM users",
        );
        let branches = query.select_nodes_with_source();

        assert_eq!(branches.len(), 2);
        assert_eq!(branches[0].1, UpdateQuerySource::FromClause);
        assert_eq!(
            branches[1].1,
            UpdateQuerySource::Subquery(SubqueryKind::Scalar),
            "Scalar subquery in SELECT list should be Scalar"
        );
    }

    #[test]
    fn test_select_nodes_with_source_from_subquery() {
        let query = parse_query("SELECT * FROM (SELECT id FROM users) sub");
        let branches = query.select_nodes_with_source();

        assert_eq!(branches.len(), 2);
        assert_eq!(branches[0].1, UpdateQuerySource::FromClause);
        assert_eq!(
            branches[1].1,
            UpdateQuerySource::Subquery(SubqueryKind::Inclusion),
            "FROM subquery should be Inclusion"
        );
    }

    #[test]
    fn test_select_nodes_with_source_cte() {
        let query = parse_query(
            "WITH active AS (SELECT id FROM users WHERE active = true) SELECT * FROM active",
        );
        let branches = query.select_nodes_with_source();

        // Outer SELECT (FromClause) + CteRef body (Subquery(Inclusion))
        // CTE definitions are NOT collected — only CteRef references are
        assert_eq!(branches.len(), 2);
        assert_eq!(branches[0].1, UpdateQuerySource::FromClause);
        assert_eq!(
            branches[1].1,
            UpdateQuerySource::Subquery(SubqueryKind::Inclusion),
            "CTE body via CteRef should be Inclusion"
        );

        // The inner branch should reference "users" table
        let inner_tables = branches[1].0.direct_table_nodes();
        assert_eq!(inner_tables.len(), 1);
        assert_eq!(inner_tables[0].name, "users");
    }

    #[test]
    fn test_deparse_or_inside_and_preserves_parentheses() {
        // When an OR expression is nested inside an AND chain, parentheses must
        // be emitted to preserve semantics. Without them:
        //   a AND (b OR c) AND d  →  a AND b OR c AND d
        // which changes evaluation due to AND binding tighter than OR.
        let sql = "SELECT * FROM t WHERE (x = 1 OR y = 2) AND z = 3";
        let ast = parse_query(sql);

        let mut buf = String::with_capacity(1024);
        ast.deparse(&mut buf);

        assert!(
            buf.contains("(x = 1 OR y = 2)"),
            "expected parentheses around OR inside AND chain, got: {buf}"
        );
    }

    #[test]
    fn test_deparse_and_inside_or_no_unnecessary_parentheses() {
        // AND inside OR doesn't need parentheses because AND already binds
        // tighter than OR. The parser produces the same AST with or without
        // parens, so the deparse should not add unnecessary ones.
        let sql = "SELECT * FROM t WHERE x = 1 OR (y = 2 AND z = 3)";
        let ast = parse_query(sql);

        let mut buf = String::with_capacity(1024);
        ast.deparse(&mut buf);

        assert_eq!(
            buf, "SELECT * FROM t WHERE x = 1 OR y = 2 AND z = 3",
            "should not add unnecessary parentheses around AND inside OR"
        );
    }

    #[test]
    fn test_select_nodes_cte_no_duplication() {
        // Both select_nodes() and select_nodes_with_source() collect CTE bodies
        // via CteRef references only, avoiding duplication from CTE definitions
        let query = parse_query("WITH x AS (SELECT id FROM users) SELECT * FROM x");

        let with_source = query.select_nodes_with_source();
        let without_source = query.select_nodes();

        // Both should have 2 (outer + CteRef body)
        assert_eq!(with_source.len(), 2);
        assert_eq!(without_source.len(), 2);
    }

    #[test]
    fn test_fingerprint_excludes_limit() {
        let q1 = parse_query("SELECT * FROM t WHERE id = 1");
        let q2 = parse_query("SELECT * FROM t WHERE id = 1 LIMIT 10");
        let q3 = parse_query("SELECT * FROM t WHERE id = 1 LIMIT 10 OFFSET 20");
        let q4 = parse_query("SELECT * FROM t WHERE id = 1 OFFSET 5");

        let fp1 = query_expr_fingerprint(&q1);
        let fp2 = query_expr_fingerprint(&q2);
        let fp3 = query_expr_fingerprint(&q3);
        let fp4 = query_expr_fingerprint(&q4);

        assert_eq!(fp1, fp2, "LIMIT should not affect fingerprint");
        assert_eq!(fp1, fp3, "LIMIT+OFFSET should not affect fingerprint");
        assert_eq!(fp1, fp4, "OFFSET should not affect fingerprint");

        // Different base queries should still produce different fingerprints
        let q_different = parse_query("SELECT * FROM t WHERE id = 2");
        assert_ne!(
            fp1,
            query_expr_fingerprint(&q_different),
            "different WHERE should produce different fingerprint"
        );
    }

    #[test]
    fn test_limit_clause_deparse() {
        let limit = LimitClause {
            count: Some(LiteralValue::Integer(10)),
            offset: Some(LiteralValue::Integer(5)),
        };
        let mut buf = String::new();
        limit.deparse(&mut buf);
        assert_eq!(buf, " LIMIT 10 OFFSET 5");

        let limit_only = LimitClause {
            count: Some(LiteralValue::Integer(10)),
            offset: None,
        };
        let mut buf = String::new();
        limit_only.deparse(&mut buf);
        assert_eq!(buf, " LIMIT 10");

        let offset_only = LimitClause {
            count: None,
            offset: Some(LiteralValue::Integer(5)),
        };
        let mut buf = String::new();
        offset_only.deparse(&mut buf);
        assert_eq!(buf, " OFFSET 5");
    }
}
