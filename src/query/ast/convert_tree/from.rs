//! FROM-clause sources: tables, CTE references, joins and subqueries.

use ecow::EcoString;
use pg_query::pg_nodes as pg;

use super::select::{cte_query_emit, query_emit};
use super::where_clause::where_emit;
use super::{ParseContext, names_extract};
use crate::cache::SubqueryKind;
use crate::query::ast::raw::{NodePtr, cast, cstr, list_is_empty, node_tag, node_tag_name};
use crate::query::ast::rope::{JoinQualKind, NodeKind, TreeBuilder};
use crate::query::ast::{AstError, JoinType};

fn count(len: usize) -> u32 {
    u32::try_from(len).unwrap_or(u32::MAX)
}

/// One FROM entry.
pub(super) unsafe fn source_emit(
    builder: &mut TreeBuilder<'_>,
    node: NodePtr,
    context: &str,
    ctx: &ParseContext,
) -> Result<(), AstError> {
    unsafe {
        match node_tag(node) {
            pg::NodeTag_T_RangeVar => table_emit(builder, cast::<pg::RangeVar>(node), ctx),
            pg::NodeTag_T_RangeSubselect => {
                subquery_emit(builder, cast::<pg::RangeSubselect>(node), ctx)
            }
            pg::NodeTag_T_JoinExpr => join_emit(builder, cast::<pg::JoinExpr>(node), ctx),
            other => Err(AstError::UnsupportedSelectFeature {
                feature: format!("{} in {context}", node_tag_name(other)),
            }),
        }
    }
}

/// A table alias: its name and its column list, or none.
struct Alias {
    name: EcoString,
    columns: Vec<EcoString>,
}

unsafe fn alias_extract(alias: *const pg::Alias) -> Option<Alias> {
    unsafe {
        (!alias.is_null()).then(|| Alias {
            name: EcoString::from(cstr((*alias).aliasname)),
            columns: names_extract((*alias).colnames),
        })
    }
}

fn alias_names_emit(builder: &mut TreeBuilder<'_>, alias: Option<&Alias>) {
    for column in alias.map(|alias| alias.columns.as_slice()).unwrap_or(&[]) {
        builder.leaf(NodeKind::Name(column.clone()));
    }
}

/// A base table, or a reference to a CTE in scope whose body is re-emitted
/// in the context it was defined in.
unsafe fn table_emit(
    builder: &mut TreeBuilder<'_>,
    range_var: *const pg::RangeVar,
    ctx: &ParseContext,
) -> Result<(), AstError> {
    unsafe {
        let schema = EcoString::from(cstr((*range_var).schemaname));
        let name = EcoString::from(cstr((*range_var).relname));
        let alias = alias_extract((*range_var).alias);
        let alias_column_count = count(alias.as_ref().map_or(0, |alias| alias.columns.len()));

        if schema.is_empty()
            && let Some((cte, cte_ctx)) = ctx.cte_find(&name)
        {
            let open = builder.open(NodeKind::CteRef {
                cte_name: name,
                column_alias_count: count(cte.column_aliases.len()),
                materialization: cte.materialization,
                alias: alias.as_ref().map(|alias| alias.name.clone()),
                alias_column_count,
            });
            for column in &cte.column_aliases {
                builder.leaf(NodeKind::Name(column.clone()));
            }
            alias_names_emit(builder, alias.as_ref());
            cte_query_emit(builder, cte, &cte_ctx)?;
            builder.close(open);
            return Ok(());
        }

        let open = builder.open(NodeKind::Table {
            schema,
            name,
            alias: alias
                .as_ref()
                .map_or_else(EcoString::new, |alias| alias.name.clone()),
            alias_column_count,
        });
        alias_names_emit(builder, alias.as_ref());
        builder.close(open);
        Ok(())
    }
}

unsafe fn subquery_emit(
    builder: &mut TreeBuilder<'_>,
    range_subselect: *const pg::RangeSubselect,
    ctx: &ParseContext,
) -> Result<(), AstError> {
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
        let alias = alias_extract((*range_subselect).alias);
        let open = builder.open(NodeKind::Subquery {
            lateral: (*range_subselect).lateral,
            alias: alias.as_ref().map(|alias| alias.name.clone()),
            alias_column_count: count(alias.as_ref().map_or(0, |alias| alias.columns.len())),
            kind: SubqueryKind::Inclusion,
        });
        alias_names_emit(builder, alias.as_ref());
        query_emit(builder, cast::<pg::SelectStmt>(subquery), ctx)?;
        builder.close(open);
        Ok(())
    }
}

unsafe fn join_emit(
    builder: &mut TreeBuilder<'_>,
    join_expr: *const pg::JoinExpr,
    ctx: &ParseContext,
) -> Result<(), AstError> {
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
        let qual = join_qual_extract(join_expr);
        let open = builder.open(NodeKind::Join {
            join_type: join_type_map((*join_expr).jointype)?,
            qual: qual.kind(),
        });
        source_emit(builder, larg, "join left argument", ctx)?;
        source_emit(builder, rarg, "join right argument", ctx)?;
        match qual {
            JoinQual::On(quals) => where_emit(builder, quals)?,
            JoinQual::Using(columns) => {
                for column in columns {
                    builder.leaf(NodeKind::Name(column));
                }
            }
            JoinQual::Natural | JoinQual::Cross => {}
        }
        builder.close(open);
        Ok(())
    }
}

/// A join's qualification with what it emits: the ON condition, or the
/// USING column names.
enum JoinQual {
    On(NodePtr),
    Using(Vec<EcoString>),
    Natural,
    Cross,
}

impl JoinQual {
    fn kind(&self) -> JoinQualKind {
        match self {
            JoinQual::On(_) => JoinQualKind::On,
            JoinQual::Using(columns) => JoinQualKind::Using(count(columns.len())),
            JoinQual::Natural => JoinQualKind::Natural,
            JoinQual::Cross => JoinQualKind::Cross,
        }
    }
}

unsafe fn join_qual_extract(join_expr: *const pg::JoinExpr) -> JoinQual {
    unsafe {
        let quals = (*join_expr).quals as NodePtr;
        if !quals.is_null() {
            JoinQual::On(quals)
        } else if !list_is_empty((*join_expr).usingClause) {
            JoinQual::Using(names_extract((*join_expr).usingClause))
        } else if (*join_expr).isNatural {
            JoinQual::Natural
        } else {
            JoinQual::Cross
        }
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
