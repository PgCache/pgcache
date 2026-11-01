//! CREATE INDEX: the statement `pg_get_indexdef` writes, converted for the
//! cache-side index rewrite. Keys reuse the scalar converter and the
//! predicate the WHERE converter; what the cache cannot use is rejected.

use ecow::EcoString;
use ordered_float::NotNan;
use pg_query::pg_nodes as pg;

use super::scalar::scalar_emit;
use super::where_clause::where_emit;
use super::{ExpressionScope, ParseContext, names_extract, null_order_map, order_dir_map};
use crate::query::ast::raw::{
    NodePtr, cast, cstr, list_is_empty, list_len, list_nodes, node_tag, node_tag_name,
};
use crate::query::ast::rope::{NodeKind, TreeBuilder};
use crate::query::ast::{AstError, LiteralValue};

fn unsupported(feature: &str) -> AstError {
    AstError::UnsupportedFeature {
        feature: format!("{feature} in CREATE INDEX"),
    }
}

pub(super) unsafe fn index_emit(
    builder: &mut TreeBuilder<'_>,
    stmt: *const pg::IndexStmt,
) -> Result<(), AstError> {
    unsafe {
        let rejected = [
            ((*stmt).concurrent, "CONCURRENTLY"),
            ((*stmt).if_not_exists, "IF NOT EXISTS"),
            (
                (*stmt).primary || (*stmt).isconstraint,
                "a constraint-backed index",
            ),
            (!(*stmt).tableSpace.is_null(), "TABLESPACE"),
            (
                !list_is_empty((*stmt).excludeOpNames),
                "exclusion operators",
            ),
            (!(*stmt).idxcomment.is_null(), "a comment"),
        ];
        if let Some((_, feature)) = rejected.iter().find(|(flag, _)| *flag) {
            return Err(unsupported(feature));
        }
        let relation = (*stmt).relation;
        if relation.is_null() {
            return Err(AstError::InvalidTableRef);
        }
        let name = cstr((*stmt).idxname);
        let ctx = ParseContext::empty();
        let open = builder.open(NodeKind::CreateIndex {
            name: (!name.is_empty()).then(|| EcoString::from(name)),
            method: EcoString::from(cstr((*stmt).accessMethod)),
            unique: (*stmt).unique,
            only: !(*relation).inh,
            nulls_not_distinct: (*stmt).nulls_not_distinct,
            key_count: list_len((*stmt).indexParams),
            include_count: list_len((*stmt).indexIncludingParams),
            option_count: list_len((*stmt).options),
            has_where: !(*stmt).whereClause.is_null(),
        });
        builder.leaf(NodeKind::Table {
            schema: EcoString::from(cstr((*relation).schemaname)),
            name: EcoString::from(cstr((*relation).relname)),
            alias: EcoString::new(),
            alias_column_count: 0,
        });
        for elem in list_nodes((*stmt).indexParams) {
            index_key_emit(builder, elem, &ctx)?;
        }
        for elem in list_nodes((*stmt).indexIncludingParams) {
            include_emit(builder, elem)?;
        }
        for def in list_nodes((*stmt).options) {
            option_emit(builder, def)?;
        }
        if !(*stmt).whereClause.is_null() {
            where_emit(builder, (*stmt).whereClause as NodePtr, &ctx)?;
        }
        builder.close(open);
        Ok(())
    }
}

unsafe fn index_elem(node: NodePtr, position: &str) -> Result<*const pg::IndexElem, AstError> {
    unsafe {
        match node_tag(node) {
            pg::NodeTag_T_IndexElem => Ok(cast::<pg::IndexElem>(node)),
            other => Err(unsupported(&format!(
                "{} as {position}",
                node_tag_name(other)
            ))),
        }
    }
}

unsafe fn index_key_emit(
    builder: &mut TreeBuilder<'_>,
    node: NodePtr,
    ctx: &ParseContext<'_>,
) -> Result<(), AstError> {
    unsafe {
        let elem = index_elem(node, "an index key")?;
        if !(*elem).indexcolname.is_null() {
            return Err(unsupported("a named index column"));
        }
        let open = builder.open(NodeKind::IndexKey {
            direction: order_dir_map((*elem).ordering)?,
            null_order: null_order_map((*elem).nulls_ordering)?,
            collation_count: list_len((*elem).collation),
            opclass_count: list_len((*elem).opclass),
            opclass_option_count: list_len((*elem).opclassopts),
        });
        let column = cstr((*elem).name);
        if column.is_empty() {
            let expr = (*elem).expr as NodePtr;
            if expr.is_null() {
                return Err(unsupported("an index key without a column or expression"));
            }
            scalar_emit(builder, expr, ExpressionScope::of(ctx))?;
        } else {
            builder.leaf(NodeKind::Column {
                table: None,
                column: EcoString::from(column),
            });
        }
        for name in names_extract((*elem).collation) {
            builder.leaf(NodeKind::Name(name));
        }
        for name in names_extract((*elem).opclass) {
            builder.leaf(NodeKind::Name(name));
        }
        for def in list_nodes((*elem).opclassopts) {
            option_emit(builder, def)?;
        }
        builder.close(open);
        Ok(())
    }
}

/// An INCLUDE entry: a plain column, the only form PostgreSQL accepts.
unsafe fn include_emit(builder: &mut TreeBuilder<'_>, node: NodePtr) -> Result<(), AstError> {
    unsafe {
        let elem = index_elem(node, "an INCLUDE column")?;
        let column = cstr((*elem).name);
        if column.is_empty() {
            return Err(unsupported("an expression in INCLUDE"));
        }
        builder.leaf(NodeKind::Name(EcoString::from(column)));
        Ok(())
    }
}

/// A `DefElem` of a WITH or opclass option list.
unsafe fn option_emit(builder: &mut TreeBuilder<'_>, node: NodePtr) -> Result<(), AstError> {
    unsafe {
        if node_tag(node) != pg::NodeTag_T_DefElem {
            return Err(unsupported(&format!(
                "{} as an option",
                node_tag_name(node_tag(node))
            )));
        }
        let def = cast::<pg::DefElem>(node);
        if !(*def).defnamespace.is_null() {
            return Err(unsupported("a namespaced option"));
        }
        let arg = (*def).arg as NodePtr;
        let value = if arg.is_null() {
            None
        } else {
            Some(option_value_literal(arg)?)
        };
        let open = builder.open(NodeKind::IndexOption {
            name: EcoString::from(cstr((*def).defname)),
            has_value: value.is_some(),
        });
        if let Some(value) = value {
            builder.leaf(NodeKind::Literal(value));
        }
        builder.close(open);
        Ok(())
    }
}

/// An option's value node. A bare word (`off`) parses as a type name and
/// is kept as the string it denotes, as `defGetString` reads it.
unsafe fn option_value_literal(node: NodePtr) -> Result<LiteralValue, AstError> {
    unsafe {
        match node_tag(node) {
            pg::NodeTag_T_Integer => Ok(LiteralValue::Integer(i64::from(
                (*cast::<pg::Integer>(node)).ival,
            ))),
            pg::NodeTag_T_Float => {
                let text = cstr((*cast::<pg::Float>(node)).fval);
                text.parse::<f64>()
                    .ok()
                    .and_then(|value| NotNan::new(value).ok())
                    .map(LiteralValue::Float)
                    .ok_or_else(|| unsupported(&format!("option value {text}")))
            }
            pg::NodeTag_T_Boolean => {
                Ok(LiteralValue::Boolean((*cast::<pg::Boolean>(node)).boolval))
            }
            pg::NodeTag_T_String => Ok(LiteralValue::String(EcoString::from(cstr(
                (*cast::<pg::String>(node)).sval,
            )))),
            pg::NodeTag_T_TypeName => {
                let parts = names_extract((*cast::<pg::TypeName>(node)).names);
                Ok(LiteralValue::String(EcoString::from(
                    parts
                        .iter()
                        .map(EcoString::as_str)
                        .collect::<Vec<_>>()
                        .join("."),
                )))
            }
            other => Err(unsupported(&format!(
                "{} as an option value",
                node_tag_name(other)
            ))),
        }
    }
}
