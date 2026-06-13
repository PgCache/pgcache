#[cfg(test)]
use crate::catalog::Oid;
use crate::query::resolved::{ResolvedSelectColumns, ResolvedSelectNode};

/// Replace SELECT columns in a ResolvedSelectNode for cache population fetches.
///
/// Strips aggregation-related clauses (GROUP BY, HAVING) because we want raw rows
/// for caching, with aggregation performed at cache retrieval time.
///
/// Enables DISTINCT to deduplicate rows that appear multiple times due to JOIN fan-out.
/// Population fetches each table independently using the full query (including JOINs),
/// so a single table row can appear once per matching row in the joined table.
pub fn resolved_select_node_replace(
    resolved: &ResolvedSelectNode,
    columns: ResolvedSelectColumns,
) -> ResolvedSelectNode {
    ResolvedSelectNode {
        distinct: true,
        columns,
        from: resolved.from.clone(),
        where_clause: resolved.where_clause.clone(),
        group_by: vec![],
        having: None,
    }
}

/// Replace SELECT columns in a ResolvedSelectNode for update queries.
///
/// Unlike `resolved_select_node_replace` (used for population), this preserves
/// GROUP BY and HAVING clauses because update queries need the full predicate
/// structure to correctly match CDC events.
pub fn resolved_select_node_update_replace(
    node: &ResolvedSelectNode,
    columns: ResolvedSelectColumns,
) -> ResolvedSelectNode {
    ResolvedSelectNode {
        distinct: node.distinct,
        columns,
        from: node.from.clone(),
        where_clause: node.where_clause.clone(),
        group_by: node.group_by.clone(),
        having: node.having.clone(),
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::ColumnMetadata;
    use crate::query::ast::{BinaryOp, LiteralValue};
    use crate::query::resolved::{
        ResolvedBinaryExpr, ResolvedColumnNode, ResolvedScalarExpr, ResolvedTableNode,
        ResolvedTableSource, ResolvedWhereExpr,
    };
    use postgres_types::Type;

    use super::*;

    #[test]
    fn test_resolved_select_node_replace_strips_group_by() {
        let col_meta = ColumnMetadata {
            name: "status".into(),
            position: 1,
            type_oid: 25,
            data_type: Type::TEXT,
            type_name: "text".into(),
            cache_type_name: "text".into(),
            is_primary_key: false,
        };

        let node = ResolvedSelectNode {
            group_by: vec![ResolvedColumnNode {
                schema: "public".into(),
                table: "orders".into(),
                table_alias: None,
                column: "status".into(),
                column_metadata: col_meta,
            }],
            ..Default::default()
        };

        let result = resolved_select_node_replace(&node, ResolvedSelectColumns::None);

        assert!(
            result.group_by.is_empty(),
            "resolved_select_node_replace should strip GROUP BY"
        );
    }

    #[test]
    fn test_resolved_select_node_replace_strips_having() {
        let node = ResolvedSelectNode {
            having: Some(ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
                op: BinaryOp::Equal,
                lexpr: Box::new(ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Literal(
                    LiteralValue::Integer(1),
                ))),
                rexpr: Box::new(ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Literal(
                    LiteralValue::Integer(1),
                ))),
            })),
            ..Default::default()
        };

        let result = resolved_select_node_replace(&node, ResolvedSelectColumns::None);

        assert!(
            result.having.is_none(),
            "resolved_select_node_replace should strip HAVING"
        );
    }

    #[test]
    fn test_resolved_select_node_replace_preserves_where() {
        let where_clause = ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
            op: BinaryOp::Equal,
            lexpr: Box::new(ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Literal(
                LiteralValue::Integer(1),
            ))),
            rexpr: Box::new(ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Literal(
                LiteralValue::Integer(1),
            ))),
        });

        let node = ResolvedSelectNode {
            where_clause: Some(where_clause),
            ..Default::default()
        };

        let result = resolved_select_node_replace(&node, ResolvedSelectColumns::None);

        assert!(
            result.where_clause.is_some(),
            "resolved_select_node_replace should preserve WHERE clause"
        );
    }

    #[test]
    fn test_resolved_select_node_replace_preserves_from() {
        let node = ResolvedSelectNode {
            from: vec![ResolvedTableSource::Table(ResolvedTableNode {
                schema: "public".into(),
                name: "orders".into(),
                alias: None,
                relation_oid: Oid::from_raw(12345),
            })],
            ..Default::default()
        };

        let result = resolved_select_node_replace(&node, ResolvedSelectColumns::None);

        assert_eq!(
            result.from.len(),
            1,
            "resolved_select_node_replace should preserve FROM clause"
        );
    }
}
