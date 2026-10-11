use crate::query::resolved::{ResolvedSelectColumns, ResolvedSelectNode};

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
