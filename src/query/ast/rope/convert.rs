//! Building a rope tree from the owned tree, in the owned tree's child
//! order.

use ecow::EcoString;

use super::build::{Remap, TreeBuilder};
use super::node::{FrameBoundKind, JoinQualKind, NodeKind};
use super::tree::QueryTree;
use crate::query::ast::{
    CaseExpr, CteDefinition, CteRefNode, FrameBound, FunctionCall, JoinNode, JoinQual,
    OrderByClause, QueryBody, QueryExpr, ScalarExpr, SelectColumn, SelectColumns, SelectNode,
    TableAlias, TableNode, TableSource, TableSubqueryNode, ValuesClause, WhereExpr, WindowSpec,
};

fn count(items: usize) -> u32 {
    u32::try_from(items).unwrap_or(u32::MAX)
}

fn alias_columns(alias: Option<&TableAlias>) -> u32 {
    alias.map_or(0, |alias| count(alias.columns.len()))
}

impl QueryTree {
    pub fn from_query_expr(query: &QueryExpr) -> QueryTree {
        let mut builder = TreeBuilder::new();
        query_build(&mut builder, query);
        builder.finish().0
    }

    /// The tree with where each id-bearing owned node landed, for oracles
    /// that compare side tables across the two id spaces.
    pub fn from_query_expr_with_origins(query: &QueryExpr) -> (QueryTree, Remap) {
        let mut builder = TreeBuilder::new();
        query_build(&mut builder, query);
        let (tree, origins) = builder.finish();
        (tree, origins)
    }
}

fn names_build(builder: &mut TreeBuilder<'_>, names: &[EcoString]) {
    for name in names {
        builder.leaf(NodeKind::Name(name.clone()));
    }
}

pub(super) fn query_build(builder: &mut TreeBuilder<'_>, query: &QueryExpr) {
    let limit = query.limit.as_ref();
    let open = builder.open_from(
        NodeKind::Query {
            cte_count: count(query.ctes.len()),
            order_by_count: count(query.order_by.len()),
            has_limit_count: limit.is_some_and(|limit| limit.count.is_some()),
            has_limit_offset: limit.is_some_and(|limit| limit.offset.is_some()),
        },
        query.id,
    );
    for cte in &query.ctes {
        cte_build(builder, cte);
    }
    match &query.body {
        QueryBody::Select(select) => select_build(builder, select),
        QueryBody::Values(values) => values_build(builder, values),
        QueryBody::SetOp(set_op) => {
            let open = builder.open_from(
                NodeKind::SetOp {
                    op: set_op.op,
                    all: set_op.all,
                },
                set_op.id,
            );
            query_build(builder, &set_op.left);
            query_build(builder, &set_op.right);
            builder.close(open);
        }
    }
    for order_by in &query.order_by {
        order_by_build(builder, order_by);
    }
    if let Some(limit) = limit {
        if let Some(count) = &limit.count {
            builder.leaf(NodeKind::Literal(count.clone()));
        }
        if let Some(offset) = &limit.offset {
            builder.leaf(NodeKind::Literal(offset.clone()));
        }
    }
    builder.close(open);
}

fn cte_build(builder: &mut TreeBuilder<'_>, cte: &CteDefinition) {
    let open = builder.open(NodeKind::Cte {
        name: cte.name.clone(),
        alias_count: count(cte.column_aliases.len()),
        materialization: cte.materialization,
    });
    names_build(builder, &cte.column_aliases);
    query_build(builder, &cte.query);
    builder.close(open);
}

fn select_build(builder: &mut TreeBuilder<'_>, select: &SelectNode) {
    let item_count = match &select.columns {
        SelectColumns::None => None,
        SelectColumns::Columns(columns) => Some(count(columns.len())),
    };
    let open = builder.open_from(
        NodeKind::Select {
            distinct: select.distinct,
            item_count,
            from_count: count(select.from.len()),
            has_where: select.where_clause.is_some(),
            group_by_count: count(select.group_by.len()),
            has_having: select.having.is_some(),
        },
        select.id,
    );
    select_items_build(builder, &select.columns);
    for source in &select.from {
        source_build(builder, source);
    }
    if let Some(where_clause) = &select.where_clause {
        where_build(builder, where_clause);
    }
    for column in &select.group_by {
        builder.leaf_from(
            NodeKind::Column {
                table: column.table.clone(),
                column: column.column.clone(),
            },
            column.id,
        );
    }
    if let Some(having) = &select.having {
        where_build(builder, having);
    }
    builder.close(open);
}

fn select_items_build(builder: &mut TreeBuilder<'_>, columns: &SelectColumns) {
    let SelectColumns::Columns(columns) = columns else {
        return;
    };
    for column in columns {
        match column {
            SelectColumn::Expr { expr, alias } => {
                let open = builder.open(NodeKind::SelectExpr {
                    alias: alias.clone(),
                });
                scalar_build(builder, expr);
                builder.close(open);
            }
            SelectColumn::Star(star) => {
                builder.leaf_from(
                    NodeKind::Star {
                        qualifier: star.qualifier.clone(),
                    },
                    star.id,
                );
            }
        }
    }
}

fn values_build(builder: &mut TreeBuilder<'_>, values: &ValuesClause) {
    let open = builder.open_from(
        NodeKind::Values {
            row_count: count(values.rows.len()),
        },
        values.id,
    );
    for row in &values.rows {
        let open = builder.open(NodeKind::ValuesRow {
            width: count(row.len()),
        });
        for literal in row {
            builder.leaf(NodeKind::Literal(literal.clone()));
        }
        builder.close(open);
    }
    builder.close(open);
}

fn order_by_build(builder: &mut TreeBuilder<'_>, order_by: &OrderByClause) {
    let open = builder.open_from(
        NodeKind::OrderBy {
            direction: order_by.direction.clone(),
            null_order: order_by.null_order,
        },
        order_by.id,
    );
    scalar_build(builder, &order_by.expr);
    builder.close(open);
}

fn source_build(builder: &mut TreeBuilder<'_>, source: &TableSource) {
    match source {
        TableSource::Table(table) => table_build(builder, table),
        TableSource::Subquery(subquery) => subquery_build(builder, subquery),
        TableSource::Join(join) => join_build(builder, join),
        TableSource::CteRef(cte_ref) => cte_ref_build(builder, cte_ref),
    }
}

fn table_build(builder: &mut TreeBuilder<'_>, table: &TableNode) {
    let open = builder.open_from(
        NodeKind::Table {
            schema: table.schema.clone().unwrap_or_default(),
            name: table.name.clone(),
            alias: table
                .alias
                .as_ref()
                .map(|alias| alias.name.clone())
                .unwrap_or_default(),
            alias_column_count: alias_columns(table.alias.as_ref()),
        },
        table.id,
    );
    if let Some(alias) = &table.alias {
        names_build(builder, &alias.columns);
    }
    builder.close(open);
}

fn subquery_build(builder: &mut TreeBuilder<'_>, subquery: &TableSubqueryNode) {
    let open = builder.open_from(
        NodeKind::Subquery {
            lateral: subquery.lateral,
            alias: subquery.alias.as_ref().map(|alias| alias.name.clone()),
            alias_column_count: alias_columns(subquery.alias.as_ref()),
            kind: subquery.kind,
        },
        subquery.id,
    );
    if let Some(alias) = &subquery.alias {
        names_build(builder, &alias.columns);
    }
    query_build(builder, &subquery.query);
    builder.close(open);
}

fn join_build(builder: &mut TreeBuilder<'_>, join: &JoinNode) {
    let qual = match &join.qual {
        JoinQual::On(_) => JoinQualKind::On,
        JoinQual::Using(columns) => JoinQualKind::Using(count(columns.len())),
        JoinQual::Natural => JoinQualKind::Natural,
        JoinQual::Cross => JoinQualKind::Cross,
    };
    let open = builder.open_from(
        NodeKind::Join {
            join_type: join.join_type,
            qual,
        },
        join.id,
    );
    source_build(builder, &join.left);
    source_build(builder, &join.right);
    match &join.qual {
        JoinQual::On(condition) => where_build(builder, condition),
        JoinQual::Using(columns) => names_build(builder, columns),
        JoinQual::Natural | JoinQual::Cross => {}
    }
    builder.close(open);
}

fn cte_ref_build(builder: &mut TreeBuilder<'_>, cte_ref: &CteRefNode) {
    let open = builder.open_from(
        NodeKind::CteRef {
            cte_name: cte_ref.cte_name.clone(),
            column_alias_count: count(cte_ref.column_aliases.len()),
            materialization: cte_ref.materialization,
            alias: cte_ref.alias.as_ref().map(|alias| alias.name.clone()),
            alias_column_count: alias_columns(cte_ref.alias.as_ref()),
        },
        cte_ref.id,
    );
    names_build(builder, &cte_ref.column_aliases);
    if let Some(alias) = &cte_ref.alias {
        names_build(builder, &alias.columns);
    }
    query_build(builder, &cte_ref.query);
    builder.close(open);
}

fn where_build(builder: &mut TreeBuilder<'_>, expr: &WhereExpr) {
    match expr {
        WhereExpr::Scalar(scalar) => scalar_build(builder, scalar),
        WhereExpr::Unary(unary) => {
            let open = builder.open_from(NodeKind::Unary { op: unary.op }, unary.id);
            where_build(builder, &unary.expr);
            builder.close(open);
        }
        WhereExpr::Binary(binary) => {
            let open = builder.open_from(NodeKind::Binary { op: binary.op }, binary.id);
            where_build(builder, &binary.lexpr);
            where_build(builder, &binary.rexpr);
            builder.close(open);
        }
        WhereExpr::Multi(multi) => {
            let open = builder.open_from(
                NodeKind::Multi {
                    op: multi.op,
                    count: count(multi.exprs.len()),
                },
                multi.id,
            );
            for expr in &multi.exprs {
                where_build(builder, expr);
            }
            builder.close(open);
        }
        WhereExpr::Subquery {
            query,
            sublink_type,
            test_expr,
        } => {
            let open = builder.open(NodeKind::WhereSubquery {
                sublink_type: *sublink_type,
                has_test_expr: test_expr.is_some(),
            });
            query_build(builder, query);
            if let Some(test_expr) = test_expr {
                scalar_build(builder, test_expr);
            }
            builder.close(open);
        }
    }
}

fn scalar_build(builder: &mut TreeBuilder<'_>, expr: &ScalarExpr) {
    match expr {
        ScalarExpr::Column(column) => {
            builder.leaf_from(
                NodeKind::Column {
                    table: column.table.clone(),
                    column: column.column.clone(),
                },
                column.id,
            );
        }
        ScalarExpr::Function(function) => function_build(builder, function),
        ScalarExpr::Literal(literal) => {
            builder.leaf(NodeKind::Literal(literal.clone()));
        }
        ScalarExpr::Case(case) => case_build(builder, case),
        ScalarExpr::Arithmetic(arithmetic) => {
            let open = builder.open_from(NodeKind::Arithmetic { op: arithmetic.op }, arithmetic.id);
            scalar_build(builder, &arithmetic.left);
            scalar_build(builder, &arithmetic.right);
            builder.close(open);
        }
        ScalarExpr::Subquery(query) => {
            let open = builder.open(NodeKind::ScalarSubquery);
            query_build(builder, query);
            builder.close(open);
        }
        ScalarExpr::Array(elements) => {
            let open = builder.open(NodeKind::Array {
                count: count(elements.len()),
            });
            for element in elements {
                scalar_build(builder, element);
            }
            builder.close(open);
        }
        ScalarExpr::TypeCast { expr, target } => {
            let open = builder.open(NodeKind::TypeCast {
                target: target.clone(),
            });
            scalar_build(builder, expr);
            builder.close(open);
        }
    }
}

fn case_build(builder: &mut TreeBuilder<'_>, case: &CaseExpr) {
    let open = builder.open_from(
        NodeKind::Case {
            has_arg: case.arg.is_some(),
            when_count: count(case.whens.len()),
            has_default: case.default.is_some(),
        },
        case.id,
    );
    if let Some(arg) = &case.arg {
        scalar_build(builder, arg);
    }
    for when in &case.whens {
        let open = builder.open_from(NodeKind::When, when.id);
        where_build(builder, &when.condition);
        scalar_build(builder, &when.result);
        builder.close(open);
    }
    if let Some(default) = &case.default {
        scalar_build(builder, default);
    }
    builder.close(open);
}

fn function_build(builder: &mut TreeBuilder<'_>, function: &FunctionCall) {
    let open = builder.open_from(
        NodeKind::Function {
            schema: function.schema.clone(),
            name: function.name.clone(),
            arg_count: count(function.args.len()),
            agg_star: function.agg_star,
            agg_distinct: function.agg_distinct,
            order_count: count(function.agg_order.len()),
            has_filter: function.agg_filter.is_some(),
            has_over: function.over.is_some(),
        },
        function.id,
    );
    for arg in &function.args {
        scalar_build(builder, arg);
    }
    for order_by in &function.agg_order {
        order_by_build(builder, order_by);
    }
    if let Some(filter) = &function.agg_filter {
        where_build(builder, filter);
    }
    if let Some(over) = &function.over {
        window_build(builder, over);
    }
    builder.close(open);
}

fn frame_bound_kind(bound: &FrameBound) -> (FrameBoundKind, Option<&ScalarExpr>) {
    match bound {
        FrameBound::UnboundedPreceding => (FrameBoundKind::UnboundedPreceding, None),
        FrameBound::OffsetPreceding(offset) => (FrameBoundKind::OffsetPreceding, Some(offset)),
        FrameBound::CurrentRow => (FrameBoundKind::CurrentRow, None),
        FrameBound::OffsetFollowing(offset) => (FrameBoundKind::OffsetFollowing, Some(offset)),
        FrameBound::UnboundedFollowing => (FrameBoundKind::UnboundedFollowing, None),
    }
}

fn window_build(builder: &mut TreeBuilder<'_>, window: &WindowSpec) {
    let open = builder.open_from(
        NodeKind::Window {
            partition_count: count(window.partition_by.len()),
            order_count: count(window.order_by.len()),
            has_frame: window.frame.is_some(),
            ref_name: window.ref_name.clone(),
        },
        window.id,
    );
    for partition in &window.partition_by {
        scalar_build(builder, partition);
    }
    for order_by in &window.order_by {
        order_by_build(builder, order_by);
    }
    if let Some(frame) = &window.frame {
        let (start, start_offset) = frame_bound_kind(&frame.start);
        let (end, end_offset) = frame_bound_kind(&frame.end);
        let open = builder.open_from(
            NodeKind::Frame {
                mode: frame.mode,
                start,
                end,
                exclusion: frame.exclusion,
            },
            frame.id,
        );
        if let Some(offset) = start_offset {
            scalar_build(builder, offset);
        }
        if let Some(offset) = end_offset {
            scalar_build(builder, offset);
        }
        builder.close(open);
    }
    builder.close(open);
}
