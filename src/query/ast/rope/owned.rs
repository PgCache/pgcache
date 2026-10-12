//! The owned tree a rope tree stands for: the test oracle for the rope,
//! read back through the views so a round trip proves both directions.

use ecow::EcoString;
use smallvec::SmallVec;

use super::tree::QueryTree;
use super::view::{
    Body, Case, Column, FrameBound as RopeFrameBound, Function, JoinQual as RopeJoinQual, Name,
    OrderBy, Query, Run, Scalar, ScalarKind, Select, SelectItem, Source, Where, WhereKind, Window,
};
use crate::query::ast::{
    ArithmeticExpr, BinaryExpr, CaseExpr, CaseWhen, ColumnNode, CteDefinition, CteRefNode,
    FrameBound, FunctionCall, JoinNode, JoinQual, LimitClause, MultiExpr, NodeId, OrderByClause,
    QueryBody, QueryExpr, ScalarExpr, SelectColumn, SelectColumns, SelectNode, SetOpNode, StarNode,
    TableAlias, TableNode, TableSource, TableSubqueryNode, UnaryExpr, ValuesClause, WhereExpr,
    WindowFrame, WindowSpec,
};

pub(super) fn query_expr_from_tree(tree: &QueryTree) -> QueryExpr {
    query_owned(tree.root())
}

fn names(run: Run<'_, Name<'_>>) -> Vec<EcoString> {
    run.map(|name| name.text().clone()).collect()
}

fn alias(name: Option<&EcoString>, columns: Run<'_, Name<'_>>) -> Option<TableAlias> {
    name.map(|name| TableAlias {
        name: name.clone(),
        columns: names(columns),
    })
}

fn query_owned(query: Query<'_>) -> QueryExpr {
    let ctes = query
        .ctes()
        .map(|cte| CteDefinition {
            name: cte.name().clone(),
            query: query_owned(cte.query()),
            column_aliases: names(cte.column_aliases()),
            materialization: cte.materialization(),
        })
        .collect();
    let body = match query.body() {
        Body::Select(select) => QueryBody::Select(Box::new(select_owned(select))),
        Body::Values(values) => QueryBody::Values(ValuesClause {
            id: NodeId::PENDING,
            rows: values
                .rows()
                .map(|row| row.literals().map(|l| l.value().clone()).collect())
                .collect(),
        }),
        Body::SetOp(set_op) => QueryBody::SetOp(SetOpNode {
            id: NodeId::PENDING,
            op: set_op.op(),
            all: set_op.all(),
            left: Box::new(query_owned(set_op.left())),
            right: Box::new(query_owned(set_op.right())),
        }),
    };
    let order_by: SmallVec<[OrderByClause; 1]> = query.order_by().map(order_by_owned).collect();
    let limit = query.has_limit().then(|| LimitClause {
        count: query.limit_count().map(|l| l.value().clone()),
        offset: query.limit_offset().map(|l| l.value().clone()),
    });
    QueryExpr {
        id: NodeId::PENDING,
        ctes,
        body,
        order_by,
        limit,
    }
}

fn select_owned(select: Select<'_>) -> SelectNode {
    let columns = match select.items() {
        None => SelectColumns::None,
        Some(items) => SelectColumns::Columns(
            items
                .map(|item| match item {
                    SelectItem::Expr(expr) => SelectColumn::Expr {
                        expr: scalar_owned(expr.expr()),
                        alias: expr.alias().cloned(),
                    },
                    SelectItem::Star(star) => SelectColumn::Star(StarNode {
                        id: NodeId::PENDING,
                        qualifier: star.qualifier().cloned(),
                    }),
                })
                .collect(),
        ),
    };
    SelectNode {
        id: NodeId::PENDING,
        distinct: select.distinct(),
        columns,
        from: select.from().map(source_owned).collect(),
        where_clause: select.where_clause().map(where_owned),
        group_by: select.group_by().map(column_owned).collect(),
        having: select.having().map(where_owned),
    }
}

fn order_by_owned(order_by: OrderBy<'_>) -> OrderByClause {
    OrderByClause {
        id: NodeId::PENDING,
        expr: scalar_owned(order_by.expr()),
        direction: order_by.direction(),
        null_order: order_by.null_order(),
    }
}

fn source_owned(source: Source<'_>) -> TableSource {
    match source {
        Source::Table(table) => TableSource::Table(TableNode {
            id: NodeId::PENDING,
            schema: table.schema().cloned(),
            name: table.name().clone(),
            alias: alias(table.alias(), table.alias_columns()),
        }),
        Source::Subquery(subquery) => TableSource::Subquery(TableSubqueryNode {
            id: NodeId::PENDING,
            lateral: subquery.lateral(),
            query: Box::new(query_owned(subquery.query())),
            alias: alias(subquery.alias(), subquery.alias_columns()),
            kind: subquery.kind(),
        }),
        Source::Join(join) => TableSource::Join(JoinNode {
            id: NodeId::PENDING,
            join_type: join.join_type(),
            left: Box::new(source_owned(join.left())),
            right: Box::new(source_owned(join.right())),
            qual: match join.qual() {
                RopeJoinQual::On(condition) => JoinQual::On(where_owned(condition)),
                RopeJoinQual::Using(columns) => JoinQual::Using(names(columns)),
                RopeJoinQual::Natural => JoinQual::Natural,
                RopeJoinQual::Cross => JoinQual::Cross,
            },
        }),
        Source::CteRef(cte_ref) => TableSource::CteRef(CteRefNode {
            id: NodeId::PENDING,
            cte_name: cte_ref.cte_name().clone(),
            query: Box::new(query_owned(cte_ref.query())),
            column_aliases: names(cte_ref.column_aliases()),
            materialization: cte_ref.materialization(),
            alias: alias(cte_ref.alias(), cte_ref.alias_columns()),
        }),
    }
}

fn column_owned(column: Column<'_>) -> ColumnNode {
    ColumnNode {
        id: NodeId::PENDING,
        table: column.table().cloned(),
        column: column.column().clone(),
    }
}

fn where_owned(expr: Where<'_>) -> WhereExpr {
    match expr.kind() {
        WhereKind::Scalar(scalar) => WhereExpr::Scalar(scalar_owned(scalar)),
        WhereKind::Unary(unary) => WhereExpr::Unary(UnaryExpr {
            id: NodeId::PENDING,
            op: unary.op(),
            expr: Box::new(where_owned(unary.expr())),
        }),
        WhereKind::Binary(binary) => WhereExpr::Binary(BinaryExpr {
            id: NodeId::PENDING,
            op: binary.op(),
            lexpr: Box::new(where_owned(binary.lexpr())),
            rexpr: Box::new(where_owned(binary.rexpr())),
        }),
        WhereKind::Multi(multi) => WhereExpr::Multi(MultiExpr {
            id: NodeId::PENDING,
            op: multi.op(),
            exprs: multi.exprs().map(where_owned).collect(),
        }),
        WhereKind::Subquery(subquery) => WhereExpr::Subquery {
            query: Box::new(query_owned(subquery.query())),
            sublink_type: subquery.sublink_type(),
            test_expr: subquery.test_expr().map(|e| Box::new(scalar_owned(e))),
        },
    }
}

fn scalar_owned(expr: Scalar<'_>) -> ScalarExpr {
    match expr.kind() {
        ScalarKind::Column(column) => ScalarExpr::Column(column_owned(column)),
        ScalarKind::Function(function) => ScalarExpr::Function(function_owned(function)),
        ScalarKind::Literal(literal) => ScalarExpr::Literal(literal.value().clone()),
        ScalarKind::Case(case) => ScalarExpr::Case(case_owned(case)),
        ScalarKind::Arithmetic(arithmetic) => ScalarExpr::Arithmetic(ArithmeticExpr {
            id: NodeId::PENDING,
            left: Box::new(scalar_owned(arithmetic.left())),
            op: arithmetic.op(),
            right: Box::new(scalar_owned(arithmetic.right())),
        }),
        ScalarKind::Subquery(subquery) => {
            ScalarExpr::Subquery(Box::new(query_owned(subquery.query())))
        }
        ScalarKind::Array(array) => ScalarExpr::Array(array.elements().map(scalar_owned).collect()),
        ScalarKind::TypeCast(cast) => ScalarExpr::TypeCast {
            expr: Box::new(scalar_owned(cast.expr())),
            target: cast.target().clone(),
        },
    }
}

fn case_owned(case: Case<'_>) -> CaseExpr {
    CaseExpr {
        id: NodeId::PENDING,
        arg: case.arg().map(|arg| Box::new(scalar_owned(arg))),
        whens: case
            .whens()
            .map(|when| CaseWhen {
                id: NodeId::PENDING,
                condition: where_owned(when.condition()),
                result: scalar_owned(when.result()),
            })
            .collect(),
        default: case.default().map(|d| Box::new(scalar_owned(d))),
    }
}

fn function_owned(function: Function<'_>) -> FunctionCall {
    FunctionCall {
        id: NodeId::PENDING,
        schema: function.schema().cloned(),
        name: function.name().clone(),
        args: function.args().map(scalar_owned).collect(),
        agg_star: function.agg_star(),
        agg_distinct: function.agg_distinct(),
        agg_order: function.agg_order().map(order_by_owned).collect(),
        agg_filter: function.filter().map(|f| Box::new(where_owned(f))),
        over: function.over().map(|w| Box::new(window_owned(w))),
    }
}

fn frame_bound_owned(bound: RopeFrameBound<'_>) -> FrameBound {
    match bound {
        RopeFrameBound::UnboundedPreceding => FrameBound::UnboundedPreceding,
        RopeFrameBound::OffsetPreceding(offset) => {
            FrameBound::OffsetPreceding(Box::new(scalar_owned(offset)))
        }
        RopeFrameBound::CurrentRow => FrameBound::CurrentRow,
        RopeFrameBound::OffsetFollowing(offset) => {
            FrameBound::OffsetFollowing(Box::new(scalar_owned(offset)))
        }
        RopeFrameBound::UnboundedFollowing => FrameBound::UnboundedFollowing,
    }
}

fn window_owned(window: Window<'_>) -> WindowSpec {
    WindowSpec {
        id: NodeId::PENDING,
        partition_by: window.partition_by().map(scalar_owned).collect(),
        order_by: window.order_by().map(order_by_owned).collect(),
        frame: window.frame().map(|frame| WindowFrame {
            id: NodeId::PENDING,
            mode: frame.mode(),
            start: frame_bound_owned(frame.start()),
            end: frame_bound_owned(frame.end()),
            exclusion: frame.exclusion(),
        }),
        ref_name: window.ref_name().cloned(),
    }
}
