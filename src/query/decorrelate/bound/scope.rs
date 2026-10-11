//! What a subquery can see: the slots its own FROM entries were given, so
//! a column bound elsewhere refers out of it; the correlation predicates of
//! its WHERE; and how a piece of it is moved into the scope it referred to.

use std::collections::HashSet;

use super::Decorrelate;
use crate::query::ast::{
    AstNode, BinaryExpr, BinaryOp, ColumnNode, CteRefNode, JoinNode, JoinQual, JoinType, NodeId,
    QueryExpr, ScalarExpr, SelectNode, SubLinkType, TableNode, TableSource, TableSubqueryNode,
    WhereExpr,
};
use crate::query::resolve::{ColumnBinding, ColumnTarget, SlotId};

/// The slots of every FROM entry under a node, nested bodies included; a
/// column bound to any other slot refers out of the node.
pub(super) struct Scope {
    inside: HashSet<SlotId>,
}

impl Scope {
    pub(super) fn of_query(state: &Decorrelate<'_>, query: &QueryExpr) -> Self {
        let mut scope = Self {
            inside: HashSet::new(),
        };
        scope.sources_add(state, query);
        scope
    }

    /// The scope of a SELECT's own FROM, which a subquery of it refers out to.
    pub(super) fn of_select(state: &Decorrelate<'_>, select: &SelectNode) -> Self {
        Self::of_select_sources(state, &select.from)
    }

    pub(super) fn of_select_sources(state: &Decorrelate<'_>, sources: &[TableSource]) -> Self {
        let mut scope = Self {
            inside: HashSet::new(),
        };
        for source in sources {
            scope.sources_add(state, source);
        }
        scope
    }

    fn sources_add<N: AstNode>(&mut self, state: &Decorrelate<'_>, node: &N) {
        let bindings = state.bound.bindings();
        let mut add = |id: NodeId| {
            if let Some(slot) = bindings.source(id) {
                self.inside.insert(slot);
            }
        };
        node.nodes::<TableNode>().for_each(|table| add(table.id));
        node.nodes::<TableSubqueryNode>()
            .for_each(|subquery| add(subquery.id));
        node.nodes::<CteRefNode>()
            .for_each(|cte_ref| add(cte_ref.id));
    }

    pub(super) fn contains(&self, binding: &ColumnBinding) -> bool {
        self.inside.contains(&binding.slot)
    }

    /// Whether `column` is bound outside this scope.
    pub(super) fn is_outer(&self, state: &Decorrelate<'_>, column: &ColumnNode) -> bool {
        matches!(
            state.bound.column(column),
            Some(ColumnTarget::Column(binding)) if !self.contains(binding)
        )
    }
}

/// Whether `query` refers to a scope enclosing it.
pub(super) fn query_is_correlated(state: &Decorrelate<'_>, query: &QueryExpr) -> bool {
    let scope = Scope::of_query(state, query);
    query
        .nodes::<ColumnNode>()
        .any(|column| scope.is_outer(state, column))
}

/// A correlated subquery predicate, borrowed out of a WHERE expression.
pub(super) struct CorrelatedSubquery<'a> {
    pub(super) sublink_type: SubLinkType,
    pub(super) query: &'a QueryExpr,
    pub(super) test_expr: Option<&'a ScalarExpr>,
}

/// `expr` as a subquery predicate that refers to a scope enclosing it.
pub(super) fn correlated_subquery<'a>(
    state: &Decorrelate<'_>,
    expr: &'a WhereExpr,
) -> Option<CorrelatedSubquery<'a>> {
    let WhereExpr::Subquery {
        query,
        sublink_type,
        test_expr,
    } = expr
    else {
        return None;
    };
    query_is_correlated(state, query).then_some(CorrelatedSubquery {
        sublink_type: *sublink_type,
        query,
        test_expr: test_expr.as_deref(),
    })
}

/// Whether a predicate holds a correlated subquery anywhere.
pub(super) fn where_has_correlation(state: &Decorrelate<'_>, expr: &WhereExpr) -> bool {
    match expr {
        WhereExpr::Subquery { query, .. } => query_is_correlated(state, query),
        WhereExpr::Scalar(scalar) => scalar_has_correlation(state, scalar),
        WhereExpr::Unary(unary) => where_has_correlation(state, &unary.expr),
        WhereExpr::Binary(binary) => {
            where_has_correlation(state, &binary.lexpr)
                || where_has_correlation(state, &binary.rexpr)
        }
        WhereExpr::Multi(multi) => multi.exprs.iter().any(|e| where_has_correlation(state, e)),
    }
}

/// Whether a scalar expression holds a correlated subquery, looking
/// through function arguments, CASE arms, arithmetic, arrays and casts.
pub(super) fn scalar_has_correlation(state: &Decorrelate<'_>, expr: &ScalarExpr) -> bool {
    match expr {
        ScalarExpr::Subquery(query) => query_is_correlated(state, query),
        ScalarExpr::Function(func) => func
            .args
            .iter()
            .any(|arg| scalar_has_correlation(state, arg)),
        ScalarExpr::Case(case) => {
            case.arg
                .as_ref()
                .is_some_and(|arg| scalar_has_correlation(state, arg))
                || case.whens.iter().any(|when| {
                    where_has_correlation(state, &when.condition)
                        || scalar_has_correlation(state, &when.result)
                })
                || case
                    .default
                    .as_ref()
                    .is_some_and(|default| scalar_has_correlation(state, default))
        }
        ScalarExpr::Arithmetic(arithmetic) => {
            scalar_has_correlation(state, &arithmetic.left)
                || scalar_has_correlation(state, &arithmetic.right)
        }
        ScalarExpr::Array(elements) => elements
            .iter()
            .any(|element| scalar_has_correlation(state, element)),
        ScalarExpr::TypeCast { expr, .. } => scalar_has_correlation(state, expr),
        ScalarExpr::Column(_) | ScalarExpr::Literal(_) => false,
    }
}

/// The column references of a predicate at its own level: not those of a
/// subquery inside it, nor those in a function's FILTER, ORDER BY or OVER.
fn where_columns<'a>(expr: &'a WhereExpr, out: &mut Vec<&'a ColumnNode>) {
    match expr {
        WhereExpr::Scalar(scalar) => scalar_columns(scalar, out),
        WhereExpr::Unary(unary) => where_columns(&unary.expr, out),
        WhereExpr::Binary(binary) => {
            where_columns(&binary.lexpr, out);
            where_columns(&binary.rexpr, out);
        }
        WhereExpr::Multi(multi) => multi.exprs.iter().for_each(|e| where_columns(e, out)),
        WhereExpr::Subquery { .. } => {}
    }
}

fn scalar_columns<'a>(expr: &'a ScalarExpr, out: &mut Vec<&'a ColumnNode>) {
    match expr {
        ScalarExpr::Column(column) => out.push(column),
        ScalarExpr::Function(func) => func.args.iter().for_each(|arg| scalar_columns(arg, out)),
        ScalarExpr::Arithmetic(arithmetic) => {
            scalar_columns(&arithmetic.left, out);
            scalar_columns(&arithmetic.right, out);
        }
        ScalarExpr::Case(case) => {
            if let Some(arg) = &case.arg {
                scalar_columns(arg, out);
            }
            for when in &case.whens {
                where_columns(&when.condition, out);
                scalar_columns(&when.result, out);
            }
            if let Some(default) = &case.default {
                scalar_columns(default, out);
            }
        }
        ScalarExpr::Array(elements) => elements.iter().for_each(|e| scalar_columns(e, out)),
        ScalarExpr::TypeCast { expr, .. } => scalar_columns(expr, out),
        ScalarExpr::Literal(_) | ScalarExpr::Subquery(_) => {}
    }
}

fn where_refers_out(state: &Decorrelate<'_>, scope: &Scope, expr: &WhereExpr) -> bool {
    let mut columns = Vec::new();
    where_columns(expr, &mut columns);
    columns.iter().any(|column| scope.is_outer(state, column))
}

/// A correlation predicate of an inner WHERE: `inner = outer`, each the
/// node as it stands in the subquery.
pub(super) struct Correlation {
    pub(super) outer: ColumnNode,
    pub(super) inner: ColumnNode,
}

/// Non-empty: existence of one guarantees at least one predicate.
pub(super) struct Correlations {
    pub(super) first: Correlation,
    pub(super) rest: Vec<Correlation>,
}

impl Correlations {
    pub(super) fn iter(&self) -> impl Iterator<Item = &Correlation> {
        std::iter::once(&self.first).chain(&self.rest)
    }
}

/// Partition an inner WHERE into its correlation predicates (column
/// equalities with exactly one side bound outside `scope`) and the residual
/// conjuncts. `None` when there are no correlation predicates, or when the
/// subquery refers out of its scope in a form that is not one.
pub(super) fn where_correlation_partition(
    state: &Decorrelate<'_>,
    scope: &Scope,
    where_clause: &WhereExpr,
) -> Option<(Correlations, Option<WhereExpr>)> {
    let mut predicates = Vec::new();
    let mut residual = Vec::new();
    for conjunct in where_clause.clone().conjuncts() {
        if let WhereExpr::Binary(binary) = &conjunct
            && binary.op == BinaryOp::Equal
            && let (
                WhereExpr::Scalar(ScalarExpr::Column(left)),
                WhereExpr::Scalar(ScalarExpr::Column(right)),
            ) = (binary.lexpr.as_ref(), binary.rexpr.as_ref())
        {
            match (scope.is_outer(state, left), scope.is_outer(state, right)) {
                (true, false) => predicates.push(Correlation {
                    outer: left.clone(),
                    inner: right.clone(),
                }),
                (false, true) => predicates.push(Correlation {
                    outer: right.clone(),
                    inner: left.clone(),
                }),
                // Both outer or both inner.
                (true, true) | (false, false) => residual.push(conjunct),
            }
            continue;
        }
        if where_refers_out(state, scope, &conjunct) {
            return None;
        }
        residual.push(conjunct);
    }
    let mut predicates = predicates.into_iter();
    let first = predicates.next()?;
    Some((
        Correlations {
            first,
            rest: predicates.collect(),
        },
        WhereExpr::conjuncts_join(residual),
    ))
}

/// `inner = outer` for every correlation predicate, AND-folded.
pub(super) fn correlations_condition(
    state: &mut Decorrelate<'_>,
    predicates: &Correlations,
    destination: &Scope,
) -> WhereExpr {
    let mut condition = None;
    for predicate in predicates.iter() {
        let equality = where_equal(
            column_moved(state, &predicate.inner, destination),
            column_moved(state, &predicate.outer, destination),
        );
        condition = Some(match condition {
            Some(acc) => where_and(acc, equality),
            None => equality,
        });
    }
    condition.unwrap_or_else(|| {
        where_equal(
            column_moved(state, &predicates.first.inner, destination),
            column_moved(state, &predicates.first.outer, destination),
        )
    })
}

pub(super) fn where_equal(left: ScalarExpr, right: ScalarExpr) -> WhereExpr {
    WhereExpr::Binary(BinaryExpr {
        id: NodeId::PENDING,
        op: BinaryOp::Equal,
        lexpr: Box::new(WhereExpr::Scalar(left)),
        rexpr: Box::new(WhereExpr::Scalar(right)),
    })
}

pub(super) fn where_and(left: WhereExpr, right: WhereExpr) -> WhereExpr {
    WhereExpr::Binary(BinaryExpr {
        id: NodeId::PENDING,
        op: BinaryOp::And,
        lexpr: Box::new(left),
        rexpr: Box::new(right),
    })
}

/// `outer AND inner`, either side optional.
pub(super) fn where_merge(outer: Option<WhereExpr>, inner: Option<WhereExpr>) -> Option<WhereExpr> {
    match (outer, inner) {
        (Some(outer), Some(inner)) => Some(where_and(outer, inner)),
        (outer, None) => outer,
        (None, inner) => inner,
    }
}

/// A column reference carried into `destination`: it keeps its node so its
/// binding follows, is qualified by its slot so the tree still says what it
/// means, and stops being correlated if it now sits beside its table. A
/// merged column is written out as the columns it stands for.
pub(super) fn column_moved(
    state: &mut Decorrelate<'_>,
    column: &ColumnNode,
    destination: &Scope,
) -> ScalarExpr {
    match state.bound.column(column) {
        Some(ColumnTarget::Column(binding)) => {
            let binding = *binding;
            if destination.contains(&binding) {
                state.uncorrelated_mark(column);
            }
            let table = state
                .bound
                .bindings()
                .slot(binding.slot)
                .map(|slot| slot.qualifier().into());
            ScalarExpr::Column(ColumnNode {
                table: table.or_else(|| column.table.clone()),
                ..column.clone()
            })
        }
        Some(ColumnTarget::Merged(id)) => state.merged_expr(*id).0,
        Some(ColumnTarget::Output) | None => ScalarExpr::Column(column.clone()),
    }
}

/// A predicate carried into `destination`, its references moved.
pub(super) fn where_moved(
    state: &mut Decorrelate<'_>,
    expr: &WhereExpr,
    destination: &Scope,
) -> WhereExpr {
    match expr {
        WhereExpr::Scalar(scalar) => WhereExpr::Scalar(scalar_moved(state, scalar, destination)),
        WhereExpr::Unary(unary) => WhereExpr::Unary(crate::query::ast::UnaryExpr {
            expr: Box::new(where_moved(state, &unary.expr, destination)),
            ..unary.clone()
        }),
        WhereExpr::Binary(binary) => WhereExpr::Binary(BinaryExpr {
            lexpr: Box::new(where_moved(state, &binary.lexpr, destination)),
            rexpr: Box::new(where_moved(state, &binary.rexpr, destination)),
            ..binary.clone()
        }),
        WhereExpr::Multi(multi) => WhereExpr::Multi(crate::query::ast::MultiExpr {
            exprs: multi
                .exprs
                .iter()
                .map(|e| where_moved(state, e, destination))
                .collect(),
            ..multi.clone()
        }),
        // A subquery is its own scope; its references resolve as before.
        WhereExpr::Subquery { .. } => expr.clone(),
    }
}

pub(super) fn scalar_moved(
    state: &mut Decorrelate<'_>,
    expr: &ScalarExpr,
    destination: &Scope,
) -> ScalarExpr {
    match expr {
        ScalarExpr::Column(column) => column_moved(state, column, destination),
        ScalarExpr::Function(func) => ScalarExpr::Function(crate::query::ast::FunctionCall {
            args: func
                .args
                .iter()
                .map(|arg| scalar_moved(state, arg, destination))
                .collect(),
            agg_order: func
                .agg_order
                .iter()
                .map(|clause| crate::query::ast::OrderByClause {
                    expr: scalar_moved(state, &clause.expr, destination),
                    ..clause.clone()
                })
                .collect(),
            agg_filter: func
                .agg_filter
                .as_ref()
                .map(|filter| Box::new(where_moved(state, filter, destination))),
            ..func.clone()
        }),
        ScalarExpr::Case(case) => ScalarExpr::Case(crate::query::ast::CaseExpr {
            arg: case
                .arg
                .as_ref()
                .map(|arg| Box::new(scalar_moved(state, arg, destination))),
            whens: case
                .whens
                .iter()
                .map(|when| crate::query::ast::CaseWhen {
                    condition: where_moved(state, &when.condition, destination),
                    result: scalar_moved(state, &when.result, destination),
                    ..when.clone()
                })
                .collect(),
            default: case
                .default
                .as_ref()
                .map(|default| Box::new(scalar_moved(state, default, destination))),
            ..case.clone()
        }),
        ScalarExpr::Arithmetic(arithmetic) => {
            ScalarExpr::Arithmetic(crate::query::ast::ArithmeticExpr {
                left: Box::new(scalar_moved(state, &arithmetic.left, destination)),
                right: Box::new(scalar_moved(state, &arithmetic.right, destination)),
                ..arithmetic.clone()
            })
        }
        ScalarExpr::Array(elements) => ScalarExpr::Array(
            elements
                .iter()
                .map(|e| scalar_moved(state, e, destination))
                .collect(),
        ),
        ScalarExpr::TypeCast { expr, target } => ScalarExpr::TypeCast {
            expr: Box::new(scalar_moved(state, expr, destination)),
            target: target.clone(),
        },
        ScalarExpr::Literal(_) | ScalarExpr::Subquery(_) => expr.clone(),
    }
}

/// A FROM entry carried into `destination`: its join conditions moved, its
/// tables and derived tables as they are.
pub(super) fn source_moved(
    state: &mut Decorrelate<'_>,
    source: &TableSource,
    destination: &Scope,
) -> TableSource {
    match source {
        TableSource::Join(join) => TableSource::Join(JoinNode {
            left: Box::new(source_moved(state, &join.left, destination)),
            right: Box::new(source_moved(state, &join.right, destination)),
            qual: match &join.qual {
                JoinQual::On(condition) => JoinQual::On(where_moved(state, condition, destination)),
                JoinQual::Using(_) | JoinQual::Natural | JoinQual::Cross => join.qual.clone(),
            },
            ..join.clone()
        }),
        TableSource::Table(_) | TableSource::Subquery(_) | TableSource::CteRef(_) => source.clone(),
    }
}

/// FROM entries as one source: a chain of cross joins, or the one entry.
/// `None` for no entries.
pub(super) fn sources_joined(sources: Vec<TableSource>) -> Option<TableSource> {
    sources.into_iter().reduce(|left, right| {
        TableSource::Join(JoinNode {
            id: NodeId::PENDING,
            join_type: JoinType::Inner,
            left: Box::new(left),
            right: Box::new(right),
            qual: JoinQual::Cross,
        })
    })
}
