//! What a subquery can see, read from the rope: the slots its own FROM
//! entries were given, so a column bound elsewhere refers out of it; the
//! correlation predicates of its WHERE; and how a piece of it is carried
//! into the scope it referred to.

use std::collections::HashSet;

use ecow::EcoString;

use super::Planner;
use super::plan::{Bind, Moved, Planned, PlannedReplacement};
use crate::query::ast::rope::{
    Column, Node, NodeKind, NodeRef, Query, Replacement, Scalar, ScalarKind, View, Where,
    WhereKind, WhereSubquery,
};
use crate::query::ast::{BinaryOp, NodeId, SubLinkType};
use crate::query::resolve::{ColumnBinding, ColumnTarget, MergedId, SlotId};

/// The slots of every FROM entry under a node, nested bodies included; a
/// column bound to any other slot refers out of the node.
pub(super) struct Scope {
    inside: HashSet<SlotId>,
}

impl Scope {
    pub(super) fn of_subtree(planner: &Planner<'_>, node: NodeRef<'_>) -> Self {
        let mut scope = Self {
            inside: HashSet::new(),
        };
        scope.subtree_add(planner, node);
        scope
    }

    pub(super) fn empty() -> Self {
        Self {
            inside: HashSet::new(),
        }
    }

    /// Add the slots of every FROM entry under `node`.
    pub(super) fn subtree_add(&mut self, planner: &Planner<'_>, node: NodeRef<'_>) {
        for position in node.subtree() {
            if matches!(
                position.kind(),
                NodeKind::Table { .. } | NodeKind::Subquery { .. } | NodeKind::CteRef { .. }
            ) && let Some(slot) = planner.bindings.source(position.id())
            {
                self.inside.insert(slot);
            }
        }
    }

    pub(super) fn contains(&self, binding: &ColumnBinding) -> bool {
        self.inside.contains(&binding.slot)
    }

    /// Whether the column at `id` is bound outside this scope.
    pub(super) fn is_outer(&self, planner: &Planner<'_>, id: NodeId) -> bool {
        matches!(
            planner.bindings.column_by_id(id),
            Some(ColumnTarget::Column(binding)) if !self.contains(binding)
        )
    }
}

/// Whether `query` refers to a scope enclosing it.
pub(super) fn query_is_correlated(planner: &Planner<'_>, query: Query<'_>) -> bool {
    let scope = Scope::of_subtree(planner, query.node_ref());
    query.node_ref().subtree().any(|position| {
        matches!(position.kind(), NodeKind::Column { .. }) && scope.is_outer(planner, position.id())
    })
}

/// A correlated subquery predicate.
pub(super) struct CorrelatedSubquery<'t> {
    pub(super) sublink_type: SubLinkType,
    pub(super) query: Query<'t>,
    pub(super) test_expr: Option<Scalar<'t>>,
}

/// `expr` as a subquery predicate that refers to a scope enclosing it.
pub(super) fn correlated_subquery<'t>(
    planner: &Planner<'_>,
    expr: Where<'t>,
) -> Option<CorrelatedSubquery<'t>> {
    let WhereKind::Subquery(subquery) = expr.kind() else {
        return None;
    };
    query_is_correlated(planner, subquery.query()).then(|| CorrelatedSubquery {
        sublink_type: subquery.sublink_type(),
        query: subquery.query(),
        test_expr: subquery.test_expr(),
    })
}

/// Whether a predicate holds a correlated subquery anywhere.
pub(super) fn where_has_correlation(planner: &Planner<'_>, expr: Where<'_>) -> bool {
    match expr.kind() {
        WhereKind::Subquery(subquery) => query_is_correlated(planner, subquery.query()),
        WhereKind::Scalar(scalar) => scalar_has_correlation(planner, scalar),
        WhereKind::Unary(unary) => where_has_correlation(planner, unary.expr()),
        WhereKind::Binary(binary) => {
            where_has_correlation(planner, binary.lexpr())
                || where_has_correlation(planner, binary.rexpr())
        }
        WhereKind::Multi(multi) => multi
            .exprs()
            .any(|expr| where_has_correlation(planner, expr)),
    }
}

/// Whether a scalar expression holds a correlated subquery, looking
/// through function arguments, CASE arms, arithmetic, arrays and casts.
pub(super) fn scalar_has_correlation(planner: &Planner<'_>, expr: Scalar<'_>) -> bool {
    match expr.kind() {
        ScalarKind::Subquery(subquery) => query_is_correlated(planner, subquery.query()),
        ScalarKind::Function(function) => function
            .args()
            .any(|arg| scalar_has_correlation(planner, arg)),
        ScalarKind::Case(case) => {
            case.arg()
                .is_some_and(|arg| scalar_has_correlation(planner, arg))
                || case.whens().any(|when| {
                    where_has_correlation(planner, when.condition())
                        || scalar_has_correlation(planner, when.result())
                })
                || case
                    .default()
                    .is_some_and(|default| scalar_has_correlation(planner, default))
        }
        ScalarKind::Arithmetic(arithmetic) => {
            scalar_has_correlation(planner, arithmetic.left())
                || scalar_has_correlation(planner, arithmetic.right())
        }
        ScalarKind::Array(array) => array
            .elements()
            .any(|element| scalar_has_correlation(planner, element)),
        ScalarKind::TypeCast(cast) => scalar_has_correlation(planner, cast.expr()),
        ScalarKind::Column(_) | ScalarKind::Literal(_) => false,
    }
}

/// The column references of a predicate at its own level: not those of a
/// subquery inside it, nor those in a function's FILTER, ORDER BY or OVER.
fn where_columns<'t>(expr: Where<'t>, out: &mut Vec<Column<'t>>) {
    match expr.kind() {
        WhereKind::Scalar(scalar) => scalar_columns(scalar, out),
        WhereKind::Unary(unary) => where_columns(unary.expr(), out),
        WhereKind::Binary(binary) => {
            where_columns(binary.lexpr(), out);
            where_columns(binary.rexpr(), out);
        }
        WhereKind::Multi(multi) => multi.exprs().for_each(|expr| where_columns(expr, out)),
        WhereKind::Subquery(_) => {}
    }
}

fn scalar_columns<'t>(expr: Scalar<'t>, out: &mut Vec<Column<'t>>) {
    match expr.kind() {
        ScalarKind::Column(column) => out.push(column),
        ScalarKind::Function(function) => {
            function.args().for_each(|arg| scalar_columns(arg, out));
        }
        ScalarKind::Arithmetic(arithmetic) => {
            scalar_columns(arithmetic.left(), out);
            scalar_columns(arithmetic.right(), out);
        }
        ScalarKind::Case(case) => {
            if let Some(arg) = case.arg() {
                scalar_columns(arg, out);
            }
            for when in case.whens() {
                where_columns(when.condition(), out);
                scalar_columns(when.result(), out);
            }
            if let Some(default) = case.default() {
                scalar_columns(default, out);
            }
        }
        ScalarKind::Array(array) => array.elements().for_each(|e| scalar_columns(e, out)),
        ScalarKind::TypeCast(cast) => scalar_columns(cast.expr(), out),
        ScalarKind::Literal(_) | ScalarKind::Subquery(_) => {}
    }
}

fn where_refers_out(planner: &Planner<'_>, scope: &Scope, expr: Where<'_>) -> bool {
    let mut columns = Vec::new();
    where_columns(expr, &mut columns);
    columns
        .iter()
        .any(|column| scope.is_outer(planner, column.id()))
}

/// A correlation predicate of an inner WHERE: `inner = outer`, each the
/// column as it stands in the subquery.
pub(super) struct Correlation<'t> {
    pub(super) outer: Column<'t>,
    pub(super) inner: Column<'t>,
}

/// Non-empty: existence of one guarantees at least one predicate.
pub(super) struct Correlations<'t> {
    pub(super) first: Correlation<'t>,
    pub(super) rest: Planned<Correlation<'t>>,
}

impl<'t> Correlations<'t> {
    pub(super) fn iter(&self) -> impl Iterator<Item = &Correlation<'t>> {
        std::iter::once(&self.first).chain(&self.rest)
    }

    pub(super) fn len(&self) -> usize {
        1 + self.rest.len()
    }
}

/// The two columns of a `column = column` equality.
fn column_equality<'t>(conjunct: Where<'t>) -> Option<(Column<'t>, Column<'t>)> {
    let WhereKind::Binary(binary) = conjunct.kind() else {
        return None;
    };
    if binary.op() != BinaryOp::Equal {
        return None;
    }
    let (WhereKind::Scalar(left), WhereKind::Scalar(right)) =
        (binary.lexpr().kind(), binary.rexpr().kind())
    else {
        return None;
    };
    let (ScalarKind::Column(left), ScalarKind::Column(right)) = (left.kind(), right.kind()) else {
        return None;
    };
    Some((left, right))
}

/// Partition an inner WHERE into its correlation predicates (column
/// equalities with exactly one side bound outside `scope`) and the residual
/// conjuncts. `None` when there are no correlation predicates, or when the
/// subquery refers out of its scope in a form that is not one.
pub(super) fn where_correlation_partition<'t>(
    planner: &Planner<'_>,
    scope: &Scope,
    where_clause: Where<'t>,
) -> Option<(Correlations<'t>, Planned<Where<'t>>)> {
    let mut predicates = Planned::new();
    let mut residual = Planned::new();
    for conjunct in where_clause.conjuncts() {
        if let Some((left, right)) = column_equality(conjunct) {
            match (
                scope.is_outer(planner, left.id()),
                scope.is_outer(planner, right.id()),
            ) {
                (true, false) => predicates.push(Correlation {
                    outer: left,
                    inner: right,
                }),
                (false, true) => predicates.push(Correlation {
                    outer: right,
                    inner: left,
                }),
                // Both outer or both inner.
                (true, true) | (false, false) => residual.push(conjunct),
            }
            continue;
        }
        if where_refers_out(planner, scope, conjunct) {
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
        residual,
    ))
}

/// The scalar subquery of a WHERE position, when it is one.
pub(super) fn where_scalar_subquery(expr: Where<'_>) -> Option<WhereSubquery<'_>> {
    match expr.kind() {
        WhereKind::Subquery(subquery) if subquery.sublink_type() == SubLinkType::Expr => {
            Some(subquery)
        }
        WhereKind::Subquery(_)
        | WhereKind::Scalar(_)
        | WhereKind::Unary(_)
        | WhereKind::Binary(_)
        | WhereKind::Multi(_) => None,
    }
}

/// The node kinds a carried piece does not enter: another query level, a
/// window, or a FROM entry's own body.
fn subtree_is_opaque(kind: &NodeKind) -> bool {
    matches!(
        kind,
        NodeKind::Query { .. }
            | NodeKind::Window { .. }
            | NodeKind::WhereSubquery { .. }
            | NodeKind::ScalarSubquery
            | NodeKind::Subquery { .. }
            | NodeKind::CteRef { .. }
            | NodeKind::Table { .. }
    )
}

impl<'t> Planner<'t> {
    /// `root` carried into `destination`: every column reference at its
    /// own level re-qualified by its slot, no longer correlated if it now
    /// sits beside its table, a merged column written out as the columns
    /// it stands for; `extra` replaces further positions.
    pub(super) fn moved(
        &mut self,
        root: NodeRef<'t>,
        destination: &Scope,
        extra: Vec<PlannedReplacement>,
    ) -> Moved<'t> {
        let mut replacements = extra;
        let end = root.end();
        let start = u32::try_from(root.id().index()).unwrap_or(u32::MAX);
        let mut id = start;
        while id < end {
            let position = root.at_position(NodeId::from_index(id));
            if id != start && subtree_is_opaque(position.kind()) {
                id = position.end();
                continue;
            }
            if let NodeKind::Column { .. } = position.kind()
                && let Some(replacement) = self.column_moved(position, destination)
            {
                replacements.push(replacement);
            }
            id += 1;
        }
        Moved { root, replacements }
    }

    /// What a column reference becomes when carried into `destination`:
    /// nothing when its qualifier already names its slot.
    fn column_moved(
        &mut self,
        column: NodeRef<'t>,
        destination: &Scope,
    ) -> Option<PlannedReplacement> {
        let NodeKind::Column {
            table,
            column: name,
        } = column.kind()
        else {
            return None;
        };
        match self.bindings.column_by_id(column.id()) {
            Some(ColumnTarget::Column(binding)) => {
                let binding = *binding;
                if destination.contains(&binding) {
                    self.uncorrelated.push(column.id());
                }
                let qualifier = self
                    .bindings
                    .slot(binding.slot)
                    .map(|slot| EcoString::from(slot.qualifier()))
                    .or_else(|| table.clone());
                if qualifier.as_ref() == table.as_ref() {
                    return None;
                }
                Some(PlannedReplacement {
                    at: column.id(),
                    replacement: Replacement::LeafFrom(NodeKind::Column {
                        table: qualifier,
                        column: name.clone(),
                    }),
                    bind: None,
                })
            }
            Some(ColumnTarget::Merged(id)) => {
                let (replacement, bind, _) = self.merged_written(*id)?;
                Some(PlannedReplacement {
                    at: column.id(),
                    replacement,
                    bind: Some(bind),
                })
            }
            Some(ColumnTarget::Output) | None => None,
        }
    }

    /// A reference to a bound column, qualified by its slot.
    pub(super) fn column_kind_for(&self, binding: ColumnBinding) -> NodeKind {
        let (table, column) = match (
            self.bindings.slot(binding.slot),
            self.bindings.binding_column(&binding),
        ) {
            (Some(slot), Some(column)) => (EcoString::from(slot.qualifier()), column.name.clone()),
            _ => {
                debug_assert!(false, "column binding without a slot or column");
                (EcoString::new(), EcoString::new())
            }
        };
        NodeKind::Column {
            table: Some(table),
            column,
        }
    }

    /// A `USING`/`NATURAL` merged column written out: the left column, or
    /// `coalesce(left, right)` under an outer join; with what the fresh
    /// nodes bind to and the column's name.
    pub(super) fn merged_written(&self, id: MergedId) -> Option<(Replacement, Bind, EcoString)> {
        let Some(merged) = self.bindings.merged(id) else {
            debug_assert!(false, "merged column without a binding");
            return None;
        };
        let left = self.column_kind_for(merged.left);
        if !merged.outer {
            return Some((
                Replacement::Leaf(left),
                Bind::Column(merged.left),
                merged.name.clone(),
            ));
        }
        let right = self.column_kind_for(merged.right);
        let nodes = vec![
            Node {
                len: 3,
                kind: NodeKind::Function {
                    schema: None,
                    name: EcoString::from("coalesce"),
                    arg_count: 2,
                    agg_star: false,
                    agg_distinct: false,
                    order_count: 0,
                    has_filter: false,
                    has_over: false,
                },
            },
            Node { len: 1, kind: left },
            Node {
                len: 1,
                kind: right,
            },
        ];
        Some((
            Replacement::Fragment(nodes),
            Bind::Merged {
                left: merged.left,
                right: Some(merged.right),
            },
            merged.name.clone(),
        ))
    }
}
