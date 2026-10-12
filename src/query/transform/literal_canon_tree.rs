//! Date-time literal canonicalisation (PGC-287 class 2) on the rope: the
//! walk of `literal_canon` over views, each rewritten literal a replacement,
//! then one copy of the tree with those positions replaced.

use super::literal_canon::{LiteralCanonOutcome, Pass, cast_kind};
use crate::query::ast::rope::{
    Body, Case, Function, JoinQual, NodeKind, Query, QueryTree, Replacement, Replacements, Scalar,
    ScalarKind, Select, SelectItem, Source, TreeBuilder, Values, View, Where, WhereKind,
};
use crate::query::ast::{LiteralValue, NodeId};
use crate::query::literal_settings::LiteralSettings;
use crate::query::resolve::{Bindings, BindingsBuilder, ResolvedQuery};
use crate::query::typed_compare::ValueKind;

/// [`super::query_literals_canonicalise`] on the rope: the same walk over views,
/// collecting each rewritten literal as a replacement, then one copy of
/// the tree with those positions replaced. `None` when no literal changes;
/// the bindings are the input's, since the tree's shape is the same.
pub fn query_literals_canonicalise_tree(
    bound: &ResolvedQuery,
    settings: &LiteralSettings,
) -> (Option<(QueryTree, Bindings)>, LiteralCanonOutcome) {
    let mut pass = TreePass {
        bound,
        literals: Pass {
            settings,
            outcome: LiteralCanonOutcome::default(),
        },
        replacements: Replacements::new(),
    };
    pass.query(bound.rope().root());
    let outcome = pass.literals.outcome;
    if pass.replacements.is_empty() {
        return (None, outcome);
    }
    let mut builder = TreeBuilder::over(bound.rope());
    builder.copy(bound.rope().root().node_ref(), &mut pass.replacements);
    let (tree, remap) = builder.finish();
    let bindings = BindingsBuilder::from_remapped(bound.rope_bindings(), &remap).finish();
    (Some((tree, bindings)), outcome)
}

/// The rope walk: [`BoundPass`] over views, literals rewritten on a copy
/// of their value and recorded by position when they change.
struct TreePass<'a> {
    bound: &'a ResolvedQuery,
    literals: Pass<'a>,
    replacements: Replacements,
}

impl TreePass<'_> {
    fn query(&mut self, query: Query<'_>) {
        match query.body() {
            Body::Select(select) => self.select(select),
            Body::SetOp(set_op) => {
                self.query(set_op.left());
                self.query(set_op.right());
            }
            Body::Values(values) => self.values(values),
        }
        for clause in query.order_by() {
            self.scalar(clause.expr(), None);
        }
    }

    fn values(&mut self, values: Values<'_>) {
        for row in values.rows() {
            for literal in row.literals() {
                self.literal(literal.node_ref().id(), literal.value(), None);
            }
        }
    }

    fn select(&mut self, select: Select<'_>) {
        for source in select.from() {
            self.source(source);
        }
        if let Some(where_clause) = select.where_clause() {
            self.where_expr(where_clause);
        }
        if let Some(having) = select.having() {
            self.where_expr(having);
        }
        for item in select.items().into_iter().flatten() {
            if let SelectItem::Expr(expr) = item {
                self.scalar(expr.expr(), None);
            }
        }
    }

    fn source(&mut self, source: Source<'_>) {
        match source {
            Source::Table(_) => {}
            Source::Subquery(subquery) => self.query(subquery.query()),
            Source::CteRef(cte_ref) => self.query(cte_ref.query()),
            Source::Join(join) => {
                self.source(join.left());
                self.source(join.right());
                if let JoinQual::On(on) = join.qual() {
                    self.where_expr(on);
                }
            }
        }
    }

    fn where_expr(&mut self, expr: Where<'_>) {
        match expr.kind() {
            WhereKind::Scalar(scalar) => self.scalar(scalar, None),
            WhereKind::Unary(unary) => self.where_expr(unary.expr()),
            WhereKind::Binary(binary) => {
                let kind = self
                    .column_kind(binary.lexpr())
                    .or_else(|| self.column_kind(binary.rexpr()));
                self.operand(binary.lexpr(), kind);
                self.operand(binary.rexpr(), kind);
            }
            WhereKind::Multi(multi) => {
                let kind = multi
                    .exprs()
                    .next()
                    .and_then(|first| self.column_kind(first));
                for (i, member) in multi.exprs().enumerate() {
                    self.operand(member, kind.filter(|_| i > 0));
                }
            }
            WhereKind::Subquery(subquery) => {
                if let Some(test) = subquery.test_expr() {
                    self.scalar(test, None);
                }
                self.query(subquery.query());
            }
        }
    }

    fn operand(&mut self, side: Where<'_>, kind: Option<ValueKind>) {
        match side.kind() {
            WhereKind::Scalar(scalar) => self.scalar(scalar, kind),
            WhereKind::Unary(_)
            | WhereKind::Binary(_)
            | WhereKind::Multi(_)
            | WhereKind::Subquery(_) => self.where_expr(side),
        }
    }

    fn scalar(&mut self, expr: Scalar<'_>, kind: Option<ValueKind>) {
        match expr.kind() {
            ScalarKind::Literal(literal) => self.literal(literal.id(), literal.value(), kind),
            ScalarKind::TypeCast(cast) => {
                let target = cast_kind(cast.target());
                let inner = cast.expr();
                if let ScalarKind::Literal(literal) = inner.kind() {
                    self.literal(literal.id(), literal.value(), target);
                    self.literals.mixed_kinds_check(kind, target);
                } else {
                    self.scalar(inner, None);
                }
            }
            ScalarKind::Array(array) => {
                for element in array.elements() {
                    self.scalar(element, kind);
                }
            }
            ScalarKind::Function(function) => self.function(function),
            ScalarKind::Case(case) => self.case(case),
            ScalarKind::Arithmetic(arithmetic) => {
                self.scalar(arithmetic.left(), None);
                self.scalar(arithmetic.right(), None);
            }
            ScalarKind::Subquery(subquery) => self.query(subquery.query()),
            ScalarKind::Column(_) => {}
        }
    }

    fn function(&mut self, function: Function<'_>) {
        for arg in function.args() {
            self.scalar(arg, None);
        }
        if let Some(filter) = function.filter() {
            self.where_expr(filter);
        }
        for clause in function.agg_order() {
            self.scalar(clause.expr(), None);
        }
    }

    fn case(&mut self, case: Case<'_>) {
        if let Some(arg) = case.arg() {
            self.scalar(arg, None);
        }
        for when in case.whens() {
            self.where_expr(when.condition());
            self.scalar(when.result(), None);
        }
        if let Some(default) = case.default() {
            self.scalar(default, None);
        }
    }

    /// Rewrite the literal at `id`; recorded as a replacement when its
    /// value changes.
    fn literal(&mut self, id: NodeId, value: &LiteralValue, kind: Option<ValueKind>) {
        let mut rewritten = value.clone();
        self.literals.literal(&mut rewritten, kind);
        if rewritten != *value {
            self.replacements
                .push(id, Replacement::Leaf(NodeKind::Literal(rewritten)));
        }
    }

    /// The kind of the column a comparison side names, looking through a
    /// cast; an inner join's merged column has its left column's kind.
    fn column_kind(&self, side: Where<'_>) -> Option<ValueKind> {
        let WhereKind::Scalar(scalar) = side.kind() else {
            return None;
        };
        self.scalar_column_kind(scalar)
    }

    fn scalar_column_kind(&self, scalar: Scalar<'_>) -> Option<ValueKind> {
        match scalar.kind() {
            ScalarKind::Column(column) => self
                .bound
                .column_type_at(column.id())
                .map(ValueKind::of_type),
            ScalarKind::TypeCast(cast) => {
                cast_kind(cast.target()).or_else(|| self.scalar_column_kind(cast.expr()))
            }
            ScalarKind::Literal(_)
            | ScalarKind::Function(_)
            | ScalarKind::Case(_)
            | ScalarKind::Arithmetic(_)
            | ScalarKind::Subquery(_)
            | ScalarKind::Array(_) => None,
        }
    }
}
