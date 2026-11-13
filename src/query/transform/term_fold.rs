//! Session-term fragments (PGC-580): the largest subtree over a
//! `current_setting(…)` or `current_user` term that is built only of kinds
//! the proxy can evaluate itself — casts, `coalesce`, `nullif`, json field
//! access on a json literal, a FROM-less scalar subquery. Such a fragment
//! folds to one literal under the session's values (`term_eval`), so the
//! key carries the folded value rather than the term's raw setting: every
//! token of one user folds to the same `sub`, and they share one entry.
//! This module knows the shape; the values come at fold time.

use crate::query::ast::rope::{
    Body, NodeKind, NodeRef, Query, QueryTree, Scalar, ScalarKind, SelectItem, View, WhereSubquery,
};
use crate::query::ast::{JsonOp, LiteralValue, NodeId, ScalarOp, SessionTerm, SubLinkType};

/// Whether `expr` is a term fragment: every node of it is one the fold
/// evaluates, and at least one leaf is a session term.
pub(crate) fn scalar_is_term_fragment(expr: Scalar<'_>) -> bool {
    fragment_shape(expr) == Some(true)
}

/// Whether a subquery in a predicate position is a term fragment: the
/// scalar form of the FROM-less wrapper, `owner = (select current_user)`,
/// which the WHERE converter emits as a sublink rather than a scalar.
pub(crate) fn where_subquery_is_term_fragment(subquery: WhereSubquery<'_>) -> bool {
    subquery.sublink_type() == SubLinkType::Expr
        && subquery.test_expr().is_none()
        && bare_select_item(subquery.query()).is_some_and(|item| fragment_shape(item) == Some(true))
}

/// Whether the node at `position` roots a term fragment, in a scalar or a
/// predicate position.
pub(super) fn position_is_term_fragment(position: NodeRef<'_>) -> bool {
    if matches!(position.kind(), NodeKind::WhereSubquery { .. }) {
        return where_subquery_is_term_fragment(WhereSubquery::from_ref(position));
    }
    let scalar = matches!(
        position.kind(),
        NodeKind::Literal(_)
            | NodeKind::Function { .. }
            | NodeKind::TypeCast { .. }
            | NodeKind::Operator { .. }
            | NodeKind::ScalarSubquery
    );
    scalar && scalar_is_term_fragment(Scalar::from_ref(position))
}

/// The roots of the maximal term fragments of `tree`, in pre-order: what
/// the proxy folds and the writer replaces, each pairing by position. A
/// fragment never holds a bind parameter, so substituting parameters
/// leaves the set and its order as they are.
pub(crate) fn query_tree_term_fragments(tree: &QueryTree) -> Vec<NodeId> {
    let mut fragments = Vec::new();
    let root = tree.root().node_ref();
    let end = root.end();
    let mut next = u32::try_from(root.id().index()).unwrap_or(u32::MAX);
    while next < end {
        let position: NodeRef<'_> = root.at_position(NodeId::from_index(next));
        if position_is_term_fragment(position) {
            fragments.push(position.id());
            next = position.end();
        } else {
            next += 1;
        }
    }
    fragments
}

/// `Some(has_term)` when every node of `expr` is fold-able.
fn fragment_shape(expr: Scalar<'_>) -> Option<bool> {
    match expr.kind() {
        ScalarKind::Literal(literal) => match literal.value() {
            // A built-in setting (`timezone`) changes under `SET` without
            // the proxy tracking a custom shadow for it; only a dotted,
            // customized name folds (ADR-062).
            LiteralValue::SessionTerm(SessionTerm::CurrentSetting { name, .. }) => {
                name.contains('.').then_some(true)
            }
            LiteralValue::SessionTerm(SessionTerm::CurrentUser | SessionTerm::SessionUser) => {
                Some(true)
            }
            LiteralValue::String(_)
            | LiteralValue::StringWithCast(..)
            | LiteralValue::OrderedText(_)
            | LiteralValue::Integer(_)
            | LiteralValue::Float(_)
            | LiteralValue::Boolean(_)
            | LiteralValue::Null
            | LiteralValue::NullWithCast(_) => Some(false),
            LiteralValue::Parameter(_) | LiteralValue::Array(..) => None,
        },
        ScalarKind::TypeCast(cast) => fragment_shape(cast.expr()),
        ScalarKind::Function(function) => {
            let name = function.name();
            if function.schema().is_some()
                || !(name.eq_ignore_ascii_case("coalesce") || name.eq_ignore_ascii_case("nullif"))
            {
                return None;
            }
            let mut has_term = false;
            for arg in function.args() {
                has_term |= fragment_shape(arg)?;
            }
            Some(has_term)
        }
        ScalarKind::Operator(operator) => {
            let ScalarOp::Json(op) = operator.op() else {
                return None;
            };
            if !matches!(
                op,
                JsonOp::Field | JsonOp::FieldText | JsonOp::Path | JsonOp::PathText
            ) {
                return None;
            }
            let left = fragment_shape(operator.left())?;
            let right = fragment_shape(operator.right())?;
            Some(left || right)
        }
        ScalarKind::Subquery(subquery) => {
            let item = bare_select_item(subquery.query())?;
            fragment_shape(item)
        }
        ScalarKind::Column(_) | ScalarKind::Case(_) | ScalarKind::Array(_) => None,
    }
}

/// The single expression of a `SELECT <expr>` with no FROM and no other
/// clause, the form the documented `(select auth.uid())` wrapper takes.
pub(super) fn bare_select_item(query: Query<'_>) -> Option<Scalar<'_>> {
    if query.ctes().next().is_some() || query.order_by().next().is_some() || query.has_limit() {
        return None;
    }
    let Body::Select(select) = query.body() else {
        return None;
    };
    if select.distinct()
        || select.from().next().is_some()
        || select.where_clause().is_some()
        || select.group_by().next().is_some()
        || select.having().is_some()
    {
        return None;
    }
    let mut items = select.items()?;
    let item = items.next()?;
    if items.next().is_some() {
        return None;
    }
    match item {
        SelectItem::Expr(expr) => Some(expr.expr()),
        SelectItem::Star(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::ast::query_tree_parse;

    fn first_item(tree: &QueryTree) -> Scalar<'_> {
        let Body::Select(select) = tree.root().body() else {
            panic!("a SELECT");
        };
        let Some(SelectItem::Expr(item)) = select.items().and_then(|mut items| items.next()) else {
            panic!("one item");
        };
        item.expr()
    }

    /// A fragment is only what the fold evaluates: a column, another
    /// function, an arithmetic operator or a subquery with a FROM breaks it,
    /// and a subtree without a term is not one.
    #[test]
    fn test_fragment_shape() {
        for expr in [
            "current_setting('a')",
            "current_setting('a')::int",
            "coalesce(current_setting('a', true), 'x')",
            "nullif(current_user, 'nobody')",
            "(current_setting('a', true)::jsonb ->> 'k')::uuid",
            "(select current_setting('a'))",
            "(select current_setting('a')::int)",
        ]
        .map(|expr| expr.replace("'a'", "'app.a'"))
        {
            let tree = query_tree_parse(&format!("SELECT {expr} FROM t")).expect("parse");
            assert!(scalar_is_term_fragment(first_item(&tree)), "{expr}");
        }
        for expr in [
            "'x'",
            "'x'::int",
            "coalesce(current_setting('a', true), col)",
            "lower(current_setting('a'))",
            "current_setting('a')::int + 1",
            "(select current_setting('a') from t)",
            "(select current_setting('a') where true)",
            "current_setting('a', true)::jsonb @> '{}'",
            "current_setting(col)",
            "current_setting('timezone')",
            "coalesce(current_setting('search_path', true), 'x')",
        ] {
            let tree = query_tree_parse(&format!("SELECT {expr} FROM t")).expect("parse");
            assert!(!scalar_is_term_fragment(first_item(&tree)), "{expr}");
        }
    }

    /// Fragments are maximal and found in pre-order wherever they stand, the
    /// FROM-less wrapper in a predicate included, which is a sublink rather
    /// than a scalar there.
    #[test]
    fn test_fragments_are_maximal_and_in_order() {
        let tree = query_tree_parse(
            "SELECT id, coalesce(current_setting('app.a', true), 'none') AS a FROM t \
             WHERE owner = current_user AND tenant = current_setting('app.tenant')::int \
             AND note = lower(current_setting('app.note'))",
        )
        .expect("parse");
        let fragments = query_tree_term_fragments(&tree);
        assert_eq!(fragments.len(), 4);
        assert!(
            fragments
                .windows(2)
                .all(|pair| pair[0].index() < pair[1].index())
        );

        let wrapped = query_tree_parse(
            "SELECT id FROM t WHERE owner = (SELECT current_setting('app.who')) AND id > 1",
        )
        .expect("parse");
        assert_eq!(query_tree_term_fragments(&wrapped).len(), 1);

        let plain = query_tree_parse("SELECT id FROM t WHERE id = $1").expect("parse");
        assert!(query_tree_term_fragments(&plain).is_empty());
    }
}
