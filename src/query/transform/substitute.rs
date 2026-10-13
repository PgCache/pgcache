//! Bind substitution on the rope: each `$n` literal becomes its bound value
//! and any arithmetic that then holds only literals folds to one, in one
//! copy of the tree. A tree with no placeholder and nothing to fold comes
//! back by refcount.
//!
//! The fold is decided bottom-up so nested arithmetic folds inside out, and
//! the topmost subtree that
//! folds is the one replaced; a subtree that does not fold (overflow,
//! division by zero, a non-numeric operand) is entered so what folds
//! inside it still does.

use super::AstTransformResult;
use super::constant_fold::literal_arithmetic_fold;
use super::parameters::literal_value_parameters_replace;
use crate::cache::QueryParameters;
use crate::query::ast::rope::{
    NodeKind, NodeRef, QueryTree, Replacement, Replacements, Scalar, ScalarKind, TreeBuilder, View,
};
use crate::query::ast::{LiteralValue, NodeId};

/// `tree` with its placeholders bound to `parameters` and the arithmetic
/// that exposes folded.
pub fn query_tree_parameters_replace(
    tree: &QueryTree,
    parameters: &QueryParameters,
) -> AstTransformResult<QueryTree> {
    tree_rewrite(tree, Some(parameters))
}

/// `tree` with its literal arithmetic folded and its placeholders left as
/// they are: the convert-time fold.
pub fn query_tree_constant_fold(tree: &QueryTree) -> QueryTree {
    match tree_rewrite(tree, None) {
        Ok(folded) => folded,
        // Without parameters nothing can fail; keep the tree as written.
        Err(_) => tree.clone(),
    }
}

fn tree_rewrite(
    tree: &QueryTree,
    parameters: Option<&QueryParameters>,
) -> AstTransformResult<QueryTree> {
    let mut pass = Pass {
        parameters,
        replacements: Replacements::new(),
    };
    pass.subtree(tree.root().node_ref())?;
    if pass.replacements.is_empty() {
        return Ok(tree.clone());
    }
    let mut builder = TreeBuilder::over(tree);
    builder.copy(tree.root().node_ref(), &mut pass.replacements);
    Ok(builder.finish().0)
}

struct Pass<'a> {
    /// `None` folds only: a placeholder is neither replaced nor foldable.
    parameters: Option<&'a QueryParameters>,
    replacements: Replacements,
}

impl Pass<'_> {
    /// Every position of `root` in pre-order, a replaced subtree skipped.
    fn subtree(&mut self, root: NodeRef<'_>) -> AstTransformResult<()> {
        let end = root.end();
        let mut next = u32::try_from(root.id().index()).unwrap_or(u32::MAX);
        while next < end {
            let position = root.at_position(NodeId::from_index(next));
            next = match self.position(position)? {
                Some(replacement) => {
                    self.replacements.push(position.id(), replacement);
                    position.end()
                }
                None => next + 1,
            };
        }
        Ok(())
    }

    /// What `position` becomes: a bound literal for a placeholder, one
    /// literal for arithmetic that folds, nothing otherwise.
    fn position(&self, position: NodeRef<'_>) -> AstTransformResult<Option<Replacement>> {
        let kind = position.kind();
        let folded = if let NodeKind::Literal(LiteralValue::Parameter(_)) = kind {
            self.bound(Scalar::from_ref(position))?
        } else if let NodeKind::Arithmetic { .. } = kind {
            self.fold(Scalar::from_ref(position))?
        } else {
            None
        };
        Ok(folded.map(|value| Replacement::Leaf(NodeKind::Literal(value))))
    }

    /// The literal `expr` folds to: a bound placeholder, a literal as it
    /// is, or arithmetic over two such; `None` for anything else.
    fn fold(&self, expr: Scalar<'_>) -> AstTransformResult<Option<LiteralValue>> {
        Ok(match expr.kind() {
            ScalarKind::Literal(literal) => {
                if let LiteralValue::Parameter(_) = literal.value() {
                    self.bound(expr)?
                } else {
                    Some(literal.value().clone())
                }
            }
            ScalarKind::Arithmetic(arithmetic) => {
                let (Some(left), Some(right)) = (
                    self.fold(arithmetic.left())?,
                    self.fold(arithmetic.right())?,
                ) else {
                    return Ok(None);
                };
                literal_arithmetic_fold(&left, arithmetic.op(), &right)
            }
            ScalarKind::Column(_)
            | ScalarKind::Function(_)
            | ScalarKind::Case(_)
            | ScalarKind::Subquery(_)
            | ScalarKind::Array(_)
            | ScalarKind::TypeCast(_) => None,
        })
    }

    /// The value bound to the placeholder at `expr`; `None` when there are
    /// no parameters to bind.
    fn bound(&self, expr: Scalar<'_>) -> AstTransformResult<Option<LiteralValue>> {
        let Some(parameters) = self.parameters else {
            return Ok(None);
        };
        let NodeKind::Literal(value) = expr.node_ref().kind() else {
            return Ok(None);
        };
        let mut value = value.clone();
        literal_value_parameters_replace(&mut value, parameters)?;
        Ok(Some(value))
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use postgres_types::Type as PgType;

    use super::query_tree_parameters_replace;
    use crate::cache::QueryParameters;
    use crate::oid::TypeOid;
    use crate::query::ast::query_tree_parse;
    use crate::query::ast::rope::deparse::query_deparse_with;
    use crate::query::ast::rope::{Plain, QueryTree};

    fn params(values: &[Option<(&[u8], PgType)>]) -> QueryParameters {
        QueryParameters {
            values: values
                .iter()
                .map(|v| v.as_ref().map(|(bytes, _)| Bytes::copy_from_slice(bytes)))
                .collect(),
            formats: vec![0; values.len()],
            oids: values
                .iter()
                .map(|v| TypeOid::from_type(v.as_ref().map_or(&PgType::TEXT, |(_, ty)| ty)))
                .collect(),
        }
    }

    fn plain(tree: &QueryTree) -> String {
        let mut buf = String::new();
        query_deparse_with(tree.root(), &mut Plain, &mut buf);
        buf
    }

    /// Every placeholder position and every arithmetic that folds or must
    /// not: the rendering after substitution, or the error.
    #[test]
    fn test_substitution_expectations() {
        let text = |value: &'static str| Some((value.as_bytes(), PgType::TEXT));
        let int = |value: &'static str| Some((value.as_bytes(), PgType::INT4));
        #[rustfmt::skip]
        let cases: Vec<(&str, QueryParameters, &str)> = vec![
            ("SELECT id FROM users WHERE id = $1", params(&[text("42")]), "SELECT id FROM users WHERE id = '42'"),
            ("SELECT id FROM users WHERE id = $1 AND name = $2", params(&[text("42"), text("alice")]), "SELECT id FROM users WHERE id = '42' AND name = 'alice'"),
            ("SELECT id FROM users WHERE name = $1", params(&[None]), "SELECT id FROM users WHERE name = NULL"),
            ("SELECT id FROM users WHERE id IN ($1, $2, 3)", params(&[int("1"), int("2")]), "SELECT id FROM users WHERE id IN (1, 2, 3)"),
            ("SELECT id FROM users WHERE id = $1 LIMIT $2 OFFSET $3", params(&[int("1"), int("10"), int("5")]), "SELECT id FROM users WHERE id = 1 LIMIT 10 OFFSET 5"),
            ("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) v WHERE column1 = $1", params(&[int("1")]), "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) v WHERE column1 = 1"),
            ("SELECT id FROM users WHERE id = $1 % 10 + 1", params(&[int("12345")]), "SELECT id FROM users WHERE id = 6"),
            ("SELECT id FROM users WHERE id = ($1 + 2) * 3 - 4 / 2", params(&[int("1")]), "SELECT id FROM users WHERE id = 7"),
            ("SELECT id FROM users WHERE id = $1 / 0", params(&[int("7")]), "SELECT id FROM users WHERE id = (7 / 0)"),
            ("SELECT id FROM users WHERE id = (1 + 2) / 0", params(&[]), "SELECT id FROM users WHERE id = (3 / 0)"),
            ("SELECT id FROM users WHERE id = id + $1 * 2", params(&[int("3")]), "SELECT id FROM users WHERE id = (id + 6)"),
            ("SELECT id FROM users WHERE total = $1 * 1.5", params(&[int("2")]), "SELECT id FROM users WHERE total = 3"),
            ("SELECT id FROM users WHERE id = 1 + 2", params(&[]), "SELECT id FROM users WHERE id = 3"),
            ("SELECT id FROM users WHERE id = $1 AND EXISTS (SELECT 1 FROM orders WHERE amount = $2 + 1)", params(&[int("1"), int("2")]), "SELECT id FROM users WHERE id = 1 AND EXISTS (SELECT 1 FROM orders WHERE amount = 3)"),
            ("SELECT count(*) FILTER (WHERE id = $1) FROM users ORDER BY $2 + 1", params(&[int("1"), int("0")]), "SELECT COUNT(*) FILTER (WHERE id = 1) FROM users ORDER BY 1 ASC"),
            ("SELECT id FROM users u JOIN orders o ON o.user_id = u.id AND o.amount > $1", params(&[int("5")]), "SELECT id FROM users u JOIN orders o ON o.user_id = u.id AND o.amount > 5"),
            ("SELECT id FROM users WHERE id = $2", params(&[text("42")]), "ERR Parameter index 1 out of bounds (have 1 parameters)"),
            ("SELECT id FROM users WHERE id = $1", params(&[Some((b"not a number", PgType::INT4))]), "ERR Invalid parameter value: invalid integer 'not a number': invalid digit found in string"),
        ];
        for (sql, parameters, expected) in cases {
            let tree = query_tree_parse(sql).expect("parse");
            let actual = match query_tree_parameters_replace(&tree, &parameters) {
                Ok(tree) => plain(&tree),
                Err(e) => format!("ERR {}", e.current_context()),
            };
            assert_eq!(actual, expected, "{sql}");
        }
    }

    /// Nothing to substitute or fold: the tree comes back shared.
    #[test]
    fn test_unchanged_tree_is_shared() {
        let tree = query_tree_parse("SELECT id FROM users WHERE id = 1").expect("parse");
        let same = query_tree_parameters_replace(&tree, &params(&[])).expect("substitute");
        assert_eq!(same, tree);
        assert_eq!(same.chunk_count(), tree.chunk_count());
    }
}
