//! Build a [`QueryTree`] directly from PostgreSQL's raw parse tree (the C
//! node structs from `pg_query::pg_nodes`), via the `pg_query::parse_raw_scoped`
//! callback. Each node is emitted into a [`TreeBuilder`] in pre-order with
//! the counts its kind declares read off the raw lists first, then its
//! children; nothing is built that is not the rope. Must run inside the
//! callback (the raw tree is freed when it returns).
//!
//! * [`select`] — queries, CTEs, set operations, VALUES, the select list,
//!   ORDER BY, GROUP BY and LIMIT.
//! * [`from`] — tables, CTE references, joins and subqueries.
//! * [`where_clause`] — predicates, in their own error domain.
//! * [`scalar`] — columns, literals, functions, casts, CASE, arithmetic.
//! * [`window`] — `OVER` clauses, named windows and frames.

#![allow(clippy::wildcard_enum_match_arm)]

use std::os::raw::c_void;

use ecow::EcoString;
use pg_query::pg_nodes as pg;

use self::select::query_emit;
use super::convert_raw::root_statement;
use super::raw::{NodePtr, cast, cstr, list_nodes, node_tag, node_tag_name, string_node_value};
use super::rope::{QueryTree, TreeBuilder};
use super::{AstError, CteMaterialization};
use crate::query::transform::query_tree_constant_fold;

mod from;
mod scalar;
mod select;
mod where_clause;
mod window;

/// Convert the root of a raw parse tree (`List *` of `RawStmt`, as an opaque
/// pointer from `parse_raw_scoped`) into a [`QueryTree`].
///
/// # Safety
/// `tree_root` must be the live `List *` handed to the `parse_raw_scoped`
/// callback, valid for the duration of this call.
pub unsafe fn query_tree_convert_raw(tree_root: *const c_void) -> Result<QueryTree, AstError> {
    unsafe {
        let stmt = root_statement(tree_root)?;
        match node_tag(stmt) {
            pg::NodeTag_T_SelectStmt => select_root_convert(cast::<pg::SelectStmt>(stmt)),
            other => Err(AstError::UnsupportedStatement {
                statement_type: node_tag_name(other).into_owned(),
            }),
        }
    }
}

/// Convert a root `SelectStmt`, then fold the literal arithmetic it holds.
pub(super) unsafe fn select_root_convert(
    select: *const pg::SelectStmt,
) -> Result<QueryTree, AstError> {
    unsafe {
        let mut builder = TreeBuilder::new();
        query_emit(&mut builder, select, &ParseContext::empty())?;
        let (tree, _) = builder.finish();
        Ok(query_tree_constant_fold(&tree))
    }
}

/// A CTE in scope: its raw definition, re-emitted at each reference as the
/// owned converter embedded a copy, and the context it was defined in.
#[derive(Clone)]
pub(super) struct RawCte {
    name: EcoString,
    node: *const pg::CommonTableExpr,
    column_aliases: Vec<EcoString>,
    materialization: CteMaterialization,
}

/// The CTEs visible to a query: the outer scopes' first, then its own in
/// order, so a CTE's own context is every entry before it.
pub(super) struct ParseContext {
    ctes: Vec<RawCte>,
}

impl ParseContext {
    fn empty() -> Self {
        Self { ctes: Vec::new() }
    }

    /// The outer context's CTEs plus this query's own.
    fn with_ctes(outer: &ParseContext, ctes: &[RawCte]) -> Self {
        let mut all = outer.ctes.clone();
        all.extend_from_slice(ctes);
        Self { ctes: all }
    }

    /// The CTE `name` refers to, with the context its body is converted in.
    fn cte_find(&self, name: &str) -> Option<(&RawCte, ParseContext)> {
        let index = self.ctes.iter().position(|cte| cte.name == name)?;
        let cte = self.ctes.get(index)?;
        Some((
            cte,
            ParseContext {
                ctes: self.ctes.get(..index).unwrap_or(&[]).to_vec(),
            },
        ))
    }
}

/// The string entries of a raw name list, as the owned converter read
/// them: other entries are skipped.
pub(super) unsafe fn names_extract(list: *const pg::List) -> Vec<EcoString> {
    unsafe {
        list_nodes(list)
            .filter_map(|n| string_node_value(n).map(EcoString::from))
            .collect()
    }
}

/// The raw definition behind a CTE node, validated as the owned converter
/// validates it.
pub(super) unsafe fn cte_raw(node: NodePtr) -> Result<RawCte, AstError> {
    unsafe {
        if node_tag(node) != pg::NodeTag_T_CommonTableExpr {
            return Err(AstError::UnsupportedFeature {
                feature: format!("{} in WITH clause", node_tag_name(node_tag(node))),
            });
        }
        let cte = cast::<pg::CommonTableExpr>(node);
        let ctename = cstr((*cte).ctename);
        if (*cte).cterecursive {
            return Err(AstError::UnsupportedFeature {
                feature: format!("recursive CTE: {ctename}"),
            });
        }
        let materialization = match (*cte).ctematerialized {
            pg::CTEMaterialize_CTEMaterializeAlways => CteMaterialization::Materialized,
            pg::CTEMaterialize_CTEMaterializeNever => CteMaterialization::NotMaterialized,
            _ => CteMaterialization::Default,
        };
        let inner = (*cte).ctequery as NodePtr;
        if inner.is_null() || node_tag(inner) != pg::NodeTag_T_SelectStmt {
            return Err(AstError::UnsupportedFeature {
                feature: format!("CTE query is not SELECT: {ctename}"),
            });
        }
        Ok(RawCte {
            name: EcoString::from(ctename),
            node: cte,
            column_aliases: names_extract((*cte).aliascolnames),
            materialization,
        })
    }
}

/// Parse `sql` straight to the rope through the raw path: the one test entry
/// for every fixture that starts from SQL.
#[cfg(test)]
pub(crate) fn query_tree_parse(sql: &str) -> Result<QueryTree, AstError> {
    pg_query::parse_raw_scoped(sql, |tree| unsafe { query_tree_convert_raw(tree) })
        .expect("parse SQL")
}

#[cfg(test)]
mod tests {
    use super::query_tree_parse;
    use crate::query::ast::convert_raw::tests::CORPUS as CONVERT_CORPUS;
    use crate::query::ast::rope::QueryTree;
    use crate::query::ast::rope::tests::CORPUS as ROPE_CORPUS;
    use crate::query::ast::{Deparse, query_expr_parse};
    use crate::query::decorrelate::fixtures::corpus as decorrelate_corpus;
    use crate::query::resolve::tests::{BINDING_CORPUS, RENDER_CORPUS};
    use crate::query::transform::predicate_pushdown::tests::CORPUS as PUSHDOWN_CORPUS;

    /// Shapes the corpora leave out, and the rejections the converter makes.
    const EXTRA: &[&str] = &[
        "SELECT a FROM t WHERE b = 1 OR c = 2 OR d = 3 AND e = 4",
        "SELECT sum(a) OVER w, rank() OVER w FROM t WINDOW w AS (PARTITION BY b ORDER BY c)",
        "SELECT sum(a) OVER w FROM t WINDOW w AS (ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING)",
        "SELECT a FROM t WHERE 1 + 2 * 3 = a AND (4 - 1) / 0 = b",
        "SELECT a FROM t WHERE b IN (1, 1 + 1, $1)",
        "SELECT CASE WHEN a THEN 1 END, nullif(a, 1), greatest(a), least(a, b, c) FROM t",
        "SELECT a::numeric(10, 2), b::text[], c::pg_catalog.int4 FROM t",
        "SELECT * FROM t AS x (p, q) JOIN u AS y (r) USING (p)",
        "SELECT a FROM t FOR UPDATE",
        "WITH RECURSIVE r AS (SELECT 1) SELECT * FROM r",
        "WITH c AS (SELECT 1) SELECT * FROM c, c AS d",
        "WITH c AS (SELECT 1 AS x), d AS (SELECT x FROM c) SELECT * FROM d JOIN c ON c.x = d.x",
        "SELECT sum(a) OVER w FROM t",
        "SELECT a FROM t ORDER BY sum(b) OVER w",
        "SELECT sum(a) OVER (w ORDER BY b) FROM t WINDOW w AS (PARTITION BY c)",
        "SELECT a FROM t LIMIT ALL",
        "SELECT a FROM t WHERE b ~ 'x'",
        "SELECT a FROM t WHERE b > ANY (SELECT c FROM u)",
        "SELECT a FROM t, u WHERE t.id = u.id",
        "SELECT * FROM (VALUES (1, 'a'), (2)) v",
        "SELECT * FROM (VALUES (1 + 1)) v",
        "SELECT a FROM t WHERE b BETWEEN 1 AND 2 AND c NOT BETWEEN SYMMETRIC 3 AND 4",
        "SELECT 1",
        "SELECT a FROM t GROUP BY a + 1",
        "SELECT NOT a FROM t",
    ];

    fn corpus() -> Vec<&'static str> {
        CONVERT_CORPUS
            .iter()
            .chain(ROPE_CORPUS)
            .chain(BINDING_CORPUS)
            .chain(RENDER_CORPUS)
            .chain(PUSHDOWN_CORPUS.iter().map(|(sql, _)| sql))
            .chain(EXTRA)
            .copied()
            .chain(decorrelate_corpus())
            .collect()
    }

    /// Over every corpus the rope converter builds the tree the owned
    /// converter builds, rendered through the plain renderer, and fails
    /// where it fails with the same error.
    #[test]
    fn test_rope_converter_matches_the_owned_converter() {
        let (mut converted, mut rejected) = (0, 0);
        for sql in corpus() {
            let expected = query_expr_parse(sql);
            let actual = query_tree_parse(sql);
            match (expected, actual) {
                (Ok(expected), Ok(actual)) => {
                    converted += 1;
                    let mut text = String::new();
                    expected.deparse(&mut text);
                    assert_eq!(actual.sql(), text, "{sql}");
                    assert_eq!(actual, QueryTree::from_query_expr(&expected), "{sql}");
                }
                (Err(expected), Err(actual)) => {
                    rejected += 1;
                    assert_eq!(actual.to_string(), expected.to_string(), "{sql}");
                }
                (Ok(_), Err(actual)) => panic!("{sql}: the rope converter failed: {actual}"),
                (Err(expected), Ok(_)) => {
                    panic!(
                        "{sql}: the rope converter accepted what the owned one rejects: {expected}"
                    )
                }
            }
        }
        assert!(
            converted > 0 && rejected > 0,
            "the corpora exercise both outcomes: {converted} converted, {rejected} rejected"
        );
    }
}
