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
use super::raw::{
    NodePtr, cast, cstr, list_nodes, node_tag, node_tag_name, string_node_value, sublink_type_name,
};
use super::rope::{QueryTree, TreeBuilder};
use super::{AstError, CteMaterialization, NullOrder, OrderDirection, SubLinkType};
use crate::query::write::{IsolationEffect, SettingsEffect, TransactionBoundary, WriteClass};

mod from;
mod scalar;
mod select;
mod where_clause;
mod window;
mod write;

/// Convert the root of a raw parse tree (`List *` of `RawStmt`, as an opaque
/// pointer from `parse_raw_scoped`) into a [`QueryTree`]. `sql_len` is the
/// statement text's length, which sizes the builder.
///
/// # Safety
/// `tree_root` must be the live `List *` handed to the `parse_raw_scoped`
/// callback, valid for the duration of this call.
pub unsafe fn query_tree_convert_raw(
    tree_root: *const c_void,
    sql_len: usize,
) -> Result<QueryTree, AstError> {
    unsafe {
        let stmt = root_statement(tree_root)?;
        match node_tag(stmt) {
            pg::NodeTag_T_SelectStmt => select_root_convert(cast::<pg::SelectStmt>(stmt), sql_len),
            other => Err(AstError::UnsupportedStatement {
                statement_type: node_tag_name(other).into_owned(),
            }),
        }
    }
}

/// Root-statement classification for the proxy's analyze path: the SELECT
/// conversion result plus enough write classification to feed the
/// per-connection read-after-write log (PGC-124).
#[derive(Debug)]
pub enum RawStatement {
    /// Root was a `SelectStmt`.
    Select {
        converted: Result<QueryTree, AstError>,
        /// `Some` when conversion failed and the WITH clause contains a
        /// data-modifying CTE — the "select" writes.
        cte_write: Option<WriteClass>,
    },
    /// Root can modify table data (DML, DDL, EXECUTE, unknown, ...).
    Write(WriteClass),
    /// Root provably cannot modify table data (txn control, SET, SHOW, ...).
    ReadOnlyUtility {
        /// Set for transaction-control statements.
        transaction: Option<TransactionBoundary>,
        /// Effect on the session's isolation-level state (PGC-387).
        isolation: IsolationEffect,
        /// Tracked session settings the statement may change (PGC-287).
        settings: SettingsEffect,
    },
}

/// Classify the root of a raw parse tree, converting a `SelectStmt` root
/// exactly as [`query_tree_convert_raw`] and classifying everything else for
/// write tracking. Errs only on structural failures (multiple statements,
/// empty statement).
///
/// # Safety
/// `tree_root` must be the live `List *` handed to the `parse_raw_scoped`
/// callback, valid for the duration of this call.
pub unsafe fn statement_convert_raw(
    tree_root: *const c_void,
    sql_len: usize,
) -> Result<RawStatement, AstError> {
    unsafe {
        let stmt = root_statement(tree_root)?;
        Ok(match node_tag(stmt) {
            pg::NodeTag_T_SelectStmt => {
                let select = cast::<pg::SelectStmt>(stmt);
                let converted = select_root_convert(select, sql_len);
                let cte_write = if converted.is_err() {
                    write::select_cte_write_class(select)
                } else {
                    None
                };
                RawStatement::Select {
                    converted,
                    cte_write,
                }
            }
            _ => match write::non_select_classify(stmt) {
                write::NonSelectClass::Write(class) => RawStatement::Write(class),
                write::NonSelectClass::ReadOnly {
                    transaction,
                    isolation,
                    settings,
                } => RawStatement::ReadOnlyUtility {
                    transaction,
                    isolation,
                    settings,
                },
            },
        })
    }
}

/// Unwrap the single root statement of a parse tree.
pub(super) unsafe fn root_statement(tree_root: *const c_void) -> Result<NodePtr, AstError> {
    unsafe {
        let mut stmts = list_nodes(tree_root as *const pg::List);
        let (Some(raw_stmt), None) = (stmts.next(), stmts.next()) else {
            return Err(AstError::MultipleStatements);
        };

        let stmt = (*cast::<pg::RawStmt>(raw_stmt)).stmt as NodePtr;
        if stmt.is_null() {
            return Err(AstError::MissingStatement);
        }
        Ok(stmt)
    }
}

/// Convert a root `SelectStmt`. The builder is sized from the statement
/// text: the bench fixtures run four to nine bytes of SQL per node, and the
/// vector is transient, so sizing for the densest only costs a short-lived
/// over-allocation on the rest.
unsafe fn select_root_convert(
    select: *const pg::SelectStmt,
    sql_len: usize,
) -> Result<QueryTree, AstError> {
    unsafe {
        let mut builder = TreeBuilder::with_capacity(sql_len / 4 + 8);
        query_emit(&mut builder, select, &ParseContext::empty())?;
        Ok(builder.finish().0)
    }
}

/// A CTE in scope: its raw definition, re-emitted at each reference, and
/// the context it was defined in.
pub(super) struct RawCte {
    name: EcoString,
    node: *const pg::CommonTableExpr,
    column_aliases: Vec<EcoString>,
    materialization: CteMaterialization,
}

/// The CTEs visible to a query: its own in order, over the scope they were
/// defined in; a CTE's own context is the entries before it. Each level
/// borrows its query's list, so a nested query or a reference costs nothing.
pub(super) struct ParseContext<'a> {
    outer: Option<&'a ParseContext<'a>>,
    ctes: &'a [RawCte],
}

impl<'a> ParseContext<'a> {
    fn empty() -> Self {
        Self {
            outer: None,
            ctes: &[],
        }
    }

    /// `ctes` in scope over `outer`.
    fn with_ctes(outer: &'a ParseContext<'a>, ctes: &'a [RawCte]) -> Self {
        Self {
            outer: Some(outer),
            ctes,
        }
    }

    /// The CTE `name` refers to, innermost scope first, with the context
    /// its body is converted in.
    fn cte_find(&self, name: &str) -> Option<(&'a RawCte, ParseContext<'a>)> {
        if let Some(index) = self.ctes.iter().position(|cte| cte.name == name) {
            let cte = self.ctes.get(index)?;
            let defined_in = ParseContext {
                outer: self.outer,
                ctes: self.ctes.get(..index).unwrap_or(&[]),
            };
            return Some((cte, defined_in));
        }
        self.outer?.cte_find(name)
    }
}

/// The string entries of a raw name list; other entries are skipped.
pub(super) unsafe fn names_extract(list: *const pg::List) -> Vec<EcoString> {
    unsafe {
        list_nodes(list)
            .filter_map(|n| string_node_value(n).map(EcoString::from))
            .collect()
    }
}

/// The raw definition behind a CTE node: not recursive, with a SELECT body.
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

// ---------- Enum mapping (C int → pgcache enum) ----------

pub(super) fn order_dir_map(dir: pg::SortByDir) -> Result<OrderDirection, AstError> {
    match dir {
        pg::SortByDir_SORTBY_ASC | pg::SortByDir_SORTBY_DEFAULT => Ok(OrderDirection::Asc),
        pg::SortByDir_SORTBY_DESC => Ok(OrderDirection::Desc),
        other => Err(AstError::UnsupportedFeature {
            feature: format!("ORDER BY direction: {other}"),
        }),
    }
}

pub(super) fn null_order_map(n: pg::SortByNulls) -> Result<NullOrder, AstError> {
    match n {
        pg::SortByNulls_SORTBY_NULLS_DEFAULT => Ok(NullOrder::Default),
        pg::SortByNulls_SORTBY_NULLS_FIRST => Ok(NullOrder::NullsFirst),
        pg::SortByNulls_SORTBY_NULLS_LAST => Ok(NullOrder::NullsLast),
        other => Err(AstError::UnsupportedFeature {
            feature: format!("ORDER BY NULLS ordering: {other}"),
        }),
    }
}

pub(super) fn sublink_type_map(t: pg::SubLinkType) -> Result<SubLinkType, AstError> {
    match t {
        pg::SubLinkType_EXISTS_SUBLINK => Ok(SubLinkType::Exists),
        pg::SubLinkType_ANY_SUBLINK => Ok(SubLinkType::Any),
        pg::SubLinkType_ALL_SUBLINK => Ok(SubLinkType::All),
        pg::SubLinkType_EXPR_SUBLINK => Ok(SubLinkType::Expr),
        other => Err(AstError::UnsupportedSubLinkType {
            sublink_type: sublink_type_name(other).into_owned(),
        }),
    }
}

/// Parse `sql` straight to the rope through the raw path: the one test entry
/// for every fixture that starts from SQL.
#[cfg(test)]
pub(crate) fn query_tree_parse(sql: &str) -> Result<QueryTree, AstError> {
    pg_query::parse_raw_scoped(sql, |tree| unsafe {
        query_tree_convert_raw(tree, sql.len())
    })
    .expect("parse SQL")
}

#[cfg(test)]
pub(crate) mod tests {
    use super::query_tree_parse;
    use crate::query::ast::rope::tests::CORPUS as ROPE_CORPUS;
    use crate::query::decorrelate::fixtures::corpus as decorrelate_corpus;
    use crate::query::resolve::tests::{BINDING_CORPUS, RENDER_CORPUS};
    use crate::query::transform::predicate_pushdown::tests::CORPUS as PUSHDOWN_CORPUS;

    /// Queries exercising every node kind the converter handles.
    pub(crate) const CORPUS: &[&str] = &[
        // basic select / projection
        "SELECT id, name FROM users WHERE id = 1",
        "SELECT * FROM products",
        "SELECT t.* FROM users t",
        "SELECT $1 FROM users",
        "SELECT id AS user_id, name AS full_name FROM users",
        "SELECT id, name FROM test.users WHERE active = true",
        "SELECT u.id, u.name FROM users u WHERE u.active = true",
        "SELECT DISTINCT category FROM products",
        // where: comparisons, boolean, params, null
        "SELECT * FROM users WHERE name = 'john' AND active = true",
        "SELECT id FROM test WHERE str = 'hello' OR str = 'world'",
        "SELECT id FROM test WHERE NOT str = 'hello'",
        "SELECT id FROM test WHERE name = 'john' AND age > 25 AND active = true",
        "SELECT id FROM test WHERE id != 123 AND id <> 99 AND id < 5 AND id <= 5 AND id > 1 AND id >= 1",
        "SELECT id FROM test WHERE data = NULL",
        "SELECT id FROM test WHERE name = $1 AND age > $2",
        "SELECT id FROM test WHERE deleted_at IS NULL AND name IS NOT NULL",
        "SELECT id FROM test WHERE active IS TRUE AND a IS NOT TRUE AND b IS FALSE AND c IS NOT FALSE",
        "SELECT id FROM test WHERE active IS UNKNOWN OR active IS NOT UNKNOWN",
        // in / between / like / any / all
        "SELECT * FROM t WHERE status IN ('active', 'pending', 'complete')",
        "SELECT * FROM t WHERE id NOT IN (1, 2, 3)",
        "SELECT * FROM t WHERE n BETWEEN 1 AND 10",
        "SELECT * FROM t WHERE n NOT BETWEEN 1 AND 10",
        "SELECT id FROM test WHERE name LIKE 'test%' AND name NOT LIKE 'x%' AND name ILIKE 'A%'",
        "SELECT * FROM t WHERE id = ANY(ARRAY[1,2,3])",
        "SELECT * FROM t WHERE id = ANY($1)",
        "SELECT * FROM t WHERE id <> ALL (SELECT x FROM y)",
        // arithmetic
        "SELECT a + b, c - d, e * f, g / h, i % j FROM t",
        "SELECT id FROM t WHERE a + b = 10",
        // joins
        "SELECT * FROM invoice JOIN product p ON p.id = invoice.product_id",
        "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id WHERE a.id = 1",
        "SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id LEFT JOIN payments p ON o.id = p.order_id",
        "SELECT * FROM a CROSS JOIN b",
        "SELECT * FROM a NATURAL JOIN b",
        "SELECT * FROM a JOIN b USING (id)",
        "SELECT * FROM a RIGHT JOIN b ON a.id = b.id",
        "SELECT * FROM a FULL JOIN b ON a.id = b.id",
        // subqueries
        "SELECT invoice.id, (SELECT x.data FROM x WHERE 1 = 1) AS one FROM invoice",
        "SELECT * FROM (SELECT * FROM invoice WHERE id = 2) inv",
        "SELECT * FROM (VALUES(1, 2, 'test'), (3, 4, 'a')) v",
        "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.id = t.id)",
        "SELECT * FROM t WHERE id IN (SELECT id FROM u)",
        "SELECT * FROM t WHERE col = (SELECT max(x) FROM u)",
        // aggregates / functions / window
        "SELECT count(*), str FROM test GROUP BY str",
        "SELECT count(DISTINCT id) FROM t",
        "SELECT count(*) FILTER (WHERE active) FROM t",
        "SELECT array_agg(id ORDER BY id DESC) FROM t",
        "SELECT row_number() OVER (PARTITION BY dept ORDER BY salary DESC) FROM emp",
        "SELECT coalesce(a, b, 0), greatest(a, b), least(a, b), nullif(a, b) FROM t",
        // case / cast
        "SELECT CASE WHEN a = 1 THEN 'one' WHEN a = 2 THEN 'two' ELSE 'other' END FROM t",
        "SELECT CASE x WHEN 1 THEN 'a' ELSE 'b' END FROM t",
        "SELECT id::text, n::numeric(10,2), tags::int[] FROM t",
        "SELECT * FROM t WHERE created::date = '2020-01-01'",
        // order by / limit / having
        "SELECT id FROM t ORDER BY name ASC, created DESC NULLS LAST LIMIT 10 OFFSET 5",
        "SELECT id FROM t ORDER BY 1 LIMIT $1",
        "SELECT dept, count(*) FROM emp GROUP BY dept HAVING count(*) > 5",
        // set ops
        "SELECT a FROM t1 UNION SELECT a FROM t2",
        "SELECT a FROM t1 UNION ALL SELECT a FROM t2",
        "SELECT a FROM t1 INTERSECT SELECT a FROM t2",
        "SELECT a FROM t1 EXCEPT SELECT a FROM t2",
        // CTEs
        "WITH c AS (SELECT id FROM t WHERE x = 1) SELECT * FROM c",
        "WITH a AS (SELECT 1 AS x), b AS (SELECT x FROM a) SELECT * FROM b",
        "WITH c AS MATERIALIZED (SELECT id FROM t) SELECT * FROM c",
    ];

    /// Shapes the corpora leave out.
    const EXTRA: &[&str] = &[
        "SELECT a FROM t WHERE b = 1 OR c = 2 OR d = 3 AND e = 4",
        "SELECT sum(a) OVER w, rank() OVER w FROM t WINDOW w AS (PARTITION BY b ORDER BY c)",
        "SELECT sum(a) OVER w FROM t WINDOW w AS (ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING)",
        "SELECT a FROM t WHERE 1 + 2 * 3 = a AND (4 - 1) / 0 = b",
        "SELECT a FROM t WHERE b IN (1, 1 + 1, $1)",
        "SELECT CASE WHEN a THEN 1 END, nullif(a, 1), greatest(a), least(a, b, c) FROM t",
        "SELECT a::numeric(10, 2), b::text[], c::pg_catalog.int4 FROM t",
        "SELECT * FROM t AS x (p, q) JOIN u AS y (r) USING (p)",
        "WITH c AS (SELECT 1) SELECT * FROM c, c AS d",
        "WITH c AS (SELECT 1 AS x), d AS (SELECT x FROM c) SELECT * FROM d JOIN c ON c.x = d.x",
        "SELECT a FROM t, u WHERE t.id = u.id",
        "SELECT a FROM t WHERE b BETWEEN 1 AND 2 AND c NOT BETWEEN SYMMETRIC 3 AND 4",
        "SELECT 1",
    ];

    /// What the converter rejects.
    const REJECTED: &[&str] = &[
        "SELECT a FROM t FOR UPDATE",
        "WITH RECURSIVE r AS (SELECT 1) SELECT * FROM r",
        "SELECT sum(a) OVER w FROM t",
        "SELECT a FROM t ORDER BY sum(b) OVER w",
        "SELECT sum(a) OVER (w ORDER BY b) FROM t WINDOW w AS (PARTITION BY c)",
        "SELECT a FROM t LIMIT ALL",
        "SELECT a FROM t WHERE b ~ 'x'",
        "SELECT a FROM t WHERE b > ANY (SELECT c FROM u)",
        "SELECT * FROM (VALUES (1 + 1)) v",
        "SELECT a FROM t GROUP BY a + 1",
        "SELECT db.myschema.f(name) FROM t",
        "SELECT NOT a FROM t",
    ];

    fn corpus() -> Vec<&'static str> {
        CORPUS
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

    /// Every corpus query converts, and its rendering converts back to the
    /// same tree.
    #[test]
    fn test_corpora_round_trip_through_rendering() {
        for sql in corpus() {
            let tree = query_tree_parse(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
            let rendered = tree.sql();
            let again =
                query_tree_parse(&rendered).unwrap_or_else(|e| panic!("{sql}: {rendered}: {e}"));
            assert_eq!(again, tree, "{sql} through {rendered}");
        }
    }

    #[test]
    fn test_rejected_shapes() {
        for sql in REJECTED {
            assert!(query_tree_parse(sql).is_err(), "{sql}");
        }
    }
}
