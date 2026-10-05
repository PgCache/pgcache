//! Build a [`QueryExpr`] directly from PostgreSQL's raw parse tree (the C node
//! structs from `pg_query::pg_nodes`), via the `pg_query::parse_raw_scoped`
//! callback — the proxy's single SQL-parsing path (PGC-192). It reads tagged C
//! node pointers through [`super::raw`] with no protobuf serialize/decode
//! round-trip. Must run inside the callback (the tree is freed when it returns).

#![allow(clippy::wildcard_enum_match_arm)]

use std::os::raw::c_void;

use pg_query::pg_nodes as pg;

use super::raw::{NodePtr, cast, list_nodes, node_tag, node_tag_name, sublink_type_name};
use super::{AstError, CteDefinition, NullOrder, OrderDirection, QueryExpr, SubLinkType};
use crate::query::transform::query_expr_constant_fold;
use crate::query::write::{IsolationEffect, TransactionBoundary, WriteClass};

mod from;
mod scalar;
mod select;
mod where_clause;
mod window;
mod write;

use select::select_stmt_to_query_expr;
use window::window_refs_assert_resolved;

/// Convert the root of a raw parse tree (`List *` of `RawStmt`, as an opaque
/// pointer from `parse_raw_scoped`) into a [`QueryExpr`].
///
/// # Safety
/// `tree_root` must be the live `List *` handed to the `parse_raw_scoped`
/// callback, valid for the duration of this call.
pub unsafe fn query_expr_convert_raw(tree_root: *const c_void) -> Result<QueryExpr, AstError> {
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

/// Root-statement classification for the proxy's analyze path: the SELECT
/// conversion result plus enough write classification to feed the
/// per-connection read-after-write log (PGC-124).
#[derive(Debug)]
pub enum RawStatement {
    /// Root was a `SelectStmt`. The converted expression is boxed to keep
    /// the enum near the size of its unit variants.
    Select {
        converted: Result<Box<QueryExpr>, AstError>,
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
    },
}

/// Classify the root of a raw parse tree, converting a `SelectStmt` root
/// exactly as [`query_expr_convert_raw`] and classifying everything else for
/// write tracking. Errs only on structural failures (multiple statements,
/// empty statement).
///
/// # Safety
/// `tree_root` must be the live `List *` handed to the `parse_raw_scoped`
/// callback, valid for the duration of this call.
pub unsafe fn statement_convert_raw(tree_root: *const c_void) -> Result<RawStatement, AstError> {
    unsafe {
        let stmt = root_statement(tree_root)?;
        Ok(match node_tag(stmt) {
            pg::NodeTag_T_SelectStmt => {
                let select = cast::<pg::SelectStmt>(stmt);
                let converted = select_root_convert(select).map(Box::new);
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
                } => RawStatement::ReadOnlyUtility {
                    transaction,
                    isolation,
                },
            },
        })
    }
}

/// Unwrap the single root statement of a parse tree.
unsafe fn root_statement(tree_root: *const c_void) -> Result<NodePtr, AstError> {
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

/// Convert a root `SelectStmt` including the post-conversion passes shared by
/// both entry points.
unsafe fn select_root_convert(select: *const pg::SelectStmt) -> Result<QueryExpr, AstError> {
    unsafe {
        let mut query = select_stmt_to_query_expr(select)?;
        window_refs_assert_resolved(&query)?;
        query_expr_constant_fold(&mut query);
        Ok(query)
    }
}

struct ParseContext {
    ctes: Vec<CteDefinition>,
}

impl ParseContext {
    fn empty() -> Self {
        Self { ctes: Vec::new() }
    }

    /// The outer context's CTEs plus this query's own.
    fn with_ctes(outer: &ParseContext, ctes: &[CteDefinition]) -> Self {
        let mut all = outer.ctes.clone();
        all.extend_from_slice(ctes);
        Self { ctes: all }
    }

    fn cte_find(&self, name: &str) -> Option<&CteDefinition> {
        self.ctes.iter().find(|c| c.name == name)
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

/// Parse SQL straight to a `QueryExpr` via the raw path. Test-only convenience
/// shared by unit tests across the crate that previously routed through the
/// (now-removed) protobuf converter.
#[cfg(test)]
pub(crate) fn query_expr_parse(sql: &str) -> Result<QueryExpr, AstError> {
    pg_query::parse_raw_scoped(sql, |tree| unsafe { query_expr_convert_raw(tree) })
        .expect("parse SQL")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::ast::Deparse;

    /// Queries exercising every node kind the converter handles. Each must
    /// convert successfully and survive a deparse→reparse roundtrip.
    const CORPUS: &[&str] = &[
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

    #[test]
    fn corpus_converts_and_roundtrips() {
        let mut failures = Vec::new();
        for sql in CORPUS {
            let Ok(query) = query_expr_parse(sql) else {
                failures.push(format!("\nSQL: {sql}\n  did not convert"));
                continue;
            };
            // Deparse → reparse must yield the same QueryExpr (deparse fidelity).
            let mut buf = String::with_capacity(256);
            query.deparse(&mut buf);
            match query_expr_parse(&buf) {
                Ok(reparsed) if reparsed == query => {}
                other => failures.push(format!(
                    "\nSQL: {sql}\n  deparsed: {buf}\n  roundtrip: {other:?}"
                )),
            }
        }
        assert!(
            failures.is_empty(),
            "raw converter corpus failures ({}):{}",
            failures.len(),
            failures.join("")
        );
    }
}
