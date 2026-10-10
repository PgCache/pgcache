//! The finalize pass: stamp every node's pre-order position (ADR-057).

use super::{AstNode, NodePositions, QueryExpr};

/// Stamp pre-order positions over the whole tree and return how many nodes
/// it has. Runs once after conversion and again after any pass that changes
/// the tree's shape; until then nodes carry `NodeId::PENDING`.
pub fn query_expr_finalize(query: &mut QueryExpr) -> u32 {
    let mut positions = NodePositions::default();
    query.finalize(&mut positions);
    positions.count()
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use postgres_types::Type;

    use super::*;
    use crate::cache::QueryParameters;
    use crate::oid::TypeOid;
    use crate::query::ast::{
        ArithmeticExpr, BinaryExpr, CaseExpr, CaseWhen, ColumnNode, CteRefNode, FunctionCall,
        JoinNode, MultiExpr, NodeId, OrderByClause, SelectNode, SetOpNode, StarNode, TableNode,
        TableSubqueryNode, UnaryExpr, ValuesClause, WindowFrame, WindowSpec, query_expr_parse,
    };
    use crate::query::transform::query_expr_parameters_replace;

    /// Every stored id in the tree as `(type, position)`, in position order.
    fn stored_ids(query: &QueryExpr) -> Vec<(&'static str, u32)> {
        fn collect<T: AstNode>(
            query: &QueryExpr,
            name: &'static str,
            id_of: fn(&T) -> NodeId,
            out: &mut Vec<(&'static str, u32)>,
        ) {
            for node in query.nodes::<T>() {
                let id = id_of(node);
                let position = u32::try_from(id.index()).expect("position fits u32");
                out.push((name, if id.is_pending() { u32::MAX } else { position }));
            }
        }
        let mut out = Vec::new();
        collect::<QueryExpr>(query, "QueryExpr", |n| n.id, &mut out);
        collect::<SelectNode>(query, "SelectNode", |n| n.id, &mut out);
        collect::<SetOpNode>(query, "SetOpNode", |n| n.id, &mut out);
        collect::<ValuesClause>(query, "ValuesClause", |n| n.id, &mut out);
        collect::<OrderByClause>(query, "OrderByClause", |n| n.id, &mut out);
        collect::<StarNode>(query, "StarNode", |n| n.id, &mut out);
        collect::<TableNode>(query, "TableNode", |n| n.id, &mut out);
        collect::<TableSubqueryNode>(query, "TableSubqueryNode", |n| n.id, &mut out);
        collect::<CteRefNode>(query, "CteRefNode", |n| n.id, &mut out);
        collect::<JoinNode>(query, "JoinNode", |n| n.id, &mut out);
        collect::<ColumnNode>(query, "ColumnNode", |n| n.id, &mut out);
        collect::<UnaryExpr>(query, "UnaryExpr", |n| n.id, &mut out);
        collect::<BinaryExpr>(query, "BinaryExpr", |n| n.id, &mut out);
        collect::<MultiExpr>(query, "MultiExpr", |n| n.id, &mut out);
        collect::<ArithmeticExpr>(query, "ArithmeticExpr", |n| n.id, &mut out);
        collect::<CaseExpr>(query, "CaseExpr", |n| n.id, &mut out);
        collect::<CaseWhen>(query, "CaseWhen", |n| n.id, &mut out);
        collect::<FunctionCall>(query, "FunctionCall", |n| n.id, &mut out);
        collect::<WindowSpec>(query, "WindowSpec", |n| n.id, &mut out);
        collect::<WindowFrame>(query, "WindowFrame", |n| n.id, &mut out);
        out.sort_by_key(|(_, position)| *position);
        out
    }

    /// Re-finalize a clone and return its node count; the clone's ids must
    /// equal the original's, which proves the tree arrived finalized.
    fn finalized_count(query: &QueryExpr) -> u32 {
        let mut again = query.clone();
        let count = query_expr_finalize(&mut again);
        assert_eq!(
            stored_ids(&again),
            stored_ids(query),
            "tree was not finalized"
        );
        count
    }

    /// SQL, its node count, and the stored `(type, position)` pairs.
    type Golden = (&'static str, u32, &'static [(&'static str, u32)]);

    /// Hand-counted pre-order positions, following each type's
    /// `try_for_each_child` order. Enum wrappers and literal values take a
    /// position but store nothing, so only id-bearing nodes are listed.
    #[test]
    fn test_finalize_matches_traversal_order() {
        let cases: &[Golden] = &[
            (
                "SELECT a FROM t WHERE b = 1",
                17,
                &[
                    ("QueryExpr", 0),
                    ("SelectNode", 2),
                    ("ColumnNode", 6),
                    ("TableNode", 8),
                    ("BinaryExpr", 10),
                    ("ColumnNode", 13),
                ],
            ),
            (
                "SELECT t.x FROM t JOIN u ON t.id = u.id",
                21,
                &[
                    ("QueryExpr", 0),
                    ("SelectNode", 2),
                    ("ColumnNode", 6),
                    ("JoinNode", 8),
                    ("TableNode", 10),
                    ("TableNode", 12),
                    ("BinaryExpr", 14),
                    ("ColumnNode", 17),
                    ("ColumnNode", 20),
                ],
            ),
            (
                "SELECT x FROM t ORDER BY x LIMIT 1",
                12,
                &[
                    ("QueryExpr", 0),
                    ("SelectNode", 2),
                    ("ColumnNode", 6),
                    ("TableNode", 8),
                    ("OrderByClause", 9),
                    ("ColumnNode", 11),
                ],
            ),
            (
                "WITH c AS (SELECT 1) SELECT * FROM c",
                22,
                &[
                    ("QueryExpr", 0),
                    ("QueryExpr", 1),
                    ("SelectNode", 3),
                    ("SelectNode", 9),
                    ("StarNode", 12),
                    ("CteRefNode", 14),
                    ("QueryExpr", 15),
                    ("SelectNode", 17),
                ],
            ),
            (
                "SELECT CASE WHEN a > 1 THEN 'x' ELSE 'y' END FROM t",
                22,
                &[
                    ("QueryExpr", 0),
                    ("SelectNode", 2),
                    ("CaseExpr", 6),
                    ("CaseWhen", 7),
                    ("BinaryExpr", 9),
                    ("ColumnNode", 12),
                    ("TableNode", 21),
                ],
            ),
            (
                "SELECT count(*) FILTER (WHERE a = 1) OVER (PARTITION BY b ORDER BY c \
                 ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
                28,
                &[
                    ("QueryExpr", 0),
                    ("SelectNode", 2),
                    ("FunctionCall", 6),
                    ("BinaryExpr", 8),
                    ("ColumnNode", 11),
                    ("WindowSpec", 15),
                    ("ColumnNode", 17),
                    ("OrderByClause", 18),
                    ("ColumnNode", 20),
                    ("WindowFrame", 21),
                    ("TableNode", 27),
                ],
            ),
            (
                "SELECT 1 UNION ALL SELECT 2",
                17,
                &[
                    ("QueryExpr", 0),
                    ("SetOpNode", 2),
                    ("QueryExpr", 3),
                    ("SelectNode", 5),
                    ("QueryExpr", 10),
                    ("SelectNode", 12),
                ],
            ),
            (
                "SELECT * FROM (VALUES (1, 'a')) v",
                13,
                &[
                    ("QueryExpr", 0),
                    ("SelectNode", 2),
                    ("StarNode", 5),
                    ("TableSubqueryNode", 7),
                    ("QueryExpr", 8),
                    ("ValuesClause", 10),
                ],
            ),
            (
                "SELECT x FROM t WHERE x IN (1, 2) AND NOT EXISTS \
                 (SELECT 1 FROM u WHERE u.id = t.id + 1)",
                46,
                &[
                    ("QueryExpr", 0),
                    ("SelectNode", 2),
                    ("ColumnNode", 6),
                    ("TableNode", 8),
                    ("BinaryExpr", 10),
                    ("MultiExpr", 12),
                    ("ColumnNode", 15),
                    ("UnaryExpr", 23),
                    ("QueryExpr", 25),
                    ("SelectNode", 27),
                    ("TableNode", 33),
                    ("BinaryExpr", 35),
                    ("ColumnNode", 38),
                    ("ArithmeticExpr", 41),
                    ("ColumnNode", 43),
                ],
            ),
        ];
        for (sql, count, expected) in cases {
            let query = query_expr_parse(sql).expect("parse");
            assert_eq!(finalized_count(&query), *count, "node count for {sql}");
            assert_eq!(stored_ids(&query), *expected, "stored ids for {sql}");
        }
    }

    const CORPUS: &[&str] = &[
        "SELECT * FROM actor WHERE actor_id = 42",
        "SELECT fa.film_id, a.first_name FROM film_actor fa \
         JOIN actor a ON a.actor_id = fa.actor_id WHERE fa.film_id = 7",
        "SELECT c.name, COUNT(*) FROM film_category fc \
         JOIN category c ON fc.category_id = c.category_id GROUP BY c.name HAVING COUNT(*) > 1",
        "SELECT title FROM film WHERE film_id IN (SELECT film_id FROM film_actor WHERE actor_id = 5)",
        "WITH recent AS (SELECT customer_id, amount FROM payment WHERE payment_date > '2024-01-15') \
         SELECT customer_id, amount FROM recent WHERE amount > 5.0",
        "SELECT a.*, b.x FROM a NATURAL JOIN b WHERE a.y = ANY(ARRAY[1, 2]) AND b.z::text LIKE 'q%'",
        "SELECT (SELECT max(x) FROM u WHERE u.k = t.k) AS m FROM t WHERE t.d BETWEEN 1 AND 2",
        "SELECT x FROM a UNION SELECT y FROM b ORDER BY x DESC NULLS LAST",
        "SELECT * FROM a JOIN b USING (id) LEFT JOIN c ON c.id = b.id WHERE a.v IS NOT NULL",
        "SELECT string_agg(name, ',' ORDER BY name) FROM t WHERE id <> ALL (SELECT id FROM u)",
    ];

    /// Over a broad corpus: no id is pending, ids are strictly increasing in
    /// visit order for every node type, and all lie below the node count.
    #[test]
    fn test_finalize_stamps_every_node_once() {
        for sql in CORPUS {
            let query = query_expr_parse(sql).expect("parse");
            let count = finalized_count(&query);
            let ids = stored_ids(&query);
            assert!(!ids.is_empty(), "no stored ids for {sql}");
            for window in ids.windows(2) {
                assert!(
                    window[0].1 < window[1].1,
                    "ids not distinct and ordered in {sql}: {window:?}"
                );
            }
            let last = ids.last().expect("at least the root").1;
            assert!(last < count, "id {last} beyond node count {count} in {sql}");
            assert_eq!(query.id.index(), 0, "root id for {sql}");
        }
    }

    /// Bind substitution folds `$1 % 10000 + 1` into one literal, which moves
    /// every later node; the substituted tree must come back finalized.
    #[test]
    fn test_parameters_replace_returns_finalized_tree() {
        let query = query_expr_parse("SELECT id FROM t WHERE user_id = $1 % 10000 + 1 AND x = 2")
            .expect("parse");
        let before = finalized_count(&query);
        let parameters = QueryParameters {
            values: vec![Some(Bytes::from_static(b"12345"))],
            formats: vec![0],
            oids: vec![TypeOid::from_type(&Type::INT8)],
        };
        let replaced = query_expr_parameters_replace(&query, &parameters).expect("replace");
        let after = finalized_count(&replaced);
        assert!(
            after < before,
            "folding should remove nodes: {before} -> {after}"
        );
        let ids = stored_ids(&replaced);
        assert!(ids.iter().all(|(_, position)| *position != u32::MAX));
    }
}
