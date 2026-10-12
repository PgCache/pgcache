#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::mem::size_of;

use super::build::{Remap, Replacement, Replacements, TreeBuilder};
use super::node::{Node, NodeKind};
use super::owned::query_expr_from_tree;
use super::tree::QueryTree;
use super::view::{Body, View};
use crate::query::ast::{Deparse, LiteralValue, NodeId, QueryExpr, query_expr_parse};

/// One query per node kind and child layout the converter produces.
pub(crate) const CORPUS: &[&str] = &[
    "SELECT a FROM t WHERE b = 1",
    "SELECT DISTINCT t.a AS x, *, u.* FROM t, u",
    "SELECT * FROM public.t AS t1 (c1, c2)",
    "SELECT a FROM t JOIN u ON t.id = u.id LEFT JOIN v USING (k) NATURAL JOIN w CROSS JOIN x",
    "SELECT a FROM t RIGHT JOIN u ON t.id = u.id FULL JOIN v ON u.id = v.id",
    "SELECT a FROM (SELECT b AS a FROM t) AS s (a)",
    "SELECT a FROM t, LATERAL (SELECT t.a) AS l",
    "WITH c (x) AS MATERIALIZED (SELECT 1), d AS NOT MATERIALIZED (SELECT 2) \
     SELECT x FROM c AS cc (y), d",
    "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS v (n, s)",
    "SELECT a FROM t UNION ALL SELECT a FROM u EXCEPT SELECT a FROM v INTERSECT SELECT a FROM w",
    "SELECT a FROM t ORDER BY a DESC NULLS FIRST, b ASC NULLS LAST LIMIT 10 OFFSET 5",
    "SELECT a FROM t LIMIT 3",
    "SELECT a FROM t OFFSET 4",
    "SELECT count(*), count(DISTINCT a), string_agg(b, ',' ORDER BY b) FILTER (WHERE c > 0), \
     pg_catalog.now() FROM t GROUP BY a, t.b HAVING count(*) > 1",
    "SELECT sum(a) OVER (PARTITION BY b, c ORDER BY d ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING \
     EXCLUDE TIES), rank() OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), \
     dense_rank() OVER (GROUPS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING EXCLUDE GROUP) FROM t",
    "SELECT a FROM t WHERE NOT (a IS NULL) AND b IS NOT NULL OR c IS TRUE AND d IS NOT FALSE",
    "SELECT a FROM t WHERE a IN (1, 2, 3) AND b NOT IN (4) AND c BETWEEN 1 AND 10 \
     AND d NOT BETWEEN SYMMETRIC 1 AND 2",
    "SELECT a FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.id = t.id) AND a IN (SELECT b FROM v) \
     AND a <> ALL (SELECT c FROM w) AND NOT EXISTS (SELECT 1 FROM x) AND a = (SELECT max(d) FROM y)",
    "SELECT (a + b) * 2 - c / 3 % 4, a::text, b::int4, c::date, d::numeric(10, 2), e::bool, \
     CASE a WHEN 1 THEN 'x' WHEN 2 THEN 'z' ELSE 'y' END, \
     CASE WHEN b > 1 THEN c END FROM t",
    "SELECT a FROM t WHERE b LIKE 'x%' AND c ILIKE 'y' AND d NOT LIKE 'z' AND e NOT ILIKE 'w'",
    "SELECT a FROM t WHERE b = $1 AND c = 'x' AND d = 1.5 AND e = true AND f IS NULL \
     AND g = NULL::int4 AND h = '{1,2}'::int4[] AND i = 'x'::date",
    "SELECT a FROM t WHERE a = 1 AND (b = 2 OR c = 3) AND d <> 4 AND e <= 5 AND f >= 6",
    "SELECT a FROM t WHERE a = ANY (ARRAY[1, 2]) AND b <> ALL (ARRAY[3, 4])",
];

fn parse(sql: &str) -> QueryExpr {
    query_expr_parse(sql).expect("parse query")
}

fn tree(sql: &str) -> QueryTree {
    QueryTree::from_query_expr(&parse(sql))
}

fn sql(query: &QueryExpr) -> String {
    let mut buf = String::new();
    query.deparse(&mut buf);
    buf
}

fn session_hash(tree: &QueryTree) -> u64 {
    let mut hasher = DefaultHasher::new();
    tree.session_hash(&mut hasher);
    hasher.finish()
}

fn full_hash(tree: &QueryTree) -> u64 {
    let mut hasher = DefaultHasher::new();
    tree.hash(&mut hasher);
    hasher.finish()
}

fn lens(tree: &QueryTree) -> Vec<u32> {
    tree.nodes().map(|node| node.len).collect()
}

/// `base` with the literal at each target position replaced: a copy of the
/// root with those positions as fresh leaves bound to nothing.
fn rewrite(base: &QueryTree, targets: &[(u32, LiteralValue)]) -> (QueryTree, Remap) {
    let mut builder = TreeBuilder::over(base);
    let mut replacements = Replacements::new();
    for (target, value) in targets {
        replacements.push(
            NodeId::from_index(*target),
            Replacement::Leaf(NodeKind::Literal(value.clone())),
        );
    }
    builder.copy(base.root().node_ref(), &mut replacements);
    builder.finish()
}

#[test]
fn test_node_size_stays_within_budget() {
    assert!(
        size_of::<Node>() <= 72,
        "Node is {} bytes (EcoString {}, Option<EcoString> {}, LiteralValue {}, CastTarget {}, \
         SubqueryKind {}, MultiOp {}, JoinQualKind {}, CteMaterialization {})",
        size_of::<Node>(),
        size_of::<ecow::EcoString>(),
        size_of::<Option<ecow::EcoString>>(),
        size_of::<LiteralValue>(),
        size_of::<crate::query::cast::CastTarget>(),
        size_of::<crate::cache::SubqueryKind>(),
        size_of::<crate::query::ast::MultiOp>(),
        size_of::<super::node::JoinQualKind>(),
        size_of::<crate::query::ast::CteMaterialization>(),
    );
    assert!(
        size_of::<NodeKind>() <= 64,
        "NodeKind is {} bytes",
        size_of::<NodeKind>()
    );
}

#[test]
fn test_round_trip_rebuilds_the_owned_tree() {
    for text in CORPUS {
        let owned = parse(text);
        let tree = QueryTree::from_query_expr(&owned);
        assert_eq!(tree.chunk_count(), 1, "{text}");
        assert_eq!(
            lens(&tree)[0],
            tree.len(),
            "{text}: the root spans the tree"
        );
        let back = query_expr_from_tree(&tree);
        assert_eq!(back, owned, "{text}");
        assert_eq!(sql(&back), sql(&owned), "{text}");
    }
}

#[test]
fn test_plain_rendering_matches_the_owned_deparse() {
    for text in CORPUS {
        let owned = parse(text);
        let rope = QueryTree::from_query_expr(&owned);
        assert_eq!(rope.sql(), sql(&owned), "{text}");
    }
}

#[test]
fn test_positions_follow_pre_order_lengths() {
    let tree = tree("SELECT a FROM t WHERE b = 1");
    assert_eq!(lens(&tree), vec![8, 7, 2, 1, 1, 3, 1, 1]);
    let select = match tree.root().body() {
        Body::Select(select) => select,
        Body::Values(_) | Body::SetOp(_) => panic!("a select"),
    };
    assert_eq!(select.id().index(), 1);
    let where_clause = select.where_clause().expect("where");
    assert_eq!(where_clause.id().index(), 5);
    let from: Vec<_> = select.from().map(|source| source.id().index()).collect();
    assert_eq!(from, vec![4]);
}

#[test]
fn test_views_read_limit_literals_from_the_values() {
    let limited = tree("SELECT a FROM t LIMIT 10 OFFSET 5");
    let root = limited.root();
    assert!(root.has_limit());
    assert_eq!(
        root.limit_count().map(|l| l.value().clone()),
        Some(LiteralValue::Integer(10))
    );
    assert_eq!(
        root.limit_offset().map(|l| l.value().clone()),
        Some(LiteralValue::Integer(5))
    );
    assert!(tree("SELECT a FROM t").root().limit_count().is_none());
}

#[test]
fn test_same_shape_rewrite_shares_every_other_node() {
    let base = tree("SELECT a FROM t WHERE b = 1");
    let (rewritten, remap) = rewrite(&base, &[(7, LiteralValue::Integer(2))]);
    assert_eq!(rewritten, tree("SELECT a FROM t WHERE b = 2"));
    // Query', Select' | SelectExpr, Column a, Table t | Binary' | Column b | Literal 2
    assert_eq!(rewritten.chunk_count(), 5);
    assert_eq!(
        remap.new_id(NodeId::from_index(2)).map(NodeId::index),
        Some(2)
    );
    assert_eq!(
        remap.new_id(NodeId::from_index(4)).map(NodeId::index),
        Some(4)
    );
    assert_eq!(
        remap.new_id(NodeId::from_index(6)).map(NodeId::index),
        Some(6)
    );
    // Re-emitted ancestors stand in for themselves; a `Leaf` replacement
    // stands in for nothing.
    for ancestor in [0, 1, 5] {
        assert_eq!(
            remap
                .new_id(NodeId::from_index(ancestor))
                .map(NodeId::index),
            Some(ancestor as usize)
        );
    }
    assert!(remap.new_id(NodeId::from_index(7)).is_none());
    assert_eq!(rewritten.compacted(), rewritten);
    assert_eq!(
        session_hash(&rewritten.compacted()),
        session_hash(&rewritten)
    );
}

#[test]
fn test_shape_change_shifts_the_kept_subtrees() {
    let base = tree("SELECT a, a + 2 FROM t WHERE b = 1");
    assert_eq!(base.len(), 12);
    let (rewritten, remap) = rewrite(&base, &[(5, LiteralValue::Integer(3))]);
    assert_eq!(rewritten, tree("SELECT a, 3 FROM t WHERE b = 1"));
    assert_eq!(rewritten.len(), 10);
    assert_eq!(
        remap.new_id(NodeId::from_index(8)).map(NodeId::index),
        Some(6)
    );
    assert_eq!(
        remap.new_id(NodeId::from_index(10)).map(NodeId::index),
        Some(8)
    );
    assert_eq!(
        remap.new_id(NodeId::from_index(11)).map(NodeId::index),
        Some(9)
    );
    assert!(remap.new_id(NodeId::from_index(6)).is_none());
    let positions: Vec<usize> = remap
        .new_ids(NodeId::from_index(9))
        .map(NodeId::index)
        .collect();
    assert_eq!(positions, vec![7]);
}

#[test]
fn test_a_subtree_kept_twice_reports_both_positions() {
    let base = tree("SELECT a FROM t WHERE b = 1");
    let mut builder = TreeBuilder::over(&base);
    let open = builder.open(NodeKind::Array { count: 2 });
    let first = builder.keep(NodeId::from_index(7));
    let second = builder.keep(NodeId::from_index(7));
    builder.close(open);
    let (tree, remap) = builder.finish();
    assert_eq!(lens(&tree), vec![3, 1, 1]);
    assert_eq!(first.index(), 1);
    assert_eq!(second.index(), 2);
    let positions: Vec<usize> = remap
        .new_ids(NodeId::from_index(7))
        .map(NodeId::index)
        .collect();
    assert_eq!(positions, vec![1, 2]);
    assert!(remap.new_id(NodeId::from_index(6)).is_none());
}

#[test]
fn test_fragmented_tables_compact_past_the_threshold() {
    let literals: Vec<String> = (1..=40).map(|n| n.to_string()).collect();
    let base = tree(&format!(
        "SELECT a FROM t WHERE b IN ({})",
        literals.join(", ")
    ));
    assert_eq!(base.len(), 47);
    // Every other literal replaced in one pass: forty-one chunks, past the
    // floor of sixteen, so the table compacts to one.
    let targets: Vec<(u32, LiteralValue)> = (0..20)
        .map(|k| (7 + 2 * k, LiteralValue::Integer(100 + i64::from(k))))
        .collect();
    let (rewritten, _) = rewrite(&base, &targets);
    assert_eq!(rewritten.chunk_count(), 1);
    let expected: Vec<String> = (0..40)
        .map(|k| {
            if k % 2 == 0 {
                (100 + k / 2).to_string()
            } else {
                (k + 1).to_string()
            }
        })
        .collect();
    assert_eq!(
        rewritten,
        tree(&format!(
            "SELECT a FROM t WHERE b IN ({})",
            expected.join(", ")
        ))
    );
    // Two literals replaced: eight chunks, under the floor, so sharing stays
    // (the thirty-seven literals kept after the second one are one entry).
    let (lightly, _) = rewrite(&base, &targets[..2]);
    assert_eq!(lightly.chunk_count(), 8);
}

#[test]
fn test_session_hash_ignores_the_root_limit_only() {
    let bare = tree("SELECT a FROM t");
    assert_eq!(session_hash(&bare), session_hash(&tree("SELECT a FROM t")));
    assert_eq!(
        session_hash(&bare),
        session_hash(&tree("SELECT a FROM t LIMIT 5"))
    );
    assert_eq!(
        session_hash(&bare),
        session_hash(&tree("SELECT a FROM t OFFSET 2"))
    );
    assert_eq!(
        session_hash(&bare),
        session_hash(&tree("SELECT a FROM t LIMIT 5 OFFSET 2"))
    );
    assert_ne!(session_hash(&bare), session_hash(&tree("SELECT a FROM u")));
    let inner = tree("SELECT a FROM t WHERE b IN (SELECT c FROM u)");
    let inner_limited = tree("SELECT a FROM t WHERE b IN (SELECT c FROM u LIMIT 1)");
    assert_ne!(session_hash(&inner), session_hash(&inner_limited));
    assert_ne!(
        full_hash(&bare),
        full_hash(&tree("SELECT a FROM t LIMIT 5"))
    );
}

#[test]
fn test_a_tree_is_equal_to_its_compacted_copy() {
    for text in CORPUS {
        let base = tree(text);
        let target = base
            .nodes()
            .enumerate()
            .find(|(_, node)| matches!(node.kind, NodeKind::Literal(_)))
            .map(|(index, _)| u32::try_from(index).unwrap());
        let Some(target) = target else {
            continue;
        };
        let (rewritten, _) = rewrite(&base, &[(target, LiteralValue::Integer(9))]);
        let compacted = rewritten.compacted();
        assert_eq!(compacted.chunk_count(), 1, "{text}");
        assert_eq!(compacted, rewritten, "{text}");
        assert_eq!(full_hash(&compacted), full_hash(&rewritten), "{text}");
        assert_eq!(
            sql(&query_expr_from_tree(&compacted)),
            sql(&query_expr_from_tree(&rewritten)),
            "{text}"
        );
    }
}

#[test]
fn test_clone_shares_the_table() {
    let base = tree("SELECT a FROM t WHERE b = 1");
    let copy = base.clone();
    assert_eq!(copy, base);
    assert_eq!(copy.chunk_count(), 1);
    assert!(std::ptr::eq(
        copy.node(NodeId::from_index(3)),
        base.node(NodeId::from_index(3))
    ));
}

/// A kind that declares more children than are emitted under it is caught
/// at `close`, where the mistake was made, rather than misread by a view.
#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "a node's kind declares the children emitted under it")]
fn test_close_checks_the_declared_children() {
    let mut builder = TreeBuilder::new();
    let open = builder.open(NodeKind::Array { count: 2 });
    builder.leaf(NodeKind::Literal(LiteralValue::Integer(1)));
    builder.close(open);
}
