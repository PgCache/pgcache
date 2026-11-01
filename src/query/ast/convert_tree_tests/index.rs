//! CREATE INDEX: what each clause converts to, and what is rejected.

use super::*;
use crate::query::ast::rope::{CreateIndex, IndexKey, Statement};
use crate::query::ast::{NullOrder, index_tree_parse};

fn parse_index(sql: &str) -> QueryTree {
    index_tree_parse(sql).unwrap_or_else(|e| panic!("{sql}: {e}"))
}

fn index_of(tree: &QueryTree) -> CreateIndex<'_> {
    match tree.statement() {
        Statement::CreateIndex(index) => index,
        Statement::Query(_) => panic!("expected a CREATE INDEX root"),
    }
}

fn key_column(key: IndexKey<'_>) -> &EcoString {
    match key.expr().kind() {
        ScalarKind::Column(column) => column.column(),
        other => panic!(
            "expected a column key, got {:?}",
            std::mem::discriminant(&other)
        ),
    }
}

fn names(run: crate::query::ast::rope::Run<'_, crate::query::ast::rope::Name<'_>>) -> Vec<String> {
    run.map(|name| name.text().to_string()).collect()
}

#[test]
fn test_index_root_carries_the_statement_flags_and_table() {
    let tree = parse_index(
        "CREATE UNIQUE INDEX t_a_idx ON ONLY public.t USING btree (a) NULLS NOT DISTINCT",
    );
    let index = index_of(&tree);
    assert_eq!(index.name().map(EcoString::as_str), Some("t_a_idx"));
    assert_eq!(index.method(), "btree");
    assert!(index.unique());
    assert!(index.only());
    assert!(index.nulls_not_distinct());
    assert_eq!(
        index.table().schema().map(EcoString::as_str),
        Some("public")
    );
    assert_eq!(index.table().name(), "t");
    assert!(index.table().alias().is_none());
    assert_eq!(index.keys().len(), 1);
    assert_eq!(index.includes().len(), 0);
    assert_eq!(index.options().len(), 0);
    assert!(index.predicate().is_none());

    let plain = index_of_tree("CREATE INDEX ON t USING hash (a)");
    assert!(plain.name().is_none());
    assert!(!plain.unique());
    assert!(!plain.only());
    assert!(plain.table().schema().is_none());
}

fn index_of_tree(sql: &str) -> CreateIndex<'static> {
    // Leak the tree so the view can be returned; test-only convenience.
    let tree: &'static QueryTree = Box::leak(Box::new(parse_index(sql)));
    index_of(tree)
}

#[test]
fn test_index_keys_carry_ordering_collation_and_opclass() {
    let tree = parse_index(
        "CREATE INDEX i ON t USING btree (a DESC NULLS LAST, b COLLATE \"C\" text_pattern_ops, \
         c pg_catalog.int4_ops (opt = 'v'), lower(d))",
    );
    let keys: Vec<IndexKey<'_>> = index_of(&tree).keys().collect();
    assert_eq!(keys.len(), 4);

    assert_eq!(key_column(keys[0]), "a");
    assert_eq!(keys[0].direction(), OrderDirection::Desc);
    assert_eq!(keys[0].null_order(), NullOrder::NullsLast);
    assert_eq!(keys[0].collation().len(), 0);
    assert_eq!(keys[0].opclass().len(), 0);

    assert_eq!(key_column(keys[1]), "b");
    assert_eq!(keys[1].direction(), OrderDirection::Asc);
    assert_eq!(keys[1].null_order(), NullOrder::Default);
    assert_eq!(names(keys[1].collation()), vec!["C"]);
    assert_eq!(names(keys[1].opclass()), vec!["text_pattern_ops"]);

    assert_eq!(names(keys[2].opclass()), vec!["pg_catalog", "int4_ops"]);
    let options: Vec<_> = keys[2].opclass_options().collect();
    assert_eq!(options.len(), 1);
    assert_eq!(options[0].name(), "opt");
    assert_eq!(
        options[0].value().map(|v| v.value().clone()),
        Some(LiteralValue::String("v".into()))
    );

    assert!(matches!(keys[3].expr().kind(), ScalarKind::Function(_)));
}

#[test]
fn test_index_includes_options_and_predicate_read_through_the_views() {
    let tree = parse_index(
        "CREATE INDEX i ON t USING btree (a) INCLUDE (b, c) \
         WITH (fillfactor = 90, deduplicate_items = off) WHERE a > 1 AND b IS NOT NULL",
    );
    let index = index_of(&tree);
    assert_eq!(names(index.includes()), vec!["b", "c"]);
    let options: Vec<(String, Option<LiteralValue>)> = index
        .options()
        .map(|option| {
            (
                option.name().to_string(),
                option.value().map(|v| v.value().clone()),
            )
        })
        .collect();
    assert_eq!(
        options,
        vec![
            ("fillfactor".to_owned(), Some(LiteralValue::Integer(90))),
            (
                "deduplicate_items".to_owned(),
                Some(LiteralValue::String("off".into()))
            ),
        ]
    );
    let predicate = index.predicate().expect("predicate");
    assert_eq!(predicate.conjuncts().len(), 2);
}

#[test]
fn test_index_shapes_the_cache_cannot_use_are_rejected() {
    let rejected = [
        "CREATE INDEX CONCURRENTLY i ON t USING btree (a)",
        "CREATE INDEX IF NOT EXISTS i ON t USING btree (a)",
        "CREATE INDEX i ON t USING btree (a) TABLESPACE fast",
        "CREATE INDEX i ON t USING btree (a) INCLUDE (lower(b))",
        "CREATE INDEX i ON t USING btree (a) WITH (toast.fillfactor = 90)",
        "SELECT a FROM t",
        "CREATE TABLE t (a int)",
    ];
    for sql in rejected {
        assert!(index_tree_parse(sql).is_err(), "{sql}");
    }
}

#[test]
fn test_query_entry_still_rejects_an_index_statement() {
    assert!(query_tree_parse("CREATE INDEX i ON t USING btree (a)").is_err());
}
