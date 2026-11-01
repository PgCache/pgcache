//! The cache-side statement for an origin index: `pg_get_indexdef`'s text
//! rebuilt through the rope against the cache table, anonymous, carrying the
//! primary key as INCLUDE columns. The generation stamp is keyed on the PK
//! (ADR-044), so an index-only scan can stamp from the index tuple only when
//! the tuple holds it (PGC-574).

use ecow::EcoString;
use postgres_protocol::escape;

use crate::query::ast::index_tree_parse;
use crate::query::ast::rope::{
    CreateIndex, IndexKey, NodeKind, QueryTree, ScalarKind, Statement, TreeBuilder,
};

/// Access methods that accept INCLUDE columns. The others (hash, gin, brin)
/// can't return index tuples for an index-only scan either.
const INCLUDE_ACCESS_METHODS: [&str; 3] = ["btree", "gist", "spgist"];

/// Falls back to the plain text retarget when the definition doesn't
/// convert, so the index is still created, just without INCLUDE.
pub(super) fn index_sql_retarget(
    definition: &str,
    is_unique: bool,
    schema: &str,
    table: &str,
    primary_key: &[EcoString],
) -> Option<String> {
    index_tree_parse(definition)
        .ok()
        .map(|base| index_tree_retarget(&base, schema, table, primary_key).sql())
        .or_else(|| index_sql_retarget_plain(definition, is_unique, schema, table))
}

fn count(len: usize) -> u32 {
    u32::try_from(len).unwrap_or(u32::MAX)
}

/// `base` with a fresh anonymous root on the cache table and the missing
/// primary-key columns appended to INCLUDE; every key, include, option and
/// the predicate is kept.
fn index_tree_retarget(
    base: &QueryTree,
    schema: &str,
    table: &str,
    primary_key: &[EcoString],
) -> QueryTree {
    let Statement::CreateIndex(index) = base.statement() else {
        debug_assert!(false, "an index definition converts to a CreateIndex root");
        return base.clone();
    };
    let missing: Vec<&EcoString> = if INCLUDE_ACCESS_METHODS.contains(&index.method().as_str()) {
        primary_key
            .iter()
            .filter(|column| !index_carries_column(index, column))
            .collect()
    } else {
        Vec::new()
    };
    let mut builder = TreeBuilder::over(base);
    let open = builder.open(NodeKind::CreateIndex {
        name: None,
        method: index.method().clone(),
        unique: index.unique(),
        only: false,
        nulls_not_distinct: index.nulls_not_distinct(),
        key_count: count(index.keys().len()),
        include_count: count(index.includes().len() + missing.len()),
        option_count: count(index.options().len()),
        has_where: index.predicate().is_some(),
    });
    builder.leaf(NodeKind::Table {
        schema: EcoString::from(schema),
        name: EcoString::from(table),
        alias: EcoString::new(),
        alias_column_count: 0,
    });
    for key in index.keys() {
        builder.keep(key.id());
    }
    for include in index.includes() {
        builder.keep(include.id());
    }
    for column in missing {
        builder.leaf(NodeKind::Name(column.clone()));
    }
    for option in index.options() {
        builder.keep(option.id());
    }
    if let Some(predicate) = index.predicate() {
        builder.keep(predicate.id());
    }
    builder.close(open);
    builder.finish().0
}

/// Whether `column` is already a plain key or an INCLUDE column.
fn index_carries_column(index: CreateIndex<'_>, column: &str) -> bool {
    index
        .keys()
        .any(|key| index_key_column(key).is_some_and(|key| key == column))
        || index.includes().any(|name| name.text() == column)
}

/// The column a plain key names; `None` for an expression.
fn index_key_column(key: IndexKey<'_>) -> Option<&EcoString> {
    if let ScalarKind::Column(column) = key.expr().kind() {
        Some(column.column())
    } else {
        None
    }
}

/// Retarget by text: `pg_get_indexdef` emits `CREATE [UNIQUE] INDEX name ON
/// schema.tbl USING ...`, and everything from `USING` onward references only
/// column names, so an anonymous index against the cache table is the tail
/// behind the cache's own schema/table. Returns `None` without a `USING`.
fn index_sql_retarget_plain(
    definition: &str,
    is_unique: bool,
    schema: &str,
    table: &str,
) -> Option<String> {
    let (_, using) = definition.split_once(" USING ")?;
    let unique = if is_unique { "UNIQUE " } else { "" };
    Some(format!(
        "CREATE {unique}INDEX ON {}.{} USING {using}",
        escape::escape_identifier(schema),
        escape::escape_identifier(table)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pk(columns: &[&str]) -> Vec<EcoString> {
        columns.iter().map(|c| EcoString::from(*c)).collect()
    }

    #[test]
    fn test_index_sql_retarget_partial() {
        let def = "CREATE INDEX posts_owneruserid_idx ON public.posts USING btree (owneruserid) WHERE (owneruserid IS NOT NULL)";
        let sql = index_sql_retarget(def, false, "public", "posts", &pk(&["id"]))
            .expect("retarget partial index");
        assert_eq!(
            sql,
            "CREATE INDEX ON public.posts USING btree (owneruserid) INCLUDE (id) WHERE owneruserid IS NOT NULL"
        );
    }

    #[test]
    fn test_index_sql_retarget_unique_desc() {
        let def = "CREATE UNIQUE INDEX foo_idx ON public.foo USING btree (created_at DESC, name)";
        let sql = index_sql_retarget(def, true, "public", "foo", &pk(&["id"]))
            .expect("retarget unique index");
        assert_eq!(
            sql,
            "CREATE UNIQUE INDEX ON public.foo USING btree (created_at DESC, name) INCLUDE (id)"
        );
    }

    #[test]
    fn test_index_sql_retarget_partition_child_retargeted() {
        // Definition read from a child partition names the child; output must
        // target the parent cache table.
        let def = "CREATE INDEX posts_2023_parentid_idx ON public.posts_2023 USING btree (parentid) WHERE (parentid IS NOT NULL)";
        let sql = index_sql_retarget(def, false, "public", "posts", &pk(&["id"]))
            .expect("retarget child index");
        assert_eq!(
            sql,
            "CREATE INDEX ON public.posts USING btree (parentid) INCLUDE (id) WHERE parentid IS NOT NULL"
        );
    }

    #[test]
    fn test_index_sql_retarget_drops_only_from_a_partitioned_parent() {
        let def = "CREATE INDEX parent_idx ON ONLY public.parent USING btree (a)";
        let sql = index_sql_retarget(def, false, "public", "parent", &pk(&["id"]))
            .expect("retarget parent index");
        assert_eq!(
            sql,
            "CREATE INDEX ON public.parent USING btree (a) INCLUDE (id)"
        );
    }

    #[test]
    fn test_index_sql_retarget_no_using_returns_none() {
        assert!(
            index_sql_retarget("not an index def", false, "public", "posts", &pk(&["id"]))
                .is_none()
        );
    }

    #[test]
    fn test_index_sql_retarget_key_covering_pk_gets_no_include() {
        let def = "CREATE INDEX t_id_idx ON public.t USING btree (id)";
        let sql = index_sql_retarget(def, false, "public", "t", &pk(&["id"])).expect("retarget");
        assert_eq!(sql, "CREATE INDEX ON public.t USING btree (id)");

        let def = "CREATE INDEX t_ba_idx ON public.t USING btree (b, a)";
        let sql =
            index_sql_retarget(def, false, "public", "t", &pk(&["a", "b"])).expect("retarget");
        assert_eq!(sql, "CREATE INDEX ON public.t USING btree (b, a)");
    }

    #[test]
    fn test_index_sql_retarget_includes_only_missing_pk_columns() {
        let def = "CREATE INDEX t_b_idx ON public.t USING btree (b)";
        let sql =
            index_sql_retarget(def, false, "public", "t", &pk(&["a", "b"])).expect("retarget");
        assert_eq!(sql, "CREATE INDEX ON public.t USING btree (b) INCLUDE (a)");
    }

    #[test]
    fn test_index_sql_retarget_extends_existing_include() {
        let def = "CREATE INDEX t_email_idx ON public.t USING btree (email) INCLUDE (name)";
        let sql = index_sql_retarget(def, false, "public", "t", &pk(&["id"])).expect("retarget");
        assert_eq!(
            sql,
            "CREATE INDEX ON public.t USING btree (email) INCLUDE (name, id)"
        );

        let def = "CREATE INDEX t_email_idx ON public.t USING btree (email) INCLUDE (id)";
        let sql = index_sql_retarget(def, false, "public", "t", &pk(&["id"])).expect("retarget");
        assert_eq!(
            sql,
            "CREATE INDEX ON public.t USING btree (email) INCLUDE (id)"
        );
    }

    #[test]
    fn test_index_sql_retarget_expression_key_gets_include() {
        let def = "CREATE INDEX t_lower_idx ON public.t USING btree (lower((name)::text))";
        let sql = index_sql_retarget(def, false, "public", "t", &pk(&["id"])).expect("retarget");
        assert_eq!(
            sql,
            "CREATE INDEX ON public.t USING btree (LOWER((name)::text)) INCLUDE (id)"
        );
    }

    #[test]
    fn test_index_sql_retarget_leaves_hash_index_alone() {
        let def = "CREATE INDEX t_email_hash ON public.t USING hash (email)";
        let sql = index_sql_retarget(def, false, "public", "t", &pk(&["id"])).expect("retarget");
        assert_eq!(sql, "CREATE INDEX ON public.t USING hash (email)");
    }

    #[test]
    fn test_index_sql_retarget_gist_gets_include() {
        let def = "CREATE INDEX t_geom_idx ON public.t USING gist (geom)";
        let sql = index_sql_retarget(def, false, "public", "t", &pk(&["id"])).expect("retarget");
        assert_eq!(
            sql,
            "CREATE INDEX ON public.t USING gist (geom) INCLUDE (id)"
        );
    }

    #[test]
    fn test_index_sql_retarget_keeps_trailing_clauses_after_include() {
        let def = "CREATE UNIQUE INDEX t_a_idx ON public.t USING btree (a) NULLS NOT DISTINCT WITH (fillfactor='90')";
        let sql = index_sql_retarget(def, true, "public", "t", &pk(&["id"])).expect("retarget");
        assert_eq!(
            sql,
            "CREATE UNIQUE INDEX ON public.t USING btree (a) INCLUDE (id) NULLS NOT DISTINCT WITH (fillfactor = '90')"
        );
    }

    #[test]
    fn test_index_sql_retarget_keeps_collation_and_opclass_on_a_key() {
        let def = "CREATE INDEX t_name_idx ON public.t USING btree (name COLLATE \"C\" text_pattern_ops DESC NULLS LAST)";
        let sql = index_sql_retarget(def, false, "public", "t", &pk(&["id"])).expect("retarget");
        assert_eq!(
            sql,
            "CREATE INDEX ON public.t USING btree (name COLLATE \"C\" text_pattern_ops DESC NULLS LAST) INCLUDE (id)"
        );
    }

    #[test]
    fn test_index_sql_retarget_quotes_identifiers() {
        let def = "CREATE INDEX \"Posts_owner_idx\" ON public.\"Posts\" USING btree (\"OwnerId\" DESC NULLS LAST)";
        let sql =
            index_sql_retarget(def, false, "public", "Posts", &pk(&["Id"])).expect("retarget");
        assert_eq!(
            sql,
            "CREATE INDEX ON public.\"Posts\" USING btree (\"OwnerId\" DESC NULLS LAST) INCLUDE (\"Id\")"
        );
    }

    #[test]
    fn test_index_sql_retarget_falls_back_to_text_when_unparsable() {
        let def = "CREATE INDEX t_idx ON public.t USING btree (a) FOO BAR";
        let sql = index_sql_retarget(def, false, "public", "t", &pk(&["id"])).expect("fallback");
        assert_eq!(
            sql,
            "CREATE INDEX ON \"public\".\"t\" USING btree (a) FOO BAR"
        );
    }

    #[test]
    fn test_index_sql_retarget_falls_back_to_text_for_a_shape_the_rope_rejects() {
        let def = "CREATE INDEX t_idx ON public.t USING btree (a) TABLESPACE fast";
        let sql = index_sql_retarget(def, false, "public", "t", &pk(&["id"])).expect("fallback");
        assert_eq!(
            sql,
            "CREATE INDEX ON \"public\".\"t\" USING btree (a) TABLESPACE fast"
        );
    }

    /// The rewrite shares every kept subtree with the parsed definition.
    #[test]
    fn test_index_tree_retarget_keeps_the_keys_and_predicate() {
        let base = index_tree_parse(
            "CREATE INDEX t_idx ON public.t USING btree (lower(a), b) WHERE a IS NOT NULL",
        )
        .expect("parse");
        let rewritten = index_tree_retarget(&base, "public", "t", &pk(&["id"]));
        // root', table' | keys and predicate kept | Name(id) in between
        assert!(rewritten.chunk_count() > 1, "kept subtrees share the base");
        assert_eq!(
            rewritten.sql(),
            "CREATE INDEX ON public.t USING btree (LOWER(a), b) INCLUDE (id) WHERE a IS NOT NULL"
        );
    }
}
