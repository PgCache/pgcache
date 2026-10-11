//! CDC membership SQL: the query with the changed relation's FROM entry
//! standing in for a `VALUES` row (or rows, or `unnest` parameters), so the
//! cache DB answers whether the row belongs to the query's result. The entry
//! and every reference bound to it render by a [`SourceOverride`] on the
//! bound query; nothing is cloned or rewritten per row.

use std::fmt::Write;

use ecow::EcoString;
use rootcause::Report;

use super::{AstTransformError, AstTransformResult};
use crate::catalog::TableMetadata;
use crate::pg::protocol::ByteString;
use crate::query::ast::{
    Clause, Deparse, DeparseContext, LiteralValue, QueryBody, SelectNode, clause_deparse_with,
    emit_escaped_string_literal, separated_deparse_with,
};
use crate::query::resolve::{ResolvedQuery, SlotId, SourceOverride, TableSlot};

/// Synthetic leading column carrying each batched row's ordinal through the
/// membership query (PGC-241). Prefixed to avoid colliding with real columns.
pub const BATCH_IDX_COLUMN: &str = "__pgc_idx";

/// A FROM occurrence of the relation being replaced: its slot is its
/// identity, so a self-join's arms never clobber each other (PGC-256), and
/// its qualifier (explicit alias, else the table name) names the stand-in.
#[derive(Debug)]
pub struct TableOccurrence {
    slot: SlotId,
    alias: EcoString,
}

/// Every top-level FROM occurrence of `table_metadata`'s relation. A
/// self-join yields one per arm.
pub fn table_occurrences_find(
    bound: &ResolvedQuery,
    table_metadata: &TableMetadata,
) -> AstTransformResult<Vec<TableOccurrence>> {
    let bindings = bound.bindings();
    let Some(select) = top_select(bound) else {
        return Err(Report::from(AstTransformError::MissingTable));
    };
    let mut occurrences = Vec::new();
    for table in select.direct_table_nodes() {
        let Some(slot) = bindings.source(table.id) else {
            continue;
        };
        if let Some(TableSlot::Base {
            table: metadata, ..
        }) = bindings.slot(slot)
            && metadata.relation_oid == table_metadata.relation_oid
        {
            occurrences.push(TableOccurrence {
                slot,
                alias: EcoString::from(bindings.slot(slot).map_or("", |s| s.qualifier())),
            });
        }
    }
    if occurrences.is_empty() {
        return Err(Report::from(AstTransformError::MissingTable));
    }
    Ok(occurrences)
}

/// The relation's sole FROM occurrence. Errors when it appears more than once —
/// the callers that use this (the batch/unnest forms and the prepared
/// `PgEvalTemplate`) all substitute a single occurrence, which is only a
/// complete membership test when there *is* only one (PGC-256).
fn table_occurrence_single(
    bound: &ResolvedQuery,
    table_metadata: &TableMetadata,
) -> AstTransformResult<TableOccurrence> {
    let mut occurrences = table_occurrences_find(bound, table_metadata)?;
    if occurrences.len() > 1 {
        return Err(AstTransformError::MultipleOccurrences {
            table: EcoString::from(table_metadata.name.as_str()),
        }
        .into());
    }
    occurrences
        .pop()
        .ok_or_else(|| Report::from(AstTransformError::MissingTable))
}

/// The query's top-level SELECT; a membership query is never a set operation.
fn top_select(bound: &ResolvedQuery) -> Option<&SelectNode> {
    match &bound.tree().body {
        QueryBody::Select(select) => Some(select),
        QueryBody::SetOp(_) | QueryBody::Values(_) => None,
    }
}

/// One VALUES row's literals for `row_data`, in `table_metadata` column
/// order, with per-column casts (NULLs carry the column type). Emits one
/// literal per column position present in `row_data`;
/// [`values_column_names`] yields the matching alias columns.
fn values_row_build(
    table_metadata: &TableMetadata,
    row_data: &[Option<ByteString>],
) -> Vec<LiteralValue> {
    let mut values = Vec::new();
    for column_meta in &table_metadata.columns {
        let position = column_meta.index();
        if let Some(row_value) = row_data.get(position) {
            let value = row_value.as_deref().map_or_else(
                || LiteralValue::NullWithCast(column_meta.type_name.clone()),
                |v| LiteralValue::StringWithCast(v.into(), column_meta.type_name.clone()),
            );
            values.push(value);
        }
    }
    values
}

/// The alias column names matching [`values_row_build`]'s emitted literals:
/// one per column position present in `row_data`, in column order.
fn values_column_names(
    table_metadata: &TableMetadata,
    row_data: &[Option<ByteString>],
) -> Vec<EcoString> {
    table_metadata
        .columns
        .iter()
        .filter(|column_meta| row_data.get(column_meta.index()).is_some())
        .map(|column_meta| EcoString::from(column_meta.name.as_str()))
        .collect()
}

/// `VALUES (…), (…)` over `rows`.
fn values_source(rows: &[Vec<LiteralValue>]) -> String {
    let mut source = String::with_capacity(64);
    source.push_str("VALUES ");
    let mut row_sep = "";
    for row in rows {
        source.push_str(row_sep);
        source.push('(');
        let mut sep = "";
        for value in row {
            source.push_str(sep);
            value.deparse(&mut source);
            sep = ", ";
        }
        source.push(')');
        row_sep = ", ";
    }
    source
}

/// `EXISTS (…)` per FROM occurrence of the relation, OR'd: a row belongs to
/// the result if it can stand in for **any** occurrence, so testing one arm
/// of a self-join under-approximates membership (PGC-256).
pub fn membership_values_all_into(
    buf: &mut String,
    bound: &ResolvedQuery,
    table_metadata: &TableMetadata,
    row_data: &[Option<ByteString>],
) -> AstTransformResult<()> {
    let mut sep = "";
    for occurrence in table_occurrences_find(bound, table_metadata)? {
        buf.push_str(sep);
        buf.push_str("EXISTS (");
        membership_values_into(buf, bound, table_metadata, row_data, &occurrence);
        buf.push(')');
        sep = " OR ";
    }
    Ok(())
}

/// The query with `occurrence` standing in for the single-row `VALUES` of
/// `row_data`, aliased as that occurrence.
pub fn membership_values_into(
    buf: &mut String,
    bound: &ResolvedQuery,
    table_metadata: &TableMetadata,
    row_data: &[Option<ByteString>],
    occurrence: &TableOccurrence,
) {
    let source = values_source(&[values_row_build(table_metadata, row_data)]);
    let columns = values_column_names(table_metadata, row_data);
    bound.sql_with_source_into(
        &SourceOverride {
            slot: occurrence.slot,
            alias: &occurrence.alias,
            source: &source,
            columns: &columns,
        },
        buf,
    );
}

/// Marker literal rendered into the template's `VALUES` row so the SQL can
/// be split into the fixed prefix/suffix around the per-row literals. Chosen
/// to be unique and escape-free, so it renders as a plain `'…'` literal.
const PG_EVAL_SENTINEL: &str = "__pgc_values_sentinel__";

/// A PgEval membership query rendered once with the CDC'd relation already
/// standing in as a `VALUES` row, split so only the per-row literals remain
/// to render (PGC-343). The hot path writes `prefix` + the row's literals +
/// `suffix` into the reused eval buffer.
#[derive(Debug, Clone)]
pub struct PgEvalTemplate {
    prefix: EcoString,
    suffix: EcoString,
    /// `(row_data index, type_name)` per column, in the `VALUES` order the
    /// template's alias expects — the full column set, so this only applies to
    /// full CDC rows (`render_into` refuses short rows).
    columns: Vec<(usize, EcoString)>,
}

impl PgEvalTemplate {
    /// Build the template for one PgEval query over `table_metadata`, or `None`
    /// if the relation does not occur exactly once or the sentinel split is
    /// ambiguous — the caller then falls back to the per-row render.
    pub fn build(bound: &ResolvedQuery, table_metadata: &TableMetadata) -> Option<Self> {
        let occurrence = table_occurrence_single(bound, table_metadata).ok()?;
        let columns: Vec<EcoString> = table_metadata
            .columns
            .iter()
            .map(|c| EcoString::from(c.name.as_str()))
            .collect();
        let source = values_source(&[vec![LiteralValue::String(EcoString::from(
            PG_EVAL_SENTINEL,
        ))]]);
        let mut sql = String::new();
        bound.sql_with_source_into(
            &SourceOverride {
                slot: occurrence.slot,
                alias: &occurrence.alias,
                source: &source,
                columns: &columns,
            },
            &mut sql,
        );

        let mut needle = String::new();
        emit_escaped_string_literal(PG_EVAL_SENTINEL, &mut needle);
        if sql.matches(needle.as_str()).count() != 1 {
            return None;
        }
        let (prefix, suffix) = sql.split_once(needle.as_str())?;
        Some(Self {
            prefix: EcoString::from(prefix),
            suffix: EcoString::from(suffix),
            columns: table_metadata
                .columns
                .iter()
                .map(|c| (c.index(), EcoString::from(c.type_name.as_str())))
                .collect(),
        })
    }

    /// Render this query's membership SELECT for `row_data` into `buf`,
    /// matching [`membership_values_into`] byte for byte. Returns `false`
    /// without touching `buf` when `row_data` doesn't cover every column (a
    /// short/partial CDC row), so the caller falls back.
    pub fn render_into(&self, buf: &mut String, row_data: &[Option<ByteString>]) -> bool {
        // Every column must be present before any write, so a short row falls
        // back cleanly (no partial output left in `buf`).
        if self
            .columns
            .iter()
            .any(|(idx, _)| row_data.get(*idx).is_none())
        {
            return false;
        }
        buf.push_str(&self.prefix);
        let mut sep = "";
        for (idx, type_name) in &self.columns {
            buf.push_str(sep);
            // Mirror `values_row_build`'s `StringWithCast` / `NullWithCast`
            // rendering. `Some(_)` is guaranteed by the guard above; a missing
            // slot renders `NULL` rather than panicking.
            match row_data.get(*idx).and_then(Option::as_ref) {
                Some(value) => {
                    emit_escaped_string_literal(value.as_str(), buf);
                    buf.push_str("::");
                    buf.push_str(type_name);
                }
                None => {
                    buf.push_str("NULL::");
                    buf.push_str(type_name);
                }
            }
            sep = ", ";
        }
        buf.push_str(&self.suffix);
        true
    }
}

/// `SELECT <projection>` over the top-level SELECT's FROM, WHERE, GROUP BY
/// and HAVING with `occurrence` standing in for `source`.
fn membership_select_into(
    buf: &mut String,
    bound: &ResolvedQuery,
    occurrence: &TableOccurrence,
    source: &str,
    columns: &[EcoString],
    projection: &str,
) -> AstTransformResult<()> {
    let select = top_select(bound).ok_or_else(|| Report::from(AstTransformError::MissingTable))?;
    let override_ = SourceOverride {
        slot: occurrence.slot,
        alias: &occurrence.alias,
        source,
        columns,
    };
    let mut cx = bound.deparse_context_with_source(&override_);
    buf.push_str("SELECT ");
    buf.push_str(projection);
    if !select.from.is_empty() {
        cx.clause_enter(Clause::From);
        buf.push_str(" FROM");
        separated_deparse_with(&mut cx, buf, &select.from, ",");
        cx.clause_leave(Clause::From);
    }
    clause_deparse_with(
        &mut cx,
        buf,
        Clause::Where,
        " WHERE ",
        select.where_clause.as_ref(),
    );
    if !select.group_by.is_empty() {
        cx.clause_enter(Clause::GroupBy);
        buf.push_str(" GROUP BY ");
        separated_deparse_with(&mut cx, buf, &select.group_by, ", ");
        cx.clause_leave(Clause::GroupBy);
    }
    clause_deparse_with(
        &mut cx,
        buf,
        Clause::Having,
        " HAVING ",
        select.having.as_ref(),
    );
    Ok(())
}

/// Multi-row membership for batched evaluation (PGC-241): the relation's sole
/// occurrence becomes a K-row `VALUES`, each row prefixed with its ordinal in
/// a synthetic [`BATCH_IDX_COLUMN`], projected as `<query ordinal>,
/// <BATCH_IDX_COLUMN>` so a `UNION ALL` of per-query arms returns the
/// matching `(query ordinal, row index)` pairs in one statement.
///
/// Only valid for queries whose membership is per-row independent
/// (`UpdateQuery::pg_batchable`): each VALUES row joins/filters independently,
/// so projecting the surviving indexes is exactly the per-row `EXISTS` answer.
pub fn membership_batch_into(
    buf: &mut String,
    bound: &ResolvedQuery,
    table_metadata: &TableMetadata,
    rows: &[&[Option<ByteString>]],
    query_ordinal: i64,
) -> AstTransformResult<()> {
    let occurrence = table_occurrence_single(bound, table_metadata)?;
    // Batch rows are uniform full-width (the caller filters odd-arity rows
    // out), so the alias columns are the relation's full column list.
    let mut columns: Vec<EcoString> = Vec::with_capacity(table_metadata.columns.len() + 1);
    columns.push(EcoString::from(BATCH_IDX_COLUMN));
    columns.extend(
        table_metadata
            .columns
            .iter()
            .map(|column_meta| EcoString::from(column_meta.name.as_str())),
    );
    let mut value_rows = Vec::with_capacity(rows.len());
    for (idx, row_data) in rows.iter().enumerate() {
        // Row index is bounded by the caller's row-chunk size: never wraps.
        #[allow(clippy::cast_possible_wrap)]
        let mut values = vec![LiteralValue::Integer(idx as i64)];
        values.extend(values_row_build(table_metadata, row_data));
        value_rows.push(values);
    }
    let source = values_source(&value_rows);
    let projection = format!("{query_ordinal}, {BATCH_IDX_COLUMN}");
    membership_select_into(buf, bound, &occurrence, &source, &columns, &projection)
}

/// Prepared-statement membership (PGC-241 stage 4): the relation's sole
/// occurrence becomes a from-less subselect of `unnest()` calls over array
/// parameters, so the SQL shape is fixed regardless of row count and the
/// statement can be prepared once per query and reused across frames:
///
/// ```sql
/// (SELECT unnest($1::int4[]) AS __pgc_idx,
///         unnest($2::text[])::int4 AS id, …) AS <alias>
/// ```
///
/// `$1` carries the row ordinals; `$2..` one `text[]` per column in
/// `table_metadata.columns` order (the binder must build arrays in the same
/// order), with an element cast to the column type — the text→type conversion
/// happens server-side exactly as the inlined literals' casts do. Multiple
/// set-returning `unnest`s in a select list expand in lockstep for equal-length
/// arrays. The projection is the surviving row ordinals.
pub fn membership_unnest_into(
    buf: &mut String,
    bound: &ResolvedQuery,
    table_metadata: &TableMetadata,
) -> AstTransformResult<()> {
    let occurrence = table_occurrence_single(bound, table_metadata)?;
    let mut source = String::with_capacity(64 + table_metadata.columns.len() * 32);
    let _ = write!(source, "SELECT unnest($1::int4[]) AS {BATCH_IDX_COLUMN}");
    for (i, column_meta) in table_metadata.columns.iter().enumerate() {
        let _ = write!(
            source,
            ", unnest(${}::text[])::{} AS ",
            i + 2,
            column_meta.type_name
        );
        column_meta.name.deparse(&mut source);
    }
    // Column names flow from the inner select's aliases; a bare table alias
    // keeps references like `<alias>.<col>` resolving.
    membership_select_into(buf, bound, &occurrence, &source, &[], BATCH_IDX_COLUMN)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use iddqd::BiHashMap;
    use postgres_types::Type;

    use super::*;
    use crate::catalog::{Catalog, ColumnMetadata, ColumnPosition, ColumnStore};
    use crate::oid::{Oid, TypeOid};
    use crate::query::ast::query_expr_parse;
    use crate::query::resolve::query_expr_resolve_bound;

    fn column_metadata(name: &str, index: usize, type_name: &str) -> ColumnMetadata {
        let (type_oid, data_type) = match type_name {
            "int4" => (TypeOid::from_type(&Type::INT4), Type::INT4),
            _ => (TypeOid::from_type(&Type::TEXT), Type::TEXT),
        };
        ColumnMetadata {
            name: name.into(),
            position: ColumnPosition::from_index(index).expect("column position in range"),
            type_oid,
            data_type,
            type_name: type_name.into(),
            cache_type_name: type_name.into(),
            is_primary_key: index == 0,
        }
    }

    fn table_metadata(name: &str, oid: u32, cols: &[(&str, &str)]) -> TableMetadata {
        let columns = ColumnStore::new(
            cols.iter()
                .enumerate()
                .map(|(i, (name, type_name))| column_metadata(name, i, type_name)),
        );
        TableMetadata {
            replica_identity_full: false,
            relation_oid: Oid::from_raw(oid),
            name: name.into(),
            schema: "public".into(),
            primary_key_columns: vec![cols[0].0.into()],
            columns,
            indexes: Vec::new(),
        }
    }

    fn catalog(tables: &[&TableMetadata]) -> Catalog {
        let mut catalog = BiHashMap::new();
        for table in tables {
            catalog.insert_overwrite((*table).clone().into());
        }
        catalog
    }

    fn bound(sql: &str, catalog: &Catalog) -> ResolvedQuery {
        let query = Arc::new(query_expr_parse(sql).expect("parse"));
        let bindings = query_expr_resolve_bound(&query, catalog, &["public"]).expect("resolve");
        ResolvedQuery::new(query, bindings)
    }

    fn row(values: &[Option<&str>]) -> Vec<Option<ByteString>> {
        values.iter().map(|v| v.map(Into::into)).collect()
    }

    /// The single-occurrence membership SQL for `row_data`.
    fn membership(
        sql: &str,
        table: &TableMetadata,
        catalog: &Catalog,
        row_data: &[Option<ByteString>],
    ) -> String {
        let bound = bound(sql, catalog);
        let occurrence = table_occurrence_single(&bound, table).expect("one occurrence");
        let mut buf = String::new();
        membership_values_into(&mut buf, &bound, table, row_data, &occurrence);
        buf
    }

    fn users() -> TableMetadata {
        table_metadata("users", 100, &[("id", "int4"), ("name", "text")])
    }

    #[test]
    fn test_table_stands_in_as_values_with_casts_and_alias_columns() {
        let users = users();
        let catalog = catalog(&[&users]);
        assert_eq!(
            membership(
                "SELECT name FROM users WHERE id = 1",
                &users,
                &catalog,
                &row(&[Some("42"), Some("alice")])
            ),
            "SELECT users.name FROM (VALUES ('42'::int4, 'alice'::text)) users(id, name) WHERE users.id = 1"
        );
        // NULLs keep their column's cast; a short row only names the columns it has.
        assert_eq!(
            membership(
                "SELECT name FROM users",
                &users,
                &catalog,
                &row(&[Some("1"), None])
            ),
            "SELECT users.name FROM (VALUES ('1'::int4, NULL::text)) users(id, name)"
        );
        assert_eq!(
            membership(
                "SELECT u.name FROM users u WHERE u.id = 1",
                &users,
                &catalog,
                &row(&[Some("1")])
            ),
            "SELECT u.name FROM (VALUES ('1'::int4)) u(id) WHERE u.id = 1"
        );
    }

    /// PGC-139 / PGC-145: references in JOIN ON, GROUP BY and HAVING follow
    /// the stand-in's alias like the select list and WHERE do; a surviving
    /// `public.table.col` would dangle against the aliased entry.
    #[test]
    fn test_every_clause_references_the_stand_in() {
        let j1 = table_metadata("j1_tbl", 5001, &[("i", "int4"), ("j", "int4")]);
        let j2 = table_metadata("j2_tbl", 5002, &[("i", "int4"), ("k", "int4")]);
        let catalog = catalog(&[&j1, &j2]);
        let sql = membership(
            "SELECT j1_tbl.j, count(*) FILTER (WHERE j2_tbl.k = 1) AS c FROM j1_tbl \
             JOIN j2_tbl ON j1_tbl.i = j2_tbl.i WHERE j1_tbl.j > 0 GROUP BY j1_tbl.j HAVING count(*) > 0",
            &j1,
            &catalog,
            &row(&[Some("1"), Some("4")]),
        );
        assert_eq!(
            sql,
            "SELECT j1_tbl.j, count(*) FILTER (WHERE public.j2_tbl.k = 1) AS c FROM \
             (VALUES ('1'::int4, '4'::int4)) j1_tbl(i, j) JOIN public.j2_tbl ON j1_tbl.i = public.j2_tbl.i \
             WHERE j1_tbl.j > 0 GROUP BY j1_tbl.j HAVING count(*) > 0"
        );
    }

    /// A correlated reference to the stand-in inside a nested subquery
    /// follows it too, wherever it sits; an inner occurrence of the same
    /// table, under its own alias or even the same one, keeps its own
    /// binding (PGC-256).
    #[test]
    fn test_nested_references_follow_their_binding() {
        let onek = table_metadata("onek", 6001, &[("unique1", "int4"), ("ten", "int4")]);
        let catalog = catalog(&[&onek]);
        let r = row(&[Some("1"), Some("7")]);
        assert_eq!(
            membership(
                "SELECT unique1 FROM onek WHERE EXISTS (SELECT 1 FROM onek o2 WHERE o2.ten = onek.ten)",
                &onek,
                &catalog,
                &r
            ),
            "SELECT onek.unique1 FROM (VALUES ('1'::int4, '7'::int4)) onek(unique1, ten) \
             WHERE EXISTS (SELECT 1 FROM public.onek o2 WHERE o2.ten = onek.ten)"
        );
        assert_eq!(
            membership(
                "SELECT unique1 FROM onek WHERE EXISTS (SELECT 1 FROM onek WHERE onek.ten = 3)",
                &onek,
                &catalog,
                &r
            ),
            "SELECT onek.unique1 FROM (VALUES ('1'::int4, '7'::int4)) onek(unique1, ten) \
             WHERE EXISTS (SELECT 1 FROM public.onek WHERE public.onek.ten = 3)"
        );
        assert_eq!(
            membership(
                "SELECT unique1 FROM onek WHERE EXISTS (SELECT 1 FROM onek o2 GROUP BY o2.ten HAVING max(o2.ten) = onek.ten)",
                &onek,
                &catalog,
                &r
            ),
            "SELECT onek.unique1 FROM (VALUES ('1'::int4, '7'::int4)) onek(unique1, ten) \
             WHERE EXISTS (SELECT 1 FROM public.onek o2 GROUP BY o2.ten HAVING max(o2.ten) = onek.ten)"
        );
    }

    /// `USING` and `NATURAL` render verbatim around the stand-in, whose alias
    /// columns carry the join column (PGC-142).
    #[test]
    fn test_using_join_keeps_its_qualifier() {
        let j1 = table_metadata("j1_tbl", 5001, &[("i", "int4"), ("j", "int4")]);
        let j2 = table_metadata("j2_tbl", 5002, &[("i", "int4"), ("k", "int4")]);
        let catalog = catalog(&[&j1, &j2]);
        assert_eq!(
            membership(
                "SELECT j FROM j1_tbl JOIN j2_tbl USING (i)",
                &j1,
                &catalog,
                &row(&[Some("1"), Some("4")])
            ),
            "SELECT j1_tbl.j FROM (VALUES ('1'::int4, '4'::int4)) j1_tbl(i, j) JOIN public.j2_tbl USING (i)"
        );
        assert_eq!(
            membership(
                "SELECT * FROM j1_tbl NATURAL JOIN j2_tbl",
                &j1,
                &catalog,
                &row(&[Some("1"), Some("4")])
            ),
            "SELECT j1_tbl.i AS i, j1_tbl.j, public.j2_tbl.k FROM (VALUES ('1'::int4, '4'::int4)) j1_tbl(i, j) JOIN public.j2_tbl USING (i)"
        );
    }

    #[test]
    fn test_self_join_tests_every_arm() {
        let users = users();
        let catalog = catalog(&[&users]);
        let bound = bound(
            "SELECT a.name FROM users a JOIN users b ON a.id = b.id",
            &catalog,
        );
        let mut buf = String::new();
        membership_values_all_into(&mut buf, &bound, &users, &row(&[Some("1"), Some("x")]))
            .expect("render");
        assert_eq!(
            buf,
            "EXISTS (SELECT a.name FROM (VALUES ('1'::int4, 'x'::text)) a(id, name) JOIN public.users b ON a.id = b.id) \
             OR EXISTS (SELECT a.name FROM public.users a JOIN (VALUES ('1'::int4, 'x'::text)) b(id, name) ON a.id = b.id)"
        );
    }

    #[test]
    fn test_batch_rows_carry_their_ordinal_and_projection() {
        let users = users();
        let catalog = catalog(&[&users]);
        let bound = bound("SELECT name FROM users WHERE id > 0", &catalog);
        let row_a = row(&[Some("1"), Some("a")]);
        let row_b = row(&[Some("2"), None]);
        let rows: Vec<&[Option<ByteString>]> = vec![&row_a, &row_b];
        let mut buf = String::new();
        membership_batch_into(&mut buf, &bound, &users, &rows, 7).expect("render");
        assert_eq!(
            buf,
            "SELECT 7, __pgc_idx FROM (VALUES (0, '1'::int4, 'a'::text), (1, '2'::int4, NULL::text)) \
             users(__pgc_idx, id, name) WHERE users.id > 0"
        );
    }

    #[test]
    fn test_unnest_shape_is_fixed_across_row_counts() {
        let users = users();
        let catalog = catalog(&[&users]);
        let bound = bound("SELECT name FROM users u WHERE u.id > 0", &catalog);
        let mut buf = String::new();
        membership_unnest_into(&mut buf, &bound, &users).expect("render");
        assert_eq!(
            buf,
            "SELECT __pgc_idx FROM (SELECT unnest($1::int4[]) AS __pgc_idx, unnest($2::text[])::int4 AS id, \
             unnest($3::text[])::text AS name) u WHERE u.id > 0"
        );
    }

    #[test]
    fn test_missing_or_repeated_occurrences_are_errors() {
        let users = users();
        let other = table_metadata("other", 101, &[("id", "int4")]);
        let catalog = catalog(&[&users, &other]);
        let bound = bound(
            "SELECT a.name FROM users a JOIN users b ON a.id = b.id",
            &catalog,
        );
        assert!(matches!(
            table_occurrence_single(&bound, &users)
                .expect_err("two arms")
                .current_context(),
            AstTransformError::MultipleOccurrences { .. }
        ));
        assert!(matches!(
            table_occurrences_find(&bound, &other)
                .expect_err("absent")
                .current_context(),
            AstTransformError::MissingTable
        ));
    }

    /// PGC-343: the template's prefix/suffix render byte-identical to the
    /// per-row render it replaces, including NULLs and escaping.
    #[test]
    fn test_pg_eval_template_matches_per_row_render() {
        let onek = table_metadata(
            "onek",
            7001,
            &[("id", "int4"), ("ten", "int4"), ("name", "text")],
        );
        let catalog = catalog(&[&onek]);
        let rows = [
            row(&[Some("1"), Some("7"), Some("alice")]),
            row(&[Some("2"), None, Some("o'brien")]),
            row(&[Some("3"), Some("0"), Some("")]),
        ];
        for sql in [
            "SELECT * FROM onek WHERE ten = 7",
            "SELECT id FROM onek WHERE name = 'x' AND ten > 3",
            "SELECT ten, count(*) AS c FROM onek GROUP BY ten HAVING count(*) > 0",
        ] {
            let bound = bound(sql, &catalog);
            let template = PgEvalTemplate::build(&bound, &onek).expect("template builds");
            for r in &rows {
                let occurrence = table_occurrence_single(&bound, &onek).expect("one occurrence");
                let mut expected = String::new();
                membership_values_into(&mut expected, &bound, &onek, r, &occurrence);
                let mut fast = String::new();
                assert!(
                    template.render_into(&mut fast, r),
                    "{sql}: full row takes the fast path"
                );
                assert_eq!(fast, expected, "{sql}");
            }
        }
    }

    #[test]
    fn test_pg_eval_template_declines_short_row() {
        let onek = table_metadata(
            "onek",
            7002,
            &[("id", "int4"), ("ten", "int4"), ("name", "text")],
        );
        let catalog = catalog(&[&onek]);
        let template =
            PgEvalTemplate::build(&bound("SELECT * FROM onek WHERE ten = 7", &catalog), &onek)
                .expect("template builds");
        let mut buf = String::from("EXISTS (");
        assert!(!template.render_into(&mut buf, &row(&[Some("1"), Some("7")])));
        assert_eq!(
            buf, "EXISTS (",
            "a declined render leaves the buffer as it was"
        );
    }
}
