use crate::cache::update_query::{ColumnChange, OrderByKey, RowChanges, UpdateQuery};
use crate::catalog::TableMetadata;
use crate::pg::protocol::ByteString;
use crate::query::ast::BinaryOp;
use crate::query::cast::cast_target_coerce_text;
use crate::query::constraints::{QueryConstraints, TableConstraint};
use crate::query::evaluate::{literal_compare, where_value_compare_string};

/// Check that every WHERE constraint for `table_metadata` matches `row_data`.
/// Returns true when there are no constraints for this table (full-scan
/// cached query), or when every constraint evaluates to true on the row.
///
/// CastComparison constraints coerce the row's wire-text via
/// `cast_target_coerce_text` and compare via `literal_compare`; a coercion
/// failure is treated as non-match (the row would have errored at origin).
pub(super) fn row_constraints_match(
    constraints: &QueryConstraints,
    table_metadata: &TableMetadata,
    row_data: &[Option<ByteString>],
) -> bool {
    constraints
        .table_constraints
        .get(table_metadata.name.as_str())
        .is_none_or(|constraints| {
            constraints
                .iter()
                .all(|constraint| constraint_row_matches(constraint, table_metadata, row_data))
        })
}

/// A constraint on a column the table or row doesn't carry is skipped.
fn constraint_row_matches(
    constraint: &TableConstraint,
    table_metadata: &TableMetadata,
    row_data: &[Option<ByteString>],
) -> bool {
    let Some(row_value) = table_metadata
        .columns
        .get(constraint_column(constraint))
        .and_then(|column_meta| row_data.get(column_meta.index()))
    else {
        return true;
    };
    // NULL never matches comparison operators
    row_value
        .as_ref()
        .is_some_and(|row_str| constraint_value_matches(constraint, row_str))
}

fn constraint_column(constraint: &TableConstraint) -> &str {
    match constraint {
        TableConstraint::Comparison(col, ..)
        | TableConstraint::AnyOf(col, ..)
        | TableConstraint::CastComparison(col, ..) => col.as_str(),
    }
}

fn constraint_value_matches(constraint: &TableConstraint, row_str: &str) -> bool {
    match constraint {
        TableConstraint::Comparison(_, op, val) => where_value_compare_string(val, row_str, *op),
        TableConstraint::AnyOf(_, values) => values
            .iter()
            .any(|v| where_value_compare_string(v, row_str, BinaryOp::Equal)),
        // Coercion failure (e.g. `'abc'::int4`): the row would error at origin
        // and never match — safe to treat as non-matching here.
        TableConstraint::CastComparison(_, cast, op, val) => cast_target_coerce_text(cast, row_str)
            .is_some_and(|coerced| literal_compare(&coerced, *op, val)),
    }
}

/// Whether this UPDATE changed the row's primary key. Under REPLICA IDENTITY
/// DEFAULT the presence of a key tuple IS the signal ('K' is sent only on PK
/// change); under FULL every update carries the complete old row ('O'), so
/// the PK columns are compared. A missing column defaults to changed — the
/// conservative direction for every consumer (old-PK delete, ladder skip).
pub(super) fn update_pk_changed(
    table_metadata: &TableMetadata,
    key_data: &[Option<ByteString>],
    new_row_data: &[Option<ByteString>],
) -> bool {
    if key_data.is_empty() {
        return false;
    }
    if !table_metadata.replica_identity_full {
        return true;
    }
    table_metadata.primary_key_columns.iter().any(|pk| {
        let Some(meta) = table_metadata.columns.get(pk.as_str()) else {
            return true;
        };
        key_data.get(meta.index()) != new_row_data.get(meta.index())
    })
}

/// Whether this UPDATE moves the cached row strictly toward the front of the
/// query's ORDER BY output — a promotion, which can only push already-cached
/// rows down and therefore never opens an uncached gap at the LIMIT-window
/// boundary (PGC-334; holds under OFFSET too, since only boundary crossings
/// change the cached prefix's membership). Compares the old and new sort
/// tuples lexicographically: the first ORDER BY key whose value changed
/// decides. Any ambiguity — no key spec, a changed window column that isn't a
/// key, an incomparable pair, a missing value — returns `false`, which callers
/// must treat as "invalidate".
pub(super) fn window_move_is_promotion(
    update_query: &UpdateQuery,
    table_metadata: &TableMetadata,
    row_data: &[Option<ByteString>],
    row_changes: &RowChanges,
) -> bool {
    let Some(keys) = &update_query.order_by_keys else {
        return false;
    };
    if !window_changes_are_order_keys(update_query, keys, row_changes) {
        return false;
    }
    for key in keys.iter() {
        let Some(change) = row_changes.get(&key.column) else {
            return false;
        };
        if !change.changed {
            continue;
        }
        return table_metadata
            .columns
            .get(key.column.as_str())
            .and_then(|meta| row_data.get(meta.index()))
            .is_some_and(|new_value| sort_key_move_is_forward(key, change, new_value.is_none()));
    }
    // `window_changed` held but no key registered a change — punt.
    false
}

/// Every changed window column must be an ORDER BY key: predicate columns are
/// already known unchanged here, but e.g. a HAVING reference isn't
/// direction-analyzable.
fn window_changes_are_order_keys(
    update_query: &UpdateQuery,
    keys: &[OrderByKey],
    row_changes: &RowChanges,
) -> bool {
    update_query.limit_window_columns.iter().all(|column| {
        !row_changes.get(column).is_some_and(|cc| cc.changed)
            || keys.iter().any(|k| k.column == *column)
    })
}

fn sort_key_move_is_forward(key: &OrderByKey, change: &ColumnChange, new_is_null: bool) -> bool {
    match (change.old_is_null, new_is_null) {
        // Contradicts `changed`; punt.
        (true, true) => false,
        // Left the NULL region: forward iff NULLs sort last.
        (true, false) => !key.nulls_first,
        // Entered the NULL region: forward iff NULLs sort first.
        (false, true) => key.nulls_first,
        (false, false) => match change.old_less_than_new {
            // Value rose: forward iff larger sorts earlier.
            Some(true) => key.descending,
            Some(false) => !key.descending,
            None => false,
        },
    }
}

/// Evaluate a LocalEval update query's WHERE against the CDC row.
///
/// Must only be called when `update_query.eval_strategy == LocalEval` — the
/// classifier has already ensured the query is single-table, FromClause, with
/// no GROUP BY / HAVING and a supported WHERE shape. A WHERE of `None` means
/// the query loads every row, so the match is unconditional.
pub(super) fn update_query_matches_locally(
    update_query: &UpdateQuery,
    row_data: &[Option<ByteString>],
) -> bool {
    // `compiled_where` is built from this query's WHERE at registration (PGC-339).
    match &update_query.compiled_where {
        None => {
            // `None` must mean "LocalEval SELECT with no WHERE", never a non-SELECT
            // shape: the old code returned `false` for a non-SELECT, and the
            // classifier only assigns LocalEval to SELECTs. Guard that invariant so
            // a future classifier change can't silently make every row match here.
            debug_assert!(
                update_query.resolved.as_select().is_some(),
                "LocalEval query must be a SELECT; classifier/compiled_where invariant broken"
            );
            true
        }
        Some(predicate) => predicate.eval(row_data),
    }
}

/// Check if a row's membership in a joined result set is unchanged.
/// Returns true when the primary key didn't change and all join columns
/// are primary key columns — meaning the row's join relationships are stable.
pub(super) fn join_membership_unchanged(
    update_query: &UpdateQuery,
    table_metadata: &TableMetadata,
    key_data: Option<&[Option<ByteString>]>,
) -> bool {
    if key_data.is_none_or(|key| !key.is_empty()) {
        return false;
    }

    // Non-empty AND every join column is a PK column — decided on the iterator
    // so this per-candidate check allocates nothing (PGC-342). `peek` proves
    // non-empty; `all` then checks every column (including the peeked one).
    let mut join_columns = update_query
        .constraints
        .table_join_columns(&table_metadata.name)
        .peekable();

    join_columns.peek().is_some()
        && join_columns.all(|col| {
            table_metadata
                .primary_key_columns
                .iter()
                .any(|pk| pk == col)
        })
}

#[cfg(test)]
mod tests {
    use ecow::EcoString;
    use tokio_postgres::types::Type;

    use super::*;
    use crate::catalog::{ColumnMetadata, ColumnStore};
    use crate::oid::Oid;
    use crate::query::ast::LiteralValue;
    use crate::query::cast::CastTarget;

    // Row layout: [id INT4 (PK), name TEXT, created_at TIMESTAMP].
    fn fixture_table() -> TableMetadata {
        let column = |name: &str, position, type_oid, data_type, type_name: &str| ColumnMetadata {
            name: name.into(),
            position,
            type_oid,
            data_type,
            type_name: type_name.into(),
            cache_type_name: type_name.into(),
            is_primary_key: position == 1,
        };
        TableMetadata {
            replica_identity_full: false,
            relation_oid: Oid::from_raw(1001),
            name: "users".into(),
            schema: "public".into(),
            primary_key_columns: vec!["id".into()],
            columns: ColumnStore::new([
                column("id", 1, 23, Type::INT4, "int4"),
                column("name", 2, 25, Type::TEXT, "text"),
                column("created_at", 3, 1114, Type::TIMESTAMP, "timestamp"),
            ]),
            indexes: Vec::new(),
        }
    }

    fn row(name: Option<&str>, created_at: Option<&str>) -> Vec<Option<ByteString>> {
        vec![
            Some("1".into()),
            name.map(Into::into),
            created_at.map(Into::into),
        ]
    }

    fn constraint_matches(constraint: TableConstraint, row: &[Option<ByteString>]) -> bool {
        let mut constraints = QueryConstraints::default();
        constraints
            .table_constraints
            .insert(EcoString::from("users"), vec![constraint]);
        row_constraints_match(&constraints, &fixture_table(), row)
    }

    fn id_equals(value: i64) -> TableConstraint {
        TableConstraint::Comparison("id".into(), BinaryOp::Equal, LiteralValue::Integer(value))
    }

    fn cast(
        column: &str,
        target: CastTarget,
        op: BinaryOp,
        value: LiteralValue,
    ) -> TableConstraint {
        TableConstraint::CastComparison(column.into(), target, op, value)
    }

    fn name_int4(op: BinaryOp, value: i64) -> TableConstraint {
        cast("name", CastTarget::Int4, op, LiteralValue::Integer(value))
    }

    #[test]
    fn no_constraints_for_table_matches() {
        let constraints = QueryConstraints::default();
        let row = row(Some("alice"), None);
        assert!(row_constraints_match(&constraints, &fixture_table(), &row));
    }

    #[test]
    fn bare_comparison_matches_when_value_equal() {
        assert!(constraint_matches(id_equals(1), &row(Some("alice"), None)));
    }

    #[test]
    fn bare_comparison_misses_when_value_differs() {
        assert!(!constraint_matches(id_equals(2), &row(Some("alice"), None)));
    }

    // PGC-182: CastComparison constraints must coerce the row's wire-text via
    // the cast target before comparing.

    #[test]
    fn cast_comparison_int4_matches_when_coerced_value_equal() {
        let constraint = name_int4(BinaryOp::Equal, 42);
        assert!(constraint_matches(constraint, &row(Some("42"), None)));
    }

    #[test]
    fn cast_comparison_int4_misses_when_coerced_value_differs() {
        let constraint = name_int4(BinaryOp::Equal, 42);
        assert!(!constraint_matches(constraint, &row(Some("99"), None)));
    }

    #[test]
    fn cast_comparison_int4_misses_when_row_unparseable() {
        // `'abc'::int4` raises in postgres; locally we treat it as non-match.
        let constraint = name_int4(BinaryOp::Equal, 42);
        assert!(!constraint_matches(constraint, &row(Some("abc"), None)));
    }

    #[test]
    fn cast_comparison_bool_matches_via_pg_bool_spelling() {
        let constraint = cast(
            "name",
            CastTarget::Bool,
            BinaryOp::Equal,
            LiteralValue::Boolean(true),
        );
        assert!(constraint_matches(constraint, &row(Some("yes"), None)));
    }

    #[test]
    fn cast_comparison_date_matches_via_timestamp_prefix() {
        let constraint = cast(
            "created_at",
            CastTarget::Date,
            BinaryOp::Equal,
            LiteralValue::String("2024-01-15".into()),
        );
        let row = row(Some("alice"), Some("2024-01-15 09:00:00"));
        assert!(constraint_matches(constraint, &row));
    }

    #[test]
    fn cast_comparison_null_row_value_misses() {
        let constraint = name_int4(BinaryOp::Equal, 42);
        assert!(!constraint_matches(constraint, &row(None, None)));
    }

    #[test]
    fn cast_comparison_inequality_compares_numerically() {
        // Locks the PGC-186 op-flip fix on the CDC pre-filter path too:
        // `name::int4 > 100` matches when name="500".
        let constraint = name_int4(BinaryOp::GreaterThan, 100);
        assert!(constraint_matches(constraint, &row(Some("500"), None)));
    }
}
