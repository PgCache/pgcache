use postgres_types::Type;

use crate::cache::update_query::{ColumnChange, OrderByKey, RowChanges, UpdateQuery};
use crate::catalog::TableMetadata;
use crate::pg::protocol::ByteString;
use crate::query::ast::BinaryOp;
use crate::query::cast::cast_target_coerce_text;
use crate::query::constraints::{ColumnConstraintKind, NullTest, TableConstraint};
use crate::query::evaluate::literal_compare;
use crate::query::typed_compare::typed_compare;

/// Check that every filter of an update query's reference matches
/// `row_data`. True when there are none (a full-scan reference).
///
/// CastComparison constraints coerce the row's wire-text via
/// `cast_target_coerce_text` and compare via `literal_compare`; a coercion
/// failure is treated as non-match (the row would have errored at origin).
pub(super) fn row_constraints_match(
    filters: &[TableConstraint],
    table_metadata: &TableMetadata,
    row_data: &[Option<ByteString>],
) -> bool {
    filters
        .iter()
        .all(|constraint| constraint_row_matches(constraint, table_metadata, row_data))
}

/// What is known of a deleted row (PGC-528). Under REPLICA IDENTITY DEFAULT
/// the DELETE tuple carries only the PK, so its other columns read as NULL
/// and can't decide a filter.
#[derive(Clone, Copy)]
pub(super) enum DeleteImage<'a> {
    /// REPLICA IDENTITY FULL: the tuple is the whole old row.
    Full(&'a [Option<ByteString>]),
    /// Recovered by the old-image ladder: real values at `known` only.
    Partial {
        row: &'a [Option<ByteString>],
        known: &'a [usize],
    },
    /// The lookup found no cache-table row with the key: nothing a Ready
    /// query reads can have held it.
    NotCached,
    /// Nothing recovered.
    Unknown,
}

/// Whether the deleted row may have met `filters`: a constraint on a column
/// the image doesn't know counts as met, and an uncached row met none.
pub(super) fn delete_filters_match(
    filters: &[TableConstraint],
    table_metadata: &TableMetadata,
    image: DeleteImage<'_>,
) -> bool {
    match image {
        DeleteImage::Full(row) => row_constraints_match(filters, table_metadata, row),
        DeleteImage::Partial { row, known } => filters.iter().all(|constraint| {
            let position = table_metadata
                .columns
                .get(constraint.column.as_str())
                .map(|column_meta| column_meta.index());
            !position.is_some_and(|position| known.contains(&position))
                || constraint_row_matches(constraint, table_metadata, row)
        }),
        DeleteImage::NotCached => false,
        DeleteImage::Unknown => true,
    }
}

/// A constraint on a column the table or row doesn't carry is skipped.
fn constraint_row_matches(
    constraint: &TableConstraint,
    table_metadata: &TableMetadata,
    row_data: &[Option<ByteString>],
) -> bool {
    let Some(column_meta) = table_metadata.columns.get(constraint.column.as_str()) else {
        return true;
    };
    let Some(row_value) = row_data.get(column_meta.index()) else {
        return true;
    };
    match row_value {
        Some(row_str) => {
            constraint_value_matches(&constraint.kind, &column_meta.data_type, row_str)
        }
        // NULL satisfies no comparison, only the null test for it.
        None => matches!(
            constraint.kind,
            ColumnConstraintKind::NullTest(NullTest::IsNull)
        ),
    }
}

/// Every consumer of a constraint match invalidates, evicts or includes on a
/// match, so a value the comparator cannot decide counts as matching: over-
/// returning costs work, under-returning serves stale data (PGC-478).
fn constraint_value_matches(kind: &ColumnConstraintKind, data_type: &Type, row_str: &str) -> bool {
    match kind {
        ColumnConstraintKind::Comparison { op, value } => {
            typed_compare(data_type, value, row_str, *op).unwrap_or(true)
        }
        ColumnConstraintKind::AnyOf(values) => values
            .iter()
            .any(|v| typed_compare(data_type, v, row_str, BinaryOp::Equal).unwrap_or(true)),
        // Coercion failure (e.g. `'abc'::int4`): the row would error at origin
        // and never match — safe to treat as non-matching here.
        ColumnConstraintKind::CastComparison { cast, op, value } => {
            cast_target_coerce_text(cast, row_str)
                .is_some_and(|coerced| literal_compare(&coerced, *op, value))
        }
        ColumnConstraintKind::NullTest(test) => *test == NullTest::IsNotNull,
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
        None => true,
        Some(predicate) => predicate.eval(row_data),
    }
}

/// Check if a row's membership in a joined result set is unchanged.
/// Returns true when the primary key didn't change and all link columns
/// are primary key columns — meaning the row's join relationships are stable.
pub(super) fn join_membership_unchanged(
    update_query: &UpdateQuery,
    table_metadata: &TableMetadata,
    key_data: Option<&[Option<ByteString>]>,
) -> bool {
    if key_data.is_none_or(|key| !key.is_empty()) {
        return false;
    }

    let link_columns = &update_query.predicates.link_columns;
    !link_columns.is_empty()
        && link_columns.iter().all(|col| {
            table_metadata
                .primary_key_columns
                .iter()
                .any(|pk| pk == col)
        })
}

#[cfg(test)]
mod tests {
    use tokio_postgres::types::Type;

    use super::*;
    use crate::catalog::{ColumnMetadata, ColumnPosition};
    use crate::oid::TypeOid;
    use crate::query::ast::LiteralValue;
    use crate::query::cast::CastTarget;

    // Row layout: [id INT4 (PK), name TEXT, created_at TIMESTAMP].
    fn fixture_table() -> TableMetadata {
        let column = |name: &str, position: i16, data_type: Type, type_name: &str| ColumnMetadata {
            name: name.into(),
            position: ColumnPosition::from_raw(position),
            type_oid: TypeOid::from_type(&data_type),
            data_type,
            type_name: type_name.into(),
            cache_type_name: type_name.into(),
            is_primary_key: position == 1,
        };
        TableMetadata::for_test(
            "users",
            1001,
            [
                column("id", 1, Type::INT4, "int4"),
                column("name", 2, Type::TEXT, "text"),
                column("created_at", 3, Type::TIMESTAMP, "timestamp"),
            ],
        )
    }

    fn row(name: Option<&str>, created_at: Option<&str>) -> Vec<Option<ByteString>> {
        vec![
            Some("1".into()),
            name.map(Into::into),
            created_at.map(Into::into),
        ]
    }

    fn constraint_matches(constraint: TableConstraint, row: &[Option<ByteString>]) -> bool {
        row_constraints_match(&[constraint], &fixture_table(), row)
    }

    fn constraint(column: &str, kind: ColumnConstraintKind) -> TableConstraint {
        TableConstraint {
            column: column.into(),
            kind,
        }
    }

    fn id_equals(value: i64) -> TableConstraint {
        constraint(
            "id",
            ColumnConstraintKind::Comparison {
                op: BinaryOp::Equal,
                value: LiteralValue::Integer(value),
            },
        )
    }

    fn cast(
        column: &str,
        target: CastTarget,
        op: BinaryOp,
        value: LiteralValue,
    ) -> TableConstraint {
        constraint(
            column,
            ColumnConstraintKind::CastComparison {
                cast: target,
                op,
                value,
            },
        )
    }

    fn name_int4(op: BinaryOp, value: i64) -> TableConstraint {
        cast("name", CastTarget::Int4, op, LiteralValue::Integer(value))
    }

    /// A deleted row is judged only on what is known of it: a full tuple on
    /// every filter, a recovered one on its known columns, an uncached one
    /// meets none, and nothing known meets all (PGC-528).
    #[test]
    fn test_delete_filters_match_by_image() {
        let table = fixture_table();
        let name_is_alice = constraint(
            "name",
            ColumnConstraintKind::Comparison {
                op: BinaryOp::Equal,
                value: LiteralValue::String("alice".into()),
            },
        );
        let filters = [name_is_alice];
        let alice = row(Some("alice"), None);
        let bob = row(Some("bob"), None);
        let pk_only = row(None, None);

        assert!(delete_filters_match(
            &filters,
            &table,
            DeleteImage::Full(&alice)
        ));
        assert!(!delete_filters_match(
            &filters,
            &table,
            DeleteImage::Full(&bob)
        ));
        // A PK-only tuple read as full would wrongly fail the filter.
        assert!(!delete_filters_match(
            &filters,
            &table,
            DeleteImage::Full(&pk_only)
        ));

        let known_name: &[usize] = &[0, 1];
        assert!(delete_filters_match(
            &filters,
            &table,
            DeleteImage::Partial {
                row: &alice,
                known: known_name
            }
        ));
        assert!(!delete_filters_match(
            &filters,
            &table,
            DeleteImage::Partial {
                row: &bob,
                known: known_name
            }
        ));
        let known_pk: &[usize] = &[0];
        assert!(delete_filters_match(
            &filters,
            &table,
            DeleteImage::Partial {
                row: &pk_only,
                known: known_pk
            }
        ));

        assert!(!delete_filters_match(
            &filters,
            &table,
            DeleteImage::NotCached
        ));
        assert!(delete_filters_match(&filters, &table, DeleteImage::Unknown));
        assert!(!delete_filters_match(&[], &table, DeleteImage::NotCached));
    }

    /// A NULL row value satisfies `IS NULL` alone; a present one satisfies
    /// `IS NOT NULL` alone. A deleted row whose image doesn't know the
    /// column may have met either (PGC-533).
    #[test]
    fn test_null_test_matches_by_nullness() {
        let is_null = constraint("name", ColumnConstraintKind::NullTest(NullTest::IsNull));
        let is_not_null = constraint("name", ColumnConstraintKind::NullTest(NullTest::IsNotNull));
        let null_name = row(None, None);
        let alice = row(Some("alice"), None);
        assert!(constraint_matches(is_null.clone(), &null_name));
        assert!(!constraint_matches(is_null.clone(), &alice));
        assert!(!constraint_matches(is_not_null.clone(), &null_name));
        assert!(constraint_matches(is_not_null.clone(), &alice));

        let table = fixture_table();
        let known_pk: &[usize] = &[0];
        let known_name: &[usize] = &[0, 1];
        assert!(delete_filters_match(
            std::slice::from_ref(&is_not_null),
            &table,
            DeleteImage::Partial {
                row: &null_name,
                known: known_pk
            }
        ));
        assert!(!delete_filters_match(
            std::slice::from_ref(&is_not_null),
            &table,
            DeleteImage::Partial {
                row: &null_name,
                known: known_name
            }
        ));
        assert!(!delete_filters_match(
            std::slice::from_ref(&is_null),
            &table,
            DeleteImage::Partial {
                row: &alice,
                known: known_name
            }
        ));
    }

    #[test]
    fn test_no_constraints_for_table_matches() {
        let row = row(Some("alice"), None);
        assert!(row_constraints_match(&[], &fixture_table(), &row));
    }

    #[test]
    fn test_bare_comparison_matches_when_value_equal() {
        assert!(constraint_matches(id_equals(1), &row(Some("alice"), None)));
    }

    #[test]
    fn test_bare_comparison_misses_when_value_differs() {
        assert!(!constraint_matches(id_equals(2), &row(Some("alice"), None)));
    }

    // PGC-182: CastComparison constraints must coerce the row's wire-text via
    // the cast target before comparing.

    #[test]
    fn test_cast_comparison_int4_matches_when_coerced_value_equal() {
        let constraint = name_int4(BinaryOp::Equal, 42);
        assert!(constraint_matches(constraint, &row(Some("42"), None)));
    }

    #[test]
    fn test_cast_comparison_int4_misses_when_coerced_value_differs() {
        let constraint = name_int4(BinaryOp::Equal, 42);
        assert!(!constraint_matches(constraint, &row(Some("99"), None)));
    }

    #[test]
    fn test_cast_comparison_int4_misses_when_row_unparseable() {
        // `'abc'::int4` raises in postgres; locally we treat it as non-match.
        let constraint = name_int4(BinaryOp::Equal, 42);
        assert!(!constraint_matches(constraint, &row(Some("abc"), None)));
    }

    #[test]
    fn test_cast_comparison_bool_matches_via_pg_bool_spelling() {
        let constraint = cast(
            "name",
            CastTarget::Bool,
            BinaryOp::Equal,
            LiteralValue::Boolean(true),
        );
        assert!(constraint_matches(constraint, &row(Some("yes"), None)));
    }

    #[test]
    fn test_cast_comparison_date_matches_via_timestamp_prefix() {
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
    fn test_cast_comparison_null_row_value_misses() {
        let constraint = name_int4(BinaryOp::Equal, 42);
        assert!(!constraint_matches(constraint, &row(None, None)));
    }

    #[test]
    fn test_cast_comparison_inequality_compares_numerically() {
        // Locks the PGC-186 op-flip fix on the CDC pre-filter path too:
        // `name::int4 > 100` matches when name="500".
        let constraint = name_int4(BinaryOp::GreaterThan, 100);
        assert!(constraint_matches(constraint, &row(Some("500"), None)));
    }
}
