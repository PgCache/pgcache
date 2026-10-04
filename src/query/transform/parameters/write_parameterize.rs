//! Bind-value substitution for parameterized write classifications, so the
//! proxy's read-after-write gate can reason about concrete rows and predicates.
//! Only the proxy consumes these, so they are compiled out of the
//! analysis-only build.

use super::replace::literal_value_parameters_replace;
use crate::cache::QueryParameters;
use crate::query::transform::AstTransformResult;
use crate::query::write::{DeleteStatement, InsertRow, InsertStatement, UpdateStatement};

/// Substitute Bind values for the `$N` cells of a parameterized `INSERT` write
/// classification (PGC-370), so the read-after-write gate can prove an inserted
/// row disjoint from a read instead of degrading to table-conservative. Concrete
/// and unknown (`None`) cells are left untouched. Errors (out-of-bounds index,
/// undecodable value) propagate so the caller degrades the whole write to
/// table-level — a partially-substituted row must never be trusted.
pub(crate) fn insert_statement_parameterize(
    stmt: &InsertStatement,
    parameters: &QueryParameters,
) -> AstTransformResult<InsertStatement> {
    let mut rows = Vec::with_capacity(stmt.rows.len());
    for row in &stmt.rows {
        let mut new_row: InsertRow = row.clone();
        for cell in new_row.iter_mut().flatten() {
            literal_value_parameters_replace(cell, parameters)?;
        }
        rows.push(new_row);
    }
    Ok(InsertStatement {
        relation: stmt.relation.clone(),
        columns: stmt.columns.clone(),
        rows,
    })
}

/// Substitute Bind values for the `$N` values of a parameterized `DELETE` write
/// classification (PGC-386), so the read-after-write gate can prove a read
/// disjoint from the delete's predicate instead of degrading to table-level. As
/// with [`insert_statement_parameterize`], any error propagates so the caller
/// degrades the whole write to table-level.
pub(crate) fn delete_statement_parameterize(
    stmt: &DeleteStatement,
    parameters: &QueryParameters,
) -> AstTransformResult<DeleteStatement> {
    let mut comparisons = stmt.comparisons.clone();
    for (_, _, value) in &mut comparisons {
        literal_value_parameters_replace(value, parameters)?;
    }
    Ok(DeleteStatement {
        relation: stmt.relation.clone(),
        comparisons,
    })
}

/// Substitute Bind values for the `$N` values of a parameterized `UPDATE` write
/// classification (PGC-386) — both the WHERE comparisons and the SET values, so
/// the gate has a concrete predicate and post-update image. A `None` SET value
/// (unknown, e.g. `SET c = c + 1`) stays unknown. Errors propagate so the caller
/// degrades the whole write to table-level.
pub(crate) fn update_statement_parameterize(
    stmt: &UpdateStatement,
    parameters: &QueryParameters,
) -> AstTransformResult<UpdateStatement> {
    let mut where_comparisons = stmt.where_comparisons.clone();
    for (_, _, value) in &mut where_comparisons {
        literal_value_parameters_replace(value, parameters)?;
    }
    let mut set = stmt.set.clone();
    for (_, value) in set.iter_mut() {
        if let Some(value) = value {
            literal_value_parameters_replace(value, parameters)?;
        }
    }
    Ok(UpdateStatement {
        relation: stmt.relation.clone(),
        where_comparisons,
        set,
    })
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use postgres_types::Type as PgType;

    use super::*;
    use crate::oid::TypeOid;
    use crate::query::ast::{BinaryOp, LiteralValue};
    use crate::query::write::RelationRef;

    fn typed_text_params(values: Vec<(Option<&[u8]>, PgType)>) -> QueryParameters {
        let len = values.len();
        let (values, oids): (Vec<_>, Vec<_>) = values
            .into_iter()
            .map(|(v, t)| (v.map(Bytes::copy_from_slice), TypeOid::from_type(&t)))
            .unzip();
        QueryParameters {
            values,
            formats: vec![0; len],
            oids,
        }
    }

    fn relation(name: &str) -> RelationRef {
        RelationRef {
            schema: None,
            name: name.into(),
        }
    }

    #[test]
    fn test_delete_statement_parameterize() {
        let stmt = DeleteStatement {
            relation: relation("t"),
            comparisons: vec![
                (
                    "id".into(),
                    BinaryOp::Equal,
                    LiteralValue::Parameter("$1".into()),
                ),
                ("v".into(), BinaryOp::GreaterThan, LiteralValue::Integer(3)),
            ],
        };
        let params = typed_text_params(vec![(Some(b"7"), PgType::INT4)]);
        let out = delete_statement_parameterize(&stmt, &params).expect("substitute");
        assert_eq!(out.comparisons[0].2, LiteralValue::Integer(7));
        // A concrete literal is left untouched.
        assert_eq!(out.comparisons[1].2, LiteralValue::Integer(3));
    }

    #[test]
    fn test_update_statement_parameterize() {
        let stmt = UpdateStatement {
            relation: relation("t"),
            where_comparisons: vec![(
                "id".into(),
                BinaryOp::Equal,
                LiteralValue::Parameter("$1".into()),
            )],
            set: vec![
                ("v".into(), Some(LiteralValue::Parameter("$2".into()))),
                // A non-literal SET stays unknown after substitution.
                ("w".into(), None),
            ],
        };
        let params =
            typed_text_params(vec![(Some(b"7"), PgType::INT4), (Some(b"9"), PgType::INT4)]);
        let out = update_statement_parameterize(&stmt, &params).expect("substitute");
        assert_eq!(out.where_comparisons[0].2, LiteralValue::Integer(7));
        assert_eq!(out.set[0].1, Some(LiteralValue::Integer(9)));
        assert_eq!(out.set[1].1, None);
    }

    #[test]
    fn test_update_statement_parameterize_out_of_bounds_errors() {
        let stmt = UpdateStatement {
            relation: relation("t"),
            where_comparisons: vec![(
                "id".into(),
                BinaryOp::Equal,
                LiteralValue::Parameter("$2".into()),
            )],
            set: vec![("v".into(), Some(LiteralValue::Integer(1)))],
        };
        let params = typed_text_params(vec![(Some(b"7"), PgType::INT4)]);
        assert!(update_statement_parameterize(&stmt, &params).is_err());
    }

    fn insert_stmt(cells: Vec<Option<LiteralValue>>) -> InsertStatement {
        InsertStatement {
            relation: crate::query::write::RelationRef {
                schema: None,
                name: "t".into(),
            },
            columns: vec!["id".into(), "v".into()],
            rows: vec![cells.into_iter().collect()],
        }
    }

    fn param(n: u32) -> Option<LiteralValue> {
        Some(LiteralValue::Parameter(format!("${n}").into()))
    }

    #[test]
    fn test_insert_statement_parameterize_substitutes_cells() {
        let stmt = insert_stmt(vec![param(1), param(2)]);
        let params = typed_text_params(vec![
            (Some(b"3"), PgType::INT4),
            (Some(b"30"), PgType::INT4),
        ]);
        let out = insert_statement_parameterize(&stmt, &params).expect("substitute");
        assert_eq!(
            out.rows[0].as_slice(),
            [
                Some(LiteralValue::Integer(3)),
                Some(LiteralValue::Integer(30))
            ]
        );
    }

    #[test]
    fn test_insert_statement_parameterize_preserves_concrete_and_unknown() {
        // A concrete literal and an unknown (`None`, e.g. DEFAULT) cell survive
        // untouched alongside a substituted parameter.
        let stmt = insert_stmt(vec![Some(LiteralValue::Integer(9)), None]);
        let params = typed_text_params(vec![(Some(b"3"), PgType::INT4)]);
        let out = insert_statement_parameterize(&stmt, &params).expect("substitute");
        assert_eq!(
            out.rows[0].as_slice(),
            [Some(LiteralValue::Integer(9)), None]
        );
    }

    #[test]
    fn test_insert_statement_parameterize_out_of_bounds_errors() {
        // `$2` with only one bound value → error, so the caller degrades to
        // table-conservative rather than trusting a partial row.
        let stmt = insert_stmt(vec![param(1), param(2)]);
        let params = typed_text_params(vec![(Some(b"3"), PgType::INT4)]);
        assert!(insert_statement_parameterize(&stmt, &params).is_err());
    }
}
