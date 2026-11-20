//! A read's LIMIT and OFFSET as the values PostgreSQL runs it with, and
//! whether a cached row cap covers them.

use rootcause::Report;

use crate::cache::query::QueryParameters;
use crate::query::ast::LiteralValue;
use crate::query::ast::rope::Query;
use crate::query::transform::{
    AstTransformError, AstTransformResult, placeholder_row_count_literal,
};

/// PostgreSQL types both clauses bigint, and a NULL in either means the
/// clause is not there.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct RowLimit {
    /// `None` without a LIMIT, and for `LIMIT NULL`.
    pub count: Option<u64>,
    /// 0 without an OFFSET, and for `OFFSET NULL`.
    pub offset: u64,
}

impl RowLimit {
    /// The root LIMIT and OFFSET of `query`, a placeholder read from
    /// `parameters`. Fails on a value PostgreSQL would reject, or would round
    /// to a bigint by rules this does not reproduce.
    pub fn decode(
        query: Query<'_>,
        parameters: Option<&QueryParameters>,
    ) -> AstTransformResult<Self> {
        let count = match query.limit_count() {
            Some(literal) => limit_value_decode(literal.value(), parameters)?,
            None => None,
        };
        let offset = match query.limit_offset() {
            Some(literal) => limit_value_decode(literal.value(), parameters)?,
            None => None,
        };
        Ok(Self {
            count,
            offset: offset.unwrap_or(0),
        })
    }

    /// Rows a serve reads to answer this: `None` when every row is needed.
    /// A sum past the largest bigint is every row: population sends it to
    /// origin as a LIMIT, which rejects anything larger.
    pub fn rows_needed(&self) -> Option<u64> {
        let rows = self.count?.checked_add(self.offset)?;
        i64::try_from(rows).is_ok().then_some(rows)
    }
}

/// Rows the root LIMIT and OFFSET of `query` need, its bind values already
/// substituted. A value that does not decode needs every row, like no LIMIT.
pub fn limit_rows_needed(query: Query<'_>) -> Option<u64> {
    RowLimit::decode(query, None)
        .ok()
        .and_then(|limit| limit.rows_needed())
}

/// One LIMIT or OFFSET value, `None` for NULL. A placeholder is read as the
/// literal substitution puts in its place.
fn limit_value_decode(
    literal: &LiteralValue,
    parameters: Option<&QueryParameters>,
) -> AstTransformResult<Option<u64>> {
    let bound;
    let literal =
        if let (LiteralValue::Parameter(placeholder), Some(parameters)) = (literal, parameters) {
            bound = placeholder_row_count_literal(placeholder, parameters)?;
            &bound
        } else {
            literal
        };
    let value = match literal {
        LiteralValue::Integer(value) => Some(*value),
        LiteralValue::Null => None,
        LiteralValue::Parameter(_) => {
            return Err(limit_value_error("is a placeholder with no value"));
        }
        LiteralValue::String(_)
        | LiteralValue::StringWithCast(..)
        | LiteralValue::Float(_)
        | LiteralValue::Boolean(_)
        | LiteralValue::NullWithCast(_)
        | LiteralValue::SessionTerm(_)
        | LiteralValue::Array(..) => return Err(limit_value_error("is not a bigint")),
    };
    value
        .map(|value| u64::try_from(value).map_err(|_| limit_value_error("is negative")))
        .transpose()
}

fn limit_value_error(problem: &str) -> Report<AstTransformError> {
    Report::from(AstTransformError::InvalidParameterValue {
        message: format!("LIMIT/OFFSET value {problem}"),
    })
}

/// Check whether the cached `max_limit` is sufficient for the incoming `needed` rows.
///
/// - `cached_max`: `None` means all rows are cached.
/// - `needed`: `None` means all rows needed (no LIMIT).
pub fn limit_is_sufficient(cached_max: Option<u64>, needed: Option<u64>) -> bool {
    match (cached_max, needed) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(cached), Some(needed)) => cached >= needed,
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use postgres_types::Type as PgType;

    use super::*;
    use crate::oid::TypeOid;
    use crate::query::ast::query_tree_parse;
    use crate::query::transform::query_tree_parameters_replace;

    /// Name, the three bind values, their declared type, and the limit
    /// decoded (`None` for an error).
    type Case<'a> = (&'a str, [Option<&'a [u8]>; 3], TypeOid, Option<RowLimit>);

    fn parameters(values: &[Option<&[u8]>], oid: TypeOid) -> QueryParameters {
        QueryParameters {
            values: values
                .iter()
                .map(|value| value.map(Bytes::copy_from_slice))
                .collect(),
            formats: vec![0],
            oids: vec![oid; values.len()],
        }
    }

    fn decoded(sql: &str, parameters: Option<&QueryParameters>) -> Option<RowLimit> {
        let tree = query_tree_parse(sql).expect("parse query");
        RowLimit::decode(tree.root(), parameters).ok()
    }

    fn limit(count: Option<u64>, offset: u64) -> Option<RowLimit> {
        Some(RowLimit { count, offset })
    }

    #[test]
    fn test_row_limit_decode_reads_the_clause_as_written() {
        assert_eq!(decoded("SELECT id FROM users", None), limit(None, 0));
        assert_eq!(
            decoded("SELECT id FROM users LIMIT 10", None),
            limit(Some(10), 0)
        );
        assert_eq!(
            decoded("SELECT id FROM users LIMIT 10 OFFSET 5", None),
            limit(Some(10), 5)
        );
        assert_eq!(
            decoded("SELECT id FROM users OFFSET 5", None),
            limit(None, 5)
        );
        assert_eq!(decoded("SELECT id FROM users LIMIT $1", None), None);
    }

    #[test]
    fn test_row_limit_decode_types_a_placeholder_by_its_position() {
        let sql = "SELECT id FROM users WHERE id > $1 LIMIT $2 OFFSET $3";
        let undeclared = TypeOid::UNSPECIFIED;
        let int4 = TypeOid::from_type(&PgType::INT4);
        let text = TypeOid::from_type(&PgType::TEXT);
        #[rustfmt::skip]
        let cases: Vec<Case<'_>> = vec![
            ("undeclared text",    [Some(b"7"), Some(b"1000"), Some(b"0")],  undeclared, limit(Some(1000), 0)),
            ("declared int4",      [Some(b"7"), Some(b"10"),   Some(b"5")],  int4,       limit(Some(10), 5)),
            ("null count",         [Some(b"7"), None,          Some(b"5")],  undeclared, limit(None, 5)),
            ("null offset",        [Some(b"7"), Some(b"10"),   None],        undeclared, limit(Some(10), 0)),
            ("null of any type",   [Some(b"7"), None,          None],        text,       limit(None, 0)),
            ("negative count",     [Some(b"7"), Some(b"-1"),   Some(b"0")],  undeclared, None),
            ("negative offset",    [Some(b"7"), Some(b"10"),   Some(b"-1")], int4,       None),
            ("not a number",       [Some(b"7"), Some(b"ten"),  Some(b"0")],  undeclared, None),
            ("declared text",      [Some(b"7"), Some(b"10"),   Some(b"0")],  text,       None),
        ];
        for (name, values, oid, expected) in cases {
            let parameters = parameters(&values, oid);
            assert_eq!(decoded(sql, Some(&parameters)), expected, "{name}");
        }
    }

    /// A LIMIT placeholder past the end of Parse's type list is undeclared,
    /// whatever the parameters before it were declared as.
    #[test]
    fn test_row_limit_decode_reads_a_placeholder_past_the_declared_types() {
        let parameters = QueryParameters {
            values: vec![
                Some(Bytes::from_static(b"alice")),
                Some(Bytes::from_static(b"10")),
            ],
            formats: Vec::new(),
            oids: vec![TypeOid::from_type(&PgType::TEXT)],
        };
        assert_eq!(
            decoded(
                "SELECT id FROM users WHERE name = $1 LIMIT $2",
                Some(&parameters)
            ),
            limit(Some(10), 0)
        );
    }

    /// Registration reads its population cap from the substituted tree, where
    /// a bound value is an integer or a NULL whatever the client declared.
    #[test]
    fn test_row_limit_decode_reads_a_substituted_tree() {
        let tree = query_tree_parse("SELECT id FROM users LIMIT $1 OFFSET $2").expect("parse");
        let substituted = |values: &[Option<&[u8]>]| {
            let parameters = parameters(values, TypeOid::UNSPECIFIED);
            let tree = query_tree_parameters_replace(&tree, &parameters).ok()?;
            RowLimit::decode(tree.root(), None).ok()
        };
        assert_eq!(
            substituted(&[Some(b"1000"), Some(b"20")]),
            limit(Some(1000), 20)
        );
        assert_eq!(substituted(&[None, None]), limit(None, 0));
        assert_eq!(substituted(&[Some(b"ten"), Some(b"0")]), None);
    }

    #[test]
    fn test_limit_rows_needed_reads_every_row_for_a_value_that_does_not_decode() {
        let needed = |sql: &str| limit_rows_needed(query_tree_parse(sql).expect("parse").root());
        assert_eq!(needed("SELECT id FROM users LIMIT 10 OFFSET 5"), Some(15));
        assert_eq!(needed("SELECT id FROM users"), None);
        assert_eq!(needed("SELECT id FROM users LIMIT $1"), None);
    }

    #[test]
    fn test_row_limit_rows_needed() {
        let needed = |count, offset| RowLimit { count, offset }.rows_needed();
        assert_eq!(needed(None, 0), None);
        assert_eq!(needed(Some(10), 0), Some(10));
        assert_eq!(needed(Some(10), 5), Some(15));
        // OFFSET alone reads to the end.
        assert_eq!(needed(None, 5), None);
        let bigint_max = u64::try_from(i64::MAX).expect("bigint max fits u64");
        assert_eq!(needed(Some(bigint_max), 0), Some(bigint_max));
        assert_eq!(needed(Some(bigint_max), 20), None);
        assert_eq!(needed(Some(u64::MAX), 5), None);
    }

    #[test]
    fn test_limit_is_sufficient() {
        // All rows cached → always sufficient
        assert!(limit_is_sufficient(None, None));
        assert!(limit_is_sufficient(None, Some(100)));

        // Some rows cached, need unlimited → insufficient
        assert!(!limit_is_sufficient(Some(50), None));

        // Some rows cached, need fewer → sufficient
        assert!(limit_is_sufficient(Some(50), Some(30)));
        assert!(limit_is_sufficient(Some(50), Some(50)));

        // Some rows cached, need more → insufficient
        assert!(!limit_is_sufficient(Some(50), Some(51)));
    }
}
