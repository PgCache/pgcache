//! Replace a `LiteralValue::Parameter` with its bound value.

use rootcause::Report;

use super::parameter_to_literal;
use crate::cache::QueryParameters;
use crate::query::ast::LiteralValue;
use crate::query::transform::{AstTransformError, AstTransformResult};

pub(crate) fn literal_value_parameters_replace(
    literal: &mut LiteralValue,
    parameters: &QueryParameters,
) -> AstTransformResult<()> {
    if let LiteralValue::Parameter(placeholder) = literal {
        let index = parameter_index_parse(placeholder)?;

        let param = parameters.get(index).ok_or_else(|| {
            Report::from(AstTransformError::ParameterOutOfBounds {
                index,
                count: parameters.len(),
            })
        })?;

        *literal = parameter_to_literal(&param)?;
    }
    Ok(())
}

/// Parse parameter index from placeholder string (e.g. `$1` → 0, `$2` → 1).
fn parameter_index_parse(placeholder: &str) -> AstTransformResult<usize> {
    let Some(index_str) = placeholder.strip_prefix('$') else {
        return Err(AstTransformError::InvalidParameterPlaceholder {
            placeholder: placeholder.to_owned(),
        }
        .into());
    };

    let param_num = index_str.parse::<usize>().map_err(|_| {
        Report::from(AstTransformError::InvalidParameterPlaceholder {
            placeholder: placeholder.to_owned(),
        })
    })?;

    if param_num == 0 {
        return Err(AstTransformError::InvalidParameterPlaceholder {
            placeholder: placeholder.to_owned(),
        }
        .into());
    }

    Ok(param_num - 1)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use postgres_types::Type as PgType;

    use crate::cache::QueryParameters;
    use crate::oid::TypeOid;
    use crate::query::ast::query_tree_parse;
    use crate::query::transform::{
        AstTransformError, AstTransformResult, query_tree_parameters_replace,
    };

    fn text_params(values: Vec<Option<&[u8]>>) -> QueryParameters {
        let len = values.len();
        QueryParameters {
            values: values
                .into_iter()
                .map(|v| v.map(Bytes::copy_from_slice))
                .collect(),
            formats: vec![0; len],
            oids: vec![TypeOid::from_type(&PgType::TEXT); len],
        }
    }

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

    /// `sql` with `params` bound, as rendered.
    fn replaced(sql: &str, params: &QueryParameters) -> AstTransformResult<String> {
        let tree = query_tree_parse(sql).expect("parse");
        Ok(query_tree_parameters_replace(&tree, params)?.sql())
    }

    /// Placeholders bound wherever they sit: predicates, joins, subqueries
    /// at every depth, scalar subqueries, the select list, ORDER BY and CTE
    /// bodies; a NULL value renders as NULL.
    #[test]
    fn test_parameters_replace_cases() {
        let int = |value: &'static [u8]| (Some(value), PgType::INT4);
        let text = |value: &'static [u8]| (Some(value), PgType::TEXT);
        #[rustfmt::skip]
        let cases: Vec<(&str, QueryParameters, &str)> = vec![
            ("SELECT id FROM users WHERE id = $1", text_params(vec![Some(b"42")]),
             "SELECT id FROM users WHERE id = '42'"),
            ("SELECT id FROM users WHERE id = $1 AND name = $2", text_params(vec![Some(b"42"), Some(b"alice")]),
             "SELECT id FROM users WHERE id = '42' AND name = 'alice'"),
            ("SELECT id FROM users WHERE name = $1", text_params(vec![None]),
             "SELECT id FROM users WHERE name = NULL"),
            ("SELECT u.id FROM users u JOIN orders o ON o.user_id = u.id WHERE o.total > $1", text_params(vec![Some(b"100")]),
             "SELECT u.id FROM users u JOIN orders o ON o.user_id = u.id WHERE o.total > '100'"),
            ("SELECT id FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > $1)", typed_text_params(vec![int(b"100")]),
             "SELECT id FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 100)"),
            ("SELECT id FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE total > $1)", typed_text_params(vec![int(b"50")]),
             "SELECT id FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE total > 50)"),
            ("SELECT id FROM users WHERE status = $1 AND id IN (SELECT user_id FROM orders WHERE total > $2)", typed_text_params(vec![text(b"active"), int(b"200")]),
             "SELECT id FROM users WHERE status = 'active' AND id IN (SELECT user_id FROM orders WHERE total > 200)"),
            ("SELECT id FROM users WHERE id IN (SELECT user_id FROM orders WHERE product_id IN (SELECT id FROM products WHERE price > $1))", typed_text_params(vec![int(b"99")]),
             "SELECT id FROM users WHERE id IN (SELECT user_id FROM orders WHERE product_id IN (SELECT id FROM products WHERE price > 99))"),
            ("SELECT id FROM users WHERE age > (SELECT avg(age) FROM users WHERE status = $1)", typed_text_params(vec![text(b"active")]),
             "SELECT id FROM users WHERE age > (SELECT AVG(age) FROM users WHERE status = 'active')"),
            // PGC-183: the select list binds too.
            ("SELECT $1 FROM users", typed_text_params(vec![int(b"42")]),
             "SELECT 42 FROM users"),
            ("SELECT id FROM users ORDER BY $1", typed_text_params(vec![int(b"1")]),
             "SELECT id FROM users ORDER BY 1 ASC"),
            ("WITH active_users AS (SELECT id, name FROM users WHERE status = $1) SELECT id FROM active_users WHERE name = $2",
             typed_text_params(vec![text(b"active"), text(b"alice")]),
             "WITH active_users AS (SELECT id, name FROM users WHERE status = 'active') SELECT id FROM active_users WHERE name = 'alice'"),
        ];
        for (sql, params, expected) in cases {
            assert_eq!(
                replaced(sql, &params).expect("to replace parameters"),
                expected,
                "{sql}"
            );
        }
    }

    /// A bound `SELECT $1` is the same tree as the inline literal form, so
    /// cache reuse is automatic (PGC-183).
    #[test]
    fn test_bound_select_list_equals_the_inline_form() {
        let tree = query_tree_parse("SELECT $1 FROM users").expect("parse");
        let params = typed_text_params(vec![(Some(b"42"), PgType::INT4)]);
        let bound = query_tree_parameters_replace(&tree, &params).expect("parameter replacement");
        assert_eq!(
            bound,
            query_tree_parse("SELECT 42 FROM users").expect("parse")
        );
    }

    #[test]
    fn test_parameters_replace_out_of_bounds() {
        let result = replaced(
            "SELECT id FROM users WHERE id = $2",
            &text_params(vec![Some(b"42")]),
        );
        match result.map_err(|e| e.into_current_context()) {
            Err(AstTransformError::ParameterOutOfBounds { index, count }) => {
                assert_eq!(index, 1);
                assert_eq!(count, 1);
            }
            _ => panic!("Expected ParameterOutOfBounds error"),
        }
    }
}
