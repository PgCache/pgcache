//! AST traversal: walk the parsed query tree and replace each
//! `LiteralValue::Parameter` node with the corresponding bound value.

use rootcause::Report;

use super::parameter_to_literal;
use crate::cache::QueryParameters;
use crate::query::ast::{LiteralValue, QueryExpr, ScalarExpr, query_expr_finalize};
use crate::query::transform::walk::{QueryWalkerMut, query_expr_walk_mut};
use crate::query::transform::{AstTransformError, AstTransformResult, query_expr_constant_fold};

pub fn query_expr_parameters_replace(
    query_expr: &QueryExpr,
    parameters: &QueryParameters,
) -> AstTransformResult<QueryExpr> {
    let mut new_query = query_expr.clone();
    let mut replacer = ParameterReplacer { parameters };
    query_expr_walk_mut(&mut new_query, &mut replacer)?;

    // Bind-time substitution can newly expose pure-literal arithmetic that the
    // convert-time fold couldn't reach (e.g. `$1 % 10 + 1` once $1 is bound).
    query_expr_constant_fold(&mut new_query);
    query_expr_finalize(&mut new_query);

    Ok(new_query)
}

struct ParameterReplacer<'a> {
    parameters: &'a QueryParameters,
}

impl QueryWalkerMut for ParameterReplacer<'_> {
    type Error = Report<AstTransformError>;

    fn visit_scalar_post(&mut self, expr: &mut ScalarExpr) -> Result<(), Self::Error> {
        if let ScalarExpr::Literal(literal) = expr {
            literal_value_parameters_replace(literal, self.parameters)?;
        }
        Ok(())
    }

    fn visit_literal(&mut self, literal: &mut LiteralValue) -> Result<(), Self::Error> {
        literal_value_parameters_replace(literal, self.parameters)
    }
}

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
    #![allow(clippy::wildcard_enum_match_arm)]

    use bytes::Bytes;
    use postgres_types::Type as PgType;

    use super::query_expr_parameters_replace;
    use crate::cache::QueryParameters;
    use crate::oid::TypeOid;
    use crate::query::ast::{Deparse, query_expr_parse};
    use crate::query::transform::{AstTransformError, AstTransformResult};

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
        let query = query_expr_parse(sql).expect("convert to QueryExpr");
        let replaced = query_expr_parameters_replace(&query, params)?;
        let mut buf = String::new();
        replaced.deparse(&mut buf);
        Ok(buf)
    }

    /// Placeholders bound wherever they sit: predicates, joins, subqueries
    /// at every depth, scalar subqueries; a NULL value renders as NULL.
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
        ];
        for (sql, params, expected) in cases {
            assert_eq!(
                replaced(sql, &params).expect("to replace parameters"),
                expected,
                "{sql}"
            );
        }
    }

    #[test]
    fn test_ast_parameters_replace_out_of_bounds() {
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

    /// `SELECT $1 FROM …` — bind-time parameter substitution must reach the
    /// SELECT projection, and the bound form should share a fingerprint with
    /// the inline-literal form so cache reuse is automatic (PGC-183).
    #[test]
    fn test_ast_parameters_replace_in_select_columns() {
        use crate::query::ast::query_expr_fingerprint;

        let query_expr = query_expr_parse("SELECT $1 FROM users").expect("convert to QueryExpr");
        let params = typed_text_params(vec![(Some(b"42"), PgType::INT4)]);
        let bound =
            query_expr_parameters_replace(&query_expr, &params).expect("parameter replacement");

        let mut buf = String::new();
        bound.deparse(&mut buf);
        assert!(!buf.contains("$1"), "SELECT $1 should be bound, got: {buf}");

        // Fingerprint must match the inline-literal form.
        let inline = query_expr_parse("SELECT 42 FROM users").expect("convert inline form");
        assert_eq!(
            query_expr_fingerprint(&bound),
            query_expr_fingerprint(&inline),
            "bound `SELECT $1` should share a fingerprint with `SELECT 42`",
        );
    }

    /// `SELECT id FROM … ORDER BY $1` — bind-time parameter substitution must
    /// reach the top-level ORDER BY clause.
    #[test]
    fn test_ast_parameters_replace_in_order_by() {
        let sql = "SELECT id FROM users ORDER BY $1";
        let query_expr = query_expr_parse(sql).expect("convert to QueryExpr");

        let params = typed_text_params(vec![(Some(b"1"), PgType::INT4)]);
        let replaced =
            query_expr_parameters_replace(&query_expr, &params).expect("parameter replacement");

        let mut buf = String::new();
        replaced.deparse(&mut buf);
        assert!(
            !buf.contains("$1"),
            "ORDER BY $1 should be bound, got: {buf}"
        );
    }

    #[test]
    fn test_cte_parameter_replacement() {
        let sql = "WITH active_users AS (SELECT id, name FROM users WHERE status = $1) \
                   SELECT id FROM active_users WHERE name = $2";
        let query_expr = query_expr_parse(sql).expect("convert to QueryExpr");

        let params = typed_text_params(vec![
            (Some(b"active"), PgType::TEXT),
            (Some(b"alice"), PgType::TEXT),
        ]);

        let replaced =
            query_expr_parameters_replace(&query_expr, &params).expect("parameter replacement");

        let mut buf = String::new();
        replaced.deparse(&mut buf);

        assert!(
            buf.contains("status = 'active'"),
            "CTE body should have $1 replaced: {buf}"
        );
        assert!(
            buf.contains("name = 'alice'"),
            "Main query should have $2 replaced: {buf}"
        );
        assert!(!buf.contains("$1"), "No unreplaced $1 should remain: {buf}");
        assert!(!buf.contains("$2"), "No unreplaced $2 should remain: {buf}");
    }
}
