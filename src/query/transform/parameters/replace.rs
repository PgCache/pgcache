//! AST traversal: walk the parsed query tree and replace each
//! `LiteralValue::Parameter` node with the corresponding bound value.

use rootcause::Report;

use crate::cache::QueryParameters;
use crate::query::ast::{LiteralValue, QueryExpr, ScalarExpr, SelectNode};

use super::super::walk::{QueryWalkerMut, query_expr_walk_mut, select_node_walk_mut};
use super::super::{AstTransformError, AstTransformResult};
use super::parameter_to_literal;

pub fn query_expr_parameters_replace(
    query_expr: &QueryExpr,
    parameters: &QueryParameters,
) -> AstTransformResult<QueryExpr> {
    let mut new_query = query_expr.clone();
    let mut replacer = ParameterReplacer { parameters };
    query_expr_walk_mut(&mut new_query, &mut replacer)?;

    // Bind-time substitution can newly expose pure-literal arithmetic that the
    // convert-time fold couldn't reach (e.g. `$1 % 10 + 1` once $1 is bound).
    super::super::query_expr_constant_fold(&mut new_query);

    Ok(new_query)
}

pub fn select_node_parameters_replace(
    select_node: &mut SelectNode,
    parameters: &QueryParameters,
) -> AstTransformResult<()> {
    let mut replacer = ParameterReplacer { parameters };
    select_node_walk_mut(select_node, &mut replacer)
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

fn literal_value_parameters_replace(
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

    use postgres_types::Type as PgType;
    use tokio_util::bytes::Bytes;

    use crate::cache::QueryParameters;
    use crate::query::ast::{Deparse, QueryBody, SelectNode, query_expr_convert};

    use super::super::super::AstTransformError;
    use super::{query_expr_parameters_replace, select_node_parameters_replace};

    fn parse_select_node(sql: &str) -> SelectNode {
        let ast = pg_query::parse(sql).expect("parse SQL");
        let query_expr = query_expr_convert(&ast).expect("convert to QueryExpr");
        match query_expr.body {
            QueryBody::Select(node) => *node,
            _ => panic!("expected SELECT"),
        }
    }

    fn text_params(values: Vec<Option<&[u8]>>) -> QueryParameters {
        let len = values.len();
        QueryParameters {
            values: values
                .into_iter()
                .map(|v| v.map(Bytes::copy_from_slice))
                .collect(),
            formats: vec![0; len],
            oids: vec![PgType::TEXT.oid(); len],
        }
    }

    fn typed_text_params(values: Vec<(Option<&[u8]>, PgType)>) -> QueryParameters {
        let len = values.len();
        let (values, oids): (Vec<_>, Vec<_>) = values
            .into_iter()
            .map(|(v, t)| (v.map(Bytes::copy_from_slice), t.oid()))
            .unzip();
        QueryParameters {
            values,
            formats: vec![0; len],
            oids,
        }
    }

    #[test]
    fn test_ast_parameters_replace_simple() {
        let mut node = parse_select_node("SELECT id FROM users WHERE id = $1");
        let params = text_params(vec![Some(b"42")]);
        select_node_parameters_replace(&mut node, &params).expect("to replace parameters");

        let mut buf = String::new();
        node.deparse(&mut buf);
        assert_eq!(buf, "SELECT id FROM users WHERE id = '42'");
    }

    #[test]
    fn test_ast_parameters_replace_multiple_params() {
        let mut node = parse_select_node("SELECT id FROM users WHERE id = $1 AND name = $2");
        let params = text_params(vec![Some(b"42"), Some(b"alice")]);
        select_node_parameters_replace(&mut node, &params).expect("to replace parameters");

        let mut buf = String::new();
        node.deparse(&mut buf);
        assert_eq!(
            buf,
            "SELECT id FROM users WHERE id = '42' AND name = 'alice'"
        );
    }

    #[test]
    fn test_ast_parameters_replace_null() {
        let mut node = parse_select_node("SELECT id FROM users WHERE name = $1");
        let params = text_params(vec![None]);
        select_node_parameters_replace(&mut node, &params).expect("to replace parameters");

        let mut buf = String::new();
        node.deparse(&mut buf);
        assert_eq!(buf, "SELECT id FROM users WHERE name = NULL");
    }

    #[test]
    fn test_ast_parameters_replace_out_of_bounds() {
        let mut node = parse_select_node("SELECT id FROM users WHERE id = $2");
        let params = text_params(vec![Some(b"42")]);
        let result = select_node_parameters_replace(&mut node, &params);

        assert!(result.is_err());
        match result.map_err(|e| e.into_current_context()) {
            Err(AstTransformError::ParameterOutOfBounds { index, count }) => {
                assert_eq!(index, 1);
                assert_eq!(count, 1);
            }
            _ => panic!("Expected ParameterOutOfBounds error"),
        }
    }

    #[test]
    fn test_ast_parameters_replace_in_join() {
        let mut node = parse_select_node(
            "SELECT u.id FROM users u JOIN orders o ON o.user_id = u.id WHERE o.total > $1",
        );
        let params = text_params(vec![Some(b"100")]);
        select_node_parameters_replace(&mut node, &params).expect("to replace parameters");

        let mut buf = String::new();
        node.deparse(&mut buf);
        assert!(buf.contains("WHERE o.total > '100'"));
    }

    #[test]
    fn test_parameters_replace_in_subquery() {
        let mut node = parse_select_node(
            "SELECT id FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > $1)",
        );

        let params = typed_text_params(vec![(Some(b"100"), PgType::INT4)]);
        select_node_parameters_replace(&mut node, &params).expect("to replace parameters");

        let mut buf = String::new();
        node.deparse(&mut buf);
        assert_eq!(
            buf,
            "SELECT id FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 100)"
        );
    }

    #[test]
    fn test_parameters_replace_exists_subquery() {
        let mut node = parse_select_node(
            "SELECT id FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE total > $1)",
        );

        let params = typed_text_params(vec![(Some(b"50"), PgType::INT4)]);
        select_node_parameters_replace(&mut node, &params).expect("to replace parameters");

        let mut buf = String::new();
        node.deparse(&mut buf);
        assert_eq!(
            buf,
            "SELECT id FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE total > 50)"
        );
    }

    #[test]
    fn test_parameters_replace_subquery_with_outer_param() {
        let mut node = parse_select_node(
            "SELECT id FROM users WHERE status = $1 AND id IN (SELECT user_id FROM orders WHERE total > $2)",
        );

        let params = typed_text_params(vec![
            (Some(b"active"), PgType::TEXT),
            (Some(b"200"), PgType::INT4),
        ]);
        select_node_parameters_replace(&mut node, &params).expect("to replace parameters");

        let mut buf = String::new();
        node.deparse(&mut buf);
        assert_eq!(
            buf,
            "SELECT id FROM users WHERE status = 'active' AND id IN (SELECT user_id FROM orders WHERE total > 200)"
        );
    }

    #[test]
    fn test_parameters_replace_nested_subquery() {
        let mut node = parse_select_node(
            "SELECT id FROM users WHERE id IN (SELECT user_id FROM orders WHERE product_id IN (SELECT id FROM products WHERE price > $1))",
        );

        let params = typed_text_params(vec![(Some(b"99"), PgType::INT4)]);
        select_node_parameters_replace(&mut node, &params).expect("to replace parameters");

        let mut buf = String::new();
        node.deparse(&mut buf);
        assert_eq!(
            buf,
            "SELECT id FROM users WHERE id IN (SELECT user_id FROM orders WHERE product_id IN (SELECT id FROM products WHERE price > 99))"
        );
    }

    #[test]
    fn test_parameters_replace_scalar_subquery_in_where() {
        let mut node = parse_select_node(
            "SELECT id FROM users WHERE age > (SELECT avg(age) FROM users WHERE status = $1)",
        );

        let params = typed_text_params(vec![(Some(b"active"), PgType::TEXT)]);
        select_node_parameters_replace(&mut node, &params).expect("to replace parameters");

        let mut buf = String::new();
        node.deparse(&mut buf);
        assert_eq!(
            buf,
            "SELECT id FROM users WHERE age > (SELECT AVG(age) FROM users WHERE status = 'active')"
        );
    }

    /// `SELECT id FROM … ORDER BY $1` — bind-time parameter substitution must
    /// reach the top-level ORDER BY clause.
    #[test]
    fn test_ast_parameters_replace_in_order_by() {
        let sql = "SELECT id FROM users ORDER BY $1";
        let ast = pg_query::parse(sql).expect("parse SQL");
        let query_expr = query_expr_convert(&ast).expect("convert to QueryExpr");

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
        let ast = pg_query::parse(sql).expect("parse SQL");
        let query_expr = query_expr_convert(&ast).expect("convert to QueryExpr");

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
