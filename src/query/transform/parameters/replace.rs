//! AST traversal: walk the parsed query tree and replace each
//! `LiteralValue::Parameter` node with the corresponding bound value.

use rootcause::Report;

use crate::cache::QueryParameters;
use crate::query::ast::{
    JoinQual, LiteralValue, QueryBody, QueryExpr, ScalarExpr, SelectNode, TableSource, WhereExpr,
};

use super::super::{AstTransformError, AstTransformResult};
use super::parameter_to_literal;

pub fn query_expr_parameters_replace(
    query_expr: &QueryExpr,
    parameters: &QueryParameters,
) -> AstTransformResult<QueryExpr> {
    let mut new_query = query_expr.clone();

    for cte in &mut new_query.ctes {
        let replaced = query_expr_parameters_replace(&cte.query, parameters)?;
        cte.query = replaced;
    }

    query_body_parameters_replace(&mut new_query.body, parameters)?;

    if let Some(limit) = &mut new_query.limit {
        if let Some(count) = &mut limit.count {
            literal_value_parameters_replace(count, parameters)?;
        }
        if let Some(offset) = &mut limit.offset {
            literal_value_parameters_replace(offset, parameters)?;
        }
    }

    // Bind-time substitution can newly expose pure-literal arithmetic that the
    // convert-time fold couldn't reach (e.g. `$1 % 10 + 1` once $1 is bound).
    super::super::query_expr_constant_fold(&mut new_query);

    Ok(new_query)
}

fn query_body_parameters_replace(
    body: &mut QueryBody,
    parameters: &QueryParameters,
) -> AstTransformResult<()> {
    match body {
        QueryBody::Select(select_node) => {
            select_node_parameters_replace(select_node, parameters)?;
        }
        QueryBody::Values(_) => {}
        QueryBody::SetOp(set_op) => {
            query_expr_parameters_replace_mut(&mut set_op.left, parameters)?;
            query_expr_parameters_replace_mut(&mut set_op.right, parameters)?;
        }
    }
    Ok(())
}

fn query_expr_parameters_replace_mut(
    query_expr: &mut QueryExpr,
    parameters: &QueryParameters,
) -> AstTransformResult<()> {
    query_body_parameters_replace(&mut query_expr.body, parameters)?;
    if let Some(limit) = &mut query_expr.limit {
        if let Some(count) = &mut limit.count {
            literal_value_parameters_replace(count, parameters)?;
        }
        if let Some(offset) = &mut limit.offset {
            literal_value_parameters_replace(offset, parameters)?;
        }
    }
    Ok(())
}

pub fn select_node_parameters_replace(
    select_node: &mut SelectNode,
    parameters: &QueryParameters,
) -> AstTransformResult<()> {
    if let Some(where_clause) = &mut select_node.where_clause {
        where_expr_parameters_replace(where_clause, parameters)?;
    }

    if let Some(having) = &mut select_node.having {
        where_expr_parameters_replace(having, parameters)?;
    }

    for table_source in &mut select_node.from {
        table_source_parameters_replace(table_source, parameters)?;
    }

    Ok(())
}

fn table_source_parameters_replace(
    table_source: &mut TableSource,
    parameters: &QueryParameters,
) -> AstTransformResult<()> {
    match table_source {
        TableSource::Join(join) => {
            if let JoinQual::On(condition) = &mut join.qual {
                where_expr_parameters_replace(condition, parameters)?;
            }
            table_source_parameters_replace(&mut join.left, parameters)?;
            table_source_parameters_replace(&mut join.right, parameters)?;
        }
        TableSource::Subquery(subquery) => {
            query_expr_parameters_replace_mut(&mut subquery.query, parameters)?;
        }
        TableSource::CteRef(cte_ref) => {
            query_expr_parameters_replace_mut(&mut cte_ref.query, parameters)?;
        }
        TableSource::Table(_) => {}
    }
    Ok(())
}

fn where_expr_parameters_replace(
    expr: &mut WhereExpr,
    parameters: &QueryParameters,
) -> AstTransformResult<()> {
    match expr {
        WhereExpr::Scalar(scalar) => scalar_expr_parameters_replace(scalar, parameters)?,
        WhereExpr::Unary(unary) => {
            where_expr_parameters_replace(&mut unary.expr, parameters)?;
        }
        WhereExpr::Binary(binary) => {
            where_expr_parameters_replace(&mut binary.lexpr, parameters)?;
            where_expr_parameters_replace(&mut binary.rexpr, parameters)?;
        }
        WhereExpr::Multi(multi) => {
            for expr in &mut multi.exprs {
                where_expr_parameters_replace(expr, parameters)?;
            }
        }
        WhereExpr::Subquery {
            query, test_expr, ..
        } => {
            query_expr_parameters_replace_mut(query, parameters)?;
            if let Some(test) = test_expr {
                scalar_expr_parameters_replace(test, parameters)?;
            }
        }
    }
    Ok(())
}

fn scalar_expr_parameters_replace(
    expr: &mut ScalarExpr,
    parameters: &QueryParameters,
) -> AstTransformResult<()> {
    match expr {
        ScalarExpr::Literal(literal) => {
            literal_value_parameters_replace(literal, parameters)?;
        }
        ScalarExpr::Column(_) => {}
        ScalarExpr::Function(func) => {
            for arg in &mut func.args {
                scalar_expr_parameters_replace(arg, parameters)?;
            }
            for clause in &mut func.agg_order {
                scalar_expr_parameters_replace(&mut clause.expr, parameters)?;
            }
            if let Some(filter) = &mut func.agg_filter {
                where_expr_parameters_replace(filter, parameters)?;
            }
            if let Some(over) = &mut func.over {
                for col in &mut over.partition_by {
                    scalar_expr_parameters_replace(col, parameters)?;
                }
                for clause in &mut over.order_by {
                    scalar_expr_parameters_replace(&mut clause.expr, parameters)?;
                }
            }
        }
        ScalarExpr::Case(case) => {
            if let Some(arg) = &mut case.arg {
                scalar_expr_parameters_replace(arg, parameters)?;
            }
            for when in &mut case.whens {
                where_expr_parameters_replace(&mut when.condition, parameters)?;
                scalar_expr_parameters_replace(&mut when.result, parameters)?;
            }
            if let Some(default) = &mut case.default {
                scalar_expr_parameters_replace(default, parameters)?;
            }
        }
        ScalarExpr::Arithmetic(arith) => {
            scalar_expr_parameters_replace(&mut arith.left, parameters)?;
            scalar_expr_parameters_replace(&mut arith.right, parameters)?;
        }
        ScalarExpr::Subquery(query) => {
            query_expr_parameters_replace_mut(query, parameters)?;
        }
        ScalarExpr::Array(elems) => {
            for elem in elems {
                scalar_expr_parameters_replace(elem, parameters)?;
            }
        }
        ScalarExpr::TypeCast { expr, .. } => {
            scalar_expr_parameters_replace(expr, parameters)?;
        }
    }
    Ok(())
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
