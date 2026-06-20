use crate::query::ast::Deparse;
use crate::query::cast::cast_target_deparse;

use super::*;

impl Deparse for ResolvedTableNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push(' ');
        self.schema.deparse(buf);
        buf.push('.');
        self.name.deparse(buf);
        if let Some(alias) = &self.alias {
            buf.push(' ');
            alias.deparse(buf);
        }
        buf
    }
}

impl Deparse for ResolvedColumnNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        // Use alias if available, otherwise use schema.table
        if let Some(alias) = &self.table_alias {
            alias.deparse(buf);
        } else {
            self.schema.deparse(buf);
            buf.push('.');
            self.table.deparse(buf);
        }
        buf.push('.');
        self.column.deparse(buf);
        buf
    }
}

impl Deparse for ResolvedWhereExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            ResolvedWhereExpr::Scalar(scalar) => scalar.deparse(buf),
            ResolvedWhereExpr::Unary(unary) => {
                match unary.op {
                    UnaryOp::IsNull
                    | UnaryOp::IsNotNull
                    | UnaryOp::IsTrue
                    | UnaryOp::IsNotTrue
                    | UnaryOp::IsFalse
                    | UnaryOp::IsNotFalse => {
                        // Postfix operators: expr IS NULL, expr IS TRUE, etc.
                        unary.expr.deparse(buf);
                        buf.push(' ');
                        unary.op.deparse(buf);
                    }
                    UnaryOp::Not => {
                        // Prefix operator: NOT expr
                        // NOT has higher precedence than AND/OR, so NOT applied
                        // to a logical binary expression needs parentheses.
                        let needs_parens = matches!(
                            unary.expr.as_ref(),
                            ResolvedWhereExpr::Binary(child) if child.op.is_logical()
                        );
                        unary.op.deparse(buf);
                        buf.push(' ');
                        if needs_parens {
                            buf.push('(');
                        }
                        unary.expr.deparse(buf);
                        if needs_parens {
                            buf.push(')');
                        }
                    }
                }
                buf
            }
            ResolvedWhereExpr::Binary(binary) => {
                let left_needs_parens = matches!(
                    (&binary.op, binary.lexpr.as_ref()),
                    (BinaryOp::And, ResolvedWhereExpr::Binary(child)) if child.op == BinaryOp::Or
                );
                let right_needs_parens = matches!(
                    (&binary.op, binary.rexpr.as_ref()),
                    (BinaryOp::And, ResolvedWhereExpr::Binary(child)) if child.op == BinaryOp::Or
                );

                if left_needs_parens {
                    buf.push('(');
                }
                binary.lexpr.deparse(buf);
                if left_needs_parens {
                    buf.push(')');
                }

                buf.push(' ');
                binary.op.deparse(buf);
                buf.push(' ');

                if right_needs_parens {
                    buf.push('(');
                }
                binary.rexpr.deparse(buf);
                if right_needs_parens {
                    buf.push(')');
                }

                buf
            }
            ResolvedWhereExpr::Multi(multi) => {
                // Format: column IN (value1, value2, ...) or column NOT IN (...)
                let [first, rest @ ..] = multi.exprs.as_slice() else {
                    return buf;
                };

                // First expression is the column/left side
                first.deparse(buf);

                match multi.op {
                    MultiOp::In => buf.push_str(" IN ("),
                    MultiOp::NotIn => buf.push_str(" NOT IN ("),
                    MultiOp::Between
                    | MultiOp::NotBetween
                    | MultiOp::BetweenSymmetric
                    | MultiOp::NotBetweenSymmetric => {
                        buf.push(' ');
                        multi.op.deparse(buf);
                        buf.push(' ');
                        // BETWEEN low AND high — exactly 2 bounds
                        let mut sep = "";
                        for expr in rest {
                            buf.push_str(sep);
                            expr.deparse(buf);
                            sep = " AND ";
                        }
                        return buf;
                    }
                    MultiOp::Any { .. } | MultiOp::All { .. } => {
                        buf.push(' ');
                        multi.op.deparse(buf);
                        buf.push_str(" (");
                    }
                }

                // Remaining expressions are the values
                let mut sep = "";
                for expr in rest {
                    buf.push_str(sep);
                    expr.deparse(buf);
                    sep = ", ";
                }
                buf.push(')');
                buf
            }
            ResolvedWhereExpr::Subquery {
                query,
                sublink_type,
                test_expr,
                ..
            } => {
                match sublink_type {
                    SubLinkType::Exists => {
                        buf.push_str("EXISTS (");
                        query.deparse(buf);
                        buf.push(')');
                    }
                    SubLinkType::Any => {
                        // IN is a special case of ANY
                        if let Some(test) = test_expr {
                            test.deparse(buf);
                            buf.push_str(" IN (");
                            query.deparse(buf);
                            buf.push(')');
                        } else {
                            buf.push('(');
                            query.deparse(buf);
                            buf.push(')');
                        }
                    }
                    SubLinkType::All => {
                        if let Some(test) = test_expr {
                            test.deparse(buf);
                            buf.push_str(" <> ALL (");
                            query.deparse(buf);
                            buf.push(')');
                        } else {
                            buf.push_str("ALL (");
                            query.deparse(buf);
                            buf.push(')');
                        }
                    }
                    SubLinkType::Expr => {
                        // Scalar subquery - just parenthesized query
                        buf.push('(');
                        query.deparse(buf);
                        buf.push(')');
                    }
                }
                buf
            }
        }
    }
}

impl Deparse for ResolvedArithmeticExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push('(');
        self.left.deparse(buf);
        buf.push(' ');
        buf.push_str(self.op.as_ref());
        buf.push(' ');
        self.right.deparse(buf);
        buf.push(')');
        buf
    }
}

impl Deparse for ResolvedFunctionCall {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push_str(&self.name);
        buf.push('(');
        if self.agg_distinct {
            buf.push_str("DISTINCT ");
        }
        if self.agg_star {
            buf.push('*');
        } else {
            let mut sep = "";
            for arg in &self.args {
                buf.push_str(sep);
                arg.deparse(buf);
                sep = ", ";
            }
        }
        if !self.agg_order.is_empty() {
            buf.push_str(" ORDER BY ");
            let mut sep = "";
            for clause in &self.agg_order {
                buf.push_str(sep);
                clause.deparse(buf);
                sep = ", ";
            }
        }
        buf.push(')');
        if let Some(filter) = &self.agg_filter {
            buf.push_str(" FILTER (WHERE ");
            filter.deparse(buf);
            buf.push(')');
        }
        if let Some(window_spec) = &self.over {
            buf.push_str(" OVER ");
            window_spec.deparse(buf);
        }
        buf
    }
}

impl Deparse for ResolvedWindowFrame {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        self.mode.deparse(buf);
        buf.push_str(" BETWEEN ");
        self.start.deparse(buf);
        buf.push_str(" AND ");
        self.end.deparse(buf);
        self.exclusion.deparse(buf);
        buf
    }
}

impl Deparse for ResolvedFrameBound {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            ResolvedFrameBound::UnboundedPreceding => buf.push_str("UNBOUNDED PRECEDING"),
            ResolvedFrameBound::CurrentRow => buf.push_str("CURRENT ROW"),
            ResolvedFrameBound::UnboundedFollowing => buf.push_str("UNBOUNDED FOLLOWING"),
            ResolvedFrameBound::OffsetPreceding(e) => {
                e.deparse(buf);
                buf.push_str(" PRECEDING");
            }
            ResolvedFrameBound::OffsetFollowing(e) => {
                e.deparse(buf);
                buf.push_str(" FOLLOWING");
            }
        }
        buf
    }
}

impl Deparse for ResolvedWindowSpec {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push('(');
        if !self.partition_by.is_empty() {
            buf.push_str("PARTITION BY ");
            let mut sep = "";
            for col in &self.partition_by {
                buf.push_str(sep);
                col.deparse(buf);
                sep = ", ";
            }
        }
        if !self.order_by.is_empty() {
            if !self.partition_by.is_empty() {
                buf.push(' ');
            }
            buf.push_str("ORDER BY ");
            let mut sep = "";
            for clause in &self.order_by {
                buf.push_str(sep);
                clause.deparse(buf);
                sep = ", ";
            }
        }
        if let Some(frame) = &self.frame {
            if !self.partition_by.is_empty() || !self.order_by.is_empty() {
                buf.push(' ');
            }
            frame.deparse(buf);
        }
        buf.push(')');
        buf
    }
}

impl Deparse for ResolvedScalarExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            ResolvedScalarExpr::Column(col) => col.deparse(buf),
            ResolvedScalarExpr::Identifier(name) => name.deparse(buf),
            ResolvedScalarExpr::Literal(lit) => lit.deparse(buf),
            ResolvedScalarExpr::Function(func) => func.deparse(buf),
            ResolvedScalarExpr::Case(case) => case.deparse(buf),
            ResolvedScalarExpr::Arithmetic(arith) => arith.deparse(buf),
            ResolvedScalarExpr::Subquery(query, _) => {
                buf.push('(');
                query.deparse(buf);
                buf.push(')');
                buf
            }
            ResolvedScalarExpr::Array(elems) => {
                buf.push_str("ARRAY[");
                let mut sep = "";
                for elem in elems {
                    buf.push_str(sep);
                    elem.deparse(buf);
                    sep = ", ";
                }
                buf.push(']');
                buf
            }
            ResolvedScalarExpr::TypeCast { expr, target } => {
                buf.push('(');
                expr.deparse(buf);
                buf.push_str(")::");
                buf.push_str(cast_target_deparse(target));
                buf
            }
        }
    }
}

impl Deparse for ResolvedCaseExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push_str("CASE");
        if let Some(arg) = &self.arg {
            buf.push(' ');
            arg.deparse(buf);
        }
        for when in &self.whens {
            buf.push_str(" WHEN ");
            when.condition.deparse(buf);
            buf.push_str(" THEN ");
            when.result.deparse(buf);
        }
        if let Some(default) = &self.default {
            buf.push_str(" ELSE ");
            default.deparse(buf);
        }
        buf.push_str(" END");
        buf
    }
}

impl Deparse for ResolvedSelectColumn {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push(' ');
        self.expr.deparse(buf);
        if let Some(alias) = &self.alias {
            buf.push_str(" AS ");
            // Quote aliases that need it (spaces, uppercase, keywords); a raw
            // push emits invalid SQL for `AS "Correlated Field"` (PGC-284).
            alias.deparse(buf);
        }
        buf
    }
}

impl Deparse for ResolvedSelectColumns {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            ResolvedSelectColumns::None => buf.push(' '),
            ResolvedSelectColumns::Columns(cols) => {
                let mut sep = "";
                for col in cols {
                    buf.push_str(sep);
                    col.deparse(buf);
                    sep = ",";
                }
            }
        }
        buf
    }
}

impl Deparse for ResolvedTableSource {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            ResolvedTableSource::Table(table) => table.deparse(buf),
            ResolvedTableSource::Join(join) => join.deparse(buf),
            ResolvedTableSource::Subquery(subquery) => subquery.deparse(buf),
        }
    }
}

impl Deparse for ResolvedTableSubqueryNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push_str(" (");
        self.query.deparse(buf);
        buf.push_str(") ");
        self.alias.deparse(buf);
        buf
    }
}

impl Deparse for ResolvedJoinNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        self.left.deparse(buf);

        // A cartesian join is `CROSS JOIN`, never a bare `JOIN` (a
        // syntax error). `Cross` only ever has an inner join type
        // (an outer join always carries ON/USING/NATURAL).
        if matches!(self.qual, ResolvedJoinQual::Cross) {
            buf.push_str(" CROSS JOIN");
        } else {
            buf.push_str(self.join_type.join_keyword());
        }

        self.right.deparse(buf);

        // `USING (c,…)` is emitted verbatim so Postgres merges the join
        // columns (matching origin's `SELECT *` width / unqualified
        // refs); the equi-`predicate` is internal-only and not emitted.
        match &self.qual {
            ResolvedJoinQual::On(condition) => {
                buf.push_str(" ON ");
                condition.deparse(buf);
            }
            ResolvedJoinQual::Using { columns, .. } => {
                buf.push_str(" USING (");
                for (i, c) in columns.iter().enumerate() {
                    if i > 0 {
                        buf.push_str(", ");
                    }
                    buf.push_str(c);
                }
                buf.push(')');
            }
            ResolvedJoinQual::Cross => {}
        }

        buf
    }
}

impl Deparse for ResolvedOrderByClause {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        self.expr.deparse(buf);
        match self.direction {
            OrderDirection::Asc => buf.push_str(" ASC"),
            OrderDirection::Desc => buf.push_str(" DESC"),
        }
        self.null_order.deparse(buf);
        buf
    }
}

impl Deparse for ResolvedSelectNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push_str("SELECT");
        if self.distinct {
            buf.push_str(" DISTINCT");
        }
        self.columns.deparse(buf);

        if !self.from.is_empty() {
            buf.push_str(" FROM");
            let mut sep = "";
            for table in &self.from {
                buf.push_str(sep);
                table.deparse(buf);
                sep = ",";
            }
        }

        if let Some(expr) = &self.where_clause {
            buf.push_str(" WHERE ");
            expr.deparse(buf);
        }

        if !self.group_by.is_empty() {
            buf.push_str(" GROUP BY ");
            let mut sep = "";
            for col in &self.group_by {
                buf.push_str(sep);
                col.deparse(buf);
                sep = ", ";
            }
        }

        if let Some(expr) = &self.having {
            buf.push_str(" HAVING ");
            expr.deparse(buf);
        }

        buf
    }
}

impl Deparse for ResolvedSetOpNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        self.left.deparse(buf);
        buf.push(' ');
        self.op.deparse(buf);
        if self.all {
            buf.push_str(" ALL");
        }
        buf.push(' ');
        self.right.deparse(buf);
        buf
    }
}

impl Deparse for ResolvedQueryBody {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            ResolvedQueryBody::Select(select) => select.deparse(buf),
            ResolvedQueryBody::Values(values) => values.deparse(buf),
            ResolvedQueryBody::SetOp(set_op) => set_op.deparse(buf),
        }
    }
}

impl Deparse for ResolvedQueryExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        self.body.deparse(buf);

        if !self.order_by.is_empty() {
            buf.push_str(" ORDER BY");
            let mut sep = "";
            for order in &self.order_by {
                buf.push_str(sep);
                buf.push(' ');
                order.deparse(buf);
                sep = ",";
            }
        }

        if let Some(limit) = &self.limit {
            if let Some(count) = &limit.count {
                buf.push_str(" LIMIT ");
                count.deparse(buf);
            }
            if let Some(offset) = &limit.offset {
                buf.push_str(" OFFSET ");
                offset.deparse(buf);
            }
        }

        buf
    }
}
