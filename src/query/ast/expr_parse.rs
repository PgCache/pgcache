#[cfg(test)]
mod tests {

    #![allow(clippy::wildcard_enum_match_arm)]

    use ecow::EcoString;

    use crate::query::ast::*;

    /// Parse SQL and extract the WHERE clause via the AST layer.
    fn where_clause_parse(sql: &str) -> Result<Option<WhereExpr>, AstError> {
        let query_expr = query_expr_parse(sql)?;
        Ok(query_expr.where_clause().cloned())
    }

    #[test]
    fn fingerprint_literals_differ() {
        let q1 = query_expr_parse("select id, str from test where str = 'hello'").unwrap();
        let q2 = query_expr_parse("select id, str from test where str = 'bye'").unwrap();

        assert_ne!(query_expr_fingerprint(&q1), query_expr_fingerprint(&q2));
    }

    #[test]
    fn select_columns() {
        let q = query_expr_parse("select id, str from test where str = 'hello'").unwrap();
        let select = q.as_select().unwrap();
        let SelectColumns::Columns(cols) = &select.columns else {
            panic!("expected explicit columns");
        };
        assert_eq!(cols.len(), 2);
        assert!(matches!(&cols[0].expr().unwrap(), ScalarExpr::Column(c) if c.column == "id"));
        assert!(matches!(&cols[1].expr().unwrap(), ScalarExpr::Column(c) if c.column == "str"));

        let q = query_expr_parse("select count(id), str from test where str = 'hihi'").unwrap();
        let select = q.as_select().unwrap();
        let SelectColumns::Columns(cols) = &select.columns else {
            panic!("expected explicit columns");
        };
        assert_eq!(cols.len(), 2);
        assert!(matches!(&cols[0].expr().unwrap(), ScalarExpr::Function(f) if f.name == "count"));
        assert!(matches!(&cols[1].expr().unwrap(), ScalarExpr::Column(c) if c.column == "str"));
    }

    #[test]
    fn where_clause_simple_equality() {
        let result = where_clause_parse("SELECT id, str FROM test WHERE str = 'hello'");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::Equal,
            lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("str"),
            }))),
            rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                LiteralValue::String("hello".into()),
            ))),
        }));

        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_integer_equality() {
        let result = where_clause_parse("SELECT id FROM test WHERE id = 123");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::Equal,
            lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("id"),
            }))),
            rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                LiteralValue::Integer(123),
            ))),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_boolean_equality() {
        let result = where_clause_parse("SELECT id FROM test WHERE active = true");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::Equal,
            lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("active"),
            }))),
            rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                LiteralValue::Boolean(true),
            ))),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_greater_than() {
        let result = where_clause_parse("SELECT id FROM test WHERE cnt > 0");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::GreaterThan,
            lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("cnt"),
            }))),
            rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                LiteralValue::Integer(0),
            ))),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_and_operation() {
        let result = where_clause_parse("SELECT id FROM test WHERE str = 'hello' AND id = 123");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::And,
            lexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Equal,
                lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                    table: None,
                    column: EcoString::from("str"),
                }))),
                rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                    LiteralValue::String("hello".into()),
                ))),
            })),
            rexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Equal,
                lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                    table: None,
                    column: EcoString::from("id"),
                }))),
                rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                    LiteralValue::Integer(123),
                ))),
            })),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_or_operation() {
        let result = where_clause_parse("SELECT id FROM test WHERE str = 'hello' OR str = 'world'");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::Or,
            lexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Equal,
                lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                    table: None,
                    column: EcoString::from("str"),
                }))),
                rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                    LiteralValue::String("hello".into()),
                ))),
            })),
            rexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Equal,
                lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                    table: None,
                    column: EcoString::from("str"),
                }))),
                rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                    LiteralValue::String("world".into()),
                ))),
            })),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_not_operation() {
        let result = where_clause_parse("SELECT id FROM test WHERE NOT str = 'hello'");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Unary(UnaryExpr {
            op: UnaryOp::Not,
            expr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Equal,
                lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                    table: None,
                    column: EcoString::from("str"),
                }))),
                rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                    LiteralValue::String("hello".into()),
                ))),
            })),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_qualified_column() {
        let result = where_clause_parse("SELECT id FROM test WHERE test.str = 'hello'");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::Equal,
            lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                table: Some(EcoString::from("test")),
                column: EcoString::from("str"),
            }))),
            rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                LiteralValue::String("hello".into()),
            ))),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_null_value() {
        let result = where_clause_parse("SELECT id FROM test WHERE data = NULL");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::Equal,
            lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("data"),
            }))),
            rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::Null))),
        }));

        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_no_where() {
        let result = where_clause_parse("SELECT id FROM test");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        assert_eq!(where_clause, None);
    }

    #[test]
    fn where_clause_not_equal_with_exclamation() {
        let result = where_clause_parse("SELECT id FROM test WHERE id != 123");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::NotEqual,
            lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("id"),
            }))),
            rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                LiteralValue::Integer(123),
            ))),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_not_equal_with_angle_brackets() {
        let result = where_clause_parse("SELECT id FROM test WHERE id <> 123");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::NotEqual,
            lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("id"),
            }))),
            rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                LiteralValue::Integer(123),
            ))),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_less_than() {
        let result = where_clause_parse("SELECT id FROM test WHERE id < 123");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::LessThan,
            lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("id"),
            }))),
            rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                LiteralValue::Integer(123),
            ))),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_less_than_or_equal() {
        let result = where_clause_parse("SELECT id FROM test WHERE id <= 123");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::LessThanOrEqual,
            lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("id"),
            }))),
            rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                LiteralValue::Integer(123),
            ))),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_greater_than_or_equal() {
        let result = where_clause_parse("SELECT id FROM test WHERE id >= 123");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::GreaterThanOrEqual,
            lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("id"),
            }))),
            rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                LiteralValue::Integer(123),
            ))),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_like() {
        let result = where_clause_parse("SELECT id FROM test WHERE name LIKE 'test%'");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Binary(binary) = where_clause else {
            panic!("expected BinaryExpr");
        };

        assert_eq!(binary.op, BinaryOp::Like);

        let WhereExpr::Scalar(ScalarExpr::Column(col)) = binary.lexpr.as_ref() else {
            panic!("expected Column on left");
        };
        assert_eq!(col.column, "name");

        let WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::String(pattern))) =
            binary.rexpr.as_ref()
        else {
            panic!("expected string pattern on right");
        };
        assert_eq!(pattern, "test%");
    }

    #[test]
    fn where_clause_chained_and_operation() {
        let result = where_clause_parse(
            "SELECT id FROM test WHERE name = 'john' AND age > 25 AND active = true",
        );

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        // Should build a left-associative tree: ((name = 'john' AND age > 25) AND active = true)
        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::And,
            lexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::And,
                lexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                    op: BinaryOp::Equal,
                    lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                        table: None,
                        column: EcoString::from("name"),
                    }))),
                    rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                        LiteralValue::String("john".into()),
                    ))),
                })),
                rexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                    op: BinaryOp::GreaterThan,
                    lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                        table: None,
                        column: EcoString::from("age"),
                    }))),
                    rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                        LiteralValue::Integer(25),
                    ))),
                })),
            })),
            rexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Equal,
                lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                    table: None,
                    column: EcoString::from("active"),
                }))),
                rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                    LiteralValue::Boolean(true),
                ))),
            })),
        }));

        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_chained_or_operation() {
        let result = where_clause_parse(
            "SELECT id FROM test WHERE name = 'john' OR name = 'jane' OR name = 'bob'",
        );

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        // Should build a left-associative tree: ((name = 'john' OR name = 'jane') OR name = 'bob')
        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::Or,
            lexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Or,
                lexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                    op: BinaryOp::Equal,
                    lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                        table: None,
                        column: EcoString::from("name"),
                    }))),
                    rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                        LiteralValue::String("john".into()),
                    ))),
                })),
                rexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                    op: BinaryOp::Equal,
                    lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                        table: None,
                        column: EcoString::from("name"),
                    }))),
                    rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                        LiteralValue::String("jane".into()),
                    ))),
                })),
            })),
            rexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Equal,
                lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                    table: None,
                    column: EcoString::from("name"),
                }))),
                rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                    LiteralValue::String("bob".into()),
                ))),
            })),
        }));

        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_parameterized_query_single() {
        let result = where_clause_parse("SELECT id FROM test WHERE id = $1");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::Equal,
            lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("id"),
            }))),
            rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                LiteralValue::Parameter("$1".into()),
            ))),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_parameterized_query_multiple() {
        let result = where_clause_parse("SELECT id FROM test WHERE name = $1 AND age > $2");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::And,
            lexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Equal,
                lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                    table: None,
                    column: EcoString::from("name"),
                }))),
                rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                    LiteralValue::Parameter("$1".into()),
                ))),
            })),
            rexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::GreaterThan,
                lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                    table: None,
                    column: EcoString::from("age"),
                }))),
                rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                    LiteralValue::Parameter("$2".into()),
                ))),
            })),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_parameterized_query_mixed_with_literals() {
        let result = where_clause_parse("SELECT id FROM test WHERE name = $1 AND active = true");

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::And,
            lexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Equal,
                lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                    table: None,
                    column: EcoString::from("name"),
                }))),
                rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                    LiteralValue::Parameter("$1".into()),
                ))),
            })),
            rexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Equal,
                lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                    table: None,
                    column: EcoString::from("active"),
                }))),
                rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                    LiteralValue::Boolean(true),
                ))),
            })),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_in_with_strings() {
        let result =
            where_clause_parse("SELECT * FROM t WHERE status IN ('active', 'pending', 'complete')");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Multi(multi) = where_clause else {
            panic!("expected MultiExpr, got {:?}", where_clause);
        };

        assert_eq!(multi.op, MultiOp::In);
        assert_eq!(multi.exprs.len(), 4); // column + 3 values

        // First element should be the column
        let WhereExpr::Scalar(ScalarExpr::Column(col)) = &multi.exprs[0] else {
            panic!("expected Column");
        };
        assert_eq!(col.column, "status");

        // Remaining elements should be string values
        let WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::String(v1))) = &multi.exprs[1]
        else {
            panic!("expected string value");
        };
        assert_eq!(v1, "active");
    }

    #[test]
    fn where_clause_not_in() {
        let result = where_clause_parse("SELECT * FROM t WHERE id NOT IN (1, 2, 3)");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Multi(multi) = where_clause else {
            panic!("expected MultiExpr");
        };

        assert_eq!(multi.op, MultiOp::NotIn);
        assert_eq!(multi.exprs.len(), 4); // column + 3 values
    }

    #[test]
    fn where_clause_in_with_integers() {
        let result = where_clause_parse("SELECT * FROM t WHERE id IN (1, 2, 3)");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Multi(multi) = where_clause else {
            panic!("expected MultiExpr");
        };

        assert_eq!(multi.op, MultiOp::In);

        // Check that values are integers
        let WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::Integer(v1))) = &multi.exprs[1]
        else {
            panic!("expected integer value");
        };
        assert_eq!(*v1, 1);
    }

    #[test]
    fn where_clause_in_combined_with_and() {
        let result = where_clause_parse(
            "SELECT * FROM t WHERE tenant_id = 1 AND status IN ('active', 'pending')",
        );

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        // Should be AND(tenant_id = 1, status IN (...))
        let WhereExpr::Binary(binary) = where_clause else {
            panic!("expected BinaryExpr");
        };

        assert_eq!(binary.op, BinaryOp::And);

        // Right side should be the IN clause
        let WhereExpr::Multi(multi) = binary.rexpr.as_ref() else {
            panic!("expected MultiExpr on right side");
        };
        assert_eq!(multi.op, MultiOp::In);
    }

    #[test]
    fn where_clause_is_null() {
        let result = where_clause_parse("SELECT id FROM test WHERE deleted_at IS NULL");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Unary(unary) = where_clause else {
            panic!("expected UnaryExpr");
        };

        assert_eq!(unary.op, UnaryOp::IsNull);

        let WhereExpr::Scalar(ScalarExpr::Column(col)) = unary.expr.as_ref() else {
            panic!("expected Column");
        };
        assert_eq!(col.column, "deleted_at");
    }

    #[test]
    fn where_clause_is_not_null() {
        let result = where_clause_parse("SELECT id FROM test WHERE name IS NOT NULL");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Unary(unary) = where_clause else {
            panic!("expected UnaryExpr");
        };

        assert_eq!(unary.op, UnaryOp::IsNotNull);

        let WhereExpr::Scalar(ScalarExpr::Column(col)) = unary.expr.as_ref() else {
            panic!("expected Column");
        };
        assert_eq!(col.column, "name");
    }

    #[test]
    fn where_clause_is_true() {
        let result = where_clause_parse("SELECT id FROM test WHERE active IS TRUE");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Unary(unary) = where_clause else {
            panic!("expected UnaryExpr");
        };

        assert_eq!(unary.op, UnaryOp::IsTrue);

        let WhereExpr::Scalar(ScalarExpr::Column(col)) = unary.expr.as_ref() else {
            panic!("expected Column");
        };
        assert_eq!(col.column, "active");
    }

    #[test]
    fn where_clause_is_false() {
        let result = where_clause_parse("SELECT id FROM test WHERE active IS FALSE");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Unary(unary) = where_clause else {
            panic!("expected UnaryExpr");
        };

        assert_eq!(unary.op, UnaryOp::IsFalse);

        let WhereExpr::Scalar(ScalarExpr::Column(col)) = unary.expr.as_ref() else {
            panic!("expected Column");
        };
        assert_eq!(col.column, "active");
    }

    #[test]
    fn where_clause_is_not_true() {
        let result = where_clause_parse("SELECT id FROM test WHERE active IS NOT TRUE");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Unary(unary) = where_clause else {
            panic!("expected UnaryExpr");
        };

        assert_eq!(unary.op, UnaryOp::IsNotTrue);

        let WhereExpr::Scalar(ScalarExpr::Column(col)) = unary.expr.as_ref() else {
            panic!("expected Column");
        };
        assert_eq!(col.column, "active");
    }

    #[test]
    fn where_clause_is_not_false() {
        let result = where_clause_parse("SELECT id FROM test WHERE active IS NOT FALSE");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Unary(unary) = where_clause else {
            panic!("expected UnaryExpr");
        };

        assert_eq!(unary.op, UnaryOp::IsNotFalse);

        let WhereExpr::Scalar(ScalarExpr::Column(col)) = unary.expr.as_ref() else {
            panic!("expected Column");
        };
        assert_eq!(col.column, "active");
    }

    #[test]
    fn where_clause_is_true_combined_with_and() {
        let result = where_clause_parse("SELECT * FROM t WHERE id = 1 AND active IS TRUE");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Binary(binary) = where_clause else {
            panic!("expected BinaryExpr");
        };

        assert_eq!(binary.op, BinaryOp::And);

        let WhereExpr::Unary(unary) = binary.rexpr.as_ref() else {
            panic!("expected UnaryExpr on right side");
        };
        assert_eq!(unary.op, UnaryOp::IsTrue);
    }

    #[test]
    fn where_clause_is_unknown_maps_to_is_null() {
        let result = where_clause_parse("SELECT id FROM test WHERE active IS UNKNOWN");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Unary(unary) = where_clause else {
            panic!("expected UnaryExpr");
        };

        // IS UNKNOWN is semantically identical to IS NULL
        assert_eq!(unary.op, UnaryOp::IsNull);
    }

    #[test]
    fn where_clause_is_not_unknown_maps_to_is_not_null() {
        let result = where_clause_parse("SELECT id FROM test WHERE active IS NOT UNKNOWN");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Unary(unary) = where_clause else {
            panic!("expected UnaryExpr");
        };

        // IS NOT UNKNOWN is semantically identical to IS NOT NULL
        assert_eq!(unary.op, UnaryOp::IsNotNull);
    }

    #[test]
    fn where_clause_is_null_combined_with_and() {
        let result = where_clause_parse("SELECT * FROM t WHERE id = 1 AND deleted_at IS NULL");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        // Should be AND(id = 1, deleted_at IS NULL)
        let WhereExpr::Binary(binary) = where_clause else {
            panic!("expected BinaryExpr");
        };

        assert_eq!(binary.op, BinaryOp::And);

        // Right side should be IS NULL
        let WhereExpr::Unary(unary) = binary.rexpr.as_ref() else {
            panic!("expected UnaryExpr on right side");
        };
        assert_eq!(unary.op, UnaryOp::IsNull);
    }

    #[test]
    fn where_clause_between_integers() {
        let result = where_clause_parse("SELECT * FROM t WHERE id BETWEEN 1 AND 10");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Multi(multi) = where_clause else {
            panic!("expected MultiExpr");
        };

        assert_eq!(multi.op, MultiOp::Between);
        assert_eq!(multi.exprs.len(), 3);

        let WhereExpr::Scalar(ScalarExpr::Column(col)) = &multi.exprs[0] else {
            panic!("expected Column");
        };
        assert_eq!(col.column, "id");

        let WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::Integer(low))) = &multi.exprs[1]
        else {
            panic!("expected integer low bound");
        };
        assert_eq!(*low, 1);

        let WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::Integer(high))) = &multi.exprs[2]
        else {
            panic!("expected integer high bound");
        };
        assert_eq!(*high, 10);
    }

    #[test]
    fn where_clause_not_between() {
        let result = where_clause_parse("SELECT * FROM t WHERE price NOT BETWEEN 100 AND 200");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Multi(multi) = where_clause else {
            panic!("expected MultiExpr");
        };

        assert_eq!(multi.op, MultiOp::NotBetween);
        assert_eq!(multi.exprs.len(), 3);
    }

    #[test]
    fn where_clause_between_with_parameters() {
        let result = where_clause_parse("SELECT * FROM t WHERE created_at BETWEEN $1 AND $2");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Multi(multi) = where_clause else {
            panic!("expected MultiExpr");
        };

        assert_eq!(multi.op, MultiOp::Between);
        assert_eq!(multi.exprs.len(), 3);

        let WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::Parameter(p1))) = &multi.exprs[1]
        else {
            panic!("expected parameter low bound");
        };
        assert_eq!(p1, "$1");

        let WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::Parameter(p2))) = &multi.exprs[2]
        else {
            panic!("expected parameter high bound");
        };
        assert_eq!(p2, "$2");
    }

    #[test]
    fn where_clause_between_combined_with_and() {
        let result =
            where_clause_parse("SELECT * FROM t WHERE tenant_id = 1 AND price BETWEEN 10 AND 50");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Binary(binary) = where_clause else {
            panic!("expected BinaryExpr");
        };

        assert_eq!(binary.op, BinaryOp::And);

        let WhereExpr::Multi(multi) = binary.rexpr.as_ref() else {
            panic!("expected MultiExpr on right side");
        };
        assert_eq!(multi.op, MultiOp::Between);
    }

    #[test]
    fn where_clause_between_strings() {
        let result = where_clause_parse("SELECT * FROM t WHERE name BETWEEN 'alice' AND 'charlie'");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Multi(multi) = where_clause else {
            panic!("expected MultiExpr");
        };

        assert_eq!(multi.op, MultiOp::Between);

        let WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::String(low))) = &multi.exprs[1]
        else {
            panic!("expected string low bound");
        };
        assert_eq!(low, "alice");

        let WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::String(high))) = &multi.exprs[2]
        else {
            panic!("expected string high bound");
        };
        assert_eq!(high, "charlie");
    }

    #[test]
    fn where_clause_between_symmetric() {
        let result = where_clause_parse("SELECT * FROM t WHERE id BETWEEN SYMMETRIC 10 AND 1");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Multi(multi) = where_clause else {
            panic!("expected MultiExpr");
        };

        assert_eq!(multi.op, MultiOp::BetweenSymmetric);
        assert_eq!(multi.exprs.len(), 3);

        let WhereExpr::Scalar(ScalarExpr::Column(col)) = &multi.exprs[0] else {
            panic!("expected Column");
        };
        assert_eq!(col.column, "id");

        let WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::Integer(low))) = &multi.exprs[1]
        else {
            panic!("expected integer low bound");
        };
        assert_eq!(*low, 10);

        let WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::Integer(high))) = &multi.exprs[2]
        else {
            panic!("expected integer high bound");
        };
        assert_eq!(*high, 1);
    }

    #[test]
    fn where_clause_not_between_symmetric() {
        let result = where_clause_parse("SELECT * FROM t WHERE id NOT BETWEEN SYMMETRIC 10 AND 1");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Multi(multi) = where_clause else {
            panic!("expected MultiExpr");
        };

        assert_eq!(multi.op, MultiOp::NotBetweenSymmetric);
        assert_eq!(multi.exprs.len(), 3);
    }

    #[test]
    fn where_clause_any_with_array() {
        let result = where_clause_parse("SELECT * FROM t WHERE id = ANY(ARRAY[1, 2, 3])");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Multi(multi) = where_clause else {
            panic!("expected MultiExpr");
        };

        assert_eq!(
            multi.op,
            MultiOp::Any {
                comparison: BinaryOp::Equal
            }
        );
        // [col, ARRAY[1, 2, 3]]
        assert_eq!(multi.exprs.len(), 2);

        let WhereExpr::Scalar(ScalarExpr::Column(col)) = &multi.exprs[0] else {
            panic!("expected Column");
        };
        assert_eq!(col.column, "id");

        let WhereExpr::Scalar(ScalarExpr::Array(elems)) = &multi.exprs[1] else {
            panic!("expected Array");
        };
        assert_eq!(elems.len(), 3);

        let ScalarExpr::Literal(LiteralValue::Integer(v1)) = &elems[0] else {
            panic!("expected integer");
        };
        assert_eq!(*v1, 1);
    }

    #[test]
    fn where_clause_any_with_parameter() {
        let result = where_clause_parse("SELECT * FROM t WHERE id = ANY($1)");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Multi(multi) = where_clause else {
            panic!("expected MultiExpr");
        };

        assert_eq!(
            multi.op,
            MultiOp::Any {
                comparison: BinaryOp::Equal
            }
        );
        // [col, $1] — parameter passed through as single value
        assert_eq!(multi.exprs.len(), 2);

        let WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::Parameter(p))) = &multi.exprs[1]
        else {
            panic!("expected parameter");
        };
        assert_eq!(p, "$1");
    }

    #[test]
    fn where_clause_all_with_array() {
        let result = where_clause_parse("SELECT * FROM t WHERE score > ALL(ARRAY[80, 90])");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Multi(multi) = where_clause else {
            panic!("expected MultiExpr");
        };

        assert_eq!(
            multi.op,
            MultiOp::All {
                comparison: BinaryOp::GreaterThan
            }
        );
        // [col, ARRAY[80, 90]]
        assert_eq!(multi.exprs.len(), 2);
    }

    #[test]
    fn where_clause_any_not_equal() {
        let result = where_clause_parse("SELECT * FROM t WHERE status <> ANY(ARRAY['a', 'b'])");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Multi(multi) = where_clause else {
            panic!("expected MultiExpr");
        };

        assert_eq!(
            multi.op,
            MultiOp::Any {
                comparison: BinaryOp::NotEqual
            }
        );
        // [col, ARRAY['a', 'b']]
        assert_eq!(multi.exprs.len(), 2);
    }

    #[test]
    fn where_clause_any_combined_with_and() {
        let result =
            where_clause_parse("SELECT * FROM t WHERE tenant_id = 1 AND id = ANY(ARRAY[10, 20])");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Binary(binary) = where_clause else {
            panic!("expected BinaryExpr");
        };

        assert_eq!(binary.op, BinaryOp::And);

        let WhereExpr::Multi(multi) = binary.rexpr.as_ref() else {
            panic!("expected MultiExpr on right side");
        };
        assert_eq!(
            multi.op,
            MultiOp::Any {
                comparison: BinaryOp::Equal
            }
        );
    }

    #[test]
    fn where_clause_not_like() {
        let result = where_clause_parse("SELECT * FROM t WHERE name NOT LIKE '%test%'");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Binary(binary) = where_clause else {
            panic!("expected BinaryExpr");
        };

        assert_eq!(binary.op, BinaryOp::NotLike);
    }

    #[test]
    fn where_clause_ilike() {
        let result = where_clause_parse("SELECT * FROM t WHERE name ILIKE '%test%'");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Binary(binary) = where_clause else {
            panic!("expected BinaryExpr");
        };

        assert_eq!(binary.op, BinaryOp::ILike);
    }

    #[test]
    fn where_clause_not_ilike() {
        let result = where_clause_parse("SELECT * FROM t WHERE name NOT ILIKE '%test%'");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Binary(binary) = where_clause else {
            panic!("expected BinaryExpr");
        };

        assert_eq!(binary.op, BinaryOp::NotILike);
    }

    #[test]
    fn where_clause_like_with_parameter() {
        let result = where_clause_parse("SELECT * FROM t WHERE name LIKE $1");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Binary(binary) = where_clause else {
            panic!("expected BinaryExpr");
        };

        assert_eq!(binary.op, BinaryOp::Like);

        let WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::Parameter(p))) =
            binary.rexpr.as_ref()
        else {
            panic!("expected parameter on right");
        };
        assert_eq!(p, "$1");
    }

    #[test]
    fn where_clause_like_combined_with_and() {
        let result =
            where_clause_parse("SELECT * FROM t WHERE tenant_id = 1 AND name LIKE 'test%'");

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Binary(binary) = where_clause else {
            panic!("expected BinaryExpr");
        };

        assert_eq!(binary.op, BinaryOp::And);

        let WhereExpr::Binary(right) = binary.rexpr.as_ref() else {
            panic!("expected BinaryExpr on right side");
        };
        assert_eq!(right.op, BinaryOp::Like);
    }

    // ------------------------------------------------------------------
    // Arithmetic in WHERE (PGC-118 layer 1)
    //
    // Layer 2 (constant_fold) runs at the end of `query_expr_convert_raw`, so
    // pure-literal arithmetic is folded away before these assertions see
    // the tree. Each test uses at least one non-literal operand (column or
    // parameter) to keep the arithmetic node observable.
    // ------------------------------------------------------------------

    /// Pull the RHS of `WHERE col = <rhs>` so each arithmetic test can
    /// assert on the scalar shape directly.
    fn where_clause_rhs(sql: &str) -> ScalarExpr {
        let WhereExpr::Binary(binary) = where_clause_parse(sql).unwrap().unwrap() else {
            panic!("expected binary WHERE");
        };
        let WhereExpr::Scalar(scalar) = *binary.rexpr else {
            panic!("expected scalar RHS");
        };
        scalar
    }

    #[test]
    fn where_clause_arithmetic_with_column_left() {
        let ScalarExpr::Arithmetic(arith) = where_clause_rhs("SELECT * FROM t WHERE x = a + 1")
        else {
            panic!("expected arithmetic RHS");
        };
        assert_eq!(arith.op, ArithmeticOp::Add);
        assert!(matches!(*arith.left, ScalarExpr::Column(ref c) if c.column == "a"));
        assert!(matches!(
            *arith.right,
            ScalarExpr::Literal(LiteralValue::Integer(1))
        ));
    }

    #[test]
    fn where_clause_arithmetic_modulo_with_parameter() {
        let ScalarExpr::Arithmetic(arith) = where_clause_rhs("SELECT * FROM t WHERE x = $1 % 10")
        else {
            panic!("expected arithmetic RHS");
        };
        assert_eq!(arith.op, ArithmeticOp::Modulo);
        assert!(matches!(
            *arith.left,
            ScalarExpr::Literal(LiteralValue::Parameter(_))
        ));
        assert!(matches!(
            *arith.right,
            ScalarExpr::Literal(LiteralValue::Integer(10))
        ));
    }

    #[test]
    fn where_clause_arithmetic_all_ops() {
        for (sql, expected_op) in [
            ("SELECT * FROM t WHERE x = a + 2", ArithmeticOp::Add),
            ("SELECT * FROM t WHERE x = a - 2", ArithmeticOp::Subtract),
            ("SELECT * FROM t WHERE x = a * 2", ArithmeticOp::Multiply),
            ("SELECT * FROM t WHERE x = a / 2", ArithmeticOp::Divide),
            ("SELECT * FROM t WHERE x = a % 3", ArithmeticOp::Modulo),
        ] {
            let ScalarExpr::Arithmetic(arith) = where_clause_rhs(sql) else {
                panic!("expected arithmetic RHS for {sql}");
            };
            assert_eq!(arith.op, expected_op, "{sql}");
        }
    }

    #[test]
    fn where_clause_arithmetic_nested_with_parameter() {
        // PGC-118 bench query shape, with a parameter to keep arithmetic observable.
        let ScalarExpr::Arithmetic(outer) =
            where_clause_rhs("SELECT * FROM t WHERE user_id = $1 % 10000 + 1")
        else {
            panic!("expected outer arithmetic");
        };
        assert_eq!(outer.op, ArithmeticOp::Add);
        let ScalarExpr::Arithmetic(inner) = &*outer.left else {
            panic!("expected inner arithmetic on left");
        };
        assert_eq!(inner.op, ArithmeticOp::Modulo);
        assert!(matches!(
            *outer.right,
            ScalarExpr::Literal(LiteralValue::Integer(1))
        ));
    }

    #[test]
    fn where_clause_arithmetic_two_columns() {
        let ScalarExpr::Arithmetic(arith) = where_clause_rhs("SELECT * FROM t WHERE x = a + b")
        else {
            panic!("expected arithmetic RHS");
        };
        assert_eq!(arith.op, ArithmeticOp::Add);
        assert!(matches!(*arith.left, ScalarExpr::Column(ref c) if c.column == "a"));
        assert!(matches!(*arith.right, ScalarExpr::Column(ref c) if c.column == "b"));
    }

    #[test]
    fn where_clause_arithmetic_deparse() {
        // Round-trip a non-foldable arithmetic query to confirm Scalar wrapping deparses cleanly.
        let q = query_expr_parse("SELECT * FROM t WHERE x = a % 10000 + 1").unwrap();
        let mut buf = String::new();
        q.deparse(&mut buf);
        // ArithmeticExpr::deparse parenthesizes each level, so nested
        // `a % 10000 + 1` round-trips as `((a % 10000) + 1)`.
        assert_eq!(buf, "SELECT * FROM t WHERE x = ((a % 10000) + 1)");
    }

    #[test]
    fn where_clause_typecast_column() {
        // PGC-120: column cast on the left of a comparison. Must parse as
        // WhereExpr::Scalar(ScalarExpr::TypeCast{...}), not UnsupportedPattern.
        let where_clause = where_clause_parse("SELECT * FROM t WHERE col::text = 'foo'")
            .unwrap()
            .unwrap();
        let WhereExpr::Binary(binary) = &where_clause else {
            panic!("expected Binary, got {where_clause:?}");
        };
        let WhereExpr::Scalar(ScalarExpr::TypeCast { expr, target }) = &*binary.lexpr else {
            panic!("expected Scalar(TypeCast), got {:?}", binary.lexpr);
        };
        assert_eq!(*target, crate::query::cast::CastTarget::Text);
        assert!(matches!(&**expr, ScalarExpr::Column(c) if c.column == "col"));
    }

    #[test]
    fn where_clause_typecast_deparse() {
        // PGC-120: round-trip a few common cast shapes through Deparse.
        for sql in [
            "SELECT * FROM t WHERE col::text = 'foo'",
            "SELECT * FROM t WHERE created_at::date = '2024-01-01'",
            "SELECT * FROM t WHERE (a + b)::int > 10",
        ] {
            let q =
                query_expr_parse(sql).unwrap_or_else(|e| panic!("convert failed for {sql}: {e}"));
            let mut buf = String::new();
            q.deparse(&mut buf);
            // Re-parse the deparsed SQL to confirm semantic round-trip.
            let q2 = query_expr_parse(&buf)
                .unwrap_or_else(|e| panic!("re-convert failed for {buf:?}: {e}"));
            assert_eq!(
                query_expr_fingerprint(&q),
                query_expr_fingerprint(&q2),
                "fingerprint mismatch after deparse round-trip\n  in:  {sql}\n  out: {buf}",
            );
        }
    }
}
