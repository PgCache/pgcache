//! Compiled WHERE predicate for the LocalEval CDC membership hot path (PGC-339).
//!
//! `where_expr_evaluate` walks the resolved WHERE AST per CDC row per candidate
//! query, re-destructuring each fixed comparison via `canonicalize_comparison`
//! and dereferencing the boxed AST + column metadata every time. For a registered
//! query that work is constant — only the row value changes. `CompiledPredicate`
//! hoists it to registration: the column position, in-table flag, cast target,
//! operator, and literal are resolved once into a flat node, leaving only the
//! row-value lookup + compare per row. The comparison semantics are unchanged —
//! leaves reuse `typed_compare` / `cast_target_coerce_text` /
//! `literal_compare`, so a compiled predicate evaluates identically to
//! `where_expr_evaluate` for the expression it was compiled from.

use crate::pg::protocol::ByteString;
use crate::query::ast::rope::{ScalarKind, Where, WhereKind};
use crate::query::ast::{BinaryOp, LiteralValue, UnaryOp};
use crate::query::cast::{CastTarget, cast_target_coerce_text, comparison_canonicalize_tree};
use crate::query::evaluate::literal_compare;
use crate::query::resolve::ResolvedQuery;
use crate::query::typed_compare::TypedLiteral;

/// Pre-canonicalized form of a LocalEval WHERE clause. Built once at registration
/// from a `ResolvedWhereExpr`; evaluated per CDC row. A total mapping of every
/// shape `where_expr_evaluate` accepts — unsupported shapes compile to
/// `ConstFalse`, mirroring its `_ => false` / Like arms.
#[derive(Debug, Clone)]
pub enum CompiledPredicate {
    And(Box<CompiledPredicate>, Box<CompiledPredicate>),
    Or(Box<CompiledPredicate>, Box<CompiledPredicate>),
    Not(Box<CompiledPredicate>),
    /// IS [NOT] NULL / IS [NOT] TRUE / IS [NOT] FALSE. `col` is the row position
    /// of the inner column when it is an in-table `Scalar(Column)`; `None`
    /// otherwise — the inner value is then unconditionally absent, matching
    /// `unary_expr_evaluate`'s `Some(Scalar(Column))`-only value lookup.
    NullCheck {
        op: UnaryOp,
        col: Option<usize>,
    },
    /// `column op literal`, canonical (column-on-LHS) form. `in_table` is the
    /// `col.table == table_name` test resolved at build time.
    Compare {
        col: usize,
        in_table: bool,
        target: Option<CastTarget>,
        op: BinaryOp,
        value: LiteralValue,
        /// `value` parsed once for the column's type, for the uncast
        /// comparison against each row's text.
        literal: TypedLiteral,
    },
    /// Any shape `where_expr_evaluate` decides `false` for without inspecting the
    /// row: Like-family, Multi, Subquery, a bare Scalar leaf, or a comparison
    /// that does not canonicalize to `column op literal`.
    ConstFalse,
}

impl CompiledPredicate {
    /// Compile a WHERE expression of `bound` for rows of `table_name`. Total —
    /// every input maps to a node.
    pub fn compile(bound: &ResolvedQuery, expr: Where<'_>, table_name: &str) -> Self {
        match expr.kind() {
            WhereKind::Binary(binary) => match binary.op() {
                BinaryOp::And => Self::And(
                    Box::new(Self::compile(bound, binary.lexpr(), table_name)),
                    Box::new(Self::compile(bound, binary.rexpr(), table_name)),
                ),
                BinaryOp::Or => Self::Or(
                    Box::new(Self::compile(bound, binary.lexpr(), table_name)),
                    Box::new(Self::compile(bound, binary.rexpr(), table_name)),
                ),
                BinaryOp::Equal
                | BinaryOp::NotEqual
                | BinaryOp::LessThan
                | BinaryOp::LessThanOrEqual
                | BinaryOp::GreaterThan
                | BinaryOp::GreaterThanOrEqual => {
                    let compare = comparison_canonicalize_tree(bound, binary).and_then(
                        |(column, target, op, value)| {
                            let binding = bound.column_binding_at(column.id())?;
                            let table = bound.column_table_at(column.id())?;
                            let data_type = bound.column_type_at(column.id())?;
                            Some(Self::Compare {
                                col: binding.position.index(),
                                in_table: table.name.as_str() == table_name,
                                target: target.cloned(),
                                op,
                                value: value.clone(),
                                literal: TypedLiteral::new(data_type, value),
                            })
                        },
                    );
                    compare.unwrap_or(Self::ConstFalse)
                }
                BinaryOp::Like | BinaryOp::ILike | BinaryOp::NotLike | BinaryOp::NotILike => {
                    Self::ConstFalse
                }
            },
            WhereKind::Unary(unary) => match unary.op() {
                UnaryOp::Not => Self::Not(Box::new(Self::compile(bound, unary.expr(), table_name))),
                op @ (UnaryOp::IsNull
                | UnaryOp::IsNotNull
                | UnaryOp::IsTrue
                | UnaryOp::IsNotTrue
                | UnaryOp::IsFalse
                | UnaryOp::IsNotFalse) => Self::NullCheck {
                    op,
                    col: column_position(bound, unary.expr(), table_name),
                },
            },
            WhereKind::Scalar(_) | WhereKind::Multi(_) | WhereKind::Subquery(_) => Self::ConstFalse,
        }
    }

    /// Evaluate against a single row. Identical result to
    /// `where_expr_evaluate(expr, row_data, table_name)` for the `expr` this was
    /// compiled from.
    pub fn eval(&self, row_data: &[Option<ByteString>]) -> bool {
        match self {
            Self::And(l, r) => l.eval(row_data) && r.eval(row_data),
            Self::Or(l, r) => l.eval(row_data) || r.eval(row_data),
            Self::Not(inner) => !inner.eval(row_data),
            Self::NullCheck { op, col } => {
                let value = col.and_then(|pos| row_data.get(pos).and_then(|v| v.as_deref()));
                match op {
                    UnaryOp::IsNull => value.is_none(),
                    UnaryOp::IsNotNull => value.is_some(),
                    UnaryOp::IsTrue => matches!(value, Some("t" | "true")),
                    UnaryOp::IsNotTrue => !matches!(value, Some("t" | "true")),
                    UnaryOp::IsFalse => matches!(value, Some("f" | "false")),
                    UnaryOp::IsNotFalse => !matches!(value, Some("f" | "false")),
                    // `Not` compiles to `CompiledPredicate::Not`, never NullCheck.
                    UnaryOp::Not => false,
                }
            }
            Self::Compare {
                col,
                in_table,
                target,
                op,
                value,
                literal,
            } => {
                if !in_table {
                    return false; // column belongs to another table (NotInTable)
                }
                match row_data.get(*col) {
                    None => false, // position past row end (NotInTable)
                    Some(None) => {
                        // row value is NULL: only `col = NULL` with no cast matches,
                        // mirroring `expr_comparison_evaluate`'s Null arm.
                        target.is_none()
                            && matches!(op, BinaryOp::Equal)
                            && matches!(value, LiteralValue::Null)
                    }
                    Some(Some(bytes)) => {
                        let row_str = bytes.as_str();
                        match target {
                            None => literal.row_compare(row_str, *op).unwrap_or(false),
                            Some(target) => match cast_target_coerce_text(target, row_str) {
                                Some(coerced) => literal_compare(&coerced, *op, value),
                                None => false,
                            },
                        }
                    }
                }
            }
            Self::ConstFalse => false,
        }
    }
}

/// Row position of `expr` when it is an in-table bare column reference, matching
/// the value `unary_expr_evaluate` reads for IS NULL/TRUE/FALSE checks: `None`
/// for any non-column inner, or a column belonging to another table.
fn column_position(bound: &ResolvedQuery, expr: Where<'_>, table_name: &str) -> Option<usize> {
    let WhereKind::Scalar(scalar) = expr.kind() else {
        return None;
    };
    let ScalarKind::Column(column) = scalar.kind() else {
        return None;
    };
    let table = bound.column_table_at(column.id())?;
    if table.name.as_str() != table_name {
        return None;
    }
    Some(bound.column_binding_at(column.id())?.position.index())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use iddqd::BiHashMap;
    use postgres_types::Type;

    use super::*;
    use crate::catalog::{Catalog, ColumnMetadata, ColumnPosition, ColumnStore, TableMetadata};
    use crate::oid::{Oid, TypeOid};
    use crate::query::ast::query_expr_parse;
    use crate::query::evaluate::where_expr_supported;
    use crate::query::resolve::{ResolvedQuery, query_expr_resolve};

    // ------------------------------------------------------------------
    // Row evaluation. Each predicate is parsed and bound as the writer
    // would see it, compiled for one table, and evaluated against rows of
    // that table given as wire text.
    // ------------------------------------------------------------------

    /// `test_table(id int4, name text, active bool, price float8)`,
    /// `ts_table(id int4, name text, created_at timestamp, received_at timestamptz)`,
    /// `other_table(id int4, name text)`.
    fn fixture_catalog() -> Catalog {
        let column = |name: &str, index: usize, ty: Type| ColumnMetadata {
            name: name.into(),
            position: ColumnPosition::from_index(index).expect("position"),
            type_oid: TypeOid::from_type(&ty),
            type_name: ty.name().into(),
            cache_type_name: ty.name().into(),
            data_type: ty,
            is_primary_key: index == 0,
        };
        let table = |name: &str, oid: u32, columns: Vec<ColumnMetadata>| TableMetadata {
            replica_identity_full: false,
            relation_oid: Oid::from_raw(oid),
            name: name.into(),
            schema: "public".into(),
            primary_key_columns: vec!["id".into()],
            columns: ColumnStore::new(columns),
            indexes: Vec::new(),
        };
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(
            table(
                "test_table",
                12345,
                vec![
                    column("id", 0, Type::INT4),
                    column("name", 1, Type::TEXT),
                    column("active", 2, Type::BOOL),
                    column("price", 3, Type::FLOAT8),
                ],
            )
            .into(),
        );
        tables.insert_overwrite(
            table(
                "ts_table",
                23456,
                vec![
                    column("id", 0, Type::INT4),
                    column("name", 1, Type::TEXT),
                    column("created_at", 2, Type::TIMESTAMP),
                    column("received_at", 3, Type::TIMESTAMPTZ),
                ],
            )
            .into(),
        );
        tables.insert_overwrite(
            table(
                "other_table",
                34567,
                vec![column("id", 0, Type::INT4), column("name", 1, Type::TEXT)],
            )
            .into(),
        );
        tables
    }

    fn fixture_bound(sql: &str) -> ResolvedQuery {
        let query = Arc::new(query_expr_parse(sql).expect("parse"));
        let bindings =
            query_expr_resolve(&query, &fixture_catalog(), &["public"]).expect("resolve");
        ResolvedQuery::new(query, bindings)
    }

    /// The WHERE of `sql` compiled for rows of `table`.
    fn compiled(sql: &str, table: &str) -> CompiledPredicate {
        let bound = fixture_bound(sql);
        let select = bound.rope_select().expect("a SELECT");
        let where_clause = select.where_clause().expect("a WHERE");
        CompiledPredicate::compile(&bound, where_clause, table)
    }

    /// Whether the WHERE of `sql` is one the compiled predicate decides.
    fn supported(sql: &str) -> bool {
        let bound = fixture_bound(sql);
        let select = bound.rope_select().expect("a SELECT");
        let where_clause = select.where_clause().expect("a WHERE");
        where_expr_supported(&bound, where_clause)
    }

    fn row(values: &[Option<&str>]) -> Vec<Option<ByteString>> {
        values.iter().map(|v| v.map(ByteString::from)).collect()
    }

    /// `predicate` over a `test_table` row `[id, name, active, price]`.
    fn matches(predicate: &str, values: &[Option<&str>]) -> bool {
        compiled(
            &format!("SELECT * FROM test_table WHERE {predicate}"),
            "test_table",
        )
        .eval(&row(values))
    }

    /// `predicate` over a `ts_table` row `[id, name, created_at, received_at]`.
    fn ts_matches(predicate: &str, values: &[Option<&str>]) -> bool {
        compiled(
            &format!("SELECT * FROM ts_table WHERE {predicate}"),
            "ts_table",
        )
        .eval(&row(values))
    }

    const JOHN: &[Option<&str>] = &[Some("1"), Some("john"), Some("true")];

    #[test]
    fn test_comparison_cases() {
        // Rows are `[id, 'john', 'true']`, or a NULL name. A NULL compares
        // equal to NULL and nothing else.
        #[rustfmt::skip]
        let cases = [
            ("name = 'john'",      Some("1"),   "name = 'john'",      true),
            ("name = 'jane'",      Some("1"),   "name = 'jane'",      false),
            ("id = 123",           Some("123"), "id = 123",           true),
            ("NULL name = NULL",   None,        "name = NULL",        true),
            ("'john' = name",      Some("1"),   "'john' = name",      true),
            ("name <> 'jane'",     Some("1"),   "name <> 'jane'",     true),
            ("name <> 'john'",     Some("1"),   "name <> 'john'",     false),
            ("id 50 < 100",        Some("50"),  "id < 100",           true),
            ("id 150 < 100",       Some("150"), "id < 100",           false),
            ("id 100 <= 100",      Some("100"), "id <= 100",          true),
            ("id 50 <= 100",       Some("50"),  "id <= 100",          true),
            ("id 150 <= 100",      Some("150"), "id <= 100",          false),
            ("id 150 > 100",       Some("150"), "id > 100",           true),
            ("id 50 > 100",        Some("50"),  "id > 100",           false),
            ("id 100 >= 100",      Some("100"), "id >= 100",          true),
            ("id 150 >= 100",      Some("150"), "id >= 100",          true),
            ("id 50 >= 100",       Some("50"),  "id >= 100",          false),
            ("'john' < 'zebra'",   Some("1"),   "name < 'zebra'",     true),
            ("'john' > 'alice'",   Some("1"),   "name > 'alice'",     true),
            ("NULL name > 'test'", None,        "name > 'test'",      false),
        ];
        for (label, id, predicate, expected) in cases {
            let values = match id {
                Some(id) => [Some(id), Some("john"), Some("true")],
                None => [Some("1"), None, Some("true")],
            };
            assert_eq!(matches(predicate, &values), expected, "{label}");
        }
    }

    #[test]
    fn test_connective_cases() {
        let values = [Some("123"), Some("john"), Some("true")];
        #[rustfmt::skip]
        let cases = [
            ("AND both true",  "id = 123 AND name = 'john'", true),
            ("AND one false",  "id = 999 AND name = 'john'", false),
            ("OR one true",    "id = 999 OR name = 'john'",  true),
            ("OR both false",  "id = 999 OR name = 'jane'",  false),
            ("NOT",            "NOT (id = 999)",             true),
            ("greater than",   "id > 100",                   true),
        ];
        for (label, predicate, expected) in cases {
            assert_eq!(matches(predicate, &values), expected, "{label}");
        }
    }

    /// Shapes the compiler cannot decide never match: the CDC path then
    /// asks the cache database.
    #[test]
    fn test_undecidable_shapes_never_match() {
        for predicate in ["upper(name)", "name LIKE 'j%'", "id", "id IN (1, 2)"] {
            assert!(!matches(predicate, JOHN), "{predicate}");
        }
    }

    /// An identity cast (`::text` on text or integer columns) reads the
    /// wire text as is; on a bool column it is not identity (`t` vs
    /// `true`) and stays opaque.
    #[test]
    fn test_identity_text_cast_cases() {
        #[rustfmt::skip]
        let cases = [
            ("text column matches",      "name::text = 'john'",   &[Some("1"), Some("john"), Some("true")][..],  true),
            ("text column differs",      "name::text = 'john'",   &[Some("1"), Some("alice"), Some("true")][..], false),
            ("int column matches",       "id::text = '42'",       &[Some("42"), Some("john"), Some("true")][..], true),
            ("bool column is opaque",    "active::text = 'true'", &[Some("1"), Some("john"), Some("t")][..],     false),
            ("cast on the right",        "'john' = name::text",   &[Some("1"), Some("john"), Some("true")][..],  true),
        ];
        for (label, predicate, values, expected) in cases {
            assert_eq!(matches(predicate, values), expected, "{label}");
        }
        assert!(supported(
            "SELECT * FROM test_table WHERE name::text = 'john'"
        ));
        assert!(!supported(
            "SELECT * FROM test_table WHERE active::text = 'true'"
        ));
    }

    /// `text_col::int4 op literal` compares numerically; an unparseable
    /// row is excluded, as the cast would raise in PostgreSQL.
    #[test]
    fn test_text_to_integer_coercion_cases() {
        let named = |name: &'static str| [Some("1"), Some(name), Some("true")];
        #[rustfmt::skip]
        let cases = [
            ("matches",              "name::int4 = 42",                     named("42"),                  true),
            ("differs",              "name::int4 = 99",                     named("42"),                  false),
            ("unparseable row",      "name::int4 = 42",                     named("abc"),                 false),
            ("string literal",       "name::int4 = '42'",                   named("42"),                  true),
            ("numeric inequality",   "name::int4 > 42",                     named("100"),                 true),
            ("int8 cast",            "name::int8 = 2147483647",             named("2147483647"),          true),
        ];
        for (label, predicate, values, expected) in cases {
            assert_eq!(matches(predicate, &values), expected, "{label}");
        }
        assert!(supported("SELECT * FROM test_table WHERE name::int4 = 42"));
        assert!(!supported(
            "SELECT * FROM test_table WHERE active::int4 = 1"
        ));
    }

    /// `WHERE 5 < col` evaluates as `col > 5`, on the bare and the cast
    /// paths alike.
    #[test]
    fn test_literal_first_comparison_flips() {
        let id_ten = [Some("10"), Some("john"), Some("true")];
        #[rustfmt::skip]
        let cases = [
            ("5 < id",          "5 < id",          true),
            ("5 > id",          "5 > id",          false),
            ("100 < id",        "100 < id",        false),
            ("10 >= id",        "10 >= id",        true),
        ];
        for (label, predicate, expected) in cases {
            assert_eq!(matches(predicate, &id_ten), expected, "{label}");
        }
        assert!(matches(
            "5 < name::int4",
            &[Some("1"), Some("10"), Some("true")]
        ));
    }

    #[test]
    fn test_text_to_bool_coercion_cases() {
        let named = |name: &'static str| [Some("1"), Some(name), Some("true")];
        #[rustfmt::skip]
        let cases = [
            ("matches",            "name::bool = true",   named("true"),    true),
            ("differs",            "name::bool = true",   named("false"),   false),
            ("string literal",     "name::bool = 't'",    named("true"),    true),
            ("integer literal",    "name::bool = 1",      named("true"),    true),
            ("inequality rejected", "name::bool < true",  named("true"),    false),
            ("unparseable row",    "name::bool = true",   named("garbage"), false),
        ];
        for (label, predicate, values, expected) in cases {
            assert_eq!(matches(predicate, &values), expected, "{label}");
        }
        for (stored, literal, expected) in [
            ("t", "true", true),
            ("yes", "true", true),
            ("1", "true", true),
            ("on", "true", true),
            ("f", "false", true),
            ("no", "false", true),
            ("0", "false", true),
            ("off", "false", true),
            ("t", "false", false),
            ("garbage", "true", false),
        ] {
            assert_eq!(
                matches(&format!("name::bool = {literal}"), &named(stored)),
                expected,
                "stored {stored:?} = {literal}"
            );
        }
        assert!(supported(
            "SELECT * FROM test_table WHERE name::bool = true"
        ));
    }

    /// `timestamp::date op 'YYYY-MM-DD'` compares by calendar day. A
    /// timestamptz column, a non-canonical spelling, a non-string literal
    /// or a cast on the literal stay on PgEval.
    #[test]
    fn test_timestamp_to_date_coercion_cases() {
        let at = |created: &'static str| [Some("1"), Some("alice"), Some(created), None];
        #[rustfmt::skip]
        let cases = [
            ("same day",            "created_at::date = '2024-01-15'",        at("2024-01-15 23:45:00"), true),
            ("next day",            "created_at::date = '2024-01-16'",        at("2024-01-15 23:45:00"), false),
            ("chronological",       "created_at::date > '2024-01-31'",        at("2024-03-15 09:00:00"), true),
            ("literal first",       "'2024-01-01' < created_at::date",        at("2024-03-15 09:00:00"), true),
        ];
        for (label, predicate, values, expected) in cases {
            assert_eq!(ts_matches(predicate, &values), expected, "{label}");
        }
        #[rustfmt::skip]
        let classified = [
            ("timestamp to date",       "SELECT * FROM ts_table WHERE created_at::date = '2024-01-15'",  true),
            ("timestamptz to date",     "SELECT * FROM ts_table WHERE received_at::date = '2024-01-15'", false),
            ("non-canonical literal",   "SELECT * FROM ts_table WHERE created_at::date = '2024-1-15'",   false),
            ("non-string literal",      "SELECT * FROM ts_table WHERE created_at::date = 20240115",      false),
            // The literal's own cast stays a cast node, which the compiler
            // does not read through.
            ("cast on the literal",     "SELECT * FROM ts_table WHERE created_at::date = '2024-01-15'::date", false),
        ];
        for (label, sql, expected) in classified {
            assert_eq!(supported(sql), expected, "{label}");
        }
    }

    #[test]
    fn test_boolean_test_cases() {
        let active = |value: Option<&'static str>| [Some("1"), Some("john"), value];
        #[rustfmt::skip]
        let cases = [
            ("IS TRUE of t",          "active IS TRUE",      Some("t"), true),
            ("IS TRUE of f",          "active IS TRUE",      Some("f"), false),
            ("IS TRUE of NULL",       "active IS TRUE",      None,      false),
            ("IS FALSE of f",         "active IS FALSE",     Some("f"), true),
            ("IS FALSE of t",         "active IS FALSE",     Some("t"), false),
            ("IS NOT TRUE of f",      "active IS NOT TRUE",  Some("f"), true),
            ("IS NOT TRUE of NULL",   "active IS NOT TRUE",  None,      true),
            ("IS NOT FALSE of t",     "active IS NOT FALSE", Some("t"), true),
            ("IS NOT FALSE of NULL",  "active IS NOT FALSE", None,      true),
            ("IS NULL of NULL",       "active IS NULL",      None,      true),
            ("IS NOT NULL of t",      "active IS NOT NULL",  Some("t"), true),
        ];
        for (label, predicate, value, expected) in cases {
            assert_eq!(matches(predicate, &active(value)), expected, "{label}");
        }
    }

    #[test]
    fn test_float_comparison() {
        let priced = [Some("1"), Some("john"), Some("true"), Some("99.50")];
        assert!(matches("price < 100.0", &priced));
        assert!(matches("price > 50.0", &priced));
    }

    /// A column of another table never matches a comparison, and reads as
    /// NULL for IS NULL, so a one-table row cannot satisfy a join row test.
    #[test]
    fn test_cross_table_column_cases() {
        let sql =
            |predicate: &str| format!("SELECT * FROM test_table, other_table WHERE {predicate}");
        assert!(!compiled(&sql("other_table.id = 1"), "test_table").eval(&row(JOHN)));
        assert!(compiled(&sql("other_table.id IS NULL"), "test_table").eval(&row(JOHN)));
    }

    /// What the compiled predicate decides, as the registration classifier
    /// asks before choosing local evaluation.
    #[test]
    fn test_supported_cases() {
        #[rustfmt::skip]
        let cases = [
            ("bare equality",           "SELECT * FROM test_table WHERE id = 5",                                         true),
            ("nested AND/OR",           "SELECT * FROM test_table WHERE (id = 1 OR id = 2) AND name IS NOT NULL",        true),
            ("IS NULL",                 "SELECT * FROM test_table WHERE active IS NULL",                                 true),
            ("IS TRUE",                 "SELECT * FROM test_table WHERE active IS TRUE",                                 true),
            ("LIKE",                    "SELECT * FROM test_table WHERE name LIKE 'j%'",                                 false),
            ("column to column",        "SELECT * FROM test_table WHERE id = id",                                        false),
            ("IN list",                 "SELECT * FROM test_table WHERE id IN (1, 2)",                                   false),
            ("function",                "SELECT * FROM test_table WHERE upper(name)",                                    false),
            ("subquery",                "SELECT * FROM test_table WHERE EXISTS (SELECT 1 FROM other_table)",             false),
            ("AND with LIKE",           "SELECT * FROM test_table WHERE id = 1 AND name LIKE 'j%'",                      false),
        ];
        for (label, sql, expected) in cases {
            assert_eq!(supported(sql), expected, "{label}");
        }
    }
}
