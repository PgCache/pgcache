//! Canonicalise the date-time literals of a resolved query under the session
//! settings they were written in (PGC-287 class 2), so the resolved form —
//! and the identity, population, serve and CDC evaluation derived from it —
//! means the same rows whatever session registered it.
//!
//! A literal is rewritten where its type is known: compared to a column, a
//! member of a column's IN / ANY list or BETWEEN bounds, or cast to a
//! date-time type. A literal the canonicaliser does not handle, or one in a
//! position whose type it cannot see and whose text could carry a date-time
//! meaning, is left as written and reported, so the entry is served only to
//! sessions on the reference settings it will be populated under.

use crate::query::ast::{LiteralValue, ValuesClause};
use crate::query::cast::{CastTarget, cast_target_from_canonical};
use crate::query::literal_settings::LiteralSettings;
use crate::query::resolved::{
    ResolvedCaseExpr, ResolvedFunctionCall, ResolvedJoinQual, ResolvedQueryBody, ResolvedQueryExpr,
    ResolvedScalarExpr, ResolvedSelectColumns, ResolvedSelectNode, ResolvedTableSource,
    ResolvedWhereExpr,
};
use crate::query::typed_compare::{ValueKind, literal_canonical_text};

/// What the pass found.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LiteralCanonOutcome {
    /// A literal was left as written whose meaning the populating session's
    /// settings decide.
    pub residual: bool,
}

/// Canonicalise every date-time literal of `resolved` under `settings`.
pub fn resolved_literals_canonicalise(
    resolved: &mut ResolvedQueryExpr,
    settings: &LiteralSettings,
) -> LiteralCanonOutcome {
    let mut pass = Pass {
        settings,
        outcome: LiteralCanonOutcome::default(),
    };
    pass.query(resolved);
    pass.outcome
}

struct Pass<'a> {
    settings: &'a LiteralSettings,
    outcome: LiteralCanonOutcome,
}

impl Pass<'_> {
    fn query(&mut self, query: &mut ResolvedQueryExpr) {
        match &mut query.body {
            ResolvedQueryBody::Select(select) => self.select(select),
            ResolvedQueryBody::SetOp(set_op) => {
                self.query(&mut set_op.left);
                self.query(&mut set_op.right);
            }
            ResolvedQueryBody::Values(values) => self.values(values),
        }
        for clause in &mut query.order_by {
            self.scalar(&mut clause.expr, None);
        }
    }

    fn select(&mut self, select: &mut ResolvedSelectNode) {
        for source in &mut select.from {
            self.source(source);
        }
        if let Some(where_clause) = &mut select.where_clause {
            self.where_expr(where_clause);
        }
        if let Some(having) = &mut select.having {
            self.where_expr(having);
        }
        if let ResolvedSelectColumns::Columns(columns) = &mut select.columns {
            for column in columns {
                self.scalar(&mut column.expr, None);
            }
        }
    }

    fn values(&mut self, values: &mut ValuesClause) {
        for row in &mut values.rows {
            for literal in row {
                self.literal(literal, None);
            }
        }
    }

    fn source(&mut self, source: &mut ResolvedTableSource) {
        match source {
            ResolvedTableSource::Table(_) => {}
            ResolvedTableSource::Subquery(subquery) => self.query(&mut subquery.query),
            ResolvedTableSource::Join(join) => {
                self.source(&mut join.left);
                self.source(&mut join.right);
                if let ResolvedJoinQual::On(on) = &mut join.qual {
                    self.where_expr(on);
                }
            }
        }
    }

    fn where_expr(&mut self, expr: &mut ResolvedWhereExpr) {
        match expr {
            ResolvedWhereExpr::Scalar(scalar) => self.scalar(scalar, None),
            ResolvedWhereExpr::Unary(unary) => self.where_expr(&mut unary.expr),
            ResolvedWhereExpr::Binary(binary) => {
                let kind = column_kind(&binary.lexpr).or_else(|| column_kind(&binary.rexpr));
                self.operand(&mut binary.lexpr, kind);
                self.operand(&mut binary.rexpr, kind);
            }
            ResolvedWhereExpr::Multi(multi) => {
                let kind = multi.exprs.first().and_then(column_kind);
                for (i, member) in multi.exprs.iter_mut().enumerate() {
                    self.operand(member, kind.filter(|_| i > 0));
                }
            }
            ResolvedWhereExpr::Subquery {
                query, test_expr, ..
            } => {
                if let Some(test) = test_expr {
                    self.scalar(test, None);
                }
                self.query(query);
            }
        }
    }

    /// One side of a comparison against a column of `kind` (when known).
    fn operand(&mut self, side: &mut ResolvedWhereExpr, kind: Option<ValueKind>) {
        if let ResolvedWhereExpr::Scalar(scalar) = side {
            self.scalar(scalar, kind);
        } else {
            self.where_expr(side);
        }
    }

    /// `kind` is the type the enclosing comparison gives a literal here.
    fn scalar(&mut self, expr: &mut ResolvedScalarExpr, kind: Option<ValueKind>) {
        match expr {
            ResolvedScalarExpr::Literal(literal) => self.literal(literal, kind),
            ResolvedScalarExpr::TypeCast {
                expr: inner,
                target,
            } => {
                let cast = cast_kind(target);
                if let ResolvedScalarExpr::Literal(literal) = inner.as_mut() {
                    self.literal(literal, cast);
                    self.mixed_kinds_check(kind, cast);
                } else {
                    self.scalar(inner, None);
                }
            }
            // An array under `= ANY (...)` takes the column's kind per element.
            ResolvedScalarExpr::Array(elems) => {
                for elem in elems {
                    self.scalar(elem, kind);
                }
            }
            ResolvedScalarExpr::Function(function) => self.function(function),
            ResolvedScalarExpr::Case(case) => self.case(case),
            ResolvedScalarExpr::Arithmetic(arith) => {
                self.scalar(&mut arith.left, None);
                self.scalar(&mut arith.right, None);
            }
            ResolvedScalarExpr::Subquery(query, _) => self.query(query),
            ResolvedScalarExpr::Column(_) | ResolvedScalarExpr::Identifier(_) => {}
        }
    }

    fn function(&mut self, function: &mut ResolvedFunctionCall) {
        for arg in &mut function.args {
            self.scalar(arg, None);
        }
        if let Some(filter) = &mut function.agg_filter {
            self.where_expr(filter);
        }
        for clause in &mut function.agg_order {
            self.scalar(&mut clause.expr, None);
        }
    }

    fn case(&mut self, case: &mut ResolvedCaseExpr) {
        if let Some(arg) = &mut case.arg {
            self.scalar(arg, None);
        }
        for when in &mut case.whens {
            self.where_expr(&mut when.condition);
            self.scalar(&mut when.result, None);
        }
        if let Some(default) = &mut case.default {
            self.scalar(default, None);
        }
    }

    /// Rewrite one literal of `kind`; `None` is a position whose type the
    /// pass cannot see.
    fn literal(&mut self, literal: &mut LiteralValue, kind: Option<ValueKind>) {
        match literal {
            LiteralValue::String(text) => self.text(text, kind),
            LiteralValue::StringWithCast(text, cast) => {
                // The literal's own cast names its kind.
                let cast = cast_kind(&cast_target_from_canonical(cast));
                self.text(text, cast);
                self.mixed_kinds_check(kind, cast);
            }
            LiteralValue::Array(elems, cast) => {
                let element = cast
                    .strip_suffix("[]")
                    .map(|name| cast_kind(&cast_target_from_canonical(name)))
                    .unwrap_or(kind);
                for elem in elems {
                    self.literal(elem, element);
                }
            }
            LiteralValue::Integer(_)
            | LiteralValue::Float(_)
            | LiteralValue::Boolean(_)
            | LiteralValue::Null
            | LiteralValue::NullWithCast(_)
            | LiteralValue::Parameter(_) => {}
        }
    }

    fn text(&mut self, text: &mut ecow::EcoString, kind: Option<ValueKind>) {
        match kind {
            Some(kind @ (ValueKind::Date | ValueKind::Timestamp | ValueKind::TimestampTz)) => {
                match literal_canonical_text(kind, text, self.settings) {
                    Some(canonical) => *text = canonical.into(),
                    None => self.outcome.residual = true,
                }
            }
            Some(
                ValueKind::Integer
                | ValueKind::Decimal
                | ValueKind::Bool
                | ValueKind::Text
                | ValueKind::Bpchar
                | ValueKind::Uuid
                | ValueKind::Enum,
            ) => {}
            Some(ValueKind::Unsupported) | None => {
                if literal_settings_sensitive(text) {
                    self.outcome.residual = true;
                }
            }
        }
    }

    /// A canonical literal of one date-time kind compared to a column of
    /// another is converted at population, and date ↔ timestamptz (or
    /// timestamp ↔ timestamptz) conversion uses the session zone.
    fn mixed_kinds_check(&mut self, column: Option<ValueKind>, literal: Option<ValueKind>) {
        let (Some(column), Some(literal)) = (column, literal) else {
            return;
        };
        if column == literal {
            return;
        }
        if column == ValueKind::TimestampTz || literal == ValueKind::TimestampTz {
            self.outcome.residual = true;
        }
    }
}

/// The kind of the column a comparison side names, looking through a cast.
fn column_kind(side: &ResolvedWhereExpr) -> Option<ValueKind> {
    let ResolvedWhereExpr::Scalar(scalar) = side else {
        return None;
    };
    scalar_column_kind(scalar)
}

fn scalar_column_kind(scalar: &ResolvedScalarExpr) -> Option<ValueKind> {
    match scalar {
        ResolvedScalarExpr::Column(column) => {
            Some(ValueKind::of_type(&column.column_metadata.data_type))
        }
        ResolvedScalarExpr::TypeCast { expr, target } => {
            cast_kind(target).or_else(|| scalar_column_kind(expr))
        }
        ResolvedScalarExpr::Identifier(_)
        | ResolvedScalarExpr::Literal(_)
        | ResolvedScalarExpr::Function(_)
        | ResolvedScalarExpr::Case(_)
        | ResolvedScalarExpr::Arithmetic(_)
        | ResolvedScalarExpr::Subquery(..)
        | ResolvedScalarExpr::Array(_) => None,
    }
}

/// The kind a cast target names. A type whose text input no tracked setting
/// changes reads as `Text`, so a literal cast to it is never residual.
fn cast_kind(target: &CastTarget) -> Option<ValueKind> {
    Some(match target {
        CastTarget::Text => ValueKind::Text,
        CastTarget::Int4 | CastTarget::Int8 => ValueKind::Integer,
        CastTarget::Bool => ValueKind::Bool,
        CastTarget::Date => ValueKind::Date,
        CastTarget::Other(name) => {
            let kind = ValueKind::of_cast_name(name);
            if kind == ValueKind::Unsupported && type_input_settings_insensitive(name) {
                ValueKind::Text
            } else {
                kind
            }
        }
    })
}

/// Types whose literal input is fixed by no tracked session setting.
fn type_input_settings_insensitive(name: &str) -> bool {
    matches!(
        name,
        "json"
            | "jsonb"
            | "bytea"
            | "inet"
            | "cidr"
            | "macaddr"
            | "macaddr8"
            | "xml"
            | "bit"
            | "varbit"
            | "citext"
            | "char"
    )
}

/// Whether a literal in a position of unknown type could carry a date-time
/// meaning: it has a digit, or is one of PostgreSQL's special date-time
/// words.
fn literal_settings_sensitive(text: &str) -> bool {
    text.bytes().any(|b| b.is_ascii_digit())
        || matches!(
            text.trim().to_ascii_lowercase().as_str(),
            "now"
                | "today"
                | "tomorrow"
                | "yesterday"
                | "epoch"
                | "infinity"
                | "-infinity"
                | "allballs"
        )
}

#[cfg(test)]
mod tests {
    use iddqd::BiHashMap;
    use postgres_types::Type;

    use super::*;
    use crate::catalog::{Catalog, ColumnMetadata, ColumnPosition, ColumnStore, TableMetadata};
    use crate::oid::{Oid, TypeOid};
    use crate::query::ast::Deparse;
    use crate::query::ast::query_expr_parse;
    use crate::query::resolve::query_expr_resolve;

    fn events() -> Catalog {
        let column = |name: &str, position: i16, ty: Type| ColumnMetadata {
            name: name.into(),
            position: ColumnPosition::from_raw(position),
            type_oid: TypeOid::from_type(&ty),
            type_name: ty.name().into(),
            cache_type_name: ty.name().into(),
            data_type: ty,
            is_primary_key: position == 1,
        };
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(
            TableMetadata {
                replica_identity_full: false,
                relation_oid: Oid::from_raw(1001),
                name: "events".into(),
                schema: "public".into(),
                primary_key_columns: vec!["id".into()],
                columns: ColumnStore::new([
                    column("id", 1, Type::INT4),
                    column("day", 2, Type::DATE),
                    column("at", 3, Type::TIMESTAMPTZ),
                    column("local_at", 4, Type::TIMESTAMP),
                    column("label", 5, Type::TEXT),
                    column("span", 6, Type::INTERVAL),
                ]),
                indexes: Vec::new(),
            }
            .into(),
        );
        tables
    }

    fn canon(sql: &str, settings: &LiteralSettings) -> (String, LiteralCanonOutcome) {
        let query = query_expr_parse(sql).expect("parse");
        let mut resolved = query_expr_resolve(&query, &events(), &["public"]).expect("resolve");
        let outcome = resolved_literals_canonicalise(&mut resolved, settings);
        let mut sql = String::new();
        resolved.deparse(&mut sql);
        (sql, outcome)
    }

    fn tokyo_dmy() -> LiteralSettings {
        LiteralSettings::from_texts("Asia/Tokyo", "ISO, DMY", "postgres", "Default")
    }

    #[test]
    fn test_canonicalises_literals_beside_columns() {
        let (sql, outcome) = canon(
            "SELECT id FROM events WHERE day = '01/02/2024' AND at > '2024-01-02 10:00' \
             AND local_at < '2024-01-02 10:00+05'",
            &tokyo_dmy(),
        );
        assert!(!outcome.residual);
        assert!(sql.contains("'2024-02-01'"), "{sql}");
        assert!(sql.contains("'2024-01-02 01:00:00+00'"), "{sql}");
        assert!(sql.contains("'2024-01-02 10:00:00'"), "{sql}");
    }

    #[test]
    fn test_canonicalises_in_any_between_and_casts() {
        let (sql, outcome) = canon(
            "SELECT id FROM events WHERE day IN ('01/02/2024', '02/03/2024') \
             OR day = ANY (ARRAY['03/04/2024']) OR day BETWEEN '04/05/2024' AND '05/06/2024' \
             OR local_at = '06/07/2024 08:00'::timestamp",
            &tokyo_dmy(),
        );
        assert!(!outcome.residual, "{sql}");
        for expected in [
            "'2024-02-01'",
            "'2024-03-02'",
            "'2024-04-03'",
            "'2024-05-04'",
            "'2024-06-05'",
            "'2024-07-06 08:00:00'",
        ] {
            assert!(sql.contains(expected), "{expected} in {sql}");
        }
    }

    #[test]
    fn test_unhandled_spellings_and_blind_positions_are_residual() {
        let settings = tokyo_dmy();
        let (_, month_name) = canon("SELECT id FROM events WHERE day = 'Jan 2 2024'", &settings);
        assert!(month_name.residual);
        let (_, function_arg) = canon(
            "SELECT id FROM events WHERE at > to_timestamp('2024-01-02', 'YYYY-MM-DD')",
            &settings,
        );
        assert!(function_arg.residual);
        let (_, interval) = canon("SELECT id FROM events WHERE span > '1 day'", &settings);
        assert!(interval.residual);
        let (_, special) = canon(
            "SELECT id FROM events WHERE label = lower('now')",
            &settings,
        );
        assert!(special.residual);
        let (_, mixed) = canon(
            "SELECT id FROM events WHERE day = '2024-01-02 10:00'::timestamptz",
            &settings,
        );
        assert!(mixed.residual);
        let (_, zoneless_unknown_zone) = canon(
            "SELECT id FROM events WHERE at > '2024-01-02 10:00'",
            &LiteralSettings::from_texts("Nowhere/Land", "ISO, MDY", "postgres", "Default"),
        );
        assert!(zoneless_unknown_zone.residual);
    }

    #[test]
    fn test_plain_text_and_typed_literals_are_never_residual() {
        let settings = tokyo_dmy();
        let (_, text) = canon(
            "SELECT id FROM events WHERE label = '2024-01-02' AND lower(label) = 'bob' \
             AND id = '7'::int4 AND label = 'order-42'::text",
            &settings,
        );
        assert!(!text.residual);
        let (_, json) = canon(
            "SELECT id FROM events WHERE label = '{\"a\": 1}'::jsonb::text",
            &settings,
        );
        assert!(!json.residual);
    }
}
