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

use crate::query::ast::{
    CaseExpr, FunctionCall, JoinQual, LiteralValue, QueryBody, QueryExpr, ScalarExpr, SelectColumn,
    SelectColumns, SelectNode, TableSource, ValuesClause, WhereExpr,
};
use crate::query::cast::{CastTarget, cast_target_from_canonical};
use crate::query::literal_settings::LiteralSettings;
use crate::query::resolve::{Bindings, ColumnTarget};
use crate::query::typed_compare::{ValueKind, literal_canonical_text};

/// What the pass found.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LiteralCanonOutcome {
    /// A literal was left as written whose meaning the populating session's
    /// settings decide.
    pub residual: bool,
}

/// Canonicalise every date-time literal of a finalized plain tree under
/// `settings`, reading column types through `bindings` (ADR-057). Values
/// change, structure does not, so the tree's ids stay valid. CTE definition
/// bodies are skipped: the resolver binds only the copy each reference
/// embeds, and that copy is canonicalised in its place.
pub fn query_literals_canonicalise(
    query: &mut QueryExpr,
    bindings: &Bindings,
    settings: &LiteralSettings,
) -> LiteralCanonOutcome {
    let mut pass = BoundPass {
        bindings,
        literals: Pass {
            settings,
            outcome: LiteralCanonOutcome::default(),
        },
    };
    pass.query(query);
    pass.literals.outcome
}

/// The plain-tree walk; literal handling is shared with the resolved walk.
struct BoundPass<'a> {
    bindings: &'a Bindings,
    literals: Pass<'a>,
}

impl BoundPass<'_> {
    fn query(&mut self, query: &mut QueryExpr) {
        match &mut query.body {
            QueryBody::Select(select) => self.select(select),
            QueryBody::SetOp(set_op) => {
                self.query(&mut set_op.left);
                self.query(&mut set_op.right);
            }
            QueryBody::Values(values) => self.literals.values(values),
        }
        for clause in &mut query.order_by {
            self.scalar(&mut clause.expr, None);
        }
    }

    fn select(&mut self, select: &mut SelectNode) {
        for source in &mut select.from {
            self.source(source);
        }
        if let Some(where_clause) = &mut select.where_clause {
            self.where_expr(where_clause);
        }
        if let Some(having) = &mut select.having {
            self.where_expr(having);
        }
        if let SelectColumns::Columns(columns) = &mut select.columns {
            for column in columns {
                if let SelectColumn::Expr { expr, .. } = column {
                    self.scalar(expr, None);
                }
            }
        }
    }

    fn source(&mut self, source: &mut TableSource) {
        match source {
            TableSource::Table(_) => {}
            TableSource::Subquery(subquery) => self.query(&mut subquery.query),
            TableSource::CteRef(cte_ref) => self.query(&mut cte_ref.query),
            TableSource::Join(join) => {
                self.source(&mut join.left);
                self.source(&mut join.right);
                if let JoinQual::On(on) = &mut join.qual {
                    self.where_expr(on);
                }
            }
        }
    }

    fn where_expr(&mut self, expr: &mut WhereExpr) {
        match expr {
            WhereExpr::Scalar(scalar) => self.scalar(scalar, None),
            WhereExpr::Unary(unary) => self.where_expr(&mut unary.expr),
            WhereExpr::Binary(binary) => {
                let kind = self
                    .column_kind(&binary.lexpr)
                    .or_else(|| self.column_kind(&binary.rexpr));
                self.operand(&mut binary.lexpr, kind);
                self.operand(&mut binary.rexpr, kind);
            }
            WhereExpr::Multi(multi) => {
                let kind = multi
                    .exprs
                    .first()
                    .and_then(|first| self.column_kind(first));
                for (i, member) in multi.exprs.iter_mut().enumerate() {
                    self.operand(member, kind.filter(|_| i > 0));
                }
            }
            WhereExpr::Subquery {
                query, test_expr, ..
            } => {
                if let Some(test) = test_expr {
                    self.scalar(test, None);
                }
                self.query(query);
            }
        }
    }

    fn operand(&mut self, side: &mut WhereExpr, kind: Option<ValueKind>) {
        if let WhereExpr::Scalar(scalar) = side {
            self.scalar(scalar, kind);
        } else {
            self.where_expr(side);
        }
    }

    fn scalar(&mut self, expr: &mut ScalarExpr, kind: Option<ValueKind>) {
        match expr {
            ScalarExpr::Literal(literal) => self.literals.literal(literal, kind),
            ScalarExpr::TypeCast {
                expr: inner,
                target,
            } => {
                let cast = cast_kind(target);
                if let ScalarExpr::Literal(literal) = inner.as_mut() {
                    self.literals.literal(literal, cast);
                    self.literals.mixed_kinds_check(kind, cast);
                } else {
                    self.scalar(inner, None);
                }
            }
            ScalarExpr::Array(elems) => {
                for elem in elems {
                    self.scalar(elem, kind);
                }
            }
            ScalarExpr::Function(function) => self.function(function),
            ScalarExpr::Case(case) => self.case(case),
            ScalarExpr::Arithmetic(arith) => {
                self.scalar(&mut arith.left, None);
                self.scalar(&mut arith.right, None);
            }
            ScalarExpr::Subquery(query) => self.query(query),
            ScalarExpr::Column(_) => {}
        }
    }

    fn function(&mut self, function: &mut FunctionCall) {
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

    fn case(&mut self, case: &mut CaseExpr) {
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

    /// The kind of the column a comparison side names, looking through a
    /// cast; an inner join's merged column has its left column's kind.
    fn column_kind(&self, side: &WhereExpr) -> Option<ValueKind> {
        let WhereExpr::Scalar(scalar) = side else {
            return None;
        };
        self.scalar_column_kind(scalar)
    }

    fn scalar_column_kind(&self, scalar: &ScalarExpr) -> Option<ValueKind> {
        match scalar {
            ScalarExpr::Column(column) => {
                let metadata = match self.bindings.column(column)? {
                    ColumnTarget::Column(binding) => self.bindings.binding_column(binding)?,
                    ColumnTarget::Merged(id) => {
                        let merged = self.bindings.merged(*id)?;
                        if merged.outer {
                            return None;
                        }
                        self.bindings.binding_column(&merged.left)?
                    }
                    ColumnTarget::Output => return None,
                };
                Some(ValueKind::of_type(&metadata.data_type))
            }
            ScalarExpr::TypeCast { expr, target } => {
                cast_kind(target).or_else(|| self.scalar_column_kind(expr))
            }
            ScalarExpr::Literal(_)
            | ScalarExpr::Function(_)
            | ScalarExpr::Case(_)
            | ScalarExpr::Arithmetic(_)
            | ScalarExpr::Subquery(_)
            | ScalarExpr::Array(_) => None,
        }
    }
}

struct Pass<'a> {
    settings: &'a LiteralSettings,
    outcome: LiteralCanonOutcome,
}

impl Pass<'_> {
    fn values(&mut self, values: &mut ValuesClause) {
        for row in &mut values.rows {
            for literal in row {
                self.literal(literal, None);
            }
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
    use std::sync::Arc;

    use iddqd::BiHashMap;
    use postgres_types::Type;

    use super::*;
    use crate::catalog::{Catalog, ColumnMetadata, ColumnPosition, ColumnStore, TableMetadata};
    use crate::oid::{Oid, TypeOid};
    use crate::query::ast::query_expr_parse;
    use crate::query::resolve::{ResolvedQuery, query_expr_resolve_bound};

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
        let mut query = query_expr_parse(sql).expect("parse");
        let bindings = query_expr_resolve_bound(&query, &events(), &["public"]).expect("resolve");
        let outcome = query_literals_canonicalise(&mut query, &bindings, settings);
        let bound = ResolvedQuery::new(Arc::new(query), bindings);
        (bound.sql(), outcome)
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
