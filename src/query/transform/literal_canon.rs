//! Canonicalise the date-time literals of a bound query under the session
//! settings they were written in (PGC-287 class 2), so the bound form — and
//! the identity, population, serve and CDC evaluation derived from it —
//! means the same rows whatever session registered it.
//!
//! A literal is rewritten where its type is known: compared to a column, a
//! member of a column's IN / ANY list or BETWEEN bounds, or cast to a
//! date-time type. A literal the canonicaliser does not handle, or one in a
//! position whose type it cannot see and whose text could carry a date-time
//! meaning, is left as written and reported, so the entry is served only to
//! sessions on the reference settings it will be populated under.

use crate::query::ast::rope::{
    Body, Case, Function, JoinQual, NodeKind, Query, Replacement, Replacements, Scalar, ScalarKind,
    Select, SelectItem, Source, TreeBuilder, Values, View, Where, WhereKind,
};
use crate::query::ast::{LiteralValue, NodeId};
use crate::query::cast::{CastTarget, cast_target_from_canonical};
use crate::query::literal_settings::LiteralSettings;
use crate::query::resolve::{BindingsBuilder, ResolvedQuery};
use crate::query::typed_compare::{ValueKind, literal_canonical_text};

/// What the pass found.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LiteralCanonOutcome {
    /// A literal was left as written whose meaning the populating session's
    /// settings decide.
    pub residual: bool,
}

/// Canonicalise every date-time literal of `bound` under `settings`,
/// reading column types through its bindings (ADR-057): a walk over the
/// views collecting each rewritten literal as a replacement, then one copy
/// of the tree with those positions replaced. The query comes back as it
/// was when no literal changes; the bindings carry over, since the tree's
/// shape is the same. CTE definition bodies are skipped: the resolver
/// binds only the copy each reference embeds, and that copy is
/// canonicalised in its place.
pub fn query_literals_canonicalise(
    bound: ResolvedQuery,
    settings: &LiteralSettings,
) -> (ResolvedQuery, LiteralCanonOutcome) {
    let (mut replacements, outcome) = {
        let mut pass = TreePass {
            bound: &bound,
            literals: Pass {
                settings,
                outcome: LiteralCanonOutcome::default(),
            },
            replacements: Replacements::new(),
        };
        pass.query(bound.tree().root());
        (pass.replacements, pass.literals.outcome)
    };
    if replacements.is_empty() {
        return (bound, outcome);
    }
    let mut builder = TreeBuilder::over(bound.tree());
    builder.copy(bound.tree().root().node_ref(), &mut replacements);
    let (tree, remap) = builder.finish();
    let bindings = BindingsBuilder::from_remapped(bound.bindings(), &remap).finish();
    (ResolvedQuery::new(tree, bindings), outcome)
}

/// The walk over views: literals rewritten on a copy of their value and
/// recorded by position when they change; the type context of each
/// position is the bindings'.
struct TreePass<'a> {
    bound: &'a ResolvedQuery,
    literals: Pass<'a>,
    replacements: Replacements,
}

impl TreePass<'_> {
    fn query(&mut self, query: Query<'_>) {
        match query.body() {
            Body::Select(select) => self.select(select),
            Body::SetOp(set_op) => {
                self.query(set_op.left());
                self.query(set_op.right());
            }
            Body::Values(values) => self.values(values),
        }
        for clause in query.order_by() {
            self.scalar(clause.expr(), None);
        }
    }

    fn values(&mut self, values: Values<'_>) {
        for row in values.rows() {
            for literal in row.literals() {
                self.literal(literal.node_ref().id(), literal.value(), None);
            }
        }
    }

    fn select(&mut self, select: Select<'_>) {
        for source in select.from() {
            self.source(source);
        }
        if let Some(where_clause) = select.where_clause() {
            self.where_expr(where_clause);
        }
        if let Some(having) = select.having() {
            self.where_expr(having);
        }
        for item in select.items().into_iter().flatten() {
            if let SelectItem::Expr(expr) = item {
                self.scalar(expr.expr(), None);
            }
        }
    }

    fn source(&mut self, source: Source<'_>) {
        match source {
            Source::Table(_) => {}
            Source::Subquery(subquery) => self.query(subquery.query()),
            Source::CteRef(cte_ref) => self.query(cte_ref.query()),
            Source::Join(join) => {
                self.source(join.left());
                self.source(join.right());
                if let JoinQual::On(on) = join.qual() {
                    self.where_expr(on);
                }
            }
        }
    }

    fn where_expr(&mut self, expr: Where<'_>) {
        match expr.kind() {
            WhereKind::Scalar(scalar) => self.scalar(scalar, None),
            WhereKind::Unary(unary) => self.where_expr(unary.expr()),
            WhereKind::Binary(binary) => {
                let kind = self
                    .column_kind(binary.lexpr())
                    .or_else(|| self.column_kind(binary.rexpr()));
                self.operand(binary.lexpr(), kind);
                self.operand(binary.rexpr(), kind);
            }
            WhereKind::Multi(multi) => {
                let kind = multi
                    .exprs()
                    .next()
                    .and_then(|first| self.column_kind(first));
                for (i, member) in multi.exprs().enumerate() {
                    self.operand(member, kind.filter(|_| i > 0));
                }
            }
            WhereKind::Subquery(subquery) => {
                if let Some(test) = subquery.test_expr() {
                    self.scalar(test, None);
                }
                self.query(subquery.query());
            }
        }
    }

    fn operand(&mut self, side: Where<'_>, kind: Option<ValueKind>) {
        match side.kind() {
            WhereKind::Scalar(scalar) => self.scalar(scalar, kind),
            WhereKind::Unary(_)
            | WhereKind::Binary(_)
            | WhereKind::Multi(_)
            | WhereKind::Subquery(_) => self.where_expr(side),
        }
    }

    fn scalar(&mut self, expr: Scalar<'_>, kind: Option<ValueKind>) {
        match expr.kind() {
            ScalarKind::Literal(literal) => self.literal(literal.id(), literal.value(), kind),
            ScalarKind::TypeCast(cast) => {
                let target = cast_kind(cast.target());
                let inner = cast.expr();
                if let ScalarKind::Literal(literal) = inner.kind() {
                    self.literal(literal.id(), literal.value(), target);
                    self.literals.mixed_kinds_check(kind, target);
                } else {
                    self.scalar(inner, None);
                }
            }
            ScalarKind::Array(array) => {
                for element in array.elements() {
                    self.scalar(element, kind);
                }
            }
            ScalarKind::Function(function) => self.function(function),
            ScalarKind::Case(case) => self.case(case),
            ScalarKind::Arithmetic(arithmetic) => {
                self.scalar(arithmetic.left(), None);
                self.scalar(arithmetic.right(), None);
            }
            ScalarKind::Subquery(subquery) => self.query(subquery.query()),
            ScalarKind::Column(_) => {}
        }
    }

    fn function(&mut self, function: Function<'_>) {
        for arg in function.args() {
            self.scalar(arg, None);
        }
        if let Some(filter) = function.filter() {
            self.where_expr(filter);
        }
        for clause in function.agg_order() {
            self.scalar(clause.expr(), None);
        }
    }

    fn case(&mut self, case: Case<'_>) {
        if let Some(arg) = case.arg() {
            self.scalar(arg, None);
        }
        for when in case.whens() {
            self.where_expr(when.condition());
            self.scalar(when.result(), None);
        }
        if let Some(default) = case.default() {
            self.scalar(default, None);
        }
    }

    /// Rewrite the literal at `id`; recorded as a replacement when its
    /// value changes.
    fn literal(&mut self, id: NodeId, value: &LiteralValue, kind: Option<ValueKind>) {
        let mut rewritten = value.clone();
        self.literals.literal(&mut rewritten, kind);
        if rewritten != *value {
            self.replacements
                .push(id, Replacement::Leaf(NodeKind::Literal(rewritten)));
        }
    }

    /// The kind of the column a comparison side names, looking through a
    /// cast; an inner join's merged column has its left column's kind.
    fn column_kind(&self, side: Where<'_>) -> Option<ValueKind> {
        let WhereKind::Scalar(scalar) = side.kind() else {
            return None;
        };
        self.scalar_column_kind(scalar)
    }

    fn scalar_column_kind(&self, scalar: Scalar<'_>) -> Option<ValueKind> {
        match scalar.kind() {
            ScalarKind::Column(column) => self
                .bound
                .column_type_at(column.id())
                .map(ValueKind::of_type),
            ScalarKind::TypeCast(cast) => {
                cast_kind(cast.target()).or_else(|| self.scalar_column_kind(cast.expr()))
            }
            ScalarKind::Literal(_)
            | ScalarKind::Function(_)
            | ScalarKind::Case(_)
            | ScalarKind::Arithmetic(_)
            | ScalarKind::Subquery(_)
            | ScalarKind::Array(_) => None,
        }
    }
}

struct Pass<'a> {
    settings: &'a LiteralSettings,
    outcome: LiteralCanonOutcome,
}

impl Pass<'_> {
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
    pub(super) fn mixed_kinds_check(
        &mut self,
        column: Option<ValueKind>,
        literal: Option<ValueKind>,
    ) {
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
    use iddqd::BiHashMap;
    use postgres_types::Type;

    use super::*;
    use crate::catalog::{Catalog, ColumnMetadata, ColumnPosition, TableMetadata};
    use crate::oid::TypeOid;
    use crate::query::resolve::ResolvedQuery;

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
            TableMetadata::for_test(
                "events",
                1001,
                [
                    column("id", 1, Type::INT4),
                    column("day", 2, Type::DATE),
                    column("at", 3, Type::TIMESTAMPTZ),
                    column("local_at", 4, Type::TIMESTAMP),
                    column("label", 5, Type::TEXT),
                    column("span", 6, Type::INTERVAL),
                ],
            )
            .into(),
        );
        tables
    }

    fn canon(sql: &str, settings: &LiteralSettings) -> (String, LiteralCanonOutcome) {
        let bound = ResolvedQuery::parse_resolve(sql, &events(), &["public"]).expect("resolve");
        let (bound, outcome) = query_literals_canonicalise(bound, settings);
        (bound.sql(), outcome)
    }

    /// A query no setting can change comes back sharing its tree.
    #[test]
    fn test_unaffected_query_keeps_its_tree() {
        let bound = ResolvedQuery::parse_resolve(
            "SELECT id FROM events WHERE label = 'x' AND id = 7",
            &events(),
            &["public"],
        )
        .expect("resolve");
        let before = bound.tree().clone();
        let (after, outcome) = query_literals_canonicalise(bound, &tokyo_dmy());
        assert_eq!(*after.tree(), before);
        assert!(!outcome.residual);
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
