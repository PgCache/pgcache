//! The SQL the cache serves for a bound query (ADR-057): tables
//! schema-qualified, columns qualified by their binding, stars expanded,
//! CTE references inlined and `NATURAL` joins spelled as `USING`. The walk
//! is the plain deparse under a [`Bound`] context; the identity and the
//! parameterised shape are the same rendering hashed.

use std::collections::hash_map::DefaultHasher;
use std::fmt::Write;
use std::hash::{Hash, Hasher};

use ecow::EcoString;

use super::bindings::{
    Bindings, ColumnBinding, ColumnTarget, MergedColumnBinding, ResolvedQuery, SlotId, StarItem,
    TableSlot,
};
use crate::oid::Oid;
use crate::query::Fingerprint;
use crate::query::ast::rope::deparse::{
    column_plain_deparse, names_deparse, order_by_deparse_with, query_deparse_with,
    query_unlimited_deparse_with, scalar_deparse_with, star_plain_deparse, table_alias_deparse,
    table_plain_deparse,
};
use crate::query::ast::rope::{
    Column, CteRef, DeparseContext, Function, Join, OrderBy, QueryTree, Scalar, ScalarKind, Select,
    SelectItem, Star, Table,
};
use crate::query::ast::{Clause, Deparse, LiteralValue, NodeId, separated_deparse};
use crate::query::shape::{QueryShape, ShapeKey};
use crate::query::transform::literal_is_parameterizable;

/// Keeps a bound fingerprint from colliding with a resolved fingerprint (3)
/// or a session key (2) hashed from the same text.
const BOUND_FINGERPRINT_DOMAIN: u8 = 4;

/// `tree` rendered as the cache serves it under `bindings`.
pub(crate) fn query_tree_sql_into(tree: &QueryTree, bindings: &Bindings, buf: &mut String) {
    query_deparse_with(tree.root(), &mut Bound::text(bindings), buf);
}

/// How literals render.
enum Mode {
    /// As written.
    Text,
    /// A literal in predicate position becomes `$N` and is collected
    /// (PGC-294); one in a projection, ordering, `VALUES` or `LIMIT`
    /// position, or anywhere beneath one, stays inline.
    Shape {
        literals: Vec<LiteralValue>,
        predicate_depth: u32,
        projection_depth: u32,
    },
}

/// A FROM entry something else stands in for: the entry renders as
/// `(<source>) alias(columns)` and every column bound to its slot as
/// `alias.column`, so a CDC row can take a relation's place in its query.
pub struct SourceOverride<'a> {
    pub slot: SlotId,
    pub alias: &'a str,
    /// The derived table's text, without its parentheses.
    pub source: &'a str,
    /// The alias column list; empty when the source names its own columns.
    pub columns: &'a [EcoString],
}

/// The context that renders a bound tree.
pub(super) struct Bound<'a> {
    bindings: &'a Bindings,
    mode: Mode,
    source: Option<&'a SourceOverride<'a>>,
}

impl<'a> Bound<'a> {
    pub(super) fn text(bindings: &'a Bindings) -> Self {
        Self {
            bindings,
            mode: Mode::Text,
            source: None,
        }
    }

    fn shape(bindings: &'a Bindings) -> Self {
        Self {
            bindings,
            mode: Mode::Shape {
                literals: Vec::new(),
                predicate_depth: 0,
                projection_depth: 0,
            },
            source: None,
        }
    }

    fn with_source(bindings: &'a Bindings, source: &'a SourceOverride<'a>) -> Self {
        Self {
            bindings,
            mode: Mode::Text,
            source: Some(source),
        }
    }

    /// The override for `slot`, when one stands in for it.
    fn source_override(&self, slot: SlotId) -> Option<&'a SourceOverride<'a>> {
        self.source.filter(|source| source.slot == slot)
    }

    fn slot_name_deparse(slot: &TableSlot, buf: &mut String) {
        match slot {
            TableSlot::Base {
                alias: Some(alias), ..
            } => {
                alias.deparse(buf);
            }
            TableSlot::Base { table, alias: None } => {
                table.schema.deparse(buf);
                buf.push('.');
                table.name.deparse(buf);
            }
            TableSlot::Derived { table, .. } => {
                table.name.deparse(buf);
            }
        }
    }

    /// `qualifier.column` for a bound column: the alias, else the
    /// schema-qualified table.
    fn column_binding_deparse(&self, binding: &ColumnBinding, buf: &mut String) {
        let (Some(slot), Some(column)) = (
            self.bindings.slot(binding.slot),
            self.bindings.binding_column(binding),
        ) else {
            debug_assert!(false, "column binding without a slot or column");
            return;
        };
        match self.source_override(binding.slot) {
            Some(source) => {
                source.alias.deparse(buf);
            }
            None => Self::slot_name_deparse(slot, buf),
        }
        buf.push('.');
        column.name.deparse(buf);
    }

    /// A `USING`/`NATURAL` merged column: the left column, or
    /// `coalesce(left, right)` under an outer join.
    fn merged_deparse(&self, merged: &MergedColumnBinding, buf: &mut String) {
        if merged.outer {
            buf.push_str("coalesce(");
            self.column_binding_deparse(&merged.left, buf);
            buf.push_str(", ");
            self.column_binding_deparse(&merged.right, buf);
            buf.push(')');
        } else {
            self.column_binding_deparse(&merged.left, buf);
        }
    }

    fn star_item_deparse(&self, item: &StarItem, buf: &mut String) {
        match item {
            StarItem::Column(binding) => self.column_binding_deparse(binding, buf),
            StarItem::Merged(id) => match self.bindings.merged(*id) {
                Some(merged) => {
                    self.merged_deparse(merged, buf);
                    buf.push_str(" AS ");
                    merged.name.deparse(buf);
                }
                None => debug_assert!(false, "star item names a missing merged column"),
            },
        }
    }

    /// One star-expanded output's rendered expression.
    fn star_item_into(&self, item: &StarItem, buf: &mut String) {
        match item {
            StarItem::Column(binding) => self.column_binding_deparse(binding, buf),
            StarItem::Merged(id) => {
                if let Some(merged) = self.bindings.merged(*id) {
                    self.merged_deparse(merged, buf);
                }
            }
        }
    }

    fn literals_take(self) -> Vec<LiteralValue> {
        match self.mode {
            Mode::Text => Vec::new(),
            Mode::Shape { literals, .. } => literals,
        }
    }
}

impl Bound<'_> {
    /// A FROM table through its slot: the source standing in for it, else
    /// the schema-qualified table with `alias`; `plain` when it has no slot.
    fn table_render(
        &self,
        slot: Option<SlotId>,
        alias: impl FnOnce(&mut String),
        plain: impl FnOnce(&mut String),
        buf: &mut String,
    ) {
        let Some(slot) = slot else {
            debug_assert!(false, "table reference without a slot");
            plain(buf);
            return;
        };
        if let Some(source) = self.source_override(slot) {
            buf.push_str(" (");
            buf.push_str(source.source);
            buf.push_str(") ");
            source.alias.deparse(buf);
            if !source.columns.is_empty() {
                buf.push('(');
                separated_deparse(buf, source.columns, ", ");
                buf.push(')');
            }
            return;
        }
        match self.bindings.slot(slot) {
            Some(slot) => {
                let table = slot.table();
                buf.push(' ');
                table.schema.deparse(buf);
                buf.push('.');
                table.name.deparse(buf);
                alias(buf);
            }
            None => {
                debug_assert!(false, "table reference without a slot");
                plain(buf);
            }
        }
    }

    /// A column reference through its target; an output name is unqualified
    /// whatever the reference wrote, and `plain` renders an unbound one.
    fn column_render(
        &self,
        target: Option<&ColumnTarget>,
        name: &EcoString,
        plain: impl FnOnce(&mut String),
        buf: &mut String,
    ) {
        match target {
            Some(ColumnTarget::Column(binding)) => self.column_binding_deparse(binding, buf),
            Some(ColumnTarget::Merged(id)) => match self.bindings.merged(*id) {
                Some(merged) => self.merged_deparse(merged, buf),
                None => {
                    debug_assert!(false, "column names a missing merged column");
                    plain(buf);
                }
            },
            Some(ColumnTarget::Output) => {
                name.deparse(buf);
            }
            None => {
                debug_assert!(false, "column reference without a binding");
                plain(buf);
            }
        }
    }

    /// A star through its expansion, `, `-separated; `plain` when it has none.
    fn star_render(
        &self,
        items: Option<&[StarItem]>,
        plain: impl FnOnce(&mut String),
        buf: &mut String,
    ) {
        let Some(items) = items else {
            debug_assert!(false, "star without an expansion");
            plain(buf);
            return;
        };
        let mut sep = "";
        for item in items {
            buf.push_str(sep);
            self.star_item_deparse(item, buf);
            sep = ", ";
        }
    }

    fn literal_render(&mut self, literal: &LiteralValue, buf: &mut String) {
        match &mut self.mode {
            Mode::Shape {
                literals,
                predicate_depth,
                projection_depth,
            } if *projection_depth == 0
                && *predicate_depth > 0
                && literal_is_parameterizable(literal) =>
            {
                literals.push(literal.clone());
                let _ = write!(buf, "${}", literals.len());
            }
            Mode::Shape { .. } | Mode::Text => {
                literal.deparse(buf);
            }
        }
    }

    fn clause_entered(&mut self, clause: Clause) {
        let Mode::Shape {
            predicate_depth,
            projection_depth,
            ..
        } = &mut self.mode
        else {
            return;
        };
        match clause {
            Clause::JoinOn | Clause::Where | Clause::Having => *predicate_depth += 1,
            Clause::With
            | Clause::SelectList
            | Clause::OrderBy
            | Clause::Limit
            | Clause::Values => *projection_depth += 1,
            Clause::From | Clause::GroupBy => {}
        }
    }

    fn clause_left(&mut self, clause: Clause) {
        let Mode::Shape {
            predicate_depth,
            projection_depth,
            ..
        } = &mut self.mode
        else {
            return;
        };
        match clause {
            Clause::JoinOn | Clause::Where | Clause::Having => *predicate_depth -= 1,
            Clause::With
            | Clause::SelectList
            | Clause::OrderBy
            | Clause::Limit
            | Clause::Values => *projection_depth -= 1,
            Clause::From | Clause::GroupBy => {}
        }
    }
}

impl DeparseContext for Bound<'_> {
    fn table_deparse(&mut self, table: Table<'_>, buf: &mut String) {
        self.table_render(
            self.bindings.source(table.id()),
            |buf| table_alias_deparse(table, buf),
            |buf| table_plain_deparse(table, buf),
            buf,
        );
    }

    fn column_deparse(&mut self, column: Column<'_>, buf: &mut String) {
        self.column_render(
            self.bindings.column_by_id(column.id()),
            column.column(),
            |buf| column_plain_deparse(column, buf),
            buf,
        );
    }

    fn star_deparse(&mut self, star: Star<'_>, buf: &mut String) {
        self.star_render(
            self.bindings.star_by_id(star.id()),
            |buf| star_plain_deparse(star, buf),
            buf,
        );
    }

    /// The CTE body inline as a derived table, named by the reference's
    /// alias or the CTE's name, with the CTE's column aliases.
    fn cte_ref_deparse(&mut self, cte_ref: CteRef<'_>, buf: &mut String) {
        buf.push_str(" (");
        query_deparse_with(cte_ref.query(), self, buf);
        buf.push_str(") ");
        match cte_ref.alias() {
            Some(alias) => alias.deparse(buf),
            None => cte_ref.cte_name().deparse(buf),
        };
        let aliases = cte_ref.column_aliases();
        if aliases.len() > 0 {
            buf.push('(');
            names_deparse(aliases, ", ", buf);
            buf.push(')');
        }
    }

    fn with_clause_renders(&self) -> bool {
        false
    }

    fn natural_join_columns(&mut self, join: Join<'_>) -> Option<Vec<EcoString>> {
        let join = join.id();
        Some(
            self.bindings
                .merged_columns()
                .iter()
                .filter(|merged| merged.join == join)
                .map(|merged| merged.name.clone())
                .collect(),
        )
    }

    fn function_name_deparse(&mut self, function: Function<'_>, buf: &mut String) {
        buf.push_str(function.name());
    }

    fn literal_deparse(&mut self, literal: &LiteralValue, buf: &mut String) {
        self.literal_render(literal, buf);
    }

    fn clause_enter(&mut self, clause: Clause) {
        self.clause_entered(clause);
    }

    fn clause_leave(&mut self, clause: Clause) {
        self.clause_left(clause);
    }
}

/// Every column of the base table a FROM entry's slot names, as the cache
/// serves them: ` q.c1, q.c2, …` with the entry's qualifier, or `c1,c2,…`
/// unqualified; nothing for a derived table or an unbound entry.
fn source_columns_render(bindings: &Bindings, source: NodeId, qualified: bool, buf: &mut String) {
    let Some(slot) = bindings.source(source).and_then(|slot| bindings.slot(slot)) else {
        debug_assert!(false, "FROM entry without a slot");
        return;
    };
    let TableSlot::Base { table, .. } = slot else {
        return;
    };
    let mut sep = "";
    for column in table.columns.iter() {
        buf.push_str(sep);
        if qualified {
            buf.push(' ');
            Bound::slot_name_deparse(slot, buf);
            buf.push('.');
        }
        column.name.deparse(buf);
        sep = ",";
    }
}

impl ResolvedQuery {
    /// The context that renders this query's nodes as the cache serves
    /// them, for a caller composing its own SQL around parts of the tree.
    pub fn deparse_context(&self) -> impl DeparseContext + '_ {
        Bound::text(self.bindings())
    }

    /// [`ResolvedQuery::deparse_context`] with `source` standing in for one
    /// FROM entry.
    pub fn deparse_context_with_source<'a>(
        &'a self,
        source: &'a SourceOverride<'a>,
    ) -> impl DeparseContext + 'a {
        Bound::with_source(self.bindings(), source)
    }

    /// The serve SQL with `source` standing in for one FROM entry.
    pub fn sql_with_source_into(&self, source: &SourceOverride<'_>, buf: &mut String) {
        let mut cx = Bound::with_source(self.bindings(), source);
        query_deparse_with(self.tree().root(), &mut cx, buf);
    }

    /// Every column of the table a FROM entry names, as the cache serves
    /// them: ` q.c1, q.c2, …` with the entry's qualifier. Nothing for a
    /// derived table or an unbound entry.
    pub fn source_columns_into(&self, source: NodeId, buf: &mut String) {
        source_columns_render(self.bindings(), source, true, buf);
    }

    /// The same columns as [`Self::source_columns_into`], in the same order,
    /// unqualified: `c1,c2,…`, for a statement that writes them back.
    pub fn source_column_names_into(&self, source: NodeId, buf: &mut String) {
        source_columns_render(self.bindings(), source, false, buf);
    }

    /// The SQL the cache serves for this query, appended to `buf`.
    pub fn sql_into(&self, buf: &mut String) {
        query_tree_sql_into(self.tree(), self.bindings(), buf);
    }

    /// The SQL the cache serves for this query.
    pub fn sql(&self) -> String {
        let mut buf = String::with_capacity(256);
        self.sql_into(&mut buf);
        buf
    }

    /// The cache identity of the query (PGC-287 class 2): a hash of its
    /// serve SQL without LIMIT/OFFSET, so two spellings of one query under
    /// any search_path or session settings share an entry and LIMIT does
    /// not split one.
    pub fn fingerprint(&self) -> Fingerprint {
        let mut buf = String::with_capacity(256);
        let mut cx = Bound::text(self.bindings());
        query_unlimited_deparse_with(self.tree().root(), &mut cx, &mut buf);
        let mut hasher = DefaultHasher::new();
        hasher.write_u8(BOUND_FINGERPRINT_DOMAIN);
        buf.hash(&mut hasher);
        Fingerprint::from_raw(hasher.finish())
    }

    /// The query's parameterised shape (PGC-294): the serve SQL with its
    /// predicate literals as `$1..$k`, keyed by a hash of that text, and
    /// the literals to bind.
    pub fn shape(&self) -> QueryShape {
        let mut cx = Bound::shape(self.bindings());
        let mut sql = String::with_capacity(256);
        query_deparse_with(self.tree().root(), &mut cx, &mut sql);
        let literals = cx.literals_take();
        let mut hasher = DefaultHasher::new();
        sql.hash(&mut hasher);
        QueryShape {
            sql: sql.into(),
            key: ShapeKey::from_raw(hasher.finish()),
            literals,
        }
    }

    /// Every catalog relation the query reads, subqueries included, sorted
    /// and deduplicated.
    pub fn relation_oids(&self) -> Vec<Oid> {
        let mut oids: Vec<Oid> = self
            .bindings()
            .slots()
            .iter()
            .filter_map(|slot| match slot {
                TableSlot::Base { table, .. } => Some(table.relation_oid),
                TableSlot::Derived { .. } => None,
            })
            .collect();
        oids.sort_unstable();
        oids.dedup();
        oids
    }

    /// The 1-based select-list position an ORDER BY item of `select` names,
    /// stars expanded: the output an output name or position names, else the
    /// one its expression renders as. `None` when no output matches.
    pub fn order_by_position(&self, select: Select<'_>, clause: OrderBy<'_>) -> Option<usize> {
        match self.order_by_resolve(select, clause) {
            OrderByResolution::Output { position, .. } => Some(position + 1),
            OrderByResolution::Own(expr) => {
                let mut text = String::new();
                scalar_deparse_with(expr, &mut Bound::text(self.bindings()), &mut text);
                self.select_output_texts(select)
                    .iter()
                    .position(|output| *output == text)
                    .map(|index| index + 1)
            }
            OrderByResolution::Unresolved => None,
        }
    }

    /// Each select-list output's rendered expression, stars expanded.
    fn select_output_texts(&self, select: Select<'_>) -> Vec<String> {
        let bindings = self.bindings();
        let mut cx = Bound::text(bindings);
        let Some(items) = select.items() else {
            return Vec::new();
        };
        let mut outputs = Vec::with_capacity(items.len());
        for item in items {
            match item {
                SelectItem::Expr(expr) => {
                    let mut text = String::new();
                    scalar_deparse_with(expr.expr(), &mut cx, &mut text);
                    outputs.push(text);
                }
                SelectItem::Star(star) => {
                    for item in bindings.star_by_id(star.id()).unwrap_or(&[]) {
                        let mut text = String::new();
                        cx.star_item_into(item, &mut text);
                        outputs.push(text);
                    }
                }
            }
        }
        outputs
    }

    /// An ORDER BY item rendered as the cache serves it.
    pub fn order_by_into(&self, clause: OrderBy<'_>, buf: &mut String) {
        order_by_deparse_with(clause, &mut Bound::text(self.bindings()), buf);
    }

    /// What `clause`, an ORDER BY item of `select`, sorts by: an output name
    /// or a position resolves to that output, any other item is its own
    /// expression. `None` when it names no output.
    pub(crate) fn order_by_source<'a>(
        &'a self,
        select: Select<'a>,
        clause: OrderBy<'a>,
    ) -> Option<OrderBySource<'a>> {
        match self.order_by_resolve(select, clause) {
            OrderByResolution::Own(expr) => Some(OrderBySource::Expr(expr)),
            OrderByResolution::Output { source, .. } => Some(source),
            OrderByResolution::Unresolved => None,
        }
    }

    /// The one resolution of an ORDER BY item behind [`Self::order_by_source`]
    /// and [`Self::order_by_position`].
    fn order_by_resolve<'a>(
        &'a self,
        select: Select<'a>,
        clause: OrderBy<'a>,
    ) -> OrderByResolution<'a> {
        let expr = clause.expr();
        let wanted = match expr.kind() {
            ScalarKind::Column(column)
                if matches!(
                    self.column_target_at(column.id()),
                    Some(ColumnTarget::Output)
                ) =>
            {
                OutputWanted::Named(column.column())
            }
            ScalarKind::Literal(literal) => {
                let LiteralValue::Integer(position) = literal.value() else {
                    return OrderByResolution::Unresolved;
                };
                let Some(index) = usize::try_from(*position)
                    .ok()
                    .and_then(|position| position.checked_sub(1))
                else {
                    return OrderByResolution::Unresolved;
                };
                OutputWanted::At(index)
            }
            ScalarKind::Column(_)
            | ScalarKind::Function(_)
            | ScalarKind::Case(_)
            | ScalarKind::Arithmetic(_)
            | ScalarKind::Subquery(_)
            | ScalarKind::Array(_)
            | ScalarKind::TypeCast(_) => return OrderByResolution::Own(expr),
        };
        let bindings = self.bindings();
        let mut position = 0;
        for item in select.items().into_iter().flatten() {
            match item {
                SelectItem::Expr(item_expr) => {
                    let name = item_expr
                        .alias()
                        .cloned()
                        .or_else(|| self.column_output_name(item_expr.expr()));
                    if wanted.matches(position, name.as_ref()) {
                        return OrderByResolution::Output {
                            position,
                            source: OrderBySource::Expr(item_expr.expr()),
                        };
                    }
                    position += 1;
                }
                SelectItem::Star(star) => {
                    for star_item in bindings.star_by_id(star.id()).unwrap_or(&[]) {
                        let name = match star_item {
                            StarItem::Column(binding) => {
                                bindings.binding_column(binding).map(|column| &column.name)
                            }
                            StarItem::Merged(id) => bindings.merged(*id).map(|merged| &merged.name),
                        };
                        if wanted.matches(position, name) {
                            return OrderByResolution::Output {
                                position,
                                source: OrderBySource::Star(star_item),
                            };
                        }
                        position += 1;
                    }
                }
            }
        }
        OrderByResolution::Unresolved
    }

    /// An ORDER BY item of `select` rendered over its FROM's columns, for a
    /// read with another select list. False, leaving `buf` alone, when it
    /// names no output.
    #[cfg(feature = "proxy")]
    pub(crate) fn order_by_source_into(
        &self,
        select: Select<'_>,
        clause: OrderBy<'_>,
        buf: &mut String,
    ) -> bool {
        let Some(source) = self.order_by_source(select, clause) else {
            return false;
        };
        let mut cx = Bound::text(self.bindings());
        match source {
            OrderBySource::Expr(expr) => scalar_deparse_with(expr, &mut cx, buf),
            OrderBySource::Star(item) => cx.star_item_into(item, buf),
        }
        buf.push(' ');
        clause.direction().deparse(buf);
        clause.null_order().deparse(buf);
        true
    }
}

/// What an ORDER BY item of a SELECT sorts by.
#[derive(Clone, Copy)]
pub(crate) enum OrderBySource<'a> {
    /// An expression over the FROM: the item's own, or the select-list
    /// expression an output name or position names.
    Expr(Scalar<'a>),
    /// A star-expanded output an output name or position names.
    Star(&'a StarItem),
}

/// How an ORDER BY item resolves against its SELECT's outputs.
enum OrderByResolution<'a> {
    /// It names no output: its own expression over the FROM.
    Own(Scalar<'a>),
    /// It names an output by name or position; zero-based, stars expanded.
    Output {
        position: usize,
        source: OrderBySource<'a>,
    },
    /// A position or name no output has.
    Unresolved,
}

/// The select-list output an ORDER BY item names.
#[derive(Clone, Copy)]
enum OutputWanted<'a> {
    Named(&'a EcoString),
    /// Zero-based, stars expanded.
    At(usize),
}

impl OutputWanted<'_> {
    fn matches(self, index: usize, name: Option<&EcoString>) -> bool {
        match self {
            OutputWanted::Named(wanted) => name == Some(wanted),
            OutputWanted::At(at) => at == index,
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::catalog::Catalog;
    use crate::query::ast::query_tree_session_key;
    use crate::query::ast::rope::{NodeKind, View};
    use crate::query::literal_settings::LiteralSettings;
    use crate::query::resolve::tests::{
        BINDING_CORPUS, RENDER_CORPUS, binding_catalog, catalog_with_columns,
    };

    fn corpus() -> impl Iterator<Item = &'static str> {
        BINDING_CORPUS.iter().chain(RENDER_CORPUS).copied()
    }

    fn bound(sql: &str, catalog: &Catalog) -> ResolvedQuery {
        ResolvedQuery::parse_resolve(sql, catalog, &["public"]).expect("resolve")
    }

    /// The renderings of every corpus query: the serve SQL, the shape SQL and
    /// the shape's literals.
    #[rustfmt::skip]
    const RENDER_EXPECTED: &[(&str, &str, &str, &[&str])] = &[
        ("SELECT u.id, name FROM users u WHERE dept_id = 'x'",
         "SELECT u.id, u.name FROM public.users u WHERE u.dept_id = 'x'",
         "SELECT u.id, u.name FROM public.users u WHERE u.dept_id = $1",
         &["'x'"]),
        ("SELECT u.id, d.name AS dname FROM users u JOIN departments d ON u.dept_id = d.id WHERE d.name = 'a' ORDER BY dname, u.id DESC",
         "SELECT u.id, d.name AS dname FROM public.users u JOIN public.departments d ON u.dept_id = d.id WHERE d.name = 'a' ORDER BY dname ASC, u.id DESC",
         "SELECT u.id, d.name AS dname FROM public.users u JOIN public.departments d ON u.dept_id = d.id WHERE d.name = $1 ORDER BY dname ASC, u.id DESC",
         &["'a'"]),
        ("SELECT * FROM users JOIN departments USING (id)",
         "SELECT public.users.id AS id, public.users.name, public.users.dept_id, public.departments.name, public.departments.manager_id FROM public.users JOIN public.departments USING (id)",
         "SELECT public.users.id AS id, public.users.name, public.users.dept_id, public.departments.name, public.departments.manager_id FROM public.users JOIN public.departments USING (id)",
         &[]),
        ("SELECT * FROM users LEFT JOIN departments USING (id) WHERE id = '1'",
         "SELECT coalesce(public.users.id, public.departments.id) AS id, public.users.name, public.users.dept_id, public.departments.name, public.departments.manager_id FROM public.users LEFT JOIN public.departments USING (id) WHERE coalesce(public.users.id, public.departments.id) = '1'",
         "SELECT coalesce(public.users.id, public.departments.id) AS id, public.users.name, public.users.dept_id, public.departments.name, public.departments.manager_id FROM public.users LEFT JOIN public.departments USING (id) WHERE coalesce(public.users.id, public.departments.id) = $1",
         &["'1'"]),
        ("SELECT id, count(*) FROM users JOIN departments USING (id) GROUP BY id",
         "SELECT public.users.id, count(*) FROM public.users JOIN public.departments USING (id) GROUP BY public.users.id",
         "SELECT public.users.id, count(*) FROM public.users JOIN public.departments USING (id) GROUP BY public.users.id",
         &[]),
        ("SELECT * FROM users NATURAL JOIN departments",
         "SELECT public.users.id AS id, public.users.name AS name, public.users.dept_id, public.departments.manager_id FROM public.users JOIN public.departments USING (id, name)",
         "SELECT public.users.id AS id, public.users.name AS name, public.users.dept_id, public.departments.manager_id FROM public.users JOIN public.departments USING (id, name)",
         &[]),
        ("SELECT d.name FROM departments d WHERE EXISTS (SELECT 1 FROM users u WHERE u.dept_id = d.id)",
         "SELECT d.name FROM public.departments d WHERE EXISTS (SELECT 1 FROM public.users u WHERE u.dept_id = d.id)",
         "SELECT d.name FROM public.departments d WHERE EXISTS (SELECT 1 FROM public.users u WHERE u.dept_id = d.id)",
         &[]),
        ("SELECT v.total, v.uid FROM (SELECT user_id AS uid, amount AS total FROM orders) v WHERE v.total = '5'",
         "SELECT v.total, v.uid FROM (SELECT public.orders.user_id AS uid, public.orders.amount AS total FROM public.orders) v WHERE v.total = '5'",
         "SELECT v.total, v.uid FROM (SELECT public.orders.user_id AS uid, public.orders.amount AS total FROM public.orders) v WHERE v.total = $1",
         &["'5'"]),
        ("WITH big AS (SELECT id, amount FROM orders WHERE amount = '9') SELECT big.id FROM big JOIN items ON items.order_id = big.id",
         "SELECT big.id FROM (SELECT public.orders.id, public.orders.amount FROM public.orders WHERE public.orders.amount = '9') big JOIN public.items ON public.items.order_id = big.id",
         "SELECT big.id FROM (SELECT public.orders.id, public.orders.amount FROM public.orders WHERE public.orders.amount = $1) big JOIN public.items ON public.items.order_id = big.id",
         &["'9'"]),
        ("SELECT id FROM users UNION SELECT id FROM departments ORDER BY id",
         "SELECT public.users.id FROM public.users UNION SELECT public.departments.id FROM public.departments ORDER BY id ASC",
         "SELECT public.users.id FROM public.users UNION SELECT public.departments.id FROM public.departments ORDER BY id ASC",
         &[]),
        ("SELECT name, count(*) FROM users GROUP BY name HAVING count(*) > 1 ORDER BY count(*) DESC",
         "SELECT public.users.name, count(*) FROM public.users GROUP BY public.users.name HAVING count(*) > 1 ORDER BY count(*) DESC",
         "SELECT public.users.name, count(*) FROM public.users GROUP BY public.users.name HAVING count(*) > $1 ORDER BY count(*) DESC",
         &["1"]),
        ("SELECT u.name, (SELECT max(o.amount) FROM orders o WHERE o.user_id = u.id) FROM users u WHERE u.id IN (SELECT user_id FROM orders WHERE amount = '1')",
         "SELECT u.name, (SELECT max(o.amount) FROM public.orders o WHERE o.user_id = u.id) FROM public.users u WHERE u.id IN (SELECT public.orders.user_id FROM public.orders WHERE public.orders.amount = '1')",
         "SELECT u.name, (SELECT max(o.amount) FROM public.orders o WHERE o.user_id = u.id) FROM public.users u WHERE u.id IN (SELECT public.orders.user_id FROM public.orders WHERE public.orders.amount = $1)",
         &["'1'"]),
        ("SELECT u.* FROM users u JOIN departments d ON d.id = u.dept_id WHERE d.id = ANY(ARRAY['1', '2']) AND u.name LIKE 'a%'",
         "SELECT u.id, u.name, u.dept_id FROM public.users u JOIN public.departments d ON d.id = u.dept_id WHERE d.id = ANY (ARRAY['1', '2']) AND u.name LIKE 'a%'",
         "SELECT u.id, u.name, u.dept_id FROM public.users u JOIN public.departments d ON d.id = u.dept_id WHERE d.id = ANY (ARRAY[$1, $2]) AND u.name LIKE $3",
         &["'1'", "'2'", "'a%'"]),
        ("SELECT CASE WHEN amount = '0' THEN sku ELSE order_id END FROM orders o JOIN items i ON i.order_id = o.id ORDER BY i.sku NULLS FIRST",
         "SELECT CASE WHEN o.amount = '0' THEN i.sku ELSE i.order_id END FROM public.orders o JOIN public.items i ON i.order_id = o.id ORDER BY i.sku ASC NULLS FIRST",
         "SELECT CASE WHEN o.amount = '0' THEN i.sku ELSE i.order_id END FROM public.orders o JOIN public.items i ON i.order_id = o.id ORDER BY i.sku ASC NULLS FIRST",
         &[]),
        ("SELECT * FROM users u LEFT JOIN departments d USING (id) ORDER BY id",
         "SELECT coalesce(u.id, d.id) AS id, u.name, u.dept_id, d.name, d.manager_id FROM public.users u LEFT JOIN public.departments d USING (id) ORDER BY id ASC",
         "SELECT coalesce(u.id, d.id) AS id, u.name, u.dept_id, d.name, d.manager_id FROM public.users u LEFT JOIN public.departments d USING (id) ORDER BY id ASC",
         &[]),
        ("SELECT u.*, d.name FROM users u NATURAL JOIN departments d",
         "SELECT u.id, u.name, u.dept_id, d.name FROM public.users u JOIN public.departments d USING (id, name)",
         "SELECT u.id, u.name, u.dept_id, d.name FROM public.users u JOIN public.departments d USING (id, name)",
         &[]),
        ("SELECT * FROM users NATURAL LEFT JOIN departments WHERE id = 1",
         "SELECT coalesce(public.users.id, public.departments.id) AS id, coalesce(public.users.name, public.departments.name) AS name, public.users.dept_id, public.departments.manager_id FROM public.users LEFT JOIN public.departments USING (id, name) WHERE coalesce(public.users.id, public.departments.id) = 1",
         "SELECT coalesce(public.users.id, public.departments.id) AS id, coalesce(public.users.name, public.departments.name) AS name, public.users.dept_id, public.departments.manager_id FROM public.users LEFT JOIN public.departments USING (id, name) WHERE coalesce(public.users.id, public.departments.id) = $1",
         &["1"]),
        ("SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount > 5 ORDER BY o.amount DESC NULLS LAST, u.id",
         "SELECT u.id, u.name, u.dept_id, o.id, o.user_id, o.amount FROM public.users u JOIN public.orders o ON u.id = o.user_id WHERE o.amount > 5 ORDER BY o.amount DESC NULLS LAST, u.id ASC",
         "SELECT u.id, u.name, u.dept_id, o.id, o.user_id, o.amount FROM public.users u JOIN public.orders o ON u.id = o.user_id WHERE o.amount > $1 ORDER BY o.amount DESC NULLS LAST, u.id ASC",
         &["5"]),
        ("WITH t AS (SELECT id, name FROM users) SELECT x.name FROM t AS x WHERE x.id = 1",
         "SELECT x.name FROM (SELECT public.users.id, public.users.name FROM public.users) x WHERE x.id = 1",
         "SELECT x.name FROM (SELECT public.users.id, public.users.name FROM public.users) x WHERE x.id = $1",
         &["1"]),
        ("WITH a AS (SELECT id FROM users), b AS (SELECT id FROM a WHERE id = 1) SELECT b.id FROM b",
         "SELECT b.id FROM (SELECT a.id FROM (SELECT public.users.id FROM public.users) a WHERE a.id = 1) b",
         "SELECT b.id FROM (SELECT a.id FROM (SELECT public.users.id FROM public.users) a WHERE a.id = $1) b",
         &["1"]),
        ("SELECT id FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount = 1 LIMIT 5) ORDER BY id LIMIT 3 OFFSET 1",
         "SELECT public.users.id FROM public.users WHERE public.users.id IN (SELECT public.orders.user_id FROM public.orders WHERE public.orders.amount = 1 LIMIT 5) ORDER BY id ASC LIMIT 3 OFFSET 1",
         "SELECT public.users.id FROM public.users WHERE public.users.id IN (SELECT public.orders.user_id FROM public.orders WHERE public.orders.amount = $1 LIMIT 5) ORDER BY id ASC LIMIT 3 OFFSET 1",
         &["1"]),
        ("SELECT id FROM users UNION ALL SELECT id FROM departments ORDER BY id DESC",
         "SELECT public.users.id FROM public.users UNION ALL SELECT public.departments.id FROM public.departments ORDER BY id DESC",
         "SELECT public.users.id FROM public.users UNION ALL SELECT public.departments.id FROM public.departments ORDER BY id DESC",
         &[]),
        ("SELECT id FROM users WHERE id > 1 INTERSECT SELECT id FROM departments WHERE id < 9",
         "SELECT public.users.id FROM public.users WHERE public.users.id > 1 INTERSECT SELECT public.departments.id FROM public.departments WHERE public.departments.id < 9",
         "SELECT public.users.id FROM public.users WHERE public.users.id > $1 INTERSECT SELECT public.departments.id FROM public.departments WHERE public.departments.id < $2",
         &["1", "9"]),
        ("SELECT name, count(*) AS n FROM users GROUP BY name HAVING count(*) > 1 ORDER BY n DESC, name",
         "SELECT public.users.name, count(*) AS n FROM public.users GROUP BY public.users.name HAVING count(*) > 1 ORDER BY n DESC, name ASC",
         "SELECT public.users.name, count(*) AS n FROM public.users GROUP BY public.users.name HAVING count(*) > $1 ORDER BY n DESC, name ASC",
         &["1"]),
        ("SELECT rank() OVER (PARTITION BY dept_id ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM users",
         "SELECT rank() OVER (PARTITION BY public.users.dept_id ORDER BY public.users.id ASC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM public.users",
         "SELECT rank() OVER (PARTITION BY public.users.dept_id ORDER BY public.users.id ASC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM public.users",
         &[]),
        ("SELECT count(*) FILTER (WHERE amount > 2), string_agg(sku, ',' ORDER BY items.id) FROM orders JOIN items ON items.order_id = orders.id",
         "SELECT count(*) FILTER (WHERE public.orders.amount > 2), string_agg(public.items.sku, ',' ORDER BY public.items.id ASC) FROM public.orders JOIN public.items ON public.items.order_id = public.orders.id",
         "SELECT count(*) FILTER (WHERE public.orders.amount > 2), string_agg(public.items.sku, ',' ORDER BY public.items.id ASC) FROM public.orders JOIN public.items ON public.items.order_id = public.orders.id",
         &[]),
        ("SELECT DISTINCT dept_id, CASE WHEN name = 'a' THEN 1 ELSE 0 END FROM users WHERE id = ANY (ARRAY[1, 2]) AND name NOT IN ('x', 'y') AND id BETWEEN 1 AND 9",
         "SELECT DISTINCT public.users.dept_id, CASE WHEN public.users.name = 'a' THEN 1 ELSE 0 END FROM public.users WHERE public.users.id = ANY (ARRAY[1, 2]) AND public.users.name NOT IN ('x', 'y') AND public.users.id BETWEEN 1 AND 9",
         "SELECT DISTINCT public.users.dept_id, CASE WHEN public.users.name = 'a' THEN 1 ELSE 0 END FROM public.users WHERE public.users.id = ANY (ARRAY[$1, $2]) AND public.users.name NOT IN ($3, $4) AND public.users.id BETWEEN $5 AND $6",
         &["1", "2", "'x'", "'y'", "1", "9"]),
        ("SELECT (id)::text FROM users WHERE id IS NOT NULL AND NOT (id = 1 OR name = 'a')",
         "SELECT (public.users.id)::text FROM public.users WHERE public.users.id IS NOT NULL AND NOT (public.users.id = 1 OR public.users.name = 'a')",
         "SELECT (public.users.id)::text FROM public.users WHERE public.users.id IS NOT NULL AND NOT (public.users.id = $1 OR public.users.name = $2)",
         &["1", "'a'"]),
        ("SELECT u.name, (SELECT max(o.amount) FROM orders o WHERE o.user_id = u.id) AS top FROM users u ORDER BY top",
         "SELECT u.name, (SELECT max(o.amount) FROM public.orders o WHERE o.user_id = u.id) AS top FROM public.users u ORDER BY top ASC",
         "SELECT u.name, (SELECT max(o.amount) FROM public.orders o WHERE o.user_id = u.id) AS top FROM public.users u ORDER BY top ASC",
         &[]),
        ("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) v",
         "SELECT FROM (VALUES (1, 'a'), (2, 'b')) v",
         "SELECT FROM (VALUES (1, 'a'), (2, 'b')) v",
         &[]),
        ("SELECT u.id FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.amount = 3) AND NOT EXISTS (SELECT 1 FROM items i WHERE i.id = u.id)",
         "SELECT u.id FROM public.users u WHERE EXISTS (SELECT 1 FROM public.orders o WHERE o.user_id = u.id AND o.amount = 3) AND NOT EXISTS (SELECT 1 FROM public.items i WHERE i.id = u.id)",
         "SELECT u.id FROM public.users u WHERE EXISTS (SELECT 1 FROM public.orders o WHERE o.user_id = u.id AND o.amount = $1) AND NOT EXISTS (SELECT 1 FROM public.items i WHERE i.id = u.id)",
         &["3"]),
        ("SELECT a.id, b.id FROM users a CROSS JOIN departments b WHERE a.id = b.id",
         "SELECT a.id, b.id FROM public.users a CROSS JOIN public.departments b WHERE a.id = b.id",
         "SELECT a.id, b.id FROM public.users a CROSS JOIN public.departments b WHERE a.id = b.id",
         &[]),
        ("SELECT (amount * 2) + 1 FROM orders WHERE amount <> ALL (ARRAY[1]) ORDER BY 1",
         "SELECT ((public.orders.amount * 2) + 1) FROM public.orders WHERE public.orders.amount != ALL (ARRAY[1]) ORDER BY 1 ASC",
         "SELECT ((public.orders.amount * 2) + 1) FROM public.orders WHERE public.orders.amount != ALL (ARRAY[$1]) ORDER BY 1 ASC",
         &["1"]),
    ];

    /// The corpus queries rendered with a stand-in for their first base
    /// table's slot: `(VALUES (1, 2)) stand_in(a, b)` in its place and every
    /// reference to it re-qualified.
    #[rustfmt::skip]
    const SOURCE_EXPECTED: &[(&str, &str)] = &[
        ("SELECT u.id, name FROM users u WHERE dept_id = 'x'",
         "SELECT stand_in.id, stand_in.name FROM (VALUES (1, 2)) stand_in(a, b) WHERE stand_in.dept_id = 'x'"),
        ("SELECT u.id, d.name AS dname FROM users u JOIN departments d ON u.dept_id = d.id WHERE d.name = 'a' ORDER BY dname, u.id DESC",
         "SELECT stand_in.id, d.name AS dname FROM (VALUES (1, 2)) stand_in(a, b) JOIN public.departments d ON stand_in.dept_id = d.id WHERE d.name = 'a' ORDER BY dname ASC, stand_in.id DESC"),
        ("SELECT * FROM users JOIN departments USING (id)",
         "SELECT stand_in.id AS id, stand_in.name, stand_in.dept_id, public.departments.name, public.departments.manager_id FROM (VALUES (1, 2)) stand_in(a, b) JOIN public.departments USING (id)"),
        ("SELECT * FROM users LEFT JOIN departments USING (id) WHERE id = '1'",
         "SELECT coalesce(stand_in.id, public.departments.id) AS id, stand_in.name, stand_in.dept_id, public.departments.name, public.departments.manager_id FROM (VALUES (1, 2)) stand_in(a, b) LEFT JOIN public.departments USING (id) WHERE coalesce(stand_in.id, public.departments.id) = '1'"),
        ("SELECT id, count(*) FROM users JOIN departments USING (id) GROUP BY id",
         "SELECT stand_in.id, count(*) FROM (VALUES (1, 2)) stand_in(a, b) JOIN public.departments USING (id) GROUP BY stand_in.id"),
        ("SELECT * FROM users NATURAL JOIN departments",
         "SELECT stand_in.id AS id, stand_in.name AS name, stand_in.dept_id, public.departments.manager_id FROM (VALUES (1, 2)) stand_in(a, b) JOIN public.departments USING (id, name)"),
        ("SELECT d.name FROM departments d WHERE EXISTS (SELECT 1 FROM users u WHERE u.dept_id = d.id)",
         "SELECT stand_in.name FROM (VALUES (1, 2)) stand_in(a, b) WHERE EXISTS (SELECT 1 FROM public.users u WHERE u.dept_id = stand_in.id)"),
        ("SELECT v.total, v.uid FROM (SELECT user_id AS uid, amount AS total FROM orders) v WHERE v.total = '5'",
         "SELECT v.total, v.uid FROM (SELECT stand_in.user_id AS uid, stand_in.amount AS total FROM (VALUES (1, 2)) stand_in(a, b)) v WHERE v.total = '5'"),
        ("SELECT id FROM users UNION SELECT id FROM departments ORDER BY id",
         "SELECT stand_in.id FROM (VALUES (1, 2)) stand_in(a, b) UNION SELECT public.departments.id FROM public.departments ORDER BY id ASC"),
        ("SELECT name, count(*) FROM users GROUP BY name HAVING count(*) > 1 ORDER BY count(*) DESC",
         "SELECT stand_in.name, count(*) FROM (VALUES (1, 2)) stand_in(a, b) GROUP BY stand_in.name HAVING count(*) > 1 ORDER BY count(*) DESC"),
        ("SELECT u.name, (SELECT max(o.amount) FROM orders o WHERE o.user_id = u.id) FROM users u WHERE u.id IN (SELECT user_id FROM orders WHERE amount = '1')",
         "SELECT u.name, (SELECT max(stand_in.amount) FROM (VALUES (1, 2)) stand_in(a, b) WHERE stand_in.user_id = u.id) FROM public.users u WHERE u.id IN (SELECT public.orders.user_id FROM public.orders WHERE public.orders.amount = '1')"),
        ("SELECT u.* FROM users u JOIN departments d ON d.id = u.dept_id WHERE d.id = ANY(ARRAY['1', '2']) AND u.name LIKE 'a%'",
         "SELECT stand_in.id, stand_in.name, stand_in.dept_id FROM (VALUES (1, 2)) stand_in(a, b) JOIN public.departments d ON d.id = stand_in.dept_id WHERE d.id = ANY (ARRAY['1', '2']) AND stand_in.name LIKE 'a%'"),
        ("SELECT CASE WHEN amount = '0' THEN sku ELSE order_id END FROM orders o JOIN items i ON i.order_id = o.id ORDER BY i.sku NULLS FIRST",
         "SELECT CASE WHEN stand_in.amount = '0' THEN i.sku ELSE i.order_id END FROM (VALUES (1, 2)) stand_in(a, b) JOIN public.items i ON i.order_id = stand_in.id ORDER BY i.sku ASC NULLS FIRST"),
        ("SELECT * FROM users u LEFT JOIN departments d USING (id) ORDER BY id",
         "SELECT coalesce(stand_in.id, d.id) AS id, stand_in.name, stand_in.dept_id, d.name, d.manager_id FROM (VALUES (1, 2)) stand_in(a, b) LEFT JOIN public.departments d USING (id) ORDER BY id ASC"),
        ("SELECT u.*, d.name FROM users u NATURAL JOIN departments d",
         "SELECT stand_in.id, stand_in.name, stand_in.dept_id, d.name FROM (VALUES (1, 2)) stand_in(a, b) JOIN public.departments d USING (id, name)"),
        ("SELECT * FROM users NATURAL LEFT JOIN departments WHERE id = 1",
         "SELECT coalesce(stand_in.id, public.departments.id) AS id, coalesce(stand_in.name, public.departments.name) AS name, stand_in.dept_id, public.departments.manager_id FROM (VALUES (1, 2)) stand_in(a, b) LEFT JOIN public.departments USING (id, name) WHERE coalesce(stand_in.id, public.departments.id) = 1"),
        ("SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount > 5 ORDER BY o.amount DESC NULLS LAST, u.id",
         "SELECT stand_in.id, stand_in.name, stand_in.dept_id, o.id, o.user_id, o.amount FROM (VALUES (1, 2)) stand_in(a, b) JOIN public.orders o ON stand_in.id = o.user_id WHERE o.amount > 5 ORDER BY o.amount DESC NULLS LAST, stand_in.id ASC"),
        ("SELECT id FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount = 1 LIMIT 5) ORDER BY id LIMIT 3 OFFSET 1",
         "SELECT stand_in.id FROM (VALUES (1, 2)) stand_in(a, b) WHERE stand_in.id IN (SELECT public.orders.user_id FROM public.orders WHERE public.orders.amount = 1 LIMIT 5) ORDER BY id ASC LIMIT 3 OFFSET 1"),
        ("SELECT id FROM users UNION ALL SELECT id FROM departments ORDER BY id DESC",
         "SELECT stand_in.id FROM (VALUES (1, 2)) stand_in(a, b) UNION ALL SELECT public.departments.id FROM public.departments ORDER BY id DESC"),
        ("SELECT id FROM users WHERE id > 1 INTERSECT SELECT id FROM departments WHERE id < 9",
         "SELECT stand_in.id FROM (VALUES (1, 2)) stand_in(a, b) WHERE stand_in.id > 1 INTERSECT SELECT public.departments.id FROM public.departments WHERE public.departments.id < 9"),
        ("SELECT name, count(*) AS n FROM users GROUP BY name HAVING count(*) > 1 ORDER BY n DESC, name",
         "SELECT stand_in.name, count(*) AS n FROM (VALUES (1, 2)) stand_in(a, b) GROUP BY stand_in.name HAVING count(*) > 1 ORDER BY n DESC, name ASC"),
        ("SELECT rank() OVER (PARTITION BY dept_id ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM users",
         "SELECT rank() OVER (PARTITION BY stand_in.dept_id ORDER BY stand_in.id ASC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM (VALUES (1, 2)) stand_in(a, b)"),
        ("SELECT count(*) FILTER (WHERE amount > 2), string_agg(sku, ',' ORDER BY items.id) FROM orders JOIN items ON items.order_id = orders.id",
         "SELECT count(*) FILTER (WHERE stand_in.amount > 2), string_agg(public.items.sku, ',' ORDER BY public.items.id ASC) FROM (VALUES (1, 2)) stand_in(a, b) JOIN public.items ON public.items.order_id = stand_in.id"),
        ("SELECT DISTINCT dept_id, CASE WHEN name = 'a' THEN 1 ELSE 0 END FROM users WHERE id = ANY (ARRAY[1, 2]) AND name NOT IN ('x', 'y') AND id BETWEEN 1 AND 9",
         "SELECT DISTINCT stand_in.dept_id, CASE WHEN stand_in.name = 'a' THEN 1 ELSE 0 END FROM (VALUES (1, 2)) stand_in(a, b) WHERE stand_in.id = ANY (ARRAY[1, 2]) AND stand_in.name NOT IN ('x', 'y') AND stand_in.id BETWEEN 1 AND 9"),
        ("SELECT (id)::text FROM users WHERE id IS NOT NULL AND NOT (id = 1 OR name = 'a')",
         "SELECT (stand_in.id)::text FROM (VALUES (1, 2)) stand_in(a, b) WHERE stand_in.id IS NOT NULL AND NOT (stand_in.id = 1 OR stand_in.name = 'a')"),
        ("SELECT u.name, (SELECT max(o.amount) FROM orders o WHERE o.user_id = u.id) AS top FROM users u ORDER BY top",
         "SELECT u.name, (SELECT max(stand_in.amount) FROM (VALUES (1, 2)) stand_in(a, b) WHERE stand_in.user_id = u.id) AS top FROM public.users u ORDER BY top ASC"),
        ("SELECT u.id FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.amount = 3) AND NOT EXISTS (SELECT 1 FROM items i WHERE i.id = u.id)",
         "SELECT stand_in.id FROM (VALUES (1, 2)) stand_in(a, b) WHERE EXISTS (SELECT 1 FROM public.orders o WHERE o.user_id = stand_in.id AND o.amount = 3) AND NOT EXISTS (SELECT 1 FROM public.items i WHERE i.id = stand_in.id)"),
        ("SELECT a.id, b.id FROM users a CROSS JOIN departments b WHERE a.id = b.id",
         "SELECT stand_in.id, b.id FROM (VALUES (1, 2)) stand_in(a, b) CROSS JOIN public.departments b WHERE stand_in.id = b.id"),
        ("SELECT (amount * 2) + 1 FROM orders WHERE amount <> ALL (ARRAY[1]) ORDER BY 1",
         "SELECT ((stand_in.amount * 2) + 1) FROM (VALUES (1, 2)) stand_in(a, b) WHERE stand_in.amount != ALL (ARRAY[1]) ORDER BY 1 ASC"),
    ];

    /// The corpus queries whose unlimited rendering differs from the serve
    /// SQL: the outer LIMIT and OFFSET dropped, inner ones kept.
    #[rustfmt::skip]
    const UNLIMITED_EXPECTED: &[(&str, &str)] = &[
        ("SELECT id FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount = 1 LIMIT 5) ORDER BY id LIMIT 3 OFFSET 1",
         "SELECT public.users.id FROM public.users WHERE public.users.id IN (SELECT public.orders.user_id FROM public.orders WHERE public.orders.amount = 1 LIMIT 5) ORDER BY id ASC"),
    ];

    /// Every corpus query renders to its captured serve SQL, shape SQL and
    /// shape literals.
    #[test]
    fn test_render_corpus_expectations() {
        let catalog = binding_catalog();
        let mut expected = RENDER_EXPECTED.iter();
        for sql in corpus() {
            let (expected_sql, served, shape_sql, literals) = expected
                .next()
                .unwrap_or_else(|| panic!("no expectation for {sql}"));
            assert_eq!(
                *expected_sql, sql,
                "the expectations follow the corpus order"
            );
            let bound = bound(sql, &catalog);
            assert_eq!(bound.sql(), *served, "{sql}");
            let shape = bound.shape();
            assert_eq!(shape.sql, *shape_sql, "{sql}");
            let rendered: Vec<String> = shape
                .literals
                .iter()
                .map(|literal| {
                    let mut text = String::new();
                    literal.deparse(&mut text);
                    text
                })
                .collect();
            assert_eq!(rendered, *literals, "{sql}");
        }
        assert!(expected.next().is_none(), "every expectation is used");
    }

    #[test]
    fn test_render_with_source_expectations() {
        let catalog = binding_catalog();
        let mut expected = SOURCE_EXPECTED.iter();
        for sql in corpus() {
            let bound = bound(sql, &catalog);
            let Some(slot) = bound
                .tree()
                .root()
                .node_ref()
                .subtree()
                .find(|position| matches!(position.kind(), NodeKind::Table { .. }))
                .and_then(|table| bound.bindings().source(table.id()))
            else {
                continue;
            };
            let (expected_sql, with_source) = expected
                .next()
                .unwrap_or_else(|| panic!("no expectation for {sql}"));
            assert_eq!(
                *expected_sql, sql,
                "the expectations follow the corpus order"
            );
            let columns = [EcoString::from("a"), EcoString::from("b")];
            let stand_in = SourceOverride {
                slot,
                alias: "stand_in",
                source: "VALUES (1, 2)",
                columns: &columns,
            };
            let mut actual = String::new();
            bound.sql_with_source_into(&stand_in, &mut actual);
            assert_eq!(actual, *with_source, "{sql}");
        }
        assert!(expected.next().is_none(), "every expectation is used");
    }

    /// The unlimited rendering is the serve SQL without the outer LIMIT and
    /// OFFSET.
    #[test]
    fn test_render_unlimited_expectations() {
        let catalog = binding_catalog();
        let unlimited_of = |bound: &ResolvedQuery| {
            let mut text = String::new();
            query_unlimited_deparse_with(
                bound.tree().root(),
                &mut Bound::text(bound.bindings()),
                &mut text,
            );
            text
        };
        for sql in corpus() {
            let bound = bound(sql, &catalog);
            let expected = UNLIMITED_EXPECTED
                .iter()
                .find(|(expected_sql, _)| *expected_sql == sql)
                .map_or_else(|| bound.sql(), |(_, unlimited)| (*unlimited).to_owned());
            assert_eq!(unlimited_of(&bound), expected, "{sql}");
        }
    }

    /// PGC-287 class 2: a bare and a qualified spelling of one table share
    /// the identity, LIMIT does not enter it, and a different table or
    /// literal does.
    #[test]
    fn test_bound_fingerprint_identity() {
        let catalog = binding_catalog();
        let fingerprint = |sql: &str| bound(sql, &catalog).fingerprint();
        let bare = fingerprint("SELECT id FROM users WHERE id = 1");
        assert_eq!(
            bare,
            fingerprint("SELECT id FROM public.users WHERE id = 1")
        );
        assert_eq!(
            bare,
            fingerprint("SELECT id FROM users WHERE id = 1 LIMIT 5 OFFSET 2")
        );
        assert_ne!(bare, fingerprint("SELECT id FROM orders WHERE id = 1"));
        assert_ne!(bare, fingerprint("SELECT id FROM users WHERE id = 2"));
    }

    #[test]
    fn test_bound_fingerprint_shares_no_value_with_other_domains() {
        let catalog = binding_catalog();
        let sql = "SELECT id FROM users WHERE id = 1";
        let bound = bound(sql, &catalog);
        assert_ne!(
            bound.fingerprint().get(),
            query_tree_session_key(
                bound.tree(),
                &["public".into()],
                &LiteralSettings::default()
            )
            .get()
        );
    }

    #[test]
    fn test_star_over_using_join_renders_merged_columns_first() {
        let catalog = binding_catalog();
        assert_eq!(
            bound("SELECT * FROM users JOIN departments USING (id)", &catalog).sql(),
            "SELECT public.users.id AS id, public.users.name, public.users.dept_id, \
             public.departments.name, public.departments.manager_id \
             FROM public.users JOIN public.departments USING (id)"
        );
        assert_eq!(
            bound(
                "SELECT * FROM users u LEFT JOIN departments d USING (id)",
                &catalog
            )
            .sql(),
            "SELECT coalesce(u.id, d.id) AS id, u.name, u.dept_id, d.name, d.manager_id \
             FROM public.users u LEFT JOIN public.departments d USING (id)"
        );
        assert_eq!(
            bound(
                "SELECT d.* FROM users u JOIN departments d USING (id)",
                &catalog
            )
            .sql(),
            "SELECT d.id, d.name, d.manager_id FROM public.users u JOIN public.departments d \
             USING (id)"
        );
    }

    #[test]
    fn test_order_by_output_name_renders_unqualified() {
        let catalog = binding_catalog();
        assert_eq!(
            bound(
                "SELECT u.name AS label FROM users u ORDER BY label DESC",
                &catalog
            )
            .sql(),
            "SELECT u.name AS label FROM public.users u ORDER BY label DESC"
        );
        assert_eq!(
            bound("SELECT u.name FROM users u ORDER BY dept_id", &catalog).sql(),
            "SELECT u.name FROM public.users u ORDER BY u.dept_id ASC"
        );
    }

    #[test]
    fn test_natural_join_without_common_columns_renders_cross_join() {
        let catalog = catalog_with_columns(&[("a", &["x"]), ("b", &["y"])]);
        assert_eq!(
            bound("SELECT * FROM a NATURAL JOIN b", &catalog).sql(),
            "SELECT public.a.x, public.b.y FROM public.a CROSS JOIN public.b"
        );
    }

    #[test]
    fn test_cte_reference_renders_inline_under_its_alias() {
        let catalog = binding_catalog();
        assert_eq!(
            bound(
                "WITH t AS (SELECT id FROM users) SELECT x.id FROM t AS x",
                &catalog
            )
            .sql(),
            "SELECT x.id FROM (SELECT public.users.id FROM public.users) x"
        );
        assert_eq!(
            bound(
                "WITH t AS (SELECT id FROM users) SELECT t.id FROM t",
                &catalog
            )
            .sql(),
            "SELECT t.id FROM (SELECT public.users.id FROM public.users) t"
        );
    }

    /// Only literals in predicate position bind (PGC-294): a projection,
    /// ordering, `VALUES` or `LIMIT` position, and everything beneath one,
    /// stays inline, while predicates beneath a predicate bind.
    #[test]
    fn test_shape_binds_only_predicate_literals() {
        use LiteralValue::{Integer, String as Text};
        #[rustfmt::skip]
        let cases: [(&str, Vec<LiteralValue>, &str); 7] = [
            ("SELECT (SELECT count(*) FROM orders WHERE amount = 1) FROM users WHERE id = 2",
             vec![Integer(2)], "amount = 1"),
            ("SELECT id FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount = 1 LIMIT 5)",
             vec![Integer(1)], "LIMIT 5"),
            ("SELECT * FROM (VALUES (1)) v WHERE 2 = 3",
             vec![Integer(2), Integer(3)], "VALUES (1)"),
            ("SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id AND o.amount = 3",
             vec![Integer(3)], "ON u.id = o.user_id AND o.amount = $1"),
            ("SELECT id FROM users GROUP BY id HAVING count(*) > 0 ORDER BY 1",
             vec![Integer(0)], "ORDER BY 1"),
            ("SELECT 7, name FROM users WHERE name = 'first' AND id = 2",
             vec![Text("first".into()), Integer(2)], "SELECT 7,"),
            ("SELECT id FROM users WHERE id = ANY (ARRAY[1, 2]) AND name = '2024-01-01'::date",
             vec![Integer(1), Integer(2), Text("2024-01-01".into())], "($3)::date"),
        ];
        let catalog = binding_catalog();
        for (sql, literals, inline) in cases {
            let shape = bound(sql, &catalog).shape();
            assert_eq!(shape.literals, literals, "{sql}");
            assert!(shape.sql.contains(inline), "{sql}: {}", shape.sql);
        }
    }

    /// `$N` binds `literals[N-1]`, wherever it sits in the shape SQL.
    #[test]
    fn test_shape_round_trips_through_its_literals() {
        let catalog = binding_catalog();
        for sql in corpus() {
            let bound = bound(sql, &catalog);
            let shape = bound.shape();
            let mut restored = shape.sql.to_string();
            for (index, literal) in shape.literals.iter().enumerate().rev() {
                let mut text = String::new();
                literal.deparse(&mut text);
                restored = restored.replace(&format!("${}", index + 1), &text);
            }
            assert_eq!(restored, bound.sql(), "{sql}");
        }
    }

    #[test]
    fn test_shape_is_shared_across_literal_values() {
        let catalog = binding_catalog();
        let first = bound("SELECT id FROM users WHERE name = 'user1'", &catalog).shape();
        let second = bound("SELECT id FROM users WHERE name = 'user2'", &catalog).shape();
        assert_eq!(first.sql, second.sql);
        assert_eq!(first.key, second.key);
        assert_eq!(first.literals, vec![LiteralValue::String("user1".into())]);
        assert_eq!(second.literals, vec![LiteralValue::String("user2".into())]);
        let by_id = bound("SELECT id FROM users WHERE id = 1", &catalog).shape();
        assert_ne!(first.key, by_id.key);
    }

    #[test]
    fn test_relation_oids_cover_subqueries_once() {
        let catalog = binding_catalog();
        let oids = bound(
            "SELECT u.id FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id) \
             AND u.id IN (SELECT user_id FROM orders)",
            &catalog,
        )
        .relation_oids();
        let mut expected: Vec<Oid> = ["users", "orders"]
            .iter()
            .map(|name| {
                catalog
                    .get2(&("public", *name))
                    .expect("table")
                    .relation_oid
            })
            .collect();
        expected.sort_unstable();
        assert_eq!(oids, expected);
    }
}
