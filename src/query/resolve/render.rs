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
    Column as RopeColumn, CteRef as RopeCteRef, Function as RopeFunction, Join as RopeJoin,
    OrderBy as RopeOrderBy, QueryTree, RopeDeparseContext, ScalarKind, Select as RopeSelect,
    SelectItem, Star as RopeStar, Table as RopeTable,
};
use crate::query::ast::{
    Clause, ColumnNode, CteRefNode, Deparse, DeparseContext, DeparseWith, FunctionCall, JoinNode,
    LiteralValue, NodeId, StarNode, TableNode, separated_deparse,
};
use crate::query::shape::{QueryShape, ShapeKey};
use crate::query::transform::literal_is_parameterizable;

/// Keeps a bound fingerprint from colliding with a resolved fingerprint (3)
/// or a session key (2) hashed from the same text.
const BOUND_FINGERPRINT_DOMAIN: u8 = 4;

/// `tree` rendered as the cache serves it under `bindings`.
pub(crate) fn query_tree_sql_into(tree: &QueryTree, bindings: &Bindings, buf: &mut String) {
    query_deparse_with(tree.root(), &mut RopeBound(Bound::text(bindings)), buf);
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

    /// One star-expanded output: its rendered expression and its name.
    pub(super) fn star_item_output(&self, item: &StarItem) -> Output {
        let mut text = String::new();
        let name = match item {
            StarItem::Column(binding) => {
                self.column_binding_deparse(binding, &mut text);
                self.bindings
                    .binding_column(binding)
                    .map(|c| c.name.clone())
            }
            StarItem::Merged(id) => {
                let merged = self.bindings.merged(*id);
                if let Some(merged) = merged {
                    self.merged_deparse(merged, &mut text);
                }
                merged.map(|merged| merged.name.clone())
            }
        };
        Output { text, name }
    }

    fn literals_take(self) -> Vec<LiteralValue> {
        match self.mode {
            Mode::Text => Vec::new(),
            Mode::Shape { literals, .. } => literals,
        }
    }
}

impl DeparseContext for Bound<'_> {
    fn table_deparse(&mut self, node: &TableNode, buf: &mut String) {
        self.table_render(
            self.bindings.source(node.id),
            |buf| node.alias_deparse(buf),
            |buf| node.deparse_plain(buf),
            buf,
        );
    }

    fn column_deparse(&mut self, node: &ColumnNode, buf: &mut String) {
        self.column_render(
            self.bindings.column(node),
            &node.column,
            |buf| node.deparse_plain(buf),
            buf,
        );
    }

    fn star_deparse(&mut self, node: &StarNode, buf: &mut String) {
        self.star_render(self.bindings.star(node), |buf| node.deparse_plain(buf), buf);
    }

    /// The CTE body inline as a derived table, named by the reference's
    /// alias or the CTE's name, with the CTE's column aliases.
    fn cte_ref_deparse(&mut self, node: &CteRefNode, buf: &mut String) {
        buf.push_str(" (");
        node.query.deparse_with(self, buf);
        buf.push_str(") ");
        match &node.alias {
            Some(alias) => alias.name.deparse(buf),
            None => node.cte_name.deparse(buf),
        };
        if !node.column_aliases.is_empty() {
            buf.push('(');
            separated_deparse(buf, &node.column_aliases, ", ");
            buf.push(')');
        }
    }

    fn with_clause_renders(&self) -> bool {
        false
    }

    fn natural_join_columns(&mut self, join: &JoinNode) -> Option<Vec<EcoString>> {
        Some(
            self.bindings
                .merged_columns()
                .iter()
                .filter(|merged| merged.join == join.id)
                .map(|merged| merged.name.clone())
                .collect(),
        )
    }

    fn function_name_deparse(&mut self, call: &FunctionCall, buf: &mut String) {
        buf.push_str(&call.name);
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

/// The context that renders a bound rope: the same decisions as [`Bound`],
/// read through views and the rope-keyed bindings.
struct RopeBound<'a>(Bound<'a>);

impl RopeDeparseContext for RopeBound<'_> {
    fn table_deparse(&mut self, table: RopeTable<'_>, buf: &mut String) {
        self.0.table_render(
            self.0.bindings.source(table.id()),
            |buf| table_alias_deparse(table, buf),
            |buf| table_plain_deparse(table, buf),
            buf,
        );
    }

    fn column_deparse(&mut self, column: RopeColumn<'_>, buf: &mut String) {
        self.0.column_render(
            self.0.bindings.column_by_id(column.id()),
            column.column(),
            |buf| column_plain_deparse(column, buf),
            buf,
        );
    }

    fn star_deparse(&mut self, star: RopeStar<'_>, buf: &mut String) {
        self.0.star_render(
            self.0.bindings.star_by_id(star.id()),
            |buf| star_plain_deparse(star, buf),
            buf,
        );
    }

    /// The CTE body inline as a derived table, named by the reference's
    /// alias or the CTE's name, with the CTE's column aliases.
    fn cte_ref_deparse(&mut self, cte_ref: RopeCteRef<'_>, buf: &mut String) {
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

    fn natural_join_columns(&mut self, join: RopeJoin<'_>) -> Option<Vec<EcoString>> {
        let join = join.id();
        Some(
            self.0
                .bindings
                .merged_columns()
                .iter()
                .filter(|merged| merged.join.index() == join.index())
                .map(|merged| merged.name.clone())
                .collect(),
        )
    }

    fn function_name_deparse(&mut self, function: RopeFunction<'_>, buf: &mut String) {
        buf.push_str(function.name());
    }

    fn literal_deparse(&mut self, literal: &LiteralValue, buf: &mut String) {
        self.0.literal_render(literal, buf);
    }

    fn clause_enter(&mut self, clause: Clause) {
        self.0.clause_entered(clause);
    }

    fn clause_leave(&mut self, clause: Clause) {
        self.0.clause_left(clause);
    }
}

/// Every column of the base table a FROM entry's slot names, as the cache
/// serves them: ` q.c1, q.c2, …` with the entry's qualifier; nothing for a
/// derived table or an unbound entry.
fn source_columns_render(bindings: &Bindings, source: NodeId, buf: &mut String) {
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
        buf.push(' ');
        Bound::slot_name_deparse(slot, buf);
        buf.push('.');
        column.name.deparse(buf);
        sep = ",";
    }
}

/// One select-list output after star expansion: its rendered expression and
/// its output name (the alias, else the column name of a column reference).
pub(super) struct Output {
    pub(super) text: String,
    pub(super) name: Option<EcoString>,
}

impl ResolvedQuery {
    /// The context that renders this query's nodes as the cache serves
    /// them, for a caller composing its own SQL around parts of the tree.
    pub fn deparse_context(&self) -> impl RopeDeparseContext + '_ {
        RopeBound(Bound::text(self.bindings()))
    }

    /// [`ResolvedQuery::deparse_context`] with `source` standing in for one
    /// FROM entry.
    pub fn deparse_context_with_source<'a>(
        &'a self,
        source: &'a SourceOverride<'a>,
    ) -> impl RopeDeparseContext + 'a {
        RopeBound(Bound::with_source(self.bindings(), source))
    }

    /// The serve SQL with `source` standing in for one FROM entry.
    pub fn sql_with_source_into(&self, source: &SourceOverride<'_>, buf: &mut String) {
        let mut cx = RopeBound(Bound::with_source(self.bindings(), source));
        query_deparse_with(self.tree().root(), &mut cx, buf);
    }

    /// Every column of the table a FROM entry names, as the cache serves
    /// them: ` q.c1, q.c2, …` with the entry's qualifier. Nothing for a
    /// derived table or an unbound entry.
    pub fn source_columns_into(&self, source: NodeId, buf: &mut String) {
        source_columns_render(self.bindings(), source, buf);
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
        let mut cx = RopeBound(Bound::text(self.bindings()));
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
        let mut cx = RopeBound(Bound::shape(self.bindings()));
        let mut sql = String::with_capacity(256);
        query_deparse_with(self.tree().root(), &mut cx, &mut sql);
        let literals = cx.0.literals_take();
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
    /// stars expanded: by output name when the item names an output, else
    /// by the rendered expression. `None` when no output matches.
    pub fn order_by_position(
        &self,
        select: RopeSelect<'_>,
        clause: RopeOrderBy<'_>,
    ) -> Option<usize> {
        let outputs = self.select_outputs(select);
        let expr = clause.expr();
        let found = if let ScalarKind::Column(column) = expr.kind()
            && matches!(
                self.column_target_at(column.id()),
                Some(ColumnTarget::Output)
            ) {
            outputs
                .iter()
                .position(|output| output.name.as_ref() == Some(column.column()))
        } else {
            let mut text = String::new();
            scalar_deparse_with(
                expr,
                &mut RopeBound(Bound::text(self.bindings())),
                &mut text,
            );
            outputs.iter().position(|output| output.text == text)
        };
        found.map(|index| index + 1)
    }

    fn select_outputs(&self, select: RopeSelect<'_>) -> Vec<Output> {
        let bindings = self.bindings();
        let cx = Bound::text(bindings);
        let Some(items) = select.items() else {
            return Vec::new();
        };
        let mut outputs = Vec::with_capacity(items.len());
        for item in items {
            match item {
                SelectItem::Expr(expr) => {
                    let mut text = String::new();
                    scalar_deparse_with(
                        expr.expr(),
                        &mut RopeBound(Bound::text(bindings)),
                        &mut text,
                    );
                    let name = expr
                        .alias()
                        .cloned()
                        .or_else(|| self.column_output_name(expr.expr()));
                    outputs.push(Output { text, name });
                }
                SelectItem::Star(star) => {
                    for item in bindings.star_by_id(star.id()).unwrap_or(&[]) {
                        outputs.push(cx.star_item_output(item));
                    }
                }
            }
        }
        outputs
    }

    /// An ORDER BY item rendered as the cache serves it.
    pub fn order_by_into(&self, clause: RopeOrderBy<'_>, buf: &mut String) {
        order_by_deparse_with(clause, &mut RopeBound(Bound::text(self.bindings())), buf);
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::catalog::Catalog;
    use crate::query::ast::rope::{NodeKind, View};
    use crate::query::ast::{AstNode, QueryExpr, query_expr_parse, query_tree_session_key};
    use crate::query::literal_settings::LiteralSettings;
    use crate::query::resolve::owned::sql_owned;
    use crate::query::resolve::query_expr_resolve;
    use crate::query::resolve::tests::{BINDING_CORPUS, binding_catalog, catalog_with_columns};

    /// Shapes the binding corpus leaves out: stars over outer and `NATURAL`
    /// joins, qualified stars, aliased and chained CTE references, set-op
    /// and alias ordering, nested predicates, windows, filters and casts.
    const RENDER_CORPUS: &[&str] = &[
        "SELECT * FROM users u LEFT JOIN departments d USING (id) ORDER BY id",
        "SELECT u.*, d.name FROM users u NATURAL JOIN departments d",
        "SELECT * FROM users NATURAL LEFT JOIN departments WHERE id = 1",
        "SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount > 5 \
         ORDER BY o.amount DESC NULLS LAST, u.id",
        "WITH t AS (SELECT id, name FROM users) SELECT x.name FROM t AS x WHERE x.id = 1",
        "WITH a AS (SELECT id FROM users), b AS (SELECT id FROM a WHERE id = 1) SELECT b.id FROM b",
        "SELECT id FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount = 1 LIMIT 5) \
         ORDER BY id LIMIT 3 OFFSET 1",
        "SELECT id FROM users UNION ALL SELECT id FROM departments ORDER BY id DESC",
        "SELECT id FROM users WHERE id > 1 INTERSECT SELECT id FROM departments WHERE id < 9",
        "SELECT name, count(*) AS n FROM users GROUP BY name HAVING count(*) > 1 ORDER BY n DESC, name",
        "SELECT rank() OVER (PARTITION BY dept_id ORDER BY id ROWS BETWEEN 1 PRECEDING AND \
         CURRENT ROW) FROM users",
        "SELECT count(*) FILTER (WHERE amount > 2), string_agg(sku, ',' ORDER BY items.id) \
         FROM orders JOIN items ON items.order_id = orders.id",
        "SELECT DISTINCT dept_id, CASE WHEN name = 'a' THEN 1 ELSE 0 END FROM users \
         WHERE id = ANY (ARRAY[1, 2]) AND name NOT IN ('x', 'y') AND id BETWEEN 1 AND 9",
        "SELECT (id)::text FROM users WHERE id IS NOT NULL AND NOT (id = 1 OR name = 'a')",
        "SELECT u.name, (SELECT max(o.amount) FROM orders o WHERE o.user_id = u.id) AS top \
         FROM users u ORDER BY top",
        "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) v",
        "SELECT u.id FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id \
         AND o.amount = 3) AND NOT EXISTS (SELECT 1 FROM items i WHERE i.id = u.id)",
        "SELECT a.id, b.id FROM users a CROSS JOIN departments b WHERE a.id = b.id",
        "SELECT (amount * 2) + 1 FROM orders WHERE amount <> ALL (ARRAY[1]) ORDER BY 1",
    ];

    fn corpus() -> impl Iterator<Item = &'static str> {
        BINDING_CORPUS.iter().chain(RENDER_CORPUS).copied()
    }

    fn bound(sql: &str, catalog: &Catalog) -> ResolvedQuery {
        ResolvedQuery::parse_resolve(sql, catalog, &["public"]).expect("resolve")
    }

    /// The owned tree and the owned resolver's bindings: what the owned
    /// renderer, the oracle for the rope renderer, reads.
    fn owned(sql: &str, catalog: &Catalog) -> (QueryExpr, Bindings) {
        let query = query_expr_parse(sql).expect("parse");
        let bindings = query_expr_resolve(&query, catalog, &["public"]).expect("resolve");
        (query, bindings)
    }

    fn owned_shape(tree: &QueryExpr, bindings: &Bindings) -> QueryShape {
        let mut cx = Bound::shape(bindings);
        let mut sql = String::new();
        tree.deparse_with(&mut cx, &mut sql);
        let literals = cx.literals_take();
        let mut hasher = DefaultHasher::new();
        sql.hash(&mut hasher);
        QueryShape {
            sql: sql.into(),
            key: ShapeKey::from_raw(hasher.finish()),
            literals,
        }
    }

    /// Every rendering of a bound query through the rope is byte for byte
    /// the owned renderer's: the serve SQL, the identity text, the shape
    /// and a rendering with a source standing in for the first table.
    #[test]
    fn test_rope_rendering_matches_the_owned_renderer() {
        let catalog = binding_catalog();
        for sql in corpus() {
            let (tree, bindings) = owned(sql, &catalog);
            let bound = bound(sql, &catalog);
            assert_eq!(bound.sql(), sql_owned(&tree, &bindings), "{sql}");
            let mut unlimited = String::new();
            tree.unlimited_deparse_with(&mut Bound::text(&bindings), &mut unlimited);
            let mut rope_unlimited = String::new();
            query_unlimited_deparse_with(
                bound.tree().root(),
                &mut RopeBound(Bound::text(bound.bindings())),
                &mut rope_unlimited,
            );
            assert_eq!(rope_unlimited, unlimited, "{sql}");
            let expected = owned_shape(&tree, &bindings);
            let shape = bound.shape();
            assert_eq!(shape.sql, expected.sql, "{sql}");
            assert_eq!(shape.key, expected.key, "{sql}");
            assert_eq!(shape.literals, expected.literals, "{sql}");
            // The first table's slot in each resolver's own slot order.
            let owned_slot = tree
                .nodes::<TableNode>()
                .next()
                .and_then(|table| bindings.source(table.id));
            let rope_slot = bound
                .tree()
                .root()
                .node_ref()
                .subtree()
                .find(|position| matches!(position.kind(), NodeKind::Table { .. }))
                .and_then(|table| bound.bindings().source(table.id()));
            let (Some(owned_slot), Some(rope_slot)) = (owned_slot, rope_slot) else {
                continue;
            };
            let columns = [EcoString::from("a"), EcoString::from("b")];
            let stand_in = |slot| SourceOverride {
                slot,
                alias: "stand_in",
                source: "VALUES (1, 2)",
                columns: &columns,
            };
            let mut expected = String::new();
            tree.deparse_with(
                &mut Bound::with_source(&bindings, &stand_in(owned_slot)),
                &mut expected,
            );
            let mut actual = String::new();
            bound.sql_with_source_into(&stand_in(rope_slot), &mut actual);
            assert_eq!(actual, expected, "{sql}");
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
