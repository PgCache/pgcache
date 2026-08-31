//! Heuristic catalog synthesized from the query corpus itself — the v0
//! schema-less mode. Every referenced relation is assumed to be a table with
//! a primary key; unqualified names live in `public`; column types default to
//! text unless literal comparisons give numeric evidence (constraint-range
//! implication uses column ordering, and text ordering disagrees with numeric,
//! so all-text columns would silently mis-count subsumption).

use std::collections::{HashMap, HashSet};

use ecow::EcoString;
use iddqd::BiHashMap;
use pgcache_lib::catalog::{ColumnMetadata, ColumnStore, TableMetadata};
use pgcache_lib::oid::Oid;
use pgcache_lib::query::ast::{
    AstNode, ColumnNode, JoinQual, LiteralValue, OrderByClause, QueryBody, QueryExpr, ScalarExpr,
    SelectColumn, SelectColumns, SelectNode, TableSource, WhereExpr,
};
use postgres_types::Type;

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct SynthesisStats {
    pub tables: usize,
    /// Unqualified columns attributed to the first FROM-order table because
    /// several tables were in scope.
    pub heuristic_attributions: usize,
    /// Columns typed int8/float8 from literal-comparison evidence.
    pub inferred_columns: usize,
    /// Columns with conflicting literal evidence, left as text.
    pub conflicted_columns: usize,
    /// Unqualified columns skipped because a derived source (subquery/CTE)
    /// was in scope, so table membership could not be assumed.
    pub skipped_unqualified: usize,
}

pub struct SynthCatalog {
    pub tables: BiHashMap<TableMetadata>,
    pub stats: SynthesisStats,
}

pub fn catalog_synthesize<'a>(queries: impl IntoIterator<Item = &'a QueryExpr>) -> SynthCatalog {
    let mut synthesizer = Synthesizer::default();
    for query in queries {
        let mut scopes = Vec::new();
        synthesizer.query_walk(query, &mut scopes);
    }
    synthesizer.finish()
}

type TableKey = (EcoString, EcoString);

#[derive(Clone, Copy, PartialEq)]
enum TypeEvidence {
    Integer,
    Float,
    /// Only string/boolean literals observed — the text default is right,
    /// and there is nothing conflicting to report.
    Text,
    /// Mixed numeric and text evidence; left as text and disclosed.
    Conflicted,
}

#[derive(Default)]
struct TableFacts {
    columns: Vec<EcoString>,
    seen: HashSet<EcoString>,
    evidence: HashMap<EcoString, TypeEvidence>,
}

struct ScopeEntry {
    alias: EcoString,
    /// `None` for derived sources (subqueries, CTE references) whose columns
    /// are not real table columns.
    key: Option<TableKey>,
}

#[derive(Default)]
struct Scope {
    entries: Vec<ScopeEntry>,
    has_derived: bool,
}

#[derive(Default)]
struct Synthesizer {
    tables: HashMap<TableKey, TableFacts>,
    order: Vec<TableKey>,
    heuristic_attributions: usize,
    skipped_unqualified: usize,
}

impl Synthesizer {
    fn query_walk(&mut self, query: &QueryExpr, scopes: &mut Vec<Scope>) {
        // CTE bodies are walked once at their definition; references in FROM
        // are treated as derived sources.
        for cte in &query.ctes {
            let mut fresh = Vec::new();
            self.query_walk(&cte.query, &mut fresh);
        }
        match &query.body {
            QueryBody::Select(select) => self.select_walk(select, &query.order_by, scopes),
            QueryBody::SetOp(set_op) => {
                self.query_walk(&set_op.left, scopes);
                self.query_walk(&set_op.right, scopes);
            }
            QueryBody::Values(_) => {}
        }
    }

    fn select_walk(
        &mut self,
        select: &SelectNode,
        order_by: &[OrderByClause],
        scopes: &mut Vec<Scope>,
    ) {
        let mut scope = Scope::default();
        let mut join_quals: Vec<&WhereExpr> = Vec::new();
        for source in &select.from {
            self.table_source_collect(source, &mut scope, scopes, &mut join_quals);
        }
        scopes.push(scope);

        let output_aliases: HashSet<&EcoString> = match &select.columns {
            SelectColumns::Columns(columns) => columns
                .iter()
                .filter_map(|c| match c {
                    SelectColumn::Expr {
                        alias: Some(alias), ..
                    } => Some(alias),
                    _ => None,
                })
                .collect(),
            SelectColumns::None => HashSet::new(),
        };

        if let SelectColumns::Columns(columns) = &select.columns {
            for column in columns {
                if let SelectColumn::Expr { expr, .. } = column {
                    self.expr_attribute(expr, scopes);
                }
            }
        }
        for qual in join_quals {
            self.expr_attribute(qual, scopes);
            self.where_infer(qual, scopes);
        }
        if let Some(where_clause) = &select.where_clause {
            self.expr_attribute(where_clause, scopes);
            self.where_infer(where_clause, scopes);
        }
        for column in &select.group_by {
            if column.table.is_none() && output_aliases.contains(&column.column) {
                continue;
            }
            self.column_attribute(column, scopes);
        }
        if let Some(having) = &select.having {
            self.expr_attribute(having, scopes);
        }
        for order in order_by {
            match &order.expr {
                // ORDER BY <output alias> and ORDER BY <position> reference
                // the select list, not table columns.
                ScalarExpr::Column(col)
                    if col.table.is_none() && output_aliases.contains(&col.column) => {}
                ScalarExpr::Literal(_) => {}
                expr => self.expr_attribute(expr, scopes),
            }
        }

        scopes.pop();
    }

    fn table_source_collect<'a>(
        &mut self,
        source: &'a TableSource,
        scope: &mut Scope,
        scopes: &mut Vec<Scope>,
        join_quals: &mut Vec<&'a WhereExpr>,
    ) {
        match source {
            TableSource::Table(table) => {
                let key = self.table_key(table.schema.as_ref(), &table.name);
                self.table_ensure(&key);
                let alias = table
                    .alias
                    .as_ref()
                    .map_or_else(|| table.name.clone(), |a| a.name.clone());
                scope.entries.push(ScopeEntry {
                    alias,
                    key: Some(key),
                });
            }
            TableSource::Subquery(subquery) => {
                scope.has_derived = true;
                if let Some(alias) = &subquery.alias {
                    scope.entries.push(ScopeEntry {
                        alias: alias.name.clone(),
                        key: None,
                    });
                }
                self.query_walk(&subquery.query, scopes);
            }
            TableSource::CteRef(cte_ref) => {
                scope.has_derived = true;
                let alias = cte_ref
                    .alias
                    .as_ref()
                    .map_or_else(|| cte_ref.cte_name.clone(), |a| a.name.clone());
                scope.entries.push(ScopeEntry { alias, key: None });
            }
            TableSource::Join(join) => {
                self.table_source_collect(&join.left, scope, scopes, join_quals);
                self.table_source_collect(&join.right, scope, scopes, join_quals);
                if let JoinQual::On(qual) = &join.qual {
                    join_quals.push(qual);
                }
            }
        }
    }

    fn table_key(&self, schema: Option<&EcoString>, name: &EcoString) -> TableKey {
        (
            schema.cloned().unwrap_or_else(|| EcoString::from("public")),
            name.clone(),
        )
    }

    fn table_ensure(&mut self, key: &TableKey) {
        if !self.tables.contains_key(key) {
            self.tables.insert(key.clone(), TableFacts::default());
            self.order.push(key.clone());
        }
    }

    fn column_add(&mut self, key: TableKey, column: EcoString) {
        self.table_ensure(&key);
        if let Some(facts) = self.tables.get_mut(&key)
            && facts.seen.insert(column.clone())
        {
            facts.columns.push(column);
        }
    }

    /// Attribute one column reference to a table, counting heuristic and
    /// skipped attributions.
    fn column_attribute(&mut self, column: &ColumnNode, scopes: &[Scope]) {
        match &column.table {
            Some(qualifier) => {
                if let Some(entry) = scope_entry_find(scopes, qualifier) {
                    if let Some(key) = entry.key.clone() {
                        self.column_add(key, column.column.clone());
                    }
                    return;
                }
                // Unknown qualifier: assume it names a table in public.
                let key = (EcoString::from("public"), qualifier.clone());
                self.column_add(key, column.column.clone());
            }
            None => {
                for scope in scopes.iter().rev() {
                    if scope.entries.is_empty() {
                        continue;
                    }
                    if scope.has_derived {
                        self.skipped_unqualified += 1;
                        return;
                    }
                    if let Some(entry) = scope.entries.first() {
                        if scope.entries.len() > 1 {
                            self.heuristic_attributions += 1;
                        }
                        if let Some(key) = entry.key.clone() {
                            self.column_add(key, column.column.clone());
                        }
                    }
                    return;
                }
            }
        }
    }

    /// Resolve a column to its table without counting or side effects — used
    /// by type inference, which runs after attribution.
    fn column_key_resolve(&self, column: &ColumnNode, scopes: &[Scope]) -> Option<TableKey> {
        match &column.table {
            Some(qualifier) => {
                if let Some(entry) = scope_entry_find(scopes, qualifier) {
                    return entry.key.clone();
                }
                let key = (EcoString::from("public"), qualifier.clone());
                self.tables.contains_key(&key).then_some(key)
            }
            None => {
                for scope in scopes.iter().rev() {
                    if scope.entries.is_empty() {
                        continue;
                    }
                    if scope.has_derived {
                        return None;
                    }
                    return scope.entries.first().and_then(|e| e.key.clone());
                }
                None
            }
        }
    }

    /// Attribute columns in an expression, descending into subqueries with
    /// their own scopes. Direct columns are separated from subquery-internal
    /// ones by identity: everything under a nested `QueryExpr` is attributed
    /// by the recursive walk, not here.
    fn expr_attribute<T: AstNode>(&mut self, expr: &T, scopes: &mut Vec<Scope>) {
        let subqueries: Vec<&QueryExpr> = expr.nodes::<QueryExpr>().collect();
        for subquery in subqueries
            .iter()
            .filter(|q| !subqueries.iter().any(|outer| query_contains(outer, q)))
        {
            self.query_walk(subquery, scopes);
        }
        let nested_columns: HashSet<*const ColumnNode> = subqueries
            .iter()
            .flat_map(|q| q.nodes::<ColumnNode>())
            .map(std::ptr::from_ref)
            .collect();
        let direct: Vec<&ColumnNode> = expr
            .nodes::<ColumnNode>()
            .filter(|c| !nested_columns.contains(&std::ptr::from_ref(*c)))
            .collect();
        for column in direct {
            self.column_attribute(column, scopes);
        }
    }

    /// Record literal-comparison type evidence from WHERE/JOIN predicates.
    fn where_infer(&mut self, expr: &WhereExpr, scopes: &[Scope]) {
        match expr {
            WhereExpr::Binary(binary) => {
                if binary.op.is_comparison() {
                    let sides = [
                        (
                            where_as_column(&binary.lexpr),
                            where_as_literal(&binary.rexpr),
                        ),
                        (
                            where_as_column(&binary.rexpr),
                            where_as_literal(&binary.lexpr),
                        ),
                    ];
                    for (column, literal) in sides {
                        if let (Some(column), Some(literal)) = (column, literal) {
                            self.evidence_note(column, literal, scopes);
                            return;
                        }
                    }
                }
                self.where_infer(&binary.lexpr, scopes);
                self.where_infer(&binary.rexpr, scopes);
            }
            WhereExpr::Multi(multi) => {
                // col IN (v1, v2, ...) / col BETWEEN a AND b shapes.
                if let Some(WhereExpr::Scalar(ScalarExpr::Column(column))) = multi.exprs.first() {
                    let literals: Vec<&LiteralValue> = multi
                        .exprs
                        .iter()
                        .skip(1)
                        .filter_map(where_as_literal)
                        .collect();
                    if !literals.is_empty() && literals.len() == multi.exprs.len() - 1 {
                        for literal in literals {
                            self.evidence_note(column, literal, scopes);
                        }
                        return;
                    }
                }
                for inner in &multi.exprs {
                    self.where_infer(inner, scopes);
                }
            }
            WhereExpr::Unary(unary) => self.where_infer(&unary.expr, scopes),
            WhereExpr::Scalar(_) | WhereExpr::Subquery { .. } => {}
        }
    }

    fn evidence_note(&mut self, column: &ColumnNode, literal: &LiteralValue, scopes: &[Scope]) {
        let observed = match literal {
            LiteralValue::Integer(_) => Some(TypeEvidence::Integer),
            LiteralValue::Float(_) => Some(TypeEvidence::Float),
            LiteralValue::String(_)
            | LiteralValue::StringWithCast(..)
            | LiteralValue::Boolean(_) => Some(TypeEvidence::Text),
            _ => None,
        };
        let Some(observed) = observed else { return };
        let Some(key) = self.column_key_resolve(column, scopes) else {
            return;
        };
        let Some(facts) = self.tables.get_mut(&key) else {
            return;
        };
        facts
            .evidence
            .entry(column.column.clone())
            .and_modify(|existing| {
                *existing = match (*existing, observed) {
                    (TypeEvidence::Integer, TypeEvidence::Integer) => TypeEvidence::Integer,
                    (TypeEvidence::Integer, TypeEvidence::Float)
                    | (TypeEvidence::Float, TypeEvidence::Integer)
                    | (TypeEvidence::Float, TypeEvidence::Float) => TypeEvidence::Float,
                    (TypeEvidence::Text, TypeEvidence::Text) => TypeEvidence::Text,
                    _ => TypeEvidence::Conflicted,
                };
            })
            .or_insert(observed);
    }

    fn finish(self) -> SynthCatalog {
        let mut tables = BiHashMap::new();
        let mut stats = SynthesisStats {
            tables: self.order.len(),
            heuristic_attributions: self.heuristic_attributions,
            skipped_unqualified: self.skipped_unqualified,
            ..SynthesisStats::default()
        };

        for (index, key) in self.order.iter().enumerate() {
            let Some(facts) = self.tables.get(key) else {
                continue;
            };
            let primary_key: EcoString = if facts.seen.contains("id") {
                "id".into()
            } else if let Some(first) = facts.columns.first() {
                first.clone()
            } else {
                "id".into()
            };
            let mut column_names = facts.columns.clone();
            if !facts.seen.contains(&primary_key) {
                column_names.insert(0, primary_key.clone());
            }

            let columns = ColumnStore::new(column_names.iter().enumerate().map(|(i, name)| {
                let is_primary_key = *name == primary_key;
                let (type_oid, data_type, type_name) = match facts.evidence.get(name) {
                    Some(TypeEvidence::Integer) => {
                        stats.inferred_columns += 1;
                        (20, Type::INT8, "int8")
                    }
                    Some(TypeEvidence::Float) => {
                        stats.inferred_columns += 1;
                        (701, Type::FLOAT8, "float8")
                    }
                    Some(TypeEvidence::Text) => (25, Type::TEXT, "text"),
                    Some(TypeEvidence::Conflicted) => {
                        stats.conflicted_columns += 1;
                        (25, Type::TEXT, "text")
                    }
                    None if is_primary_key => (23, Type::INT4, "int4"),
                    None => (25, Type::TEXT, "text"),
                };
                ColumnMetadata {
                    name: name.clone(),
                    position: i16::try_from(i + 1).unwrap_or(i16::MAX),
                    type_oid,
                    data_type,
                    type_name: type_name.into(),
                    cache_type_name: type_name.into(),
                    is_primary_key,
                }
            }));

            tables.insert_overwrite(TableMetadata {
                replica_identity_full: false,
                relation_oid: Oid::from_raw(100_000 + u32::try_from(index).unwrap_or(0)),
                name: key.1.clone(),
                schema: key.0.clone(),
                primary_key_columns: vec![primary_key],
                columns,
                indexes: Vec::new(),
            });
        }

        SynthCatalog { tables, stats }
    }
}

/// Innermost scope entry whose alias matches a column qualifier.
fn scope_entry_find<'a>(scopes: &'a [Scope], qualifier: &EcoString) -> Option<&'a ScopeEntry> {
    scopes
        .iter()
        .rev()
        .find_map(|scope| scope.entries.iter().find(|e| &e.alias == qualifier))
}

fn query_contains(outer: &QueryExpr, inner: &QueryExpr) -> bool {
    !std::ptr::eq(outer, inner) && outer.nodes::<QueryExpr>().any(|q| std::ptr::eq(q, inner))
}

fn where_as_column(expr: &WhereExpr) -> Option<&ColumnNode> {
    match expr {
        WhereExpr::Scalar(ScalarExpr::Column(column)) => Some(column),
        _ => None,
    }
}

fn where_as_literal(expr: &WhereExpr) -> Option<&LiteralValue> {
    match expr {
        WhereExpr::Scalar(ScalarExpr::Literal(literal)) => Some(literal),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::classify::query_parse;

    fn synthesize(sqls: &[&str]) -> SynthCatalog {
        let queries: Vec<QueryExpr> = sqls.iter().map(|s| query_parse(s)).collect();
        catalog_synthesize(queries.iter())
    }

    fn table<'a>(catalog: &'a SynthCatalog, name: &str) -> &'a TableMetadata {
        catalog
            .tables
            .iter()
            .find(|t| t.name == name)
            .expect("table synthesized")
    }

    #[test]
    fn test_single_table_columns_and_pk() {
        let catalog = synthesize(&["SELECT name, email FROM users WHERE id = 5"]);
        let users = table(&catalog, "users");
        assert_eq!(users.primary_key_columns, vec![EcoString::from("id")]);
        assert_eq!(users.schema, "public");
        let names: Vec<&str> = users.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"name") && names.contains(&"email") && names.contains(&"id"));
    }

    #[test]
    fn test_qualified_attribution_under_join() {
        let catalog =
            synthesize(&["SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id"]);
        let users = table(&catalog, "users");
        let orders = table(&catalog, "orders");
        assert!(users.columns.iter().any(|c| c.name == "name"));
        assert!(orders.columns.iter().any(|c| c.name == "total"));
        assert!(orders.columns.iter().any(|c| c.name == "user_id"));
        assert_eq!(catalog.stats.heuristic_attributions, 0);
    }

    #[test]
    fn test_unqualified_under_join_goes_to_first_table() {
        let catalog = synthesize(&["SELECT name FROM users u JOIN orders o ON u.id = o.user_id"]);
        let users = table(&catalog, "users");
        assert!(users.columns.iter().any(|c| c.name == "name"));
        assert_eq!(catalog.stats.heuristic_attributions, 1);
    }

    #[test]
    fn test_type_inference_from_literals() {
        let catalog = synthesize(&[
            "SELECT * FROM orders WHERE total > 100 AND status = 'open' AND weight < 2.5",
        ]);
        let orders = table(&catalog, "orders");
        let column = |name: &str| {
            orders
                .columns
                .iter()
                .find(|c| c.name == name)
                .expect("column attributed")
        };
        assert_eq!(column("total").type_name, "int8");
        assert_eq!(column("weight").type_name, "float8");
        assert_eq!(column("status").type_name, "text");
        assert_eq!(catalog.stats.inferred_columns, 2);
        // Uniform text evidence is not a conflict.
        assert_eq!(catalog.stats.conflicted_columns, 0);
    }

    #[test]
    fn test_mixed_numeric_and_text_evidence_is_conflicted() {
        let catalog = synthesize(&[
            "SELECT * FROM orders WHERE code = 5",
            "SELECT * FROM orders WHERE code = 'x'",
        ]);
        let orders = table(&catalog, "orders");
        let code = orders
            .columns
            .iter()
            .find(|c| c.name == "code")
            .expect("column attributed");
        assert_eq!(code.type_name, "text");
        assert_eq!(catalog.stats.conflicted_columns, 1);
    }

    #[test]
    fn test_star_only_table_gets_synthetic_pk() {
        let catalog = synthesize(&["SELECT * FROM logs"]);
        let logs = table(&catalog, "logs");
        assert_eq!(logs.primary_key_columns, vec![EcoString::from("id")]);
        assert!(
            logs.columns
                .iter()
                .any(|c| c.name == "id" && c.is_primary_key)
        );
    }

    #[test]
    fn test_subquery_alias_columns_not_attributed() {
        let catalog =
            synthesize(&["SELECT s.total FROM (SELECT sum(amount) AS total FROM sales) s"]);
        let sales = table(&catalog, "sales");
        assert!(sales.columns.iter().any(|c| c.name == "amount"));
        // `s.total` is a derived-source column, not a table column.
        assert!(!sales.columns.iter().any(|c| c.name == "total"));
    }

    #[test]
    fn test_subquery_in_where_uses_own_scope() {
        let catalog =
            synthesize(&["SELECT name FROM users WHERE id IN (SELECT user_id FROM orders)"]);
        let users = table(&catalog, "users");
        let orders = table(&catalog, "orders");
        assert!(users.columns.iter().any(|c| c.name == "name"));
        assert!(orders.columns.iter().any(|c| c.name == "user_id"));
        assert!(!users.columns.iter().any(|c| c.name == "user_id"));
    }
}
