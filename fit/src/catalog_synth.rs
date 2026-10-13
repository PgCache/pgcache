//! Heuristic catalog synthesized from the query corpus itself — the v0
//! schema-less mode. Every referenced relation is assumed to be a table with
//! a primary key; unqualified names live in `public`; column types default to
//! text unless literal comparisons give numeric evidence (constraint-range
//! implication uses column ordering, and text ordering disagrees with numeric,
//! so all-text columns would silently mis-count subsumption).

use std::collections::{HashMap, HashSet};

use ecow::EcoString;
use iddqd::BiHashMap;
use pgcache_lib::catalog::{Catalog, ColumnMetadata, ColumnPosition, ColumnStore, TableMetadata};
use pgcache_lib::oid::{Oid, TypeOid};
use pgcache_lib::query::ast::LiteralValue;
use postgres_types::Type;

mod tree;

pub use tree::catalog_synthesize;

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
    pub tables: Catalog,
    pub stats: SynthesisStats,
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
    fn column_attribute(
        &mut self,
        table: Option<&EcoString>,
        column: &EcoString,
        scopes: &[Scope],
    ) {
        match table {
            Some(qualifier) => {
                if let Some(entry) = scope_entry_find(scopes, qualifier) {
                    if let Some(key) = entry.key.clone() {
                        self.column_add(key, column.clone());
                    }
                    return;
                }
                // Unknown qualifier: assume it names a table in public.
                let key = (EcoString::from("public"), qualifier.clone());
                self.column_add(key, column.clone());
            }
            None => match scope_unqualified_resolve(scopes) {
                UnqualifiedResolution::Table { entry, ambiguous } => {
                    if ambiguous {
                        self.heuristic_attributions += 1;
                    }
                    if let Some(key) = entry.key.clone() {
                        self.column_add(key, column.clone());
                    }
                }
                UnqualifiedResolution::Derived => self.skipped_unqualified += 1,
                UnqualifiedResolution::NoScope => {}
            },
        }
    }

    /// Resolve a column to its table without counting or side effects — used
    /// by type inference, which runs after attribution.
    fn column_key_resolve(&self, table: Option<&EcoString>, scopes: &[Scope]) -> Option<TableKey> {
        match table {
            Some(qualifier) => {
                if let Some(entry) = scope_entry_find(scopes, qualifier) {
                    return entry.key.clone();
                }
                let key = (EcoString::from("public"), qualifier.clone());
                self.tables.contains_key(&key).then_some(key)
            }
            None => match scope_unqualified_resolve(scopes) {
                UnqualifiedResolution::Table { entry, .. } => entry.key.clone(),
                UnqualifiedResolution::Derived | UnqualifiedResolution::NoScope => None,
            },
        }
    }

    fn evidence_note(
        &mut self,
        table: Option<&EcoString>,
        column: &EcoString,
        literal: &LiteralValue,
        scopes: &[Scope],
    ) {
        let observed = match literal {
            LiteralValue::Integer(_) => Some(TypeEvidence::Integer),
            LiteralValue::Float(_) => Some(TypeEvidence::Float),
            LiteralValue::String(_)
            | LiteralValue::StringWithCast(..)
            | LiteralValue::Boolean(_) => Some(TypeEvidence::Text),
            _ => None,
        };
        let Some(observed) = observed else { return };
        let Some(key) = self.column_key_resolve(table, scopes) else {
            return;
        };
        let Some(facts) = self.tables.get_mut(&key) else {
            return;
        };
        facts
            .evidence
            .entry(column.clone())
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
        let mut tables: Catalog = BiHashMap::new();
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
                let (data_type, type_name) = match facts.evidence.get(name) {
                    Some(TypeEvidence::Integer) => {
                        stats.inferred_columns += 1;
                        (Type::INT8, "int8")
                    }
                    Some(TypeEvidence::Float) => {
                        stats.inferred_columns += 1;
                        (Type::FLOAT8, "float8")
                    }
                    Some(TypeEvidence::Text) => (Type::TEXT, "text"),
                    Some(TypeEvidence::Conflicted) => {
                        stats.conflicted_columns += 1;
                        (Type::TEXT, "text")
                    }
                    None if is_primary_key => (Type::INT4, "int4"),
                    None => (Type::TEXT, "text"),
                };
                ColumnMetadata {
                    name: name.clone(),
                    position: ColumnPosition::from_index(i).unwrap_or(ColumnPosition::FIRST),
                    type_oid: TypeOid::from_type(&data_type),
                    data_type,
                    type_name: type_name.into(),
                    cache_type_name: type_name.into(),
                    is_primary_key,
                }
            }));

            tables.insert_overwrite(
                TableMetadata {
                    replica_identity_full: false,
                    relation_oid: Oid::from_raw(100_000 + u32::try_from(index).unwrap_or(0)),
                    name: key.1.clone(),
                    schema: key.0.clone(),
                    primary_key_columns: vec![primary_key],
                    columns,
                    indexes: Vec::new(),
                }
                .into(),
            );
        }

        SynthCatalog { tables, stats }
    }
}

/// Resolution of an unqualified column against the innermost scope that has
/// any entries — the single source for both attribution and type-evidence
/// lookup, so the two can never disagree.
enum UnqualifiedResolution<'a> {
    /// First FROM-order table of the innermost scope; `ambiguous` when more
    /// than one table was in that scope.
    Table {
        entry: &'a ScopeEntry,
        ambiguous: bool,
    },
    /// A derived source (subquery/CTE) is in scope: table membership cannot
    /// be assumed.
    Derived,
    NoScope,
}

fn scope_unqualified_resolve(scopes: &[Scope]) -> UnqualifiedResolution<'_> {
    for scope in scopes.iter().rev() {
        let Some(entry) = scope.entries.first() else {
            continue;
        };
        if scope.has_derived {
            return UnqualifiedResolution::Derived;
        }
        return UnqualifiedResolution::Table {
            entry,
            ambiguous: scope.entries.len() > 1,
        };
    }
    UnqualifiedResolution::NoScope
}

/// Innermost scope entry whose alias matches a column qualifier.
fn scope_entry_find<'a>(scopes: &'a [Scope], qualifier: &EcoString) -> Option<&'a ScopeEntry> {
    scopes
        .iter()
        .rev()
        .find_map(|scope| scope.entries.iter().find(|e| &e.alias == qualifier))
}

#[cfg(test)]
mod tests {
    use pgcache_lib::query::ast::rope::QueryTree;

    use super::*;
    use crate::classify::query_parse;

    fn synthesize(sqls: &[&str]) -> SynthCatalog {
        let trees: Vec<QueryTree> = sqls.iter().map(|s| query_parse(s)).collect();
        catalog_synthesize(trees.iter())
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
