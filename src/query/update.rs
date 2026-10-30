#[cfg(feature = "proxy")]
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

#[cfg(feature = "proxy")]
use ecow::EcoString;

use crate::cache::{SubqueryKind, UpdateQuerySource};
use crate::catalog::TableMetadata;
use crate::oid::Oid;
use crate::query::ast::rope::{
    Body, JoinQual, Query, Scalar, ScalarKind, Select, SelectItem, Source, Table, Where, WhereKind,
};
use crate::query::ast::{SubLinkType, UnaryOp};
#[cfg(feature = "proxy")]
use crate::query::constraints::TableRanges;
use crate::query::predicates::{OuterJoinRole, RelationPredicates};
use crate::query::resolve::{QueryBranch, ResolvedQuery, TableSlot};

/// One base-table reference of a read with what its branch's predicate
/// model says about it: `None` when the model can't project the reference's
/// filters onto the table's columns.
pub struct BaseReference<'a> {
    pub table: &'a TableMetadata,
    pub predicates: Option<Arc<RelationPredicates>>,
}

/// The base-table references of a read across its SELECT branches: the
/// root, every subquery, and a CTE body once per reference to it.
pub struct BaseReferences<'a> {
    pub references: Vec<BaseReference<'a>>,
    /// Base tables no branch lists as a reference: read some way the model
    /// doesn't cover.
    pub uncovered: Vec<&'a TableMetadata>,
}

pub fn query_base_references(bound: &ResolvedQuery) -> BaseReferences<'_> {
    let bindings = bound.bindings();
    let mut references = Vec::new();
    let mut seen = HashSet::new();
    for branch in bound.predicates().branches() {
        let predicates = &branch.predicates;
        for reference in predicates.table_references() {
            let Some(TableSlot::Base { table, .. }) = bindings.slot(reference.slot) else {
                continue;
            };
            seen.insert(reference.slot.index());
            references.push(BaseReference {
                table,
                predicates: predicates.relation_predicates(reference.slot).cloned(),
            });
        }
    }
    let uncovered = bindings
        .slots()
        .iter()
        .enumerate()
        .filter_map(|(index, slot)| match slot {
            TableSlot::Base { table, .. } if !seen.contains(&index) => Some(table.as_ref()),
            TableSlot::Base { .. } | TableSlot::Derived { .. } => None,
        })
        .collect();
    BaseReferences {
        references,
        uncovered,
    }
}

/// One table reference's ranges, shared with its update query; the gate
/// reads the bare ones.
#[cfg(feature = "proxy")]
pub(crate) type ReferenceRanges = TableRanges;

/// One base relation of a read as the read-after-write gate sees it.
#[cfg(feature = "proxy")]
#[derive(Debug)]
pub(crate) struct ReadRelation {
    pub(crate) schema: EcoString,
    pub(crate) name: EcoString,
    /// The columns the read references on it: an UPDATE assigning none of
    /// them cannot change the read. `None` when unknown, which proves
    /// nothing.
    pub(crate) columns: Option<HashSet<EcoString>>,
    /// Each reference's per-column ranges, every one with at least one
    /// column; `None` when the read proves nothing row-wise about the
    /// relation.
    pub(crate) ranges: Option<Vec<ReferenceRanges>>,
}

/// The base relations a read touches, by OID, for the read-after-write
/// gate (PGC-542): the exact relations the writer resolved the read's names
/// to, so same-named tables in different schemas stay apart. A reference's
/// filters are a necessary condition on the rows that reach its branch's
/// result, so a pending write disjoint from every reference of a relation
/// can't change the read (PGC-512); a reference without filters (an
/// unfiltered join side), one whose filters the model can't project to
/// column names, or one read some way the model doesn't cover proves
/// nothing and drops the relation's ranges. The NULL-filled side of an
/// outer join takes no filter from the WHERE (an insert there can remove
/// preserved rows), only what the preserved side's filters carry over the
/// ON equality.
#[cfg(feature = "proxy")]
#[derive(Debug, Default)]
pub(crate) struct ReadRelations {
    relations: HashMap<Oid, ReadRelation>,
}

#[cfg(feature = "proxy")]
impl ReadRelations {
    /// `columns` are the read's referenced columns per relation, as
    /// `privilege_needs` collects them.
    pub(crate) fn build(bound: &ResolvedQuery, columns: &HashMap<Oid, HashSet<EcoString>>) -> Self {
        let mut relations: HashMap<Oid, ReadRelation> = HashMap::new();
        for slot in bound.bindings().slots() {
            if let TableSlot::Base { table, .. } = slot {
                relations
                    .entry(table.relation_oid)
                    .or_insert_with(|| ReadRelation {
                        schema: table.schema.clone(),
                        name: table.name.clone(),
                        columns: columns.get(&table.relation_oid).cloned(),
                        ranges: Some(Vec::new()),
                    });
            }
        }
        let base = query_base_references(bound);
        for reference in base.references {
            let Some(relation) = relations.get_mut(&reference.table.relation_oid) else {
                continue;
            };
            match (reference.predicates, &mut relation.ranges) {
                (Some(predicates), Some(references)) => {
                    references.push(predicates.ranges.clone());
                }
                (Some(_), None) => {}
                (None, ranges) => *ranges = None,
            }
        }
        for table in base.uncovered {
            if let Some(relation) = relations.get_mut(&table.relation_oid) {
                relation.ranges = None;
            }
        }
        Self::from_relations(relations)
    }

    /// `relations`, dropping the ranges of any whose references don't all
    /// have one.
    pub(crate) fn from_relations(relations: impl IntoIterator<Item = (Oid, ReadRelation)>) -> Self {
        let relations = relations
            .into_iter()
            .map(|(oid, mut relation)| {
                let unusable = relation.ranges.as_ref().is_some_and(|references| {
                    references.is_empty() || references.iter().any(TableRanges::is_empty)
                });
                if unusable {
                    relation.ranges = None;
                }
                (oid, relation)
            })
            .collect();
        Self { relations }
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (Oid, &ReadRelation)> {
        self.relations
            .iter()
            .map(|(oid, relation)| (*oid, relation))
    }
}

/// One table's update query: the branch it sits in, as the cache
/// evaluates a changed row of the table against it.
pub struct TableUpdateQuery<'a> {
    pub table: Table<'a>,
    pub relation_oid: Oid,
    pub branch: QueryBranch<'a>,
    pub source: UpdateQuerySource,
    /// What the table's reference answers to in its branch.
    pub predicates: Arc<RelationPredicates>,
}

/// Every (table, branch) pair of the query: each SELECT, the root and
/// every subquery, once per table directly in its FROM.
pub fn query_table_update_queries(bound: &ResolvedQuery) -> Vec<TableUpdateQuery<'_>> {
    let mut result = Vec::new();
    let bindings = bound.bindings();
    for model_branch in bound.predicates().branches() {
        let Some(select) = bound.select_node(model_branch.select) else {
            debug_assert!(false, "a branch of the model is a SELECT of the tree");
            continue;
        };
        let source = model_branch.source;
        let predicates = &model_branch.predicates;
        let branch = QueryBranch { bound, select };
        for table in select.direct_tables() {
            let slot = bindings.source(table.id());
            let (Some(slot), Some(table_slot)) = (slot, slot.and_then(|slot| bindings.slot(slot)))
            else {
                debug_assert!(false, "a direct table of a resolved query has a slot");
                continue;
            };
            let relation_oid = table_slot.table().relation_oid;
            let role = predicates
                .table_reference_predicates(slot)
                .map(|reference| reference.outer_join_role);
            let relation = predicates
                .relation_predicates(slot)
                .cloned()
                .unwrap_or_else(|| {
                    debug_assert!(false, "a direct table is a reference of its branch");
                    Arc::new(RelationPredicates::unknown(table_slot.table()))
                });
            let effective_source = match (source, role) {
                (UpdateQuerySource::FromClause, Some(OuterJoinRole::Terminal)) => {
                    UpdateQuerySource::OuterJoinTerminal
                }
                // A reference the model can't place is treated as optional.
                (UpdateQuerySource::FromClause, Some(OuterJoinRole::Optional) | None) => {
                    UpdateQuerySource::OuterJoinOptional
                }
                (UpdateQuerySource::FromClause, Some(OuterJoinRole::Kept))
                | (
                    UpdateQuerySource::Subquery(_)
                    | UpdateQuerySource::OuterJoinTerminal
                    | UpdateQuerySource::OuterJoinOptional,
                    _,
                ) => source,
            };
            result.push(TableUpdateQuery {
                table,
                relation_oid,
                branch,
                source: effective_source,
                predicates: relation,
            });
        }
    }
    result
}

/// Every SELECT of `query` with where it sits: the root body
/// (`FromClause`), or a subquery with its kind. CTE definitions are not
/// collected directly: their branches are collected through each reference,
/// inheriting the reference site's context.
pub fn select_branches_with_source(query: Query<'_>) -> Vec<(Select<'_>, UpdateQuerySource)> {
    let mut branches = Vec::new();
    query_branches_collect(query, &mut branches, UpdateQuerySource::FromClause, false);
    branches
}

/// `negated` tracks NOT-wrapping, which flips Inclusion and Exclusion.
fn query_branches_collect<'t>(
    query: Query<'t>,
    branches: &mut Vec<(Select<'t>, UpdateQuerySource)>,
    outer_source: UpdateQuerySource,
    negated: bool,
) {
    match query.body() {
        Body::Select(select) => {
            branches.push((select, outer_source));
            for source in select.from() {
                source_branches_collect(source, branches, negated);
            }
            if let Some(where_clause) = select.where_clause() {
                where_branches_collect(where_clause, branches, negated);
            }
            if let Some(having) = select.having() {
                where_branches_collect(having, branches, negated);
            }
            if let Some(items) = select.items() {
                for item in items {
                    if let SelectItem::Expr(expr) = item {
                        scalar_branches_collect(expr.expr(), branches);
                    }
                }
            }
        }
        Body::SetOp(set_op) => {
            query_branches_collect(set_op.left(), branches, outer_source, negated);
            query_branches_collect(set_op.right(), branches, outer_source, negated);
        }
        Body::Values(_) => {}
    }
}

fn source_branches_collect<'t>(
    source: Source<'t>,
    branches: &mut Vec<(Select<'t>, UpdateQuerySource)>,
    negated: bool,
) {
    match source {
        Source::Table(_) => {}
        Source::Subquery(subquery) => {
            let kind = match (subquery.kind(), negated) {
                (SubqueryKind::Scalar, _) => SubqueryKind::Scalar,
                (SubqueryKind::Inclusion, true) => SubqueryKind::Exclusion,
                (SubqueryKind::Exclusion, true) => SubqueryKind::Inclusion,
                (kind, false) => kind,
            };
            query_branches_collect(
                subquery.query(),
                branches,
                UpdateQuerySource::Subquery(kind),
                negated,
            );
        }
        Source::CteRef(cte_ref) => {
            let kind = if negated {
                SubqueryKind::Exclusion
            } else {
                SubqueryKind::Inclusion
            };
            query_branches_collect(
                cte_ref.query(),
                branches,
                UpdateQuerySource::Subquery(kind),
                negated,
            );
        }
        Source::Join(join) => {
            source_branches_collect(join.left(), branches, negated);
            source_branches_collect(join.right(), branches, negated);
            if let JoinQual::On(condition) = join.qual() {
                where_branches_collect(condition, branches, negated);
            }
        }
    }
}

fn where_branches_collect<'t>(
    expr: Where<'t>,
    branches: &mut Vec<(Select<'t>, UpdateQuerySource)>,
    negated: bool,
) {
    match expr.kind() {
        WhereKind::Scalar(scalar) => scalar_branches_collect(scalar, branches),
        WhereKind::Binary(binary) => {
            where_branches_collect(binary.lexpr(), branches, negated);
            where_branches_collect(binary.rexpr(), branches, negated);
        }
        WhereKind::Unary(unary) => {
            let child_negated = if unary.op() == UnaryOp::Not {
                !negated
            } else {
                negated
            };
            where_branches_collect(unary.expr(), branches, child_negated);
        }
        WhereKind::Multi(multi) => {
            for operand in multi.exprs() {
                where_branches_collect(operand, branches, negated);
            }
        }
        WhereKind::Subquery(subquery) => {
            let kind = match subquery.sublink_type() {
                SubLinkType::Expr => SubqueryKind::Scalar,
                SubLinkType::Any | SubLinkType::Exists => {
                    if negated {
                        SubqueryKind::Exclusion
                    } else {
                        SubqueryKind::Inclusion
                    }
                }
                SubLinkType::All => {
                    if negated {
                        SubqueryKind::Inclusion
                    } else {
                        SubqueryKind::Exclusion
                    }
                }
            };
            query_branches_collect(
                subquery.query(),
                branches,
                UpdateQuerySource::Subquery(kind),
                negated,
            );
            if let Some(test) = subquery.test_expr() {
                scalar_branches_collect(test, branches);
            }
        }
    }
}

/// Like [`where_branches_collect`] but every collected subquery is
/// `SubqueryKind::Scalar` whatever its sublink type: its truth value feeds
/// an enclosing scalar (aggregate FILTER, CASE WHEN condition), so
/// shrink-skip rules are unsound (PGC-107).
fn where_branches_collect_as_scalar<'t>(
    expr: Where<'t>,
    branches: &mut Vec<(Select<'t>, UpdateQuerySource)>,
) {
    let start = branches.len();
    where_branches_collect(expr, branches, false);
    for (_, source) in branches.iter_mut().skip(start) {
        if let UpdateQuerySource::Subquery(kind) = source {
            *kind = SubqueryKind::Scalar;
        }
    }
}

fn scalar_branches_collect<'t>(
    expr: Scalar<'t>,
    branches: &mut Vec<(Select<'t>, UpdateQuerySource)>,
) {
    match expr.kind() {
        ScalarKind::Column(_) | ScalarKind::Literal(_) => {}
        ScalarKind::Function(function) => {
            for arg in function.args() {
                scalar_branches_collect(arg, branches);
            }
            for clause in function.agg_order() {
                scalar_branches_collect(clause.expr(), branches);
            }
            if let Some(filter) = function.filter() {
                where_branches_collect_as_scalar(filter, branches);
            }
            if let Some(over) = function.over() {
                for partition in over.partition_by() {
                    scalar_branches_collect(partition, branches);
                }
                for clause in over.order_by() {
                    scalar_branches_collect(clause.expr(), branches);
                }
            }
        }
        ScalarKind::Case(case) => {
            if let Some(arg) = case.arg() {
                scalar_branches_collect(arg, branches);
            }
            for when in case.whens() {
                where_branches_collect_as_scalar(when.condition(), branches);
                scalar_branches_collect(when.result(), branches);
            }
            if let Some(default) = case.default() {
                scalar_branches_collect(default, branches);
            }
        }
        ScalarKind::Arithmetic(arithmetic) => {
            scalar_branches_collect(arithmetic.left(), branches);
            scalar_branches_collect(arithmetic.right(), branches);
        }
        ScalarKind::Subquery(subquery) => {
            let source = UpdateQuerySource::Subquery(SubqueryKind::Scalar);
            query_branches_collect(subquery.query(), branches, source, false);
        }
        ScalarKind::Array(array) => {
            for element in array.elements() {
                scalar_branches_collect(element, branches);
            }
        }
        ScalarKind::TypeCast(cast) => scalar_branches_collect(cast.expr(), branches),
    }
}

#[cfg(test)]
mod tests {

    #![allow(clippy::wildcard_enum_match_arm)]

    use std::collections::HashMap;

    use iddqd::BiHashMap;
    use postgres_types::Type;

    use super::*;
    use crate::cache::SubqueryKind;
    use crate::cache::UpdateQuerySource::{FromClause, OuterJoinOptional, OuterJoinTerminal};
    use crate::cache::query::CacheableQuery;
    use crate::catalog::{Catalog, ColumnMetadata, ColumnPosition, TableMetadata};
    use crate::oid::TypeOid;
    #[cfg(feature = "proxy")]
    use crate::query::ast::LiteralValue;
    use crate::query::ast::query_tree_parse;
    #[cfg(feature = "proxy")]
    use crate::query::constraints::ColumnRange;

    /// One reference's sorted (column, range) pairs.
    #[cfg(feature = "proxy")]
    type ReferenceLines = Vec<(String, String)>;
    /// Sorted (table, references) lines of a read.
    #[cfg(feature = "proxy")]
    type ReadLines = Vec<(String, Vec<ReferenceLines>)>;

    /// The relations of a read that have ranges, as sorted (table,
    /// references) lines, each reference its sorted (column, range) pairs.
    #[cfg(feature = "proxy")]
    fn read_ranges_of(sql: &str) -> ReadLines {
        let int_column = |name: &str, position: i16| ColumnMetadata {
            name: name.into(),
            position: ColumnPosition::from_raw(position),
            type_oid: TypeOid::from_raw(23),
            data_type: Type::INT4,
            type_name: "int4".into(),
            cache_type_name: "int4".into(),
            is_primary_key: position == 1,
        };
        let mut tables = BiHashMap::new();
        for (oid, name) in [(1001, "t"), (1002, "u")] {
            tables.insert_overwrite(
                TableMetadata::for_test(name, oid, [int_column("id", 1), int_column("x", 2)])
                    .into(),
            );
        }
        let bound = ResolvedQuery::parse_resolve(sql, &tables, &["public"]).expect("resolve");
        let mut tables: ReadLines = ReadRelations::build(&bound, &HashMap::new())
            .relations
            .values()
            .filter_map(|relation| Some((&relation.name, relation.ranges.as_ref()?)))
            .map(|(table, references)| {
                let mut references: Vec<ReferenceLines> = references
                    .iter()
                    .map(|ranges| {
                        let mut ranges: ReferenceLines = ranges
                            .bare_ranges()
                            .map(|(column, range)| (column.to_string(), format!("{range:?}")))
                            .collect();
                        ranges.sort();
                        ranges
                    })
                    .collect();
                references.sort();
                (table.to_string(), references)
            })
            .collect();
        tables.sort();
        tables
    }

    /// Every base-table reference of a read contributes its own filters'
    /// ranges under its table, whatever else the read does: an unrecognized
    /// conjunct, a derived table, a join, a subquery, a set operation or a
    /// CTE (its body once per reference to it). A table with an unfiltered
    /// reference, a self-join's other side, is left out. The NULL-filled
    /// side of an anti-join takes nothing from the WHERE (its `IS NULL`
    /// holds on the padding) but does carry the preserved side's filter over
    /// the ON equality: only a row matching a preserved row in the result
    /// can change it (PGC-512).
    #[cfg(feature = "proxy")]
    #[test]
    fn test_read_ranges() {
        let equal = |column: &str, value: i64| {
            vec![(column.to_owned(), format!("Equal(Integer({value}))"))]
        };
        let expected = |tables: Vec<(&str, Vec<ReferenceLines>)>| {
            let mut tables: ReadLines = tables
                .into_iter()
                .map(|(table, mut references)| {
                    references.sort();
                    (table.to_owned(), references)
                })
                .collect();
            tables.sort();
            tables
        };
        let cases = [
            (
                "SELECT * FROM t WHERE id = 5",
                expected(vec![("t", vec![equal("id", 5)])]),
            ),
            (
                "SELECT * FROM t WHERE id = 5 AND x + 1 = 2",
                expected(vec![("t", vec![equal("id", 5)])]),
            ),
            (
                "SELECT * FROM (SELECT * FROM t WHERE id = 1) d",
                expected(vec![("t", vec![equal("id", 1)])]),
            ),
            (
                "SELECT * FROM t JOIN u ON u.id = t.id WHERE t.id = 5",
                expected(vec![
                    ("t", vec![equal("id", 5)]),
                    ("u", vec![equal("id", 5)]),
                ]),
            ),
            (
                "SELECT * FROM t a JOIN t b ON b.id = a.x WHERE a.id = 5",
                expected(vec![]),
            ),
            (
                "SELECT * FROM t WHERE id = 5 AND x IN (SELECT id FROM u WHERE id = 7)",
                expected(vec![
                    ("t", vec![equal("id", 5)]),
                    ("u", vec![equal("id", 7)]),
                ]),
            ),
            (
                "SELECT * FROM t LEFT JOIN u ON u.id = t.id WHERE u.id IS NULL AND t.id = 5",
                expected(vec![
                    ("t", vec![equal("id", 5)]),
                    ("u", vec![equal("id", 5)]),
                ]),
            ),
            (
                "SELECT * FROM t LEFT JOIN u ON u.id = t.x WHERE u.id IS NULL",
                expected(vec![]),
            ),
            (
                "SELECT id FROM t WHERE id = 1 UNION SELECT id FROM t WHERE id = 2",
                expected(vec![("t", vec![equal("id", 1), equal("id", 2)])]),
            ),
            (
                "SELECT id FROM t WHERE id = 1 EXCEPT SELECT id FROM u WHERE x = 2",
                expected(vec![
                    ("t", vec![equal("id", 1)]),
                    ("u", vec![equal("x", 2)]),
                ]),
            ),
            (
                "WITH c AS (SELECT id FROM t WHERE id = 1) \
                 SELECT id FROM c UNION ALL SELECT id FROM c",
                expected(vec![("t", vec![equal("id", 1), equal("id", 1)])]),
            ),
        ];
        for (sql, expected) in cases {
            assert_eq!(read_ranges_of(sql), expected, "{sql}");
        }
    }

    /// Relations are keyed by OID, so two same-named tables in different
    /// schemas keep their own ranges and columns: the filtered one keeps its
    /// ranges when the other side of the join is unfiltered (PGC-544).
    #[cfg(feature = "proxy")]
    #[test]
    fn test_read_relations_keyed_by_relation() {
        let int_column = |name: &str, position: i16| ColumnMetadata {
            name: name.into(),
            position: ColumnPosition::from_raw(position),
            type_oid: TypeOid::from_raw(23),
            data_type: Type::INT4,
            type_name: "int4".into(),
            cache_type_name: "int4".into(),
            is_primary_key: position == 1,
        };
        let mut tables = BiHashMap::new();
        for (oid, schema) in [(2001, "s1"), (2002, "s2")] {
            let mut table =
                TableMetadata::for_test("orders", oid, [int_column("id", 1), int_column("x", 2)]);
            table.schema = schema.into();
            tables.insert_overwrite(table.into());
        }
        let bound = ResolvedQuery::parse_resolve(
            "SELECT a.id FROM s1.orders a JOIN s2.orders b ON b.id = a.x WHERE a.id = 5",
            &tables,
            &["public"],
        )
        .expect("resolve");
        let needs = bound.privilege_needs();
        let relations = ReadRelations::build(&bound, &needs.relations);
        let s1 = relations
            .relations
            .get(&Oid::from_raw(2001))
            .expect("s1.orders is a relation of the read");
        assert_eq!((s1.schema.as_str(), s1.name.as_str()), ("s1", "orders"));
        let ranges = s1
            .ranges
            .as_ref()
            .expect("the filtered side keeps its ranges");
        assert_eq!(ranges.len(), 1);
        assert!(matches!(
            ranges[0].bare_range("id"),
            Some(ColumnRange::Equal(LiteralValue::Integer(5)))
        ));
        let s1_columns = s1.columns.as_ref().expect("s1.orders columns known");
        assert!(s1_columns.contains("id") && s1_columns.contains("x"));
        let s2 = relations
            .relations
            .get(&Oid::from_raw(2002))
            .expect("s2.orders is a relation of the read");
        assert_eq!(s2.schema.as_str(), "s2");
        assert!(s2.ranges.is_none(), "the unfiltered side proves nothing");
        let s2_columns = s2.columns.as_ref().expect("s2.orders columns known");
        assert!(s2_columns.contains("id") && !s2_columns.contains("x"));
    }

    /// Helper to parse SQL and return a CacheableQuery
    fn parse_cacheable(sql: &str) -> CacheableQuery {
        let tree = query_tree_parse(sql).expect("parse");
        CacheableQuery::try_new(tree, &HashMap::new()).expect("query to be cacheable")
    }

    /// Create test table metadata with given column names.
    /// First column is the primary key (INT4), rest are TEXT.
    fn test_table(name: &str, relation_oid: Oid, column_names: &[&str]) -> TableMetadata {
        let columns = column_names.iter().enumerate().map(|(i, col_name)| {
            let is_pk = i == 0;
            ColumnMetadata {
                name: (*col_name).into(),
                position: ColumnPosition::from_index(i).expect("column position in range"),
                type_oid: TypeOid::from_raw(if is_pk { 23 } else { 25 }),
                data_type: if is_pk { Type::INT4 } else { Type::TEXT },
                type_name: if is_pk { "int4" } else { "text" }.into(),
                cache_type_name: if is_pk { "int4" } else { "text" }.into(),
                is_primary_key: is_pk,
            }
        });
        TableMetadata::for_test(name, relation_oid.get(), columns)
    }

    /// Every table the tests reference: name, oid, and columns, the first
    /// of which is the primary key.
    const TEST_TABLES: &[(&str, u32, &[&str])] = &[
        ("a", 1, &["id", "name", "val", "tenant_id", "status"]),
        (
            "b",
            2,
            &[
                "id",
                "name",
                "a_id",
                "x",
                "val",
                "b_id",
                "user_id",
                "tenant_id",
                "status",
            ],
        ),
        ("c", 3, &["id", "x", "b_id", "val", "tenant_id"]),
        ("d", 4, &["id"]),
        (
            "users",
            10,
            &[
                "id",
                "name",
                "email",
                "user_id",
                "status",
                "active",
                "uuid",
                "age",
                "tenant_id",
            ],
        ),
        ("location", 11, &["user_id"]),
        ("orders", 12, &["id", "user_id", "total", "product_id"]),
        ("admins", 13, &["id", "tenant_id"]),
        ("active_users", 14, &["id", "user_id"]),
        ("banned_users", 15, &["id", "user_id"]),
        ("products", 16, &["id", "store_id", "name", "category_id"]),
        ("stores", 17, &["id", "region_id"]),
        ("regions", 18, &["id", "name"]),
        ("excluded_regions", 19, &["id", "region_id"]),
        ("categories", 20, &["id", "name"]),
        ("excluded_categories", 21, &["id"]),
        ("blacklist", 22, &["id", "product_id", "category_id"]),
    ];

    /// The catalog of every table the tests reference.
    fn test_tables() -> Catalog {
        let mut tables = BiHashMap::new();
        for &(name, oid, columns) in TEST_TABLES {
            tables.insert_overwrite(test_table(name, Oid::from_raw(oid), columns).into());
        }
        tables
    }

    /// Parse SQL, bind it, and derive the update queries as
    /// `(table, update SQL, source)`.
    fn update_queries(sql: &str) -> Vec<(String, String, UpdateQuerySource)> {
        let tables = test_tables();
        let cacheable = parse_cacheable(sql);
        let tree = cacheable.tree().clone();
        let bound = ResolvedQuery::resolve(tree, &tables, &["public"]).expect("resolve");
        query_table_update_queries(&bound)
            .into_iter()
            .map(|update| {
                (
                    update.table.name().to_string(),
                    update.branch.sql(),
                    update.source,
                )
            })
            .collect()
    }

    /// `sql` yields exactly one update query per entry of `expected`, each
    /// table's with the source named for it.
    fn sources_expect(sql: &str, expected: &[(&str, UpdateQuerySource)]) {
        let result = update_queries(sql);
        assert_eq!(result.len(), expected.len(), "{sql}");
        for (table, source) in expected {
            let found = result
                .iter()
                .find(|(t, _, _)| t.as_str() == *table)
                .unwrap_or_else(|| panic!("no update query for {table}"));
            assert_eq!(found.2, *source, "{table}");
        }
    }

    #[test]
    fn test_query_table_update_query_simple_select() {
        let result = update_queries("SELECT id, name, email FROM users WHERE id = 1");

        assert_eq!(result.len(), 1);

        assert_eq!(
            result[0].1,
            "SELECT true FROM public.users WHERE public.users.id = 1"
        );
    }

    #[test]
    fn test_query_table_update_query_simple_join() {
        let result = update_queries(
            "SELECT id, name, email FROM users \
             JOIN location ON location.user_id = users.user_id \
             WHERE users.user_id = 1",
        );

        assert_eq!(result.len(), 2);

        let expected = "SELECT true FROM public.users JOIN public.location ON public.location.user_id = public.users.user_id WHERE public.users.user_id = 1";

        assert_eq!(result[0].1, expected);
        assert_eq!(result[1].1, expected);
    }

    #[test]
    fn test_query_table_update_query_three_table_join() {
        let result = update_queries(
            "SELECT * FROM users \
             JOIN location ON location.user_id = users.user_id \
             JOIN orders ON orders.user_id = users.user_id \
             WHERE users.user_id = 1",
        );

        assert_eq!(result.len(), 3, "Should have 3 update queries for 3 tables");

        // All three queries should produce the same SQL (the full join query with SELECT true)
        let update1 = &result[0].1;
        assert!(update1.contains("SELECT true"));
        assert!(update1.contains("JOIN"));

        // Verify each table is associated with an update query
        let table_names: Vec<_> = result.iter().map(|(t, _, _)| t.as_str()).collect();
        assert!(table_names.contains(&"users"));
        assert!(table_names.contains(&"location"));
        assert!(table_names.contains(&"orders"));
    }

    #[test]
    fn test_query_table_update_query_four_table_join() {
        let result = update_queries(
            "SELECT * FROM a \
             JOIN b ON a.id = b.id \
             JOIN c ON b.id = c.id \
             JOIN d ON c.id = d.id \
             WHERE a.id = 1",
        );

        assert_eq!(result.len(), 4, "Should have 4 update queries for 4 tables");

        // Verify each table is associated with an update query
        let table_names: Vec<_> = result.iter().map(|(t, _, _)| t.as_str()).collect();
        assert!(table_names.contains(&"a"));
        assert!(table_names.contains(&"b"));
        assert!(table_names.contains(&"c"));
        assert!(table_names.contains(&"d"));
    }

    // ==================== Set Operation Update Query Tests ====================
    //
    // Update queries for set operations are now branch-specific:
    // - Each table gets an update query derived from just its branch
    // - No UNION structure is preserved in update queries
    // - This enables CDC to check individual branch predicates

    #[test]
    fn test_query_table_update_query_union() {
        let result = update_queries(
            "SELECT id FROM users WHERE tenant_id = 1 \
             UNION SELECT id FROM admins WHERE tenant_id = 1",
        );

        assert_eq!(result.len(), 2, "Should have 2 update queries for 2 tables");

        // Verify each table is associated with an update query
        let table_names: Vec<_> = result.iter().map(|(t, _, _)| t.as_str()).collect();
        assert!(table_names.contains(&"users"));
        assert!(table_names.contains(&"admins"));

        // Update queries should be branch-specific (no UNION)
        for (table_name, sql, _) in &result {
            assert!(
                !sql.contains("UNION"),
                "Update query for {table_name} should NOT contain UNION: {sql}",
            );
            assert!(
                sql.contains("SELECT true"),
                "Update query should have SELECT true: {sql}",
            );
            assert!(
                sql.contains(table_name.as_str()),
                "Update query for {table_name} should reference that table: {sql}",
            );
        }
    }

    #[test]
    fn test_query_table_update_query_nested_union() {
        let result = update_queries(
            "SELECT id FROM a WHERE tenant_id = 1 \
             UNION SELECT id FROM b WHERE tenant_id = 1 \
             UNION SELECT id FROM c WHERE tenant_id = 1",
        );

        assert_eq!(result.len(), 3, "Should have 3 update queries for 3 tables");

        let table_names: Vec<_> = result.iter().map(|(t, _, _)| t.as_str()).collect();
        assert!(table_names.contains(&"a"));
        assert!(table_names.contains(&"b"));
        assert!(table_names.contains(&"c"));

        // Verify each update query is branch-specific (no UNION)
        for (table_name, sql, _) in &result {
            assert!(
                !sql.contains("UNION"),
                "Update query for {table_name} should NOT contain UNION: {sql}",
            );
        }
    }

    #[test]
    fn test_query_table_update_query_union_with_join() {
        let result = update_queries(
            "SELECT a.id FROM a JOIN b ON a.id = b.a_id WHERE a.tenant_id = 1 \
             UNION SELECT id FROM c WHERE tenant_id = 1",
        );

        assert_eq!(result.len(), 3, "Should have 3 update queries (a, b, c)");

        let table_names: Vec<_> = result.iter().map(|(t, _, _)| t.as_str()).collect();
        assert!(table_names.contains(&"a"));
        assert!(table_names.contains(&"b"));
        assert!(table_names.contains(&"c"));
    }

    // ==================== CTE Update Query Tests ====================

    #[test]
    fn test_cte_update_queries() {
        let result = update_queries(
            "WITH active_users AS (SELECT id, name FROM users WHERE status = 'active') \
             SELECT active_users.id, orders.total \
             FROM active_users \
             JOIN orders ON orders.user_id = active_users.id",
        );

        // The CTE body contains "users" and the main query joins with "orders".
        // select_nodes_with_source traverses into CTE bodies via CteRef,
        // so we should get tables from both the CTE body and the main query.
        let table_names: Vec<_> = result.iter().map(|(t, _, _)| t.as_str()).collect();
        assert!(
            table_names.contains(&"users"),
            "Should have update query for 'users' table from CTE body: {table_names:?}"
        );
        assert!(
            table_names.contains(&"orders"),
            "Should have update query for 'orders' table from main query: {table_names:?}"
        );
    }

    // ==================== UpdateQuerySource Tests ====================

    #[test]
    fn test_update_query_source_simple() {
        let result = update_queries("SELECT id FROM users WHERE id = 1");

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].2, UpdateQuerySource::FromClause);
    }

    #[test]
    fn test_update_query_source_join() {
        let result = update_queries(
            "SELECT a.id FROM users a JOIN orders b ON a.id = b.user_id WHERE a.id = 1",
        );

        assert_eq!(result.len(), 2);
        // Both tables in a JOIN are FromClause
        assert!(
            result
                .iter()
                .all(|(_, _, src)| *src == UpdateQuerySource::FromClause),
            "All JOIN tables should be FromClause"
        );
    }

    #[test]
    fn test_update_query_source_where_in() {
        let result =
            update_queries("SELECT * FROM users WHERE id IN (SELECT user_id FROM active_users)");

        assert_eq!(result.len(), 2);

        let users = result.iter().find(|(t, _, _)| t == "users").unwrap();
        assert_eq!(users.2, UpdateQuerySource::FromClause);

        let active = result.iter().find(|(t, _, _)| t == "active_users").unwrap();
        assert_eq!(
            active.2,
            UpdateQuerySource::Subquery(SubqueryKind::Inclusion)
        );
    }

    #[test]
    fn test_update_query_source_not_in() {
        let result = update_queries(
            "SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM banned_users)",
        );

        assert_eq!(result.len(), 2);

        let banned = result.iter().find(|(t, _, _)| t == "banned_users").unwrap();
        assert_eq!(
            banned.2,
            UpdateQuerySource::Subquery(SubqueryKind::Exclusion),
            "NOT IN subquery table should be Exclusion"
        );
    }

    #[test]
    fn test_update_query_source_cte() {
        let result = update_queries(
            "WITH active AS (SELECT id FROM users WHERE active = true) \
             SELECT * FROM active",
        );

        // CTE body "users" should be Subquery(Inclusion)
        let users = result.iter().find(|(t, _, _)| t == "users").unwrap();
        assert_eq!(
            users.2,
            UpdateQuerySource::Subquery(SubqueryKind::Inclusion),
            "CTE body table should be Subquery(Inclusion)"
        );
    }

    #[test]
    fn test_update_query_source_from_subquery() {
        let result = update_queries("SELECT * FROM (SELECT id FROM users WHERE id = 1) sub");

        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].2,
            UpdateQuerySource::Subquery(SubqueryKind::Inclusion),
            "FROM subquery table should be Subquery(Inclusion)"
        );
    }

    /// IN inside IN: every inner table is Inclusion.
    #[test]
    fn test_update_query_source_nested_in_in() {
        sources_expect(
            "SELECT name FROM products \
             WHERE store_id IN ( \
                 SELECT id FROM stores \
                 WHERE region_id IN ( \
                     SELECT id FROM regions WHERE name = 'East' \
                 ) \
             )",
            &[
                ("products", FromClause),
                (
                    "stores",
                    UpdateQuerySource::Subquery(SubqueryKind::Inclusion),
                ),
                (
                    "regions",
                    UpdateQuerySource::Subquery(SubqueryKind::Inclusion),
                ),
            ],
        );
    }

    /// NOT IN inside IN: the outer IN isn't negated, so the inner NOT IN is
    /// Exclusion.
    #[test]
    fn test_update_query_source_nested_not_in_inside_in() {
        sources_expect(
            "SELECT name FROM products \
             WHERE store_id IN ( \
                 SELECT id FROM stores \
                 WHERE region_id NOT IN ( \
                     SELECT region_id FROM excluded_regions \
                 ) \
             )",
            &[
                ("products", FromClause),
                (
                    "stores",
                    UpdateQuerySource::Subquery(SubqueryKind::Inclusion),
                ),
                (
                    "excluded_regions",
                    UpdateQuerySource::Subquery(SubqueryKind::Exclusion),
                ),
            ],
        );
    }

    /// IN inside NOT IN: the NOT IN sets negated, which the inner IN
    /// inherits, flipping its Inclusion to Exclusion.
    #[test]
    fn test_update_query_source_nested_in_inside_not_in() {
        sources_expect(
            "SELECT name FROM products \
             WHERE id NOT IN ( \
                 SELECT product_id FROM blacklist \
                 WHERE category_id IN ( \
                     SELECT id FROM categories WHERE name = 'Restricted' \
                 ) \
             )",
            &[
                ("products", FromClause),
                (
                    "blacklist",
                    UpdateQuerySource::Subquery(SubqueryKind::Exclusion),
                ),
                (
                    "categories",
                    UpdateQuerySource::Subquery(SubqueryKind::Exclusion),
                ),
            ],
        );
    }

    /// NOT IN inside NOT IN: the double negation flips the inner table back
    /// to Inclusion.
    #[test]
    fn test_update_query_source_nested_not_in_inside_not_in() {
        sources_expect(
            "SELECT name FROM products \
             WHERE id NOT IN ( \
                 SELECT product_id FROM blacklist \
                 WHERE category_id NOT IN ( \
                     SELECT id FROM excluded_categories \
                 ) \
             )",
            &[
                ("products", FromClause),
                (
                    "blacklist",
                    UpdateQuerySource::Subquery(SubqueryKind::Exclusion),
                ),
                (
                    "excluded_categories",
                    UpdateQuerySource::Subquery(SubqueryKind::Inclusion),
                ),
            ],
        );
    }

    // ==================== Outer Join Source Tests ====================

    /// The preserved side is FromClause; the terminal optional side is
    /// OuterJoinTerminal.
    #[test]
    fn test_update_query_source_terminal_left_join() {
        sources_expect(
            "SELECT a.id, b.name FROM a LEFT JOIN b ON a.id = b.a_id WHERE a.id = 1",
            &[("a", FromClause), ("b", OuterJoinTerminal)],
        );
    }

    /// An optional side mentioned above its join is OuterJoinOptional.
    #[test]
    fn test_update_query_source_non_terminal_left_join() {
        sources_expect(
            "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id WHERE b.status IS NULL",
            &[("a", FromClause), ("b", OuterJoinOptional)],
        );
    }

    /// `b` is non-terminal, its columns being in the outer LEFT JOIN's
    /// condition; `c` appears only in its own ON, so it is terminal.
    #[test]
    fn test_update_query_source_chained_outer_joins() {
        sources_expect(
            "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id LEFT JOIN c ON b.x = c.x WHERE a.id = 1",
            &[
                ("a", FromClause),
                ("b", OuterJoinOptional),
                ("c", OuterJoinTerminal),
            ],
        );
    }

    /// Both inner-joined tables are FromClause; the LEFT-joined `c` is the
    /// terminal optional side.
    #[test]
    fn test_update_query_source_mixed_inner_left() {
        sources_expect(
            "SELECT * FROM a JOIN b ON a.id = b.a_id LEFT JOIN c ON b.id = c.b_id WHERE a.id = 1",
            &[
                ("a", FromClause),
                ("b", FromClause),
                ("c", OuterJoinTerminal),
            ],
        );
    }

    /// An inner join's ON above a LEFT join rejects NULL for `b`, which
    /// reduces the LEFT join to an inner one, so `b` is maintained as an
    /// inner-join table.
    #[test]
    fn test_update_query_source_reduced_by_inner_join() {
        sources_expect(
            "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id JOIN c ON b.val = c.val WHERE a.id = 1",
            &[("a", FromClause), ("b", FromClause), ("c", FromClause)],
        );
    }

    /// Each table reference's source, in FROM order, from its own outer-join
    /// role rather than its table's name.
    #[test]
    fn test_update_query_source_per_reference() {
        #[rustfmt::skip]
        let cases: &[(&str, &[UpdateQuerySource])] = &[
            ("SELECT * FROM users WHERE id = 1", &[FromClause]),
            // WHERE rejects NULL for b: the join is reduced.
            ("SELECT * FROM a LEFT JOIN b ON a.id = b.a_id WHERE b.status = 'active'",
             &[FromClause, FromClause]),
            ("SELECT * FROM a RIGHT JOIN b ON a.id = b.a_id WHERE b.id = 1",
             &[OuterJoinTerminal, FromClause]),
            ("SELECT * FROM a RIGHT JOIN b ON a.id = b.a_id WHERE a.status IS NULL",
             &[OuterJoinOptional, FromClause]),
            // `IS NOT NULL` on the nullable side reduces the join to inner.
            ("SELECT * FROM a LEFT JOIN b ON a.id = b.a_id WHERE b.status IS NOT NULL",
             &[FromClause, FromClause]),
            ("SELECT a.id, t.name FROM a LEFT JOIN b t ON a.id = t.a_id WHERE a.id = 1",
             &[FromClause, OuterJoinTerminal]),
            // A null test in the join's own ON leaves either side's role alone.
            ("SELECT * FROM a LEFT JOIN b ON a.id = b.a_id AND b.status IS NULL WHERE a.id = 1",
             &[FromClause, OuterJoinTerminal]),
            ("SELECT * FROM a LEFT JOIN b ON a.id = b.a_id AND a.status IS NULL",
             &[FromClause, OuterJoinTerminal]),
            // Each reference of a self-join has its own role.
            ("SELECT * FROM a a1 LEFT JOIN a a2 ON a2.id = a1.val WHERE a1.status = 'x'",
             &[FromClause, OuterJoinTerminal]),
            // The preserved `a` behind an inner join is not terminal because
            // another reference to `a` is.
            ("SELECT * FROM a JOIN b ON b.id = a.val LEFT JOIN a a2 ON a2.id = a.tenant_id",
             &[FromClause, FromClause, OuterJoinTerminal]),
            // A partner beside it on the NULL-filled side keeps b from being a leaf.
            ("SELECT * FROM a LEFT JOIN (b JOIN c ON c.id = b.id) ON b.a_id = a.id",
             &[FromClause, OuterJoinOptional, OuterJoinOptional]),
        ];
        for (sql, expected) in cases {
            let sources: Vec<UpdateQuerySource> = update_queries(sql)
                .into_iter()
                .map(|(_, _, source)| source)
                .collect();
            assert_eq!(sources, *expected, "{sql}");
        }
    }
}
