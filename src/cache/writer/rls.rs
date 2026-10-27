//! Row-level security by policy inlining (PGC-113). The writer loads each
//! secured relation's policies from origin, decides which apply to the role
//! a read runs as (attributes, ownership, `FORCE`, membership in the
//! policies' role lists, all origin's own answers), substitutes the session
//! values the read carries, and ANDs the result onto the relation's
//! references before the query is resolved for its fingerprint. The inlined
//! predicate is an ordinary conjunct from then on: population, CDC and
//! serving need nothing more. What cannot be inlined is refused, and the
//! read forwards.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use ecow::EcoString;
use tokio_postgres::Row;
use tracing::{debug, warn};

use super::core::WriterCore;
use crate::cache::query::CacheableQuery;
use crate::cache::types::RlsRelations;
use crate::cache::{CacheError, CacheResult, MapIntoReport, Report, ReportExt};
use crate::catalog::Catalog;
use crate::oid::Oid;
use crate::pg::identifier_quote_into;
use crate::query::ast::rope::{
    Body, NodeKind, NodeRef, QueryTree, Select, Source, TreeBuilder, View, Where,
};
use crate::query::ast::{BinaryOp, JoinType, NodeId, query_tree_parse};
use crate::query::keying_settings::KeyingSettings;
use crate::query::resolve::{ColumnTarget, ResolvedQuery, TableSlot};
use crate::query::transform::{
    SessionTermError, query_tree_session_terms_replace, query_tree_setting_names,
};
use crate::result::error_chain_format;

/// Whether the writer's own role sees every row of a secured relation
/// (superuser or `BYPASSRLS`): what inlining relies on for population.
const RLS_BYPASS_SQL: &str =
    "SELECT r.rolsuper OR r.rolbypassrls FROM pg_roles r WHERE r.rolname = current_user";

/// Every relation origin applies row-level security to.
const RLS_TABLES_SQL: &str = "SELECT c.oid FROM pg_class c \
     WHERE c.relrowsecurity AND c.relkind IN ('r', 'p')";

const RLS_TABLE_SQL: &str = "SELECT n.nspname, c.relname, c.relrowsecurity \
     FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = $1";

/// The policies a `SELECT` is subject to: `FOR SELECT` and `FOR ALL`, with
/// their USING expression deparsed under the connection's search_path.
const RLS_POLICIES_SQL: &str = "SELECT p.polname::text, p.polpermissive, \
            pg_get_expr(p.polqual, p.polrelid) \
     FROM pg_policy p \
     WHERE p.polrelid = $1 AND p.polqual IS NOT NULL AND p.polcmd IN ('r', '*') \
     ORDER BY p.polname";

/// Whether `$1` is exempt from `$2`'s policies (superuser, `BYPASSRLS`, or
/// the owner without `FORCE`), and the policies whose role list it has the
/// privileges of, as origin decides (`has_privs_of_role`). No row: no such
/// role or relation.
const RLS_APPLICABILITY_SQL: &str = "SELECT c.relrowsecurity, \
            r.rolsuper OR r.rolbypassrls OR (c.relowner = r.oid AND NOT c.relforcerowsecurity), \
            ARRAY(SELECT p.polname::text FROM pg_policy p \
                   WHERE p.polrelid = c.oid \
                     AND (0 = ANY(p.polroles) \
                          OR EXISTS (SELECT 1 FROM unnest(p.polroles) AS pr(oid) \
                                      WHERE pg_has_role(r.oid, pr.oid, 'USAGE')))) \
     FROM pg_roles r, pg_class c WHERE r.rolname = $1 AND c.oid = $2";

/// A policy's USING expression, parsed as the WHERE of `SELECT 1 FROM
/// <table>` with every table it names schema-qualified, and the positions
/// of the column references that are the policy table's own (as opposed to
/// a subquery's), which take the reference's qualifier when inlined.
struct ParsedQual {
    tree: QueryTree,
    own_columns: HashSet<NodeId>,
}

struct RlsPolicy {
    name: EcoString,
    permissive: bool,
    /// The parsed qual, or why it cannot be inlined.
    qual: Result<ParsedQual, EcoString>,
}

struct RlsTable {
    name: EcoString,
    policies: Vec<RlsPolicy>,
    /// The custom settings the policies read, for the proxy's keying.
    settings: Vec<EcoString>,
}

/// The secured relations the writer knows, by relation OID.
#[derive(Default)]
pub(super) struct RlsCatalog {
    tables: HashMap<Oid, RlsTable>,
    /// Whether pgcache's role bypasses the policies. Without it a population
    /// of an inlined query would still be filtered by origin for pgcache's
    /// own role, so nothing secured is inlined and such reads forward.
    population_bypasses: bool,
}

/// What origin does with a role's read of a secured relation.
enum Applicability {
    /// Not secured after all, or the role bypasses the policies.
    Exempt,
    /// The policies, by name, whose role list the role has the privileges of.
    Policies(Vec<EcoString>),
}

/// One policy's predicate, ready to inline under a reference's qualifier.
struct InlinedPolicy {
    tree: QueryTree,
    own_columns: HashSet<NodeId>,
    permissive: bool,
}

/// The predicates to AND onto one table reference.
struct ReferencePredicate {
    qualifier: EcoString,
    /// At least one, or there is no predicate: no permissive policy is
    /// default deny, decided before this is built.
    permissive: Vec<InlinedPolicy>,
    restrictive: Vec<InlinedPolicy>,
}

impl ReferencePredicate {
    /// `None` when no policy is permissive: the reference admits no row.
    fn new(qualifier: EcoString, policies: Vec<InlinedPolicy>) -> Option<Self> {
        let (permissive, restrictive): (Vec<_>, Vec<_>) =
            policies.into_iter().partition(|policy| policy.permissive);
        (!permissive.is_empty()).then_some(Self {
            qualifier,
            permissive,
            restrictive,
        })
    }
}

impl WriterCore {
    /// Load every secured relation's policies at writer start and publish
    /// their names, so a connection's first read over one is keyed on the
    /// session values the policies read. A relation that fails to load is
    /// skipped and loaded again at its first registration.
    pub(super) async fn rls_startup_load(&mut self) -> CacheResult<()> {
        let bypass = self
            .db_origin
            .query_one(RLS_BYPASS_SQL, &[])
            .await
            .map_into_report::<CacheError>()
            .attach_loc("reading whether the writer's role bypasses row-level security")?;
        self.rls.population_bypasses = bypass.try_get(0).map_into_report::<CacheError>()?;
        let rows = self
            .db_origin
            .query(RLS_TABLES_SQL, &[])
            .await
            .map_into_report::<CacheError>()
            .attach_loc("listing the relations with row-level security")?;
        if !rows.is_empty() && !self.rls.population_bypasses {
            warn!(
                secured_relations = rows.len(),
                "pgcache's role cannot bypass row-level security: reads over secured relations \
                 forward until it is a superuser or has BYPASSRLS"
            );
        }
        for row in &rows {
            let oid: Oid = row.try_get(0).map_into_report::<CacheError>()?;
            if let Err(e) = self.rls_table_load(oid).await {
                warn!(
                    relation_oid = %oid,
                    "loading a secured relation's policies failed, retried at its first read: {}",
                    error_chain_format(e.current_context())
                );
            }
        }
        self.rls_relations_publish();
        Ok(())
    }

    /// A secured relation's policies changed, or it stopped or started being
    /// secured: reload it, republish the names, and drop every alias whose
    /// inlined form may have changed with it.
    pub(super) async fn rls_table_reload(&mut self, relation_oid: Oid) -> CacheResult<()> {
        let was_secured = self.rls.tables.contains_key(&relation_oid);
        let secured = self.rls_table_load(relation_oid).await?;
        // An unsecured table's ALTER changes nothing an alias depends on.
        if was_secured || secured {
            self.rls_relations_publish();
            self.rls_aliases_clear();
        }
        Ok(())
    }

    /// The relation a policy signal names, from origin's catalog: the one
    /// place the writer learns of a relation it has never cached or loaded.
    pub(super) async fn rls_table_oid_lookup(
        &self,
        schema: &str,
        name: &str,
    ) -> CacheResult<Option<Oid>> {
        let row = self
            .db_origin
            .query_opt(
                "SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = $1 AND c.relname = $2",
                &[&schema, &name],
            )
            .await
            .map_into_report::<CacheError>()
            .attach_loc("resolving a policy's table")?;
        row.map(|row| row.try_get(0).map_into_report::<CacheError>())
            .transpose()
    }

    /// Forget every alias keyed with an RLS context (PGC-113): the next read
    /// under each resolves again, onto the same fingerprint when nothing
    /// changed for it.
    pub(super) fn rls_aliases_clear(&self) {
        self.state_view.aliases.retain(|_, alias| !alias.rls);
    }

    fn rls_relations_publish(&self) {
        let relations = RlsRelations::new(
            self.rls
                .tables
                .values()
                .map(|table| (table.name.clone(), table.settings.clone())),
        );
        self.state_view.rls_relations.store(Arc::new(relations));
    }

    /// Load (or drop, when origin no longer secures it) one relation's
    /// policies. Each USING expression is parsed and bound against the
    /// tables it names; one the converter cannot model is kept as
    /// uninlinable, so the relation's reads forward while its name is
    /// still published.
    async fn rls_table_load(&mut self, relation_oid: Oid) -> CacheResult<bool> {
        let Some(row) = self
            .db_origin
            .query_opt(RLS_TABLE_SQL, &[&relation_oid])
            .await
            .map_into_report::<CacheError>()
            .attach_loc("reading a secured relation")?
        else {
            self.rls.tables.remove(&relation_oid);
            return Ok(false);
        };
        let schema: &str = row.try_get(0).map_into_report::<CacheError>()?;
        let name: &str = row.try_get(1).map_into_report::<CacheError>()?;
        let secured: bool = row.try_get(2).map_into_report::<CacheError>()?;
        if !secured {
            self.rls.tables.remove(&relation_oid);
            return Ok(false);
        }
        let (schema, name) = (EcoString::from(schema), EcoString::from(name));
        let rows = self
            .db_origin
            .query(RLS_POLICIES_SQL, &[&relation_oid])
            .await
            .map_into_report::<CacheError>()
            .attach_loc("reading a relation's policies")?;
        let mut policies = Vec::with_capacity(rows.len());
        let mut settings: Vec<EcoString> = Vec::new();
        for row in &rows {
            let policy = self
                .rls_policy_build(row, relation_oid, &schema, &name)
                .await?;
            if let Ok(parsed) = &policy.qual {
                settings.extend(query_tree_setting_names(&parsed.tree).cloned());
            }
            policies.push(policy);
        }
        settings.sort();
        settings.dedup();
        debug!(
            relation_oid = %relation_oid,
            policies = policies.len(),
            "loaded a secured relation's policies"
        );
        self.rls.tables.insert(
            relation_oid,
            RlsTable {
                name,
                policies,
                settings,
            },
        );
        Ok(true)
    }

    async fn rls_policy_build(
        &mut self,
        row: &Row,
        relation_oid: Oid,
        schema: &str,
        name: &str,
    ) -> CacheResult<RlsPolicy> {
        let policy_name: &str = row.try_get(0).map_into_report::<CacheError>()?;
        let permissive: bool = row.try_get(1).map_into_report::<CacheError>()?;
        let qual: &str = row.try_get(2).map_into_report::<CacheError>()?;
        let qual = self
            .rls_qual_parse(qual, relation_oid, schema, name)
            .await
            .map_err(|reason| {
                debug!(
                    relation_oid = %relation_oid,
                    policy = policy_name,
                    "policy cannot be inlined: {reason}"
                );
                reason
            });
        Ok(RlsPolicy {
            name: EcoString::from(policy_name),
            permissive,
            qual,
        })
    }

    /// `qual` as the WHERE of a one-table SELECT, its table names
    /// schema-qualified as the writer's connection resolves them (the
    /// context `pg_get_expr` deparsed them under), bound to find the
    /// policy table's own column references. A catalog read failure is an
    /// uninlinable policy, not a failed load.
    async fn rls_qual_parse(
        &mut self,
        qual: &str,
        relation_oid: Oid,
        schema: &str,
        name: &str,
    ) -> Result<ParsedQual, EcoString> {
        let mut sql = String::with_capacity(qual.len() + 48);
        sql.push_str("SELECT 1 FROM ");
        identifier_quote_into(schema, &mut sql);
        sql.push('.');
        identifier_quote_into(name, &mut sql);
        sql.push_str(" WHERE ");
        sql.push_str(qual);
        let tree = query_tree_parse(&sql).map_err(|e| EcoString::from(e.to_string()))?;
        let qualified = self.rls_tables_qualify(&tree).await?;
        // A second reference to the policy table is a different row set than
        // the one the predicate filters; not modelled here.
        let self_references = qualified
            .table_names()
            .filter(|(table_schema, table_name)| {
                *table_schema == Some(schema) && *table_name == name
            })
            .count();
        if self_references > 1 {
            return Err("the policy reads its own table in a subquery".into());
        }
        let mut catalog = Catalog::new();
        for (table_schema, table_name) in qualified.table_names() {
            let Some(table_schema) = table_schema else {
                return Err("a table the policy reads has no schema".into());
            };
            if catalog.contains_key2(&(table_schema, table_name)) {
                continue;
            }
            let metadata = self
                .query_table_metadata(Some(table_schema), table_name)
                .await
                .map_err(|e| EcoString::from(error_chain_format(e.current_context())))?;
            // Origin applies another secured table's own policies to the
            // subquery too, which is not modelled here.
            if metadata.relation_oid != relation_oid && metadata.row_security {
                return Err(format!(
                    "the policy reads {}.{}, which has row-level security of its own",
                    metadata.schema, metadata.name
                )
                .into());
            }
            catalog.insert_overwrite(metadata.into());
        }
        let bound = ResolvedQuery::resolve(qualified, &catalog, &[])
            .map_err(|e| EcoString::from(error_chain_format(e.current_context())))?;
        // A built-in setting is not tracked per connection the way a custom
        // (dotted) one is, so its value at origin could differ from any the
        // read is keyed under.
        if let Some(name) = query_tree_setting_names(bound.tree()).find(|name| !name.contains('.'))
        {
            return Err(format!("the policy reads the built-in setting {name}").into());
        }
        Ok(parsed_qual_of(&bound, relation_oid))
    }

    /// `tree` with every bare table name given the schema it resolves to on
    /// the writer connection's search_path.
    async fn rls_tables_qualify(&mut self, tree: &QueryTree) -> Result<QueryTree, EcoString> {
        let mut schemas: HashMap<EcoString, EcoString> = HashMap::new();
        for (schema, name) in tree.table_names() {
            if schema.is_some() || schemas.contains_key(name) {
                continue;
            }
            let path = Arc::clone(&self.origin_search_path);
            let resolved = self
                .schema_for_table_find(name, &path)
                .await
                .map_err(|e| EcoString::from(error_chain_format(e.current_context())))?;
            schemas.insert(EcoString::from(name), resolved);
        }
        if schemas.is_empty() {
            return Ok(tree.clone());
        }
        let mut builder = TreeBuilder::new();
        tree_emit_mapped(&mut builder, tree.root().node_ref(), &|node| {
            let NodeKind::Table {
                schema,
                name,
                alias,
                alias_column_count,
            } = node.kind()
            else {
                return None;
            };
            if !schema.is_empty() {
                return None;
            }
            schemas.get(name).map(|resolved| NodeKind::Table {
                schema: resolved.clone(),
                name: name.clone(),
                alias: alias.clone(),
                alias_column_count: *alias_column_count,
            })
        });
        Ok(builder.finish().0)
    }

    async fn rls_applicability(&self, role: &str, relation_oid: Oid) -> CacheResult<Applicability> {
        let Some(row) = self
            .db_origin
            .query_opt(RLS_APPLICABILITY_SQL, &[&role, &relation_oid])
            .await
            .map_into_report::<CacheError>()
            .attach_loc("reading a role's policy applicability")?
        else {
            return Err(rls_refusal(CacheError::RlsRoleUnknown));
        };
        let secured: bool = row.try_get(0).map_into_report::<CacheError>()?;
        let exempt: bool = row.try_get(1).map_into_report::<CacheError>()?;
        if !secured || exempt {
            return Ok(Applicability::Exempt);
        }
        let names: Vec<String> = row.try_get(2).map_into_report::<CacheError>()?;
        Ok(Applicability::Policies(
            names.into_iter().map(EcoString::from).collect(),
        ))
    }

    /// `bound` with the policies that apply to the read's role inlined onto
    /// every reference of a secured relation, or `None` when nothing is
    /// secured or the role is exempt everywhere. A read the writer cannot
    /// inline for is refused with the reason, counted, and forwards.
    pub(super) async fn rls_inline(
        &mut self,
        bound: &ResolvedQuery,
        keying: &KeyingSettings,
    ) -> CacheResult<Option<QueryTree>> {
        let secured = secured_references(bound);
        if secured.is_empty() {
            return Ok(None);
        }
        if !self.rls.population_bypasses {
            return Err(rls_refusal(CacheError::RlsRoleCannotBypass));
        }
        for (_, relation_oid) in &secured {
            if !self.rls.tables.contains_key(relation_oid) {
                self.rls_table_load(*relation_oid).await?;
                self.rls_relations_publish();
            }
        }
        let Some(rls) = &keying.rls else {
            // The proxy keyed the read without a context: its published
            // relation set predates this relation. The names are out now.
            return Err(rls_refusal(CacheError::RlsNoContext));
        };
        let Some(role) = &rls.terms.current_user else {
            return Err(rls_refusal(CacheError::RlsRoleUnknown));
        };

        let mut predicates: HashMap<NodeId, ReferencePredicate> = HashMap::new();
        let mut applicability: HashMap<Oid, Applicability> = HashMap::new();
        for (reference, relation_oid) in &secured {
            if !applicability.contains_key(relation_oid) {
                let applies = self.rls_applicability(role, *relation_oid).await?;
                applicability.insert(*relation_oid, applies);
            }
            let Some(Applicability::Policies(names)) = applicability.get(relation_oid) else {
                continue;
            };
            if rls.row_security == Some(false) {
                return Err(rls_refusal(CacheError::RlsRowSecurityOff));
            }
            let Some(table) = self.rls.tables.get(relation_oid) else {
                return Err(rls_refusal(CacheError::RlsUnsupported {
                    reason: "a secured relation's policies are not loaded",
                }));
            };
            let mut policies = Vec::new();
            for policy in table.policies.iter().filter(|p| names.contains(&p.name)) {
                let parsed = policy.qual.as_ref().map_err(|reason| {
                    rls_refusal(CacheError::RlsUninlinable {
                        policy: policy.name.clone(),
                        reason: reason.clone(),
                    })
                })?;
                let tree = query_tree_session_terms_replace(&parsed.tree, &rls.terms)
                    .map_err(|e| rls_refusal(term_error(e)))?;
                CacheableQuery::cacheable(&tree, &self.function_volatility).map_err(|e| {
                    rls_refusal(CacheError::RlsUninlinable {
                        policy: policy.name.clone(),
                        reason: EcoString::from(e.to_string()),
                    })
                })?;
                policies.push(InlinedPolicy {
                    tree,
                    own_columns: parsed.own_columns.clone(),
                    permissive: policy.permissive,
                });
            }
            let qualifier = reference_qualifier(bound.tree(), *reference);
            let Some(predicate) = ReferencePredicate::new(qualifier, policies) else {
                return Err(rls_refusal(CacheError::RlsDefaultDeny));
            };
            predicates.insert(*reference, predicate);
        }
        if predicates.is_empty() {
            return Ok(None);
        }
        rls_rewrite(bound, &predicates).map(Some)
    }
}

/// A bound policy qual (`SELECT 1 FROM <table> WHERE <qual>`) with the
/// positions of the policy table's own column references.
fn parsed_qual_of(bound: &ResolvedQuery, relation_oid: Oid) -> ParsedQual {
    let bindings = bound.bindings();
    let own_columns = bound
        .tree()
        .root()
        .node_ref()
        .subtree()
        .filter(|node| matches!(node.kind(), NodeKind::Column { .. }))
        .filter(|node| match bindings.column_by_id(node.id()) {
            Some(ColumnTarget::Column(binding)) => bindings
                .slot(binding.slot)
                .is_some_and(|slot| slot.table().relation_oid == relation_oid),
            Some(ColumnTarget::Merged(_) | ColumnTarget::Output) | None => false,
        })
        .map(|node| node.id())
        .collect();
    ParsedQual {
        tree: bound.tree().clone(),
        own_columns,
    }
}

/// `bound` with each reference's predicate ANDed onto the WHERE of the
/// SELECT it belongs to.
fn rls_rewrite(
    bound: &ResolvedQuery,
    predicates: &HashMap<NodeId, ReferencePredicate>,
) -> CacheResult<QueryTree> {
    let plan = select_plan(bound.tree(), predicates)?;
    let mut inliner = Inliner {
        builder: TreeBuilder::over(bound.tree()),
        plan: &plan,
        predicates,
    };
    inliner.emit(bound.tree().root().node_ref());
    Ok(inliner.builder.finish().0)
}

/// Every base table reference of a secured relation: the `Table` node and
/// its relation.
fn secured_references(bound: &ResolvedQuery) -> Vec<(NodeId, Oid)> {
    let bindings = bound.bindings();
    bound
        .tree()
        .root()
        .node_ref()
        .subtree()
        .filter(|node| matches!(node.kind(), NodeKind::Table { .. }))
        .filter_map(|node| {
            let slot = bindings.slot(bindings.source(node.id())?)?;
            match slot {
                TableSlot::Base { table, .. } if table.row_security => {
                    Some((node.id(), table.relation_oid))
                }
                TableSlot::Base { .. } | TableSlot::Derived { .. } => None,
            }
        })
        .collect()
}

/// The name a reference's columns are qualified by: its alias, else the
/// table name.
fn reference_qualifier(tree: &QueryTree, reference: NodeId) -> EcoString {
    let NodeKind::Table { name, alias, .. } = &tree.node(reference).kind else {
        debug_assert!(false, "a secured reference is a table node");
        return EcoString::new();
    };
    if alias.is_empty() {
        name.clone()
    } else {
        alias.clone()
    }
}

/// Which SELECT each secured reference belongs to, and so takes its
/// predicate in its WHERE. A reference on the nullable side of an outer
/// join cannot take it there (origin filters the relation before the join
/// preserves the other side's rows), so such a read is refused.
fn select_plan(
    tree: &QueryTree,
    predicates: &HashMap<NodeId, ReferencePredicate>,
) -> CacheResult<HashMap<NodeId, Vec<NodeId>>> {
    let mut plan: HashMap<NodeId, Vec<NodeId>> = HashMap::new();
    for node in tree.root().node_ref().subtree() {
        if !matches!(node.kind(), NodeKind::Select { .. }) {
            continue;
        }
        let select = Select::from_ref(node);
        let mut references = Vec::new();
        for source in select.from() {
            source_references(source, false, &mut references);
        }
        for (reference, nullable) in references {
            if !predicates.contains_key(&reference) {
                continue;
            }
            if nullable {
                return Err(rls_refusal(CacheError::RlsUnsupported {
                    reason: "a secured relation on the nullable side of an outer join",
                }));
            }
            plan.entry(select.id()).or_default().push(reference);
        }
    }
    Ok(plan)
}

/// The base table references under `source` with whether each sits on a
/// side an outer join can null.
fn source_references(source: Source<'_>, nullable: bool, out: &mut Vec<(NodeId, bool)>) {
    match source {
        Source::Table(table) => out.push((table.id(), nullable)),
        Source::Join(join) => {
            let (left_nullable, right_nullable) = match join.join_type() {
                JoinType::Inner => (false, false),
                JoinType::Left => (false, true),
                JoinType::Right => (true, false),
                JoinType::Full => (true, true),
            };
            source_references(join.left(), nullable || left_nullable, out);
            source_references(join.right(), nullable || right_nullable, out);
        }
        // A derived table's own SELECT is planned on its own.
        Source::Subquery(_) | Source::CteRef(_) => {}
    }
}

/// The error a session term that cannot be substituted refuses the read with.
fn term_error(error: SessionTermError) -> CacheError {
    match error {
        SessionTermError::UserUnknown => CacheError::RlsRoleUnknown,
        SessionTermError::SettingUnknown { name } | SessionTermError::SettingUnset { name } => {
            CacheError::RlsSettingUnknown { name }
        }
    }
}

/// Count a refusal by reason and wrap it as the registration's error.
fn rls_refusal(error: CacheError) -> Report<CacheError> {
    let m = crate::metrics::handles();
    let counter = if matches!(error, CacheError::RlsSettingUnknown { .. }) {
        &m.query.rls_forward_setting_unknown
    } else if matches!(error, CacheError::RlsDefaultDeny) {
        &m.query.rls_forward_default_deny
    } else if matches!(error, CacheError::RlsRowSecurityOff) {
        &m.query.rls_forward_row_security_off
    } else if matches!(
        error,
        CacheError::RlsUninlinable { .. } | CacheError::RlsRoleCannotBypass
    ) {
        &m.query.rls_skipped
    } else {
        &m.query.rls_forward_unsupported
    };
    counter.increment(1);
    Report::from(error)
}

/// Emit `node` and its subtree afresh, each node's kind through `map` (a
/// replacement kind, or the node's own).
fn tree_emit_mapped(
    builder: &mut TreeBuilder<'_>,
    node: NodeRef<'_>,
    map: &dyn Fn(NodeRef<'_>) -> Option<NodeKind>,
) {
    let kind = map(node).unwrap_or_else(|| node.kind().clone());
    let open = builder.open(kind);
    for child in node.children() {
        tree_emit_mapped(builder, child, map);
    }
    builder.close(open);
}

/// The rewrite: the base tree with each planned SELECT re-emitted, its
/// WHERE extended by the predicates of the secured references it holds.
struct Inliner<'t, 'p> {
    builder: TreeBuilder<'t>,
    plan: &'p HashMap<NodeId, Vec<NodeId>>,
    predicates: &'p HashMap<NodeId, ReferencePredicate>,
}

impl<'t> Inliner<'t, '_> {
    fn emit(&mut self, node: NodeRef<'t>) {
        if let Some(references) = self.plan.get(&node.id()) {
            self.select_emit(Select::from_ref(node), references);
        } else if !self.plan_within(node) {
            self.builder.keep(node.id());
        } else {
            let open = self.builder.open(node.kind().clone());
            for child in node.children() {
                self.emit(child);
            }
            self.builder.close(open);
        }
    }

    /// Whether a planned SELECT lies under `node`.
    fn plan_within(&self, node: NodeRef<'t>) -> bool {
        let start = node.id().index();
        let end = node.end() as usize;
        self.plan
            .keys()
            .any(|select| select.index() > start && select.index() < end)
    }

    fn select_emit(&mut self, select: Select<'t>, references: &[NodeId]) {
        let mut kind = select.node_ref().kind().clone();
        if let NodeKind::Select { has_where, .. } = &mut kind {
            *has_where = true;
        }
        let open = self.builder.open(kind);
        for item in select.items().into_iter().flatten() {
            self.emit(item.node_ref());
        }
        for source in select.from() {
            self.emit(source.node_ref());
        }
        let existing = select.where_clause();
        let count = usize::from(existing.is_some()) + references.len();
        self.where_chain_emit(existing, references, count);
        for column in select.group_by() {
            self.emit(column.node_ref());
        }
        if let Some(having) = select.having() {
            self.emit(having.node_ref());
        }
        self.builder.close(open);
    }

    /// The first `count` conjuncts (the existing WHERE, then one predicate
    /// per reference) as a left-associated AND chain.
    fn where_chain_emit(
        &mut self,
        existing: Option<Where<'t>>,
        references: &[NodeId],
        count: usize,
    ) {
        if count > 1 {
            let open = self.builder.open(NodeKind::Binary { op: BinaryOp::And });
            self.where_chain_emit(existing, references, count - 1);
            self.conjunct_emit(existing, references, count - 1);
            self.builder.close(open);
        } else {
            self.conjunct_emit(existing, references, 0);
        }
    }

    /// Conjunct `index`: 0 is the existing WHERE when there is one, the
    /// rest the references' predicates in order.
    fn conjunct_emit(&mut self, existing: Option<Where<'t>>, references: &[NodeId], index: usize) {
        let reference = match existing {
            Some(existing) if index == 0 => {
                self.emit(existing.node_ref());
                return;
            }
            Some(_) => index - 1,
            None => index,
        };
        let Some(predicate) = references
            .get(reference)
            .and_then(|reference| self.predicates.get(reference))
        else {
            debug_assert!(false, "every planned reference has a predicate");
            self.builder
                .leaf(NodeKind::Literal(crate::query::ast::LiteralValue::Boolean(
                    true,
                )));
            return;
        };
        self.predicate_emit(predicate);
    }

    /// `(p1 OR p2 …) AND r1 AND r2 …`: the permissive policies ORed, the
    /// restrictive ones ANDed on, as origin combines them.
    fn predicate_emit(&mut self, predicate: &ReferencePredicate) {
        let permissive: Vec<&InlinedPolicy> = predicate.permissive.iter().collect();
        let restrictive: Vec<&InlinedPolicy> = predicate.restrictive.iter().collect();
        let mut opens = Vec::with_capacity(restrictive.len());
        for _ in &restrictive {
            opens.push(self.builder.open(NodeKind::Binary { op: BinaryOp::And }));
        }
        self.policies_chain_emit(&permissive, BinaryOp::Or, &predicate.qualifier);
        for (policy, open) in restrictive.iter().zip(opens.into_iter().rev()) {
            self.policy_emit(policy, &predicate.qualifier);
            self.builder.close(open);
        }
    }

    fn policies_chain_emit(
        &mut self,
        policies: &[&InlinedPolicy],
        op: BinaryOp,
        qualifier: &EcoString,
    ) {
        let Some((last, init)) = policies.split_last() else {
            return;
        };
        if init.is_empty() {
            self.policy_emit(last, qualifier);
            return;
        }
        let open = self.builder.open(NodeKind::Binary { op });
        self.policies_chain_emit(init, op, qualifier);
        self.policy_emit(last, qualifier);
        self.builder.close(open);
    }

    /// A policy's WHERE, its own column references qualified by the
    /// reference's name. A source inside the policy (a subquery's table or
    /// alias) spelled like that name would capture them, so such a source
    /// is renamed, with the references to it, before the policy is emitted.
    fn policy_emit(&mut self, policy: &InlinedPolicy, qualifier: &EcoString) {
        let Body::Select(select) = policy.tree.root().body() else {
            debug_assert!(false, "a policy qual is parsed as a SELECT");
            return;
        };
        let Some(where_clause) = select.where_clause() else {
            debug_assert!(false, "a policy qual is parsed with a WHERE");
            return;
        };
        let shadowed = where_clause
            .node_ref()
            .subtree()
            .any(|node| source_qualifier(node.kind()).is_some_and(|name| name == qualifier));
        let renamed = EcoString::from(SHADOW_RENAME);
        tree_emit_mapped(&mut self.builder, where_clause.node_ref(), &|node| {
            let kind = node.kind();
            if let NodeKind::Column { column, .. } = kind
                && policy.own_columns.contains(&node.id())
            {
                return Some(NodeKind::Column {
                    table: Some(qualifier.clone()),
                    column: column.clone(),
                });
            }
            if !shadowed {
                return None;
            }
            if let NodeKind::Column {
                table: Some(table),
                column,
            } = kind
                && table == qualifier
            {
                return Some(NodeKind::Column {
                    table: Some(renamed.clone()),
                    column: column.clone(),
                });
            }
            if source_qualifier(kind) != Some(qualifier) {
                return None;
            }
            source_renamed(kind, &renamed)
        });
    }
}

/// The alias a source inside a policy is renamed to when it would shadow
/// the outer reference's qualifier. Not an identifier a client would write.
const SHADOW_RENAME: &str = "__pgcache_rls";

/// The name a FROM source's columns are qualified by, when the node is one.
fn source_qualifier(kind: &NodeKind) -> Option<&EcoString> {
    if let NodeKind::Table { name, alias, .. } = kind {
        return Some(if alias.is_empty() { name } else { alias });
    }
    if let NodeKind::Subquery { alias, .. } | NodeKind::CteRef { alias, .. } = kind {
        return alias.as_ref();
    }
    None
}

/// A FROM source under the alias `renamed`; `None` for any other node.
fn source_renamed(kind: &NodeKind, renamed: &EcoString) -> Option<NodeKind> {
    if let NodeKind::Table {
        schema,
        name,
        alias_column_count,
        ..
    } = kind
    {
        return Some(NodeKind::Table {
            schema: schema.clone(),
            name: name.clone(),
            alias: renamed.clone(),
            alias_column_count: *alias_column_count,
        });
    }
    if let NodeKind::Subquery {
        lateral,
        alias_column_count,
        kind: subquery_kind,
        ..
    } = kind
    {
        return Some(NodeKind::Subquery {
            lateral: *lateral,
            alias: Some(renamed.clone()),
            alias_column_count: *alias_column_count,
            kind: *subquery_kind,
        });
    }
    if let NodeKind::CteRef {
        cte_name,
        column_alias_count,
        materialization,
        alias_column_count,
        ..
    } = kind
    {
        return Some(NodeKind::CteRef {
            cte_name: cte_name.clone(),
            column_alias_count: *column_alias_count,
            materialization: *materialization,
            alias: Some(renamed.clone()),
            alias_column_count: *alias_column_count,
        });
    }
    None
}

#[cfg(test)]
mod tests {
    use iddqd::BiHashMap;
    use postgres_types::Type;

    use super::*;
    use crate::catalog::{ColumnMetadata, ColumnPosition, TableMetadata};
    use crate::oid::TypeOid;

    /// `docs` (secured) and `memberships`, text columns, OIDs 1001 and 1002.
    fn catalog() -> Catalog {
        let specs: [(&str, &[&str], bool); 2] = [
            ("docs", &["id", "tenant", "owner"], true),
            ("memberships", &["user_name", "tenant"], false),
        ];
        let mut tables = BiHashMap::new();
        for (oid, (name, columns, secured)) in (1001..).zip(specs) {
            let store = columns
                .iter()
                .enumerate()
                .map(|(i, column)| ColumnMetadata {
                    name: (*column).into(),
                    position: ColumnPosition::from_index(i).expect("column position in range"),
                    type_oid: TypeOid::from_raw(25),
                    data_type: Type::TEXT,
                    type_name: "text".into(),
                    cache_type_name: "text".into(),
                    is_primary_key: i == 0,
                });
            let mut metadata = TableMetadata::for_test(name, oid, store);
            metadata.row_security = secured;
            tables.insert_overwrite(metadata.into());
        }
        tables
    }

    fn bind(sql: &str, tables: &Catalog) -> ResolvedQuery {
        ResolvedQuery::parse_resolve(sql, tables, &["public"])
            .unwrap_or_else(|e| panic!("{sql} resolves: {e:?}"))
    }

    fn policy(qual: &str, permissive: bool, tables: &Catalog) -> InlinedPolicy {
        let bound = bind(&format!("SELECT 1 FROM docs WHERE {qual}"), tables);
        let parsed = parsed_qual_of(&bound, Oid::from_raw(1001));
        InlinedPolicy {
            tree: parsed.tree,
            own_columns: parsed.own_columns,
            permissive,
        }
    }

    /// `sql` rewritten with `policies` on every reference of `docs`.
    fn rewrite(sql: &str, policies: Vec<InlinedPolicy>) -> CacheResult<String> {
        let tables = catalog();
        let bound = bind(sql, &tables);
        let predicates: HashMap<NodeId, ReferencePredicate> = secured_references(&bound)
            .into_iter()
            .map(|(reference, _)| {
                let policies = policies
                    .iter()
                    .map(|p| InlinedPolicy {
                        tree: p.tree.clone(),
                        own_columns: p.own_columns.clone(),
                        permissive: p.permissive,
                    })
                    .collect();
                let qualifier = reference_qualifier(bound.tree(), reference);
                let predicate =
                    ReferencePredicate::new(qualifier, policies).expect("a permissive policy");
                (reference, predicate)
            })
            .collect();
        Ok(rls_rewrite(&bound, &predicates)?.sql())
    }

    #[test]
    fn test_policy_is_anded_onto_the_where_under_the_reference_qualifier() {
        let tables = catalog();
        let tenant = policy("tenant = '7'", true, &tables);
        assert_eq!(
            rewrite("SELECT id FROM docs d WHERE id = '1'", vec![tenant]).expect("rewrites"),
            "SELECT id FROM docs d WHERE id = '1' AND d.tenant = '7'"
        );
        let tenant = policy("tenant = '7'", true, &tables);
        assert_eq!(
            rewrite("SELECT id FROM docs", vec![tenant]).expect("rewrites"),
            "SELECT id FROM docs WHERE docs.tenant = '7'"
        );
    }

    #[test]
    fn test_permissive_policies_or_and_restrictive_policies_and() {
        let tables = catalog();
        let rewritten = rewrite(
            "SELECT id FROM docs",
            vec![
                policy("tenant = '7'", true, &tables),
                policy("owner = 'alice'", true, &tables),
                policy("owner <> 'nobody'", false, &tables),
            ],
        )
        .expect("rewrites");
        assert_eq!(
            rewritten,
            "SELECT id FROM docs WHERE (docs.tenant = '7' OR docs.owner = 'alice') AND docs.owner != 'nobody'"
        );
    }

    #[test]
    fn test_policy_subquery_keeps_its_own_tables_and_requalifies_the_outer_column() {
        let tables = catalog();
        let membership = policy(
            "EXISTS (SELECT 1 FROM memberships m WHERE m.user_name = 'alice' AND m.tenant = docs.tenant)",
            true,
            &tables,
        );
        let rewritten = rewrite("SELECT id FROM docs d", vec![membership]).expect("rewrites");
        assert!(
            rewritten.contains("m.user_name = 'alice' AND m.tenant = d.tenant"),
            "{rewritten}"
        );
    }

    /// A source inside the policy spelled like the outer reference's alias
    /// would capture the requalified outer columns; it is renamed instead.
    #[test]
    fn test_policy_source_shadowing_the_reference_alias_is_renamed() {
        let tables = catalog();
        let membership = policy(
            "EXISTS (SELECT 1 FROM memberships m WHERE m.user_name = 'alice' AND m.tenant = docs.tenant)",
            true,
            &tables,
        );
        let rewritten = rewrite("SELECT id FROM docs m", vec![membership]).expect("rewrites");
        assert_eq!(
            rewritten,
            "SELECT id FROM docs m WHERE EXISTS (SELECT 1 FROM memberships __pgcache_rls \
             WHERE __pgcache_rls.user_name = 'alice' AND __pgcache_rls.tenant = m.tenant)"
        );
    }

    #[test]
    fn test_secured_reference_in_an_inner_join_takes_the_predicate_in_the_where() {
        let tables = catalog();
        let tenant = policy("tenant = '7'", true, &tables);
        let rewritten = rewrite(
            "SELECT d.id FROM memberships m JOIN docs d ON d.tenant = m.tenant WHERE m.user_name = 'a'",
            vec![tenant],
        )
        .expect("rewrites");
        assert!(
            rewritten.ends_with("WHERE m.user_name = 'a' AND d.tenant = '7'"),
            "{rewritten}"
        );
    }

    #[test]
    fn test_secured_reference_on_the_nullable_side_of_an_outer_join_is_refused() {
        let tables = catalog();
        let tenant = policy("tenant = '7'", true, &tables);
        let refused = rewrite(
            "SELECT m.user_name FROM memberships m LEFT JOIN docs d ON d.tenant = m.tenant",
            vec![tenant],
        );
        assert!(
            matches!(
                refused.as_ref().map_err(|e| e.current_context()),
                Err(CacheError::RlsUnsupported { .. })
            ),
            "{refused:?}"
        );
        // The preserved side takes it as usual.
        let tenant = policy("tenant = '7'", true, &tables);
        let rewritten = rewrite(
            "SELECT d.id FROM docs d LEFT JOIN memberships m ON m.tenant = d.tenant",
            vec![tenant],
        )
        .expect("rewrites");
        assert!(rewritten.ends_with("WHERE d.tenant = '7'"), "{rewritten}");
    }
}
