//! The tables pgcache would cache: ownership, primary keys, views and RLS.

use tokio_postgres::{Client, Error};

use super::CheckResult;
use crate::settings::Allowlist;

const NAMES_SHOWN: usize = 5;

fn names_list(names: &[String]) -> String {
    let shown = names.iter().take(NAMES_SHOWN).cloned().collect::<Vec<_>>();
    let rest = names.len().saturating_sub(NAMES_SHOWN);
    if rest > 0 {
        format!("{} and {rest} more", shown.join(", "))
    } else {
        shown.join(", ")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RelationKind {
    Table,
    View,
    MaterializedView,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Relation {
    pub schema: String,
    pub name: String,
    pub kind: RelationKind,
    pub owned: bool,
    pub has_primary_key: bool,
    pub row_security: bool,
}

impl Relation {
    fn qualified_name(&self) -> String {
        format!("{}.{}", self.schema, self.name)
    }
}

fn relation_allowed(allowlist: &Allowlist, schema: &str, name: &str) -> bool {
    let Some(entries) = allowlist else {
        return true;
    };
    entries.iter().any(|(entry_schema, entry_name)| {
        entry_name.eq_ignore_ascii_case(name)
            && entry_schema
                .as_deref()
                .is_none_or(|s| s.eq_ignore_ascii_case(schema))
    })
}

/// Load every user relation, scoped to the allowlist when one is set, since
/// a relation pgcache will never touch should not produce a finding.
/// Extension-owned relations (pg_stat_statements views etc.) are skipped.
pub(super) async fn relations_load(
    client: &Client,
    allowlist: &Allowlist,
) -> Result<Vec<Relation>, Error> {
    let rows = client
        .query(
            "SELECT n.nspname::text, c.relname::text, c.relkind::text, \
                    pg_get_userbyid(c.relowner) = current_user, \
                    EXISTS (SELECT 1 FROM pg_constraint k \
                             WHERE k.conrelid = c.oid AND k.contype = 'p'), \
                    c.relrowsecurity \
             FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relkind IN ('r', 'p', 'v', 'm') AND NOT c.relispartition \
               AND n.nspname NOT IN ('pg_catalog', 'information_schema') \
               AND n.nspname NOT LIKE 'pg\\_%' \
               AND NOT EXISTS (SELECT 1 FROM pg_depend d \
                                WHERE d.classid = 'pg_class'::regclass \
                                  AND d.objid = c.oid AND d.deptype = 'e') \
             ORDER BY 1, 2",
            &[],
        )
        .await?;
    let mut relations = Vec::with_capacity(rows.len());
    for row in rows {
        let schema: String = row.try_get(0)?;
        let name: String = row.try_get(1)?;
        if !relation_allowed(allowlist, &schema, &name) {
            continue;
        }
        let kind = match row.try_get::<_, String>(2)?.as_str() {
            "v" => RelationKind::View,
            "m" => RelationKind::MaterializedView,
            _ => RelationKind::Table,
        };
        relations.push(Relation {
            schema,
            name,
            kind,
            owned: row.try_get(3)?,
            has_primary_key: row.try_get(4)?,
            row_security: row.try_get(5)?,
        });
    }
    Ok(relations)
}

fn tables(relations: &[Relation]) -> impl Iterator<Item = &Relation> {
    relations.iter().filter(|r| r.kind == RelationKind::Table)
}

pub(super) fn table_ownership_evaluate(
    relations: &[Relation],
    superuser: bool,
    role_name: &str,
) -> CheckResult {
    let name = "table ownership";
    if superuser {
        return CheckResult::pass(name, "superuser; any table can join the publication");
    }
    let total = tables(relations).count();
    let unowned = tables(relations)
        .filter(|r| !r.owned)
        .map(Relation::qualified_name)
        .collect::<Vec<_>>();
    if unowned.is_empty() {
        return CheckResult::pass(name, format!("{role_name} owns all {total} tables"));
    }
    CheckResult::warn(
        name,
        format!(
            "{} of {total} tables are not owned by {role_name}, so pgcache cannot add them to \
             its publication and forwards queries on them: {}",
            unowned.len(),
            names_list(&unowned)
        ),
        format!(
            "ALTER TABLE <table> OWNER TO \"{role_name}\"; for each, or run pgcache as the \
             tables' owner."
        ),
    )
}

pub(super) fn primary_keys_evaluate(relations: &[Relation]) -> CheckResult {
    let name = "primary keys";
    let total = tables(relations).count();
    if total == 0 {
        return CheckResult::warn(
            name,
            "no tables found",
            "pgcache caches queries on tables; this database (or the allowlisted set) has none.",
        );
    }
    let missing = tables(relations)
        .filter(|r| !r.has_primary_key)
        .map(Relation::qualified_name)
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return CheckResult::pass(name, format!("all {total} tables have a primary key"));
    }
    CheckResult::warn(
        name,
        format!(
            "{} of {total} tables have no primary key and are forwarded, never cached: {}",
            missing.len(),
            names_list(&missing)
        ),
        "Add one: ALTER TABLE <table> ADD PRIMARY KEY (<column>);",
    )
}

pub(super) fn views_evaluate(relations: &[Relation]) -> CheckResult {
    let name = "views";
    let views = relations
        .iter()
        .filter(|r| r.kind != RelationKind::Table)
        .map(Relation::qualified_name)
        .collect::<Vec<_>>();
    if views.is_empty() {
        return CheckResult::pass(name, "none");
    }
    CheckResult::warn(
        name,
        format!(
            "{} views; queries that reference a view are forwarded, never cached: {}",
            views.len(),
            names_list(&views)
        ),
        "View support is not available yet. Query the underlying tables directly where you \
         want caching.",
    )
}

pub(super) fn row_level_security_evaluate(relations: &[Relation]) -> CheckResult {
    let name = "row-level security";
    let secured = tables(relations)
        .filter(|r| r.row_security)
        .map(Relation::qualified_name)
        .collect::<Vec<_>>();
    if secured.is_empty() {
        return CheckResult::pass(name, "no tables have row-level security enabled");
    }
    CheckResult::fail(
        name,
        format!(
            "{} tables have row-level security enabled, which pgcache does not support yet: {}",
            secured.len(),
            names_list(&secured)
        ),
        "pgcache populates the cache as its own role and would serve those rows to every client, \
         bypassing the policies. Exclude these tables with ALLOWED_TABLES (--allowed_tables), \
         listing only the tables to cache.",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::preflight::Verdict;

    fn relation(name: &str, kind: RelationKind) -> Relation {
        Relation {
            schema: "public".to_owned(),
            name: name.to_owned(),
            kind,
            owned: true,
            has_primary_key: true,
            row_security: false,
        }
    }

    #[test]
    fn test_primary_keys_evaluate_names_tables_without_one() {
        let mut logs = relation("logs", RelationKind::Table);
        logs.has_primary_key = false;
        let relations = vec![relation("users", RelationKind::Table), logs];
        let result = primary_keys_evaluate(&relations);
        assert_eq!(result.verdict, Verdict::Warn);
        assert!(result.finding.contains("1 of 2 tables"));
        assert!(result.finding.contains("public.logs"));
        assert_eq!(primary_keys_evaluate(&[]).verdict, Verdict::Warn);
        assert_eq!(
            primary_keys_evaluate(&relations[..1]).verdict,
            Verdict::Pass
        );
    }

    #[test]
    fn test_views_evaluate_counts_views_and_matviews() {
        let relations = vec![
            relation("users", RelationKind::Table),
            relation("active_users", RelationKind::View),
            relation("daily_totals", RelationKind::MaterializedView),
        ];
        let result = views_evaluate(&relations);
        assert_eq!(result.verdict, Verdict::Warn);
        assert!(result.finding.starts_with("2 views"));
        assert_eq!(views_evaluate(&relations[..1]).verdict, Verdict::Pass);
    }

    #[test]
    fn test_row_level_security_evaluate_fails_on_secured_table() {
        let mut tenants = relation("tenants", RelationKind::Table);
        tenants.row_security = true;
        let relations = vec![relation("users", RelationKind::Table), tenants];
        let result = row_level_security_evaluate(&relations);
        assert_eq!(result.verdict, Verdict::Fail);
        assert!(result.finding.contains("public.tenants"));
        assert_eq!(
            row_level_security_evaluate(&relations[..1]).verdict,
            Verdict::Pass
        );
    }

    #[test]
    fn test_table_ownership_evaluate_superuser_passes() {
        let mut orders = relation("orders", RelationKind::Table);
        orders.owned = false;
        let relations = vec![relation("users", RelationKind::Table), orders];
        assert_eq!(
            table_ownership_evaluate(&relations, true, "app").verdict,
            Verdict::Pass
        );
        let result = table_ownership_evaluate(&relations, false, "app");
        assert_eq!(result.verdict, Verdict::Warn);
        assert!(result.finding.contains("public.orders"));
    }

    #[test]
    fn test_relation_allowed_matches_schema_qualified_and_bare_entries() {
        let allowlist: Allowlist = Some(vec![
            (None, "users".to_owned()),
            (Some("audit".to_owned()), "events".to_owned()),
        ]);
        assert!(relation_allowed(&allowlist, "public", "users"));
        assert!(relation_allowed(&allowlist, "other", "Users"));
        assert!(relation_allowed(&allowlist, "audit", "events"));
        assert!(!relation_allowed(&allowlist, "public", "events"));
        assert!(relation_allowed(&None, "public", "anything"));
    }

    #[test]
    fn test_names_list_truncates_after_five() {
        let names = (1..=7).map(|i| format!("t{i}")).collect::<Vec<_>>();
        assert_eq!(names_list(&names), "t1, t2, t3, t4, t5 and 2 more");
        assert_eq!(names_list(&names[..2]), "t1, t2");
    }
}
