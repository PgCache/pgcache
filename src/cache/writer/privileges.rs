//! The writer's half of the hit-path privilege check (PGC-463): one catalog
//! read per lookup, asking origin what `role` may read of each relation and
//! whether it may execute each function, recorded in the state view. Also
//! the role poll (PGC-548): role commands reach no event trigger and are
//! not decoded, so a digest of the role catalog is read every interval and
//! a change forgets every stored verdict.

use std::collections::HashSet;
use std::sync::Arc;

use ecow::EcoString;
use tokio_postgres::Client;
use tracing::debug;

use super::core::WriterCore;
use crate::cache::privileges::{
    CatalogSnapshot, FunctionVerdict, PrivilegeCheck, PrivilegeLookup, PrivilegeResults,
    RelationVerdict,
};
use crate::cache::{CacheError, CacheResult, Report};
use crate::oid::Oid;
use crate::query::resolve::PrivilegeNeeds;
use crate::result::{MapIntoReport, ReportExt};

/// Per relation: `USAGE` on its schema, `SELECT` on the table, and the
/// columns `SELECT` is granted on (table-level `SELECT` grants them all).
const RELATIONS_SQL: &str = "SELECT c.oid, \
            has_schema_privilege($1::text, c.relnamespace, 'USAGE'), \
            has_table_privilege($1::text, c.oid, 'SELECT'), \
            (SELECT array_agg(a.attname::text) FROM pg_attribute a \
              WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped \
                AND has_column_privilege($1::text, c.oid, a.attnum, 'SELECT')) \
     FROM pg_class c WHERE c.oid = ANY($2::oid[])";

/// Per name: `EXECUTE` on every overload in any schema, as the volatility
/// map takes the worst case over overloads. A name with no function is
/// left to origin.
const FUNCTIONS_SQL: &str = "SELECT n.name, \
            coalesce(bool_and(has_function_privilege($1::text, p.oid, 'EXECUTE')), true) \
     FROM unnest($2::text[]) AS n(name) LEFT JOIN pg_proc p ON p.proname = n.name \
     GROUP BY n.name";

/// The snapshot the catalog reads that follow run under (a later statement
/// sees at least what this one does), and whether the role exists.
const SNAPSHOT_SQL: &str = "SELECT pg_current_snapshot()::text, \
            EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1::text)";

/// A digest of everything about roles that a privilege verdict depends on
/// and no DDL signal reports: each role's identity and the attributes that
/// decide inheritance, superuser and RLS bypass, plus every membership row.
/// Public views only, so pgcache's role needs no privilege to read them. The
/// snapshot is the one the digest's reads ran under.
const ROLES_DIGEST_SQL: &str = "SELECT md5(string_agg(entry, ',' ORDER BY entry)), \
            pg_current_snapshot()::text \
     FROM (SELECT 'r' || oid::text || ':' || rolname || ':' \
                  || rolsuper::text || rolinherit::text || rolbypassrls::text \
             FROM pg_roles \
           UNION ALL \
           SELECT 'm' || m::text FROM pg_auth_members m) AS entries(entry)";

/// The role catalog's digest and the snapshot it was read under.
pub(super) struct RolesDigest {
    pub(super) digest: EcoString,
    pub(super) snapshot: CatalogSnapshot,
}

/// Read the role catalog's digest on `client`.
pub(super) async fn roles_digest_read(client: &Client) -> CacheResult<RolesDigest> {
    let row = client
        .query_one(ROLES_DIGEST_SQL, &[])
        .await
        .map_into_report::<CacheError>()
        .attach_loc("reading the role catalog digest")?;
    let digest: &str = row.try_get(0).map_into_report::<CacheError>()?;
    let snapshot_text: &str = row.try_get(1).map_into_report::<CacheError>()?;
    let snapshot = CatalogSnapshot::parse(snapshot_text).ok_or_else(|| {
        Report::from(CacheError::PrivilegeSnapshot {
            text: snapshot_text.to_owned(),
        })
    })?;
    Ok(RolesDigest {
        digest: EcoString::from(digest),
        snapshot,
    })
}

impl WriterCore {
    /// One poll of the role catalog: a digest unlike the last one means a
    /// role, an attribute or a membership changed, and every stored verdict
    /// is forgotten. A poll that cannot be read is treated as a change, so
    /// an unobservable interval never serves on verdicts it might have
    /// stale; the digest kept is the last one read.
    pub(super) async fn roles_poll(&mut self) {
        let Some(last) = self.roles_digest.as_ref() else {
            return;
        };
        let snapshot = match roles_digest_read(&self.db_origin).await {
            Ok(read) if read.digest == *last => return,
            Ok(read) => {
                debug!("role catalog changed, privilege verdicts forgotten");
                self.roles_digest = Some(read.digest);
                Some(read.snapshot)
            }
            Err(e) => {
                debug!(
                    "role catalog poll failed, privilege verdicts forgotten: {}",
                    crate::result::error_chain_format(e.current_context())
                );
                None
            }
        };
        self.state_view.privileges.roles_changed(snapshot);
        crate::metrics::handles().cache.role_changes.increment(1);
    }

    /// Fetch the verdicts `role` lacks for `needs`, if any, before the
    /// query they belong to is published Ready.
    pub(super) async fn privilege_prefetch(&self, role: &EcoString, needs: &PrivilegeNeeds) {
        if let PrivilegeCheck::Pending(lookup) = self.state_view.privileges.check(role, needs)
            && !lookup.is_empty()
        {
            self.privilege_lookup(&lookup).await;
        }
    }

    /// Answer `lookup` from origin's catalog and record the verdicts. A
    /// failed read clears the pending marks so the next hit asks again;
    /// it is not an error of the writer.
    pub(super) async fn privilege_lookup(&self, lookup: &PrivilegeLookup) {
        match self.privilege_results_read(lookup).await {
            Ok(results) => self.state_view.privileges.apply(lookup, results),
            Err(e) => {
                debug!(
                    "privilege lookup for a role failed, hits forward until retried: {}",
                    crate::result::error_chain_format(e.current_context())
                );
                self.state_view.privileges.pending_clear(lookup);
            }
        }
    }

    async fn privilege_results_read(
        &self,
        lookup: &PrivilegeLookup,
    ) -> CacheResult<PrivilegeResults> {
        let row = self
            .db_origin
            .query_one(SNAPSHOT_SQL, &[&lookup.role.as_str()])
            .await
            .map_into_report::<CacheError>()
            .attach_loc("reading the catalog snapshot")?;
        let snapshot_text: &str = row.try_get(0).map_into_report::<CacheError>()?;
        let exists: bool = row.try_get(1).map_into_report::<CacheError>()?;
        let snapshot = CatalogSnapshot::parse(snapshot_text).ok_or_else(|| {
            Report::from(CacheError::PrivilegeSnapshot {
                text: snapshot_text.to_owned(),
            })
        })?;
        if !exists {
            return Ok(denied_all(lookup, snapshot));
        }
        Ok(PrivilegeResults {
            snapshot,
            relations: self
                .relation_verdicts_read(&lookup.role, &lookup.relations)
                .await?,
            functions: self
                .function_verdicts_read(&lookup.role, &lookup.functions)
                .await?,
        })
    }

    async fn relation_verdicts_read(
        &self,
        role: &EcoString,
        relations: &[Oid],
    ) -> CacheResult<Vec<(Oid, RelationVerdict)>> {
        if relations.is_empty() {
            return Ok(Vec::new());
        }
        let rows = self
            .db_origin
            .query(RELATIONS_SQL, &[&role.as_str(), &relations])
            .await
            .map_into_report::<CacheError>()
            .attach_loc("reading relation privileges")?;
        let mut verdicts = Vec::with_capacity(relations.len());
        for row in &rows {
            let oid: Oid = row.try_get(0).map_into_report::<CacheError>()?;
            let schema: bool = row.try_get(1).map_into_report::<CacheError>()?;
            let table: bool = row.try_get(2).map_into_report::<CacheError>()?;
            let columns: Option<Vec<String>> = row.try_get(3).map_into_report::<CacheError>()?;
            verdicts.push((oid, relation_verdict(schema, table, columns)));
        }
        // A relation gone from the catalog: its queries are being evicted;
        // until then, forward.
        let seen: HashSet<Oid> = verdicts.iter().map(|(oid, _)| *oid).collect();
        verdicts.extend(
            relations
                .iter()
                .filter(|oid| !seen.contains(oid))
                .map(|oid| (*oid, RelationVerdict::Denied)),
        );
        Ok(verdicts)
    }

    async fn function_verdicts_read(
        &self,
        role: &EcoString,
        functions: &[EcoString],
    ) -> CacheResult<Vec<(EcoString, FunctionVerdict)>> {
        if functions.is_empty() {
            return Ok(Vec::new());
        }
        let names: Vec<&str> = functions.iter().map(EcoString::as_str).collect();
        let rows = self
            .db_origin
            .query(FUNCTIONS_SQL, &[&role.as_str(), &names])
            .await
            .map_into_report::<CacheError>()
            .attach_loc("reading function privileges")?;
        let mut verdicts = Vec::with_capacity(rows.len());
        for row in &rows {
            let name: &str = row.try_get(0).map_into_report::<CacheError>()?;
            let allowed: bool = row.try_get(1).map_into_report::<CacheError>()?;
            let verdict = if allowed {
                FunctionVerdict::Allowed
            } else {
                FunctionVerdict::Denied
            };
            verdicts.push((EcoString::from(name), verdict));
        }
        Ok(verdicts)
    }
}

/// A role that does not exist reads nothing.
fn denied_all(lookup: &PrivilegeLookup, snapshot: CatalogSnapshot) -> PrivilegeResults {
    PrivilegeResults {
        snapshot,
        relations: lookup
            .relations
            .iter()
            .map(|oid| (*oid, RelationVerdict::Denied))
            .collect(),
        functions: lookup
            .functions
            .iter()
            .map(|name| (name.clone(), FunctionVerdict::Denied))
            .collect(),
    }
}

/// One relation's verdict from its three catalog answers.
fn relation_verdict(schema: bool, table: bool, columns: Option<Vec<String>>) -> RelationVerdict {
    match (schema, table, columns) {
        (false, _, _) => RelationVerdict::Denied,
        (true, true, _) => RelationVerdict::Full,
        (true, false, Some(columns)) if !columns.is_empty() => {
            RelationVerdict::Columns(Arc::new(columns.into_iter().map(EcoString::from).collect()))
        }
        (true, false, _) => RelationVerdict::Denied,
    }
}
