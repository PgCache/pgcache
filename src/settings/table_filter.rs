use std::fmt;

use ecow::EcoString;

/// One `allowed_tables` / `excluded_tables` entry, lowercased: `table`
/// names the table in any schema, `schema.table` in that schema alone.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TableEntry {
    schema: Option<EcoString>,
    name: EcoString,
}

impl TableEntry {
    fn parse(entry: &str) -> Self {
        let entry = entry.trim();
        match entry.rsplit_once('.') {
            Some((schema, name)) => Self {
                schema: Some(schema.to_lowercase().into()),
                name: name.to_lowercase().into(),
            },
            None => Self {
                schema: None,
                name: entry.to_lowercase().into(),
            },
        }
    }

    fn name_matches(&self, name: &str) -> bool {
        eq_lowercased(&self.name, name)
    }

    /// Whether every table this entry names is one `other` names too.
    fn covered_by(&self, other: &TableEntry) -> bool {
        self.name == other.name && (other.schema.is_none() || other.schema == self.schema)
    }
}

impl fmt::Display for TableEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.schema {
            Some(schema) => write!(f, "{schema}.{}", self.name),
            None => f.write_str(&self.name),
        }
    }
}

fn entries_parse(tables: Option<&[String]>) -> Vec<TableEntry> {
    tables
        .unwrap_or_default()
        .iter()
        .filter(|entry| !entry.trim().is_empty())
        .map(|entry| TableEntry::parse(entry))
        .collect()
}

/// Compares without allocating a lowercased copy of `raw`: the query-level
/// check runs on every cacheable request.
fn eq_lowercased(lowered: &str, raw: &str) -> bool {
    lowered.chars().eq(raw.chars().flat_map(char::to_lowercase))
}

/// What the table filter makes of a query's tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableVerdict {
    /// Every table is allowed and none is excluded.
    Cacheable,
    /// A table is outside `allowed_tables`.
    NotAllowed,
    /// A table is in `excluded_tables`; wins over `NotAllowed`.
    Excluded,
}

/// Which tables may be cached: the `allowed_tables` allowlist and the
/// `excluded_tables` blocklist, parsed for matching. A table is cacheable
/// when the allowlist admits it and the blocklist does not name it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableFilter {
    /// `None`: every table is allowed.
    allowed: Option<Vec<TableEntry>>,
    excluded: Vec<TableEntry>,
}

impl TableFilter {
    /// An empty or missing list is no list. An allowlist of blank entries
    /// alone is still an allowlist, one that allows nothing: a list that
    /// was meant to name tables must not come to allow them all.
    pub fn new(allowed: Option<&[String]>, excluded: Option<&[String]>) -> Self {
        Self {
            allowed: allowed
                .filter(|entries| !entries.is_empty())
                .map(|entries| entries_parse(Some(entries))),
            excluded: entries_parse(excluded),
        }
    }

    /// Whether `allowed_tables` restricts what is cached.
    pub fn has_allowlist(&self) -> bool {
        self.allowed.is_some()
    }

    /// Whether `excluded_tables` names any table.
    pub fn has_exclusions(&self) -> bool {
        !self.excluded.is_empty()
    }

    /// Whether the allowlist admits the table. A reference without a schema
    /// is admitted only by an entry without one: where it resolves is not
    /// known here, and an allowlist errs toward forwarding.
    pub fn allows(&self, schema: Option<&str>, name: &str) -> bool {
        let Some(entries) = &self.allowed else {
            return true;
        };
        entries.iter().any(|entry| {
            entry.name_matches(name)
                && match &entry.schema {
                    Some(entry_schema) => schema.is_some_and(|s| eq_lowercased(entry_schema, s)),
                    None => true,
                }
        })
    }

    /// Whether the blocklist names the table. A reference without a schema
    /// matches a schema-qualified entry of its name: it may resolve to that
    /// schema on the session's `search_path`, and a blocklist errs toward
    /// forwarding too.
    pub fn excludes(&self, schema: Option<&str>, name: &str) -> bool {
        self.excluded.iter().any(|entry| {
            entry.name_matches(name)
                && match (&entry.schema, schema) {
                    (Some(entry_schema), Some(schema)) => eq_lowercased(entry_schema, schema),
                    (None, _) | (_, None) => true,
                }
        })
    }

    /// The verdict on one table: a reference as a query writes it, or a
    /// table known by the schema it resolved to.
    pub fn table_verdict(&self, schema: Option<&str>, name: &str) -> TableVerdict {
        if self.excludes(schema, name) {
            TableVerdict::Excluded
        } else if self.allows(schema, name) {
            TableVerdict::Cacheable
        } else {
            TableVerdict::NotAllowed
        }
    }

    /// The verdict on a query from the table references it writes.
    pub fn references_verdict<'a>(
        &self,
        references: impl Iterator<Item = (Option<&'a str>, &'a str)>,
    ) -> TableVerdict {
        if self.allowed.is_none() && self.excluded.is_empty() {
            return TableVerdict::Cacheable;
        }
        let mut verdict = TableVerdict::Cacheable;
        for (schema, name) in references {
            match self.table_verdict(schema, name) {
                TableVerdict::Excluded => return TableVerdict::Excluded,
                TableVerdict::NotAllowed => verdict = TableVerdict::NotAllowed,
                TableVerdict::Cacheable => {}
            }
        }
        verdict
    }

    /// The `allowed_tables` entries an `excluded_tables` entry covers
    /// whole, which therefore never take effect.
    pub fn allowed_but_excluded(&self) -> Vec<String> {
        self.allowed
            .iter()
            .flatten()
            .filter(|allowed| self.excluded.iter().any(|e| allowed.covered_by(e)))
            .map(ToString::to_string)
            .collect()
    }

    /// Warn of `allowed_tables` entries that `excluded_tables` cancels.
    pub fn conflicts_warn(&self) {
        let conflicts = self.allowed_but_excluded();
        if !conflicts.is_empty() {
            tracing::warn!(
                "tables in both allowed_tables and excluded_tables are never cached: {}",
                conflicts.join(", ")
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filter(allowed: &[&str], excluded: &[&str]) -> TableFilter {
        let owned = |names: &[&str]| names.iter().map(|n| (*n).to_owned()).collect::<Vec<_>>();
        TableFilter::new(Some(&owned(allowed)), Some(&owned(excluded)))
    }

    #[test]
    fn test_empty_lists_are_no_lists() {
        let open = filter(&[], &[]);
        assert_eq!(open, TableFilter::new(None, None));
        assert!(!open.has_allowlist());
        assert!(!open.has_exclusions());
        assert!(open.allows(None, "anything"));
        assert!(!open.excludes(None, "anything"));
    }

    /// A list that held only blanks was meant to name tables: as an
    /// allowlist it allows nothing, as a blocklist it excludes nothing.
    #[test]
    fn test_blank_entries_name_no_table() {
        let blank = filter(&[" ", ""], &[""]);
        assert!(blank.has_allowlist());
        assert!(!blank.allows(None, "users"));
        assert!(!blank.allows(None, ""));
        assert!(!blank.has_exclusions());
        assert_eq!(
            blank.references_verdict([(None, "users")].into_iter()),
            TableVerdict::NotAllowed
        );
    }

    #[test]
    fn test_allows_matches_schema_and_case() {
        let f = filter(&["Users", "audit.events"], &[]);
        assert!(f.allows(None, "users"));
        assert!(f.allows(Some("other"), "USERS"));
        assert!(f.allows(Some("Audit"), "events"));
        assert!(!f.allows(Some("public"), "events"));
        assert!(!f.allows(None, "events"));
        assert!(!f.allows(None, "orders"));
    }

    #[test]
    fn test_excludes_matches_schema_and_case() {
        let f = filter(&[], &["Audit_Logs", "analytics.events"]);
        assert!(f.excludes(None, "audit_logs"));
        assert!(f.excludes(Some("any"), "AUDIT_LOGS"));
        assert!(f.excludes(Some("Analytics"), "events"));
        assert!(!f.excludes(Some("public"), "events"));
        assert!(!f.excludes(None, "orders"));
    }

    /// A bare `events` may be `analytics.events` on the session's path.
    #[test]
    fn test_excludes_unqualified_reference_of_a_qualified_entry() {
        let f = filter(&[], &["analytics.events"]);
        assert!(f.excludes(None, "events"));
    }

    #[test]
    fn test_references_verdict_exclusion_wins() {
        let f = filter(&["users", "events"], &["events"]);
        let verdict = |tables: &[(Option<&'static str>, &'static str)]| {
            f.references_verdict(tables.iter().copied())
        };
        assert_eq!(verdict(&[(None, "users")]), TableVerdict::Cacheable);
        assert_eq!(verdict(&[(None, "orders")]), TableVerdict::NotAllowed);
        assert_eq!(verdict(&[(None, "events")]), TableVerdict::Excluded);
        assert_eq!(
            verdict(&[(None, "orders"), (None, "users"), (None, "events")]),
            TableVerdict::Excluded
        );
        assert_eq!(verdict(&[]), TableVerdict::Cacheable);
    }

    #[test]
    fn test_table_verdict_on_resolved_names() {
        let f = filter(&["public.users", "events"], &["audit.events"]);
        let verdict = |schema, name| f.table_verdict(Some(schema), name);
        assert_eq!(verdict("public", "users"), TableVerdict::Cacheable);
        assert_eq!(verdict("other", "users"), TableVerdict::NotAllowed);
        assert_eq!(verdict("public", "events"), TableVerdict::Cacheable);
        assert_eq!(verdict("audit", "events"), TableVerdict::Excluded);
        assert_eq!(verdict("public", "orders"), TableVerdict::NotAllowed);
    }

    /// An allowed entry is reported when an exclusion covers every table
    /// it names, not when the exclusion only narrows it.
    #[test]
    fn test_allowed_but_excluded_reports_covered_entries() {
        let f = filter(
            &["Users", "public.orders", "audit.events", "jobs", "items"],
            &["users", "PUBLIC.orders", "events", "audit.jobs"],
        );
        assert_eq!(
            f.allowed_but_excluded(),
            ["users", "public.orders", "audit.events"]
        );
        assert!(filter(&["users"], &[]).allowed_but_excluded().is_empty());
        assert!(filter(&[], &["users"]).allowed_but_excluded().is_empty());
    }

    #[test]
    fn test_references_verdict_exclusions_alone() {
        let f = filter(&[], &["events"]);
        assert_eq!(
            f.references_verdict([(None, "users"), (Some("public"), "orders")].into_iter()),
            TableVerdict::Cacheable
        );
        assert_eq!(
            f.references_verdict([(None, "users"), (Some("public"), "events")].into_iter()),
            TableVerdict::Excluded
        );
    }
}
