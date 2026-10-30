//! The same-shape join parents (ADR-024, PGC-417): every cached
//! single-branch join whose WHERE the model read whole, indexed by its shape
//! over its WHERE region, so a join of the same shape under a WHERE that
//! implies it is served from the rows the parent loaded. Like the
//! per-relation subsumption index, only a LIMIT-free parent is indexed;
//! readiness is checked per lookup.

use std::collections::HashMap;

use crate::query::constraint_index::ConstraintIndex;
use crate::query::constraints::TableRanges;
use crate::query::join_region::{JoinRegion, JoinShape};
use crate::query::{Fingerprint, FingerprintMap};

/// A registered parent's region, with the shape it is indexed under.
#[derive(Debug)]
struct JoinParent {
    shape: JoinShape,
    /// As the parent's WHERE reads, for the precise coverage check.
    ranges: TableRanges,
    /// In their key forms, for the shape index (PGC-478).
    keyed_ranges: TableRanges,
    has_limit: bool,
}

/// One candidate of a shape at lookup time.
pub struct JoinCandidate<'a> {
    pub region: &'a TableRanges,
    pub has_limit: bool,
}

#[derive(Debug, Default)]
pub struct JoinSubsumers {
    by_shape: HashMap<JoinShape, ConstraintIndex<Fingerprint>>,
    parents: FingerprintMap<JoinParent>,
}

impl JoinSubsumers {
    /// Register `fingerprint` as a parent of `region`'s shape, replacing any
    /// registration it had. Indexed only while it has no LIMIT.
    pub fn fingerprint_insert(
        &mut self,
        fingerprint: Fingerprint,
        region: &JoinRegion,
        has_limit: bool,
    ) {
        self.fingerprint_remove(fingerprint);
        let parent = JoinParent {
            shape: region.shape,
            ranges: region.ranges.clone(),
            keyed_ranges: region.keyed_ranges.clone(),
            has_limit,
        };
        if !has_limit {
            self.by_shape
                .entry(region.shape)
                .or_default()
                .insert(fingerprint, &parent.keyed_ranges);
        }
        self.parents.insert(fingerprint, parent);
    }

    /// Drop `fingerprint`'s registration, if any.
    pub fn fingerprint_remove(&mut self, fingerprint: Fingerprint) {
        let Some(parent) = self.parents.remove(&fingerprint) else {
            return;
        };
        if let Some(index) = self.by_shape.get_mut(&parent.shape) {
            index.remove(fingerprint);
            if index.is_empty() {
                self.by_shape.remove(&parent.shape);
            }
        }
    }

    /// Set whether `fingerprint`'s query has a LIMIT, which takes it out of
    /// the index or puts it back.
    pub fn has_limit_set(&mut self, fingerprint: Fingerprint, has_limit: bool) {
        let Some(parent) = self.parents.get_mut(&fingerprint) else {
            return;
        };
        parent.has_limit = has_limit;
        if has_limit {
            if let Some(index) = self.by_shape.get_mut(&parent.shape) {
                index.remove(fingerprint);
                if index.is_empty() {
                    self.by_shape.remove(&parent.shape);
                }
            }
        } else {
            self.by_shape
                .entry(parent.shape)
                .or_default()
                .insert(fingerprint, &parent.keyed_ranges);
        }
    }

    /// The parents of `shape` whose region might hold every row `keyed`
    /// admits, `keyed` being the probe's ranges in their key forms.
    pub fn candidates<'a>(
        &'a self,
        shape: JoinShape,
        keyed: &TableRanges,
    ) -> impl Iterator<Item = (Fingerprint, JoinCandidate<'a>)> + use<'a> {
        let found: Vec<Fingerprint> = self
            .by_shape
            .get(&shape)
            .into_iter()
            .flat_map(|index| index.candidates(keyed))
            .collect();
        found.into_iter().filter_map(move |fingerprint| {
            let parent = self.parents.get(&fingerprint)?;
            Some((
                fingerprint,
                JoinCandidate {
                    region: &parent.ranges,
                    has_limit: parent.has_limit,
                },
            ))
        })
    }

    pub fn is_empty(&self) -> bool {
        self.parents.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::Catalog;
    use iddqd::BiHashMap;
    use postgres_types::Type;

    use super::*;
    use crate::catalog::{ColumnMetadata, ColumnPosition, TableMetadata};
    use crate::oid::TypeOid;
    use crate::query::join_region::query_join_region;
    use crate::query::resolve::ResolvedQuery;

    fn catalog() -> Catalog {
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
        for (oid, name) in (1001..).zip(["a", "b"]) {
            tables.insert_overwrite(
                TableMetadata::for_test(name, oid, [int_column("id", 1), int_column("x", 2)])
                    .into(),
            );
        }
        tables
    }

    fn region(tables: &Catalog, sql: &str) -> JoinRegion {
        let bound = ResolvedQuery::parse_resolve(sql, tables, &["public"]).expect("resolve");
        query_join_region(&bound).expect("a join region").clone()
    }

    /// The fingerprints offered to a probe of `sql`.
    fn offered(subsumers: &JoinSubsumers, tables: &Catalog, sql: &str) -> Vec<u64> {
        let probe = region(tables, sql);
        let mut found: Vec<u64> = subsumers
            .candidates(probe.shape, &probe.keyed_ranges)
            .map(|(fingerprint, _)| fingerprint.get())
            .collect();
        found.sort_unstable();
        found
    }

    /// A parent is offered to a probe of its shape under its region, and
    /// not to one of another shape; a LIMIT takes it out and back, and
    /// removal clears it.
    #[test]
    fn test_join_subsumers_by_shape_and_limit() {
        const JOIN: &str = "SELECT * FROM a JOIN b ON b.id = a.id";
        let tables = catalog();
        let mut subsumers = JoinSubsumers::default();
        let wide = Fingerprint::from_raw(1);
        let narrow = Fingerprint::from_raw(2);
        subsumers.fingerprint_insert(wide, &region(&tables, JOIN), false);
        subsumers.fingerprint_insert(
            narrow,
            &region(&tables, &format!("{JOIN} WHERE a.x = 1")),
            false,
        );
        let child = format!("{JOIN} WHERE a.x = 1 AND b.x = 2");
        assert_eq!(offered(&subsumers, &tables, &child), [1, 2]);
        assert_eq!(
            offered(&subsumers, &tables, &format!("{JOIN} WHERE b.x = 2")),
            [1]
        );
        assert!(
            offered(
                &subsumers,
                &tables,
                "SELECT * FROM a LEFT JOIN b ON b.id = a.id WHERE a.x = 1"
            )
            .is_empty(),
            "another shape"
        );

        subsumers.has_limit_set(wide, true);
        assert_eq!(offered(&subsumers, &tables, &child), [2]);
        subsumers.has_limit_set(wide, false);
        assert_eq!(offered(&subsumers, &tables, &child), [1, 2]);

        subsumers.fingerprint_remove(narrow);
        assert_eq!(offered(&subsumers, &tables, &child), [1]);
        subsumers.fingerprint_remove(wide);
        assert!(subsumers.is_empty());
        assert!(subsumers.by_shape.is_empty(), "an emptied shape is dropped");
    }
}
