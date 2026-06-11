//! Shared group-space geometry, used by both scenarios.
//!
//! Group-id space (disjoint, so the invariants stay well-defined):
//! - **Normal groups** `0..groups` — targets of the single-row version bump,
//!   delete, PK-update, and (two-table) item-insert ops.
//! - **Paired groups** at [`PAIR_GROUP_BASE`]`..` — only ever bumped two-at-a-
//!   time in one transaction, so a pair must always show equal versions; never
//!   targeted by row mutations, so their queries stay stable.
//!
//! Every item `id` (the PK) comes from the `stress_pk` sequence — seed rows,
//! inserts, and the PK-update target alike — so ids are globally unique and
//! never reused, which makes the PK-update op collision-free without range
//! juggling.

/// Item table (single-table: holds `version`; two-table: joins to groups).
pub const TABLE: &str = "stress_items";

/// Groups table (two-table scenario only).
pub const GROUPS_TABLE: &str = "stress_groups";

/// Upper bound for the random `data` column. The cross-group read probe
/// selects the lower half of this range.
pub const DATA_MAX: i32 = 1_000_000;

/// Seed expression for the TOASTed `payload` column (PGC-264): ~3.2KB of
/// random text, out-of-line under `STORAGE EXTERNAL` (no compression past the
/// ~2KB threshold). The write mix never touches it, so every UPDATE of a
/// seeded row elides it from the CDC image as unchanged-toast.
pub const PAYLOAD_EXPR: &str = "repeat(md5(random()::text), 100)";

/// First group id reserved for paired groups.
pub const PAIR_GROUP_BASE: i32 = 1_000_000;

/// Paired-group ids: pair `i` is `(PAIR_GROUP_BASE + 2i, PAIR_GROUP_BASE + 2i + 1)`.
pub fn pair_groups(pairs: i32) -> Vec<(i32, i32)> {
    (0..pairs)
        .map(|i| (PAIR_GROUP_BASE + 2 * i, PAIR_GROUP_BASE + 2 * i + 1))
        .collect()
}

/// The seeded group-id layout, shared by the workload and the checks.
#[derive(Debug, Clone)]
pub struct Model {
    /// Normal groups occupy `0..groups`.
    pub groups: i32,
    /// Paired groups, always bumped together.
    pub pairs: Vec<(i32, i32)>,
}

impl Model {
    pub fn new(groups: i32, pairs: i32) -> Self {
        Self {
            groups,
            pairs: pair_groups(pairs),
        }
    }

    /// Every seeded group id: normal groups followed by each paired group.
    pub fn all_groups(&self) -> Vec<i32> {
        (0..self.groups)
            .chain(self.pairs.iter().flat_map(|&(a, b)| [a, b]))
            .collect()
    }
}
