//! The two schema scenarios and all the SQL that differs between them.
//!
//! - **Single-table**: one `stress_items(id, group_id, version, data)`; the
//!   version lives on the row. Single-table queries are maintained in-place and
//!   never invalidated, so each query populates exactly once — the
//!   population/CDC race only has a window at first touch.
//! - **Two-table**: `stress_groups(group_id, version)` + `stress_items(id,
//!   group_id, data)`; the oracle reads the join. Inserting an item *grows* the
//!   per-group join result, which invalidates that query, so it repopulates
//!   throughout the run — the race fires continuously, not just at first touch.
//!
//! Both keep the group-version invariant by construction: bumps only touch the
//! version (the row, or the groups row); item insert/delete/pk-update never
//! touch it, so all items of a group always share the group's version.

use anyhow::{Context, Result};
use clap::ValueEnum;
use tokio_postgres::Client;

use crate::schema::{DATA_MAX, GROUPS_TABLE, Model, PAIR_GROUP_BASE, TABLE};

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Variant {
    /// One table; version on the row.
    SingleTable,
    /// Groups + items; version reached through a join.
    TwoTable,
}

#[derive(Clone)]
pub struct Scenario {
    pub variant: Variant,
    pub model: Model,
}

impl Scenario {
    pub fn new(variant: Variant, groups: i32, pairs: i32) -> Self {
        Self {
            variant,
            model: Model::new(groups, pairs),
        }
    }

    /// Create the schema and seed every normal and paired group with
    /// `rows_per_group` items at version 0. Run against origin directly.
    pub async fn provision(&self, client: &Client, rows_per_group: i32) -> Result<()> {
        let last_pair = PAIR_GROUP_BASE + 2 * self.model.pairs.len() as i32 - 1;
        let groups = self.model.groups;
        let data_expr = format!("(random() * {DATA_MAX})::int");
        match self.variant {
            Variant::SingleTable => {
                let ddl = format!(
                    "DROP TABLE IF EXISTS {TABLE};
                     DROP SEQUENCE IF EXISTS stress_pk;
                     DROP SEQUENCE IF EXISTS stress_group;
                     CREATE SEQUENCE stress_pk;
                     CREATE SEQUENCE stress_group START 2000000;
                     CREATE TABLE {TABLE} (
                         id       int PRIMARY KEY DEFAULT nextval('stress_pk'),
                         group_id int NOT NULL,
                         version  int NOT NULL,
                         data     int NOT NULL
                     );"
                );
                client.batch_execute(&ddl).await.context("provision single")?;
                seed_groups_rows(
                    client,
                    TABLE,
                    "group_id, version, data",
                    &format!("g, 0, {data_expr}"),
                    groups,
                    last_pair,
                    rows_per_group,
                )
                .await?;
            }
            Variant::TwoTable => {
                let ddl = format!(
                    "DROP TABLE IF EXISTS {TABLE};
                     DROP TABLE IF EXISTS {GROUPS_TABLE};
                     DROP SEQUENCE IF EXISTS stress_pk;
                     CREATE SEQUENCE stress_pk;
                     CREATE TABLE {GROUPS_TABLE} (
                         group_id int PRIMARY KEY,
                         version  int NOT NULL
                     );
                     CREATE TABLE {TABLE} (
                         id       int PRIMARY KEY DEFAULT nextval('stress_pk'),
                         group_id int NOT NULL REFERENCES {GROUPS_TABLE}(group_id),
                         data     int NOT NULL
                     );"
                );
                client.batch_execute(&ddl).await.context("provision two")?;
                // Groups first (FK target), then items.
                let seed_groups = format!(
                    "INSERT INTO {GROUPS_TABLE} (group_id, version)
                     SELECT g, 0 FROM generate_series(0, {} - 1) g;
                     INSERT INTO {GROUPS_TABLE} (group_id, version)
                     SELECT g, 0 FROM generate_series({PAIR_GROUP_BASE}, {last_pair}) g;",
                    groups
                );
                client
                    .batch_execute(&seed_groups)
                    .await
                    .context("seed groups")?;
                seed_groups_rows(
                    client,
                    TABLE,
                    "group_id, data",
                    &format!("g, {data_expr}"),
                    groups,
                    last_pair,
                    rows_per_group,
                )
                .await?;
            }
        }
        Ok(())
    }

    /// `WHERE group_id = $1` → `(id, group_id, version, data)`.
    pub fn per_group_select(&self) -> String {
        match self.variant {
            Variant::SingleTable => {
                format!("SELECT id, group_id, version, data FROM {TABLE} WHERE group_id = $1")
            }
            Variant::TwoTable => format!(
                "SELECT i.id, i.group_id, g.version, i.data \
                 FROM {TABLE} i JOIN {GROUPS_TABLE} g ON i.group_id = g.group_id \
                 WHERE i.group_id = $1"
            ),
        }
    }

    /// `WHERE group_id = ANY($1)` → `(group_id, version)`, one atomic snapshot.
    pub fn pair_select(&self) -> String {
        match self.variant {
            Variant::SingleTable => {
                format!("SELECT group_id, version FROM {TABLE} WHERE group_id = ANY($1)")
            }
            Variant::TwoTable => format!(
                "SELECT i.group_id, g.version \
                 FROM {TABLE} i JOIN {GROUPS_TABLE} g ON i.group_id = g.group_id \
                 WHERE i.group_id = ANY($1)"
            ),
        }
    }

    /// Predicated probe over the lower-half `data` range → `(group_id, version)`.
    pub fn cross_group_select(&self, data_hi: i32) -> String {
        match self.variant {
            Variant::SingleTable => format!(
                "SELECT group_id, version FROM {TABLE} WHERE data BETWEEN 0 AND {data_hi}"
            ),
            Variant::TwoTable => format!(
                "SELECT i.group_id, g.version \
                 FROM {TABLE} i JOIN {GROUPS_TABLE} g ON i.group_id = g.group_id \
                 WHERE i.data BETWEEN 0 AND {data_hi}"
            ),
        }
    }

    /// Whole-table read ordered by item id → `(id, group_id, version, data)`.
    pub fn full_table_select(&self) -> String {
        match self.variant {
            Variant::SingleTable => {
                format!("SELECT id, group_id, version, data FROM {TABLE} ORDER BY id")
            }
            Variant::TwoTable => format!(
                "SELECT i.id, i.group_id, g.version, i.data \
                 FROM {TABLE} i JOIN {GROUPS_TABLE} g ON i.group_id = g.group_id ORDER BY i.id"
            ),
        }
    }

    /// Version bump, `$1 = group_id[]` (one group, or N for a fat frame). One
    /// statement / one transaction regardless of N, so it stays atomic per
    /// group and the invariant holds.
    pub fn version_bump(&self) -> String {
        match self.variant {
            Variant::SingleTable => {
                format!("UPDATE {TABLE} SET version = version + 1 WHERE group_id = ANY($1)")
            }
            Variant::TwoTable => {
                format!("UPDATE {GROUPS_TABLE} SET version = version + 1 WHERE group_id = ANY($1)")
            }
        }
    }

    /// Delete one item from a group, `$1 = group_id`.
    pub fn item_delete(&self) -> String {
        format!(
            "DELETE FROM {TABLE} WHERE id IN \
             (SELECT id FROM {TABLE} WHERE group_id = $1 LIMIT 1)"
        )
    }

    /// Reassign one item's PK, `$1 = group_id`.
    pub fn pk_update(&self) -> String {
        format!(
            "UPDATE {TABLE} SET id = nextval('stress_pk') WHERE id IN \
             (SELECT id FROM {TABLE} WHERE group_id = $1 LIMIT 1)"
        )
    }

    /// Insert. Single-table: a whole new group at version 0 (`$1 = data`).
    /// Two-table: a new item in an existing group (`$1 = group_id, $2 = data`),
    /// which grows — and thus invalidates — that group's join query.
    pub fn item_insert(&self) -> String {
        match self.variant {
            Variant::SingleTable => format!(
                "INSERT INTO {TABLE} (group_id, version, data) \
                 VALUES (nextval('stress_group'), 0, $1)"
            ),
            Variant::TwoTable => {
                format!("INSERT INTO {TABLE} (group_id, data) VALUES ($1, $2)")
            }
        }
    }

    /// One transaction bumping both groups of a pair.
    pub fn cross_group_txn(&self, a: i32, b: i32) -> String {
        let table = match self.variant {
            Variant::SingleTable => TABLE,
            Variant::TwoTable => GROUPS_TABLE,
        };
        format!(
            "BEGIN;
             UPDATE {table} SET version = version + 1 WHERE group_id = {a};
             UPDATE {table} SET version = version + 1 WHERE group_id = {b};
             COMMIT;"
        )
    }
}

/// Seed `rows_per_group` rows for every normal group `0..groups` and every
/// paired group `PAIR_GROUP_BASE..=last_pair`, projecting `select_expr` into
/// `columns`.
async fn seed_groups_rows(
    client: &Client,
    table: &str,
    columns: &str,
    select_expr: &str,
    groups: i32,
    last_pair: i32,
    rows_per_group: i32,
) -> Result<()> {
    let normal = format!(
        "INSERT INTO {table} ({columns})
         SELECT {select_expr}
         FROM generate_series(0, {groups} - 1) g, generate_series(1, {rows_per_group}) r;"
    );
    client.batch_execute(&normal).await.context("seed normal")?;

    if last_pair >= PAIR_GROUP_BASE {
        let paired = format!(
            "INSERT INTO {table} ({columns})
             SELECT {select_expr}
             FROM generate_series({PAIR_GROUP_BASE}, {last_pair}) g,
                  generate_series(1, {rows_per_group}) r;"
        );
        client.batch_execute(&paired).await.context("seed paired")?;
    }
    Ok(())
}
