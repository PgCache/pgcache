# ADR-070: Leaf-identity CDC for partitioned tables

## Status
Accepted

## Context
A partitioned table can be read at any level: the root, a partition in between, or a leaf. Each name a query uses gets its own cache table, keyed by that relation's OID, and each is added to the publication on its own.

The publication ran with `publish_via_partition_root`. Under it a row change streams under the topmost ancestor that is in the publication, so the identity a partition's rows arrive under depended on which other cached tables happened to be published. A partition cached alone received its changes; once its root was read as well, every change streamed as the root's and the partition's cache table went stale for good. Registration order did not matter, and a row moving between partitions failed the same way.

Table inheritance has a related problem with no such option. A read of a parent returns its children's rows, which stream under the children, and a child may repeat the parent's primary key, so one cache table keyed on that key cannot hold the tree. `ONLY` was dropped by the converter and served from the plain read's entry.

Alternatives for partitions: rewriting a partition read into a root read plus the partition bound (one cache table per tree, but hash bounds are not evaluable in the cache database, a default partition's bound moves with its siblings, and privileges and row-level security are checked on the named partition), or forwarding any read that names a partition.

## Decision
- **Publish without `publish_via_partition_root`.** A row change always streams under the leaf partition that holds the row, whatever else is published.
- **The writer routes a leaf's rows to every cached table whose partition tree holds the leaf.** The routes are derived from the active tables and the partition trees their metadata already carries, and rebuilt whenever either changes. A table with no partitions takes no route.
- **Rows are carried into each target's column layout by name.** A leaf's tuples arrive in the leaf's own layout, known from its `Relation` message; a target whose columns or key the leaf's tuples do not fit is evicted and registered again from the catalog.
- **The stream's `Relation` messages are the source of leaf layouts**, kept for the session rather than per registration, since the walsender describes a relation once.
- **A leaf nobody announced evicts the tables above it.** A partition attached, or created where the event triggers are not installed, was not in the tree those tables were populated with.
- **A truncated partition empties its own cache table in place and evicts the cached tables above it**, unless the statement truncated those as well. The per-partition truncate trigger of ADR-058 is gone.
- **A table counts as `REPLICA IDENTITY FULL` only while every partition holding its rows is**, since old tuples are written under the leaf's identity.
- **Inheritance parents and `ONLY` are forwarded.** A table with inheritance children is refused at registration; one that gains a child is evicted. Publication entries are named with `ONLY`.

## Rationale
- **Stream identity becomes a function of the row alone.** The mapping from stream relation to cache tables no longer changes with the publication's membership, which pgcache alters at every registration and eviction.
- **The partition tree was already held.** ADR-058 put the tree in table metadata for schema signals; routing reads the same lists.
- **Cache tables stay what queries name.** Privileges, row-level security and the read-after-write gate keep one identity per table, which a rewrite onto the root would split in two.
- **The stream reports more by itself.** A partition's truncate and a row's move between partitions arrive as ordinary events on the leaves involved.
- **Inheritance is rare enough to forward.** Supporting it needs cache tables keyed on the source relation as well as the key.

## Consequences

### Positive
- A partition and its ancestors can be cached together, in any order, and stay current.
- No trigger sits on user partitions any more: partition truncate comes from the stream.
- An attached partition with a different column order is applied correctly rather than by position.
- Reads of inheritance parents and `ONLY` reads are correct.

### Negative
- A row in a tree cached at several levels is applied once per level.
- Every `Relation` message is retained for the session, and one for a relation the writer does not know costs a catalog read.
- A created or altered table costs a catalog read to learn whether it became an inheritance child.
- Without the event triggers, a child added to a cached table by inheritance goes unnoticed: it is not in the publication, so nothing on the stream reports it.

## Implementation Notes
Routes, layouts and column maps live in `cache/writer/stream_route.rs`; the writer consults them at command intake in `writer/cdc/dispatch.rs`, before a row enters the frame, so everything downstream sees cached-table OIDs only. The CDC processor filters on the shared stream-relation set and passes a partition a create signal announced until the writer has added it. Tests: `tests/partition_test.rs`, `tests/inheritance_test.rs`.
