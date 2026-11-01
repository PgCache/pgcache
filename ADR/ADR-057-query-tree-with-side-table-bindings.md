# ADR-057: One query tree with side-table bindings

## Status
Accepted

## Context
pgcache kept two parallel hierarchies for one query shape: the plain tree the converter built from pg_query, and the resolved tree (ADR-002) that resolution rebuilt node by node with names and catalog facts attached. The `ast_pipeline` bench measured the cost of that duplication: a resolved column reference was 144 bytes against 40 plain because it embedded a copy of the column's catalog metadata plus four name strings; the resolved expression enums were 168 bytes against 112 because rare fat variants sat inline; resolving a five-table join allocated forty nodes, and a star over a fourteen-column table cloned fourteen metadata copies. Trees were cloned freely along the path (the extended-protocol path cloned the plain tree per request to substitute binds), and a cached entry retained the plain tree, the resolved tree and one more resolved copy per referenced table. Traversal downcast every node through `Any` and the collecting form allocated a `Vec`, which is why the session key allocated on the hit path for an IN list.

The alternatives were: keep two hierarchies and slim each (duplication and the resolve-time rebuild stay); one hierarchy parameterised by phase, the "trees that grow" pattern (no duplication by hand, but every traversal and transform becomes generic over the phase and resolution still rebuilds); binding slots written into the plain nodes in place (no second tree, but the compile-time distinction between resolved and unresolved is lost and resolution must own, so clone, the tree); and bindings as a side table keyed by node identity, with a wrapper type carrying the tree and its bindings together.

## Decision
- **One immutable tree, stored as a rope.** The query tree is a table of chunks over shared pre-order node slices (`query/ast/rope/`). A node is its kind, with the counts and flags that say which children follow, and its subtree length; it stores no position, so a subtree is valid at any offset in any tree. A clone is a refcount. The converter emits the rope straight from pg_query's raw parse tree.
- **Resolution is a side table.** Resolving produces `Bindings`, dense vectors keyed by a node's pre-order position (`NodeId`), and the bound form is `ResolvedQuery { tree, bindings }`. A column binding is a table slot and a column position; a table slot holds the resolved schema and a shared `Arc<TableMetadata>`. Names, types and metadata live once, in the catalog entry. A function that needs resolved facts takes `&ResolvedQuery`; only the resolver produces a `Bindings`.
- **Structural results of resolution are annotations.** Star expansion binds the star to the columns it stands for; USING and NATURAL joins bind to their common columns, and the equi-join predicate is derived from the binding; a CTE reference carries its definition's body. The tree never changes shape under resolution, and the rendering from bindings emits the expanded star and the inlined CTE text, so the cache DB and the serve shape (ADR-036, PGC-294) are unchanged.
- **Consumers read through views.** A view is a position in a tree with typed accessors for its kind; it carries the absolute position a side table is keyed by. Nothing indexes nodes by hand and nothing builds an owned node to read.
- **A rewrite is a builder over the base tree.** It keeps the subtrees it leaves alone as table entries over the base's slices, writes only its own nodes, and returns the kept ranges plus the origin of every re-emitted node as the remap its side tables follow. Nothing mutates a tree in place.
- **The hit path allocates nothing.** Hashing for the session key walks the slices; a parameterized read is keyed on its placeholder tree and bind values, and the substituted tree is built only for the writer.
- **Each long-lived owner keeps the smallest derived product that serves it**, as `CompiledPredicate` does for row evaluation; a cached entry retains one shared tree and its bindings.

## Rationale
- **The side table restores the compile-time distinction the in-place slot design lost, and strengthens it.** The type states what resolution added instead of restating the whole shape.
- **Resolution becomes additive**, so the tree is shared rather than rebuilt, and resolving costs a few small vectors instead of a node per reference.
- **Pre-order positions are the rope's indices**, so a side table keyed on them needs no lookup structure, and a rewrite's remap is one shift per kept range.
- **Views keep the typed reading the owned structs gave** without the allocation: a view is two words, and a child or sibling is an offset.
- **Measurements as tests** keep the gains: the bench's size table and allocation counts are the regression baseline.

## Consequences

### Positive
- A node is 64 bytes whatever its kind; a resolved reference is that node plus four bytes in a vector; resolution, rendering and the session key stop allocating per reference.
- One set of node kinds, views and transforms to maintain; the owned tree, its deparser, resolver and walkers are gone.
- A parse is one conversion: the rope straight from the raw tree, with each kind's declared children checked at close in debug builds.
- The fingerprint hashes the bound query's rendered serve text, and rendering from bindings allocates once instead of per reference.

### Negative
- Every reader goes through views; code that pattern-matched owned enums reads a kind and walks positions instead, which is a learning cost for each new consumer.
- Structural rewrites (decorrelation, pushdown, canonicalisation) are builders that must remap their side tables; mutation in place of structure is gone.
- A kind's child layout is a contract between the converter, the views and the builder; adding a child means touching all three.

## Implementation Notes
The rope, its views and builder are `query/ast/rope/`; the converter is `query/ast/convert_tree/`, one submodule per clause family, emitting from pg_query's raw parse tree inside the `parse_raw_scoped` callback; the resolver is `query/resolve/`. The walkthrough is [docs/walkthroughs/query-tree.md](../docs/walkthroughs/query-tree.md). Landed under PGC-481 in slices that each kept the suite green, with the owned tree as the oracle for every rope twin until the converter targeted the rope and the owned tree was deleted.

A tree's root is read by statement kind: `QueryTree::root()` is the `Query` view every tree the SELECT converter builds has, and `QueryTree::statement()` distinguishes it from a `CreateIndex` root (PGC-575). CREATE INDEX is the first non-query statement in the rope — `CreateIndex`, `IndexKey` and `IndexOption` kinds, converted by `convert_tree/index.rs` through its own entry (`index_tree_convert_raw`), so the proxy's statement classification still treats an index as a write — and exists for the writer's cache-side index rewrite (`writer/cache_index.rs`), a builder over the parsed `pg_get_indexdef` text. A statement shape the cache needs is added to the rope this way rather than through pg_query's protobuf tree, which the crate no longer reads.

`benches/ast_pipeline.rs` holds the size table and per-stage allocation counts that are the regression baseline: a parse allocates once per structure the tree keeps plus the builder's node vector, resolving a five-table join fell from 32 allocations to 5, and canonicalisation, pushdown, decorrelation and the session key allocate nothing on a query they leave alone.
