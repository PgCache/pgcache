# ADR-057: One query tree with side-table bindings

## Status
Accepted

## Context
pgcache keeps two parallel hierarchies for one query shape: the plain tree the converter builds from pg_query, and the resolved tree (ADR-002) that resolution rebuilds node by node with names and catalog facts attached. The `ast_pipeline` bench measured the cost of that duplication: a resolved column reference is 144 bytes against 40 plain because it embeds a copy of the column's catalog metadata plus four name strings; the resolved expression enums are 168 bytes against 112 because rare fat variants sit inline; resolving a five-table join allocates forty nodes, and a star over a fourteen-column table clones fourteen metadata copies. Trees are cloned freely along the path (the extended-protocol path clones the plain tree per request to substitute binds), and a cached entry retains the plain tree, the resolved tree and one more resolved copy per referenced table. Traversal downcasts every node through `Any` and the collecting form allocates a `Vec`, which is why the session key allocates on the hit path for an IN list.

The alternatives were: keep two hierarchies and slim each (duplication and the resolve-time rebuild stay); one hierarchy parameterised by phase, the "trees that grow" pattern (no duplication by hand, but every traversal and transform becomes generic over the phase and resolution still rebuilds); binding slots written into the plain nodes in place (no second tree, but the compile-time distinction between resolved and unresolved is lost and resolution must own, so clone, the tree); and bindings as a side table keyed by node identity, with a wrapper type carrying the tree and its bindings together.

## Decision
- **One immutable tree.** The plain tree is the only hierarchy. Once finalized it is shared by `Arc` across the cacheability memo, the writer and the cached entry; nothing clones it to resolve.
- **Resolution is a side table.** Resolving produces `Bindings`, dense vectors keyed by node id, and the resolved form is `ResolvedQuery { tree: Arc<QueryExpr>, bindings: Bindings }`. A column binding is a table slot and a column position; a table slot holds the resolved schema and a shared `Arc<TableMetadata>`. Names, types and metadata live once, in the catalog entry.
- **Structural results of resolution are annotations.** Star expansion binds the star node to the column references it stands for; USING and NATURAL joins bind to their common columns, and the equi-join predicate is derived from the binding; a CTE reference binds to its definition. The tree never changes shape under resolution. The resolved deparse keeps emitting the expanded star and the inlined CTE text, so the cache DB and the serve shape (ADR-036, PGC-294) are unchanged.
- **Every traversal node has a `NodeId`**: its pre-order position in the finalized tree, a `u32` stored on the node. Constructors never allocate ids; nodes are built `PENDING` and a finalize pass stamps positions once after conversion and once after each structural rewrite. A rewrite is a builder returning a fresh finalized tree plus the old-to-new remap for its side tables. Ids are identity metadata, never part of structural equality or hashing.
- **Nodes pay for the common case.** Enum sizes are set by the leaf variants; rare fat variants (window, case, subquery) are boxed, and node sizes are pinned by tests.
- **The hit path allocates nothing.** Traversals that answer a predicate yield through a callback; hashing for the session key never collects.
- **Each long-lived owner keeps the smallest derived product that serves it**, as `CompiledPredicate` already does for row evaluation; the resolved tree is retained once per entry and shared.

## Rationale
- **The side table restores the compile-time distinction the in-place slot design lost, and strengthens it.** A function needing resolved facts takes `&ResolvedQuery`; nothing can call it with a bare tree, and only the resolver produces a `Bindings`. The type states what resolution added instead of restating the whole shape.
- **Resolution becomes additive**, so the tree is shared rather than rebuilt, and resolving costs a few small vectors instead of a node per reference.
- **Pre-order positions are the arena's indices.** The arena (one `Vec` per tree, children by index, clone as one copy, hash over contiguous memory) is the named second round; defining the id as pre-order position now means every side table and consumer keyed on `NodeId` survives that migration unchanged.
- **Universal ids make annotation a pattern, not a special case**: expression types, settings sensitivity, subquery kinds and volatility become side tables owned by named phase products, without touching node definitions.
- **Measurements as tests** keep the gains: the bench's size table and allocation counts become assertions.

## Consequences

### Positive
- Resolved references shrink from 144 bytes plus a metadata copy to a 40-byte plain node plus four bytes in a vector; resolution and deparse stop allocating per reference.
- One set of node types, traversals and transforms to maintain instead of two.
- The fingerprint hashes the bound query's rendered serve text, and rendering from bindings allocates once instead of per reference.
- The arena migration is a change of store, not of API.

### Negative
- Every consumer of the resolved hierarchy migrates to the plain node plus bindings, threading `&Bindings` or a view type through; this is where the type safety comes from and also the bulk of the work.
- Structural rewrites (decorrelation, update-query derivation, CTE handling) must maintain ids and remap their side tables; mutation-in-place of structure goes away.
- Four bytes per node for the id; the few structs where it does not land in padding grow.
- A universal id invites annotation sprawl; the rule is that every side table belongs to a named phase product.

## Implementation Notes
Landed in six slices under PGC-481, each keeping the suite green: ids and the finalize pass; `Bindings` emitted beside the resolved tree and proven equivalent; render, shape and fingerprint from bindings; predicate pushdown, decorrelation, update-query derivation, population and CDC membership on `ResolvedQuery`; constraint analysis, the classifiers and the compiled predicate on the bound query, and the resolved tree dropped from every cached entry; the resolver rewritten to emit bindings alone, with the old resolver as its oracle until the resolved hierarchy was deleted. `LiteralValue` stays a leaf value with no stored id until a phase needs to annotate literals by id. The bench is `benches/ast_pipeline.rs`.

Measured with the bench's allocation counts (count / bytes per query, resolved resolve before → bound resolve after): `read_simple` 13/2384 → 9/768, `join_five` 32/6128 → 11/1008, `wide_star` 17/4832 → 13/1392, `in_list_50` 12/10568 → 9/720, `cte_datetime` 28/5220 → 19/1852. A cached entry now retains one shared tree and its bindings; the resolved tree, its per-entry clone and the re-resolutions of pushed-down and decorrelated trees are gone. Each update query still clones its branch and remaps its bindings, which the retention audit measures as the largest remaining per-entry copy. Still open: that audit's changes and the arena.

Rewrites obtain their remap by refinalizing. A subtree keeps a contiguous id range in any tree, so a builder copies the pieces it keeps with their ids, builds what is new `PENDING` (or under a provisional id when it must bind it), and the refinalize pass records every old-to-new pair as it restamps; `BindingsBuilder::from_remapped` carries the bindings over and the builder binds its provisional nodes through the same slot-and-position API the resolver writes through. The only slot a rewrite makes is the derived table scalar decorrelation builds. A builder qualifies every reference it moves into another scope and writes stars out, so the rewritten tree still says what it means; the test oracle resolves that tree from scratch and compares the bindings. What a derived table stands for in CDC terms (`SubqueryKind`) is a field of the node, since the resolver cannot recover it. The CDC membership query is not a rewrite: a `SourceOverride` on the rendering context stands the row in for one FROM entry's slot, which also retargets every reference bound to that slot, nested scopes included.
