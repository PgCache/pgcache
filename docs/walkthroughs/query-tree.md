# Query tree walkthrough

Traces one query from pg_query's raw parse tree into the rope, reads it back through views, binds it, and rewrites one literal. Mirrored by `test_positions_follow_pre_order_lengths` and `test_same_shape_rewrite_shares_every_other_node` in `src/query/ast/rope/tests.rs`.

Decision behind the structure: ADR-057 (one immutable tree, bindings as a side table keyed by pre-order position).

## Vocabulary

- **Node**: a kind plus the length of its subtree, itself included. The kind carries the counts and flags that say which children follow it, in a fixed order.
- **Position**: a node's index in pre-order. The first child of the node at `p` is at `p + 1`; its next sibling is at `p + len`. Nothing in a node is absolute, so a subtree is the same bytes at any offset.
- **Chunk**: one slice of nodes. A tree is a table of chunks; a freshly converted tree is one chunk.
- **View**: a position in a tree with accessors for one kind. `Query`, `Select`, `Source`, `Where`, `Scalar` and the rest check the kind they expect and walk by offsets.
- **Bindings**: what resolution adds, in vectors keyed by position. `ResolvedQuery` is a tree plus its bindings.

## Conversion

`SELECT a FROM t WHERE b = 1` converts to eight nodes:

| position | kind | len | children |
|---|---|---|---|
| 0 | `Query { cte_count: 0, order_by_count: 0, no limit }` | 8 | the body |
| 1 | `Select { item_count: 1, from_count: 1, has_where }` | 7 | items, FROM, WHERE |
| 2 | `SelectExpr { alias: None }` | 2 | the expression |
| 3 | `Column { table: None, column: "a" }` | 1 | |
| 4 | `Table { name: "t" }` | 1 | |
| 5 | `Binary { op: Equal }` | 3 | left, right |
| 6 | `Column { table: None, column: "b" }` | 1 | |
| 7 | `Literal(Integer(1))` | 1 | |

The converter emits each node with its counts read off the raw lists first, then its children, so the subtree length is known when the node closes. In debug builds the builder asserts at close that the children emitted match what the kind declares.

## Reading

`tree.root()` is a `Query` view at 0. `root.body()` steps to position 1 and, seeing a `Select` kind, yields `Body::Select`. From there `select.items()` is a run of one item starting at 2, `select.from()` a run of one source at 4 (2 + the items' lengths), and `select.where_clause()` the `Where` at 5. Each view's `id()` is its position, which is what the test asserts: the SELECT at 1, the FROM entry at 4, the WHERE at 5.

## Binding

The resolver walks the same views. It gives the table at 4 a slot (schema `public`, the catalog entry for `t`, no alias), binds the columns at 3 and 6 to that slot and their catalog positions, and records them in `Bindings` as `(position, target)` pairs in position order. Asking `bound.column_table_at(id)` for the column at 6 finds the pair by binary search and returns `t`'s metadata. Rendering walks the views and, at each reference, asks the bindings what to write: `SELECT public.t.a FROM public.t WHERE public.t.b = 1`.

## Rewriting

Substituting the literal at 7 opens a builder over the base tree and copies the root with one replacement. Every ancestor of the replaced node is re-emitted, because its subtree length could change; every other subtree is kept as a reference to the base's slice. The result is five chunks: `Query`, `Select` re-emitted; positions 2 through 4 kept in one slice (the select item and the table); `Binary` re-emitted; position 6 kept; the fresh `Literal`. Nothing changed length here, so the remap says every kept node and every re-emitted ancestor kept its number, and the replaced node maps to nothing; the bindings carry over unchanged. A rewrite that does change a subtree's length shifts everything after it, and the remap records the shift per kept range; `BindingsBuilder::from_remapped` moves every binding through it.
