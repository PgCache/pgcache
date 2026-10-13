# Walkthroughs

Worked examples that trace one subsystem end to end on concrete inputs. They complement the ADRs (`../../ADR/`), which record *why* a design was chosen and deliberately stay free of implementation narrative. A walkthrough shows *how* the pieces act on a small example, with the intermediate state spelled out.

Conventions:

- One file per walkthrough, named for the subsystem.
- Each walkthrough names the test that mirrors its example, so a behavior change fails the test and flags the prose for update.
- Link the walkthrough from the Implementation Notes of the ADRs it illustrates.

| Walkthrough | Subsystem | Mirrored by |
|---|---|---|
| [subsumption-index.md](subsumption-index.md) | Constraint-containment index, region probe, precise subsumption check (ADR-024/029/030/037) | `test_walkthrough_two_parents_four_queries` in `src/query/constraint_index/tests.rs` |
| [query-tree.md](query-tree.md) | The rope query tree: conversion, views, bindings and a rewrite (ADR-057) | `test_positions_follow_pre_order_lengths` and `test_same_shape_rewrite_shares_every_other_node` in `src/query/ast/rope/tests.rs` |
