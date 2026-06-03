# ADR-033: Unified Serving Runtime

## Status
Proposed

## Context
pgcache's serving path historically ran on several dedicated single-threaded executors: N connection threads, a central cache coordinator, and a cache worker — each its own `current_thread` Tokio runtime with a `LocalSet`. The stated reason was `!Send` state: pooled connections and cache structures were assumed to require thread-locality. A consequence is that a single cache hit hops across three runtimes (connection → coordinator → worker → connection), and each hop is a cross-runtime wake — an eventfd/condvar unpark plus a context switch. Profiling on the demo box attributed ~14% of process CPU to this handoff rather than useful work.

Two findings reframed the problem. First, a `Send` audit showed the `!Send` assumption was almost entirely vestigial: the per-connection future, the worker, and all cache state are already `Send`; the *only* obstacle on the serving path was one field, the coordinator's coalescing queue (`Rc<RefCell<…>>`). Second, the connection-thread fast path (PGC-237) removed the coordinator hop for clean hits but left the remaining hops and, more importantly, a single-threaded worker that funnels every cache-DB serve through one core.

The open question was whether collapsing the per-thread executors into one work-stealing runtime would pay off, or whether losing per-connection cache locality would erase the gains. A probe was built to measure it.

## Decision
Run connections, the coordinator, and the worker as tasks on a **single shared multi-thread Tokio runtime**. Keep the writer and CDC consumer as dedicated threads, since they are serialization points reached only through `Send` channels and gain nothing from joining the pool.

- The coordinator's coalescing queue moves from `Rc<RefCell<…>>` to a `Send` container, making `QueryCache` `Send`.
- Serving-path tasks are scheduled with `tokio::spawn` instead of `spawn_local`; cache-DB serves spread across all runtime threads rather than one worker thread (the connection pool still bounds concurrency).
- The accept loop spawns a task per connection onto the shared runtime; there are no longer dedicated connection threads.

## Rationale
- **The `!Send` barrier was not real.** Collapsing the executors required one type change plus runtime plumbing, not a migration.
- **Handoffs become intra-runtime.** Connection ↔ coordinator ↔ worker hops turn into run-queue pushes (often on-core) instead of cross-runtime eventfd wakeups.
- **It removes a single-thread bottleneck.** The worker was the largest CPU consumer and ran on one thread; spreading serves across the pool lifts that cap.
- **Work-stealing balances skewed load** automatically, where static connection-to-thread assignment could not.
- **The locality concern did not materialize.** Measured throughput rose and per-hit CPU fell; the cost of cross-core task migration was far outweighed by eliminating the handoffs and the bottleneck.

## Consequences

### Positive
- Throughput up ~26–29% over the fast-path-only build (interleaved A/B at 1 and 4 workers), achieved with *fewer* threads.
- CPU per cache hit down ~35% versus the fast-path build (~42% versus the original multi-runtime design).
- Fewer moving parts on the serving path: no per-connection runtimes, no central worker thread.
- Writer and CDC remain cleanly isolated as the only dedicated threads.

### Negative
- Loses guaranteed per-connection locality; relies on the work-stealing scheduler. Measured to be a net win here, but it is a real trade and could differ for other workloads.
- The supervisor/restart model must be reworked: with serving on the shared runtime, only writer/CDC remain a restartable unit, and serving degrades to origin-forward while they recover. **Deferred** — the probe does not respawn on backend failure.
- `num_workers` now means "shared runtime worker-thread count," not "connection-thread count," while still doubling as the multiplier for the connection pool, command-channel, and population-pool sizes. This conflation should be untangled and the setting renamed.
- The coordinator is still a single task (one consumer of one channel). It is off the hot hit path (PGC-237), but remains a serialization point for misses and coalescing; deleting it via fully inline dispatch is a natural follow-up.

## Implementation Notes
The connection-thread fast path (PGC-237) stays useful and becomes more central: making `QueryCache` `Send` enables every connection to dispatch inline, which is the path toward removing the coordinator task entirely. Thread-per-core (shared-nothing, no work-stealing) remains an alternative end-state worth comparing against this design.
