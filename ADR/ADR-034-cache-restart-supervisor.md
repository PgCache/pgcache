# ADR-034: Cache Restart Supervisor

## Status
Proposed (depends on ADR-033)

## Context
ADR-033 collapsed serving onto a shared runtime and, in doing so, left only the writer and CDC consumer as dedicated threads — making the cache subsystem (writer + CDC threads plus the worker and coalesce-drain tasks) a single restartable unit. That ADR explicitly deferred the restart model: the probe did not respawn on backend failure. A fatal error anywhere in the subsystem cancelled it and connections degraded to origin-forward **permanently**, until the whole process was restarted.

The scaffolding for recovery was already present but unused: `QueryCache` and the admin status sender are published through `watch` channels (`QueryCacheUpdater`, `StatusSenderUpdater`) so connections and the admin server pick up a new backend without being recreated. What was missing was the component that detects death and rebuilds.

The constraints: the accept loop and live connections must keep running across a cache restart (degrading to origin, not erroring); a rebuild must not run while the previous generation is still touching the cache database; and first-start behaviour must keep the existing contract that a listening proxy implies a ready cache (relied on by operators and the test harness).

## Decision
Supervise the cache subsystem as a sequence of **generations**, each a self-contained unit: a cancel token, the writer/CDC scoped threads, and the worker/coalesce tasks, publishing a fresh `QueryCache` and status sender through the existing watch channels.

- A **supervisor runs on the proxy thread** — the thread that owns the inner `thread::scope` and both watch updaters. The accept loop moves to its own dedicated scoped thread so the proxy thread is free to supervise.
- **Generation 1 is built fail-fast during startup, before the proxy accepts.** A failure there fails startup, exactly as before, preserving "listening ⇒ cache ready." The supervisor handles only generations 2+.
- A **single cancel token signals subsystem death.** CDC-fatal, connection-pool failure, and writer death (surfaced through the writer→drain notify channel closing) all cancel it. The supervisor parks on that token.
- On death the supervisor **clears the watches** (connections degrade to origin), **reaps the dead generation's threads by joining them** — which gates the next database reset — then rebuilds with **exponential backoff** (500 ms → 30 s), retrying until a generation comes up or the process shuts down.
- **Replication is provisioned per generation**, not once at startup. Each generation re-runs `replication_provision` (idempotent for the slot, recreating the publication empty) before starting CDC, so a restart re-establishes a slot the origin lost — otherwise the CDC thread would fail to resume and the generation would run with dead CDC (stale-read risk). If the origin is unreachable, provisioning fails and the supervisor backs off rather than publishing a half-live generation.
- The **accept thread cancels the proxy token on exit**, so a startup or accept-loop failure unwinds the supervisor rather than leaving it restarting the cache against a dead listener.
- Each successful rebuild increments `pgcache.cache.restarts_total` for observability.

## Rationale
- **Serving is decoupled from the cache lifecycle.** Dispatch already reads `QueryCache` from a watch; clearing it on death and republishing on rebuild makes connections degrade to origin during the gap with no accept-loop involvement.
- **Generations isolate state.** Fresh channels, cancel token, and cache state per generation mean a rebuild cannot inherit a half-dead predecessor; joining the old writer before the next database reset prevents two writers racing on the cache DB.
- **Fail-fast generation 1 keeps the readiness contract** and matches pre-ADR-033 startup; resilience is added only for post-startup failures, where the alternative was permanent degradation.
- **Supervisor on the proxy thread keeps ownership co-located.** It is the sole writer of both watches across every generation; placing it on the thread that already owns the scope avoids hoisting the updaters out of the scope, leaving the accept loop and admin server with read handles only.

## Consequences

### Positive
- Backend failures (writer, CDC, connection pool) now self-heal instead of degrading the cache for the life of the process — closing the item deferred in ADR-033.
- A cache restart is invisible to clients beyond a window of origin-forwarded queries; the accept loop and existing connections are untouched.
- The admin `/status` sender and published `QueryCache` hot-swap to the new generation automatically through the watch channels.

### Negative
- A *wedged* (rather than cleanly failed) writer blocks restart, because reaping joins its thread; a true hang does not recover on its own.
- A restart drops and recreates the cache database, so cached results are wiped on every restart (cold start). Acceptable — the writer's in-memory state is gone regardless — but it forgoes any warm-cache preservation.
- The previous generation's worker/coalesce *tasks* are not joined (only the writer/CDC threads are); they self-exit on the cancel and their cache-DB connections are terminated by the next reset, a brief and benign overlap.
- Re-provisioning per generation drops and recreates the publication on every restart; tables re-add themselves as queries re-register against the cold cache, so this is correct but adds origin churn on each restart.

## Verification
A fault-injection integration test drives a real death → rebuild cycle: a sentinel CDC insert makes the writer exit, and the test asserts `cache_restarts_total` advances and the rebuilt cache serves hits again. A *graceful* origin bounce that preserves the slot is handled by the unchanged CDC reconnect path (ADR-033 era) but is not covered by an automated test, because the test harness's temporary Postgres cannot restart in place.

## Implementation Notes
Reuses the watch-based hot-swap scaffolding (`QueryCacheUpdater`, `StatusSenderUpdater`) left in place by ADR-033. The process now nests two `thread::scope`s: the outer one (in `main`) owns the proxy thread; the inner one (in the proxy thread) owns the writer/CDC threads and the accept thread, and is where the supervisor builds and reaps generations.
