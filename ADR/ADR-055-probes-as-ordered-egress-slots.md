# ADR-055: Probe replies as ordered egress slots

## Status
Accepted

## Context
The proxy injects three queries of its own on a client connection's origin socket and swallows their replies: `SHOW search_path` (pre-PG18, ADR-006), `SHOW default_transaction_isolation` (ADR-054) and `SELECT pg_current_wal_insert_lsn()` (ADR-048). Each was an *origin intercept*: a per-connection state that, once armed, consumed every origin message up to the next `ReadyForQuery`. That is only correct if the probe's reply is the next thing origin sends — i.e. the socket was quiescent when the probe was written. The commit-LSN probe checked that; the two discovery probes assumed it, and one of them was chained onto the completion of the other. A client statement already in flight behind the first probe then had its response swallowed by the second probe's intercept and received the probe's rows instead (0.7.0: the first statement of most fresh connections on PG 17 and earlier answered with "read committed"; the same hazard for any client pipelining extended-protocol batches across a re-probe).

Guarding every probe on quiescence would fix the ordering but starves a connection that always has a request outstanding: the isolation default would stay unknown (in-block reads forward) and, pre-PG18, search_path would stay unknown after every transaction end (every cacheable read forwards). The commit-LSN probe already had that starvation.

## Decision
A probe is not an intercept but a position in origin's reply order. The egress queue (the connection's single ordering structure for client-bound responses) gains a `Probe` slot kind: pushing one reserves the probe's place after every slot already on the origin socket, origin replies route to the oldest unsealed `Origin`-or-`Probe` slot, and a probe slot's reply is swallowed there whatever client requests surround it. Probe slots carry nothing for the client and are invisible to the flush driver. Injection is state-based only — at any origin `ReadyForQuery`, client-visible or a probe's own, whenever the connection's state calls for a probe and none of that kind is outstanding — with no quiescence condition for any probe.

Two further rules keep the two orders aligned and the samples valid. A probe is placed before the first queued `Cache` slot (a cache slot is never on the socket and, since the connection reads no client request while one is queued, nothing behind it is), so a cache fallback that forwards to origin lands behind the probe in both orders. A probe that must not run inside a transaction block (isolation default, commit LSN) is skipped while a block is open or a forwarded `BEGIN` is unacknowledged, and its own `ReadyForQuery` status is the final check: anything but idle discards the sample. Injection is also skipped while an extended-protocol group has bytes on the socket outside any slot (a Flush-terminated Describe, a Close ahead of its Sync), whose replies carry no `ReadyForQuery` to order against.

## Rationale
- **Order is a property of the socket, not of time.** Origin answers a session's requests in the order they were written. Recording the probe's position at write time is exact; "nothing else is in flight" was an approximation of it that held only for non-pipelining clients.
- **One ordering structure.** The egress queue already orders every client-bound response and already routes origin bytes by position; adding the proxy's own requests to it means there is no second order to keep consistent (the former intercept + quiescence guard was that second order).
- **No starvation.** Every probe can be injected at every `ReadyForQuery`, so unknown state resolves after one round trip regardless of client concurrency.
- **The commit-LSN sample stays sound without quiescence.** The probe executes after every write forwarded before it on a single session, so its LSN bounds exactly the writes recorded through the sequence snapshotted at injection; later writes have a higher sequence and are left for the next probe. Only "not inside a block" matters, and the probe's own status byte verifies it exactly.

## Consequences

### Positive
- A client statement in flight across a probe injection gets its own response, under pipelining of any depth and at connect time.
- Discovery and commit-LSN probes no longer wait for an idle socket; busy pipelining connections keep their in-block and pre-PG18 cache hits.
- The intercept state machine shrinks to the two cases that strip proxy-added parts out of a client request's *own* response (lazy `Parse`, piggybacked `SHOW search_path`).
- A probe sampled inside a block is discarded by its own status byte, closing a gap for a pipelined `BEGIN` the analyzer could not see.

### Negative
- The flush driver must skip probe slots at the head, so head operations index into the queue rather than popping the front.
- A probe can now be outstanding concurrently with a cache serve or a forwarded request; its reply is parsed by position, so a future "whole reply" intercept must not be added without a slot.
- Pipelined-startup clients (not spec-compliant, but tolerated by PostgreSQL under trust auth) are ordered correctly only because the startup handshake now opens its slot explicitly.

## Implementation Notes
`proxy/egress.rs` holds the `Probe` slot and reply routing; `proxy/connection/probe.rs` holds injection conditions and reply application; `search_path_intercept.rs` keeps the two in-batch intercepts. Integration coverage: `tests/probe_ordering_test.rs` (pipelined startup and re-probe), plus a pipelined-write case in `tests/read_after_write_test.rs`.
