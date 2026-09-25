# ADR-054: In-transaction cache serving at READ COMMITTED

## Status

Accepted

## Context

Every statement inside a transaction block was forwarded to origin; the
per-connection read-after-write log (ADR-048) only *recorded* in-block writes so
post-commit reads would not be served stale. Applications that wrap request
handling in a transaction therefore got no cache hits at all, even for reads
that touch nothing the block has written.

Three things stood between an in-block read and a correct cache serve:

- **Own uncommitted writes.** A READ COMMITTED statement sees the latest
  committed state plus the block's own writes. The cache holds the former; the
  write log already holds the latter, unstamped (the commit-LSN probe fires
  only at an idle ReadyForQuery), so the existing gate forwards any read that
  intersects them and serves provably disjoint ones. No new machinery was
  needed for the delta.
- **Isolation level.** REPEATABLE READ and SERIALIZABLE pin a snapshot the
  cache cannot reproduce. The effective level is invisible on the wire:
  neither `transaction_isolation` nor `default_transaction_isolation` is a
  GUC_REPORT parameter, and the default can come from `ALTER ROLE/DATABASE
  SET`, PGOPTIONS, or the startup `options` parameter without a statement ever
  crossing the proxy.
- **Wire-level transaction state.** A cache serve appended a static idle
  ReadyForQuery. Inside a block the status byte must be `T`, or drivers that
  track transaction state from it believe the block has ended. A block in the
  failed (`E`) state rejects every statement at origin, so serving rows there
  would be wrong even at READ COMMITTED.

## Decision

Cacheable reads inside a transaction block are dispatched to the cache and
gated per slot; the block's own writes are handled by the unchanged
read-after-write gate.

- **Gate in the OriginDrain arm, not at message time.** A read is queued as a
  cache candidate when it arrives; the decision is made when its slot reaches
  the head of the egress queue, after every earlier response (a pipelined
  `BEGIN`'s ReadyForQuery, an error that failed the block) has been sealed.
  Serve requires status `T` (never `E`) and an effective isolation level of
  READ COMMITTED; the read-after-write gate then runs as it does outside a
  block. Extended-protocol batches use the same per-slot gate.
- **Isolation discovery: one probe plus statement tracking.** The session
  default is discovered once per connection by an injected `SHOW
  default_transaction_isolation` (an origin intercept, sharing the search_path
  probe's quiescent-ReadyForQuery injection point), and never while a block is
  open — there it would report the default, not the block, and in a failed
  block it would only error. Every statement that can change it is classified
  in the memoized cacheability analysis, bundled with its write class and
  transaction boundary as one `StatementEffects` value that every forward path
  applies through a single method: `BEGIN/START TRANSACTION ... ISOLATION
  LEVEL` sets the level of the block it opens and `SET TRANSACTION` tightens
  the open block (outside one it is PostgreSQL's no-op); `SET [SESSION]
  default_transaction_isolation` and `SET SESSION CHARACTERISTICS` set the
  default; `RESET`, `RESET ALL`, `DISCARD ALL`, `SET ... TO DEFAULT`, `SET
  LOCAL`, an unreadable value, an unparseable statement, and any text
  mentioning `set_config` mark the default unknown. Unknown means forward, and
  unknown is transient: the next quiescent ReadyForQuery re-probes. A mutation
  inside a block flags a re-probe at block end, since a `SET` in a
  rolled-back block reverts and `SET LOCAL` always does.
- **A block's level is fixed when it starts.** As PostgreSQL reads the default
  at `BEGIN`, the proxy snapshots its session state into a block-scoped state
  on the idle→in-block ReadyForQuery edge (a forwarded `BEGIN` clause takes
  precedence), so a mid-block change to the default cannot loosen the open
  block. Effects are applied at forward time, which can run ahead of the
  ReadyForQuery that reports the block (pipelining), so block membership counts
  forwarded `BEGIN`s not yet acknowledged by that edge rather than trusting the
  lagging status.
- **Statements only tighten.** An explicit READ COMMITTED (on `BEGIN`, `SET
  TRANSACTION`, or the default) is never trusted directly: a block-level one is
  ignored, a session-level one marks unknown so the probe confirms it. Only the
  probe establishes READ COMMITTED. A statement that failed at origin (e.g.
  `SET TRANSACTION` after the first query) can therefore never make the proxy
  serve wrongly.
- **Transaction status threaded to the serve path.** The connection tracks the
  ReadyForQuery status as a tri-state (`Idle` / `InTransaction` / `Failed`) and
  passes it with every cache dispatch; the serve, memo, and explain paths
  append a ReadyForQuery carrying it. Coalesced waiters key on it, so a
  follower in a block never receives a leader's idle status. A serve that
  fails after bytes reached the client *inside* a block sends the error alone
  and closes the connection: no status byte is truthful there (`T` would let
  the client commit what it saw fail, `E` would misreport origin), and the
  close makes origin roll the block back.
- **Pre-PG18 search_path inside a block.** `ROLLBACK TO SAVEPOINT` reverts a
  `SET search_path` made after the savepoint while the block stays open, so it
  now marks search_path unknown like a block end does; in-block serving made
  the drift observable.
- **Always on.** The `read_your_writes` setting was removed (the gate is the
  correctness mechanism, not an option) and no `transaction_serving` setting
  was added. The integration harness keeps a fault-injection-only disable for
  the write log, as it did through the setting.

## Rationale

- **Per-slot gating is the only correct place.** Message-time checks race the
  responses still in flight; the OriginDrain arm sees the state that will hold
  when the response is written. This also closes the pipelined `BEGIN` then
  read case, where the read was classified as out-of-block before the
  `BEGIN`'s ReadyForQuery arrived.
- **Probe plus tracking over per-transaction probing.** Every driver sets
  isolation through a statement the proxy can see (`SET SESSION
  CHARACTERISTICS`, `default_transaction_isolation`, the `BEGIN` clause); only
  server-side configuration is invisible, and one probe at connect covers it.
  A `SHOW transaction_isolation` after every `BEGIN` would cost an origin
  round trip per block, including write-only ones.
- **Unknown forwards rather than assumes READ COMMITTED.** The probe makes the
  unknown window one statement long, so the conservative rule costs nothing
  measurable while protecting the DBA-configured stricter default — the case
  where a snapshot-breaking serve would go unnoticed longest.

## Consequences

### Positive

- Reads inside READ COMMITTED blocks that are disjoint from the block's own
  writes are cache hits; single-table reads keep their in-place maintenance.
- Own writes remain visible inside the block by the same gate that protects
  post-commit reads; rollback needs no special handling (false positives clear
  on the watermark as before).
- Cache-served responses carry the correct transaction status everywhere,
  including the pre-existing bare-`Sync` synthesized ReadyForQuery.

### Negative

- One `SHOW default_transaction_isolation` round trip per connection at
  connect, and one after each isolation mutation.
- A block that explicitly asks for READ COMMITTED under a stricter session
  default is forwarded wholesale (the clause is not trusted).
- A mid-response serve failure inside a block drops the client connection
  (origin rolls the block back). The alternative — forwarding a deliberately
  failing statement to move origin's block to the failed state and reporting
  `E` — would keep the connection but adds an intercept for a rare path.
- **Not observable, documented limitations:** a `postgresql.conf` reload that
  changes `default_transaction_isolation` under a live session, and a `SET`
  executed from inside a function body, are not seen; the proxy keeps the
  level it last observed or probed.
- The same-session monotonic-read window the base cache accepts (an
  origin-forwarded read observing another connection's commit, followed by a
  cache-served read at an older watermark) is now also visible *inside* a
  block. READ COMMITTED permits per-statement snapshot changes, so this is an
  explicitly accepted property; PGC-396 (bounded wait-for-apply) is the
  mechanism if it ever needs closing.
- Writes made by triggers or cascades to tables other than the statement's
  target are invisible to the name-matched write log inside a block, exactly
  as they are after commit (ADR-048's known exclusion).

## Implementation Notes

The gate and status plumbing live in the connection (`transaction_isolation.rs`
holds the isolation state machine and probe intercept; the tri-state status and
gate are in `relay.rs`); statement classification is in
`query/ast/convert_raw/write.rs` and surfaces through `Action`. Metrics:
`pgcache.txn.served`, `pgcache.txn.forwards{reason}`,
`pgcache.txn.isolation_probes`. Integration coverage is
`tests/transaction_serving_test.rs`; the consistency harness's `--txn-reads`
and `--txn-isolation` add an in-block read mix with an own-write visibility
check and an end-of-run serving expectation per isolation level.
