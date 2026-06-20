# ADR-042: Hot-Path Allocation Elimination

## Status
Accepted

## Context
pgcache's value proposition is sub-origin latency on cache hits and the ability to keep a registered query's rows live under a high-rate CDC stream. Both of those are the hottest paths in the system — a cache hit runs on every served request, and the CDC apply runs on every committed origin change to a cached relation — so any per-request or per-event heap allocation is paid millions of times and shows up directly in tail latency, in allocator contention across the worker pool, and in CDC apply throughput (which gates replication-slot lag).

ADR-025 already removed allocation from one slice of the hit path: it replaced `tokio_postgres` with a frame-level cache connection so a cache-DB *lookup* response is forwarded to the client without deserializing rows, and it recycled the connection's read buffer across queries. But that work stopped at the lookup. The response still had to be assembled and written toward the client, the CDC apply path still decoded replication tuples into owned strings and allocated fresh row vectors per event, and every cache hit allocated a fresh `tokio::sync::oneshot` to carry the reply from the worker back to the connection. Each of these is a per-request or per-event allocation on a path that runs at request/event rate.

The unifying constraint is simple to state and was applied uniformly: **no heap allocation per request on a hot path, and none per event on the CDC apply path**, after warmup. Buffers and channels are sized once and reused; values that already live in a refcounted frame are viewed, not copied. The four facets below are this one principle applied to the four places that still allocated.

## Decision
Extend the no-allocation principle from ADR-025's frame forwarding to cover the serve *response* encoding and the entire CDC apply path, via four reuse mechanisms:

- **Serve response encoded into reusable, zero-copy buffers.** The serve path assembles its client-bound response in a `WriteQueue` of `Bytes` chunks rather than a contiguous per-hit buffer. Frame data read from the cache DB is forwarded by reference (the codec's `BytesMut` freezes to `Bytes` at zero cost), and fixed protocol messages (ParseComplete, BindComplete, ReadyForQuery, the synthetic serve-error frame) are `Bytes::from_static` — neither copies. The queue is an inline ring of chunks with a heap spill that only allocates when a slow client lets many chunks accumulate live at once, so a typical response stays entirely inline. The SQL the serve issues to the cache DB is rendered into the connection's recycled `sql_buf`, and `LIMIT`/`OFFSET` binds format into a stack-resident `itoa::Buffer`, so the common integer case touches no heap.
- **CDC tuples decoded zero-copy as `ByteString` views.** A replication `TupleData::Text` value is wrapped in a `ByteString` — a refcounted view into the replication frame — rather than copied into an owned `String`. The frame is pinned by the view and bounded by the row size. Invalid UTF-8 (which PG should never send in text-format replication) falls back to a lossy owned copy rather than dropping the event. `CdcValue` distinguishes `Null`, `Text(ByteString)`, and `Toasted` so the unchanged-TOAST marker is preserved through decode without an owning copy.
- **Row vectors reused end-to-end on the CDC path.** Tuple decode (`tuple_data_parse`) reuses the source `Vec<TupleData>`'s allocation by consuming it in place — the in-place collect specialization, guarded by a compile-time assertion that `TupleData` and `CdcValue` share size and alignment so a future layout change fails the build instead of silently degrading to per-event allocation. Downstream, `cdc_values_convert` appends into a caller-supplied row `Vec` drawn from a writer-owned pool (`row_vec_pool`), and a replay-drained event's row vectors are returned to that pool, so steady-state apply allocates no row vectors.
- **Per-query reply via a reusable per-connection slot.** The worker→connection reply channel is a `ReplySlot` allocated once per connection and reused for every query on it, replacing the per-query `oneshot` (one heap allocation per cache hit). Minting a per-query `ReplySender` is an `Arc` refcount bump; the receiver's wait is a stack-resident `Notified` future. The strict one-outstanding-query-per-connection invariant means the slot holds at most one reply, so reuse is sound.

## Rationale
- **One principle, four facets, not four features.** ADR-025 established frame-level zero-copy for lookups; treating serve encoding, CDC decode, row-vec lifetime, and the reply channel as the same "no per-request/per-event allocation" rule keeps the hot paths uniformly allocation-free rather than leaving residual allocations that dominate once the obvious ones are gone.
- **View, don't copy, when the bytes already live in a frame.** Both the serve response (`Bytes` chunks over codec frames and static messages) and CDC decode (`ByteString` over the replication frame) exploit refcounted buffers that already exist and outlive the operation, so a copy would be pure overhead. The view's lifetime cost is pinning a frame bounded by one row/response.
- **Pool what must be owned.** Row vectors can't be views (they're rebuilt per event and handed to the writer), so they're pooled and recycled instead, with a bounded pool cap so the pool can't grow without limit under a burst.
- **A reusable slot over a per-query channel** is sound only because of an existing invariant (one outstanding query per connection); the slot's mint-time state reset plus an explicit `ReplyState` (`Empty`/`Sent`/`Dropped`) is what lets a reused `Notify` tolerate a stale permit from a never-awaited sender, which a fresh per-query `oneshot` would not have to reason about. This is the cost paid to remove the allocation. The design went directly from the per-query `oneshot` to the reusable slot; pooling `oneshot`s was not considered (a pool still reallocates each single-use channel, whereas the slot is reused in place).
- **A compile-time layout assertion guards the in-place collect** because the allocation reuse is a silent optimization the compiler may or may not apply; failing the build on a layout divergence is the only way to keep the regression visible.

## Consequences

### Positive
- No per-hit heap allocation on the serve path after warmup (response buffer, SQL rendering, LIMIT/OFFSET binds, and reply channel all reuse), and no per-event allocation on the CDC apply path (tuple decode, value conversion, and row vectors all reuse or view) — lower tail latency, less allocator contention across workers, and higher CDC apply throughput (less slot lag).
- Zero-copy `ByteString` decode means cloning a decoded CDC value (e.g. into a batch overlay) is a refcount bump, not a text copy.
- The compile-time layout assertion turns a future silent regression in tuple decode into a build failure.

### Negative
- The reusable reply slot is more subtle than a per-query `oneshot`: a reused `Notify` can surface a stale permit, so the receiver must consult `ReplyState` rather than trusting a bare wakeup, and the sender has distinct drop/disarm paths. This is hot-path correctness logic that a fresh-channel design would not need.
- Zero-copy `ByteString` and pooled row vectors pin and retain memory: a view keeps its replication frame alive for the value's lifetime, and the row-vec pool holds recycled allocations up to its cap, so steady-state resident memory is traded for allocation-rate.
- The `WriteQueue` spill still allocates when a slow client lets many chunks accumulate live; the no-allocation guarantee is the common case, not a hard bound.

## Implementation Notes
- Serve response assembly and the serve state machine: `src/cache/serve.rs`, using `WriteQueue` (`src/cache/write_queue.rs`) — an inline `Bytes` ring (`INLINE_CAP` chunks) with a `VecDeque` spill, written via vectored I/O. Fixed protocol frames come from `src/pg/protocol/encode.rs` constants as `Bytes::from_static`. SQL rendering reuses the connection's `sql_buf`; LIMIT/OFFSET binds format via stack `itoa::Buffer`.
- Reusable reply channel: `ReplySlot`/`ReplySender`/`ReplyState` in `src/cache/reply.rs`; the `ReplySender` is moved through `ProxyMessage` (`src/cache/messages.rs`) to the worker.
- CDC zero-copy decode: `tuple_data_parse` and the `size_of`/`align_of` assertion in `src/cache/cdc.rs`; `CdcValue` (`Null`/`Text(ByteString)`/`Toasted`) and `cdc_values_convert` in `src/cache/messages.rs`; `ByteString` itself in `src/pg/protocol/mod.rs`.
- Row-vec pooling: `row_vec_pool` on `WriterCore` (`src/cache/writer/core.rs`), with the pop/convert and recycle helpers and the `ROW_VEC_POOL_MAX` cap in `src/cache/writer/frame.rs`.
- `ByteString` also backs zero-copy SQL views in the extended-query path (`src/pg/protocol/extended.rs`), the same refcounted-view technique applied off the CDC path.
