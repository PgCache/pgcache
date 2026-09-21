# ADR-051: RaW tier saturation folds into the newest tier

## Status

Accepted

## Context

The per-connection read-after-write gate (ADR-048) batches forwarded writes
into per-table tiers, each stamped with a commit-LSN bound and cleared when
the settled watermark passes it. The tier queue was capped at 2; at
saturation the two *oldest* tiers merged under the later of their bounds.

Under sustained churn that policy is pathological: every new stamp re-pushes
the merged (oldest) tier's bound, so the longest-waiting writes' clearance
requirement rides one tier behind the head of the write stream. Once settle
lag exceeds `cap × per-table stamp interval`, gated reads stop clearing when
*their* writes settle and instead wait for full catch-up — the collapse
regime measured in PGC-440 (merges ≈ 1/write, hit rate halved, forwards
unbounded while churn persists), where a bounded settle lag `L` should cost
only ~`L` of forwarding per write.

## Decision

1. **At saturation, fold the incoming batch into the *newest* waiting tier**
   under the later bound. Older tiers keep their anchored bounds and drain on
   schedule; coarsening lands on the writes whose clearance is farthest away
   regardless.
2. **Raise the tier cap to 8** (`WAITING_TIERS_MAX`), widening the merge-free
   lag tolerance from ~2 to ~8 stamp intervals. The SmallVec inline size
   stays 2: depth beyond that exists only while settle lags, and heap-spills.
   The cap is a constant, not a setting, until evidence demands otherwise.

## Rationale

- Any merge must gate the union under the later bound — that is forced by
  soundness. The only free choice is *who pays*: merge-oldest starves the
  readers who have waited longest; merge-newest charges the writes that were
  going to wait longest anyway. Same memory, same invariants.
- Anchored bounds also fix the forward-attribution metric (PGC-440): a merged
  bound no longer overstates the blocking write's true clearance requirement.
- The tolerance formula (`no merges while settle lag < cap × stamp
  interval`) makes 8 a measured choice: local settle lag is milliseconds
  against ~150ms stamp spacing (never saturates); the AWS bench's
  multi-second lag against ~1s spacing saturated a cap of 2 constantly and
  fits inside 8.

## Consequences

### Positive

- Sustained settle lag `L` degrades to ~`L` of forwarding per write instead
  of indefinite forwarding; recovery no longer requires full catch-up.
- `tier_merges` remains the saturation signal (now meaning head-tier
  precision loss, not a clearance treadmill).

### Negative

- Reads intersecting the newest writes wait on the folded head bound during
  saturation — bounded by the head's natural clearance distance.
- Per-table worst-case gate memory rises with the deeper cap (bounded by the
  existing per-aggregate predicate caps), and `decide()` scans up to 8 tiers
  on a table with pending writes; both terms exist only while settle lags.

## Implementation Notes

- `proxy/connection/write_log/tiers.rs` — `TableTiers::stamp` fold-at-cap,
  `WAITING_TIERS_MAX`; the connection scope is a single slot and unchanged.
- Depth occupancy is self-adaptive below the cap (tiers exist only per
  unsettled probe batch); a future depth controller would only move the cap.
- Validated against PGC-440's local harness arms; see the ticket for the
  before/after merge-rate and forward data.
