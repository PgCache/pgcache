# pgcache-fit

Offline "will pgcache help my workload?" analyzer. Point it at a trace of your
SQL and it tells you which statements pgcache would cache, which it would pass
through to the origin (and why), and an upper bound on the cache hit rate — all
without a running pgcache, a running database, or a schema dump.

It runs pgcache's *actual* query-analysis pipeline (cacheability, resolution,
constraint analysis, admission) against your queries, so a verdict here is the
verdict the proxy would reach. It does not connect to anything or send your
queries anywhere.

## Build

pgcache-fit is a member of the pgcache workspace:

```sh
cargo build --release -p pgcache-fit
# binary at ../target/release/pgcache-fit
```

## Usage

```sh
pgcache-fit check   <trace>   # classify statements: cacheable / passthrough / write
pgcache-fit hitrate <trace>   # [experimental] ceiling on the cache hit rate
```

Both accept `--json` for machine-readable output and `--format <fmt>` to
override input auto-detection.

### `check`

```
$ pgcache-fit check queries.sql
pgcache-fit check — 6 statements, 6 calls

Cacheable:    50.0% of calls (3 statements)
Passthrough:  33.3% of calls
  unsupported FROM clause          16.7% of calls (1)
  non-immutable function           16.7% of calls (1)
Writes:       16.7% of calls (1 statements)

Write mix by table:
  users                           1 calls

Shapes: 6 distinct statements → 3 fingerprints → 3 shapes → 3 after subsumption

Assumptions (schema-less mode):
  ...
```

- **Cacheable** — pgcache would cache this SELECT.
- **Passthrough** — a SELECT pgcache would forward to the origin, grouped by
  reason (unsupported construct, non-immutable function, system-catalog
  reference, and so on).
- **Writes** — INSERT/UPDATE/DELETE, with a per-table breakdown. In the proxy
  these drive cache invalidation; here they're only counted.
- **Shapes** — how the distinct statements collapse into fingerprints, query
  shapes, and finally shapes after subsumption (one cached query serving
  several). Fewer shapes means better cache density.

`--json` additionally emits a per-statement verdict list (each statement, its
verdict, and the passthrough reason).

### `hitrate` (experimental)

Replays the trace *in arrival order* against an infinite cache and reports the
ceiling on cacheable hit rate. The replay itself is faithful — it runs
pgcache's real serve-time decision (admission threshold, LIMIT sufficiency,
subsumption, the in-transaction gate) — but it does **not** yet model
write-driven invalidation, which is the dominant effect on a real hit rate.
Read the number as a ceiling, and expect it to change once invalidation
simulation lands.

```
$ pgcache-fit hitrate queries.sql
pgcache-fit hitrate [experimental] — 6 statements, 6 calls

Writes:        1 calls (invalidation not simulated — future mode)
Utility:       0 calls
Non-cacheable: 2 calls
Cacheable:     3 calls
  hits              0
  subsumption hits  0
  cold misses       3

Hit rate: 0.0% of cacheable SELECTs / 0.0% of all SELECTs / 0.0% of all statements
```

`--admission-threshold N` matches pgcache's `admission_threshold`: a query
isn't registered (and so is forwarded) until its Nth sighting. Default 1
(register on first sight).

Because invalidation isn't simulated, a "hit" here is a hit only if nothing
wrote to the underlying tables in between — so the ceiling is tight for a
read-heavy workload and increasingly optimistic the more you write to cached
tables. Ordering matters, so `hitrate` needs a real trace (a `.sql` script or
a log) — it rejects `pg_stat_statements` input, which is pre-normalized and has
no arrival order.

## Capturing a trace

pgcache-fit auto-detects four input formats.

**Plain SQL** (`.sql`) — one or more statements, semicolon-separated. Good for a
quick check of a handful of queries.

**pg_stat_statements** (CSV, `check` only) — a snapshot of the queries your
database has actually run. Statements are already normalized to `$1, $2, …`,
which is exactly the shape `check` reasons about:

```sql
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;  -- once, then let it accumulate
\copy (SELECT query, calls FROM pg_stat_statements) TO 'workload.csv' WITH CSV HEADER
```

**PostgreSQL csvlog** — a real trace with arrival order, usable by both
subcommands. Turn on statement logging (a session, or `postgresql.conf` +
reload):

```
log_destination = 'csvlog'
logging_collector = on
log_statement = 'all'        # or: log_min_duration_statement = 0
```

Then feed the `*.csv` file from your log directory to pgcache-fit.

**PostgreSQL stderr log** (best-effort) — if you already have `log_statement`
output going to a stderr-format logfile, pgcache-fit can parse `statement:` /
`execute` lines from it. csvlog is more reliable; prefer it when you can.

## Schema-less mode (and its caveats)

pgcache-fit does not read your schema. It synthesizes a catalog from the query
corpus itself, so every report ends with an explicit assumptions block:

- every table is assumed to have a primary key;
- every relation is assumed to be a table (views can't be told apart without a
  schema);
- unqualified names are assumed to be in schema `public`;
- enum/composite types aren't detectable;
- function volatility comes from a builtin PostgreSQL snapshot — unknown
  (extension or user-defined) functions are treated as non-immutable, i.e.
  passthrough;
- column types are inferred from literal comparisons where possible.

These assumptions are conservative in pgcache's favor for a couple of them
(notably the primary-key assumption — pgcache only caches tables that have a
PK), so treat the cacheable percentage as optimistic where your tables lack
PKs or where "tables" are really views.

## Not simulated yet (planned)

- write-driven cache invalidation (hitrate is an infinite-cache upper bound);
- schema-dump input (`--schema`) and live-database catalogs;
