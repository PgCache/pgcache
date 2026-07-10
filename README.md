# pgcache

**A PostgreSQL cache that never serves stale data.**

![License: Elastic License 2.0](https://img.shields.io/badge/license-Elastic%202.0-blue)
![PostgreSQL 16 | 17 | 18](https://img.shields.io/badge/PostgreSQL-16%20%7C%2017%20%7C%2018-336791)
![Status: active development](https://img.shields.io/badge/status-active%20development-orange)

Most caches force a choice between speed and correctness. pgcache doesn't. It's a
wire-compatible PostgreSQL proxy that caches query results and keeps them consistent
with the origin in real time using logical replication — every insert, update, and
delete is either applied *in place* to the cached result or the affected query is
invalidated. A cached query is therefore always either up to date or transparently
forwarded to origin. Never a stale read, and not a line of application code changes.

Think of it as a **smart read replica that copies only the data you actually read** —
the hot working set driving most of your traffic, instead of a full second copy of a
database that only keeps growing. Learn more at [pgcache.com](https://www.pgcache.com).

## What it is

pgcache sits between your application and PostgreSQL and speaks the same wire protocol,
so applications connect to it exactly as they would to Postgres. For each query it:

- **Analyzes** the query to decide whether it is cacheable.
- **Serves** cacheable queries from its local cache.
- **Forwards** everything else straight to the origin database, unchanged.
- **Maintains consistency** by subscribing to the origin's logical replication stream
  (CDC) and reconciling every committed change against the cache.

No Redis, no schema migration, no manual invalidation logic.

## Quickstart

pgcache needs three things: your **origin** database with logical replication enabled, a
dedicated **cache** PostgreSQL that has the [`pgcache_pgrx`](#the-pgcache_pgrx-extension)
extension, and a config file (or CLI flags) telling it how to reach both.

**1.** Enable logical replication on the origin:

```ini
# postgresql.conf (origin)
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
```

pgcache creates and manages the publication and replication slot itself (named in the
config below) — you don't create them by hand.

**2.** Tell pgcache how to reach both databases. The simplest way is a config file:

```toml
# pgcache.toml
num_workers = 4

[origin]
host = "your-db-host"
port = 5432
user = "pgcache"
database = "myapp"

[cache]
host = "localhost"
port = 5432
user = "postgres"
database = "cache"

[cdc]
publication_name = "pgcache_pub"
slot_name = "pgcache_slot"

[listen]
socket = "0.0.0.0:6432"
```

```bash
pgcache -c pgcache.toml
```

Every setting also has an equivalent CLI flag (`--origin_host`, `--cache_host`,
`--cdc_publication_name`, `--listen_socket`, `--num_workers`, …) and environment variable;
run `pgcache --help` for the full list.

**3.** Point your application at pgcache by changing one connection string:

```diff
- DATABASE_URL=postgres://user@your-db-host:5432/myapp
+ DATABASE_URL=postgres://user@pgcache-host:6432/myapp
```

Writes still go to your primary; reads are served from cache when safe and forwarded to
origin otherwise.

> **Just want to try it?** The [Docker image](../pgcache-docker) bundles the cache
> PostgreSQL (with `pgcache_pgrx` already preloaded) and wraps all of the above behind a
> single `--upstream postgres://…` flag.

## How it works

```
                 reads + writes
   application ───────────────────▶  pgcache  ──────────────▶  origin
       ▲                             │  cache       writes &     PostgreSQL
       └─────────────────────────────┘  cacheable   forwarded
            results                       reads      queries
                                            ▲
                                            │  logical replication (CDC)
                                            └──────────────────────────────
                                               every commit, in real time
```

The cache is kept consistent through PostgreSQL's logical replication stream rather than
by expiring data on a timer. When a change arrives, pgcache decides whether it can be
applied to the cache directly or whether the affected query must be invalidated:

- **Single-table queries are never invalidated.** An insert, update, or delete can always
  be applied directly to the cached result, keeping it consistent without re-fetching.
- **Joins and complex queries** turn on whether the change requires fetching more data from origin or if the data already in the cache can still serve the query after the change. If the change requires fetching data from origin, the query is invalidated, and the up-to-date data will fetched on the next request.

At any moment a cached query is either up to date with every relevant committed change, or it
is not served from cache at all — so applications never see data a committed write has
superseded.

## The pgcache_pgrx extension

pgcache keeps its cached data in a dedicated PostgreSQL instance, and that instance **must**
have the [`pgcache_pgrx`](../pgcache_pgrx) extension installed. pgcache runs `CREATE EXTENSION pgcache_pgrx` when it initializes the cache database on startup, and it will not run without it.

`pgcache_pgrx` is a small extension (built with [pgrx](https://github.com/pgcentralfoundation/pgrx))
that provides the generation-based tracking pgcache uses to garbage-collect the cache. The extension
records which cached rows are still in use so unused rows can be reclaimed.

Because it allocates shared memory, it must be preloaded, not just created:

```ini
# postgresql.conf of the cache database
shared_preload_libraries = 'pgcache_pgrx'
```

You usually don't set this up by hand: the Docker image and the AWS Marketplace AMI bundle a
PostgreSQL that already has `pgcache_pgrx` built, installed, and preloaded. You only need to
install it yourself when running pgcache from source or against a PostgreSQL you manage — see
[`pgcache_pgrx`](../pgcache_pgrx) for build and install steps.

## Status

pgcache is under active development and not yet recommended for production use.

- **PostgreSQL:** 16, 17, and 18.
- **Deployment:** AWS (Marketplace AMI), Docker, or bare metal.
- **Caching scope:** tables must have a primary key to be cached; queries against tables
  without one are forwarded to origin unchanged.

## Design & internals

Full documentation lives at [pgcache.com/docs](https://www.pgcache.com/docs).

The architecture and the reasoning behind it are documented in-repo as Architecture
Decision Records. Start with the [ADR index](ADR/README.md) for a map of the subsystems —
query parsing and cacheability analysis, the CDC apply model, cache population, and the
generation-based garbage collector.

## License

pgcache is source-available under the [Elastic License 2.0](LICENSE.txt).
