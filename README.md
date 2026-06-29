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

Run pgcache in front of your database with Docker:

```bash
docker run -d -p 5432:5432 pgcache:latest \
  --upstream postgres://user@your-db-host:5432/myapp
```

The origin database needs logical replication enabled and a publication for pgcache to
subscribe to:

```ini
# postgresql.conf
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
```

```sql
-- on the origin database
CREATE PUBLICATION pgcache_pub FOR ALL TABLES;
```

Then point your application at pgcache by changing one connection string:

```diff
- DATABASE_URL=postgres://user@your-db-host:5432/myapp
+ DATABASE_URL=postgres://user@pgcache-host:5432/myapp
```

Writes still go to your primary; reads are served from cache when safe and forwarded to
origin otherwise. Common options: `--listen` (default `0.0.0.0:5432`), `--workers`,
`--publication` (default `pgcache_pub`).

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
