# ADR-068: Cache database under origin's locale, on a glibc host

## Status
Accepted

## Context
Text served from cache is ordered by the cache PostgreSQL: every `ORDER BY`, `min`/`max` and range predicate over a text column is evaluated there, under the cache database's default collation. The cache database was created with the cache cluster's template default, and the Docker image ran that cluster on Alpine, where musl has no collation support: `strcoll` is `strcmp`, `localeconv` is fixed to C, and `setlocale` accepts any name. The cluster reported `en_US.utf8` and sorted by code point, so a cached `ORDER BY name` could disagree with origin on every mixed-case or accented value, and the startup check that applies origin's `lc_monetary`, `lc_numeric` and `lc_time` on the cache (ADR-056) passed vacuously (PGC-587).

Nothing installable fixes musl; ICU at `initdb` bypasses libc but matches only an ICU origin, and most origins collate through libc. Wolfi (glibc, apk, small) was evaluated and rejected for the build rewrite it needs and for Chainguard's tag and retention policies. Even a glibc cache does not guarantee origin's order: Amazon RDS pins a collation library compatible with glibc 2.26 on purpose, and ICU and glibc disagree on punctuation.

## Decision
- **The cache database mirrors origin's database locale.** At setup pgcache reads origin's `pg_database` row (provider, encoding, collate, ctype, ICU locale and rules, collation version) and creates the cache database from `template0` with the same values. A locale the cache cluster cannot create refuses startup; there is no fallback to the template default.
- **A startup ordering probe fails closed.** pgcache sorts one fixed sample of strings (case pairs, digits, spaces, hyphens, punctuation, accents, `ß`, a few scripts) on origin and on the new cache database and refuses to start when the orders differ, naming the first disagreeing pair and both locales. A collation version difference with an identical order is a warning.
- **The Docker image runs on Debian with every glibc locale.** `postgres:18-trixie` with `locales-all` for all three build stages; `initdb` uses a neutral `C.UTF-8` cluster default. The image's `HEALTHCHECK` runs `pgcache --healthcheck`, since the Debian image ships neither `wget` nor `curl`.

## Rationale
- **Parity, not stability, is the requirement.** The cache database is dropped and recreated on every start, so a glibc upgrade can never corrupt a persisted index; what matters is that the cache orders text as origin does today. Mirroring the locale name gets the same rules requested; the probe verifies the host honours them.
- **A wrong order is a correctness failure, so the probe refuses rather than warns.** The constraint machinery already treats string ordering as undecidable (PGC-446), so the only consumer of collation order is the served result itself, and there is no degraded mode that keeps it right.
- **Debian is the smallest change that gives real locales.** The official image already installs PGDG, creates `postgres` as UID 999 as the entrypoint assumed, and keeps the `locales` package; the build keeps its three-stage shape and drops the musl `crt-static` workaround.
- **`locales-all` over generating locales at startup.** Generating on demand would need origin's locale before `initdb`, a writable locale archive for a non-root process and `LOCPATH` on the server; the extra space is cheaper than that machinery.

## Consequences

### Positive
- A text `ORDER BY` served from cache returns origin's order on glibc origins from Debian, Ubuntu, Amazon Linux 2023 and the Kubernetes operators.
- The `lc_monetary`, `lc_numeric` and `lc_time` gate becomes real: glibc rejects an uninstalled locale, so the refusal and its message now hold.
- An origin the cache host cannot order like refuses startup with the disagreeing pair named, instead of serving reordered results silently.
- Column-level collations stay the per-column work of PGC-459; the database default is what this ADR fixes.

### Negative
- About 110 MB more compressed image, almost all locale data; the Debian package set scans with fewer findings than the Alpine one did.
- An RDS or Aurora origin needs a cache PostgreSQL on the same glibc lineage; ADR-069 adds one to the image.
- Mirroring adds one origin round trip at setup and a second `pg_database` read on the cache; both are startup-only.

## Implementation Notes
`cache/runtime/database_locale.rs` holds the probe and the `CREATE DATABASE` builder, version-aware over the `pg_database` columns that arrived in 15, 16 and 17; `cache/runtime/collation_probe.rs` holds the sample and the comparison, with a fault-injection hook that reverses the cache side for the refusal test. `pgcache-docker/Dockerfile` carries the base-image reasoning in its header.
