# ADR-069: A second cache PostgreSQL on Amazon Linux 2's glibc

## Status
Accepted

## Context
ADR-068 made the startup ordering probe fail closed, and a real RDS origin showed what that costs: RDS and Aurora collate with Amazon Linux 2's glibc (`datcollversion 2.26-59.amzn2`), whose ISO 14651 tables predate the glibc 2.28 rewrite. Under `en_US.UTF-8` RDS ignores spaces and punctuation at the first level (`a` before ` a` and `_a`), while trixie's glibc 2.41 and Amazon Linux 2023's 2.34 do not, so the Debian image and the Marketplace AMI both refused to start behind RDS, their main audience (PGC-587, PGC-594).

Collation is computed by `strcoll` inside the libc a server binary runs against, from that libc's tables. Nothing in PostgreSQL swaps it, and no current distribution ships glibc 2.26.

## Decision
- **The image carries two cache PostgreSQL builds.** The Debian one from the base image, and a second PostgreSQL 18 compiled on `amazonlinux:2`, installed under `/opt/pg-al2` together with AL2's loader, libc, the libraries it resolves and every UTF-8 locale as `LOCPATH` directories. Its binaries and modules are `patchelf`ed to that interpreter and library directory (DT_RPATH, so glibc's NSS modules resolve there too), so it runs on the Debian root without a chroot.
- **The entrypoint picks the flavour before `initdb`.** `pgcache --origin_locale` prints origin's provider, locale and collation version under the resolved settings; a libc origin whose glibc is older than 2.28 gets the AL2 build, everything else the host's own (`native`) build, and `CACHE_PG_FLAVOUR` overrides. A data directory remembers the flavour that initialised it and is re-initialised when the choice changes.
- **The probe still fails closed.** An origin that neither flavour orders like is refused with the disagreeing pair named, as in ADR-068. There is no per-query degrade.

## Rationale
- **Exact parity beats approximation.** The same glibc lineage gives byte-identical collation tables; the probe confirmed RDS and the AL2 build agree on the whole sample where Debian differs on eleven positions.
- **The split is at glibc 2.28, not at "RDS".** Every libc origin older than 2.28 (RDS, Aurora, self-managed Amazon Linux 2, Ubuntu 18.04) shares the old tables, so the selector is a version rule rather than a vendor rule, and the probe catches any origin the rule misjudges.
- **A second server, not a second image.** One image serving both origin kinds keeps one tag, one entrypoint and one health check; the AL2 tree costs about 37 MB compressed, most of it locale data.
- **Fail closed rather than forward collation-sensitive queries.** Forwarding would need a complete enumeration of every ordering position in the query tree and would silently cost cache hits on exactly the queries operators notice; a flavour that matches is both simpler and faster.

## Consequences

### Positive
- RDS and Aurora origins start and serve text ordering as origin does, on the image and on the AMI.
- The selector and the AL2 cluster are testable without RDS: the suite checks the selector's rule and that the AL2 cluster orders the sample as glibc 2.26 does.

### Negative
- AL2's glibc is past standard end of life. The copied files carry no package metadata, so they do not appear in the image's SBOM or in scanners; the image README documents the build and its exposure instead. It backs a PostgreSQL that listens only on localhost for pgcache.
- Two server builds mean two `pgcache_pgrx` builds and a longer image build (PostgreSQL from source on AL2, `cargo-pgrx` against `openssl11`).
- The AL2 build has no OpenSSL, ICU or readline; none is needed for the cache cluster.

## Implementation Notes
Stage 3 of `pgcache-docker/Dockerfile` builds and collects the tree; `pgcache-docker/pgcache-cache-flavour` holds the version rule; the entrypoint's flavour block sets `PG_BIN`, `LOCPATH` and the `initdb` locale (`C`, since glibc 2.26 has no `C.UTF-8`). `cache/runtime/database_locale.rs` provides `database_locale_describe` behind `--origin_locale`.
