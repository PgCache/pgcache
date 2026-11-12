#!/bin/sh
# Regenerate the builtin pg_proc snapshots from a current PostgreSQL.
# The functions query is FUNCTION_CATALOG_SQL (pgcache/src/catalog/function.rs)
# restricted to the schemas PostgreSQL ships; keep the column list in step.
# The columns after the twelfth (identity, body digest, the body and what
# inlining it needs) serve the writer's check of what the cache database
# holds and the body inliner, neither of which fit makes for a builtin, so
# they are cut.
# Usage: functions_regenerate.sh [psql options]
set -eu
cd "$(dirname "$0")"
version=$(psql "$@" -Atc "SHOW server_version")
{
  echo "# Builtin pg_proc snapshot (PostgreSQL $version): the columns of"
  echo "# FUNCTION_CATALOG_SQL, pg_catalog and information_schema only."
  echo "# Regenerate with functions_regenerate.sh."
  psql "$@" -At -F "$(printf '\t')" -c "SELECT p.oid, n.nspname, p.proname, \
        p.pronargs, p.pronargdefaults, p.provariadic <> 0, p.provolatile::text, \
        l.lanname, p.prosecdef, p.proretset, p.prorettype, p.prokind::text, \
        format('%I.%I(%s)', n.nspname, p.proname, oidvectortypes(p.proargtypes)), \
        md5(coalesce(pg_get_function_sqlbody(p.oid), p.prosrc)), \
        p.proisstrict, p.proconfig IS NOT NULL, p.proargnames, \
        oidvectortypes(p.proargtypes), format_type(p.prorettype, NULL), \
        CASE WHEN l.lanname = 'sql' AND p.prosqlbody IS NULL \
              AND rt.typtype IN ('b', 'd', 'e', 'r') THEN p.prosrc END \
 FROM pg_proc p \
 JOIN pg_namespace n ON p.pronamespace = n.oid \
 JOIN pg_language l ON l.oid = p.prolang \
 JOIN pg_type rt ON rt.oid = p.prorettype \
 WHERE n.nspname IN ('pg_catalog', 'information_schema') \
 ORDER BY n.nspname, p.proname, p.oid" | cut -f 1-12
} > functions.tsv
{
  echo "# Builtin aggregate names (PostgreSQL $version): AGGREGATE_FUNCTIONS_SQL"
  echo "# over pg_catalog. Regenerate with functions_regenerate.sh."
  psql "$@" -Atc "SELECT DISTINCT lower(p.proname) FROM pg_proc p \
 WHERE p.prokind = 'a' AND p.pronamespace = 'pg_catalog'::regnamespace ORDER BY 1"
} > aggregates.tsv
