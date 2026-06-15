//! Loader for the upstream PostgreSQL regression fixtures.
//!
//! Phase 1 ports `aggregates.sql`/`select.sql`/etc., which reference the
//! canonical `onek` pseudo-random table. Its data file is vendored under
//! `conformance/data/` (see `data/ATTRIBUTION.md`) and embedded at
//! compile time so the binary is self-contained.

use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::SinkExt;
use futures_util::pin_mut;
use tokio_postgres::Client;

/// `onek` schema from `src/test/regress/sql/test_setup.sql`, plus a
/// synthetic `PRIMARY KEY (unique1)` not present upstream. pgcache only
/// caches tables with a primary key (PGC-135: `table.rs` metadata SQL
/// inner-joins `pg_constraint` on `contype='p'`); `unique1` is a unique,
/// non-null 0..999 permutation so it is a valid key over the vendored
/// data.
const ONEK_DDL: &str = "CREATE TABLE onek (
    unique1     int4 PRIMARY KEY,
    unique2     int4,
    two         int4,
    four        int4,
    ten         int4,
    twenty      int4,
    hundred     int4,
    thousand    int4,
    twothousand int4,
    fivethous   int4,
    tenthous    int4,
    odd         int4,
    even        int4,
    stringu1    name,
    stringu2    name,
    string4     name
)";

/// 1000 rows, tab-delimited, in PostgreSQL COPY text format.
const ONEK_DATA: &str = include_str!("../data/onek.data");

/// `J1_TBL` / `J2_TBL` from `src/test/regress/sql/join.sql`, verbatim.
/// These have NULLs and a duplicate `(5,-5)` row, so no natural key
/// exists — they forward (no PK). Kept at their exact upstream column
/// count so the column-alias-list tests (`J1_TBL t1 (a, b, c)`) work.
const J1_TBL_DDL: &str = "CREATE TABLE j1_tbl (i integer, j integer, t text)";

const J1_TBL_DATA: &str = "INSERT INTO j1_tbl VALUES
    (1, 4, 'one'), (2, 3, 'two'), (3, 2, 'three'), (4, 1, 'four'),
    (5, 0, 'five'), (6, 6, 'six'), (7, 7, 'seven'), (8, 8, 'eight'),
    (0, NULL, 'zero'), (NULL, NULL, 'null'), (NULL, 0, 'zero')";

const J2_TBL_DDL: &str = "CREATE TABLE j2_tbl (i integer, k integer)";

const J2_TBL_DATA: &str = "INSERT INTO j2_tbl VALUES
    (1, -1), (2, 2), (3, -3), (2, 4), (5, -5), (5, -5),
    (0, NULL), (NULL, NULL), (NULL, 0)";

/// `t1`/`t2`/`t3` (multiway/natural-join section) and `x`/`y` (outer-join
/// nullability section) from join.sql. A PRIMARY KEY is added on the
/// naturally-unique column (`name`, `x1`/`y1`) for cache coverage; no
/// extra column is introduced, so the column-alias tests still line up.
/// `(table, ddl, insert)`.
const JOIN_AUX_TABLES: &[(&str, &str, &str)] = &[
    (
        "t1",
        "CREATE TABLE t1 (name text PRIMARY KEY, n integer)",
        "INSERT INTO t1 VALUES ('bb', 11)",
    ),
    (
        "t2",
        "CREATE TABLE t2 (name text PRIMARY KEY, n integer)",
        "INSERT INTO t2 VALUES ('bb', 12), ('cc', 22), ('ee', 42)",
    ),
    (
        "t3",
        "CREATE TABLE t3 (name text PRIMARY KEY, n integer)",
        "INSERT INTO t3 VALUES ('bb', 13), ('cc', 23), ('dd', 33)",
    ),
    (
        "x",
        "CREATE TABLE x (x1 integer PRIMARY KEY, x2 integer)",
        "INSERT INTO x VALUES (1,11), (2,22), (3,NULL), (4,44), (5,NULL)",
    ),
    (
        "y",
        "CREATE TABLE y (y1 integer PRIMARY KEY, y2 integer)",
        "INSERT INTO y VALUES (1,111), (2,222), (3,333), (4,NULL)",
    ),
];

/// `foo` from `src/test/regress/sql/select.sql` (the ORDER BY / NULLS
/// section). Upstream `foo (f1 int)` with rows `(42),(3),(10),(7),
/// (null),(null),(1)`. A surrogate `foo_pk` is appended (pgcache caches
/// only tables with a PK — PGC-135) so the NULL-ordering queries are
/// actually served from cache, not just forwarded; suites add it as a
/// deterministic ORDER BY tiebreaker since the two NULL rows' relative
/// order is otherwise unspecified.
const FOO_DDL: &str = "CREATE TABLE foo (
    f1     integer,
    foo_pk integer PRIMARY KEY
)";

const FOO_DATA: &str = "INSERT INTO foo (f1, foo_pk) VALUES
    (42, 1),
    (3, 2),
    (10, 3),
    (7, 4),
    (NULL, 5),
    (NULL, 6),
    (1, 7)";

/// Quote a PostgreSQL identifier for safe interpolation.
fn ident_quote(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Add `table` to pgcache's CDC publication so its rows replicate to the
/// cache. Idempotent. A no-op when no publication is configured (an
/// external pgcache that hasn't provisioned one yet); the table simply
/// won't be cacheable, which the suite annotations tolerate.
async fn publication_table_ensure(
    client: &Client,
    publication: Option<&str>,
    table: &str,
) -> Result<()> {
    let Some(pubname) = publication else {
        tracing::warn!(
            table,
            "no --publication given; table will not be added to pgcache's CDC \
             publication and may not replicate"
        );
        return Ok(());
    };
    let already: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_publication_tables \
             WHERE pubname = $1 AND schemaname = 'public' AND tablename = $2)",
            &[&pubname, &table],
        )
        .await
        .context("checking publication membership")?
        .get(0);
    if already {
        tracing::info!(publication = pubname, table, "already in publication");
        return Ok(());
    }
    client
        .batch_execute(&format!(
            "ALTER PUBLICATION {} ADD TABLE {}",
            ident_quote(pubname),
            ident_quote(table)
        ))
        .await
        .with_context(|| format!("adding {table} to publication {pubname} (does it exist?)"))?;
    tracing::info!(publication = pubname, table, "added to publication");
    Ok(())
}

/// Create and populate `j1_tbl` + `j2_tbl` on origin (idempotent), and
/// add them to pgcache's publication so the join suite can assert
/// `cached` routing. Mirrors [`onek_load`]; data is small enough to
/// inline rather than vendor a file.
pub async fn join_tables_load(client: &Client, publication: Option<&str>) -> Result<()> {
    client
        .batch_execute("DROP TABLE IF EXISTS j1_tbl, j2_tbl, t1, t2, t3, x, y CASCADE")
        .await
        .context("dropping existing join fixtures")?;
    // j1_tbl/j2_tbl are keyless → forwarded, and a keyless table must not
    // enter the CDC publication (pgcache's decoder stalls on changes for a
    // relation with no replica identity). The keyed aux tables are published.
    let tables = [
        ("j1_tbl", J1_TBL_DDL, J1_TBL_DATA, false),
        ("j2_tbl", J2_TBL_DDL, J2_TBL_DATA, false),
    ]
    .into_iter()
    .chain(JOIN_AUX_TABLES.iter().map(|(t, d, i)| (*t, *d, *i, true)));
    for (table, ddl, data, publish) in tables {
        client
            .batch_execute(ddl)
            .await
            .with_context(|| format!("creating {table}"))?;
        if publish {
            publication_table_ensure(client, publication, table).await?;
        }
        client
            .batch_execute(data)
            .await
            .with_context(|| format!("loading {table} data"))?;
        tracing::info!(table, "loaded join fixture");
    }
    Ok(())
}

// ──────────────────────────────────────────────────────────────────────
// Shared regression environment (src/test/regress/sql/test_setup.sql).
//
// These tables are referenced by the bulk of the regress query files. Data
// is verbatim from upstream (inline tables) or the vendored COPY data files
// (tenk/person/emp/student/stud_emp/streets). The only deviation from
// upstream is a PRIMARY KEY added on naturally-unique columns (onek2 /
// tenk1 / tenk2 unique1, the int/float/text single-value tables) so those
// tables are cacheable — pgcache forwards PK-less tables, which is correct
// but leaves the cache path unexercised. Tables with no natural key
// (char/varchar/point, the person inheritance chain, the road path tables)
// are kept verbatim and simply forward. Indexes, tablespaces, and the C
// helper functions from test_setup.sql are omitted: indexes/tablespaces
// affect only plans (not results), and the C functions need a compiled
// regress.so used solely by the catalog-sanity tests, not the query files.
// ──────────────────────────────────────────────────────────────────────

const TENK_DATA: &str = include_str!("../data/tenk.data");
const PERSON_DATA: &str = include_str!("../data/person.data");
const EMP_DATA: &str = include_str!("../data/emp.data");
const STUDENT_DATA: &str = include_str!("../data/student.data");
const STUD_EMP_DATA: &str = include_str!("../data/stud_emp.data");
const STREETS_DATA: &str = include_str!("../data/streets.data");

/// Inline-INSERT shared tables (verbatim values from test_setup.sql; a PK
/// is added on unique columns). `(table, ddl, insert, publish)` — only
/// PK-bearing tables are added to the CDC publication; the keyless ones
/// (char/varchar/point) forward, and point's geometric type must not enter
/// the replication stream.
const INLINE_TABLES: &[(&str, &str, &str, bool)] = &[
    (
        "char_tbl",
        "CREATE TABLE char_tbl (f1 char(4))",
        "INSERT INTO char_tbl (f1) VALUES ('a'),('ab'),('abcd'),('abcd    ')",
        false,
    ),
    (
        "float8_tbl",
        "CREATE TABLE float8_tbl (f1 float8 PRIMARY KEY)",
        "INSERT INTO float8_tbl(f1) VALUES \
         ('0.0'),('-34.84'),('-1004.30'),('-1.2345678901234e+200'),('-1.2345678901234e-200')",
        true,
    ),
    (
        "int2_tbl",
        "CREATE TABLE int2_tbl (f1 int2 PRIMARY KEY)",
        "INSERT INTO int2_tbl(f1) VALUES ('0   '),('  1234 '),('    -1234'),('32767'),('-32767')",
        true,
    ),
    (
        "int4_tbl",
        "CREATE TABLE int4_tbl (f1 int4 PRIMARY KEY)",
        "INSERT INTO int4_tbl(f1) VALUES ('   0  '),('123456     '),('    -123456'),\
         ('2147483647'),('-2147483647')",
        true,
    ),
    (
        "int8_tbl",
        "CREATE TABLE int8_tbl (q1 int8, q2 int8, PRIMARY KEY (q1, q2))",
        "INSERT INTO int8_tbl VALUES ('  123   ','  456'),('123   ','4567890123456789'),\
         ('4567890123456789','123'),(+4567890123456789,'4567890123456789'),\
         ('+4567890123456789','-4567890123456789')",
        true,
    ),
    (
        "point_tbl",
        "CREATE TABLE point_tbl (f1 point)",
        "INSERT INTO point_tbl(f1) VALUES ('(0.0,0.0)'),('(-10.0,0.0)'),('(-3.0,4.0)'),\
         ('(5.1, 34.5)'),('(-5.0,-12.0)'),('(1e-300,-1e-300)'),('(1e+300,Inf)'),\
         ('(Inf,1e+300)'),(' ( Nan , NaN ) '),('10.0,10.0')",
        false,
    ),
    (
        "text_tbl",
        "CREATE TABLE text_tbl (f1 text PRIMARY KEY)",
        "INSERT INTO text_tbl VALUES ('doh!'),('hi de ho neighbor')",
        true,
    ),
    (
        "varchar_tbl",
        "CREATE TABLE varchar_tbl (f1 varchar(4))",
        "INSERT INTO varchar_tbl (f1) VALUES ('a'),('ab'),('abcd'),('abcd    ')",
        false,
    ),
];

/// `tenk1` mirrors `onek`'s 16-column schema (a synthetic `PRIMARY KEY
/// (unique1)` added so it is cacheable); 10000 rows from the vendored
/// `tenk.data` (COPY text format).
const TENK1_DDL: &str = "CREATE TABLE tenk1 (
    unique1     int4 PRIMARY KEY,
    unique2     int4,
    two         int4,
    four        int4,
    ten         int4,
    twenty      int4,
    hundred     int4,
    thousand    int4,
    twothousand int4,
    fivethous   int4,
    tenthous    int4,
    odd         int4,
    even        int4,
    stringu1    name,
    stringu2    name,
    string4     name
)";

/// COPY-from-file shared tables. The person inheritance chain and the road
/// path tables have no natural key (forwarded). `(table, ddl, data)`.
const COPY_TABLES: &[(&str, &str, &str)] = &[
    (
        "person",
        "CREATE TABLE person (name text, age int4, location point)",
        PERSON_DATA,
    ),
    (
        "emp",
        "CREATE TABLE emp (salary int4, manager name) INHERITS (person)",
        EMP_DATA,
    ),
    (
        "student",
        "CREATE TABLE student (gpa float8) INHERITS (person)",
        STUDENT_DATA,
    ),
    (
        "stud_emp",
        "CREATE TABLE stud_emp (percent int4) INHERITS (emp, student)",
        STUD_EMP_DATA,
    ),
    (
        "road",
        "CREATE TABLE road (name text, thepath path)",
        STREETS_DATA,
    ),
];

/// COPY rows from `data` into `table` (text format, as test_setup.sql does).
async fn table_copy(client: &Client, table: &str, data: &str) -> Result<()> {
    let sink = client
        .copy_in::<_, Bytes>(&format!("COPY {table} FROM STDIN"))
        .await
        .with_context(|| format!("starting COPY {table}"))?;
    pin_mut!(sink);
    sink.send(Bytes::copy_from_slice(data.as_bytes()))
        .await
        .with_context(|| format!("streaming {table} data"))?;
    let rows = sink
        .finish()
        .await
        .with_context(|| format!("finishing COPY {table}"))?;
    tracing::info!(table, rows, "loaded test_setup fixture");
    Ok(())
}

/// Build the shared regression environment from `test_setup.sql`. Assumes
/// [`onek_load`] has already run (`onek2` is a copy of `onek`).
pub async fn test_setup_load(client: &Client, publication: Option<&str>) -> Result<()> {
    // Drop in reverse dependency order (inheritance children first).
    client
        .batch_execute(
            "DROP TABLE IF EXISTS shighway, ihighway, road, stud_emp, student, emp, person, \
             tenk2, tenk1, onek2, char_tbl, float8_tbl, int2_tbl, int4_tbl, int8_tbl, \
             point_tbl, text_tbl, varchar_tbl CASCADE",
        )
        .await
        .context("dropping existing test_setup tables")?;

    for (table, ddl, insert, publish) in INLINE_TABLES {
        client
            .batch_execute(ddl)
            .await
            .with_context(|| format!("creating {table}"))?;
        if *publish {
            publication_table_ensure(client, publication, table).await?;
        }
        client
            .batch_execute(insert)
            .await
            .with_context(|| format!("loading {table}"))?;
    }

    // tenk1 (COPY) + tenk2 (copy of tenk1).
    client
        .batch_execute(TENK1_DDL)
        .await
        .context("creating tenk1")?;
    publication_table_ensure(client, publication, "tenk1").await?;
    table_copy(client, "tenk1", TENK_DATA).await?;

    client
        .batch_execute("CREATE TABLE tenk2 AS SELECT * FROM tenk1")
        .await
        .context("creating tenk2")?;
    client
        .batch_execute("ALTER TABLE tenk2 ADD PRIMARY KEY (unique1)")
        .await
        .context("keying tenk2")?;
    publication_table_ensure(client, publication, "tenk2").await?;

    // onek2 (copy of onek, loaded by onek_load).
    client
        .batch_execute("CREATE TABLE onek2 AS SELECT * FROM onek")
        .await
        .context("creating onek2")?;
    client
        .batch_execute("ALTER TABLE onek2 ADD PRIMARY KEY (unique1)")
        .await
        .context("keying onek2")?;
    publication_table_ensure(client, publication, "onek2").await?;

    // person inheritance chain + road path tables (no PK → forwarded; not
    // added to the publication).
    for (table, ddl, data) in COPY_TABLES {
        client
            .batch_execute(ddl)
            .await
            .with_context(|| format!("creating {table}"))?;
        table_copy(client, table, data).await?;
    }
    client
        .batch_execute(
            "CREATE TABLE ihighway () INHERITS (road); \
             INSERT INTO ihighway SELECT * FROM ONLY road WHERE name ~ 'I- .*'; \
             CREATE TABLE shighway (surface text) INHERITS (road); \
             INSERT INTO shighway SELECT *, 'asphalt' FROM ONLY road WHERE name ~ 'State Hwy.*'",
        )
        .await
        .context("creating highway tables")?;

    // SQL-defined types and helper functions (the C functions are omitted —
    // see the section comment).
    client
        .batch_execute(
            "CREATE TYPE stoplight AS ENUM ('red', 'yellow', 'green'); \
             CREATE TYPE float8range AS RANGE (subtype = float8, subtype_diff = float8mi); \
             CREATE TYPE textrange AS RANGE (subtype = text, collation = \"C\")",
        )
        .await
        .context("creating regress types")?;
    client
        .batch_execute(
            "CREATE FUNCTION fipshash(bytea) RETURNS text \
               STRICT IMMUTABLE PARALLEL SAFE LEAKPROOF \
               RETURN substr(encode(sha256($1), 'hex'), 1, 32); \
             CREATE FUNCTION fipshash(text) RETURNS text \
               STRICT IMMUTABLE PARALLEL SAFE LEAKPROOF \
               RETURN substr(encode(sha256($1::bytea), 'hex'), 1, 32)",
        )
        .await
        .context("creating fipshash functions")?;

    tracing::info!("loaded test_setup shared environment");
    Ok(())
}

const AGG_DATA: &str = include_str!("../data/agg.data");

/// bool_test rows from aggregates.sql (the upstream `COPY ... NULL 'null'`
/// block, rewritten with the standard `\N` null marker). No natural key.
const BOOL_TEST_DATA: &str =
    "TRUE\t\\N\tFALSE\t\\N\nFALSE\tTRUE\t\\N\t\\N\n\\N\tTRUE\tFALSE\t\\N\n";

/// `aggtest` / `regr_test` / `bool_test` from
/// `src/test/regress/sql/aggregates.sql`. `aggtest.a` and `regr_test.x`
/// are unique, so a PRIMARY KEY is added (cacheable); `bool_test` has
/// nullable, non-unique columns and forwards.
pub async fn aggregates_tables_load(client: &Client, publication: Option<&str>) -> Result<()> {
    client
        .batch_execute("DROP TABLE IF EXISTS aggtest, regr_test, bool_test CASCADE")
        .await
        .context("dropping existing aggregate fixtures")?;

    client
        .batch_execute("CREATE TABLE aggtest (a int2 PRIMARY KEY, b float4)")
        .await
        .context("creating aggtest")?;
    publication_table_ensure(client, publication, "aggtest").await?;
    table_copy(client, "aggtest", AGG_DATA).await?;

    client
        .batch_execute(
            "CREATE TABLE regr_test (x float8 PRIMARY KEY, y float8); \
             INSERT INTO regr_test VALUES (10,150),(20,250),(30,350),(80,540),(100,200)",
        )
        .await
        .context("creating regr_test")?;
    publication_table_ensure(client, publication, "regr_test").await?;

    client
        .batch_execute("CREATE TABLE bool_test (b1 bool, b2 bool, b3 bool, b4 bool)")
        .await
        .context("creating bool_test")?;
    table_copy(client, "bool_test", BOOL_TEST_DATA).await?;

    tracing::info!("loaded aggregate fixtures");
    Ok(())
}

/// `subselect_tbl` from `src/test/regress/sql/subselect.sql`. Upstream is
/// keyless; `(f1, f2)` is unique, so a composite PRIMARY KEY is added for
/// cache coverage (PGC-135). NB: row-constructor IN-subqueries over a
/// composite-PK table currently hang population (PGC-285).
pub async fn subselect_tables_load(client: &Client, publication: Option<&str>) -> Result<()> {
    client
        .batch_execute("DROP TABLE IF EXISTS subselect_tbl CASCADE")
        .await
        .context("dropping existing subselect_tbl")?;
    client
        .batch_execute(
            "CREATE TABLE subselect_tbl (f1 integer, f2 integer, f3 float, PRIMARY KEY (f1, f2))",
        )
        .await
        .context("creating subselect_tbl")?;
    publication_table_ensure(client, publication, "subselect_tbl").await?;
    client
        .batch_execute(
            "INSERT INTO subselect_tbl VALUES (1,2,3),(2,3,4),(3,4,5),(1,1,1),\
             (2,2,2),(3,3,3),(6,7,8),(8,9,NULL)",
        )
        .await
        .context("loading subselect_tbl")?;
    tracing::info!(table = "subselect_tbl", "loaded subselect fixture");
    Ok(())
}

/// `case_tbl` / `case2_tbl` from `src/test/regress/sql/case.sql`.
/// `case_tbl.i` is unique → PRIMARY KEY (cacheable); `case2_tbl` has
/// duplicate/NULL keys → keyless, forwarded.
pub async fn case_tables_load(client: &Client, publication: Option<&str>) -> Result<()> {
    client
        .batch_execute("DROP TABLE IF EXISTS case_tbl, case2_tbl CASCADE")
        .await
        .context("dropping existing case fixtures")?;
    client
        .batch_execute(
            "CREATE TABLE case_tbl (i integer PRIMARY KEY, f double precision); \
             INSERT INTO case_tbl VALUES (1,10.1),(2,20.2),(3,-30.3),(4,NULL)",
        )
        .await
        .context("creating case_tbl")?;
    publication_table_ensure(client, publication, "case_tbl").await?;
    client
        .batch_execute(
            "CREATE TABLE case2_tbl (i integer, j integer); \
             INSERT INTO case2_tbl VALUES (1,-1),(2,-2),(3,-3),(2,-4),(1,NULL),(NULL,-6)",
        )
        .await
        .context("creating case2_tbl")?;
    tracing::info!("loaded case fixtures");
    Ok(())
}

/// `department` from `src/test/regress/sql/with.sql` — the canonical
/// hierarchical fixture for recursive-CTE tree traversal. `id` is the
/// PRIMARY KEY (upstream also self-references via FK, dropped here as it
/// doesn't affect the queries).
pub async fn with_tables_load(client: &Client, publication: Option<&str>) -> Result<()> {
    client
        .batch_execute("DROP TABLE IF EXISTS department CASCADE")
        .await
        .context("dropping existing department")?;
    client
        .batch_execute(
            "CREATE TABLE department (id integer PRIMARY KEY, parent_department integer, name text)",
        )
        .await
        .context("creating department")?;
    publication_table_ensure(client, publication, "department").await?;
    client
        .batch_execute(
            "INSERT INTO department VALUES (0,NULL,'ROOT'),(1,0,'A'),(2,1,'B'),(3,2,'C'),\
             (4,2,'D'),(5,0,'E'),(6,4,'F'),(7,5,'G')",
        )
        .await
        .context("loading department")?;
    tracing::info!(table = "department", "loaded with fixture");
    Ok(())
}

/// `empsalary` from `src/test/regress/sql/window.sql` — the canonical
/// window-function fixture. `empno` is unique, so it is the PRIMARY KEY
/// (upstream is keyless) for cache coverage.
pub async fn window_tables_load(client: &Client, publication: Option<&str>) -> Result<()> {
    client
        .batch_execute("DROP TABLE IF EXISTS empsalary CASCADE")
        .await
        .context("dropping existing empsalary")?;
    client
        .batch_execute(
            "CREATE TABLE empsalary (depname varchar, empno bigint PRIMARY KEY, \
             salary int, enroll_date date)",
        )
        .await
        .context("creating empsalary")?;
    publication_table_ensure(client, publication, "empsalary").await?;
    client
        .batch_execute(
            "INSERT INTO empsalary VALUES \
             ('develop',10,5200,'2007-08-01'),('sales',1,5000,'2006-10-01'),\
             ('personnel',5,3500,'2007-12-10'),('sales',4,4800,'2007-08-08'),\
             ('personnel',2,3900,'2006-12-23'),('develop',7,4200,'2008-01-01'),\
             ('develop',9,4500,'2008-01-01'),('sales',3,4800,'2007-08-01'),\
             ('develop',8,6000,'2006-10-01'),('develop',11,5200,'2007-08-15')",
        )
        .await
        .context("loading empsalary")?;
    tracing::info!(table = "empsalary", "loaded window fixture");
    Ok(())
}

/// Create and populate `foo` on origin (idempotent) and add it to
/// pgcache's publication, so the select suite's NULL-ordering queries
/// are cached. Mirrors [`join_tables_load`].
pub async fn select_tables_load(client: &Client, publication: Option<&str>) -> Result<()> {
    client
        .batch_execute("DROP TABLE IF EXISTS foo")
        .await
        .context("dropping any existing foo")?;
    client
        .batch_execute(FOO_DDL)
        .await
        .context("creating foo")?;
    publication_table_ensure(client, publication, "foo").await?;
    client
        .batch_execute(FOO_DATA)
        .await
        .context("loading foo data")?;
    tracing::info!(table = "foo", "loaded select fixture");
    Ok(())
}

/// Create and populate `onek` on origin (idempotent).
///
/// pgcache adds the table to its own publication and registers it when a
/// query first references it (requires the synthetic PK — see
/// `ONEK_DDL`). The manual `ALTER PUBLICATION` is a best-effort nudge for
/// external pgcache instances that may not have provisioned it yet.
pub async fn onek_load(client: &Client, publication: Option<&str>) -> Result<()> {
    client
        .batch_execute("DROP TABLE IF EXISTS onek")
        .await
        .context("dropping any existing onek")?;
    client
        .batch_execute(ONEK_DDL)
        .await
        .context("creating onek")?;

    publication_table_ensure(client, publication, "onek").await?;

    let sink = client
        .copy_in::<_, Bytes>("COPY onek FROM STDIN")
        .await
        .context("starting COPY onek")?;
    pin_mut!(sink);
    sink.send(Bytes::from_static(ONEK_DATA.as_bytes()))
        .await
        .context("streaming onek data")?;
    let rows = sink.finish().await.context("finishing COPY onek")?;

    tracing::info!(rows, "loaded onek fixture");
    Ok(())
}
