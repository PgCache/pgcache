# Vendored test data

The `.data` files are copied verbatim from the PostgreSQL source tree
(`src/test/regress/data/`, https://github.com/postgres/postgres):

- `onek.data`, `tenk.data` — the `onek` / `tenk1` pseudo-random tables.
- `person.data`, `emp.data`, `student.data`, `stud_emp.data` — the
  `person` table-inheritance chain.
- `streets.data` — the `road` (geometric `path`) table.

The table schemas are taken from `src/test/regress/sql/test_setup.sql`
(replicated in `conformance/src/fixtures.rs::test_setup_load`), with one
deliberate deviation: a `PRIMARY KEY` is added on the naturally-unique
column(s) of the cacheable tables (`onek`/`onek2`/`tenk1`/`tenk2.unique1`,
the `int*`/`float8`/`text` single-value tables) — not present upstream —
because pgcache only caches tables that have a primary key (PGC-135).
Keyless tables (char/varchar/point, the person chain, road) are kept
verbatim and forward. All vendored data is unmodified.

The `j1_tbl` / `j2_tbl` fixtures (defined inline in
`conformance/src/fixtures.rs`, not as a data file) are the `J1_TBL` /
`J2_TBL` tables and rows from `src/test/regress/sql/join.sql`. The row
data is unmodified; a surrogate `*_pk` primary-key column is appended
(same rationale as `onek` above — pgcache only caches tables with a
primary key, PGC-135 — since these tables have no natural key upstream).

The `foo` fixture (inline in `conformance/src/fixtures.rs`) is the
`foo (f1 int)` table and rows from the ORDER BY / NULLS section of
`src/test/regress/sql/select.sql`. Row data unmodified; a surrogate
`foo_pk` primary key is appended (same PGC-135 rationale).

Used unmodified solely as fixture data for the pgcache SQL conformance
harness. PostgreSQL license notice (from the upstream `COPYRIGHT` file):

```
PostgreSQL Database Management System
(formerly known as Postgres, then as Postgres95)

Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group

Portions Copyright (c) 1994, The Regents of the University of California

Permission to use, copy, modify, and distribute this software and its
documentation for any purpose, without fee, and without a written agreement
is hereby granted, provided that the above copyright notice and this
paragraph and the following two paragraphs appear in all copies.

IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS
ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
```
