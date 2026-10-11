//! AST pipeline benchmarks (PGC-480 follow-up): what each stage of the query
//! pipeline costs on representative shapes, measured three ways in one run.
//!
//! * **Node sizes** (`size_of`) of the plain and resolved tree types: a fat
//!   enum shows up here before any restructuring.
//! * **Allocations per stage per query**, counted by a wrapping global
//!   allocator and printed once before criterion starts. Deterministic, so it
//!   is the number to compare across commits on a laptop. A deep clone's
//!   allocation count is a direct read of a tree's heap node count.
//! * **Criterion timings** per stage, each stage fed the previous stage's
//!   output as a fixed input, so a regression lands on one bar.
//!
//! The catalog is an in-memory pagila subset (no database needed). pg_query's
//! C-side parse allocates outside the Rust allocator, so the parse stage's
//! count covers conversion only.
//!
//! Run: `cargo bench --bench ast_pipeline` (`-- --save-baseline before`, then
//! `-- --baseline before` on the next commit).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::mem::size_of;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use ecow::EcoString;
use iddqd::BiHashMap;
use postgres_types::Type;

use pgcache_lib::catalog::{Catalog, ColumnMetadata, ColumnPosition, ColumnStore, TableMetadata};
use pgcache_lib::oid::{Oid, TypeOid};
use pgcache_lib::pg::ByteString;
use pgcache_lib::query::ast::{
    BinaryExpr, ColumnNode, CteDefinition, Deparse, FunctionCall, JoinNode, LiteralValue,
    OrderByClause, QueryBody, QueryExpr, ScalarExpr, SelectColumn, SelectNode, TableNode,
    TableSource, WhereExpr, query_expr_convert_raw, query_expr_session_key,
};
use pgcache_lib::query::literal_settings::LiteralSettings;
use pgcache_lib::query::predicate::CompiledPredicate;
use pgcache_lib::query::resolve::{
    Bindings, ColumnBinding, ColumnTarget, ResolvedQuery, StarItem, TableSlot, query_expr_resolve,
    query_expr_resolve_bound,
};
use pgcache_lib::query::resolved::{
    ResolvedBinaryExpr, ResolvedColumnNode, ResolvedFunctionCall, ResolvedJoinNode,
    ResolvedOrderByClause, ResolvedQueryBody, ResolvedQueryExpr, ResolvedScalarExpr,
    ResolvedSelectColumn, ResolvedSelectNode, ResolvedTableNode, ResolvedTableSource,
    ResolvedWhereExpr, resolved_query_fingerprint,
};
use pgcache_lib::query::transform::resolved_literals_canonicalise;

// ---------------------------------------------------------------------------
// Counting allocator
// ---------------------------------------------------------------------------

struct CountingAllocator;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);

// SAFETY: every call forwards to `System` unchanged; the counters are plain
// relaxed atomics touched before the forward.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(new_size.saturating_sub(layout.size()), Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

/// Run `f` and return its result with the allocations and bytes it requested.
fn allocations_count<T>(f: impl FnOnce() -> T) -> (T, usize, usize) {
    let allocations = ALLOCATIONS.load(Ordering::Relaxed);
    let bytes = BYTES.load(Ordering::Relaxed);
    let value = f();
    (
        value,
        ALLOCATIONS.load(Ordering::Relaxed) - allocations,
        BYTES.load(Ordering::Relaxed) - bytes,
    )
}

// ---------------------------------------------------------------------------
// Fixture: a pagila subset and the query shapes
// ---------------------------------------------------------------------------

fn column(name: &str, index: usize, data_type: Type, is_primary_key: bool) -> ColumnMetadata {
    ColumnMetadata {
        name: name.into(),
        position: ColumnPosition::from_index(index).expect("position in range"),
        type_oid: TypeOid::from_type(&data_type),
        type_name: data_type.name().into(),
        cache_type_name: data_type.name().into(),
        data_type,
        is_primary_key,
    }
}

fn table(oid: u32, name: &str, primary_key: &str, columns: Vec<ColumnMetadata>) -> TableMetadata {
    TableMetadata {
        relation_oid: Oid::from_raw(oid),
        name: name.into(),
        schema: "public".into(),
        primary_key_columns: vec![primary_key.into()],
        columns: ColumnStore::new(columns),
        indexes: Vec::new(),
        replica_identity_full: false,
    }
}

fn catalog() -> Catalog {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(
        table(
            1001,
            "actor",
            "actor_id",
            vec![
                column("actor_id", 0, Type::INT4, true),
                column("first_name", 1, Type::TEXT, false),
                column("last_name", 2, Type::TEXT, false),
                column("last_update", 3, Type::TIMESTAMPTZ, false),
            ],
        )
        .into(),
    );
    tables.insert_overwrite(
        table(
            1002,
            "film",
            "film_id",
            vec![
                column("film_id", 0, Type::INT4, true),
                column("title", 1, Type::TEXT, false),
                column("description", 2, Type::TEXT, false),
                column("release_year", 3, Type::INT4, false),
                column("language_id", 4, Type::INT4, false),
                column("original_language_id", 5, Type::INT4, false),
                column("rental_duration", 6, Type::INT2, false),
                column("rental_rate", 7, Type::NUMERIC, false),
                column("length", 8, Type::INT2, false),
                column("replacement_cost", 9, Type::NUMERIC, false),
                column("rating", 10, Type::TEXT, false),
                column("last_update", 11, Type::TIMESTAMPTZ, false),
                column("special_features", 12, Type::TEXT_ARRAY, false),
                column("fulltext", 13, Type::TEXT, false),
            ],
        )
        .into(),
    );
    tables.insert_overwrite(
        table(
            1003,
            "film_actor",
            "actor_id",
            vec![
                column("actor_id", 0, Type::INT4, true),
                column("film_id", 1, Type::INT4, true),
                column("last_update", 2, Type::TIMESTAMPTZ, false),
            ],
        )
        .into(),
    );
    tables.insert_overwrite(
        table(
            1004,
            "category",
            "category_id",
            vec![
                column("category_id", 0, Type::INT4, true),
                column("name", 1, Type::TEXT, false),
                column("last_update", 2, Type::TIMESTAMPTZ, false),
            ],
        )
        .into(),
    );
    tables.insert_overwrite(
        table(
            1005,
            "film_category",
            "film_id",
            vec![
                column("film_id", 0, Type::INT4, true),
                column("category_id", 1, Type::INT4, true),
                column("last_update", 2, Type::TIMESTAMPTZ, false),
            ],
        )
        .into(),
    );
    tables.insert_overwrite(
        table(
            1006,
            "payment",
            "payment_id",
            vec![
                column("payment_id", 0, Type::INT4, true),
                column("customer_id", 1, Type::INT4, false),
                column("staff_id", 2, Type::INT4, false),
                column("rental_id", 3, Type::INT4, false),
                column("amount", 4, Type::NUMERIC, false),
                column("payment_date", 5, Type::TIMESTAMPTZ, false),
            ],
        )
        .into(),
    );
    tables.insert_overwrite(
        table(
            1007,
            "customer",
            "customer_id",
            vec![
                column("customer_id", 0, Type::INT4, true),
                column("store_id", 1, Type::INT4, false),
                column("first_name", 2, Type::TEXT, false),
                column("last_name", 3, Type::TEXT, false),
                column("email", 4, Type::TEXT, false),
                column("address_id", 5, Type::INT4, false),
                column("activebool", 6, Type::BOOL, false),
                column("create_date", 7, Type::DATE, false),
                column("last_update", 8, Type::TIMESTAMPTZ, false),
                column("active", 9, Type::INT4, false),
            ],
        )
        .into(),
    );
    tables
}

/// One benchmark query: its name, SQL, the table a predicate compiles for and
/// a row of that table for the evaluation stage.
struct Shape {
    name: &'static str,
    sql: String,
    predicate_table: &'static str,
    row: &'static [Option<&'static str>],
}

const ACTOR_ROW: &[Option<&str>] = &[
    Some("42"),
    Some("Ada"),
    Some("Lovelace"),
    Some("2024-01-15 10:00:00+00"),
];
const FILM_ROW: &[Option<&str>] = &[
    Some("7"),
    Some("Academy Dinosaur"),
    Some("A Epic Drama"),
    Some("2006"),
    Some("1"),
    None,
    Some("6"),
    Some("0.99"),
    Some("86"),
    Some("20.99"),
    Some("PG"),
    Some("2024-01-15 10:00:00+00"),
    Some("{Trailers}"),
    Some("'academi':1"),
];
const PAYMENT_ROW: &[Option<&str>] = &[
    Some("1"),
    Some("3"),
    Some("1"),
    Some("9"),
    Some("7.99"),
    Some("2024-01-16 10:00:00+00"),
];

fn shapes() -> Vec<Shape> {
    let in_list: Vec<String> = (1..=50).map(|n| n.to_string()).collect();
    vec![
        Shape {
            name: "read_simple",
            sql: "SELECT * FROM actor WHERE actor_id = 42".to_owned(),
            predicate_table: "actor",
            row: ACTOR_ROW,
        },
        Shape {
            name: "read_join",
            sql: "SELECT fa.film_id, a.first_name, a.last_name FROM film_actor fa \
                  JOIN actor a ON a.actor_id = fa.actor_id WHERE fa.film_id = 7"
                .to_owned(),
            predicate_table: "film_actor",
            row: &[Some("5"), Some("7"), Some("2024-01-15 10:00:00+00")],
        },
        Shape {
            name: "read_agg",
            sql: "SELECT c.name, COUNT(*) FROM film_category fc \
                  JOIN category c ON fc.category_id = c.category_id GROUP BY c.name"
                .to_owned(),
            predicate_table: "category",
            row: &[Some("1"), Some("Action"), Some("2024-01-15 10:00:00+00")],
        },
        Shape {
            name: "join_five",
            sql: "SELECT f.title, a.first_name, c.name FROM film f \
                  JOIN film_actor fa ON fa.film_id = f.film_id \
                  JOIN actor a ON a.actor_id = fa.actor_id \
                  JOIN film_category fc ON fc.film_id = f.film_id \
                  JOIN category c ON c.category_id = fc.category_id \
                  WHERE f.film_id = 7 AND a.last_name <> 'X' ORDER BY a.first_name"
                .to_owned(),
            predicate_table: "film",
            row: FILM_ROW,
        },
        Shape {
            name: "in_subquery",
            sql: "SELECT title FROM film WHERE film_id IN \
                  (SELECT film_id FROM film_actor WHERE actor_id = 5)"
                .to_owned(),
            predicate_table: "film",
            row: FILM_ROW,
        },
        Shape {
            name: "cte_datetime",
            sql: "WITH recent AS (SELECT customer_id, amount FROM payment \
                  WHERE payment_date > '2024-01-15 10:00:00+00') \
                  SELECT customer_id, amount FROM recent WHERE amount > 5.0"
                .to_owned(),
            predicate_table: "payment",
            row: PAYMENT_ROW,
        },
        Shape {
            name: "wide_star",
            sql: "SELECT * FROM film WHERE film_id = 7".to_owned(),
            predicate_table: "film",
            row: FILM_ROW,
        },
        Shape {
            name: "in_list_50",
            sql: format!(
                "SELECT actor_id, first_name FROM actor WHERE actor_id IN ({})",
                in_list.join(", ")
            ),
            predicate_table: "actor",
            row: ACTOR_ROW,
        },
    ]
}

// ---------------------------------------------------------------------------
// Stages
// ---------------------------------------------------------------------------

fn parse(sql: &str) -> QueryExpr {
    pg_query::parse_raw_scoped(sql, |tree| unsafe { query_expr_convert_raw(tree) })
        .expect("pg_query parses the fixture SQL")
        .expect("the fixture SQL converts")
}

fn resolve(query: &QueryExpr, catalog: &Catalog) -> ResolvedQueryExpr {
    query_expr_resolve(query, catalog, &["public"]).expect("the fixture SQL resolves")
}

fn resolve_bound(query: &QueryExpr, catalog: &Catalog) -> (ResolvedQueryExpr, Bindings) {
    query_expr_resolve_bound(query, catalog, &["public"]).expect("the fixture SQL resolves")
}

fn canonicalise(resolved: &ResolvedQueryExpr, settings: &LiteralSettings) -> ResolvedQueryExpr {
    let mut canonical = resolved.clone();
    let _ = resolved_literals_canonicalise(&mut canonical, settings);
    canonical
}

fn deparse(resolved: &ResolvedQueryExpr) -> String {
    let mut sql = String::new();
    resolved.deparse(&mut sql);
    sql
}

fn predicate_compile(bound: &ResolvedQuery, table: &str) -> Option<CompiledPredicate> {
    let where_clause = bound.select()?.where_clause.as_ref()?;
    Some(CompiledPredicate::compile(bound, where_clause, table))
}

fn row_build(row: &[Option<&str>]) -> Vec<Option<ByteString>> {
    row.iter()
        .map(|cell| {
            cell.map(|text| {
                ByteString::from_utf8(Bytes::copy_from_slice(text.as_bytes()))
                    .expect("fixture cells are UTF-8")
            })
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Structural report (printed once, before criterion)
// ---------------------------------------------------------------------------

fn sizes_report() {
    println!("\n== node sizes (bytes)");
    println!("{:<28}{:>8}    {:<28}{:>8}", "plain", "", "resolved", "");
    let plain: [(&str, usize); 15] = [
        ("QueryExpr", size_of::<QueryExpr>()),
        ("QueryBody", size_of::<QueryBody>()),
        ("SelectNode", size_of::<SelectNode>()),
        ("SelectColumn", size_of::<SelectColumn>()),
        ("TableSource", size_of::<TableSource>()),
        ("TableNode", size_of::<TableNode>()),
        ("JoinNode", size_of::<JoinNode>()),
        ("ScalarExpr", size_of::<ScalarExpr>()),
        ("WhereExpr", size_of::<WhereExpr>()),
        ("BinaryExpr", size_of::<BinaryExpr>()),
        ("ColumnNode", size_of::<ColumnNode>()),
        ("LiteralValue", size_of::<LiteralValue>()),
        ("FunctionCall", size_of::<FunctionCall>()),
        ("OrderByClause", size_of::<OrderByClause>()),
        ("CteDefinition", size_of::<CteDefinition>()),
    ];
    let resolved: [(&str, usize); 15] = [
        ("ResolvedQueryExpr", size_of::<ResolvedQueryExpr>()),
        ("ResolvedQueryBody", size_of::<ResolvedQueryBody>()),
        ("ResolvedSelectNode", size_of::<ResolvedSelectNode>()),
        ("ResolvedSelectColumn", size_of::<ResolvedSelectColumn>()),
        ("ResolvedTableSource", size_of::<ResolvedTableSource>()),
        ("ResolvedTableNode", size_of::<ResolvedTableNode>()),
        ("ResolvedJoinNode", size_of::<ResolvedJoinNode>()),
        ("ResolvedScalarExpr", size_of::<ResolvedScalarExpr>()),
        ("ResolvedWhereExpr", size_of::<ResolvedWhereExpr>()),
        ("ResolvedBinaryExpr", size_of::<ResolvedBinaryExpr>()),
        ("ResolvedColumnNode", size_of::<ResolvedColumnNode>()),
        ("ColumnMetadata", size_of::<ColumnMetadata>()),
        ("ResolvedFunctionCall", size_of::<ResolvedFunctionCall>()),
        ("ResolvedOrderByClause", size_of::<ResolvedOrderByClause>()),
        ("Type", size_of::<Type>()),
    ];
    for ((plain_name, plain_size), (resolved_name, resolved_size)) in plain.iter().zip(&resolved) {
        println!("{plain_name:<28}{plain_size:>8}    {resolved_name:<28}{resolved_size:>8}");
    }
    println!(
        "{:<28}{:>8}    {:<28}{:>8}",
        "EcoString",
        size_of::<EcoString>(),
        "TableMetadata",
        size_of::<TableMetadata>()
    );
    println!("\n== bindings (bytes)");
    let bindings: [(&str, usize); 6] = [
        ("ResolvedQuery", size_of::<ResolvedQuery>()),
        ("Bindings", size_of::<Bindings>()),
        ("TableSlot", size_of::<TableSlot>()),
        ("ColumnBinding", size_of::<ColumnBinding>()),
        ("ColumnTarget", size_of::<ColumnTarget>()),
        ("StarItem", size_of::<StarItem>()),
    ];
    for (name, size) in bindings {
        println!("{name:<28}{size:>8}");
    }
}

fn allocations_report(shapes: &[Shape], catalog: &Catalog) {
    let settings = LiteralSettings::default();
    let search_path: [EcoString; 1] = ["public".into()];
    println!("\n== allocations per stage (count / bytes)");
    println!(
        "{:<14}{:>14}{:>14}{:>14}{:>14}{:>14}{:>14}{:>14}{:>14}{:>14}{:>14}{:>14}{:>14}{:>14}{:>14}",
        "query",
        "convert",
        "plain_clone",
        "session_key",
        "resolve",
        "resolved_clone",
        "bindings",
        "bound_clone",
        "canonicalise",
        "fingerprint",
        "deparse",
        "bound_fp",
        "bound_render",
        "bound_shape",
        "pred_compile"
    );
    for shape in shapes {
        let (query, convert_n, convert_b) = allocations_count(|| parse(&shape.sql));
        let (_, plain_clone_n, plain_clone_b) = allocations_count(|| black_box(query.clone()));
        let (_, key_n, key_b) =
            allocations_count(|| query_expr_session_key(&query, &search_path, &settings));
        let (resolved, resolve_n, resolve_b) = allocations_count(|| resolve(&query, catalog));
        let (_, resolved_clone_n, resolved_clone_b) =
            allocations_count(|| black_box(resolved.clone()));
        let ((_, bindings), bound_n, bound_b) =
            allocations_count(|| resolve_bound(&query, catalog));
        let bound = ResolvedQuery::new(Arc::new(query.clone()), bindings);
        let (_, bound_clone_n, bound_clone_b) = allocations_count(|| black_box(bound.clone()));
        let (canonical, canon_n, canon_b) =
            allocations_count(|| canonicalise(&resolved, &settings));
        let (_, fingerprint_n, fingerprint_b) =
            allocations_count(|| resolved_query_fingerprint(&canonical));
        let (_, deparse_n, deparse_b) = allocations_count(|| deparse(&canonical));
        let (_, bound_fp_n, bound_fp_b) = allocations_count(|| bound.fingerprint());
        let (_, bound_render_n, bound_render_b) = allocations_count(|| bound.sql());
        let (_, bound_shape_n, bound_shape_b) = allocations_count(|| bound.shape());
        let (_, compile_n, compile_b) =
            allocations_count(|| predicate_compile(&bound, shape.predicate_table));
        let cell = |n: usize, b: usize| format!("{n}/{b}");
        println!(
            "{:<14}{:>14}{:>14}{:>14}{:>14}{:>14}{:>14}{:>14}{:>14}{:>14}{:>14}{:>14}{:>14}{:>14}{:>14}",
            shape.name,
            cell(convert_n, convert_b),
            cell(plain_clone_n, plain_clone_b),
            cell(key_n, key_b),
            cell(resolve_n, resolve_b),
            cell(resolved_clone_n, resolved_clone_b),
            cell(
                bound_n.saturating_sub(resolve_n),
                bound_b.saturating_sub(resolve_b)
            ),
            cell(bound_clone_n, bound_clone_b),
            cell(
                canon_n - resolved_clone_n,
                canon_b.saturating_sub(resolved_clone_b)
            ),
            cell(fingerprint_n, fingerprint_b),
            cell(deparse_n, deparse_b),
            cell(bound_fp_n, bound_fp_b),
            cell(bound_render_n, bound_render_b),
            cell(bound_shape_n, bound_shape_b),
            cell(compile_n, compile_b),
        );
    }
    println!(
        "(canonicalise excludes the clone it starts from; bindings is the bound resolve's cost \
         over resolve; convert excludes pg_query's C-side parse)\n"
    );
}

// ---------------------------------------------------------------------------
// Criterion
// ---------------------------------------------------------------------------

fn bench_pipeline(c: &mut Criterion) {
    let catalog = catalog();
    let shapes = shapes();
    sizes_report();
    allocations_report(&shapes, &catalog);

    let settings = LiteralSettings::default();
    let search_path: [EcoString; 1] = ["public".into()];

    let mut group = c.benchmark_group("ast_pipeline");
    for shape in &shapes {
        let query = parse(&shape.sql);
        let resolved = resolve(&query, &catalog);
        let (_, bindings) = resolve_bound(&query, &catalog);
        let bound = ResolvedQuery::new(Arc::new(query.clone()), bindings);
        let canonical = canonicalise(&resolved, &settings);
        let predicate = predicate_compile(&bound, shape.predicate_table);
        let row = row_build(shape.row);

        group.bench_with_input(
            BenchmarkId::new("convert", shape.name),
            &shape.sql,
            |b, sql| {
                b.iter(|| black_box(parse(sql)));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("plain_clone", shape.name),
            &query,
            |b, q| {
                b.iter(|| black_box(q.clone()));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("session_key", shape.name),
            &query,
            |b, q| {
                b.iter(|| black_box(query_expr_session_key(q, &search_path, &settings)));
            },
        );
        group.bench_with_input(BenchmarkId::new("resolve", shape.name), &query, |b, q| {
            b.iter(|| black_box(resolve(q, &catalog)));
        });
        group.bench_with_input(
            BenchmarkId::new("resolved_clone", shape.name),
            &resolved,
            |b, r| {
                b.iter(|| black_box(r.clone()));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("resolve_bound", shape.name),
            &query,
            |b, q| {
                b.iter(|| black_box(resolve_bound(q, &catalog)));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("bound_clone", shape.name),
            &bound,
            |b, r| {
                b.iter(|| black_box(r.clone()));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("canonicalise", shape.name),
            &resolved,
            |b, r| {
                b.iter(|| black_box(canonicalise(r, &settings)));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("fingerprint", shape.name),
            &canonical,
            |b, r| {
                b.iter(|| black_box(resolved_query_fingerprint(r)));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("deparse", shape.name),
            &canonical,
            |b, r| {
                b.iter(|| black_box(deparse(r)));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("bound_fingerprint", shape.name),
            &bound,
            |b, r| {
                b.iter(|| black_box(r.fingerprint()));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("bound_render", shape.name),
            &bound,
            |b, r| {
                b.iter(|| black_box(r.sql()));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("bound_shape", shape.name),
            &bound,
            |b, r| {
                b.iter(|| black_box(r.shape()));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("predicate_compile", shape.name),
            &bound,
            |b, r| {
                b.iter(|| black_box(predicate_compile(r, shape.predicate_table)));
            },
        );
        if let Some(predicate) = &predicate {
            group.bench_with_input(
                BenchmarkId::new("predicate_eval", shape.name),
                &(predicate, &row),
                |b, (p, row)| {
                    b.iter(|| black_box(p.eval(row)));
                },
            );
        }
        group.bench_with_input(
            BenchmarkId::new("end_to_end", shape.name),
            &shape.sql,
            |b, sql| {
                b.iter(|| {
                    let query = parse(sql);
                    let key = query_expr_session_key(&query, &search_path, &settings);
                    let resolved = resolve(&query, &catalog);
                    let canonical = canonicalise(&resolved, &settings);
                    let fingerprint = resolved_query_fingerprint(&canonical);
                    black_box((key, fingerprint, deparse(&canonical)))
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_pipeline);
criterion_main!(benches);
