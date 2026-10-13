//! AST pipeline benchmarks (PGC-480 follow-up): what each stage of the query
//! pipeline costs on representative shapes, measured three ways in one run.
//!
//! * **Node sizes** (`size_of`) of the tree's node types: a fat
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
use std::collections::HashSet;
use std::hint::black_box;
use std::mem::size_of;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use ecow::EcoString;
use iddqd::BiHashMap;
use postgres_types::Type;

use pgcache_lib::cache::admission::{AdmissionDepth, AdmissionQuery, query_admission_analyze};
use pgcache_lib::catalog::{Catalog, ColumnMetadata, ColumnPosition, ColumnStore, TableMetadata};
use pgcache_lib::oid::{Oid, TypeOid};
use pgcache_lib::pg::ByteString;
use pgcache_lib::query::ast::rope::{Node, NodeKind, QueryTree};
use pgcache_lib::query::ast::{LiteralValue, query_tree_convert_raw, query_tree_session_key};
use pgcache_lib::query::decorrelate::query_decorrelate;
use pgcache_lib::query::literal_settings::LiteralSettings;
use pgcache_lib::query::predicate::CompiledPredicate;
use pgcache_lib::query::resolve::{
    Bindings, ColumnBinding, ColumnTarget, ResolvedQuery, StarItem, TableSlot,
};
use pgcache_lib::query::transform::{query_literals_canonicalise, query_predicate_pushdown};
use pgcache_lib::query::update::query_table_update_queries;

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

/// The rope straight from the raw parse tree.
fn parse(sql: &str) -> QueryTree {
    pg_query::parse_raw_scoped(sql, |tree| unsafe { query_tree_convert_raw(tree) })
        .expect("pg_query parses the fixture SQL")
        .expect("the fixture SQL converts")
}

fn resolve_bound(tree: &QueryTree, catalog: &Catalog) -> ResolvedQuery {
    ResolvedQuery::resolve(tree.clone(), catalog, &["public"]).expect("the fixture SQL resolves")
}

/// The bound query with its literals canonicalised, as registration
/// builds it; the clone is what registration's own copy costs.
fn canonicalise(bound: &ResolvedQuery, settings: &LiteralSettings) -> ResolvedQuery {
    query_literals_canonicalise(bound.clone(), settings).0
}

fn predicate_compile(bound: &ResolvedQuery, table: &str) -> Option<CompiledPredicate> {
    let where_clause = bound.select()?.where_clause()?;
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
    println!("\n== tree (bytes)");
    let tree: [(&str, usize); 4] = [
        ("Node", size_of::<Node>()),
        ("NodeKind", size_of::<NodeKind>()),
        ("LiteralValue", size_of::<LiteralValue>()),
        ("QueryTree", size_of::<QueryTree>()),
    ];
    for (name, size) in tree {
        println!("{name:<28}{size:>8}");
    }
    println!("\n== metadata (bytes)");
    let metadata: [(&str, usize); 4] = [
        ("EcoString", size_of::<EcoString>()),
        ("Type", size_of::<Type>()),
        ("ColumnMetadata", size_of::<ColumnMetadata>()),
        ("TableMetadata", size_of::<TableMetadata>()),
    ];
    for (name, size) in metadata {
        println!("{name:<28}{size:>8}");
    }
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
    // The aggregates the writer loads from `pg_proc` that the fixtures use.
    let aggregates: HashSet<EcoString> = ["count", "sum", "avg", "min", "max"]
        .into_iter()
        .map(EcoString::from)
        .collect();
    println!("\n== allocations per stage (count / bytes)");
    let header = [
        "query",
        "convert",
        "session_key",
        "rope_clone",
        "resolve",
        "bound_clone",
        "canonicalise",
        "pushdown",
        "decorrelate",
        "fingerprint",
        "render",
        "shape",
        "pred_compile",
        "update_queries",
        "admission",
    ];
    row_print(&header.map(str::to_owned));
    for shape in shapes {
        let (rope, convert_n, convert_b) = allocations_count(|| parse(&shape.sql));
        let (_, key_n, key_b) =
            allocations_count(|| query_tree_session_key(&rope, &search_path, &settings));
        let (_, rope_clone_n, rope_clone_b) = allocations_count(|| black_box(rope.clone()));
        let (resolved, resolve_n, resolve_b) = allocations_count(|| resolve_bound(&rope, catalog));
        let (_, bound_clone_n, bound_clone_b) = allocations_count(|| black_box(resolved.clone()));
        let (bound, canon_n, canon_b) = allocations_count(|| canonicalise(&resolved, &settings));
        let (_, pushdown_n, pushdown_b) =
            allocations_count(|| black_box(query_predicate_pushdown(&bound)));
        let (_, decorrelate_n, decorrelate_b) =
            allocations_count(|| black_box(query_decorrelate(&bound, &aggregates)));
        let (fingerprint, fingerprint_n, fingerprint_b) = allocations_count(|| bound.fingerprint());
        let (_, render_n, render_b) = allocations_count(|| bound.sql());
        let (_, shape_n, shape_b) = allocations_count(|| bound.shape());
        let (_, compile_n, compile_b) =
            allocations_count(|| predicate_compile(&bound, shape.predicate_table));
        let (_, update_n, update_b) =
            allocations_count(|| black_box(query_table_update_queries(&bound)));
        let shared = Arc::new(bound.clone());
        let (admission, admission_n, admission_b) = allocations_count(|| {
            query_admission_analyze(
                AdmissionQuery {
                    bound: &shared,
                    decorrelated: None,
                },
                fingerprint,
                bound.tree().root().has_limit(),
                &aggregates,
                catalog,
                AdmissionDepth::Full,
            )
        });
        let admission = admission.expect("the fixture SQL is admitted");
        black_box(admission.tables.len());
        let cell = |n: usize, b: usize| format!("{n}/{b}");
        row_print(&[
            shape.name.to_owned(),
            cell(convert_n, convert_b),
            cell(key_n, key_b),
            cell(rope_clone_n, rope_clone_b),
            cell(resolve_n, resolve_b),
            cell(bound_clone_n, bound_clone_b),
            cell(
                canon_n.saturating_sub(bound_clone_n),
                canon_b.saturating_sub(bound_clone_b),
            ),
            cell(pushdown_n, pushdown_b),
            cell(decorrelate_n, decorrelate_b),
            cell(fingerprint_n, fingerprint_b),
            cell(render_n, render_b),
            cell(shape_n, shape_b),
            cell(compile_n, compile_b),
            cell(update_n, update_b),
            cell(admission_n, admission_b),
        ]);
    }
}

/// One row of the allocations table: the query name, then a cell per stage.
fn row_print(cells: &[String; 15]) {
    let mut line = String::new();
    for (index, cell) in cells.iter().enumerate() {
        if index == 0 {
            line.push_str(&format!("{cell:<14}"));
        } else {
            line.push_str(&format!("{cell:>14}"));
        }
    }
    println!("{line}");
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
    let aggregates: HashSet<EcoString> = ["count", "sum", "avg", "min", "max"]
        .into_iter()
        .map(EcoString::from)
        .collect();

    let mut group = c.benchmark_group("ast_pipeline");
    for shape in &shapes {
        let tree = parse(&shape.sql);
        let resolved = resolve_bound(&tree, &catalog);
        let bound = canonicalise(&resolved, &settings);
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
            BenchmarkId::new("session_key", shape.name),
            &tree,
            |b, t| {
                b.iter(|| black_box(query_tree_session_key(t, &search_path, &settings)));
            },
        );
        group.bench_with_input(BenchmarkId::new("resolve", shape.name), &tree, |b, t| {
            b.iter(|| black_box(resolve_bound(t, &catalog)));
        });
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
        group.bench_with_input(BenchmarkId::new("pushdown", shape.name), &bound, |b, r| {
            b.iter(|| black_box(query_predicate_pushdown(r)));
        });
        group.bench_with_input(
            BenchmarkId::new("decorrelate", shape.name),
            &bound,
            |b, r| {
                b.iter(|| black_box(query_decorrelate(r, &aggregates)));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("fingerprint", shape.name),
            &bound,
            |b, r| {
                b.iter(|| black_box(r.fingerprint()));
            },
        );
        group.bench_with_input(BenchmarkId::new("render", shape.name), &bound, |b, r| {
            b.iter(|| black_box(r.sql()));
        });
        group.bench_with_input(BenchmarkId::new("shape", shape.name), &bound, |b, r| {
            b.iter(|| black_box(r.shape()));
        });
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
                    let tree = parse(sql);
                    let key = query_tree_session_key(&tree, &search_path, &settings);
                    let resolved = resolve_bound(&tree, &catalog);
                    let bound = canonicalise(&resolved, &settings);
                    black_box((key, bound.fingerprint(), bound.sql()))
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_pipeline);
criterion_main!(benches);
