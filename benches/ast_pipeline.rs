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
use pgcache_lib::query::Fingerprint;
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
/// Allocations and bytes one run of a stage makes.
type Cost = (usize, usize);

/// The cost of `f`, its product dropped.
fn cost<T>(f: impl FnOnce() -> T) -> Cost {
    let (product, allocations, bytes) = allocations_count(f);
    black_box(product);
    (allocations, bytes)
}

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

/// The pagila subset: `(oid, name, primary key, columns as (name, type, in
/// the primary key))`.
#[rustfmt::skip]
type CatalogTable = (u32, &'static str, &'static str, &'static [(&'static str, Type, bool)]);

const CATALOG_TABLES: &[CatalogTable] = &[
    (
        1001,
        "actor",
        "actor_id",
        &[
            ("actor_id", Type::INT4, true),
            ("first_name", Type::TEXT, false),
            ("last_name", Type::TEXT, false),
            ("last_update", Type::TIMESTAMPTZ, false),
        ],
    ),
    (
        1002,
        "film",
        "film_id",
        &[
            ("film_id", Type::INT4, true),
            ("title", Type::TEXT, false),
            ("description", Type::TEXT, false),
            ("release_year", Type::INT4, false),
            ("language_id", Type::INT4, false),
            ("original_language_id", Type::INT4, false),
            ("rental_duration", Type::INT2, false),
            ("rental_rate", Type::NUMERIC, false),
            ("length", Type::INT2, false),
            ("replacement_cost", Type::NUMERIC, false),
            ("rating", Type::TEXT, false),
            ("last_update", Type::TIMESTAMPTZ, false),
            ("special_features", Type::TEXT_ARRAY, false),
            ("fulltext", Type::TEXT, false),
        ],
    ),
    (
        1003,
        "film_actor",
        "actor_id",
        &[
            ("actor_id", Type::INT4, true),
            ("film_id", Type::INT4, true),
            ("last_update", Type::TIMESTAMPTZ, false),
        ],
    ),
    (
        1004,
        "category",
        "category_id",
        &[
            ("category_id", Type::INT4, true),
            ("name", Type::TEXT, false),
            ("last_update", Type::TIMESTAMPTZ, false),
        ],
    ),
    (
        1005,
        "film_category",
        "film_id",
        &[
            ("film_id", Type::INT4, true),
            ("category_id", Type::INT4, true),
            ("last_update", Type::TIMESTAMPTZ, false),
        ],
    ),
    (
        1006,
        "payment",
        "payment_id",
        &[
            ("payment_id", Type::INT4, true),
            ("customer_id", Type::INT4, false),
            ("staff_id", Type::INT4, false),
            ("rental_id", Type::INT4, false),
            ("amount", Type::NUMERIC, false),
            ("payment_date", Type::TIMESTAMPTZ, false),
        ],
    ),
    (
        1007,
        "customer",
        "customer_id",
        &[
            ("customer_id", Type::INT4, true),
            ("store_id", Type::INT4, false),
            ("first_name", Type::TEXT, false),
            ("last_name", Type::TEXT, false),
            ("email", Type::TEXT, false),
            ("address_id", Type::INT4, false),
            ("activebool", Type::BOOL, false),
            ("create_date", Type::DATE, false),
            ("last_update", Type::TIMESTAMPTZ, false),
            ("active", Type::INT4, false),
        ],
    ),
];

fn catalog() -> Catalog {
    let mut tables = BiHashMap::new();
    for (oid, name, primary_key, columns) in CATALOG_TABLES {
        let columns = columns
            .iter()
            .enumerate()
            .map(|(index, (name, data_type, is_primary_key))| {
                column(name, index, data_type.clone(), *is_primary_key)
            })
            .collect();
        tables.insert_overwrite(table(*oid, name, primary_key, columns).into());
    }
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

/// The fixed query shapes: `(name, SQL, predicate table, row)`; the 50-item
/// IN list is generated.
#[rustfmt::skip]
const FIXED_SHAPES: &[(&str, &str, &str, &[Option<&str>])] = &[
    ("read_simple", "SELECT * FROM actor WHERE actor_id = 42", "actor", ACTOR_ROW),
    ("read_join",
     "SELECT fa.film_id, a.first_name, a.last_name FROM film_actor fa \
      JOIN actor a ON a.actor_id = fa.actor_id WHERE fa.film_id = 7",
     "film_actor", &[Some("5"), Some("7"), Some("2024-01-15 10:00:00+00")]),
    ("read_agg",
     "SELECT c.name, COUNT(*) FROM film_category fc \
      JOIN category c ON fc.category_id = c.category_id GROUP BY c.name",
     "category", &[Some("1"), Some("Action"), Some("2024-01-15 10:00:00+00")]),
    ("join_five",
     "SELECT f.title, a.first_name, c.name FROM film f \
      JOIN film_actor fa ON fa.film_id = f.film_id \
      JOIN actor a ON a.actor_id = fa.actor_id \
      JOIN film_category fc ON fc.film_id = f.film_id \
      JOIN category c ON c.category_id = fc.category_id \
      WHERE f.film_id = 7 AND a.last_name <> 'X' ORDER BY a.first_name",
     "film", FILM_ROW),
    ("in_subquery",
     "SELECT title FROM film WHERE film_id IN \
      (SELECT film_id FROM film_actor WHERE actor_id = 5)",
     "film", FILM_ROW),
    ("cte_datetime",
     "WITH recent AS (SELECT customer_id, amount FROM payment \
      WHERE payment_date > '2024-01-15 10:00:00+00') \
      SELECT customer_id, amount FROM recent WHERE amount > 5.0",
     "payment", PAYMENT_ROW),
    ("wide_star", "SELECT * FROM film WHERE film_id = 7", "film", FILM_ROW),
];

fn shapes() -> Vec<Shape> {
    let in_list: Vec<String> = (1..=50).map(|n| n.to_string()).collect();
    let mut shapes: Vec<Shape> = FIXED_SHAPES
        .iter()
        .map(|(name, sql, predicate_table, row)| Shape {
            name,
            sql: (*sql).to_owned(),
            predicate_table,
            row,
        })
        .collect();
    shapes.push(Shape {
        name: "in_list_50",
        sql: format!(
            "SELECT actor_id, first_name FROM actor WHERE actor_id IN ({})",
            in_list.join(", ")
        ),
        predicate_table: "actor",
        row: ACTOR_ROW,
    });
    shapes
}

// ---------------------------------------------------------------------------
// Stages
// ---------------------------------------------------------------------------

/// The rope straight from the raw parse tree.
fn parse(sql: &str) -> QueryTree {
    pg_query::parse_raw_scoped(sql, |tree| unsafe {
        query_tree_convert_raw(tree, sql.len())
    })
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

/// The aggregates the writer loads from `pg_proc` that the fixtures use.
fn aggregates() -> HashSet<EcoString> {
    ["count", "sum", "avg", "min", "max"]
        .into_iter()
        .map(EcoString::from)
        .collect()
}

/// What admitting `bound` costs, with the products the admission reads.
fn admission_cost(
    bound: &Arc<ResolvedQuery>,
    fingerprint: Fingerprint,
    aggregates: &HashSet<EcoString>,
    catalog: &Catalog,
) -> Cost {
    let (admission, allocations, bytes) = allocations_count(|| {
        query_admission_analyze(
            AdmissionQuery {
                bound,
                decorrelated: None,
            },
            fingerprint,
            bound.tree().root().has_limit(),
            aggregates,
            catalog,
            AdmissionDepth::Full,
        )
    });
    black_box(admission.expect("the fixture SQL is admitted").tables.len());
    (allocations, bytes)
}

/// The cost of each stage on one shape, in column order. The products of
/// the chain (parse, resolve, canonicalise, fingerprint) are kept as they
/// are made, since each later stage reads them.
fn stage_costs(shape: &Shape, catalog: &Catalog) -> Vec<(&'static str, Cost)> {
    let settings = LiteralSettings::default();
    let search_path: [EcoString; 1] = ["public".into()];
    let aggregates = aggregates();
    let (rope, convert_n, convert_b) = allocations_count(|| parse(&shape.sql));
    let (resolved, resolve_n, resolve_b) = allocations_count(|| resolve_bound(&rope, catalog));
    let bound_clone = cost(|| resolved.clone());
    let (bound, canon_n, canon_b) = allocations_count(|| canonicalise(&resolved, &settings));
    let (fingerprint, fingerprint_n, fingerprint_b) = allocations_count(|| bound.fingerprint());
    let shared = Arc::new(bound.clone());
    vec![
        ("convert", (convert_n, convert_b)),
        (
            "session_key",
            cost(|| query_tree_session_key(&rope, &search_path, &settings)),
        ),
        ("rope_clone", cost(|| rope.clone())),
        ("resolve", (resolve_n, resolve_b)),
        ("bound_clone", bound_clone),
        // Canonicalising clones the bound query first; the cell is its own cost.
        (
            "canonicalise",
            (
                canon_n.saturating_sub(bound_clone.0),
                canon_b.saturating_sub(bound_clone.1),
            ),
        ),
        ("pushdown", cost(|| query_predicate_pushdown(&bound))),
        (
            "decorrelate",
            cost(|| query_decorrelate(&bound, &aggregates)),
        ),
        ("fingerprint", (fingerprint_n, fingerprint_b)),
        ("render", cost(|| bound.sql())),
        ("shape", cost(|| bound.shape())),
        (
            "pred_compile",
            cost(|| predicate_compile(&bound, shape.predicate_table)),
        ),
        (
            "update_queries",
            cost(|| query_table_update_queries(&bound)),
        ),
        (
            "admission",
            admission_cost(&shared, fingerprint, &aggregates, catalog),
        ),
    ]
}

fn allocations_report(shapes: &[Shape], catalog: &Catalog) {
    println!("\n== allocations per stage (count / bytes)");
    let mut header_printed = false;
    for shape in shapes {
        let costs = stage_costs(shape, catalog);
        if !header_printed {
            let header: Vec<String> = std::iter::once("query")
                .chain(costs.iter().map(|(stage, _)| *stage))
                .map(str::to_owned)
                .collect();
            row_print(&header);
            header_printed = true;
        }
        let row: Vec<String> = std::iter::once(shape.name.to_owned())
            .chain(costs.iter().map(|(_, (n, b))| format!("{n}/{b}")))
            .collect();
        row_print(&row);
    }
}

/// One row of the allocations table: the query name, then a cell per stage.
fn row_print(cells: &[String]) {
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

/// One timed stage: its name and the work criterion repeats.
type Stage<'a> = (&'a str, Box<dyn Fn() + 'a>);

/// What the pipeline shares across shapes.
struct Environment {
    catalog: Catalog,
    settings: LiteralSettings,
    search_path: [EcoString; 1],
    aggregates: HashSet<EcoString>,
}

/// One shape's products, each stage fed the previous stage's output as a
/// fixed input so a regression lands on one bar.
struct Products {
    tree: QueryTree,
    resolved: ResolvedQuery,
    bound: ResolvedQuery,
    predicate: Option<CompiledPredicate>,
    row: Vec<Option<ByteString>>,
}

fn products(shape: &Shape, env: &Environment) -> Products {
    let tree = parse(&shape.sql);
    let resolved = resolve_bound(&tree, &env.catalog);
    let bound = canonicalise(&resolved, &env.settings);
    let predicate = predicate_compile(&bound, shape.predicate_table);
    let row = row_build(shape.row);
    Products {
        tree,
        resolved,
        bound,
        predicate,
        row,
    }
}

fn timed_stages<'a>(shape: &'a Shape, made: &'a Products, env: &'a Environment) -> Vec<Stage<'a>> {
    let Products {
        tree,
        resolved,
        bound,
        predicate,
        row,
    } = made;
    let mut stages: Vec<Stage<'a>> = vec![
        (
            "convert",
            Box::new(|| {
                black_box(parse(&shape.sql));
            }),
        ),
        (
            "session_key",
            Box::new(|| {
                black_box(query_tree_session_key(
                    tree,
                    &env.search_path,
                    &env.settings,
                ));
            }),
        ),
        (
            "resolve",
            Box::new(|| {
                black_box(resolve_bound(tree, &env.catalog));
            }),
        ),
        (
            "bound_clone",
            Box::new(|| {
                black_box(bound.clone());
            }),
        ),
        (
            "canonicalise",
            Box::new(|| {
                black_box(canonicalise(resolved, &env.settings));
            }),
        ),
        (
            "pushdown",
            Box::new(|| {
                black_box(query_predicate_pushdown(bound));
            }),
        ),
        (
            "decorrelate",
            Box::new(|| {
                let _ = black_box(query_decorrelate(bound, &env.aggregates));
            }),
        ),
        (
            "fingerprint",
            Box::new(|| {
                black_box(bound.fingerprint());
            }),
        ),
        (
            "render",
            Box::new(|| {
                black_box(bound.sql());
            }),
        ),
        (
            "shape",
            Box::new(|| {
                black_box(bound.shape());
            }),
        ),
        (
            "predicate_compile",
            Box::new(|| {
                black_box(predicate_compile(bound, shape.predicate_table));
            }),
        ),
        (
            "end_to_end",
            Box::new(|| {
                let tree = parse(&shape.sql);
                let key = query_tree_session_key(&tree, &env.search_path, &env.settings);
                let resolved = resolve_bound(&tree, &env.catalog);
                let bound = canonicalise(&resolved, &env.settings);
                black_box((key, bound.fingerprint(), bound.sql()));
            }),
        ),
    ];
    if let Some(predicate) = predicate {
        stages.push((
            "predicate_eval",
            Box::new(|| {
                black_box(predicate.eval(row));
            }),
        ));
    }
    stages
}

fn bench_pipeline(c: &mut Criterion) {
    let env = Environment {
        catalog: catalog(),
        settings: LiteralSettings::default(),
        search_path: ["public".into()],
        aggregates: aggregates(),
    };
    let shapes = shapes();
    sizes_report();
    allocations_report(&shapes, &env.catalog);

    let mut group = c.benchmark_group("ast_pipeline");
    for shape in &shapes {
        let made = products(shape, &env);
        for (stage, run) in timed_stages(shape, &made, &env) {
            group.bench_function(BenchmarkId::new(stage, shape.name), |b| b.iter(&run));
        }
    }
    group.finish();
}

criterion_group!(benches, bench_pipeline);
criterion_main!(benches);
