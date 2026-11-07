//! Embedded snapshot of builtin `pg_proc` metadata: the function catalog
//! and the aggregate names, the two catalog inputs the analysis pipeline
//! normally loads from a live database (`FunctionCatalog::load` /
//! `aggregate_functions_load`).
//!
//! `data/functions_regenerate.sh` rewrites `data/functions.tsv` and
//! `data/aggregates.tsv` from a current PostgreSQL with the loaders' own
//! queries, restricted to the schemas PostgreSQL ships.

use std::collections::HashSet;

use ecow::EcoString;
use pgcache_lib::catalog::{FunctionCatalog, FunctionKind, FunctionRow, FunctionVolatility};
use pgcache_lib::oid::{FunctionOid, TypeOid};

static FUNCTIONS_TSV: &str = include_str!("../data/functions.tsv");
static AGGREGATES_TSV: &str = include_str!("../data/aggregates.tsv");

pub struct BuiltinFunctions {
    /// Every builtin non-aggregate function. A name absent from it degrades
    /// to non-immutable in the cacheability check, matching proxy behavior
    /// for unknown functions.
    pub catalog: FunctionCatalog,
    /// Names with at least one aggregate overload; drives decorrelation's
    /// GROUP BY decision for scalar subqueries.
    pub aggregates: HashSet<EcoString>,
}

pub fn builtin_functions_load() -> BuiltinFunctions {
    let rows = data_lines(FUNCTIONS_TSV)
        .filter_map(function_row_parse)
        .collect();
    let aggregates = data_lines(AGGREGATES_TSV).map(EcoString::from).collect();
    BuiltinFunctions {
        catalog: FunctionCatalog::from_rows(rows),
        aggregates,
    }
}

fn data_lines(tsv: &str) -> impl Iterator<Item = &str> {
    tsv.lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
}

/// One `psql -At` line of `FUNCTION_CATALOG_SQL`'s columns.
fn function_row_parse(line: &str) -> Option<FunctionRow> {
    let mut fields = line.split('\t');
    let oid = fields.next()?.parse().ok()?;
    let schema = fields.next()?;
    let name = fields.next()?;
    let arg_count = fields.next()?.parse().ok()?;
    let default_count = fields.next()?.parse().ok()?;
    let variadic = fields.next()? == "t";
    let volatility = FunctionVolatility::from_provolatile(fields.next()?)?;
    let language = fields.next()?;
    let security_definer = fields.next()? == "t";
    let returns_set = fields.next()? == "t";
    let return_type = fields.next()?.parse().ok()?;
    let kind = FunctionKind::from_prokind(fields.next()?)?;
    Some(FunctionRow {
        oid: FunctionOid::from_raw(oid),
        schema: EcoString::from(schema),
        name: EcoString::from(name),
        arg_count,
        default_count,
        variadic,
        volatility,
        language: EcoString::from(language),
        security_definer,
        returns_set,
        return_type: TypeOid::from_raw(return_type),
        kind,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builtin_functions_snapshot_sane() {
        let b = builtin_functions_load();
        let worst = |name, arg_count| b.catalog.builtin_worst_volatility(None, name, arg_count);
        assert_eq!(worst("abs", 1), Some(FunctionVolatility::Immutable));
        assert_eq!(worst("now", 0), Some(FunctionVolatility::Stable));
        assert_eq!(worst("random", 0), Some(FunctionVolatility::Volatile));
        // `num_nonnulls` is variadic; `concat` is STABLE (it renders its
        // arguments under the session's settings).
        assert_eq!(
            worst("num_nonnulls", 5),
            Some(FunctionVolatility::Immutable)
        );
        assert_eq!(worst("concat", 3), Some(FunctionVolatility::Stable));
        assert_eq!(worst("substring", 2), Some(FunctionVolatility::Immutable));
        // An aggregate is in the catalog but never judged by volatility.
        assert_eq!(worst("max", 1), None);
        assert!(
            b.catalog
                .named("max")
                .any(|row| row.kind == FunctionKind::Aggregate)
        );
        assert!(b.aggregates.contains("max"));
        assert!(b.aggregates.contains("count"));
        assert!(b.aggregates.contains("sum"));
        assert!(!b.aggregates.contains("lower"));
        assert!(b.catalog.len() > 3000, "{}", b.catalog.len());
    }
}
