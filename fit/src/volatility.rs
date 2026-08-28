//! Embedded snapshot of builtin `pg_proc` metadata: worst-case function
//! volatility and aggregate names, the two catalog inputs the analysis
//! pipeline normally loads from a live database
//! (`function_volatility_map_load` / `aggregate_functions_load`).
//!
//! Regenerate `data/functions.tsv` against a current PostgreSQL with:
//!
//! ```sql
//! SELECT p.proname,
//!        COALESCE(MAX(CASE p.provolatile WHEN 'v' THEN 'v' WHEN 's' THEN 's' ELSE 'i' END)
//!                 FILTER (WHERE p.prokind NOT IN ('a', 'w')), '-'),
//!        CASE WHEN bool_or(p.prokind = 'a') THEN 1 ELSE 0 END
//! FROM pg_proc p
//! JOIN pg_namespace n ON p.pronamespace = n.oid
//! GROUP BY p.proname
//! ORDER BY p.proname
//! ```
//!
//! (`psql -At -F $'\t'`.) The volatility half must keep excluding
//! `prokind IN ('a', 'w')` to match the proxy's loader — a plain GROUP BY
//! over all of pg_proc would poison names like `max` with aggregate rows.

use std::collections::{HashMap, HashSet};

use ecow::EcoString;
use pgcache_lib::catalog::FunctionVolatility;

static FUNCTIONS_TSV: &str = include_str!("../data/functions.tsv");

pub struct BuiltinFunctions {
    /// Function name → worst-case volatility across non-aggregate overloads.
    /// Names absent from the map degrade to non-immutable in the cacheability
    /// check, matching proxy behavior for unknown functions.
    pub volatility: HashMap<EcoString, FunctionVolatility>,
    /// Names with at least one aggregate overload; drives decorrelation's
    /// GROUP BY decision for scalar subqueries.
    pub aggregates: HashSet<EcoString>,
}

pub fn builtin_functions_load() -> BuiltinFunctions {
    let mut volatility = HashMap::new();
    let mut aggregates = HashSet::new();
    for line in FUNCTIONS_TSV.lines() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut fields = line.split('\t');
        let (Some(name), Some(vol), Some(agg)) = (fields.next(), fields.next(), fields.next())
        else {
            continue;
        };
        let parsed = match vol {
            "i" => Some(FunctionVolatility::Immutable),
            "s" => Some(FunctionVolatility::Stable),
            "v" => Some(FunctionVolatility::Volatile),
            // "-": every overload is aggregate/window; absent from the
            // proxy's volatility map too.
            _ => None,
        };
        if let Some(v) = parsed {
            volatility.insert(EcoString::from(name), v);
        }
        if agg == "1" {
            aggregates.insert(EcoString::from(name));
        }
    }
    BuiltinFunctions {
        volatility,
        aggregates,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builtin_functions_snapshot_sane() {
        let b = builtin_functions_load();
        assert_eq!(
            b.volatility.get("abs"),
            Some(&FunctionVolatility::Immutable)
        );
        assert_eq!(b.volatility.get("now"), Some(&FunctionVolatility::Stable));
        assert_eq!(
            b.volatility.get("random"),
            Some(&FunctionVolatility::Volatile)
        );
        // Aggregate-only names must not appear in the volatility map.
        assert!(!b.volatility.contains_key("max"));
        assert!(b.aggregates.contains("max"));
        assert!(b.aggregates.contains("count"));
        assert!(b.aggregates.contains("sum"));
        assert!(!b.aggregates.contains("lower"));
    }
}
