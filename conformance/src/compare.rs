//! Result-set comparison between origin (the oracle) and pgcache,
//! following standard sqllogictest sort semantics.
//!
//! Origin's output is authoritative; any divergence is a failure. The
//! sort strategy is chosen per statement: `nosort` compares row order
//! verbatim (only sound when the query has an `ORDER BY`), `rowsort`
//! compares as a sorted multiset of rows, `valuesort` as a sorted
//! multiset of individual values.

use crate::drivers::QueryResult;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortStrategy {
    /// Compare rows in received order (query must be `ORDER BY`-stable).
    None,
    /// Compare as a sorted multiset of rows.
    Rows,
    /// Compare as a sorted multiset of individual values.
    Values,
}

/// Compare two result sets. `Ok(())` means they match; `Err` carries a
/// short human-readable reason for the failure bucket.
pub fn results_match(
    origin: &QueryResult,
    cache: &QueryResult,
    strategy: SortStrategy,
) -> Result<(), String> {
    if strategy != SortStrategy::Values && origin.column_count != cache.column_count {
        return Err(format!(
            "column count differs: origin {}, pgcache {}",
            origin.column_count, cache.column_count
        ));
    }
    if origin.rows.len() != cache.rows.len() {
        return Err(format!(
            "row count differs: origin {}, pgcache {}",
            origin.rows.len(),
            cache.rows.len()
        ));
    }

    let (o, c) = match strategy {
        SortStrategy::None => (origin.rows.clone(), cache.rows.clone()),
        SortStrategy::Rows => {
            let mut o = origin.rows.clone();
            let mut c = cache.rows.clone();
            o.sort();
            c.sort();
            (o, c)
        }
        SortStrategy::Values => {
            let mut o: Vec<String> = origin.rows.iter().flatten().cloned().collect();
            let mut c: Vec<String> = cache.rows.iter().flatten().cloned().collect();
            o.sort();
            c.sort();
            (vec![o], vec![c])
        }
    };

    if o == c {
        return Ok(());
    }

    // First differing row, for a one-line diagnosis.
    for (i, (or, cr)) in o.iter().zip(c.iter()).enumerate() {
        if or != cr {
            return Err(format!("row {i} differs: origin {or:?}, pgcache {cr:?}"));
        }
    }
    Err("result sets differ".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn qr(rows: &[&[&str]]) -> QueryResult {
        let rows: Vec<Vec<String>> = rows
            .iter()
            .map(|r| r.iter().map(|s| s.to_string()).collect())
            .collect();
        QueryResult {
            column_count: rows.first().map(|r| r.len()).unwrap_or(0),
            rows,
        }
    }

    #[test]
    fn identical_matches_in_every_mode() {
        let a = qr(&[&["1", "x"], &["2", "y"]]);
        for s in [SortStrategy::None, SortStrategy::Rows, SortStrategy::Values] {
            assert!(results_match(&a, &a, s).is_ok());
        }
    }

    #[test]
    fn row_order_fails_nosort_but_passes_rowsort() {
        let a = qr(&[&["1"], &["2"]]);
        let b = qr(&[&["2"], &["1"]]);
        assert!(results_match(&a, &b, SortStrategy::None).is_err());
        assert!(results_match(&a, &b, SortStrategy::Rows).is_ok());
    }

    #[test]
    fn valuesort_ignores_row_shape() {
        let a = qr(&[&["1", "2"], &["3", "4"]]);
        let b = qr(&[&["4", "3"], &["2", "1"]]);
        assert!(results_match(&a, &b, SortStrategy::Values).is_ok());
        assert!(results_match(&a, &b, SortStrategy::Rows).is_err());
    }

    #[test]
    fn divergent_value_is_reported() {
        let a = qr(&[&["1"], &["2"]]);
        let b = qr(&[&["1"], &["9"]]);
        let err = results_match(&a, &b, SortStrategy::None).unwrap_err();
        assert!(err.contains("row 1 differs"));
    }

    #[test]
    fn row_count_mismatch_is_reported() {
        let a = qr(&[&["1"]]);
        let b = qr(&[&["1"], &["2"]]);
        assert!(
            results_match(&a, &b, SortStrategy::Rows)
                .unwrap_err()
                .contains("row count differs")
        );
    }
}
