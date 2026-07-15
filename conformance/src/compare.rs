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

const FLOAT4_OID: u32 = 700;
const FLOAT8_OID: u32 = 701;

/// Relative tolerance for a float result column: float aggregates are
/// accumulation-order sensitive, and the cache's row order legitimately
/// differs from origin's heap order (PGC-281 — origin itself returns a
/// different float4 sum when its rows are visited in another order).
/// Sized to ulp-scale drift, far below any genuine value bug.
fn float_tolerance(type_oid: u32) -> Option<f64> {
    match type_oid {
        FLOAT4_OID => Some(1e-6),
        FLOAT8_OID => Some(1e-12),
        _ => None,
    }
}

/// Equal within `tol` relative difference; NaN matches NaN and
/// infinities must match exactly (sign included).
fn float_text_matches(origin: &str, cache: &str, tol: f64) -> bool {
    let (Ok(o), Ok(c)) = (origin.parse::<f64>(), cache.parse::<f64>()) else {
        return false;
    };
    if o.is_nan() || c.is_nan() {
        return o.is_nan() && c.is_nan();
    }
    if o.is_infinite() || c.is_infinite() {
        return o == c;
    }
    (o - c).abs() <= tol * o.abs().max(c.abs())
}

/// Exact match, or float-tolerant match when the column type allows it.
fn value_matches(origin: &str, cache: &str, type_oid: Option<u32>) -> bool {
    origin == cache
        || type_oid
            .and_then(float_tolerance)
            .is_some_and(|tol| float_text_matches(origin, cache, tol))
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
    // Result column types are part of the contract: a cached result
    // serving e.g. float8 where origin serves float4 is a bug even when
    // the text happens to match.
    if !origin.column_type_oids.is_empty() && !cache.column_type_oids.is_empty() {
        for (i, (ot, ct)) in origin
            .column_type_oids
            .iter()
            .zip(cache.column_type_oids.iter())
            .enumerate()
        {
            if ot != ct {
                return Err(format!(
                    "column {i} type differs: origin oid {ot}, pgcache oid {ct}"
                ));
            }
        }
    }

    match strategy {
        SortStrategy::None => rows_match(&origin.rows, &cache.rows, &origin.column_type_oids),
        SortStrategy::Rows => {
            let mut o = origin.rows.clone();
            let mut c = cache.rows.clone();
            o.sort();
            c.sort();
            rows_match(&o, &c, &origin.column_type_oids)
        }
        SortStrategy::Values => {
            // Column identity is lost in a value-sorted multiset, so the
            // float tolerance is only sound when every column is a float
            // type — otherwise it would bleed onto numeric/text values
            // that happen to parse as floats.
            let per_column: Option<Vec<f64>> = origin
                .column_type_oids
                .iter()
                .map(|&oid| float_tolerance(oid))
                .collect();
            let tol = per_column.and_then(|tols| tols.into_iter().reduce(f64::max));
            let mut o: Vec<String> = origin.rows.iter().flatten().cloned().collect();
            let mut c: Vec<String> = cache.rows.iter().flatten().cloned().collect();
            if o.len() != c.len() {
                return Err(format!(
                    "value count differs: origin {}, pgcache {}",
                    o.len(),
                    c.len()
                ));
            }
            o.sort();
            c.sort();
            for (ov, cv) in o.iter().zip(c.iter()) {
                let matched = ov == cv || tol.is_some_and(|tol| float_text_matches(ov, cv, tol));
                if !matched {
                    return Err(format!("value differs: origin {ov:?}, pgcache {cv:?}"));
                }
            }
            Ok(())
        }
    }
}

/// Pairwise row comparison with per-column float tolerance. Rows are
/// pre-sorted by the caller when the strategy requires it; the sort is
/// textual, so two float values inside tolerance can in principle pair
/// against different neighbors — acceptable for a conformance harness.
fn rows_match(
    origin: &[Vec<String>],
    cache: &[Vec<String>],
    column_type_oids: &[u32],
) -> Result<(), String> {
    for (i, (or, cr)) in origin.iter().zip(cache.iter()).enumerate() {
        let row_matches = or.len() == cr.len()
            && or
                .iter()
                .zip(cr.iter())
                .enumerate()
                .all(|(j, (ov, cv))| value_matches(ov, cv, column_type_oids.get(j).copied()));
        if !row_matches {
            return Err(format!("row {i} differs: origin {or:?}, pgcache {cr:?}"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn qr(rows: &[&[&str]]) -> QueryResult {
        qr_typed(rows, &[])
    }

    fn qr_typed(rows: &[&[&str]], column_type_oids: &[u32]) -> QueryResult {
        let rows: Vec<Vec<String>> = rows
            .iter()
            .map(|r| r.iter().map(|s| s.to_string()).collect())
            .collect();
        QueryResult {
            column_count: rows.first().map(|r| r.len()).unwrap_or(0),
            column_type_oids: column_type_oids.to_vec(),
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

    /// PGC-281: ulp-scale float4 accumulation-order drift is tolerated.
    #[test]
    fn float4_column_tolerates_ulp_drift() {
        let a = qr_typed(&[&["431.7726"]], &[FLOAT4_OID]);
        let b = qr_typed(&[&["431.77258"]], &[FLOAT4_OID]);
        assert!(results_match(&a, &b, SortStrategy::None).is_ok());
    }

    #[test]
    fn float4_column_rejects_real_divergence() {
        let a = qr_typed(&[&["431.7726"]], &[FLOAT4_OID]);
        let b = qr_typed(&[&["431.8"]], &[FLOAT4_OID]);
        assert!(results_match(&a, &b, SortStrategy::None).is_err());
    }

    /// Tolerance is gated on the column type: text/numeric stay exact.
    #[test]
    fn non_float_column_stays_exact() {
        let a = qr_typed(&[&["431.7726"]], &[1700]); // numeric
        let b = qr_typed(&[&["431.77258"]], &[1700]);
        assert!(results_match(&a, &b, SortStrategy::None).is_err());
    }

    #[test]
    fn float8_column_uses_tight_tolerance() {
        let a = qr_typed(&[&["431.77260909229517"]], &[FLOAT8_OID]);
        let b = qr_typed(&[&["431.77260909229523"]], &[FLOAT8_OID]);
        assert!(results_match(&a, &b, SortStrategy::None).is_ok());
        let c = qr_typed(&[&["431.77258"]], &[FLOAT8_OID]);
        assert!(results_match(&a, &c, SortStrategy::None).is_err());
    }

    #[test]
    fn nan_matches_nan_infinity_is_exact() {
        let a = qr_typed(&[&["NaN", "Infinity"]], &[FLOAT8_OID, FLOAT8_OID]);
        assert!(results_match(&a, &a, SortStrategy::None).is_ok());
        let b = qr_typed(&[&["NaN", "-Infinity"]], &[FLOAT8_OID, FLOAT8_OID]);
        assert!(results_match(&a, &b, SortStrategy::None).is_err());
    }

    /// A served result type that differs from origin is a failure even
    /// when the text matches (the widening bug PGC-281 alleged).
    #[test]
    fn column_type_divergence_is_reported() {
        let a = qr_typed(&[&["431.7726"]], &[FLOAT4_OID]);
        let b = qr_typed(&[&["431.7726"]], &[FLOAT8_OID]);
        assert!(
            results_match(&a, &b, SortStrategy::None)
                .unwrap_err()
                .contains("type differs")
        );
    }

    #[test]
    fn valuesort_applies_float_tolerance_when_all_columns_float() {
        let a = qr_typed(&[&["1.5", "431.7726"]], &[FLOAT4_OID, FLOAT4_OID]);
        let b = qr_typed(&[&["1.5", "431.77258"]], &[FLOAT4_OID, FLOAT4_OID]);
        assert!(results_match(&a, &b, SortStrategy::Values).is_ok());
    }

    /// Value-sort loses column identity, so tolerance must not bleed onto
    /// non-float columns that happen to parse as floats.
    #[test]
    fn valuesort_mixed_columns_stay_exact() {
        let a = qr_typed(&[&["431.7726", "431.7726"]], &[1700, FLOAT4_OID]);
        let b = qr_typed(&[&["431.77258", "431.7726"]], &[1700, FLOAT4_OID]);
        assert!(results_match(&a, &b, SortStrategy::Values).is_err());
    }

    /// A dropped column shortens the flattened multiset; zip must not
    /// silently truncate the comparison.
    #[test]
    fn valuesort_value_count_mismatch_is_reported() {
        let a = qr_typed(&[&["1", "x"], &["2", "y"]], &[23, 25]);
        let b = qr_typed(&[&["1"], &["2"]], &[23]);
        assert!(
            results_match(&a, &b, SortStrategy::Values)
                .unwrap_err()
                .contains("value count differs")
        );
    }
}
