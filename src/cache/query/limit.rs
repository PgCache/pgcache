//! How many rows a LIMIT/OFFSET needs, and whether a cached row cap covers it.

use crate::query::ast::{LimitClause, LiteralValue};

/// Extract the total rows needed from a LIMIT clause.
///
/// Returns `None` if there is no LIMIT count (= unlimited rows needed).
/// Returns `Some(limit + offset)` when a LIMIT count is present.
pub fn limit_rows_needed(limit: &Option<LimitClause>) -> Option<u64> {
    let limit_clause = limit.as_ref()?;
    let count = match &limit_clause.count {
        Some(LiteralValue::Integer(n)) => u64::try_from(*n).ok()?,
        _ => return None,
    };
    let offset = match &limit_clause.offset {
        Some(LiteralValue::Integer(n)) => u64::try_from(*n).unwrap_or(0),
        _ => 0,
    };
    Some(count + offset)
}

/// Check whether the cached `max_limit` is sufficient for the incoming `needed` rows.
///
/// - `cached_max`: `None` means all rows are cached.
/// - `needed`: `None` means all rows needed (no LIMIT).
pub fn limit_is_sufficient(cached_max: Option<u64>, needed: Option<u64>) -> bool {
    match (cached_max, needed) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(cached), Some(needed)) => cached >= needed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_limit_rows_needed() {
        // No limit clause
        assert_eq!(limit_rows_needed(&None), None);

        // LIMIT 10
        assert_eq!(
            limit_rows_needed(&Some(LimitClause {
                count: Some(LiteralValue::Integer(10)),
                offset: None,
            })),
            Some(10)
        );

        // LIMIT 10 OFFSET 5
        assert_eq!(
            limit_rows_needed(&Some(LimitClause {
                count: Some(LiteralValue::Integer(10)),
                offset: Some(LiteralValue::Integer(5)),
            })),
            Some(15)
        );

        // OFFSET only (no count) = unlimited
        assert_eq!(
            limit_rows_needed(&Some(LimitClause {
                count: None,
                offset: Some(LiteralValue::Integer(5)),
            })),
            None
        );
    }

    #[test]
    fn test_limit_is_sufficient() {
        // All rows cached → always sufficient
        assert!(limit_is_sufficient(None, None));
        assert!(limit_is_sufficient(None, Some(100)));

        // Some rows cached, need unlimited → insufficient
        assert!(!limit_is_sufficient(Some(50), None));

        // Some rows cached, need fewer → sufficient
        assert!(limit_is_sufficient(Some(50), Some(30)));
        assert!(limit_is_sufficient(Some(50), Some(50)));

        // Some rows cached, need more → insufficient
        assert!(!limit_is_sufficient(Some(50), Some(51)));
    }
}
