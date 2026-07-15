use std::io::Error;

use tokio_postgres::SimpleQueryMessage;

/// All data rows of a simple-query result, in order.
#[allow(clippy::wildcard_enum_match_arm)]
pub fn rows_of(messages: &[SimpleQueryMessage]) -> Vec<&tokio_postgres::SimpleQueryRow> {
    messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect()
}

/// Extract a row from SimpleQueryMessage results at the specified index.
/// Returns an error with context if the message at that index is not a Row.
pub fn extract_row(
    results: &[SimpleQueryMessage],
    index: usize,
) -> Result<&tokio_postgres::SimpleQueryRow, Error> {
    match results.get(index) {
        Some(SimpleQueryMessage::Row(row)) => Ok(row),
        Some(_) => Err(Error::other(format!(
            "Expected SimpleQueryMessage::Row at index {}, found different variant",
            index
        ))),
        None => Err(Error::other(format!(
            "Index {} out of bounds for results with length {}",
            index,
            results.len()
        ))),
    }
}

/// Assert that a row contains the expected field values.
/// Panics with a descriptive message if any assertion fails.
pub fn assert_row_fields(row: &tokio_postgres::SimpleQueryRow, expected: &[(&str, &str)]) {
    for (field, expected_value) in expected {
        assert_eq!(
            row.get::<&str>(field),
            Some(*expected_value),
            "Field '{}' did not match expected value",
            field
        );
    }
}

/// Convenience function combining row extraction and field assertion.
pub fn assert_row_at(
    results: &[SimpleQueryMessage],
    index: usize,
    expected: &[(&str, &str)],
) -> Result<(), Error> {
    let row = extract_row(results, index)?;
    assert_row_fields(row, expected);
    Ok(())
}
