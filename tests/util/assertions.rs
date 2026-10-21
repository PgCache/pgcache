use std::io::Error;

use tokio_postgres::SimpleQueryMessage;

use super::context::TestContext;

/// All data rows of a simple-query result, in order.
#[allow(clippy::wildcard_enum_match_arm)]
pub(crate) fn rows_of(messages: &[SimpleQueryMessage]) -> Vec<&tokio_postgres::SimpleQueryRow> {
    messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect()
}

/// Every row of a simple-query result as its column values, in order.
pub(crate) fn result_rows(messages: &[SimpleQueryMessage]) -> Vec<Vec<Option<String>>> {
    rows_of(messages)
        .iter()
        .map(|row| {
            (0..row.len())
                .map(|i| row.get(i).map(str::to_owned))
                .collect()
        })
        .collect()
}

/// Extract a row from SimpleQueryMessage results at the specified index.
/// Returns an error with context if the message at that index is not a Row.
pub(crate) fn extract_row(
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
pub(crate) fn assert_row_fields(row: &tokio_postgres::SimpleQueryRow, expected: &[(&str, &str)]) {
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
pub(crate) fn assert_row_at(
    results: &[SimpleQueryMessage],
    index: usize,
    expected: &[(&str, &str)],
) -> Result<(), Error> {
    let row = extract_row(results, index)?;
    assert_row_fields(row, expected);
    Ok(())
}

/// Asserts that `query`'s ORDER BY decides which rows its LIMIT returns on
/// origin: without the ORDER BY, the same LIMIT returns other rows. A
/// population that ignored the order would then cache the wrong rows, so a
/// test calling this before populating can't pass by accident (PGC-523).
pub(crate) async fn assert_limit_order_decides(
    ctx: &TestContext,
    query: &str,
) -> Result<(), Error> {
    let upper = query.to_ascii_uppercase();
    let parts = upper.find(" ORDER BY ").zip(upper.rfind(" LIMIT "));
    let Some((head, tail)) =
        parts.and_then(|(order, limit)| query.get(..order).zip(query.get(limit..)))
    else {
        return Err(Error::other(format!("expected ORDER BY … LIMIT: {query}")));
    };
    let unordered = format!("{head}{tail}");
    let origin_rows = async |sql: &str| -> Result<Vec<Vec<Option<String>>>, Error> {
        let messages = ctx.origin.simple_query(sql).await.map_err(Error::other)?;
        let mut rows = result_rows(&messages);
        rows.sort();
        Ok(rows)
    };
    let ordered_rows = origin_rows(query).await?;
    let unordered_rows = origin_rows(&unordered).await?;
    if ordered_rows == unordered_rows {
        return Err(Error::other(format!(
            "the rows' physical order already matches the ORDER BY, so an unordered population would pass: {query}"
        )));
    }
    Ok(())
}

/// Asserts that `query` through pgcache returns the same rows as origin,
/// in any order.
pub(crate) async fn assert_proxy_rows_match_origin(
    ctx: &mut TestContext,
    query: &str,
) -> Result<(), Error> {
    let (mut proxied, mut origin) = ctx.proxy_and_origin_rows(query).await?;
    proxied.sort();
    origin.sort();
    if proxied != origin {
        return Err(Error::other(format!(
            "pgcache served {proxied:?}, origin returned {origin:?}: {query}"
        )));
    }
    Ok(())
}
