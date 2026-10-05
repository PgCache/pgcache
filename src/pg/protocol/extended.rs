use ecow::EcoString;
use rootcause::Report;
use tokio_util::bytes::{Buf, Bytes, BytesMut};

use super::session::ResultFormats;
use super::{ByteString, ProtocolError, ProtocolResult};
use crate::oid::TypeOid;

/// Convert a wire-protocol count (`i16` or `i32`) into a `usize`, returning a parse
/// error for negative values rather than silently sign-extending into a huge length.
fn count_to_usize<T>(value: T, what: &'static str) -> ProtocolResult<usize>
where
    usize: TryFrom<T>,
    T: std::fmt::Display + Copy,
{
    usize::try_from(value).map_err(|_| {
        ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("invalid {what}: {value}"),
        ))
        .into()
    })
}

/// Parsed Parse message data
#[derive(Debug, Clone)]
pub(crate) struct ParsedParseMessage {
    pub statement_name: EcoString,
    /// Zero-copy view into the Parse frame passed to `parse_parse_message`.
    pub sql: ByteString,
    pub parameter_oids: Vec<TypeOid>,
}

/// Parsed Bind message data
#[derive(Debug, Clone)]
pub(crate) struct ParsedBindMessage {
    pub portal_name: EcoString,
    pub statement_name: EcoString,
    pub parameter_formats: Vec<i16>,
    pub parameter_values: Vec<Option<Bytes>>,
    pub result_formats: ResultFormats,
}

/// Parsed Execute message data
#[derive(Debug, Clone)]
pub(crate) struct ParsedExecuteMessage {
    pub portal_name: EcoString,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "cache hits ignore partial-fetch Execute (PGC-467)"
        )
    )]
    pub max_rows: i32,
}

/// Parsed Describe message data
#[derive(Debug, Clone)]
pub(crate) struct ParsedDescribeMessage {
    pub describe_type: u8, // b'S' for statement, b'P' for portal
    pub name: EcoString,
}

/// Parsed ParameterDescription message data (backend response)
#[derive(Debug, Clone)]
pub(crate) struct ParsedParameterDescription {
    pub parameter_oids: Vec<TypeOid>,
}

/// Parsed Close message data
#[derive(Debug, Clone)]
pub(crate) struct ParsedCloseMessage {
    pub close_type: u8, // b'S' for statement, b'P' for portal
    pub name: EcoString,
}

/// A truncated-message error describing what was missing.
fn truncated(what: &'static str) -> Report<ProtocolError> {
    ProtocolError::IoError(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, what)).into()
}

/// The message body past the tag byte and length word, or `too_short`.
fn message_body<'a>(data: &'a [u8], too_short: &'static str) -> ProtocolResult<&'a [u8]> {
    data.get(5..).ok_or_else(|| truncated(too_short))
}

fn read_u8(buf: &mut &[u8], missing: &'static str) -> ProtocolResult<u8> {
    if buf.is_empty() {
        return Err(truncated(missing));
    }
    Ok(buf.get_u8())
}

fn read_i16(buf: &mut &[u8], missing: &'static str) -> ProtocolResult<i16> {
    if buf.len() < 2 {
        return Err(truncated(missing));
    }
    Ok(buf.get_i16())
}

fn read_i32(buf: &mut &[u8], missing: &'static str) -> ProtocolResult<i32> {
    if buf.len() < 4 {
        return Err(truncated(missing));
    }
    Ok(buf.get_i32())
}

fn read_u32(buf: &mut &[u8], missing: &'static str) -> ProtocolResult<u32> {
    if buf.len() < 4 {
        return Err(truncated(missing));
    }
    Ok(buf.get_u32())
}

/// Read a null-terminated string from the buffer
fn read_cstring<'a>(buf: &mut &'a [u8]) -> ProtocolResult<&'a str> {
    let null_pos = buf
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| truncated("missing null terminator"))?;

    let (bytes, rest) = buf.split_at(null_pos);
    let s = std::str::from_utf8(bytes).map_err(|_| {
        ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid UTF-8 in string",
        ))
    })?;

    // Skip past the null terminator
    *buf = rest.get(1..).unwrap_or_default();
    Ok(s)
}

/// An `Int16` count followed by that many type OIDs (`Int32`).
fn type_oids_read(
    buf: &mut &[u8],
    count_missing: &'static str,
    count_what: &'static str,
    oid_missing: &'static str,
) -> ProtocolResult<Vec<TypeOid>> {
    let count = count_to_usize(read_i16(buf, count_missing)?, count_what)?;
    let mut oids = Vec::with_capacity(count);
    for _ in 0..count {
        oids.push(TypeOid::from_raw(read_u32(buf, oid_missing)?));
    }
    Ok(oids)
}

/// Parse a Parse message ('P')
///
/// Format:
/// Byte1('P')
/// Int32 - message length
/// String - statement name (empty string for unnamed)
/// String - SQL query
/// Int16 - number of parameter data types
/// For each parameter:
///     Int32 - OID of parameter data type (0 = unspecified)
pub(crate) fn parse_parse_message(data: &Bytes) -> ProtocolResult<ParsedParseMessage> {
    let mut buf = message_body(data, "Parse message too short")?;

    let statement_name = read_cstring(&mut buf)?.into();
    // Zero-copy: the SQL is a refcounted slice of the frame, not a fresh String.
    let sql_str = read_cstring(&mut buf)?;
    let sql = ByteString::from_utf8(data.slice_ref(sql_str.as_bytes())).map_err(|_| {
        ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid UTF-8 in SQL",
        ))
    })?;
    let parameter_oids = type_oids_read(
        &mut buf,
        "missing parameter count",
        "Parse parameter count",
        "missing parameter OID",
    )?;

    Ok(ParsedParseMessage {
        statement_name,
        sql,
        parameter_oids,
    })
}

/// Parse a Bind message ('B')
///
/// Format:
/// Byte1('B')
/// Int32 - message length
/// String - portal name (empty string for unnamed)
/// String - statement name
/// Int16 - number of parameter format codes
/// For each format code:
///     Int16 - format code (0=text, 1=binary)
/// Int16 - number of parameter values
/// For each parameter:
///     Int32 - parameter length (-1 = NULL)
///     Byte[n] - parameter value
/// Int16 - number of result column format codes
/// For each format code:
///     Int16 - format code (0=text, 1=binary)
pub(crate) fn parse_bind_message(data: &BytesMut) -> ProtocolResult<ParsedBindMessage> {
    let mut buf = message_body(data, "Bind message too short")?;

    let portal_name = read_cstring(&mut buf)?.into();
    let statement_name = read_cstring(&mut buf)?.into();
    let parameter_formats = parameter_formats_read(&mut buf)?;
    let parameter_values = parameter_values_read(&mut buf)?;
    let result_formats = result_formats_read(&mut buf)?;

    Ok(ParsedBindMessage {
        portal_name,
        statement_name,
        parameter_formats,
        parameter_values,
        result_formats,
    })
}

fn parameter_formats_read(buf: &mut &[u8]) -> ProtocolResult<Vec<i16>> {
    let count = count_to_usize(
        read_i16(buf, "missing format code count")?,
        "Bind format code count",
    )?;
    let mut formats = Vec::with_capacity(count);
    for _ in 0..count {
        formats.push(read_i16(buf, "missing format code")?);
    }
    Ok(formats)
}

/// The Bind parameter values: each an `Int32` length (`-1` = NULL) and that
/// many bytes.
fn parameter_values_read(buf: &mut &[u8]) -> ProtocolResult<Vec<Option<Bytes>>> {
    let count = count_to_usize(
        read_i16(buf, "missing parameter value count")?,
        "Bind parameter value count",
    )?;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(parameter_value_read(buf)?);
    }
    Ok(values)
}

fn parameter_value_read(buf: &mut &[u8]) -> ProtocolResult<Option<Bytes>> {
    let param_len = read_i32(buf, "missing parameter length")?;
    if param_len == -1 {
        return Ok(None);
    }
    let param_len = count_to_usize(param_len, "Bind parameter length")?;
    let value_bytes = buf
        .get(..param_len)
        .ok_or_else(|| truncated("parameter value truncated"))?;
    let value = Bytes::copy_from_slice(value_bytes);
    buf.advance(param_len);
    Ok(Some(value))
}

fn result_formats_read(buf: &mut &[u8]) -> ProtocolResult<ResultFormats> {
    let count = count_to_usize(
        read_i16(buf, "missing result format code count")?,
        "Bind result format count",
    )?;
    let mut result_formats = ResultFormats::Implicit;
    for index in 0..count {
        let code = read_i16(buf, "missing result format code")?;
        result_formats = result_format_push(result_formats, index, code);
    }
    Ok(result_formats)
}

/// Fold the `index`th result format code into the formats so far: uniform
/// while every code matches, per-column from the first that differs.
fn result_format_push(formats: ResultFormats, index: usize, code: i16) -> ResultFormats {
    match formats {
        ResultFormats::Implicit => ResultFormats::Uniform(code),
        ResultFormats::Uniform(first) if first == code => ResultFormats::Uniform(first),
        ResultFormats::Uniform(first) => {
            let mut codes = vec![first; index];
            codes.push(code);
            ResultFormats::PerColumn(codes)
        }
        ResultFormats::PerColumn(mut codes) => {
            codes.push(code);
            ResultFormats::PerColumn(codes)
        }
    }
}

/// Parse an Execute message ('E')
///
/// Format:
/// Byte1('E')
/// Int32 - message length
/// String - portal name (empty string for unnamed)
/// Int32 - maximum number of rows to return (0 = unlimited)
pub(crate) fn parse_execute_message(data: &BytesMut) -> ProtocolResult<ParsedExecuteMessage> {
    let mut buf = message_body(data, "Execute message too short")?;
    let portal_name = read_cstring(&mut buf)?.into();
    let max_rows = read_i32(&mut buf, "missing max_rows")?;
    Ok(ParsedExecuteMessage {
        portal_name,
        max_rows,
    })
}

/// A Describe or Close body: `Byte1` — 'S' for statement, 'P' for portal —
/// then the statement or portal name.
fn target_message_parse(data: &[u8], too_short: &'static str) -> ProtocolResult<(u8, EcoString)> {
    let mut buf = message_body(data, too_short)?;
    let target = read_u8(&mut buf, too_short)?;
    let name = read_cstring(&mut buf)?.into();
    Ok((target, name))
}

/// Parse a Describe message ('D')
///
/// Format:
/// Byte1('D')
/// Int32 - message length
/// Byte1 - 'S' for statement, 'P' for portal
/// String - name of statement or portal
pub(crate) fn parse_describe_message(data: &BytesMut) -> ProtocolResult<ParsedDescribeMessage> {
    let (describe_type, name) = target_message_parse(data, "Describe message too short")?;
    Ok(ParsedDescribeMessage {
        describe_type,
        name,
    })
}

/// Parse a Close message ('C')
///
/// Format:
/// Byte1('C')
/// Int32 - message length
/// Byte1 - 'S' for statement, 'P' for portal
/// String - name of statement or portal
pub(crate) fn parse_close_message(data: &BytesMut) -> ProtocolResult<ParsedCloseMessage> {
    let (close_type, name) = target_message_parse(data, "Close message too short")?;
    Ok(ParsedCloseMessage { close_type, name })
}

/// Parse a ParameterDescription message ('t') from backend
///
/// Format:
/// Byte1('t')
/// Int32 - message length
/// Int16 - number of parameters
/// For each parameter:
///     Int32 - OID of parameter data type
pub(crate) fn parse_parameter_description(
    data: &[u8],
) -> ProtocolResult<ParsedParameterDescription> {
    let too_short = "ParameterDescription message too short";
    let mut buf = message_body(data, too_short)?;
    let parameter_oids = type_oids_read(
        &mut buf,
        too_short,
        "ParameterDescription parameter count",
        "missing parameter OID in ParameterDescription",
    )?;
    Ok(ParsedParameterDescription { parameter_oids })
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use super::*;
    use crate::cache::query::CacheableQuery;
    use crate::query::ast::{QueryBody, query_expr_parse};

    /// A frontend message: `tag`, the length word (counting itself), `body`.
    fn frame(tag: u8, body: &[u8]) -> BytesMut {
        let len = i32::try_from(body.len() + 4).expect("test frame length fits i32");
        let mut data = BytesMut::new();
        data.extend_from_slice(&[tag]);
        data.extend_from_slice(&len.to_be_bytes());
        data.extend_from_slice(body);
        data
    }

    #[test]
    fn test_parse_parse_message() {
        // Parse message: statement name "stmt1", SQL "SELECT 1", no parameters
        let mut data = BytesMut::new();
        data.extend_from_slice(b"P"); // tag
        data.extend_from_slice(&[0, 0, 0, 20]); // length (21 bytes total - 1 for tag = 20)
        data.extend_from_slice(b"stmt1\0"); // statement name
        data.extend_from_slice(b"SELECT 1\0"); // SQL
        data.extend_from_slice(&[0, 0]); // 0 parameters

        let result = parse_parse_message(&data.freeze()).unwrap();
        assert_eq!(result.statement_name, "stmt1");
        assert_eq!(result.sql, "SELECT 1");
        assert_eq!(result.parameter_oids.len(), 0);
    }

    #[test]
    fn test_parse_parse_message_with_params() {
        // Parse message with 2 parameters
        let mut data = BytesMut::new();
        data.extend_from_slice(b"P");
        data.extend_from_slice(&[0, 0, 0, 33]); // length
        data.extend_from_slice(b"\0"); // unnamed statement
        data.extend_from_slice(b"SELECT $1, $2\0"); // SQL
        data.extend_from_slice(&[0, 2]); // 2 parameters
        data.extend_from_slice(&[0, 0, 0, 23]); // OID 23 (int4)
        data.extend_from_slice(&[0, 0, 0, 25]); // OID 25 (text)

        let result = parse_parse_message(&data.freeze()).unwrap();
        assert_eq!(result.statement_name, "");
        assert_eq!(result.sql, "SELECT $1, $2");
        assert_eq!(
            result.parameter_oids,
            vec![TypeOid::from_raw(23), TypeOid::from_raw(25)]
        );
    }

    #[test]
    fn test_parse_bind_message() {
        // Bind message: portal "p1", statement "s1", 1 text param "42", text result
        let mut data = BytesMut::new();
        data.extend_from_slice(b"B"); // tag
        data.extend_from_slice(&[0, 0, 0, 22]); // length
        data.extend_from_slice(b"p1\0"); // portal name
        data.extend_from_slice(b"s1\0"); // statement name
        data.extend_from_slice(&[0, 1]); // 1 format code
        data.extend_from_slice(&[0, 0]); // format 0 (text)
        data.extend_from_slice(&[0, 1]); // 1 parameter value
        data.extend_from_slice(&[0, 0, 0, 2]); // length 2
        data.extend_from_slice(b"42"); // value "42"
        data.extend_from_slice(&[0, 1]); // 1 result format code
        data.extend_from_slice(&[0, 0]); // format 0 (text)

        let result = parse_bind_message(&data).unwrap();
        assert_eq!(result.portal_name, "p1");
        assert_eq!(result.statement_name, "s1");
        assert_eq!(result.parameter_formats, vec![0]);
        assert_eq!(result.parameter_values.len(), 1);
        assert_eq!(result.parameter_values[0], Some(Bytes::from_static(b"42")));
        assert_eq!(result.result_formats, ResultFormats::Uniform(0));
    }

    #[test]
    fn test_parse_bind_message_with_null() {
        // Bind message with NULL parameter
        let mut data = BytesMut::new();
        data.extend_from_slice(b"B");
        data.extend_from_slice(&[0, 0, 0, 18]); // length
        data.extend_from_slice(b"\0"); // unnamed portal
        data.extend_from_slice(b"\0"); // unnamed statement
        data.extend_from_slice(&[0, 0]); // 0 format codes (use default text)
        data.extend_from_slice(&[0, 1]); // 1 parameter value
        data.extend_from_slice(&[255, 255, 255, 255]); // length -1 (NULL)
        data.extend_from_slice(&[0, 0]); // 0 result format codes

        let result = parse_bind_message(&data).unwrap();
        assert_eq!(result.portal_name, "");
        assert_eq!(result.statement_name, "");
        assert_eq!(result.parameter_values.len(), 1);
        assert_eq!(result.parameter_values[0], None);
    }

    #[test]
    fn test_parse_execute_message() {
        // Execute message: portal "p1", max_rows 100
        let data = frame(b'E', b"p1\0\0\0\0\x64");

        let result = parse_execute_message(&data).unwrap();
        assert_eq!(result.portal_name, "p1");
        assert_eq!(result.max_rows, 100);
    }

    #[test]
    fn test_parse_execute_message_unlimited() {
        // Execute message: unnamed portal, max_rows = 0 (unlimited)
        let data = frame(b'E', b"\0\0\0\0\0");

        let result = parse_execute_message(&data).unwrap();
        assert_eq!(result.portal_name, "");
        assert_eq!(result.max_rows, 0);
    }

    #[test]
    fn test_parse_describe_message_statement() {
        let data = frame(b'D', b"Sstmt1\0");

        let result = parse_describe_message(&data).unwrap();
        assert_eq!(result.describe_type, b'S');
        assert_eq!(result.name, "stmt1");
    }

    #[test]
    fn test_parse_describe_message_portal() {
        let data = frame(b'D', b"Pp1\0");

        let result = parse_describe_message(&data).unwrap();
        assert_eq!(result.describe_type, b'P');
        assert_eq!(result.name, "p1");
    }

    #[test]
    fn test_parse_close_message_statement() {
        let data = frame(b'C', b"Sstmt1\0");

        let result = parse_close_message(&data).unwrap();
        assert_eq!(result.close_type, b'S');
        assert_eq!(result.name, "stmt1");
    }

    #[test]
    fn test_parse_close_message_portal() {
        let data = frame(b'C', b"Pp1\0");

        let result = parse_close_message(&data).unwrap();
        assert_eq!(result.close_type, b'P');
        assert_eq!(result.name, "p1");
    }

    #[test]
    fn test_parse_parse_message_truncated() {
        // Truncated Parse message
        let mut data = BytesMut::new();
        data.extend_from_slice(b"P");
        data.extend_from_slice(&[0, 0, 0, 10]);

        let result = parse_parse_message(&data.freeze());
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_cacheable_select() {
        // Simple SELECT with WHERE clause - should be cacheable
        let mut data = BytesMut::new();
        data.extend_from_slice(b"P");
        data.extend_from_slice(&[0, 0, 0, 50]); // length
        data.extend_from_slice(b"stmt1\0"); // statement name
        data.extend_from_slice(b"SELECT id, data FROM test WHERE id = $1\0"); // SQL
        data.extend_from_slice(&[0, 1]); // 1 parameter
        data.extend_from_slice(&[0, 0, 0, 23]); // OID 23 (int4)

        let result = parse_parse_message(&data.freeze()).unwrap();
        assert_eq!(result.statement_name, "stmt1");
        assert_eq!(result.sql, "SELECT id, data FROM test WHERE id = $1");
        assert_eq!(result.parameter_oids, vec![TypeOid::from_raw(23)]);

        // Test that this SQL would be cacheable
        let query = query_expr_parse(&result.sql).unwrap();
        let cacheable = CacheableQuery::try_new(query, &HashMap::new());
        assert!(
            cacheable.is_ok(),
            "Simple SELECT with equality should be cacheable"
        );
    }

    #[test]
    fn test_parse_cacheable_subquery() {
        // SELECT with subquery - now cacheable (non-correlated subqueries are supported)
        let mut data = BytesMut::new();
        data.extend_from_slice(b"P");
        data.extend_from_slice(&[0, 0, 0, 80]); // length
        data.extend_from_slice(b"\0"); // unnamed statement
        data.extend_from_slice(
            b"SELECT id FROM test WHERE id IN (SELECT id FROM other WHERE val = $1)\0",
        );
        data.extend_from_slice(&[0, 1]); // 1 parameter
        data.extend_from_slice(&[0, 0, 0, 25]); // OID 25 (text)

        let result = parse_parse_message(&data.freeze()).unwrap();
        assert_eq!(result.statement_name, "");

        // Test that this SQL parses and IS cacheable (non-correlated subquery)
        let query = query_expr_parse(&result.sql).expect("subquery should parse to AST");

        // Verify has_subqueries() detects the subquery
        let QueryBody::Select(select) = &query.body else {
            panic!("expected SELECT");
        };
        assert!(
            select.has_subqueries(),
            "has_subqueries() should detect subquery in WHERE"
        );

        // Verify cacheability check accepts it (non-correlated subqueries are now cacheable)
        let cacheable_result = CacheableQuery::try_new(query, &HashMap::new());
        assert!(
            cacheable_result.is_ok(),
            "SELECT with non-correlated subquery should be cacheable"
        );
    }

    #[test]
    fn test_parse_cacheable_insert() {
        // INSERT statement - not a SELECT, should not be cacheable
        let mut data = BytesMut::new();
        data.extend_from_slice(b"P");
        data.extend_from_slice(&[0, 0, 0, 50]); // length
        data.extend_from_slice(b"\0"); // unnamed
        data.extend_from_slice(b"INSERT INTO test (id, data) VALUES ($1, $2)\0");
        data.extend_from_slice(&[0, 2]); // 2 parameters
        data.extend_from_slice(&[0, 0, 0, 23]); // OID 23 (int4)
        data.extend_from_slice(&[0, 0, 0, 25]); // OID 25 (text)

        let result = parse_parse_message(&data.freeze()).unwrap();

        // Test that INSERT is not cacheable (not a SELECT)
        let cacheable_result = query_expr_parse(&result.sql);
        assert!(
            cacheable_result.is_err(),
            "INSERT should not convert to cacheable query"
        );
    }
}
