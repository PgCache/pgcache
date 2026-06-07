use std::sync::Arc;

use ecow::EcoString;
use tokio_util::bytes::{Buf, Bytes, BytesMut};

use super::{ProtocolError, ProtocolResult};
use crate::cache::query::CacheableQuery;

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

/// Classification of a prepared statement based on SQL analysis
#[derive(Debug, Clone)]
pub enum StatementType {
    /// SELECT statement that can be cached
    Cacheable(Arc<CacheableQuery>),
    /// Non-SELECT statement (INSERT, UPDATE, DELETE, DDL, etc.)
    NonSelect,
    /// SELECT statement that cannot be cached (complex features)
    UncacheableSelect,
    /// Failed to parse the SQL
    ParseError,
}

/// Prepared statement stored in connection state
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    pub name: EcoString,
    pub sql: String,
    /// Parameter type OIDs as resolved by origin's `ParameterDescription`,
    /// falling back to the client-supplied OIDs until origin replies. Used
    /// for query fingerprinting under cacheable execution.
    pub parameter_oids: Vec<u32>,
    /// Immutable snapshot of the client-supplied OIDs from `Parse`. Used in
    /// the describe-cache key so populate and lookup hash identically.
    pub client_parameter_oids: Vec<u32>,
    pub sql_type: StatementType,
    /// Raw ParameterDescription bytes from origin, used for Describe('S') in
    /// pipeline. `Bytes` (not `BytesMut`): written once from origin, then only
    /// read and cheaply (refcount) cloned into the describe cache and synth path.
    pub parameter_description: Option<Bytes>,
    /// Raw RowDescription bytes from origin's Describe('S') response. `None`
    /// when origin returned `NoData`; pair with `describe_no_data` to
    /// distinguish "not yet captured" from "captured, no result columns".
    pub row_description: Option<Bytes>,
    /// Origin's Describe('S') response was `NoData`. See `row_description`.
    pub describe_no_data: bool,
    /// True when origin has acknowledged this statement (ParseComplete received).
    /// Gates pipeline activation — the proxy only buffers Parse/Bind/Execute
    /// when origin_prepared is true.
    pub origin_prepared: bool,
    /// Raw Parse message bytes, stored for proactive origin forwarding.
    /// On cache hit for a named statement, origin never sees Parse — these
    /// bytes let the proxy send Parse+Sync to origin so subsequent Bind-only
    /// or transaction paths work correctly.
    pub parse_bytes: Option<BytesMut>,
}

/// Portal (bound prepared statement) stored in connection state
#[derive(Debug, Clone)]
pub struct Portal {
    pub name: EcoString,
    pub statement_name: EcoString,
    pub parameter_values: Vec<Option<Bytes>>,
    pub parameter_formats: Vec<i16>, // 0=text, 1=binary
    pub result_formats: Vec<i16>,
}

impl Portal {
    /// Check if any parameter uses binary format (format code 1).
    /// Returns true if binary format is detected, false otherwise.
    pub fn has_binary_parameters(&self) -> bool {
        self.parameter_formats.contains(&1)
    }
}

/// Parsed Parse message data
#[derive(Debug, Clone)]
pub struct ParsedParseMessage {
    pub statement_name: EcoString,
    pub sql: String,
    pub parameter_oids: Vec<u32>,
}

/// Parsed Bind message data
#[derive(Debug, Clone)]
pub struct ParsedBindMessage {
    pub portal_name: EcoString,
    pub statement_name: EcoString,
    pub parameter_formats: Vec<i16>,
    pub parameter_values: Vec<Option<Bytes>>,
    pub result_formats: Vec<i16>,
}

/// Parsed Execute message data
#[derive(Debug, Clone)]
pub struct ParsedExecuteMessage {
    pub portal_name: EcoString,
    pub max_rows: i32,
}

/// Parsed Describe message data
#[derive(Debug, Clone)]
pub struct ParsedDescribeMessage {
    pub describe_type: u8, // b'S' for statement, b'P' for portal
    pub name: EcoString,
}

/// Parsed ParameterDescription message data (backend response)
#[derive(Debug, Clone)]
pub struct ParsedParameterDescription {
    pub parameter_oids: Vec<u32>,
}

/// Parsed Close message data
#[derive(Debug, Clone)]
pub struct ParsedCloseMessage {
    pub close_type: u8, // b'S' for statement, b'P' for portal
    pub name: EcoString,
}

/// Read a null-terminated string from the buffer
fn read_cstring<'a>(buf: &mut &'a [u8]) -> ProtocolResult<&'a str> {
    let null_pos = buf.iter().position(|&b| b == 0).ok_or_else(|| {
        ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "missing null terminator",
        ))
    })?;

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
pub fn parse_parse_message(data: &BytesMut) -> ProtocolResult<ParsedParseMessage> {
    let Some(buf) = data.get(5..) else {
        return Err(ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Parse message too short",
        ))
        .into());
    };

    let mut buf = buf; // Skip message tag (1 byte) and length (4 bytes)

    let statement_name = read_cstring(&mut buf)?.into();
    let sql = read_cstring(&mut buf)?.to_owned();

    if buf.len() < 2 {
        return Err(ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "missing parameter count",
        ))
        .into());
    }

    let param_count = count_to_usize(buf.get_i16(), "Parse parameter count")?;
    let mut parameter_oids = Vec::with_capacity(param_count);

    for _ in 0..param_count {
        if buf.len() < 4 {
            return Err(ProtocolError::IoError(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "missing parameter OID",
            ))
            .into());
        }
        parameter_oids.push(buf.get_u32());
    }

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
pub fn parse_bind_message(data: &BytesMut) -> ProtocolResult<ParsedBindMessage> {
    let Some(buf) = data.get(5..) else {
        return Err(ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Bind message too short",
        ))
        .into());
    };

    let mut buf = buf; // Skip message tag and length

    let portal_name = read_cstring(&mut buf)?.into();
    let statement_name = read_cstring(&mut buf)?.into();

    // Read parameter format codes
    if buf.len() < 2 {
        return Err(ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "missing format code count",
        ))
        .into());
    }
    let format_code_count = count_to_usize(buf.get_i16(), "Bind format code count")?;
    let mut parameter_formats = Vec::with_capacity(format_code_count);

    for _ in 0..format_code_count {
        if buf.len() < 2 {
            return Err(ProtocolError::IoError(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "missing format code",
            ))
            .into());
        }
        parameter_formats.push(buf.get_i16());
    }

    // Read parameter values
    if buf.len() < 2 {
        return Err(ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "missing parameter value count",
        ))
        .into());
    }
    let param_value_count = count_to_usize(buf.get_i16(), "Bind parameter value count")?;
    let mut parameter_values = Vec::with_capacity(param_value_count);

    for _ in 0..param_value_count {
        if buf.len() < 4 {
            return Err(ProtocolError::IoError(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "missing parameter length",
            ))
            .into());
        }
        let param_len = buf.get_i32();

        if param_len == -1 {
            // NULL value
            parameter_values.push(None);
        } else {
            let param_len = count_to_usize(param_len, "Bind parameter length")?;
            let Some(value_bytes) = buf.get(..param_len) else {
                return Err(ProtocolError::IoError(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "parameter value truncated",
                ))
                .into());
            };
            let value = Bytes::copy_from_slice(value_bytes);
            buf.advance(param_len);
            parameter_values.push(Some(value));
        }
    }

    // Read result format codes
    if buf.len() < 2 {
        return Err(ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "missing result format code count",
        ))
        .into());
    }
    let result_format_count = count_to_usize(buf.get_i16(), "Bind result format count")?;
    let mut result_formats = Vec::with_capacity(result_format_count);

    for _ in 0..result_format_count {
        if buf.len() < 2 {
            return Err(ProtocolError::IoError(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "missing result format code",
            ))
            .into());
        }
        result_formats.push(buf.get_i16());
    }

    Ok(ParsedBindMessage {
        portal_name,
        statement_name,
        parameter_formats,
        parameter_values,
        result_formats,
    })
}

/// Parse an Execute message ('E')
///
/// Format:
/// Byte1('E')
/// Int32 - message length
/// String - portal name (empty string for unnamed)
/// Int32 - maximum number of rows to return (0 = unlimited)
pub fn parse_execute_message(data: &BytesMut) -> ProtocolResult<ParsedExecuteMessage> {
    let Some(buf) = data.get(5..) else {
        return Err(ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Execute message too short",
        ))
        .into());
    };

    let mut buf = buf; // Skip message tag and length

    let portal_name = read_cstring(&mut buf)?.into();

    if buf.len() < 4 {
        return Err(ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "missing max_rows",
        ))
        .into());
    }
    let max_rows = buf.get_i32();

    Ok(ParsedExecuteMessage {
        portal_name,
        max_rows,
    })
}

/// Parse a Describe message ('D')
///
/// Format:
/// Byte1('D')
/// Int32 - message length
/// Byte1 - 'S' for statement, 'P' for portal
/// String - name of statement or portal
pub fn parse_describe_message(data: &BytesMut) -> ProtocolResult<ParsedDescribeMessage> {
    let Some(buf) = data.get(5..) else {
        return Err(ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Describe message too short",
        ))
        .into());
    };

    // Need at least 1 byte for describe_type
    if buf.is_empty() {
        return Err(ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Describe message too short",
        ))
        .into());
    }

    let mut buf = buf; // Skip message tag and length

    let describe_type = buf.get_u8();
    let name = read_cstring(&mut buf)?.into();

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
pub fn parse_close_message(data: &BytesMut) -> ProtocolResult<ParsedCloseMessage> {
    let Some(buf) = data.get(5..) else {
        return Err(ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Close message too short",
        ))
        .into());
    };

    // Need at least 1 byte for close_type
    if buf.is_empty() {
        return Err(ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Close message too short",
        ))
        .into());
    }

    let mut buf = buf; // Skip message tag and length

    let close_type = buf.get_u8();
    let name = read_cstring(&mut buf)?.into();

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
pub fn parse_parameter_description(data: &[u8]) -> ProtocolResult<ParsedParameterDescription> {
    // Need at least 7 bytes: tag(1) + length(4) + param_count(2)
    let Some(buf) = data.get(5..) else {
        return Err(ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "ParameterDescription message too short",
        ))
        .into());
    };

    if buf.len() < 2 {
        return Err(ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "ParameterDescription message too short",
        ))
        .into());
    }

    let mut buf = buf; // Skip message tag and length

    let param_count = count_to_usize(buf.get_i16(), "ParameterDescription parameter count")?;
    let mut parameter_oids = Vec::with_capacity(param_count);

    for _ in 0..param_count {
        if buf.len() < 4 {
            return Err(ProtocolError::IoError(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "missing parameter OID in ParameterDescription",
            ))
            .into());
        }
        parameter_oids.push(buf.get_u32());
    }

    Ok(ParsedParameterDescription { parameter_oids })
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use super::*;
    use crate::cache::query::CacheableQuery;
    use crate::query::ast::{QueryBody, query_expr_parse};

    #[test]
    fn test_parse_parse_message() {
        // Parse message: statement name "stmt1", SQL "SELECT 1", no parameters
        let mut data = BytesMut::new();
        data.extend_from_slice(b"P"); // tag
        data.extend_from_slice(&[0, 0, 0, 20]); // length (21 bytes total - 1 for tag = 20)
        data.extend_from_slice(b"stmt1\0"); // statement name
        data.extend_from_slice(b"SELECT 1\0"); // SQL
        data.extend_from_slice(&[0, 0]); // 0 parameters

        let result = parse_parse_message(&data).unwrap();
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

        let result = parse_parse_message(&data).unwrap();
        assert_eq!(result.statement_name, "");
        assert_eq!(result.sql, "SELECT $1, $2");
        assert_eq!(result.parameter_oids, vec![23, 25]);
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
        assert_eq!(result.result_formats, vec![0]);
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
        let mut data = BytesMut::new();
        data.extend_from_slice(b"E");
        data.extend_from_slice(&[0, 0, 0, 11]); // length
        data.extend_from_slice(b"p1\0"); // portal name
        data.extend_from_slice(&[0, 0, 0, 100]); // max_rows = 100

        let result = parse_execute_message(&data).unwrap();
        assert_eq!(result.portal_name, "p1");
        assert_eq!(result.max_rows, 100);
    }

    #[test]
    fn test_parse_execute_message_unlimited() {
        // Execute message: unnamed portal, unlimited rows
        let mut data = BytesMut::new();
        data.extend_from_slice(b"E");
        data.extend_from_slice(&[0, 0, 0, 9]); // length
        data.extend_from_slice(b"\0"); // unnamed portal
        data.extend_from_slice(&[0, 0, 0, 0]); // max_rows = 0 (unlimited)

        let result = parse_execute_message(&data).unwrap();
        assert_eq!(result.portal_name, "");
        assert_eq!(result.max_rows, 0);
    }

    #[test]
    fn test_parse_describe_message_statement() {
        // Describe statement "stmt1"
        let mut data = BytesMut::new();
        data.extend_from_slice(b"D");
        data.extend_from_slice(&[0, 0, 0, 11]); // length
        data.extend_from_slice(b"S"); // describe statement
        data.extend_from_slice(b"stmt1\0"); // name

        let result = parse_describe_message(&data).unwrap();
        assert_eq!(result.describe_type, b'S');
        assert_eq!(result.name, "stmt1");
    }

    #[test]
    fn test_parse_describe_message_portal() {
        // Describe portal "p1"
        let mut data = BytesMut::new();
        data.extend_from_slice(b"D");
        data.extend_from_slice(&[0, 0, 0, 8]); // length
        data.extend_from_slice(b"P"); // describe portal
        data.extend_from_slice(b"p1\0"); // name

        let result = parse_describe_message(&data).unwrap();
        assert_eq!(result.describe_type, b'P');
        assert_eq!(result.name, "p1");
    }

    #[test]
    fn test_parse_close_message_statement() {
        // Close statement "stmt1"
        let mut data = BytesMut::new();
        data.extend_from_slice(b"C");
        data.extend_from_slice(&[0, 0, 0, 11]); // length
        data.extend_from_slice(b"S"); // close statement
        data.extend_from_slice(b"stmt1\0"); // name

        let result = parse_close_message(&data).unwrap();
        assert_eq!(result.close_type, b'S');
        assert_eq!(result.name, "stmt1");
    }

    #[test]
    fn test_parse_close_message_portal() {
        // Close portal "p1"
        let mut data = BytesMut::new();
        data.extend_from_slice(b"C");
        data.extend_from_slice(&[0, 0, 0, 8]); // length
        data.extend_from_slice(b"P"); // close portal
        data.extend_from_slice(b"p1\0"); // name

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

        let result = parse_parse_message(&data);
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

        let result = parse_parse_message(&data).unwrap();
        assert_eq!(result.statement_name, "stmt1");
        assert_eq!(result.sql, "SELECT id, data FROM test WHERE id = $1");
        assert_eq!(result.parameter_oids, vec![23]);

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

        let result = parse_parse_message(&data).unwrap();
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

        let result = parse_parse_message(&data).unwrap();

        // Test that INSERT is not cacheable (not a SELECT)
        let cacheable_result = query_expr_parse(&result.sql);
        assert!(
            cacheable_result.is_err(),
            "INSERT should not convert to cacheable query"
        );
    }

    #[test]
    fn test_portal_has_binary_parameters_all_text() {
        let portal = Portal {
            name: "p1".into(),
            statement_name: "s1".into(),
            parameter_values: vec![Some(Bytes::from_static(b"42"))],
            parameter_formats: vec![0], // text format
            result_formats: vec![0],
        };

        assert!(
            !portal.has_binary_parameters(),
            "All text parameters should return false"
        );
    }

    #[test]
    fn test_portal_has_binary_parameters_with_binary() {
        let portal = Portal {
            name: "p1".into(),
            statement_name: "s1".into(),
            parameter_values: vec![Some(Bytes::from_static(&[0, 0, 0, 42]))],
            parameter_formats: vec![1], // binary format
            result_formats: vec![0],
        };

        assert!(
            portal.has_binary_parameters(),
            "Binary parameter should return true"
        );
    }

    #[test]
    fn test_portal_has_binary_parameters_mixed() {
        let portal = Portal {
            name: "p1".into(),
            statement_name: "s1".into(),
            parameter_values: vec![
                Some(Bytes::from_static(b"text")),
                Some(Bytes::from_static(&[0, 0, 0, 42])),
            ],
            parameter_formats: vec![0, 1], // text, then binary
            result_formats: vec![0],
        };

        assert!(
            portal.has_binary_parameters(),
            "Mixed formats with any binary should return true"
        );
    }

    #[test]
    fn test_portal_has_binary_parameters_empty() {
        let portal = Portal {
            name: "p1".into(),
            statement_name: "s1".into(),
            parameter_values: vec![],
            parameter_formats: vec![],
            result_formats: vec![],
        };

        assert!(
            !portal.has_binary_parameters(),
            "No parameters should return false"
        );
    }
}
