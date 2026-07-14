//! Wire-level SQLSTATE extraction from a backend `ErrorResponse` frame.

/// SQLSTATE `42P01` — `undefined_table`. The expected outcome when the cache
/// table is dropped between dispatch and SELECT (eviction-window race).
pub(crate) const SQLSTATE_UNDEFINED_TABLE: [u8; 5] = *b"42P01";

/// Extract the 5-char SQLSTATE from a backend `ErrorResponse` frame.
///
/// Frame layout: `'E' (1 byte) | len (4 bytes BE) | field* | 0`, where each
/// field is `code (1 byte) | value (null-terminated string)`. Field code `'C'`
/// carries SQLSTATE — always exactly 5 ASCII bytes per the protocol.
/// Returns `None` when the frame is malformed or the field is missing.
pub(super) fn sqlstate_extract(frame_data: &[u8]) -> Option<[u8; 5]> {
    let payload = frame_data.get(5..)?;
    let mut i = 0;
    while i < payload.len() {
        let code = *payload.get(i)?;
        if code == 0 {
            return None;
        }
        let value_start = i + 1;
        let rest = payload.get(value_start..)?;
        let value_len = rest.iter().position(|&b| b == 0)?;
        if code == b'C' && value_len == 5 {
            let value = rest.get(..5)?;
            let mut out = [0u8; 5];
            out.copy_from_slice(value);
            return Some(out);
        }
        i = value_start + value_len + 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn error_response_frame(fields: &[(u8, &[u8])]) -> Vec<u8> {
        let mut payload = Vec::new();
        for (code, value) in fields {
            payload.push(*code);
            payload.extend_from_slice(value);
            payload.push(0);
        }
        payload.push(0);
        let mut frame = vec![b'E'];
        let len = u32::try_from(payload.len()).expect("test frame fits u32") + 4;
        frame.extend_from_slice(&len.to_be_bytes());
        frame.extend_from_slice(&payload);
        frame
    }

    #[test]
    fn sqlstate_extract_undefined_table() {
        let frame = error_response_frame(&[
            (b'S', b"ERROR"),
            (b'C', b"42P01"),
            (b'M', b"relation does not exist"),
        ]);
        assert_eq!(sqlstate_extract(&frame), Some(SQLSTATE_UNDEFINED_TABLE));
    }

    #[test]
    fn sqlstate_extract_first_field() {
        let frame = error_response_frame(&[(b'C', b"23505"), (b'M', b"dup")]);
        assert_eq!(sqlstate_extract(&frame), Some(*b"23505"));
    }

    #[test]
    fn sqlstate_extract_missing_returns_none() {
        let frame = error_response_frame(&[(b'S', b"ERROR"), (b'M', b"no code")]);
        assert_eq!(sqlstate_extract(&frame), None);
    }

    #[test]
    fn sqlstate_extract_wrong_length_returns_none() {
        let frame = error_response_frame(&[(b'C', b"42P0")]);
        assert_eq!(sqlstate_extract(&frame), None);
    }

    #[test]
    fn sqlstate_extract_short_frame_returns_none() {
        assert_eq!(sqlstate_extract(&[b'E', 0, 0]), None);
    }
}
