//! Binary-format parameter decoding (Bind format=1). Each PG type has its
//! own wire format; we decode to a typed `LiteralValue` (or a
//! `StringWithCast` carrying canonical PG text) so the deparsed SQL
//! re-binds safely without round-tripping through PG's text input parser.

use std::fmt::Write as _;

use ecow::EcoString;
use fallible_iterator::FallibleIterator;
use ordered_float::NotNan;
use postgres_protocol::types as pg_types;
use postgres_types::Type as PgType;
use rootcause::Report;

use crate::query::ast::LiteralValue;

use super::super::{AstTransformError, AstTransformResult};

/// JDN of `2000-01-01`, PG's epoch for both `date` and `timestamp[tz]`.
const POSTGRES_EPOCH_JDATE: i32 = 2_451_545;
const USECS_PER_DAY: i64 = 86_400_000_000;

// `numeric` sign codes — see PG `src/backend/utils/adt/numeric.c`.
const NUMERIC_POS: u16 = 0x0000;
const NUMERIC_NEG: u16 = 0x4000;
const NUMERIC_NAN: u16 = 0xC000;
const NUMERIC_PINF: u16 = 0xD000;
const NUMERIC_NINF: u16 = 0xF000;

pub(super) fn binary_parameter_to_literal(
    bytes: &[u8],
    oid: u32,
) -> AstTransformResult<LiteralValue> {
    if let Some(ty) = PgType::from_oid(oid)
        && matches!(ty.kind(), postgres_types::Kind::Array(_))
    {
        return binary_array_to_literal(bytes, oid);
    }
    binary_parameter_to_literal_scalar(bytes, oid)
}

fn binary_parameter_to_literal_scalar(bytes: &[u8], oid: u32) -> AstTransformResult<LiteralValue> {
    let pg_type = PgType::from_oid(oid);

    // Fail-closed Kind dispatch: types without an explicit per-OID arm
    // below route to `UnsupportedBinaryFormat` so the query falls through
    // to origin uncached. The previous UTF-8 catch-all silently corrupted
    // SQL whenever a binary wire format happened to be valid UTF-8.
    if let Some(ref ty) = pg_type {
        match ty.kind() {
            postgres_types::Kind::Array(_) => {
                return Err(AstTransformError::UnsupportedBinaryFormat { oid }.into());
            }
            postgres_types::Kind::Domain(base) => {
                return binary_parameter_to_literal_scalar(bytes, base.oid());
            }
            postgres_types::Kind::Enum(_) => {
                let s = std::str::from_utf8(bytes).map_err(|_| {
                    Report::from(AstTransformError::InvalidParameterValue {
                        message: "binary enum value is not valid UTF-8".to_owned(),
                    })
                })?;
                return Ok(LiteralValue::String(s.to_owned()));
            }
            postgres_types::Kind::Composite(_)
            | postgres_types::Kind::Range(_)
            | postgres_types::Kind::Multirange(_)
            | postgres_types::Kind::Pseudo => {
                return Err(AstTransformError::UnsupportedBinaryFormat { oid }.into());
            }
            postgres_types::Kind::Simple => {}
            // `Kind` is `#[non_exhaustive]`; new variants must be opted in.
            _ => {
                return Err(AstTransformError::UnsupportedBinaryFormat { oid }.into());
            }
        }
    }

    match pg_type {
        Some(PgType::BOOL) => {
            let value = pg_types::bool_from_sql(bytes).map_err(|e| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!("invalid binary bool: {e}"),
                })
            })?;
            Ok(LiteralValue::Boolean(value))
        }
        Some(PgType::INT2) => {
            let value = pg_types::int2_from_sql(bytes).map_err(|e| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!("invalid binary int2: {e}"),
                })
            })?;
            Ok(LiteralValue::Integer(value as i64))
        }
        Some(PgType::INT4) => {
            let value = pg_types::int4_from_sql(bytes).map_err(|e| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!("invalid binary int4: {e}"),
                })
            })?;
            Ok(LiteralValue::Integer(value as i64))
        }
        Some(PgType::INT8) => {
            let value = pg_types::int8_from_sql(bytes).map_err(|e| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!("invalid binary int8: {e}"),
                })
            })?;
            Ok(LiteralValue::Integer(value))
        }
        Some(PgType::FLOAT4) => {
            let value = pg_types::float4_from_sql(bytes).map_err(|e| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!("invalid binary float4: {e}"),
                })
            })?;
            let value = NotNan::new(value as f64).map_err(|_| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: "NaN is not a valid float value".to_owned(),
                })
            })?;
            Ok(LiteralValue::Float(value))
        }
        Some(PgType::FLOAT8) => {
            let value = pg_types::float8_from_sql(bytes).map_err(|e| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!("invalid binary float8: {e}"),
                })
            })?;
            let value = NotNan::new(value).map_err(|_| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: "NaN is not a valid float value".to_owned(),
                })
            })?;
            Ok(LiteralValue::Float(value))
        }
        Some(
            PgType::TEXT
            | PgType::VARCHAR
            | PgType::BPCHAR
            | PgType::NAME
            | PgType::CHAR
            | PgType::UNKNOWN,
        ) => {
            let value = pg_types::text_from_sql(bytes).map_err(|e| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!("invalid binary text: {e}"),
                })
            })?;
            Ok(LiteralValue::String(value.to_owned()))
        }
        Some(PgType::UUID) => {
            let bytes: &[u8; 16] = bytes.try_into().map_err(|_| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!(
                        "invalid UUID length: expected 16 bytes, got {}",
                        bytes.len()
                    ),
                })
            })?;
            let uuid_str = format!(
                "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                bytes[0],
                bytes[1],
                bytes[2],
                bytes[3],
                bytes[4],
                bytes[5],
                bytes[6],
                bytes[7],
                bytes[8],
                bytes[9],
                bytes[10],
                bytes[11],
                bytes[12],
                bytes[13],
                bytes[14],
                bytes[15]
            );
            Ok(LiteralValue::String(uuid_str))
        }
        Some(PgType::BYTEA) => {
            // The leading `\` in `\x<hex>` forces `escape_literal` into
            // E-string form so the SQL stays well-formed.
            let raw = pg_types::bytea_from_sql(bytes);
            Ok(LiteralValue::StringWithCast(
                bytea_to_hex_literal(raw),
                PgType::BYTEA.name().into(),
            ))
        }
        Some(PgType::TIME) => {
            let micros = pg_types::time_from_sql(bytes).map_err(|e| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!("invalid binary time: {e}"),
                })
            })?;
            Ok(LiteralValue::StringWithCast(
                time_micros_to_text(micros),
                PgType::TIME.name().into(),
            ))
        }
        Some(PgType::DATE) => {
            // ±i32 sentinels are PG14+ `infinity` / `-infinity`.
            let days = pg_types::date_from_sql(bytes).map_err(|e| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!("invalid binary date: {e}"),
                })
            })?;
            let text = match days {
                i32::MAX => "infinity".to_owned(),
                i32::MIN => "-infinity".to_owned(),
                _ => {
                    let (y, m, d) = pg_days_to_ymd(days);
                    ymd_to_text(y, m, d)
                }
            };
            Ok(LiteralValue::StringWithCast(
                text,
                PgType::DATE.name().into(),
            ))
        }
        Some(PgType::TIMESTAMP) => {
            let micros = pg_types::timestamp_from_sql(bytes).map_err(|e| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!("invalid binary timestamp: {e}"),
                })
            })?;
            let text = match micros {
                i64::MAX => "infinity".to_owned(),
                i64::MIN => "-infinity".to_owned(),
                _ => timestamp_micros_to_text(micros),
            };
            Ok(LiteralValue::StringWithCast(
                text,
                PgType::TIMESTAMP.name().into(),
            ))
        }
        Some(PgType::TIMESTAMPTZ) => {
            // Emit explicit `+00` so PG re-parses with zone regardless of
            // session `TimeZone` setting. Wire format matches TIMESTAMP.
            let micros = pg_types::timestamp_from_sql(bytes).map_err(|e| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!("invalid binary timestamptz: {e}"),
                })
            })?;
            let text = match micros {
                i64::MAX => "infinity".to_owned(),
                i64::MIN => "-infinity".to_owned(),
                _ => format!("{}+00", timestamp_micros_to_text(micros)),
            };
            Ok(LiteralValue::StringWithCast(
                text,
                PgType::TIMESTAMPTZ.name().into(),
            ))
        }
        Some(PgType::MACADDR) => {
            let octets = pg_types::macaddr_from_sql(bytes).map_err(|e| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!("invalid binary macaddr: {e}"),
                })
            })?;
            Ok(LiteralValue::StringWithCast(
                macaddr_to_text(&octets),
                PgType::MACADDR.name().into(),
            ))
        }
        Some(PgType::MACADDR8) => {
            let octets: &[u8; 8] = bytes.try_into().map_err(|_| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!(
                        "invalid macaddr8 length: expected 8 bytes, got {}",
                        bytes.len()
                    ),
                })
            })?;
            Ok(LiteralValue::StringWithCast(
                macaddr_to_text(octets),
                PgType::MACADDR8.name().into(),
            ))
        }
        Some(PgType::INET) => {
            let inet = pg_types::inet_from_sql(bytes).map_err(|e| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!("invalid binary inet: {e}"),
                })
            })?;
            Ok(LiteralValue::StringWithCast(
                inet_to_text(&inet, false),
                PgType::INET.name().into(),
            ))
        }
        Some(PgType::CIDR) => {
            let inet = pg_types::inet_from_sql(bytes).map_err(|e| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!("invalid binary cidr: {e}"),
                })
            })?;
            Ok(LiteralValue::StringWithCast(
                inet_to_text(&inet, true),
                PgType::CIDR.name().into(),
            ))
        }
        Some(PgType::NUMERIC) => {
            let (weight, sign, dscale, digits) = numeric_parse_wire(bytes)?;
            let text = numeric_to_text(weight, sign, dscale, &digits).ok_or_else(|| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!("invalid numeric sign code: 0x{sign:04x}"),
                })
            })?;
            Ok(LiteralValue::StringWithCast(
                text,
                PgType::NUMERIC.name().into(),
            ))
        }
        Some(PgType::TIMETZ) => {
            // 12 bytes: i64 micros-since-midnight + i32 zone-secs-west-of-UTC.
            let arr: &[u8; 12] = bytes.try_into().map_err(|_| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!(
                        "invalid timetz length: expected 12 bytes, got {}",
                        bytes.len()
                    ),
                })
            })?;
            let micros = i64::from_be_bytes(arr[0..8].try_into().expect("8-byte time component"));
            let zone = i32::from_be_bytes(arr[8..12].try_into().expect("4-byte zone component"));
            Ok(LiteralValue::StringWithCast(
                timetz_to_text(micros, zone),
                PgType::TIMETZ.name().into(),
            ))
        }
        Some(PgType::INTERVAL) => {
            // 16 bytes: i64 micros + i32 days + i32 months.
            let arr: &[u8; 16] = bytes.try_into().map_err(|_| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!(
                        "invalid interval length: expected 16 bytes, got {}",
                        bytes.len()
                    ),
                })
            })?;
            let micros = i64::from_be_bytes(arr[0..8].try_into().expect("8-byte time component"));
            let days = i32::from_be_bytes(arr[8..12].try_into().expect("4-byte days component"));
            let months =
                i32::from_be_bytes(arr[12..16].try_into().expect("4-byte months component"));
            Ok(LiteralValue::StringWithCast(
                interval_to_text(micros, days, months),
                PgType::INTERVAL.name().into(),
            ))
        }
        Some(PgType::JSON) => {
            // JSON binary format is plain UTF-8 JSON text — no version
            // prefix or other framing — so it round-trips as a string
            // literal, just with an explicit `::json` cast to preserve
            // the column's expected type.
            let value = pg_types::text_from_sql(bytes).map_err(|e| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!("invalid binary json: {e}"),
                })
            })?;
            Ok(LiteralValue::StringWithCast(
                value.to_owned(),
                PgType::JSON.name().into(),
            ))
        }
        Some(PgType::JSONB) => {
            // JSONB binary format prepends a 1-byte version (currently
            // 0x01); the rest is UTF-8 JSON text. Strip the prefix so the
            // deparsed SQL is `'<json>'::jsonb` rather than carrying the
            // SOH control byte into the literal.
            match bytes.split_first() {
                Some((&0x01, json)) => {
                    let value = std::str::from_utf8(json).map_err(|_| {
                        Report::from(AstTransformError::InvalidParameterValue {
                            message: "jsonb body is not valid UTF-8".to_owned(),
                        })
                    })?;
                    Ok(LiteralValue::StringWithCast(
                        value.to_owned(),
                        PgType::JSONB.name().into(),
                    ))
                }
                _ => Err(AstTransformError::InvalidParameterValue {
                    message: "missing or unknown jsonb version byte".to_owned(),
                }
                .into()),
            }
        }
        // Fail-closed catch-all — see the Kind dispatch above.
        _ => Err(AstTransformError::UnsupportedBinaryFormat { oid }.into()),
    }
}

/// Format raw bytea bytes as PG's hex text representation `\x<lowercase-hex>`.
fn bytea_to_hex_literal(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(2 + bytes.len() * 2);
    out.push_str("\\x");
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Format `i64` microseconds since midnight as `HH:MM:SS.uuuuuu`. Always
/// emits 6 fractional digits so two distinct binary parameter values can
/// never produce the same deparsed text (cache fingerprint stability).
fn time_micros_to_text(micros: i64) -> String {
    let total_secs = micros / 1_000_000;
    let frac = micros % 1_000_000;
    let h = total_secs / 3600;
    let m = (total_secs % 3600) / 60;
    let s = total_secs % 60;
    format!("{h:02}:{m:02}:{s:02}.{frac:06}")
}

/// PostgreSQL `j2date` algorithm: convert `i32` days-since-2000-01-01
/// into proleptic-Gregorian `(year, month, day)`. Year 0 means 1 BC,
/// year -1 means 2 BC, etc. The year is saturated to `i32` range so an
/// out-of-range wire value never panics.
#[allow(clippy::cast_sign_loss)]
fn pg_days_to_ymd(days: i32) -> (i32, u32, u32) {
    // Cast i32 → u32 by 2's-complement bit pattern, mirroring the
    // upstream C `(unsigned int)`. `wrapping_add` keeps the algebra
    // identical to the source.
    let jd = days.wrapping_add(POSTGRES_EPOCH_JDATE) as u32;
    let mut julian = jd.wrapping_add(32_044);
    let quad1 = julian / 146_097;
    let extra = (julian - quad1 * 146_097) * 4 + 3;
    julian += 60 + quad1 * 3 + extra / 146_097;
    let quad2 = julian / 1_461;
    julian -= quad2 * 1_461;
    let y = julian * 4 / 1_461;
    julian = if y != 0 {
        (julian + 305) % 365
    } else {
        (julian + 306) % 366
    } + 123;
    let q = julian * 2_141 / 65_536;
    let day = julian - 7_834 * q / 256;
    let month = (q + 10) % 12 + 1;

    let year_combined = i64::from(y) + i64::from(quad2) * 4 - 4_800;
    let year = i32::try_from(year_combined.clamp(i64::from(i32::MIN), i64::from(i32::MAX)))
        .expect("clamped to i32 range");
    (year, month, day)
}

/// Format `(year, month, day)` as PG's canonical text (`YYYY-MM-DD`,
/// or `YYYY-MM-DD BC` for proleptic-Gregorian year ≤ 0).
fn ymd_to_text(year: i32, month: u32, day: u32) -> String {
    if year > 0 {
        format!("{year:04}-{month:02}-{day:02}")
    } else {
        let bc_year = 1_i32.saturating_sub(year);
        format!("{bc_year:04}-{month:02}-{day:02} BC")
    }
}

/// Format `i64` micros-since-2000-01-01 as `YYYY-MM-DD HH:MM:SS.uuuuuu`.
/// Used by both TIMESTAMP and TIMESTAMPTZ; the latter appends `+00`
/// at the call site.
fn timestamp_micros_to_text(micros: i64) -> String {
    let days_i64 = micros.div_euclid(USECS_PER_DAY);
    let days = i32::try_from(days_i64).unwrap_or(if days_i64 < 0 { i32::MIN } else { i32::MAX });
    let sub_day_micros = micros.rem_euclid(USECS_PER_DAY);
    let (year, month, day) = pg_days_to_ymd(days);
    format!(
        "{} {}",
        ymd_to_text(year, month, day),
        time_micros_to_text(sub_day_micros)
    )
}

/// Format a MAC address (6 or 8 octets) as colon-separated lowercase hex
/// pairs. PG accepts this for both `macaddr` and `macaddr8` types.
fn macaddr_to_text(octets: &[u8]) -> String {
    let mut out = String::with_capacity(octets.len().saturating_mul(3));
    let mut first = true;
    for b in octets {
        if !first {
            out.push(':');
        }
        first = false;
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Format a PG inet value as `addr` or `addr/prefix`. CIDR always emits
/// the prefix; INET omits it when the netmask equals the address-family
/// default (32 for v4, 128 for v6) — this matches PG's canonical output
/// so binary and text binds of the same value cache to the same key.
fn inet_to_text(inet: &pg_types::Inet, force_prefix: bool) -> String {
    let default_mask: u8 = if inet.addr().is_ipv4() { 32 } else { 128 };
    if force_prefix || inet.netmask() != default_mask {
        format!("{}/{}", inet.addr(), inet.netmask())
    } else {
        inet.addr().to_string()
    }
}

/// Parse a binary `numeric` payload (8-byte header plus `ndigits` × i16
/// base-10000 digits) into `(weight, sign, dscale, digits)`. PG has no
/// `numeric_from_sql` helper, so this lives here.
fn numeric_parse_wire(bytes: &[u8]) -> AstTransformResult<(i16, u16, usize, Vec<i16>)> {
    let (header, rest) = bytes.split_first_chunk::<8>().ok_or_else(|| {
        Report::from(AstTransformError::InvalidParameterValue {
            message: format!("invalid numeric: header needs 8 bytes, got {}", bytes.len()),
        })
    })?;
    let &[n0, n1, w0, w1, s0, s1, d0, d1] = header;
    let raw_ndigits = i16::from_be_bytes([n0, n1]);
    let weight = i16::from_be_bytes([w0, w1]);
    let sign = u16::from_be_bytes([s0, s1]);
    let raw_dscale = i16::from_be_bytes([d0, d1]);
    let ndigits = usize::try_from(raw_ndigits).map_err(|_| {
        Report::from(AstTransformError::InvalidParameterValue {
            message: format!("invalid numeric: ndigits {raw_ndigits} is negative"),
        })
    })?;
    let dscale = usize::try_from(raw_dscale).map_err(|_| {
        Report::from(AstTransformError::InvalidParameterValue {
            message: format!("invalid numeric: dscale {raw_dscale} is negative"),
        })
    })?;
    if rest.len() != 2 * ndigits {
        return Err(AstTransformError::InvalidParameterValue {
            message: format!(
                "invalid numeric: expected {} digit bytes for {ndigits} digits, got {}",
                2 * ndigits,
                rest.len()
            ),
        }
        .into());
    }
    let mut digits: Vec<i16> = Vec::with_capacity(ndigits);
    for chunk in rest.chunks_exact(2) {
        let pair: [u8; 2] = chunk.try_into().expect("2-byte digit pair");
        let d = i16::from_be_bytes(pair);
        if !(0..=9999).contains(&d) {
            return Err(AstTransformError::InvalidParameterValue {
                message: format!("invalid numeric digit out of [0,9999]: {d}"),
            }
            .into());
        }
        digits.push(d);
    }
    Ok((weight, sign, dscale, digits))
}

/// Format a PG numeric value from its wire-format components into the
/// canonical text representation. Returns `None` for an unrecognized
/// sign code.
///
/// Numeric is stored as `digits[i]` at weight `weight - i`, each digit
/// in `[0, 9999]` representing 4 decimal places. Positions outside the
/// `digits` window are implicit zero. `dscale` is the count of decimal
/// digits to emit after the point — fractional digits are zero-padded
/// or truncated to exactly that many.
fn numeric_to_text(weight: i16, sign: u16, dscale: usize, digits: &[i16]) -> Option<String> {
    match sign {
        NUMERIC_NAN => return Some("NaN".to_owned()),
        NUMERIC_PINF => return Some("Infinity".to_owned()),
        NUMERIC_NINF => return Some("-Infinity".to_owned()),
        NUMERIC_POS | NUMERIC_NEG => {}
        _ => return None,
    }
    let neg = sign == NUMERIC_NEG;

    let digit_at = |w: i32| -> i16 {
        let idx = i32::from(weight) - w;
        usize::try_from(idx)
            .ok()
            .and_then(|i| digits.get(i).copied())
            .unwrap_or(0)
    };

    let int_top = i32::from(weight).max(0);
    let int_digit_count = usize::from(weight.max(0).unsigned_abs()) + 1;
    // Upper bound: sign byte + ≤4 chars per base-10000 digit + dot + dscale.
    let capacity = usize::from(neg) + 4 * int_digit_count + if dscale > 0 { 1 + dscale } else { 0 };
    let mut out = String::with_capacity(capacity);

    if neg {
        out.push('-');
    }

    let mut first_int = true;
    for w in (0..=int_top).rev() {
        let d = digit_at(w);
        if first_int {
            // First digit has no leading zeros (`5` not `0005`).
            let _ = write!(out, "{d}");
            first_int = false;
        } else {
            let _ = write!(out, "{d:04}");
        }
    }

    if dscale > 0 {
        out.push('.');
        let frac_start = out.len();
        let mut w: i32 = -1;
        while out.len() - frac_start < dscale {
            let d = digit_at(w);
            let _ = write!(out, "{d:04}");
            w -= 1;
        }
        // Last 4-char chunk may overshoot when dscale isn't a multiple of
        // 4; trim to exactly `dscale` fractional digits.
        out.truncate(frac_start + dscale);
    }

    Some(out)
}

/// Format a timetz value (time-of-day micros + zone seconds-west-of-UTC)
/// as PG's canonical text. Matches `EncodeTimezone` from the PG source:
/// emit `+HH:MM:SS` if seconds part is non-zero, else `+HH:MM` if minutes
/// are non-zero, else just `+HH`. Sign is inverted from the stored zone
/// (PG stores seconds *west* of UTC; output shows +east/-west).
fn timetz_to_text(micros: i64, zone_secs: i32) -> String {
    let time_part = time_micros_to_text(micros);
    let sign = if zone_secs <= 0 { '+' } else { '-' };
    let abs_zone = zone_secs.unsigned_abs();
    let zh = abs_zone / 3600;
    let zm = (abs_zone / 60) % 60;
    let zs = abs_zone % 60;
    let zone_part = if zs != 0 {
        format!("{sign}{zh:02}:{zm:02}:{zs:02}")
    } else if zm != 0 {
        format!("{sign}{zh:02}:{zm:02}")
    } else {
        format!("{sign}{zh:02}")
    };
    format!("{time_part}{zone_part}")
}

/// Format an interval (months, days, micros) as PG's postgres-style text.
/// Each component is signed independently — PG accepts e.g.
/// `'-1 mons -2 days -01:00:00.000000'::interval`.
fn interval_to_text(micros: i64, days: i32, months: i32) -> String {
    let sign = if micros < 0 { "-" } else { "" };
    let abs_micros = micros.unsigned_abs();
    let total_secs = abs_micros / 1_000_000;
    let frac = abs_micros % 1_000_000;
    let h = total_secs / 3600;
    let m = (total_secs % 3600) / 60;
    let s = total_secs % 60;
    format!("{months} mons {days} days {sign}{h:02}:{m:02}:{s:02}.{frac:06}")
}

/// Decode a binary-format array parameter into a `LiteralValue::StringWithCast`
/// holding a PG text array literal like `'{1,2,NULL,3}'::int4[]`.
///
/// Multi-dim arrays and arrays whose element type isn't in the supported
/// scalar set (the same set `binary_parameter_to_literal` handles directly)
/// return `UnsupportedBinaryFormat` so the caller can fall through to
/// origin uncached.
fn binary_array_to_literal(bytes: &[u8], oid: u32) -> AstTransformResult<LiteralValue> {
    let array = pg_types::array_from_sql(bytes).map_err(|e| {
        Report::from(AstTransformError::InvalidParameterValue {
            message: format!("invalid binary array: {e}"),
        })
    })?;

    // 1-D arrays only. Multi-dim falls through to origin uncached.
    let mut dims = array.dimensions();
    let bad_dim = |_| Report::from(AstTransformError::UnsupportedBinaryFormat { oid });
    let _ = dims.next().map_err(bad_dim)?;
    if dims.next().map_err(bad_dim)?.is_some() {
        return Err(AstTransformError::UnsupportedBinaryFormat { oid }.into());
    }

    let element_type = PgType::from_oid(array.element_type())
        .ok_or_else(|| Report::from(AstTransformError::UnsupportedBinaryFormat { oid }))?;
    let element_oid = element_type.oid();

    // Element errors are remapped to the array OID — the query falls
    // through to origin uncached either way, but error context names the
    // outer type the caller asked about.
    let mut values = array.values();
    let mut elements: Vec<LiteralValue> = Vec::with_capacity(values.size_hint().0);
    while let Some(value) = values.next().map_err(bad_dim)? {
        let lit = match value {
            None => LiteralValue::Null,
            Some(elem_bytes) => binary_parameter_to_literal_scalar(elem_bytes, element_oid)
                .map_err(|_| Report::from(AstTransformError::UnsupportedBinaryFormat { oid }))?,
        };
        elements.push(lit);
    }

    let mut cast = EcoString::from(element_type.name());
    cast.push_str("[]");
    Ok(LiteralValue::Array(elements, cast))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::wildcard_enum_match_arm)]

    use postgres_types::Type as PgType;
    use tokio_util::bytes::Bytes;

    use crate::cache::{QueryParameter, QueryParameters};
    use crate::query::ast::{
        Deparse, LiteralValue, QueryBody, SelectNode, query_expr_fingerprint, query_expr_parse,
    };

    use super::super::super::AstTransformError;
    use super::super::{
        parameter_to_literal, query_expr_parameters_replace, select_node_parameters_replace,
    };
    use super::{NUMERIC_NEG, NUMERIC_NINF, NUMERIC_PINF, NUMERIC_POS, USECS_PER_DAY};
    // NUMERIC_NAN is also used; pull in too.
    use super::NUMERIC_NAN;

    fn parse_select_node(sql: &str) -> SelectNode {
        let query_expr = query_expr_parse(sql).expect("convert to QueryExpr");
        match query_expr.body {
            QueryBody::Select(node) => *node,
            _ => panic!("expected SELECT"),
        }
    }

    fn binary_params(values: Vec<(Option<&[u8]>, PgType)>) -> QueryParameters {
        let len = values.len();
        let (values, oids): (Vec<_>, Vec<_>) = values
            .into_iter()
            .map(|(v, t)| (v.map(Bytes::copy_from_slice), t.oid()))
            .unzip();
        QueryParameters {
            values,
            formats: vec![1; len],
            oids,
        }
    }

    /// Build the 20-byte header (`ndim=1, hasnull=0, elemtype, dim_len,
    /// dim_lower`) shared by every 1-D binary array test payload.
    fn array_header_bytes(elem_type: PgType, n_elements: i32) -> Vec<u8> {
        let mut buf = Vec::with_capacity(20);
        buf.extend_from_slice(&1_i32.to_be_bytes());
        buf.extend_from_slice(&0_i32.to_be_bytes());
        buf.extend_from_slice(&elem_type.oid().to_be_bytes());
        buf.extend_from_slice(&n_elements.to_be_bytes());
        buf.extend_from_slice(&1_i32.to_be_bytes());
        buf
    }

    fn timetz_bytes(micros: i64, zone_secs: i32) -> Vec<u8> {
        let mut buf = Vec::with_capacity(12);
        buf.extend_from_slice(&micros.to_be_bytes());
        buf.extend_from_slice(&zone_secs.to_be_bytes());
        buf
    }

    fn interval_bytes(micros: i64, days: i32, months: i32) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16);
        buf.extend_from_slice(&micros.to_be_bytes());
        buf.extend_from_slice(&days.to_be_bytes());
        buf.extend_from_slice(&months.to_be_bytes());
        buf
    }

    fn inet_bytes(addr: &[u8], netmask: u8, is_cidr: bool) -> Vec<u8> {
        let family: u8 = if addr.len() == 4 { 2 } else { 3 };
        let mut buf = Vec::with_capacity(4 + addr.len());
        buf.push(family);
        buf.push(netmask);
        buf.push(u8::from(is_cidr));
        buf.push(u8::try_from(addr.len()).expect("test addr fits in u8"));
        buf.extend_from_slice(addr);
        buf
    }

    /// Build a binary `numeric` payload from its component fields.
    fn numeric_bytes(weight: i16, sign: u16, dscale: i16, digits: &[i16]) -> Vec<u8> {
        let ndigits = i16::try_from(digits.len()).expect("test numeric digit count fits in i16");
        let mut buf = Vec::with_capacity(8 + 2 * digits.len());
        buf.extend_from_slice(&ndigits.to_be_bytes());
        buf.extend_from_slice(&weight.to_be_bytes());
        buf.extend_from_slice(&sign.to_be_bytes());
        buf.extend_from_slice(&dscale.to_be_bytes());
        for d in digits {
            buf.extend_from_slice(&d.to_be_bytes());
        }
        buf
    }

    fn assert_numeric(bytes: Vec<u8>, expected: &str) {
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::NUMERIC.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode binary numeric");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast(expected.to_owned(), "numeric".into()),
            "wire bytes {bytes:?}"
        );
    }

    /// Encode a single binary text-array element: i32 length prefix
    /// followed by the UTF-8 bytes.
    fn array_text_element_bytes(s: &str) -> Vec<u8> {
        let len = i32::try_from(s.len()).expect("test element fits in i32");
        let mut buf = Vec::with_capacity(4 + s.len());
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(s.as_bytes());
        buf
    }

    fn binary_int4_array_42_100() -> Vec<u8> {
        vec![
            0x00, 0x00, 0x00, 0x01, // ndim = 1
            0x00, 0x00, 0x00, 0x00, // hasnull = 0
            0x00, 0x00, 0x00, 0x17, // elemtype = 23 (int4)
            0x00, 0x00, 0x00, 0x02, // dim 0 length = 2
            0x00, 0x00, 0x00, 0x01, // dim 0 lower bound = 1
            0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x2A, // 42
            0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x64, // 100
        ]
    }

    #[test]
    fn test_binary_parameter_bool_true() {
        let param = QueryParameter {
            value: Some(Bytes::from_static(&[1])),
            format: 1,
            oid: PgType::BOOL.oid(),
        };
        let result = parameter_to_literal(&param).expect("to convert parameter");
        assert_eq!(result, LiteralValue::Boolean(true));
    }

    #[test]
    fn test_binary_parameter_bool_false() {
        let param = QueryParameter {
            value: Some(Bytes::from_static(&[0])),
            format: 1,
            oid: PgType::BOOL.oid(),
        };
        let result = parameter_to_literal(&param).expect("to convert parameter");
        assert_eq!(result, LiteralValue::Boolean(false));
    }

    #[test]
    fn test_binary_parameter_int2() {
        let param = QueryParameter {
            value: Some(Bytes::from_static(&[0x00, 0x2A])),
            format: 1,
            oid: PgType::INT2.oid(),
        };
        let result = parameter_to_literal(&param).expect("to convert parameter");
        assert_eq!(result, LiteralValue::Integer(42));
    }

    #[test]
    fn test_binary_parameter_int4() {
        let param = QueryParameter {
            value: Some(Bytes::from_static(&[0x00, 0x00, 0x00, 0x2A])),
            format: 1,
            oid: PgType::INT4.oid(),
        };
        let result = parameter_to_literal(&param).expect("to convert parameter");
        assert_eq!(result, LiteralValue::Integer(42));
    }

    #[test]
    fn test_binary_parameter_int8() {
        let param = QueryParameter {
            value: Some(Bytes::from_static(&[
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A,
            ])),
            format: 1,
            oid: PgType::INT8.oid(),
        };
        let result = parameter_to_literal(&param).expect("to convert parameter");
        assert_eq!(result, LiteralValue::Integer(42));
    }

    #[test]
    fn test_binary_parameter_float4() {
        let value: f32 = 2.73;
        let bytes = value.to_be_bytes();
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::FLOAT4.oid(),
        };
        let result = parameter_to_literal(&param).expect("to convert parameter");
        match result {
            LiteralValue::Float(f) => {
                assert!((f.into_inner() - 2.73).abs() < 0.001);
            }
            _ => panic!("Expected Float literal"),
        }
    }

    #[test]
    fn test_binary_parameter_float8() {
        let value: f64 = 2.73821;
        let bytes = value.to_be_bytes();
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::FLOAT8.oid(),
        };
        let result = parameter_to_literal(&param).expect("to convert parameter");
        match result {
            LiteralValue::Float(f) => {
                assert!((f.into_inner() - 2.73821).abs() < 0.00001);
            }
            _ => panic!("Expected Float literal"),
        }
    }

    #[test]
    fn test_binary_parameter_text() {
        let param = QueryParameter {
            value: Some(Bytes::from_static(b"hello world")),
            format: 1,
            oid: PgType::TEXT.oid(),
        };
        let result = parameter_to_literal(&param).expect("to convert parameter");
        assert_eq!(result, LiteralValue::String("hello world".to_owned()));
    }

    #[test]
    fn test_binary_parameter_uuid() {
        let uuid_bytes: [u8; 16] = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ];
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&uuid_bytes)),
            format: 1,
            oid: PgType::UUID.oid(),
        };
        let result = parameter_to_literal(&param).expect("to convert parameter");
        assert_eq!(
            result,
            LiteralValue::String("550e8400-e29b-41d4-a716-446655440000".to_owned())
        );
    }

    #[test]
    fn test_binary_parameter_unsupported_type_with_invalid_utf8() {
        // POINT has no decoder arm; bytes are arbitrary garbage. The
        // assertion is that the function rejects rather than coercing.
        let param = QueryParameter {
            value: Some(Bytes::from_static(&[0xFF, 0xFE])),
            format: 1,
            oid: PgType::POINT.oid(),
        };
        let result = parameter_to_literal(&param).map_err(|e| e.into_current_context());
        assert!(matches!(
            result,
            Err(AstTransformError::UnsupportedBinaryFormat { .. })
        ));
    }

    #[test]
    fn test_binary_parameter_range_rejected() {
        // `Kind::Range(Int4)`: builtin int4range. Even if the binary range
        // wire format happens to be valid UTF-8, the Kind dispatch rejects.
        let param = QueryParameter {
            value: Some(Bytes::from_static(&[0x01])),
            format: 1,
            oid: PgType::INT4_RANGE.oid(),
        };
        let result = parameter_to_literal(&param).map_err(|e| e.into_current_context());
        assert!(matches!(
            result,
            Err(AstTransformError::UnsupportedBinaryFormat { .. })
        ));
    }

    #[test]
    fn test_binary_parameter_multirange_rejected() {
        let param = QueryParameter {
            value: Some(Bytes::from_static(&[0x00, 0x00, 0x00, 0x00])),
            format: 1,
            oid: PgType::INT4MULTI_RANGE.oid(),
        };
        let result = parameter_to_literal(&param).map_err(|e| e.into_current_context());
        assert!(matches!(
            result,
            Err(AstTransformError::UnsupportedBinaryFormat { .. })
        ));
    }

    #[test]
    fn test_binary_parameter_pseudo_rejected() {
        // `record` and `any` are both `Kind::Pseudo`; reject either.
        let param = QueryParameter {
            value: Some(Bytes::from_static(&[0x00, 0x00, 0x00, 0x01])),
            format: 1,
            oid: PgType::RECORD.oid(),
        };
        let result = parameter_to_literal(&param).map_err(|e| e.into_current_context());
        assert!(matches!(
            result,
            Err(AstTransformError::UnsupportedBinaryFormat { .. })
        ));
    }

    #[test]
    fn test_binary_parameter_unknown_oid_rejected() {
        // OID that `PgType::from_oid` can't resolve falls through the Kind
        // dispatch into the fail-closed catch-all in the OID match.
        let param = QueryParameter {
            value: Some(Bytes::from_static(b"42")),
            format: 1,
            oid: 999_999,
        };
        let result = parameter_to_literal(&param).map_err(|e| e.into_current_context());
        assert!(matches!(
            result,
            Err(AstTransformError::UnsupportedBinaryFormat { .. })
        ));
    }

    #[test]
    fn test_binary_parameter_json() {
        // JSON binary format is plain UTF-8 JSON, no framing.
        let json = br#"{"a":1,"b":[2,3]}"#;
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(json)),
            format: 1,
            oid: PgType::JSON.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode binary json");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast(r#"{"a":1,"b":[2,3]}"#.to_owned(), "json".into())
        );
    }

    #[test]
    fn test_binary_parameter_jsonb_strips_version_byte() {
        // JSONB binary: 0x01 version + UTF-8 JSON. The 0x01 SOH byte must
        // not end up in the deparsed SQL literal.
        let mut bytes = vec![0x01u8];
        bytes.extend_from_slice(br#"{"k":"v"}"#);
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::JSONB.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode binary jsonb");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast(r#"{"k":"v"}"#.to_owned(), "jsonb".into())
        );
    }

    #[test]
    fn test_binary_parameter_jsonb_unknown_version_rejected() {
        // Anything other than the documented 0x01 prefix is malformed.
        let bytes = [0x02, b'{', b'}'];
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::JSONB.oid(),
        };
        let result = parameter_to_literal(&param).map_err(|e| e.into_current_context());
        assert!(matches!(
            result,
            Err(AstTransformError::InvalidParameterValue { .. })
        ));
    }

    #[test]
    fn test_binary_parameter_bytea() {
        let param = QueryParameter {
            value: Some(Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef])),
            format: 1,
            oid: PgType::BYTEA.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode binary bytea");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast("\\xdeadbeef".to_owned(), "bytea".into())
        );
    }

    #[test]
    fn test_binary_parameter_bytea_empty() {
        let param = QueryParameter {
            value: Some(Bytes::from_static(&[])),
            format: 1,
            oid: PgType::BYTEA.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode empty bytea");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast("\\x".to_owned(), "bytea".into())
        );
    }

    #[test]
    fn test_binary_bytea_in_query_renders_clean_sql() {
        let mut node = parse_select_node("SELECT id FROM blobs WHERE data = $1");
        let params = binary_params(vec![(Some(&[0xde, 0xad, 0xbe, 0xef]), PgType::BYTEA)]);
        select_node_parameters_replace(&mut node, &params).expect("substitute binary bytea");

        let mut buf = String::new();
        node.deparse(&mut buf);
        assert!(
            !buf.as_bytes().contains(&0),
            "deparsed SQL must not contain NUL bytes; got {buf:?}"
        );
        assert_eq!(
            buf,
            r"SELECT id FROM blobs WHERE data = E'\\xdeadbeef'::bytea"
        );
    }

    #[test]
    fn test_binary_bytea_array() {
        let mut bytes = array_header_bytes(PgType::BYTEA, 2);
        bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x01, 0x01]);
        bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x01, 0xAB]);
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::BYTEA_ARRAY.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode bytea[]");
        assert_eq!(
            literal,
            LiteralValue::Array(
                vec![
                    LiteralValue::StringWithCast("\\x01".to_owned(), "bytea".into()),
                    LiteralValue::StringWithCast("\\xab".to_owned(), "bytea".into()),
                ],
                "bytea[]".into()
            )
        );

        // Each `\` is doubled twice on the way out: once by PG-array-text
        // quoting (`\x01` → `"\\x01"`), once by SQL E-string escaping
        // (`\\` → `\\\\`). End result: four backslashes per element.
        let mut buf = String::new();
        literal.deparse(&mut buf);
        assert_eq!(buf, r#"E'{"\\\\x01","\\\\xab"}'::bytea[]"#);
    }

    #[test]
    fn test_binary_parameter_time_noon() {
        let micros: i64 = 12 * 3600 * 1_000_000;
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&micros.to_be_bytes())),
            format: 1,
            oid: PgType::TIME.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode binary time");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast("12:00:00.000000".to_owned(), "time".into())
        );
    }

    #[test]
    fn test_binary_parameter_time_with_micros() {
        // 13:45:30.123456 — exercises the fractional path.
        let micros: i64 = (13 * 3600 + 45 * 60 + 30) * 1_000_000 + 123_456;
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&micros.to_be_bytes())),
            format: 1,
            oid: PgType::TIME.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode binary time");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast("13:45:30.123456".to_owned(), "time".into())
        );
    }

    #[test]
    fn test_binary_time_in_query_renders_clean_sql() {
        let mut node = parse_select_node("SELECT id FROM events WHERE start = $1");
        let micros: i64 = 9 * 3600 * 1_000_000;
        let bytes = micros.to_be_bytes();
        let params = binary_params(vec![(Some(&bytes), PgType::TIME)]);
        select_node_parameters_replace(&mut node, &params).expect("substitute time");

        let mut buf = String::new();
        node.deparse(&mut buf);
        assert!(!buf.as_bytes().contains(&0));
        assert_eq!(
            buf,
            "SELECT id FROM events WHERE start = '09:00:00.000000'::time"
        );
    }

    #[test]
    fn test_binary_time_array() {
        let t0: i64 = 0;
        let t1: i64 = 12 * 3600 * 1_000_000;
        let mut bytes = array_header_bytes(PgType::TIME, 2);
        for t in [t0, t1] {
            bytes.extend_from_slice(&8_i32.to_be_bytes());
            bytes.extend_from_slice(&t.to_be_bytes());
        }

        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::TIME_ARRAY.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode time[]");
        assert_eq!(
            literal,
            LiteralValue::Array(
                vec![
                    LiteralValue::StringWithCast("00:00:00.000000".to_owned(), "time".into()),
                    LiteralValue::StringWithCast("12:00:00.000000".to_owned(), "time".into()),
                ],
                "time[]".into()
            )
        );
    }

    #[test]
    fn test_binary_parameter_timetz_utc() {
        let micros: i64 = 12 * 3600 * 1_000_000;
        let bytes = timetz_bytes(micros, 0);
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::TIMETZ.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode binary timetz");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast("12:00:00.000000+00".to_owned(), "timetz".into())
        );
    }

    #[test]
    fn test_binary_parameter_timetz_east_of_utc() {
        // `'12:00:00+05:00'::timetz` — PG stores zone = -18000 (seconds
        // west; +05 east is negative-west).
        let micros: i64 = 12 * 3600 * 1_000_000;
        let bytes = timetz_bytes(micros, -5 * 3600);
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::TIMETZ.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode binary timetz");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast("12:00:00.000000+05".to_owned(), "timetz".into())
        );
    }

    #[test]
    fn test_binary_parameter_timetz_west_of_utc() {
        let micros: i64 = 12 * 3600 * 1_000_000;
        let bytes = timetz_bytes(micros, 8 * 3600);
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::TIMETZ.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode binary timetz");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast("12:00:00.000000-08".to_owned(), "timetz".into())
        );
    }

    #[test]
    fn test_binary_parameter_timetz_half_hour_offset() {
        // India: UTC+05:30 → zone = -(5*3600 + 30*60) = -19800.
        let micros: i64 = 9 * 3600 * 1_000_000;
        let bytes = timetz_bytes(micros, -(5 * 3600 + 30 * 60));
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::TIMETZ.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode binary timetz");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast("09:00:00.000000+05:30".to_owned(), "timetz".into())
        );
    }

    #[test]
    fn test_binary_timetz_invalid_length_rejected() {
        let param = QueryParameter {
            value: Some(Bytes::from_static(&[0u8; 11])),
            format: 1,
            oid: PgType::TIMETZ.oid(),
        };
        let result = parameter_to_literal(&param).map_err(|e| e.into_current_context());
        assert!(matches!(
            result,
            Err(AstTransformError::InvalidParameterValue { .. })
        ));
    }

    #[test]
    fn test_binary_parameter_interval_zero() {
        let bytes = interval_bytes(0, 0, 0);
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::INTERVAL.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode binary interval");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast(
                "0 mons 0 days 00:00:00.000000".to_owned(),
                "interval".into()
            )
        );
    }

    #[test]
    fn test_binary_parameter_interval_mixed() {
        // 2 months, 3 days, 4h 5m 6.7s
        let micros: i64 = (4 * 3600 + 5 * 60 + 6) * 1_000_000 + 700_000;
        let bytes = interval_bytes(micros, 3, 2);
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::INTERVAL.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode binary interval");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast(
                "2 mons 3 days 04:05:06.700000".to_owned(),
                "interval".into()
            )
        );
    }

    #[test]
    fn test_binary_parameter_interval_negative_components() {
        // Each component is signed independently. -1 month, -2 days, -1 hour.
        let micros: i64 = -3_600_000_000;
        let bytes = interval_bytes(micros, -2, -1);
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::INTERVAL.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode binary interval");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast(
                "-1 mons -2 days -01:00:00.000000".to_owned(),
                "interval".into()
            )
        );
    }

    #[test]
    fn test_binary_interval_in_query_renders_clean_sql() {
        let mut node = parse_select_node("SELECT id FROM events WHERE age > $1");
        let bytes = interval_bytes(0, 7, 0);
        let params = binary_params(vec![(Some(&bytes), PgType::INTERVAL)]);
        select_node_parameters_replace(&mut node, &params).expect("substitute interval");

        let mut buf = String::new();
        node.deparse(&mut buf);
        assert!(!buf.as_bytes().contains(&0));
        assert_eq!(
            buf,
            "SELECT id FROM events WHERE age > '0 mons 7 days 00:00:00.000000'::interval"
        );
    }

    #[test]
    fn test_binary_interval_invalid_length_rejected() {
        let param = QueryParameter {
            value: Some(Bytes::from_static(&[0u8; 15])),
            format: 1,
            oid: PgType::INTERVAL.oid(),
        };
        let result = parameter_to_literal(&param).map_err(|e| e.into_current_context());
        assert!(matches!(
            result,
            Err(AstTransformError::InvalidParameterValue { .. })
        ));
    }

    #[test]
    fn test_binary_parameter_date_epoch() {
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&0_i32.to_be_bytes())),
            format: 1,
            oid: PgType::DATE.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode binary date");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast("2000-01-01".to_owned(), "date".into())
        );
    }

    #[test]
    fn test_binary_parameter_date_next_day() {
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&1_i32.to_be_bytes())),
            format: 1,
            oid: PgType::DATE.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode binary date");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast("2000-01-02".to_owned(), "date".into())
        );
    }

    #[test]
    fn test_binary_parameter_date_yesterday() {
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&(-1_i32).to_be_bytes())),
            format: 1,
            oid: PgType::DATE.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode binary date");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast("1999-12-31".to_owned(), "date".into())
        );
    }

    #[test]
    fn test_binary_parameter_date_year_1_ad() {
        // 0001-01-01 (proleptic Gregorian) is JDN 1721426; days from
        // 2000-01-01 (JDN 2451545) is -730119.
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&(-730_119_i32).to_be_bytes())),
            format: 1,
            oid: PgType::DATE.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode binary date");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast("0001-01-01".to_owned(), "date".into())
        );
    }

    #[test]
    fn test_binary_parameter_date_one_bc() {
        // 1 BC Jan 1 (year 0 in proleptic Gregorian) is JDN 1721060;
        // days from 2000-01-01 = -730485.
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&(-730_485_i32).to_be_bytes())),
            format: 1,
            oid: PgType::DATE.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode binary date");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast("0001-01-01 BC".to_owned(), "date".into())
        );
    }

    #[test]
    fn test_binary_parameter_date_infinity() {
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&i32::MAX.to_be_bytes())),
            format: 1,
            oid: PgType::DATE.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode infinity date");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast("infinity".to_owned(), "date".into())
        );
    }

    #[test]
    fn test_binary_parameter_date_negative_infinity() {
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&i32::MIN.to_be_bytes())),
            format: 1,
            oid: PgType::DATE.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode -infinity date");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast("-infinity".to_owned(), "date".into())
        );
    }

    #[test]
    fn test_binary_parameter_timestamp_epoch() {
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&0_i64.to_be_bytes())),
            format: 1,
            oid: PgType::TIMESTAMP.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode binary timestamp");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast(
                "2000-01-01 00:00:00.000000".to_owned(),
                "timestamp".into()
            )
        );
    }

    #[test]
    fn test_binary_parameter_timestamp_with_time() {
        // 2000-01-02 12:34:56.123456 = 1 day + 12h34m56.123456s.
        let micros: i64 = USECS_PER_DAY + (12 * 3600 + 34 * 60 + 56) * 1_000_000 + 123_456;
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&micros.to_be_bytes())),
            format: 1,
            oid: PgType::TIMESTAMP.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode binary timestamp");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast(
                "2000-01-02 12:34:56.123456".to_owned(),
                "timestamp".into()
            )
        );
    }

    #[test]
    fn test_binary_parameter_timestamp_just_before_epoch() {
        // Negative micros must use floor-division (rem_euclid) so the
        // sub-day component stays in [0, USECS_PER_DAY). Otherwise -1
        // would yield "2000-01-01 -00:00:00.-000001".
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&(-1_i64).to_be_bytes())),
            format: 1,
            oid: PgType::TIMESTAMP.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode binary timestamp");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast(
                "1999-12-31 23:59:59.999999".to_owned(),
                "timestamp".into()
            )
        );
    }

    #[test]
    fn test_binary_parameter_timestamp_infinity() {
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&i64::MAX.to_be_bytes())),
            format: 1,
            oid: PgType::TIMESTAMP.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode infinity timestamp");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast("infinity".to_owned(), "timestamp".into())
        );
    }

    #[test]
    fn test_binary_parameter_timestamptz_epoch() {
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&0_i64.to_be_bytes())),
            format: 1,
            oid: PgType::TIMESTAMPTZ.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode binary timestamptz");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast(
                "2000-01-01 00:00:00.000000+00".to_owned(),
                "timestamptz".into()
            )
        );
    }

    #[test]
    fn test_binary_parameter_timestamptz_negative_infinity() {
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&i64::MIN.to_be_bytes())),
            format: 1,
            oid: PgType::TIMESTAMPTZ.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode -infinity timestamptz");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast("-infinity".to_owned(), "timestamptz".into())
        );
    }

    #[test]
    fn test_binary_date_in_query_renders_clean_sql() {
        let mut node = parse_select_node("SELECT id FROM events WHERE day = $1");
        let bytes = 1_i32.to_be_bytes();
        let params = binary_params(vec![(Some(&bytes), PgType::DATE)]);
        select_node_parameters_replace(&mut node, &params).expect("substitute date");

        let mut buf = String::new();
        node.deparse(&mut buf);
        assert!(!buf.as_bytes().contains(&0));
        assert_eq!(buf, "SELECT id FROM events WHERE day = '2000-01-02'::date");
    }

    #[test]
    fn test_binary_date_array() {
        let mut bytes = array_header_bytes(PgType::DATE, 2);
        for days in [0_i32, 1] {
            bytes.extend_from_slice(&4_i32.to_be_bytes());
            bytes.extend_from_slice(&days.to_be_bytes());
        }
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::DATE_ARRAY.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode date[]");
        assert_eq!(
            literal,
            LiteralValue::Array(
                vec![
                    LiteralValue::StringWithCast("2000-01-01".to_owned(), "date".into()),
                    LiteralValue::StringWithCast("2000-01-02".to_owned(), "date".into()),
                ],
                "date[]".into()
            )
        );
    }

    #[test]
    fn test_binary_parameter_macaddr() {
        let param = QueryParameter {
            value: Some(Bytes::from_static(&[0x00, 0x11, 0x22, 0x33, 0x44, 0x55])),
            format: 1,
            oid: PgType::MACADDR.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode binary macaddr");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast("00:11:22:33:44:55".to_owned(), "macaddr".into())
        );
    }

    #[test]
    fn test_binary_parameter_macaddr_invalid_length_rejected() {
        let param = QueryParameter {
            value: Some(Bytes::from_static(&[0x00, 0x11, 0x22, 0x33, 0x44])),
            format: 1,
            oid: PgType::MACADDR.oid(),
        };
        let result = parameter_to_literal(&param).map_err(|e| e.into_current_context());
        assert!(matches!(
            result,
            Err(AstTransformError::InvalidParameterValue { .. })
        ));
    }

    #[test]
    fn test_binary_parameter_macaddr8() {
        let param = QueryParameter {
            value: Some(Bytes::from_static(&[
                0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
            ])),
            format: 1,
            oid: PgType::MACADDR8.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode binary macaddr8");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast("00:11:22:33:44:55:66:77".to_owned(), "macaddr8".into())
        );
    }

    #[test]
    fn test_binary_parameter_inet_v4_with_prefix() {
        let bytes = inet_bytes(&[192, 168, 1, 0], 24, false);
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::INET.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode binary inet");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast("192.168.1.0/24".to_owned(), "inet".into())
        );
    }

    #[test]
    fn test_binary_parameter_inet_v4_host_omits_prefix() {
        // INET with default mask (32) for v4 omits `/32` to match PG's
        // canonical output.
        let bytes = inet_bytes(&[192, 168, 1, 1], 32, false);
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::INET.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode binary inet");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast("192.168.1.1".to_owned(), "inet".into())
        );
    }

    #[test]
    fn test_binary_parameter_inet_v6_with_prefix() {
        // 2001:db8::/64
        let mut addr = [0u8; 16];
        addr[0] = 0x20;
        addr[1] = 0x01;
        addr[2] = 0x0d;
        addr[3] = 0xb8;
        let bytes = inet_bytes(&addr, 64, false);
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::INET.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode binary inet v6");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast("2001:db8::/64".to_owned(), "inet".into())
        );
    }

    #[test]
    fn test_binary_parameter_cidr_v4() {
        let bytes = inet_bytes(&[10, 0, 0, 0], 8, true);
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::CIDR.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode binary cidr");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast("10.0.0.0/8".to_owned(), "cidr".into())
        );
    }

    #[test]
    fn test_binary_parameter_cidr_v4_full_host_keeps_prefix() {
        // Unlike INET, CIDR always emits the prefix even at the default
        // mask — `/32` distinguishes it semantically from a bare host.
        let bytes = inet_bytes(&[192, 168, 1, 1], 32, true);
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::CIDR.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode binary cidr");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast("192.168.1.1/32".to_owned(), "cidr".into())
        );
    }

    #[test]
    fn test_binary_inet_in_query_renders_clean_sql() {
        let mut node = parse_select_node("SELECT id FROM nodes WHERE addr = $1");
        let bytes = inet_bytes(&[10, 0, 0, 5], 32, false);
        let params = binary_params(vec![(Some(&bytes), PgType::INET)]);
        select_node_parameters_replace(&mut node, &params).expect("substitute inet");

        let mut buf = String::new();
        node.deparse(&mut buf);
        assert!(!buf.as_bytes().contains(&0));
        assert_eq!(buf, "SELECT id FROM nodes WHERE addr = '10.0.0.5'::inet");
    }

    #[test]
    fn test_binary_macaddr_array() {
        let mut bytes = array_header_bytes(PgType::MACADDR, 2);
        for octets in [
            [0x00u8, 0x11, 0x22, 0x33, 0x44, 0x55],
            [0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff],
        ] {
            bytes.extend_from_slice(&6_i32.to_be_bytes());
            bytes.extend_from_slice(&octets);
        }
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::MACADDR_ARRAY.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode macaddr[]");
        assert_eq!(
            literal,
            LiteralValue::Array(
                vec![
                    LiteralValue::StringWithCast("00:11:22:33:44:55".to_owned(), "macaddr".into()),
                    LiteralValue::StringWithCast("aa:bb:cc:dd:ee:ff".to_owned(), "macaddr".into()),
                ],
                "macaddr[]".into()
            )
        );
    }

    #[test]
    fn test_binary_parameter_numeric_simple_int() {
        assert_numeric(numeric_bytes(0, NUMERIC_POS, 0, &[42]), "42");
    }

    #[test]
    fn test_binary_parameter_numeric_zero_no_scale() {
        assert_numeric(numeric_bytes(0, NUMERIC_POS, 0, &[]), "0");
    }

    #[test]
    fn test_binary_parameter_numeric_zero_with_scale() {
        // dscale > 0 forces trailing zeros after the decimal point.
        assert_numeric(numeric_bytes(0, NUMERIC_POS, 4, &[]), "0.0000");
    }

    #[test]
    fn test_binary_parameter_numeric_decimal() {
        // 42.5: digits at weight 0 and -1, dscale 1 truncates the second
        // digit (5000) to a single character "5".
        assert_numeric(numeric_bytes(0, NUMERIC_POS, 1, &[42, 5000]), "42.5");
    }

    #[test]
    fn test_binary_parameter_numeric_decimal_trailing_zeros() {
        // 1.50 keeps the trailing zero because dscale = 2.
        assert_numeric(numeric_bytes(0, NUMERIC_POS, 2, &[1, 5000]), "1.50");
    }

    #[test]
    fn test_binary_parameter_numeric_negative() {
        assert_numeric(numeric_bytes(0, NUMERIC_NEG, 2, &[3, 1400]), "-3.14");
    }

    #[test]
    fn test_binary_parameter_numeric_big_with_implicit_zeros() {
        // 1.5 × 10⁸ = 150_000_000 — digits[0]=1 at weight 2, digits[1]=5000
        // at weight 1, weight 0 implicit zero.
        assert_numeric(numeric_bytes(2, NUMERIC_POS, 0, &[1, 5000]), "150000000");
    }

    #[test]
    fn test_binary_parameter_numeric_small_fraction() {
        // 0.00001 = 1000 × 10000⁻², single digit at weight -2.
        assert_numeric(numeric_bytes(-2, NUMERIC_POS, 5, &[1000]), "0.00001");
    }

    #[test]
    fn test_binary_parameter_numeric_nan() {
        assert_numeric(numeric_bytes(0, NUMERIC_NAN, 0, &[]), "NaN");
    }

    #[test]
    fn test_binary_parameter_numeric_infinity() {
        assert_numeric(numeric_bytes(0, NUMERIC_PINF, 0, &[]), "Infinity");
    }

    #[test]
    fn test_binary_parameter_numeric_negative_infinity() {
        assert_numeric(numeric_bytes(0, NUMERIC_NINF, 0, &[]), "-Infinity");
    }

    #[test]
    fn test_binary_parameter_numeric_invalid_sign_rejected() {
        // 0xE000 isn't a defined sign code.
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&numeric_bytes(0, 0xE000, 0, &[]))),
            format: 1,
            oid: PgType::NUMERIC.oid(),
        };
        let result = parameter_to_literal(&param).map_err(|e| e.into_current_context());
        assert!(matches!(
            result,
            Err(AstTransformError::InvalidParameterValue { .. })
        ));
    }

    #[test]
    fn test_binary_parameter_numeric_negative_ndigits_rejected() {
        // Construct a header with ndigits = -1 directly (the safe builder
        // rejects negative lengths via try_from).
        let mut bytes = Vec::with_capacity(8);
        bytes.extend_from_slice(&(-1_i16).to_be_bytes());
        bytes.extend_from_slice(&0_i16.to_be_bytes());
        bytes.extend_from_slice(&0_u16.to_be_bytes());
        bytes.extend_from_slice(&0_i16.to_be_bytes());

        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::NUMERIC.oid(),
        };
        let result = parameter_to_literal(&param).map_err(|e| e.into_current_context());
        assert!(matches!(
            result,
            Err(AstTransformError::InvalidParameterValue { .. })
        ));
    }

    #[test]
    fn test_binary_parameter_numeric_truncated_rejected() {
        // Header claims 2 digits but only one digit's bytes follow.
        let mut bytes = Vec::with_capacity(10);
        bytes.extend_from_slice(&2_i16.to_be_bytes());
        bytes.extend_from_slice(&0_i16.to_be_bytes());
        bytes.extend_from_slice(&0_u16.to_be_bytes());
        bytes.extend_from_slice(&0_i16.to_be_bytes());
        bytes.extend_from_slice(&42_i16.to_be_bytes());

        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::NUMERIC.oid(),
        };
        let result = parameter_to_literal(&param).map_err(|e| e.into_current_context());
        assert!(matches!(
            result,
            Err(AstTransformError::InvalidParameterValue { .. })
        ));
    }

    #[test]
    fn test_binary_parameter_numeric_digit_out_of_range_rejected() {
        // 10000 is one past the legal max.
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&numeric_bytes(
                0,
                NUMERIC_POS,
                0,
                &[10000],
            ))),
            format: 1,
            oid: PgType::NUMERIC.oid(),
        };
        let result = parameter_to_literal(&param).map_err(|e| e.into_current_context());
        assert!(matches!(
            result,
            Err(AstTransformError::InvalidParameterValue { .. })
        ));
    }

    #[test]
    fn test_binary_numeric_in_query_renders_clean_sql() {
        let mut node = parse_select_node("SELECT id FROM ledger WHERE balance > $1");
        let bytes = numeric_bytes(0, NUMERIC_POS, 2, &[3, 1400]);
        let params = binary_params(vec![(Some(&bytes), PgType::NUMERIC)]);
        select_node_parameters_replace(&mut node, &params).expect("substitute numeric");

        let mut buf = String::new();
        node.deparse(&mut buf);
        assert!(!buf.as_bytes().contains(&0));
        assert_eq!(buf, "SELECT id FROM ledger WHERE balance > '3.14'::numeric");
    }

    #[test]
    fn test_binary_numeric_array() {
        let mut bytes = array_header_bytes(PgType::NUMERIC, 2);
        for elem in [
            numeric_bytes(0, NUMERIC_POS, 1, &[42, 5000]), // "42.5"
            numeric_bytes(0, NUMERIC_NEG, 2, &[3, 1400]),  // "-3.14"
        ] {
            let len = i32::try_from(elem.len()).expect("test element fits in i32");
            bytes.extend_from_slice(&len.to_be_bytes());
            bytes.extend_from_slice(&elem);
        }

        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::NUMERIC_ARRAY.oid(),
        };
        let literal = parameter_to_literal(&param).expect("decode numeric[]");
        assert_eq!(
            literal,
            LiteralValue::Array(
                vec![
                    LiteralValue::StringWithCast("42.5".to_owned(), "numeric".into()),
                    LiteralValue::StringWithCast("-3.14".to_owned(), "numeric".into()),
                ],
                "numeric[]".into()
            )
        );
    }

    #[test]
    fn test_binary_parameter_unsupported_simple_type_rejected_valid_utf8() {
        // Valid-UTF-8 bytes for an unsupported `Kind::Simple` type must
        // not fall through to a `String` literal — that would silently
        // corrupt SQL. POINT exercises the per-OID match's fail-closed
        // catch-all rather than the Kind-dispatch path.
        let param = QueryParameter {
            value: Some(Bytes::from_static(&[
                0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            ])),
            format: 1,
            oid: PgType::POINT.oid(),
        };
        let result = parameter_to_literal(&param).map_err(|e| e.into_current_context());
        assert!(matches!(
            result,
            Err(AstTransformError::UnsupportedBinaryFormat { .. })
        ));
    }

    #[test]
    fn test_binary_parameter_null() {
        let param = QueryParameter {
            value: None,
            format: 1,
            oid: PgType::INT4.oid(),
        };
        let result = parameter_to_literal(&param).expect("to convert parameter");
        assert_eq!(result, LiteralValue::Null);
    }

    #[test]
    fn test_binary_int4_in_query() {
        let mut node = parse_select_node("SELECT id FROM users WHERE id = $1");
        let params = binary_params(vec![(Some(&[0x00, 0x00, 0x00, 0x2A]), PgType::INT4)]);
        select_node_parameters_replace(&mut node, &params).expect("to replace parameters");

        let mut buf = String::new();
        node.deparse(&mut buf);
        assert_eq!(buf, "SELECT id FROM users WHERE id = 42");
    }

    #[test]
    fn test_binary_int4_array_decoded() {
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&binary_int4_array_42_100())),
            format: 1,
            oid: PgType::INT4_ARRAY.oid(),
        };

        let literal = parameter_to_literal(&param).expect("decode binary int4[]");
        assert_eq!(
            literal,
            LiteralValue::Array(
                vec![LiteralValue::Integer(42), LiteralValue::Integer(100)],
                "int4[]".into()
            )
        );
    }

    #[test]
    fn test_binary_int4_array_distinct_values_produce_distinct_fingerprints() {
        // Sanity check that two different binary int4[] parameter values
        // substituted into the same query template produce different
        // post-substitution fingerprints. Otherwise pgcache would route
        // them to the same cache entry and one query's results would bleed
        // into the other's.
        let q1 = query_expr_parse("SELECT id FROM widgets WHERE id = ANY($1)")
            .expect("convert to QueryExpr");
        let q2 = q1.clone();

        let arr1 = vec![
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x17, 0x00, 0x00,
            0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x01,
            0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x02,
        ];
        let arr2 = vec![
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x17, 0x00, 0x00,
            0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x03,
            0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00,
            0x00, 0x05,
        ];
        let p1 = binary_params(vec![(Some(&arr1), PgType::INT4_ARRAY)]);
        let p2 = binary_params(vec![(Some(&arr2), PgType::INT4_ARRAY)]);

        let r1 = query_expr_parameters_replace(&q1, &p1).expect("replace 1");
        let r2 = query_expr_parameters_replace(&q2, &p2).expect("replace 2");

        let mut s1 = String::new();
        let mut s2 = String::new();
        r1.deparse(&mut s1);
        r2.deparse(&mut s2);
        assert_ne!(s1, s2, "deparsed SQL should differ between arrays");

        let f1 = query_expr_fingerprint(&r1);
        let f2 = query_expr_fingerprint(&r2);
        assert_ne!(
            f1, f2,
            "different binary int4[] values must produce different fingerprints"
        );
    }

    #[test]
    fn test_binary_int4_array_in_query_renders_clean_sql() {
        let mut node = parse_select_node("SELECT id FROM users WHERE id = ANY($1)");

        let array_bytes = binary_int4_array_42_100();
        let params = binary_params(vec![(Some(&array_bytes), PgType::INT4_ARRAY)]);

        select_node_parameters_replace(&mut node, &params).expect("substitute binary int4[]");

        let mut buf = String::new();
        node.deparse(&mut buf);

        assert!(
            !buf.as_bytes().contains(&0),
            "deparsed SQL must not contain NUL bytes; got {buf:?}"
        );
        assert_eq!(
            buf,
            "SELECT id FROM users WHERE id = ANY ('{42,100}'::int4[])"
        );
    }

    #[test]
    fn test_binary_int4_array_empty() {
        let bytes = vec![
            0x00, 0x00, 0x00, 0x00, // ndim = 0
            0x00, 0x00, 0x00, 0x00, // hasnull = 0
            0x00, 0x00, 0x00, 0x17, // elemtype = 23 (int4)
        ];
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::INT4_ARRAY.oid(),
        };

        let literal = parameter_to_literal(&param).expect("decode empty int4[]");
        assert_eq!(literal, LiteralValue::Array(vec![], "int4[]".into()));
    }

    #[test]
    fn test_binary_int4_array_with_null_element() {
        // `{1, NULL, 3}::int4[]`: NULL elements have length-prefix = -1.
        let bytes = vec![
            0x00, 0x00, 0x00, 0x01, // ndim = 1
            0x00, 0x00, 0x00, 0x01, // hasnull = 1
            0x00, 0x00, 0x00, 0x17, // elemtype = 23 (int4)
            0x00, 0x00, 0x00, 0x03, // dim 0 length = 3
            0x00, 0x00, 0x00, 0x01, // dim 0 lower bound = 1
            0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00,
            0x00, 0x04, 0x00, 0x00, 0x00, 0x03,
        ];
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::INT4_ARRAY.oid(),
        };

        let literal = parameter_to_literal(&param).expect("decode int4[] with null");
        assert_eq!(
            literal,
            LiteralValue::Array(
                vec![
                    LiteralValue::Integer(1),
                    LiteralValue::Null,
                    LiteralValue::Integer(3),
                ],
                "int4[]".into()
            )
        );
    }

    #[test]
    fn test_binary_text_array_quoting() {
        // `text[]` with elements that all need PG-array-text quoting.
        let mut bytes = vec![
            0x00, 0x00, 0x00, 0x01, // ndim = 1
            0x00, 0x00, 0x00, 0x00, // hasnull = 0
            0x00, 0x00, 0x00, 0x19, // elemtype = 25 (text)
            0x00, 0x00, 0x00, 0x05, // dim 0 length = 5
            0x00, 0x00, 0x00, 0x01, // dim 0 lower bound = 1
        ];
        for s in ["plain", "with,comma", "has\"quote", "back\\slash", ""] {
            bytes.extend_from_slice(&array_text_element_bytes(s));
        }

        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::TEXT_ARRAY.oid(),
        };

        let literal = parameter_to_literal(&param).expect("decode text[]");
        assert_eq!(
            literal,
            LiteralValue::Array(
                vec![
                    LiteralValue::String("plain".to_owned()),
                    LiteralValue::String("with,comma".to_owned()),
                    LiteralValue::String("has\"quote".to_owned()),
                    LiteralValue::String("back\\slash".to_owned()),
                    LiteralValue::String(String::new()),
                ],
                "text[]".into()
            )
        );

        let mut buf = String::new();
        literal.deparse(&mut buf);
        assert_eq!(
            buf,
            r#"E'{plain,"with,comma","has\\"quote","back\\\\slash",""}'::text[]"#
        );
    }

    #[test]
    fn test_binary_text_array_with_null_string_element() {
        // The element value `"null"` (case-insensitive) must be quoted so
        // PG doesn't read it as a NULL marker.
        let mut bytes = vec![
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x19, 0x00, 0x00,
            0x00, 0x02, 0x00, 0x00, 0x00, 0x01,
        ];
        bytes.extend_from_slice(&array_text_element_bytes("NULL"));
        bytes.extend_from_slice(&array_text_element_bytes("a"));

        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::TEXT_ARRAY.oid(),
        };

        let literal = parameter_to_literal(&param).expect("decode text[]");
        assert_eq!(
            literal,
            LiteralValue::Array(
                vec![
                    LiteralValue::String("NULL".to_owned()),
                    LiteralValue::String("a".to_owned()),
                ],
                "text[]".into()
            )
        );

        let mut buf = String::new();
        literal.deparse(&mut buf);
        assert_eq!(buf, r#"'{"NULL",a}'::text[]"#);
    }

    #[test]
    fn test_binary_multidim_array_rejected() {
        // 2-D arrays fall through to origin uncached.
        let bytes = vec![
            0x00, 0x00, 0x00, 0x02, // ndim = 2
            0x00, 0x00, 0x00, 0x00, // hasnull = 0
            0x00, 0x00, 0x00, 0x17, // elemtype = 23 (int4)
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
            0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x2A,
        ];
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::INT4_ARRAY.oid(),
        };

        let result = parameter_to_literal(&param).map_err(|e| e.into_current_context());
        assert!(
            matches!(
                result,
                Err(AstTransformError::UnsupportedBinaryFormat { .. })
            ),
            "expected UnsupportedBinaryFormat for 2-D array, got {result:?}"
        );
    }

    #[test]
    fn test_binary_array_unsupported_element_type_rejected() {
        // POINT has no scalar arm; element bytes are placeholder garbage.
        let mut bytes = array_header_bytes(PgType::POINT, 1);
        bytes.extend_from_slice(&16_i32.to_be_bytes());
        bytes.extend_from_slice(&[0u8; 16]);

        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::POINT_ARRAY.oid(),
        };

        let result = parameter_to_literal(&param).map_err(|e| e.into_current_context());
        assert!(
            matches!(
                result,
                Err(AstTransformError::UnsupportedBinaryFormat { .. })
            ),
            "expected UnsupportedBinaryFormat for point[], got {result:?}"
        );
    }
}
