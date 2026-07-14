//! Render a decoded binary parameter as canonical PostgreSQL text.
//!
//! The other direction from `parameters::text`, which *parses* text-format
//! parameters. Everything here takes an already-decoded value (micros, a
//! Julian day number, numeric wire digits) and produces the text PG itself
//! would emit — so the deparsed SQL re-binds identically without a round-trip
//! through PG's text input parser.
//!
//! Pure and dependency-free: no wire decoding, no error domain beyond the
//! numeric wire walk.

use std::fmt::Write as _;

use postgres_protocol::types as pg_types;
use rootcause::Report;

use super::super::super::{AstTransformError, AstTransformResult};

/// JDN of `2000-01-01`, PG's epoch for both `date` and `timestamp[tz]`.
pub(super) const POSTGRES_EPOCH_JDATE: i32 = 2_451_545;
pub(super) const USECS_PER_DAY: i64 = 86_400_000_000;

// `numeric` sign codes — see PG `src/backend/utils/adt/numeric.c`.
pub(super) const NUMERIC_POS: u16 = 0x0000;
pub(super) const NUMERIC_NEG: u16 = 0x4000;
pub(super) const NUMERIC_NAN: u16 = 0xC000;
pub(super) const NUMERIC_PINF: u16 = 0xD000;
pub(super) const NUMERIC_NINF: u16 = 0xF000;

/// Format raw bytea bytes as PG's hex text representation `\x<lowercase-hex>`.
pub(super) fn bytea_to_hex_literal(bytes: &[u8]) -> String {
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
pub(super) fn time_micros_to_text(micros: i64) -> String {
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
pub(super) fn pg_days_to_ymd(days: i32) -> (i32, u32, u32) {
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
pub(super) fn ymd_to_text(year: i32, month: u32, day: u32) -> String {
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
pub(super) fn timestamp_micros_to_text(micros: i64) -> String {
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
pub(super) fn macaddr_to_text(octets: &[u8]) -> String {
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
pub(super) fn inet_to_text(inet: &pg_types::Inet, force_prefix: bool) -> String {
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
pub(super) fn numeric_parse_wire(bytes: &[u8]) -> AstTransformResult<(i16, u16, usize, Vec<i16>)> {
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
pub(super) fn numeric_to_text(
    weight: i16,
    sign: u16,
    dscale: usize,
    digits: &[i16],
) -> Option<String> {
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
pub(super) fn timetz_to_text(micros: i64, zone_secs: i32) -> String {
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
pub(super) fn interval_to_text(micros: i64, days: i32, months: i32) -> String {
    let sign = if micros < 0 { "-" } else { "" };
    let abs_micros = micros.unsigned_abs();
    let total_secs = abs_micros / 1_000_000;
    let frac = abs_micros % 1_000_000;
    let h = total_secs / 3600;
    let m = (total_secs % 3600) / 60;
    let s = total_secs % 60;
    format!("{months} mons {days} days {sign}{h:02}:{m:02}:{s:02}.{frac:06}")
}
