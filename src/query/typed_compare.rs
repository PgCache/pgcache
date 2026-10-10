//! Type-aware comparison of a predicate literal against a row's wire text
//! (PGC-478, PGC-287 class 2).
//!
//! The CDC fast path decides membership in Rust by comparing the literal a
//! query was registered with against the text pgoutput renders for a row.
//! That text is one fixed shape per type once the replication session's
//! `DateStyle` output is ISO; the literal is whatever the client wrote. Both
//! sides are parsed by the column's type and compared as values, so
//! `'2024-01-15 10:00'` equals the wire `2024-01-15 10:00:00` and `'10.50'`
//! equals `10.5`. A literal the parser does not handle for the type makes the
//! predicate PgEval at classification ([`literal_locally_comparable`] is the
//! single gate the classifier asks), so evaluation never meets one.
//!
//! A `timestamptz` literal without an offset is left to PgEval: class 2's
//! canonicalisation rewrites one it can read into an instant with an explicit
//! offset before any literal reaches this module.

use std::cmp::Ordering;

use jiff::civil::{Date, DateTime};
use jiff::tz::TimeZone;
use jiff::{Timestamp, Zoned};
use ordered_float::NotNan;
use postgres_types::{Kind, Type};

use crate::query::ast::{BinaryOp, Deparse, LiteralValue};
use crate::query::cast::bool_literal_parse;
use crate::query::literal_settings::{DateOrder, LiteralSettings};

/// How a column's values compare, derived from its type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueKind {
    /// `int2`, `int4`, `int8`, `oid`: exact integers.
    Integer,
    /// `numeric`, `float4`, `float8`: exact decimal comparison of the text.
    Decimal,
    Bool,
    Date,
    /// `timestamp` without time zone: a civil date-time.
    Timestamp,
    /// `timestamp with time zone`: an instant.
    TimestampTz,
    /// `text`, `varchar`, `name`: bytes, every operator (ordering is byte
    /// order, as it was; collation-aware ordering is PGC-459).
    Text,
    /// `bpchar`: bytes with trailing spaces ignored.
    Bpchar,
    /// `uuid`: case-insensitive hex.
    Uuid,
    /// An enum: equality on the label only.
    Enum,
    /// Anything else: the cache database decides (PgEval).
    Unsupported,
}

impl ValueKind {
    pub fn of_type(ty: &Type) -> Self {
        if let Kind::Enum(_) = ty.kind() {
            return Self::Enum;
        }
        if let Kind::Domain(base) = ty.kind() {
            return Self::of_type(base);
        }
        match *ty {
            Type::INT2 | Type::INT4 | Type::INT8 | Type::OID => Self::Integer,
            Type::NUMERIC | Type::FLOAT4 | Type::FLOAT8 => Self::Decimal,
            Type::BOOL => Self::Bool,
            Type::DATE => Self::Date,
            Type::TIMESTAMP => Self::Timestamp,
            Type::TIMESTAMPTZ => Self::TimestampTz,
            Type::TEXT | Type::VARCHAR | Type::NAME => Self::Text,
            Type::BPCHAR => Self::Bpchar,
            Type::UUID => Self::Uuid,
            _ => Self::Unsupported,
        }
    }

    /// The kind a literal's cast names (`'…'::timestamptz`), from the
    /// canonical type name the converter stores on the literal.
    pub(crate) fn of_cast_name(name: &str) -> Self {
        match name {
            "int2" | "int4" | "int8" | "oid" => Self::Integer,
            "numeric" | "float4" | "float8" => Self::Decimal,
            "bool" => Self::Bool,
            "date" => Self::Date,
            "timestamp" => Self::Timestamp,
            "timestamptz" => Self::TimestampTz,
            "text" | "varchar" | "name" => Self::Text,
            "bpchar" => Self::Bpchar,
            "uuid" => Self::Uuid,
            _ => Self::Unsupported,
        }
    }
}

/// A value parsed for comparison. `Ord` within one kind only.
#[derive(Debug, Clone, PartialEq, Eq)]
enum TypedValue {
    Integer(i128),
    Decimal(Decimal),
    Bool(bool),
    Date(Date),
    Timestamp(DateTime),
    TimestampTz(Timestamp),
    Text(String),
    /// `infinity` / `-infinity` of a date-time kind, `Infinity` / `-Infinity`
    /// of a numeric one: beyond every finite value.
    Infinite {
        negative: bool,
    },
    /// Numeric `NaN`: above everything, equal only to itself, as at origin.
    NaN,
}

impl TypedValue {
    fn cmp_same_kind(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::NaN, Self::NaN) => return Some(Ordering::Equal),
            (Self::NaN, _) => return Some(Ordering::Greater),
            (_, Self::NaN) => return Some(Ordering::Less),
            (Self::Infinite { negative: a }, Self::Infinite { negative: b }) => {
                return Some(b.cmp(a));
            }
            (Self::Infinite { negative }, _) => {
                return Some(if *negative {
                    Ordering::Less
                } else {
                    Ordering::Greater
                });
            }
            (_, Self::Infinite { negative }) => {
                return Some(if *negative {
                    Ordering::Greater
                } else {
                    Ordering::Less
                });
            }
            _ => {}
        }
        Some(match (self, other) {
            (Self::Integer(a), Self::Integer(b)) => a.cmp(b),
            (Self::Decimal(a), Self::Decimal(b)) => a.cmp(b),
            // An integer column against a fractional literal (or the
            // reverse spelling) compares as numeric, as at origin.
            (Self::Integer(a), Self::Decimal(b)) => Decimal::from_int(*a).cmp(b),
            (Self::Decimal(a), Self::Integer(b)) => a.cmp(&Decimal::from_int(*b)),
            (Self::Bool(a), Self::Bool(b)) => a.cmp(b),
            (Self::Date(a), Self::Date(b)) => a.cmp(b),
            (Self::Timestamp(a), Self::Timestamp(b)) => a.cmp(b),
            (Self::TimestampTz(a), Self::TimestampTz(b)) => a.cmp(b),
            (Self::Text(a), Self::Text(b)) => a.cmp(b),
            _ => return None,
        })
    }
}

/// An exact decimal: sign, digits with the point removed, and the exponent
/// such that value = digits × 10^exponent. Normalised (no leading or trailing
/// zeros in `digits`) so equal values compare equal.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Decimal {
    negative: bool,
    digits: Vec<u8>,
    exponent: i32,
}

impl Decimal {
    fn from_int(n: i128) -> Self {
        // An integer's decimal text always parses.
        Self::parse(&n.to_string()).unwrap_or(Self {
            negative: false,
            digits: Vec::new(),
            exponent: 0,
        })
    }

    fn parse(text: &str) -> Option<Self> {
        let (negative, rest) = sign_split(text.trim());
        let (mantissa, exponent) = match rest.split_once(['e', 'E']) {
            Some((mantissa, exponent)) => (mantissa, exponent.parse::<i32>().ok()?),
            None => (rest, 0),
        };
        let (int_part, frac_part) = mantissa.split_once('.').unwrap_or((mantissa, ""));
        if int_part.is_empty() && frac_part.is_empty() {
            return None;
        }
        if !digits_only(int_part) || !digits_only(frac_part) {
            return None;
        }
        let digits = int_part
            .bytes()
            .chain(frac_part.bytes())
            .map(|b| b - b'0')
            .collect();
        let exponent = exponent - i32::try_from(frac_part.len()).ok()?;
        Some(Self::normalised(negative, digits, exponent))
    }

    /// Strip leading and trailing zeros so equal values compare equal; zero
    /// is unsigned with exponent 0.
    fn normalised(negative: bool, mut digits: Vec<u8>, mut exponent: i32) -> Self {
        while digits.last() == Some(&0) {
            digits.pop();
            exponent += 1;
        }
        let leading = digits.iter().take_while(|d| **d == 0).count();
        digits.drain(..leading);
        if digits.is_empty() {
            return Self {
                negative: false,
                digits,
                exponent: 0,
            };
        }
        Self {
            negative,
            digits,
            exponent,
        }
    }

    /// -1, 0 or 1: the order of signs, zero between.
    fn sign(&self) -> i8 {
        match (self.digits.is_empty(), self.negative) {
            (true, _) => 0,
            (false, true) => -1,
            (false, false) => 1,
        }
    }

    /// The nearest `f64`: exact for what fits, approximate beyond (an index
    /// key may over-return; the precise comparison stays exact).
    fn to_f64(&self) -> f64 {
        let digits: String = self.digits.iter().map(|d| char::from(b'0' + d)).collect();
        let text = format!(
            "{}{}e{}",
            if self.negative { "-" } else { "" },
            if digits.is_empty() { "0" } else { &digits },
            self.exponent
        );
        text.parse().unwrap_or(0.0)
    }

    /// Position of the most significant digit relative to the point.
    fn magnitude(&self) -> i64 {
        i64::try_from(self.digits.len()).unwrap_or(i64::MAX) + i64::from(self.exponent)
    }
}

impl Ord for Decimal {
    fn cmp(&self, other: &Self) -> Ordering {
        let by_sign = self.sign().cmp(&other.sign());
        if by_sign != Ordering::Equal {
            return by_sign;
        }
        let magnitude = self
            .magnitude()
            .cmp(&other.magnitude())
            .then_with(|| self.digits.cmp(&other.digits));
        if self.negative {
            magnitude.reverse()
        } else {
            magnitude
        }
    }
}

/// `(negative, rest)` for an optional leading sign.
fn sign_split(text: &str) -> (bool, &str) {
    if let Some(rest) = text.strip_prefix('-') {
        (true, rest)
    } else {
        (false, text.strip_prefix('+').unwrap_or(text))
    }
}

fn digits_only(text: &str) -> bool {
    text.bytes().all(|b| b.is_ascii_digit())
}

impl PartialOrd for Decimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Whether the classifier may keep `column op literal` local for a column of
/// `ty`: the literal parses as that kind, and the operator is one the kind
/// orders under.
pub fn literal_locally_comparable(ty: &Type, literal: &LiteralValue, op: BinaryOp) -> bool {
    let kind = ValueKind::of_type(ty);
    if kind == ValueKind::Unsupported {
        return false;
    }
    if matches!(kind, ValueKind::Enum) && !matches!(op, BinaryOp::Equal | BinaryOp::NotEqual) {
        return false;
    }
    match literal {
        // NULL and parameters never match a non-null row; the evaluator
        // decides them without parsing.
        LiteralValue::Null | LiteralValue::NullWithCast(_) | LiteralValue::Parameter(_) => true,
        LiteralValue::Array(..) => false,
        LiteralValue::String(_)
        | LiteralValue::StringWithCast(..)
        | LiteralValue::Integer(_)
        | LiteralValue::Float(_)
        | LiteralValue::Boolean(_) => literal_parse(kind, literal).is_some(),
    }
}

/// Compare `literal` against a row's wire text for a column of `ty`. `None`
/// when either side does not parse as the type (the classifier keeps such
/// literals off this path; a malformed row never matches).
pub fn typed_compare(
    ty: &Type,
    literal: &LiteralValue,
    row_text: &str,
    op: BinaryOp,
) -> Option<bool> {
    TypedLiteral::new(ty, literal).row_compare(row_text, op)
}

/// A comparison's literal parsed once for its column's kind, so a predicate
/// evaluated per CDC row parses only the row's text ([`Self::row_compare`]).
#[derive(Debug, Clone)]
pub struct TypedLiteral {
    kind: ValueKind,
    parsed: LiteralParse,
}

#[derive(Debug, Clone)]
enum LiteralParse {
    /// NULL or a parameter: a present row value never matches it.
    Never,
    /// A literal the comparator cannot decide for the kind.
    Undecidable,
    Value(TypedValue),
}

impl TypedLiteral {
    pub fn new(ty: &Type, literal: &LiteralValue) -> Self {
        let kind = ValueKind::of_type(ty);
        let parsed = match literal {
            LiteralValue::Null | LiteralValue::NullWithCast(_) | LiteralValue::Parameter(_) => {
                LiteralParse::Never
            }
            LiteralValue::Array(..) => LiteralParse::Undecidable,
            LiteralValue::String(_)
            | LiteralValue::StringWithCast(..)
            | LiteralValue::Integer(_)
            | LiteralValue::Float(_)
            | LiteralValue::Boolean(_) => {
                literal_parse(kind, literal).map_or(LiteralParse::Undecidable, LiteralParse::Value)
            }
        };
        Self { kind, parsed }
    }

    /// `row_text op literal` for a present (non-NULL) row value; `None` when
    /// the comparison is undecidable (the caller picks the conservative side).
    pub fn row_compare(&self, row_text: &str, op: BinaryOp) -> Option<bool> {
        let filter = match &self.parsed {
            LiteralParse::Never => return Some(false),
            LiteralParse::Undecidable => return None,
            LiteralParse::Value(value) => value,
        };
        let row = text_parse(self.kind, row_text)?;
        let ordering = row.cmp_same_kind(filter)?;
        Some(match op {
            BinaryOp::Equal => ordering == Ordering::Equal,
            BinaryOp::NotEqual => ordering != Ordering::Equal,
            BinaryOp::LessThan => ordering == Ordering::Less,
            BinaryOp::LessThanOrEqual => ordering != Ordering::Greater,
            BinaryOp::GreaterThan => ordering == Ordering::Greater,
            BinaryOp::GreaterThanOrEqual => ordering != Ordering::Less,
            BinaryOp::And
            | BinaryOp::Or
            | BinaryOp::Like
            | BinaryOp::ILike
            | BinaryOp::NotLike
            | BinaryOp::NotILike => return None,
        })
    }
}

/// The index-key form of a literal for a column of `kind` (PGC-478):
/// integers and decimals as numbers, days and microseconds as `Float`, text
/// normalised, bools as `Boolean`. Numbers key as `ValueKey::Num` either way,
/// so a literal and a row's wire text that mean the same value share one
/// bucket. `None` when the literal does not parse as the kind.
pub fn literal_index_key(kind: ValueKind, literal: &LiteralValue) -> Option<LiteralValue> {
    literal_parse(kind, literal).and_then(TypedValue::into_index_key)
}

/// The index-key form of a row's wire text for a column of `kind`; the same
/// mapping as [`literal_index_key`], so the point probe lands in the entry's
/// bucket.
pub fn wire_text_index_key(kind: ValueKind, text: &str) -> Option<LiteralValue> {
    text_parse(kind, text).and_then(TypedValue::into_index_key)
}

impl TypedValue {
    /// Unix epoch as a civil date, the origin of the day count key.
    const EPOCH_DATE: Date = Date::constant(1970, 1, 1);

    fn into_index_key(self) -> Option<LiteralValue> {
        let float = |x: f64| NotNan::new(x).ok().map(LiteralValue::Float);
        match self {
            // Every number keys as `Float` so an entry's literal and a row's
            // text share one `ValueKey::Num` bucket whatever their spelling;
            // past 2^53 the key is approximate, which can only over-return.
            #[allow(clippy::cast_precision_loss)]
            Self::Integer(n) => float(n as f64),
            Self::Decimal(decimal) => float(decimal.to_f64()),
            Self::Bool(b) => Some(LiteralValue::Boolean(b)),
            Self::Date(date) => float(f64::from(date.since(Self::EPOCH_DATE).ok()?.get_days())),
            #[allow(clippy::cast_precision_loss)]
            Self::Timestamp(civil) => {
                let micros = civil
                    .to_zoned(TimeZone::UTC)
                    .ok()?
                    .timestamp()
                    .as_microsecond();
                float(micros as f64)
            }
            #[allow(clippy::cast_precision_loss)]
            Self::TimestampTz(instant) => float(instant.as_microsecond() as f64),
            Self::Text(text) => Some(LiteralValue::String(text.into())),
            Self::Infinite { negative } => float(if negative {
                f64::NEG_INFINITY
            } else {
                f64::INFINITY
            }),
            // Above every finite key, like the positive infinity it shares a
            // bucket with; the comparator tells them apart.
            Self::NaN => float(f64::INFINITY),
        }
    }
}

/// The out-of-range words PostgreSQL prints and accepts for `kind`.
fn special_parse(kind: ValueKind, text: &str) -> Option<TypedValue> {
    let word = text.trim().to_ascii_lowercase();
    match kind {
        ValueKind::Decimal => match word.as_str() {
            "nan" => Some(TypedValue::NaN),
            "infinity" | "+infinity" | "inf" | "+inf" => {
                Some(TypedValue::Infinite { negative: false })
            }
            "-infinity" | "-inf" => Some(TypedValue::Infinite { negative: true }),
            _ => None,
        },
        ValueKind::Date | ValueKind::Timestamp | ValueKind::TimestampTz => match word.as_str() {
            "infinity" | "+infinity" => Some(TypedValue::Infinite { negative: false }),
            "-infinity" => Some(TypedValue::Infinite { negative: true }),
            _ => None,
        },
        ValueKind::Integer
        | ValueKind::Bool
        | ValueKind::Text
        | ValueKind::Bpchar
        | ValueKind::Uuid
        | ValueKind::Enum
        | ValueKind::Unsupported => None,
    }
}

/// The canonical spelling of a date-time literal for a column of `kind`
/// under the session's settings: ISO date, ISO date-time, or an instant with
/// an explicit `+00` offset — unambiguous under any session. `None` when the
/// text is not a spelling this parser handles (a month name, `infinity`, a
/// zone-less `timestamptz` under an unknown zone), or `kind` is not a
/// date-time kind.
pub fn literal_canonical_text(
    kind: ValueKind,
    text: &str,
    settings: &LiteralSettings,
) -> Option<String> {
    if matches!(
        kind,
        ValueKind::Date | ValueKind::Timestamp | ValueKind::TimestampTz
    ) && let Some(TypedValue::Infinite { negative }) = special_parse(kind, text)
    {
        return Some(if negative { "-infinity" } else { "infinity" }.to_owned());
    }
    match kind {
        ValueKind::Date => {
            let (date, _, _) = datetime_split_ordered(text, settings.date_order)?;
            Some(date.to_string())
        }
        ValueKind::Timestamp => {
            let (date, time, _) = datetime_split_ordered(text, settings.date_order)?;
            let civil = civil_parse_date(date, time)?;
            Some(civil_text(civil))
        }
        ValueKind::TimestampTz => {
            let (date, time, offset) = datetime_split_ordered(text, settings.date_order)?;
            let civil = civil_parse_date(date, time)?;
            let instant = match offset {
                Some(offset) => civil
                    .to_zoned(TimeZone::fixed(offset_parse(offset)?))
                    .ok()?
                    .timestamp(),
                None => civil.to_zoned(settings.zone()?).ok()?.timestamp(),
            };
            Some(format!(
                "{}+00",
                civil_text(instant.to_zoned(TimeZone::UTC).datetime())
            ))
        }
        ValueKind::Integer
        | ValueKind::Decimal
        | ValueKind::Bool
        | ValueKind::Text
        | ValueKind::Bpchar
        | ValueKind::Uuid
        | ValueKind::Enum
        | ValueKind::Unsupported => None,
    }
}

/// `YYYY-MM-DD HH:MM:SS[.ffffff]` as PostgreSQL prints a `timestamp`.
fn civil_text(civil: DateTime) -> String {
    let base = format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        civil.year(),
        civil.month(),
        civil.day(),
        civil.hour(),
        civil.minute(),
        civil.second()
    );
    let micros = civil.subsec_nanosecond() / 1000;
    if micros == 0 {
        base
    } else {
        format!("{base}.{}", format!("{micros:06}").trim_end_matches('0'))
    }
}

/// Like [`datetime_split`], also accepting a numeric date in the session's
/// field order (`01/15/2024`, `15.01.2024`, `15-01-2024`); a four-digit first
/// field is year-first whatever the order, as in PostgreSQL. The date part
/// is returned normalised to `YYYY-MM-DD`.
fn datetime_split_ordered(
    text: &str,
    order: DateOrder,
) -> Option<(Date, Option<&str>, Option<&str>)> {
    if let Some((date, time, offset)) = datetime_split(text) {
        return Some((date.parse().ok()?, time, offset));
    }
    let text = text.trim();
    let (date_text, rest) = match text.find([' ', 'T']) {
        Some(i) => text.split_at_checked(i)?,
        None => (text, ""),
    };
    let date = numeric_date_parse(date_text, order)?;
    if rest.is_empty() {
        return Some((date, None, None));
    }
    let (time, offset) = time_offset_split(rest)?;
    Some((date, time, offset))
}

/// A numeric date in the session's field order; a four-digit first field is
/// year-first whatever the order, as in PostgreSQL.
fn numeric_date_parse(date_text: &str, order: DateOrder) -> Option<Date> {
    let parts: Vec<&str> = date_text.split(['/', '.', '-']).collect();
    let [a, b, c] = parts.as_slice() else {
        return None;
    };
    if !parts.iter().all(|p| !p.is_empty() && digits_only(p)) {
        return None;
    }
    let (year, month, day) = if a.len() == 4 {
        (a, b, c)
    } else {
        match order {
            DateOrder::MonthDayYear => (c, a, b),
            DateOrder::DayMonthYear => (c, b, a),
            DateOrder::YearMonthDay => (a, b, c),
        }
    };
    Date::new(
        year_field_parse(year)?,
        month.parse().ok()?,
        day.parse().ok()?,
    )
    .ok()
}

/// A numeric year field; two digits follow PostgreSQL's rule: below 70 is
/// 20xx, else 19xx.
fn year_field_parse(field: &str) -> Option<i16> {
    let year: i16 = field.parse().ok()?;
    Some(if field.len() > 2 {
        year
    } else if year < 70 {
        2000 + year
    } else {
        1900 + year
    })
}

fn civil_parse_date(date: Date, time: Option<&str>) -> Option<DateTime> {
    match time {
        Some(time) => Some(date.to_datetime(time_parse(time)?)),
        None => Some(date.to_datetime(jiff::civil::Time::midnight())),
    }
}

/// Parse a predicate literal as `kind`. A typed literal (`'…'::type`) must
/// name the same kind as the column.
fn literal_parse(kind: ValueKind, literal: &LiteralValue) -> Option<TypedValue> {
    match literal {
        LiteralValue::String(s) => text_parse(kind, s),
        LiteralValue::StringWithCast(s, cast) => {
            if ValueKind::of_cast_name(cast) != kind {
                return None;
            }
            text_parse(kind, s)
        }
        // A bare number or boolean only ever meets a numeric or boolean
        // column at origin; its text parses as that kind (an integer column
        // against a fractional literal compares as numeric, as at origin).
        LiteralValue::Integer(n) if matches!(kind, ValueKind::Integer | ValueKind::Decimal) => {
            Some(TypedValue::Integer(i128::from(*n)))
        }
        LiteralValue::Boolean(b) if kind == ValueKind::Bool => Some(TypedValue::Bool(*b)),
        LiteralValue::Integer(_) | LiteralValue::Float(_) | LiteralValue::Boolean(_) => {
            if !matches!(
                kind,
                ValueKind::Integer | ValueKind::Decimal | ValueKind::Bool
            ) {
                return None;
            }
            let mut text = String::new();
            literal.deparse(&mut text);
            text_parse(kind, &text)
        }
        LiteralValue::Null
        | LiteralValue::NullWithCast(_)
        | LiteralValue::Parameter(_)
        | LiteralValue::Array(..) => None,
    }
}

/// Parse text (a literal's or a row's) as `kind`.
fn text_parse(kind: ValueKind, text: &str) -> Option<TypedValue> {
    match kind {
        ValueKind::Integer => match text.trim().parse::<i128>() {
            Ok(n) => Some(TypedValue::Integer(n)),
            // `'10.0'` against an integer column compares as numeric.
            Err(_) => Decimal::parse(text).map(TypedValue::Decimal),
        },
        ValueKind::Decimal => {
            special_parse(kind, text).or_else(|| Decimal::parse(text).map(TypedValue::Decimal))
        }
        ValueKind::Bool => bool_literal_parse(text).map(TypedValue::Bool),
        ValueKind::Date => {
            special_parse(kind, text).or_else(|| date_parse(text).map(TypedValue::Date))
        }
        ValueKind::Timestamp => {
            special_parse(kind, text).or_else(|| timestamp_parse(text).map(TypedValue::Timestamp))
        }
        ValueKind::TimestampTz => special_parse(kind, text)
            .or_else(|| timestamptz_parse(text).map(TypedValue::TimestampTz)),
        ValueKind::Text => Some(TypedValue::Text(text.to_owned())),
        ValueKind::Bpchar => Some(TypedValue::Text(text.trim_end_matches(' ').to_owned())),
        ValueKind::Uuid => Some(TypedValue::Text(text.trim().to_ascii_lowercase())),
        ValueKind::Enum => Some(TypedValue::Text(text.to_owned())),
        ValueKind::Unsupported => None,
    }
}

/// Split ISO-shaped date-time text into `(date, time, offset)` parts:
/// `YYYY-MM-DD[{ |T}HH:MM[:SS[.ffffff]]][±HH[:MM[:SS]] | Z]`. Anything else
/// (`BC`, `infinity`, month names, day-first orders) is `None`.
fn datetime_split(text: &str) -> Option<(&str, Option<&str>, Option<&str>)> {
    let text = text.trim();
    let (date, rest) = text.split_at_checked(10)?;
    if !date_shape_ok(date) {
        return None;
    }
    if rest.is_empty() {
        return Some((date, None, None));
    }
    let (time, offset) = time_offset_split(rest)?;
    Some((date, time, offset))
}

/// The `{ |T}[HH:MM[:SS[.ffffff]]][±HH[:MM[:SS]] | Z]` tail after a date as
/// `(time, offset)`; `None` for any other shape, a bare separator included.
fn time_offset_split(rest: &str) -> Option<(Option<&str>, Option<&str>)> {
    let rest = rest.strip_prefix([' ', 'T'])?;
    let (time, offset) = match rest.find(['+', '-', 'Z', 'z']) {
        Some(i) => {
            let (time, offset) = rest.split_at_checked(i)?;
            (time, Some(offset))
        }
        None => (rest, None),
    };
    let time = time.trim_end();
    if time.is_empty() && offset.is_none() {
        return None;
    }
    Some(((!time.is_empty()).then_some(time), offset))
}

fn date_shape_ok(date: &str) -> bool {
    let Ok([y0, y1, y2, y3, dash1, m0, m1, dash2, d0, d1]) = <[u8; 10]>::try_from(date.as_bytes())
    else {
        return false;
    };
    dash1 == b'-'
        && dash2 == b'-'
        && [y0, y1, y2, y3, m0, m1, d0, d1]
            .iter()
            .all(u8::is_ascii_digit)
}

/// PostgreSQL prints a year before 1 AD with a ` BC` suffix, after any
/// offset; 1 BC is year 0.
fn bc_strip(text: &str) -> (&str, bool) {
    let trimmed = text.trim_end();
    match trimmed
        .strip_suffix(" BC")
        .or_else(|| trimmed.strip_suffix(" bc"))
    {
        Some(rest) => (rest, true),
        None => (text, false),
    }
}

fn date_bc_adjust(date: Date, bc: bool) -> Option<Date> {
    if !bc {
        return Some(date);
    }
    Date::new(1 - date.year(), date.month(), date.day()).ok()
}

fn civil_bc_adjust(civil: DateTime, bc: bool) -> Option<DateTime> {
    Some(date_bc_adjust(civil.date(), bc)?.to_datetime(civil.time()))
}

fn date_parse(text: &str) -> Option<Date> {
    let (text, bc) = bc_strip(text);
    let (date, _, _) = datetime_split(text)?;
    date_bc_adjust(date.parse().ok()?, bc)
}

/// `HH:MM[:SS[.ffffff]]` as a jiff civil time, via the ISO string form.
fn time_parse(time: &str) -> Option<jiff::civil::Time> {
    let padded = match time.matches(':').count() {
        1 => format!("{time}:00"),
        2 => time.to_owned(),
        _ => return None,
    };
    padded.parse().ok()
}

fn civil_parse(date: &str, time: Option<&str>) -> Option<DateTime> {
    let date: Date = date.parse().ok()?;
    match time {
        Some(time) => Some(date.to_datetime(time_parse(time)?)),
        None => Some(date.to_datetime(jiff::civil::Time::midnight())),
    }
}

fn timestamp_parse(text: &str) -> Option<DateTime> {
    // An offset on a `timestamp without time zone` input is ignored, as
    // PostgreSQL ignores it.
    let (text, bc) = bc_strip(text);
    let (date, time, _) = datetime_split(text)?;
    civil_bc_adjust(civil_parse(date, time)?, bc)
}

/// A `timestamptz`: an instant, so the text needs an offset or `Z`; without
/// one it is a civil time in some session's zone, which this module does not
/// know (see the module doc).
fn timestamptz_parse(text: &str) -> Option<Timestamp> {
    let (text, bc) = bc_strip(text);
    let (date, time, offset) = datetime_split(text)?;
    let civil = civil_bc_adjust(civil_parse(date, time)?, bc)?;
    let offset = offset_parse(offset?)?;
    civil
        .to_zoned(TimeZone::fixed(offset))
        .ok()
        .map(|z: Zoned| z.timestamp())
}

/// `Z`, `±HH`, `±HHMM`, `±HH:MM`, `±HH:MM:SS` as a fixed offset.
fn offset_parse(text: &str) -> Option<jiff::tz::Offset> {
    if matches!(text, "Z" | "z") {
        return Some(jiff::tz::Offset::UTC);
    }
    let (sign, rest) = match sign_split(text) {
        (true, rest) => (-1i32, rest),
        (false, rest) if rest.len() < text.len() => (1i32, rest),
        (false, _) => return None,
    };
    let parts: Vec<&str> = match (rest.contains(':'), rest.len()) {
        (true, _) => rest.split(':').collect(),
        (false, 2) => vec![rest],
        (false, 4) => {
            let (hours, minutes) = rest.split_at_checked(2)?;
            vec![hours, minutes]
        }
        (false, _) => return None,
    };
    if parts.len() > 3 {
        return None;
    }
    let mut seconds = 0i32;
    for (part, scale) in parts.iter().zip([3600, 60, 1]) {
        seconds += two_digits_parse(part)? * scale;
    }
    jiff::tz::Offset::from_seconds(sign * seconds).ok()
}

fn two_digits_parse(part: &str) -> Option<i32> {
    (part.len() == 2).then(|| part.parse().ok()).flatten()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::ast::BinaryOp::{Equal, GreaterThan, LessThan, NotEqual};

    /// The pre-parsed literal decides exactly as the one-shot comparator for
    /// every literal shape, including the undecidable and never-matching ones.
    #[test]
    fn test_typed_literal_matches_one_shot_compare() {
        let cases: &[(Type, LiteralValue, &str, BinaryOp)] = &[
            (Type::INT4, LiteralValue::Integer(10), "10", Equal),
            (Type::INT4, LiteralValue::String("10.0".into()), "10", Equal),
            (Type::NUMERIC, LiteralValue::Integer(10), "10.50", LessThan),
            (
                Type::NUMERIC,
                LiteralValue::Float(NotNan::new(1.5).expect("finite")),
                "1.50",
                Equal,
            ),
            (Type::BOOL, LiteralValue::Boolean(true), "t", Equal),
            (Type::BOOL, LiteralValue::Integer(1), "t", Equal),
            (
                Type::TIMESTAMP,
                LiteralValue::String("2024-01-15 10:00".into()),
                "2024-01-15 10:00:00",
                Equal,
            ),
            (
                Type::TEXT,
                LiteralValue::String("b".into()),
                "a",
                GreaterThan,
            ),
            (Type::TEXT, LiteralValue::Integer(1), "1", Equal),
            (Type::INT4, LiteralValue::Null, "1", Equal),
            (
                Type::INT4,
                LiteralValue::Parameter("$1".into()),
                "1",
                NotEqual,
            ),
            (Type::INT4, LiteralValue::String("abc".into()), "1", Equal),
            (Type::INT4, LiteralValue::Integer(1), "1", BinaryOp::Like),
        ];
        for (ty, literal, row, op) in cases {
            assert_eq!(
                TypedLiteral::new(ty, literal).row_compare(row, *op),
                typed_compare(ty, literal, row, *op),
                "{ty:?} {literal:?} {row} {op:?}"
            );
        }
    }

    fn text(s: &str) -> LiteralValue {
        LiteralValue::String(s.into())
    }

    fn cmp(ty: &Type, literal: LiteralValue, row: &str, op: BinaryOp) -> Option<bool> {
        typed_compare(ty, &literal, row, op)
    }

    #[test]
    fn test_integer_column_parses_numeric_spellings() {
        assert_eq!(cmp(&Type::INT4, text("0042"), "42", Equal), Some(true));
        assert_eq!(
            cmp(&Type::INT8, LiteralValue::Integer(9), "10", GreaterThan),
            Some(true)
        );
        assert_eq!(
            cmp(&Type::INT8, LiteralValue::Integer(9), "10", LessThan),
            Some(false)
        );
        assert_eq!(cmp(&Type::INT4, text("10.0"), "10", Equal), Some(true));
        assert_eq!(cmp(&Type::INT4, text("abc"), "10", Equal), None);
        assert!(!literal_locally_comparable(
            &Type::INT4,
            &text("abc"),
            Equal
        ));
    }

    #[test]
    fn test_decimal_is_exact() {
        assert_eq!(
            cmp(&Type::NUMERIC, text("10.50"), "10.5", Equal),
            Some(true)
        );
        assert_eq!(
            cmp(&Type::NUMERIC, text("9.5"), "10", GreaterThan),
            Some(true)
        );
        assert_eq!(cmp(&Type::INT4, text("9.5"), "10", GreaterThan), Some(true));
        assert_eq!(cmp(&Type::FLOAT8, text("1e3"), "1000", Equal), Some(true));
        assert_eq!(
            cmp(
                &Type::NUMERIC,
                text("12345678901234567890.10"),
                "12345678901234567890.1",
                Equal
            ),
            Some(true)
        );
        assert_eq!(
            cmp(
                &Type::NUMERIC,
                text("12345678901234567890.1"),
                "12345678901234567890.2",
                Equal
            ),
            Some(false)
        );
        assert_eq!(cmp(&Type::NUMERIC, text("-0.0"), "0", Equal), Some(true));
        assert_eq!(cmp(&Type::NUMERIC, text("-1"), "-2", LessThan), Some(true));
    }

    #[test]
    fn test_date_and_timestamp_spellings_compare_as_values() {
        assert_eq!(
            cmp(&Type::DATE, text("2024-01-15"), "2024-01-15", Equal),
            Some(true)
        );
        assert_eq!(
            cmp(&Type::DATE, text("2024-01-15 10:00"), "2024-01-15", Equal),
            Some(true)
        );
        assert_eq!(
            cmp(&Type::DATE, text("01/15/2024"), "2024-01-15", Equal),
            None
        );
        assert_eq!(
            cmp(
                &Type::TIMESTAMP,
                text("2024-01-15 10:00"),
                "2024-01-15 10:00:00",
                Equal
            ),
            Some(true)
        );
        assert_eq!(
            cmp(
                &Type::TIMESTAMP,
                text("2024-01-15T10:00:00.5"),
                "2024-01-15 10:00:00.5",
                Equal
            ),
            Some(true)
        );
        assert_eq!(
            cmp(
                &Type::TIMESTAMP,
                text("2024-01-15"),
                "2024-01-15 00:00:00",
                Equal
            ),
            Some(true)
        );
        assert_eq!(
            cmp(
                &Type::TIMESTAMP,
                text("2024-01-16"),
                "2024-01-15 23:59:59",
                GreaterThan
            ),
            Some(false)
        );
        assert_eq!(
            cmp(
                &Type::TIMESTAMP,
                text("Jan 15 2024"),
                "2024-01-15 00:00:00",
                Equal
            ),
            None
        );
    }

    #[test]
    fn test_timestamptz_compares_instants_across_offsets() {
        assert_eq!(
            cmp(
                &Type::TIMESTAMPTZ,
                text("2024-01-15 10:00:00+00"),
                "2024-01-15 05:00:00-05",
                Equal
            ),
            Some(true)
        );
        assert_eq!(
            cmp(
                &Type::TIMESTAMPTZ,
                text("2024-01-15T10:00Z"),
                "2024-01-15 15:30:00+05:30",
                Equal
            ),
            Some(true)
        );
        assert_eq!(
            cmp(
                &Type::TIMESTAMPTZ,
                text("2024-01-15 10:00:00+0530"),
                "2024-01-15 04:30:00+00",
                Equal
            ),
            Some(true)
        );
        // Zone-less literal: not an instant without the session zone, which
        // canonicalisation supplies before a literal reaches here.
        assert_eq!(
            cmp(
                &Type::TIMESTAMPTZ,
                text("2024-01-15 10:00"),
                "2024-01-15 10:00:00+00",
                Equal
            ),
            None
        );
        assert!(!literal_locally_comparable(
            &Type::TIMESTAMPTZ,
            &text("2024-01-15"),
            Equal
        ));
        // Canonicalised under Asia/Tokyo, the same literal is an instant.
        assert_eq!(
            cmp(
                &Type::TIMESTAMPTZ,
                text("2024-01-15 01:00:00+00"),
                "2024-01-15 01:00:00+00",
                Equal
            ),
            Some(true)
        );
    }

    #[test]
    fn test_typed_literal_must_match_the_column_kind() {
        let ts =
            LiteralValue::StringWithCast("2024-01-15 10:00:00+00".into(), "timestamptz".into());
        assert_eq!(
            cmp(
                &Type::TIMESTAMPTZ,
                ts.clone(),
                "2024-01-15 10:00:00+00",
                Equal
            ),
            Some(true)
        );
        assert_eq!(
            cmp(&Type::TIMESTAMP, ts, "2024-01-15 10:00:00", Equal),
            None
        );
    }

    #[test]
    fn test_text_family_rules() {
        assert_eq!(cmp(&Type::TEXT, text("abc"), "abc", Equal), Some(true));
        assert_eq!(
            cmp(&Type::TEXT, text("abc"), "abd", GreaterThan),
            Some(true)
        );
        assert_eq!(cmp(&Type::BPCHAR, text("abc"), "abc  ", Equal), Some(true));
        assert_eq!(
            cmp(
                &Type::UUID,
                text("A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11"),
                "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
                Equal
            ),
            Some(true)
        );
        assert_eq!(cmp(&Type::BOOL, text("yes"), "t", Equal), Some(true));
        assert_eq!(
            cmp(&Type::BOOL, LiteralValue::Integer(0), "f", Equal),
            Some(true)
        );
    }

    #[test]
    fn test_canonical_text_under_settings() {
        let mdy = LiteralSettings::from_texts("Asia/Tokyo", "ISO, MDY", "postgres", "Default");
        let dmy = LiteralSettings::from_texts("UTC", "ISO, DMY", "postgres", "Default");
        let canon = |kind, text: &str, s: &LiteralSettings| literal_canonical_text(kind, text, s);
        assert_eq!(
            canon(ValueKind::Date, "01/02/2024", &mdy).as_deref(),
            Some("2024-01-02")
        );
        assert_eq!(
            canon(ValueKind::Date, "01/02/2024", &dmy).as_deref(),
            Some("2024-02-01")
        );
        assert_eq!(
            canon(ValueKind::Date, "2024-01-02", &dmy).as_deref(),
            Some("2024-01-02")
        );
        assert_eq!(
            canon(ValueKind::Date, "2024/01/02", &dmy).as_deref(),
            Some("2024-01-02")
        );
        assert_eq!(canon(ValueKind::Date, "Jan 2 2024", &mdy), None);
        assert_eq!(
            canon(ValueKind::Timestamp, "2024-01-02 10:00", &mdy).as_deref(),
            Some("2024-01-02 10:00:00")
        );
        assert_eq!(
            canon(ValueKind::Timestamp, "2024-01-02T10:00:00.250", &mdy).as_deref(),
            Some("2024-01-02 10:00:00.25")
        );
        // Zone-less timestamptz: the session zone decides the instant.
        assert_eq!(
            canon(ValueKind::TimestampTz, "2024-01-02 10:00", &mdy).as_deref(),
            Some("2024-01-02 01:00:00+00")
        );
        assert_eq!(
            canon(ValueKind::TimestampTz, "2024-01-02 10:00+05:30", &dmy).as_deref(),
            Some("2024-01-02 04:30:00+00")
        );
        let unknown =
            LiteralSettings::from_texts("Nowhere/Land", "ISO, MDY", "postgres", "Default");
        assert_eq!(
            canon(ValueKind::TimestampTz, "2024-01-02 10:00", &unknown),
            None
        );
        assert_eq!(
            canon(ValueKind::TimestampTz, "2024-01-02 10:00Z", &unknown).as_deref(),
            Some("2024-01-02 10:00:00+00")
        );
        assert_eq!(canon(ValueKind::Integer, "1", &mdy), None);
    }

    #[test]
    fn test_unsupported_types_and_null_literals() {
        assert!(!literal_locally_comparable(
            &Type::JSONB,
            &text("{}"),
            Equal
        ));
        assert_eq!(cmp(&Type::JSONB, text("{}"), "{}", Equal), None);
        assert_eq!(
            cmp(&Type::INT4, LiteralValue::Null, "1", Equal),
            Some(false)
        );
        assert!(literal_locally_comparable(
            &Type::INT4,
            &LiteralValue::Null,
            Equal
        ));
        assert_eq!(
            cmp(
                &Type::INT4,
                LiteralValue::Parameter("$1".into()),
                "1",
                Equal
            ),
            Some(false)
        );
        assert_eq!(cmp(&Type::TEXT, text("a"), "b", NotEqual), Some(true));
    }

    /// Row text beyond the ISO shapes that the typed parsers must still
    /// order: the infinities, `NaN`, and BC dates. `None` here drops a row
    /// from a cached result it belongs to.
    #[test]
    fn test_infinite_nan_and_bc_row_text_compare() {
        let bound = text("2024-06-01 00:00+00");
        assert_eq!(
            cmp(&Type::TIMESTAMPTZ, bound.clone(), "infinity", GreaterThan),
            Some(true)
        );
        assert_eq!(
            cmp(&Type::TIMESTAMPTZ, bound.clone(), "-infinity", LessThan),
            Some(true)
        );
        assert_eq!(
            cmp(&Type::TIMESTAMPTZ, bound.clone(), "-infinity", GreaterThan),
            Some(false)
        );
        assert_eq!(
            cmp(
                &Type::TIMESTAMP,
                text("2024-06-01"),
                "infinity",
                GreaterThan
            ),
            Some(true)
        );
        assert_eq!(
            cmp(&Type::DATE, text("infinity"), "infinity", Equal),
            Some(true)
        );
        assert_eq!(
            cmp(&Type::DATE, text("2024-06-01"), "0100-01-01 BC", LessThan),
            Some(true)
        );
        assert_eq!(
            cmp(
                &Type::TIMESTAMPTZ,
                bound,
                "0100-01-01 00:00:00+00 BC",
                LessThan
            ),
            Some(true)
        );
        assert_eq!(
            cmp(&Type::NUMERIC, text("5"), "NaN", GreaterThan),
            Some(true)
        );
        assert_eq!(cmp(&Type::NUMERIC, text("NaN"), "NaN", Equal), Some(true));
        assert_eq!(
            cmp(&Type::NUMERIC, text("5"), "Infinity", GreaterThan),
            Some(true)
        );
        assert_eq!(
            cmp(&Type::NUMERIC, text("Infinity"), "NaN", GreaterThan),
            Some(true)
        );
        assert_eq!(
            cmp(&Type::FLOAT8, text("5"), "-Infinity", LessThan),
            Some(true)
        );
        assert!(wire_text_index_key(ValueKind::TimestampTz, "infinity").is_some());
        assert!(wire_text_index_key(ValueKind::Decimal, "NaN").is_some());
        assert_eq!(
            literal_canonical_text(ValueKind::Date, "Infinity", &LiteralSettings::default())
                .as_deref(),
            Some("infinity")
        );
    }

    /// A two-digit year follows PostgreSQL's rule: below 70 is 20xx, else 19xx.
    #[test]
    fn test_two_digit_year_expands_as_postgres() {
        let mdy = LiteralSettings::default();
        let ymd = LiteralSettings::from_texts("UTC", "ISO, YMD", "postgres", "Default");
        let canon =
            |text: &str, s: &LiteralSettings| literal_canonical_text(ValueKind::Date, text, s);
        assert_eq!(canon("01/02/24", &mdy).as_deref(), Some("2024-01-02"));
        assert_eq!(canon("01/02/70", &mdy).as_deref(), Some("1970-01-02"));
        assert_eq!(canon("24/01/02", &ymd).as_deref(), Some("2024-01-02"));
    }
}
