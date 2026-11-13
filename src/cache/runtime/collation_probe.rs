//! Startup check that the cache host orders text as origin does (PGC-587).
//!
//! Mirroring origin's locale onto the cache database (`database_locale`)
//! names the same collation on both sides, but the ordering comes from the
//! host's libc or ICU build, and those disagree across libc families (musl
//! orders by code point), across the glibc 2.28 boundary (RDS pins the older
//! order on purpose) and between glibc and ICU on punctuation. A served
//! `ORDER BY` in the wrong order is a correctness failure, so a mismatch
//! refuses startup instead of being logged.

use postgres_protocol::escape::escape_identifier;
use rootcause::Report;
use tokio_postgres::Client;
use tracing::warn;

use super::database_locale::DatabaseLocale;
use crate::cache::{CacheError, CacheResult, MapIntoReport, ReportExt, SetupError};
use crate::catalog::CollationName;

/// Strings whose relative order separates the known collation families:
/// case pairs, digits against letters, leading and embedded spaces, hyphens,
/// underscores and punctuation (ignored at the first level by glibc before
/// 2.28 and by ICU, not by glibc 2.28+), accented Latin, the `ß` expansion,
/// and a few non-Latin scripts. Sent as UTF-8 text: pgcache supports UTF8
/// origins only.
const SAMPLE: &[&str] = &[
    "a", "A", "b", "B", "z", "Z", "ä", "Ä", "é", "e", "E", "É", "ö", "Ö", "ø", "o", "ß", "ss",
    "st", "œ", "oe", "1", "10", "2", "a1", "a10", "a2", "A1", " a", "a b", "ab", "a-b", "a_b",
    "a.b", "a,b", "-a", "_a", "(a)", "[a]", "@a", "#a", "Ω", "ω", "Ж", "ж", "日", "本",
];

/// `SAMPLE` as the database default collation orders it.
async fn sample_sorted(client: &Client) -> CacheResult<Vec<String>> {
    let rows = client
        .query(
            "SELECT s FROM unnest($1::text[]) AS t(s) ORDER BY s",
            &[&SAMPLE],
        )
        .await
        .map_into_report::<CacheError>()
        .attach_loc("sorting the collation sample")?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

/// `SAMPLE` as the named collation orders it, ties (a nondeterministic
/// collation has them) broken by bytes so two servers agree on them. Errors
/// when the database has no such collation.
pub(crate) async fn sample_sorted_under(
    client: &Client,
    collation: &CollationName,
) -> CacheResult<Vec<String>> {
    let sql = format!(
        "SELECT s FROM unnest($1::text[]) AS t(s) ORDER BY s COLLATE {}.{}, s COLLATE \"C\"",
        escape_identifier(&collation.schema),
        escape_identifier(&collation.name),
    );
    let rows = client
        .query(&sql, &[&SAMPLE])
        .await
        .map_into_report::<CacheError>()?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

/// The first position where the two orders disagree, with the string each
/// side put there.
fn first_difference<'a>(
    origin: &'a [String],
    cache: &'a [String],
) -> Option<(usize, &'a str, &'a str)> {
    origin
        .iter()
        .zip(cache)
        .enumerate()
        .find(|(_, (o, c))| o != c)
        .map(|(i, (o, c))| (i, o.as_str(), c.as_str()))
}

/// Test-only (fault-injection feature): `PGCACHE_FAULT_COLLATION_MISMATCH`
/// set reverses the cache side's order so the refusal path can be exercised
/// on one host.
#[cfg(feature = "fault-injection")]
fn fault_apply(cache: &mut [String]) {
    if std::env::var_os("PGCACHE_FAULT_COLLATION_MISMATCH").is_some() {
        cache.reverse();
    }
}

#[cfg(not(feature = "fault-injection"))]
fn fault_apply(_cache: &mut [String]) {}

/// Refuse to start unless the cache database runs origin's locale provider
/// and `cache` sorts the sample exactly as `origin` does. A differing
/// collation version with an identical order is only a warning: glibc stamps
/// every release, and most releases change nothing we sort.
pub(super) async fn collation_probe(
    origin: &Client,
    cache: &Client,
    origin_locale: &DatabaseLocale,
    cache_locale: &DatabaseLocale,
) -> CacheResult<()> {
    if origin_locale.provider != cache_locale.provider {
        return Err(Report::from(CacheError::from(
            SetupError::LocaleProviderMismatch {
                origin: origin_locale.provider.as_str(),
                cache: cache_locale.provider.as_str(),
            },
        )));
    }

    let origin_order = sample_sorted(origin)
        .await
        .attach_loc("sorting the collation sample on origin")?;
    let mut cache_order = sample_sorted(cache)
        .await
        .attach_loc("sorting the collation sample on the cache database")?;
    fault_apply(&mut cache_order);

    if let Some((position, origin_string, cache_string)) =
        first_difference(&origin_order, &cache_order)
    {
        return Err(Report::from(CacheError::from(
            SetupError::CollationMismatch {
                position,
                origin_string: origin_string.to_owned(),
                cache_string: cache_string.to_owned(),
                origin_locale: origin_locale.describe(),
                cache_locale: cache_locale.describe(),
            },
        )));
    }
    if origin_locale.collation_version != cache_locale.collation_version {
        warn!(
            "collation version differs from origin (origin {:?}, cache {:?}); the probe sample \
             sorts identically, so serving continues",
            origin_locale.collation_version, cache_locale.collation_version
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn test_sample_has_no_duplicates() {
        let distinct: HashSet<&str> = SAMPLE.iter().copied().collect();
        assert_eq!(distinct.len(), SAMPLE.len());
    }

    #[test]
    fn test_first_difference_names_the_position_and_both_strings() {
        let origin: Vec<String> = ["a", "A", "b"].map(String::from).to_vec();
        let cache: Vec<String> = ["A", "a", "b"].map(String::from).to_vec();
        assert_eq!(first_difference(&origin, &cache), Some((0, "a", "A")));
        assert_eq!(first_difference(&origin, &origin), None);
    }

    /// Byte order (musl, or `C`) against a case-interleaving order: the
    /// sample must tell them apart.
    #[test]
    fn test_sample_separates_code_point_order_from_case_interleaved_order() {
        let mut bytes: Vec<String> = SAMPLE.iter().map(|s| (*s).to_owned()).collect();
        bytes.sort();
        let mut case_folded: Vec<String> = bytes.clone();
        case_folded.sort_by_key(|s| (s.to_lowercase(), s.clone()));
        assert!(first_difference(&bytes, &case_folded).is_some());
    }
}
