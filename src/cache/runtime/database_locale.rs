//! Origin's database-level locale, mirrored onto the cache database so a
//! text `ORDER BY`, `min`/`max` or range predicate served from cache sorts as
//! origin sorts it (PGC-587). A cache database created with the template's
//! default collates under whatever the cache host's `initdb` chose, which is
//! not what origin's rows were loaded under.

use postgres_protocol::escape::escape_literal;
use tokio_postgres::Client;

use crate::cache::{CacheError, CacheResult, MapIntoReport, ReportExt};

/// Which library orders text under the database default collation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LocaleProvider {
    Libc,
    Icu,
    Builtin,
}

impl LocaleProvider {
    fn from_catalog(code: &str) -> Self {
        match code {
            "i" => Self::Icu,
            "b" => Self::Builtin,
            _ => Self::Libc,
        }
    }

    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Libc => "libc",
            Self::Icu => "icu",
            Self::Builtin => "builtin",
        }
    }
}

/// The `pg_database` locale facts of one database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct DatabaseLocale {
    pub(super) provider: LocaleProvider,
    pub(super) encoding: String,
    pub(super) collate: String,
    pub(super) ctype: String,
    /// The ICU or builtin locale name; `None` under libc.
    pub(super) locale: Option<String>,
    pub(super) icu_rules: Option<String>,
    /// The provider's collation version as origin recorded it; `None` before
    /// PostgreSQL 15 and for the builtin provider.
    pub(super) collation_version: Option<String>,
}

/// `server_version_num` of the connected server.
pub(super) async fn server_version_num(client: &Client) -> CacheResult<i32> {
    Ok(client
        .query_one("SELECT current_setting('server_version_num')::int", &[])
        .await
        .map_into_report::<CacheError>()
        .attach_loc("reading the server version")?
        .get(0))
}

/// `pg_database` grew its provider columns over releases: the provider and
/// ICU locale in 15, ICU rules in 16, and 17 renamed `daticulocale` to
/// `datlocale` when the builtin provider arrived.
fn probe_sql(server_version_num: i32) -> &'static str {
    match server_version_num {
        ..150000 => {
            "SELECT 'c', pg_encoding_to_char(encoding), datcollate, datctype, \
             NULL::text, NULL::text, NULL::text \
             FROM pg_database WHERE datname = current_database()"
        }
        150000..160000 => {
            "SELECT datlocprovider::text, pg_encoding_to_char(encoding), datcollate, datctype, \
             daticulocale, NULL::text, datcollversion \
             FROM pg_database WHERE datname = current_database()"
        }
        160000..170000 => {
            "SELECT datlocprovider::text, pg_encoding_to_char(encoding), datcollate, datctype, \
             daticulocale, daticurules, datcollversion \
             FROM pg_database WHERE datname = current_database()"
        }
        _ => {
            "SELECT datlocprovider::text, pg_encoding_to_char(encoding), datcollate, datctype, \
             datlocale, daticurules, datcollversion \
             FROM pg_database WHERE datname = current_database()"
        }
    }
}

impl DatabaseLocale {
    /// Read the connected database's locale.
    pub(super) async fn probe(client: &Client) -> CacheResult<Self> {
        let version = server_version_num(client).await?;
        let row = client
            .query_one(probe_sql(version), &[])
            .await
            .map_into_report::<CacheError>()
            .attach_loc("reading the database locale from pg_database")?;
        let provider: String = row.get(0);
        Ok(Self {
            provider: LocaleProvider::from_catalog(&provider),
            encoding: row.get(1),
            collate: row.get(2),
            ctype: row.get(3),
            locale: row.get(4),
            icu_rules: row.get(5),
            collation_version: row.get(6),
        })
    }

    /// `CREATE DATABASE` for a database that collates as this one does, on a
    /// server of `server_version_num`. `template0` because a locale other
    /// than the template's needs it. The provider is named explicitly
    /// wherever the server knows the clause (15+): without it a cluster
    /// initialised under ICU would hand a libc origin an ICU database.
    pub(super) fn create_database_sql(&self, name: &str, server_version_num: i32) -> String {
        let mut sql = format!(
            "CREATE DATABASE {name} TEMPLATE template0 ENCODING {} LC_COLLATE {} LC_CTYPE {}",
            escape_literal(&self.encoding),
            escape_literal(&self.collate),
            escape_literal(&self.ctype),
        );
        let provider_clause = server_version_num >= 150000;
        match self.provider {
            LocaleProvider::Libc => {
                if provider_clause {
                    sql.push_str(" LOCALE_PROVIDER libc");
                }
            }
            LocaleProvider::Icu => {
                sql.push_str(" LOCALE_PROVIDER icu");
                if let Some(locale) = &self.locale {
                    sql.push_str(&format!(" ICU_LOCALE {}", escape_literal(locale)));
                }
                if let Some(rules) = &self.icu_rules {
                    sql.push_str(&format!(" ICU_RULES {}", escape_literal(rules)));
                }
            }
            LocaleProvider::Builtin => {
                sql.push_str(" LOCALE_PROVIDER builtin");
                if let Some(locale) = &self.locale {
                    sql.push_str(&format!(" BUILTIN_LOCALE {}", escape_literal(locale)));
                }
            }
        }
        sql
    }

    /// One line naming the locale, for logs and the refusal message.
    pub(super) fn describe(&self) -> String {
        let mut text = format!(
            "provider={} encoding={} lc_collate={} lc_ctype={}",
            self.provider.as_str(),
            self.encoding,
            self.collate,
            self.ctype
        );
        if let Some(locale) = &self.locale {
            text.push_str(&format!(" locale={locale}"));
        }
        if let Some(version) = &self.collation_version {
            text.push_str(&format!(" collation_version={version}"));
        }
        text
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn libc_en_us() -> DatabaseLocale {
        DatabaseLocale {
            provider: LocaleProvider::Libc,
            encoding: "UTF8".into(),
            collate: "en_US.UTF-8".into(),
            ctype: "en_US.UTF-8".into(),
            locale: None,
            icu_rules: None,
            collation_version: Some("2.41".into()),
        }
    }

    #[test]
    fn test_create_database_sql_libc_names_the_provider_from_15() {
        assert_eq!(
            libc_en_us().create_database_sql("pgcache_cache", 180001),
            "CREATE DATABASE pgcache_cache TEMPLATE template0 ENCODING 'UTF8' \
             LC_COLLATE 'en_US.UTF-8' LC_CTYPE 'en_US.UTF-8' LOCALE_PROVIDER libc"
        );
    }

    #[test]
    fn test_create_database_sql_libc_omits_the_clause_before_15() {
        assert_eq!(
            libc_en_us().create_database_sql("c", 140012),
            "CREATE DATABASE c TEMPLATE template0 ENCODING 'UTF8' \
             LC_COLLATE 'en_US.UTF-8' LC_CTYPE 'en_US.UTF-8'"
        );
    }

    #[test]
    fn test_create_database_sql_icu_names_locale_and_rules() {
        let locale = DatabaseLocale {
            provider: LocaleProvider::Icu,
            locale: Some("en-US".into()),
            icu_rules: Some("&a < b".into()),
            collate: "C".into(),
            ctype: "C".into(),
            ..libc_en_us()
        };
        assert_eq!(
            locale.create_database_sql("c", 160004),
            "CREATE DATABASE c TEMPLATE template0 ENCODING 'UTF8' LC_COLLATE 'C' LC_CTYPE 'C' \
             LOCALE_PROVIDER icu ICU_LOCALE 'en-US' ICU_RULES '&a < b'"
        );
    }

    #[test]
    fn test_create_database_sql_builtin_names_the_builtin_locale() {
        let locale = DatabaseLocale {
            provider: LocaleProvider::Builtin,
            locale: Some("C.UTF-8".into()),
            collate: "C".into(),
            ctype: "C".into(),
            collation_version: None,
            ..libc_en_us()
        };
        assert_eq!(
            locale.create_database_sql("c", 170000),
            "CREATE DATABASE c TEMPLATE template0 ENCODING 'UTF8' LC_COLLATE 'C' LC_CTYPE 'C' \
             LOCALE_PROVIDER builtin BUILTIN_LOCALE 'C.UTF-8'"
        );
    }

    #[test]
    fn test_create_database_sql_quotes_locale_names() {
        let locale = DatabaseLocale {
            collate: "it's".into(),
            ..libc_en_us()
        };
        assert!(
            locale
                .create_database_sql("c", 180000)
                .contains("LC_COLLATE 'it''s'")
        );
    }

    #[test]
    fn test_probe_sql_follows_the_catalog_by_version() {
        assert!(probe_sql(140000).starts_with("SELECT 'c'"));
        assert!(probe_sql(150004).contains("daticulocale, NULL::text"));
        assert!(probe_sql(160002).contains("daticulocale, daticurules"));
        assert!(probe_sql(170000).contains("datlocale, daticurules"));
        assert!(probe_sql(180001).contains("datlocale, daticurules"));
    }

    #[test]
    fn test_describe_names_provider_and_version() {
        assert_eq!(
            libc_en_us().describe(),
            "provider=libc encoding=UTF8 lc_collate=en_US.UTF-8 lc_ctype=en_US.UTF-8 \
             collation_version=2.41"
        );
    }
}
