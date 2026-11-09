use rootcause::Report;
use tokio::runtime::Builder;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, Config, NoTls};
use tracing::{debug, error};

use super::database_locale::{DatabaseLocale, server_version_num};
use crate::cache::{CacheError, CacheResult, MapIntoReport, ReportExt, SetupError};
use crate::result::error_chain_format;
use crate::settings::Settings;

/// The merge chunk's range stamp (PGC-427); a merge fails without it.
const STAMP_RANGE_SIGNATURE: &str =
    "pgcache_generation_stamp_range(oid, oid, bigint, integer, integer)";

/// Reset the cache database by dropping and recreating it under origin's
/// locale, so text collates on the cache as it does on origin (PGC-587).
pub(super) fn cache_database_reset(
    settings: &Settings,
    locale: &DatabaseLocale,
) -> CacheResult<()> {
    let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_into_report::<CacheError>()?;

    rt.block_on(async {
        // Connect to postgres maintenance database
        let (admin_client, admin_conn) = Config::new()
            .host(&settings.cache.host)
            .port(settings.cache.port)
            .user(&settings.cache.user)
            .dbname("postgres")
            .connect(NoTls)
            .await
            .map_into_report::<CacheError>()?;

        tokio::spawn(async move {
            if let Err(e) = admin_conn.await {
                error!("admin connection error: {}", error_chain_format(&e));
            }
        });

        let db_name = &settings.cache.database;
        debug!("resetting cache database: {db_name}");

        // Terminate existing connections to the database
        admin_client
            .execute(
                &format!(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                     WHERE datname = '{db_name}' AND pid <> pg_backend_pid()"
                ),
                &[],
            )
            .await
            .map_into_report::<CacheError>()
            .attach_loc("terminating existing connections")?;

        admin_client
            .execute(&format!("DROP DATABASE IF EXISTS {db_name}"), &[])
            .await
            .map_into_report::<CacheError>()
            .attach_loc("dropping cache database")?;

        // No fallback to the template's locale: a cache that sorts text
        // differently from origin serves wrong orders, so refuse instead.
        // Only a locale the server rejects is a locale problem; a full disk or
        // a busy template0 keeps its own error.
        let cache_version = server_version_num(&admin_client).await?;
        if let Err(e) = admin_client
            .execute(&locale.create_database_sql(db_name, cache_version), &[])
            .await
        {
            let locale_rejected = matches!(
                e.code(),
                Some(&SqlState::INVALID_PARAMETER_VALUE)
                    | Some(&SqlState::FEATURE_NOT_SUPPORTED)
                    | Some(&SqlState::SYNTAX_ERROR)
            );
            return Err(if locale_rejected {
                Report::<CacheError>::from(CacheError::from(SetupError::DatabaseLocale {
                    locale: locale.describe(),
                }))
                .attach_loc(format!("creating cache database: {e}"))
            } else {
                Report::<CacheError>::from(CacheError::from(e))
                    .attach_loc("creating cache database")
            });
        }

        // Connect to fresh cache database and create extension
        let (cache_client, cache_conn) = Config::new()
            .host(&settings.cache.host)
            .port(settings.cache.port)
            .user(&settings.cache.user)
            .dbname(db_name)
            .connect(NoTls)
            .await
            .map_into_report::<CacheError>()?;

        tokio::spawn(async move {
            if let Err(e) = cache_conn.await {
                error!("cache connection error: {}", error_chain_format(&e));
            }
        });

        cache_client
            .execute("CREATE EXTENSION pg_stat_statements", &[])
            .await
            .map_into_report::<CacheError>()
            .attach_loc("creating pg_stat_statements extension")?;
        cache_client
            .execute("CREATE EXTENSION pgcache_pgrx", &[])
            .await
            .map_into_report::<CacheError>()
            .attach_loc("creating pgcache_pgrx extension")?;
        extension_function_require(&cache_client, STAMP_RANGE_SIGNATURE).await?;

        // Dedicated schema for materialized query results. Tables here are named
        // pgcache_mv.q_<fingerprint> and are managed by the MV subsystem (population,
        // rebuild, eviction). Not pgrx-tracked — consistency is managed via MvState.
        cache_client
            .execute("CREATE SCHEMA IF NOT EXISTS pgcache_mv", &[])
            .await
            .map_into_report::<CacheError>()
            .attach_loc("creating pgcache_mv schema")?;

        // Dedicated schema for population staging tables (PGC-250). A population
        // streams its origin snapshot into pgcache_stage.stage_<fp>_<gen>_<oid>,
        // then the writer merges it into the shared cache table (filtering rows
        // CDC removed during the population) when no CDC frame is open. Regular
        // tables (not temp) so the writer's connection can read what a worker
        // connection loaded. Swept by the DROP DATABASE on reset, like pgcache_mv.
        cache_client
            .execute("CREATE SCHEMA IF NOT EXISTS pgcache_stage", &[])
            .await
            .map_into_report::<CacheError>()
            .attach_loc("creating pgcache_stage schema")?;

        Ok(())
    })
}

/// Fail startup when the installed extension lacks a function pgcache calls.
async fn extension_function_require(client: &Client, signature: &'static str) -> CacheResult<()> {
    let present: bool = client
        .query_one("SELECT to_regprocedure($1) IS NOT NULL", &[&signature])
        .await
        .map_into_report::<CacheError>()
        .attach_loc("probing pgcache_pgrx for a required function")?
        .get(0);
    if present {
        Ok(())
    } else {
        Err(Report::from(CacheError::from(
            SetupError::ExtensionOutdated {
                function: signature,
            },
        )))
    }
}
