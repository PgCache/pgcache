use tokio::runtime::Builder;
use tokio_postgres::{Config, NoTls};
use tracing::{debug, error};

use crate::cache::{CacheError, CacheResult, MapIntoReport, ReportExt};
use crate::result::error_chain_format;
use crate::settings::Settings;

/// Reset the cache database by dropping and recreating it
pub(super) fn cache_database_reset(settings: &Settings) -> CacheResult<()> {
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

        admin_client
            .execute(&format!("CREATE DATABASE {db_name}"), &[])
            .await
            .map_into_report::<CacheError>()
            .attach_loc("creating cache database")?;

        // Disable IndexOnlyScan on the cache db (PGC-100). pgcache_pgrx's
        // tracker handles IOS correctly via a TID-based heap fetch, but that
        // path defeats IOS's heap-skipping benefit; on the cache db's mostly-
        // all-visible local heap, IOS isn't worth the slot-shape complexity.
        admin_client
            .execute(
                &format!("ALTER DATABASE {db_name} SET enable_indexonlyscan = off"),
                &[],
            )
            .await
            .map_into_report::<CacheError>()
            .attach_loc("disabling enable_indexonlyscan on cache database")?;

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
