//! The individual origin checks. Each has a `*_load` that queries the origin
//! and a pure `*_evaluate` that turns the rows into a verdict, so the verdict
//! logic is unit-testable without a database.

use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, Error};

use crate::pg::cdc::connect_replication;
use crate::pg::connect;
use crate::result::error_chain_format;
use crate::settings::{Allowlist, CdcSettings, PgSettings};

use super::{CheckResult, Environment};

const MIN_SERVER_VERSION_NUM: i32 = 160_000;
const NAMES_SHOWN: usize = 5;

fn in_docker() -> bool {
    std::env::var_os("PGCACHE_DOCKER").is_some()
}

fn host_is_loopback(host: &str) -> bool {
    matches!(host, "localhost" | "127.0.0.1" | "::1")
}

fn host_looks_pooled(host: &str) -> bool {
    host.contains("pooler") || host.contains("pgbouncer")
}

fn names_list(names: &[String]) -> String {
    let shown = names.iter().take(NAMES_SHOWN).cloned().collect::<Vec<_>>();
    let rest = names.len().saturating_sub(NAMES_SHOWN);
    if rest > 0 {
        format!("{} and {rest} more", shown.join(", "))
    } else {
        shown.join(", ")
    }
}

// --- connection ---------------------------------------------------------

pub(super) async fn origin_connect_check(
    settings: &PgSettings,
) -> Result<(CheckResult, Client), CheckResult> {
    match connect(settings, "preflight").await {
        Ok(client) => {
            let version = client
                .query_one("SELECT current_setting('server_version')", &[])
                .await
                .and_then(|row| row.try_get::<_, String>(0))
                .unwrap_or_else(|_| "unknown version".to_owned());
            Ok((
                CheckResult::pass("connection", format!("PostgreSQL {version}")),
                client,
            ))
        }
        Err(e) => Err(connect_failure(settings, &e)),
    }
}

fn connect_failure(settings: &PgSettings, error: &Error) -> CheckResult {
    let detail = error_chain_format(error);
    let remediation = match error.as_db_error() {
        Some(db) => db_connect_remediation(settings, db.code(), db.message()),
        None => transport_remediation(settings, &detail),
    };
    CheckResult::fail(
        "connection",
        format!(
            "could not connect to {}:{}: {detail}",
            settings.host, settings.port
        ),
        remediation,
    )
}

fn db_connect_remediation(settings: &PgSettings, code: &SqlState, message: &str) -> String {
    if *code == SqlState::INVALID_PASSWORD {
        return format!(
            "The password for role {} was rejected. Check it, or pass it with ORIGIN_PASSWORD.",
            settings.user
        );
    }
    if *code == SqlState::INVALID_CATALOG_NAME {
        return format!(
            "Database {} does not exist on this server. Check ORIGIN_DATABASE.",
            settings.database
        );
    }
    if *code == SqlState::INVALID_AUTHORIZATION_SPECIFICATION {
        if message.contains("SSL") || message.contains("encryption") {
            return "The server requires TLS for this client. Set ORIGIN_SSL_MODE=require \
                    (or add sslmode=require to the URL)."
                .to_owned();
        }
        return format!(
            "Add a pg_hba.conf entry that lets this client in, for example:\n  \
             host  {}  {}  <pgcache address>/32  scram-sha-256\n\
             then reload: SELECT pg_reload_conf();",
            settings.database, settings.user
        );
    }
    "Check the credentials and the server log for the rejection reason.".to_owned()
}

fn transport_remediation(settings: &PgSettings, detail: &str) -> String {
    if detail.contains("password missing") {
        return "The server asked for a password and none was given. Pass it with \
                ORIGIN_PASSWORD (or in the URL)."
            .to_owned();
    }
    let unreachable = detail.contains("refused") || detail.contains("timed out");
    if unreachable && host_is_loopback(&settings.host) && in_docker() {
        return "Inside a container, localhost is the container itself. Use host.docker.internal \
                as the origin host (on Linux add --add-host=host.docker.internal:host-gateway \
                to docker run)."
            .to_owned();
    }
    if detail.contains("refused") {
        "Nothing is listening at that host and port. Check that PostgreSQL is running, that \
         listen_addresses covers this interface, and that the port is right."
            .to_owned()
    } else if detail.contains("timed out") {
        "The host did not answer. Check the hostname, firewall or security-group rules, and \
         that the port is open."
            .to_owned()
    } else if detail.contains("lookup") || detail.contains("resolve") {
        "The hostname did not resolve. Check it for a typo.".to_owned()
    } else if detail.contains("tls") || detail.contains("TLS") || detail.contains("certificate") {
        "TLS negotiation failed. Try ORIGIN_SSL_MODE=require, or verify-full only when the \
         server certificate is signed by a trusted CA."
            .to_owned()
    } else {
        "Check the host, port and network path to the origin.".to_owned()
    }
}

// --- environment --------------------------------------------------------

pub(super) async fn environment_detect(client: &Client) -> Environment {
    let Ok(rows) = client
        .query(
            "SELECT rolname::text FROM pg_roles \
             WHERE rolname IN ('rds_superuser', 'cloudsqlsuperuser', 'neon_superuser')",
            &[],
        )
        .await
    else {
        return Environment::SelfHosted;
    };
    for row in rows {
        match row.try_get::<_, String>(0).as_deref() {
            Ok("rds_superuser") => return Environment::Rds,
            Ok("cloudsqlsuperuser") => return Environment::CloudSql,
            Ok("neon_superuser") => return Environment::Neon,
            _ => {}
        }
    }
    Environment::SelfHosted
}

// --- server version -----------------------------------------------------

pub(super) async fn server_version_check(client: &Client) -> CheckResult {
    match server_version_load(client).await {
        Ok((num, text)) => server_version_evaluate(num, &text),
        Err(e) => CheckResult::skipped("server version", e),
    }
}

async fn server_version_load(client: &Client) -> Result<(i32, String), Error> {
    let row = client
        .query_one(
            "SELECT current_setting('server_version_num')::int, current_setting('server_version')",
            &[],
        )
        .await?;
    Ok((row.try_get(0)?, row.try_get(1)?))
}

fn server_version_evaluate(num: i32, text: &str) -> CheckResult {
    if num >= MIN_SERVER_VERSION_NUM {
        CheckResult::pass("server version", format!("PostgreSQL {text}"))
    } else {
        CheckResult::fail(
            "server version",
            format!("PostgreSQL {text} is too old"),
            "pgcache needs PostgreSQL 16 or newer on the origin.",
        )
    }
}

// --- wal_level ----------------------------------------------------------

pub(super) async fn wal_level_check(client: &Client, environment: Environment) -> CheckResult {
    let level = client
        .query_one("SELECT current_setting('wal_level')", &[])
        .await
        .and_then(|row| row.try_get::<_, String>(0));
    match level {
        Ok(level) => wal_level_evaluate(&level, environment),
        Err(e) => CheckResult::skipped("wal_level", e),
    }
}

fn wal_level_evaluate(level: &str, environment: Environment) -> CheckResult {
    if level == "logical" {
        return CheckResult::pass("wal_level", "logical");
    }
    let remediation = match environment {
        Environment::SelfHosted => {
            "ALTER SYSTEM SET wal_level = logical;\n\
             then restart PostgreSQL (a reload is not enough)."
        }
        Environment::Rds => {
            "Set rds.logical_replication = 1 in the instance's DB parameter group, then reboot \
             the instance."
        }
        Environment::CloudSql => {
            "Set the cloudsql.logical_decoding flag to on; Cloud SQL restarts the instance to \
             apply it."
        }
        Environment::Neon => {
            "Enable logical replication for the project in the Neon console (Project settings, \
             Logical replication)."
        }
    };
    CheckResult::fail(
        "wal_level",
        format!("wal_level is '{level}'; logical replication needs 'logical'"),
        remediation,
    )
}

// --- replication slot capacity -----------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SlotCapacity {
    pub max_slots: i32,
    pub used_slots: i32,
    pub max_senders: i32,
    pub active_senders: i32,
    /// Our own slot already exists, so it needs no free capacity.
    pub own_slot_exists: bool,
    pub inactive_slots: Vec<String>,
}

pub(super) async fn replication_slots_check(
    client: &Client,
    slot_name: &str,
    environment: Environment,
) -> CheckResult {
    match slot_capacity_load(client, slot_name).await {
        Ok(capacity) => replication_slots_evaluate(&capacity, environment),
        Err(e) => CheckResult::skipped("replication slots", e),
    }
}

async fn slot_capacity_load(client: &Client, slot_name: &str) -> Result<SlotCapacity, Error> {
    let row = client
        .query_one(
            "SELECT current_setting('max_replication_slots')::int, \
                    (SELECT count(*)::int FROM pg_replication_slots), \
                    current_setting('max_wal_senders')::int, \
                    (SELECT count(*)::int FROM pg_stat_replication), \
                    EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1), \
                    (SELECT coalesce(array_agg(slot_name::text ORDER BY slot_name), '{}') \
                       FROM pg_replication_slots WHERE NOT active AND slot_name <> $1)",
            &[&slot_name],
        )
        .await?;
    Ok(SlotCapacity {
        max_slots: row.try_get(0)?,
        used_slots: row.try_get(1)?,
        max_senders: row.try_get(2)?,
        active_senders: row.try_get(3)?,
        own_slot_exists: row.try_get(4)?,
        inactive_slots: row.try_get(5)?,
    })
}

fn replication_slots_evaluate(capacity: &SlotCapacity, environment: Environment) -> CheckResult {
    let slot_free = capacity.own_slot_exists || capacity.used_slots < capacity.max_slots;
    let sender_free = capacity.active_senders < capacity.max_senders;
    let usage = format!(
        "{} of {} replication slots and {} of {} wal senders in use",
        capacity.used_slots, capacity.max_slots, capacity.active_senders, capacity.max_senders
    );
    if slot_free && sender_free {
        return CheckResult::pass("replication slots", usage);
    }

    let mut lines = Vec::new();
    let raise_hint = match environment {
        Environment::SelfHosted => {
            if !slot_free {
                lines.push(format!(
                    "ALTER SYSTEM SET max_replication_slots = {};",
                    capacity.max_slots + 1
                ));
            }
            if !sender_free {
                lines.push(format!(
                    "ALTER SYSTEM SET max_wal_senders = {};",
                    capacity.max_senders + 1
                ));
            }
            "then restart PostgreSQL."
        }
        Environment::Rds => {
            "Raise max_replication_slots and max_wal_senders in the DB parameter group, then \
             reboot the instance."
        }
        Environment::CloudSql | Environment::Neon => {
            "Raise max_replication_slots and max_wal_senders through the provider's settings \
             (a restart is required)."
        }
    };
    lines.push(raise_hint.to_owned());
    if let Some(first_inactive) = capacity.inactive_slots.first().filter(|_| !slot_free) {
        lines.push(format!(
            "Or drop an inactive slot you no longer need: SELECT pg_drop_replication_slot('{first_inactive}');"
        ));
    }
    CheckResult::fail(
        "replication slots",
        format!("{usage}; pgcache needs one of each"),
        lines.join("\n"),
    )
}

// --- role privileges ----------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RoleInfo {
    pub name: String,
    pub superuser: bool,
    pub replication: bool,
    pub create_on_database: bool,
    pub rds_replication: bool,
}

pub(super) async fn role_info_load(client: &Client) -> Result<RoleInfo, Error> {
    let row = client
        .query_one(
            "SELECT current_user::text, r.rolsuper, r.rolreplication, \
                    has_database_privilege(current_database(), 'CREATE'), \
                    CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rds_replication') \
                         THEN pg_has_role(current_user, 'rds_replication', 'MEMBER') \
                         ELSE false END \
             FROM pg_roles r WHERE r.rolname = current_user",
            &[],
        )
        .await?;
    Ok(RoleInfo {
        name: row.try_get(0)?,
        superuser: row.try_get(1)?,
        replication: row.try_get(2)?,
        create_on_database: row.try_get(3)?,
        rds_replication: row.try_get(4)?,
    })
}

pub(super) fn role_privileges_evaluate(
    role: &RoleInfo,
    origin: &PgSettings,
    environment: Environment,
) -> CheckResult {
    if role.superuser {
        return CheckResult::pass("role privileges", format!("{} is a superuser", role.name));
    }
    let can_replicate = role.replication || role.rds_replication;
    if can_replicate && role.create_on_database {
        return CheckResult::pass(
            "role privileges",
            format!("{} has REPLICATION and CREATE on the database", role.name),
        );
    }

    let mut missing = Vec::new();
    let mut statements = Vec::new();
    if !can_replicate {
        missing.push("REPLICATION");
        statements.push(match environment {
            Environment::Rds => format!("GRANT rds_replication TO \"{}\";", role.name),
            Environment::SelfHosted | Environment::CloudSql | Environment::Neon => {
                format!("ALTER ROLE \"{}\" REPLICATION;", role.name)
            }
        });
    }
    if !role.create_on_database {
        missing.push("CREATE on the database");
        statements.push(format!(
            "GRANT CREATE ON DATABASE \"{}\" TO \"{}\";",
            origin.database, role.name
        ));
    }
    let run_as = match environment {
        Environment::Rds => "Run as the master user:",
        Environment::SelfHosted | Environment::CloudSql | Environment::Neon => {
            "Run as a superuser:"
        }
    };
    CheckResult::fail(
        "role privileges",
        format!(
            "{} lacks {} (needed to create the publication and slot)",
            role.name,
            missing.join(" and ")
        ),
        format!("{run_as}\n{}", statements.join("\n")),
    )
}

// --- existing CDC objects -----------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CdcObjects {
    pub publication_exists: bool,
    /// `Some(active)` when a slot with our name already exists.
    pub slot_active: Option<bool>,
    pub slot_retained_wal: Option<String>,
}

pub(super) async fn cdc_objects_check(client: &Client, cdc: &CdcSettings) -> CheckResult {
    match cdc_objects_load(client, cdc).await {
        Ok(objects) => cdc_objects_evaluate(&objects, cdc),
        Err(e) => CheckResult::skipped("existing cdc objects", e),
    }
}

async fn cdc_objects_load(client: &Client, cdc: &CdcSettings) -> Result<CdcObjects, Error> {
    let row = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1), \
                    s.active, \
                    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), s.restart_lsn)) \
             FROM (SELECT 1) AS one \
             LEFT JOIN pg_replication_slots s ON s.slot_name = $2",
            &[&cdc.publication_name, &cdc.slot_name],
        )
        .await?;
    Ok(CdcObjects {
        publication_exists: row.try_get(0)?,
        slot_active: row.try_get(1)?,
        slot_retained_wal: row.try_get(2)?,
    })
}

fn cdc_objects_evaluate(objects: &CdcObjects, cdc: &CdcSettings) -> CheckResult {
    let name = "existing cdc objects";
    match objects.slot_active {
        Some(true) => CheckResult::fail(
            name,
            format!(
                "replication slot '{}' is already active: another pgcache or consumer is \
                 attached to it",
                cdc.slot_name
            ),
            "Stop the other instance, or give this one distinct names with CDC_SUFFIX \
             (or --cdc_slot_name and --cdc_publication_name).",
        ),
        Some(false) => CheckResult::warn(
            name,
            format!(
                "slot '{}' exists from an earlier run and holds {} of WAL",
                cdc.slot_name,
                objects
                    .slot_retained_wal
                    .as_deref()
                    .unwrap_or("an unknown amount")
            ),
            format!(
                "pgcache will reuse it. If it is stale, drop it so the origin can reclaim WAL:\n\
                 SELECT pg_drop_replication_slot('{}');",
                cdc.slot_name
            ),
        ),
        None if objects.publication_exists => CheckResult::pass(
            name,
            format!(
                "publication '{}' exists (pgcache recreates it at startup); slot '{}' will be \
                 created",
                cdc.publication_name, cdc.slot_name
            ),
        ),
        None => CheckResult::pass(
            name,
            format!(
                "none yet; pgcache creates publication '{}' and slot '{}' at startup",
                cdc.publication_name, cdc.slot_name
            ),
        ),
    }
}

// --- replication connection --------------------------------------------

pub(super) async fn replication_connect_check(
    settings: &PgSettings,
    environment: Environment,
) -> CheckResult {
    match connect_replication(settings, "preflight").await {
        Ok(_client) => CheckResult::pass(
            "replication connection",
            format!(
                "logical replication connection to {}:{} accepted",
                settings.host, settings.port
            ),
        ),
        Err(e) => CheckResult::fail(
            "replication connection",
            format!(
                "could not open a logical replication connection to {}:{}: {}",
                settings.host,
                settings.port,
                error_chain_format(&e)
            ),
            replication_remediation(settings, &e, environment),
        ),
    }
}

fn replication_remediation(
    settings: &PgSettings,
    error: &Error,
    environment: Environment,
) -> String {
    if host_looks_pooled(&settings.host) {
        return "This host looks like a connection pooler, and logical replication needs a direct \
                connection. Point REPLICATION_URL (or REPLICATION_HOST) at the origin's direct \
                endpoint."
            .to_owned();
    }
    let Some(db) = error.as_db_error() else {
        return "If a pooler sits between pgcache and the origin, point REPLICATION_URL at the \
                direct endpoint."
            .to_owned();
    };
    let code = db.code();
    if *code == SqlState::INVALID_AUTHORIZATION_SPECIFICATION {
        return format!(
            "Add a pg_hba.conf entry that lets role {} reach database {} (logical replication \
             connections match the database name, not the 'replication' keyword), then \
             SELECT pg_reload_conf();",
            settings.user, settings.database
        );
    }
    if *code == SqlState::INSUFFICIENT_PRIVILEGE {
        return match environment {
            Environment::Rds => format!("GRANT rds_replication TO \"{}\";", settings.user),
            Environment::SelfHosted | Environment::CloudSql | Environment::Neon => {
                format!("ALTER ROLE \"{}\" REPLICATION;", settings.user)
            }
        };
    }
    if *code == SqlState::TOO_MANY_CONNECTIONS {
        return "Every wal sender is in use. Raise max_wal_senders (restart required) or stop an \
                unused replication client."
            .to_owned();
    }
    "Check the origin server log for the rejection reason.".to_owned()
}

// --- relations: ownership, primary keys, views, RLS ---------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RelationKind {
    Table,
    View,
    MaterializedView,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Relation {
    pub schema: String,
    pub name: String,
    pub kind: RelationKind,
    pub owned: bool,
    pub has_primary_key: bool,
    pub row_security: bool,
}

impl Relation {
    fn qualified_name(&self) -> String {
        format!("{}.{}", self.schema, self.name)
    }
}

fn relation_allowed(allowlist: &Allowlist, schema: &str, name: &str) -> bool {
    let Some(entries) = allowlist else {
        return true;
    };
    entries.iter().any(|(entry_schema, entry_name)| {
        entry_name.eq_ignore_ascii_case(name)
            && entry_schema
                .as_deref()
                .is_none_or(|s| s.eq_ignore_ascii_case(schema))
    })
}

/// Load every user relation, scoped to the allowlist when one is set, since
/// a relation pgcache will never touch should not produce a finding.
/// Extension-owned relations (pg_stat_statements views etc.) are skipped.
pub(super) async fn relations_load(
    client: &Client,
    allowlist: &Allowlist,
) -> Result<Vec<Relation>, Error> {
    let rows = client
        .query(
            "SELECT n.nspname::text, c.relname::text, c.relkind::text, \
                    pg_get_userbyid(c.relowner) = current_user, \
                    EXISTS (SELECT 1 FROM pg_constraint k \
                             WHERE k.conrelid = c.oid AND k.contype = 'p'), \
                    c.relrowsecurity \
             FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relkind IN ('r', 'p', 'v', 'm') AND NOT c.relispartition \
               AND n.nspname NOT IN ('pg_catalog', 'information_schema') \
               AND n.nspname NOT LIKE 'pg\\_%' \
               AND NOT EXISTS (SELECT 1 FROM pg_depend d \
                                WHERE d.classid = 'pg_class'::regclass \
                                  AND d.objid = c.oid AND d.deptype = 'e') \
             ORDER BY 1, 2",
            &[],
        )
        .await?;
    let mut relations = Vec::with_capacity(rows.len());
    for row in rows {
        let schema: String = row.try_get(0)?;
        let name: String = row.try_get(1)?;
        if !relation_allowed(allowlist, &schema, &name) {
            continue;
        }
        let kind = match row.try_get::<_, String>(2)?.as_str() {
            "v" => RelationKind::View,
            "m" => RelationKind::MaterializedView,
            _ => RelationKind::Table,
        };
        relations.push(Relation {
            schema,
            name,
            kind,
            owned: row.try_get(3)?,
            has_primary_key: row.try_get(4)?,
            row_security: row.try_get(5)?,
        });
    }
    Ok(relations)
}

fn tables(relations: &[Relation]) -> impl Iterator<Item = &Relation> {
    relations.iter().filter(|r| r.kind == RelationKind::Table)
}

pub(super) fn table_ownership_evaluate(
    relations: &[Relation],
    superuser: bool,
    role_name: &str,
) -> CheckResult {
    let name = "table ownership";
    if superuser {
        return CheckResult::pass(name, "superuser; any table can join the publication");
    }
    let total = tables(relations).count();
    let unowned = tables(relations)
        .filter(|r| !r.owned)
        .map(Relation::qualified_name)
        .collect::<Vec<_>>();
    if unowned.is_empty() {
        return CheckResult::pass(name, format!("{role_name} owns all {total} tables"));
    }
    CheckResult::warn(
        name,
        format!(
            "{} of {total} tables are not owned by {role_name}, so pgcache cannot add them to \
             its publication and forwards queries on them: {}",
            unowned.len(),
            names_list(&unowned)
        ),
        format!(
            "ALTER TABLE <table> OWNER TO \"{role_name}\"; for each, or run pgcache as the \
             tables' owner."
        ),
    )
}

pub(super) fn primary_keys_evaluate(relations: &[Relation]) -> CheckResult {
    let name = "primary keys";
    let total = tables(relations).count();
    if total == 0 {
        return CheckResult::warn(
            name,
            "no tables found",
            "pgcache caches queries on tables; this database (or the allowlisted set) has none.",
        );
    }
    let missing = tables(relations)
        .filter(|r| !r.has_primary_key)
        .map(Relation::qualified_name)
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return CheckResult::pass(name, format!("all {total} tables have a primary key"));
    }
    CheckResult::warn(
        name,
        format!(
            "{} of {total} tables have no primary key and are forwarded, never cached: {}",
            missing.len(),
            names_list(&missing)
        ),
        "Add one: ALTER TABLE <table> ADD PRIMARY KEY (<column>);",
    )
}

pub(super) fn views_evaluate(relations: &[Relation]) -> CheckResult {
    let name = "views";
    let views = relations
        .iter()
        .filter(|r| r.kind != RelationKind::Table)
        .map(Relation::qualified_name)
        .collect::<Vec<_>>();
    if views.is_empty() {
        return CheckResult::pass(name, "none");
    }
    CheckResult::warn(
        name,
        format!(
            "{} views; queries that reference a view are forwarded, never cached: {}",
            views.len(),
            names_list(&views)
        ),
        "View support is not available yet. Query the underlying tables directly where you \
         want caching.",
    )
}

pub(super) fn row_level_security_evaluate(relations: &[Relation]) -> CheckResult {
    let name = "row-level security";
    let secured = tables(relations)
        .filter(|r| r.row_security)
        .map(Relation::qualified_name)
        .collect::<Vec<_>>();
    if secured.is_empty() {
        return CheckResult::pass(name, "no tables have row-level security enabled");
    }
    CheckResult::fail(
        name,
        format!(
            "{} tables have row-level security enabled, which pgcache does not support yet: {}",
            secured.len(),
            names_list(&secured)
        ),
        "pgcache populates the cache as its own role and would serve those rows to every client, \
         bypassing the policies. Exclude these tables with ALLOWED_TABLES (--allowed_tables), \
         listing only the tables to cache.",
    )
}

// --- pg_stat_statements -------------------------------------------------

pub(super) async fn pg_stat_statements_check(client: &Client) -> CheckResult {
    let name = "pg_stat_statements";
    let installed = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')",
            &[],
        )
        .await
        .and_then(|row| row.try_get::<_, bool>(0));
    match installed {
        Ok(true) => CheckResult::pass(
            name,
            "installed; export it to the Fit Analyzer (pgcache.com/fit) to score this workload",
        ),
        Ok(false) => CheckResult::pass(
            name,
            "not installed (optional; only the Fit Analyzer uses it)",
        ),
        Err(e) => CheckResult::skipped(name, e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::preflight::Verdict;
    use crate::settings::SslMode;

    fn origin() -> PgSettings {
        PgSettings {
            host: "db".to_owned(),
            port: 5432,
            user: "app".to_owned(),
            password: None,
            database: "shop".to_owned(),
            ssl_mode: SslMode::Disable,
        }
    }

    fn relation(name: &str, kind: RelationKind) -> Relation {
        Relation {
            schema: "public".to_owned(),
            name: name.to_owned(),
            kind,
            owned: true,
            has_primary_key: true,
            row_security: false,
        }
    }

    #[test]
    fn test_server_version_evaluate_rejects_pre_16() {
        assert_eq!(
            server_version_evaluate(150_004, "15.4").verdict,
            Verdict::Fail
        );
        assert_eq!(
            server_version_evaluate(160_000, "16.0").verdict,
            Verdict::Pass
        );
    }

    #[test]
    fn test_wal_level_evaluate_words_remediation_by_environment() {
        assert_eq!(
            wal_level_evaluate("logical", Environment::Rds).verdict,
            Verdict::Pass
        );
        let self_hosted = wal_level_evaluate("replica", Environment::SelfHosted);
        assert_eq!(self_hosted.verdict, Verdict::Fail);
        assert!(
            self_hosted
                .remediation
                .as_deref()
                .is_some_and(|r| r.contains("ALTER SYSTEM SET wal_level = logical"))
        );
        let rds = wal_level_evaluate("replica", Environment::Rds);
        assert!(
            rds.remediation
                .as_deref()
                .is_some_and(|r| r.contains("rds.logical_replication"))
        );
    }

    #[test]
    fn test_replication_slots_evaluate_counts_own_slot_as_free() {
        let full = SlotCapacity {
            max_slots: 2,
            used_slots: 2,
            max_senders: 10,
            active_senders: 0,
            own_slot_exists: false,
            inactive_slots: vec!["old_slot".to_owned()],
        };
        let result = replication_slots_evaluate(&full, Environment::SelfHosted);
        assert_eq!(result.verdict, Verdict::Fail);
        let remediation = result.remediation.unwrap_or_default();
        assert!(remediation.contains("max_replication_slots = 3"));
        assert!(remediation.contains("pg_drop_replication_slot('old_slot')"));

        let own = SlotCapacity {
            own_slot_exists: true,
            ..full
        };
        assert_eq!(
            replication_slots_evaluate(&own, Environment::SelfHosted).verdict,
            Verdict::Pass
        );
    }

    #[test]
    fn test_role_privileges_evaluate_lists_missing_grants() {
        let role = RoleInfo {
            name: "app".to_owned(),
            superuser: false,
            replication: false,
            create_on_database: false,
            rds_replication: false,
        };
        let result = role_privileges_evaluate(&role, &origin(), Environment::SelfHosted);
        assert_eq!(result.verdict, Verdict::Fail);
        assert!(
            result
                .finding
                .contains("REPLICATION and CREATE on the database")
        );
        let remediation = result.remediation.unwrap_or_default();
        assert!(remediation.contains("ALTER ROLE \"app\" REPLICATION;"));
        assert!(remediation.contains("GRANT CREATE ON DATABASE \"shop\" TO \"app\";"));

        let rds = role_privileges_evaluate(&role, &origin(), Environment::Rds);
        assert!(
            rds.remediation
                .as_deref()
                .is_some_and(|r| r.contains("GRANT rds_replication TO \"app\";"))
        );

        let granted = RoleInfo {
            replication: true,
            create_on_database: true,
            ..role
        };
        assert_eq!(
            role_privileges_evaluate(&granted, &origin(), Environment::SelfHosted).verdict,
            Verdict::Pass
        );
    }

    #[test]
    fn test_cdc_objects_evaluate_active_slot_fails_inactive_warns() {
        let cdc = CdcSettings {
            publication_name: "pgcache_pub".to_owned(),
            slot_name: "pgcache_slot".to_owned(),
        };
        let active = CdcObjects {
            publication_exists: true,
            slot_active: Some(true),
            slot_retained_wal: Some("16 MB".to_owned()),
        };
        assert_eq!(cdc_objects_evaluate(&active, &cdc).verdict, Verdict::Fail);
        let inactive = CdcObjects {
            slot_active: Some(false),
            ..active
        };
        let result = cdc_objects_evaluate(&inactive, &cdc);
        assert_eq!(result.verdict, Verdict::Warn);
        assert!(result.finding.contains("16 MB"));
        let none = CdcObjects {
            publication_exists: false,
            slot_active: None,
            slot_retained_wal: None,
        };
        assert_eq!(cdc_objects_evaluate(&none, &cdc).verdict, Verdict::Pass);
    }

    #[test]
    fn test_primary_keys_evaluate_names_tables_without_one() {
        let mut logs = relation("logs", RelationKind::Table);
        logs.has_primary_key = false;
        let relations = vec![relation("users", RelationKind::Table), logs];
        let result = primary_keys_evaluate(&relations);
        assert_eq!(result.verdict, Verdict::Warn);
        assert!(result.finding.contains("1 of 2 tables"));
        assert!(result.finding.contains("public.logs"));
        assert_eq!(primary_keys_evaluate(&[]).verdict, Verdict::Warn);
        assert_eq!(
            primary_keys_evaluate(&relations[..1]).verdict,
            Verdict::Pass
        );
    }

    #[test]
    fn test_views_evaluate_counts_views_and_matviews() {
        let relations = vec![
            relation("users", RelationKind::Table),
            relation("active_users", RelationKind::View),
            relation("daily_totals", RelationKind::MaterializedView),
        ];
        let result = views_evaluate(&relations);
        assert_eq!(result.verdict, Verdict::Warn);
        assert!(result.finding.starts_with("2 views"));
        assert_eq!(views_evaluate(&relations[..1]).verdict, Verdict::Pass);
    }

    #[test]
    fn test_row_level_security_evaluate_fails_on_secured_table() {
        let mut tenants = relation("tenants", RelationKind::Table);
        tenants.row_security = true;
        let relations = vec![relation("users", RelationKind::Table), tenants];
        let result = row_level_security_evaluate(&relations);
        assert_eq!(result.verdict, Verdict::Fail);
        assert!(result.finding.contains("public.tenants"));
        assert_eq!(
            row_level_security_evaluate(&relations[..1]).verdict,
            Verdict::Pass
        );
    }

    #[test]
    fn test_table_ownership_evaluate_superuser_passes() {
        let mut orders = relation("orders", RelationKind::Table);
        orders.owned = false;
        let relations = vec![relation("users", RelationKind::Table), orders];
        assert_eq!(
            table_ownership_evaluate(&relations, true, "app").verdict,
            Verdict::Pass
        );
        let result = table_ownership_evaluate(&relations, false, "app");
        assert_eq!(result.verdict, Verdict::Warn);
        assert!(result.finding.contains("public.orders"));
    }

    #[test]
    fn test_relation_allowed_matches_schema_qualified_and_bare_entries() {
        let allowlist: Allowlist = Some(vec![
            (None, "users".to_owned()),
            (Some("audit".to_owned()), "events".to_owned()),
        ]);
        assert!(relation_allowed(&allowlist, "public", "users"));
        assert!(relation_allowed(&allowlist, "other", "Users"));
        assert!(relation_allowed(&allowlist, "audit", "events"));
        assert!(!relation_allowed(&allowlist, "public", "events"));
        assert!(relation_allowed(&None, "public", "anything"));
    }

    #[test]
    fn test_names_list_truncates_after_five() {
        let names = (1..=7).map(|i| format!("t{i}")).collect::<Vec<_>>();
        assert_eq!(names_list(&names), "t1, t2, t3, t4, t5 and 2 more");
        assert_eq!(names_list(&names[..2]), "t1, t2");
    }
}
