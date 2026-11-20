use std::fs;

use super::cli::*;
use super::dynamic::*;
use super::toml_file::*;
use super::*;

fn base_settings() -> PgSettings {
    PgSettings {
        host: "base.example.com".to_owned(),
        port: 5432,
        user: "base_user".to_owned(),
        password: Some("base_password".to_owned()),
        database: "base_db".to_owned(),
        ssl_mode: SslMode::Disable,
    }
}

#[test]
fn test_partial_merge_empty_uses_all_base_values() {
    let base = base_settings();
    let partial = PgSettingsPartial::default();

    let result = partial.merge_with(&base);

    assert_eq!(result.host, "base.example.com");
    assert_eq!(result.port, 5432);
    assert_eq!(result.user, "base_user");
    assert_eq!(result.password, Some("base_password".to_owned()));
    assert_eq!(result.database, "base_db");
    assert_eq!(result.ssl_mode, SslMode::Disable);
}

#[test]
fn test_partial_merge_host_only_override() {
    let base = base_settings();
    let partial = PgSettingsPartial {
        host: Some("override.example.com".to_owned()),
        ..Default::default()
    };

    let result = partial.merge_with(&base);

    assert_eq!(result.host, "override.example.com");
    assert_eq!(result.port, 5432);
    assert_eq!(result.user, "base_user");
    assert_eq!(result.password, Some("base_password".to_owned()));
    assert_eq!(result.database, "base_db");
    assert_eq!(result.ssl_mode, SslMode::Disable);
}

#[test]
fn test_partial_merge_multiple_fields_override() {
    let base = base_settings();
    let partial = PgSettingsPartial {
        host: Some("override.example.com".to_owned()),
        port: Some(6432),
        ssl_mode: Some(SslMode::Require),
        ..Default::default()
    };

    let result = partial.merge_with(&base);

    assert_eq!(result.host, "override.example.com");
    assert_eq!(result.port, 6432);
    assert_eq!(result.user, "base_user");
    assert_eq!(result.password, Some("base_password".to_owned()));
    assert_eq!(result.database, "base_db");
    assert_eq!(result.ssl_mode, SslMode::Require);
}

#[test]
fn test_partial_merge_all_fields_override() {
    let base = base_settings();
    let partial = PgSettingsPartial {
        host: Some("override.example.com".to_owned()),
        port: Some(6432),
        user: Some("override_user".to_owned()),
        password: Some("override_password".to_owned()),
        database: Some("override_db".to_owned()),
        ssl_mode: Some(SslMode::Require),
    };

    let result = partial.merge_with(&base);

    assert_eq!(result.host, "override.example.com");
    assert_eq!(result.port, 6432);
    assert_eq!(result.user, "override_user");
    assert_eq!(result.password, Some("override_password".to_owned()));
    assert_eq!(result.database, "override_db");
    assert_eq!(result.ssl_mode, SslMode::Require);
}

#[test]
fn test_partial_merge_password_override_when_base_has_none() {
    let mut base = base_settings();
    base.password = None;

    let partial = PgSettingsPartial {
        password: Some("new_password".to_owned()),
        ..Default::default()
    };

    let result = partial.merge_with(&base);

    assert_eq!(result.password, Some("new_password".to_owned()));
}

#[test]
fn test_partial_merge_password_inherited_when_partial_has_none() {
    let base = base_settings();
    let partial = PgSettingsPartial {
        host: Some("override.example.com".to_owned()),
        password: None, // Not specified, should inherit from base
        ..Default::default()
    };

    let result = partial.merge_with(&base);

    assert_eq!(result.password, Some("base_password".to_owned()));
}

#[test]
fn test_partial_merge_both_passwords_none() {
    let mut base = base_settings();
    base.password = None;

    let partial = PgSettingsPartial::default();

    let result = partial.merge_with(&base);

    assert_eq!(result.password, None);
}

#[test]
fn test_toml_parse_no_replication_section() {
    let toml_str = r#"
num_workers = 4

[origin]
host = "origin.example.com"
port = 5432
user = "origin_user"
password = "origin_password"
database = "origin_db"

[cache]
host = "localhost"
port = 5433
user = "cache_user"
database = "cache_db"

[cdc]
publication_name = "test_pub"
slot_name = "test_slot"

[listen]
socket = "127.0.0.1:5434"
"#;

    let settings: SettingsToml = toml::from_str(toml_str).expect("parse TOML");

    assert!(settings.replication.is_none());

    // When replication is None, it should default to origin
    let replication = match settings.replication {
        Some(partial) => partial.merge_with(&settings.origin),
        None => settings.origin,
    };

    assert_eq!(replication.host, "origin.example.com");
    assert_eq!(replication.port, 5432);
    assert_eq!(replication.user, "origin_user");
    assert_eq!(replication.password, Some("origin_password".to_owned()));
    assert_eq!(replication.database, "origin_db");
}

#[test]
fn test_toml_parse_partial_replication_section() {
    let toml_str = r#"
num_workers = 4

[origin]
host = "pgbouncer.example.com"
port = 6432
user = "app_user"
password = "secret"
database = "mydb"
ssl_mode = "require"

[replication]
host = "postgres.example.com"
port = 5432

[cache]
host = "localhost"
port = 5433
user = "cache_user"
database = "cache_db"

[cdc]
publication_name = "test_pub"
slot_name = "test_slot"

[listen]
socket = "127.0.0.1:5434"
"#;

    let settings: SettingsToml = toml::from_str(toml_str).expect("parse TOML");

    assert!(settings.replication.is_some());
    let partial = settings.replication.as_ref().expect("replication section");

    assert_eq!(partial.host, Some("postgres.example.com".to_owned()));
    assert_eq!(partial.port, Some(5432));
    assert_eq!(partial.user, None);
    assert_eq!(partial.password, None);
    assert_eq!(partial.database, None);
    assert_eq!(partial.ssl_mode, None);

    // After merging, unspecified fields should come from origin
    let replication = partial.merge_with(&settings.origin);

    assert_eq!(replication.host, "postgres.example.com");
    assert_eq!(replication.port, 5432);
    assert_eq!(replication.user, "app_user");
    assert_eq!(replication.password, Some("secret".to_owned()));
    assert_eq!(replication.database, "mydb");
    assert_eq!(replication.ssl_mode, SslMode::Require);
}

#[test]
fn test_toml_parse_full_replication_section() {
    let toml_str = r#"
num_workers = 4

[origin]
host = "pgbouncer.example.com"
port = 6432
user = "app_user"
password = "secret"
database = "mydb"

[replication]
host = "postgres.example.com"
port = 5432
user = "replication_user"
password = "replication_secret"
database = "mydb"
ssl_mode = "require"

[cache]
host = "localhost"
port = 5433
user = "cache_user"
database = "cache_db"

[cdc]
publication_name = "test_pub"
slot_name = "test_slot"

[listen]
socket = "127.0.0.1:5434"
"#;

    let settings: SettingsToml = toml::from_str(toml_str).expect("parse TOML");
    let partial = settings.replication.as_ref().expect("replication section");

    let replication = partial.merge_with(&settings.origin);

    assert_eq!(replication.host, "postgres.example.com");
    assert_eq!(replication.port, 5432);
    assert_eq!(replication.user, "replication_user");
    assert_eq!(replication.password, Some("replication_secret".to_owned()));
    assert_eq!(replication.database, "mydb");
    assert_eq!(replication.ssl_mode, SslMode::Require);
}

#[test]
fn test_toml_parse_replication_host_only() {
    let toml_str = r#"
num_workers = 4

[origin]
host = "pgbouncer.example.com"
port = 6432
user = "app_user"
database = "mydb"

[replication]
host = "postgres.example.com"

[cache]
host = "localhost"
port = 5433
user = "cache_user"
database = "cache_db"

[cdc]
publication_name = "test_pub"
slot_name = "test_slot"

[listen]
socket = "127.0.0.1:5434"
"#;

    let settings: SettingsToml = toml::from_str(toml_str).expect("parse TOML");
    let partial = settings.replication.as_ref().expect("replication section");

    let replication = partial.merge_with(&settings.origin);

    // Only host is overridden
    assert_eq!(replication.host, "postgres.example.com");
    // All other fields come from origin
    assert_eq!(replication.port, 6432);
    assert_eq!(replication.user, "app_user");
    assert_eq!(replication.password, None);
    assert_eq!(replication.database, "mydb");
    assert_eq!(replication.ssl_mode, SslMode::Disable);
}

// ==================== replication_settings_resolve Tests ====================

#[test]
fn test_replication_resolve_no_toml_no_cli_defaults_to_origin() {
    let origin = base_settings();

    let result = replication_settings_resolve(&origin, None, PgSettingsPartial::default());

    assert_eq!(result.host, origin.host);
    assert_eq!(result.port, origin.port);
    assert_eq!(result.user, origin.user);
    assert_eq!(result.password, origin.password);
    assert_eq!(result.database, origin.database);
    assert_eq!(result.ssl_mode, origin.ssl_mode);
}

#[test]
fn test_replication_resolve_toml_partial_merges_with_origin() {
    let origin = base_settings();
    let toml_partial = PgSettingsPartial {
        host: Some("replica.example.com".to_owned()),
        port: Some(5433),
        ..Default::default()
    };

    let result =
        replication_settings_resolve(&origin, Some(toml_partial), PgSettingsPartial::default());

    assert_eq!(result.host, "replica.example.com");
    assert_eq!(result.port, 5433);
    assert_eq!(result.user, "base_user");
    assert_eq!(result.password, Some("base_password".to_owned()));
    assert_eq!(result.database, "base_db");
    assert_eq!(result.ssl_mode, SslMode::Disable);
}

#[test]
fn test_replication_resolve_cli_overrides_origin_when_no_toml() {
    let origin = base_settings();
    let cli = PgSettingsPartial {
        host: Some("cli-host.example.com".to_owned()),
        ..Default::default()
    };

    let result = replication_settings_resolve(&origin, None, cli);

    assert_eq!(result.host, "cli-host.example.com");
    assert_eq!(result.port, origin.port);
    assert_eq!(result.user, origin.user);
    assert_eq!(result.password, origin.password);
    assert_eq!(result.database, origin.database);
}

#[test]
fn test_replication_resolve_cli_overrides_toml_partial() {
    let origin = base_settings();
    let toml_partial = PgSettingsPartial {
        host: Some("toml-replica.example.com".to_owned()),
        port: Some(5433),
        ..Default::default()
    };
    let cli = PgSettingsPartial {
        host: Some("cli-replica.example.com".to_owned()),
        ..Default::default()
    };

    let result = replication_settings_resolve(&origin, Some(toml_partial), cli);

    // CLI host wins over TOML host
    assert_eq!(result.host, "cli-replica.example.com");
    // TOML port wins over origin port (CLI didn't specify)
    assert_eq!(result.port, 5433);
    // Remaining fields from origin
    assert_eq!(result.user, "base_user");
    assert_eq!(result.password, Some("base_password".to_owned()));
    assert_eq!(result.database, "base_db");
}

#[test]
fn test_replication_resolve_cli_overrides_all_fields() {
    let origin = base_settings();
    let toml_partial = PgSettingsPartial {
        host: Some("toml-replica.example.com".to_owned()),
        user: Some("toml_user".to_owned()),
        ..Default::default()
    };
    let cli = PgSettingsPartial {
        host: Some("cli-host.example.com".to_owned()),
        port: Some(6432),
        user: Some("cli_user".to_owned()),
        password: Some("cli_password".to_owned()),
        database: Some("cli_db".to_owned()),
        ssl_mode: Some(SslMode::Require),
    };

    let result = replication_settings_resolve(&origin, Some(toml_partial), cli);

    assert_eq!(result.host, "cli-host.example.com");
    assert_eq!(result.port, 6432);
    assert_eq!(result.user, "cli_user");
    assert_eq!(result.password, Some("cli_password".to_owned()));
    assert_eq!(result.database, "cli_db");
    assert_eq!(result.ssl_mode, SslMode::Require);
}

#[test]
fn test_replication_resolve_cli_password_overrides_toml_password() {
    let origin = base_settings();
    let toml_partial = PgSettingsPartial {
        password: Some("toml_password".to_owned()),
        ..Default::default()
    };
    let cli = PgSettingsPartial {
        password: Some("cli_password".to_owned()),
        ..Default::default()
    };

    let result = replication_settings_resolve(&origin, Some(toml_partial), cli);

    assert_eq!(result.password, Some("cli_password".to_owned()));
}

#[test]
fn test_replication_resolve_cli_password_not_set_preserves_toml_password() {
    let origin = base_settings();
    let toml_partial = PgSettingsPartial {
        password: Some("toml_password".to_owned()),
        ..Default::default()
    };

    let result =
        replication_settings_resolve(&origin, Some(toml_partial), PgSettingsPartial::default());

    assert_eq!(result.password, Some("toml_password".to_owned()));
}

#[test]
fn test_replication_resolve_full_toml_with_no_cli_uses_toml() {
    let origin = base_settings();
    let toml_partial = PgSettingsPartial {
        host: Some("replica.example.com".to_owned()),
        port: Some(5433),
        user: Some("repl_user".to_owned()),
        password: Some("repl_pass".to_owned()),
        database: Some("repl_db".to_owned()),
        ssl_mode: Some(SslMode::Require),
    };

    let result =
        replication_settings_resolve(&origin, Some(toml_partial), PgSettingsPartial::default());

    assert_eq!(result.host, "replica.example.com");
    assert_eq!(result.port, 5433);
    assert_eq!(result.user, "repl_user");
    assert_eq!(result.password, Some("repl_pass".to_owned()));
    assert_eq!(result.database, "repl_db");
    assert_eq!(result.ssl_mode, SslMode::Require);
}

#[test]
fn test_replication_resolve_cli_port_only_with_toml_host() {
    let origin = base_settings();
    let toml_partial = PgSettingsPartial {
        host: Some("toml-host.example.com".to_owned()),
        ..Default::default()
    };
    let cli = PgSettingsPartial {
        port: Some(6432),
        ..Default::default()
    };

    let result = replication_settings_resolve(&origin, Some(toml_partial), cli);

    // TOML host preserved
    assert_eq!(result.host, "toml-host.example.com");
    // CLI port applied
    assert_eq!(result.port, 6432);
    // Origin fills the rest
    assert_eq!(result.user, "base_user");
    assert_eq!(result.database, "base_db");
}

// ==================== settings_build Tests ====================

fn base_toml_config() -> SettingsToml {
    SettingsToml {
        population_workers_min: None,
        population_workers_max: None,
        role_poll_interval: None,
        origin: PgSettings {
            host: "origin.example.com".to_owned(),
            port: 5432,
            user: "origin_user".to_owned(),
            password: Some("origin_pass".to_owned()),
            database: "origin_db".to_owned(),
            ssl_mode: SslMode::Disable,
        },
        replication: None,
        cache: Some(PgSettings {
            host: "localhost".to_owned(),
            port: 5433,
            user: "cache_user".to_owned(),
            password: None,
            database: "cache_db".to_owned(),
            ssl_mode: SslMode::Disable,
        }),
        cdc: CdcSettings {
            publication_name: "pub".to_owned(),
            slot_name: "slot".to_owned(),
        },
        listen: ListenSettings {
            socket: "127.0.0.1:6432".parse().expect("valid socket addr"),
        },
        num_workers: 4,
        cache_size: None,
        tls_cert: None,
        tls_key: None,
        metrics: None,
        log_level: None,
        cache_policy: None,
        admission_threshold: None,
        mv_size_ratio: None,
        mv_compute_min_rows: None,
        memo_cache_size: None,
        memory_limit: None,
        disk_limit: None,
        allowed_tables: None,
        excluded_tables: None,
        pinned_queries: None,
        pinned_tables: None,
        telemetry: None,
    }
}

#[test]
fn test_settings_build_no_replication_defaults_to_origin() {
    let config = base_toml_config();
    let args = CliArgs::default();

    let settings = settings_build(args, Some(config), None).expect("build settings");

    assert_eq!(settings.replication.host, "origin.example.com");
    assert_eq!(settings.replication.port, 5432);
    assert_eq!(settings.replication.user, "origin_user");
    assert_eq!(
        settings.replication.password,
        Some("origin_pass".to_owned())
    );
    assert_eq!(settings.replication.database, "origin_db");
}

#[test]
fn test_settings_build_toml_replication_partial_merges_with_origin() {
    let mut config = base_toml_config();
    config.replication = Some(PgSettingsPartial {
        host: Some("replica.example.com".to_owned()),
        ..Default::default()
    });
    let args = CliArgs::default();

    let settings = settings_build(args, Some(config), None).expect("build settings");

    assert_eq!(settings.replication.host, "replica.example.com");
    assert_eq!(settings.replication.port, 5432);
    assert_eq!(settings.replication.user, "origin_user");
    assert_eq!(
        settings.replication.password,
        Some("origin_pass".to_owned())
    );
}

#[test]
fn test_settings_build_cli_replication_overrides_no_toml_section() {
    let config = base_toml_config();
    let args = CliArgs {
        replication: PgSettingsPartial {
            host: Some("cli-replica.example.com".to_owned()),
            port: Some(6432),
            ..Default::default()
        },
        ..Default::default()
    };

    let settings = settings_build(args, Some(config), None).expect("build settings");

    assert_eq!(settings.replication.host, "cli-replica.example.com");
    assert_eq!(settings.replication.port, 6432);
    // Remaining fields cascade from origin
    assert_eq!(settings.replication.user, "origin_user");
    assert_eq!(
        settings.replication.password,
        Some("origin_pass".to_owned())
    );
    assert_eq!(settings.replication.database, "origin_db");
}

#[test]
fn test_settings_build_cli_replication_overrides_toml_replication() {
    let mut config = base_toml_config();
    config.replication = Some(PgSettingsPartial {
        host: Some("toml-replica.example.com".to_owned()),
        port: Some(5433),
        ..Default::default()
    });
    let args = CliArgs {
        replication: PgSettingsPartial {
            host: Some("cli-replica.example.com".to_owned()),
            ..Default::default()
        },
        ..Default::default()
    };

    let settings = settings_build(args, Some(config), None).expect("build settings");

    // CLI host wins over TOML host
    assert_eq!(settings.replication.host, "cli-replica.example.com");
    // TOML port preserved (CLI didn't specify)
    assert_eq!(settings.replication.port, 5433);
    // Origin fills unspecified fields
    assert_eq!(settings.replication.user, "origin_user");
}

#[test]
fn test_settings_build_cli_origin_override_cascades_to_replication() {
    let config = base_toml_config();
    let args = CliArgs {
        origin: PgSettingsPartial {
            host: Some("cli-origin.example.com".to_owned()),
            ..Default::default()
        },
        ..Default::default()
    };

    let settings = settings_build(args, Some(config), None).expect("build settings");

    // Origin was overridden by CLI
    assert_eq!(settings.origin.host, "cli-origin.example.com");
    // Replication inherits the CLI-overridden origin (no TOML replication section)
    assert_eq!(settings.replication.host, "cli-origin.example.com");
}

#[test]
fn test_settings_build_cdc_names_lowercased() {
    let mut config = base_toml_config();
    config.cdc.publication_name = "MY_PUB".to_owned();
    config.cdc.slot_name = "MY_SLOT".to_owned();
    let args = CliArgs::default();

    let settings = settings_build(args, Some(config), None).expect("build settings");

    assert_eq!(settings.cdc.publication_name, "my_pub");
    assert_eq!(settings.cdc.slot_name, "my_slot");
}

// ==================== Config file without [cache] ====================

/// A config file as the site documents it. It has no `[cache]` table: the
/// image's entrypoint gives the cache connection by flag (PGC-636).
const CONFIG_WITHOUT_CACHE_TABLE: &str = r#"
num_workers = 2

[origin]
host = "origin.example.com"
port = 6432
user = "admin"
database = "postgres"

[cdc]
publication_name = "pgcache_pub"
slot_name = "pgcache_slot"

[listen]
socket = "0.0.0.0:5432"

[metrics]
socket = "0.0.0.0:9090"
"#;

/// The cache connection flags the image's entrypoint always passes.
fn cache_flags() -> CliArgs {
    CliArgs {
        cache: PgSettingsPartial {
            host: Some("127.0.0.1".to_owned()),
            port: Some(5433),
            user: Some("postgres".to_owned()),
            database: Some("pgcache_cache".to_owned()),
            ..Default::default()
        },
        ..Default::default()
    }
}

#[test]
fn test_settings_build_config_without_cache_table_takes_cache_flags() {
    let config: SettingsToml =
        toml::from_str(CONFIG_WITHOUT_CACHE_TABLE).expect("parse TOML without [cache]");

    let settings = settings_build(cache_flags(), Some(config), None).expect("build settings");

    assert_eq!(
        settings.cache,
        PgSettings {
            host: "127.0.0.1".to_owned(),
            port: 5433,
            user: "postgres".to_owned(),
            password: None,
            database: "pgcache_cache".to_owned(),
            ssl_mode: SslMode::Disable,
        }
    );
    assert_eq!(settings.origin.host, "origin.example.com");
    assert_eq!(settings.num_workers, 2);
}

#[test]
fn test_settings_build_config_without_cache_table_or_flags_names_the_flag() {
    let config: SettingsToml =
        toml::from_str(CONFIG_WITHOUT_CACHE_TABLE).expect("parse TOML without [cache]");

    let err = settings_build(CliArgs::default(), Some(config), None)
        .expect_err("no cache connection given");

    assert!(err.to_string().contains("cache_host"), "{err}");
}

#[test]
fn test_settings_build_cache_flags_override_cache_table() {
    let mut args = CliArgs::default();
    args.cache.port = Some(7777);

    let settings = settings_build(args, Some(base_toml_config()), None).expect("build settings");

    assert_eq!(settings.cache.port, 7777);
    assert_eq!(settings.cache.host, "localhost");
    assert_eq!(settings.cache.database, "cache_db");
}

/// A file with no `[cache]` table does not differ from the running cache
/// connection, which came from flags; an edited static field still does.
#[test]
fn test_restart_required_ignores_an_absent_cache_table() {
    let dir = std::env::temp_dir().join("pgcache_test_config_without_cache_table");
    fs::create_dir_all(&dir).expect("create temp dir");
    let path = dir.join("config.toml");
    fs::write(&path, CONFIG_WITHOUT_CACHE_TABLE).expect("write test TOML");
    let config: SettingsToml =
        toml::from_str(CONFIG_WITHOUT_CACHE_TABLE).expect("parse TOML without [cache]");

    let settings =
        settings_build(cache_flags(), Some(config), Some(path.clone())).expect("build settings");
    assert!(!settings.dynamic.restart_required());

    let edited = CONFIG_WITHOUT_CACHE_TABLE.replace("num_workers = 2", "num_workers = 3");
    fs::write(&path, edited).expect("write edited TOML");
    assert!(settings.dynamic.restart_required());

    fs::remove_dir_all(&dir).expect("remove temp dir");
}

// ==================== restart_required ====================

/// Settings built from `content` written to a fresh file under `dir_name`,
/// with `args` over it. Returns the file's path for later edits.
fn settings_build_from_file(
    dir_name: &str,
    content: &str,
    args: CliArgs,
) -> (Settings, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(dir_name);
    fs::create_dir_all(&dir).expect("create temp dir");
    let path = dir.join("config.toml");
    fs::write(&path, content).expect("write test TOML");
    let config: SettingsToml = toml::from_str(content).expect("parse test TOML");
    let settings = settings_build(args, Some(config), Some(path.clone())).expect("build settings");
    (settings, path)
}

/// A flag that overrides the file is not an edit to the file: an unedited
/// file never asks for a restart, whatever the flags (PGC-638).
#[test]
fn test_restart_required_false_when_flags_override_an_unedited_file() {
    let mut args = cache_flags();
    args.num_workers = Some(4);
    args.listen_socket = Some("127.0.0.1:7000".parse().expect("valid socket addr"));
    args.cdc_slot_name = Some("Other_Slot".to_owned());
    args.origin.password = Some("from-the-environment".to_owned());
    let content = CONFIG_WITHOUT_CACHE_TABLE.replace("pgcache_pub", "PgCache_Pub");

    let (settings, path) =
        settings_build_from_file("pgcache_test_restart_flags_override", &content, args);

    assert_eq!(settings.cdc.publication_name, "pgcache_pub");
    assert!(!settings.dynamic.restart_required());
    fs::remove_dir_all(path.parent().expect("temp dir")).expect("remove temp dir");
}

/// `restart_required` answers whether a restart would change anything: an
/// edit to a value a flag overrides would not, an edit to any other static
/// value would.
#[test]
fn test_restart_required_follows_what_a_restart_would_change() {
    let mut args = cache_flags();
    args.num_workers = Some(4);
    let (settings, path) = settings_build_from_file(
        "pgcache_test_restart_masked_edit",
        CONFIG_WITHOUT_CACHE_TABLE,
        args,
    );

    let masked = CONFIG_WITHOUT_CACHE_TABLE.replace("num_workers = 2", "num_workers = 8");
    fs::write(&path, &masked).expect("write masked edit");
    assert!(
        !settings.dynamic.restart_required(),
        "the num_workers flag still decides the value"
    );

    let effective = masked.replace("origin.example.com", "other.example.com");
    fs::write(&path, effective).expect("write effective edit");
    assert!(settings.dynamic.restart_required());

    fs::remove_dir_all(path.parent().expect("temp dir")).expect("remove temp dir");
}

// ==================== settings_build CLI-only Tests ====================

/// All required CLI fields populated, no config file.
fn base_cli_args() -> CliArgs {
    CliArgs {
        origin: PgSettingsPartial {
            host: Some("origin.example.com".to_owned()),
            port: Some(5432),
            user: Some("origin_user".to_owned()),
            password: Some("origin_pass".to_owned()),
            database: Some("origin_db".to_owned()),
            ..Default::default()
        },
        cache: PgSettingsPartial {
            host: Some("localhost".to_owned()),
            port: Some(5433),
            user: Some("cache_user".to_owned()),
            database: Some("cache_db".to_owned()),
            ..Default::default()
        },
        cdc_publication_name: Some("pub".to_owned()),
        cdc_slot_name: Some("slot".to_owned()),
        listen_socket: Some("127.0.0.1:6432".parse().expect("valid socket addr")),
        num_workers: Some(4),
        ..Default::default()
    }
}

#[test]
fn test_settings_build_cli_only_replication_defaults_to_origin() {
    let args = base_cli_args();

    let settings = settings_build(args, None, None).expect("build settings");

    assert_eq!(settings.replication.host, "origin.example.com");
    assert_eq!(settings.replication.port, 5432);
    assert_eq!(settings.replication.user, "origin_user");
    assert_eq!(
        settings.replication.password,
        Some("origin_pass".to_owned())
    );
    assert_eq!(settings.replication.database, "origin_db");
    assert_eq!(settings.replication.ssl_mode, SslMode::Disable);
}

#[test]
fn test_settings_build_cli_only_replication_host_override() {
    let args = CliArgs {
        replication: PgSettingsPartial {
            host: Some("replica.example.com".to_owned()),
            ..Default::default()
        },
        ..base_cli_args()
    };

    let settings = settings_build(args, None, None).expect("build settings");

    assert_eq!(settings.replication.host, "replica.example.com");
    // Remaining fields inherited from origin
    assert_eq!(settings.replication.port, 5432);
    assert_eq!(settings.replication.user, "origin_user");
    assert_eq!(
        settings.replication.password,
        Some("origin_pass".to_owned())
    );
    assert_eq!(settings.replication.database, "origin_db");
}

#[test]
fn test_settings_build_cli_only_replication_all_fields_override() {
    let args = CliArgs {
        replication: PgSettingsPartial {
            host: Some("replica.example.com".to_owned()),
            port: Some(6432),
            user: Some("repl_user".to_owned()),
            password: Some("repl_pass".to_owned()),
            database: Some("repl_db".to_owned()),
            ssl_mode: Some(SslMode::Require),
        },
        ..base_cli_args()
    };

    let settings = settings_build(args, None, None).expect("build settings");

    assert_eq!(settings.replication.host, "replica.example.com");
    assert_eq!(settings.replication.port, 6432);
    assert_eq!(settings.replication.user, "repl_user");
    assert_eq!(settings.replication.password, Some("repl_pass".to_owned()));
    assert_eq!(settings.replication.database, "repl_db");
    assert_eq!(settings.replication.ssl_mode, SslMode::Require);
    // Origin unchanged
    assert_eq!(settings.origin.host, "origin.example.com");
    assert_eq!(settings.origin.ssl_mode, SslMode::Disable);
}

#[test]
fn test_settings_build_cli_only_missing_origin_host_errors() {
    let mut args = base_cli_args();
    args.origin.host = None;

    let err = settings_build(args, None, None).expect_err("missing origin_host");
    assert!(err.to_string().contains("origin_host"));
}

#[test]
fn test_settings_build_cli_only_defaults() {
    let args = base_cli_args();

    let settings = settings_build(args, None, None).expect("build settings");

    assert_eq!(settings.origin.ssl_mode, SslMode::Disable);
    let dynamic = settings.dynamic.load();
    assert_eq!(dynamic.cache_policy, CachePolicy::Clock);
    assert_eq!(dynamic.admission_threshold, 1);
    assert_eq!(settings.cache.ssl_mode, SslMode::Disable);
    assert_eq!(settings.cache.password, None);
}

#[test]
fn test_settings_build_cli_only_cdc_names_lowercased() {
    let args = CliArgs {
        cdc_publication_name: Some("MY_PUB".to_owned()),
        cdc_slot_name: Some("MY_SLOT".to_owned()),
        ..base_cli_args()
    };

    let settings = settings_build(args, None, None).expect("build settings");

    assert_eq!(settings.cdc.publication_name, "my_pub");
    assert_eq!(settings.cdc.slot_name, "my_slot");
}

// ==================== pinned_queries Tests ====================

#[test]
fn test_settings_build_pinned_queries_default_none() {
    let args = base_cli_args();
    let settings = settings_build(args, None, None).expect("build settings");
    assert!(settings.pinned_queries.is_none());
}

#[test]
fn test_settings_build_pinned_queries_from_toml() {
    let mut config = base_toml_config();
    config.pinned_queries = Some(vec![
        "SELECT * FROM users".to_owned(),
        "SELECT * FROM orders".to_owned(),
    ]);
    let args = CliArgs::default();

    let settings = settings_build(args, Some(config), None).expect("build settings");

    let pinned = settings.pinned_queries.expect("pinned queries set");
    assert_eq!(pinned.len(), 2);
    assert_eq!(pinned[0], "SELECT * FROM users");
    assert_eq!(pinned[1], "SELECT * FROM orders");
}

#[test]
fn test_settings_build_pinned_queries_cli_semicolon() {
    let args = CliArgs {
        pinned_queries: Some("SELECT id, name FROM a;SELECT id, name FROM b".to_owned()),
        ..base_cli_args()
    };

    let settings = settings_build(args, None, None).expect("build settings");

    let pinned = settings.pinned_queries.expect("pinned queries set");
    assert_eq!(pinned.len(), 2);
    assert_eq!(pinned[0], "SELECT id, name FROM a");
    assert_eq!(pinned[1], "SELECT id, name FROM b");
}

#[test]
fn test_settings_build_pinned_queries_cli_overrides_toml() {
    let mut config = base_toml_config();
    config.pinned_queries = Some(vec!["SELECT * FROM toml_table".to_owned()]);
    let args = CliArgs {
        pinned_queries: Some("SELECT * FROM cli_table".to_owned()),
        ..CliArgs::default()
    };

    let settings = settings_build(args, Some(config), None).expect("build settings");

    let pinned = settings.pinned_queries.expect("pinned queries set");
    assert_eq!(pinned.len(), 1);
    assert_eq!(pinned[0], "SELECT * FROM cli_table");
}

#[test]
fn test_toml_parse_pinned_queries() {
    let toml_str = r#"
num_workers = 4

pinned_queries = [
"SELECT * FROM settings",
"SELECT id, name FROM lookup",
]

[origin]
host = "origin.example.com"
port = 5432
user = "origin_user"
database = "origin_db"

[cache]
host = "localhost"
port = 5433
user = "cache_user"
database = "cache_db"

[cdc]
publication_name = "test_pub"
slot_name = "test_slot"

[listen]
socket = "127.0.0.1:5434"
"#;

    let settings: SettingsToml = toml::from_str(toml_str).expect("parse TOML");

    let pinned = settings.pinned_queries.expect("pinned_queries present");
    assert_eq!(pinned.len(), 2);
    assert_eq!(pinned[0], "SELECT * FROM settings");
    assert_eq!(pinned[1], "SELECT id, name FROM lookup");
}

// ==================== pinned_tables Tests ====================

#[test]
fn test_settings_build_pinned_tables_expands_to_queries() {
    let mut config = base_toml_config();
    config.pinned_tables = Some(vec!["settings".to_owned(), "products".to_owned()]);
    let args = CliArgs::default();

    let settings = settings_build(args, Some(config), None).expect("build settings");

    let pinned = settings.pinned_queries.expect("pinned queries set");
    assert_eq!(pinned.len(), 2);
    assert_eq!(pinned[0], "SELECT * FROM settings");
    assert_eq!(pinned[1], "SELECT * FROM products");
}

#[test]
fn test_settings_build_pinned_tables_merged_with_pinned_queries() {
    let mut config = base_toml_config();
    config.pinned_queries = Some(vec!["SELECT id, name FROM users".to_owned()]);
    config.pinned_tables = Some(vec!["settings".to_owned()]);
    let args = CliArgs::default();

    let settings = settings_build(args, Some(config), None).expect("build settings");

    let pinned = settings.pinned_queries.expect("pinned queries set");
    assert_eq!(pinned.len(), 2);
    assert_eq!(pinned[0], "SELECT id, name FROM users");
    assert_eq!(pinned[1], "SELECT * FROM settings");
}

#[test]
fn test_settings_build_pinned_tables_cli_csv() {
    let args = CliArgs {
        pinned_tables: Some("settings,products".to_owned()),
        ..base_cli_args()
    };

    let settings = settings_build(args, None, None).expect("build settings");

    let pinned = settings.pinned_queries.expect("pinned queries set");
    assert_eq!(pinned.len(), 2);
    assert_eq!(pinned[0], "SELECT * FROM settings");
    assert_eq!(pinned[1], "SELECT * FROM products");
}

#[test]
fn test_settings_build_excluded_tables_cli_csv() {
    let mut args = base_cli_args();
    args.dynamic.excluded_tables = Some("audit_logs, analytics.events".to_owned());

    let settings = settings_build(args, None, None).expect("build settings");

    let dynamic = settings.dynamic.load();
    assert_eq!(
        dynamic.excluded_tables,
        Some(vec!["audit_logs".to_owned(), "analytics.events".to_owned()])
    );
    assert!(dynamic.table_filter.excludes(None, "audit_logs"));
    assert!(dynamic.table_filter.excludes(Some("analytics"), "events"));
    assert!(!dynamic.table_filter.excludes(Some("public"), "events"));
    assert!(!dynamic.table_filter.has_allowlist());
}

#[test]
fn test_settings_build_excluded_tables_cli_overrides_toml() {
    let mut config = base_toml_config();
    config.excluded_tables = Some(vec!["sessions".to_owned()]);
    let from_toml =
        settings_build(CliArgs::default(), Some(config.clone()), None).expect("build settings");
    assert!(
        from_toml
            .dynamic
            .load()
            .table_filter
            .excludes(None, "sessions")
    );

    let mut args = CliArgs::default();
    args.dynamic.excluded_tables = Some("jobs".to_owned());
    let settings = settings_build(args, Some(config), None).expect("build settings");
    let dynamic = settings.dynamic.load();
    assert!(dynamic.table_filter.excludes(None, "jobs"));
    assert!(!dynamic.table_filter.excludes(None, "sessions"));
}

#[test]
fn test_settings_build_pinned_tables_schema_qualified() {
    let mut config = base_toml_config();
    config.pinned_tables = Some(vec!["analytics.events".to_owned()]);
    let args = CliArgs::default();

    let settings = settings_build(args, Some(config), None).expect("build settings");

    let pinned = settings.pinned_queries.expect("pinned queries set");
    assert_eq!(pinned.len(), 1);
    assert_eq!(pinned[0], "SELECT * FROM analytics.events");
}

#[test]
fn test_settings_build_pinned_tables_cli_merges_with_pinned_queries_cli() {
    let args = CliArgs {
        pinned_queries: Some("SELECT id FROM users".to_owned()),
        pinned_tables: Some("settings".to_owned()),
        ..base_cli_args()
    };

    let settings = settings_build(args, None, None).expect("build settings");

    let pinned = settings.pinned_queries.expect("pinned queries set");
    assert_eq!(pinned.len(), 2);
    assert_eq!(pinned[0], "SELECT id FROM users");
    assert_eq!(pinned[1], "SELECT * FROM settings");
}

#[test]
fn test_toml_parse_pinned_tables() {
    let toml_str = r#"
num_workers = 4

pinned_tables = ["settings", "products"]

[origin]
host = "origin.example.com"
port = 5432
user = "origin_user"
database = "origin_db"

[cache]
host = "localhost"
port = 5433
user = "cache_user"
database = "cache_db"

[cdc]
publication_name = "test_pub"
slot_name = "test_slot"

[listen]
socket = "127.0.0.1:5434"
"#;

    let settings: SettingsToml = toml::from_str(toml_str).expect("parse TOML");

    let tables = settings.pinned_tables.expect("pinned_tables present");
    assert_eq!(tables.len(), 2);
    assert_eq!(tables[0], "settings");
    assert_eq!(tables[1], "products");
}

// ==================== DynamicConfigPatch Tests ====================

fn base_dynamic_config() -> DynamicConfig {
    DynamicConfig::new(
        Some(1_000_000),
        Some(CachePolicy::Clock),
        Some(2),
        Some(vec!["public.users".to_owned()]),
        Some(vec!["audit_logs".to_owned()]),
        Some("info".to_owned()),
        None,
        None,
        None,
        None,
        None,
    )
}

#[test]
fn test_config_patch_apply_empty_preserves_current() {
    let current = base_dynamic_config();
    let patch = DynamicConfigPatch {
        cache_size: None,
        cache_policy: None,
        admission_threshold: None,
        allowed_tables: None,
        excluded_tables: None,
        log_level: None,
        mv_size_ratio: None,
        mv_compute_min_rows: None,
        memo_cache_size: None,
        memory_limit: None,
        disk_limit: None,
    };
    let result = patch.apply(&current);
    assert_eq!(result.cache_size, Some(1_000_000));
    assert_eq!(result.cache_policy, CachePolicy::Clock);
    assert_eq!(result.admission_threshold, 2);
    assert_eq!(result.allowed_tables, Some(vec!["public.users".to_owned()]));
    assert_eq!(result.excluded_tables, Some(vec!["audit_logs".to_owned()]));
    assert_eq!(result.table_filter, current.table_filter);
    assert_eq!(result.log_level, Some("info".to_owned()));
    assert_eq!(result.mv_size_ratio, DEFAULT_MV_SIZE_RATIO);
    assert_eq!(result.mv_compute_min_rows, DEFAULT_MV_COMPUTE_MIN_ROWS);
}

#[test]
fn test_config_patch_apply_set_values() {
    let current = base_dynamic_config();
    let patch = DynamicConfigPatch {
        cache_size: Some(Some(2_000_000)),
        cache_policy: Some(CachePolicy::Fifo),
        admission_threshold: Some(5),
        allowed_tables: Some(Some(vec!["orders".to_owned()])),
        excluded_tables: Some(Some(vec!["analytics.events".to_owned()])),
        log_level: Some(Some("debug".to_owned())),
        mv_size_ratio: Some(25),
        mv_compute_min_rows: Some(2000),
        memo_cache_size: Some(8_000_000),
        memory_limit: None,
        disk_limit: None,
    };
    let result = patch.apply(&current);
    assert_eq!(result.cache_size, Some(2_000_000));
    assert_eq!(result.cache_policy, CachePolicy::Fifo);
    assert_eq!(result.admission_threshold, 5);
    assert_eq!(result.memo_cache_size, 8_000_000);
    assert_eq!(result.allowed_tables, Some(vec!["orders".to_owned()]));
    assert_eq!(
        result.excluded_tables,
        Some(vec!["analytics.events".to_owned()])
    );
    assert!(result.table_filter.allows(None, "orders"));
    assert!(!result.table_filter.allows(None, "users"));
    assert!(result.table_filter.excludes(Some("analytics"), "events"));
    assert!(!result.table_filter.excludes(None, "audit_logs"));
    assert_eq!(result.log_level, Some("debug".to_owned()));
    assert_eq!(result.mv_size_ratio, 25);
    assert_eq!(result.mv_compute_min_rows, 2000);
}

#[test]
fn test_config_patch_apply_unset_optional_fields() {
    let current = base_dynamic_config();
    let patch = DynamicConfigPatch {
        cache_size: Some(None),
        cache_policy: None,
        admission_threshold: None,
        allowed_tables: Some(None),
        excluded_tables: Some(None),
        log_level: Some(None),
        mv_size_ratio: None,
        mv_compute_min_rows: None,
        memo_cache_size: None,
        memory_limit: None,
        disk_limit: None,
    };
    let result = patch.apply(&current);
    assert_eq!(result.cache_size, None);
    assert_eq!(result.allowed_tables, None);
    assert_eq!(result.excluded_tables, None);
    assert_eq!(result.table_filter, TableFilter::default());
    assert_eq!(result.log_level, None);
}

#[test]
fn test_config_patch_json_deserialize() {
    let json = r#"{"cache_size": 500, "admission_threshold": 3}"#;
    let patch: DynamicConfigPatch = serde_json::from_str(json).expect("parse JSON");
    assert_eq!(patch.cache_size, Some(Some(500)));
    assert_eq!(patch.admission_threshold, Some(3));
    assert!(patch.cache_policy.is_none());
    assert!(patch.allowed_tables.is_none());
    assert!(patch.excluded_tables.is_none());
    assert!(patch.log_level.is_none());
}

/// Patching one list keeps the other in the parsed filter.
#[test]
fn test_config_patch_excluded_tables_keeps_the_allowlist() {
    let current = base_dynamic_config();
    let json = r#"{"excluded_tables": ["Sessions", "analytics.events"]}"#;
    let patch: DynamicConfigPatch = serde_json::from_str(json).expect("parse JSON");
    let result = patch.apply(&current);
    assert_eq!(result.allowed_tables, Some(vec!["public.users".to_owned()]));
    assert!(result.table_filter.allows(Some("public"), "users"));
    assert!(result.table_filter.excludes(None, "sessions"));
    assert!(!result.table_filter.excludes(None, "audit_logs"));

    let unset: DynamicConfigPatch =
        serde_json::from_str(r#"{"excluded_tables": null}"#).expect("parse JSON");
    assert_eq!(unset.excluded_tables, Some(None));
    let result = unset.apply(&result);
    assert_eq!(result.excluded_tables, None);
    assert!(!result.table_filter.has_exclusions());
    assert!(result.table_filter.has_allowlist());
}

#[test]
fn test_config_patch_json_null_unsets() {
    let json = r#"{"cache_size": null, "log_level": null}"#;
    let patch: DynamicConfigPatch = serde_json::from_str(json).expect("parse JSON");
    assert_eq!(patch.cache_size, Some(None));
    assert_eq!(patch.log_level, Some(None));
}

#[test]
fn test_config_file_toml_round_trip() {
    let toml_content = r#"# Main config
num_workers = 4
cache_size = 1000000
cache_policy = "clock"
admission_threshold = 2
log_level = "info"
allowed_tables = ["public.users"]

[origin]
host = "localhost"
port = 5432
user = "test"
database = "testdb"

[cache]
host = "localhost"
port = 5433
user = "test"
database = "cachedb"

[cdc]
publication_name = "pub"
slot_name = "slot"

[listen]
socket = "127.0.0.1:6432"
"#;

    let dir = std::env::temp_dir().join("pgcache_test_config");
    let _ = fs::create_dir_all(&dir);
    let path = dir.join("test_round_trip.toml");
    fs::write(&path, toml_content).expect("write test TOML");

    // Apply a patch
    let patch = DynamicConfigPatch {
        cache_size: Some(Some(2_000_000)),
        cache_policy: Some(CachePolicy::Fifo),
        admission_threshold: None,
        allowed_tables: None,
        excluded_tables: Some(Some(vec!["audit_logs".to_owned()])),
        log_level: Some(None),
        mv_size_ratio: None,
        mv_compute_min_rows: None,
        memo_cache_size: None,
        memory_limit: None,
        disk_limit: None,
    };
    config_file_dynamic_update(&path, &patch).expect("update TOML");

    // Re-read to verify the changes
    let result =
        config_file_dynamic_extract(&path, &DynamicArgs::default()).expect("extract after update");
    assert_eq!(result.cache_size, Some(2_000_000));
    assert_eq!(result.cache_policy, CachePolicy::Fifo);
    assert_eq!(result.admission_threshold, 2); // unchanged
    assert!(result.log_level.is_none()); // unset
    assert_eq!(result.excluded_tables, Some(vec!["audit_logs".to_owned()]));
    assert!(result.table_filter.excludes(None, "audit_logs"));
    assert_eq!(result.allowed_tables, Some(vec!["public.users".to_owned()]));

    let updated = fs::read_to_string(&path).expect("read updated TOML");
    assert!(updated.contains("# Main config"));
    assert!(updated.contains("cache_size = 2000000"));
    assert!(updated.contains(r#"cache_policy = "fifo""#));
    assert!(!updated.contains("log_level")); // removed

    let _ = fs::remove_file(&path);
}

/// A reload resolves the file under the startup flags: a list a flag set
/// survives a file that does not name it, and a list the file sets is read
/// again from the file.
#[test]
fn test_config_reload_keeps_what_a_flag_set() {
    let dir = config_dir_fresh("pgcache_test_config_reload_flags");
    fs::create_dir_all(&dir).expect("create temp dir");
    let path = dir.join("config.toml");
    let file = |allowed: &str| {
        format!(
            "num_workers = 4\nallowed_tables = [\"{allowed}\"]\n\n\
             [origin]\nhost = \"localhost\"\nport = 5432\nuser = \"test\"\ndatabase = \"testdb\"\n\n\
             [cache]\nhost = \"localhost\"\nport = 5433\nuser = \"test\"\ndatabase = \"cachedb\"\n\n\
             [cdc]\npublication_name = \"pub\"\nslot_name = \"slot\"\n\n\
             [listen]\nsocket = \"127.0.0.1:6432\"\n"
        )
    };
    fs::write(&path, file("users")).expect("write config");
    let config: SettingsToml =
        toml::from_str(&fs::read_to_string(&path).expect("read config")).expect("parse config");
    let mut args = CliArgs::default();
    args.dynamic.excluded_tables = Some("audit_logs".to_owned());
    let settings = settings_build(args, Some(config), Some(path.clone())).expect("build settings");
    assert!(
        settings
            .dynamic
            .load()
            .table_filter
            .excludes(None, "audit_logs")
    );

    fs::write(&path, file("orders")).expect("rewrite config");
    let reloaded = settings
        .dynamic
        .config_file_resolve(&path)
        .expect("resolve the edited file");
    assert_eq!(
        reloaded.excluded_tables,
        Some(vec!["audit_logs".to_owned()])
    );
    assert!(reloaded.table_filter.excludes(None, "audit_logs"));
    assert_eq!(reloaded.allowed_tables, Some(vec!["orders".to_owned()]));

    fs::remove_dir_all(&dir).expect("remove temp dir");
}

#[test]
fn test_population_workers_bounds_defaults_derive_from_num_workers() {
    use super::cli::population_workers_bounds;

    // Small box: floor of 2 applies, ceiling scales with num_workers.
    assert_eq!(population_workers_bounds(2, None, None), (2, 16));
    // Larger box: min follows num_workers.
    assert_eq!(population_workers_bounds(16, None, None), (16, 128));
    // num_workers = 1 still gets the floor of 2.
    assert_eq!(population_workers_bounds(1, None, None), (2, 8));
}

#[test]
fn test_population_workers_bounds_explicit_values_and_clamps() {
    use super::cli::population_workers_bounds;

    assert_eq!(population_workers_bounds(2, Some(4), Some(32)), (4, 32));
    // An explicit max below an *explicit* min: the floor wins (liveness).
    assert_eq!(population_workers_bounds(2, Some(6), Some(3)), (6, 6));
    // min of 0 is raised to 1.
    assert_eq!(population_workers_bounds(2, Some(0), None), (1, 16));
    // An explicit max below the *derived* floor lowers the floor instead of
    // silently discarding the user's ceiling (PGC-457).
    assert_eq!(population_workers_bounds(16, None, Some(4)), (4, 4));
    assert_eq!(population_workers_bounds(2, None, Some(1)), (1, 1));
}

/// A config file that does not parse is reported by message and line only.
/// The file holds passwords, and the parsers' own rendering includes the
/// offending line (`Display`) or the whole document (`Debug`) (PGC-605).
#[test]
fn test_config_parse_error_never_echoes_the_file() {
    let misplaced = "[origin]\nhost = \"h\"\nport = 5432\nuser = \"u\"\npassword = \"s3cret-value\"\n\
                     database = \"d\"\n\n[cache]\nhost = \"h\"\nport = 5432\nuser = \"u\"\ndatabase = \"d\"\n\n\
                     [cdc]\npublication_name = \"p\"\nslot_name = \"s\"\n\n[listen]\nsocket = \"127.0.0.1:1\"\n\n\
                     [metrics]\nsocket = \"127.0.0.1:2\"\nnum_workers = 2\n";
    let error = toml::from_str::<SettingsToml>(misplaced)
        .map_err(|e| toml_parse_error(e.message(), e.span(), misplaced))
        .expect_err("num_workers sits under [metrics]");
    let rendered = format!("{error} / {error:?}");
    assert!(
        rendered.contains("unknown field `num_workers`"),
        "{rendered}"
    );
    assert!(
        rendered.contains(
            "at line 23 (`num_workers` is a top-level key and must come before the first [table] header)"
        ),
        "{rendered}"
    );
    assert!(!rendered.contains("s3cret"), "{rendered}");

    let broken_password_line = "[origin]\nhost = \"h\"\npassword = \"s3cret-value\n";
    let error = toml::from_str::<SettingsToml>(broken_password_line)
        .map_err(|e| toml_parse_error(e.message(), e.span(), broken_password_line))
        .expect_err("unterminated string");
    let rendered = format!("{error} / {error:?}");
    assert!(rendered.contains("at line 3"), "{rendered}");
    assert!(!rendered.contains("s3cret"), "{rendered}");

    let dir = std::env::temp_dir().join("pgcache_test_config_parse_error");
    fs::create_dir_all(&dir).expect("create temp dir");
    let path = dir.join("broken.toml");
    fs::write(&path, broken_password_line).expect("write test TOML");
    let report = config_file_dynamic_extract(&path, &DynamicArgs::default())
        .expect_err("the file does not parse");
    let rendered = format!("{report} / {report:?}");
    assert!(rendered.contains("config file:"), "{rendered}");
    assert!(!rendered.contains("s3cret"), "{rendered}");
    fs::remove_dir_all(&dir).expect("remove temp dir");
}

/// A complete config file, for a test to add a key to.
const VALID_CONFIG: &str = "num_workers = 2\n\n[origin]\nhost = \"h\"\nport = 5432\nuser = \"u\"\n\
     database = \"d\"\n\n[cdc]\npublication_name = \"p\"\nslot_name = \"s\"\n\n\
     [listen]\nsocket = \"127.0.0.1:1\"\n";

fn config_parse_error(content: &str) -> String {
    toml::from_str::<SettingsToml>(content)
        .map_err(|e| toml_parse_error(e.message(), e.span(), content))
        .expect_err("the file names a key no table takes")
        .to_string()
}

/// A key no table takes is refused rather than ignored: an optional key
/// appended below the last `[table]` header, or misspelled, would otherwise
/// never apply and nothing would say so (PGC-644).
#[test]
fn test_config_parse_refuses_unknown_keys() {
    toml::from_str::<SettingsToml>(VALID_CONFIG).expect("the base file parses");

    // A top-level key appended to the end lands in the last table.
    let appended = format!("{VALID_CONFIG}excluded_tables = [\"audit_logs\"]\n");
    let rendered = config_parse_error(&appended);
    assert!(
        rendered.contains("unknown field `excluded_tables`"),
        "{rendered}"
    );
    assert!(
        rendered.contains(
            "at line 15 (`excluded_tables` is a top-level key and must come before the first [table] header)"
        ),
        "{rendered}"
    );

    // A misspelled top-level key is no key anywhere: no hint.
    let misspelled = format!("exclude_tables = [\"audit_logs\"]\n{VALID_CONFIG}");
    let rendered = config_parse_error(&misspelled);
    assert!(
        rendered.contains("unknown field `exclude_tables`"),
        "{rendered}"
    );
    assert!(rendered.contains("at line 1"), "{rendered}");
    assert!(!rendered.contains("[table] header"), "{rendered}");

    // Nor is a key a table does not take and the top level does not either.
    let in_table = VALID_CONFIG.replace("port = 5432\n", "port = 5432\nsslmode = \"require\"\n");
    let rendered = config_parse_error(&in_table);
    assert!(rendered.contains("unknown field `sslmode`"), "{rendered}");
    assert!(!rendered.contains("[table] header"), "{rendered}");
}

/// The misplaced-key hint is only for a key the top level takes. A table
/// the file leaves out, or a field missing from a table while another
/// table sets a key of that name, gets no such hint (PGC-636).
#[test]
fn test_config_parse_error_hints_only_at_a_misplaced_key() {
    let no_cdc_table = "num_workers = 2\n\n[origin]\nhost = \"h\"\nport = 5432\nuser = \"u\"\n\
                        database = \"d\"\n\n[listen]\nsocket = \"127.0.0.1:1\"\n";
    let error = toml::from_str::<SettingsToml>(no_cdc_table)
        .map_err(|e| toml_parse_error(e.message(), e.span(), no_cdc_table))
        .expect_err("the file has no [cdc] table");
    let rendered = error.to_string();
    assert!(rendered.contains("missing field `cdc`"), "{rendered}");
    assert!(!rendered.contains("[table] header"), "{rendered}");

    let no_origin_host = "num_workers = 2\n\n[origin]\nport = 5432\nuser = \"u\"\ndatabase = \"d\"\n\n\
                          [replication]\nhost = \"r\"\n\n[cdc]\npublication_name = \"p\"\n\
                          slot_name = \"s\"\n\n[listen]\nsocket = \"127.0.0.1:1\"\n";
    let error = toml::from_str::<SettingsToml>(no_origin_host)
        .map_err(|e| toml_parse_error(e.message(), e.span(), no_origin_host))
        .expect_err("[origin] has no host");
    let rendered = error.to_string();
    assert!(rendered.contains("missing field `host`"), "{rendered}");
    assert!(!rendered.contains("[table] header"), "{rendered}");
}

// ==================== config_file_create ====================

/// An empty directory under the temp dir for one test's config file.
fn config_dir_fresh(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(name);
    let _ = fs::remove_dir_all(&dir);
    dir
}

/// The flags of a first start in create mode: every required setting, both
/// passwords, and a few optional settings.
fn config_create_args() -> CliArgs {
    let mut args = base_cli_args();
    args.cdc_slot_name = Some("My_Slot".to_owned());
    args.origin.ssl_mode = Some(SslMode::Require);
    args.replication.host = Some("replica.example.com".to_owned());
    args.replication.password = Some("repl_pass".to_owned());
    args.metrics_socket = Some("127.0.0.1:9090".parse().expect("valid socket addr"));
    args.dynamic.cache_policy = Some(CachePolicy::Fifo);
    args.dynamic.allowed_tables = Some("users,audit.orders".to_owned());
    args.dynamic.excluded_tables = Some("sessions,audit.events".to_owned());
    args.pinned_queries = Some("SELECT 1;SELECT 2".to_owned());
    args.telemetry_off = true;
    args
}

/// A created config file holds the required settings and the optional ones a
/// flag set; passwords, the cache connection and unset optional settings
/// stay out (PGC-641).
#[test]
fn test_config_file_create_writes_required_and_flagged_settings_only() {
    let dir = config_dir_fresh("pgcache_test_config_create_content");
    let path = dir.join("nested").join("config.toml");

    config_file_create(&path, &config_create_args()).expect("create config file");
    let content = fs::read_to_string(&path).expect("read created file");

    for written in [
        "num_workers = 4",
        "[origin]",
        "host = \"origin.example.com\"",
        "ssl_mode = \"require\"",
        "[replication]",
        "host = \"replica.example.com\"",
        "publication_name = \"pub\"",
        "slot_name = \"my_slot\"",
        "[listen]",
        "socket = \"127.0.0.1:6432\"",
        "[metrics]",
        "socket = \"127.0.0.1:9090\"",
        "cache_policy = \"fifo\"",
        "allowed_tables = [\"users\", \"audit.orders\"]",
        "excluded_tables = [\"sessions\", \"audit.events\"]",
        "pinned_queries = [\"SELECT 1\", \"SELECT 2\"]",
        "telemetry = false",
    ] {
        assert!(
            content.contains(written),
            "missing `{written}` in:\n{content}"
        );
    }
    for left_out in [
        "origin_pass",
        "repl_pass",
        "password =",
        "[cache]",
        "cache_user",
        "admission_threshold",
        "memo_cache_size",
        "disk_limit",
        "log_level",
        "population_workers",
    ] {
        assert!(
            !content.contains(left_out),
            "`{left_out}` should not be in:\n{content}"
        );
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(&path)
            .expect("stat created file")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    fs::remove_dir_all(&dir).expect("remove temp dir");
}

/// The start that creates the file and a later start from the file resolve to
/// the settings the flags alone give. The later start passes only what the
/// file does not hold: the cache connection and the passwords.
#[test]
fn test_config_file_create_round_trips_to_the_same_settings() {
    let dir = config_dir_fresh("pgcache_test_config_create_round_trip");
    let path = dir.join("config.toml");
    let from_flags = settings_build(config_create_args(), None, None).expect("build from flags");

    let created = config_file_create(&path, &config_create_args()).expect("create config file");
    let first = settings_build(config_create_args(), Some(created), Some(path.clone()))
        .expect("build the creating start");

    let mut later_args = CliArgs {
        cache: base_cli_args().cache,
        ..Default::default()
    };
    later_args.origin.password = Some("origin_pass".to_owned());
    later_args.replication.password = Some("repl_pass".to_owned());
    let reread: SettingsToml =
        toml::from_str(&fs::read_to_string(&path).expect("read created file"))
            .expect("parse created file");
    let later = settings_build(later_args, Some(reread), Some(path)).expect("build a later start");

    for settings in [&first, &later] {
        assert_eq!(settings.origin, from_flags.origin);
        assert_eq!(settings.replication, from_flags.replication);
        assert_eq!(settings.cache, from_flags.cache);
        assert_eq!(settings.cdc, from_flags.cdc);
        assert_eq!(settings.listen, from_flags.listen);
        assert_eq!(settings.num_workers, from_flags.num_workers);
        assert_eq!(
            settings.metrics.as_ref().map(|m| m.socket),
            from_flags.metrics.as_ref().map(|m| m.socket)
        );
        assert_eq!(settings.pinned_queries, from_flags.pinned_queries);
        assert_eq!(settings.telemetry, from_flags.telemetry);
        let (dynamic, expected) = (settings.dynamic.load(), from_flags.dynamic.load());
        assert_eq!(dynamic.cache_policy, expected.cache_policy);
        assert_eq!(dynamic.allowed_tables, expected.allowed_tables);
        assert_eq!(dynamic.excluded_tables, expected.excluded_tables);
        assert_eq!(dynamic.admission_threshold, expected.admission_threshold);
        assert!(!settings.dynamic.restart_required());
    }

    fs::remove_dir_all(&dir).expect("remove temp dir");
}

#[test]
fn test_config_file_create_leaves_an_existing_file_alone() {
    let dir = config_dir_fresh("pgcache_test_config_create_existing");
    fs::create_dir_all(&dir).expect("create temp dir");
    let path = dir.join("config.toml");
    fs::write(&path, "kept").expect("write existing file");

    config_file_create(&path, &config_create_args()).expect_err("the file already exists");

    assert_eq!(
        fs::read_to_string(&path).expect("read existing file"),
        "kept"
    );
    fs::remove_dir_all(&dir).expect("remove temp dir");
}

#[test]
fn test_config_file_create_without_a_required_flag_writes_nothing() {
    let dir = config_dir_fresh("pgcache_test_config_create_incomplete");
    let path = dir.join("config.toml");
    let mut args = config_create_args();
    args.num_workers = None;

    let err = config_file_create(&path, &args).expect_err("num_workers is not given");

    assert!(err.to_string().contains("num_workers"), "{err}");
    assert!(!path.exists());
}
