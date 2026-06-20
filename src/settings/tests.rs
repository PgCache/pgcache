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
fn partial_merge_empty_uses_all_base_values() {
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
fn partial_merge_host_only_override() {
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
fn partial_merge_multiple_fields_override() {
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
fn partial_merge_all_fields_override() {
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
fn partial_merge_password_override_when_base_has_none() {
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
fn partial_merge_password_inherited_when_partial_has_none() {
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
fn partial_merge_both_passwords_none() {
    let mut base = base_settings();
    base.password = None;

    let partial = PgSettingsPartial::default();

    let result = partial.merge_with(&base);

    assert_eq!(result.password, None);
}

#[test]
fn toml_parse_no_replication_section() {
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
fn toml_parse_partial_replication_section() {
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
fn toml_parse_full_replication_section() {
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
fn toml_parse_replication_host_only() {
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
fn replication_resolve_no_toml_no_cli_defaults_to_origin() {
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
fn replication_resolve_toml_partial_merges_with_origin() {
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
fn replication_resolve_cli_overrides_origin_when_no_toml() {
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
fn replication_resolve_cli_overrides_toml_partial() {
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
fn replication_resolve_cli_overrides_all_fields() {
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
fn replication_resolve_cli_password_overrides_toml_password() {
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
fn replication_resolve_cli_password_not_set_preserves_toml_password() {
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
fn replication_resolve_full_toml_with_no_cli_uses_toml() {
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
fn replication_resolve_cli_port_only_with_toml_host() {
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
        origin: PgSettings {
            host: "origin.example.com".to_owned(),
            port: 5432,
            user: "origin_user".to_owned(),
            password: Some("origin_pass".to_owned()),
            database: "origin_db".to_owned(),
            ssl_mode: SslMode::Disable,
        },
        replication: None,
        cache: PgSettings {
            host: "localhost".to_owned(),
            port: 5433,
            user: "cache_user".to_owned(),
            password: None,
            database: "cache_db".to_owned(),
            ssl_mode: SslMode::Disable,
        },
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
        memo_cache_size: None,
        memory_limit: None,
        disk_limit: None,
        allowed_tables: None,
        pinned_queries: None,
        pinned_tables: None,
        telemetry: None,
    }
}

#[test]
fn settings_build_no_replication_defaults_to_origin() {
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
fn settings_build_toml_replication_partial_merges_with_origin() {
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
fn settings_build_cli_replication_overrides_no_toml_section() {
    let config = base_toml_config();
    let args = CliArgs {
        replication_host: Some("cli-replica.example.com".to_owned()),
        replication_port: Some(6432),
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
fn settings_build_cli_replication_overrides_toml_replication() {
    let mut config = base_toml_config();
    config.replication = Some(PgSettingsPartial {
        host: Some("toml-replica.example.com".to_owned()),
        port: Some(5433),
        ..Default::default()
    });
    let args = CliArgs {
        replication_host: Some("cli-replica.example.com".to_owned()),
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
fn settings_build_cli_origin_override_cascades_to_replication() {
    let config = base_toml_config();
    let args = CliArgs {
        origin_host: Some("cli-origin.example.com".to_owned()),
        ..Default::default()
    };

    let settings = settings_build(args, Some(config), None).expect("build settings");

    // Origin was overridden by CLI
    assert_eq!(settings.origin.host, "cli-origin.example.com");
    // Replication inherits the CLI-overridden origin (no TOML replication section)
    assert_eq!(settings.replication.host, "cli-origin.example.com");
}

#[test]
fn settings_build_cdc_names_lowercased() {
    let mut config = base_toml_config();
    config.cdc.publication_name = "MY_PUB".to_owned();
    config.cdc.slot_name = "MY_SLOT".to_owned();
    let args = CliArgs::default();

    let settings = settings_build(args, Some(config), None).expect("build settings");

    assert_eq!(settings.cdc.publication_name, "my_pub");
    assert_eq!(settings.cdc.slot_name, "my_slot");
}

// ==================== settings_build CLI-only Tests ====================

/// All required CLI fields populated, no config file.
fn base_cli_args() -> CliArgs {
    CliArgs {
        origin_host: Some("origin.example.com".to_owned()),
        origin_port: Some(5432),
        origin_user: Some("origin_user".to_owned()),
        origin_password: Some("origin_pass".to_owned()),
        origin_database: Some("origin_db".to_owned()),
        cache_host: Some("localhost".to_owned()),
        cache_port: Some(5433),
        cache_user: Some("cache_user".to_owned()),
        cache_database: Some("cache_db".to_owned()),
        cdc_publication_name: Some("pub".to_owned()),
        cdc_slot_name: Some("slot".to_owned()),
        listen_socket: Some("127.0.0.1:6432".parse().expect("valid socket addr")),
        num_workers: Some(4),
        ..Default::default()
    }
}

#[test]
fn settings_build_cli_only_replication_defaults_to_origin() {
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
fn settings_build_cli_only_replication_host_override() {
    let args = CliArgs {
        replication_host: Some("replica.example.com".to_owned()),
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
fn settings_build_cli_only_replication_all_fields_override() {
    let args = CliArgs {
        replication_host: Some("replica.example.com".to_owned()),
        replication_port: Some(6432),
        replication_user: Some("repl_user".to_owned()),
        replication_password: Some("repl_pass".to_owned()),
        replication_database: Some("repl_db".to_owned()),
        replication_ssl_mode: Some(SslMode::Require),
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
fn settings_build_cli_only_missing_origin_host_errors() {
    let mut args = base_cli_args();
    args.origin_host = None;

    let err = settings_build(args, None, None).expect_err("missing origin_host");
    assert!(err.to_string().contains("origin_host"));
}

#[test]
fn settings_build_cli_only_defaults() {
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
fn settings_build_cli_only_cdc_names_lowercased() {
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
fn settings_build_pinned_queries_default_none() {
    let args = base_cli_args();
    let settings = settings_build(args, None, None).expect("build settings");
    assert!(settings.pinned_queries.is_none());
}

#[test]
fn settings_build_pinned_queries_from_toml() {
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
fn settings_build_pinned_queries_cli_semicolon() {
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
fn settings_build_pinned_queries_cli_overrides_toml() {
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
fn toml_parse_pinned_queries() {
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
fn settings_build_pinned_tables_expands_to_queries() {
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
fn settings_build_pinned_tables_merged_with_pinned_queries() {
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
fn settings_build_pinned_tables_cli_csv() {
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
fn settings_build_pinned_tables_schema_qualified() {
    let mut config = base_toml_config();
    config.pinned_tables = Some(vec!["analytics.events".to_owned()]);
    let args = CliArgs::default();

    let settings = settings_build(args, Some(config), None).expect("build settings");

    let pinned = settings.pinned_queries.expect("pinned queries set");
    assert_eq!(pinned.len(), 1);
    assert_eq!(pinned[0], "SELECT * FROM analytics.events");
}

#[test]
fn settings_build_pinned_tables_cli_merges_with_pinned_queries_cli() {
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
fn toml_parse_pinned_tables() {
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
        Some("info".to_owned()),
        None,
        None,
        None,
        None,
    )
}

#[test]
fn config_patch_apply_empty_preserves_current() {
    let current = base_dynamic_config();
    let patch = DynamicConfigPatch {
        cache_size: None,
        cache_policy: None,
        admission_threshold: None,
        allowed_tables: None,
        log_level: None,
        mv_size_ratio: None,
        memo_cache_size: None,
        memory_limit: None,
        disk_limit: None,
    };
    let result = patch.apply(&current);
    assert_eq!(result.cache_size, Some(1_000_000));
    assert_eq!(result.cache_policy, CachePolicy::Clock);
    assert_eq!(result.admission_threshold, 2);
    assert_eq!(result.allowed_tables, Some(vec!["public.users".to_owned()]));
    assert_eq!(result.log_level, Some("info".to_owned()));
    assert_eq!(result.mv_size_ratio, DEFAULT_MV_SIZE_RATIO);
}

#[test]
fn config_patch_apply_set_values() {
    let current = base_dynamic_config();
    let patch = DynamicConfigPatch {
        cache_size: Some(Some(2_000_000)),
        cache_policy: Some(CachePolicy::Fifo),
        admission_threshold: Some(5),
        allowed_tables: Some(Some(vec!["orders".to_owned()])),
        log_level: Some(Some("debug".to_owned())),
        mv_size_ratio: Some(25),
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
    assert_eq!(result.log_level, Some("debug".to_owned()));
    assert_eq!(result.mv_size_ratio, 25);
}

#[test]
fn config_patch_apply_unset_optional_fields() {
    let current = base_dynamic_config();
    let patch = DynamicConfigPatch {
        cache_size: Some(None),
        cache_policy: None,
        admission_threshold: None,
        allowed_tables: Some(None),
        log_level: Some(None),
        mv_size_ratio: None,
        memo_cache_size: None,
        memory_limit: None,
        disk_limit: None,
    };
    let result = patch.apply(&current);
    assert_eq!(result.cache_size, None);
    assert_eq!(result.allowed_tables, None);
    assert!(result.allowed_tables_parsed.is_none());
    assert_eq!(result.log_level, None);
}

#[test]
fn config_patch_json_deserialize() {
    let json = r#"{"cache_size": 500, "admission_threshold": 3}"#;
    let patch: DynamicConfigPatch = serde_json::from_str(json).expect("parse JSON");
    assert_eq!(patch.cache_size, Some(Some(500)));
    assert_eq!(patch.admission_threshold, Some(3));
    assert!(patch.cache_policy.is_none());
    assert!(patch.allowed_tables.is_none());
    assert!(patch.log_level.is_none());
}

#[test]
fn config_patch_json_null_unsets() {
    let json = r#"{"cache_size": null, "log_level": null}"#;
    let patch: DynamicConfigPatch = serde_json::from_str(json).expect("parse JSON");
    assert_eq!(patch.cache_size, Some(None));
    assert_eq!(patch.log_level, Some(None));
}

#[test]
fn config_file_toml_round_trip() {
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
        log_level: Some(None),
        mv_size_ratio: None,
        memo_cache_size: None,
        memory_limit: None,
        disk_limit: None,
    };
    config_file_dynamic_update(&path, &patch).expect("update TOML");

    // Re-read to verify the changes
    let result = config_file_dynamic_extract(&path).expect("extract after update");
    assert_eq!(result.cache_size, Some(2_000_000));
    assert_eq!(result.cache_policy, CachePolicy::Fifo);
    assert_eq!(result.admission_threshold, 2); // unchanged
    assert!(result.log_level.is_none()); // unset
    assert_eq!(result.allowed_tables, Some(vec!["public.users".to_owned()]));

    let updated = fs::read_to_string(&path).expect("read updated TOML");
    assert!(updated.contains("# Main config"));
    assert!(updated.contains("cache_size = 2000000"));
    assert!(updated.contains(r#"cache_policy = "fifo""#));
    assert!(!updated.contains("log_level")); // removed

    let _ = fs::remove_file(&path);
}
