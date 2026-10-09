//! The parsed `search_path` value: pure, safe string parsing and resolution.
//! Which statements change it is the analyzer's business
//! (`SettingsEffect`), and the per-connection state lives in
//! `proxy::session_settings`.

mod value;

pub use value::{SearchPath, SearchPathEntry};
