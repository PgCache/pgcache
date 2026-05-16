//! Tail the pgcache log file over a statement's time window and scan
//! for swallowed errors.
//!
//! A matching log line fails the statement even when the result set
//! happened to match origin — this is the check PGC-102 needs, where a
//! wrong result was produced silently while the error was only logged.

use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

/// Substrings that mark a swallowed error in the pgcache log.
pub const DEFAULT_PATTERNS: &[&str] = &["mv build failed", "ERROR"];

pub struct LogTailer {
    path: PathBuf,
    /// Byte offset marking the start of the current statement's window.
    mark: u64,
    patterns: Vec<String>,
}

impl LogTailer {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            path: path.as_ref().to_path_buf(),
            mark: 0,
            patterns: DEFAULT_PATTERNS.iter().map(|s| s.to_string()).collect(),
        })
    }

    /// Override the default swallowed-error patterns.
    pub fn with_patterns(mut self, patterns: Vec<String>) -> Self {
        self.patterns = patterns;
        self
    }

    fn current_len(&self) -> Result<u64> {
        match std::fs::metadata(&self.path) {
            Ok(m) => Ok(m.len()),
            // The file may not exist yet if pgcache has not logged
            // anything; treat that as an empty log rather than an error.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(e) => Err(e).with_context(|| format!("stat {}", self.path.display())),
        }
    }

    /// Record the current end-of-file as the start of a statement window.
    pub fn mark(&mut self) -> Result<()> {
        self.mark = self.current_len()?;
        Ok(())
    }

    /// Return any offending lines written since the last [`mark`], and
    /// advance the mark to the new end-of-file.
    ///
    /// A truncated/rotated log (length shrank below the mark) resets the
    /// window to the whole file rather than panicking on a bad seek.
    pub fn offending_since_mark(&mut self) -> Result<Vec<String>> {
        let len = self.current_len()?;
        if len == 0 {
            return Ok(Vec::new());
        }
        let start = if len < self.mark { 0 } else { self.mark };

        let mut file = std::fs::File::open(&self.path)
            .with_context(|| format!("open {}", self.path.display()))?;
        file.seek(SeekFrom::Start(start))
            .with_context(|| format!("seek {} to {start}", self.path.display()))?;
        let mut buf = String::new();
        file.read_to_string(&mut buf)
            .with_context(|| format!("read {}", self.path.display()))?;

        self.mark = len;

        Ok(buf
            .lines()
            .filter(|line| self.patterns.iter().any(|p| line.contains(p)))
            .map(|line| line.to_string())
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    fn temp_log() -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "pgcache-conf-log-{}-{}.log",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        p
    }

    fn append(path: &Path, s: &str) {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        f.write_all(s.as_bytes()).unwrap();
    }

    #[test]
    fn detects_only_lines_in_the_window() {
        let path = temp_log();
        append(&path, "INFO startup\nINFO ready\n");

        let mut t = LogTailer::open(&path).unwrap();
        t.mark().unwrap();

        append(&path, "INFO serving query\n");
        append(&path, "ERROR mv build failed for fp 7\n");

        let hits = t.offending_since_mark().unwrap();
        assert_eq!(hits.len(), 1);
        assert!(hits[0].contains("mv build failed"));

        // Pre-window ERROR-free run produces nothing on the next window.
        append(&path, "INFO another query\n");
        assert!(t.offending_since_mark().unwrap().is_empty());

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn missing_file_is_empty_not_error() {
        let path = temp_log();
        let mut t = LogTailer::open(&path).unwrap();
        t.mark().unwrap();
        assert!(t.offending_since_mark().unwrap().is_empty());
    }
}
