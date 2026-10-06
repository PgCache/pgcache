use std::fmt;

/// A query generation: the garbage-collection epoch `pgcache_pgrx` stamps on
/// cached rows (ADR-044). Zero is reserved for CDC-written rows; allocated
/// generations start at 1. Backed by `i64` because the cache database stores
/// it as `bigint`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Generation(i64);

impl Generation {
    pub const ZERO: Self = Self(0);

    /// The generation after this one. Saturates rather than wrapping; reaching
    /// `i64::MAX` would take ~9×10¹⁸ admissions.
    pub fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    /// The generation before this one, never below [`Generation::ZERO`].
    pub fn previous(self) -> Self {
        Self(self.0.saturating_sub(1).max(0))
    }

    /// The value as the cache database stores it.
    pub fn get(self) -> i64 {
        self.0
    }

    #[cfg(test)]
    pub(crate) const fn from_raw(value: i64) -> Self {
        Self(value)
    }
}

impl fmt::Display for Generation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
