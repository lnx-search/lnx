use std::fmt::{Debug, Formatter};
use std::ops::{Deref, DerefMut};
use std::time::Duration;

#[derive(Debug, Clone, Default)]
/// Statistics relating to the read of a file blob.
pub struct ReadStatistics {
    /// The number of hits on the file cache.
    pub cache_hits: usize,
    /// The number of misses on the file cache.
    pub cache_misses: usize,
    /// The amount of bytes that had been cached and did
    /// not need to incur IO on the disk.
    pub cached_bytes: u64,
    /// The amount of bytes that had to be read from the disk
    /// incurring an IO cost.
    pub io_bytes: u64,
    /// The time the system spent scheduling the read.
    pub schedule_time: Duration,
}

#[derive(Debug, Clone, Default)]
/// Statistics relating to the writing of a file blob.
pub struct WriteStatistics {
    /// The total amount of bytes written.
    pub io_bytes: u64,
    /// How many entries from the file cache were removed.
    pub cache_evictions: usize,
    /// The total amount of bytes evicted from the cache.
    pub evicted_bytes: u64,
}

#[derive(Debug, Copy, Clone)]
/// Statistics returning is enabled.
pub struct On;
#[derive(Debug, Copy, Clone)]
/// Statistics returning is disabled.
pub struct Off;

/// A trait to indicate if statistics are enabled or
/// disabled via the type system and provides a type wrapper
/// for transparently returning statistics and the result
/// or just the result.
pub trait StatisticsEnabled: Send + Sync + 'static {
    type Wrapped<T, S>;

    /// Wrap the inner `T` with the stats `S` depending on the impl rules.
    fn wrap<T, S>(inner: T, stats: S) -> Self::Wrapped<T, S>;
}

impl StatisticsEnabled for On {
    type Wrapped<T, S> = WithStats<T, S>;

    #[inline]
    fn wrap<T, S>(inner: T, stats: S) -> Self::Wrapped<T, S> {
        WithStats { inner, stats }
    }
}

impl StatisticsEnabled for Off {
    type Wrapped<T, S> = T;

    #[inline]
    fn wrap<T, S>(inner: T, _stats: S) -> Self::Wrapped<T, S> {
        inner
    }
}

/// A wrapper type around an inner `T` and some statistics `S` associated with the
/// creation of `T`.
pub struct WithStats<T, S> {
    pub inner: T,
    pub stats: S,
}

impl<T, S> Deref for WithStats<T, S> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T, S> DerefMut for WithStats<T, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<T, S> Debug for WithStats<T, S>
where
    T: Debug,
    S: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WithStats")
            .field("inner", &self.inner)
            .field("stats", &self.stats)
            .finish()
    }
}
