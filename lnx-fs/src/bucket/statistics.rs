use std::fmt::Debug;
use std::time::Duration;

#[derive(Debug, Clone)]
/// Statistics relating to the read of a file blob.
pub struct ReadStatistics {
    /// The number of hits on the file cache.
    pub cache_hits: usize,
    /// The number of misses on the file cache.
    pub cache_misses: usize,
    /// The amount of bytes that had been cached and did
    /// not need to incur IO on the disk.
    pub cached_bytes: usize,
    /// The amount of bytes that had to be read from the disk
    /// incurring an IO cost.
    pub io_bytes: usize,
    /// The time the system spent scheduling the read.
    pub schedule_time: Duration,
}

#[derive(Debug, Clone)]
/// Statistics relating to the writing of a file blob.
pub struct WriteStatistics {
    /// The total amount of bytes written.
    pub io_bytes: usize,
    /// How many entries from the file cache were removed.
    pub cache_evictions: usize,
}
