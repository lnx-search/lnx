use std::fmt::Debug;
use std::io;
use std::io::ErrorKind;
use std::ops::RangeBounds;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use moka::policy::EvictionPolicy;
use tracing::{debug, info, instrument, trace};

use crate::config::{DEFAULT_MAX_OPEN_READERS, DEFAULT_TTI_SECS};
use crate::fscache::{CacheParts, FileSystemCache, MaybeCached};
use crate::io::{Positions, RuntimeDispatcher, TabletReader, TabletReaderOptions};
use crate::metastore::{Metastore, TabletId};
use crate::statistics::{ReadStatistics, StatisticsEnabled, WriteStatistics};
use crate::{fscache, Body, BodySender, BucketConfig, FileSystemError};

/// Handles reading file within the bucket and caching common operations.
pub(super) struct BucketReader {
    /// An LRU cache of open tablet readers.
    ///
    /// The size of the cache can be configured to hold a variable number
    /// of open files to help minimize file descriptor errors.
    readers: moka::sync::Cache<TabletId, TabletReader, ahash::RandomState>,
    /// The IO runtime for the bucket.
    runtime: RuntimeDispatcher,
    /// The bucket metastore.
    metastore: Metastore,
    /// A file cache which holds chunks of files in memory for
    /// speeding up IO requests.
    cache: FileSystemCache,
    /// The bucket config.
    config: BucketConfig,
    /// The path where tablet files are kept.
    tablets_path: PathBuf,
}

impl BucketReader {
    pub(super) fn new(
        config: BucketConfig,
        tablets_path: PathBuf,
        runtime: RuntimeDispatcher,
        metastore: Metastore,
    ) -> Self {
        let max_open_readers = config
            .max_open_readers()
            .unwrap_or(DEFAULT_MAX_OPEN_READERS);
        let time_to_idle = Duration::from_secs(
            config
                .readers_time_to_idle_secs()
                .unwrap_or(DEFAULT_TTI_SECS),
        );

        info!(
            max_open_readers = max_open_readers,
            time_to_idle = ?time_to_idle,
            "Creating bucket reader",
        );
        let readers = moka::sync::CacheBuilder::new(max_open_readers as u64)
            .eviction_policy(EvictionPolicy::lru())
            .time_to_idle(time_to_idle)
            .build_with_hasher(ahash::RandomState::new());

        let cache_options = fscache::FileSystemCacheOptions::builder()
            .maybe_cache_capacity_bytes(config.read_cache_capacity_bytes())
            .build();
        let cache = FileSystemCache::new(cache_options);

        Self {
            readers,
            runtime,
            metastore,
            cache,
            config,
            tablets_path,
        }
    }

    #[instrument(skip(self))]
    /// Read part of the contents of the given file if it exists.
    ///
    /// This may return statistics if S = [statistics::On].
    pub(super) async fn read_range<S, R>(
        &self,
        path: &str,
        range: R,
    ) -> Result<S::Wrapped<Body, ReadStatistics>, FileSystemError>
    where
        S: StatisticsEnabled,
        R: RangeBounds<u64> + Debug,
    {
        let mut statistics = ReadStatistics::default();

        trace!("Begin reading blob");

        let entry = self
            .metastore
            .get_file(path)
            .ok_or_else(|| FileSystemError::FileNotFound(path.to_string()))?;

        let relative_pos = entry
            .resolve_range_bounds(range)
            .map_err(|range| FileSystemError::ReadOutOfRange(path.to_string(), range))?;

        let parts = self.cache.lookup(path, relative_pos, entry.metadata.size());
        for entry in parts.iter() {
            match entry {
                MaybeCached::Hit(chunk) => {
                    statistics.cached_bytes += chunk.len() as u64;
                    statistics.cache_hits += 1;
                },
                MaybeCached::Missed { aligned_pos, .. } => {
                    statistics.io_bytes += aligned_pos.end - aligned_pos.start;
                    statistics.cache_misses += 1;
                },
            }
        }

        if statistics.cache_misses == 0 {
            let (tx, body) = Body::channel_with_capacity(parts.len() + 1);

            let chunks = parts.into_iter().flat_map(|entry| match entry {
                MaybeCached::Hit(chunk) => Some(chunk),
                _ => None,
            });

            // Cannot await here because it'll cause a deadlock as the only reader is currently
            // `body`. This should never panic because the capacity is at least the length of chunks + 1.
            for chunk in chunks {
                tx.try_send(chunk).unwrap();
            }
            tx.try_finish().unwrap();
            return Ok(S::wrap(body, statistics));
        }

        let schedule_start = Instant::now();

        let positions = parts
            .iter()
            .filter_map(|entry| match entry {
                MaybeCached::Hit(_) => None,
                MaybeCached::Missed { aligned_pos, .. } => Some(aligned_pos),
            })
            .cloned()
            .collect::<Positions>();

        let reader = self.get_or_create_reader(entry.metadata.tablet_id).await?;
        let incoming = reader.read_many(positions).await?;
        let cache = self.cache.clone();

        let (tx, body) = Body::channel();
        tokio::spawn(interleave_cached_and_uncached_results(
            path.to_string(),
            cache,
            parts,
            incoming,
            tx,
        ));

        statistics.schedule_time = schedule_start.elapsed();

        Ok(S::wrap(body, statistics))
    }

    async fn get_or_create_reader(
        &self,
        tablet_id: TabletId,
    ) -> Result<TabletReader, FileSystemError> {
        if let Some(reader) = self.readers.get(&tablet_id) {
            return Ok(reader);
        }

        debug!("Reader is not cached, creating new");
        let options = TabletReaderOptions::builder()
            .base_path(self.tablets_path.clone())
            .tablet_id(tablet_id)
            .maybe_max_concurrent_reads(self.config.max_concurrent_tablet_reads())
            .build();

        let reader = TabletReader::open(options, self.runtime.clone()).await?;
        self.readers.insert(tablet_id, reader.clone());

        Ok(reader)
    }

    pub(super) fn evict_path_from_cache(
        &self,
        path: &str,
        statistics: &mut WriteStatistics,
    ) {
        if let Some(old) = self.metastore.get_file(path) {
            let (num_entries, num_bytes) =
                self.cache.evict(&old.path, 0..old.metadata.size());
            statistics.cache_evictions += num_entries;
            statistics.evicted_bytes += num_bytes;
        }
    }
}

/// Interleaves the cached chunks of the file with the read results from the incoming
/// IO stream while maintaining the order of results.
async fn interleave_cached_and_uncached_results(
    path: String,
    cache: FileSystemCache,
    parts: CacheParts,
    io_stream: Body,
    sender: BodySender,
) {
    let len = parts.len();
    let mut cursor = 0;
    for maybe_cached in parts {
        cursor += 1;

        let did_send = match maybe_cached {
            MaybeCached::Hit(chunk) => sender.send(chunk).await,
            MaybeCached::Missed {
                true_pos,
                aligned_pos,
            } => {
                let chunk = io_stream.next().await.and_then(|maybe_chunk| {
                    maybe_chunk.ok_or_else(|| {
                        io::Error::new(
                            ErrorKind::Interrupted,
                            "File reader finished before returning all expected chunks",
                        )
                    })
                });

                match chunk {
                    Err(e) => {
                        sender.error(e).await;
                        return;
                    },
                    Ok(chunk) => {
                        cache.insert(&path, aligned_pos, chunk.clone(), cursor == len);
                        sender.send(chunk.slice(true_pos)).await
                    },
                }
            },
        };

        if !did_send {
            return;
        }
    }

    sender.finish().await;
}
