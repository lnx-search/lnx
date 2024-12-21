use std::cmp;
use std::hash::{Hash, Hasher};
use std::ops::Range;

use bon::Builder;
use bytes::Bytes;
use moka::policy::EvictionPolicy;
use smallvec::SmallVec;

use crate::config::READ_SPLIT_SIZE;

const DEFAULT_CACHE_SIZE_BYTES: u64 = 1 << 30;
const CACHE_BLOCK_SIZE: u64 = READ_SPLIT_SIZE;

#[derive(Debug, Builder)]
/// Cache configuration options.
pub struct FileSystemCacheOptions {
    #[builder(default = DEFAULT_CACHE_SIZE_BYTES)]
    /// The maximum size the cache is allowed to grow to in bytes.
    ///
    /// If this is not set, the system will allow upto 1GB of cache.
    cache_capacity_bytes: u64,
}

#[derive(Clone)]
/// The memory caching layer for disk reads.
///
/// Because the main IO readers use DirectIO, they bypass the OS' file system cache,
/// instead, we use this system to cache commonly read file sections and can control
/// exactly how much cache is allowed for what.
pub struct FileSystemCache {
    cache: moka::sync::Cache<FileCacheKey, Bytes, ahash::RandomState>,
}

impl Default for FileSystemCache {
    fn default() -> Self {
        let options = FileSystemCacheOptions::builder().build();
        Self::new(options)
    }
}

impl FileSystemCache {
    /// Create a new [FileSystemCache] with the given [FileSystemCacheOptions].
    pub fn new(options: FileSystemCacheOptions) -> Self {
        let cache = moka::sync::CacheBuilder::new(options.cache_capacity_bytes)
            .eviction_policy(EvictionPolicy::tiny_lfu())
            .weigher(|_k: &FileCacheKey, v: &Bytes| {
                let size = size_of::<FileSystemCache>() + v.len();
                size as u32
            })
            .build_with_hasher(ahash::RandomState::new());

        Self { cache }
    }

    /// Lookup the file in the cache and select as many parts of the file
    /// which are cached and within the target range, return the rest as
    /// pre-split positions to read from disk.
    ///
    /// NOTE:
    /// Positions in which there is a cache miss are aligned to [CACHE_BLOCK_SIZE]
    /// which may read more data than strictly necessary but is required
    /// for storing the retrieved data in the cache afterward.
    pub fn lookup(&self, path: &str, range: Range<u64>) -> CacheLookupResult {
        if range.end == 0 {
            return CacheLookupResult {
                missed: SmallVec::new(),
                cached: SmallVec::new(),
            };
        }

        let file_id = file_id(path);
        let aligned_start = align_down(range.start);
        let aligned_end = align_up(range.end);

        let start_offset_by = (range.start - aligned_start) as usize;
        let end_offset_by = (CACHE_BLOCK_SIZE - (aligned_end - range.end)) as usize;

        let cache_block_id_start = (aligned_start / CACHE_BLOCK_SIZE) as u32;
        let cache_block_id_end = (aligned_end / CACHE_BLOCK_SIZE) as u32;

        let mut cached = SmallVec::new();
        let mut missed = SmallVec::new();
        for cache_block_id in cache_block_id_start..cache_block_id_end {
            let key = FileCacheKey {
                file_id,
                cache_block_id,
            };

            match self.cache.get(&key) {
                None => {
                    let offset = cache_block_id as u64 * CACHE_BLOCK_SIZE;
                    missed.push(offset..offset + CACHE_BLOCK_SIZE);
                },
                Some(chunk) if cache_block_id == cache_block_id_start => {
                    cached.push(chunk.slice(start_offset_by..));
                },
                Some(chunk) if cache_block_id == (cache_block_id_end - 1) => {
                    cached.push(chunk.slice(..end_offset_by));
                },
                Some(chunk) => {
                    cached.push(chunk);
                },
            }
        }

        CacheLookupResult { cached, missed }
    }

    /// Inserts a new chunk of a file into the cache.
    ///
    /// The entry may or may not be admitted depending on the LFU policy.
    ///
    /// Chunks are expected to be aligned to the [CACHE_BLOCK_SIZE].
    ///
    /// Returns whether the chunk was attempted to be inserted into the cache.
    pub fn insert(
        &self,
        path: &str,
        range: Range<u64>,
        chunk: Bytes,
        is_end: bool,
    ) -> bool {
        if range.end == 0
            || range.start % CACHE_BLOCK_SIZE != 0
            || ((range.end % CACHE_BLOCK_SIZE != 0) && !is_end)
        {
            return false;
        }

        let file_id = file_id(path);

        let cache_block_id_start = (range.start / CACHE_BLOCK_SIZE) as u32;

        let mut start = 0;
        let mut relative_block_id = 0;
        while start < chunk.len() {
            let end = cmp::min(start + CACHE_BLOCK_SIZE as usize, chunk.len());

            let key = FileCacheKey {
                file_id,
                cache_block_id: relative_block_id + cache_block_id_start,
            };
            let sliced = chunk.slice(start..end);
            self.cache.insert(key, sliced);

            start += CACHE_BLOCK_SIZE as usize;
            relative_block_id += 1;
        }

        true
    }
}

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
/// A single cache entry within the file cache.
struct FileCacheKey {
    /// The unique hash of the file key path.
    file_id: u64,
    /// The cache block chunk ID for the file.
    cache_block_id: u32,
}

#[derive(Clone)]
/// The result from the cache lookup.
///
/// This contains the cached slices of the file
/// and the positions that need to be read from disk.
pub struct CacheLookupResult {
    pub cached: SmallVec<[Bytes; 4]>,
    pub missed: SmallVec<[Range<u64>; 4]>,
}

/// Aligns the value _down_ to the nearest [CACHE_BLOCK_SIZE]
fn align_down(value: u64) -> u64 {
    let remainder = value % CACHE_BLOCK_SIZE;
    value - remainder
}

/// Aligns the value _up_ to the nearest [CACHE_BLOCK_SIZE]
fn align_up(value: u64) -> u64 {
    let remainder = value % CACHE_BLOCK_SIZE;
    if remainder == 0 {
        value
    } else {
        value + CACHE_BLOCK_SIZE - remainder
    }
}

fn file_id(path: &str) -> u64 {
    let mut hasher = ahash::AHasher::default();
    path.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_align_helpers() {
        assert_eq!(align_down(0), 0);
        assert_eq!(align_down(25), 0);
        assert_eq!(align_down(CACHE_BLOCK_SIZE), CACHE_BLOCK_SIZE);
        assert_eq!(align_down(CACHE_BLOCK_SIZE + 42), CACHE_BLOCK_SIZE);
        assert_eq!(align_down(CACHE_BLOCK_SIZE * 16 + 1), CACHE_BLOCK_SIZE * 16);

        assert_eq!(align_up(0), 0);
        assert_eq!(align_up(25), CACHE_BLOCK_SIZE);
        assert_eq!(align_up(CACHE_BLOCK_SIZE), CACHE_BLOCK_SIZE);
        assert_eq!(align_up(CACHE_BLOCK_SIZE + 42), CACHE_BLOCK_SIZE * 2);
        assert_eq!(align_up(CACHE_BLOCK_SIZE * 16 + 1), CACHE_BLOCK_SIZE * 17);
    }

    #[test]
    fn test_cache_insert_aligned_chunk() {
        let cache = FileSystemCache::default();

        let data = Bytes::from(vec![0; CACHE_BLOCK_SIZE as usize]);
        cache.insert("example.txt", 0..CACHE_BLOCK_SIZE, data, true);

        let result = cache.lookup("example.txt", 0..CACHE_BLOCK_SIZE);
        assert_eq!(result.cached.len(), 1);
        assert!(result.missed.is_empty());
    }

    #[test]
    fn test_cache_insert_single_unaligned_chunk_is_whole_file() {
        let cache = FileSystemCache::default();

        let data = Bytes::from(vec![0; 13]);
        cache.insert("example.txt", 0..13, data, true);

        let result = cache.lookup("example.txt", 0..13);
        assert_eq!(result.cached.len(), 1);
        assert!(result.missed.is_empty());
    }

    #[test]
    fn test_cache_insert_unaligned_chunk() {
        let cache = FileSystemCache::default();

        let data = Bytes::from(vec![0; 13]);
        let valid = cache.insert("example.txt", 0..13, data, false);
        assert!(!valid);

        let result = cache.lookup("example.txt", 0..13);
        assert!(result.cached.is_empty());
        assert_eq!(result.missed.len(), 1);
    }

    #[test]
    fn test_cache_insert_multiple_chunks() {
        let cache = FileSystemCache::default();

        let data = Bytes::from(vec![0; CACHE_BLOCK_SIZE as usize]);
        let data_small = Bytes::from_static(b"Hello, world!");
        cache.insert("example.txt", 0..CACHE_BLOCK_SIZE, data.clone(), false);
        cache.insert(
            "example.txt",
            CACHE_BLOCK_SIZE..CACHE_BLOCK_SIZE * 2,
            data.clone(),
            false,
        );
        cache.insert(
            "example.txt",
            CACHE_BLOCK_SIZE * 2..CACHE_BLOCK_SIZE * 2 + 13,
            data_small,
            true,
        );

        let result = cache.lookup("example.txt", 0..CACHE_BLOCK_SIZE * 2 + 13);
        assert_eq!(result.cached.len(), 3);
        assert!(result.missed.is_empty());
    }

    #[test]
    fn test_multiple_files() {
        let cache = FileSystemCache::default();

        let data = Bytes::from(vec![0; CACHE_BLOCK_SIZE as usize]);
        cache.insert("example1.txt", 0..CACHE_BLOCK_SIZE, data.clone(), true);
        cache.insert("example2.txt", 0..CACHE_BLOCK_SIZE, data.clone(), false);
        cache.insert(
            "example2.txt",
            CACHE_BLOCK_SIZE..CACHE_BLOCK_SIZE * 2,
            data,
            true,
        );

        let result = cache.lookup("example1.txt", 0..CACHE_BLOCK_SIZE);
        assert_eq!(result.cached.len(), 1);
        assert!(result.missed.is_empty());
        let result = cache.lookup("example2.txt", 0..CACHE_BLOCK_SIZE * 2);
        assert_eq!(result.cached.len(), 2);
        assert!(result.missed.is_empty());
    }

    #[test]
    fn test_cache_get_edge_cases() {
        let cache = FileSystemCache::default();

        let data_small = Bytes::from_static(b"Hello, world!");
        cache.insert("example.txt", 0..13, data_small.clone(), true);

        let result = cache.lookup("example.txt", 4..13);
        assert_eq!(
            result.cached.as_slice(),
            &[Bytes::from_static(b"o, world!")]
        );
        assert!(result.missed.is_empty());

        let result = cache.lookup("example.txt", 13..13);
        assert_eq!(result.cached.as_slice(), &[Bytes::new()]);
        assert!(result.missed.is_empty());

        let result = cache.lookup("example.txt", 12..13);
        assert_eq!(result.cached.as_slice(), &[Bytes::from_static(b"!")]);
        assert!(result.missed.is_empty());
    }
}
