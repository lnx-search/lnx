use std::future::Future;
use std::io;
use std::io::{Cursor, ErrorKind, Write};
use std::path::PathBuf;
use std::sync::Arc;

use lnx_document::DocBlockReader;
use lnx_metastore::Metastore;
use rkyv::AlignedVec;
use yorick::{StorageBackend, WriteId, YorickStorageService};

mod caches;
mod compaction;

/// The maximum size of the storage files in bytes.
const MAX_FILE_SIZE: u64 = 10 << 30;

/// The caching mode to use.
pub enum CacheMode {
    /// Use LRU based caching.
    Lru,
    /// Use FIFO based cache.
    Fifo,
}

/// The configuration for the storage service.
pub struct StorageConfig {
    /// If enabled the system will attempt to use the direct IO backend.
    pub try_use_directio: bool,
    /// The base storage path to use.
    pub base_path: PathBuf,
    /// The caching mode to use.
    pub cache_mode: CacheMode,
    /// The cache size in bytes.
    pub cache_size: u64,
}

/// The primary block storage service.
///
/// This wraps the yorick storage service and adds a caching layer on top.
pub struct BlockStorageService {
    cache: caches::CacheSelector,
    inner: YorickStorageService,
}

impl BlockStorageService {
    /// Creates a new block storage service with a given config.
    pub async fn create(
        config: StorageConfig,
        metastore: Metastore,
    ) -> io::Result<Self> {
        let backend = create_backend(config.try_use_directio).await?;
        let service_config = yorick::StorageServiceConfig {
            base_path: config.base_path,
            max_file_size: MAX_FILE_SIZE,
        };

        let inner = YorickStorageService::create_with_compaction(
            backend,
            service_config,
            compaction::CommitAwareCompactionPolicy::new(metastore),
        )
        .await?;

        let cache = match config.cache_mode {
            CacheMode::Fifo => {
                caches::CacheSelector::fifo_with_capacity(config.cache_size)
            },
            CacheMode::Lru => {
                caches::CacheSelector::lru_with_capacity(config.cache_size)
            },
        };

        Ok(Self { cache, inner })
    }

    #[inline]
    /// Attempts to get a block from the storage service.
    ///
    /// This will use the cached reader if it exists.
    pub async fn get_block(
        &self,
        block_id: u64,
    ) -> anyhow::Result<Option<DocBlockReader>> {
        if let Some(hit) = self.cache.get(block_id) {
            return Ok(Some(hit));
        }

        let read = self.inner.create_read_ctx();
        match read.read_blob(block_id).await? {
            None => Ok(None),
            Some(result) => {
                let decompressed = decompress_block(result.data).await?;
                let reader = DocBlockReader::using_data(Arc::new(decompressed))?;
                self.cache.put(block_id, reader.clone());

                Ok(Some(reader))
            },
        }
    }

    #[inline]
    /// Creates a new write context for writing to the storage service.
    pub fn create_write_ctx(&self) -> WriteContext {
        let ctx = self.inner.create_write_ctx();
        WriteContext { ctx }
    }
}

/// A write context that wraps the inner yorick context.
pub struct WriteContext {
    ctx: yorick::WriteContext,
}

impl WriteContext {
    /// Writes a uncompressed block to the store.
    pub async fn write_block<D>(
        &mut self,
        block_id: u64,
        index_id: u64,
        block_data: D,
    ) -> io::Result<WriteId>
    where
        D: AsRef<[u8]> + Send + 'static,
    {
        let compressed = compress_block(block_data).await?;
        self.write_block_compressed(block_id, index_id, compressed)
            .await
    }

    #[inline]
    /// Writes a compressed block to the store.
    pub fn write_block_compressed<D>(
        &mut self,
        block_id: u64,
        index_id: u64,
        compressed_data: D,
    ) -> impl Future<Output = io::Result<WriteId>> + '_
    where
        D: AsRef<[u8]> + Send + 'static,
    {
        self.ctx.write_blob(block_id, index_id, compressed_data)
    }

    /// Commits the blocks written to the store.
    pub fn commit(self) -> impl Future<Output = io::Result<()>> {
        self.ctx.commit()
    }
}

/// Compresses a block of data using LZ4 compression.
pub async fn compress_block<D>(data: D) -> io::Result<Vec<u8>>
where
    D: AsRef<[u8]> + Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        let data = data.as_ref();

        let mut buffer = Vec::with_capacity(1 << 10);
        buffer.extend_from_slice(&(data.len() as u32).to_le_bytes());
        let mut writer = Cursor::new(buffer);
        writer.set_position(4);

        let mut encoder = lz4_flex::frame::FrameEncoder::new(writer);
        encoder.write_all(data)?;
        encoder.flush()?;
        Ok(encoder.into_inner().into_inner())
    })
    .await
    .expect("Spawn background thread")
}

/// Decompresses a block of data using LZ4 compression.
///
/// This returns an [AlignedVec].
pub async fn decompress_block<D>(data: D) -> io::Result<AlignedVec>
where
    D: AsRef<[u8]> + Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        let compressed = data.as_ref();
        let (size, data) = uncompressed_size(compressed)?;

        let mut aligned = AlignedVec::with_capacity(size);
        let mut reader = lz4_flex::frame::FrameDecoder::new(data);

        aligned.extend_from_reader(&mut reader)?;

        Ok(aligned)
    })
    .await
    .expect("Spawn background thread")
}

#[inline]
fn uncompressed_size(input: &[u8]) -> io::Result<(usize, &[u8])> {
    let size = input.get(..4).ok_or_else(|| {
        io::Error::new(
            ErrorKind::Other,
            "Compressed input missing minimum number of bytes",
        )
    })?;
    let size: &[u8; 4] = size.try_into().unwrap();
    let uncompressed_size = u32::from_le_bytes(*size) as usize;
    let rest = &input[4..];
    Ok((uncompressed_size, rest))
}

#[allow(unused_variables)]
async fn create_backend(try_use_directio: bool) -> io::Result<StorageBackend> {
    #[cfg(feature = "direct-io")]
    let maybe_direct_io_backend: Option<StorageBackend> = {
        if try_use_directio {
            let config = yorick::DirectIoConfig {
                num_threads: 2,
                ..Default::default()
            };

            match StorageBackend::create_direct_io(config).await {
                Err(e) => {
                    tracing::error!(error = ?e, "Failed to use direct IO backend due to error");
                    None
                },
                Ok(backend) => Some(backend),
            }
        } else {
            None
        }
    };

    #[cfg(not(feature = "direct-io"))]
    let maybe_direct_io_backend: Option<StorageBackend> = None;

    match maybe_direct_io_backend {
        Some(backend) => Ok(backend),
        None => {
            let config = yorick::BufferedIoConfig { io_threads: 4 };

            StorageBackend::create_buffered_io(config).await
        },
    }
}
