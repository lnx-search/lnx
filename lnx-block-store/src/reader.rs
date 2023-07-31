use std::fs::File;
use std::io;
use std::path::Path;
use std::sync::Arc;
use anyhow::Context;

use lnx_document::DocBlockReader;
use memmap2::Mmap;
use rkyv::AlignedVec;
use tracing::warn;
use crate::{DEFAULT_FILE_EXT, FileKey, Readers};
use crate::metastore::BlockStorageMetastore;

const BLOCK_LENGTH_BYTES: usize = 4;


/// Walks through the directory (non-recursively) and loads and
/// files which are suitable to be read.
pub fn get_file_readers(
    metastore: &BlockStorageMetastore,
    base_path: &Path,
) -> anyhow::Result<Readers> {

    let dir = std::fs::read_dir(base_path)
        .context("Read directory entries")?;

    let mut readers = Readers::default();
    for entry in dir {
        let entry = entry.context("Get directory entry")?;

        let path = entry.path();
        if !path.is_file() {
            continue
        }

        let file_key = match maybe_get_file_key(&path) {
            None => {
                warn!(path = %path.display(), "Ignoring file located in block store folder as it is not a valid file name");
                continue
            },
            Some(key) => key,
        };

        let checkpoint = metastore.get_file_commit_checkpoint(file_key)?;
        let reader = BlockStoreReader::open(&path, checkpoint)?;

        readers.insert(file_key, reader);
    }

    Ok(readers)
}


/// Attempts to get the file key for a given file if it's valid.
///
/// This means the file name must be in the format of:
/// `<file_key>.v1blocks`
fn maybe_get_file_key(path: &Path) -> Option<FileKey> {
    let file_name_raw = path.file_name()?;
    let file_name_str = file_name_raw.to_str()?;

    let (name, ext) = file_name_str.split_once('.')?;

    if ext != DEFAULT_FILE_EXT {
        return None
    }

    name.parse::<u64>().ok().map(FileKey)
}



#[derive(Debug, thiserror::Error)]
/// An error when reading a doc block.
pub enum BlockReadError {
    #[error("Failed to read block length for position: {0}")]
    BlockLength(usize),
    #[error("Decompression Error: {0}")]
    Decompression(#[from] lz4_flex::block::DecompressError),
    #[error("Unable to read block: {0}")]
    InvalidBlockData(anyhow::Error),
}

#[derive(Clone)]
/// A cheap to clone reader for a given block store segment.
///
/// The reader internally keeps a MMAP to the segment but does
/// not handle any caching internally.
pub struct BlockStoreReader {
    data: Arc<Mmap>,
    commit_checkpoint: Option<u64>,
}

impl BlockStoreReader {
    /// Attempts to open a segment file and create a new reader.
    pub fn open(path: &Path, commit_checkpoint: Option<u64>) -> io::Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        Ok(Self {
            data: Arc::new(mmap),
            commit_checkpoint,
        })
    }

    #[inline]
    /// Returns the reader's commit checkpoint.
    ///
    /// This is the position in bytes of where the file has currently
    /// been indexed and committed.
    pub fn commit_checkpoint(&self) -> Option<u64> {
        self.commit_checkpoint
    }

    /// Gets a slice into the file with the block data in compressed form.
    pub fn get_compressed_block_data(
        &self,
        pos: usize,
    ) -> Result<&[u8], BlockReadError> {
        let block_length = u32::from_le_bytes(
            self.data[pos..pos + BLOCK_LENGTH_BYTES]
                .try_into()
                .map_err(|_| BlockReadError::BlockLength(pos))?,
        ) as usize;
        let offset = pos + BLOCK_LENGTH_BYTES;

        Ok(&self.data[offset..offset + block_length])
    }

    /// Gets a block and decompresses the data.
    pub fn get_block_data(&self, pos: usize) -> Result<AlignedVec, BlockReadError> {
        let compressed_block = self.get_compressed_block_data(pos)?;

        let (length, slice) = lz4_flex::block::uncompressed_size(compressed_block)?;
        let mut buffer = AlignedVec::with_capacity(length);
        buffer.fill(0); // TODO: This technically doesnt need to happen, but it requires some support from lz4_flex.
        lz4_flex::decompress_into(slice, &mut buffer)?;

        Ok(buffer)
    }

    /// Gets a block, decompresses the data and creates a block reader.
    pub fn get_block(&self, pos: usize) -> Result<DocBlockReader, BlockReadError> {
        let buffer = self.get_block_data(pos)?;

        DocBlockReader::using_data(Arc::new(buffer))
            .map_err(BlockReadError::InvalidBlockData)
    }
}
