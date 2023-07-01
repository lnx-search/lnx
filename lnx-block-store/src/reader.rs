use std::fs::File;
use std::io;
use std::path::Path;
use std::sync::Arc;

use lnx_document::DocBlockReader;
use memmap2::Mmap;
use rkyv::AlignedVec;

const BLOCK_LENGTH_BYTES: usize = 4;

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
}

impl BlockStoreReader {
    /// Attempts to open a segment file and create a new reader.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        Ok(Self {
            data: Arc::new(mmap),
        })
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

        DocBlockReader::using_data(buffer).map_err(BlockReadError::InvalidBlockData)
    }
}
