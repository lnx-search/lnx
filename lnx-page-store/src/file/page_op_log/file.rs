use std::io;
use std::mem::MaybeUninit;

use crate::layout::log::LogBlock;

const BLOCK_SIZE: usize = 512;

pub struct LogFile {
    file: std::fs::File,
    log_offset: u64,
    current_pos: u64,
    block_buffer: Vec<MaybeUninit<u8>>,
}

impl LogFile {
    /// Seek in the log file to the target block index.
    pub fn seek_to_block(&mut self, block: usize) {
        self.current_pos = self.log_offset + (block * BLOCK_SIZE) as u64;
    }

    /// Write a set of blocks to the log file at the current position.
    pub async fn write_blocks(&mut self, block: &LogBlock) -> io::Result<()> {
        Ok(())
    }

    /// Read a set of blocks from the log file at the current position.
    pub async fn read_blocks(&mut self) -> io::Result<()> {
        Ok(())
    }
}
