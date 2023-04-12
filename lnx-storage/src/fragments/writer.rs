use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::{BufWriter, ErrorKind, Seek, SeekFrom, Write};
use std::ops::Range;
use std::time::Instant;
use std::{io, mem};

use bytecheck::CheckBytes;
use bytes::Bytes;
use datacake::eventual_consistency::Document;
use datacake::rpc::{Body, Status};
use hyper::body::HttpBody;
use jocky::metadata::{write_metadata_offsets, SegmentMetadata};
use lnx_io::file::SyncOnFlushFile;
use puppet::{derive_message, puppet_actor, ActorMailbox};
use rkyv::{AlignedVec, Archive, Deserialize, Serialize};
use tokio::task::yield_now;

use super::block::BlockLocations;
use crate::fragments::block::{BlockId, BlockInfo};
use crate::metastore::{BlockMetadata, Metastore};
use crate::resolvers::{BLOCK_LOCATIONS_PATH, FRAGMENT_INFO_PATH};
use crate::{EnvCtx, FragmentInfo, SharedSlice};

/// The number of bytes that prefix a given block
pub const BLOCK_HEADER_SIZE: usize = 16;

/// A writer that exports received documents blocks into
/// the start of a index fragment.
pub struct FragmentWriter {
    env: EnvCtx,
    id: u64,
    cursor: usize,
    metadata: SegmentMetadata,
    block_locations: BlockLocations,
    writer: BufWriter<SyncOnFlushFile>,
    block_metadata_changes: Vec<(BlockId, BlockMetadata)>,
    metastore: Metastore,
    should_remove_file_on_drop: bool,
}

#[puppet_actor]
impl FragmentWriter {
    /// Create a new block writer.
    pub fn new(
        env: EnvCtx,
        id: u64,
        file: impl Into<SyncOnFlushFile>,
        metastore: Metastore,
    ) -> ActorMailbox<Self> {
        Self::from_existing_state(env, id, file, metastore, Vec::new())
    }

    /// Create a new block writer from an existing file and state.
    pub fn from_existing_state(
        env: EnvCtx,
        id: u64,
        file: impl Into<SyncOnFlushFile>,
        metastore: Metastore,
        block_locations: BlockLocations,
    ) -> ActorMailbox<Self> {
        let (tx, rx) = flume::bounded(25);

        let actor = Self {
            env,
            id,
            cursor: 0,
            metadata: SegmentMetadata::default(),
            block_locations,
            writer: BufWriter::new(file.into()),
            block_metadata_changes: Vec::new(),
            metastore,
            should_remove_file_on_drop: false,
        };

        lnx_executor::spawn_task(actor.run_actor(rx));

        ActorMailbox::new(tx, Cow::Borrowed("block-writer-actor"))
    }

    #[puppet]
    async fn remove_file_on_drop(&mut self, _msg: RemoveOnDrop) {
        self.should_remove_file_on_drop = true;
    }

    #[puppet]
    async fn get_current_state(&self, _msg: GetCurrentState) -> WriterState {
        WriterState {
            files: self.metadata.files().clone(),
            existing_blocks: self.block_locations.clone(),
        }
    }

    #[instrument("fragment-io-write", skip_all)]
    async fn write_all(&mut self, mut buffer: &[u8]) -> io::Result<usize> {
        let mut yields = 0;
        let start = Instant::now();
        loop {
            let n = self.writer.write(buffer)?;
            self.cursor += n;

            if n == buffer.len() {
                debug!(num_yields = yields, elapsed = ?start.elapsed(), "Write bytes");
                return Ok(self.cursor);
            }

            buffer = &buffer[n..];
            yield_now().await;
            yields += 1;
        }
    }

    #[instrument("fragment-io-copy-stream", skip_all)]
    async fn copy_stream(&mut self, msg: FragmentStream) -> Result<(), StreamError> {
        let stream = msg.body;

        let mut current_pos = self.cursor;

        while let Ok(chunk) = stream.recv_async().await {
            // TODO: Handle errors
            self.write_all(&chunk).await?;
        }

        for (block_id, len, checksum) in msg.blocks {
            let info = BlockInfo {
                location: current_pos as u32..current_pos as u32 + len,
                checksum,
            };
            self.block_locations.push((block_id, info));

            let metadata = BlockMetadata {
                fragment_id: self.id,
                start: current_pos as u32,
                end: current_pos as u32 + len,
                checksum,
            };
            self.block_metadata_changes.push((block_id, metadata));
            current_pos += len as usize;
        }

        for (file, len) in msg.files {
            self.metadata
                .add_file(file, current_pos as u64..current_pos as u64 + len as u64);
            current_pos += len as usize;
        }

        Ok(())
    }

    #[instrument("fragment-io-write-len", skip_all)]
    fn write_block_header(
        &mut self,
        len: u32,
        id: u64,
        checksum: u32,
    ) -> io::Result<u32> {
        let bytes = checksum.to_le_bytes();
        self.writer.write_all(&bytes)?;
        self.cursor += bytes.len();

        let bytes = id.to_le_bytes();
        self.writer.write_all(&bytes)?;
        self.cursor += bytes.len();

        let bytes = len.to_le_bytes();
        self.writer.write_all(&bytes)?;
        self.cursor += bytes.len();

        Ok(self.cursor as u32)
    }

    #[puppet]
    /// Write a doc to the writer.
    async fn write_block(&mut self, msg: WriteDocBlock) -> io::Result<usize> {
        let buffer = msg.block.data();
        let len = buffer.len();
        let cursor_start = self.cursor;

        // Write the length of the block as the prefix.
        // This lets us walk through the block to recover data.
        let start = self.write_block_header(len as u32, msg.block.id(), msg.checksum)?;
        let res = self.write_all(buffer).await;

        if res.is_ok() {
            let info = BlockInfo {
                location: start..self.cursor as u32,
                checksum: msg.checksum,
            };
            self.block_locations.push((msg.block.id(), info));

            let metadata = BlockMetadata {
                fragment_id: self.id,
                start,
                end: self.cursor as u32,
                checksum: msg.checksum,
            };
            self.block_metadata_changes.push((msg.block.id(), metadata));
        } else {
            // We attempt to reset the cursor here to prevent us having to do
            // more work in the recovery state and cut down on wasted space.
            if let Err(e) = self
                .writer
                .get_mut()
                .seek(SeekFrom::Start(cursor_start as u64))
            {
                warn!(error = ?e, "Failed to reset writer cursor, this may lead to write amplification");
            } else {
                trace!(
                    cursor = cursor_start,
                    "Reset cursor position to minimise write amplification"
                );
                self.cursor = cursor_start;
            }
        }

        res
    }

    #[puppet]
    /// Write a file to the fragment.
    async fn write_file(&mut self, msg: WriteFile) -> io::Result<()> {
        let start = self.cursor;

        if let Err(e) = self.write_all(&msg.bytes).await {
            if let Err(e) = self.writer.get_mut().seek(SeekFrom::Start(start as u64)) {
                warn!(error = ?e, "Failed to reset writer cursor, this may lead to write amplification");
            } else {
                trace!(
                    cursor = start,
                    "Reset cursor position to minimise write amplification"
                );
                self.cursor = start;
            }
            return Err(e);
        }

        let end = self.cursor;
        self.metadata.add_file(msg.file, start as u64..end as u64);
        Ok(())
    }

    #[puppet]
    /// Merges a fragment stream into the writer.
    ///
    /// It expects the order of bytes to be in the following order:
    ///
    /// - Blocks
    /// - Files
    async fn merge_stream(&mut self, msg: FragmentStream) -> Result<(), StreamError> {
        let start_cursor = self.cursor;

        let start = Instant::now();
        let res = self.copy_stream(msg).await;

        if res.is_err() {
            if let Err(e) = self
                .writer
                .get_mut()
                .seek(SeekFrom::Start(start_cursor as u64))
            {
                warn!(error = ?e, "Failed to reset writer cursor, this may lead to write amplification");
            } else {
                trace!(
                    cursor = start_cursor,
                    "Reset cursor position to minimise write amplification"
                );
                self.cursor = start_cursor;
            }
        } else {
            info!(elapsed = ?start.elapsed(), "Successfully copied data from fragment stream");
        }

        res
    }

    #[puppet]
    /// Attempt to flush the buffer contents to disk.
    async fn flush(&mut self, _msg: Flush) -> io::Result<()> {
        let start = Instant::now();
        self.writer.flush()?;

        // We only persist the metadata of the blocks once we know it's safely on disk.
        for (block_id, metadata) in mem::take(&mut self.block_metadata_changes) {
            self.metastore
                .insert_block(block_id, metadata)
                .map_err(|e| io::Error::new(ErrorKind::Other, e))?;
        }

        debug!(elapsed = ?start.elapsed(), "Flush complete");

        Ok(())
    }

    #[instrument(name = "fragment-sealer", skip_all, fields(fragment_id = self.id))]
    #[puppet]
    /// Seal the fragment and write all metadata.
    ///
    /// Once this operation is complete the file can be
    /// opened by a fragment reader.
    async fn seal(&mut self, msg: Seal) -> io::Result<()> {
        let start_time = Instant::now();

        let block_locations_bytes = rkyv::to_bytes::<_, 4096>(&self.block_locations)
            .map_err(|e| io::Error::new(ErrorKind::Other, e.to_string()))?;
        let fragment_info = rkyv::to_bytes::<_, 4096>(&msg.0)
            .map_err(|e| io::Error::new(ErrorKind::Other, e.to_string()))?;

        let start = self.cursor;
        self.write_all(&block_locations_bytes).await?;
        let end = self.cursor;
        self.metadata
            .add_file(BLOCK_LOCATIONS_PATH.to_string(), start as u64..end as u64);

        let start = self.cursor;
        self.write_all(&fragment_info).await?;
        let end = self.cursor;
        self.metadata
            .add_file(FRAGMENT_INFO_PATH.to_string(), start as u64..end as u64);

        let metadata_bytes = self.metadata.to_bytes()?;
        let start = self.cursor;
        let len = metadata_bytes.len();

        self.write_all(&metadata_bytes).await?;
        write_metadata_offsets(&mut self.writer, start as u64, len as u64)?;
        self.flush(Flush).await?;

        self.metastore
            .seal_fragment(self.id)
            .map_err(|e| io::Error::new(ErrorKind::Other, e))?;
        info!(elapsed = ?start_time.elapsed(), "Fragment is sealed");

        // Remove the blocks that are now in the sealed segments.
        // This prevents us trying to open sealed segments for
        // recovery unnecessarily.
        self.metastore
            .remove_blocks(self.block_locations.iter().map(|(k, _)| k).copied())
            .map_err(|e| io::Error::new(ErrorKind::Other, e))?;

        Ok(())
    }
}

impl Drop for FragmentWriter {
    fn drop(&mut self) {
        if self.should_remove_file_on_drop {
            let path =
                crate::resolvers::get_fragment_location(&self.env.root_path, self.id);
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Write some bytes to the file.
///
/// Returns the current position of the cursor.
pub struct WriteBytes(pub Bytes);
derive_message!(WriteBytes, io::Result<usize>);

#[repr(C)]
#[derive(Serialize, Deserialize, Archive, Clone)]
#[archive_attr(derive(CheckBytes))]
/// Write a block to the file.
///
/// Returns the current position of the cursor.
pub struct WriteDocBlock {
    /// The block data.
    pub block: Document,
    /// The checksum of the block.
    pub checksum: u32,
}
derive_message!(WriteDocBlock, io::Result<usize>);

/// Write some bytes to the file.
///
/// Returns the current position of the cursor.
pub struct WriteFile {
    pub file: String,
    pub bytes: SharedSlice,
}
derive_message!(WriteFile, io::Result<()>);

/// Attempts to complete a fragment using the given
/// data stream.
///
/// It expects the order of bytes to be in the following order:
///
/// - Blocks
/// - Files
pub struct FragmentStream {
    /// The files being merged into the writer mapping path and file length
    /// allowing the writer to determine the block's positions within the file.
    pub files: Vec<(String, u32)>,
    /// A tuple of block ID, block length and checksum allowing
    /// the writer to determine the block's positions within the file.
    pub blocks: Vec<(BlockId, u32, u32)>,
    /// The body of the request to start streaming data from.
    pub body: flume::Receiver<Bytes>,
}
derive_message!(FragmentStream, Result<(), StreamError>);

impl datacake::rpc::TryIntoBody for FragmentStream {
    fn try_into_body(self) -> Result<Body, Status> {
        let files = rkyv::to_bytes::<_, 1028>(&self.files).map_err(Status::internal)?;
        let blocks =
            rkyv::to_bytes::<_, 1028>(&self.blocks).map_err(Status::internal)?;

        let mut header = Vec::with_capacity(files.len() + blocks.len() + 16);
        header.extend_from_slice(&(files.len() as u32).to_le_bytes());
        header.extend_from_slice(&files);
        header.extend_from_slice(&(blocks.len() as u32).to_le_bytes());
        header.extend_from_slice(&blocks);

        let stream = self.body;
        let (mut tx, body) = hyper::Body::channel();

        tokio::spawn(async move {
            tx.send_data(Bytes::from(header)).await.map_err(log_err)?;

            // TODO: Handle errors
            while let Ok(chunk) = stream.recv_async().await {
                tx.send_data(chunk).await.map_err(log_err)?;
            }

            Ok::<_, hyper::Error>(())
        });

        Ok(body.into())
    }
}

#[datacake::rpc::async_trait]
impl datacake::rpc::RequestContents for FragmentStream {
    type Content = Self;

    async fn from_body(body: Body) -> Result<Self::Content, Status> {
        let mut incoming = body.into_inner();

        let mut blocks = Vec::new();
        let mut files = Vec::new();
        let mut has_read_header = false;
        let mut temp_buffer = AlignedVec::new();
        let mut remaining = Bytes::new();
        let (tx, rx) = flume::bounded(10);

        // Read the first part of the data
        while let Some(chunk) = incoming.data().await {
            let chunk = chunk.map_err(log_err).map_err(Status::internal)?;
            temp_buffer.extend_from_slice(&chunk);

            let files_length_bytes = &temp_buffer[..mem::size_of::<u32>()];
            let files_length =
                u32::from_le_bytes(files_length_bytes.try_into().unwrap()) as usize;
            let min_length = files_length + mem::size_of::<u32>();

            if temp_buffer.len() < min_length {
                continue;
            }

            let files_buffer_end = mem::size_of::<u32>() + files_length;
            let files_buffer = &temp_buffer[mem::size_of::<u32>()..files_buffer_end];

            let blocks_length_bytes =
                &temp_buffer[files_buffer_end..files_buffer_end + mem::size_of::<u32>()];
            let blocks_length =
                u32::from_le_bytes(blocks_length_bytes.try_into().unwrap()) as usize;
            let min_length = files_length + blocks_length + (mem::size_of::<u32>() * 2);

            if temp_buffer.len() < min_length {
                continue;
            }

            has_read_header = true;

            let blocks_buffer_start = mem::size_of::<u32>() + files_buffer_end;
            let blocks_buffer =
                &temp_buffer[blocks_buffer_start..blocks_buffer_start + blocks_length];

            files = rkyv::from_bytes(files_buffer).map_err(Status::internal)?;
            blocks = rkyv::from_bytes(blocks_buffer).map_err(Status::internal)?;

            if temp_buffer.len() > blocks_buffer_start + blocks_length {
                remaining = Bytes::copy_from_slice(
                    &temp_buffer[blocks_buffer_start + blocks_length..],
                );
            }

            break;
        }

        if !has_read_header {
            return Err(Status::invalid());
        }

        // Stream the remainder of the chunks
        tokio::spawn(async move {
            if !remaining.is_empty() && tx.send_async(remaining).await.is_err() {
                return;
            }

            copy_to_upstream(incoming, tx).await;
        });

        Ok(Self {
            files,
            blocks,
            body: rx,
        })
    }
}

async fn copy_to_upstream(mut incoming: hyper::Body, upstream: flume::Sender<Bytes>) {
    while let Some(chunk) = incoming.data().await {
        match chunk {
            Ok(chunk) => {
                if upstream.send_async(chunk).await.is_err() {
                    return;
                }
            },
            Err(e) => {
                warn!(error = ?e, "Failed to receive data chunk from remote");
                return;
            },
        }
    }
}

/// Flush the current writer buffer.
///
/// This internally calls `fdatasync`.
pub struct Flush;
derive_message!(Flush, io::Result<()>);

/// Seals the segment, writing the metadata and footer
/// to the file.
pub struct Seal(pub FragmentInfo);
derive_message!(Seal, io::Result<()>);

pub struct GetCurrentState;
derive_message!(GetCurrentState, WriterState);

pub struct RemoveOnDrop;
derive_message!(RemoveOnDrop, ());

#[derive(Default)]
pub struct WriterState {
    pub files: BTreeMap<String, Range<u64>>,
    pub existing_blocks: BlockLocations,
}

#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    #[error("Hyper Error: {0}")]
    Hyper(#[from] hyper::Error),
    #[error("IO Error: {0}")]
    Io(#[from] io::Error),
}

fn log_err<E>(e: E) -> E
where
    E: Debug,
{
    error!(error = ?e, "Failed to complete body send due to error");
    e
}
