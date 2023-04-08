use std::io;
use std::io::ErrorKind;
use std::ops::{Deref, Range};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use hashbrown::HashMap;
use jocky::metadata::{get_metadata_offsets, SegmentMetadata, METADATA_HEADER_SIZE};
use memmap2::Mmap;
use ownedbytes::OwnedBytes;
use rkyv::AlignedVec;
use stable_deref_trait::StableDeref;

use crate::fragments::block::{BlockId, BlockInfo, BlockLocations};

#[derive(Clone)]
/// A lightweight fragment reader that can be cheaply cloned and sliced
/// like a file.
pub struct FragmentReader {
    should_remove_on_drop: Arc<AtomicBool>,
    file_contents: OwnedBytes,
    metadata: Arc<SegmentMetadata>,
    blocks: Arc<HashMap<BlockId, BlockInfo>>,
}

impl FragmentReader {
    /// Creates a new fragment reader from [OwnedBytes].
    ///
    /// This expects the fragment to be completely sealed.
    pub fn new(
        bytes: OwnedBytes,
        should_remove_on_drop: Arc<AtomicBool>,
    ) -> io::Result<Self> {
        let len = bytes.len();
        let offsets_slice = &bytes[len - METADATA_HEADER_SIZE..];
        let (start, len) = get_metadata_offsets(offsets_slice)
            .map_err(|e| io::Error::new(ErrorKind::Other, e))?;

        let metadata_slice = &bytes[start as usize..(start + len) as usize];
        let metadata = SegmentMetadata::from_buffer(metadata_slice)?;

        let block_locations_range = metadata.get_location(crate::resolvers::BLOCK_LOCATIONS_PATH)
            .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "Failed to read block locations, fragment is corrupted and must be repaired"))?;
        let mut block_locations_bytes = AlignedVec::with_capacity(
            (block_locations_range.end - block_locations_range.start) as usize,
        );
        block_locations_bytes.extend_from_slice(
            &bytes[block_locations_range.start as usize
                ..block_locations_range.end as usize],
        );
        let block_locations_iter = rkyv::from_bytes::<BlockLocations>(&block_locations_bytes)
            .map_err(|_| io::Error::new(ErrorKind::InvalidData, "Failed to read block locations, fragment is corrupted and must be repaired"))?;
        let block_locations = HashMap::from_iter(block_locations_iter);

        Ok(Self {
            should_remove_on_drop: should_remove_on_drop.clone(),
            file_contents: bytes,
            metadata: Arc::new(metadata),
            blocks: Arc::new(block_locations),
        })
    }

    /// Get an iterator over all blocks in the fragment.
    pub fn get_fragment_blocks(&self) -> impl Iterator<Item = (&BlockId, &BlockInfo)> {
        self.blocks.iter()
    }

    /// Get an iterator over all files in the fragment.
    pub fn get_file_locations(&self) -> impl Iterator<Item = (&String, &Range<u64>)> {
        self.metadata.files().iter()
    }

    /// Open a fragment read
    pub async fn open_mmap(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        lnx_executor::spawn_task(async move { Self::open_mmap_blocking(path) })
            .await
            .expect("Spawn background thread")
    }

    /// Open a fragment read
    pub fn open_mmap_blocking(path: PathBuf) -> io::Result<Self> {
        let file = std::fs::File::open(&path)?;
        let map = unsafe { Mmap::map(&file)? };
        let should_remove_on_drop = Arc::new(AtomicBool::new(false));
        let wrapper = MmapWrapper {
            inner: Some(map),
            path,
            should_remove_on_drop: should_remove_on_drop.clone(),
        };
        Self::new(OwnedBytes::new(wrapper), should_remove_on_drop)
    }

    /// Read a virtual file from the fragment.
    pub fn read_file(&self, path: &str) -> Option<OwnedBytes> {
        let range = self.metadata.get_location(path)?;
        Some(
            self.file_contents
                .slice(range.start as usize..range.end as usize),
        )
    }

    /// Reads a block from the fragment but leaves it in it's compressed form.
    pub fn read_block(&self, id: u64) -> Option<OwnedBytes> {
        let info = self.blocks.get(&id)?.clone();
        Some(self.file_contents.slice(info.location_usize()))
    }

    /// Tells the reader that it should remove the file once all references
    /// to it have been dropped.
    pub fn set_remove_on_drop(&self) {
        self.should_remove_on_drop.store(true, Ordering::Relaxed);
    }
}

struct MmapWrapper {
    inner: Option<Mmap>,
    should_remove_on_drop: Arc<AtomicBool>,
    path: PathBuf,
}

impl Deref for MmapWrapper {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.inner.as_ref().unwrap().deref()
    }
}

impl Drop for MmapWrapper {
    fn drop(&mut self) {
        drop(self.inner.take());

        if self.should_remove_on_drop.load(Ordering::Relaxed) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

unsafe impl StableDeref for MmapWrapper {}
