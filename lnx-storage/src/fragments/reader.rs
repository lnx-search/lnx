use std::any::type_name;
use std::io::ErrorKind;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::{io, mem};

use bytecheck::CheckBytes;
use hashbrown::HashMap;
use jocky::metadata::{get_metadata_offsets, SegmentMetadata, METADATA_HEADER_SIZE};
use memmap2::Mmap;
use rkyv::de::deserializers::SharedDeserializeMap;
use rkyv::validation::validators::DefaultValidator;
use rkyv::{AlignedVec, Archive, Deserialize};

use crate::fragments::block::{BlockId, BlockInfo, BlockLocations};
use crate::resolvers::{BLOCK_LOCATIONS_PATH, FRAGMENT_INFO_PATH};
use crate::{FragmentInfo, SharedSlice};

#[derive(Clone)]
/// A lightweight fragment reader that can be cheaply cloned and sliced
/// like a file.
pub struct FragmentReader {
    info: Arc<FragmentInfo>,
    should_remove_on_drop: Arc<AtomicBool>,
    file_contents: SharedSlice,
    metadata: Arc<SegmentMetadata>,
    blocks: Arc<HashMap<BlockId, BlockInfo>>,
}

impl FragmentReader {
    /// Creates a new fragment reader from [OwnedBytes].
    ///
    /// This expects the fragment to be completely sealed.
    pub fn new(
        bytes: SharedSlice,
        should_remove_on_drop: Arc<AtomicBool>,
    ) -> io::Result<Self> {
        let len = bytes.len();
        let offsets_slice = &bytes[len - METADATA_HEADER_SIZE..];
        let (start, len) = get_metadata_offsets(offsets_slice)
            .map_err(|e| io::Error::new(ErrorKind::Other, e))?;

        let mut aligned_metadata = AlignedVec::with_capacity(len as usize);
        aligned_metadata
            .extend_from_slice(&bytes[start as usize..(start + len) as usize]);
        let metadata = SegmentMetadata::from_buffer(&aligned_metadata)?;
        let fragment_info =
            deserialize_file::<FragmentInfo>(FRAGMENT_INFO_PATH, &metadata, &bytes)?;
        let block_locations_iter =
            deserialize_file::<BlockLocations>(BLOCK_LOCATIONS_PATH, &metadata, &bytes)?;
        let block_locations = HashMap::from_iter(block_locations_iter);

        Ok(Self {
            info: Arc::new(fragment_info),
            should_remove_on_drop,
            file_contents: bytes,
            metadata: Arc::new(metadata),
            blocks: Arc::new(block_locations),
        })
    }

    /// Get the fragment ID.
    pub fn id(&self) -> u64 {
        self.info.fragment_id
    }

    /// Get the fragment metadata
    pub fn info(&self) -> &FragmentInfo {
        &self.info
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
        let file = std::fs::File::open(path)?;
        let map = unsafe { Mmap::map(&file)? };
        let should_remove_on_drop = Arc::new(AtomicBool::new(false));
        Self::new(SharedSlice::from(map), should_remove_on_drop)
    }

    /// Read a virtual file from the fragment.
    pub fn read_file(&self, path: &str) -> Option<SharedSlice> {
        let range = self.metadata.get_location(path)?;
        Some(
            self.file_contents
                .slice(range.start as usize..range.end as usize),
        )
    }

    /// Reads a block from the fragment but leaves it in it's compressed form.
    pub fn read_block(&self, id: u64) -> Option<SharedSlice> {
        let info = self.blocks.get(&id)?.clone();
        Some(self.file_contents.slice(info.location_usize()))
    }

    /// Tells the reader that it should remove the file once all references
    /// to it have been dropped.
    pub fn set_remove_on_drop(&self) {
        self.should_remove_on_drop.store(true, Ordering::Relaxed);
    }
}

fn deserialize_file<T>(
    file_path: &str,
    metadata: &SegmentMetadata,
    data: &[u8],
) -> io::Result<T>
where
    T: Archive + 'static,
    T::Archived: CheckBytes<DefaultValidator<'static>>
        + Deserialize<T, SharedDeserializeMap>
        + 'static,
{
    let range = metadata.get_location(file_path).ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidData,
            format!("Failed to read file type {}, fragment is corrupted and must be repaired", type_name::<T>()),
        )
    })?;

    let range = range.start as usize..range.end as usize;
    let mut aligned = AlignedVec::with_capacity(range.len());
    aligned.extend_from_slice(&data[range]);

    // SAFETY:
    //      We ensure the target `T` is `'static` and contains only owned data so it's safe to
    //      temporarily extend the lifetime so we can allocate the type entirely.
    let slice = unsafe { mem::transmute::<&[u8], &'static [u8]>(aligned.as_slice()) };
    rkyv::from_bytes::<T>(slice).map_err(|_| {
        io::Error::new(
            ErrorKind::InvalidData,
            format!("Failed to read file type {}, fragment is corrupted and must be repaired", type_name::<T>()),
        )
    })
}
