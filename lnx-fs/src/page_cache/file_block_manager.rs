use std::collections::VecDeque;

use dashmap::mapref::one::Ref;
use parking_lot::Mutex;

use crate::page_cache::block::PageId;
use crate::page_cache::utils::NoOpRandomState;
use crate::page_cache::{CacheError, FileBlockState, FileId};

/// A tracker that holds onto pages waiting to be marked as dirty and waiting to be freed.
pub struct FileBlockManager {
    /// The file blocks backing pages.
    file_blocks: dashmap::DashMap<FileId, FileBlockState, NoOpRandomState>,
    pending_evictions: Mutex<VecDeque<(FileId, PageId)>>,
    new_evictions: crossbeam_queue::SegQueue<(FileId, PageId)>,
}

impl Default for FileBlockManager {
    fn default() -> Self {
        Self {
            file_blocks: dashmap::DashMap::<u64, FileBlockState, _>::with_hasher(
                NoOpRandomState,
            ),
            pending_evictions: Mutex::default(),
            new_evictions: crossbeam_queue::SegQueue::new(),
        }
    }
}

impl FileBlockManager {
    #[inline]
    pub(super) fn get_block(
        &self,
        file_id: FileId,
    ) -> Result<Ref<'_, FileId, FileBlockState>, CacheError> {
        self.file_blocks
            .get(&file_id)
            .ok_or(CacheError::FileNotFound)
    }

    #[inline]
    pub(super) fn contains_block(&self, file_id: FileId) -> bool {
        self.file_blocks.contains_key(&file_id)
    }

    #[inline]
    pub(super) fn remove_block(&self, file_id: FileId) -> Option<FileBlockState> {
        self.file_blocks.remove(&file_id).map(|entry| entry.1)
    }

    #[inline]
    pub(super) fn entry(
        &self,
        file_id: FileId,
    ) -> dashmap::Entry<'_, FileId, FileBlockState> {
        self.file_blocks.entry(file_id)
    }

    pub(super) fn rebuild_block(&self, file_id: FileId) -> Result<(), CacheError> {
        let file_block = self.get_block(file_id)?;

        let size = file_block.block.allocated_size();
        let page_size = file_block.block.page_size();
        drop(file_block);

        // Create a new blank file block which allows us to zero the cache
        // for that file without scanning the existing cache.
        let block = FileBlockState::allocate(file_id, size, page_size)?;

        self.file_blocks.insert(file_id, block);

        Ok(())
    }

    pub(super) fn push_eviction(&self, file_id: FileId, page_id: PageId) {
        self.new_evictions.push((file_id, page_id));
        self.try_clear_evictions();
    }

    pub(super) fn try_clear_evictions(&self) {
        if let Some(mut buffer) = self.pending_evictions.try_lock() {
            while let Some(pair) = self.new_evictions.pop() {
                buffer.push_back(pair);
            }

            while let Some((file_id, page_id)) = buffer.pop_front() {
                if let Some(block) = self.file_blocks.get(&file_id) {
                    let did_set = block.try_mark_for_deletion(page_id);
                    if !did_set {
                        buffer.push_front((file_id, page_id));
                    }
                }
            }
        }
    }
}
