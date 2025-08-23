use parking_lot::RwLock;

use super::dirty_marker::DirtyMarkerTable;
use crate::PageId;
use crate::layout::page_metadata::{PageMetadata, PageMetadataX63Bock};

#[derive(Default)]
/// A [LocalPageTable] contains a lookup table mapping [PageId]s to their
/// respective [PageMetadata] entries for a page file.
pub(super) struct LocalPageTable {
    blocks: RwLock<Vec<RwLock<PageMetadataX63Bock>>>,
    dirty_marker_table: DirtyMarkerTable,
}

impl LocalPageTable {
    /// Retrieves the page metadata for the associated page if it exists.
    pub(super) fn get_page(&self, page_id: PageId) -> Option<PageMetadata> {
        let block_id = (page_id.0 / 63) as usize;
        let page_index = (page_id.0 % 63) as usize;

        let blocks = self.blocks.read();
        // If the block ID is beyond the range of currently allocated blocks
        // the page must not exist.
        if block_id >= blocks.len() {
            return None;
        }

        let block = blocks[block_id].read();
        let page = &block.pages[page_index];
        if page.is_empty() { None } else { Some(*page) }
    }

    /// Set the page metadata for a target page.
    pub(super) fn insert_page(&self, metadata: PageMetadata) {
        let page_id = metadata.id;

        let block_id = (page_id.0 / 63) as usize;
        let page_index = (page_id.0 % 63) as usize;

        // Quick path, acquire reader and then drop down the block-level lock.
        let fill_n = {
            let blocks = self.blocks.read();
            if block_id < blocks.len() {
                let mut block = blocks[block_id].write();
                block.pages[page_index] = metadata;
                return;
            }
            block_id - blocks.len()
        };

        // Slow path, extend blocks array and then write.
        // Small optimisation to avoid double write locking is to pre-fill the block
        // we're about to modify.
        let mut blocks = self.blocks.write();
        for _ in 0..fill_n {
            blocks.push(RwLock::new(PageMetadataX63Bock::empty()));
        }

        let mut block = PageMetadataX63Bock::empty();
        block.pages[page_index] = metadata;
        blocks.push(RwLock::new(block));
        dbg!(blocks.len());
    }
}
