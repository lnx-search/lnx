use crate::PageId;

#[derive(Debug, Default)]
/// The [DirtyMarkerTable] tracks what pages are currently dirty and not represented
/// by the serialized page tables on disk.
///
/// This is done by representing each [PageMetadataX63Bock](crate::layout::page_metadata::PageMetadataX63Bock) as a `u64`
/// with the first `63` bits representing each individual page.
pub(super) struct DirtyMarkerTable {
    blocks: Vec<u64>,
}

impl DirtyMarkerTable {
    #[inline]
    /// Mark the given page as dirty.
    pub fn set_dirty(&mut self, page_id: PageId) {
        let (slot, bit) = get_slot_and_bit(page_id);
        self.ensure_num_slots(slot + 1);
        self.blocks[slot] |= 1 << bit;
    }

    #[inline]
    /// Iterate over any dirty blocks in the table producing the index of each
    /// block that is dirty.
    pub fn iter_dirty_blocks(&self) -> impl Iterator<Item = usize> {
        self.blocks
            .iter()
            .enumerate()
            .filter(|(_, bitset)| **bitset != 0)
            .map(|(index, _)| index)
    }

    fn ensure_num_slots(&mut self, num_slots: usize) {
        if self.blocks.len() < num_slots {
            self.blocks.resize(num_slots, 0);
        }
    }
}

fn get_slot_and_bit(page_id: PageId) -> (usize, u8) {
    let slot = (page_id.0 / 63) as usize;
    let bit = (page_id.0 % 63) as u8;
    (slot, bit)
}

#[cfg(all(test, not(feature = "test-miri")))]
mod tests {
    use super::*;

    #[rstest::rstest]
    #[case(0, (0, 0))]
    #[case(1, (0, 1))]
    #[case(2, (0, 2))]
    #[case(62, (0, 62))]
    #[case(63, (1, 0))]
    fn test_get_slot_and_bit(
        #[case] page_id: u32,
        #[case] expected_slot_and_bit: (usize, u8),
    ) {
        let actual = get_slot_and_bit(PageId(page_id));
        assert_eq!(actual, expected_slot_and_bit);
    }

    #[rstest::rstest]
    fn test_dirty_marker_table_grow(#[values(0, 1, 62, 63)] page_id: u32) {
        let mut table = DirtyMarkerTable::default();
        assert_eq!(table.blocks.len(), 0);

        table.set_dirty(PageId(page_id));
        assert_eq!(table.blocks.len(), (page_id / 63) as usize + 1);
    }

    #[rstest::rstest]
    fn test_dirty_marker_set(#[values(0, 1, 62, 63)] page_id: u32) {
        let mut table = DirtyMarkerTable::default();
        table.set_dirty(PageId(page_id));

        let target_block = (page_id / 63) as usize;

        let mut blocks = table.iter_dirty_blocks();
        assert_eq!(blocks.next(), Some(target_block));
        assert_eq!(blocks.next(), None);
    }
}