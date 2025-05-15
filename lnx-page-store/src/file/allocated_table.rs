use rkyv::{Archive, Deserialize, Serialize};

use crate::PageId;

#[derive(Default, Clone)]
/// A table header signalling what pages are allocated and what point in the operations
/// log the table accounts for.
pub struct PageAllocationTable {
    /// The current number of operations this table accounts for, there might be newer
    /// operations that this table has yet to account for via the operations log.
    checkpoint: u64,
    /// The page pages allocated up to the specified generation.
    allocated_pages: SimpleBitSet,
}

impl PageAllocationTable {
    /// Creates a new [PageAllocationTable] which can `size` number of pages
    /// rounded up to the nearest `64`.
    pub fn new(size: usize) -> Self {
        Self {
            checkpoint: 0,
            allocated_pages: SimpleBitSet::new(size),
        }
    }

    /// Returns if the current page is allocated or not.
    pub fn is_allocated(&self, page_id: PageId) -> bool {
        self.allocated_pages.get(page_id.0 as usize)
    }

    /// Returns the current checkpoint of the page table.
    pub fn checkpoint(&self) -> u64 {
        self.checkpoint
    }

    /// Advance the table's checkpoint by a given amount.
    pub fn advance_checkpoint(&mut self, by: u64) {
        self.checkpoint += by;
    }

    /// Mark the page as allocated.
    pub fn mark_allocated(&mut self, page_id: PageId) {
        self.allocated_pages.set(page_id.0 as usize);
    }

    /// Mark the given page as free / available for reuse.
    pub fn mark_free(&mut self, page_id: PageId) {
        self.allocated_pages.clear(page_id.0 as usize);
    }

    /// Serializes the current table into bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let true_size = size_of::<u64>()
            + size_of::<u32>()
            + (self.allocated_pages.size() * size_of::<u64>());
        let pad_by = true_size % 512;
        let padded_size = true_size + pad_by;

        let mut buffer = Vec::with_capacity(padded_size);
        buffer.extend_from_slice(&self.checkpoint.to_le_bytes());

        buffer
            .extend_from_slice(&(self.allocated_pages.data.len() as u32).to_le_bytes());
        for sector in self.allocated_pages.data.iter() {
            buffer.extend_from_slice(&sector.to_le_bytes());
        }

        // Padding to align on the disk sector.
        buffer.extend(std::iter::repeat_n(0, pad_by));

        buffer
    }

    /// Deserializes the table from the given set of bytes.
    pub fn from_bytes(buffer: &[u8]) -> Option<Self> {
        if buffer.len() < (size_of::<u64>() + size_of::<u32>()) {
            return None;
        }

        let generation = u64::from_le_bytes(buffer[..8].try_into().unwrap());
        let bitset_size = u32::from_le_bytes(buffer[8..12].try_into().unwrap()) as usize;

        let true_size =
            size_of::<u64>() + size_of::<u32>() + (bitset_size * size_of::<u64>());
        if buffer.len() < true_size {
            return None;
        }

        const OFFSET: usize = 12;
        let mut bitset = SimpleBitSet::new(bitset_size);
        for pos in 0..bitset_size {
            let bytes_start = OFFSET + pos * size_of::<u64>();
            let bytes_end = bytes_start + size_of::<u64>();

            let sector =
                u64::from_le_bytes(buffer[bytes_start..bytes_end].try_into().unwrap());
            bitset.data[pos] = sector;
        }

        Some(Self {
            checkpoint: generation,
            allocated_pages: bitset,
        })
    }
}

#[derive(Default, Clone)]
/// A simple bit set which can be serialized and deserialized with rkyv.
///
/// The size of the bitset is always aligned to 64.
struct SimpleBitSet {
    data: Box<[u64]>,
}

impl SimpleBitSet {
    /// Creates a new [SimpleBitSet] with the given `size` rounded up to the nearest
    /// 64 entries.
    fn new(mut size: usize) -> Self {
        size += size % 64;

        let data = vec![0; size];

        Self {
            data: data.into_boxed_slice(),
        }
    }

    /// Returns the size of the bitset.
    fn size(&self) -> usize {
        self.data.len() * 64
    }

    /// Set the flag at a given index.
    fn set(&mut self, index: usize) {
        let sector = index / 64;
        let offset = index % 64;
        self.data[sector] |= 1 << offset;
    }

    /// Clear the flag at a given index.
    fn clear(&mut self, index: usize) {
        let sector = index / 64;
        let offset = index % 64;
        self.data[sector] &= !(1 << offset);
    }

    /// Read the flag at the given index.
    fn get(&self, index: usize) -> bool {
        let sector = index / 64;
        let offset = index % 64;
        self.data[sector] & (1 << offset) != 0
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(14)]
    #[case(512)]
    #[should_panic]
    #[case(0)]
    fn test_bitset(#[case] size: usize) {
        let mut bitset = SimpleBitSet::new(size);

        assert!(!bitset.get(0));
        assert!(!bitset.get(7));
        assert!(!bitset.get(size - 1));

        bitset.set(0);
        bitset.set(7);
        bitset.set(size - 1);

        assert!(bitset.get(0));
        assert!(!bitset.get(1));
        assert!(!bitset.get(6));
        assert!(bitset.get(7));
        assert!(!bitset.get(size - 2));
        assert!(bitset.get(size - 1));

        bitset.clear(0);
        bitset.clear(7);
        bitset.clear(size - 1);

        assert!(!bitset.get(0));
        assert!(!bitset.get(7));
        assert!(!bitset.get(size - 1));
    }

    #[rstest]
    #[case(14)]
    #[case(512)]
    fn test_allocation_table(#[case] size: usize) {
        let mut table = PageAllocationTable::new(size);

        assert!(!table.is_allocated(PageId(0)));
        assert!(!table.is_allocated(PageId(5)));
        assert!(!table.is_allocated(PageId((size - 1) as u32)));

        table.mark_allocated(PageId(0));
        table.mark_allocated(PageId(5));
        table.mark_allocated(PageId((size - 1) as u32));

        assert!(table.is_allocated(PageId(0)));
        assert!(table.is_allocated(PageId(5)));
        assert!(table.is_allocated(PageId((size - 1) as u32)));

        let buffer = table.to_bytes();
        let copy = PageAllocationTable::from_bytes(&buffer)
            .expect("Page table should be able to be deserialized");

        assert!(table.is_allocated(PageId(0)));
        assert!(table.is_allocated(PageId(5)));
        assert!(table.is_allocated(PageId((size - 1) as u32)));
    }
}
