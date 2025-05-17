use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};

/// The page is empty and can be freely written to.
pub const PAGE_FREE: u64 = 0b00;
/// The page is allocated and contains valid data.
///
/// It should not be altered until it is freed.
pub const PAGE_ALLOCATED: u64 = 0b10;
/// The page data has become old/dirty and scheduled be freed
/// or overwritten.
///
/// It is _NOT_ valid to set the flag back to [PAGE_ALLOCATED]
/// instead of re-reading.
pub const PAGE_EVICTION_SCHEDULED: u64 = 0b01;
/// The page data is still valid, but scheduled to be freed
/// because of memory pressure.
///
/// It is valid to simply set the flag back to [PAGE_ALLOCATED]
/// instead of re-reading.
pub const PAGE_REVERTIBLE_EVICTION_SCHEDULED: u64 = 0b11;

#[derive(Default)]
/// A set of [PageFlags] stored within a [AtomicU64].
pub struct AtomicPageFlags(AtomicU64);

impl Debug for AtomicPageFlags {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let flags = self.load(Ordering::Relaxed);
        flags.fmt(f)
    }
}

impl AtomicPageFlags {
    #[inline]
    /// Load the page flags with a given ordering.
    pub fn load(&self, ordering: Ordering) -> PageFlags {
        PageFlags(self.0.load(ordering))
    }

    pub(super) fn set_free(&self) {
        self.0.store(PAGE_FREE, Ordering::Relaxed);
    }

    pub(super) fn set_allocated(&self) {
        self.0.store(PAGE_ALLOCATED, Ordering::Relaxed);
    }

    pub(super) fn set_eviction(&self, generation: u64) {
        self.0.store(
            pack_generation(PAGE_EVICTION_SCHEDULED, generation),
            Ordering::Relaxed,
        );
    }

    pub(super) fn set_revertible_eviction(&self, generation: u64) {
        self.0.store(
            pack_generation(PAGE_REVERTIBLE_EVICTION_SCHEDULED, generation),
            Ordering::Relaxed,
        );
    }
}

#[derive(Default)]
/// The current flags of the page.
pub struct PageFlags(u64);

impl Debug for PageFlags {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PageFlags(allocated={}, marked_for_eviction={}, free={}, dirty={})",
            self.is_allocated(),
            self.is_marked_for_eviction(),
            self.is_free(),
            self.is_dirty(),
        )
    }
}

impl PageFlags {
    /// Is the page currently allocated and valid.
    pub fn is_allocated(&self) -> bool {
        self.0 & PAGE_ALLOCATED != 0
    }

    /// Is the page currently marked for eviction in order to be freed.
    pub fn is_marked_for_eviction(&self) -> bool {
        self.0 & PAGE_EVICTION_SCHEDULED != 0
    }

    /// Is the page empty/free.
    pub fn is_free(&self) -> bool {
        self.0 == PAGE_FREE
    }

    /// Is the page dirty/needs to be rewritten.
    pub fn is_dirty(&self) -> bool {
        !self.is_allocated() && self.is_marked_for_eviction()
    }

    /// Extracts the bit packed generation counter
    /// from the flags if applicable.
    pub fn extract_generation(&self) -> Option<u64> {
        if self.is_marked_for_eviction() {
            Some(extract_generation(self.0))
        } else {
            None
        }
    }
}

fn extract_generation(packed_bits: u64) -> u64 {
    packed_bits >> 2
}

fn pack_generation(flag: u64, generation: u64) -> u64 {
    assert!(generation < (1 << 62), "generation counter is too big");
    let shifted = generation << 2;
    shifted | flag
}

#[cfg(test)]
mod tests {
    use super::*;

    #[rstest::rstest]
    #[case::pack_allocated_flag(PAGE_ALLOCATED, 2)]
    #[case::pack_eviction_flag(PAGE_EVICTION_SCHEDULED, 3)]
    #[case::pack_revertible_eviction_flag(PAGE_REVERTIBLE_EVICTION_SCHEDULED, 4)]
    #[case::generation_big(PAGE_REVERTIBLE_EVICTION_SCHEDULED, 1 << 61)]
    #[should_panic]
    #[case::panic_generation_too_big(PAGE_REVERTIBLE_EVICTION_SCHEDULED, 1 << 62)]
    fn test_generation_bit_packing(#[case] flag: u64, #[case] generation: u64) {
        let packed = pack_generation(flag, generation);
        let extracted = extract_generation(packed);
        assert_eq!(
            extracted, generation,
            "extracted generation should match input"
        );
        assert_ne!(packed & flag, 0, "flags should still be maintained");
    }

    #[test]
    fn test_page_flags() {
        let flags = PageFlags(PAGE_FREE);
        assert!(flags.is_free());
        assert!(!flags.is_allocated());
        assert!(!flags.is_marked_for_eviction());
        assert!(!flags.is_dirty());
        assert!(flags.extract_generation().is_none());

        let flags = PageFlags(PAGE_ALLOCATED);
        assert!(!flags.is_free());
        assert!(flags.is_allocated());
        assert!(!flags.is_marked_for_eviction());
        assert!(!flags.is_dirty());
        assert!(flags.extract_generation().is_none());

        let flags = PageFlags(PAGE_REVERTIBLE_EVICTION_SCHEDULED);
        assert!(!flags.is_free());
        assert!(flags.is_allocated());
        assert!(flags.is_marked_for_eviction());
        assert!(!flags.is_dirty());
        assert_eq!(flags.extract_generation(), Some(0));

        let flags = PageFlags(PAGE_EVICTION_SCHEDULED);
        assert!(!flags.is_free());
        assert!(!flags.is_allocated());
        assert!(flags.is_marked_for_eviction());
        assert!(flags.is_dirty());
        assert_eq!(flags.extract_generation(), Some(0));
    }
}
