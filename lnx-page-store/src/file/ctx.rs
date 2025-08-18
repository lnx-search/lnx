use crate::file::arena::ArenaAllocator;
use crate::file::buffer;
use crate::layout::encrypt;
use crate::layout::file_metadata::Encryption;

/// The file context contains general settings and information
/// for the file reader and writers of any type to use.
pub struct FileContext {
    cipher: Option<encrypt::Cipher>,
    arena_allocator: ArenaAllocator,
}

impl FileContext {
    #[cfg(test)]
    /// Create a new file context for testing.
    pub fn for_test(encryption: bool) -> Self {
        use chacha20poly1305::aead::Key;
        use chacha20poly1305::{KeyInit, XChaCha20Poly1305};

        let cipher = if encryption {
            let key = Key::<XChaCha20Poly1305>::from_slice(
                b"F8E4FeD0098cF3Bf7968E1AC7Bbfacee",
            );
            Some(encrypt::Cipher::new(key))
        } else {
            None
        };

        Self {
            cipher,
            arena_allocator: ArenaAllocator::new(100),
        }
    }

    #[inline]
    /// Returns the encryption cipher if enabled.
    pub fn cipher(&self) -> Option<&encrypt::Cipher> {
        self.cipher.as_ref()
    }

    #[inline]
    /// Get the [Encryption] status for the given context.
    pub fn get_encryption_status(&self) -> Encryption {
        if self.cipher.is_none() {
            Encryption::Disabled
        } else {
            Encryption::Enabled
        }
    }

    /// Allocates a new DMA buffer of a given size ([N]) in bytes.
    ///
    /// This will attempt to use an arena allocator first and then fallback
    /// to the system allocator if no space is available.
    pub fn alloc<const N: usize>(&self) -> buffer::DmaBuffer {
        const {
            assert!(
                N % super::ALLOC_PAGE_SIZE == 0,
                "buffer size is not aligned to a 4kb size"
            )
        };

        if N == 0 {
            return buffer::DmaBuffer::alloc_empty();
        }

        if let Some(alloc) = self.arena_allocator.alloc(N / super::ALLOC_PAGE_SIZE) {
            buffer::DmaBuffer::from_arena(alloc)
        } else {
            buffer::DmaBuffer::alloc_sys(N / super::ALLOC_PAGE_SIZE)
        }
    }

    /// Allocates a new DMA buffer of a given a fixed number of pages.
    ///
    /// This will attempt to use an arena allocator first and then fallback
    /// to the system allocator if no space is available.
    ///
    /// Your should prefer [Self::alloc] over this implementation if at all possible.
    pub fn alloc_pages(&self, num_pages: usize) -> buffer::DmaBuffer {
        if num_pages == 0 {
            return buffer::DmaBuffer::alloc_empty();
        }

        if let Some(alloc) = self.arena_allocator.alloc(num_pages) {
            buffer::DmaBuffer::from_arena(alloc)
        } else {
            buffer::DmaBuffer::alloc_sys(num_pages)
        }
    }
}

/// Computes the associated data to tag file data with.
///
/// This method is used on all files and is used to prevent replay attacks
/// and a bad actor gaining information about the system by taking and swapping
/// around data in the files.
pub fn associated_data(file_id: u64, start_pos: u64) -> [u8; 16] {
    let mut buffer = [0; 16];
    buffer[0..8].copy_from_slice(&file_id.to_le_bytes());
    buffer[8..16].copy_from_slice(&start_pos.to_le_bytes());
    buffer
}

#[cfg(all(test, not(feature = "test-miri")))]
mod tests {
    use super::*;
    use crate::file::buffer::BufferKind;

    #[test]
    fn test_associated_data() {
        let data = associated_data(1, 4);
        assert_eq!(data, [1, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn test_encryption_status() {
        let ctx = FileContext::for_test(false);
        assert_eq!(ctx.get_encryption_status(), Encryption::Disabled);

        let ctx = FileContext::for_test(true);
        assert_eq!(ctx.get_encryption_status(), Encryption::Enabled);
    }

    #[rstest::rstest]
    #[case::empty(0, BufferKind::Empty)]
    #[case::arena(50, BufferKind::Arena)]
    #[case::arena(200, BufferKind::System)]
    fn test_alloc_pages(#[case] num_pages: usize, #[case] backing_type: BufferKind) {
        let ctx = FileContext::for_test(false);

        let buffer = ctx.alloc_pages(num_pages);
        assert_eq!(buffer.len(), crate::file::ALLOC_PAGE_SIZE * num_pages);
        assert_eq!(buffer.kind(), backing_type);
    }

    #[test]
    fn test_const_alloc() {
        let ctx = FileContext::for_test(false);

        let buffer = ctx.alloc::<0>();
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.kind(), BufferKind::Empty);

        let buffer = ctx.alloc::<{ 50 * crate::file::ALLOC_PAGE_SIZE }>();
        assert_eq!(buffer.len(), 50 * crate::file::ALLOC_PAGE_SIZE);
        assert_eq!(buffer.kind(), BufferKind::Arena);

        let buffer = ctx.alloc::<{ 200 * crate::file::ALLOC_PAGE_SIZE }>();
        assert_eq!(buffer.len(), 200 * crate::file::ALLOC_PAGE_SIZE);
        assert_eq!(buffer.kind(), BufferKind::System);
    }
}
