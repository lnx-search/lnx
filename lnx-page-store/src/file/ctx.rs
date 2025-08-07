use std::sync::atomic::AtomicUsize;

use crate::file::arena::ArenaAllocator;
use crate::file::buffer;
use crate::layout::encrypt;

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
            arena_allocator: ArenaAllocator::new(4),
        }
    }

    /// Returns the encryption cipher if enabled.
    pub fn cipher(&self) -> Option<&encrypt::Cipher> {
        self.cipher.as_ref()
    }

    /// Allocates a new DMA buffer of a given number of a given size.
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

        if let Some(alloc) = self.arena_allocator.alloc(N) {
            buffer::DmaBuffer::from_arena(alloc)
        } else {
            buffer::DmaBuffer::alloc_sys(N / super::ALLOC_PAGE_SIZE)
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
    #[test]
    fn test_associated_data() {
        let data = super::associated_data(1, 4);
        assert_eq!(data, [1, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0]);
    }
}
