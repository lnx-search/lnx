use std::mem::MaybeUninit;

use super::PAGE_SIZE;

#[derive(Clone)]
/// A writeable buffer of memory for encoding a page.
pub struct PageEncodeBuffer {
    initialised_len: usize,
    buffer: Box<[MaybeUninit<u8>]>,
}

impl Default for PageEncodeBuffer {
    fn default() -> Self {
        Self {
            initialised_len: 0,
            buffer: Box::new_uninit_slice(PAGE_SIZE),
        }
    }
}

impl AsRef<[u8]> for PageEncodeBuffer {
    fn as_ref(&self) -> &[u8] {
        // Safety: The buffer controls the initialised bytes and knows that all bytes up to
        //         `initialised_len` are valid and safe to read.
        unsafe {
            std::slice::from_raw_parts(
                self.buffer.as_ptr() as *const u8,
                self.initialised_len,
            )
        }
    }
}

impl PageEncodeBuffer {
    /// Write a set of bytes into the buffer.
    ///
    /// This method will panic if the bytes being written would go out of bounds.
    pub fn write_bytes(&mut self, bytes: &[u8]) {
        assert!(
            self.initialised_len + bytes.len() <= self.buffer.len(),
            "Page encoding tried to write out of bounds"
        );

        let write_ptr = self.buffer.as_mut_ptr() as *mut u8;
        let read_ptr = bytes.as_ptr();

        // Safety: We have already checked that the write will not go out of bounds.
        unsafe { std::ptr::copy_nonoverlapping(read_ptr, write_ptr, bytes.len()) };

        self.initialised_len += bytes.len();
    }

    /// Returns the slice of remaining uninitialized bytes.
    pub fn remaining_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        &mut self.buffer[self.initialised_len..]
    }

    /// Advances the internal cursor of initialised bytes by the given number of bytes.
    ///
    /// # Safety
    /// The caller must ensure no uninitialized bytes remain upto the point they are advancing
    /// the cursor by.
    pub unsafe fn advance_initialised_cursor(&mut self, by: usize) {
        assert!(
            self.initialised_len + by <= self.buffer.len(),
            "Cursor would go out of bounds"
        );
        self.initialised_len += by;
    }
}
