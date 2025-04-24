use std::mem::MaybeUninit;

use super::PAGE_SIZE;

#[derive(Clone)]
/// A writeable buffer of memory for encoding a page.
pub struct PageEncodeBuffer {
    initialised_len: usize,
    buffer: Box<[u8]>,
}

impl Default for PageEncodeBuffer {
    fn default() -> Self {
        let buffer = vec![0; PAGE_SIZE];
        Self {
            initialised_len: 0,
            buffer: buffer.into_boxed_slice(),
        }
    }
}

impl AsRef<[u8]> for PageEncodeBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.buffer[..self.initialised_len]
    }
}

impl PageEncodeBuffer {
    pub(crate) fn write_bytes(&mut self, bytes: &[u8]) {
        assert!(
            self.initialised_len + bytes.len() <= self.buffer.len(),
            "Page encoding tried to write out of bounds"
        );

        let start = self.initialised_len;
        let end = self.initialised_len + bytes.len();
        self.buffer[start..end].copy_from_slice(bytes);

        self.advance_cursor(bytes.len());
    }

    pub(crate) fn remaining_mut(&mut self) -> &mut [u8] {
        &mut self.buffer[self.initialised_len..]
    }

    pub(crate) fn advance_cursor(&mut self, by: usize) {
        self.set_cursor(self.initialised_len + by)
    }

    pub(crate) fn set_cursor(&mut self, pos: usize) {
        assert!(pos <= self.buffer.len(), "position out of bounds");
        self.initialised_len = pos;
    }

    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.buffer
    }

    pub(crate) fn total_size(&self) -> usize {
        self.buffer.len()
    }
}
