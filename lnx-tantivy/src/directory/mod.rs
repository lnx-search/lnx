mod memory;
mod vfs;

use std::ops::Deref;

use bytes::Bytes;

pub use self::memory::MemoryDirectory;
pub use self::vfs::VFSDirectory;

pub(crate) struct BytesWrapper(Bytes);

impl Deref for BytesWrapper {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

unsafe impl stable_deref_trait::StableDeref for BytesWrapper {}
