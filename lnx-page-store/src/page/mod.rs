mod builder;
mod mem;
mod metadata;
#[cfg(test)]
mod tests;
mod version;
mod view;

pub use self::builder::DiskPageBuilder;
pub use self::mem::PageEncodeBuffer;
pub use self::version::{ArchivedLayoutVersion, LayoutVersion};
pub use self::view::{DiskPageView, OwnedDiskPageView};

/// The total size of a page (metadata included) on disk.
pub const PAGE_SIZE: usize = 8 << 10;
