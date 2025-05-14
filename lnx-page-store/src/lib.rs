mod page;
mod file;

#[derive(
    Debug,
    Copy,
    Clone,
    Ord,
    PartialOrd,
    Eq,
    PartialEq,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[rkyv(derive(Debug))]
/// A unique identifier for a block of pages.
pub struct BlockId(pub(crate) u64);

#[derive(
    Debug,
    Copy,
    Clone,
    Ord,
    PartialOrd,
    Eq,
    PartialEq,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[rkyv(derive(Debug))]
/// A unique ID for a page of data within a storage file.
pub struct PageId(pub(crate) u32);

/// A RDMS style disk storage system.
///
/// All reads and writes are performed by io_uring, providing full asynchronous operations.
///
pub struct PageStore {}

impl PageStore {}
