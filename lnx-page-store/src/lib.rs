mod cache;
mod io;
mod layout;

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
#[rkyv(derive(Debug), compare(PartialEq))]
/// A unique identifier for a group of pages.
pub struct PageGroupId(pub(crate) u64);

#[derive(
    Debug,
    Copy,
    Clone,
    Ord,
    PartialOrd,
    Eq,
    PartialEq,
    Hash,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[rkyv(derive(Debug), compare(PartialEq))]
/// A unique identifier for a file of pages.
pub struct PageFileId(pub(crate) u64);

#[derive(
    Debug,
    Copy,
    Clone,
    Ord,
    PartialOrd,
    Eq,
    PartialEq,
    Hash,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[rkyv(derive(Debug), compare(PartialEq))]
/// A unique ID for a page of data within a storage file.
pub struct PageId(pub(crate) u32);

/// A RDMS style disk storage system.
///
/// All reads and writes are performed by io_uring, providing full asynchronous operations.
///
pub struct PageStore {}

impl PageStore {}
