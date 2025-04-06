#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
/// A set of upto 64k flags describing information about the page.
pub struct PageFlags(u16);

impl PageFlags {
    /// The page is encoded using the V1 standard encoder/decoder.
    pub const ENCODING_STD_V1: PageFlags = PageFlags(1 << 1);
    #[allow(unused)] // TODO: Implement encryption at rest.
    /// The page is encoded using the V1 encryption at rest encoder/decoder.
    pub const ENCODING_ENCRYPTED_V1: PageFlags = PageFlags(1 << 2);
}
