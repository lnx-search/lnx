use std::ops::{BitAnd, BitAndAssign, BitOr, BitOrAssign};

#[derive(Debug, Copy, Clone, Default, Ord, PartialOrd, Eq, PartialEq)]
/// A set of upto 64k flags describing information about the page.
pub struct PageFlags(u16);

impl PageFlags {
    /// The page is encoded using the V1 standard encoder/decoder.
    pub const ENCODING_STD_V1: PageFlags = PageFlags(1 << 1);
    #[allow(unused)] // TODO: Implement encryption at rest.
    /// The page is encoded using the V1 encryption at rest encoder/decoder.
    pub const ENCODING_ENCRYPTED_V1: PageFlags = PageFlags(1 << 2);

    /// Encode the flags into raw bytes.
    pub fn to_bytes(&self) -> [u8; 2] {
        self.0.to_le_bytes()
    }

    /// Decode the flags from raw bytes.
    pub fn from_bytes(bytes: [u8; 2]) -> Self {
        Self(u16::from_le_bytes(bytes))
    }
}

impl BitOr for PageFlags {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

impl BitOrAssign for PageFlags {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0
    }
}

impl BitAnd for PageFlags {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self::Output {
        Self(self.0 & rhs.0)
    }
}

impl BitAndAssign for PageFlags {
    fn bitand_assign(&mut self, rhs: Self) {
        self.0 &= rhs.0
    }
}
