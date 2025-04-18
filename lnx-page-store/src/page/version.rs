use super::PAGE_SIZE;
use super::metadata::DiskPageMetadataRef;

#[repr(u16)]
#[derive(
    Debug, Copy, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Eq, PartialEq,
)]
#[rkyv(derive(Debug, Copy, Clone))]
/// The version of the page layout.
pub enum LayoutVersion {
    /// V1 encoding wo/encryption.
    V1 = 0x01,
    /// V1 encoding w/encryption.
    V1Enc = 0x02,
}

impl LayoutVersion {
    pub(super) fn to_bytes(&self) -> [u8; 2] {
        (*self as u16).to_le_bytes()
    }

    pub(super) fn maybe_from_bytes(bytes: [u8; 2]) -> Option<Self> {
        let value = u16::from_le_bytes(bytes);
        match value {
            0x01 => Some(LayoutVersion::V1),
            0x02 => Some(LayoutVersion::V1Enc),
            _ => None,
        }
    }

    pub(super) const fn reserved_space(&self) -> usize {
        match self {
            LayoutVersion::V1 => 0,
            LayoutVersion::V1Enc => 120,
        }
    }

    pub(super) const fn max_data_size(&self) -> usize {
        /// The size rkyv takes up laying out the metadata in bytes.
        const RKYV_METADATA_OVERHEAD: usize = size_of::<DiskPageMetadataRef>();
        /// The overhead every page will have.
        ///
        /// Currently made up of the layout version bytes and remaining 6 bytes to keep
        /// buffer alignment and future signals.
        const CORE_OVERHEAD: usize = size_of::<LayoutVersion>() + 6;

        let variable_overhead = match self {
            LayoutVersion::V1 => RKYV_METADATA_OVERHEAD,
            LayoutVersion::V1Enc => RKYV_METADATA_OVERHEAD,
        };

        let mut total_overhead =
            CORE_OVERHEAD + variable_overhead + self.reserved_space();
        total_overhead += total_overhead % 8; // Align to 8 bytes.

        PAGE_SIZE - total_overhead
    }
}

impl From<ArchivedLayoutVersion> for LayoutVersion {
    fn from(value: ArchivedLayoutVersion) -> Self {
        match value {
            ArchivedLayoutVersion::V1 => Self::V1,
            ArchivedLayoutVersion::V1Enc => Self::V1Enc,
        }
    }
}
