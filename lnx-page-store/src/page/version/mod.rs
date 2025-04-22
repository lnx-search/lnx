mod v1;
mod v1_enc;

use std::any::{Any, TypeId};
use super::{DiskPageView, PAGE_SIZE};
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
        /// The size of the encryption tag from ChaCha20.
        const CHA_CHA_20_TAG_SIZE: usize = 16;

        let variable_overhead = match self {
            LayoutVersion::V1 => RKYV_METADATA_OVERHEAD,
            LayoutVersion::V1Enc => RKYV_METADATA_OVERHEAD + CHA_CHA_20_TAG_SIZE,
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

/// A version processor decodes/encodes the inner page data in order
/// to apply additional rules or operations (i.e. encryption.)
trait VersionProcessor {
    /// Decode the provided page data so it can be read by the [DiskPageView].
    ///
    /// The reserved bytes for the given version is also provided.
    fn decode(
        &self,
        encoded_bytes: &mut [u8],
        reserved_bytes: &[u8],
    ) -> anyhow::Result<()>;

    /// Encode the raw page data with the version encoding.
    ///
    /// A pre-allocated buffer is provided for writing into the version's reserved space on the page.
    fn encode(
        &self,
        raw_bytes: &mut [u8],
        reserved_bytes: &mut [u8],
    ) -> anyhow::Result<()>;
}

#[derive(Debug, Default)]
/// A registry that stores pre-configured version encoders and decoders.
///
/// A [VersionProcessor] must be registered for the given [LayoutVersion] in order
/// to be decoded or encoded, otherwise an error will be returned when attempting
/// to use it.
pub struct VersionProcessorRegistry {
    processors: ahash::HashMap<TypeId, Box<dyn Any>>,
}

impl VersionProcessorRegistry {
    /// Creates a new [VersionProcessorRegistry] that has the default [v1::VersionV1Processor]
    /// attached.
    pub fn with_default_processors() -> Self {
        let mut slf = Self::default();
        slf.insert_processor(v1::VersionV1Processor::default());
        slf
    }
    
    /// Retrieve an existing, pre-configured [VersionProcessor] if it exists within the registry.
    fn get_processor<P: Any + VersionProcessor>(&self) -> Option<&P> {
        let type_id = TypeId::of::<P>();
        self.processors
            .get(&type_id)
            .and_then(|p| p.downcast_ref())
    }
    
    /// Insert a new [VersionProcessor] into the registry, replacing an existing entry if it
    /// already had a processor associated with the type.
    pub fn insert_processor<P: Any + VersionProcessor>(&mut self, processor: P) { 
        let type_id = TypeId::of::<P>();
        let boxed = Box::new(processor) as Box<dyn Any>;
        self.processors
            .insert(type_id, boxed);
    }
}