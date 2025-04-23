mod builder;
mod mem;
mod metadata;
#[cfg(test)]
mod tests;
mod version;
mod view;

use rkyv::rancor;

pub use self::builder::DiskPageBuilder;
pub use self::mem::PageEncodeBuffer;
use self::version::VersionProcessorRegistry;
pub use self::version::{ArchivedLayoutVersion, LayoutVersion, processors};
pub use self::view::{DiskPageView, OwnedDiskPageView};
use crate::page::metadata::{DiskPageMetadata, DiskPageMetadataRef};
use crate::{BlockId, PageId};

/// The total size of a page (metadata included) on disk.
pub const PAGE_SIZE: usize = 8 << 10;

#[derive(Debug, thiserror::Error)]
/// An error that prevented the page from being decoded into its view form.
pub enum PageDecodeError {
    #[error("{0}")]
    /// The [VersionProcessor](version::VersionProcessor) was unable to decode
    /// the raw data.
    ///
    /// This normally means the page is malformed/corrupted.
    Processor(anyhow::Error),
    #[error("processor for layout version {0:?} does not exist within the registry")]
    /// The processor required for decoding the target [LayoutVersion] does not exist
    /// within the [VersionProcessorRegistry] provided to the decode function.
    ProcessorNotFound(LayoutVersion),
    #[error("page data size is not {PAGE_SIZE}B, got {0}B")]
    /// The provided page data does not match the expected [PAGE_SIZE].
    IncorrectPageSize(usize),
    #[error("page malformed")]
    /// The page data was unable to be decoded into a [DiskPageView].
    PageMalformed,
    #[error("unknown layout version {0}")]
    /// The declared layout version of the page is unknown.
    UnknownLayoutVersion(String),
    #[error("page integrity check failed")]
    /// The page was decoded into a [DiskPageView] successfully but failed
    /// the additional integrity checks making the page logically malformed/corrupt.
    ///
    /// This can be the result of a partial data corruption of bad actor tamporing with the
    /// data located at the given page positions.
    IntegrityCheckFailed,
}

/// Additional parameters used to validate the integrity of the page data.
pub struct IntegrityCheckConditions {
    /// The expected block ID the data belongs to.
    pub block_id: BlockId,
    /// The expected page ID the data belongs to.
    pub page_id: PageId,
}

/// Decode a page into its [DiskPageView] and validate the integrity of the page.
///
/// This method expects the page data to be of size [PAGE_SIZE] in order to process
/// the page.
pub fn decode_page<'buf>(
    registry: &VersionProcessorRegistry,
    page_data: &'buf mut [u8],
    integrity_conditions: IntegrityCheckConditions,
) -> Result<DiskPageView<'buf>, PageDecodeError> {
    const HEADER_START_POS: usize = 8;

    if page_data.len() != PAGE_SIZE {
        return Err(PageDecodeError::IncorrectPageSize(page_data.len()));
    }

    let version_bytes = page_data[..2].try_into().unwrap();
    let layout_version =
        LayoutVersion::maybe_from_bytes(version_bytes).ok_or_else(|| {
            let code = format!("{:0x}", u16::from_le_bytes(version_bytes));
            PageDecodeError::UnknownLayoutVersion(code)
        })?;

    // let _pad_bytes = &page_data[2..8];

    let reserved_space_start = page_data.len() - layout_version.reserved_space();

    let slices_positions = [
        reserved_space_start..page_data.len(),
        HEADER_START_POS..reserved_space_start,
    ];
    let [reserved_space, encoded_bytes] =
        page_data.get_disjoint_mut(slices_positions).unwrap();

    let processor = registry
        .get_processor(layout_version)
        .ok_or_else(|| PageDecodeError::ProcessorNotFound(layout_version))?;

    processor
        .decode(encoded_bytes, reserved_space)
        .map_err(PageDecodeError::Processor)?;

    let page_view = DiskPageView::decode(encoded_bytes).map_err(|e| {
        tracing::error!(error = %e, "failed to decode page data");
        PageDecodeError::PageMalformed
    })?;

    let page_metadata = page_view.metadata();

    // The layout the page was actually encoded with is different to what was used to
    // decode the page, this should fail because we don't know if it worked by chance
    // or by a nefarious user.
    let is_valid = page_metadata.layout_version() == layout_version
        && page_metadata.block() == integrity_conditions.block_id
        && page_metadata.id() == integrity_conditions.page_id;

    if is_valid {
        Ok(page_view)
    } else {
        Err(PageDecodeError::IntegrityCheckFailed)
    }
}
