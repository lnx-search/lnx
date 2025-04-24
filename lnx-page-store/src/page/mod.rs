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
use crate::{BlockId, PageId};

/// The total size of a page (metadata included) on disk.
pub const PAGE_SIZE: usize = 8 << 10;
const HEADER_START_POS: usize = 8;

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
    #[error("page malformed: {0}")]
    /// The page data was unable to be decoded into a [DiskPageView].
    PageMalformed(rancor::Error),
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
///
/// A set of additional [IntegrityCheckConditions] should also be passed in order to
/// validate the integrity of the page aligns with what the system was expecting.
pub fn decode_page<'buf>(
    registry: &VersionProcessorRegistry,
    page_data: &'buf mut [u8],
    integrity_conditions: IntegrityCheckConditions,
) -> Result<DiskPageView<'buf>, PageDecodeError> {
    if page_data.len() != PAGE_SIZE {
        return Err(PageDecodeError::IncorrectPageSize(page_data.len()));
    }

    let (declared_version, page_bytes) =
        version_aware_decode_buffer(registry, page_data)?;

    let page_view =
        DiskPageView::decode(page_bytes).map_err(PageDecodeError::PageMalformed)?;

    let page_metadata = page_view.metadata();

    // The layout the page was actually encoded with is different to what was used to
    // decode the page, this should fail because we don't know if it worked by chance
    // or by a nefarious user.
    let is_valid = page_metadata.layout_version() == declared_version
        && page_metadata.block() == integrity_conditions.block_id
        && page_metadata.id() == integrity_conditions.page_id;

    if is_valid {
        Ok(page_view)
    } else {
        Err(PageDecodeError::IntegrityCheckFailed)
    }
}

#[derive(Debug, thiserror::Error)]
/// An error that prevented the system from encoding and serializing the page data into
/// a binary buffer.
pub enum PageEncodeError {
    #[error("{0}")]
    /// The [VersionProcessor](version::VersionProcessor) was unable to decode
    /// the raw data.
    ///
    /// This normally means the page is malformed/corrupted.
    Processor(anyhow::Error),
    #[error("processor for layout version {0:?} does not exist within the registry")]
    /// The processor required for encoding the target [LayoutVersion] does not exist
    /// within the [VersionProcessorRegistry] provided to the encode function.
    ProcessorNotFound(LayoutVersion),
    #[error("metadata serialize fail: {0}")]
    /// The page metadata could not be serialized to its binary format correctly.
    MetadataSerialize(rancor::Error),
}

/// Encode/serialize the given page data into a binary buffer for storing on disk.
pub fn encode_page(
    registry: &VersionProcessorRegistry,
    builder: DiskPageBuilder,
    buffer: &mut PageEncodeBuffer,
) -> Result<(), PageEncodeError> {
    let processor = registry
        .get_processor(builder.metadata.layout_version)
        .ok_or_else(|| {
            PageEncodeError::ProcessorNotFound(builder.metadata.layout_version)
        })?;

    builder
        .encode(buffer)
        .map_err(PageEncodeError::MetadataSerialize)?;

    let total_size = buffer.total_size();
    let reserved_bytes_start =
        total_size - builder.metadata.layout_version.reserved_space();

    let page_data = buffer.as_mut_slice();

    let slice_positions = [
        HEADER_START_POS..reserved_bytes_start,
        reserved_bytes_start..total_size,
    ];
    let [page_data, reserved_bytes] =
        page_data.get_disjoint_mut(slice_positions).unwrap();

    processor
        .encode(page_data, reserved_bytes)
        .map_err(PageEncodeError::Processor)?;

    // Set the cursor to the end of the buffer to mark it as complete.
    buffer.set_cursor(total_size);

    Ok(())
}

fn version_aware_decode_buffer<'buf>(
    registry: &VersionProcessorRegistry,
    page_data: &'buf mut [u8],
) -> Result<(LayoutVersion, &'buf mut [u8]), PageDecodeError> {
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

    Ok((layout_version, encoded_bytes))
}
