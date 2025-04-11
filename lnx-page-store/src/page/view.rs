use stable_deref_trait::StableDeref;
use super::metadata::DiskPageMetadataRef;

/// A static [DiskPageView] that owns the buffer used by the page.
///
/// The buffer must have correct alignment and must implement [StableDeref].
pub struct OwnedPageView<B> {
    /// Page lives for `'self` lifetime.
    inner: DiskPageView<'static>,
    /// The inner buffer holding the page data.
    buffer: B,
}

// Some repeating impls to ensure the lifetimes are correct while maintaining
// utility.
impl<B> OwnedPageView<B>
where
    B: StableDeref
{
    /// Returns a reference to the page metadata.
    pub fn metadata(&self) -> &DiskPageMetadataRef {
        self.inner.metadata
    }

    /// Returns the slice of data stored within the page.
    pub fn data(&self) -> &[u8] {
        self.inner.data
    }
}

/// A page is a fixed 8KB size of data that holds basic metadata with some inner
/// chunk of data representing part of a block.
pub struct DiskPageView<'buf> {
    /// The decoded page metadata.
    metadata: &'buf DiskPageMetadataRef,
    /// The decoded page data.
    data: &'buf [u8],
}

impl<'buf> DiskPageView<'buf> {
    
    
    /// Returns a reference to the page metadata.
    pub fn metadata(&self) -> &'buf DiskPageMetadataRef {
        self.metadata
    }

    /// Returns the slice of data stored within the page.
    pub fn data(&self) -> &'buf [u8] {
        self.data
    }
}