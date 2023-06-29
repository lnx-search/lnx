use std::alloc::Layout;
use std::error::Error;
use std::ptr::NonNull;
use std::{fmt, io};
use std::hash::Hasher;

use rkyv::ser::serializers::{AllocScratch, BufferScratch, FallbackScratch, HeapScratch, SharedSerializeMap};
use rkyv::ser::{ScratchSpace, Serializer, SharedSerializeRegistry};
use rkyv::{AlignedBytes, AlignedVec, Archive, ArchiveUnsized, Fallible, Infallible};

const STACK_SCRATCH: usize = 1024;


#[derive(Default)]
pub struct ChecksumDocWriter {
    inner: AlignedVec,
    hasher: crc32fast::Hasher,
}

impl From<AlignedVec> for ChecksumDocWriter {
    fn from(inner: AlignedVec) -> Self {
        Self {
            inner,
            hasher: crc32fast::Hasher::new(),
        }
    }
}

impl ChecksumDocWriter {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: AlignedVec::with_capacity(capacity),
            hasher: crc32fast::Hasher::new(),
        }
    }

    #[inline]
    pub fn finish(mut self) -> AlignedVec {
        let checksum = self.hasher.finalize();
        self.inner.extend_from_slice(&checksum.to_le_bytes());
        self.inner
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl io::Write for ChecksumDocWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.hasher.write(buf);
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// The default serializer error.
#[derive(Debug)]
pub enum DocSerializerError<const N: usize, S> {
    /// An error occurred while serializing
    Serializer(S),
    /// An error occurred while using scratch space
    ScratchSpace(<FallbackScratch<StackScratch<STACK_SCRATCH>, FallbackScratch<HeapScratch<N>, AllocScratch>> as Fallible>::Error),
    /// An error occurred while serializing shared memory
    Shared(<SharedSerializeMap as Fallible>::Error),
}

impl<S, const N: usize> fmt::Display for DocSerializerError<N, S>
where
    S: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Serializer(e) => write!(f, "serialization error: {}", e),
            Self::ScratchSpace(e) => write!(f, "scratch space error: {}", e),
            Self::Shared(e) => write!(f, "shared memory error: {}", e),
        }
    }
}

impl<S, const N: usize> Error for DocSerializerError<N, S>
where
    S: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Serializer(e) => Some(e as &dyn Error),
            Self::ScratchSpace(e) => Some(e as &dyn Error),
            Self::Shared(e) => Some(e as &dyn Error),
        }
    }
}

/// A serializer built from composable pieces.
#[derive(Debug)]
pub struct DocSerializer<const N: usize, S = Infallible> {
    serializer: S,
    scratch: FallbackScratch<StackScratch<STACK_SCRATCH>, FallbackScratch<HeapScratch<N>, AllocScratch>>,
    shared: SharedSerializeMap,
}

impl<const N: usize, S> DocSerializer<N, S> {
    /// Creates a new composite serializer from serializer components.
    #[inline]
    pub fn new(serializer: S) -> Self {
        Self {
            serializer,
            scratch: FallbackScratch::new(
                StackScratch::new(),
                FallbackScratch::new(
                    HeapScratch::new(),
                    AllocScratch::new(),
                )
            ),
            shared: SharedSerializeMap::new(),
        }
    }

    #[inline]
    /// Returns a mutable reference to the inner serializer
    pub fn inner_mut(&mut self) -> &mut S {
        &mut self.serializer
    }

    #[inline]
    /// Returns a reference to the inner serializer
    pub fn inner(&self) -> &S {
        &self.serializer
    }

    pub fn into_inner_serializer(self) -> S {
        self.serializer
    }
}

impl<S: Default, const N: usize> Default for DocSerializer<N, S> {
    #[inline]
    fn default() -> Self {
        Self::new(S::default())
    }
}

impl<S: Fallible, const N: usize> Fallible for DocSerializer<N, S> {
    type Error = DocSerializerError<N, S::Error>;
}

impl<S: Serializer, const N: usize> Serializer for DocSerializer<N, S> {
    #[inline]
    fn pos(&self) -> usize {
        self.serializer.pos()
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.serializer
            .write(bytes)
            .map_err(DocSerializerError::Serializer)
    }

    #[inline]
    fn pad(&mut self, padding: usize) -> Result<(), Self::Error> {
        self.serializer
            .pad(padding)
            .map_err(DocSerializerError::Serializer)
    }

    #[inline]
    fn align(&mut self, align: usize) -> Result<usize, Self::Error> {
        self.serializer
            .align(align)
            .map_err(DocSerializerError::Serializer)
    }

    #[inline]
    fn align_for<T>(&mut self) -> Result<usize, Self::Error> {
        self.serializer
            .align_for::<T>()
            .map_err(DocSerializerError::Serializer)
    }

    #[inline]
    unsafe fn resolve_aligned<T: Archive + ?Sized>(
        &mut self,
        value: &T,
        resolver: T::Resolver,
    ) -> Result<usize, Self::Error> {
        self.serializer
            .resolve_aligned::<T>(value, resolver)
            .map_err(DocSerializerError::Serializer)
    }

    #[inline]
    unsafe fn resolve_unsized_aligned<T: ArchiveUnsized + ?Sized>(
        &mut self,
        value: &T,
        to: usize,
        metadata_resolver: T::MetadataResolver,
    ) -> Result<usize, Self::Error> {
        self.serializer
            .resolve_unsized_aligned(value, to, metadata_resolver)
            .map_err(DocSerializerError::Serializer)
    }
}

impl<S: Fallible, const N: usize> ScratchSpace for DocSerializer<N, S> {
    #[inline]
    unsafe fn push_scratch(
        &mut self,
        layout: Layout,
    ) -> Result<NonNull<[u8]>, Self::Error> {
        self.scratch
            .push_scratch(layout)
            .map_err(DocSerializerError::ScratchSpace)
    }

    #[inline]
    unsafe fn pop_scratch(
        &mut self,
        ptr: NonNull<u8>,
        layout: Layout,
    ) -> Result<(), Self::Error> {
        self.scratch
            .pop_scratch(ptr, layout)
            .map_err(DocSerializerError::ScratchSpace)
    }
}

impl<S: Fallible, const N: usize> SharedSerializeRegistry for DocSerializer<N, S> {
    #[inline]
    fn get_shared_ptr(&self, value: *const u8) -> Option<usize> {
        self.shared.get_shared_ptr(value)
    }

    #[inline]
    fn add_shared_ptr(
        &mut self,
        value: *const u8,
        pos: usize,
    ) -> Result<(), Self::Error> {
        self.shared
            .add_shared_ptr(value, pos)
            .map_err(DocSerializerError::Shared)
    }
}

#[derive(Debug)]
/// A fixed size stack allocated scratch space.
pub struct StackScratch<const N: usize> {
    inner: BufferScratch<AlignedBytes<N>>,
}

impl<const N: usize> StackScratch<N> {
    /// Creates a new stack scratch space.
    pub fn new() -> Self {
        Self {
            inner: BufferScratch::new(AlignedBytes::default()),
        }
    }
}

impl<const N: usize> Default for StackScratch<N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize> Fallible for StackScratch<N> {
    type Error = <BufferScratch<AlignedBytes<N>> as Fallible>::Error;
}

impl<const N: usize> ScratchSpace for StackScratch<N> {
    #[inline]
    unsafe fn push_scratch(
        &mut self,
        layout: Layout,
    ) -> Result<NonNull<[u8]>, Self::Error> {
        self.inner.push_scratch(layout)
    }

    #[inline]
    unsafe fn pop_scratch(
        &mut self,
        ptr: NonNull<u8>,
        layout: Layout,
    ) -> Result<(), Self::Error> {
        self.inner.pop_scratch(ptr, layout)
    }
}

#[derive(Debug)]
pub struct DocWriteSerializer<W: io::Write> {
    inner: W,
    pos: usize,
}

impl<W: io::Write> DocWriteSerializer<W> {
    /// Creates a new serializer from a writer.
    #[inline]
    pub fn new(inner: W) -> Self {
        Self { inner, pos: 0 }
    }

    #[inline]
    /// Returns a mutable reference to the inner writer.
    pub fn writer_mut(&mut self) -> &mut W {
        &mut self.inner
    }

    #[inline]
    /// Returns a reference to the inner writer.
    pub fn writer(&self) -> &W {
        &self.inner
    }

    #[inline]
    /// Returns the inner writer
    pub fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: io::Write> Fallible for DocWriteSerializer<W> {
    type Error = io::Error;
}

impl<W: io::Write> Serializer for DocWriteSerializer<W> {
    #[inline]
    fn pos(&self) -> usize {
        self.pos
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.inner.write_all(bytes)?;
        self.pos += bytes.len();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use rkyv::ser::serializers::{AlignedSerializer, AllocScratch, CompositeSerializer};
    use rkyv::AlignedVec;

    use super::*;

    #[test]
    fn test_serializer() {
        let mut serializer =
            DocSerializer::<64, _>::new(AlignedSerializer::new(AlignedVec::new()));

        let msg = "Hello, world!".to_string();
        let pos = serializer.serialize_value(&msg).expect("Serialize string");

        assert_eq!(pos, 16, "Position start should be 16");
        let data = serializer.into_inner_serializer().into_inner();

        let msg_returned = rkyv::from_bytes::<String>(&data).expect("Deserialize type.");
        assert_eq!(msg, msg_returned);
    }

    #[test]
    fn test_writer_serializer() {
        let mut serializer = CompositeSerializer::new(
            DocWriteSerializer::new(AlignedVec::new()),
            AllocScratch::new(),
            SharedSerializeMap::new(),
        );

        let msg = "Hello, world!".to_string();
        let pos = serializer.serialize_value(&msg).expect("Serialize string");

        assert_eq!(pos, 16, "Position start should be 16");
        let mut serializer = serializer.into_serializer();
        serializer.writer_mut().flush().expect("Flush data");
        let data = serializer.into_inner();

        let msg_returned = rkyv::from_bytes::<String>(&data).expect("Deserialize type.");
        assert_eq!(msg, msg_returned);
    }
}
