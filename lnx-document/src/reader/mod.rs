mod json;
pub mod view_access;

use std::mem;

use anyhow::{bail, Context, Result};
use rkyv::AlignedVec;

use crate::block_builder::DocBlock;
use crate::{ArchivedFieldType, Document};
use crate::reader::view_access::{DocViewDeserialize, DocViewDeserializer};

pub struct DocBlockReader {
    /// A view into the owned `data` as the doc block type rather than some bytes.
    ///
    /// It's important that `data` lives *longer* than this view as the view only lives
    /// for as long as `data.
    view: &'static rkyv::Archived<DocBlock<'static>>,
    data: AlignedVec,
}

impl DocBlockReader {
    #[inline]
    /// Attempts to create a new reader using some aligned data.
    ///
    /// The reader expects the last 4 bytes of the data to contain the crc32 checksum
    /// of the block which can be used to validate the state of the data.
    ///
    /// If the checksums do not match the block will be unable to be read.
    pub fn using_data(data: AlignedVec) -> Result<Self> {
        let slice_at = data.len() - mem::size_of::<u32>();
        let expected_checksum = u32::from_le_bytes(
            data[slice_at..]
                .try_into()
                .context("Cannot read checksum bytes, data corrupted")?,
        );
        let actual_checksum = crc32fast::hash(&data[..slice_at]);

        if expected_checksum != actual_checksum {
            bail!("Checksums of doc block do not match, expected: {expected_checksum} actual: {actual_checksum}");
        }

        // Safety:
        // We ensure the data tied to the slice we're borrowing lives at least for as long
        // as the view we're creating.
        // The slice is aligned to 16 bytes from the `AlignedVec` which is more than enough
        // for the doc block structure which requires an alignment of 8.
        let view = unsafe {
            let buffer = mem::transmute::<&[u8], &'static [u8]>(data.as_slice());
            rkyv::archived_root::<DocBlock<'static>>(&buffer[..slice_at])
        };

        Ok(Self { data, view })
    }

    #[inline]
    /// Returns the memory usage of the reader and it's data.
    pub fn memory_usage(&self) -> usize {
        self.data.len()
    }

    #[inline]
    /// Create a view for a given document within the block.
    pub fn doc(&self, idx: usize) -> DocumentView {
        DocumentView {
            block: self.view,
            doc: &self.view.documents[idx],
        }
    }
}

#[derive(Copy, Clone)]
/// A zero-copy view into a document within a block.
pub struct DocumentView<'block> {
    pub(crate) block: &'block rkyv::Archived<DocBlock<'static>>,
    pub(crate) doc: &'block rkyv::Archived<Document>,
}

impl<'block> DocumentView<'block> {
    #[inline]
    /// Returns the length of the document.
    pub fn len(&self) -> usize {
        self.doc.len as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.doc.len as usize == 0
    }

    /// Deserializes the view into a new type.
    pub fn deserialize_into<T>(&self) -> Result<T>
    where
        T: DocViewDeserialize<'block> + 'block
    {
        let deserializer = DocViewDeserializer::new(*self);
        T::deserialize(deserializer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rkyv_serializer::DocWriteSerializer;
    use crate::{ChecksumDocWriter, DocBlockBuilder, DocSerializer, DynamicDocument};

    #[test]
    fn test_reading_empty_block() {
        let mut builder = DocBlockBuilder::default();

        let writer = ChecksumDocWriter::from(AlignedVec::new());
        let mut serializer =
            DocSerializer::<512, _>::new(DocWriteSerializer::new(writer));
        builder
            .serialize_with(&mut serializer)
            .expect("serialization should be ok");

        let buffer = serializer.into_inner_serializer().into_inner();
        let data = buffer.finish();

        let view = DocBlockReader::using_data(data).expect("Read block successfully");
        assert!(
            view.view.documents.is_empty(),
            "No documents should be in block"
        );
    }

    #[test]
    fn test_reading_empty_doc() {
        let mut builder = DocBlockBuilder::default();

        let doc = DynamicDocument::default();

        let is_full = builder.add_document(doc);
        assert!(!is_full, "Builder should not be full");

        let writer = ChecksumDocWriter::from(AlignedVec::new());
        let mut serializer =
            DocSerializer::<512, _>::new(DocWriteSerializer::new(writer));
        builder
            .serialize_with(&mut serializer)
            .expect("serialization should be ok");

        let buffer = serializer.into_inner_serializer().into_inner();
        let data = buffer.finish();

        let view = DocBlockReader::using_data(data).expect("Read block successfully");
        let doc_view = view.doc(0);
        assert!(doc_view.is_empty(), "Document should be empty")
    }
}
