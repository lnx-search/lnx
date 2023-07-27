mod stack_str;
pub mod walker;

use std::mem;
use std::sync::Arc;

use lnx_document::DocBlockReader;
use lnx_schema::indexing::IndexingSchema;
use tantivy::schema::document::{
    DeserializeError,
    DocumentDeserialize,
    DocumentDeserializer,
};
use tantivy::schema::DocumentAccess;

use self::walker::{IndexingDocData, IndexingDocWalker};

/// A document able to be indexed by tantivy.
pub struct IndexingDoc {
    /// The indexing schema used to return field values.
    schema: Arc<IndexingSchema>,
    /// The flattened document data.
    ///
    /// WARNING:
    /// This is not actually 'static and instead only lives for as long as `reader`.
    data: IndexingDocData<'static>,
    /// The owned document data.
    reader: Arc<DocBlockReader>,
}

impl IndexingDoc {
    /// Creates a new indexing doc from a block reader and doc id.
    pub fn new(
        reader: Arc<DocBlockReader>,
        doc_id: usize,
        schema: Arc<IndexingSchema>,
    ) -> Self {
        let view = reader.doc(doc_id);

        // Data inside here lives for as long as `Arc<DocBlockReader>`.
        let mut data = IndexingDocData::default();
        let walker = IndexingDocWalker::new(&schema, &mut data);

        // This is infallible for our walker.
        view.traverse(walker).unwrap();

        // SAFETY:
        // The lifetime is artificially extended to be static, but it only lives for as long as the
        // block reader, this is an unavoidable issue unfortunately so care must be taken to ensure
        // this data does not out live the block reader.
        let data = unsafe {
            mem::transmute::<IndexingDocData<'_>, IndexingDocData<'static>>(data)
        };
        Self {
            schema,
            data,
            reader,
        }
    }
}

impl DocumentDeserialize for IndexingDoc {
    fn deserialize<'de, D>(_deserializer: D) -> Result<Self, DeserializeError>
    where
        D: DocumentDeserializer<'de>,
    {
        unimplemented!()
    }
}

// impl DocumentAccess for IndexingDoc {
//     type Value<'a> = IndexingValue<'a>;
//     type FieldsValuesIter<'a> = FieldValueIter<'a>;
//
//     fn iter_fields_and_values(&self) -> Self::FieldsValuesIter<'_> {
//         todo!()
//     }
// }
//
//
// pub struct FieldValueIter<'a> {
//     _foo: &'a (),
// }
//
// impl<'a> Iterator for FieldValueIter<'a> {
//     type Item = ();
//
//     fn next(&mut self) -> Option<Self::Item> {
//         todo!()
//     }
// }
//
// pub struct IndexingValue<'a> {
//     _foo: &'a (),
// }
