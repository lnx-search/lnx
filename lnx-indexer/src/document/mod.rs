mod stack_str;
pub mod walker;

use std::fmt::Debug;
use std::mem;
use std::net::Ipv6Addr;

use lnx_document::{DateTime, DocBlockReader};
use lnx_schema::indexing::IndexingSchema;
use smallvec::SmallVec;
use tantivy::schema::document::{
    DeserializeError,
    DocumentDeserialize,
    DocumentDeserializer,
    ReferenceValue,
};
use tantivy::schema::{DocValue, DocumentAccess, Facet, Field};

use self::walker::{IndexingDocData, IndexingDocWalker};
use crate::document::stack_str::SmallStr;
use crate::document::walker::FieldValue;

/// A document able to be indexed by tantivy.
pub struct IndexingDoc {
    /// The document metadata.
    metadata: BlockMetadata,
    /// The flattened document data.
    ///
    /// WARNING:
    /// This is not actually 'static and instead only lives for as long as `reader`.
    data: IndexingDocData<'static>,
    /// The owned document data.
    _reader: DocBlockReader,
}

impl IndexingDoc {
    /// Creates a new indexing doc from a block reader and doc id.
    pub fn new(
        metadata: BlockMetadata,
        reader: DocBlockReader,
        doc_id: usize,
        schema: &IndexingSchema,
    ) -> Self {
        let view = reader.doc(doc_id);

        // Data inside here lives for as long as `Arc<DocBlockReader>`.
        let mut data = IndexingDocData::default();
        let walker = IndexingDocWalker::new(schema, &mut data);

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
            metadata,
            data,
            _reader: reader,
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

impl DocumentAccess for IndexingDoc {
    type Value<'a> = IndexingValue<'a>;
    type FieldsValuesIter<'a> = FieldValueIter<'a>;

    fn iter_fields_and_values(&self) -> Self::FieldsValuesIter<'_> {
        FieldValueIter {
            metadata: DocMetadataIter::new(self.metadata),
            inner_iter: self.data.entries.iter(),
            field_names: &self.data.field_name,
        }
    }
}

/// A document fields iterator.
///
/// This iterator resolves any field keys if specified.
/// Since this iterates over flat data the values it produces
/// will only ever have one level of nesting.
pub struct FieldValueIter<'a> {
    /// The document metadata.
    metadata: DocMetadataIter,
    /// The inner document field iterator.
    inner_iter: std::slice::Iter<'a, FieldValue<'static>>,
    /// The field names which are referenced by the doc fields.
    field_names: &'a SmallVec<[SmallStr; 32]>,
}

impl<'a> Iterator for FieldValueIter<'a> {
    type Item = (Field, IndexingValue<'a>);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((field, value)) = self.metadata.next() {
            return Some((
                field,
                IndexingValue::Single(SingleIndexingValue::U64(value)),
            ));
        }

        let entry = self.inner_iter.next()?;

        let field_id = entry.field_id();
        let value = if let Some(key) = entry.key() {
            let key = self.field_names[key].as_str();
            IndexingValue::Object {
                key,
                value: SingleIndexingValue::from(entry.value()),
            }
        } else {
            IndexingValue::Single(SingleIndexingValue::from(entry.value()))
        };

        Some((field_id, value))
    }
}

#[derive(Copy, Clone, Debug)]
/// A value that can be used by tantivy indexers.
///
/// This is a slightly specialized enum for handling single values
/// and then the key-value pairs of an object (since that's the only type
/// which cannot be represented as a single value.)
pub enum IndexingValue<'a> {
    /// A single value entry.
    Single(SingleIndexingValue<'a>),
    /// A single key-value pair for an object.
    Object {
        key: &'a str,
        value: SingleIndexingValue<'a>,
    },
}

impl<'a> DocValue<'a> for IndexingValue<'a> {
    type ChildValue = SingleIndexingValue<'a>;
    type ArrayIter = std::iter::Empty<ReferenceValue<'a, Self::ChildValue>>;
    type ObjectIter = std::iter::Once<(&'a str, ReferenceValue<'a, Self::ChildValue>)>;

    #[inline]
    fn as_value(&self) -> ReferenceValue<'a, Self> {
        match self {
            IndexingValue::Single(value) => value.into_reference_value(),
            IndexingValue::Object { key, value } => {
                let field_key = &**key;
                let iter = std::iter::once((field_key, value.into_reference_value()));
                ReferenceValue::Object(iter)
            },
        }
    }
}

#[derive(Copy, Clone, Debug)]
/// A single raw value for indexing.
///
/// This value contains no collections, and is a slightly
/// awkward wrapper type for the `walker::Value` type.
pub enum SingleIndexingValue<'a> {
    /// A null value.
    Null,
    /// A string value.
    Str(&'a str),
    /// A u64 value.
    U64(u64),
    /// A i64 value.
    I64(i64),
    /// A f64 value.
    F64(f64),
    /// A bool value.
    Bool(bool),
    /// A ip value.
    Ip(Ipv6Addr),
    /// A date value.
    Date(DateTime),
    /// A facet value.
    Facet(&'a Facet),
    /// A bytes value.
    Bytes(&'a [u8]),
}

impl<'a> DocValue<'a> for SingleIndexingValue<'a> {
    // We never produce child values.
    type ChildValue = UnImplementedValue;
    type ArrayIter = std::iter::Empty<ReferenceValue<'a, Self::ChildValue>>;
    type ObjectIter = std::iter::Empty<(&'a str, ReferenceValue<'a, Self::ChildValue>)>;

    #[inline]
    fn as_value(&self) -> ReferenceValue<'a, Self> {
        self.into_reference_value()
    }
}

impl<'a> From<&'a walker::Value<'static>> for SingleIndexingValue<'a> {
    fn from(value: &'a walker::Value<'static>) -> Self {
        match value {
            walker::Value::Null => Self::Null,
            walker::Value::U64(v) => Self::U64(*v),
            walker::Value::I64(v) => Self::I64(*v),
            walker::Value::F64(v) => Self::F64(*v),
            walker::Value::Bool(v) => Self::Bool(*v),
            walker::Value::IpAddr(v) => Self::Ip(*v),
            walker::Value::Datetime(v) => Self::Date(*v),
            walker::Value::Str(v) => Self::Str(v),
            walker::Value::Bytes(v) => Self::Bytes(v),
            walker::Value::Facet(v) => Self::Facet(v),
        }
    }
}

impl<'a> SingleIndexingValue<'a> {
    fn into_reference_value<T>(self) -> ReferenceValue<'a, T>
    where
        T: DocValue<'a>,
    {
        match self {
            SingleIndexingValue::Null => ReferenceValue::Null,
            SingleIndexingValue::Str(val) => ReferenceValue::Str(val),
            SingleIndexingValue::U64(val) => ReferenceValue::U64(val),
            SingleIndexingValue::I64(val) => ReferenceValue::I64(val),
            SingleIndexingValue::F64(val) => ReferenceValue::F64(val),
            SingleIndexingValue::Bool(val) => ReferenceValue::Bool(val),
            SingleIndexingValue::Ip(val) => ReferenceValue::IpAddr(val),
            SingleIndexingValue::Date(val) => {
                ReferenceValue::Date(val.as_tantivy_value())
            },
            SingleIndexingValue::Facet(val) => ReferenceValue::Facet(val),
            SingleIndexingValue::Bytes(val) => ReferenceValue::Bytes(val),
        }
    }
}

#[derive(Debug)]
/// A doc value that implements no logic.
pub struct UnImplementedValue;
impl<'a> DocValue<'a> for UnImplementedValue {
    type ChildValue = Self;
    type ArrayIter = std::iter::Empty<ReferenceValue<'a, Self::ChildValue>>;
    type ObjectIter = std::iter::Empty<(&'a str, ReferenceValue<'a, Self::ChildValue>)>;

    fn as_value(&self) -> ReferenceValue<'a, Self> {
        unimplemented!()
    }
}

#[derive(Debug, Copy, Clone)]
/// The metadata for the specific document.
pub struct BlockMetadata {
    /// The field ID of the store file.
    pub block_store_file_id_field: Field,
    /// The actual file ID from the block store.
    pub block_store_file_id: u64,
    /// The field ID of the bytes offset.
    pub block_store_offset_field: Field,
    /// The number of bytes to skip before the doc block starts.
    pub block_store_offset: u64,
    /// The field ID of the HLC timestamp.
    pub doc_hlc_timestamp_field: Field,
    /// The HLC timestamp for the given document.
    pub doc_hlc_timestamp: u64,
}

/// A utility macro for defining iteration steps on the metadata iter.
macro_rules! iter_step {
    ($slf:expr, $field:ident, $field_id:ident, $value:ident) => {{
        if !$slf.$field {
            $slf.$field = true;

            return Some(($slf.metadata.$field_id, $slf.metadata.$value));
        }
    }};
}

/// A iterator that steps through the document metadata values.
struct DocMetadataIter {
    metadata: BlockMetadata,
    has_completed_file_id: bool,
    has_completed_offset: bool,
    has_completed_timestamp: bool,
}

impl DocMetadataIter {
    fn new(metadata: BlockMetadata) -> Self {
        Self {
            metadata,
            has_completed_file_id: false,
            has_completed_offset: false,
            has_completed_timestamp: false,
        }
    }
}

impl Iterator for DocMetadataIter {
    type Item = (Field, u64);

    fn next(&mut self) -> Option<Self::Item> {
        iter_step!(
            self,
            has_completed_file_id,
            block_store_file_id_field,
            block_store_file_id
        );
        iter_step!(
            self,
            has_completed_offset,
            block_store_offset_field,
            block_store_offset
        );
        iter_step!(
            self,
            has_completed_timestamp,
            doc_hlc_timestamp_field,
            doc_hlc_timestamp
        );

        None
    }
}
