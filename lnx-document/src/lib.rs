extern crate core;

mod block_builder;
mod helpers;
mod reader;
mod rkyv_serializer;
mod serde_support;
mod value;
mod wrappers;

pub use helpers::UserDisplayType;
use rkyv::{Archive, Serialize};
pub use value::{DateTime, DynamicDocument, KeyValues, KeyValuesIter, Value};

pub use self::block_builder::DocBlockBuilder;
pub use self::reader::{DocBlockReader, DocumentView, view_access};
pub use self::rkyv_serializer::{ChecksumDocWriter, DocSerializer, DocSerializerError};

#[repr(u8)]
#[derive(Copy, Clone, Debug, Archive, Serialize, Eq, PartialEq)]
#[archive_attr(derive(Debug, Copy, Clone))]
pub enum FieldType {
    /// the field is of type `str`.
    String = 0,
    /// the field is of type `bytes`.
    Bytes = 1,
    /// the field is of type `bool`.
    Bool = 2,
    /// the field is of type `u64`.
    U64 = 3,
    /// the field is of type `i64`.
    I64 = 4,
    /// the field is of type `f64`.
    F64 = 5,
    /// the field is of type `null`.
    Null = 6,
    /// A datetime value stored as 2 x u64s.
    IpAddr = 7,

    // Aliased Types
    // These are types which are stored as another type because it makes more
    // sense to do so.
    /// A datetime value stored as a i64s.
    DateTime = 8,
    /// A hierarchical facet stored as a string.
    Facet = 9,

    // Collection special types
    /// An Array type.
    Array = 10,
    /// An Object type.
    Object = 11,
}

#[repr(C)]
#[derive(Copy, Clone, Debug, Archive, Serialize)]
#[cfg_attr(test, derive(PartialEq))]
#[archive_attr(derive(Debug))]
/// The raw binary structure describing part of the layout of a doc.
///
/// This type should be `4` bytes in size without padding.
pub struct Step {
    /// The ID of the field name.
    ///
    /// If the `field_id` is a `u16::MAX` value the value is part of an array
    /// or parent collection, and should not be treated as a `field_id`.
    field_id: u16,
    /// The length of the collection or the number of consecutive elements of the same types
    /// within an array.
    ///
    /// I.e. an array with 4 elements: `["hello", "bar", 1, false]` will have a length of **3**
    /// because it has 3 sets of consecutive types which can be merged:
    ///
    /// ```no_check
    /// [
    ///     Step { field_id: 1, field_length: 1, field_type: FieldType::Array },
    ///     Step { field_id: u16::MAX, field_length: 2, field_type: FieldType::String },
    ///     Step { field_id: u16::MAX, field_length: 1, field_type: FieldType::U64 },
    ///     Step { field_id: u16::MAX, field_length: 1, field_type: FieldType::Bool },
    /// ]
    /// ```
    ///
    /// The max length of a type is also the capacity of a `u8`, this is because the storage
    /// layout is designed in an optimistic fashion where it's
    field_length: u16,
    /// The type of the step, this can be both a value type and also a collection type.
    field_type: FieldType,
}

#[repr(C)]
#[derive(Clone, Debug, Default, Archive, Serialize)]
#[cfg_attr(test, derive(PartialEq))]
#[archive_attr(derive(Debug))]
pub struct Document {
    /// The number of top level fields in the document object.
    pub(crate) len: u32,
    #[with(rkyv::with::AsBox)]
    #[with(rkyv::with::CopyOptimize)]
    /// The core top-level fields in the document.
    pub(crate) layout: Vec<Step>,
}

impl Document {
    pub fn new(len: u32, capacity: usize) -> Self {
        Self {
            len,
            layout: Vec::with_capacity(capacity),
        }
    }

    pub fn add_step(&mut self, step: Step) {
        self.layout.push(step);
    }

    pub fn add_single_value_field(&mut self, field_id: u16, field_type: FieldType) {
        self.add_step(Step {
            field_id,
            field_type,
            field_length: 1,
        });
    }

    pub fn add_multi_value_field(
        &mut self,
        field_id: u16,
        field_type: FieldType,
        field_length: u16,
    ) {
        self.add_step(Step {
            field_id,
            field_type,
            field_length,
        });
    }

    #[inline]
    pub fn add_array(&mut self, field_id: u16, collection_length: u16) -> usize {
        self.add_step(Step {
            field_id,
            field_length: collection_length,
            field_type: FieldType::Array,
        });
        self.layout.len() - 1
    }

    #[inline]
    pub fn set_length(&mut self, idx: usize, collection_length: u16) {
        self.layout[idx].field_length = collection_length;
    }

    #[inline]
    pub fn add_array_entry(&mut self, field_type: FieldType, field_len: u16) -> usize {
        self.add_step(Step {
            field_id: u16::MAX,
            field_length: field_len,
            field_type,
        });
        self.layout.len() - 1
    }

    #[inline]
    pub fn add_object(&mut self, field_id: u16, collection_length: u16) -> usize {
        self.add_step(Step {
            field_id,
            field_length: collection_length,
            field_type: FieldType::Object,
        });
        self.layout.len() - 1
    }
}
