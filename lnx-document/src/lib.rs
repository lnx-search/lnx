extern crate core;

mod decoder;
mod encoder;
mod helpers;
pub mod json_value;
mod serializer;
pub mod typed_value;
mod wrappers;

pub use helpers::UserDiplayType;
use rkyv::{Archive, Serialize};

pub use self::decoder::{
    Archiver,
    BufferWalker,
    CheckedArchiver,
    Decoder,
    UnsafeArchiver,
};
pub use self::encoder::{ChecksumAndLenWriter, Encoder, DEFAULT_SCRATCH_SPACE};
pub use self::serializer::{DocSerializer, DocSerializerError};
use crate::wrappers::{Bytes, CopyWrapper, RawWrapper, Text};

#[repr(C)]
#[derive(Copy, Clone, Debug, Archive, Serialize)]
#[archive(check_bytes)]
/// The document information describing the field, number of values for the field
/// and the target type.
pub struct OffsetInfo {
    /// The unique ID for the given field.
    field_id: u16,
    /// The number of values for the given type within the field.
    field_length: u16,
    /// the target type of the field.
    ///
    /// A field may contain multiple types but each type is it's own
    /// [OffsetInfo] entry.
    field_type: FieldType,
}

#[repr(u16)]
#[derive(Copy, Clone, Debug, Archive, Serialize)]
#[archive(check_bytes)]
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

    // Aliased Types
    // These are types which are stored as another type because it makes more
    // sense to do so.
    /// A datetime value stored as a i64s.
    DateTime = 7,
    /// A datetime value stored as 2 x u64s.
    IpAddr = 8,
}

#[repr(C)]
#[derive(Clone, Debug, Default, Archive, Serialize)]
#[archive(check_bytes)]
/// A document block representing a collection of documents.
pub struct DocumentHeaderBlock {
    #[with(rkyv::with::AsBox)]
    /// The field mapping of field name to field ID (index in array).
    field_mapping: Vec<String>,
    #[with(rkyv::with::AsBox)]
    #[with(rkyv::with::Raw)]
    doc_offsets: Vec<u32>,
}

#[repr(C)]
#[derive(Clone, Debug, Default, Archive, Serialize)]
#[archive(check_bytes)]
pub struct Document<'a> {
    #[with(rkyv::with::AsBox)]
    #[with(rkyv::with::CopyOptimize)]
    /// The fields contained within the documents.
    info: Vec<OffsetInfo>,
    #[with(rkyv::with::AsBox)]
    /// All string values within the document.
    strings: Vec<Text<'a>>,
    #[with(rkyv::with::AsBox)]
    /// All bytes values within the document.
    bytes: Vec<Bytes>,
    /// All bool values within the document.
    bools: CopyWrapper<bool>,
    /// All u64 values within the document.
    u64s: RawWrapper<u64>,
    /// All i64 values within the document.
    i64s: RawWrapper<i64>,
    /// All f64 values within the document.
    f64s: RawWrapper<f64>,
}
