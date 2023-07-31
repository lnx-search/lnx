use std::borrow::Cow;
use std::collections::BTreeMap;
use std::mem::MaybeUninit;
use std::net::Ipv6Addr;
use std::{io, mem};

use anyhow::Result;
use rkyv::ser::Serializer;
use rkyv::{Archive, Serialize};

use crate::rkyv_serializer::DocWriteSerializer;
use crate::value::{DateTime, DynamicDocument, Value};
use crate::wrappers::{Bytes, CopyWrapper, RawWrapper, Text};
use crate::{DocSerializer, Document, Facet, FieldType};

/// The target size of a doc block in bytes.
const CAPACITY: usize = 512 << 10;

#[repr(C)]
#[derive(Clone, Debug, Archive, Serialize)]
#[cfg_attr(test, derive(PartialEq))]
pub struct DocBlock<'a> {
    #[with(rkyv::with::AsBox)]
    /// The field mapping of field name to field ID (index in array).
    pub(crate) field_mapping: Vec<Box<str>>,
    /// The documents and the layouts they have.
    #[with(rkyv::with::AsBox)]
    pub(crate) documents: Vec<Document>,
    // The block data
    #[with(rkyv::with::AsBox)]
    /// All string values within the block.
    pub(crate) strings: Vec<Text<'a>>,
    #[with(rkyv::with::AsBox)]
    /// All bytes values within the block.
    pub(crate) bytes: Vec<Bytes>,
    /// All bool values within the block.
    pub(crate) bools: CopyWrapper<bool>,
    /// All u64 values within the block.
    pub(crate) u64s: RawWrapper<u64>,
    /// All i64 values within the block.
    pub(crate) i64s: RawWrapper<i64>,
    /// All f64 values within the block.
    pub(crate) f64s: RawWrapper<f64>,
    /// All ip fields within the block.
    pub(crate) ips: CopyWrapper<Ipv6Addr>,
}

impl<'a> Default for DocBlock<'a> {
    fn default() -> Self {
        Self {
            field_mapping: Vec::with_capacity(4),
            documents: Vec::with_capacity(4),
            strings: Vec::with_capacity(2),
            bytes: Vec::new(),
            bools: Default::default(),
            u64s: Default::default(),
            i64s: Default::default(),
            f64s: Default::default(),
            ips: Default::default(),
        }
    }
}

impl<'a> ArchivedDocBlock<'a> {
    #[inline]
    pub fn get_field(&self, field_id: u16) -> &str {
        &self.field_mapping[field_id as usize]
    }
}

#[derive(Debug)]
pub struct DocBlockBuilder<'a> {
    unordered_key_lookup: BTreeMap<Cow<'a, str>, u16>,
    block: DocBlock<'a>,
    /// The approximate size of the block on disk.
    approx_data_size: usize,
}

impl<'a> Default for DocBlockBuilder<'a> {
    fn default() -> Self {
        Self {
            unordered_key_lookup: BTreeMap::new(),
            block: DocBlock::default(),
            approx_data_size: mem::size_of::<Self>(),
        }
    }
}

impl<'a> DocBlockBuilder<'a> {
    /// Adds a document object into the block.
    ///
    /// The map will be converted to the `[Document]` type and serialized.
    pub fn add_document(&mut self, doc: DynamicDocument<'a>) -> bool {
        // TODO: We can remove these small allocations if we re-use the document.
        let mut document = Document::new(doc.len() as u32, doc.len());
        for (key, value) in doc.0 {
            let field_id = self.get_key_idx(key);
            self.convert_value(&mut document, field_id, value);
        }

        self.block.documents.push(document);

        self.is_full()
    }

    /// Is the builder full.
    pub fn is_full(&self) -> bool {
        self.approx_data_size >= CAPACITY
    }

    /// Compresses the serialized buffer and reset's the serializer.
    pub fn serialize_with<const N: usize, W: io::Write>(
        &mut self,
        serializer: &mut DocSerializer<N, DocWriteSerializer<W>>,
    ) -> Result<()> {
        let field_mapping = self.build_field_mapping();
        self.block.field_mapping = field_mapping;
        serializer.serialize_value(&self.block)?;
        Ok(())
    }

    /// Builds a mapping of keys to their IDs.
    ///
    /// The ID of the field matches the index of the field.
    pub fn build_field_mapping(&self) -> Vec<Box<str>> {
        let mut keys: Vec<MaybeUninit<Box<str>>> =
            Vec::with_capacity(self.unordered_key_lookup.len());
        for _ in 0..self.unordered_key_lookup.len() {
            keys.push(MaybeUninit::uninit());
        }

        for (key, idx) in self.unordered_key_lookup.iter() {
            keys[*idx as usize].write(key.to_string().into_boxed_str());
        }

        unsafe { mem::transmute(keys) }
    }

    /// Get the key ID for this block.
    fn get_key_idx(&mut self, key: Cow<'a, str>) -> u16 {
        let key_size = key.as_bytes().len();
        let next_id = self.unordered_key_lookup.len() as u16;
        *self.unordered_key_lookup.entry(key).or_insert_with(|| {
            self.approx_data_size += key_size;
            next_id
        })
    }

    fn convert_value(&mut self, doc: &mut Document, field_id: u16, value: Value<'a>) {
        match value {
            Value::Null => {
                doc.add_single_value_field(field_id, FieldType::Null);
            },
            Value::Str(v) => {
                doc.add_single_value_field(field_id, FieldType::String);
                self.approx_data_size += v.as_bytes().len();
                self.block.add_value(v);
            },
            Value::U64(v) => {
                doc.add_single_value_field(field_id, FieldType::U64);
                self.approx_data_size += mem::size_of::<u64>();
                self.block.add_value(v);
            },
            Value::I64(v) => {
                doc.add_single_value_field(field_id, FieldType::I64);
                self.approx_data_size += mem::size_of::<i64>();
                self.block.add_value(v);
            },
            Value::F64(v) => {
                doc.add_single_value_field(field_id, FieldType::F64);
                self.approx_data_size += mem::size_of::<f64>();
                self.block.add_value(v);
            },
            Value::Bool(v) => {
                doc.add_single_value_field(field_id, FieldType::Bool);
                self.approx_data_size += mem::size_of::<bool>();
                self.block.add_value(v);
            },
            Value::Facet(facet) => {
                doc.add_single_value_field(field_id, FieldType::Facet);
                self.approx_data_size += facet.0.as_bytes().len();
                self.block.add_value(facet);
            },
            Value::DateTime(v) => {
                doc.add_single_value_field(field_id, FieldType::DateTime);
                self.approx_data_size += mem::size_of::<DateTime>();
                self.block.add_value(v);
            },
            Value::IpAddr(v) => {
                doc.add_single_value_field(field_id, FieldType::IpAddr);
                self.approx_data_size += mem::size_of::<Ipv6Addr>();
                self.block.add_value(v);
            },
            Value::Bytes(v) => {
                doc.add_single_value_field(field_id, FieldType::Bytes);
                self.approx_data_size += v.len();
                self.block.add_value(v);
            },
            Value::Array(elements) => {
                self.convert_array(doc, field_id, elements);
            },
            Value::Object(mapping) => {
                assert!(
                    mapping.len() <= u16::MAX as usize,
                    "Only a maximum of {} values are supported per-collection.",
                    u16::MAX,
                );
                doc.add_object(field_id, mapping.len() as u16);

                for (key, value) in mapping {
                    let field_id = self.get_key_idx(key);
                    self.convert_value(doc, field_id, value);
                }
            },
        }
    }

    fn convert_array(
        &mut self,
        doc: &mut Document,
        field_id: u16,
        elements: Vec<Value<'a>>,
    ) {
        let idx = doc.add_array(field_id, 0);

        let mut num_steps_added = 0;
        let mut current_type = FieldType::String;
        let mut current_count = 0;
        for value in elements {
            handle_array_entry_type_change(
                &mut current_count,
                &mut current_type,
                &mut num_steps_added,
                value.as_field_type(),
                doc,
            );

            match value {
                Value::Null => {},
                Value::Str(v) => {
                    self.approx_data_size += v.as_bytes().len();
                    self.block.add_value(v);
                },
                Value::U64(v) => {
                    self.approx_data_size += mem::size_of::<u64>();
                    self.block.add_value(v);
                },
                Value::I64(v) => {
                    self.approx_data_size += mem::size_of::<i64>();
                    self.block.add_value(v);
                },
                Value::F64(v) => {
                    self.approx_data_size += mem::size_of::<f64>();
                    self.block.add_value(v);
                },
                Value::Bool(v) => {
                    self.approx_data_size += mem::size_of::<bool>();
                    self.block.add_value(v);
                },
                Value::Facet(facet) => {
                    self.approx_data_size += facet.0.as_bytes().len();
                    self.block.add_value(facet);
                },
                Value::DateTime(v) => {
                    self.approx_data_size += mem::size_of::<DateTime>();
                    self.block.add_value(v);
                },
                Value::IpAddr(v) => {
                    self.approx_data_size += mem::size_of::<Ipv6Addr>();
                    self.block.add_value(v);
                },
                Value::Bytes(v) => {
                    self.approx_data_size += v.len();
                    self.block.add_value(v);
                },
                Value::Array(entries) => {
                    // Order of execution is important here.
                    // We need to add the previous type before the collections are added
                    // otherwise we wont re-produce the document correctly.
                    if current_count > 0 {
                        doc.add_array_entry(current_type, current_count);

                        current_count = 0;
                        current_type = FieldType::Array;
                        num_steps_added += 1;
                    }

                    self.convert_array(doc, u16::MAX, entries);

                    // Collections are always going to contribute 1 step.
                    num_steps_added += 1;
                },
                Value::Object(mapping) => {
                    if current_count > 0 {
                        doc.add_array_entry(current_type, current_count);

                        current_count = 0;
                        current_type = FieldType::Object;
                        num_steps_added += 1;
                    }
                    // Collections are always going to contribute 1 step.
                    num_steps_added += 1;

                    assert!(
                        mapping.len() <= u16::MAX as usize,
                        "Only a maximum of {} values are supported per-collection.",
                        u16::MAX,
                    );
                    doc.add_array_entry(FieldType::Object, mapping.len() as u16);
                    for (key, value) in mapping {
                        let field_id = self.get_key_idx(key);
                        self.convert_value(doc, field_id, value);
                    }
                },
            }
        }

        if current_count > 0 {
            doc.add_array_entry(current_type, current_count);
            num_steps_added += 1;
        }

        doc.set_length(idx, num_steps_added);
    }
}

fn handle_array_entry_type_change(
    counter: &mut u16,
    current_type: &mut FieldType,
    num_steps_added: &mut u16,
    value_type: FieldType,
    doc: &mut Document,
) {
    if *current_type == value_type {
        maybe_increment(value_type, counter);
        return;
    }

    if *counter > 0 {
        doc.add_array_entry(*current_type, *counter);
        (*num_steps_added) += 1;
        (*counter) = 0;
    }
    (*current_type) = value_type;
    maybe_increment(value_type, counter);
}

fn maybe_increment(value_type: FieldType, counter: &mut u16) {
    // We have to special case the collection types here so we don't
    // incorrectly add 2 steps for the same collection.
    if !matches!(value_type, FieldType::Object | FieldType::Array) {
        (*counter) += 1;
    }
}

/// Describes a type supported for storage in the doc block.
pub trait DocValue<T> {
    fn add_value(&mut self, value: T);
}

impl<'a> DocValue<Cow<'a, str>> for DocBlock<'a> {
    fn add_value(&mut self, value: Cow<'a, str>) {
        self.strings.push(Text::from(value));
    }
}

impl<'a> DocValue<String> for DocBlock<'a> {
    fn add_value(&mut self, value: String) {
        self.strings.push(Text::from(value));
    }
}

impl<'a> DocValue<&'a str> for DocBlock<'a> {
    fn add_value(&mut self, value: &'a str) {
        self.strings.push(Text::from(value));
    }
}

impl<'a> DocValue<Text<'a>> for DocBlock<'a> {
    fn add_value(&mut self, value: Text<'a>) {
        self.strings.push(value);
    }
}

impl<'a> DocValue<Facet<'a>> for DocBlock<'a> {
    fn add_value(&mut self, value: Facet<'a>) {
        self.strings.push(Text::from(value.0));
    }
}

impl<'a> DocValue<u64> for DocBlock<'a> {
    fn add_value(&mut self, value: u64) {
        self.u64s.push(value);
    }
}

impl<'a> DocValue<i64> for DocBlock<'a> {
    fn add_value(&mut self, value: i64) {
        self.i64s.push(value);
    }
}

impl<'a> DocValue<f64> for DocBlock<'a> {
    fn add_value(&mut self, value: f64) {
        self.f64s.push(value);
    }
}

impl<'a> DocValue<DateTime> for DocBlock<'a> {
    fn add_value(&mut self, value: DateTime) {
        self.i64s.push(value.as_micros());
    }
}

impl<'a> DocValue<Ipv6Addr> for DocBlock<'a> {
    fn add_value(&mut self, value: Ipv6Addr) {
        self.ips.push(value);
    }
}

impl<'a> DocValue<Bytes> for DocBlock<'a> {
    fn add_value(&mut self, value: Bytes) {
        self.bytes.push(value);
    }
}

impl<'a> DocValue<Vec<u8>> for DocBlock<'a> {
    fn add_value(&mut self, value: Vec<u8>) {
        self.bytes.push(Bytes::from(value));
    }
}

impl<'a> DocValue<bool> for DocBlock<'a> {
    fn add_value(&mut self, value: bool) {
        self.bools.push(value);
    }
}

#[cfg(test)]
mod test {
    use rkyv::AlignedVec;

    use super::*;
    use crate::Step;

    macro_rules! doc {
        () => {{
            crate::DynamicDocument::default()
        }};
        ($($key:expr => $value:expr $(,)?)+) => {{
            let doc = vec![
                $(
                    (Cow::Borrowed($key), $value.into()),
                )+
            ];

            crate::DynamicDocument::from(doc)
        }};
    }

    macro_rules! test_basic_type {
        ($value:expr, $tp:expr, $attr:ident) => {{
            let mut builder = DocBlockBuilder::default();
            let doc = doc! {
                "field_demo" => $value
            };

            let len = doc.len() as u32;
            let is_full = builder.add_document(doc);
            assert!(!is_full, "Builder should not be full");
            assert_eq!(
                builder.block,
                DocBlock {
                    documents: vec![Document {
                        len,
                        layout: vec![Step {
                            field_id: 0,
                            field_length: 1,
                            field_type: $tp,
                        },],
                    }],
                    $attr: vec![$value].into(),
                    ..Default::default()
                }
            );
        }};
    }

    #[test]
    fn test_single_values() {
        test_basic_type!(
            Text::from("Billy Bobby Biggings"),
            FieldType::String,
            strings
        );
        test_basic_type!(12u64, FieldType::U64, u64s);
        test_basic_type!(-124i64, FieldType::I64, i64s);
        test_basic_type!(12.30, FieldType::F64, f64s);
        test_basic_type!(false, FieldType::Bool, bools);
        test_basic_type!(Bytes(b"Hello".to_vec()), FieldType::Bytes, bytes);
        test_basic_type!(Ipv6Addr::LOCALHOST, FieldType::IpAddr, ips);

        let mut builder = DocBlockBuilder::default();
        let doc = doc! {
            "field_demo" => DateTime::MAX
        };
        let is_full = builder.add_document(doc);
        assert!(!is_full, "Builder should not be full");
        assert_eq!(
            builder.block,
            DocBlock {
                documents: vec![Document {
                    len: 1,
                    layout: vec![Step {
                        field_id: 0,
                        field_length: 1,
                        field_type: FieldType::DateTime,
                    },],
                }],
                i64s: vec![DateTime::MAX.as_micros()].into(),
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_arrays() {
        let mut builder = DocBlockBuilder::default();
        let doc = doc! {
            "names" => vec![
                "Bobby",
                "Timmy",
                "John",
            ]
        };

        let len = doc.len() as u32;
        let is_full = builder.add_document(doc);
        assert!(!is_full, "Builder should not be full");
        assert_eq!(
            builder.block,
            DocBlock {
                documents: vec![Document {
                    len,
                    layout: vec![
                        Step {
                            field_id: 0,
                            field_length: 1,
                            field_type: FieldType::Array,
                        },
                        Step {
                            field_id: u16::MAX,
                            field_length: 3,
                            field_type: FieldType::String,
                        },
                    ],
                }],
                strings: vec![
                    Text::from("Bobby"),
                    Text::from("Timmy"),
                    Text::from("John"),
                ],
                ..Default::default()
            }
        );

        let mut builder = DocBlockBuilder::default();
        let doc = doc! {
            "ages" => vec![
                1231u64,
                12u64,
                912u64
            ],
            "names" => vec![
                "Bobby",
                "Timmy",
                "John",
            ],
        };

        let len = doc.len() as u32;
        let is_full = builder.add_document(doc);
        assert!(!is_full, "Builder should not be full");
        assert_eq!(
            builder.block,
            DocBlock {
                documents: vec![Document {
                    len,
                    layout: vec![
                        Step {
                            field_id: 0,
                            field_length: 1,
                            field_type: FieldType::Array,
                        },
                        Step {
                            field_id: u16::MAX,
                            field_length: 3,
                            field_type: FieldType::U64,
                        },
                        Step {
                            field_id: 1,
                            field_length: 1,
                            field_type: FieldType::Array,
                        },
                        Step {
                            field_id: u16::MAX,
                            field_length: 3,
                            field_type: FieldType::String,
                        },
                    ],
                }],
                strings: vec![
                    Text::from("Bobby"),
                    Text::from("Timmy"),
                    Text::from("John"),
                ],
                u64s: vec![1231, 12, 912].into(),
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_nested_arrays() {
        let mut builder = DocBlockBuilder::default();
        let doc = doc! {
            "field_demo" => vec![
                vec![
                    "nested-one",
                    "nested-two",
                    "nested-three",
                ],
            ]
        };

        let len = doc.len() as u32;
        let is_full = builder.add_document(doc);
        assert!(!is_full, "Builder should not be full");
        assert_eq!(
            builder.block,
            DocBlock {
                documents: vec![Document {
                    len,
                    layout: vec![
                        Step {
                            field_id: 0,
                            field_length: 1,
                            field_type: FieldType::Array,
                        },
                        Step {
                            field_id: u16::MAX,
                            field_length: 1,
                            field_type: FieldType::Array,
                        },
                        Step {
                            field_id: u16::MAX,
                            field_length: 3,
                            field_type: FieldType::String,
                        },
                    ],
                }],
                strings: vec![
                    Text::from("nested-one"),
                    Text::from("nested-two"),
                    Text::from("nested-three"),
                ],
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_objects_in_arrays() {
        let mut builder = DocBlockBuilder::default();
        let doc = doc! {
            "field_demo" => vec![
                vec![
                    doc! {
                        "name" => "bobby"
                    },
                ],
            ]
        };

        let len = doc.len() as u32;
        let is_full = builder.add_document(doc);
        assert!(!is_full, "Builder should not be full");
        assert_eq!(
            builder.block,
            DocBlock {
                documents: vec![Document {
                    len,
                    layout: vec![
                        Step {
                            field_id: 0,
                            field_length: 1,
                            field_type: FieldType::Array,
                        },
                        Step {
                            field_id: u16::MAX,
                            field_length: 1,
                            field_type: FieldType::Array,
                        },
                        Step {
                            field_id: u16::MAX,
                            field_length: 1,
                            field_type: FieldType::Object,
                        },
                        Step {
                            field_id: 1,
                            field_length: 1,
                            field_type: FieldType::String,
                        },
                    ],
                }],
                strings: vec![Text::from("bobby"),],
                ..Default::default()
            }
        );

        let mut builder = DocBlockBuilder::default();
        let doc = doc! {
            "field_demo" => vec![
                doc! {
                    "name" => "bobby"
                },
            ]
        };

        let len = doc.len() as u32;
        let is_full = builder.add_document(doc);
        assert!(!is_full, "Builder should not be full");
        assert_eq!(
            builder.block,
            DocBlock {
                documents: vec![Document {
                    len,
                    layout: vec![
                        Step {
                            field_id: 0,
                            field_length: 1,
                            field_type: FieldType::Array,
                        },
                        Step {
                            field_id: u16::MAX,
                            field_length: 1,
                            field_type: FieldType::Object,
                        },
                        Step {
                            field_id: 1,
                            field_length: 1,
                            field_type: FieldType::String,
                        },
                    ],
                }],
                strings: vec![Text::from("bobby"),],
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_value_ordering() {
        let mut builder = DocBlockBuilder::default();
        let doc = doc! {
            "before-nesting" => "something",
            "field_demo" => vec![
                vec![
                    doc! {
                        "name" => "bobby",
                        "something-else" => "bobby2",
                    },
                ],
            ],
            "some_other_field" => "name-here",
        };

        let len = doc.len() as u32;
        let is_full = builder.add_document(doc);
        assert!(!is_full, "Builder should not be full");
        assert_eq!(
            builder.block,
            DocBlock {
                documents: vec![Document {
                    len,
                    layout: vec![
                        Step {
                            field_id: 0,
                            field_length: 1,
                            field_type: FieldType::String,
                        },
                        Step {
                            field_id: 1,
                            field_length: 1,
                            field_type: FieldType::Array,
                        },
                        Step {
                            field_id: u16::MAX,
                            field_length: 1,
                            field_type: FieldType::Array,
                        },
                        Step {
                            field_id: u16::MAX,
                            field_length: 2,
                            field_type: FieldType::Object,
                        },
                        Step {
                            field_id: 2,
                            field_length: 1,
                            field_type: FieldType::String,
                        },
                        Step {
                            field_id: 3,
                            field_length: 1,
                            field_type: FieldType::String,
                        },
                        Step {
                            field_id: 4,
                            field_length: 1,
                            field_type: FieldType::String,
                        },
                    ],
                }],
                strings: vec![
                    Text::from("something"),
                    Text::from("bobby"),
                    Text::from("bobby2"),
                    Text::from("name-here"),
                ],
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_serialization() {
        let mut builder = DocBlockBuilder::default();
        let doc = doc! {
            "field_demo" => vec![
                vec![
                    doc! {
                        "name" => "bobby"
                    },
                ],
            ]
        };
        let is_full = builder.add_document(doc);
        assert!(!is_full, "Builder should not be full");
        let mut serializer =
            DocSerializer::<512, _>::new(DocWriteSerializer::new(AlignedVec::new()));
        builder
            .serialize_with(&mut serializer)
            .expect("serialization should be ok");

        let mut builder = DocBlockBuilder::default();
        let doc = doc! {};
        let is_full = builder.add_document(doc);
        assert!(!is_full, "Builder should not be full");
        let mut serializer =
            DocSerializer::<512, _>::new(DocWriteSerializer::new(AlignedVec::new()));
        builder
            .serialize_with(&mut serializer)
            .expect("serialization should be ok");
    }

    #[test]
    fn test_multi_type_arrays() {
        let mut builder = DocBlockBuilder::default();
        let doc = doc! {
            "names" => vec![
                Value::from("Bobby"),
                Value::from(123u64),
                Value::from("John"),
            ]
        };

        let len = doc.len() as u32;
        let is_full = builder.add_document(doc);
        assert!(!is_full, "Builder should not be full");
        assert_eq!(
            builder.block,
            DocBlock {
                documents: vec![Document {
                    len,
                    layout: vec![
                        Step {
                            field_id: 0,
                            field_length: 3,
                            field_type: FieldType::Array,
                        },
                        Step {
                            field_id: u16::MAX,
                            field_length: 1,
                            field_type: FieldType::String,
                        },
                        Step {
                            field_id: u16::MAX,
                            field_length: 1,
                            field_type: FieldType::U64,
                        },
                        Step {
                            field_id: u16::MAX,
                            field_length: 1,
                            field_type: FieldType::String,
                        },
                    ],
                }],
                strings: vec![Text::from("Bobby"), Text::from("John"),],
                u64s: vec![123].into(),
                ..Default::default()
            }
        );

        let mut builder = DocBlockBuilder::default();
        let doc = doc! {
            "names" => vec![
                Value::from("Bobby"),
                Value::from(vec![
                    Value::from(1223u64),
                    Value::from(-1223i64),
                    Value::from("Timmy"),
                ]),
                Value::from("John"),
            ]
        };

        let len = doc.len() as u32;
        let is_full = builder.add_document(doc);
        assert!(!is_full, "Builder should not be full");
        assert_eq!(
            builder.block,
            DocBlock {
                documents: vec![Document {
                    len,
                    layout: vec![
                        Step {
                            field_id: 0,
                            field_length: 3,
                            field_type: FieldType::Array,
                        },
                        Step {
                            field_id: u16::MAX,
                            field_length: 1,
                            field_type: FieldType::String,
                        },
                        Step {
                            field_id: u16::MAX,
                            field_length: 3,
                            field_type: FieldType::Array,
                        },
                        Step {
                            field_id: u16::MAX,
                            field_length: 1,
                            field_type: FieldType::U64,
                        },
                        Step {
                            field_id: u16::MAX,
                            field_length: 1,
                            field_type: FieldType::I64,
                        },
                        Step {
                            field_id: u16::MAX,
                            field_length: 1,
                            field_type: FieldType::String,
                        },
                        Step {
                            field_id: u16::MAX,
                            field_length: 1,
                            field_type: FieldType::String,
                        },
                    ],
                }],
                strings: vec![
                    Text::from("Bobby"),
                    Text::from("Timmy"),
                    Text::from("John"),
                ],
                u64s: vec![1223,].into(),
                i64s: vec![-1223,].into(),
                ..Default::default()
            }
        );
    }

    #[test]
    /// This test was created after an edge case with some existing data
    /// showed that we sometimes added an additional step when we shouldn't.
    ///
    /// This was originally caused by the `counter` in `handle_array_entry_type_change`
    /// being incorrectly incremented for Array and Object types.
    fn test_nested_array_of_objects_bug() {
        let mut builder = DocBlockBuilder::default();
        let doc = doc! {
            "this-was-a-bug" => vec![
                Value::Object(Vec::new()),
                Value::Object(Vec::new()),
            ]
        };

        let len = doc.len() as u32;
        let is_full = builder.add_document(doc);
        assert!(!is_full, "Builder should not be full");
        assert_eq!(
            builder.block,
            DocBlock {
                documents: vec![Document {
                    len,
                    layout: vec![
                        Step {
                            field_id: 0,
                            field_length: 2,
                            field_type: FieldType::Array,
                        },
                        Step {
                            field_id: u16::MAX,
                            field_length: 0,
                            field_type: FieldType::Object,
                        },
                        Step {
                            field_id: u16::MAX,
                            field_length: 0,
                            field_type: FieldType::Object,
                        },
                    ],
                }],
                ..Default::default()
            }
        );
    }
}
