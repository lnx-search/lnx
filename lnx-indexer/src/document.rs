use std::borrow::Cow;
use std::convert::Infallible;
use std::net::Ipv6Addr;

use lnx_document::traverse::ViewWalker;
use lnx_document::DateTime;
use smallvec::SmallVec;
use tantivy::schema::Facet;

#[derive(Debug, PartialEq)]
/// A flattened field value.
pub struct FieldValue<'block> {
    /// The index of the field name.
    key: usize,
    value: Value<'block>,
}

impl<'block> FieldValue<'block> {
    /// The index for the field name.
    pub fn key(&self) -> usize {
        self.key
    }

    /// The value of the field.
    pub fn value(&self) -> &Value<'block> {
        &self.value
    }
}

#[derive(Debug, Default, PartialEq)]
/// A document for indexing values.
pub struct IndexingDoc<'block> {
    /// The flattened document entries.
    entries: SmallVec<[FieldValue<'block>; 32]>,

    /// The owned field names which field values point to.
    field_name: SmallVec<[Cow<'block, str>; 32]>,
}

pub struct IndexingDocWalker<'block, 'a> {
    /// A builder for flattened field names.
    key_builder: KeyBuilder<'block>,

    /// The document data for indexing.
    doc: &'a mut IndexingDoc<'block>,

    /// A LIFO queue of the flattened field keys.
    field_name_stack: SmallVec<[usize; 32]>,

    /// The number of key parts that make up the parent's key.
    parent_key_length: usize,

    /// True if this is the first value for the new key.
    just_started_new_key: bool,
}

impl<'block, 'a> IndexingDocWalker<'block, 'a> {
    /// Creates a new walker.
    pub fn new(doc: &'a mut IndexingDoc<'block>) -> Self {
        Self {
            key_builder: KeyBuilder::default(),
            doc,
            field_name_stack: SmallVec::default(),
            parent_key_length: 0,
            just_started_new_key: false,
        }
    }

    /// Creates and adds a new key to the field name stack
    /// if it's the first time the key is getting a value assigned to it.
    ///
    /// This allows us to avoid allocating new strings for objects which are just heavily nested.
    fn maybe_create_new_key(&mut self) {
        if self.just_started_new_key {
            self.just_started_new_key = false;

            let key = self.key_builder.to_key();
            self.doc.field_name.push(key);
            self.field_name_stack.push(self.doc.field_name.len() - 1);
        }
    }

    /// Gets the current key index for the field.
    fn get_current_key_index(&mut self) -> usize {
        self.maybe_create_new_key();
        self.field_name_stack.pop().unwrap_or_default()
    }

    /// Adds a new field entry to the doc.
    fn push_field(&mut self, key: usize, value: Value<'block>) {
        self.doc.entries.push(FieldValue { key, value })
    }
}

impl<'block, 'a> ViewWalker<'block> for IndexingDocWalker<'block, 'a> {
    type Err = Infallible;

    fn visit_null(&mut self, _is_last: bool) -> Result<(), Self::Err> {
        let key = self.get_current_key_index();
        self.push_field(key, Value::Null);
        Ok(())
    }

    fn visit_str(&mut self, _is_last: bool, val: &'block str) -> Result<(), Self::Err> {
        let key = self.get_current_key_index();
        self.push_field(key, Value::Str(val));
        Ok(())
    }

    fn visit_bytes(
        &mut self,
        _is_last: bool,
        val: &'block [u8],
    ) -> Result<(), Self::Err> {
        let key = self.get_current_key_index();
        self.push_field(key, Value::Bytes(val));
        Ok(())
    }

    fn visit_bool(&mut self, _is_last: bool, val: bool) -> Result<(), Self::Err> {
        let key = self.get_current_key_index();
        self.push_field(key, Value::Bool(val));
        Ok(())
    }

    fn visit_u64(&mut self, _is_last: bool, val: u64) -> Result<(), Self::Err> {
        let key = self.get_current_key_index();
        self.push_field(key, Value::U64(val));
        Ok(())
    }

    fn visit_i64(&mut self, _is_last: bool, val: i64) -> Result<(), Self::Err> {
        let key = self.get_current_key_index();
        self.push_field(key, Value::I64(val));
        Ok(())
    }

    fn visit_f64(&mut self, _is_last: bool, val: f64) -> Result<(), Self::Err> {
        let key = self.get_current_key_index();
        self.push_field(key, Value::F64(val));
        Ok(())
    }

    fn visit_ip(&mut self, _is_last: bool, val: Ipv6Addr) -> Result<(), Self::Err> {
        let key = self.get_current_key_index();
        self.push_field(key, Value::IpAddr(val));
        Ok(())
    }

    fn visit_date(&mut self, _is_last: bool, val: DateTime) -> Result<(), Self::Err> {
        let key = self.get_current_key_index();
        self.push_field(key, Value::Datetime(val));
        Ok(())
    }

    fn visit_facet(
        &mut self,
        _is_last: bool,
        val: &'block str,
    ) -> Result<(), Self::Err> {
        let key = self.get_current_key_index();
        let facet = Facet::from_encoded(val.to_string().into_bytes())
            .expect("Facet value should be valid.");
        self.push_field(key, Value::Facet(facet));
        Ok(())
    }

    fn visit_map_key(&mut self, key: &'block str) -> Result<(), Self::Err> {
        if self.key_builder.num_parts() > (self.parent_key_length - 1) {
            self.key_builder.truncate_parts(self.parent_key_length - 1);
        }

        self.key_builder.push_part(key);
        self.just_started_new_key = true;

        Ok(())
    }

    fn start_array(&mut self, _size_hint: usize) -> Result<(), Self::Err> {
        Ok(())
    }

    fn end_array(&mut self, _is_last: bool) -> Result<(), Self::Err> {
        Ok(())
    }

    fn start_map(&mut self, _size_hint: usize) -> Result<(), Self::Err> {
        self.parent_key_length += 1;
        Ok(())
    }

    fn end_map(&mut self, _is_last: bool) -> Result<(), Self::Err> {
        self.parent_key_length -= 1;
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
/// A document value for indexing.
pub enum Value<'block> {
    /// A null value.
    Null,
    /// A u64 value.
    U64(u64),
    /// A i64 value.
    I64(i64),
    /// A f64 value.
    F64(f64),
    /// A bool value.
    Bool(bool),
    /// A ip address value.
    IpAddr(Ipv6Addr),
    /// A datetime value.
    Datetime(DateTime),
    /// A string value.
    Str(&'block str),
    /// A bytes value.
    Bytes(&'block [u8]),
    /// A facet value.
    Facet(Facet),
}

#[derive(Default)]
/// A structure for incrementally building nested keys
/// as their flattened counter parts.
pub struct KeyBuilder<'block> {
    length: usize,
    parts: SmallVec<[&'block str; 6]>,
}

impl<'block> KeyBuilder<'block> {
    /// Pops the last-added key part.
    fn pop_part(&mut self) {
        if let Some(part) = self.parts.pop() {
            self.length -= part.len();
        }
    }

    /// Appends a new part to the *front* of the key.
    fn push_part(&mut self, s: &'block str) {
        self.length += s.len();
        self.parts.push(s);
    }

    /// Truncates the end of the key parts.
    fn truncate_parts(&mut self, len: usize) {
        self.parts.truncate(len)
    }

    /// Returns the length of the string if it was fully built.
    fn string_length(&self) -> usize {
        self.length + (self.parts.len() - 1)
    }

    /// Returns the number of parts in the temporary key.
    fn num_parts(&self) -> usize {
        self.parts.len()
    }

    /// Builds a new key from the internal set of parts.
    fn to_key(&self) -> Cow<'block, str> {
        if self.parts.len() == 1 {
            return Cow::Borrowed(self.parts[0]);
        }

        let num_parts = self.parts.len();
        let mut s = String::with_capacity(self.string_length());

        for (i, part) in self.parts.iter().rev().enumerate() {
            s.push_str(part);

            if i < (num_parts - 1) {
                s.push('.');
            }
        }

        Cow::Owned(s)
    }
}

#[cfg(test)]
mod tests {
    use std::mem;

    use lnx_document::{
        ChecksumDocWriter,
        DocBlockBuilder,
        DocBlockReader,
        DocSerializer,
        DocWriteSerializer,
    };
    use rkyv::AlignedVec;
    use serde_json::json;
    use smallvec::smallvec;

    use super::*;

    fn get_view_of(json_value: serde_json::Value) -> DocBlockReader {
        let json_text = json_value.to_string();
        let doc = serde_json::from_str(&json_text).unwrap();
        let mut builder = DocBlockBuilder::default();

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

        DocBlockReader::using_data(data).expect("Read block successfully")
    }

    fn check_doc(value: serde_json::Value, expected_doc: IndexingDoc) {
        let reader = get_view_of(value);

        let mut actual_doc = IndexingDoc::default();
        let walker = IndexingDocWalker::new(&mut actual_doc);
        let view = reader.doc(0);
        view.traverse(walker)
            .expect("Traversing document should not error");

        // The compiler gets confused with all the lifetimes, since
        // this is just for testing this is fine.
        let actual_doc = unsafe { mem::transmute(actual_doc) };
        assert_eq!(expected_doc, actual_doc);
    }

    #[test]
    fn test_basic_doc() {
        check_doc(
            json!({
                "hello": "world",
            }),
            IndexingDoc {
                entries: smallvec![FieldValue {
                    key: 0,
                    value: Value::Str("world")
                },],
                field_name: smallvec![Cow::Borrowed("hello"),],
            },
        );

        check_doc(
            json!({
                "pos": 123,
                "neg": -123,
                "float": 12.30
            }),
            IndexingDoc {
                entries: smallvec![
                    FieldValue {
                        key: 0,
                        value: Value::F64(12.30)
                    },
                    FieldValue {
                        key: 1,
                        value: Value::I64(-123)
                    },
                    FieldValue {
                        key: 2,
                        value: Value::U64(123)
                    },
                ],
                field_name: smallvec![
                    Cow::Borrowed("float"),
                    Cow::Borrowed("neg"),
                    Cow::Borrowed("pos"),
                ],
            },
        );

        check_doc(
            json!({
                "hello": "world",
                "name": "bobby",
                "age": 32
            }),
            IndexingDoc {
                entries: smallvec![
                    FieldValue {
                        key: 0,
                        value: Value::U64(32)
                    },
                    FieldValue {
                        key: 1,
                        value: Value::Str("world")
                    },
                    FieldValue {
                        key: 2,
                        value: Value::Str("bobby")
                    },
                ],
                field_name: smallvec![
                    Cow::Borrowed("age"),
                    Cow::Borrowed("hello"),
                    Cow::Borrowed("name"),
                ],
            },
        );
    }

    #[test]
    fn test_nested_flattening_doc() {
        check_doc(
            json!({
                "hello": "world",
                "name": "bobby",
                "age": 32
            }),
            IndexingDoc {
                entries: smallvec![
                    FieldValue {
                        key: 0,
                        value: Value::U64(32)
                    },
                    FieldValue {
                        key: 1,
                        value: Value::Str("world")
                    },
                    FieldValue {
                        key: 2,
                        value: Value::Str("bobby")
                    },
                ],
                field_name: smallvec![
                    Cow::Borrowed("age"),
                    Cow::Borrowed("hello"),
                    Cow::Borrowed("name"),
                ],
            },
        );
    }
}
