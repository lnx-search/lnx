use std::collections::HashMap;
use std::convert::Infallible;
use std::net::Ipv6Addr;

use lnx_document::traverse::ViewWalker;
use lnx_document::DateTime;
use lnx_schema::indexing::{FieldInfo, IndexingSchema};
use lnx_tools::hashers::NoOpRandomState;
use smallvec::SmallVec;
use tantivy::schema::{Facet, Field};

use crate::document::stack_str::SmallStr;

macro_rules! skip_if_excluded {
    ($is_excluded:expr) => {{
        if $is_excluded.is_none() {
            return Ok(());
        }
    }};
}

#[derive(Debug, PartialEq)]
/// A flattened field value.
pub struct FieldValue<'block> {
    /// The ID of the schema field.
    field_id: Field,
    /// The index of the field name (if it has one.)
    key: Option<usize>,
    value: Value<'block>,
}

impl<'block> FieldValue<'block> {
    /// The field ID for the value.
    pub fn field_id(&self) -> Field {
        self.field_id
    }

    /// The index for the field name if it has a extra name.
    pub fn key(&self) -> Option<usize> {
        self.key
    }

    /// The value of the field.
    pub fn value(&self) -> &Value<'block> {
        &self.value
    }
}

#[derive(Debug, Default, PartialEq)]
/// A document for indexing values.
pub struct IndexingDocData<'block> {
    /// The flattened document entries.
    pub(crate) entries: SmallVec<[FieldValue<'block>; 32]>,

    /// The owned field names which field values point to.
    pub(crate) field_name: SmallVec<[SmallStr; 32]>,
}

pub struct IndexingDocWalker<'block, 'a> {
    /// Schema information.
    schema: &'a IndexingSchema,

    /// A builder for flattened field names at the entire qualified path.
    key_builder: KeyBuilder,

    /// The document data for indexing.
    doc: &'a mut IndexingDocData<'block>,

    /// A fast lookup table re-using previously allocated strings.
    field_name_lookup: HashMap<u64, usize, NoOpRandomState>,

    /// The number of key parts that make up the parent's key.
    parent_key_length: usize,

    /// True if this is the first value for the new key.
    just_started_new_key: bool,

    /// True if the current field is a collection type.
    is_collection: bool,

    /// The tantivy schema field ID if the field exists in the schema.
    ///
    /// If the field_id is `None`, the field is excluded.
    current_field_id: Option<Field>,

    /// A stack of parent dynamic fields.
    ///
    /// This lets the walker decide what field it should fall back to if
    /// a field isn't explicitly declared, otherwise it will fall back
    /// to the schema's catch all, but this might not always be wanted.
    parent_dynamic_field_stack: SmallVec<[(Field, usize); 4]>,

    /// The offset (in bytes) that the field ID covers as part of the prefix.
    ///
    /// I.e. if the schema field is `foo.bar` and our key is `foo.bar.baz.bob`
    /// our actual key for the object is `baz.bob`.
    current_field_name_offset: usize,
}

impl<'block, 'a> IndexingDocWalker<'block, 'a> {
    /// Creates a new walker.
    pub(crate) fn new(
        schema: &'a IndexingSchema,
        doc: &'a mut IndexingDocData<'block>,
    ) -> Self {
        Self {
            schema,
            key_builder: KeyBuilder::default(),
            doc,
            field_name_lookup: HashMap::with_capacity_and_hasher(32, NoOpRandomState),
            parent_key_length: 0,
            just_started_new_key: false,
            is_collection: false,
            current_field_id: None,
            current_field_name_offset: 0,
            parent_dynamic_field_stack: SmallVec::default(),
        }
    }

    fn get_field_key(&self) -> Option<&str> {
        let full_field_name = self.key_builder.as_str();

        if self.current_field_name_offset == 0 {
            return Some(full_field_name.as_str());
        }

        if self.current_field_name_offset == full_field_name.len() {
            return None;
        }

        // +1 here as we're cutting off the `.` which will be at the start of the key.
        let sliced_field_name =
            full_field_name.slice_at(self.current_field_name_offset + 1);
        if sliced_field_name.is_empty() {
            return None;
        }

        // This should never be invalid providing our offsets are not invalid
        // if the offset is invalid the way we flatten data and produce keys is incorrect.
        let key = if cfg!(debug_assertions) {
            std::str::from_utf8(sliced_field_name).unwrap()
        } else {
            unsafe { std::str::from_utf8_unchecked(sliced_field_name) }
        };

        Some(key)
    }

    /// Gets the current key index for the field.
    fn get_current_key_index(&mut self) -> Option<usize> {
        let field_key = self.get_field_key()?;

        let hash = self.schema.hash_str(field_key);
        if let Some(id) = self.field_name_lookup.get(&hash) {
            Some(*id)
        } else {
            self.doc.field_name.push(SmallStr::from(field_key));
            let index = self.doc.field_name.len() - 1;
            self.field_name_lookup.insert(hash, index);
            Some(index)
        }
    }

    /// Adds a new field entry to the doc.
    fn push_field(&mut self, key: Option<usize>, value: Value<'block>) {
        if let Some(field_id) = self.current_field_id {
            self.doc.entries.push(FieldValue {
                field_id,
                key,
                value,
            });
        }
    }
}

impl<'block, 'a> ViewWalker<'block> for IndexingDocWalker<'block, 'a> {
    type Err = Infallible;

    #[inline]
    fn visit_null(&mut self, _is_last: bool) -> Result<(), Self::Err> {
        skip_if_excluded!(self.current_field_id);

        let key = self.get_current_key_index();
        self.push_field(key, Value::Null);
        Ok(())
    }

    #[inline]
    fn visit_str(&mut self, _is_last: bool, val: &'block str) -> Result<(), Self::Err> {
        skip_if_excluded!(self.current_field_id);

        let key = self.get_current_key_index();
        self.push_field(key, Value::Str(val));
        Ok(())
    }

    #[inline]
    fn visit_bytes(
        &mut self,
        _is_last: bool,
        val: &'block [u8],
    ) -> Result<(), Self::Err> {
        skip_if_excluded!(self.current_field_id);

        let key = self.get_current_key_index();
        self.push_field(key, Value::Bytes(val));
        Ok(())
    }

    #[inline]
    fn visit_bool(&mut self, _is_last: bool, val: bool) -> Result<(), Self::Err> {
        skip_if_excluded!(self.current_field_id);

        let key = self.get_current_key_index();
        self.push_field(key, Value::Bool(val));
        Ok(())
    }

    #[inline]
    fn visit_u64(&mut self, _is_last: bool, val: u64) -> Result<(), Self::Err> {
        skip_if_excluded!(self.current_field_id);

        let key = self.get_current_key_index();
        self.push_field(key, Value::U64(val));
        Ok(())
    }

    #[inline]
    fn visit_i64(&mut self, _is_last: bool, val: i64) -> Result<(), Self::Err> {
        skip_if_excluded!(self.current_field_id);

        let key = self.get_current_key_index();
        self.push_field(key, Value::I64(val));
        Ok(())
    }

    #[inline]
    fn visit_f64(&mut self, _is_last: bool, val: f64) -> Result<(), Self::Err> {
        skip_if_excluded!(self.current_field_id);

        let key = self.get_current_key_index();
        self.push_field(key, Value::F64(val));
        Ok(())
    }

    #[inline]
    fn visit_ip(&mut self, _is_last: bool, val: Ipv6Addr) -> Result<(), Self::Err> {
        skip_if_excluded!(self.current_field_id);

        let key = self.get_current_key_index();
        self.push_field(key, Value::IpAddr(val));
        Ok(())
    }

    #[inline]
    fn visit_date(&mut self, _is_last: bool, val: DateTime) -> Result<(), Self::Err> {
        skip_if_excluded!(self.current_field_id);

        let key = self.get_current_key_index();
        self.push_field(key, Value::Datetime(val));
        Ok(())
    }

    #[inline]
    fn visit_facet(
        &mut self,
        _is_last: bool,
        val: &'block str,
    ) -> Result<(), Self::Err> {
        skip_if_excluded!(self.current_field_id);

        let key = self.get_current_key_index();
        let facet = Facet::from_text(val).expect("Facet value should be valid.");
        self.push_field(key, Value::Facet(facet));
        Ok(())
    }

    fn visit_map_key(&mut self, key: &'block str) -> Result<(), Self::Err> {
        if self.is_collection && !self.key_builder.is_empty() {
            skip_if_excluded!(self.current_field_id);
        }

        if self.key_builder.num_parts() > (self.parent_key_length - 1) {
            self.key_builder.pop_part()
        }
        self.key_builder.push_part(key);

        self.just_started_new_key = true;

        let key = self.key_builder.as_str();
        let field_id = self.schema.hash_str(key);
        let info = self.schema.get_field_info(field_id);

        match info {
            FieldInfo::Exclude => {
                self.current_field_id = None;
            },
            FieldInfo::Field(field) => {
                self.current_field_id = Some(field);
                self.current_field_name_offset = key.len();
            },
            FieldInfo::Dynamic(field) => {
                self.current_field_id = Some(field);
                self.current_field_name_offset = key.len();
                self.parent_dynamic_field_stack.push((field, key.len()));
            },
            FieldInfo::CatchAll(field) => {
                // Our parent takes priority when handling dynamic values.
                if let Some((parent, len)) = self.parent_dynamic_field_stack.last() {
                    self.current_field_id = Some(*parent);
                    self.current_field_name_offset = *len;
                } else {
                    self.current_field_id = Some(field);
                    self.current_field_name_offset = 0;
                }
            },
        }

        Ok(())
    }

    #[inline]
    fn start_array(&mut self, _size_hint: usize) -> Result<(), Self::Err> {
        self.is_collection = true;
        Ok(())
    }

    #[inline]
    fn end_array(&mut self, _is_last: bool) -> Result<(), Self::Err> {
        self.is_collection = false;
        Ok(())
    }

    #[inline]
    fn start_map(&mut self, _size_hint: usize) -> Result<(), Self::Err> {
        self.is_collection = true;
        self.parent_key_length += 1;
        Ok(())
    }

    #[inline]
    fn end_map(&mut self, _is_last: bool) -> Result<(), Self::Err> {
        self.is_collection = false;
        self.parent_key_length -= 1;
        self.key_builder.truncate_parts(self.parent_key_length);

        let key = self.key_builder.as_str();
        let field_id = self.schema.hash_str(key);
        let info = self.schema.get_field_info(field_id);

        if matches!(info, FieldInfo::Dynamic(_)) {
            self.parent_dynamic_field_stack.pop();
        }

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
pub struct KeyBuilder {
    string: SmallStr,
    parts: SmallVec<[usize; 12]>,
}

impl KeyBuilder {
    /// Returns if the builder is empty or not.
    fn is_empty(&self) -> bool {
        self.parts.is_empty()
    }

    /// Pops the last-added key part.
    fn pop_part(&mut self) {
        if let Some(part) = self.parts.pop() {
            self.string.truncate(part)
        }
    }

    /// Appends a new part to the *front* of the key.
    fn push_part(&mut self, s: &str) {
        self.parts.push(self.string.len());

        if self.parts.len() > 1 {
            self.string.push_byte(b'.');
        }
        self.string.push_str(s);
    }

    /// Truncates the end of the key parts.
    fn truncate_parts(&mut self, len: usize) {
        let cut_to = self
            .parts
            .get(len)
            .copied()
            .unwrap_or_else(|| self.string_length());

        self.parts.truncate(len);
        self.string.truncate(cut_to);
    }

    /// Returns the length of the string if it was fully built.
    fn string_length(&self) -> usize {
        self.string.len()
    }

    /// Returns the number of parts in the temporary key.
    fn num_parts(&self) -> usize {
        self.parts.len()
    }

    /// Returns the string representation of the key.
    fn as_str(&self) -> &SmallStr {
        &self.string
    }
}

#[cfg(test)]
mod tests {
    use std::mem;
    use std::sync::Arc;

    use lnx_document::{
        ChecksumDocWriter,
        DocBlockBuilder,
        DocBlockReader,
        DocSerializer,
        DocWriteSerializer,
    };
    use lnx_schema::indexing::FieldType;
    use rkyv::AlignedVec;
    use serde_json::json;
    use smallvec::smallvec;

    use super::*;

    fn get_view_of(json_value: serde_json::Value) -> DocBlockReader {
        let json_text = json_value.to_string();
        let doc = serde_json::from_str(&json_text).unwrap();
        let mut builder = DocBlockBuilder::with_index_id(0);

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

        DocBlockReader::using_data(Arc::new(data)).expect("Read block successfully")
    }

    fn check_doc(value: serde_json::Value, expected_doc: IndexingDocData) {
        let mut schema = IndexingSchema::default();
        schema.set_catch_unknown_fields(Field::from_field_id(0));
        check_doc_with_schema(value, expected_doc, &schema)
    }

    fn check_doc_with_schema(
        value: serde_json::Value,
        expected_doc: IndexingDocData,
        schema: &IndexingSchema,
    ) {
        let reader = get_view_of(value);

        let mut actual_doc = IndexingDocData::default();
        let walker = IndexingDocWalker::new(schema, &mut actual_doc);
        let view = reader.doc(0);
        view.traverse(walker)
            .expect("Traversing document should not error");

        // The compiler gets confused with all the lifetimes, since
        // this is just for testing this is fine.
        let actual_doc = unsafe { mem::transmute(actual_doc) };
        assert_eq!(expected_doc, actual_doc);
    }

    #[test]
    fn test_key_builder() {
        let mut builder = KeyBuilder::default();

        builder.push_part("hello");
        assert_eq!(builder.as_str(), "hello");

        builder.push_part("world");
        assert_eq!(builder.as_str(), "hello.world");

        builder.push_part(".bar");
        assert_eq!(builder.as_str(), "hello.world..bar");

        builder.pop_part();
        assert_eq!(builder.as_str(), "hello.world");

        builder.pop_part();
        assert_eq!(builder.as_str(), "hello");

        builder.pop_part();
        assert_eq!(builder.as_str(), "");

        builder.pop_part();
        assert_eq!(builder.as_str(), "");
    }

    #[test]
    fn test_basic_doc() {
        check_doc(
            json!({
                "hello": "world",
            }),
            IndexingDocData {
                entries: smallvec![FieldValue {
                    field_id: Field::from_field_id(0),
                    key: Some(0),
                    value: Value::Str("world")
                },],
                field_name: smallvec!["hello".into()],
            },
        );

        check_doc(
            json!({
                "pos": 123,
                "neg": -123,
                "float": 12.30
            }),
            IndexingDocData {
                entries: smallvec![
                    FieldValue {
                        field_id: Field::from_field_id(0),
                        key: Some(0),
                        value: Value::F64(12.30)
                    },
                    FieldValue {
                        field_id: Field::from_field_id(0),
                        key: Some(1),
                        value: Value::I64(-123)
                    },
                    FieldValue {
                        field_id: Field::from_field_id(0),
                        key: Some(2),
                        value: Value::U64(123)
                    },
                ],
                field_name: smallvec!["float".into(), "neg".into(), "pos".into(),],
            },
        );

        check_doc(
            json!({
                "hello": "world",
                "name": "bobby",
                "age": 32
            }),
            IndexingDocData {
                entries: smallvec![
                    FieldValue {
                        field_id: Field::from_field_id(0),
                        key: Some(0),
                        value: Value::U64(32)
                    },
                    FieldValue {
                        field_id: Field::from_field_id(0),
                        key: Some(1),
                        value: Value::Str("world")
                    },
                    FieldValue {
                        field_id: Field::from_field_id(0),
                        key: Some(2),
                        value: Value::Str("bobby")
                    },
                ],
                field_name: smallvec!["age".into(), "hello".into(), "name".into(),],
            },
        );
    }

    #[test]
    fn test_schema_field_selection() {
        let value = json!({
            "payload": {
                "before": "2d06657267b32e0c8e193c617039da200f710195",
                "commits": [
                    {"hello": "world"}
                ],
                "nested": {
                    "demo": [1, 2, 3],
                    "something": {
                        "else": "here"
                    }
                }
            }
        });

        let mut schema = IndexingSchema::default();
        schema.set_catch_unknown_fields(Field::from_field_id(0));
        schema.add_field(
            "payload.nested",
            FieldType::DynamicObject {
                field_id: Field::from_field_id(1),
            },
        );
        schema.add_field(
            "payload.nested.demo",
            FieldType::U64 {
                field_id: Field::from_field_id(2),
            },
        );

        check_doc_with_schema(
            value.clone(),
            IndexingDocData {
                entries: smallvec![
                    FieldValue {
                        field_id: Field::from_field_id(0),
                        key: Some(0),
                        value: Value::Str("2d06657267b32e0c8e193c617039da200f710195"),
                    },
                    FieldValue {
                        field_id: Field::from_field_id(0),
                        key: Some(1),
                        value: Value::Str("world"),
                    },
                    FieldValue {
                        field_id: Field::from_field_id(2),
                        key: None,
                        value: Value::U64(1),
                    },
                    FieldValue {
                        field_id: Field::from_field_id(2),
                        key: None,
                        value: Value::U64(2),
                    },
                    FieldValue {
                        field_id: Field::from_field_id(2),
                        key: None,
                        value: Value::U64(3),
                    },
                    FieldValue {
                        field_id: Field::from_field_id(1),
                        key: Some(2),
                        value: Value::Str("here"),
                    },
                ],
                field_name: smallvec![
                    "payload.before".into(),
                    "payload.commits.hello".into(),
                    "something.else".into(),
                ],
            },
            &schema,
        );

        let mut schema = IndexingSchema::default();
        schema.set_catch_unknown_fields(Field::from_field_id(0));
        schema.add_field(
            "payload.nested",
            FieldType::DynamicObject {
                field_id: Field::from_field_id(1),
            },
        );

        check_doc_with_schema(
            value.clone(),
            IndexingDocData {
                entries: smallvec![
                    FieldValue {
                        field_id: Field::from_field_id(0),
                        key: Some(0),
                        value: Value::Str("2d06657267b32e0c8e193c617039da200f710195"),
                    },
                    FieldValue {
                        field_id: Field::from_field_id(0),
                        key: Some(1),
                        value: Value::Str("world"),
                    },
                    FieldValue {
                        field_id: Field::from_field_id(1),
                        key: Some(2),
                        value: Value::U64(1),
                    },
                    FieldValue {
                        field_id: Field::from_field_id(1),
                        key: Some(2),
                        value: Value::U64(2),
                    },
                    FieldValue {
                        field_id: Field::from_field_id(1),
                        key: Some(2),
                        value: Value::U64(3),
                    },
                    FieldValue {
                        field_id: Field::from_field_id(1),
                        key: Some(3),
                        value: Value::Str("here"),
                    },
                ],
                field_name: smallvec![
                    "payload.before".into(),
                    "payload.commits.hello".into(),
                    "demo".into(),
                    "something.else".into(),
                ],
            },
            &schema,
        );

        let mut schema = IndexingSchema::default();
        schema.set_catch_unknown_fields(Field::from_field_id(0));
        schema.exclude_field("payload.nested.demo");
        schema.add_field(
            "payload.nested",
            FieldType::DynamicObject {
                field_id: Field::from_field_id(1),
            },
        );

        check_doc_with_schema(
            value,
            IndexingDocData {
                entries: smallvec![
                    FieldValue {
                        field_id: Field::from_field_id(0),
                        key: Some(0),
                        value: Value::Str("2d06657267b32e0c8e193c617039da200f710195"),
                    },
                    FieldValue {
                        field_id: Field::from_field_id(0),
                        key: Some(1),
                        value: Value::Str("world"),
                    },
                    FieldValue {
                        field_id: Field::from_field_id(1),
                        key: Some(2),
                        value: Value::Str("here"),
                    },
                ],
                field_name: smallvec![
                    "payload.before".into(),
                    "payload.commits.hello".into(),
                    "something.else".into(),
                ],
            },
            &schema,
        );
    }

    #[test]
    fn test_excluded_fields() {
        let value = json!({
            "payload": {
                "before": "2d06657267b32e0c8e193c617039da200f710195",
                "commits": [
                    {"hello": "world"}
                ],
                "nested": {
                    "demo": [1, 2, 3],
                    "something": {
                        "else": "here"
                    }
                }
            }
        });

        let mut schema = IndexingSchema::default();
        schema.set_catch_unknown_fields(Field::from_field_id(0));
        schema.exclude_field("payload.nested.something");

        check_doc_with_schema(
            value,
            IndexingDocData {
                entries: smallvec![
                    FieldValue {
                        field_id: Field::from_field_id(0),
                        key: Some(0),
                        value: Value::Str("2d06657267b32e0c8e193c617039da200f710195"),
                    },
                    FieldValue {
                        field_id: Field::from_field_id(0),
                        key: Some(1),
                        value: Value::Str("world"),
                    },
                    FieldValue {
                        field_id: Field::from_field_id(0),
                        key: Some(2),
                        value: Value::U64(1),
                    },
                    FieldValue {
                        field_id: Field::from_field_id(0),
                        key: Some(2),
                        value: Value::U64(2),
                    },
                    FieldValue {
                        field_id: Field::from_field_id(0),
                        key: Some(2),
                        value: Value::U64(3),
                    },
                ],
                field_name: smallvec![
                    "payload.before".into(),
                    "payload.commits.hello".into(),
                    "payload.nested.demo".into(),
                ],
            },
            &schema,
        );
    }

    #[test]
    fn test_nested_flattening_doc() {
        let complex = json!({
            "payload": {
                "before": "2d06657267b32e0c8e193c617039da200f710195",
                "distinct_size": 4,
                "head": "fa6048ec9b9eeafd12cee5f81324f355e1f2a198",
                "push_id": 536752122,
                "ref": "refs/heads/master",
                "size": 4,
                "commits": []
            }
        });

        check_doc(
            complex,
            IndexingDocData {
                entries: smallvec![
                    FieldValue {
                        field_id: Field::from_field_id(0),
                        key: Some(0),
                        value: Value::Str("2d06657267b32e0c8e193c617039da200f710195"),
                    },
                    FieldValue {
                        field_id: Field::from_field_id(0),
                        key: Some(1),
                        value: Value::U64(4),
                    },
                    FieldValue {
                        field_id: Field::from_field_id(0),
                        key: Some(2),
                        value: Value::Str("fa6048ec9b9eeafd12cee5f81324f355e1f2a198"),
                    },
                    FieldValue {
                        field_id: Field::from_field_id(0),
                        key: Some(3),
                        value: Value::U64(536752122),
                    },
                    FieldValue {
                        field_id: Field::from_field_id(0),
                        key: Some(4),
                        value: Value::Str("refs/heads/master"),
                    },
                    FieldValue {
                        field_id: Field::from_field_id(0),
                        key: Some(5),
                        value: Value::U64(4),
                    },
                ],
                field_name: smallvec![
                    "payload.before".into(),
                    "payload.distinct_size".into(),
                    "payload.head".into(),
                    "payload.push_id".into(),
                    "payload.ref".into(),
                    "payload.size".into(),
                ],
            },
        );

        let complex = json!({
            "id": "2489395767",
            "type":"PushEvent",
            "actor": {
                "id":1310570,
                "login":"soumith",
                "gravatar_id": "some-id",
                "url": "https://api.github.com/users/soumith",
                "avatar_url": "https://avatars.githubusercontent.com/u/1310570?",
            },
            "repo": {
                "id": 28067809,
                "name": "soumith/fbcunn",
                "url": "https://api.github.com/repos/soumith/fbcunn"
            },
            "payload": {}
        });

        check_doc(
            complex,
            IndexingDocData {
                entries: smallvec![
                    FieldValue {
                        field_id: Field::from_field_id(0),
                        key: Some(0),
                        value: Value::Str(
                            "https://avatars.githubusercontent.com/u/1310570?"
                        )
                    },
                    FieldValue {
                        field_id: Field::from_field_id(0),
                        key: Some(1),
                        value: Value::Str("some-id")
                    },
                    FieldValue {
                        field_id: Field::from_field_id(0),
                        key: Some(2),
                        value: Value::U64(1310570)
                    },
                    FieldValue {
                        field_id: Field::from_field_id(0),
                        key: Some(3),
                        value: Value::Str("soumith")
                    },
                    FieldValue {
                        field_id: Field::from_field_id(0),
                        key: Some(4),
                        value: Value::Str("https://api.github.com/users/soumith")
                    },
                    FieldValue {
                        field_id: Field::from_field_id(0),
                        key: Some(5),
                        value: Value::Str("2489395767")
                    },
                    FieldValue {
                        field_id: Field::from_field_id(0),
                        key: Some(6),
                        value: Value::U64(28067809)
                    },
                    FieldValue {
                        field_id: Field::from_field_id(0),
                        key: Some(7),
                        value: Value::Str("soumith/fbcunn")
                    },
                    FieldValue {
                        field_id: Field::from_field_id(0),
                        key: Some(8),
                        value: Value::Str("https://api.github.com/repos/soumith/fbcunn")
                    },
                    FieldValue {
                        field_id: Field::from_field_id(0),
                        key: Some(9),
                        value: Value::Str("PushEvent")
                    },
                ],
                field_name: smallvec![
                    "actor.avatar_url".into(),
                    "actor.gravatar_id".into(),
                    "actor.id".into(),
                    "actor.login".into(),
                    "actor.url".into(),
                    "id".into(),
                    "repo.id".into(),
                    "repo.name".into(),
                    "repo.url".into(),
                    "type".into(),
                ],
            },
        );

        let complex = json!({
            "id": "2489395767",
            "type":"PushEvent",
            "actor": {
                "id":1310570,
                "login":"soumith",
                "gravatar_id": "",
                "url": "https://api.github.com/users/soumith",
                "avatar_url": "https://avatars.githubusercontent.com/u/1310570?"
            },
            "repo": {
                "id": 28067809,
                "name": "soumith/fbcunn",
                "url": "https://api.github.com/repos/soumith/fbcunn"
            },
            "payload": {
                "push_id": 536752122,
                "size": 4,
                "distinct_size": 4,
                "ref": "refs/heads/master",
                "head": "fa6048ec9b9eeafd12cee5f81324f355e1f2a198",
                "before": "2d06657267b32e0c8e193c617039da200f710195",
                "commits": [
                    {
                        "sha": "dbd68d30ee1f7b60d404553fc1c6226ebb374c8e",
                        "author": {
                            "email": "88de463b5797707cf3425f85a415c3d869db732b@gmail.com",
                            "name": "Soumith Chintala"
                        },
                        "message": "back to old structure, except lua files moved out",
                        "distinct": true,
                        "url": "https://api.github.com/repos/soumith/fbcunn/commits/dbd68d30ee1f7b60d404553fc1c6226ebb374c8e"
                    },
                    {
                        "sha":"5567f9f5a83d7fe3320b18e5b89405e8a5ca77e6",
                        "author": {
                            "email":"88de463b5797707cf3425f85a415c3d869db732b@gmail.com",
                            "name":"Soumith Chintala"
                        },
                        "message": "...",
                        "distinct": true,
                        "url": "https://api.github.com/repos/soumith/fbcunn/commits/5567f9f5a83d7fe3320b18e5b89405e8a5ca77e6"
                    },
                    {
                        "sha":"58a83b277328eca811d3a37cf171b2fc4fcd87af",
                        "author": {
                            "email":"88de463b5797707cf3425f85a415c3d869db732b@gmail.com",
                            "name":"Soumith Chintala",
                        },
                        "message": "...",
                        "distinct": true,
                        "url": "https://api.github.com/repos/soumith/fbcunn/commits/58a83b277328eca811d3a37cf171b2fc4fcd87af"
                    },
                    {
                        "sha":"fa6048ec9b9eeafd12cee5f81324f355e1f2a198",
                        "author": {
                            "email":"88de463b5797707cf3425f85a415c3d869db732b@gmail.com",
                            "name":"Soumith Chintala"
                        },
                        "message": "...",
                        "distinct": true,
                        "url":"https://api.github.com/repos/soumith/fbcunn/commits/fa6048ec9b9eeafd12cee5f81324f355e1f2a198"
                    }
                ]
            },
            "public":true,
            "created_at":"2015-01-01T01:00:00Z"
        });

        check_doc(
            complex,
            IndexingDocData {
                entries: smallvec![
                    FieldValue { field_id: Field::from_field_id(0), key: Some(0), value: Value::Str("https://avatars.githubusercontent.com/u/1310570?") },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(1), value: Value::Str("") },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(2), value: Value::U64(1310570) },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(3), value: Value::Str("soumith") },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(4), value: Value::Str("https://api.github.com/users/soumith") },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(5), value: Value::Str("2015-01-01T01:00:00Z") },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(6), value: Value::Str("2489395767") },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(7), value: Value::Str("2d06657267b32e0c8e193c617039da200f710195") },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(8), value: Value::Str("88de463b5797707cf3425f85a415c3d869db732b@gmail.com") },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(9), value: Value::Str("Soumith Chintala") },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(10), value: Value::Bool(true) },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(11), value: Value::Str("back to old structure, except lua files moved out") },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(12), value: Value::Str("dbd68d30ee1f7b60d404553fc1c6226ebb374c8e") },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(13), value: Value::Str("https://api.github.com/repos/soumith/fbcunn/commits/dbd68d30ee1f7b60d404553fc1c6226ebb374c8e") },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(8), value: Value::Str("88de463b5797707cf3425f85a415c3d869db732b@gmail.com") },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(9), value: Value::Str("Soumith Chintala") },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(10), value: Value::Bool(true) },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(11), value: Value::Str("...") },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(12), value: Value::Str("5567f9f5a83d7fe3320b18e5b89405e8a5ca77e6") },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(13), value: Value::Str("https://api.github.com/repos/soumith/fbcunn/commits/5567f9f5a83d7fe3320b18e5b89405e8a5ca77e6") },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(8), value: Value::Str("88de463b5797707cf3425f85a415c3d869db732b@gmail.com") },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(9), value: Value::Str("Soumith Chintala") },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(10), value: Value::Bool(true) },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(11), value: Value::Str("...") },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(12), value: Value::Str("58a83b277328eca811d3a37cf171b2fc4fcd87af") },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(13), value: Value::Str("https://api.github.com/repos/soumith/fbcunn/commits/58a83b277328eca811d3a37cf171b2fc4fcd87af") },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(8), value: Value::Str("88de463b5797707cf3425f85a415c3d869db732b@gmail.com") },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(9), value: Value::Str("Soumith Chintala") },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(10), value: Value::Bool(true) },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(11), value: Value::Str("...") },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(12), value: Value::Str("fa6048ec9b9eeafd12cee5f81324f355e1f2a198") },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(13), value: Value::Str("https://api.github.com/repos/soumith/fbcunn/commits/fa6048ec9b9eeafd12cee5f81324f355e1f2a198") },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(14), value: Value::U64(4) },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(15), value: Value::Str("fa6048ec9b9eeafd12cee5f81324f355e1f2a198") },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(16), value: Value::U64(536752122) },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(17), value: Value::Str("refs/heads/master") },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(18), value: Value::U64(4) },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(19), value: Value::Bool(true) },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(20), value: Value::U64(28067809) },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(21), value: Value::Str("soumith/fbcunn") },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(22), value: Value::Str("https://api.github.com/repos/soumith/fbcunn") },
                    FieldValue { field_id: Field::from_field_id(0), key: Some(23), value: Value::Str("PushEvent") }
                ],
                field_name: smallvec![
                    "actor.avatar_url".into(),
                    "actor.gravatar_id".into(),
                    "actor.id".into(),
                    "actor.login".into(),
                    "actor.url".into(),
                    "created_at".into(),
                    "id".into(),
                    "payload.before".into(),
                    "payload.commits.author.email".into(),
                    "payload.commits.author.name".into(),
                    "payload.commits.distinct".into(),
                    "payload.commits.message".into(),
                    "payload.commits.sha".into(),
                    "payload.commits.url".into(),
                    "payload.distinct_size".into(),
                    "payload.head".into(),
                    "payload.push_id".into(),
                    "payload.ref".into(),
                    "payload.size".into(),
                    "public".into(),
                    "repo.id".into(),
                    "repo.name".into(),
                    "repo.url".into(),
                    "type".into(),
                ],
            },
        );
    }
}
