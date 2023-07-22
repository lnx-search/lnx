use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::Infallible;
use std::hash::{BuildHasher, Hasher};
use std::net::Ipv6Addr;

use lnx_document::traverse::ViewWalker;
use lnx_document::DateTime;
use lnx_tools::hashers::NoOpRandomState;
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

    /// A fast lookup table re-using previously allocated strings.
    field_name_lookup: HashMap<u64, usize, NoOpRandomState>,

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
            field_name_lookup: HashMap::with_capacity_and_hasher(32, NoOpRandomState),
            parent_key_length: 0,
            just_started_new_key: false,
        }
    }

    /// Gets the current key index for the field.
    fn get_current_key_index(&mut self) -> usize {
        let hash = self.key_builder.to_hash_key();
        if let Some(id) = self.field_name_lookup.get(&hash) {
            *id
        } else {
            let key = self.key_builder.to_key();
            self.doc.field_name.push(key);
            let index = self.doc.field_name.len() - 1;
            self.field_name_lookup.insert(hash, index);
            index
        }
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
        let facet = Facet::from_text(val).expect("Facet value should be valid.");
        self.push_field(key, Value::Facet(facet));
        Ok(())
    }

    fn visit_map_key(&mut self, key: &'block str) -> Result<(), Self::Err> {
        if self.key_builder.num_parts() > (self.parent_key_length - 1) {
            self.key_builder.pop_part()
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
        self.key_builder.truncate_parts(self.parent_key_length);
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
    random_state: ahash::RandomState,
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

    /// Builds a new key as a hash.
    /// This avoid the string allocation.
    fn to_hash_key(&self) -> u64 {
        let mut hasher = self.random_state.build_hasher();

        for key in self.parts.iter() {
            hasher.write(key.as_bytes());
        }

        hasher.finish()
    }

    /// Builds a new key from the internal set of parts.
    fn to_key(&self) -> Cow<'block, str> {
        if self.parts.len() == 1 {
            return Cow::Borrowed(self.parts[0]);
        }

        let num_parts = self.parts.len();
        let mut s = String::with_capacity(self.string_length());

        for (i, part) in self.parts.iter().enumerate() {
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
            IndexingDoc {
                entries: smallvec![
                    FieldValue {
                        key: 0,
                        value: Value::Str("2d06657267b32e0c8e193c617039da200f710195"),
                    },
                    FieldValue {
                        key: 1,
                        value: Value::U64(4),
                    },
                    FieldValue {
                        key: 2,
                        value: Value::Str("fa6048ec9b9eeafd12cee5f81324f355e1f2a198"),
                    },
                    FieldValue {
                        key: 3,
                        value: Value::U64(536752122),
                    },
                    FieldValue {
                        key: 4,
                        value: Value::Str("refs/heads/master"),
                    },
                    FieldValue {
                        key: 5,
                        value: Value::U64(4),
                    },
                ],
                field_name: smallvec![
                    Cow::Borrowed("payload.before"),
                    Cow::Borrowed("payload.distinct_size"),
                    Cow::Borrowed("payload.head"),
                    Cow::Borrowed("payload.push_id"),
                    Cow::Borrowed("payload.ref"),
                    Cow::Borrowed("payload.size"),
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
            IndexingDoc {
                entries: smallvec![
                    FieldValue {
                        key: 0,
                        value: Value::Str(
                            "https://avatars.githubusercontent.com/u/1310570?"
                        )
                    },
                    FieldValue {
                        key: 1,
                        value: Value::Str("some-id")
                    },
                    FieldValue {
                        key: 2,
                        value: Value::U64(1310570)
                    },
                    FieldValue {
                        key: 3,
                        value: Value::Str("soumith")
                    },
                    FieldValue {
                        key: 4,
                        value: Value::Str("https://api.github.com/users/soumith")
                    },
                    FieldValue {
                        key: 5,
                        value: Value::Str("2489395767")
                    },
                    FieldValue {
                        key: 6,
                        value: Value::U64(28067809)
                    },
                    FieldValue {
                        key: 7,
                        value: Value::Str("soumith/fbcunn")
                    },
                    FieldValue {
                        key: 8,
                        value: Value::Str("https://api.github.com/repos/soumith/fbcunn")
                    },
                    FieldValue {
                        key: 9,
                        value: Value::Str("PushEvent")
                    },
                ],
                field_name: smallvec![
                    Cow::Borrowed("actor.avatar_url"),
                    Cow::Borrowed("actor.gravatar_id"),
                    Cow::Borrowed("actor.id"),
                    Cow::Borrowed("actor.login"),
                    Cow::Borrowed("actor.url"),
                    Cow::Borrowed("id"),
                    Cow::Borrowed("repo.id"),
                    Cow::Borrowed("repo.name"),
                    Cow::Borrowed("repo.url"),
                    Cow::Borrowed("type"),
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
            IndexingDoc {
                entries: smallvec![
                    FieldValue { key: 0, value: Value::Str("https://avatars.githubusercontent.com/u/1310570?") },
                    FieldValue { key: 1, value: Value::Str("") },
                    FieldValue { key: 2, value: Value::U64(1310570) },
                    FieldValue { key: 3, value: Value::Str("soumith") },
                    FieldValue { key: 4, value: Value::Str("https://api.github.com/users/soumith") },
                    FieldValue { key: 5, value: Value::Str("2015-01-01T01:00:00Z") },
                    FieldValue { key: 6, value: Value::Str("2489395767") },
                    FieldValue { key: 7, value: Value::Str("2d06657267b32e0c8e193c617039da200f710195") },
                    FieldValue { key: 8, value: Value::Str("88de463b5797707cf3425f85a415c3d869db732b@gmail.com") },
                    FieldValue { key: 9, value: Value::Str("Soumith Chintala") },
                    FieldValue { key: 10, value: Value::Bool(true) },
                    FieldValue { key: 11, value: Value::Str("back to old structure, except lua files moved out") },
                    FieldValue { key: 12, value: Value::Str("dbd68d30ee1f7b60d404553fc1c6226ebb374c8e") },
                    FieldValue { key: 13, value: Value::Str("https://api.github.com/repos/soumith/fbcunn/commits/dbd68d30ee1f7b60d404553fc1c6226ebb374c8e") },
                    FieldValue { key: 8, value: Value::Str("88de463b5797707cf3425f85a415c3d869db732b@gmail.com") },
                    FieldValue { key: 9, value: Value::Str("Soumith Chintala") },
                    FieldValue { key: 10, value: Value::Bool(true) },
                    FieldValue { key: 11, value: Value::Str("...") },
                    FieldValue { key: 12, value: Value::Str("5567f9f5a83d7fe3320b18e5b89405e8a5ca77e6") },
                    FieldValue { key: 13, value: Value::Str("https://api.github.com/repos/soumith/fbcunn/commits/5567f9f5a83d7fe3320b18e5b89405e8a5ca77e6") },
                    FieldValue { key: 8, value: Value::Str("88de463b5797707cf3425f85a415c3d869db732b@gmail.com") },
                    FieldValue { key: 9, value: Value::Str("Soumith Chintala") },
                    FieldValue { key: 10, value: Value::Bool(true) },
                    FieldValue { key: 11, value: Value::Str("...") },
                    FieldValue { key: 12, value: Value::Str("58a83b277328eca811d3a37cf171b2fc4fcd87af") },
                    FieldValue { key: 13, value: Value::Str("https://api.github.com/repos/soumith/fbcunn/commits/58a83b277328eca811d3a37cf171b2fc4fcd87af") },
                    FieldValue { key: 8, value: Value::Str("88de463b5797707cf3425f85a415c3d869db732b@gmail.com") },
                    FieldValue { key: 9, value: Value::Str("Soumith Chintala") },
                    FieldValue { key: 10, value: Value::Bool(true) },
                    FieldValue { key: 11, value: Value::Str("...") },
                    FieldValue { key: 12, value: Value::Str("fa6048ec9b9eeafd12cee5f81324f355e1f2a198") },
                    FieldValue { key: 13, value: Value::Str("https://api.github.com/repos/soumith/fbcunn/commits/fa6048ec9b9eeafd12cee5f81324f355e1f2a198") },
                    FieldValue { key: 14, value: Value::U64(4) },
                    FieldValue { key: 15, value: Value::Str("fa6048ec9b9eeafd12cee5f81324f355e1f2a198") },
                    FieldValue { key: 16, value: Value::U64(536752122) },
                    FieldValue { key: 17, value: Value::Str("refs/heads/master") },
                    FieldValue { key: 18, value: Value::U64(4) },
                    FieldValue { key: 19, value: Value::Bool(true) },
                    FieldValue { key: 20, value: Value::U64(28067809) },
                    FieldValue { key: 21, value: Value::Str("soumith/fbcunn") },
                    FieldValue { key: 22, value: Value::Str("https://api.github.com/repos/soumith/fbcunn") },
                    FieldValue { key: 23, value: Value::Str("PushEvent") }
                ],
                field_name: smallvec![
                    Cow::Borrowed("actor.avatar_url"),
                    Cow::Borrowed("actor.gravatar_id"),
                    Cow::Borrowed("actor.id"),
                    Cow::Borrowed("actor.login"),
                    Cow::Borrowed("actor.url"),
                    Cow::Borrowed("created_at"),
                    Cow::Borrowed("id"),
                    Cow::Borrowed("payload.before"),
                    Cow::Borrowed("payload.commits.author.email"),
                    Cow::Borrowed("payload.commits.author.name"),
                    Cow::Borrowed("payload.commits.distinct"),
                    Cow::Borrowed("payload.commits.message"),
                    Cow::Borrowed("payload.commits.sha"),
                    Cow::Borrowed("payload.commits.url"),
                    Cow::Borrowed("payload.distinct_size"),
                    Cow::Borrowed("payload.head"),
                    Cow::Borrowed("payload.push_id"),
                    Cow::Borrowed("payload.ref"),
                    Cow::Borrowed("payload.size"),
                    Cow::Borrowed("public"),
                    Cow::Borrowed("repo.id"),
                    Cow::Borrowed("repo.name"),
                    Cow::Borrowed("repo.url"),
                    Cow::Borrowed("type"),
                ],
            },
        );
    }
}
