use std::io;

use anyhow::Result;

use crate::{helpers, ArchivedFieldType, DocumentView, Step};

// JSON methods.,
impl<'block> DocumentView<'block> {
    /// Serializes the view to a JSON string.
    pub fn to_json_string(&self) -> Result<String> {
        let mut buffer = Vec::new();
        self.to_json(&mut buffer)?;
        Ok(String::from_utf8(buffer).expect("Data should be guaranteed UTF-8"))
    }

    /// Serializes the view to a JSON formatted value in a given writer.
    pub fn to_json<W: io::Write>(&self, writer: &mut W) -> Result<()> {
        write!(writer, "{{")?;

        let mut cursors = TypeCursors::default();

        let mut i = 0;
        let mut step_idx = 0;
        while step_idx < self.doc.layout.len() {
            let step: &rkyv::Archived<Step> = &self.doc.layout[step_idx];

            assert_ne!(
                step.field_id,
                u16::MAX,
                "Invalid doc layout, top level object cannot have array elements as part of the data (Indicated by the u16::MAX ID)"
            );

            self.json_serialize_map_field(&mut step_idx, &mut cursors, step, writer)?;

            if i < (self.doc.len as usize - 1) {
                write!(writer, ",")?;
            }

            step_idx += 1;
            i += 1;
        }

        write!(writer, "}}")?;

        Ok(())
    }

    fn json_serialize_map_field<W: io::Write>(
        &self,
        step_idx: &mut usize,
        cursors: &mut TypeCursors,
        step: &rkyv::Archived<Step>,
        writer: &mut W,
    ) -> io::Result<()> {
        debug_assert_ne!(step.field_id, u16::MAX, "Object field should not be an array sub element. This is a bug.\n{step_idx}\n {step:?}");

        let key: &str = self.block.field_mapping[step.field_id as usize].as_ref();

        if !matches!(
            step.field_type,
            ArchivedFieldType::Array | ArchivedFieldType::Object
        ) {
            assert_eq!(step.field_length, 1, "Field length for object values which are not collections should be single values.");
        }

        match step.field_type {
            ArchivedFieldType::Null => write!(writer, "\"{key}\":null")?,
            ArchivedFieldType::String => {
                let v: &str = self.block.strings[cursors.strings].as_ref();
                write!(writer, "\"{key}\":\"{v}\"")?;

                cursors.strings += 1;
            },
            ArchivedFieldType::Bytes => {
                let v =
                    helpers::to_base64_string(self.block.bytes[cursors.bytes].as_ref());
                write!(writer, "\"{key}\":\"{v}\"")?;

                cursors.bytes += 1;
            },
            ArchivedFieldType::Bool => {
                let v = &self.block.bools[cursors.bools];

                if *v {
                    write!(writer, "\"{key}\":true")?;
                } else {
                    write!(writer, "\"{key}\":false")?;
                }

                cursors.bools += 1;
            },
            ArchivedFieldType::U64 => {
                let v = &self.block.u64s[cursors.u64s];

                let mut buffer = itoa::Buffer::new();
                let s = buffer.format(*v);
                write!(writer, "\"{key}\":{s}")?;

                cursors.u64s += 1;
            },
            ArchivedFieldType::I64 => {
                let v = &self.block.i64s[cursors.i64s];

                let mut buffer = itoa::Buffer::new();
                let s = buffer.format(*v);
                write!(writer, "\"{key}\":{s}")?;

                cursors.i64s += 1;
            },
            ArchivedFieldType::F64 => {
                let v = &self.block.f64s[cursors.f64s];

                let mut buffer = ryu::Buffer::new();
                let s = buffer.format(*v);
                write!(writer, "\"{key}\":{s}")?;

                cursors.f64s += 1;
            },
            ArchivedFieldType::IpAddr => {
                let ip = self.block.ips[cursors.ips];

                if let Some(ipv4) = ip.to_ipv4() {
                    write!(writer, "\"{key}\":\"{ipv4}\"")?;
                } else {
                    write!(writer, "\"{key}\":\"{}\"", ip.as_ipv6())?;
                }

                cursors.ips += 1;
            },
            ArchivedFieldType::DateTime => {
                // TODO: Handle datetime formats correctly.
                let v = &self.block.i64s[cursors.i64s];
                write!(writer, "\"{key}\":{}", *v)?;

                cursors.i64s += 1;
            },
            ArchivedFieldType::Facet => {
                let v: &str = self.block.strings[cursors.strings].as_ref();
                write!(writer, "\"{key}\":\"{v}\"")?;

                cursors.strings += 1;
            },
            ArchivedFieldType::Array => {
                let collection_length = step.field_length as usize;

                write!(writer, "\"{key}\":[")?;
                for i in 0..collection_length {
                    (*step_idx) += 1;

                    let step = &self.doc.layout[*step_idx];
                    self.json_serialize_array_element(step_idx, cursors, step, writer)?;

                    if i < (collection_length - 1) {
                        write!(writer, ",")?;
                    }
                }
                write!(writer, "]")?;
            },
            ArchivedFieldType::Object => {
                let collection_length = step.field_length as usize;

                write!(writer, "\"{key}\":{{")?;
                for i in 0..collection_length {
                    (*step_idx) += 1;

                    let step = &self.doc.layout[*step_idx];
                    self.json_serialize_map_field(step_idx, cursors, step, writer)?;

                    if i < (collection_length - 1) {
                        write!(writer, ",")?;
                    }
                }
                write!(writer, "}}")?;
            },
        }

        Ok(())
    }

    #[inline]
    fn json_serialize_array_element<W: io::Write>(
        &self,
        step_idx: &mut usize,
        cursors: &mut TypeCursors,
        step: &rkyv::Archived<Step>,
        writer: &mut W,
    ) -> io::Result<()> {
        assert_eq!(
            step.field_id,
            u16::MAX,
            "Got non-array element step. This likely means the layout was read incorrectly. This is a bug."
        );

        match step.field_type {
            ArchivedFieldType::Null => write!(writer, "null")?,
            ArchivedFieldType::String => {
                let num_entries = step.field_length as usize;
                for i in 0..num_entries {
                    let v: &str = self.block.strings[cursors.strings].as_ref();
                    write!(writer, "\"{v}\"")?;

                    if i < (num_entries - 1) {
                        write!(writer, ",")?;
                    }

                    cursors.strings += 1;
                }
            },
            ArchivedFieldType::Bytes => {
                let num_entries = step.field_length as usize;
                for i in 0..num_entries {
                    let v = helpers::to_base64_string(
                        self.block.bytes[cursors.bytes].as_ref(),
                    );
                    write!(writer, "\"{v}\"")?;

                    if i < (num_entries - 1) {
                        write!(writer, ",")?;
                    }

                    cursors.bytes += 1;
                }
            },
            ArchivedFieldType::Bool => {
                let num_entries = step.field_length as usize;
                for i in 0..num_entries {
                    let v = &self.block.bools[cursors.bools];

                    if *v {
                        write!(writer, "true")?;
                    } else {
                        write!(writer, "false")?;
                    }

                    if i < (num_entries - 1) {
                        write!(writer, ",")?;
                    }

                    cursors.bools += 1;
                }
            },
            ArchivedFieldType::U64 => {
                let num_entries = step.field_length as usize;
                for i in 0..num_entries {
                    let v = &self.block.u64s[cursors.u64s];

                    let mut buffer = itoa::Buffer::new();
                    let s = buffer.format(*v);
                    write!(writer, "{s}")?;

                    if i < (num_entries - 1) {
                        write!(writer, ",")?;
                    }

                    cursors.u64s += 1;
                }
            },
            ArchivedFieldType::I64 => {
                let num_entries = step.field_length as usize;
                for i in 0..num_entries {
                    let v = &self.block.i64s[cursors.i64s];

                    let mut buffer = itoa::Buffer::new();
                    let s = buffer.format(*v);
                    write!(writer, "{s}")?;

                    if i < (num_entries - 1) {
                        write!(writer, ",")?;
                    }

                    cursors.i64s += 1;
                }
            },
            ArchivedFieldType::F64 => {
                let num_entries = step.field_length as usize;
                for i in 0..num_entries {
                    let v = &self.block.f64s[cursors.f64s];

                    let mut buffer = ryu::Buffer::new();
                    let s = buffer.format(*v);
                    write!(writer, "{s}")?;

                    if i < (num_entries - 1) {
                        write!(writer, ",")?;
                    }

                    cursors.f64s += 1;
                }
            },
            ArchivedFieldType::IpAddr => {
                let num_entries = step.field_length as usize;
                for i in 0..num_entries {
                    let ip = self.block.ips[cursors.ips];

                    if let Some(ipv4) = ip.to_ipv4() {
                        write!(writer, "\"{ipv4}\"")?;
                    } else {
                        write!(writer, "\"{}\"", ip.as_ipv6())?;
                    }

                    if i < (num_entries - 1) {
                        write!(writer, ",")?;
                    }

                    cursors.ips += 1;
                }
            },
            ArchivedFieldType::DateTime => {
                let num_entries = step.field_length as usize;
                for i in 0..num_entries {
                    let v = &self.block.i64s[cursors.i64s];

                    write!(writer, "{v}")?;

                    if i < (num_entries - 1) {
                        write!(writer, ",")?;
                    }

                    cursors.i64s += 1;
                }
            },
            ArchivedFieldType::Facet => {
                let num_entries = step.field_length as usize;
                for i in 0..num_entries {
                    let v: &str = self.block.strings[cursors.strings].as_ref();
                    write!(writer, "\"{v}\"")?;

                    if i < (num_entries - 1) {
                        write!(writer, ",")?;
                    }

                    cursors.strings += 1;
                }
            },
            ArchivedFieldType::Array => {
                let collection_length = step.field_length as usize;

                write!(writer, "[")?;
                for i in 0..collection_length {
                    (*step_idx) += 1;

                    let step = &self.doc.layout[*step_idx];
                    self.json_serialize_array_element(step_idx, cursors, step, writer)?;

                    if i < (collection_length - 1) {
                        write!(writer, ",")?;
                    }
                }
                write!(writer, "]")?;
            },
            ArchivedFieldType::Object => {
                let collection_length = step.field_length as usize;

                write!(writer, "{{")?;
                for i in 0..collection_length {
                    (*step_idx) += 1;

                    let step = &self.doc.layout[*step_idx];
                    self.json_serialize_map_field(step_idx, cursors, step, writer)?;

                    if i < (collection_length - 1) {
                        write!(writer, ",")?;
                    }
                }
                write!(writer, "}}")?;
            },
        }

        Ok(())
    }
}

#[derive(Copy, Clone, Default)]
struct TypeCursors {
    strings: usize,
    u64s: usize,
    i64s: usize,
    f64s: usize,
    ips: usize,
    bools: usize,
    bytes: usize,
}

#[cfg(test)]
mod tests {
    use rkyv::AlignedVec;
    use serde_json::json;

    use crate::rkyv_serializer::DocWriteSerializer;
    use crate::{ChecksumDocWriter, DocBlockBuilder, DocBlockReader, DocSerializer};

    fn get_view_of(json_text: &str) -> DocBlockReader {
        let doc = serde_json::from_str(json_text).unwrap();
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

    fn validate_full_json_cycle(value: serde_json::Value) {
        let expected_text = serde_json::to_string(&value).unwrap();
        let view = get_view_of(&expected_text);
        assert_eq!(view.doc(0).to_json_string().unwrap(), expected_text);
    }

    #[test]
    fn test_simple_doc() {
        validate_full_json_cycle(json!({"nulled": null}));
        validate_full_json_cycle(json!({"name": "bobby"}));
        validate_full_json_cycle(json!({"age": 123}));
        validate_full_json_cycle(json!({"x": -123}));
        validate_full_json_cycle(json!({"is_old": true}));
        validate_full_json_cycle(
            json!({"my-nested-value": {"age": 12, "name": "timmy"}}),
        );
        validate_full_json_cycle(json!({"my-array": [123, null, "foo"]}));
    }

    #[test]
    fn test_empty_values() {
        let complex = json!({});
        validate_full_json_cycle(complex);

        let complex = json!({
            "foo": {}
        });
        validate_full_json_cycle(complex);

        let complex = json!({
            "foo": []
        });
        validate_full_json_cycle(complex);

        let complex = json!({
            "foo": {
                "bar": {
                    "baz": {}
                }
            }
        });
        validate_full_json_cycle(complex);

        let complex = json!({
            "foo": [[], [[]]]
        });
        validate_full_json_cycle(complex);

        let complex = json!({
            "foo": {
                "bar": [
                    [
                        [
                            {}
                        ]
                    ]
                ]
            }
        });
        validate_full_json_cycle(complex);

        let complex = json!({
            "foo": {
                "bar": [
                    [
                        [
                            {"something": [[]]}
                        ]
                    ]
                ]
            }
        });
        validate_full_json_cycle(complex);
    }

    #[test]
    fn test_github_archive_docs() {
        let complex = json!({
            "payload": {
                "push_id": 536752122,
                "size": 4,
                "distinct_size": 4,
                "ref": "refs/heads/master",
                "head": "fa6048ec9b9eeafd12cee5f81324f355e1f2a198",
                "before": "2d06657267b32e0c8e193c617039da200f710195",
                "commits": []
            }
        });
        validate_full_json_cycle(complex);

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
            "payload": {}
        });
        validate_full_json_cycle(complex);

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
        validate_full_json_cycle(complex);
    }
}
