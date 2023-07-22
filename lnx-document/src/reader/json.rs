use std::io;
use std::net::Ipv6Addr;

use crate::traverse::ViewWalker;
use crate::DateTime;

/// A view walker that writes the document data
/// to a given writer in a compact JSON format.
pub(crate) struct JSONWalker<'a, W>
where
    W: io::Write,
{
    writer: &'a mut W,
}

impl<'a, W> JSONWalker<'a, W>
where
    W: io::Write,
{
    /// Creates a new JSON walker with a given writer.
    pub(crate) fn new(writer: &'a mut W) -> Self {
        Self { writer }
    }

    fn maybe_write_seperator(&mut self, is_last: bool) -> io::Result<()> {
        if is_last {
            Ok(())
        } else {
            write!(self.writer, ",")
        }
    }
}

impl<'block, 'a: 'block, W> ViewWalker<'block> for JSONWalker<'a, W>
where
    W: io::Write,
{
    type Err = io::Error;

    #[inline]
    fn visit_null(&mut self, is_last: bool) -> Result<(), Self::Err> {
        write!(self.writer, "null")?;

        self.maybe_write_seperator(is_last)
    }

    #[inline]
    fn visit_str(&mut self, is_last: bool, val: &'block str) -> Result<(), Self::Err> {
        write!(self.writer, "\"{val}\"")?;
        self.maybe_write_seperator(is_last)
    }

    #[inline]
    fn visit_bytes(
        &mut self,
        is_last: bool,
        val: &'block [u8],
    ) -> Result<(), Self::Err> {
        let data = crate::helpers::to_base64_string(val);
        write!(self.writer, "\"{data}\"")?;
        self.maybe_write_seperator(is_last)
    }

    #[inline]
    fn visit_bool(&mut self, is_last: bool, val: bool) -> Result<(), Self::Err> {
        if val {
            write!(self.writer, "true")?;
        } else {
            write!(self.writer, "false")?;
        }
        self.maybe_write_seperator(is_last)
    }

    #[inline]
    fn visit_u64(&mut self, is_last: bool, val: u64) -> Result<(), Self::Err> {
        let mut buffer = itoa::Buffer::new();
        let s = buffer.format(val);
        write!(self.writer, "{s}")?;
        self.maybe_write_seperator(is_last)
    }

    #[inline]
    fn visit_i64(&mut self, is_last: bool, val: i64) -> Result<(), Self::Err> {
        let mut buffer = itoa::Buffer::new();
        let s = buffer.format(val);
        write!(self.writer, "{s}")?;
        self.maybe_write_seperator(is_last)
    }

    #[inline]
    fn visit_f64(&mut self, is_last: bool, val: f64) -> Result<(), Self::Err> {
        let mut buffer = ryu::Buffer::new();
        let s = buffer.format(val);
        write!(self.writer, "{s}")?;
        self.maybe_write_seperator(is_last)
    }

    #[inline]
    fn visit_ip(&mut self, is_last: bool, val: Ipv6Addr) -> Result<(), Self::Err> {
        if let Some(ipv4) = val.to_ipv4_mapped() {
            write!(self.writer, "{ipv4}")?;
        } else {
            write!(self.writer, "{val}")?;
        }
        self.maybe_write_seperator(is_last)
    }

    #[inline]
    fn visit_date(&mut self, is_last: bool, val: DateTime) -> Result<(), Self::Err> {
        // TODO: Add correct datetime formatting... This currently is just the timestamp
        self.visit_i64(is_last, val.as_micros())
    }

    #[inline]
    fn visit_facet(&mut self, is_last: bool, val: &'block str) -> Result<(), Self::Err> {
        self.visit_str(is_last, val)
    }

    #[inline]
    fn visit_map_key(&mut self, key: &'block str) -> Result<(), Self::Err> {
        write!(self.writer, "\"{key}\":")
    }

    #[inline]
    fn start_array(&mut self, _size_hint: usize) -> Result<(), Self::Err> {
        write!(self.writer, "[")
    }

    #[inline]
    fn end_array(&mut self, is_last: bool) -> Result<(), Self::Err> {
        write!(self.writer, "]")?;
        self.maybe_write_seperator(is_last)
    }

    #[inline]
    fn start_map(&mut self, _size_hint: usize) -> Result<(), Self::Err> {
        write!(self.writer, "{{")
    }

    #[inline]
    fn end_map(&mut self, is_last: bool) -> Result<(), Self::Err> {
        write!(self.writer, "}}")?;
        self.maybe_write_seperator(is_last)
    }
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
        validate_full_json_cycle(json!({"my-array": [null, null, null]}));
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
