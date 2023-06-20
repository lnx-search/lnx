use anyhow::{anyhow, bail, Result};
use lnx_document::{json_value, typed_value, UserDiplayType};

use crate::op_codes::TransformOp;
use crate::TypeCast;

pub type TransformedFields<'a> = Vec<(&'a str, typed_value::Value<'a>)>;

pub enum StackEntry<'a> {
    Missing,
    Json(json_value::Value<'a>),
    Typed(typed_value::Value<'a>),
}

/// A VM for transforming and casting documents against a given
/// op-code program.
pub struct TransformVM {
    program: Vec<TransformOp>,
    allocator: bumpalo::Bump,
}

impl TransformVM {
    pub fn new(program: Vec<TransformOp>) -> Self {
        Self {
            program,
            allocator: bumpalo::Bump::with_capacity(64 << 10),
        }
    }

    /// Transforms and converts a JSON object into a typed document.
    pub fn transform<'a, 'b: 'a>(
        &'b self,
        doc: json_value::JsonMap<'a>,
    ) -> Result<TransformedFields<'a>> {
        let mut fields = Vec::new();

        let mut keys_history =
            bumpalo::collections::Vec::with_capacity_in(4, &self.allocator);

        let mut stack = bumpalo::collections::Vec::with_capacity_in(4, &self.allocator);
        stack.push(StackEntry::Json(json_value::Value::Object(doc)));

        for op in self.program.iter() {
            let mut stack_entry = stack.pop().ok_or_else(|| {
                anyhow!("TransformVM ran out of stack values during operations")
            })?;

            match op {
                TransformOp::Load { key, keep_object } => {
                    keys_history.push(key.as_str());
                    let entry = get_key(key, &mut stack_entry, &keys_history)?;

                    if *keep_object {
                        stack.push(stack_entry);
                    }

                    stack.push(entry);
                },
                TransformOp::LoadOptional { key, keep_object } => {
                    keys_history.push(key.as_str());

                    let entry = get_key_optional(key, &mut stack_entry, &keys_history)?;

                    if *keep_object {
                        stack.push(stack_entry);
                    }

                    stack.push(entry);
                },
                TransformOp::Store { as_key } => {
                    store(&mut fields, as_key, stack_entry)?;
                    keys_history.pop();
                },
                TransformOp::Cast { ty } => {
                    let entry = cast(ty, stack_entry, &keys_history)?;
                    stack.push(entry);
                },
                TransformOp::RejectNull => {
                    let entry = check_not_null(stack_entry, &keys_history)?;
                    stack.push(entry);
                },
            }
        }

        Ok(fields)
    }
}

fn get_key<'a>(
    key: &str,
    entry: &mut StackEntry<'a>,
    key_history: &[&str],
) -> Result<StackEntry<'a>> {
    let map_data = extract_map(key, entry, key_history)?;

    let entry_res = match map_data {
        ExtractedMap::Json(fields) => fields.remove(key).map(StackEntry::Json),
        ExtractedMap::Typed(fields) => fields.remove(key).map(StackEntry::Typed),
    };

    if entry_res.is_none() && !key_history.is_empty() {
        bail!(
            "Missing required field: {key:?} at location: {:?}",
            render_history(key_history)
        );
    } else if entry_res.is_none() {
        bail!("Missing required field: {key:?}");
    } else {
        Ok(entry_res.unwrap())
    }
}

fn get_key_optional<'a>(
    key: &str,
    entry: &mut StackEntry<'a>,
    key_history: &[&str],
) -> Result<StackEntry<'a>> {
    let map_data = extract_map(key, entry, key_history)?;

    let entry = match map_data {
        ExtractedMap::Json(fields) => fields
            .remove(key)
            .map(StackEntry::Json)
            .unwrap_or(StackEntry::Missing),
        ExtractedMap::Typed(fields) => fields
            .remove(key)
            .map(StackEntry::Typed)
            .unwrap_or(StackEntry::Missing),
    };

    Ok(entry)
}

enum ExtractedMap<'a, 'b> {
    Json(&'a mut json_value::JsonMap<'b>),
    Typed(&'a mut typed_value::TypedMap<'b>),
}

fn extract_map<'a, 'b: 'a>(
    key: &str,
    entry: &'a mut StackEntry<'b>,
    key_history: &[&str],
) -> Result<ExtractedMap<'a, 'b>> {
    match entry {
        StackEntry::Missing => {
            bail!(
                "Cannot get key ({key:?}) from field ({:?}) as the field is missing",
                render_history(key_history)
            )
        },
        StackEntry::Json(value) => {
            match value {
                json_value::Value::Object(map) => Ok(ExtractedMap::Json(map)),
                other => bail!(
                    "Cannot get key ({key:?}) from field ({:?}) as the value is not a object, got: `{}`",
                    render_history(key_history),
                    other.type_name(),
                ),
            }
        },
        StackEntry::Typed(value) => {
            match value {
                typed_value::Value::Object(map) => Ok(ExtractedMap::Typed(map)),
                other => bail!(
                    "Cannot get key ({key:?}) from field ({:?}) as the value is not a object, got: `{}`",
                    render_history(key_history),
                    other.type_name(),
                ),
            }
        },
    }
}

fn store<'a, 'b: 'a>(
    fields: &mut TransformedFields<'a>,
    key: &'b str,
    entry: StackEntry<'a>,
) -> Result<()> {
    match entry {
        StackEntry::Missing => fields.push((key, typed_value::Value::Null)),
        StackEntry::Json(value) => fields.push((key, value.into_typed_as_is())),
        StackEntry::Typed(value) => fields.push((key, value)),
    };

    Ok(())
}

fn cast<'a>(
    ty: &TypeCast,
    entry: StackEntry<'a>,
    keys_history: &[&str],
) -> Result<StackEntry<'a>> {
    match entry {
        StackEntry::Missing => Ok(StackEntry::Missing),
        StackEntry::Json(object) => {
            let cast = ty.try_cast_json(object, keys_history)?;
            Ok(StackEntry::Typed(cast))
        },
        StackEntry::Typed(object) => {
            let cast = ty.try_cast_typed(object, keys_history)?;
            Ok(StackEntry::Typed(cast))
        },
    }
}

fn check_not_null<'a>(
    entry: StackEntry<'a>,
    keys_history: &[&str],
) -> Result<StackEntry<'a>> {
    match entry {
        StackEntry::Json(json_value::Value::Null) => {
            bail!(
                "Value must not be null for field ({:?})",
                render_history(keys_history)
            )
        },
        StackEntry::Typed(typed_value::Value::Null) => {
            bail!(
                "Value must not be null for field ({:?})",
                render_history(keys_history)
            )
        },
        entry => Ok(entry),
    }
}

fn render_history(keys_history: &[&str]) -> String {
    keys_history.join(".")
}

#[cfg(test)]
mod tests {
    use std::sync::OnceLock;

    use serde_json::json;

    use super::*;

    macro_rules! doc {
        ($($json:tt)+) => {{
            static DOC: OnceLock<String> = OnceLock::new();
            let data = DOC.get_or_init(|| {
                json!($($json)*).to_string()
            });
            serde_json::from_str::<lnx_document::json_value::JsonMap<'static>>(data).unwrap()
        }};
    }

    macro_rules! typed {
        ($($key:expr => $value:expr $(,)?)+) => {{
            let mut map = std::collections::BTreeMap::new();
             $(
                 let k = std::borrow::Cow::Borrowed($key);
                 let v = typed_value::Value::from($value);
                 map.insert(k, v);
             )*
            map
        }};
    }

    macro_rules! transformed {
        ($($key:expr => $value:expr $(,)?)*) => {{
            [
                $((
                    $key,
                    typed_value::Value::from($value),
                ),)*
            ]
        }};
    }

    #[test]
    fn test_basic_load_store() {
        let doc = doc!({
            "user": {
                "name": "Bobby",
                "age": 45,
                "username": "bigbobby12"
            }
        });

        let program = vec![
            TransformOp::load("user"),
            TransformOp::store("processed_user"),
        ];

        let vm = TransformVM::new(program);
        let result = vm.transform(doc).unwrap();

        assert_eq!(
            result,
            transformed!(
                "processed_user" => typed!(
                    "age" => 45u64,
                    "name" => "Bobby",
                    "username" => "bigbobby12",
                ),
            )
        )
    }

    #[test]
    fn test_cast_object_err() {
        let doc = doc!({
            "user": {
                "name": "Bobby",
                "age": 45,
                "username": "bigbobby12"
            }
        });

        let program = vec![
            TransformOp::load("user"),
            TransformOp::cast(TypeCast::I64),
            TransformOp::store("processed_user"),
        ];

        let vm = TransformVM::new(program);
        let result = vm.transform(doc).unwrap_err();
        assert_eq!(
            result.to_string(),
            "Cannot cast `object` to `i64` for field (\"user\")"
        );
    }

    #[test]
    fn test_load_nested_cast_age() {
        let doc = doc!({
            "user": {
                "name": "Bobby",
                "age": 45,
                "username": "bigbobby12"
            }
        });

        let program = vec![
            TransformOp::load("user"),
            TransformOp::load("age"),
            TransformOp::cast(TypeCast::I64),
            TransformOp::store("age"),
            TransformOp::store("processed_user"),
        ];

        let vm = TransformVM::new(program);
        let result = vm.transform(doc).unwrap();

        assert_eq!(
            result,
            transformed!(
                "age" => 45i64,
                "processed_user" => typed!(
                    "name" => "Bobby",
                    "username" => "bigbobby12",
                ),
            )
        )
    }

    #[test]
    fn test_load_nested_cast_err() {
        let doc = doc!({
            "user": {
                "name": "Bobby",
                "age": 45,
                "username": "bigbobby12"
            }
        });

        let program = vec![
            TransformOp::load("user"),
            TransformOp::load("name"),
            TransformOp::cast(TypeCast::I64),
            TransformOp::store("age"),
            TransformOp::store("processed_user"),
        ];

        let vm = TransformVM::new(program);
        let result = vm.transform(doc).unwrap_err();
        assert_eq!(
            result.to_string(),
            "Cannot cast `string` to `i64` for field (\"user.name\") due to an invalid value being provided: \"Bobby\""
        );
    }

    #[test]
    fn test_load_nested_flatten_rename() {
        let doc = doc!({
            "user": {
                "name": "Bobby",
                "age": 45,
                "username": "bigbobby12"
            }
        });

        let program = vec![
            TransformOp::load("user"),
            TransformOp::load("username"),
            TransformOp::store("user.username"),
            TransformOp::store("processed_user"),
        ];

        let vm = TransformVM::new(program);
        let result = vm.transform(doc).unwrap();

        assert_eq!(
            result,
            transformed!(
                "user.username" => "bigbobby12",
                "processed_user" => typed!(
                    "name" => "Bobby",
                    "age" => 45u64,
                ),
            )
        )
    }

    #[test]
    fn test_reject_null() {
        let doc = doc!({
            "user": null,
            "working": "success"
        });

        let program = vec![TransformOp::load("user"), TransformOp::reject_null()];
        let vm = TransformVM::new(program);
        let result = vm.transform(doc.clone()).unwrap_err();
        assert_eq!(
            result.to_string(),
            "Value must not be null for field (\"user\")"
        );

        let program = vec![
            TransformOp::load("working"),
            TransformOp::reject_null(),
            TransformOp::store("working"),
        ];
        let vm = TransformVM::new(program);
        let result = vm.transform(doc).unwrap();

        assert_eq!(result, transformed!("working" => "success"),)
    }

    #[test]
    fn test_missing_reject_null_ok() {
        let doc = doc!({
            "working": "success",
            "user": null,
        });

        let program = vec![
            TransformOp::load_opt("some-missing-field"),
            TransformOp::reject_null(),
            TransformOp::store("this_key_now_exists"),
        ];
        let vm = TransformVM::new(program);
        let result = vm.transform(doc.clone()).unwrap();

        assert_eq!(
            result,
            transformed!("this_key_now_exists" => typed_value::Value::Null)
        )
    }

    #[test]
    fn test_nested_arrays_edge_cast() {
        let doc = doc!({
            "arrays_of_arrays": [
                ["hello", "world"],
                [123, 456],
            ],
        });

        let program = vec![
            TransformOp::load("arrays_of_arrays"),
            // This should not be allowed as we reject casting on nested arrays.
            TransformOp::cast(TypeCast::String),
        ];
        let vm = TransformVM::new(program);
        let result = vm.transform(doc.clone()).unwrap_err();

        assert_eq!(
            result.to_string(),
            "Cannot cast `array` to `string` for field (\"arrays_of_arrays\") due to field containing an array of arrays or array of objects"
        );
    }
}
