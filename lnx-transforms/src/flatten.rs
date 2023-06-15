use std::borrow::Cow;
use lnx_document::json_value::{JsonMap, Value};


/// Flattens a given JSON object map producing a set of fields and their flattened values.
///
/// This implementation also flattens nested arrays, appending a `_lnx_{idx}` pattern as
/// if it was a nested object.
///
/// A prefix of `_` is also added to each field so the system can split on `._` to get the
/// original field names back with a lower change of causing a conflict.
///
/// This turns a map like:
/// ```json
/// {
///     "hello": {
///         "world": {
///             "foo": [1, 2, 3, 4],
///             "bar": [2, 2, 3 ,5],
///             "super_nested": [
///                 ["lol", "yeet"],
///                 2,
///                 true,
///                 5,
///             ],
///         },
///         "demo": false,
///         "bar": "my world lol"
///     }
/// }
/// ```
///
/// Into:
/// ```norun
/// [
///     ("hello.world.foo", [1, 2, 3, 4]),
///     ("hello.world.bar", [2, 2, 3, 5]),
///     ("hello.world.super_nested._lnx_0", ["lol", "yeet"]),
///     ("hello.world.super_nested", [2, true, 5]),
///     ("hello.demo", false),
///     ("hello.bar", "my world lol"),
/// ]
/// ```
pub fn flatten_object(object: JsonMap) -> Vec<(Cow<str>, Value)> {
    let mut fields = Vec::new();

    for (key, value) in object {
        unpack(&mut fields, key, value);
    }

    fields
}

fn unpack<'a>(
    fields: &mut Vec<(Cow<'a, str>, Value<'a>)>,
    key: Cow<'a, str>,
    value: Value<'a>,
) {
    match value {
        Value::Array(elements) => {
            let mut flattened_elements = Vec::new();
            for (i, value) in elements.into_iter().enumerate() {
                match value {
                    Value::Array(nested_elements) => {
                        let key = Cow::Owned(format!("{key}._lnx_{i}"));
                        unpack(fields, key, Value::Array(nested_elements));
                    },
                    Value::Object(object) => {
                        unpack(fields, key.clone(), Value::Object(object))
                    },
                    other => flattened_elements.push(other),
                }
            }
            fields.push((key, Value::Array(flattened_elements)));
        },
        Value::Object(object) => {
            for (sub_key, value) in object {
                unpack(fields, Cow::Owned(format!("{key}._{sub_key}")),  value);
            }
        },
        other => {
            fields.push((key, other));
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flatten() {
        let value = serde_json::json!({
            "hello": {
                "world": {
                    "foo": [1, 2, 3, 4],
                    "bar": [2, 2, 3 ,5],
                    "demo2": [
                        ["lol", "yeet"],
                        2,
                        true,
                        5,
                    ],
                },
                "demo": false,
                "bar": "my world lol"
            }
        }).to_string();

        let value: Value = serde_json::from_str(&value).unwrap();
        let map = match value {
            Value::Object(v) => v,
            _ => unreachable!(),
        };

        let flattened = flatten_object(map);
        assert_eq!(
            flattened,
            [
                (Cow::Borrowed("hello._bar"), Value::from("my world lol")),
                (Cow::Borrowed("hello._demo"), Value::from(false)),
                (Cow::Borrowed("hello._world._bar"), Value::from(vec![2u64, 2, 3, 5])),
                (Cow::Borrowed("hello._world._demo2._lnx_0"), Value::from(vec!["lol", "yeet"])),
                (Cow::Borrowed("hello._world._demo2"), Value::from(vec![
                    Value::from(2u64),
                    Value::from(true),
                    Value::from(5u64),
                ])),
                (Cow::Borrowed("hello._world._foo"), Value::from(vec![1u64, 2, 3, 4])),
            ]
        );
    }
}