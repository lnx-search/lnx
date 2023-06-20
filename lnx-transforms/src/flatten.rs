use std::borrow::Cow;

use lnx_document::typed_value::{TypedMap, TypedMapIter, Value};
use smallvec::SmallVec;

type ValueStack<'a> = SmallVec<[(Cow<'a, str>, Value<'a>); 10]>;

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
pub struct ObjectFlattener<'a> {
    object: TypedMapIter<'a>,
    fields: ValueStack<'a>,
}

impl<'a> From<TypedMap<'a>> for ObjectFlattener<'a> {
    fn from(map: TypedMap<'a>) -> Self {
        Self::from(map.into_iter())
    }
}

impl<'a> From<TypedMapIter<'a>> for ObjectFlattener<'a> {
    fn from(object: TypedMapIter<'a>) -> Self {
        Self {
            object,
            fields: ValueStack::new(),
        }
    }
}

impl<'a> Iterator for ObjectFlattener<'a> {
    type Item = (Cow<'a, str>, Value<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(item) = self.fields.pop() {
            return Some(item);
        }

        let (key, value) = self.object.next()?;
        unpack(&mut self.fields, key, value);

        self.fields.pop()
    }
}

fn unpack<'a>(fields: &mut ValueStack<'a>, key: Cow<'a, str>, value: Value<'a>) {
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
                unpack(fields, Cow::Owned(format!("{key}._{sub_key}")), value);
            }
        },
        other => {
            fields.push((key, other));
        },
    }
}
