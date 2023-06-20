use core::fmt;
use std::borrow::Cow;
use std::collections::BTreeMap;

use serde::de::value::MapAccessDeserializer;
use serde::de::{MapAccess, SeqAccess};
use serde::Deserialize;

use crate::UserDiplayType;

pub type JsonMap<'a> = BTreeMap<Cow<'a, str>, Value<'a>>;
pub type JsonMapIter<'a> =
    std::collections::btree_map::IntoIter<Cow<'a, str>, Value<'a>>;

#[derive(Debug, Clone, PartialEq)]
pub enum Value<'a> {
    Null,
    Str(Cow<'a, str>),
    U64(u64),
    I64(i64),
    F64(f64),
    Bool(bool),
    Array(Vec<Value<'a>>),
    Object(JsonMap<'a>),
}

impl<'a> UserDiplayType for Value<'a> {
    fn type_name(&self) -> Cow<'static, str> {
        match self {
            Value::Null => Cow::Borrowed("null"),
            Value::Str(_) => Cow::Borrowed("string"),
            Value::U64(_) => Cow::Borrowed("u64"),
            Value::I64(_) => Cow::Borrowed("i64"),
            Value::F64(_) => Cow::Borrowed("f64"),
            Value::Bool(_) => Cow::Borrowed("bool"),
            Value::Array(_) => Cow::Borrowed("array"),
            Value::Object(_) => Cow::Borrowed("object"),
        }
    }
}

impl<'a> Value<'a> {
    /// Converts the JSON value into typed value as is.
    pub fn into_typed_as_is(self) -> crate::typed_value::Value<'a> {
        match self {
            Value::Null => crate::typed_value::Value::Null,
            Value::Str(v) => crate::typed_value::Value::Str(v),
            Value::U64(v) => crate::typed_value::Value::U64(v),
            Value::I64(v) => crate::typed_value::Value::I64(v),
            Value::F64(v) => crate::typed_value::Value::F64(v),
            Value::Bool(v) => crate::typed_value::Value::Bool(v),
            Value::Array(elements) => crate::typed_value::Value::Array(
                elements.into_iter().map(|v| v.into_typed_as_is()).collect(),
            ),
            Value::Object(object) => crate::typed_value::Value::Object(
                object
                    .into_iter()
                    .map(|(k, v)| (k, v.into_typed_as_is()))
                    .collect(),
            ),
        }
    }
}

impl<'a> From<&'a str> for Value<'a> {
    fn from(value: &'a str) -> Self {
        Self::Str(Cow::Borrowed(value))
    }
}

impl<'a> From<String> for Value<'a> {
    fn from(value: String) -> Self {
        Self::Str(Cow::Owned(value))
    }
}

impl<'a> From<u64> for Value<'a> {
    fn from(value: u64) -> Self {
        Self::U64(value)
    }
}

impl<'a> From<i64> for Value<'a> {
    fn from(value: i64) -> Self {
        Self::I64(value)
    }
}

impl<'a> From<f64> for Value<'a> {
    fn from(value: f64) -> Self {
        Self::F64(value)
    }
}

impl<'a> From<bool> for Value<'a> {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl<'a, T: Into<Value<'a>>> From<Vec<T>> for Value<'a> {
    fn from(value: Vec<T>) -> Self {
        Self::Array(value.into_iter().map(|v| v.into()).collect())
    }
}

impl<'a, T: Into<Value<'a>>> From<BTreeMap<Cow<'a, str>, T>> for Value<'a> {
    fn from(value: BTreeMap<Cow<'a, str>, T>) -> Self {
        Self::Object(value.into_iter().map(|(k, v)| (k, v.into())).collect())
    }
}

impl<'de> serde::Deserialize<'de> for Value<'de> {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Value<'de>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ValueVisitor;

        impl<'de> serde::de::Visitor<'de> for ValueVisitor {
            type Value = Value<'de>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("any valid JSON value")
            }

            #[inline]
            fn visit_bool<E>(self, value: bool) -> Result<Value<'de>, E> {
                Ok(Value::Bool(value))
            }

            #[inline]
            fn visit_i64<E>(self, value: i64) -> Result<Value<'de>, E> {
                Ok(Value::I64(value))
            }

            #[inline]
            fn visit_u64<E>(self, value: u64) -> Result<Value<'de>, E> {
                Ok(Value::U64(value))
            }

            #[inline]
            fn visit_f64<E>(self, value: f64) -> Result<Value<'de>, E> {
                Ok(Value::F64(value))
            }

            #[inline]
            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Value::Str(Cow::Owned(v.to_owned())))
            }

            #[inline]
            fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Value::Str(Cow::Borrowed(v)))
            }

            #[inline]
            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Value::Str(Cow::Owned(v)))
            }

            #[inline]
            fn visit_none<E>(self) -> Result<Value<'de>, E> {
                Ok(Value::Null)
            }

            #[inline]
            fn visit_some<D>(self, deserializer: D) -> Result<Value<'de>, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                Deserialize::deserialize(deserializer)
            }

            #[inline]
            fn visit_unit<E>(self) -> Result<Value<'de>, E> {
                Ok(Value::Null)
            }

            #[inline]
            fn visit_seq<V>(self, mut visitor: V) -> Result<Value<'de>, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let mut vec = Vec::with_capacity(visitor.size_hint().unwrap_or(0));

                while let Some(elem) = visitor.next_element()? {
                    vec.push(elem);
                }

                Ok(Value::Array(vec))
            }

            #[inline]
            fn visit_map<V>(self, visitor: V) -> Result<Value<'de>, V::Error>
            where
                V: MapAccess<'de>,
            {
                let de = MapAccessDeserializer::new(visitor);
                Ok(Value::Object(BTreeMap::deserialize(de)?))
            }
        }

        deserializer.deserialize_any(ValueVisitor)
    }
}
