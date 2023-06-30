use std::borrow::Cow;
use std::fmt::Formatter;
use serde::{Deserialize, Deserializer};
use serde::de::{MapAccess, SeqAccess, Visitor};

use crate::value::{DynamicDocument, Value};


impl<'de> Deserialize<'de> for DynamicDocument<'de> {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de> 
    {
        struct DocumentVisitor;

        impl<'de> Visitor<'de> for DocumentVisitor {
            type Value = DynamicDocument<'de>;
            
            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("a key-value object")
            }

            fn visit_map<A>(self, mut visitor: A) -> Result<Self::Value, A::Error> 
            where A: MapAccess<'de>
            {
                let mut values = Vec::with_capacity(visitor.size_hint().unwrap_or(0));

                while let Some((key, value)) = visitor.next_entry()? {
                    values.push((key, value));
                }

                Ok(DynamicDocument(values))
            }
        }
        
        deserializer.deserialize_map(DocumentVisitor)
    }
}

impl<'de> Deserialize<'de> for Value<'de> {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de>
    {
        struct ValueVisitor;

        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = Value<'de>;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
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
            where E: serde::de::Error {
                Ok(Value::Str(Cow::Owned(v.to_owned())))
            }

            #[inline]
            fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
            where E: serde::de::Error {
                Ok(Value::Str(Cow::Borrowed(v)))
            }

            #[inline]
            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where E: serde::de::Error {
                Ok(Value::Str(Cow::Owned(v)))
            }

            #[inline]
            fn visit_none<E>(self) -> Result<Value<'de>, E> {
                Ok(Value::Null)
            }

            #[inline]
            fn visit_some<D>(self, deserializer: D) -> Result<Value<'de>, D::Error>
            where D: Deserializer<'de> {
                Deserialize::deserialize(deserializer)
            }

            #[inline]
            fn visit_unit<E>(self) -> Result<Value<'de>, E> {
                Ok(Value::Null)
            }

            #[inline]
            fn visit_seq<V>(self, mut visitor: V) -> Result<Value<'de>, V::Error>
            where V: SeqAccess<'de> {
                let mut vec = Vec::with_capacity(visitor.size_hint().unwrap_or(0));

                while let Some(elem) = visitor.next_element()? {
                    vec.push(elem);
                }

                Ok(Value::Array(vec))
            }

            #[inline]
            fn visit_map<V>(self, mut visitor: V) -> Result<Value<'de>, V::Error>
            where V: MapAccess<'de> {
                let mut values = Vec::with_capacity(visitor.size_hint().unwrap_or(0));

                while let Some((key, value)) = visitor.next_entry()? {
                    values.push((key, value));
                }

                Ok(Value::Object(values))
            }
        }

        deserializer.deserialize_any(ValueVisitor)
    }
}