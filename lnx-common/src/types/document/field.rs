use std::fmt;
use std::str::FromStr;

use serde::de::value::SeqAccessDeserializer;
use serde::de::{SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use bincode::{Encode, Decode};

use crate::types::Value;

#[derive(Debug, Clone, Encode, Decode)]
pub enum DocField {
    Empty,
    Single(Value),
    Multi(Vec<Value>),
}

impl<'de> Deserialize<'de> for DocField {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DocFieldVisitor;

        impl<'de> Visitor<'de> for DocFieldVisitor {
            type Value = DocField;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a string, int, float, base64 encoded bytes or a list of those types")
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E> {
                Ok(DocField::Single(Value::from(v)))
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> {
                Ok(DocField::Single(Value::from(v)))
            }

            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E> {
                Ok(DocField::Single(Value::from(v)))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> {
                if let Ok(dt) = tantivy::DateTime::from_str(v) {
                    return Ok(DocField::Single(Value::from(dt)));
                }

                if let Ok(bytes) = base64::decode(v) {
                    return Ok(DocField::Single(Value::from(bytes)));
                }

                Ok(DocField::Single(Value::from(v.to_owned())))
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E> {
                if let Ok(dt) = tantivy::DateTime::from_str(&v) {
                    return Ok(DocField::Single(Value::from(dt)));
                }

                if let Ok(bytes) = base64::decode(&v) {
                    return Ok(DocField::Single(Value::from(bytes)));
                }

                Ok(DocField::Single(Value::from(v)))
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E> {
                Ok(DocField::Single(Value::from(v)))
            }

            fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E> {
                Ok(DocField::Single(Value::from(v)))
            }

            fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                Vec::deserialize(SeqAccessDeserializer::new(seq)).map(DocField::Multi)
            }
        }

        deserializer.deserialize_any(DocFieldVisitor)
    }
}

impl Serialize for DocField {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            DocField::Empty => None::<Value>.serialize(serializer),
            DocField::Single(v) => v.serialize(serializer),
            DocField::Multi(v) => v.serialize(serializer),
        }
    }
}
