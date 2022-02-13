use std::fmt;
use std::str::FromStr;

use serde::de::value::MapAccessDeserializer;
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tantivy::tokenizer::PreTokenizedString;

use super::Value;

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ValueVisitor;

        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = Value;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a string, int, float, base64 encoded bytes or map of pre-tokenized text")
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E> {
                Ok(Value::I64(v))
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> {
                Ok(Value::U64(v))
            }

            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E> {
                Ok(Value::F64(v))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> {
                if let Ok(dt) = tantivy::DateTime::from_str(v) {
                    return Ok(Value::DateTime(dt));
                }

                if let Ok(bytes) = base64::decode(v) {
                    return Ok(Value::Bytes(bytes));
                }

                Ok(Value::Text(v.to_owned()))
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E> {
                if let Ok(dt) = tantivy::DateTime::from_str(&v) {
                    return Ok(Value::DateTime(dt));
                }

                if let Ok(bytes) = base64::decode(&v) {
                    return Ok(Value::Bytes(bytes));
                }

                Ok(Value::Text(v))
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E> {
                Ok(Value::Bytes(v.to_vec()))
            }

            fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E> {
                Ok(Value::Bytes(v))
            }

            fn visit_map<M>(self, map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let data =
                    PreTokenizedString::deserialize(MapAccessDeserializer::new(map))?;
                Ok(Value::PreTokenizedText(data))
            }
        }

        deserializer.deserialize_any(ValueVisitor)
    }
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Value::I64(v) => v.serialize(serializer),
            Value::U64(v) => v.serialize(serializer),
            Value::F64(v) => v.serialize(serializer),
            Value::PreTokenizedText(v) => v.serialize(serializer),
            Value::DateTime(v) => v.to_rfc3339().serialize(serializer),
            Value::Text(v) => v.serialize(serializer),
            Value::Bytes(v) => {
                let data = base64::encode(v);
                data.serialize(serializer)
            },
        }
    }
}
