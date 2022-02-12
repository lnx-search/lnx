use std::fmt;
use serde::{Serialize, Deserialize, Serializer, Deserializer};
use serde::de::{Error, SeqAccess, Visitor};
use serde::de::value::SeqAccessDeserializer;

use crate::types::Value;

#[derive(Debug, Clone)]
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

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "a string, int, float, map or a list of strings, ints, floats or DateTimes.")
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: Error
            {
                Ok(DocField::Empty)
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error> 
            where D: Deserializer<'de>
            {
                Ok(DocField::Single(Value::deserialize(deserializer)?))
            }

            fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                Vec::deserialize(SeqAccessDeserializer::new(seq))
                    .map(DocField::Multi)
            }
        }

        deserializer.deserialize_any(DocFieldVisitor)
    }
}

impl Serialize for DocField {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer
    {
        match self {
            DocField::Empty => None::<()>.serialize(serializer),
            DocField::Single(v) => v.serialize(serializer),
            DocField::Multi(v) => v.serialize(serializer),
        }
    }
}
