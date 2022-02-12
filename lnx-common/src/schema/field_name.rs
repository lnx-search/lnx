use std::ops::Deref;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::Error;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct FieldName(pub String);

impl AsRef<str> for FieldName {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl Deref for FieldName {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Serialize for FieldName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer
    {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for FieldName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de>
    {
        let s = String::deserialize(deserializer)?;

        if !s.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Err(D::Error::custom("field names must only consistent of alphanumeric characters within the ASCII range."))
        }

        Ok(Self(s))
    }
}