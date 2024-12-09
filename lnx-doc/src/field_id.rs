use std::fmt::Formatter;
use serde::de::Error;
use serde::Deserializer;

#[derive(
    Debug,
    Hash,
    Copy,
    Clone,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[rkyv(derive(Eq, PartialEq, Hash, Ord, PartialOrd))]
/// A unique field identifier.
pub struct FieldId(u64);

impl From<&str> for FieldId {
    #[inline]
    fn from(value: &str) -> Self {
        let id = cityhasher::hash(value);
        Self(id)
    }
}

impl<'de> serde::de::Deserialize<'de> for FieldId {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(FieldIdVisitor)
    }
}

struct FieldIdVisitor;

impl<'de> serde::de::Visitor<'de> for FieldIdVisitor {
    type Value = FieldId;

    #[inline]
    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        write!(formatter, "A string value")
    }

    #[inline]
    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(FieldId::from(v))
    }

    #[inline]
    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(FieldId::from(v))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use super::*;
    
    #[test]
    fn test_field_id_str_ok() {
        let sample = json!("example_field");
        let field_id: FieldId = serde_json::from_value(sample).unwrap();
        assert_eq!(field_id, FieldId(9420560873118935924));
    }
    
    #[test]
    fn test_field_id_invalid_type() {
        let sample = json!(12431);
        let err = serde_json::from_value::<FieldId>(sample)
            .expect_err("FieldId should not support deserializing integer");
        assert_eq!(err.to_string(), "invalid type: integer `12431`, expected A string value");
    }
}