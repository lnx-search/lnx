use std::borrow::Cow;
use std::ops::Deref;

use poem_openapi::registry::{MetaSchemaRef, Registry};
use poem_openapi::types::{
    ParseError,
    ParseFromJSON,
    ParseFromParameter,
    ParseResult,
    ToJSON,
    Type,
};
use serde::de::Error;
use serde::Deserializer;
use serde_derive::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize)]
#[serde(transparent)]
/// Represents a unique index identifier, internally represented as a string.
///
/// An [IndexId] has some required validation prerequisites that must be met for it to
/// be considered valid:
///
/// - Be made up of characters alphanumerical, `-` and `_`.
/// - Be at least 3 characters in length.
/// - No longer than 256 characters in length.
pub struct IndexId(String);

impl IndexId {
    /// Attempts to create a new [IndexId] instance using the provided ID string.
    ///
    /// This will return an error in the event the ID does _not_ match the required
    /// syntax.
    pub fn try_from_string(id: String) -> Result<Self, IndexIdValidationError> {
        if id.len() < 3 {
            return Err(IndexIdValidationError::TooShort(id.len()));
        }

        if id.len() > 256 {
            return Err(IndexIdValidationError::TooLong(id.len()));
        }

        for char in id.chars() {
            if !char.is_alphanumeric() {
                return Err(IndexIdValidationError::IllegalCharacters(id));
            }
        }

        Ok(Self(id))
    }
}

impl Deref for IndexId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0.as_str()
    }
}

impl<'de> serde::Deserialize<'de> for IndexId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let id = String::deserialize(deserializer)?;
        Self::try_from_string(id).map_err(Error::custom)
    }
}

impl Type for IndexId {
    const IS_REQUIRED: bool = String::IS_REQUIRED;
    type RawValueType = String;
    type RawElementValueType = String;

    fn name() -> Cow<'static, str> {
        Cow::Borrowed("IndexId<String>")
    }

    fn schema_ref() -> MetaSchemaRef {
        String::schema_ref()
    }

    fn register(registry: &mut Registry) {
        String::register(registry)
    }

    fn as_raw_value(&self) -> Option<&Self::RawValueType> {
        Some(&self.0)
    }

    fn raw_element_iter<'a>(
        &'a self,
    ) -> Box<dyn Iterator<Item = &'a Self::RawElementValueType> + 'a> {
        Box::new(std::iter::once(&self.0))
    }
}

impl ToJSON for IndexId {
    fn to_json(&self) -> Option<Value> {
        Some(Value::String(self.0.clone()))
    }

    fn to_json_string(&self) -> String {
        serde_json::to_string(&self.0).unwrap()
    }
}

impl ParseFromJSON for IndexId {
    fn parse_from_json(value: Option<Value>) -> ParseResult<Self> {
        match value {
            Some(val) => serde_json::from_value(val).map_err(ParseError::custom),
            None => Err(ParseError::expected_type(Value::Null)),
        }
    }

    fn parse_from_json_string(s: &str) -> ParseResult<Self> {
        serde_json::from_str(s).map_err(ParseError::custom)
    }
}

impl ParseFromParameter for IndexId {
    fn parse_from_parameter(value: &str) -> ParseResult<Self> {
        serde_json::from_str(value).map_err(ParseError::custom)
    }
}

#[derive(Debug, thiserror::Error)]
/// An error that originates when attempting to validate a provided [IndexId] as a raw string.
pub enum IndexIdValidationError {
    #[error("provided ID is too long, expected upto 256 characters, got: {0}")]
    TooLong(usize),
    #[error("provided ID is too short, expected 3 or more characters, got: {0}")]
    TooShort(usize),
    #[error(
        "provided ID contains characters that are not alphanumerical, `-` or `_`: {0:?}"
    )]
    IllegalCharacters(String),
}
