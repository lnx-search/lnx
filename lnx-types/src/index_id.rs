use std::ops::Deref;

use serde::de::Error;
use serde::Deserializer;
use serde_derive::Serialize;

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
