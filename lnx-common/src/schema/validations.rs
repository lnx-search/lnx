use std::cmp::min;
use std::ops::Deref;
use regex::Regex;
use thiserror::Error;
use serde::{Serialize, Deserialize, Serializer, Deserializer};
use serde::de::Error;
use crate::types::document::DocField;


pub trait FieldValidator {
    fn validate(&self, field: &DocField) -> Option<ValidationFailure>;
}

#[derive(Debug, Error)]
pub enum ValidationFailure {
    #[error("The numeric value is beyond the constraints range {0}, got {1}")]
    OutOfRange(String, String),

    #[error("Too many items in the container, expected at most {0}, got {1}")]
    TooManyItems(usize, usize),

    #[error("Too few items in the container, expected a minimum of {0}, got {1}")]
    TooFewItems(usize, usize),

    #[error("The value {0:?} did not match the regex: {1:?}")]
    NoRegexMatch(String, String),
}


macro_rules! numeric_validation {
    ($name:ident, $tp:ty) => {
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
        pub struct $name {
            /// The field must be greater than this size.
            pub gt: Option<$tp>,

            /// The field must be greater than or equal to this size.
            pub ge: Option<$tp>,

            /// The field must be less than this size.
            pub lt: Option<$tp>,

            /// The field must be less than or equal to this size.
            pub le: Option<$tp>,

            #[serde(flatten)]
            pub container_validations: ContainerLengthValidations,
        }
    }
}

numeric_validation!(F64Validations, f64);
numeric_validation!(U64Validations, u64);
numeric_validation!(I64Validations, i64);


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TextValidations {
    /// The value must align with the given regex.
    pub regex: Option<RegexValidator>,

    /// The maximum length a given string/bytes/facet value can be.
    pub max_length: Option<usize>,

    /// The minimum length a given string/bytes/facet value can be.
    pub min_length: Option<usize>,

    #[serde(flatten)]
    pub container_validations: ContainerLengthValidations,
}

impl TextValidations {
    fn as_range(&self) -> String {
        match (self.min_length, self.max_length) {
            (None, None) => "len".to_string(),
            (Some(min), None) => format!("{} <= len", min),
            (None, Some(max)) => format!("len <= {}", max),
            (Some(min), Some(max)) => format!("{} <= len <= {}", min, max),
        }
    }
}

impl FieldValidator for TextValidations {
    fn validate(&self, field: &DocField) -> Option<ValidationFailure> {
        if let Some(min_container_length) = self.container_validations.min_length {
            if field.len() < min_container_length {
                return Some(ValidationFailure::TooFewItems(min_container_length, field.len()))
            }
        }

        if let Some(max_container_length) = self.container_validations.max_length {
            if field.len() < max_container_length {
                return Some(ValidationFailure::TooManyItems(max_container_length, field.len()))
            }
        }

        for value in field.to_multi() {
            let text = match value.as_text() {
                None => continue,
                Some(text) => text,
            };

            if let Some(min_length) = self.min_length {
                if text.len() < min_length {
                    return Some(ValidationFailure::OutOfRange(self.as_range(), text.len().to_string()))
                }
            }

            if let Some(max_length) = self.max_length {
                if text.len() < max_length {
                    return Some(ValidationFailure::OutOfRange(self.as_range(), text.len().to_string()))
                }
            }
        }


        todo!()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StandardValidations {
    /// The maximum length a given string/bytes/facet value can be.
    pub max_length: Option<usize>,

    /// The minimum length a given string/bytes/facet value can be.
    pub min_length: Option<usize>,

    #[serde(flatten)]
    pub container_validations: ContainerLengthValidations,
}


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ContainerLengthValidations {
    /// The maximum number of items a multi-value field can contain.
    pub max_length: Option<usize>,

    /// The minimum number of items a multi-value field can contain.
    pub min_length: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct RegexValidator(Regex);

impl Deref for RegexValidator {
    type Target = Regex;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq for RegexValidator {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_str() == other.0.as_str()
    }
}

impl Serialize for RegexValidator {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        self.0.as_str().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RegexValidator {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error> where D: Deserializer<'de> {
        let pattern = String::deserialize(deserializer)?;
        let re = Regex::new(&pattern).map_err(D::Error::custom)?;
        Ok(Self(re))
    }
}

mod helpers {
    use std::fmt::Display;

    #[inline]
    pub fn format_range<T1: Display, T2: Display>(
        upper: T1,
        lower: T2,
        upper_eq: bool,
        lower_eq: bool,
    ) -> String {
        format!(
            "{upper}{ueq}..{leq}{lower}",
            upper = upper,
            lower = lower,
            ueq = if upper_eq { "=" } else { "" },
            leq = if lower_eq { "=" } else { "" },
        )
    }
}