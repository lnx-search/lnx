use std::cmp::min;
use std::fmt::{Display, Formatter};
use std::ops::Deref;

use regex::Regex;
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

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

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum LowerBound<T> {
    /// The field must be greater than this size.
    Gt(T),

    /// The field must be greater than or equal to this size.
    Ge(T),

    /// There is no lower bound
    None,
}

impl<T> Default for LowerBound<T> {
    fn default() -> Self {
        Self::None
    }
}

impl<T: PartialOrd> LowerBound<T> {
    fn is_ok(&self, v: T) -> bool {
        match self {
            Self::Gt(cmp) => v > *cmp,
            Self::Ge(cmp) => v >= *cmp,
            Self::None => true,
        }
    }
}

impl<T: Display> Display for LowerBound<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Gt(v) => write!(f, "{} <", v),
            Self::Ge(v) => write!(f, "{} <=", v),
            Self::None => write!(f, ""),
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum UpperBound<T> {
    /// The field must be less than this size.
    Lt(T),

    /// The field must be less than or equal to this size.
    Le(T),

    /// There is no upper bound
    None,
}

impl<T> Default for UpperBound<T> {
    fn default() -> Self {
        Self::None
    }
}

impl<T: PartialOrd> UpperBound<T> {
    fn is_ok(&self, v: T) -> bool {
        match self {
            Self::Lt(cmp) => v < *cmp,
            Self::Le(cmp) => v <= *cmp,
            Self::None => true,
        }
    }
}

impl<T: Display> Display for UpperBound<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Lt(v) => write!(f, "< {}", v),
            Self::Le(v) => write!(f, "<= {}", v),
            Self::None => write!(f, ""),
        }
    }
}

macro_rules! numeric_validation {
    ($name:ident, $tp:ty, $transformer:ident) => {
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
        pub struct $name {
            #[serde(flatten, default)]
            pub lower: LowerBound<$tp>,

            #[serde(flatten, default)]
            pub upper: UpperBound<$tp>,

            #[serde(flatten)]
            pub container_validations: ContainerLengthValidations,
        }

        impl $name {
            fn as_range(&self) -> String {
                format!("{} value {}", self.lower, self.upper)
            }
        }

        impl FieldValidator for $name {
            fn validate(&self, field: &DocField) -> Option<ValidationFailure> {
                if let Some(fail) = self.container_validations.validate(field) {
                    return Some(fail);
                }

                for value in field.to_multi() {
                    let v = match value.$transformer() {
                        None => continue,
                        Some(v) => v,
                    };

                    if !self.lower.is_ok(*v) {
                        return Some(ValidationFailure::OutOfRange(
                            self.as_range(),
                            v.to_string(),
                        ));
                    }

                    if !self.upper.is_ok(*v) {
                        return Some(ValidationFailure::OutOfRange(
                            self.as_range(),
                            v.to_string(),
                        ));
                    }
                }

                None
            }
        }
    };
}

numeric_validation!(F64Validations, f64, as_f64);
numeric_validation!(U64Validations, u64, as_u64);
numeric_validation!(I64Validations, i64, as_i64);

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
        helpers::as_range(self.min_length, self.max_length)
    }
}

impl FieldValidator for TextValidations {
    fn validate(&self, field: &DocField) -> Option<ValidationFailure> {
        if let Some(fail) = self.container_validations.validate(field) {
            return Some(fail);
        }

        for value in field.to_multi() {
            let text = match value.as_text() {
                None => continue,
                Some(text) => text,
            };

            if let Some(min_length) = self.min_length {
                if text.len() < min_length {
                    return Some(ValidationFailure::OutOfRange(
                        self.as_range(),
                        text.len().to_string(),
                    ));
                }
            }

            if let Some(max_length) = self.max_length {
                if text.len() < max_length {
                    return Some(ValidationFailure::OutOfRange(
                        self.as_range(),
                        text.len().to_string(),
                    ));
                }
            }

            if let Some(re) = &self.regex {
                if !re.is_match(text) {
                    return Some(ValidationFailure::NoRegexMatch(
                        text.to_string(),
                        re.as_str().to_string(),
                    ));
                }
            }
        }

        None
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

impl StandardValidations {
    fn as_range(&self) -> String {
        helpers::as_range(self.min_length, self.max_length)
    }
}

impl FieldValidator for StandardValidations {
    fn validate(&self, field: &DocField) -> Option<ValidationFailure> {
        if let Some(fail) = self.container_validations.validate(field) {
            return Some(fail);
        }

        for value in field.to_multi() {
            let text = match value.as_text() {
                None => continue,
                Some(text) => text,
            };

            if let Some(min_length) = self.min_length {
                if text.len() < min_length {
                    return Some(ValidationFailure::OutOfRange(
                        self.as_range(),
                        text.len().to_string(),
                    ));
                }
            }

            if let Some(max_length) = self.max_length {
                if text.len() < max_length {
                    return Some(ValidationFailure::OutOfRange(
                        self.as_range(),
                        text.len().to_string(),
                    ));
                }
            }
        }

        None
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ContainerLengthValidations {
    /// The maximum number of items a multi-value field can contain.
    pub max_length: Option<usize>,

    /// The minimum number of items a multi-value field can contain.
    pub min_length: Option<usize>,
}

impl FieldValidator for ContainerLengthValidations {
    fn validate(&self, field: &DocField) -> Option<ValidationFailure> {
        if let Some(min_container_length) = self.min_length {
            if field.len() < min_container_length {
                return Some(ValidationFailure::TooFewItems(
                    min_container_length,
                    field.len(),
                ));
            }
        }

        if let Some(max_container_length) = self.max_length {
            if field.len() < max_container_length {
                return Some(ValidationFailure::TooManyItems(
                    max_container_length,
                    field.len(),
                ));
            }
        }

        None
    }
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
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.as_str().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RegexValidator {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let pattern = String::deserialize(deserializer)?;
        let re = Regex::new(&pattern).map_err(D::Error::custom)?;
        Ok(Self(re))
    }
}

mod helpers {
    use std::fmt::Display;

    #[inline]
    pub fn as_range<T1: Display, T2: Display>(
        min: Option<T1>,
        max: Option<T2>,
    ) -> String {
        match (min, max) {
            (None, None) => "len".to_string(),
            (Some(min), None) => format!("{} <= len", min),
            (None, Some(max)) => format!("len <= {}", max),
            (Some(min), Some(max)) => format!("{} <= len <= {}", min, max),
        }
    }
}
