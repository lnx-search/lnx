use std::ops::Deref;
use regex::Regex;
use serde::{Serialize, Deserialize, Serializer, Deserializer};
use serde::de::Error;
use crate::types::DateTime;


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
            container_validations: ContainerLengthValidations,
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
    container_validations: ContainerLengthValidations,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StandardValidations {
    /// The maximum length a given string/bytes/facet value can be.
    pub max_length: Option<usize>,

    /// The minimum length a given string/bytes/facet value can be.
    pub min_length: Option<usize>,

    #[serde(flatten)]
    container_validations: ContainerLengthValidations,
}


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ContainerLengthValidations {
    /// The maximum number of items a multi-value field can contain.
    pub max_container_length: Option<usize>,

    /// The minimum number of items a multi-value field can contain.
    pub min_container_length: Option<usize>,
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