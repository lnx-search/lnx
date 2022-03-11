use thiserror::Error;

use crate::schema::validations::ValidationFailure;

#[derive(Debug, Error)]
pub enum SchemaError {
    #[error("at least one indexed field must be defined in the schema")]
    MissingIndexedFields,

    #[error(
        "{0} fields must be defined in the schema, got the following unknowns {0:?}"
    )]
    UnknownFields(&'static str, Vec<String>),

    #[error("the stored schema does not match the local copy of the index. Has the data changed?")]
    CorruptedSchema,
}

#[derive(Debug, Error)]
pub enum ConstraintViolation {
    #[error("Missing required field {0:?}.")]
    MissingRequiredField(String),

    #[error("Got `null` or `[]` instead of value of type {0} for field {1:?}")]
    UnExpectedNullValue(String, String),

    #[error(
        "The field {0:?} is not a multi-value field. Got {1} values but expected 1."
    )]
    TooManyValues(String, usize),

    #[error("The field {0:?} failed it's validation checks: {1}")]
    ValidatorError(String, ValidationFailure),
}
