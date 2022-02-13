use thiserror::Error;

#[derive(Debug, Error)]
pub enum SchemaError {
    #[error("at least one indexed field must be defined in the schema")]
    MissingIndexedFields,

    #[error(
        "{0} fields must be defined in the schema, got the following unknowns {0:?}"
    )]
    UnknownFields(&'static str, Vec<String>),
}
