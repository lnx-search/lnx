mod boost;
mod error;
mod field_info;
mod field_name;
mod options;
#[allow(clippy::module_inception)]
mod schema;
mod validations;

pub use boost::BoostFactor;
pub use error::{ConstraintViolation, SchemaError};
pub use field_info::FieldInfo;
pub use field_name::FieldName;
pub use options::{BaseOptions, BytesOptions, CalculatedIntOptions};
pub use schema::Schema;

pub static INDEX_PK: &str = "lnx_doc_id";
pub static SEGMENT_KEY: &str = "lnx_segment_id";
