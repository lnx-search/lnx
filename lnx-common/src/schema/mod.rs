#[allow(clippy::module_inception)]
mod schema;
mod field_name;
mod field_info;
mod options;
mod boost;
mod error;

pub use schema::Schema;
pub use options::{BaseOptions, BytesOptions, CalculatedIntOptions};
pub use field_name::FieldName;
pub use field_info::FieldInfo;
pub use boost::BoostFactor;
pub use error::SchemaError;