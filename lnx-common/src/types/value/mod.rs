mod convert;
mod serde;
#[allow(clippy::module_inception)]
mod value;

pub use value::Value;
pub use convert::ConversionError;
