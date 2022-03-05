mod convert;
mod serde;
#[allow(clippy::module_inception)]
mod value;

pub use convert::ConversionError;
pub use value::Value;
