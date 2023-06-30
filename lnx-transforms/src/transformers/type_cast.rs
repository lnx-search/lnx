use lnx_document::{UserDisplayType, Value};

use crate::pipeline::{Transform, TransformError};
use crate::TypeCast;

/// A type casting transformer.
///
/// This will attempt to cast a value to a given
/// desired type if it can be performed correctly.
pub struct TypeCastTransformer {
    cast: TypeCast,
}

impl TypeCastTransformer {
    /// Create a new type cast transformer.
    pub fn new(cast: TypeCast) -> Self {
        Self { cast }
    }
}

impl Transform for TypeCastTransformer {
    fn expecting(&self, type_name: &str) -> TransformError {
        TransformError::new(format!("Cannot cast `{type_name}` to `{}`", self.cast.type_name()))
    }

    #[inline]
    fn transform<'a>(&self, value: Value<'a>) -> Result<Value<'a>, TransformError> {
        self.cast
            .try_cast_value(value)
            .map_err(|e| TransformError::new(e.to_string()))
    }
}