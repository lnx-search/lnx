use std::borrow::Cow;
use std::net::Ipv6Addr;

use hashbrown::HashMap;
use lnx_document::{DateTime, DynamicDocument, KeyValues, UserDisplayType, Value};
use smallvec::SmallVec;


/// A transform pipeline applies multiple transform operations on a object.
pub struct TransformPipeline {
    pipeline_name: String,
    stages: HashMap<String, SmallVec<[Box<dyn Transform>; 2]>>,
}

impl TransformPipeline {
    /// Create a new transform pipeline with a given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            pipeline_name: name.into(),
            stages: HashMap::new(),
        }
    }

    /// Adds a new stage for a given key in the pipeline.
    pub fn add_stage<T: Transform>(&mut self, key: String, transform: T) {
        self.stages
            .entry(key)
            .or_default()
            .push(Box::new(transform))
    }

    #[inline]
    /// Apply the transform pipeline to a given document.
    pub fn transform_doc<'a>(&self, document: DynamicDocument<'a>) -> Result<DynamicDocument<'a>, TransformError> {
        self.transform_key_values(document.0).map(DynamicDocument)
    }

    #[inline]
    /// Apply the transform pipeline to a given set of key-value pairs.
    pub fn transform_key_values<'a>(&self, object: KeyValues<'a>) -> Result<KeyValues<'a>, TransformError> {
        object
            .into_iter()
            .map(|(key, mut value)| {
                if let Some(transformers) = self.stages.get(key.as_ref()) {
                    for transformer in transformers {
                        value = transformer
                            .transform(value)
                            .with_context_key(|| key.to_string())?;
                    }
                }
                Ok((key, value))
            })
            .collect::<Result<KeyValues, TransformError>>()
    }
}

impl Transform for TransformPipeline {
    fn expecting(&self, type_name: &str) -> TransformError {
        let msg = format!(
            "Cannot transform `{type_name}` as it is not an object, \
            required by the {:?} transform pipeline stage",
            self.pipeline_name,
        );

        TransformError::new(msg)
    }

    #[inline]
    fn transform_object<'a>(&self, object: KeyValues<'a>) -> Result<Value<'a>, TransformError> {
        self.transform_key_values(object).map(Value::Object)
    }
}


/// Defines a transform operation.
///
/// By default this performs no transformations and returns an error produced
/// by [`Transform::expecting`], methods can be overriden in order to modify this
/// default behaviour.
pub trait Transform: Send + 'static {
    /// The error message to display when a type cannot be transformed
    /// by the implementor.
    fn expecting(&self, type_name: &str) -> TransformError;

    #[inline]
    /// Applies the transformation to a dynamic value, returning the modified
    /// value or a [TransformError].
    fn transform<'a>(&self, value: Value<'a>) -> Result<Value<'a>, TransformError> {
        match value {
            Value::Null => self.transform_null(),
            Value::Str(value) => self.transform_str(value),
            Value::U64(value) => self.transform_u64(value),
            Value::I64(value) => self.transform_i64(value),
            Value::F64(value) => self.transform_f64(value),
            Value::Bool(value) => self.transform_bool(value),
            Value::Facet(value) => self.transform_facet(value),
            Value::DateTime(value) => self.transform_datetime(value),
            Value::IpAddr(value) => self.transform_ip(value),
            Value::Bytes(value) => self.transform_bytes(value),
            Value::Array(elements) => self.transform_array(elements),
            Value::Object(object) => self.transform_object(object),
        }
    }

    #[inline]
    /// Applies the transform to a `null` value.
    fn transform_null<'a>(&self) -> anyhow::Result<Value<'a>, TransformError> {
        Err(self.expecting(&Value::Null.type_name()))
    }

    #[inline]
    /// Applies the transform to a `string` value.
    fn transform_str<'a>(&self, value: Cow<'a, str>) -> Result<Value<'a>, TransformError> {
        Err(self.expecting(&value.type_name()))
    }

    #[inline]
    /// Applies the transform to a `u64` value.
    fn transform_u64<'a>(&self, value: u64) -> Result<Value<'a>, TransformError> {
        Err(self.expecting(&value.type_name()))
    }

    #[inline]
    /// Applies the transform to a `i64` value.
    fn transform_i64<'a>(&self, value: i64) -> Result<Value<'a>, TransformError> {
        Err(self.expecting(&value.type_name()))
    }

    #[inline]
    /// Applies the transform to a `f64` value.
    fn transform_f64<'a>(&self, value: f64) -> Result<Value<'a>, TransformError> {
        Err(self.expecting(&value.type_name()))
    }

    #[inline]
    /// Applies the transform to a `bool` value.
    fn transform_bool<'a>(&self, value: bool) -> Result<Value<'a>, TransformError> {
        Err(self.expecting(&value.type_name()))
    }

    #[inline]
    /// Applies the transform to a `facet` value.
    fn transform_facet<'a>(&self, value: tantivy::schema::Facet) -> Result<Value<'a>, TransformError> {
        Err(self.expecting(&value.type_name()))
    }

    #[inline]
    /// Applies the transform to a `datetime` value.
    fn transform_datetime<'a>(&self, value: DateTime) -> Result<Value<'a>, TransformError> {
        Err(self.expecting(&value.type_name()))
    }

    #[inline]
    /// Applies the transform to a `bytes` value.
    fn transform_bytes<'a>(&self, value: Vec<u8>) -> Result<Value<'a>, TransformError> {
        Err(self.expecting(&value.type_name()))
    }

    #[inline]
    /// Applies the transform to a `ip` value.
    fn transform_ip<'a>(&self, value: Ipv6Addr) -> Result<Value<'a>, TransformError> {
        Err(self.expecting(&value.type_name()))
    }

    #[inline]
    /// Applies the transform to an array of values.
    ///
    /// By default this loops over the array and applies the transform to each
    /// array element, which may or may not be desired.
    fn transform_array<'a>(
        &self,
        elements: Vec<Value<'a>>,
    ) -> Result<Value<'a>, TransformError> {
        // This implementation should allow the compiler to re-use the existing allocation.
        elements
            .into_iter()
            .map(|v| self.transform(v))
            .collect::<Result<Vec<Value>, TransformError>>()
            .map(Value::Array)
    }

    #[inline]
    /// Applies the transform to an object.
    fn transform_object<'a>(&self, object: KeyValues<'a>) -> Result<Value<'a>, TransformError> {
        Err(self.expecting(&object.type_name()))
    }
}


#[derive(Debug, thiserror::Error)]
#[error("Transform Error originating from field path: {context_keys:?}: {message}")]
pub struct TransformError {
    message: String,
    context_keys: Vec<String>,
}

impl TransformError {
    /// Creates a new transform error with a given error message.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            context_keys: Vec::new(),
        }
    }

    #[inline]
    /// Gets a reference to the error message.
    pub fn message(&self) -> &str {
        self.message.as_str()
    }

    #[inline]
    /// Gets a reference to effective object keys stack trace.
    pub fn context_keys(&self) -> &[String] {
        &self.context_keys
    }
}

impl TransformErrorContext for TransformError {
    fn with_context_key<CB>(mut self, ctx: CB) -> Self
    where
        CB: FnOnce() -> String
    {
        self.context_keys.push(ctx());
        self
    }
}

pub trait TransformErrorContext {
    fn with_context_key<CB>(self, ctx: CB) -> Self
    where
        CB: FnOnce() -> String;
}

impl<T, E> TransformErrorContext for Result<T, E>
where
    E: TransformErrorContext
{
    fn with_context_key<CB>(self, ctx: CB) -> Self
    where
        CB: FnOnce() -> String
    {
        match self {
            Ok(v) => Ok(v),
            Err(e) => Err(e.with_context_key(ctx))
        }
    }
}