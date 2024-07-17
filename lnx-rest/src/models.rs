use std::borrow::Cow;
use std::fmt::Display;
use std::io;
use std::marker::PhantomData;

use poem::http::StatusCode;
use poem::Response;
use poem_openapi::{ApiResponse, Object};
use poem_openapi::registry::{MetaResponses, MetaSchemaRef, Registry};
use poem_openapi::types::{ParseError, ParseFromJSON, ParseFromParameter, ParseResult, ToJSON, Type};
use serde_derive::Serialize;
use serde_json::Value;

/// An [IndexId] wrapper that can be deserialized by poem.
pub type WrappedIndexId = WrapSerde<(), String>;


#[derive(Debug, thiserror::Error, Object, Serialize)]
#[error("{message}")]
/// Represents a generic API error that can occur with standardized formatting.
pub struct ApiError {
    /// The error message detailing the cause of the error.
    message: String,
    /// The status code associated with the HTTP response.
    status: u16,
}

impl ApiError {
    /// Creates a new API error representing a bad request response.
    pub fn bad_request(msg: impl Display) -> Self {
        Self {
            message: msg.to_string(),
            status: StatusCode::BAD_REQUEST.as_u16(),
        }
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(value: anyhow::Error) -> Self {
        Self {
            message: value.to_string(),
            status: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
        }
    }
}

impl From<poem::Error> for ApiError {
    fn from(value: poem::Error) -> Self {
        Self {
            status: value.status().as_u16(),
            message: value.to_string()
        }
    }
}

impl From<io::Error> for ApiError {
    fn from(value: io::Error) -> Self {
        Self::from_any_error(value)
    }
}

impl ApiError {
    /// Creates a new [ApiError] instance from the given error that implements [std::error::Error].
    pub fn from_any_error(error: impl std::error::Error) -> Self {
        Self {
            message: error.to_string(),
            status: StatusCode::INTERNAL_SERVER_ERROR.as_u16()
        }
    }
}

impl poem::error::ResponseError for ApiError {
    fn status(&self) -> StatusCode {
        StatusCode::from_u16(self.status).unwrap()
    }

    fn as_response(&self) -> Response {
        let body = poem::Body::from_json(self)
            .unwrap_or_else(|_| poem::Body::from_string("Internal server error".to_owned()));
        Response::builder()
            .status(self.status())
            .body(body)
    }
}

impl ApiResponse for ApiError {
    const BAD_REQUEST_HANDLER: bool = true;

    fn meta() -> MetaResponses {
        poem::Error::meta()
    }

    fn register(registry: &mut Registry) {
        poem::Error::register(registry)
    }

    fn from_parse_request_error(err: poem::Error) -> Self {
        Self::from(err)
    }
}


/// Wraps a desired type `T` that supports serde (de)serialization.
///
/// The outer type `O` indicates the type that should represent the desired type
/// in the OpenAPI spec, this defaults to `T`.
pub struct WrapSerde<T, O = T> {
    inner: T,
    phantom: PhantomData<O>,
}

impl<T, O> Type for WrapSerde<T, O>
where
    O: Type,
    T: Send + Sync,
{
    const IS_REQUIRED: bool = O::IS_REQUIRED;
    type RawValueType = T;
    type RawElementValueType = T;

    fn name() -> Cow<'static, str> {
        O::name()
    }

    fn schema_ref() -> MetaSchemaRef {
        O::schema_ref()
    }

    fn register(registry: &mut Registry) {
        O::register(registry)
    }

    fn as_raw_value(&self) -> Option<&Self::RawValueType> {
        Some(&self.inner)
    }

    fn raw_element_iter<'a>(&'a self) -> Box<dyn Iterator<Item=&'a Self::RawElementValueType> + 'a> {
        Box::new(std::iter::once(&self.inner))
    }
}

impl<T, O> ToJSON for WrapSerde<T, O>
where
    T: serde::Serialize + Send + Sync,
    O: Type,
{
    fn to_json(&self) -> Option<Value> {
        todo!()
    }

    fn to_json_string(&self) -> String {
        todo!()
    }
}

impl<T, O> ParseFromJSON for WrapSerde<T, O>
where
    T: serde::de::DeserializeOwned + Send + Sync,
    O: Type,
{
    fn parse_from_json(value: Option<Value>) -> ParseResult<Self> {
        let value = value
            .ok_or_else(|| ParseError::custom("Unable to deserialize `null` value"))?;
        let result = serde_json::from_value(value)
            .map_err(|e| ParseError::custom(
                format!("Type can not be deserialized from JSON payload due to error {e}")
            ))?;
        Ok(Self { inner: result, phantom: PhantomData })
    }

    fn parse_from_json_string(s: &str) -> ParseResult<Self> {
        let result = serde_json::from_str(s)
            .map_err(ParseError::custom)?;
        Ok(Self { inner: result, phantom: PhantomData })
    }
}
impl<T, O> ParseFromParameter for WrapSerde<T, O>
where
    T: serde::de::DeserializeOwned + Send + Sync,
    O: Type,
{
    fn parse_from_parameter(value: &str) -> ParseResult<Self> {
        let result = serde_json::from_str(value)
            .map_err(|e| ParseError::custom(
                format!("Type can not be deserialized from query parameter due to error {e}")
            ))?;
        Ok(Self { inner: result, phantom: PhantomData })
    }
}