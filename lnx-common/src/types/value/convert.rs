use std::convert::Infallible;
use std::error::Error;
use std::fmt;
use std::fmt::{Debug, Display};
use std::str::FromStr;

use tantivy::chrono::{NaiveDateTime, Utc};
use tantivy::fastfield::FastValue;
use tantivy::schema::Facet;
use tantivy::DateTime;

use super::Value;

#[derive(Debug)]
pub struct ConversionError {
    message: String,
}

impl ConversionError {
    fn new(from: &str, to: &str, v: impl Debug) -> Self {
        Self {
            message: format!(
                "cannot convert from type {:?} to {:?} for value {:?}",
                from, to, v
            ),
        }
    }
}

impl Display for ConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for ConversionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}

impl From<Infallible> for ConversionError {
    fn from(v: Infallible) -> Self {
        Self {
            message: v.to_string(),
        }
    }
}

// Actual conversion for the Value type.
//
// We try to be forgiving without being too forgiving and wildly
// converting types that really shouldn't be converted.
//
// E.g. We allow u64 -> f64 but we do not allow f64 -> u64.
impl TryInto<String> for Value {
    type Error = Infallible;

    fn try_into(self) -> Result<String, Self::Error> {
        Ok(self.to_string())
    }
}

impl TryInto<i64> for Value {
    type Error = ConversionError;

    fn try_into(self) -> Result<i64, Self::Error> {
        let v = match self {
            Value::I64(v) => v,
            Value::U64(v) => v
                .try_into()
                .map_err(|_| ConversionError::new("u64", "i64", v))?,
            Value::F64(v) => return Err(ConversionError::new("Float", "i64", v)),
            Value::DateTime(v) => v.timestamp(),
            Value::Text(v) => v
                .parse::<i64>()
                .map_err(|_| ConversionError::new("Text", "i64", v))?,
            Value::Bytes(v) => {
                return Err(ConversionError::new(
                    "Bytes",
                    "u64",
                    format!("<bytes len={}>", v.len()),
                ))
            },
        };

        Ok(v)
    }
}

impl TryInto<u64> for Value {
    type Error = ConversionError;

    fn try_into(self) -> Result<u64, Self::Error> {
        let v = match self {
            Value::I64(v) => v
                .try_into()
                .map_err(|_| ConversionError::new("i64", "u64", v))?,
            Value::U64(v) => v,
            Value::F64(v) => return Err(ConversionError::new("f64", "u64", v)),
            Value::DateTime(v) => v.as_u64(),
            Value::Text(v) => v
                .parse::<u64>()
                .map_err(|_| ConversionError::new("Text", "u64", v))?,
            Value::Bytes(v) => {
                return Err(ConversionError::new(
                    "Bytes",
                    "u64",
                    format!("<bytes len={}>", v.len()),
                ))
            },
        };

        Ok(v)
    }
}

impl TryInto<f64> for Value {
    type Error = ConversionError;

    fn try_into(self) -> Result<f64, Self::Error> {
        let v = match self {
            Value::I64(v) => v as f64,
            Value::U64(v) => v as f64,
            Value::F64(v) => v,
            Value::DateTime(v) => {
                return Err(ConversionError::new("DateTime", "f64", v))
            },
            Value::Text(v) => v
                .parse::<f64>()
                .map_err(|_| ConversionError::new("Text", "f64", v))?,
            Value::Bytes(v) => {
                return Err(ConversionError::new(
                    "Bytes",
                    "f64",
                    format!("<bytes len={}>", v.len()),
                ))
            },
        };

        Ok(v)
    }
}

impl TryInto<Facet> for Value {
    type Error = ConversionError;

    fn try_into(self) -> Result<Facet, Self::Error> {
        let err = match self {
            Value::I64(v) => ConversionError::new("i64", "Facet", v),
            Value::U64(v) => ConversionError::new("u64", "Facet", v),
            Value::F64(v) => ConversionError::new("f64", "Facet", v),
            Value::DateTime(v) => ConversionError::new("DateTime", "Facet", v),
            Value::Text(v) => {
                return Facet::from_text(&v)
                    .map_err(|_| ConversionError::new("Text", "Face", v))
            },
            Value::Bytes(v) => ConversionError::new(
                "Bytes",
                "Facet",
                format!("<bytes len={}>", v.len()),
            ),
        };

        Err(err)
    }
}

impl TryInto<DateTime> for Value {
    type Error = ConversionError;

    fn try_into(self) -> Result<DateTime, Self::Error> {
        let v = match self {
            Value::I64(v) => DateTime::from_utc(
                NaiveDateTime::from_timestamp_opt(v, 0)
                    .ok_or_else(|| ConversionError::new("i64", "DateTime", v))?,
                Utc,
            ),
            Value::U64(v) => DateTime::from_utc(
                NaiveDateTime::from_timestamp_opt(v as i64, 0)
                    .ok_or_else(|| ConversionError::new("i64", "DateTime", v))?,
                Utc,
            ),
            Value::F64(v) => return Err(ConversionError::new("f64", "DateTime", v)),
            Value::DateTime(v) => v.into_inner(),
            Value::Text(v) => DateTime::from_str(&v)
                .map_err(|_| ConversionError::new("Text", "DateTime", v))?,
            Value::Bytes(v) => {
                return Err(ConversionError::new(
                    "Bytes",
                    "DateTime",
                    format!("<bytes len={}>", v.len()),
                ))
            },
        };

        Ok(v)
    }
}

impl TryInto<Vec<u8>> for Value {
    type Error = ConversionError;

    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        let err = match self {
            Value::I64(v) => ConversionError::new("i64", "Bytes", v),
            Value::U64(v) => ConversionError::new("u64", "Bytes", v),
            Value::F64(v) => ConversionError::new("f64", "Bytes", v),
            Value::DateTime(v) => ConversionError::new("DateTime", "Bytes", v),
            Value::Text(v) => return Ok(v.as_bytes().to_vec()),
            Value::Bytes(v) => return Ok(v),
        };

        Err(err)
    }
}

impl TryInto<serde_json::Map<String, serde_json::Value>> for Value {
    type Error = ConversionError;

    fn try_into(self) -> Result<serde_json::Map<String, serde_json::Value>, Self::Error> {
        let err = match self {
            Value::I64(v) => ConversionError::new("i64", "JSON Object", v),
            Value::U64(v) => ConversionError::new("u64", "JSON Object", v),
            Value::F64(v) => ConversionError::new("f64", "JSON Object", v),
            Value::DateTime(v) => ConversionError::new("DateTime", "JSON Object", v),
            Value::Text(v) => ConversionError::new("Text", "JSON Object", v),
            Value::Bytes(v) => ConversionError::new("Bytes", "JSON Object", v),
        };

        Err(err)
    }
}