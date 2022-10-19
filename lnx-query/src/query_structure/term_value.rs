use std::fmt;

use serde::{Deserialize, Deserializer, de::Visitor};
use tantivy::{Term, schema::{FieldEntry, Field, FieldType}, DateTime};
use time::{format_description::well_known::Rfc3339};
use utoipa::ToSchema;

use super::{AsQueryTerm, base::InvalidTermValue};

#[derive(Debug, Clone, ToSchema)]
pub enum TermValue {    
    #[schema(example = "Hello, world")]
    /// Plain UTF-8 encoded string.
    Text(String),

    #[schema(example = json!(123))]
    /// A unsigned 64 bit integer (positive integer)
    U64(u64),

    #[schema(example = json!(-123))]
    /// A unsigned 64 bit integer (positive and negative integer)
    I64(i64),

    #[schema(example = json!(1.23))]
    /// A 64 bit float.
    F64(f64),
}

impl AsQueryTerm for TermValue {
    fn as_term(&self, field: Field, entry: &FieldEntry) -> Result<Term, InvalidTermValue> {
        match entry.field_type() {
            FieldType::Str(_) => {
                let val: String = self.clone().try_into()?;
                Ok(Term::from_field_text(field, &val))
            },
            FieldType::U64(_) => {
                let val: u64 = self.clone().try_into()?;
                Ok(Term::from_field_u64(field, val))

            },
            FieldType::I64(_) => {
                let val: i64 = self.clone().try_into()?;
                Ok(Term::from_field_i64(field, val))
                
            },
            FieldType::F64(_) => {
                let val: f64 = self.clone().try_into()?;
                Ok(Term::from_field_f64(field, val))
                
            },
            FieldType::Facet(_) => {
                match self {
                    Self::U64(_) => Err(InvalidTermValue("invalid type expected 'str' got 'f64'.".to_string())),
                    Self::I64(_) => Err(InvalidTermValue("invalid type expected 'str' got 'i64'.".to_string())),
                    Self::F64(_) => Err(InvalidTermValue("invalid type expected 'str' got 'u64'.".to_string())),
                    Self::Text(val) => Ok(Term::from_field_text(field, val)),
                }                
            },
            FieldType::JsonObject(_) => {
                let field = match self {
                    Self::U64(val) => Term::from_field_u64(field, *val),
                    Self::I64(val) => Term::from_field_i64(field, *val),
                    Self::F64(val) => Term::from_field_f64(field, *val),
                    Self::Text(val) => Term::from_field_text(field, val),
                };
                
                Ok(field)
            },
            FieldType::Bytes(_) => {
                let val: Vec<u8> = self.clone().try_into()?;
                Ok(Term::from_field_bytes(field, &val))
            },
            FieldType::Date(_) => {
                let val: DateTime = self.clone().try_into()?;
                Ok(Term::from_field_date(field, val))                
            },
            _ => unimplemented!("Unsupported field types for conversion."),
        }
    }
}

impl<'de> Deserialize<'de> for TermValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ValueVisitor;

        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = TermValue;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a string, integer or float")
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E> {
                Ok(TermValue::I64(v))
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> {
                Ok(TermValue::U64(v))
            }

            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E> {
                Ok(TermValue::F64(v))
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(TermValue::Text(v.to_string()))
            }

            fn visit_string<E: serde::de::Error>(self, v: String) -> Result<Self::Value, E> {
                Ok(TermValue::Text(v))
            }
        }

        deserializer.deserialize_any(ValueVisitor)
    }
}

impl TryInto<u64> for TermValue {
    type Error = InvalidTermValue;

    fn try_into(self) -> Result<u64, Self::Error> {
        match self {
            Self::U64(val) => Ok(val),
            Self::I64(val) => val.try_into().map_err(InvalidTermValue::from),
            Self::F64(_) => Err(InvalidTermValue::from("cannot convert type 'f64' to 'u64'.")),
            Self::Text(val) => val.parse().map_err(InvalidTermValue::from),
        }
    }
}

impl TryInto<i64> for TermValue {
    type Error = InvalidTermValue;

    fn try_into(self) -> Result<i64, Self::Error> {
        match self {
            Self::U64(val) => val.try_into().map_err(InvalidTermValue::from),
            Self::I64(val) => Ok(val),
            Self::F64(_) => Err(InvalidTermValue::from("cannot convert type 'f64' to 'i64'.")),
            Self::Text(val) => val.parse().map_err(InvalidTermValue::from),
        }
    }
}

impl TryInto<f64> for TermValue {
    type Error = InvalidTermValue;

    fn try_into(self) -> Result<f64, Self::Error> {
        match self {
            Self::U64(_) => Err(InvalidTermValue::from("cannot convert type 'u64' to 'f64'.")),
            Self::I64(_) => Err(InvalidTermValue::from("cannot convert type 'i64' to 'f64'.")),
            Self::F64(val) => Ok(val),
            Self::Text(val) => val.parse::<f64>().map_err(InvalidTermValue::from),
        }
    }
}

impl TryInto<String> for TermValue {
    type Error = InvalidTermValue;

    fn try_into(self) -> Result<String, Self::Error> {
        match self {
            Self::U64(val) => Ok(val.to_string()),
            Self::I64(val) => Ok(val.to_string()),
            Self::F64(val) => Ok(val.to_string()),
            Self::Text(val) => Ok(val),
        }
    }
}

impl TryInto<Vec<u8>> for TermValue {
    type Error = InvalidTermValue;

    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        match self {
            Self::U64(_) => Err(InvalidTermValue::from("cannot convert type 'u64' to 'bytes'.")),
            Self::I64(_) => Err(InvalidTermValue::from("cannot convert type 'i64' to 'bytes'.")),
            Self::F64(_) => Err(InvalidTermValue::from("cannot convert type 'f64' to 'bytes'.")),
            Self::Text(val) => {
                base64::decode(val)
                    .map_err(|_| InvalidTermValue::from("invalid base64 encoded string given for 'bytes' field."))
            },
        }
    }
}

impl TryInto<DateTime> for TermValue {
    type Error = InvalidTermValue;

    fn try_into(self) -> Result<DateTime, Self::Error> {
        match self {
            Self::U64(val) => {
                let val: i64 = val.try_into().map_err(InvalidTermValue::from)?;
                let dt = time::OffsetDateTime::from_unix_timestamp(val)
                    .map_err(InvalidTermValue::from)?;
                
                Ok(tantivy::DateTime::from_utc(dt))
            },
            Self::I64(val) => {
                let dt = time::OffsetDateTime::from_unix_timestamp(val)
                    .map_err(InvalidTermValue::from)?;

                Ok(tantivy::DateTime::from_utc(dt))
            },
            Self::F64(_) => Err(InvalidTermValue::from("cannot convert type 'f64' to 'datetime'.")),
            Self::Text(val) => {
                let dt = time::OffsetDateTime::parse(&val, &Rfc3339)
                    .map_err(InvalidTermValue::from)?;

                Ok(tantivy::DateTime::from_utc(dt))
            },
        }
    }
}