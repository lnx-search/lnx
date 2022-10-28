use std::fmt;
use std::ops::Bound;

use serde::de::Visitor;
use serde::{Deserialize, Deserializer};
use tantivy::query::RangeQuery;
use tantivy::schema::{Field, FieldEntry};
use tantivy::{DateTime, Term};
use time::format_description::well_known::Rfc3339;
use utoipa::ToSchema;

use super::base::{AsQuery, InvalidTermValue};
use super::AsQueryTerm;

#[derive(Debug, Clone, Copy, Deserialize, ToSchema)]
/// The required values / context for a `range` query.
pub struct Range {
    #[schema(inline)]
    #[serde(flatten)]
    /// The gt/ge bounds.
    ///
    /// These are grouped together because they are mutually exclusive.
    pub greater_bound: Option<GreaterBounds>,

    #[schema(inline)]
    #[serde(flatten)]
    /// The lt/le bounds.
    ///
    /// These are grouped together because they are mutually exclusive.
    pub lesser_bounds: Option<LesserBounds>,
}

impl AsQuery for Range {
    fn as_query(
        &self,
        field: Field,
        entry: &FieldEntry,
    ) -> Result<Box<dyn tantivy::query::Query>, InvalidTermValue> {
        let left_bound = self
            .greater_bound
            .as_ref()
            .map(|v| v.as_bound(field, entry))
            .unwrap_or(Ok(Bound::Unbounded))?;

        let right_bound = self
            .lesser_bounds
            .as_ref()
            .map(|v| v.as_bound(field, entry))
            .unwrap_or(Ok(Bound::Unbounded))?;

        let value_type = entry.field_type().value_type();
        let qry =
            RangeQuery::new_term_bounds(field, value_type, &left_bound, &right_bound);

        Ok(Box::new(qry))
    }
}

#[derive(Debug, Clone, Copy, Deserialize, ToSchema)]
pub enum GreaterBounds {
    #[serde(rename = "$gt")]
    /// Greater than (`>`)
    Gt(RangeValue),

    #[serde(rename = "$ge")]
    /// Greater than or equal to (`>=`)
    Ge(RangeValue),
}

impl GreaterBounds {
    fn as_bound(
        &self,
        field: Field,
        entry: &FieldEntry,
    ) -> Result<Bound<Term>, InvalidTermValue> {
        match self {
            GreaterBounds::Gt(val) => {
                let term = val.as_term(field, entry)?;
                Ok(Bound::Excluded(term))
            },
            GreaterBounds::Ge(val) => {
                let term = val.as_term(field, entry)?;
                Ok(Bound::Included(term))
            },
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, ToSchema)]
pub enum LesserBounds {
    #[serde(rename = "$lt")]
    /// Less than (`<`)
    Lt(RangeValue),

    #[serde(rename = "$le")]
    /// Less than or equal to (`<=`)
    Le(RangeValue),
}

impl LesserBounds {
    fn as_bound(
        &self,
        field: Field,
        entry: &FieldEntry,
    ) -> Result<Bound<Term>, InvalidTermValue> {
        match self {
            LesserBounds::Lt(val) => {
                let term = val.as_term(field, entry)?;
                Ok(Bound::Excluded(term))
            },
            LesserBounds::Le(val) => {
                let term = val.as_term(field, entry)?;
                Ok(Bound::Included(term))
            },
        }
    }
}

#[derive(Debug, ToSchema, Clone, Copy)]
pub enum RangeValue {
    #[schema(example = json!(123))]
    /// A unsigned 64 bit integer (positive integer)
    U64(u64),

    #[schema(example = json!(-123))]
    /// A unsigned 64 bit integer (positive and negative integer)
    I64(i64),

    #[schema(example = json!(1.23))]
    /// A 64 bit float.
    F64(f64),

    #[schema(example = "2002-10-02T15:00:00.05Z")]
    /// A date-time value.
    ///
    /// This expects to be in the format of a `RFC 3339` formatted date time
    /// string which is assumed to be in the UTC timezone.
    DateTime(DateTime),
}

impl AsQueryTerm for RangeValue {
    fn as_term(
        &self,
        field: Field,
        _entry: &FieldEntry,
    ) -> Result<Term, InvalidTermValue> {
        let term = match *self {
            RangeValue::U64(val) => Term::from_field_u64(field, val),
            RangeValue::I64(val) => Term::from_field_i64(field, val),
            RangeValue::F64(val) => Term::from_field_f64(field, val),
            RangeValue::DateTime(val) => Term::from_field_date(field, val),
        };

        Ok(term)
    }
}

impl<'de> Deserialize<'de> for RangeValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ValueVisitor;

        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = RangeValue;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a string, integer or float")
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E> {
                Ok(RangeValue::I64(v))
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> {
                Ok(RangeValue::U64(v))
            }

            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E> {
                Ok(RangeValue::F64(v))
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
                if let Ok(dt) = time::OffsetDateTime::parse(v, &Rfc3339) {
                    return Ok(RangeValue::DateTime(tantivy::DateTime::from_utc(dt)));
                }

                if let Ok(int) = v.parse::<u64>() {
                    return Ok(RangeValue::U64(int));
                }

                if let Ok(int) = v.parse::<i64>() {
                    return Ok(RangeValue::I64(int));
                }

                if let Ok(int) = v.parse::<f64>() {
                    return Ok(RangeValue::F64(int));
                }

                Err(serde::de::Error::invalid_value(
                    serde::de::Unexpected::Str(v),
                    &"2002-10-02T15:00:00.05Z",
                ))
            }
        }

        deserializer.deserialize_any(ValueVisitor)
    }
}
