use std::borrow::Cow;

use lnx_common::configuration::NUM_SEGMENTS;
use lnx_common::types::document::{DocField, DocId, TypeSafeDocument};
use lnx_storage::types::{SegmentId, Timestamp};
use lnx_utils::{FromBytes, ToBytes};
use scylla::cql_to_rust::{FromCqlValError, FromRowError};
use scylla::frame::response::result::Row;
use scylla::frame::value::{SerializedResult, SerializedValues, ValueList, ValueTooBig};
use lnx_common::schema::Schema;
use lnx_common::types::Value;

#[derive(Debug)]
pub struct ScyllaSafeDocument<'a>(pub DocId, pub &'a TypeSafeDocument);

impl<'a> ScyllaSafeDocument<'a> {
    pub fn from_row_and_layout(
        mut row: Row,
        layout: &[String],
    ) -> Result<(DocId, SegmentId, TypeSafeDocument), FromRowError> {
        let doc_id = row
            .columns
            .remove(0)
            .ok_or(FromRowError::WrongRowSize {
                expected: 2 + layout.len(),
                actual: 0,
            })?
            .as_uuid()
            .ok_or(FromRowError::BadCqlVal {
                err: FromCqlValError::BadCqlType,
                column: 0,
            })?;

        let token_id = row
            .columns
            .remove(0)
            .ok_or(FromRowError::WrongRowSize {
                expected: 2 + layout.len(),
                actual: 1,
            })?
            .as_bigint()
            .ok_or(FromRowError::BadCqlVal {
                err: FromCqlValError::BadCqlType,
                column: 1,
            })?;

        let mut items = Vec::with_capacity(layout.len());
        for (column, value) in layout.iter().zip(row.columns) {
            let value = match value {
                None => DocField::Empty,
                Some(v) => match v.into_blob() {
                    None => DocField::Empty,
                    Some(b) => DocField::from_bytes(&b).unwrap(),
                },
            };

            items.push((column.to_string(), value))
        }

        Ok((doc_id, token_id % NUM_SEGMENTS, TypeSafeDocument(items)))
    }
}

impl<'a> ValueList for ScyllaSafeDocument<'a> {
    fn serialized(&self) -> SerializedResult<'_> {
        let mut result = SerializedValues::with_capacity(self.1.len() + 2);
        result.add_value(&self.0)?; // PK

        for (_, value) in self.1.iter() {
            match value {
                DocField::Empty => {}
                DocField::Single(v) => {}
                DocField::Multi(v) => {}
            }
        }

        Ok(Cow::Owned(result))
    }
}


fn serialize_value(
    result: &mut SerializedValues,
    value: &Value,
) -> Result<(), scylla::frame::value::SerializeValuesError> {
    Vec::serialize()
    match value {
        Value::I64(v) => result.add_value(v),
        Value::U64(v) => result.add_value(&(*v as i64)),
        Value::F64(v) => result.add_value(v),
        Value::DateTime(v) => result.add_value(&Timestamp::from(v.timestamp_millis())),
        Value::Text(v) => result.add_value(v),
        Value::Bytes(v) => result.add_value(v),
        Value::Json(v) => todo!(),
    }
}