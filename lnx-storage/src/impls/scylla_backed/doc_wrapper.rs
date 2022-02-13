use std::borrow::Cow;

use hashbrown::HashMap;
use lnx_common::schema::FieldName;
use lnx_common::types::document::{DocField, Document};
use lnx_utils::{FromBytes, ToBytes};
use scylla::cql_to_rust::{FromCqlValError, FromRowError};
use scylla::frame::response::result::Row;
use scylla::frame::value::{
    SerializeValuesError,
    SerializedResult,
    SerializedValues,
    ValueList,
};

use crate::DocId;

pub static DOCUMENT_PRIMARY_KEY: &str = "_lnx_doc_id";

#[derive(Debug)]
pub struct ScyllaSafeDocument(pub DocId, pub Document);

impl ScyllaSafeDocument {
    #[inline]
    pub fn into_parts(self) -> (DocId, Document) {
        (self.0, self.1)
    }

    pub fn from_row_and_layout(
        mut row: Row,
        layout: Vec<String>,
    ) -> Result<Self, FromRowError> {
        let doc_id = row
            .columns
            .remove(0)
            .ok_or_else(|| FromRowError::WrongRowSize {
                expected: 1 + layout.len(),
                actual: 0,
            })?
            .as_uuid()
            .ok_or(FromRowError::BadCqlVal {
                err: FromCqlValError::BadCqlType,
                column: 0,
            })?;

        let mut items = HashMap::with_capacity(layout.len());
        for (i, (column, value)) in layout.into_iter().zip(row.columns).enumerate() {
            match value {
                None => items.insert(FieldName(column), DocField::Empty),
                Some(v) => {
                    let field = match v.as_blob() {
                        Some(b) => DocField::from_bytes(b).map_err(|_| {
                            FromRowError::BadCqlVal {
                                err: FromCqlValError::BadCqlType,
                                column: i + 1,
                            }
                        })?,
                        None => DocField::Empty,
                    };

                    items.insert(FieldName(column), field)
                },
            };
        }

        Ok(Self(doc_id, Document(items)))
    }
}

impl ValueList for ScyllaSafeDocument {
    fn serialized(&self) -> SerializedResult<'_> {
        let mut result = SerializedValues::with_capacity(self.1.len());
        result.add_named_value(DOCUMENT_PRIMARY_KEY, &self.0)?;

        for (name, field) in self.1.iter() {
            let buff = match field {
                DocField::Empty => None,
                other => Some(
                    other
                        .to_bytes()
                        .map_err(|_| SerializeValuesError::ParseError)?,
                ),
            };

            result.add_named_value(&name, &buff)?;
        }

        Ok(Cow::Owned(result))
    }
}
