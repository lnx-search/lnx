use anyhow::{Result, Error};
use hashbrown::HashMap;

use tantivy::schema::Schema;
use tantivy::Document;

use crate::structures::FieldValue;

pub type DocumentPayload = HashMap<String, FieldValue>;

pub trait FromValue {
    fn from_value_map(payload: DocumentPayload, schema: Schema) -> Result<Document>;
}


impl FromValue for Document {
    fn from_value_map(mut payload: DocumentPayload, schema: Schema) -> Result<Document>  {
        let mut document = Document::new();

        for (key, value) in payload.drain() {
            let field = schema.get_field(&key);

            if field.is_none() {
                return Err(Error::msg("unknown field given in payload"))
            }

            match value {
                FieldValue::F64(v) => document.add_f64(field.unwrap(), v),
                FieldValue::I64(v) => document.add_i64(field.unwrap(), v),
                FieldValue::U64(v) => document.add_u64(field.unwrap(), v),
                FieldValue::Datetime(v) => document.add_date(field.unwrap(), &v),
                FieldValue::Bytes(v) => document.add_bytes(field.unwrap(), v),
                FieldValue::Text(v) => document.add_text(field.unwrap(), v),
            }
        }

        Ok(document)
    }
}