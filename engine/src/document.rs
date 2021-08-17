use anyhow::{Error, Result};
use hashbrown::{HashMap, HashSet};

use tantivy::schema::Schema;
use tantivy::Document;

use crate::structures::FieldValue;

pub type DocumentPayload = HashMap<String, FieldValue>;

macro_rules! add_values {
    ($doc:ident.$cb:ident, $field:expr, $sv:expr) => {{
        for value in $sv {
            $doc.$cb($field, value);
        }
    }}
}

pub trait FromValue {
    fn from_value_map(
        payload: DocumentPayload,
        schema: &Schema,
        allowed_fields: &HashSet<String>,
    ) -> Result<Document>;

    fn from_many_value_map(
        payload: Vec<DocumentPayload>,
        schema: &Schema,
        allowed_fields: &HashSet<String>,
    ) -> Result<Vec<Document>>;
}

impl FromValue for Document {
    fn from_value_map(
        mut payload: DocumentPayload,
        schema: &Schema,
        allowed_fields: &HashSet<String>,
    ) -> Result<Document> {
        if payload.len() != allowed_fields.len() {
            let mut missing: Vec<&str> = vec![];
            for field in allowed_fields.iter() {
                if !payload.contains_key(field) {
                    missing.push(field.as_str())
                }
            }

            let render: String = missing.join("\n");
            return Err(Error::msg(format!(
                "missing required fields according to schema. Missing fields: [{}]",
                render
            )));
        }

        let mut document = Document::new();

        for (key, value) in payload.drain() {
            if !allowed_fields.contains(&key) {
                return Err(Error::msg(format!("unknown field '{}' in payload", key)));
            }

            let field = schema.get_field(&key);

            // This should never happen because of the above check, but just in case.
            if field.is_none() {
                return Err(Error::msg("unknown field given in payload"));
            }

            match value {
                FieldValue::F64(v) => add_values!(document.add_f64, field.unwrap(), v),
                FieldValue::I64(v) => add_values!(document.add_i64, field.unwrap(), v),
                FieldValue::U64(v) => add_values!(document.add_u64, field.unwrap(), v),
                FieldValue::Datetime(v) => add_values!(document.add_date, field.unwrap(), &v),
                FieldValue::Bytes(v) => add_values!(document.add_bytes, field.unwrap(), v),
                FieldValue::Text(v) => add_values!(document.add_text, field.unwrap(), v),
            }
        }

        Ok(document)
    }

    fn from_many_value_map(
        payload: Vec<DocumentPayload>,
        schema: &Schema,
        allowed_fields: &HashSet<String>,
    ) -> Result<Vec<Document>> {
        let mut out = vec![];
        for doc in payload {
            let doc = Self::from_value_map(doc, schema, &allowed_fields)?;

            out.push(doc);
        }

        Ok(out)
    }
}
