mod field;
mod writer;

use std::collections::BTreeMap;

pub use field::{BaseOptions, Field, NumericFieldOptions, TextOptions};
use tantivy::Score;
pub use writer::WriterSettings;

pub static RESERVED_DOCUMENT_ID_FIELD: &str = "_lnx_doc_id";

#[derive(
    Debug,
    Clone,
    validator::Validate,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[validate(schema(
    function = "validators::validate_schema",
    skip_on_field_errors = true
))]
/// A index schema.
///
/// The schema contains all the information required to create
/// and manage the index itself, including tokenizers, fields, and validation.
pub struct Schema {
    #[validate(length(min = 2, max = 32), custom = "validators::validate_index_name")]
    /// The name of the index.
    ///
    /// This can be made up of alpha-numeric characters, `-` and `_`.
    pub name: String,

    #[serde(default)]
    /// Options and configuration for indexing documents.
    pub indexing_options: WriterSettings,

    #[serde(default)]
    #[validate(custom = "validators::validate_schema_fields")]
    /// The schema's fields.
    ///
    /// If this is set to `null` the system will become schema-less.
    /// This is not recommended for most production systems but is useful for
    /// prototyping.
    pub fields: Option<BTreeMap<String, Field>>,

    #[serde(default)]
    /// The searchable `text` fields that are used by default when
    /// querying data.
    ///
    /// If this is not passed, the index will default to all searchable
    /// text fields.
    pub search_fields: Vec<String>,

    #[serde(default)]
    #[validate(custom = "validators::validate_boost_fields")]
    /// Additional multipliers to apply to the score of each field.
    ///
    /// This can be used to add bias to you search results for certain fields.
    /// If this is not provided, there will be no towards any particular field bias.
    pub boosted_fields: BTreeMap<String, Score>,
}

mod validators {
    use std::borrow::Cow;
    use std::collections::{BTreeMap, HashSet};

    use serde_json::{json, Value};
    use tantivy::Score;
    use validator::ValidationError;

    use super::{Field, Schema};

    pub fn validate_schema(schema: &Schema) -> Result<(), ValidationError> {
        let fields = match &schema.fields {
            None => return Ok(()),
            Some(fields) => fields,
        };

        let mut error = ValidationError::new("bad_schema");
        error.message = Some(Cow::Borrowed(
            "Search fields and boost fields must be defined within the `fields` property when not using the schema-less system.",
        ));

        let keys = fields.keys().cloned().collect::<HashSet<String>>();

        let param_key = Cow::Borrowed("search_fields");
        let mut is_fail = false;
        for field_name in schema.search_fields.iter() {
            if keys.contains(field_name) {
                continue;
            }

            is_fail = true;
            error
                .params
                .entry(param_key.clone())
                .and_modify(|v| {
                    if let Value::Array(inner) = v {
                        inner.push(json!(field_name))
                    }
                })
                .or_insert_with(|| json!(vec![field_name.clone()]));
        }

        let param_key = Cow::Borrowed("boost_fields");
        for field_name in schema.search_fields.iter() {
            if keys.contains(field_name) {
                continue;
            }

            is_fail = true;
            error
                .params
                .entry(param_key.clone())
                .and_modify(|v| {
                    if let Value::Array(inner) = v {
                        inner.push(json!(field_name))
                    }
                })
                .or_insert_with(|| json!(vec![field_name.clone()]));
        }

        if is_fail {
            return Err(error);
        }

        Ok(())
    }

    pub fn validate_schema_fields(
        fields: &BTreeMap<String, Field>,
    ) -> Result<(), ValidationError> {
        let mut error = ValidationError::new("bad_field_name");

        if fields.is_empty() {
            error.message = Some(Cow::Borrowed(
                "Must contain at least one field or be set to `null`.",
            ));
            return Err(error);
        }

        error.message = Some(Cow::Borrowed(
            "The field name must only contain alpha-numeric characters, `-` or `_` and must not start or end with `-` or `_`.",
        ));

        let param_key = Cow::Borrowed("keys");
        let mut is_fail = false;
        for field_name in fields.keys() {
            if field_name.starts_with('-')
                || field_name.starts_with('_')
                || field_name.ends_with('-')
                || field_name.ends_with('_')
                || field_name
                    .chars()
                    .any(|c| !(c.is_alphanumeric() || c == '-' || c == '_'))
            {
                is_fail = true;
                error
                    .params
                    .entry(param_key.clone())
                    .and_modify(|v| {
                        if let Value::Array(inner) = v {
                            inner.push(json!(field_name))
                        }
                    })
                    .or_insert_with(|| json!(vec![field_name.clone()]));
            }
        }

        if is_fail {
            return Err(error);
        }

        Ok(())
    }

    pub fn validate_index_name(value: &str) -> Result<(), ValidationError> {
        let mut err = ValidationError::new("bad_index_name");
        err.params.insert(Cow::Borrowed("value"), json!(value));

        if value
            .chars()
            .any(|c| !(c.is_alphanumeric() || c == '-' || c == '_'))
        {
            err.message = Some(Cow::Borrowed(
                "Value must only contain alpha-numeric characters, `-` or `_`",
            ));

            return Err(err);
        }

        if value.starts_with('-') || value.starts_with('_') {
            err.message = Some(Cow::Borrowed("Value must not start with `-` or `_`"));
            return Err(err);
        }

        if value.ends_with('-') || value.ends_with('_') {
            err.message = Some(Cow::Borrowed("Value must not end with `-` or `_`"));
            return Err(err);
        }

        Ok(())
    }

    pub fn validate_boost_fields(
        map: &BTreeMap<String, Score>,
    ) -> Result<(), ValidationError> {
        let mut error = ValidationError::new("bad_boost_factor");
        error.message = Some(Cow::Borrowed(
            "The boosting multiplier must be greater than `0.0`.",
        ));

        let param_key = Cow::Borrowed("fields");

        for (field, &score) in map {
            if score < 0.0f32 {
                error
                    .params
                    .entry(param_key.clone())
                    .and_modify(|v| {
                        if let Value::Array(inner) = v {
                            inner.push(json!(field))
                        }
                    })
                    .or_insert_with(|| json!(vec![field.clone()]));
            }
        }

        Ok(())
    }
}
