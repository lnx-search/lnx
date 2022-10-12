mod field;

use std::collections::BTreeMap;
use tantivy::Score;

use field::Field;

#[derive(
    Debug,
    Clone,
    validator::Validate,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    utoipa::ToSchema,
)]
#[validate(schema(function = "validators::validate_schema", skip_on_field_errors = false))]
/// A index schema.
///
/// The schema contains all the information required to create
/// and manage the index itself, including tokenizers, fields, and validation.
pub struct Schema {
    #[schema(example = "my-index")]
    #[validate(length(min = 2, max = 32), custom = "validators::alpha_numeric")]
    /// The name of the index.
    ///
    /// This can be made up of alpha-numeric characters, `-` and `_`.
    pub name: String,

    #[serde(default)]
    /// The schema's fields.
    ///
    /// If this is set to `null` the system will become schemaless.
    /// This is not recommended for most production systems but is useful for
    /// prototyping.
    pub fields: Option<BTreeMap<String, Field>>,

    #[serde(default)]
    #[schema(example = json!(["title", "body", "tags"]))]
    /// The searchable `text` fields that are used by default when
    /// querying data.
    ///
    /// If this is not passed, the index will default to all searchable
    /// text fields.
    pub search_fields: Vec<String>,

    #[serde(default)]
    #[validate(custom = "validators::validate_boost_fields")]
    #[schema(example = json!({"title": 1.5, "body": 1.0, "tags": 1.2}))]
    /// Additional multipliers to apply to the score of each field.
    ///
    /// This can be used to add bias to you search results for certain fields.
    /// If this is not provided, there will be no towards any particular field bias.
    pub boosted_fields: BTreeMap<String, Score>,
}


mod validators {
    use std::borrow::Cow;
    use std::collections::BTreeMap;
    use serde_json::{json, Value};
    use tantivy::Score;
    use validator::ValidationError;

    use super::Schema;

    pub fn validate_schema(schema: &Schema) -> Result<(), ValidationError> {


        Ok(())
    }

    pub fn alpha_numeric(value: &str) -> Result<(), ValidationError> {
        let mut err = ValidationError::new("bad_str");
        err.params.insert(Cow::Borrowed("value"), json!(value));

        if value.chars().any(|c| !(c.is_alphanumeric() || c == '-' || c == '_')) {
            err.message = Some(Cow::Borrowed("Value must only contain alpha-numeric characters, `-` or `_`"));

            return Err(err)
        }

        if value.starts_with('-') || value.starts_with('_') {
            err.message = Some(Cow::Borrowed("Value must not start with `-` or `_`"));
            return Err(err)
        }

        if value.ends_with('-') || value.ends_with('_') {
            err.message = Some(Cow::Borrowed("Value must not end with `-` or `_`"));
            return Err(err)
        }

        Ok(())
    }

    pub fn validate_boost_fields(map: &BTreeMap<String, Score>) -> Result<(), ValidationError> {
        let mut error = ValidationError::new("bad_str");
        error.message = Some(Cow::Borrowed("The boosting multiplier must be greater than `0.0`."));

        let param_key = Cow::Borrowed("fields");

        for (field, &score) in map {
            if score < 0.0f32 {
                error.params
                    .entry(param_key.clone())
                    .and_modify(|v| {
                        if let Value::Array(inner) = v {
                            inner.push(field.clone().into())
                        }
                    })
                    .or_insert_with(|| Value::Array(vec![field.clone().into()]));
            }
        }

        Ok(())
    }
}




#[cfg(test)]
mod tests {
    use utoipa::OpenApi;

    use super::*;

    #[test]
    fn test_utopia() {
        #[derive(OpenApi)]
        #[openapi(
            components(schemas(Schema)),
            tags(
                (name = "todo", description = "Todo items management API")
            )
        )]
        struct ApiDoc;

        tantivy::schema::BytesOptions::default();

        let doc = ApiDoc::openapi();
        println!("{}", doc.to_pretty_json().unwrap());
    }
}