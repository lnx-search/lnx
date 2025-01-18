use tantivy::schema::Field;

use crate::validate::{ErrorCode, ValidationError, ValidatorContext};

/// Validates that the provided fields can be distinct.
///
/// An error is returned if the fields are not all marked as fast.
pub fn validate_can_be_distinct(
    context: &mut ValidatorContext,
    schema: &tantivy::schema::Schema,
    fields: &[Field],
) -> Result<(), ValidationError> {
    for (idx, field) in fields.iter().enumerate() {
        context.push_location(idx);

        let entry = schema.get_field_entry(*field);
        if !entry.is_fast() {
            let help = "distinct queries require all fields being selected to have a columnar index, \
            all fields are `columnar: true` by default".to_string();
            let message = format!("field {:?} has no columnar index", entry.name());
            let error =
                context.build_error(ErrorCode::FieldIsNotFast, message, Some(help));
            return Err(error);
        }

        context.pop_location();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use tantivy::schema::{FAST, STORED};

    use super::*;

    #[test]
    fn test_validate_and_get_table_rejects_non_fast_field() {
        let mut schema_builder = tantivy::schema::Schema::builder();
        let example_field = schema_builder.add_text_field("example", STORED);
        let schema = schema_builder.build();

        let mut context = ValidatorContext::default();
        let error = validate_can_be_distinct(&mut context, &schema, &[example_field])
            .expect_err("Validator should reject non-fast fields");
        assert_eq!(error.code, ErrorCode::FieldIsNotFast);
        assert_eq!(
            error.help.as_deref(),
            Some("distinct queries require all fields being selected to have a columnar index, \
            all fields are `columnar: true` by default"),
        );
        assert_eq!(error.message, "field \"example\" has no columnar index");
        assert_eq!(error.location, "[0]");
    }

    #[test]
    fn test_validate_and_get_table_accepts_known_table() {
        let mut schema_builder = tantivy::schema::Schema::builder();
        let example_field = schema_builder.add_text_field("example", STORED | FAST);
        let schema = schema_builder.build();

        let mut context = ValidatorContext::default();
        validate_can_be_distinct(&mut context, &schema, &[example_field])
            .expect("Validator should allow fast fields");
    }
}
