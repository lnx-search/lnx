use std::collections::BTreeSet;

use tantivy::schema::Field;

use crate::validate::{ErrorCode, ValidationError, ValidatorContext};

/// Validates the fields that are attempting to be selected by the query.
pub fn validate_select_fields(
    context: &mut ValidatorContext,
    table_name: &str,
    schema: &tantivy::schema::Schema,
    fields: &[String],
) -> Result<Vec<Field>, ValidationError> {
    if fields.is_empty() {
        let message =
            "at least one field or wildcard must be provided to the `$select` field"
                .to_string();
        let help = "you can use a wildcard to select all fields: `$select: [\"*\"]`"
            .to_string();
        let error =
            context.build_error(ErrorCode::MissingSelectFields, message, Some(help));
        return Err(error);
    }

    if fields.len() == 1 && fields[0] == "*" {
        let all_fields = schema
            .fields()
            .filter(|(_, entry)| entry.is_stored())
            .map(|(field_id, _)| field_id)
            .collect();
        return Ok(all_fields);
    }

    let existing_fields = schema
        .fields()
        .map(|(_, entry)| entry.name())
        .collect::<Vec<_>>();

    let mut seen = BTreeSet::new();
    let mut field_ids = Vec::with_capacity(fields.len());
    for (idx, field_name) in fields.iter().enumerate() {
        context.push_location(idx);

        if field_name == "*" {
            return Err(make_bad_wildcard_error(context));
        }

        let field = schema.get_field(field_name).map_err(|_| {
            make_field_does_not_exist_error(
                context,
                table_name,
                field_name,
                &existing_fields,
            )
        })?;

        let entry = schema.get_field_entry(field);
        if !entry.is_stored() {
            return Err(make_field_not_stored_error(context, table_name, field_name));
        }

        let did_insert = seen.insert(field_name);
        if !did_insert {
            return Err(make_duplicate_field_error(context, table_name, field_name));
        }

        field_ids.push(field);
        context.pop_location();
    }

    Ok(field_ids)
}

fn make_field_does_not_exist_error(
    context: &ValidatorContext,
    table_name: &str,
    field_name: &str,
    fields: &[&str],
) -> ValidationError {
    let help = if field_name.trim() == "*" {
        "did you mean \"*\"?".to_string()
    } else {
        fields
            .iter()
            .filter(|name| crate::utils::damerau_levenshtein(field_name, name) <= 3)
            .min_by_key(|name| crate::utils::damerau_levenshtein(field_name, name))
            .map(|closest| format!("did you mean {closest:?}?"))
            .unwrap_or_else(|| "you can use a wildcard `*` to select all fields and see what fields are available".to_string())
    };

    let message =
        format!("the field {field_name:?} does not exist within table {table_name:?}");
    context.build_error(ErrorCode::UnknownField, message, Some(help))
}

fn make_field_not_stored_error(
    context: &ValidatorContext,
    table_name: &str,
    field_name: &str,
) -> ValidationError {
    let help =
        "fields are always stored by default unless explicitly disabled, to re-enable \
     storing after it has been disabled requires deleting and re-creating the table"
            .to_string();
    let message =
        format!("the field {field_name:?} within table {table_name:?} is not stored");
    context.build_error(ErrorCode::FieldIsNotStored, message, Some(help))
}

fn make_bad_wildcard_error(context: &ValidatorContext) -> ValidationError {
    let help =
        "remove one of either the wildcard or explicitly declared fields".to_string();
    let message = "wildcard field was not mutually exclusive, must be either a \
    single wildcard or many explicitly declared fields"
        .to_string();
    context.build_error(ErrorCode::WildcardNotAllowed, message, Some(help))
}

fn make_duplicate_field_error(
    context: &ValidatorContext,
    table_name: &str,
    field_name: &str,
) -> ValidationError {
    let help = "remove one of the duplicate fields from the select so all field names are unique"
            .to_string();
    let message = format!(
        "the field {field_name:?} within table {table_name:?} has already been declared"
    );
    context.build_error(ErrorCode::DuplicateField, message, Some(help))
}

#[cfg(test)]
mod tests {
    use tantivy::schema::{FAST, STORED};

    use super::*;

    #[test]
    fn test_validate_select_fields_wildcard() {
        let mut schema_builder = tantivy::schema::Schema::builder();
        schema_builder.add_text_field("example", STORED);
        let schema = schema_builder.build();

        let mut context = ValidatorContext::default();
        validate_select_fields(&mut context, "test", &schema, &["*".to_string()])
            .expect("Validator should allow wildcard");
    }

    #[test]
    fn test_validate_select_fields_rejects_wildcard_and_other() {
        let mut schema_builder = tantivy::schema::Schema::builder();
        schema_builder.add_text_field("example", STORED);
        let schema = schema_builder.build();

        let mut context = ValidatorContext::default();
        let error = validate_select_fields(
            &mut context,
            "test",
            &schema,
            &["*".to_string(), "other".to_string()],
        )
        .expect_err("Validator should reject wildcard + additional fields");
        assert_eq!(error.code, ErrorCode::WildcardNotAllowed);
        assert_eq!(
            error.help.as_deref(),
            Some("remove one of either the wildcard or explicitly declared fields"),
        );
        assert_eq!(
            error.message,
            "wildcard field was not mutually exclusive, must be either a \
            single wildcard or many explicitly declared fields"
        );
        assert_eq!(error.location, "[0]");
    }

    #[test]
    fn test_validate_select_fields_valid_fields() {
        let mut schema_builder = tantivy::schema::Schema::builder();
        schema_builder.add_text_field("example", STORED);
        let schema = schema_builder.build();

        let mut context = ValidatorContext::default();
        validate_select_fields(&mut context, "test", &schema, &["example".to_string()])
            .expect("Validator should allow valid fields");
    }

    #[test]
    fn test_validate_select_fields_rejects_empty_fields() {
        let mut schema_builder = tantivy::schema::Schema::builder();
        schema_builder.add_text_field("example", STORED);
        let schema = schema_builder.build();

        let mut context = ValidatorContext::default();
        let error = validate_select_fields(&mut context, "test", &schema, &[])
            .expect_err("Validator should reject empty fields");
        assert_eq!(error.code, ErrorCode::MissingSelectFields);
        assert_eq!(
            error.help.as_deref(),
            Some("you can use a wildcard to select all fields: `$select: [\"*\"]`"),
        );
        assert_eq!(
            error.message,
            "at least one field or wildcard must be provided to the `$select` field"
        );
        assert_eq!(error.location, "");
    }

    #[test]
    fn test_validate_select_fields_rejects_unknown_field() {
        let mut schema_builder = tantivy::schema::Schema::builder();
        schema_builder.add_text_field("example", STORED);
        let schema = schema_builder.build();

        let mut context = ValidatorContext::default();
        let error = validate_select_fields(
            &mut context,
            "test",
            &schema,
            &["example".to_string(), "lettuce".to_string()],
        )
        .expect_err("Validator should reject unknown fields");
        assert_eq!(error.code, ErrorCode::UnknownField);
        assert_eq!(
            error.help.as_deref(),
            Some("you can use a wildcard `*` to select all fields and see what fields are available"),
        );
        assert_eq!(
            error.message,
            "the field \"lettuce\" does not exist within table \"test\""
        );
        assert_eq!(error.location, "[1]");

        let mut context = ValidatorContext::default();
        let error = validate_select_fields(
            &mut context,
            "test",
            &schema,
            &["exampl".to_string()],
        )
        .expect_err("Validator should reject unknown fields");
        assert_eq!(error.code, ErrorCode::UnknownField);
        assert_eq!(error.help.as_deref(), Some("did you mean \"example\"?"),);
        assert_eq!(
            error.message,
            "the field \"exampl\" does not exist within table \"test\""
        );
        assert_eq!(error.location, "[0]");
    }

    #[test]
    fn test_validate_select_fields_rejects_not_stored() {
        let mut schema_builder = tantivy::schema::Schema::builder();
        schema_builder.add_text_field("example", FAST);
        let schema = schema_builder.build();

        let mut context = ValidatorContext::default();
        let error = validate_select_fields(
            &mut context,
            "test",
            &schema,
            &["example".to_string()],
        )
        .expect_err("Validator should reject field which isn't stored");
        assert_eq!(error.code, ErrorCode::FieldIsNotStored);
        assert_eq!(
            error.help.as_deref(),
            Some("fields are always stored by default unless explicitly disabled, to re-enable \
            storing after it has been disabled requires deleting and re-creating the table"),
        );
        assert_eq!(
            error.message,
            "the field \"example\" within table \"test\" is not stored"
        );
        assert_eq!(error.location, "[0]");
    }

    #[test]
    fn test_validate_select_fields_rejects_duplicates() {
        let mut schema_builder = tantivy::schema::Schema::builder();
        schema_builder.add_text_field("example", STORED);
        let schema = schema_builder.build();

        let mut context = ValidatorContext::default();
        let error = validate_select_fields(
            &mut context,
            "test",
            &schema,
            &["example".to_string(), "example".to_string()],
        )
        .expect_err("Validator should reject duplicate");
        assert_eq!(error.code, ErrorCode::DuplicateField);
        assert_eq!(
            error.help.as_deref(),
            Some("remove on of the duplicate fields from the select so there is all field names are unique"),
        );
        assert_eq!(
            error.message,
            "the field \"example\" within table \"test\" has already been declared"
        );
        assert_eq!(error.location, "[1]");
    }
}
