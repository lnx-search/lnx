use std::collections::BTreeSet;

use tantivy::schema::Field;

use crate::validate::{ErrorCode, ValidationError, ValidatorContext};

/// Additional field requirements.
pub mod field_requirements {
    /// Any field is valid as long as it exists.
    pub const ANY: usize = 0;
    /// The field must be indexed.
    pub const INDEXED: usize = 1 << 0;
    /// The field must be fast.
    pub const FAST: usize = 1 << 1;
}

/// Validates that the provided query fields are correct and returns
/// the field IDs associated with the tantivy schema.
pub fn validate_and_get_query_fields(
    context: &mut ValidatorContext,
    table_name: &str,
    schema: &tantivy::schema::Schema,
    fields: &[String],
    requirements: usize,
) -> Result<Vec<Field>, ValidationError> {
    if fields.is_empty() {
        let message = "at least one query field must be specified".to_string();
        let error = context.build_error(ErrorCode::MissingQueryFields, message, None);
        return Err(error);
    }

    let existing_fields = schema
        .fields()
        .map(|(_, entry)| entry.name())
        .collect::<Vec<_>>();

    let needs_fast = requirements & field_requirements::FAST != 0;
    let needs_indexed = requirements & field_requirements::INDEXED != 0;

    let mut seen = BTreeSet::new();
    let mut field_ids = Vec::with_capacity(fields.len());
    for (idx, field_name) in fields.iter().enumerate() {
        context.push_location(idx);

        let field = schema.get_field(field_name).map_err(|_| {
            make_field_does_not_exist_error(
                context,
                table_name,
                field_name,
                &existing_fields,
            )
        })?;

        let entry = schema.get_field_entry(field);
        if needs_indexed && !entry.is_indexed() {
            return Err(make_field_not_indexed_error(
                context, table_name, field_name,
            ));
        }

        if needs_fast && !entry.is_fast() {
            return Err(make_field_not_fast_error(context, table_name, field_name));
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

/// Validates that the provided boost multiplies is within acceptable ranges.
pub fn validate_boost(
    context: &mut ValidatorContext,
    boost: f32,
) -> Result<(), ValidationError> {
    if boost < 0.0 {
        let help = "the boost value is a multiplier which is applied to the score which cannot be less than 0.0".to_string();
        let message =
            format!("boost multiplier values cannot be less than 0.0, got: {boost}");
        let error =
            context.build_error(ErrorCode::InvalidBoostMultiplier, message, Some(help));
        Err(error)
    } else {
        Ok(())
    }
}

/// Validates and produces a [regex::Regex] from the provided pattern.
pub fn validate_and_get_regex(
    context: &mut ValidatorContext,
    pattern: &str,
) -> Result<regex::Regex, ValidationError> {
    match regex::Regex::new(pattern) {
        Ok(re) => Ok(re),
        Err(error) => {
            let help = "you can find the regex syntax lnx supports here: https://docs.rs/regex/latest/regex/#syntax".to_string();
            let message = format!("invalid regex pattern: {error}");
            let error =
                context.build_error(ErrorCode::InvalidRegexPattern, message, Some(help));
            Err(error)
        },
    }
}

fn make_field_not_indexed_error(
    context: &ValidatorContext,
    table_name: &str,
    field_name: &str,
) -> ValidationError {
    let help =
        "the query type you are trying to use requires the field be indexed in order to be \
        used, make sure `indexed: true` is set for the field in your index settings".to_string();
    let message =
        format!("the field {field_name:?} within table {table_name:?} is not indexed");
    context.build_error(ErrorCode::FieldIsNotIndexed, message, Some(help))
}

fn make_field_not_fast_error(
    context: &ValidatorContext,
    table_name: &str,
    field_name: &str,
) -> ValidationError {
    let help =
        "the query type you are trying to use requires the field have a columnar index in order to be \
        used, this is enabled by default which means you may have explicitly \
        disabled this option for this field".to_string();
    let message = format!(
        "the field {field_name:?} in table {table_name:?} has no columnar index"
    );
    context.build_error(ErrorCode::FieldIsNotFast, message, Some(help))
}

fn make_field_does_not_exist_error(
    context: &ValidatorContext,
    table_name: &str,
    field_name: &str,
    fields: &[&str],
) -> ValidationError {
    let help = fields
        .iter()
        .filter(|name| crate::utils::damerau_levenshtein(field_name, name) <= 3)
        .min_by_key(|name| crate::utils::damerau_levenshtein(field_name, name))
        .map(|closest| format!("did you mean {closest:?}?"));

    let message =
        format!("the field {field_name:?} does not exist within table {table_name:?}");
    context.build_error(ErrorCode::UnknownField, message, help)
}

fn make_duplicate_field_error(
    context: &ValidatorContext,
    table_name: &str,
    field_name: &str,
) -> ValidationError {
    let help = "remove one of the duplicate fields from the `$fields` attribute so all field names are unique"
        .to_string();
    let message = format!(
        "the field {field_name:?} within table {table_name:?} has already been declared"
    );
    context.build_error(ErrorCode::DuplicateField, message, Some(help))
}

#[cfg(test)]
mod tests {
    use tantivy::schema::{FAST, STORED, TEXT};

    use super::*;

    #[test]
    fn test_validate_and_get_query_fields_any_valid_field() {
        let mut schema_builder = tantivy::schema::Schema::builder();
        schema_builder.add_text_field("example", STORED);
        let schema = schema_builder.build();

        validate_and_get_query_fields(
            &mut ValidatorContext::default(),
            "test",
            &schema,
            &["example".into()],
            field_requirements::ANY,
        )
        .expect("Validator should allow parameters");
    }

    #[test]
    fn test_validate_and_get_query_fields_indexed_valid_field() {
        let mut schema_builder = tantivy::schema::Schema::builder();
        schema_builder.add_text_field("example", STORED | TEXT);
        let schema = schema_builder.build();

        validate_and_get_query_fields(
            &mut ValidatorContext::default(),
            "test",
            &schema,
            &["example".into()],
            field_requirements::INDEXED,
        )
        .expect("Validator should allow parameters");
    }

    #[test]
    fn test_validate_and_get_query_fields_fast_valid_field() {
        let mut schema_builder = tantivy::schema::Schema::builder();
        schema_builder.add_text_field("example", STORED | FAST);
        let schema = schema_builder.build();

        validate_and_get_query_fields(
            &mut ValidatorContext::default(),
            "test",
            &schema,
            &["example".into()],
            field_requirements::FAST,
        )
        .expect("Validator should allow parameters");
    }

    #[test]
    fn test_validate_and_get_query_fields_reject_non_indexed_field() {
        let mut schema_builder = tantivy::schema::Schema::builder();
        schema_builder.add_text_field("example", STORED);
        let schema = schema_builder.build();

        let error = validate_and_get_query_fields(
            &mut ValidatorContext::default(),
            "test",
            &schema,
            &["example".into()],
            field_requirements::INDEXED,
        )
        .expect_err("Validator should not allow parameters");
        assert_eq!(error.code, ErrorCode::FieldIsNotIndexed);
        assert_eq!(error.location, "[0]");
        assert_eq!(
            error.message,
            "the field \"example\" within table \"test\" is not indexed"
        );
        assert_eq!(
            error.help.as_deref(),
            Some(
                "the query type you are trying to use requires the field be \
            indexed in order to be used, make sure `indexed: true` is set for \
            the field in your index settings"
            )
        );
    }

    #[test]
    fn test_validate_and_get_query_fields_reject_non_fast_field() {
        let mut schema_builder = tantivy::schema::Schema::builder();
        schema_builder.add_text_field("example", STORED);
        let schema = schema_builder.build();

        let error = validate_and_get_query_fields(
            &mut ValidatorContext::default(),
            "test",
            &schema,
            &["example".into()],
            field_requirements::FAST,
        )
        .expect_err("Validator should not allow parameters");
        assert_eq!(error.code, ErrorCode::FieldIsNotFast);
        assert_eq!(error.location, "[0]");
        assert_eq!(
            error.message,
            "the field \"example\" in table \"test\" has no columnar index"
        );
        assert_eq!(
            error.help.as_deref(),
            Some("the query type you are trying to use requires the field have a columnar index in \
            order to be used, this is enabled by default which means you may have explicitly \
            disabled this option for this field")
        );
    }

    #[test]
    fn test_validate_and_get_query_fields_reject_unknown_field() {
        let mut schema_builder = tantivy::schema::Schema::builder();
        schema_builder.add_text_field("example", STORED);
        let schema = schema_builder.build();

        let error = validate_and_get_query_fields(
            &mut ValidatorContext::default(),
            "test",
            &schema,
            &["exampl".into()],
            field_requirements::ANY,
        )
        .expect_err("Validator should not allow parameters");
        assert_eq!(error.code, ErrorCode::UnknownField);
        assert_eq!(error.location, "[0]");
        assert_eq!(
            error.message,
            "the field \"exampl\" does not exist within table \"test\""
        );
        assert_eq!(error.help.as_deref(), Some("did you mean \"example\"?"));
    }

    #[test]
    fn test_validate_and_get_query_fields_reject_duplicate_field() {
        let mut schema_builder = tantivy::schema::Schema::builder();
        schema_builder.add_text_field("example", STORED);
        let schema = schema_builder.build();

        let error = validate_and_get_query_fields(
            &mut ValidatorContext::default(),
            "test",
            &schema,
            &["example".into(), "example".into()],
            field_requirements::ANY,
        )
        .expect_err("Validator should not allow parameters");
        assert_eq!(error.code, ErrorCode::DuplicateField);
        assert_eq!(error.location, "[1]");
        assert_eq!(
            error.message,
            "the field \"example\" within table \"test\" has already been declared"
        );
        assert_eq!(error.help.as_deref(), Some("remove one of the duplicate fields from the `$fields` attribute so all field names are unique"));
    }

    #[test]
    fn test_validate_and_get_query_fields_reject_empty_field() {
        let mut schema_builder = tantivy::schema::Schema::builder();
        schema_builder.add_text_field("example", STORED);
        let schema = schema_builder.build();

        let error = validate_and_get_query_fields(
            &mut ValidatorContext::default(),
            "test",
            &schema,
            &[],
            field_requirements::ANY,
        )
        .expect_err("Validator should not allow parameters");
        assert_eq!(error.code, ErrorCode::MissingQueryFields);
        assert_eq!(error.location, "");
        assert_eq!(error.message, "at least one query field must be specified");
        assert_eq!(error.help.as_deref(), None);
    }

    #[test]
    fn test_validate_boost() {
        validate_boost(&mut ValidatorContext::default(), 1.0)
            .expect("Validator should allow boost value");

        validate_boost(&mut ValidatorContext::default(), 0.0)
            .expect("Validator should allow boost value");

        validate_boost(&mut ValidatorContext::default(), 1000.0)
            .expect("Validator should allow boost value");

        let error = validate_boost(&mut ValidatorContext::default(), -1.0)
            .expect_err("Validator should reject boost value");
        assert_eq!(error.code, ErrorCode::InvalidBoostMultiplier);
        assert_eq!(error.location, "");
        assert_eq!(
            error.message,
            "boost multiplier values cannot be less than 0.0, got: -1"
        );
        assert_eq!(error.help.as_deref(), Some("the boost value is a multiplier which is applied to the score which cannot be less than 0.0"));
    }

    #[test]
    fn test_validate_and_get_regex() {
        validate_and_get_regex(&mut ValidatorContext::default(), "(?:\\w+)")
            .expect("Validator should allow boost value");

        validate_and_get_regex(&mut ValidatorContext::default(), "[0-9]{1,3}")
            .expect("Validator should allow boost value");

        validate_and_get_regex(&mut ValidatorContext::default(), "hell[oO0]")
            .expect("Validator should allow boost value");

        let error = validate_and_get_regex(&mut ValidatorContext::default(), "[0-9][")
            .expect_err("Validator should reject boost value");
        assert_eq!(error.code, ErrorCode::InvalidRegexPattern);
        assert_eq!(error.location, "");
        assert_eq!(error.message, "invalid regex pattern: regex parse error:\n    [0-9][\n         ^\nerror: unclosed character class");
        assert_eq!(error.help.as_deref(), Some("you can find the regex syntax lnx supports here: https://docs.rs/regex/latest/regex/#syntax"));
    }
}
